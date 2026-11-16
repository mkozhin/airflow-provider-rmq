from __future__ import annotations

import json
import logging
from datetime import timedelta
from unittest.mock import MagicMock, patch

import pendulum
import pika.exceptions
import pytest
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models.dag import DAG

from airflow_provider_rmq.sensors.rmq import RMQSensor
from tests.conftest import (
    BATCH_DROPPED_TEXT,
    DROPPED_TEXT,
    EMPTY_BATCH_TEXT,
    ERROR_TEXT,
    STREAK_TEXT,
    log_lines,
    make_method_frame,
    make_properties,
)


HOOK_PATH = "airflow_provider_rmq.sensors.rmq.RMQHook"
#: Start of the batch line when nothing matched, with or without the dropped clause.
_NOTHING_MATCHED_TEXT = "read from queue 'q' passed the filter"


def _make_raw_msg(body="msg", delivery_tag=1, routing_key="rk", exchange="", headers=None):
    return {
        "method": make_method_frame(delivery_tag=delivery_tag, routing_key=routing_key, exchange=exchange),
        "properties": make_properties(headers=headers),
        "body": body,
    }


@pytest.fixture
def mock_hook():
    with patch(HOOK_PATH) as MockHook:
        hook_instance = MagicMock()
        MockHook.return_value = hook_instance
        yield MockHook, hook_instance


# ---------------------------------------------------------------------------
# Poke mode — empty queue
# ---------------------------------------------------------------------------
class TestPokeEmpty:
    def test_empty_queue_returns_false(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = []
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        result = sensor.poke(context={})
        assert result is False

    def test_nonexistent_queue_returns_false(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = []
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        result = sensor.poke(context={})
        assert result is False


# ---------------------------------------------------------------------------
# Poke mode — no filter
# ---------------------------------------------------------------------------
class TestPokeNoFilter:
    def test_first_message_matches(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="hello", delivery_tag=1, headers={"x": "y"}),
        ]
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        result = sensor.poke(context={})
        assert result is True
        assert sensor._return_value == {
            "body": "hello",
            "headers": {"x": "y"},
            "routing_key": "rk",
            "exchange": "",
        }
        hook.ack.assert_called_once_with(1)

    def test_remaining_messages_nacked(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1),
            _make_raw_msg(body="b", delivery_tag=2),
            _make_raw_msg(body="c", delivery_tag=3),
        ]
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        sensor.poke(context={})
        # First message matched and acked
        hook.ack.assert_called_once_with(1)
        # Remaining nacked with requeue
        assert hook.nack.call_count == 2
        hook.nack.assert_any_call(2, requeue=True)
        hook.nack.assert_any_call(3, requeue=True)


# ---------------------------------------------------------------------------
# Poke mode — dict filter
# ---------------------------------------------------------------------------
class TestPokeDictFilter:
    def test_matching_message(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "order"}),
        ]
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            filter_headers={"x-type": "order"},
        )
        result = sensor.poke(context={})
        assert result is True
        assert sensor._return_value["body"] == "b"
        # First non-matching nacked, second matched and acked
        hook.nack.assert_called_once_with(1, requeue=True)
        hook.ack.assert_called_once_with(2)

    def test_no_matching_message(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "invoice"}),
        ]
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            filter_headers={"x-type": "order"},
        )
        result = sensor.poke(context={})
        assert result is False
        assert hook.nack.call_count == 2
        hook.ack.assert_not_called()

    def test_body_filter(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body=json.dumps({"status": "pending"}), delivery_tag=1),
            _make_raw_msg(body=json.dumps({"status": "ready"}), delivery_tag=2),
        ]
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            filter_headers={"body.status": "ready"},
        )
        result = sensor.poke(context={})
        assert result is True
        assert sensor._return_value["body"] == json.dumps({"status": "ready"})


# ---------------------------------------------------------------------------
# Poke mode — callable filter
# ---------------------------------------------------------------------------
class TestPokeCallableFilter:
    def test_callable_matching(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="skip", delivery_tag=1),
            _make_raw_msg(body="important", delivery_tag=2),
        ]
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            filter_callable=lambda props, body: "important" in body,
        )
        result = sensor.poke(context={})
        assert result is True
        assert sensor._return_value["body"] == "important"
        hook.nack.assert_called_once_with(1, requeue=True)
        hook.ack.assert_called_once_with(2)


# ---------------------------------------------------------------------------
# Poke mode — hook close
# ---------------------------------------------------------------------------
class TestPokeHookClose:
    def test_hook_closed_after_poke(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = []
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        sensor.poke(context={})
        hook.close.assert_called_once()

    def test_hook_closed_on_exception(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.side_effect = RuntimeError("boom")
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        with pytest.raises(RuntimeError):
            sensor.poke(context={})
        hook.close.assert_called_once()


# ---------------------------------------------------------------------------
# Poke mode — ChannelClosedByBroker
# ---------------------------------------------------------------------------
class TestPokeChannelClosed:
    def test_returns_false_on_nonexistent_queue(self, mock_hook):
        _, hook = mock_hook
        from pika.exceptions import ChannelClosedByBroker
        hook.consume_messages.side_effect = ChannelClosedByBroker(404, "NOT_FOUND")
        sensor = RMQSensor(task_id="t", queue_name="nonexistent", poke_interval=1)
        result = sensor.poke(context={})
        assert result is False
        hook.close.assert_called_once()


# ---------------------------------------------------------------------------
# Poke mode — configurable batch size
# ---------------------------------------------------------------------------
class TestPokeBatchSize:
    def test_default_batch_size(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = []
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        sensor.poke(context={})
        hook.consume_messages.assert_called_once_with(
            queue_name="q", max_messages=100, auto_ack=False,
        )

    def test_custom_batch_size(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = []
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1, poke_batch_size=50)
        sensor.poke(context={})
        hook.consume_messages.assert_called_once_with(
            queue_name="q", max_messages=50, auto_ack=False,
        )


# ---------------------------------------------------------------------------
# Execute — poke mode returns _return_value via XCom
# ---------------------------------------------------------------------------
class TestExecute:
    def test_execute_poke_returns_value(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="result", delivery_tag=1, headers={"k": "v"}),
        ]
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        # Call poke directly (execute() would call poke in a loop via BaseSensorOperator)
        assert sensor.poke(context={}) is True
        assert sensor._return_value == {
            "body": "result",
            "headers": {"k": "v"},
            "routing_key": "rk",
            "exchange": "",
        }


# ---------------------------------------------------------------------------
# Deferrable mode
# ---------------------------------------------------------------------------
class TestDeferrable:
    def test_defer_calls_self_defer(self, mock_hook):
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            deferrable=True, filter_headers={"x-type": "order"},
        )
        mock_trigger_cls = MagicMock()
        mock_trigger_instance = MagicMock()
        mock_trigger_instance.queue_name = "q"
        mock_trigger_instance.rmq_conn_id = "rmq_default"
        mock_trigger_instance.filter_data = {"filter_headers": {"x-type": "order"}}
        mock_trigger_cls.return_value = mock_trigger_instance

        with patch.object(sensor, "defer") as mock_defer:
            with patch.dict("sys.modules", {"airflow_provider_rmq.triggers.rmq": MagicMock(RMQTrigger=mock_trigger_cls)}):
                sensor._defer()
                mock_defer.assert_called_once()
                call_kwargs = mock_defer.call_args.kwargs
                assert call_kwargs["method_name"] == "execute_complete"
                mock_trigger_cls.assert_called_once()

    def test_deferrable_with_callable_raises(self):
        with pytest.raises(ValueError, match="Callable-based filters are not supported"):
            RMQSensor(
                task_id="t", queue_name="q", poke_interval=1,
                deferrable=True,
                filter_callable=lambda p, b: True,
            )

    def test_deferrable_without_filter_ok(self):
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            deferrable=True,
        )
        assert sensor.deferrable is True


# ---------------------------------------------------------------------------
# execute_complete
# ---------------------------------------------------------------------------
class TestExecuteComplete:
    def test_success_event(self):
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        event = {
            "status": "success",
            "message": {"body": "data", "headers": {}, "routing_key": "rk", "exchange": ""},
        }
        result = sensor.execute_complete(context={}, event=event)
        assert result == {"body": "data", "headers": {}, "routing_key": "rk", "exchange": ""}

    def test_error_event_raises(self):
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        event = {"status": "error", "error": "connection lost"}
        with pytest.raises(RuntimeError, match="connection lost"):
            sensor.execute_complete(context={}, event=event)

    def test_unknown_status_raises(self):
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        event = {"status": "unknown"}
        with pytest.raises(RuntimeError, match="unknown error"):
            sensor.execute_complete(context={}, event=event)

# ---------------------------------------------------------------------------
# Deferrable mode — trigger_mode / message_wait_timeout parameters
# ---------------------------------------------------------------------------
class TestDeferrableNewParams:
    def test_defer_passes_trigger_mode_push_to_trigger(self):
        from airflow_provider_rmq.triggers.rmq import RMQTrigger

        sensor = RMQSensor(
            task_id="t", queue_name="orders", poke_interval=1,
            deferrable=True, trigger_mode="push",
        )
        with patch.object(sensor, "defer") as mock_defer:
            sensor._defer()

        mock_defer.assert_called_once()
        trigger = mock_defer.call_args.kwargs["trigger"]
        assert isinstance(trigger, RMQTrigger)
        assert trigger.mode == "push"

    def test_defer_passes_message_wait_timeout_to_trigger(self):
        from airflow_provider_rmq.triggers.rmq import RMQTrigger

        sensor = RMQSensor(
            task_id="t", queue_name="orders", poke_interval=1,
            deferrable=True, trigger_mode="push", message_wait_timeout=30.0,
        )
        with patch.object(sensor, "defer") as mock_defer:
            sensor._defer()

        trigger = mock_defer.call_args.kwargs["trigger"]
        assert isinstance(trigger, RMQTrigger)
        assert trigger.message_wait_timeout == 30.0

    def test_execute_complete_timeout_raises_skip_exception(self):
        from airflow.exceptions import AirflowSkipException
        sensor = RMQSensor(task_id="t", queue_name="orders", poke_interval=1)
        with pytest.raises(AirflowSkipException, match="orders"):
            sensor.execute_complete(context={}, event={"status": "timeout"})

    def test_message_wait_timeout_with_pull_mode_raises_value_error(self):
        with pytest.raises(
            ValueError, match="message_wait_timeout is only supported with trigger_mode='push'",
        ):
            RMQSensor(
                task_id="t", queue_name="q", poke_interval=1,
                trigger_mode="pull", message_wait_timeout=10,
            )

    def test_message_wait_timeout_with_the_default_trigger_mode_raises_value_error(self):
        with pytest.raises(ValueError, match="trigger_mode='push'"):
            RMQSensor(
                task_id="t", queue_name="q", poke_interval=1,
                deferrable=True, message_wait_timeout=10,
            )

    def test_message_wait_timeout_with_deprecated_mode_push_is_accepted(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            sensor = RMQSensor(
                task_id="t", queue_name="q", poke_interval=1,
                deferrable=True, mode="push", message_wait_timeout=10,
            )
        assert sensor.trigger_mode == "push"
        assert sensor.message_wait_timeout == 10


# ---------------------------------------------------------------------------
# template_fields — message_wait_timeout поддерживает Jinja / XCom
# ---------------------------------------------------------------------------
class TestTemplateFields:
    def test_message_wait_timeout_in_template_fields(self):
        assert "message_wait_timeout" in RMQSensor.template_fields

    def test_queue_name_still_in_template_fields(self):
        assert "queue_name" in RMQSensor.template_fields

    def test_defer_coerces_string_timeout_to_float(self):
        """После рендера Jinja message_wait_timeout приходит строкой — _defer() должен кастовать."""
        from airflow_provider_rmq.triggers.rmq import RMQTrigger

        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            deferrable=True, trigger_mode="push",
        )
        # Имитируем результат рендера Jinja-шаблона: строка вместо float
        sensor.message_wait_timeout = "3600.0"

        with patch.object(sensor, "defer") as mock_defer:
            sensor._defer()

        trigger = mock_defer.call_args.kwargs["trigger"]
        assert isinstance(trigger, RMQTrigger)
        assert trigger.message_wait_timeout == 3600.0
        assert isinstance(trigger.message_wait_timeout, float)

    def test_defer_coerces_integer_string_timeout(self):
        """Целочисленные строки тоже кастуются корректно."""
        from airflow_provider_rmq.triggers.rmq import RMQTrigger

        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            deferrable=True, trigger_mode="push",
        )
        sensor.message_wait_timeout = "1800"

        with patch.object(sensor, "defer") as mock_defer:
            sensor._defer()

        trigger = mock_defer.call_args.kwargs["trigger"]
        assert trigger.message_wait_timeout == 1800.0

    def test_defer_none_timeout_stays_none(self):
        """None не трогается — отсутствие таймаута."""
        from airflow_provider_rmq.triggers.rmq import RMQTrigger

        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            deferrable=True, trigger_mode="push",
        )
        assert sensor.message_wait_timeout is None

        with patch.object(sensor, "defer") as mock_defer:
            sensor._defer()

        trigger = mock_defer.call_args.kwargs["trigger"]
        assert trigger.message_wait_timeout is None


# ---------------------------------------------------------------------------
# ack_unmatched
# ---------------------------------------------------------------------------



class TestAckUnmatched:
    def test_the_rest_of_the_batch_goes_back_to_the_queue(self, mock_hook):
        """The filter never read what sits behind the match, so acking it would be blind."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1),
            _make_raw_msg(body="b", delivery_tag=2),
            _make_raw_msg(body="c", delivery_tag=3),
        ]
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1, ack_unmatched=True,
        )
        assert sensor.poke(context={}) is True
        assert sensor._return_value["body"] == "a"
        hook.ack.assert_called_once_with(1)
        assert [c.args[0] for c in hook.nack.call_args_list] == [2, 3]
        assert all(c.kwargs == {"requeue": True} for c in hook.nack.call_args_list)

    def test_rejected_messages_are_acknowledged(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "receipt"}),
        ]
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            filter_headers={"x-type": "order"}, ack_unmatched=True,
        )
        assert sensor.poke(context={}) is False
        assert [c.args[0] for c in hook.ack.call_args_list] == [1, 2]
        hook.nack.assert_not_called()

    def test_default_returns_rejected_messages_to_the_queue(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "order"}),
        ]
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            filter_headers={"x-type": "order"},
        )
        assert sensor.poke(context={}) is True
        hook.nack.assert_called_once_with(1, requeue=True)
        hook.ack.assert_called_once_with(2)

    def test_a_failing_callable_acknowledges_nothing(self, mock_hook, caplog):
        """A filter that could not be evaluated is not a miss: the message stays."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1),
            _make_raw_msg(body="b", delivery_tag=2),
        ]

        def _boom(props, body):
            raise RuntimeError("filter service is down")

        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            filter_callable=_boom, ack_unmatched=True,
        )
        with caplog.at_level(logging.WARNING):
            assert sensor.poke(context={}) is False

        hook.ack.assert_not_called()
        assert [c.args[0] for c in hook.nack.call_args_list] == [1, 2]
        first = log_lines(caplog, ERROR_TEXT)
        assert len(first) == 1
        assert "RuntimeError: filter service is down" in first[0].getMessage()

    def test_non_bool_is_rejected(self):
        with pytest.raises(ValueError, match="ack_unmatched"):
            RMQSensor(task_id="t", queue_name="q", poke_interval=1, ack_unmatched="false")

    def test_deferrable_carries_the_value_into_the_trigger(self):
        from airflow_provider_rmq.triggers.rmq import RMQTrigger

        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            deferrable=True, ack_unmatched=True,
        )
        with patch.object(sensor, "defer") as mock_defer:
            sensor._defer()

        trigger = mock_defer.call_args.kwargs["trigger"]
        assert isinstance(trigger, RMQTrigger)
        assert trigger.ack_unmatched is True

    def test_deferrable_default_reaches_the_trigger_as_false(self):
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1, deferrable=True)
        with patch.object(sensor, "defer") as mock_defer:
            sensor._defer()
        assert mock_defer.call_args.kwargs["trigger"].ack_unmatched is False

    def test_the_base_sensor_mode_is_untouched(self):
        """``mode`` keeps whatever it had; ``ack_unmatched`` does not reach it."""
        assert RMQSensor(
            task_id="t", queue_name="q", poke_interval=1, ack_unmatched=True,
        ).mode == "poke"
        assert RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            mode="reschedule", ack_unmatched=True,
        ).mode == "reschedule"

    def test_the_streak_outlives_a_poke(self, mock_hook, caplog):
        """``execute`` reuses the instance, so a hundred single-message pokes reach 100."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
        ]
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            filter_headers={"x-type": "order"}, poke_batch_size=1,
        )
        with caplog.at_level(logging.WARNING):
            for _ in range(100):
                assert sensor.poke(context={}) is False

        streak_lines = log_lines(caplog, STREAK_TEXT)
        assert len(streak_lines) == 1
        assert streak_lines[0].getMessage().startswith("100 ")
        assert streak_lines[0].name == "airflow.task"

    def test_reschedule_never_reaches_the_threshold(self, mock_hook, caplog):
        """``AirflowRescheduleException`` ends the task instance, so the next poke runs
        on a new object and the run of misses starts over. The streak threshold is out
        of reach in that mode; the line per poke reports the misses instead."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
        ]
        with caplog.at_level(logging.WARNING):
            for _ in range(100):
                sensor = RMQSensor(
                    task_id="t", queue_name="q", poke_interval=1, mode="reschedule",
                    filter_headers={"x-type": "order"}, poke_batch_size=1,
                )
                assert sensor.poke(context={}) is False
                assert sensor._miss_state.streak == 1

        assert log_lines(caplog, STREAK_TEXT) == []
        assert len(log_lines(caplog, EMPTY_BATCH_TEXT)) == 100

    def test_a_hundred_pokes_report_no_batch_line(self, mock_hook, caplog):
        """In ``poke`` mode the counters span the pokes: a line per poke is the noise the
        threshold avoids, so a poke reports no batch.

        The run and the total are the same hundred here, so they are one line — the run's,
        which already says the messages were acknowledged and dropped.
        """
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
        ]
        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1,
            filter_headers={"x-type": "order"}, poke_batch_size=1,
            ack_unmatched=True,
        )
        with caplog.at_level(logging.WARNING):
            for _ in range(100):
                sensor.poke(context={})

        assert log_lines(caplog, _NOTHING_MATCHED_TEXT) == []
        assert log_lines(caplog, BATCH_DROPPED_TEXT) == []
        streak_lines = log_lines(caplog, STREAK_TEXT)
        assert len(streak_lines) == 1
        assert "acknowledged and dropped" in streak_lines[0].getMessage()
        assert log_lines(caplog, DROPPED_TEXT) == []


# ---------------------------------------------------------------------------
# trigger_mode and the base class's mode
# ---------------------------------------------------------------------------
def _deferred_trigger(sensor):
    """The trigger and the ``defer`` kwargs ``_defer`` hands to ``self.defer``."""
    with patch.object(sensor, "defer") as mock_defer:
        sensor._defer()
    mock_defer.assert_called_once()
    return mock_defer.call_args.kwargs["trigger"], mock_defer.call_args.kwargs


def _dag(default_args: dict) -> DAG:
    return DAG(
        "d", start_date=pendulum.datetime(2024, 1, 1), schedule=None, default_args=default_args
    )


class TestTriggerMode:
    def test_defaults(self):
        sensor = RMQSensor(task_id="t", queue_name="q")
        assert sensor.trigger_mode == "pull"
        assert sensor.mode == "poke"
        assert sensor.reschedule is False

    def test_trigger_mode_push(self, recwarn):
        sensor = RMQSensor(task_id="t", queue_name="q", trigger_mode="push")
        assert sensor.trigger_mode == "push"
        assert sensor.mode == "poke"
        assert not [w for w in recwarn if issubclass(w.category, DeprecationWarning)]

    @pytest.mark.parametrize("value", ["pull", "push"])
    def test_deprecated_mode_moves_to_trigger_mode(self, value):
        with pytest.warns(AirflowProviderDeprecationWarning, match=f"trigger_mode='{value}'") as rec:
            sensor = RMQSensor(task_id="t", queue_name="q", mode=value)
        assert sensor.trigger_mode == value
        assert sensor.mode == "poke"
        assert rec[0].filename == __file__

    def test_deprecated_mode_push_reaches_the_trigger(self):
        with pytest.warns(AirflowProviderDeprecationWarning):
            sensor = RMQSensor(task_id="t", queue_name="q", deferrable=True, mode="push")
        trigger, _ = _deferred_trigger(sensor)
        assert trigger.mode == "push"

    @pytest.mark.parametrize("value", ["pull", "push"])
    def test_deprecated_mode_in_dag_default_args_moves_to_trigger_mode(self, value):
        default_args = {"mode": value, "retries": 2}
        with (
            pytest.warns(AirflowProviderDeprecationWarning, match=f"trigger_mode='{value}'"),
            _dag(default_args),
        ):
            sensor = RMQSensor(task_id="t", queue_name="q", deferrable=True)
        assert sensor.trigger_mode == value
        assert sensor.mode == "poke"
        assert sensor.retries == 2
        assert default_args == {"mode": value, "retries": 2}
        trigger, _ = _deferred_trigger(sensor)
        assert trigger.mode == value

    def test_deprecated_mode_in_sensor_default_args_moves_to_trigger_mode(self):
        with pytest.warns(AirflowProviderDeprecationWarning, match="trigger_mode='push'"):
            sensor = RMQSensor(task_id="t", queue_name="q", default_args={"mode": "push"})
        assert sensor.trigger_mode == "push"
        assert sensor.mode == "poke"

    def test_trigger_mode_wins_over_mode_in_default_args(self):
        with pytest.warns(AirflowProviderDeprecationWarning), _dag({"mode": "push"}):
            sensor = RMQSensor(task_id="t", queue_name="q", trigger_mode="pull")
        assert sensor.trigger_mode == "pull"
        assert sensor.mode == "poke"

    def test_explicit_mode_wins_over_mode_in_default_args(self, recwarn):
        with _dag({"mode": "push"}):
            sensor = RMQSensor(task_id="t", queue_name="q", mode="reschedule")
        assert sensor.mode == "reschedule"
        assert sensor.trigger_mode == "pull"
        assert not [w for w in recwarn if issubclass(w.category, AirflowProviderDeprecationWarning)]

    @pytest.mark.parametrize("value", ["pull", "push"])
    def test_deprecated_mode_given_and_in_default_args(self, value):
        with pytest.warns(AirflowProviderDeprecationWarning), _dag({"mode": "push"}):
            sensor = RMQSensor(task_id="t", queue_name="q", mode=value)
        assert sensor.trigger_mode == value
        assert sensor.mode == "poke"

    def test_deprecated_mode_given_with_reschedule_in_default_args(self):
        with pytest.warns(AirflowProviderDeprecationWarning), _dag({"mode": "reschedule"}):
            sensor = RMQSensor(task_id="t", queue_name="q", mode="push")
        assert sensor.trigger_mode == "push"
        assert sensor.mode == "reschedule"

    def test_reschedule_in_default_args_reaches_the_base_class(self, recwarn):
        with _dag({"mode": "reschedule"}):
            sensor = RMQSensor(task_id="t", queue_name="q")
        assert sensor.mode == "reschedule"
        assert sensor.trigger_mode == "pull"
        assert not [w for w in recwarn if issubclass(w.category, AirflowProviderDeprecationWarning)]

    def test_mode_none_counts_as_not_given(self, recwarn):
        sensor = RMQSensor(task_id="t", queue_name="q", mode=None)
        assert sensor.mode == "poke"
        assert sensor.trigger_mode == "pull"
        assert not [w for w in recwarn if issubclass(w.category, DeprecationWarning)]

    def test_mode_none_with_trigger_mode(self):
        sensor = RMQSensor(task_id="t", queue_name="q", mode=None, trigger_mode="push")
        assert sensor.mode == "poke"
        assert sensor.trigger_mode == "push"

    def test_reschedule_with_deferrable_hands_pull_to_the_trigger(self):
        sensor = RMQSensor(task_id="t", queue_name="q", mode="reschedule", deferrable=True)
        assert sensor.mode == "reschedule"
        assert sensor.reschedule is True
        trigger, _ = _deferred_trigger(sensor)
        assert trigger.mode == "pull"

    def test_reschedule_and_trigger_mode_push_coexist(self):
        sensor = RMQSensor(
            task_id="t", queue_name="q", mode="reschedule", trigger_mode="push", deferrable=True,
        )
        assert sensor.mode == "reschedule"
        trigger, _ = _deferred_trigger(sensor)
        assert trigger.mode == "push"

    def test_unknown_mode_is_rejected_by_the_base_class(self):
        with pytest.raises(AirflowException, match="bogus"):
            RMQSensor(task_id="t", queue_name="q", mode="bogus")

    @pytest.mark.parametrize("value", ["pull", "push"])
    def test_both_parameters_raise(self, value):
        with pytest.raises(ValueError, match="trigger_mode"):
            RMQSensor(task_id="t", queue_name="q", mode=value, trigger_mode="push")

    @pytest.mark.parametrize("value", ["bogus", "poke", "", "PUSH"])
    def test_invalid_trigger_mode_raises(self, value):
        with pytest.raises(ValueError, match="trigger_mode must be 'pull' or 'push'"):
            RMQSensor(task_id="t", queue_name="q", trigger_mode=value)

    def test_debug_executor_switches_mode_and_keeps_trigger_mode(self):
        """``DebugExecutor`` is the executor with ``change_sensor_mode_to_reschedule``."""
        from airflow.executors.debug_executor import DebugExecutor

        assert DebugExecutor.change_sensor_mode_to_reschedule is True
        sensor = RMQSensor(task_id="t", queue_name="q", trigger_mode="push", deferrable=True)
        with patch(
            "airflow.sensors.base.ExecutorLoader.import_default_executor_cls",
            return_value=(DebugExecutor, None),
        ):
            task = sensor.prepare_for_execution()
        assert task.mode == "reschedule"
        assert task.trigger_mode == "push"


# ---------------------------------------------------------------------------
# _defer — the sensor's timeout bounds the deferral
# ---------------------------------------------------------------------------
class TestDeferTimeout:
    def test_default_timeout_reaches_defer(self):
        sensor = RMQSensor(task_id="t", queue_name="q", deferrable=True)
        _, kwargs = _deferred_trigger(sensor)
        assert kwargs["timeout"] == timedelta(seconds=sensor.timeout)
        assert kwargs["method_name"] == "execute_complete"

    def test_timeout_in_seconds(self):
        sensor = RMQSensor(task_id="t", queue_name="q", deferrable=True, timeout=120)
        _, kwargs = _deferred_trigger(sensor)
        assert kwargs["timeout"] == timedelta(seconds=120)

    def test_timeout_as_timedelta(self):
        sensor = RMQSensor(
            task_id="t", queue_name="q", deferrable=True, timeout=timedelta(minutes=5),
        )
        assert sensor.timeout == 300
        _, kwargs = _deferred_trigger(sensor)
        assert kwargs["timeout"] == timedelta(minutes=5)

    @pytest.mark.parametrize("trigger_mode", ["pull", "push"])
    def test_trigger_gets_the_trigger_mode(self, trigger_mode):
        sensor = RMQSensor(
            task_id="t", queue_name="q", deferrable=True, trigger_mode=trigger_mode,
        )
        trigger, _ = _deferred_trigger(sensor)
        assert trigger.mode == trigger_mode

    def test_execute_raises_task_deferred_with_the_timeout(self):
        from airflow.exceptions import TaskDeferred

        sensor = RMQSensor(
            task_id="t", queue_name="q", deferrable=True, trigger_mode="push", timeout=90,
        )
        with pytest.raises(TaskDeferred) as exc_info:
            sensor.execute(context={})
        assert exc_info.value.timeout == timedelta(seconds=90)
        assert exc_info.value.trigger.mode == "push"

    @pytest.mark.parametrize("timeout", [float("inf"), 1e12, 1e15], ids=["inf", "past-9999", "huge"])
    def test_timeout_no_date_can_reach_defers_without_a_limit(self, timeout):
        """Airflow 2.9 keeps ``timeout`` as given; later versions refuse some of these
        values in the constructor, so the value is set on the built sensor."""
        from airflow.exceptions import TaskDeferred

        sensor = RMQSensor(task_id="t", queue_name="q", deferrable=True)
        sensor.timeout = timeout
        with pytest.raises(TaskDeferred) as exc_info:
            sensor.execute(context={})
        assert exc_info.value.timeout is None


# ---------------------------------------------------------------------------
# reschedule — one batch line per poke
# ---------------------------------------------------------------------------
class TestReschedulePokeReport:
    def _sensor(self, mode, **kwargs):
        return RMQSensor(
            task_id="t", queue_name="q", poke_interval=1, mode=mode,
            filter_headers={"x-type": "order"}, **kwargs,
        )

    def test_poke_without_a_match_reports_the_batch(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "receipt"}),
        ]
        sensor = self._sensor("reschedule")
        with caplog.at_level(logging.WARNING):
            assert sensor.poke(context={}) is False

        lines = log_lines(caplog, EMPTY_BATCH_TEXT)
        assert len(lines) == 1
        assert lines[0].getMessage().startswith("None of the 2 messages")
        assert lines[0].name == "airflow.task"
        assert "acknowledged and dropped" not in lines[0].getMessage()

    def test_poke_without_a_match_reports_what_it_dropped(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "receipt"}),
        ]
        sensor = self._sensor("reschedule", ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            sensor.poke(context={})

        lines = log_lines(caplog, _NOTHING_MATCHED_TEXT)
        assert len(lines) == 1
        assert "2 of them were acknowledged and dropped" in lines[0].getMessage()

    def test_match_after_dropped_misses_reports_the_drops(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "order"}),
            _make_raw_msg(body="c", delivery_tag=3, headers={"x-type": "invoice"}),
        ]
        sensor = self._sensor("reschedule", ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            assert sensor.poke(context={}) is True

        assert log_lines(caplog, EMPTY_BATCH_TEXT) == []
        lines = log_lines(caplog, BATCH_DROPPED_TEXT)
        assert len(lines) == 1
        assert lines[0].getMessage().startswith("1 of the 2 messages")

    def test_match_without_drops_reports_nothing(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "order"}),
        ]
        sensor = self._sensor("reschedule")
        with caplog.at_level(logging.WARNING):
            assert sensor.poke(context={}) is True

        assert log_lines(caplog, EMPTY_BATCH_TEXT) == []
        assert log_lines(caplog, BATCH_DROPPED_TEXT) == []

    def test_errors_are_not_evaluated(self, mock_hook, caplog):
        """A body the filter could not read says nothing about ``filter_data``."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "invoice"}),
        ]
        calls = iter([RuntimeError("boom"), False])

        def _filter(props, body):
            outcome = next(calls)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1, mode="reschedule",
            filter_callable=_filter,
        )
        with caplog.at_level(logging.WARNING):
            sensor.poke(context={})

        lines = log_lines(caplog, EMPTY_BATCH_TEXT)
        assert len(lines) == 1
        assert lines[0].getMessage().startswith("None of the 1 messages")

    def test_only_errors_report_no_batch(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = [_make_raw_msg(body="a", delivery_tag=1)]

        def _boom(props, body):
            raise RuntimeError("boom")

        sensor = RMQSensor(
            task_id="t", queue_name="q", poke_interval=1, mode="reschedule",
            filter_callable=_boom,
        )
        with caplog.at_level(logging.WARNING):
            sensor.poke(context={})

        assert log_lines(caplog, EMPTY_BATCH_TEXT) == []
        assert len(log_lines(caplog, ERROR_TEXT)) == 1

    def test_empty_queue_reports_nothing(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = []
        sensor = self._sensor("reschedule")
        with caplog.at_level(logging.WARNING):
            assert sensor.poke(context={}) is False
        assert log_lines(caplog, EMPTY_BATCH_TEXT) == []

    def test_each_poke_reports_only_its_own_drops(self, mock_hook, caplog):
        """The counters on one instance are cumulative; the line counts this poke only."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
        ]
        sensor = self._sensor("reschedule", ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            sensor.poke(context={})
            sensor.poke(context={})

        lines = log_lines(caplog, _NOTHING_MATCHED_TEXT)
        assert len(lines) == 2
        assert all(
            "1 of them were acknowledged and dropped" in line.getMessage() for line in lines
        )

    def test_poke_mode_reports_no_batch(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "receipt"}),
        ]
        sensor = self._sensor("poke", ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            assert sensor.poke(context={}) is False

        assert log_lines(caplog, _NOTHING_MATCHED_TEXT) == []
        assert log_lines(caplog, BATCH_DROPPED_TEXT) == []

    def test_poke_mode_reports_no_batch_without_ack_unmatched(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "invoice"}),
        ]
        sensor = self._sensor("poke")
        with caplog.at_level(logging.WARNING):
            assert sensor.poke(context={}) is False

        assert log_lines(caplog, EMPTY_BATCH_TEXT) == []

    def test_queue_closed_by_the_broker_reports_no_batch(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.side_effect = pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND")
        sensor = self._sensor("reschedule")
        with caplog.at_level(logging.WARNING):
            assert sensor.poke(context={}) is False

        assert log_lines(caplog, _NOTHING_MATCHED_TEXT) == []
        assert log_lines(caplog, BATCH_DROPPED_TEXT) == []
        (line,) = log_lines(caplog, "is not available")
        assert "NOT_FOUND" in line.getMessage()
