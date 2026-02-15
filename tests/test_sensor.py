from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from apache_airflow_provider_rmq.sensors.rmq import RMQSensor
from tests.conftest import make_method_frame, make_properties


HOOK_PATH = "apache_airflow_provider_rmq.sensors.rmq.RMQHook"


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
        hook.queue_info.return_value = {"message_count": 0, "exists": True}
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        result = sensor.poke(context={})
        assert result is False

    def test_nonexistent_queue_returns_false(self, mock_hook):
        _, hook = mock_hook
        hook.queue_info.return_value = {"message_count": 0, "exists": False}
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        result = sensor.poke(context={})
        assert result is False


# ---------------------------------------------------------------------------
# Poke mode — no filter
# ---------------------------------------------------------------------------
class TestPokeNoFilter:
    def test_first_message_matches(self, mock_hook):
        _, hook = mock_hook
        hook.queue_info.return_value = {"message_count": 1, "exists": True}
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
        hook.queue_info.return_value = {"message_count": 3, "exists": True}
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
        hook.queue_info.return_value = {"message_count": 2, "exists": True}
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
        hook.queue_info.return_value = {"message_count": 2, "exists": True}
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
        hook.queue_info.return_value = {"message_count": 2, "exists": True}
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
        hook.queue_info.return_value = {"message_count": 2, "exists": True}
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
        hook.queue_info.return_value = {"message_count": 0, "exists": True}
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        sensor.poke(context={})
        hook.close.assert_called_once()

    def test_hook_closed_on_exception(self, mock_hook):
        _, hook = mock_hook
        hook.queue_info.side_effect = RuntimeError("boom")
        sensor = RMQSensor(task_id="t", queue_name="q", poke_interval=1)
        with pytest.raises(RuntimeError):
            sensor.poke(context={})
        hook.close.assert_called_once()


# ---------------------------------------------------------------------------
# Execute — poke mode returns _return_value via XCom
# ---------------------------------------------------------------------------
class TestExecute:
    def test_execute_poke_returns_value(self, mock_hook):
        _, hook = mock_hook
        hook.queue_info.return_value = {"message_count": 1, "exists": True}
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
            with patch.dict("sys.modules", {"apache_airflow_provider_rmq.triggers.rmq": MagicMock(RMQTrigger=mock_trigger_cls)}):
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