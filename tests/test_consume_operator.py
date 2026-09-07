from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pytest

from airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator
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


HOOK_PATH = "airflow_provider_rmq.operators.rmq_consume.RMQHook"


def _make_raw_msg(body="msg", delivery_tag=1, routing_key="rk", exchange="", headers=None):
    """Create a raw message dict as returned by hook.consume_messages()."""
    return {
        "method": make_method_frame(delivery_tag=delivery_tag, routing_key=routing_key, exchange=exchange),
        "properties": make_properties(headers=headers),
        "body": body,
    }


@pytest.fixture
def mock_hook():
    with patch(HOOK_PATH) as MockHook:
        hook_instance = MagicMock()
        MockHook.return_value.__enter__ = MagicMock(return_value=hook_instance)
        MockHook.return_value.__exit__ = MagicMock(return_value=False)
        yield MockHook, hook_instance


class TestEmptyQueue:
    def test_returns_empty_list(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = []
        op = RMQConsumeOperator(task_id="t", queue_name="q")
        result = op.execute(context={})
        assert result == []

    def test_nonexistent_queue(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = []
        op = RMQConsumeOperator(task_id="t", queue_name="q")
        result = op.execute(context={})
        assert result == []


class TestNoFilter:
    def test_all_messages_consumed(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1),
            _make_raw_msg(body="b", delivery_tag=2),
            _make_raw_msg(body="c", delivery_tag=3),
        ]
        op = RMQConsumeOperator(task_id="t", queue_name="q")
        result = op.execute(context={})
        assert len(result) == 3
        assert [m["body"] for m in result] == ["a", "b", "c"]
        assert hook.ack.call_count == 3
        hook.nack.assert_not_called()

    def test_result_structure(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="data", delivery_tag=1, routing_key="my.key",
                          exchange="my_ex", headers={"x-type": "order"}),
        ]
        op = RMQConsumeOperator(task_id="t", queue_name="q")
        result = op.execute(context={})
        assert len(result) == 1
        msg = result[0]
        assert msg["body"] == "data"
        assert msg["headers"] == {"x-type": "order"}
        assert msg["routing_key"] == "my.key"
        assert msg["exchange"] == "my_ex"


class TestDictFilter:
    def test_matching_messages_acked(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "order"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "invoice"}),
            _make_raw_msg(body="c", delivery_tag=3, headers={"x-type": "order"}),
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q", filter_headers={"x-type": "order"},
        )
        result = op.execute(context={})
        assert len(result) == 2
        assert [m["body"] for m in result] == ["a", "c"]

    def test_non_matching_nacked_with_requeue(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "order"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "invoice"}),
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q", filter_headers={"x-type": "order"},
        )
        op.execute(context={})
        hook.ack.assert_called_once_with(1)
        hook.nack.assert_called_once_with(2, requeue=True)

    def test_body_filter(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body=json.dumps({"status": "active"}), delivery_tag=1),
            _make_raw_msg(body=json.dumps({"status": "inactive"}), delivery_tag=2),
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q", filter_headers={"body.status": "active"},
        )
        result = op.execute(context={})
        assert len(result) == 1
        assert result[0]["body"] == json.dumps({"status": "active"})


class TestCallableFilter:
    def test_callable_matching(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="important data", delivery_tag=1),
            _make_raw_msg(body="normal data", delivery_tag=2),
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q",
            filter_callable=lambda props, body: "important" in body,
        )
        result = op.execute(context={})
        assert len(result) == 1
        assert result[0]["body"] == "important data"
        hook.ack.assert_called_once_with(1)
        hook.nack.assert_called_once_with(2, requeue=True)


class TestMaxMessages:
    def test_respects_limit(self, mock_hook):
        _, hook = mock_hook
        # consume_messages is now called with max_messages=2 directly,
        # so it returns at most 2 messages
        hook.consume_messages.return_value = [
            _make_raw_msg(body="msg1", delivery_tag=1),
            _make_raw_msg(body="msg2", delivery_tag=2),
        ]
        op = RMQConsumeOperator(task_id="t", queue_name="q", max_messages=2)
        result = op.execute(context={})
        assert len(result) == 2
        assert hook.ack.call_count == 2
        hook.consume_messages.assert_called_once_with(
            queue_name="q", max_messages=2, auto_ack=False,
        )

    def test_max_messages_with_filter(self, mock_hook):
        _, hook = mock_hook
        # With max_messages=2, consume_messages returns 2 messages.
        # Filtering happens on the consumed batch.
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "order"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "invoice"}),
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q",
            filter_headers={"x-type": "order"}, max_messages=2,
        )
        result = op.execute(context={})
        assert len(result) == 1
        assert result[0]["body"] == "a"
        hook.ack.assert_called_once_with(1)
        hook.nack.assert_called_once_with(2, requeue=True)


class TestChannelClosedByBroker:
    def test_returns_empty_on_nonexistent_queue(self, mock_hook):
        _, hook = mock_hook
        from pika.exceptions import ChannelClosedByBroker
        hook.consume_messages.side_effect = ChannelClosedByBroker(404, "NOT_FOUND")
        op = RMQConsumeOperator(task_id="t", queue_name="nonexistent")
        result = op.execute(context={})
        assert result == []


class TestConnectionId:
    def test_default_conn_id(self, mock_hook):
        MockHook, hook = mock_hook
        hook.consume_messages.return_value = []
        op = RMQConsumeOperator(task_id="t", queue_name="q")
        op.execute(context={})
        MockHook.assert_called_once_with(rmq_conn_id="rmq_default", qos=None)

    def test_custom_conn_id_and_qos(self, mock_hook):
        MockHook, hook = mock_hook
        hook.consume_messages.return_value = []
        qos = {"prefetch_count": 10}
        op = RMQConsumeOperator(
            task_id="t", queue_name="q", rmq_conn_id="custom", qos=qos,
        )
        op.execute(context={})
        MockHook.assert_called_once_with(rmq_conn_id="custom", qos=qos)

# ---------------------------------------------------------------------------
# ack_unmatched
# ---------------------------------------------------------------------------



def _filter_lines(caplog):
    """Records the filter-miss counters produced, whatever the threshold behind them."""
    texts = (STREAK_TEXT, DROPPED_TEXT, ERROR_TEXT, EMPTY_BATCH_TEXT, BATCH_DROPPED_TEXT)
    return [r for r in caplog.records if any(t in r.getMessage() for t in texts)]


class TestAckUnmatched:
    def test_rejected_messages_are_acknowledged(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "order"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "invoice"}),
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q",
            filter_headers={"x-type": "order"}, ack_unmatched=True,
        )
        result = op.execute(context={})
        assert [m["body"] for m in result] == ["a"]
        assert sorted(c.args[0] for c in hook.ack.call_args_list) == [1, 2]
        hook.nack.assert_not_called()

    def test_default_returns_rejected_messages_to_the_queue(self, mock_hook):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1, headers={"x-type": "order"}),
            _make_raw_msg(body="b", delivery_tag=2, headers={"x-type": "invoice"}),
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q", filter_headers={"x-type": "order"},
        )
        result = op.execute(context={})
        assert [m["body"] for m in result] == ["a"]
        hook.ack.assert_called_once_with(1)
        hook.nack.assert_called_once_with(2, requeue=True)

    def test_a_failing_callable_acknowledges_nothing(self, mock_hook, caplog):
        """A filter that could not be evaluated is not a miss: the message stays."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="a", delivery_tag=1),
            _make_raw_msg(body="b", delivery_tag=2),
        ]

        def _boom(props, body):
            raise RuntimeError("filter service is down")

        op = RMQConsumeOperator(
            task_id="t", queue_name="q", filter_callable=_boom, ack_unmatched=True,
        )
        with caplog.at_level(logging.WARNING):
            result = op.execute(context={})

        assert result == []
        hook.ack.assert_not_called()
        assert [c.args[0] for c in hook.nack.call_args_list] == [1, 2]
        first = log_lines(caplog, ERROR_TEXT)
        assert len(first) == 1
        assert "RuntimeError: filter service is down" in first[0].getMessage()
        # The filter reached no verdict on either message, so the batch line — whose
        # advice is to look at filter_data — has nothing to say about this batch.
        assert log_lines(caplog, EMPTY_BATCH_TEXT) == []

    def test_non_bool_is_rejected(self):
        with pytest.raises(ValueError, match="ack_unmatched"):
            RMQConsumeOperator(task_id="t", queue_name="q", ack_unmatched="false")

    def test_streak_threshold_warns_on_the_hundredth_miss(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body=str(i), delivery_tag=i, headers={"x-type": "invoice"})
            for i in range(1, 101)
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q",
            filter_headers={"x-type": "order"}, max_messages=100,
        )
        with caplog.at_level(logging.WARNING):
            op.execute(context={})
        streak_lines = log_lines(caplog, STREAK_TEXT)
        assert len(streak_lines) == 1
        assert streak_lines[0].getMessage().startswith("100 ")

    def test_a_batch_below_the_threshold_still_reports_no_matches(self, mock_hook, caplog):
        """``max_messages=50`` never reaches a streak of a hundred, so the batch line carries it."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body=str(i), delivery_tag=i, headers={"x-type": "invoice"})
            for i in range(1, 51)
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q",
            filter_headers={"x-type": "order"}, max_messages=50,
        )
        with caplog.at_level(logging.WARNING):
            op.execute(context={})
        assert log_lines(caplog, STREAK_TEXT) == []
        assert len(log_lines(caplog, EMPTY_BATCH_TEXT)) == 1

    def test_a_single_match_does_not_silence_the_dropped_line(self, mock_hook, caplog):
        """One match clears the streak and ninety-nine drops never reach the ``dropped`` floor."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body="mine", delivery_tag=1, headers={"x-type": "order"}),
        ] + [
            _make_raw_msg(body=str(i), delivery_tag=i, headers={"x-type": "invoice"})
            for i in range(2, 101)
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q",
            filter_headers={"x-type": "order"}, max_messages=100, ack_unmatched=True,
        )
        with caplog.at_level(logging.WARNING):
            result = op.execute(context={})
        assert len(result) == 1
        assert log_lines(caplog, STREAK_TEXT) == []
        assert log_lines(caplog, DROPPED_TEXT) == []
        assert log_lines(caplog, EMPTY_BATCH_TEXT) == []
        dropped_lines = log_lines(caplog, BATCH_DROPPED_TEXT)
        assert len(dropped_lines) == 1
        assert dropped_lines[0].getMessage().startswith("99 of the 100 ")

    def test_nothing_is_reported_as_dropped_by_default(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body=str(i), delivery_tag=i, headers={"x-type": "invoice"})
            for i in range(1, 101)
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q",
            filter_headers={"x-type": "order"}, max_messages=100,
        )
        with caplog.at_level(logging.WARNING):
            op.execute(context={})
        assert log_lines(caplog, DROPPED_TEXT) == []
        assert log_lines(caplog, BATCH_DROPPED_TEXT) == []

    def test_threshold_records_go_to_the_task_logger(self, mock_hook, caplog):
        """``FileTaskHandler`` hangs off ``airflow.task``: another logger misses the task log."""
        _, hook = mock_hook
        hook.consume_messages.return_value = [
            _make_raw_msg(body=str(i), delivery_tag=i, headers={"x-type": "invoice"})
            for i in range(1, 101)
        ]
        op = RMQConsumeOperator(
            task_id="t", queue_name="q",
            filter_headers={"x-type": "order"}, max_messages=100,
        )
        with caplog.at_level(logging.WARNING):
            op.execute(context={})
        assert [r.name for r in log_lines(caplog, STREAK_TEXT)] == ["airflow.task"]
        assert [r.name for r in log_lines(caplog, EMPTY_BATCH_TEXT)] == ["airflow.task"]

    def test_an_empty_queue_reports_nothing(self, mock_hook, caplog):
        _, hook = mock_hook
        hook.consume_messages.return_value = []
        op = RMQConsumeOperator(task_id="t", queue_name="q", ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            assert op.execute(context={}) == []
        assert _filter_lines(caplog) == []
        hook.ack.assert_not_called()
        hook.nack.assert_not_called()

    def test_an_unavailable_queue_reports_only_its_own_warning(self, mock_hook, caplog):
        _, hook = mock_hook
        from pika.exceptions import ChannelClosedByBroker
        hook.consume_messages.side_effect = ChannelClosedByBroker(404, "NOT_FOUND")
        op = RMQConsumeOperator(task_id="t", queue_name="q", ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            assert op.execute(context={}) == []
        assert _filter_lines(caplog) == []
        assert log_lines(caplog, "is not available") != []
        hook.ack.assert_not_called()
        hook.nack.assert_not_called()
