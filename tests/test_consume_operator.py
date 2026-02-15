from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from apache_airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator
from tests.conftest import make_method_frame, make_properties


HOOK_PATH = "apache_airflow_provider_rmq.operators.rmq_consume.RMQHook"


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