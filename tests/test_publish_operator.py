from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import pika
import pytest
from airflow.exceptions import AirflowException
from pika.adapters.blocking_connection import ReturnedMessage
from pika.exceptions import ChannelClosedByBroker, NackError, UnroutableError

from airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator


HOOK_PATH = "airflow_provider_rmq.operators.rmq_publish.RMQHook"


@pytest.fixture
def mock_hook():
    """Return a mocked RMQHook used as context manager.

    The mock follows ``RMQHook``'s signatures, so a keyword the hook does not accept
    fails the test.
    """
    with patch(HOOK_PATH, autospec=True) as MockHook:
        hook_instance = MockHook.return_value
        hook_instance.__enter__.return_value = hook_instance
        hook_instance.__exit__.return_value = False
        yield MockHook, hook_instance


class TestPublishViaExchange:
    def test_publish_single_string(self, mock_hook):
        MockHook, hook = mock_hook
        op = RMQPublishOperator(
            task_id="test", exchange="my_ex", routing_key="rk", message="hello",
        )
        op.execute(context={})
        hook.basic_publish.assert_called_once()
        call_kwargs = hook.basic_publish.call_args.kwargs
        assert call_kwargs["exchange"] == "my_ex"
        assert call_kwargs["routing_key"] == "rk"
        assert call_kwargs["body"] == "hello"

    def test_publish_uses_exchange_and_routing_key(self, mock_hook):
        MockHook, hook = mock_hook
        op = RMQPublishOperator(
            task_id="test", exchange="orders", routing_key="new.order", message="data",
        )
        op.execute(context={})
        call_kwargs = hook.basic_publish.call_args.kwargs
        assert call_kwargs["exchange"] == "orders"
        assert call_kwargs["routing_key"] == "new.order"


class TestPublishToQueue:
    def test_queue_name_sets_default_exchange(self, mock_hook):
        MockHook, hook = mock_hook
        op = RMQPublishOperator(
            task_id="test", queue_name="my_queue", message="hello",
        )
        assert op.exchange == ""
        assert op.routing_key == "my_queue"
        op.execute(context={})
        call_kwargs = hook.basic_publish.call_args.kwargs
        assert call_kwargs["exchange"] == ""
        assert call_kwargs["routing_key"] == "my_queue"

    def test_queue_name_overrides_exchange(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="test", exchange="ignored", routing_key="ignored",
            queue_name="my_queue", message="msg",
        )
        assert op.exchange == ""
        assert op.routing_key == "my_queue"


class TestMessageTypes:
    def test_single_string(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message="text")
        op.execute(context={})
        assert hook.basic_publish.call_count == 1
        assert hook.basic_publish.call_args.kwargs["body"] == "text"

    def test_list_of_strings(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="", routing_key="q", message=["a", "b", "c"],
        )
        op.execute(context={})
        assert hook.basic_publish.call_count == 3
        bodies = [c.kwargs["body"] for c in hook.basic_publish.call_args_list]
        assert bodies == ["a", "b", "c"]

    def test_single_dict(self, mock_hook):
        _, hook = mock_hook
        data = {"key": "value", "num": 42}
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=data)
        op.execute(context={})
        assert hook.basic_publish.call_count == 1
        body = hook.basic_publish.call_args.kwargs["body"]
        assert json.loads(body) == data

    def test_list_of_dicts(self, mock_hook):
        _, hook = mock_hook
        msgs = [{"id": 1}, {"id": 2}]
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=msgs)
        op.execute(context={})
        assert hook.basic_publish.call_count == 2
        bodies = [json.loads(c.kwargs["body"]) for c in hook.basic_publish.call_args_list]
        assert bodies == msgs

    def test_mixed_list(self, mock_hook):
        _, hook = mock_hook
        msgs = ["plain", {"key": "val"}]
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=msgs)
        op.execute(context={})
        assert hook.basic_publish.call_count == 2
        b0 = hook.basic_publish.call_args_list[0].kwargs["body"]
        b1 = hook.basic_publish.call_args_list[1].kwargs["body"]
        assert b0 == "plain"
        assert json.loads(b1) == {"key": "val"}

    def test_none_message(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=None)
        op.execute(context={})
        hook.basic_publish.assert_not_called()

    def test_integer_message(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message=123)
        op.execute(context={})
        assert hook.basic_publish.call_args.kwargs["body"] == "123"


class TestMessageProperties:
    def test_properties_passed(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="", routing_key="q", message="msg",
            content_type="application/json",
            delivery_mode=2,
            headers={"x-custom": "val"},
            priority=5,
            expiration="60000",
            correlation_id="corr-123",
            reply_to="reply_queue",
            message_id="msg-456",
        )
        op.execute(context={})
        props = hook.basic_publish.call_args.kwargs["properties"]
        assert isinstance(props, pika.BasicProperties)
        assert props.content_type == "application/json"
        assert props.delivery_mode == 2
        assert props.headers == {"x-custom": "val"}
        assert props.priority == 5
        assert props.expiration == "60000"
        assert props.correlation_id == "corr-123"
        assert props.reply_to == "reply_queue"
        assert props.message_id == "msg-456"

    def test_default_properties_are_none(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message="msg")
        op.execute(context={})
        props = hook.basic_publish.call_args.kwargs["properties"]
        assert props.content_type is None
        assert props.delivery_mode is None
        assert props.headers is None

    def test_same_properties_for_batch(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="", routing_key="q",
            message=["a", "b"], delivery_mode=2,
        )
        op.execute(context={})
        props_list = [c.kwargs["properties"] for c in hook.basic_publish.call_args_list]
        assert all(p.delivery_mode == 2 for p in props_list)


class TestConnectionId:
    def test_default_conn_id(self, mock_hook):
        MockHook, _ = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message="m")
        op.execute(context={})
        MockHook.assert_called_once_with(rmq_conn_id="rmq_default", publisher_confirms=True)

    def test_custom_conn_id(self, mock_hook):
        MockHook, _ = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="", routing_key="q", message="m",
            rmq_conn_id="my_rmq",
        )
        op.execute(context={})
        MockHook.assert_called_once_with(rmq_conn_id="my_rmq", publisher_confirms=True)


def _returned(reply_code: int = 312, reply_text: str = "NO_ROUTE") -> ReturnedMessage:
    method = pika.spec.Basic.Return(
        reply_code=reply_code, reply_text=reply_text, exchange="", routing_key="q",
    )
    return ReturnedMessage(method, pika.BasicProperties(), b"body")


def _published_mandatory(hook: MagicMock) -> list[bool]:
    return [c.kwargs["mandatory"] for c in hook.basic_publish.call_args_list]


class TestMandatoryResolution:
    def test_queue_name_is_mandatory(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", queue_name="q", message="m")
        op.execute(context={})
        assert _published_mandatory(hook) == [True]

    def test_default_exchange_is_mandatory(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="", routing_key="q", message="m")
        op.execute(context={})
        assert _published_mandatory(hook) == [True]

    def test_named_exchange_is_not_mandatory(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", exchange="ex", routing_key="rk", message="m")
        op.execute(context={})
        assert _published_mandatory(hook) == [False]

    def test_templated_exchange_rendered_to_default_is_mandatory(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="{{ params.ex }}", routing_key="q", message="m",
        )
        op.render_template_fields(context={"params": {"ex": ""}})
        assert op.exchange == ""
        op.execute(context={})
        assert _published_mandatory(hook) == [True]

    def test_templated_exchange_rendered_to_named_is_not_mandatory(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="{{ params.ex }}", routing_key="q", message="m",
        )
        op.render_template_fields(context={"params": {"ex": "orders"}})
        op.execute(context={})
        assert _published_mandatory(hook) == [False]

    def test_explicit_false_wins_on_default_exchange(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", queue_name="q", message="m", mandatory=False)
        op.execute(context={})
        assert _published_mandatory(hook) == [False]

    def test_explicit_true_wins_on_named_exchange(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(
            task_id="t", exchange="ex", routing_key="rk", message="m", mandatory=True,
        )
        op.execute(context={})
        assert _published_mandatory(hook) == [True]

    def test_no_confirm_resolves_to_false(self, mock_hook):
        _, hook = mock_hook
        op = RMQPublishOperator(task_id="t", queue_name="q", message="m", confirm=False)
        op.execute(context={})
        assert _published_mandatory(hook) == [False]

    def test_mandatory_without_confirm_raises(self):
        with pytest.raises(ValueError, match="mandatory=True requires confirm=True"):
            RMQPublishOperator(
                task_id="t", queue_name="q", message="m", confirm=False, mandatory=True,
            )

    def test_mandatory_false_without_confirm_is_allowed(self):
        op = RMQPublishOperator(
            task_id="t", queue_name="q", message="m", confirm=False, mandatory=False,
        )
        assert op.mandatory is False


class TestPublisherConfirms:
    def test_hook_created_without_confirms(self, mock_hook):
        MockHook, _ = mock_hook
        op = RMQPublishOperator(task_id="t", queue_name="q", message="m", confirm=False)
        op.execute(context={})
        assert MockHook.call_args.kwargs["publisher_confirms"] is False

    def test_unroutable_message_fails_task_with_its_number(self, mock_hook):
        _, hook = mock_hook
        error = UnroutableError([_returned()])
        hook.basic_publish.side_effect = [None, None, error, None, None]
        op = RMQPublishOperator(
            task_id="t", queue_name="missing", message=["a", "b", "c", "d", "e"],
        )
        with pytest.raises(AirflowException) as excinfo:
            op.execute(context={})
        text = str(excinfo.value)
        assert "message 3 of 5" in text
        assert "exchange=''" in text
        assert "routing_key='missing'" in text
        assert "returned by the broker: 312 NO_ROUTE" in text
        assert excinfo.value.__cause__ is error
        assert hook.basic_publish.call_count == 3
        assert _published_mandatory(hook) == [True, True, True]

    def test_nack_fails_task(self, mock_hook):
        _, hook = mock_hook
        error = NackError([])
        hook.basic_publish.side_effect = error
        op = RMQPublishOperator(task_id="t", exchange="ex", routing_key="rk", message="m")
        with pytest.raises(AirflowException, match=r"message 1 of 1 .*rejected by the broker \(nack\)") as excinfo:
            op.execute(context={})
        assert excinfo.value.__cause__ is error

    def test_channel_closed_by_broker_fails_task(self, mock_hook):
        _, hook = mock_hook
        error = ChannelClosedByBroker(404, "NOT_FOUND - no exchange 'nope' in vhost '/'")
        hook.basic_publish.side_effect = error
        op = RMQPublishOperator(task_id="t", exchange="nope", routing_key="rk", message="m")
        with pytest.raises(AirflowException) as excinfo:
            op.execute(context={})
        text = str(excinfo.value)
        assert "exchange='nope'" in text
        assert "broker closed the channel: 404 NOT_FOUND - no exchange 'nope'" in text
        assert excinfo.value.__cause__ is error

    def test_without_confirm_channel_close_is_not_wrapped(self, mock_hook):
        _, hook = mock_hook
        error = ChannelClosedByBroker(404, "NOT_FOUND")
        hook.basic_publish.side_effect = error
        op = RMQPublishOperator(
            task_id="t", exchange="ex", routing_key="rk", message="m", confirm=False,
        )
        with pytest.raises(ChannelClosedByBroker) as excinfo:
            op.execute(context={})
        assert excinfo.value is error

    def test_other_exceptions_are_not_wrapped(self, mock_hook):
        _, hook = mock_hook
        hook.basic_publish.side_effect = RuntimeError("boom")
        op = RMQPublishOperator(task_id="t", queue_name="q", message="m")
        with pytest.raises(RuntimeError, match="boom"):
            op.execute(context={})


class TestEmptyPublish:
    @pytest.mark.parametrize("message", [None, []])
    def test_empty_message_warns_and_publishes_nothing(self, mock_hook, caplog, message):
        MockHook, hook = mock_hook
        op = RMQPublishOperator(task_id="t", queue_name="q", message=message)
        with caplog.at_level(logging.WARNING, logger="airflow.task"):
            op.execute(context={})
        hook.basic_publish.assert_not_called()
        MockHook.assert_not_called()
        assert any("nothing to publish" in r.getMessage() for r in caplog.records)
