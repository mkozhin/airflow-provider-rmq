from __future__ import annotations

import json
import ssl
from unittest.mock import MagicMock, PropertyMock, patch

import pika
import pika.exceptions
import pytest

from apache_airflow_provider_rmq.hooks.rmq import RMQHook
from tests.conftest import FakeAirflowConnection, make_message


HOOK_MODULE = "apache_airflow_provider_rmq.hooks.rmq"


@pytest.fixture
def hook_with_mocks(fake_connection, mock_connection, mock_channel):
    """Create an RMQHook with mocked connection and channel."""
    with patch.object(RMQHook, "get_connection", return_value=fake_connection):
        with patch(f"{HOOK_MODULE}.pika.BlockingConnection", return_value=mock_connection):
            hook = RMQHook(rmq_conn_id="rmq_default")
            yield hook, mock_connection, mock_channel
            hook._connection = None
            hook._channel = None


# ---------------------------------------------------------------------------
# 3.3.1 — Connection params
# ---------------------------------------------------------------------------
class TestConnectionParams:
    def test_default_params(self, fake_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            hook = RMQHook()
            params = hook._get_connection_params()
            assert params.host == "localhost"
            assert params.port == 5672
            assert params.virtual_host == "/"
            assert params.credentials.username == "guest"
            assert params.credentials.password == "guest"

    def test_custom_host_port(self):
        conn = FakeAirflowConnection(host="rmq.example.com", port=5673, schema="/myvhost")
        with patch.object(RMQHook, "get_connection", return_value=conn):
            hook = RMQHook()
            params = hook._get_connection_params()
            assert params.host == "rmq.example.com"
            assert params.port == 5673
            assert params.virtual_host == "/myvhost"

    def test_vhost_override(self, fake_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            hook = RMQHook(vhost="/override")
            params = hook._get_connection_params()
            assert params.virtual_host == "/override"

    def test_credentials(self):
        conn = FakeAirflowConnection(login="admin", password="secret")
        with patch.object(RMQHook, "get_connection", return_value=conn):
            hook = RMQHook()
            params = hook._get_connection_params()
            assert params.credentials.username == "admin"
            assert params.credentials.password == "secret"

    def test_heartbeat_from_extras(self):
        conn = FakeAirflowConnection(extra=json.dumps({"heartbeat": 300}))
        with patch.object(RMQHook, "get_connection", return_value=conn):
            hook = RMQHook()
            params = hook._get_connection_params()
            assert params.heartbeat == 300

    def test_default_heartbeat(self, fake_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            hook = RMQHook()
            params = hook._get_connection_params()
            assert params.heartbeat == 600


# ---------------------------------------------------------------------------
# 3.3.2 — SSL params
# ---------------------------------------------------------------------------
class TestSSLParams:
    def test_ssl_disabled_by_default(self, fake_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            hook = RMQHook()
            params = hook._get_connection_params()
            assert params.ssl_options is None

    def test_ssl_enabled(self):
        conn = FakeAirflowConnection(port=0, extra=json.dumps({"ssl_enabled": True}))
        with patch.object(RMQHook, "get_connection", return_value=conn):
            hook = RMQHook()
            params = hook._get_connection_params()
            assert params.ssl_options is not None
            assert params.port == 5671

    def test_ssl_cert_none(self):
        conn = FakeAirflowConnection(
            extra=json.dumps({
                "ssl_enabled": True,
                "ssl_options": {"cert_reqs": "CERT_NONE"},
            })
        )
        with patch.object(RMQHook, "get_connection", return_value=conn):
            with patch("apache_airflow_provider_rmq.utils.ssl.ssl.create_default_context") as mock_ctx:
                ctx_instance = MagicMock(spec=ssl.SSLContext)
                mock_ctx.return_value = ctx_instance
                hook = RMQHook()
                hook._get_connection_params()
                assert ctx_instance.check_hostname is False
                assert ctx_instance.verify_mode == ssl.CERT_NONE

    def test_ssl_with_custom_port(self):
        conn = FakeAirflowConnection(port=5672, extra=json.dumps({"ssl_enabled": True}))
        with patch.object(RMQHook, "get_connection", return_value=conn):
            hook = RMQHook()
            params = hook._get_connection_params()
            # Explicit port takes precedence
            assert params.port == 5672


# ---------------------------------------------------------------------------
# 3.3.3 — Lazy connection
# ---------------------------------------------------------------------------
class TestLazyConnection:
    def test_no_connection_on_init(self):
        hook = RMQHook()
        assert hook._connection is None
        assert hook._channel is None

    def test_connection_created_on_get_channel(self, fake_connection, mock_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            with patch(f"{HOOK_MODULE}.pika.BlockingConnection", return_value=mock_connection):
                hook = RMQHook()
                channel = hook.get_channel()
                assert hook._connection is mock_connection
                assert channel is not None

    def test_channel_reused_on_second_call(self, hook_with_mocks):
        hook, mock_conn, mock_ch = hook_with_mocks
        ch1 = hook.get_channel()
        ch2 = hook.get_channel()
        assert ch1 is ch2
        mock_conn.channel.assert_called_once()


# ---------------------------------------------------------------------------
# 3.3.4 — Context manager
# ---------------------------------------------------------------------------
class TestContextManager:
    def test_enter_returns_hook(self, fake_connection, mock_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            with patch(f"{HOOK_MODULE}.pika.BlockingConnection", return_value=mock_connection):
                hook = RMQHook()
                result = hook.__enter__()
                assert result is hook
                hook.__exit__(None, None, None)

    def test_exit_closes(self, fake_connection, mock_connection, mock_channel):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            with patch(f"{HOOK_MODULE}.pika.BlockingConnection", return_value=mock_connection):
                with RMQHook() as hook:
                    pass
                assert hook._connection is None
                assert hook._channel is None


# ---------------------------------------------------------------------------
# 3.3.5 — Retry logic
# ---------------------------------------------------------------------------
class TestRetryLogic:
    def test_retries_on_connection_error(self, fake_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            mock_conn = MagicMock()
            type(mock_conn).is_closed = PropertyMock(return_value=False)
            with patch(
                f"{HOOK_MODULE}.pika.BlockingConnection",
                side_effect=[
                    pika.exceptions.AMQPConnectionError("fail1"),
                    pika.exceptions.AMQPConnectionError("fail2"),
                    mock_conn,
                ],
            ):
                hook = RMQHook(retry_count=3, retry_delay=0.01)
                result = hook._establish_connection()
                assert result is mock_conn

    def test_raises_after_max_retries(self, fake_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            with patch(
                f"{HOOK_MODULE}.pika.BlockingConnection",
                side_effect=pika.exceptions.AMQPConnectionError("always fail"),
            ):
                hook = RMQHook(retry_count=2, retry_delay=0.01)
                with pytest.raises(pika.exceptions.AMQPConnectionError):
                    hook._establish_connection()


# ---------------------------------------------------------------------------
# 3.3.6 — Queue operations
# ---------------------------------------------------------------------------
class TestQueueOperations:
    def test_queue_declare(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.queue_declare("test_queue", durable=True)
        mock_ch.queue_declare.assert_called_once_with(
            queue="test_queue", passive=False, durable=True,
            exclusive=False, auto_delete=False, arguments=None,
        )

    def test_queue_delete(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.queue_delete("test_queue", if_unused=True)
        mock_ch.queue_delete.assert_called_once_with(
            queue="test_queue", if_unused=True, if_empty=False,
        )

    def test_queue_bind(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.queue_bind(queue="q", exchange="ex", routing_key="rk")
        mock_ch.queue_bind.assert_called_once_with(
            queue="q", exchange="ex", routing_key="rk", arguments=None,
        )

    def test_queue_unbind(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.queue_unbind(queue="q", exchange="ex", routing_key="rk")
        mock_ch.queue_unbind.assert_called_once_with(
            queue="q", exchange="ex", routing_key="rk", arguments=None,
        )

    def test_queue_purge(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.queue_purge("test_queue")
        mock_ch.queue_purge.assert_called_once_with(queue="test_queue")

    def test_queue_info_existing(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        method = MagicMock()
        method.method.queue = "test_queue"
        method.method.message_count = 5
        method.method.consumer_count = 2
        mock_ch.queue_declare.return_value = method
        info = hook.queue_info("test_queue")
        assert info == {
            "queue": "test_queue",
            "message_count": 5,
            "consumer_count": 2,
            "exists": True,
        }


# ---------------------------------------------------------------------------
# 3.3.7 — Exchange operations
# ---------------------------------------------------------------------------
class TestExchangeOperations:
    def test_exchange_declare(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.exchange_declare("test_ex", exchange_type="topic", durable=True)
        mock_ch.exchange_declare.assert_called_once_with(
            exchange="test_ex", exchange_type="topic", passive=False,
            durable=True, auto_delete=False, internal=False, arguments=None,
        )

    def test_exchange_delete(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.exchange_delete("test_ex", if_unused=True)
        mock_ch.exchange_delete.assert_called_once_with(
            exchange="test_ex", if_unused=True,
        )

    def test_exchange_bind(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.exchange_bind(destination="dst", source="src", routing_key="rk")
        mock_ch.exchange_bind.assert_called_once_with(
            destination="dst", source="src", routing_key="rk", arguments=None,
        )

    def test_exchange_unbind(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.exchange_unbind(destination="dst", source="src", routing_key="rk")
        mock_ch.exchange_unbind.assert_called_once_with(
            destination="dst", source="src", routing_key="rk", arguments=None,
        )


# ---------------------------------------------------------------------------
# 3.3.8 — Publish and consume
# ---------------------------------------------------------------------------
class TestPublishAndConsume:
    def test_basic_publish_string(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.basic_publish(exchange="ex", routing_key="rk", body="hello")
        mock_ch.basic_publish.assert_called_once()
        call_kwargs = mock_ch.basic_publish.call_args
        assert call_kwargs.kwargs["body"] == b"hello"
        assert call_kwargs.kwargs["exchange"] == "ex"
        assert call_kwargs.kwargs["routing_key"] == "rk"

    def test_basic_publish_bytes(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.basic_publish(exchange="", routing_key="q", body=b"raw bytes")
        call_kwargs = mock_ch.basic_publish.call_args
        assert call_kwargs.kwargs["body"] == b"raw bytes"

    def test_basic_publish_with_properties(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        props = pika.BasicProperties(content_type="application/json")
        hook.basic_publish(exchange="", routing_key="q", body="data", properties=props)
        call_kwargs = mock_ch.basic_publish.call_args
        assert call_kwargs.kwargs["properties"] is props

    def test_consume_messages(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        msg1 = make_message(body="msg1", delivery_tag=1)
        msg2 = make_message(body="msg2", delivery_tag=2)
        mock_ch.consume.return_value = iter([msg1, msg2, (None, None, None)])
        result = hook.consume_messages("test_queue", max_messages=10)
        assert len(result) == 2
        assert result[0]["body"] == "msg1"
        assert result[1]["body"] == "msg2"
        mock_ch.cancel.assert_called_once()

    def test_consume_messages_respects_max(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        msg1 = make_message(body="msg1", delivery_tag=1)
        msg2 = make_message(body="msg2", delivery_tag=2)
        mock_ch.consume.return_value = iter([msg1, msg2])
        result = hook.consume_messages("test_queue", max_messages=1)
        assert len(result) == 1
        assert result[0]["body"] == "msg1"

    def test_consume_messages_empty_queue(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        mock_ch.consume.return_value = iter([(None, None, None)])
        result = hook.consume_messages("test_queue", max_messages=10)
        assert result == []


# ---------------------------------------------------------------------------
# 3.3.9 — Ack / Nack
# ---------------------------------------------------------------------------
class TestAckNack:
    def test_ack(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.ack(delivery_tag=42)
        mock_ch.basic_ack.assert_called_once_with(delivery_tag=42)

    def test_nack_with_requeue(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.nack(delivery_tag=42, requeue=True)
        mock_ch.basic_nack.assert_called_once_with(delivery_tag=42, requeue=True)

    def test_nack_without_requeue(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        hook.nack(delivery_tag=42, requeue=False)
        mock_ch.basic_nack.assert_called_once_with(delivery_tag=42, requeue=False)


# ---------------------------------------------------------------------------
# 3.3.10 — DLQ helper
# ---------------------------------------------------------------------------
class TestDLQHelper:
    def test_basic_dlq(self):
        args = RMQHook.build_dlq_arguments(dlx_exchange="dlx")
        assert args == {"x-dead-letter-exchange": "dlx"}

    def test_dlq_with_routing_key(self):
        args = RMQHook.build_dlq_arguments(dlx_exchange="dlx", dlx_routing_key="dead")
        assert args == {
            "x-dead-letter-exchange": "dlx",
            "x-dead-letter-routing-key": "dead",
        }

    def test_dlq_with_ttl(self):
        args = RMQHook.build_dlq_arguments(dlx_exchange="dlx", message_ttl=60000)
        assert args == {
            "x-dead-letter-exchange": "dlx",
            "x-message-ttl": 60000,
        }

    def test_dlq_full(self):
        args = RMQHook.build_dlq_arguments(
            dlx_exchange="dlx", dlx_routing_key="dead", message_ttl=30000
        )
        assert args == {
            "x-dead-letter-exchange": "dlx",
            "x-dead-letter-routing-key": "dead",
            "x-message-ttl": 30000,
        }

    def test_dlq_default_exchange(self):
        args = RMQHook.build_dlq_arguments()
        assert args == {"x-dead-letter-exchange": ""}


# ---------------------------------------------------------------------------
# 3.3.11 — queue_info with ChannelClosedByBroker
# ---------------------------------------------------------------------------
class TestQueueInfoChannelClosed:
    def test_queue_info_nonexistent(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        mock_ch.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND")
        info = hook.queue_info("nonexistent_queue")
        assert info == {
            "queue": "nonexistent_queue",
            "message_count": 0,
            "exists": False,
        }
        # Channel should be reset
        assert hook._channel is None


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------
class TestTestConnection:
    def test_success(self, fake_connection, mock_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            with patch(f"{HOOK_MODULE}.pika.BlockingConnection", return_value=mock_connection):
                hook = RMQHook()
                ok, msg = hook.test_connection()
                assert ok is True
                assert msg == "Connection successfully tested"
                mock_connection.close.assert_called_once()

    def test_failure(self, fake_connection):
        with patch.object(RMQHook, "get_connection", return_value=fake_connection):
            with patch(
                f"{HOOK_MODULE}.pika.BlockingConnection",
                side_effect=pika.exceptions.AMQPConnectionError("refused"),
            ):
                hook = RMQHook(retry_count=1)
                ok, msg = hook.test_connection()
                assert ok is False
                assert "refused" in msg


# ---------------------------------------------------------------------------
# get_connection_form_widgets
# ---------------------------------------------------------------------------
class TestConnectionFormWidgets:
    def test_returns_dict_with_expected_keys(self):
        widgets = RMQHook.get_connection_form_widgets()
        assert isinstance(widgets, dict)
        assert set(widgets.keys()) == {"ssl_enabled", "ca_certs", "certfile", "keyfile"}


# ---------------------------------------------------------------------------
# consume_messages — channel.cancel() safety
# ---------------------------------------------------------------------------
class TestConsumeMessagesCancel:
    def test_cancel_error_does_not_propagate(self, hook_with_mocks):
        hook, _, mock_ch = hook_with_mocks
        mock_ch.consume.return_value = iter([(None, None, None)])
        mock_ch.cancel.side_effect = RuntimeError("cancel failed")
        # Should not raise
        result = hook.consume_messages("q")
        assert result == []