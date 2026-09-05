from __future__ import annotations

import asyncio
from collections import deque
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow_provider_rmq.triggers.rmq import RMQTrigger
from airflow_provider_rmq.utils.amqp import DEFAULT_HEARTBEAT, match_and_ack
from tests.conftest import FakeAirflowConnection


# ---------------------------------------------------------------------------
# Serialize / Deserialize
# ---------------------------------------------------------------------------
class TestSerialize:
    def test_serialize_full(self):
        trigger = RMQTrigger(
            rmq_conn_id="my_conn",
            queue_name="my_queue",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=10.0,
        )
        classpath, kwargs = trigger.serialize()
        assert classpath == "airflow_provider_rmq.triggers.rmq.RMQTrigger"
        assert kwargs == {
            "rmq_conn_id": "my_conn",
            "queue_name": "my_queue",
            "filter_data": {"filter_headers": {"x-type": "order"}},
            "poll_interval": 10.0,
            "mode": "pull",
            "message_wait_timeout": None,
            "ack_unmatched": False,
        }

    def test_serialize_defaults(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        _, kwargs = trigger.serialize()
        assert kwargs["filter_data"] == {}
        assert kwargs["poll_interval"] == 5.0

    def test_roundtrip(self):
        original = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"key": "val"}},
            poll_interval=3.0,
        )
        _, kwargs = original.serialize()
        restored = RMQTrigger(**kwargs)
        assert restored.rmq_conn_id == original.rmq_conn_id
        assert restored.queue_name == original.queue_name
        assert restored.filter_data == original.filter_data
        assert restored.poll_interval == original.poll_interval


# ---------------------------------------------------------------------------
# Helper to run async trigger and collect events
# ---------------------------------------------------------------------------
async def _collect_events(trigger: RMQTrigger) -> list[dict]:
    events = []
    async for event in trigger.run():
        events.append(event.payload)
    return events


def _make_fake_message(body=b"hello", headers=None, routing_key="rk", exchange=""):
    """Create a fake aio_pika message with required attributes."""
    msg = MagicMock()
    msg.body = body
    msg.headers = headers or {}
    msg.routing_key = routing_key
    msg.exchange = exchange
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    return msg


def _make_fake_connection(fake_queue):
    """Create fake aio_pika connection/channel wired to given queue."""
    fake_channel = MagicMock()
    fake_channel.declare_queue = AsyncMock(return_value=fake_queue)

    fake_connection = AsyncMock()
    fake_connection.__aenter__ = AsyncMock(return_value=fake_connection)
    fake_connection.__aexit__ = AsyncMock(return_value=False)
    fake_connection.channel = AsyncMock(return_value=fake_channel)
    return fake_connection


# ---------------------------------------------------------------------------
# Run — matching message (no filter)
# ---------------------------------------------------------------------------
class TestRunNoFilter:
    @pytest.mark.asyncio
    async def test_any_message_matches(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_message = _make_fake_message(
            body=b"hello", headers={"x-source": "test"}, routing_key="rk", exchange="my_ex",
        )
        fake_queue = MagicMock()
        fake_queue.get = AsyncMock(return_value=fake_message)

        fake_connection = _make_fake_connection(fake_queue)
        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "hello"
        assert events[0]["message"]["headers"] == {"x-source": "test"}
        assert events[0]["message"]["routing_key"] == "rk"
        assert events[0]["message"]["exchange"] == "my_ex"
        fake_message.ack.assert_awaited_once()


# ---------------------------------------------------------------------------
# Run — with dict filter, matching
# ---------------------------------------------------------------------------
class TestRunWithFilter:
    @pytest.mark.asyncio
    async def test_matching_message_acked(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
        )

        fake_message = _make_fake_message(
            body=b"order data", headers={"x-type": "order"}, routing_key="orders",
        )
        fake_queue = MagicMock()
        fake_queue.get = AsyncMock(return_value=fake_message)

        fake_connection = _make_fake_connection(fake_queue)
        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "order data"
        fake_message.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_non_matching_then_matching(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=0.01,
        )

        non_match_msg = _make_fake_message(
            body=b"invoice", headers={"x-type": "invoice"}, routing_key="invoices",
        )
        match_msg = _make_fake_message(
            body=b"order", headers={"x-type": "order"}, routing_key="orders",
        )

        fake_queue = MagicMock()
        fake_queue.get = AsyncMock(side_effect=[non_match_msg, match_msg])

        fake_connection = _make_fake_connection(fake_queue)
        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "order"
        non_match_msg.nack.assert_awaited_once_with(requeue=True)
        match_msg.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_nack_uses_short_delay_not_poll_interval(self):
        """After NACK, sleep should use a short delay (0.1s), not poll_interval."""
        poll_interval = 30.0
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=poll_interval,
        )

        non_match_msg = _make_fake_message(
            body=b"invoice", headers={"x-type": "invoice"}, routing_key="invoices",
        )
        match_msg = _make_fake_message(
            body=b"order", headers={"x-type": "order"}, routing_key="orders",
        )

        fake_queue = MagicMock()
        fake_queue.get = AsyncMock(side_effect=[non_match_msg, match_msg])

        fake_connection = _make_fake_connection(fake_queue)
        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)
                with patch("airflow_provider_rmq.triggers.rmq.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                    events = await _collect_events(trigger)

        assert len(events) == 1
        # After NACK the trigger must sleep with the short delay, not poll_interval
        mock_sleep.assert_awaited_once_with(0.1)
        non_match_msg.nack.assert_awaited_once_with(requeue=True)


# ---------------------------------------------------------------------------
# Run — empty queue polls
# ---------------------------------------------------------------------------
class TestRunEmptyQueue:
    @pytest.mark.asyncio
    async def test_polls_then_gets_message(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            poll_interval=0.01,
        )

        fake_message = _make_fake_message(body=b"delayed", headers={}, routing_key="rk")

        fake_queue = MagicMock()
        fake_queue.get = AsyncMock(side_effect=[None, None, fake_message])

        fake_connection = _make_fake_connection(fake_queue)
        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "delayed"
        assert fake_queue.get.await_count == 3


# ---------------------------------------------------------------------------
# Run — connection error
# ---------------------------------------------------------------------------
class TestRunError:
    @pytest.mark.asyncio
    async def test_connection_error_yields_error_event(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("refused"))
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "error"
        assert "refused" in events[0]["error"]


# ---------------------------------------------------------------------------
# SSL URL construction
# ---------------------------------------------------------------------------
class TestSSLUrl:
    @pytest.mark.asyncio
    async def test_ssl_uses_amqps_scheme_and_default_port(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_conn_info = FakeAirflowConnection(
            host="rmq.example.com", port=None,
            extra='{"ssl_enabled": true}',
        )

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("test"))
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        url = call_kwargs["url"]
        assert url.startswith("amqps://")
        assert "rmq.example.com:5671" in url

    @pytest.mark.asyncio
    async def test_ssl_passes_ssl_context(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_conn_info = FakeAirflowConnection(
            host="rmq.example.com", port=None,
            extra='{"ssl_enabled": true}',
        )

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("test"))
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                await _collect_events(trigger)

        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert "ssl_context" in call_kwargs
        import ssl
        assert isinstance(call_kwargs["ssl_context"], ssl.SSLContext)

    @pytest.mark.asyncio
    async def test_no_ssl_context_when_disabled(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("test"))
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                await _collect_events(trigger)

        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert "ssl_context" not in call_kwargs


class TestHeartbeatUrl:
    @pytest.mark.asyncio
    async def test_url_carries_default_heartbeat(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("test"))
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                await _collect_events(trigger)

        url = mock_aio_pika.connect_robust.call_args.kwargs["url"]
        assert url.endswith(f"?heartbeat={DEFAULT_HEARTBEAT}")

    @pytest.mark.asyncio
    async def test_url_carries_heartbeat_from_extra(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_conn_info = FakeAirflowConnection(extra='{"heartbeat": 12}')

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("test"))
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                await _collect_events(trigger)

        url = mock_aio_pika.connect_robust.call_args.kwargs["url"]
        assert url.endswith("?heartbeat=12")


# ---------------------------------------------------------------------------
# URL encoding of credentials
# ---------------------------------------------------------------------------
class TestUrlEncoding:
    @pytest.mark.asyncio
    async def test_special_chars_in_credentials_are_encoded(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")

        fake_conn_info = FakeAirflowConnection(
            login="user@domain", password="p@ss:word/123",
        )

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("test"))
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                await _collect_events(trigger)

        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        url = call_kwargs["url"]
        # @ in login must be encoded as %40
        assert "user%40domain" in url
        # : and / in password must be encoded
        assert "p%40ss%3Aword%2F123" in url
        # The URL structure should still be valid (scheme://user:pass@host:port/vhost)
        assert url.startswith("amqp://")
        assert "@localhost:" in url

# ---------------------------------------------------------------------------
# Push mode helpers
# ---------------------------------------------------------------------------
def _make_push_queue(messages: list):
    """Fake queue with iterator() yielding the given messages."""
    class _AsyncIter:
        def __init__(self, items):
            self._items = iter(items)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return next(self._items)
            except StopIteration:
                raise StopAsyncIteration

    class _IterCM:
        async def __aenter__(self):
            return _AsyncIter(messages)

        async def __aexit__(self, *a):
            return False

    q = MagicMock()
    q.get = AsyncMock(return_value=None)  # pull path unused
    q.iterator = MagicMock(side_effect=lambda: _IterCM())
    return q


def _make_blocking_push_queue():
    """Fake queue whose iterator blocks forever — for timeout tests."""
    class _BlockingIter:
        def __aiter__(self):
            return self

        async def __anext__(self):
            await asyncio.sleep(999)  # cancelled by asyncio.wait_for

    class _IterCM:
        async def __aenter__(self):
            return _BlockingIter()

        async def __aexit__(self, *a):
            return False

    q = MagicMock()
    q.iterator = MagicMock(side_effect=lambda: _IterCM())
    return q


# ---------------------------------------------------------------------------
# Push mode — no filter
# ---------------------------------------------------------------------------
class TestPushModeNoFilter:
    @pytest.mark.asyncio
    async def test_first_message_taken(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")

        fake_message = _make_fake_message(
            body=b"hello", headers={"x-source": "test"}, routing_key="rk", exchange="ex",
        )
        fake_queue = _make_push_queue([fake_message])
        fake_connection = _make_fake_connection(fake_queue)
        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "hello"
        assert events[0]["message"]["headers"] == {"x-source": "test"}
        assert events[0]["message"]["routing_key"] == "rk"
        assert events[0]["message"]["exchange"] == "ex"
        fake_message.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_binary_body_is_decoded_with_replacement(self):
        """The ACK is already behind the decode, so a binary payload must not raise:
        the event would be gone with nothing left to redeliver."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")

        fake_message = _make_fake_message(
            body=b"\xff\xfe not utf-8", headers={}, routing_key="rk", exchange="ex",
        )
        fake_queue = _make_push_queue([fake_message])
        fake_connection = _make_fake_connection(fake_queue)

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=FakeAirflowConnection())

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert "\ufffd" in events[0]["message"]["body"]
        fake_message.ack.assert_awaited_once()


# ---------------------------------------------------------------------------
# Push mode — with filter
# ---------------------------------------------------------------------------
class TestPushModeWithFilter:
    @pytest.mark.asyncio
    async def test_non_matching_nacked_matching_acked(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            mode="push",
            filter_data={"filter_headers": {"x-type": "order"}},
        )

        non_match = _make_fake_message(body=b"noise", headers={"x-type": "analytics"})
        match = _make_fake_message(body=b"order", headers={"x-type": "order"}, routing_key="orders")

        fake_queue = _make_push_queue([non_match, match])
        fake_connection = _make_fake_connection(fake_queue)
        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "order"
        non_match.nack.assert_awaited_once_with(requeue=True)
        match.ack.assert_awaited_once()


# ---------------------------------------------------------------------------
# Push mode — timeout
# ---------------------------------------------------------------------------
class TestPushModeTimeout:
    @pytest.mark.asyncio
    async def test_message_wait_timeout_yields_timeout_event(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            mode="push",
            message_wait_timeout=0.05,
        )

        fake_queue = _make_blocking_push_queue()
        fake_connection = _make_fake_connection(fake_queue)
        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "timeout"


# ---------------------------------------------------------------------------
# Push mode — serialize / roundtrip
# ---------------------------------------------------------------------------
class TestPushModeSerialize:
    def test_serialize_with_mode_and_message_wait_timeout(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            mode="push",
            message_wait_timeout=30.0,
        )
        _, kwargs = trigger.serialize()
        assert kwargs["mode"] == "push"
        assert kwargs["message_wait_timeout"] == 30.0

    def test_serialize_defaults_mode_pull(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        _, kwargs = trigger.serialize()
        assert kwargs["mode"] == "pull"
        assert kwargs["message_wait_timeout"] is None

    def test_roundtrip_push_mode(self):
        original = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            mode="push",
            message_wait_timeout=15.0,
        )
        _, kwargs = original.serialize()
        restored = RMQTrigger(**kwargs)
        assert restored.mode == "push"
        assert restored.message_wait_timeout == 15.0


# ---------------------------------------------------------------------------
# Push mode — connection error
# ---------------------------------------------------------------------------
class TestPushModeError:
    @pytest.mark.asyncio
    async def test_connection_error_yields_error_event(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")

        fake_conn_info = FakeAirflowConnection()

        with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
            mock_aio_pika.connect_robust = AsyncMock(side_effect=ConnectionError("refused"))
            with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
                mock_base.get_connection = MagicMock(return_value=fake_conn_info)

                events = await _collect_events(trigger)

        assert len(events) == 1
        assert events[0]["status"] == "error"
        assert "refused" in events[0]["error"]


# ---------------------------------------------------------------------------
# ack_unmatched — serialization and validation
# ---------------------------------------------------------------------------
class TestAckUnmatchedParameter:
    def test_serialize_carries_the_flag_and_restores_it(self):
        original = RMQTrigger(rmq_conn_id="conn", queue_name="q", ack_unmatched=True)
        _, kwargs = original.serialize()
        assert kwargs["ack_unmatched"] is True

        restored = RMQTrigger(**kwargs)
        assert restored.ack_unmatched is True

    def test_default_is_false(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        assert trigger.ack_unmatched is False
        assert trigger.serialize()[1]["ack_unmatched"] is False

    @pytest.mark.parametrize("value", ["false", "true", 1, 0, None, [], "yes"])
    def test_non_bool_is_rejected(self, value):
        with pytest.raises(ValueError, match="ack_unmatched"):
            RMQTrigger(rmq_conn_id="conn", queue_name="q", ack_unmatched=value)


# ---------------------------------------------------------------------------
# ack_unmatched — pull and push behaviour
# ---------------------------------------------------------------------------
def _make_broker_queue(stray_bodies, tail):
    """Fake pull queue that models the broker: a requeued NACK puts the message back.

    :param stray_bodies: Headers of the messages already sitting in the queue.
    :param tail: Values ``get`` returns once the queue has drained — ``None`` stands for
        an empty poll, a message for a new arrival.
    :returns: ``(queue, pending, messages)``.
    """
    pending: deque = deque()

    def _make(headers):
        msg = _make_fake_message(body=b"stray", headers=headers, routing_key="rk")

        async def _nack(requeue=False):
            if requeue:
                pending.append(msg)

        msg.nack = AsyncMock(side_effect=_nack)
        return msg

    messages = [_make(h) for h in stray_bodies]
    pending.extend(messages)

    tail_iter = iter(tail)

    async def _get(fail=False):
        if pending:
            return pending.popleft()
        return next(tail_iter)

    q = MagicMock()
    q.get = AsyncMock(side_effect=_get)
    return q, pending, messages


async def _run_against(trigger, fake_queue):
    fake_connection = _make_fake_connection(fake_queue)
    with patch("airflow_provider_rmq.triggers.rmq.aio_pika") as mock_aio_pika:
        mock_aio_pika.connect_robust = AsyncMock(return_value=fake_connection)
        with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
            mock_base.get_connection = MagicMock(return_value=FakeAirflowConnection())
            return await _collect_events(trigger)


class TestAckUnmatchedPullMode:
    @pytest.mark.asyncio
    async def test_non_matching_message_is_acked(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            ack_unmatched=True,
        )
        match_msg = _make_fake_message(body=b"order", headers={"x-type": "order"})
        fake_queue, _pending, strays = _make_broker_queue(
            [{"x-type": "invoice"}], tail=[match_msg],
        )

        events = await _run_against(trigger, fake_queue)

        assert events[0]["status"] == "success"
        strays[0].ack.assert_awaited_once()
        strays[0].nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ack_unmatched_off_returns_the_message_to_the_queue(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=0.01,
        )
        match_msg = _make_fake_message(body=b"order", headers={"x-type": "order"})
        stray = _make_fake_message(body=b"invoice", headers={"x-type": "invoice"})
        fake_queue = MagicMock()
        fake_queue.get = AsyncMock(side_effect=[stray, match_msg])

        events = await _run_against(trigger, fake_queue)

        assert events[0]["status"] == "success"
        stray.nack.assert_awaited_once_with(requeue=True)
        stray.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_queue_of_stray_messages_drains_in_one_turn_each(self):
        """N stray messages take N turns and leave the queue empty, not a redelivery loop."""
        n = 5
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=0.01,
            ack_unmatched=True,
        )
        match_msg = _make_fake_message(body=b"order", headers={"x-type": "order"})
        # None stands for the empty poll that follows the drain: from there the trigger
        # waits for new deliveries instead of turning over the same messages again.
        fake_queue, pending, strays = _make_broker_queue(
            [{"x-type": "invoice"}] * n, tail=[None, match_msg],
        )

        events = await _run_against(trigger, fake_queue)

        assert events[0]["status"] == "success"
        assert len(pending) == 0
        assert fake_queue.get.await_count == n + 2
        for stray in strays:
            stray.ack.assert_awaited_once()
            stray.nack.assert_not_awaited()


class TestAckUnmatchedPushMode:
    @pytest.mark.asyncio
    async def test_non_matching_message_is_acked(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            mode="push",
            ack_unmatched=True,
        )
        stray = _make_fake_message(body=b"invoice", headers={"x-type": "invoice"})
        match_msg = _make_fake_message(body=b"order", headers={"x-type": "order"})
        fake_queue = _make_push_queue([stray, match_msg])

        events = await _run_against(trigger, fake_queue)

        assert events[0]["status"] == "success"
        stray.ack.assert_awaited_once()
        stray.nack.assert_not_awaited()
        match_msg.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ack_unmatched_off_returns_the_message_to_the_queue(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            mode="push",
        )
        stray = _make_fake_message(body=b"invoice", headers={"x-type": "invoice"})
        match_msg = _make_fake_message(body=b"order", headers={"x-type": "order"})
        fake_queue = _make_push_queue([stray, match_msg])

        events = await _run_against(trigger, fake_queue)

        assert events[0]["status"] == "success"
        stray.nack.assert_awaited_once_with(requeue=True)
        stray.ack.assert_not_awaited()


# ---------------------------------------------------------------------------
# Filter miss state
# ---------------------------------------------------------------------------
class TestFilterMissState:
    @pytest.mark.asyncio
    async def test_state_names_the_queue_and_logs_to_the_task_logger(self):
        """The threshold WARNING must reach the task log, which hangs off ``airflow.task``."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="my_queue", ack_unmatched=True)
        captured = []

        async def _fake_match_and_ack(message, msg_filter, miss_state):
            captured.append(miss_state)
            return True

        fake_queue = MagicMock()
        fake_queue.get = AsyncMock(return_value=_make_fake_message())

        with patch("airflow_provider_rmq.triggers.rmq.match_and_ack", new=_fake_match_and_ack):
            await _run_against(trigger, fake_queue)

        assert captured[0].queue_name == "my_queue"
        assert captured[0].log.name == "airflow.task"
        assert captured[0].ack_unmatched is True

    @pytest.mark.asyncio
    async def test_one_state_per_run_counting_across_deliveries(self):
        """Counters span the whole run(): a per-delivery state would never reach a threshold."""
        real_match_and_ack = match_and_ack
        seen = []
        streaks = []

        async def _recording(message, msg_filter, miss_state):
            seen.append(miss_state)
            result = await real_match_and_ack(message, msg_filter, miss_state)
            streaks.append(miss_state.streak)
            return result

        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=0.01,
            ack_unmatched=True,
        )
        match_msg = _make_fake_message(body=b"order", headers={"x-type": "order"})
        fake_queue, _pending, _strays = _make_broker_queue(
            [{"x-type": "invoice"}] * 3, tail=[match_msg],
        )

        with patch("airflow_provider_rmq.triggers.rmq.match_and_ack", new=_recording):
            events = await _run_against(trigger, fake_queue)

        assert events[0]["status"] == "success"
        assert all(state is seen[0] for state in seen)
        # The run of misses grows across deliveries and the match ends it; dropped is
        # cleared by nothing and keeps the three messages that are gone for good.
        assert streaks == [1, 2, 3, 0]
        assert seen[0].dropped == 3

    @pytest.mark.asyncio
    async def test_push_mode_shares_the_same_state(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            mode="push",
            ack_unmatched=True,
        )
        seen = []
        real_match_and_ack = match_and_ack

        async def _recording(message, msg_filter, miss_state):
            seen.append(miss_state)
            return await real_match_and_ack(message, msg_filter, miss_state)

        messages = [
            _make_fake_message(body=b"invoice", headers={"x-type": "invoice"}),
            _make_fake_message(body=b"invoice", headers={"x-type": "invoice"}),
            _make_fake_message(body=b"order", headers={"x-type": "order"}),
        ]
        fake_queue = _make_push_queue(messages)

        with patch("airflow_provider_rmq.triggers.rmq.match_and_ack", new=_recording):
            events = await _run_against(trigger, fake_queue)

        assert events[0]["status"] == "success"
        assert all(state is seen[0] for state in seen)
        assert seen[0].dropped == 2
