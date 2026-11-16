from __future__ import annotations

import asyncio
import contextlib
import itertools
import ssl
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import aiormq
import pamqp.frame
import pytest
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import ChannelInvalidStateError
from pamqp import commands as spec
from pamqp.header import ContentHeader

from airflow_provider_rmq.triggers.rmq import (
    RMQTrigger,
    _ConnectionLost,
    _step,
    _StepTimeout,
)
from airflow_provider_rmq.utils.amqp import DEFAULT_HEARTBEAT, match_and_ack
from airflow_provider_rmq.utils.filters import FilterVerdict
from tests.conftest import STREAK_TEXT, FakeAirflowConnection, log_lines

ACK_TEXT = "ACK not confirmed"


# ---------------------------------------------------------------------------
# aiormq-level fakes
# ---------------------------------------------------------------------------
@dataclass(eq=False)
class Msg:
    """A message sitting in the fake broker's queue."""

    body: bytes = b"hello"
    headers: dict | None = None
    routing_key: str = "rk"
    exchange: str = ""


def _future() -> asyncio.Future:
    """A future whose exception is read when it lands, as ``aiormq`` does for ``closing``."""
    future = asyncio.get_running_loop().create_future()
    future.add_done_callback(lambda f: None if f.cancelled() else f.exception())
    return future


async def _forever() -> None:
    await asyncio.get_running_loop().create_future()


def hang():
    """Hook: the broker call never answers."""
    async def _hook(**_ctx):
        await _forever()
    return _hook


def fail(exc: BaseException):
    """Hook: the broker call raises ``exc``."""
    async def _hook(**_ctx):
        raise exc
    return _hook


@dataclass(eq=False)
class FakeBroker:
    """One queue behind ``aiormq.Connection``: connections and channels made on demand.

    ``pending`` is the queue. ``basic_get`` takes from it, then from ``tail`` (``None``
    there is an empty poll), and waits forever once both are drained. ``basic_consume``
    hands every pending message to the consumer before it returns, the way deliveries
    already in flight reach the callback before the caller looks. A requeued NACK puts
    the message back at the end of ``pending``.

    ``hooks`` maps a step name — ``connect``, ``channel``, ``declare``, ``get``,
    ``consume``, ``ack``, ``nack`` — to a coroutine function called with the
    connection and the channel before the step does its work.
    """

    pending: deque = field(default_factory=deque)
    tail: deque = field(default_factory=deque)
    hooks: dict = field(default_factory=dict)
    calls: list = field(default_factory=list)
    connections: list = field(default_factory=list)
    acked: list = field(default_factory=list)
    nacked: list = field(default_factory=list)

    def factory(self, url, *, context=None):
        connection = FakeConnection(self, url, context)
        self.connections.append(connection)
        return connection

    async def call(self, step, connection, channel=None):
        self.calls.append(step)
        hook = self.hooks.get(step)
        if hook is not None:
            await hook(connection=connection, channel=channel)


class FakeConnection:
    basic_nack = True

    def __init__(self, broker: FakeBroker, url: str, context: Any):
        self.broker = broker
        self.url = url
        self.context = context
        self.closing = _future()
        self.channels: list[FakeChannel] = []
        self.close_calls = 0

    @property
    def is_closed(self) -> bool:
        return self.closing.done()

    async def connect(self):
        await self.broker.call("connect", self)
        return True

    async def channel(self, publisher_confirms=True):
        assert publisher_confirms is False
        channel = FakeChannel(self)
        self.channels.append(channel)
        await self.broker.call("channel", self, channel)
        return channel

    def lose(self, reason: BaseException) -> None:
        """Close the connection and its channels with ``reason``, as ``aiormq`` does."""
        for channel in self.channels:
            if not channel.closing.done():
                channel.closing.set_exception(reason)
        if not self.closing.done():
            self.closing.set_exception(reason)

    async def close(self):
        self.close_calls += 1
        self.lose(asyncio.CancelledError())


class FakeChannel:
    def __init__(self, connection: FakeConnection):
        self.connection = connection
        self.broker = connection.broker
        self.closing = _future()
        self.consumers: dict = {}
        self.tags = itertools.count(1)
        self.in_flight: dict[int, Msg] = {}
        self.declared: list = []

    @property
    def is_closed(self) -> bool:
        return self.closing.done()

    def _delivered(self, msg: Msg, consumer_tag: str | None = None) -> DeliveredMessage:
        tag = next(self.tags)
        self.in_flight[tag] = msg
        if consumer_tag is None:
            frame = spec.Basic.GetOk(
                delivery_tag=tag, routing_key=msg.routing_key, exchange=msg.exchange, message_count=0
            )
        else:
            frame = spec.Basic.Deliver(
                consumer_tag=consumer_tag, delivery_tag=tag,
                routing_key=msg.routing_key, exchange=msg.exchange,
            )
        header = ContentHeader(
            body_size=len(msg.body), properties=spec.Basic.Properties(headers=msg.headers)
        )
        return DeliveredMessage(delivery=frame, header=header, body=msg.body, channel=self)

    async def queue_declare(self, queue, passive=False):
        await self.broker.call("declare", self.connection, self)
        self.declared.append((queue, passive))
        return spec.Queue.DeclareOk(queue=queue)

    async def basic_get(self, queue, no_ack=False):
        await self.broker.call("get", self.connection, self)
        assert no_ack is False
        if self.broker.pending:
            return self._delivered(self.broker.pending.popleft())
        if self.broker.tail:
            item = self.broker.tail.popleft()
            if item is not None:
                return self._delivered(item)
            return DeliveredMessage(
                delivery=spec.Basic.GetEmpty(), header=ContentHeader(), body=b"", channel=self
            )
        await _forever()

    async def basic_consume(self, queue, callback, *, consumer_tag=None):
        await self.broker.call("consume", self.connection, self)
        self.consumers[consumer_tag] = callback
        while self.broker.pending:
            await callback(self._delivered(self.broker.pending.popleft(), consumer_tag))
        return spec.Basic.ConsumeOk(consumer_tag=consumer_tag)

    async def deliver(self, msg: Msg) -> None:
        """A message arriving at the consumer after ``basic_consume`` returned."""
        (consumer_tag, callback), = self.consumers.items()
        await callback(self._delivered(msg, consumer_tag))

    async def basic_ack(self, delivery_tag, multiple=False):
        await self.broker.call("ack", self.connection, self)
        self.broker.acked.append(self.in_flight.pop(delivery_tag))

    async def basic_nack(self, delivery_tag, multiple=False, requeue=True):
        await self.broker.call("nack", self.connection, self)
        msg = self.in_flight.pop(delivery_tag)
        self.broker.nacked.append((msg, requeue))
        if requeue:
            self.broker.pending.append(msg)


@contextlib.contextmanager
def _patched(broker: FakeBroker, conn_info: FakeAirflowConnection | None = None):
    with patch.object(aiormq, "Connection", broker.factory), patch(
        "airflow_provider_rmq.triggers.rmq.BaseHook"
    ) as mock_base:
        mock_base.get_connection = MagicMock(return_value=conn_info or FakeAirflowConnection())
        yield


async def _collect_events(trigger: RMQTrigger) -> list[dict]:
    events = []
    async for event in trigger.run():
        events.append(event.payload)
    return events


async def _run_against(trigger, broker, conn_info=None) -> list[dict]:
    with _patched(broker, conn_info):
        return await _collect_events(trigger)


def _broker(*messages: Msg, tail=()) -> FakeBroker:
    return FakeBroker(pending=deque(messages), tail=deque(tail))


def _timeouts(rpc: float = 30, connect: float = 15) -> FakeAirflowConnection:
    return FakeAirflowConnection(extra=f'{{"rpc_timeout": {rpc}, "connect_timeout": {connect}}}')


async def _settle() -> None:
    """Let abandoned calls run their cancellation out."""
    for _ in range(5):
        await asyncio.sleep(0)


def _stray_tasks() -> list[asyncio.Task]:
    current = asyncio.current_task()
    return [t for t in asyncio.all_tasks() if t is not current and not t.done()]


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

    @pytest.mark.asyncio
    async def test_trigger_restored_from_stored_kwargs_runs(self):
        """A trigger row written to the metadatabase carries ``mode`` and still runs."""
        stored = {
            "rmq_conn_id": "conn",
            "queue_name": "q",
            "filter_data": {},
            "poll_interval": 5.0,
            "mode": "push",
            "message_wait_timeout": None,
            "ack_unmatched": False,
        }
        trigger = RMQTrigger(**stored)

        events = await _run_against(trigger, _broker(Msg(body=b"restored")))

        assert events == [{
            "status": "success",
            "message": {"body": "restored", "headers": {}, "routing_key": "rk", "exchange": ""},
        }]


# ---------------------------------------------------------------------------
# Pull mode
# ---------------------------------------------------------------------------
class TestRunNoFilter:
    @pytest.mark.asyncio
    async def test_any_message_matches(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(
            Msg(body=b"hello", headers={"x-source": "test"}, routing_key="rk", exchange="my_ex")
        )

        events = await _run_against(trigger, broker)

        assert events == [{
            "status": "success",
            "message": {
                "body": "hello",
                "headers": {"x-source": "test"},
                "routing_key": "rk",
                "exchange": "my_ex",
            },
        }]
        assert len(broker.acked) == 1

    @pytest.mark.asyncio
    async def test_attempt_steps_and_close(self):
        """Connect, channel without confirms, passive declare, get — then the connection closes."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="orders")
        broker = _broker(Msg())

        await _run_against(trigger, broker)

        assert broker.calls == ["connect", "channel", "declare", "get", "ack"]
        (connection,) = broker.connections
        assert connection.channels[0].declared == [("orders", True)]
        assert connection.close_calls == 1


class TestRunWithFilter:
    @pytest.mark.asyncio
    async def test_matching_message_acked(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
        )
        broker = _broker(Msg(body=b"order data", headers={"x-type": "order"}))

        events = await _run_against(trigger, broker)

        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "order data"
        assert len(broker.acked) == 1

    @pytest.mark.asyncio
    async def test_non_matching_then_matching(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=0.01,
        )
        non_match = Msg(body=b"invoice", headers={"x-type": "invoice"})
        matching = Msg(body=b"order", headers={"x-type": "order"})
        broker = _broker(non_match, matching)

        events = await _run_against(trigger, broker)

        assert events[0]["message"]["body"] == "order"
        assert broker.nacked == [(non_match, True)]
        assert broker.acked == [matching]

    @pytest.mark.asyncio
    async def test_nack_uses_short_delay_not_poll_interval(self):
        """After NACK, sleep should use a short delay (0.1s), not poll_interval."""
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=30.0,
        )
        broker = _broker(
            Msg(body=b"invoice", headers={"x-type": "invoice"}),
            Msg(body=b"order", headers={"x-type": "order"}),
        )

        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            events = await _run_against(trigger, broker)

        assert events[0]["status"] == "success"
        mock_sleep.assert_awaited_once_with(0.1)


class TestRunEmptyQueue:
    @pytest.mark.asyncio
    async def test_polls_then_gets_message(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", poll_interval=0.01)
        broker = _broker(tail=[None, None, Msg(body=b"delayed")])

        events = await _run_against(trigger, broker)

        assert events[0]["message"]["body"] == "delayed"
        assert broker.calls.count("get") == 3


class TestRunError:
    @pytest.mark.asyncio
    async def test_connection_error_yields_error_event(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("refused"))})

        events = await _run_against(trigger, broker)

        assert len(events) == 1
        assert events[0]["status"] == "error"
        assert "refused" in events[0]["error"]
        assert broker.connections[0].close_calls == 1

    @pytest.mark.asyncio
    async def test_push_connection_error_yields_error_event(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("refused"))})

        events = await _run_against(trigger, broker)

        assert events[0]["status"] == "error"
        assert "refused" in events[0]["error"]


# ---------------------------------------------------------------------------
# Connection URL and TLS context
# ---------------------------------------------------------------------------
class TestConnectionArguments:
    @pytest.mark.asyncio
    async def test_ssl_uses_amqps_scheme_and_default_port(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("test"))})
        conn_info = FakeAirflowConnection(
            host="rmq.example.com", port=None, extra='{"ssl_enabled": true}'
        )

        await _run_against(trigger, broker, conn_info)

        url = broker.connections[0].url
        assert url.startswith("amqps://")
        assert "rmq.example.com:5671" in url

    @pytest.mark.asyncio
    async def test_ssl_passes_ssl_context(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("test"))})
        conn_info = FakeAirflowConnection(
            host="rmq.example.com", port=None, extra='{"ssl_enabled": true}'
        )

        await _run_against(trigger, broker, conn_info)

        assert isinstance(broker.connections[0].context, ssl.SSLContext)

    @pytest.mark.asyncio
    async def test_no_ssl_context_when_disabled(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("test"))})

        await _run_against(trigger, broker)

        assert broker.connections[0].context is None
        assert broker.connections[0].url.startswith("amqp://")

    @pytest.mark.asyncio
    async def test_url_carries_default_heartbeat(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("test"))})

        await _run_against(trigger, broker)

        assert broker.connections[0].url.endswith(f"?heartbeat={DEFAULT_HEARTBEAT}")

    @pytest.mark.asyncio
    async def test_url_carries_heartbeat_from_extra(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("test"))})

        await _run_against(trigger, broker, FakeAirflowConnection(extra='{"heartbeat": 12}'))

        assert broker.connections[0].url.endswith("?heartbeat=12")

    @pytest.mark.asyncio
    async def test_special_chars_in_credentials_are_encoded(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("test"))})
        conn_info = FakeAirflowConnection(login="user@domain", password="p@ss:word/123")

        await _run_against(trigger, broker, conn_info)

        url = broker.connections[0].url
        assert "user%40domain" in url
        assert "p%40ss%3Aword%2F123" in url
        assert url.startswith("amqp://")
        assert "@localhost:" in url


# ---------------------------------------------------------------------------
# Push mode
# ---------------------------------------------------------------------------
class TestPushModeNoFilter:
    @pytest.mark.asyncio
    async def test_first_message_taken(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = _broker(Msg(body=b"hello", headers={"x-source": "test"}, routing_key="rk", exchange="ex"))

        events = await _run_against(trigger, broker)

        assert events == [{
            "status": "success",
            "message": {"body": "hello", "headers": {"x-source": "test"}, "routing_key": "rk", "exchange": "ex"},
        }]
        assert len(broker.acked) == 1
        assert broker.calls[:4] == ["connect", "channel", "declare", "consume"]
        assert broker.connections[0].close_calls == 1

    @pytest.mark.asyncio
    async def test_binary_body_is_decoded_with_replacement(self):
        """The ACK is already behind the decode, so a binary payload must not raise:
        the event would be gone with nothing left to redeliver."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")

        events = await _run_against(trigger, _broker(Msg(body=b"\xff\xfe not utf-8")))

        assert events[0]["status"] == "success"
        assert "�" in events[0]["message"]["body"]

    @pytest.mark.asyncio
    async def test_message_arriving_later_is_delivered(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()

        async def _later(connection, channel):
            async def _send():
                await asyncio.sleep(0.02)
                await connection.channels[0].deliver(Msg(body=b"late"))
            asyncio.get_running_loop().create_task(_send())

        broker.hooks["consume"] = _later

        events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "late"

    @pytest.mark.asyncio
    async def test_consumer_tag_is_registered_per_attempt(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = _broker(Msg())

        await _run_against(trigger, broker)

        (tag,) = broker.connections[0].channels[0].consumers
        assert tag.startswith("rmq-trigger.")


class TestPushModeWithFilter:
    @pytest.mark.asyncio
    async def test_non_matching_nacked_matching_acked(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            mode="push",
            filter_data={"filter_headers": {"x-type": "order"}},
        )
        non_match = Msg(body=b"noise", headers={"x-type": "analytics"})
        matching = Msg(body=b"order", headers={"x-type": "order"}, routing_key="orders")
        broker = _broker(non_match, matching)

        events = await _run_against(trigger, broker)

        assert events[0]["message"]["body"] == "order"
        assert broker.nacked == [(non_match, True)]
        assert broker.acked == [matching]

    @pytest.mark.asyncio
    async def test_buffered_message_is_handled_without_waiting_for_a_turn(self):
        """A delivery already in the local queue is taken at once, not after ``rpc_timeout``."""
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            mode="push",
            filter_data={"filter_headers": {"x-type": "order"}},
            ack_unmatched=True,
        )
        broker = _broker(
            Msg(headers={"x-type": "invoice"}),
            Msg(headers={"x-type": "invoice"}),
            Msg(body=b"order", headers={"x-type": "order"}),
        )

        started = time.monotonic()
        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 5)

        assert events[0]["message"]["body"] == "order"
        assert time.monotonic() - started < 1


class TestPushModeTimeout:
    @pytest.mark.asyncio
    async def test_message_wait_timeout_yields_timeout_event(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=0.05,
        )
        broker = FakeBroker()

        started = time.monotonic()
        events = await _run_against(trigger, broker, _timeouts(rpc=30))

        assert events == [{"status": "timeout"}]
        assert time.monotonic() - started < 1
        assert broker.connections[0].close_calls == 1


class TestPushModeSerialize:
    def test_serialize_with_mode_and_message_wait_timeout(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=30.0,
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
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=15.0,
        )
        _, kwargs = original.serialize()
        restored = RMQTrigger(**kwargs)
        assert restored.mode == "push"
        assert restored.message_wait_timeout == 15.0


class TestPushModeChannelEvents:
    @pytest.mark.asyncio
    async def test_closed_channel_ends_the_wait(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()

        async def _close_later(connection, channel):
            async def _close():
                await asyncio.sleep(0.02)
                connection.lose(Exception())
            asyncio.get_running_loop().create_task(_close())

        broker.hooks["consume"] = _close_later

        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events[0]["status"] == "error"
        assert "connection lost during wait" in events[0]["error"]

    @pytest.mark.asyncio
    async def test_consumer_cancelled_by_the_broker_ends_the_wait(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()

        async def _cancel_consumer(connection, channel):
            async def _cancel():
                await asyncio.sleep(0.01)
                channel.consumers.clear()
            asyncio.get_running_loop().create_task(_cancel())

        broker.hooks["consume"] = _cancel_consumer

        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert events[0]["status"] == "error"
        assert "cancelled by the broker" in events[0]["error"]


# ---------------------------------------------------------------------------
# Step bounds
# ---------------------------------------------------------------------------
class TestStepBounds:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["pull", "push"])
    @pytest.mark.parametrize("step", ["channel", "declare"])
    async def test_hanging_setup_step_times_out(self, step, mode):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode)
        broker = FakeBroker(hooks={step: hang()})

        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert events[0]["status"] == "error"
        assert f"{step} did not complete within 0.05s" in events[0]["error"]
        assert broker.connections[0].close_calls == 1
        await _settle()
        assert _stray_tasks() == []

    @pytest.mark.asyncio
    async def test_hanging_get_times_out(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"get": hang()})

        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert "get did not complete within 0.05s" in events[0]["error"]

    @pytest.mark.asyncio
    async def test_hanging_consume_times_out(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker(hooks={"consume": hang()})

        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert "consume did not complete within 0.05s" in events[0]["error"]

    @pytest.mark.asyncio
    async def test_hanging_connect_times_out(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"connect": hang()})

        events = await asyncio.wait_for(
            _run_against(trigger, broker, _timeouts(rpc=30, connect=0.05)), 2
        )

        assert "connect did not complete within 0.05s" in events[0]["error"]
        assert broker.connections[0].close_calls == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["pull", "push"])
    async def test_cancelling_run_closes_the_connection(self, mode):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode, poll_interval=30)
        broker = _broker(tail=[None]) if mode == "pull" else FakeBroker()

        with _patched(broker, _timeouts(rpc=30)):
            task = asyncio.ensure_future(_collect_events(trigger))
            waited_on = "get" if mode == "pull" else "consume"
            while waited_on not in broker.calls:
                await asyncio.sleep(0.005)
            await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert broker.connections[0].close_calls == 1
        await _settle()
        assert _stray_tasks() == []


class TestAckOfMatchingMessage:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["pull", "push"])
    async def test_ack_timeout_still_fires_success(self, mode, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="orders", mode=mode)
        broker = _broker(Msg(body=b"payload"))
        broker.hooks["ack"] = hang()

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "payload"
        (line,) = log_lines(caplog, ACK_TEXT)
        assert line.name == "airflow.task"
        assert "may be delivered again" in line.getMessage()
        assert "'orders'" in line.getMessage()

    @pytest.mark.asyncio
    async def test_connection_lost_during_ack_still_fires_success(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"payload"))

        async def _lose(connection, channel):
            connection.lose(asyncio.CancelledError())
            await _forever()

        broker.hooks["ack"] = _lose

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events[0]["status"] == "success"
        assert len(log_lines(caplog, ACK_TEXT)) == 1

    @pytest.mark.asyncio
    async def test_ack_on_closed_channel_still_fires_success(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"payload"))
        broker.hooks["ack"] = fail(ChannelInvalidStateError("closed"))

        with caplog.at_level("WARNING"):
            events = await _run_against(trigger, broker, _timeouts(rpc=30))

        assert events[0]["status"] == "success"
        assert len(log_lines(caplog, ACK_TEXT)) == 1

    @pytest.mark.asyncio
    async def test_nack_timeout_of_a_miss_is_not_success(self, caplog):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
        )
        broker = _broker(Msg(headers={"x-type": "invoice"}))
        broker.hooks["nack"] = hang()

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert events[0]["status"] == "error"
        assert "ack did not complete within 0.05s" in events[0]["error"]
        assert log_lines(caplog, ACK_TEXT) == []

    @pytest.mark.asyncio
    async def test_unexpected_error_on_a_match_is_not_swallowed(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg())
        broker.hooks["ack"] = fail(ValueError("bug"))

        events = await _run_against(trigger, broker)

        assert events[0]["status"] == "error"
        assert "bug" in events[0]["error"]


# ---------------------------------------------------------------------------
# _step
# ---------------------------------------------------------------------------
class TestStep:
    @pytest.mark.asyncio
    async def test_returns_the_result(self):
        async def _call():
            return 42

        assert await _step(_call(), 1, "x", _future()) == 42

    @pytest.mark.asyncio
    async def test_raises_the_call_error_while_the_connection_lives(self):
        async def _call():
            raise KeyError("boom")

        with pytest.raises(KeyError):
            await _step(_call(), 1, "x", _future())

    @pytest.mark.asyncio
    async def test_timeout_is_its_own_exception(self):
        with pytest.raises(_StepTimeout) as info:
            await _step(_forever(), 0.01, "declare", _future())
        assert not isinstance(info.value, OSError)
        assert info.value.step == "declare"
        assert info.value.timeout == 0.01
        await _settle()
        assert _stray_tasks() == []

    @pytest.mark.asyncio
    async def test_cancelled_call_with_live_run_is_a_lost_connection(self):
        """aiormq ends a call whose connection went away by cancelling it."""
        async def _call():
            raise asyncio.CancelledError()

        with pytest.raises(_ConnectionLost) as info:
            await _step(_call(), 1, "get", _future())
        assert info.value.reason is None

    @pytest.mark.asyncio
    async def test_closing_first_is_a_lost_connection_with_its_reason(self):
        closing = _future()
        reason = asyncio.CancelledError()

        async def _call():
            closing.set_exception(reason)
            await _forever()

        with pytest.raises(_ConnectionLost) as info:
            await _step(_call(), 5, "get", closing)
        assert info.value.reason is reason
        await _settle()
        assert _stray_tasks() == []

    @pytest.mark.asyncio
    async def test_connect_step_does_not_race_closing(self):
        """Without ``closing`` the call's own exception comes out as it is."""
        async def _call():
            raise ConnectionRefusedError("refused")

        with pytest.raises(ConnectionRefusedError):
            await _step(_call(), 1, "connect")

    @pytest.mark.asyncio
    async def test_cancelling_run_after_the_connection_closed_during_poll_interval(self):
        """``closing`` stays finished, but the cancellation is the caller's and passes on."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", poll_interval=30)
        broker = _broker(tail=[None])
        events: list = []

        async def _run():
            async for event in trigger.run():
                events.append(event)

        with _patched(broker, _timeouts(rpc=30)):
            task = asyncio.ensure_future(_run())
            while "get" not in broker.calls:
                await asyncio.sleep(0.005)
            await asyncio.sleep(0.01)
            broker.connections[0].lose(asyncio.CancelledError())
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert events == []
        assert len(broker.connections) == 1
        assert broker.calls.count("get") == 1

    @pytest.mark.asyncio
    async def test_cancelling_run_when_the_connection_closes_during_a_step(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker()
        events: list = []
        holder: dict = {}

        async def _close_and_cancel(connection, channel):
            connection.lose(asyncio.CancelledError())
            holder["task"].cancel()
            await _forever()

        broker.hooks["get"] = _close_and_cancel

        async def _run():
            async for event in trigger.run():
                events.append(event)

        with _patched(broker, _timeouts(rpc=30)):
            holder["task"] = task = asyncio.ensure_future(_run())
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)

        assert events == []
        assert len(broker.connections) == 1
        assert broker.connections[0].close_calls == 1
        await _settle()
        assert _stray_tasks() == []


# ---------------------------------------------------------------------------
# Real aiormq.Connection against a local server
# ---------------------------------------------------------------------------
def _frame(channel: int, frame: Any) -> bytes:
    return pamqp.frame.marshal(frame, channel)


async def _read_frame(reader: asyncio.StreamReader) -> Any:
    head = await reader.readexactly(7)
    size = int.from_bytes(head[3:7], "big")
    rest = await reader.readexactly(size + 1)
    _, _, frame = pamqp.frame.unmarshal(head + rest)
    return frame


async def _handshake(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await reader.readexactly(8)
    writer.write(_frame(0, spec.Connection.Start(
        server_properties={"capabilities": {"basic.nack": True, "publisher_confirms": True}},
    )))
    await _read_frame(reader)
    writer.write(_frame(0, spec.Connection.Tune(channel_max=2047, frame_max=131072, heartbeat=0)))
    await _read_frame(reader)
    await _read_frame(reader)
    writer.write(_frame(0, spec.Connection.OpenOk()))
    await writer.drain()


@contextlib.asynccontextmanager
async def _local_server(handler):
    """TCP server on 127.0.0.1 running ``handler(reader, writer)`` per client."""
    writers: list[asyncio.StreamWriter] = []
    handlers: list[asyncio.Task] = []

    async def _on_client(reader, writer):
        writers.append(writer)
        handlers.append(asyncio.current_task())
        with contextlib.suppress(Exception):
            await handler(reader, writer)

    server = await asyncio.start_server(_on_client, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        server.close()
        for writer in writers:
            writer.close()
        for task in handlers:
            task.cancel()
        await asyncio.gather(*handlers, return_exceptions=True)
        await server.wait_closed()


class TestRealAiormqConnection:
    @pytest.mark.asyncio
    async def test_silent_handshake_times_out_and_the_connection_is_closed(self):
        async def _silent(reader, writer):
            await _forever()

        created: list = []
        real = aiormq.Connection

        def _spy(*args, **kwargs):
            connection = real(*args, **kwargs)
            created.append(connection)
            return connection

        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        async with _local_server(_silent) as port:
            conn_info = FakeAirflowConnection(
                host="127.0.0.1", port=port, extra='{"connect_timeout": 0.2}'
            )
            with patch.object(aiormq, "Connection", _spy), patch(
                "airflow_provider_rmq.triggers.rmq.BaseHook"
            ) as mock_base:
                mock_base.get_connection = MagicMock(return_value=conn_info)
                started = time.monotonic()
                events = await asyncio.wait_for(_collect_events(trigger), 5)

        assert time.monotonic() - started < 2
        assert events[0]["status"] == "error"
        assert "connect did not complete within 0.2s" in events[0]["error"]
        (connection,) = created
        assert connection.is_closed

    @pytest.mark.asyncio
    async def test_socket_closed_mid_step_is_a_lost_connection(self):
        """The step ends on ``connection.closing``, long before its own bound."""
        async def _drop_on_channel_open(reader, writer):
            await _handshake(reader, writer)
            await _read_frame(reader)
            writer.close()
            await _forever()

        async with _local_server(_drop_on_channel_open) as port:
            connection = aiormq.Connection(f"amqp://guest:guest@127.0.0.1:{port}/")
            try:
                await asyncio.wait_for(connection.connect(), 5)
                started = time.monotonic()
                with pytest.raises(_ConnectionLost):
                    await _step(
                        connection.channel(publisher_confirms=False), 10, "channel", connection.closing
                    )
                assert time.monotonic() - started < 5
            finally:
                await connection.close()


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
class TestAckUnmatchedPullMode:
    @pytest.mark.asyncio
    async def test_non_matching_message_is_acked(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            ack_unmatched=True,
        )
        stray = Msg(body=b"stray", headers={"x-type": "invoice"})
        broker = _broker(stray, Msg(body=b"order", headers={"x-type": "order"}))

        events = await _run_against(trigger, broker)

        assert events[0]["status"] == "success"
        assert stray in broker.acked
        assert broker.nacked == []

    @pytest.mark.asyncio
    async def test_ack_unmatched_off_returns_the_message_to_the_queue(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=0.01,
        )
        stray = Msg(body=b"invoice", headers={"x-type": "invoice"})
        broker = _broker(stray, Msg(body=b"order", headers={"x-type": "order"}))

        events = await _run_against(trigger, broker)

        assert events[0]["status"] == "success"
        assert broker.nacked == [(stray, True)]
        assert stray not in broker.acked

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
        strays = [Msg(body=b"stray", headers={"x-type": "invoice"}) for _ in range(n)]
        # None stands for the empty poll that follows the drain: from there the trigger
        # waits for new deliveries instead of turning over the same messages again.
        broker = _broker(*strays, tail=[None, Msg(body=b"order", headers={"x-type": "order"})])

        events = await _run_against(trigger, broker)

        assert events[0]["status"] == "success"
        assert len(broker.pending) == 0
        assert broker.calls.count("get") == n + 2
        assert broker.acked[:n] == strays
        assert broker.nacked == []


class TestAckUnmatchedPushMode:
    @pytest.mark.asyncio
    async def test_non_matching_message_is_acked(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            mode="push",
            ack_unmatched=True,
        )
        stray = Msg(body=b"invoice", headers={"x-type": "invoice"})
        matching = Msg(body=b"order", headers={"x-type": "order"})
        broker = _broker(stray, matching)

        events = await _run_against(trigger, broker)

        assert events[0]["status"] == "success"
        assert broker.acked == [stray, matching]
        assert broker.nacked == []

    @pytest.mark.asyncio
    async def test_ack_unmatched_off_returns_the_message_to_the_queue(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            mode="push",
        )
        stray = Msg(body=b"invoice", headers={"x-type": "invoice"})
        broker = _broker(stray, Msg(body=b"order", headers={"x-type": "order"}))

        events = await _run_against(trigger, broker)

        assert events[0]["status"] == "success"
        assert broker.nacked == [(stray, True)]
        assert stray not in broker.acked


# ---------------------------------------------------------------------------
# Filter miss state
# ---------------------------------------------------------------------------
class TestFilterMissState:
    @pytest.mark.asyncio
    async def test_state_names_the_queue_and_logs_to_the_task_logger(self):
        """The threshold WARNING must reach the task log, which hangs off ``airflow.task``."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="my_queue", ack_unmatched=True)
        captured = []

        async def _fake_match_and_ack(message, msg_filter, miss_state, verdict=None):
            captured.append(miss_state)
            return True

        with patch("airflow_provider_rmq.triggers.rmq.match_and_ack", new=_fake_match_and_ack):
            await _run_against(trigger, _broker(Msg()))

        assert captured[0].queue_name == "my_queue"
        assert captured[0].log.name == "airflow.task"
        assert captured[0].ack_unmatched is True

    @pytest.mark.asyncio
    async def test_verdict_is_reached_once_and_handed_over(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", filter_data={"filter_headers": {"x-type": "order"}},
        )
        verdicts = []

        async def _recording(message, msg_filter, miss_state, verdict=None):
            verdicts.append(verdict)
            return await match_and_ack(message, msg_filter, miss_state, verdict=verdict)

        with patch("airflow_provider_rmq.triggers.rmq.match_and_ack", new=_recording), patch(
            "airflow_provider_rmq.utils.filters.MessageFilter.evaluate",
            autospec=True,
            return_value=(FilterVerdict.MATCH, None),
        ) as evaluate:
            events = await _run_against(trigger, _broker(Msg(headers={"x-type": "order"})))

        assert events[0]["status"] == "success"
        assert verdicts[0] is not None
        assert evaluate.call_count == 1

    @pytest.mark.asyncio
    async def test_one_state_per_run_counting_across_deliveries(self):
        """Counters span the whole run(): a per-delivery state would never reach a threshold."""
        seen = []
        streaks = []

        async def _recording(message, msg_filter, miss_state, verdict=None):
            seen.append(miss_state)
            result = await match_and_ack(message, msg_filter, miss_state, verdict=verdict)
            streaks.append(miss_state.streak)
            return result

        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=0.01,
            ack_unmatched=True,
        )
        broker = _broker(
            *[Msg(body=b"stray", headers={"x-type": "invoice"}) for _ in range(3)],
            tail=[Msg(body=b"order", headers={"x-type": "order"})],
        )

        with patch("airflow_provider_rmq.triggers.rmq.match_and_ack", new=_recording):
            events = await _run_against(trigger, broker)

        assert events[0]["status"] == "success"
        assert all(state is seen[0] for state in seen)
        # The run of misses grows across deliveries and the match ends it; dropped is
        # cleared by nothing and keeps the three messages that are gone for good.
        assert streaks == [1, 2, 3, 0]
        assert seen[0].dropped == 3

    @pytest.mark.asyncio
    async def test_the_threshold_line_goes_to_the_task_logger(self, caplog):
        """The run of misses spans the whole run(), so the powers of ten are reached.

        The assertion is on the name of the emitted record and not only on its text:
        ``caplog`` attaches to the root and catches records from any logger, so a test
        reading the message alone stays green whatever logger the line came from.
        """
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="stray_orders",
            filter_data={"filter_headers": {"x-type": "order"}},
            mode="push",
            ack_unmatched=True,
        )
        messages = [Msg(body=b"invoice", headers={"x-type": "invoice"}) for _ in range(1000)]
        messages.append(Msg(body=b"order", headers={"x-type": "order"}))

        with caplog.at_level("WARNING"):
            events = await _run_against(trigger, _broker(*messages))

        assert events[0]["status"] == "success"
        streak_lines = log_lines(caplog, STREAK_TEXT)
        assert [r.getMessage().split()[0] for r in streak_lines] == ["100", "1000"]
        assert [r.name for r in streak_lines] == ["airflow.task", "airflow.task"]
        # A name of its own: "q" is a substring of "queue" and would be found in any
        # wording of the line.
        assert all("stray_orders" in r.getMessage() for r in streak_lines)

    @pytest.mark.asyncio
    async def test_push_mode_shares_the_same_state(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            mode="push",
            ack_unmatched=True,
        )
        seen = []

        async def _recording(message, msg_filter, miss_state, verdict=None):
            seen.append(miss_state)
            return await match_and_ack(message, msg_filter, miss_state, verdict=verdict)

        broker = _broker(
            Msg(body=b"invoice", headers={"x-type": "invoice"}),
            Msg(body=b"invoice", headers={"x-type": "invoice"}),
            Msg(body=b"order", headers={"x-type": "order"}),
        )

        with patch("airflow_provider_rmq.triggers.rmq.match_and_ack", new=_recording):
            events = await _run_against(trigger, broker)

        assert events[0]["status"] == "success"
        assert all(state is seen[0] for state in seen)
        assert seen[0].dropped == 2
