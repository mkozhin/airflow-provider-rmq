from __future__ import annotations

import asyncio
import contextlib
import itertools
import logging
import re
import ssl
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlsplit

import aiormq
import pamqp.frame
import pytest
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import (
    AMQPConnectionError,
    AuthenticationError,
    ChannelAccessRefused,
    ChannelInvalidStateError,
    ChannelLockedResource,
    ChannelNotFoundEntity,
    ChannelPreconditionFailed,
    ConnectionClosed,
    ConnectionCommandInvalid,
    ConnectionFrameError,
    ConnectionNotAllowed,
    ConnectionNotImplemented,
    IncompatibleProtocolError,
    ProbableAuthenticationError,
)
from pamqp import commands as spec
from pamqp.exceptions import AMQPInternalError
from pamqp.header import ContentHeader

from airflow_provider_rmq.triggers.rmq import (
    RMQTrigger,
    _ConnectionLost,
    _step,
    _StepTimeout,
)
from airflow_provider_rmq.utils import amqp as amqp_utils
from airflow_provider_rmq.utils.amqp import DEFAULT_HEARTBEAT, match_and_ack
from airflow_provider_rmq.utils.filters import FilterVerdict
from tests.conftest import STREAK_TEXT, FakeAirflowConnection, log_lines

ACK_TEXT = "ACK not confirmed"
#: Reply texts of RabbitMQ: the 530 refusing a user access to a vhost, and the 404 on a
#: durable classic queue whose node is restarting.
VHOST_REFUSED = "NOT_ALLOWED - access to vhost 'test' refused for user 'airflow'"
HOME_NODE_DOWN = (
    "NOT_FOUND - home node 'rabbit@node-1' of durable queue 'q' in vhost 'test' is down or inaccessible"
)


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


def on(hook, *attempts: int):
    """Hook: ``hook`` on the given attempts (counted from 1), the call proceeds on the rest."""
    async def _hook(connection, channel):
        if connection.attempt in attempts:
            await hook(connection=connection, channel=channel)
    return _hook


def lose(reason: BaseException):
    """Hook: the connection goes away with ``reason`` and the call never answers."""
    async def _hook(connection, channel):
        connection.lose(reason)
        await _forever()
    return _hook


def close_channel(reason: BaseException):
    """Hook: the broker closes the channel with ``reason``; the connection lives on."""
    async def _hook(connection, channel):
        channel.closing.set_exception(reason)
        raise reason
    return _hook


@dataclass(eq=False)
class FakeBroker:
    """One queue behind ``aiormq.Connection``: connections and channels made on demand.

    ``pending`` is the queue. ``basic_get`` takes from it, then from ``tail`` (``None``
    there is an empty poll), and waits forever once both are drained. ``basic_consume``
    hands every pending message to the consumer before it returns, the way deliveries
    already in flight reach the callback before the caller looks; every callback runs
    as a task of its own, as ``aiormq`` runs it. A requeued NACK puts the message back
    at the end of ``pending``; ``multiple=True`` NACKs every unacknowledged tag up to
    the one given.

    ``hooks`` maps a step name — ``connect``, ``channel``, ``declare``, ``get``,
    ``consume``, ``ack``, ``nack``, ``cancel``, ``close`` — to a coroutine function
    called with the connection and the channel before the step does its work. Every
    step lands in ``calls``. A call on a closed channel raises
    :exc:`ChannelInvalidStateError`, as it does in ``aiormq``. The ACK is recorded
    before its ``ack`` hook runs, the way ``aiormq`` queues the frame before waiting
    for the drain; ``ack:write`` is a hook that runs before the frame is queued, and
    does not land in ``calls``.
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
        connection.attempt = len(self.connections)
        return connection

    async def before_write(self, step, connection, channel):
        hook = self.hooks.get(f"{step}:write")
        if hook is not None:
            await hook(connection=connection, channel=channel)

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
        self.attempt = 0

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
        try:
            await self.broker.call("close", self)
        finally:
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
        self.callbacks: list[asyncio.Task] = []

    @property
    def is_closed(self) -> bool:
        return self.closing.done()

    def _check_open(self) -> None:
        if self.closing.done():
            raise ChannelInvalidStateError("channel is closed")

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
        self._check_open()
        await self.broker.call("declare", self.connection, self)
        self.declared.append((queue, passive))
        return spec.Queue.DeclareOk(queue=queue)

    async def basic_get(self, queue, no_ack=False):
        self._check_open()
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
        self._check_open()
        await self.broker.call("consume", self.connection, self)
        self.consumers[consumer_tag] = callback
        while self.broker.pending:
            self._dispatch(callback, self._delivered(self.broker.pending.popleft(), consumer_tag))
        return spec.Basic.ConsumeOk(consumer_tag=consumer_tag)

    def _dispatch(self, callback, delivered: DeliveredMessage) -> None:
        """Run the consumer callback as a task, the way ``aiormq`` does."""
        self.callbacks.append(asyncio.get_running_loop().create_task(callback(delivered)))

    async def deliver(self, msg: Msg) -> None:
        """A message arriving at the consumer after ``basic_consume`` returned."""
        (consumer_tag, callback), = self.consumers.items()
        self._dispatch(callback, self._delivered(msg, consumer_tag))
        await asyncio.sleep(0)

    async def basic_ack(self, delivery_tag, multiple=False):
        # aiormq puts the Basic.Ack frame on the write queue before it waits for the
        # drain: an ACK that hangs or is cancelled in the "ack" hook has been sent.
        self._check_open()
        await self.broker.before_write("ack", self.connection, self)
        self.broker.acked.append(self.in_flight.pop(delivery_tag))
        await self.broker.call("ack", self.connection, self)

    async def basic_nack(self, delivery_tag, multiple=False, requeue=True):
        self._check_open()
        await self.broker.call("nack", self.connection, self)
        tags = sorted(t for t in self.in_flight if t <= delivery_tag) if multiple else [delivery_tag]
        for tag in tags:
            msg = self.in_flight.pop(tag)
            self.broker.nacked.append((msg, requeue))
            if requeue:
                self.broker.pending.append(msg)

    async def basic_cancel(self, consumer_tag):
        self._check_open()
        await self.broker.call("cancel", self.connection, self)
        self.consumers.pop(consumer_tag, None)
        return spec.Basic.CancelOk(consumer_tag=consumer_tag)


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


def _broker(*messages: Msg, tail=(), hooks=None) -> FakeBroker:
    return FakeBroker(pending=deque(messages), tail=deque(tail), hooks=dict(hooks or {}))


def _query(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query)


def _timeouts(rpc: float = 30, connect: float = 15) -> FakeAirflowConnection:
    return FakeAirflowConnection(extra=f'{{"rpc_timeout": {rpc}, "connect_timeout": {connect}}}')


async def _settle() -> None:
    """Let abandoned calls run their cancellation out."""
    for _ in range(5):
        await asyncio.sleep(0)


def _stray_tasks() -> list[asyncio.Task]:
    current = asyncio.current_task()
    return [t for t in asyncio.all_tasks() if t is not current and not t.done()]


@pytest.fixture(autouse=True)
def pauses():
    """Pauses between attempts, in seconds, in the order they were taken.

    A full pause of 15 to 60 seconds passes at once; a pause that
    ``message_wait_timeout`` cut to under a second is slept for real.
    """
    taken: list[float] = []

    async def _record(seconds: float) -> None:
        taken.append(seconds)
        await asyncio.sleep(seconds if seconds < 1 else 0)

    with patch("airflow_provider_rmq.triggers.rmq._pause", _record):
        yield taken


def _retry_lines(caplog) -> list[logging.LogRecord]:
    return log_lines(caplog, "; retrying queue")


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

        assert broker.calls == ["connect", "channel", "declare", "get", "ack", "close"]
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
        """After a NACK the next ``basic_get`` follows the 0.1 s pause, not ``poll_interval``."""
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
            poll_interval=30.0,
        )
        broker = _broker(
            Msg(body=b"invoice", headers={"x-type": "invoice"}),
            Msg(body=b"order", headers={"x-type": "order"}),
        )

        started = time.monotonic()
        events = await asyncio.wait_for(_run_against(trigger, broker), 5)

        assert events[0]["status"] == "success"
        assert 0.1 <= time.monotonic() - started < 5


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
    @pytest.mark.parametrize("mode", ["pull", "push"])
    async def test_url_the_connection_rejects_yields_error_event(self, mode, caplog):
        """``aiormq.Connection`` raises on a URL ``build_amqp_connection`` lets through."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode)

        def _reject(url, *, context=None):
            raise ValueError("Port out of range 0-65535")

        with patch.object(aiormq, "Connection", _reject), patch(
            "airflow_provider_rmq.triggers.rmq.BaseHook"
        ) as mock_base, caplog.at_level("ERROR"):
            mock_base.get_connection = MagicMock(return_value=FakeAirflowConnection())
            events = await asyncio.wait_for(_collect_events(trigger), 2)

        assert events == [{"status": "error", "error": "Port out of range 0-65535"}]
        assert log_lines(caplog, "RMQTrigger encountered an error")

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

        assert _query(broker.connections[0].url)["heartbeat"] == [str(DEFAULT_HEARTBEAT)]

    @pytest.mark.asyncio
    async def test_url_carries_heartbeat_from_extra(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("test"))})

        await _run_against(trigger, broker, FakeAirflowConnection(extra='{"heartbeat": 12}'))

        assert _query(broker.connections[0].url)["heartbeat"] == ["12"]

    @pytest.mark.asyncio
    async def test_connection_is_named_after_the_queue(self):
        """``aiormq`` hands ``name`` to the broker as ``connection_name``."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="orders.v2")
        broker = FakeBroker(hooks={"connect": fail(ConnectionError("test"))})

        await _run_against(trigger, broker)

        url = broker.connections[0].url
        assert _query(url)["name"] == ["rmq-trigger:orders.v2"]
        assert aiormq.Connection(url).connection_name == "rmq-trigger:orders.v2"

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
        broker = FakeBroker()
        tags: list = []

        async def _consume(connection, channel):
            async def _later():
                await asyncio.sleep(0.01)
                tags.extend(channel.consumers)
                if connection.attempt == 1:
                    channel.consumers.clear()
                else:
                    await channel.deliver(Msg())
            asyncio.get_running_loop().create_task(_later())

        broker.hooks["consume"] = _consume

        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert events[0]["status"] == "success"
        assert len(tags) == 2
        assert tags[0] != tags[1]
        assert all(tag.startswith("rmq-trigger.") for tag in tags)


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
    @pytest.mark.parametrize(
        "reason",
        [
            asyncio.CancelledError(),
            AMQPConnectionError("Server connection unexpectedly closed"),
            Exception(),
        ],
        ids=["heartbeat", "peer-hung-up", "at-eof"],
    )
    async def test_closed_channel_is_retried(self, reason, caplog):
        """A bare CancelledError (heartbeat), an AMQPConnectionError (the peer hung up) or a
        bare Exception (``aiormq`` at EOF) on ``channel.closing`` is a lost connection:
        WARNING, a new attempt, and ``run()`` carries on."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()

        async def _consume(connection, channel):
            async def _later():
                await asyncio.sleep(0.02)
                if connection.attempt == 1:
                    connection.lose(reason)
                else:
                    await channel.deliver(Msg(body=b"after"))
            asyncio.get_running_loop().create_task(_later())

        broker.hooks["consume"] = _consume

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events[0]["message"]["body"] == "after"
        (line,) = _retry_lines(caplog)
        assert "connection lost during wait" in line.getMessage()
        assert "(attempt 1)" in line.getMessage()
        assert broker.connections[0].close_calls == 1

    @pytest.mark.asyncio
    async def test_channel_closed_with_another_code_is_retried(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()

        async def _consume(connection, channel):
            async def _later():
                await asyncio.sleep(0.02)
                if connection.attempt == 1:
                    channel.closing.set_exception(ChannelPreconditionFailed("PRECONDITION_FAILED"))
                else:
                    await channel.deliver(Msg(body=b"after"))
            asyncio.get_running_loop().create_task(_later())

        broker.hooks["consume"] = _consume

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        assert "PRECONDITION_FAILED" in _retry_lines(caplog)[0].getMessage()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("attempt", [1, 2])
    async def test_406_answering_basic_consume_is_an_error(self, attempt):
        """A stream queue takes no consumer without a prefetch limit: the same 406 every time."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()
        text = "PRECONDITION_FAILED - consumer prefetch count is not set for stream queue 'q'"
        if attempt == 2:
            broker.hooks["declare"] = on(lose(Exception()), 1)
        broker.hooks["consume"] = on(close_channel(ChannelPreconditionFailed(text)), attempt)

        events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["status"] == "error"
        assert "prefetch count is not set" in events[0]["error"]
        assert len(broker.connections) == attempt
        assert all(connection.close_calls == 1 for connection in broker.connections)

    @pytest.mark.asyncio
    async def test_channel_closed_with_404_is_an_error(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()

        async def _consume(connection, channel):
            async def _later():
                await asyncio.sleep(0.02)
                channel.closing.set_exception(ChannelNotFoundEntity("NOT_FOUND - no queue 'q'"))
            asyncio.get_running_loop().create_task(_later())

        broker.hooks["consume"] = _consume

        events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["status"] == "error"
        assert "NOT_FOUND - no queue 'q'" in events[0]["error"]
        assert len(broker.connections) == 1

    @pytest.mark.asyncio
    async def test_consumer_cancelled_by_the_broker_is_retried_then_404(self, caplog):
        """The queue was deleted: the consumer goes, and the next attempt gets the 404."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker(hooks={"declare": on(fail(ChannelNotFoundEntity("NOT_FOUND - no queue 'q'")), 2)})

        async def _cancel_consumer(connection, channel):
            async def _cancel():
                await asyncio.sleep(0.01)
                channel.consumers.clear()
            asyncio.get_running_loop().create_task(_cancel())

        broker.hooks["consume"] = _cancel_consumer

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        (line,) = _retry_lines(caplog)
        assert "consumer on queue 'q' was cancelled by the broker" in line.getMessage()
        assert events[0]["status"] == "error"
        assert "no queue 'q'" in events[0]["error"]
        assert len(broker.connections) == 2

    @pytest.mark.asyncio
    async def test_message_arriving_between_turns_is_handled(self):
        """Several empty turns pass before the delivery; it is handled like any other."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()

        async def _later(connection, channel):
            async def _send():
                await asyncio.sleep(0.12)
                await channel.deliver(Msg(body=b"late"))
            asyncio.get_running_loop().create_task(_send())

        broker.hooks["consume"] = _later

        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.03)), 2)

        assert events[0]["message"]["body"] == "late"
        assert len(broker.connections) == 1


# ---------------------------------------------------------------------------
# Step bounds
# ---------------------------------------------------------------------------
class TestStepBounds:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["pull", "push"])
    @pytest.mark.parametrize("step", ["channel", "declare"])
    async def test_hanging_setup_step_times_out_and_is_retried(self, step, mode, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode)
        broker = _broker(Msg(body=b"second"))
        broker.hooks[step] = on(hang(), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert events[0]["message"]["body"] == "second"
        (line,) = _retry_lines(caplog)
        assert f"{step} did not complete within 0.05s" in line.getMessage()
        assert broker.connections[0].close_calls == 1
        await _settle()
        assert _stray_tasks() == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(("mode", "step"), [("pull", "get"), ("push", "consume")])
    async def test_hanging_call_times_out_and_is_retried(self, mode, step, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode)
        broker = _broker(Msg(body=b"second"))
        broker.hooks[step] = on(hang(), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert events[0]["message"]["body"] == "second"
        assert f"{step} did not complete within 0.05s" in _retry_lines(caplog)[0].getMessage()

    @pytest.mark.asyncio
    async def test_hanging_connect_times_out_and_is_retried(self, caplog):
        """A connect out of time on the first attempt is retried, on any Python: it comes
        out as the trigger's own timeout, not as the OSError TimeoutError is from 3.11."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"second"))
        broker.hooks["connect"] = on(hang(), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(
                _run_against(trigger, broker, _timeouts(rpc=30, connect=0.05)), 2
            )

        assert events[0]["message"]["body"] == "second"
        (line,) = _retry_lines(caplog)
        assert line.name == "airflow.task"
        assert line.getMessage() == (
            "RMQTrigger: connect did not complete within 0.05s on conn_id='conn' (attempt 1); "
            "retrying queue 'q' in 15s"
        )
        assert broker.connections[0].close_calls == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["pull", "push"])
    async def test_cancelling_run_closes_the_connection(self, mode):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode, poll_interval=30)
        broker = _broker(tail=[None]) if mode == "pull" else FakeBroker()

        with _patched(broker, _timeouts(rpc=30)):
            task = asyncio.ensure_future(_collect_events(trigger))
            waited_on = "get" if mode == "pull" else "consume"
            while waited_on not in broker.calls and not task.done():
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
    @pytest.mark.parametrize("mode", ["pull", "push"])
    async def test_connection_lost_during_ack_still_fires_success(self, mode, caplog):
        """One WARNING: in push mode there is nothing left to give back on a lost connection."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode)
        broker = _broker(Msg(body=b"payload"), Msg(body=b"unread"))

        async def _lose(connection, channel):
            connection.lose(asyncio.CancelledError())
            await _forever()

        broker.hooks["ack"] = _lose

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events[0]["status"] == "success"
        (line,) = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert ACK_TEXT in line.getMessage()
        assert "cancel" not in broker.calls

    @pytest.mark.asyncio
    @pytest.mark.parametrize("mode", ["pull", "push"])
    @pytest.mark.parametrize(
        "exc",
        [
            ChannelPreconditionFailed("PRECONDITION_FAILED - unknown delivery tag 1"),
            ConnectionClosed(320, "CONNECTION_FORCED - broker forced connection closure"),
            ConnectionResetError(104, "Connection reset by peer"),
        ],
        ids=["channel-closed-406", "connection-closed-320", "connection-reset"],
    )
    async def test_broker_side_failure_of_the_ack_still_fires_success(self, exc, mode, caplog):
        """The frame may have been written before the failure: the outcome is unknown."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode)
        broker = _broker(Msg(body=b"payload"))
        broker.hooks["ack"] = fail(exc)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events[0]["status"] == "success"
        assert events[0]["message"]["body"] == "payload"
        assert len(log_lines(caplog, ACK_TEXT)) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mode", "step"), [("pull", "close"), ("push", "cancel"), ("push", "close")]
    )
    async def test_cancelling_run_while_the_attempt_is_left_keeps_the_event(self, mode, step):
        """The triggerer reads the event as soon as it is yielded and resumes ``run()``
        straight away; a cancellation that lands while the consumer is left or the
        connection closes after an acknowledged match finds the event already out."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode)
        broker = _broker(Msg(body=b"payload"), Msg(body=b"unread"))
        broker.hooks[step] = hang()
        events: list[dict] = []

        async def _triggerer() -> None:
            async for event in trigger.run():
                events.append(event.payload)

        with _patched(broker, _timeouts(rpc=30)):
            task = asyncio.ensure_future(_triggerer())
            while step not in broker.calls and not task.done():
                await asyncio.sleep(0.005)
            assert not task.done()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert [e["status"] for e in events] == ["success"]
        assert events[0]["message"]["body"] == "payload"
        assert [m.body for m in broker.acked] == [b"payload"]
        assert broker.connections[0].close_calls == 1
        assert broker.connections[0].is_closed
        await _settle()
        assert _stray_tasks() == []

    @pytest.mark.asyncio
    async def test_ack_on_closed_channel_is_retried(self, caplog):
        """Nothing was written to a closed channel: the broker puts the message back."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        first, again = Msg(body=b"payload"), Msg(body=b"payload again")
        broker = _broker(first, tail=[again])
        broker.hooks["ack:write"] = on(fail(ChannelInvalidStateError("closed")), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events[0]["message"]["body"] == "payload again"
        assert log_lines(caplog, ACK_TEXT) == []
        (line,) = _retry_lines(caplog)
        assert "ack failed: ChannelInvalidStateError" in line.getMessage()
        assert broker.acked == [again]

    @pytest.mark.asyncio
    async def test_push_connection_lost_between_buffered_deliveries_is_retried(self, caplog):
        """A buffered delivery of a closed connection is the broker's again: no success for it."""
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", ack_unmatched=True,
            filter_data={"filter_headers": {"x-type": "order"}},
        )
        miss = Msg(body=b"miss", headers={"x-type": "invoice"})
        order = Msg(body=b"order", headers={"x-type": "order"})
        broker = _broker(miss, order)
        lost = []

        async def _lose_behind_the_miss(connection, channel):
            # The ACK of the miss goes out, the connection closes right behind it, and
            # the broker puts the unacknowledged order back in the queue.
            if not lost:
                lost.append(connection.attempt)
                connection.lose(ConnectionClosed(320, "CONNECTION_FORCED"))
                broker.pending.append(order)

        broker.hooks["ack"] = _lose_behind_the_miss

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events == [
            {"status": "success", "message": {
                "body": "order", "headers": {"x-type": "order"}, "routing_key": "rk", "exchange": "",
            }},
        ]
        assert lost == [1]
        assert log_lines(caplog, ACK_TEXT) == []
        (line,) = _retry_lines(caplog)
        assert "(attempt 1)" in line.getMessage()
        assert "connection lost during wait: ConnectionClosed" in line.getMessage()
        assert broker.acked == [miss, order]

    @pytest.mark.asyncio
    async def test_pull_connection_lost_with_the_get_is_retried(self, caplog):
        """``basic_get`` and the close land in one turn: the message is not acknowledged."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        order = Msg(body=b"order")
        broker = _broker(order)

        async def _lose_with_the_get(connection, channel):
            if connection.attempt == 1:
                connection.lose(ConnectionClosed(320, "CONNECTION_FORCED"))
                broker.pending.append(order)

        broker.hooks["get"] = _lose_with_the_get

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert [e["status"] for e in events] == ["success"]
        assert events[0]["message"]["body"] == "order"
        assert log_lines(caplog, ACK_TEXT) == []
        (line,) = _retry_lines(caplog)
        assert "(attempt 1)" in line.getMessage()
        assert "connection lost during ack: ConnectionClosed" in line.getMessage()
        assert broker.acked == [order]

    @pytest.mark.asyncio
    async def test_ack_refused_by_a_channel_closed_under_it_is_retried(self, caplog):
        """The connection closes as the ACK starts, and the ACK raises before writing."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        first, again = Msg(body=b"payload"), Msg(body=b"payload again")
        broker = _broker(first, tail=[again])

        async def _lose_then_refuse(connection, channel):
            connection.lose(ConnectionClosed(320, "CONNECTION_FORCED"))
            raise ChannelInvalidStateError("closed")

        broker.hooks["ack:write"] = on(_lose_then_refuse, 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events[0]["message"]["body"] == "payload again"
        assert log_lines(caplog, ACK_TEXT) == []
        (line,) = _retry_lines(caplog)
        assert "connection lost during ack: ConnectionClosed" in line.getMessage()
        assert broker.acked == [again]

    @pytest.mark.asyncio
    async def test_nack_timeout_of_a_miss_is_not_success(self, caplog):
        """A NACK of a miss out of time ends the attempt; the next one goes on looking."""
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q",
            filter_data={"filter_headers": {"x-type": "order"}},
        )
        order = Msg(body=b"order", headers={"x-type": "order"})
        broker = _broker(Msg(headers={"x-type": "invoice"}), tail=[order])
        broker.hooks["nack"] = on(hang(), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.05)), 2)

        assert events[0]["message"]["body"] == "order"
        assert "ack did not complete within 0.05s" in _retry_lines(caplog)[0].getMessage()
        assert log_lines(caplog, ACK_TEXT) == []
        assert broker.acked == [order]

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
        assert str(info.value) == "declare did not complete within 0.01s"
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
            while "get" not in broker.calls and not task.done():
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


async def _until_open(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Serve the handshake up to and including reading ``Connection.Open``."""
    await reader.readexactly(8)
    writer.write(_frame(0, spec.Connection.Start(
        server_properties={"capabilities": {"basic.nack": True, "publisher_confirms": True}},
    )))
    await _read_frame(reader)
    writer.write(_frame(0, spec.Connection.Tune(channel_max=2047, frame_max=131072, heartbeat=0)))
    await _read_frame(reader)
    await _read_frame(reader)


async def _handshake(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    await _until_open(reader, writer)
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


def _close_after_start_ok(code: int, text: str):
    async def _handler(reader, writer):
        await reader.readexactly(8)
        writer.write(_frame(0, spec.Connection.Start(server_properties={})))
        await _read_frame(reader)
        writer.write(_frame(0, spec.Connection.Close(
            reply_code=code, reply_text=text, class_id=10, method_id=11,
        )))
        await writer.drain()
        await _forever()
    return _handler


def _close_on_open(code: int, text: str):
    async def _handler(reader, writer):
        await _until_open(reader, writer)
        writer.write(_frame(0, spec.Connection.Close(
            reply_code=code, reply_text=text, class_id=10, method_id=40,
        )))
        await writer.drain()
        await _forever()
    return _handler


async def _eof(reader, writer):
    writer.close()


async def _silent(reader, writer):
    await _forever()


def _channel_close_on_declare(text: str):
    async def _handler(reader, writer):
        await _handshake(reader, writer)
        await _read_frame(reader)
        writer.write(_frame(1, spec.Channel.OpenOk()))
        await _read_frame(reader)
        writer.write(_frame(1, spec.Channel.Close(
            reply_code=404, reply_text=text, class_id=50, method_id=10,
        )))
        await writer.drain()
        await _forever()
    return _handler


class _Script:
    """Per-connection server handlers, in order; connections past the list stay silent."""

    def __init__(self, *handlers):
        self.handlers = handlers
        self.accepted = 0

    async def __call__(self, reader, writer):
        self.accepted += 1
        index = self.accepted - 1
        await (self.handlers[index] if index < len(self.handlers) else _silent)(reader, writer)


async def _run_real(trigger, script: _Script, until_accepted: int | None = None) -> list[dict]:
    """Run ``trigger`` against a local server playing ``script``.

    :param until_accepted: Cancel ``run()`` once the server has taken this many
        connections; ``None`` waits for ``run()`` to end by itself.
    """
    events: list[dict] = []

    async def _run():
        async for event in trigger.run():
            events.append(event.payload)

    async with _local_server(script) as port:
        conn_info = FakeAirflowConnection(
            host="127.0.0.1", port=port, extra='{"connect_timeout": 0.2, "rpc_timeout": 2}'
        )
        with patch("airflow_provider_rmq.triggers.rmq.BaseHook") as mock_base:
            mock_base.get_connection = MagicMock(return_value=conn_info)
            task = asyncio.ensure_future(_run())
            if until_accepted is not None:
                while script.accepted < until_accepted and not task.done():
                    await asyncio.sleep(0.01)
                task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(task, 5)
    return events


class TestRealAiormqConnection:
    @pytest.mark.asyncio
    async def test_silent_handshake_times_out_and_the_connection_is_closed(self, caplog):
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
            ) as mock_base, caplog.at_level("WARNING"):
                mock_base.get_connection = MagicMock(return_value=conn_info)
                started = time.monotonic()
                task = asyncio.ensure_future(_collect_events(trigger))
                while len(created) < 2 and not task.done():
                    await asyncio.sleep(0.01)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, 2)

        assert time.monotonic() - started < 2
        (line, *_) = _retry_lines(caplog)
        assert "connect did not complete within 0.2s" in line.getMessage()
        assert all(connection.is_closed for connection in created)

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

        async def _fake_match_and_ack(message, msg_filter, miss_state, evaluation=None):
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

        async def _recording(message, msg_filter, miss_state, evaluation=None):
            verdicts.append(evaluation)
            return await match_and_ack(message, msg_filter, miss_state, evaluation=evaluation)

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

        async def _recording(message, msg_filter, miss_state, evaluation=None):
            seen.append(miss_state)
            result = await match_and_ack(message, msg_filter, miss_state, evaluation=evaluation)
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

        async def _recording(message, msg_filter, miss_state, evaluation=None):
            seen.append(miss_state)
            return await match_and_ack(message, msg_filter, miss_state, evaluation=evaluation)

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


# ---------------------------------------------------------------------------
# Classification of a failed attempt
# ---------------------------------------------------------------------------
def _open_refused(code: int, text: str) -> AMQPInternalError:
    """What aiormq raises when the broker answers ``Connection.Open`` with a close."""
    frame = spec.Connection.Close(reply_code=code, reply_text=text, class_id=10, method_id=40)
    return AMQPInternalError("one of ['Connection.OpenOk']", frame)


def _fails_on(attempt: int, failure):
    """Hook: the call never answers before ``attempt``, and ``failure`` runs on it."""
    async def _hook(connection, channel):
        if connection.attempt < attempt:
            await _forever()
        if connection.attempt == attempt:
            await failure(connection=connection, channel=channel)
    return _hook


STREAM_GET = "NOT_IMPLEMENTED - basic.get not supported by stream queues"

#: ``(step, hook, text the error event carries)``.
REFUSALS = [
    pytest.param(
        "connect", fail(ProbableAuthenticationError("ACCESS_REFUSED - Login was refused")),
        "ACCESS_REFUSED - Login was refused", id="auth",
    ),
    pytest.param("connect", fail(AuthenticationError("AMQPLAIN", ["PLAIN"])), "AMQPLAIN", id="sasl"),
    pytest.param("connect", fail(_open_refused(530, VHOST_REFUSED)), VHOST_REFUSED, id="vhost-530"),
    pytest.param(
        "connect", fail(_open_refused(403, "ACCESS_REFUSED - vhost")), "ACCESS_REFUSED - vhost",
        id="vhost-403",
    ),
    pytest.param(
        "get", fail(ConnectionNotAllowed("NOT_ALLOWED - vhost deleted")), "NOT_ALLOWED - vhost deleted",
        id="not-allowed",
    ),
    pytest.param(
        "get", lose(ConnectionNotAllowed("NOT_ALLOWED - vhost deleted")),
        "connection lost during get: ConnectionNotAllowed: NOT_ALLOWED - vhost deleted",
        id="not-allowed-closing",
    ),
    pytest.param(
        "declare", fail(ChannelAccessRefused("ACCESS_REFUSED - queue 'q'")), "ACCESS_REFUSED - queue 'q'",
        id="403",
    ),
    pytest.param(
        "declare",
        fail(ChannelLockedResource("RESOURCE_LOCKED - cannot obtain exclusive access to locked queue 'q'")),
        "RESOURCE_LOCKED", id="405",
    ),
    pytest.param(
        "declare", fail(ChannelNotFoundEntity("NOT_FOUND - no queue 'q'")), "NOT_FOUND - no queue 'q'",
        id="404",
    ),
    pytest.param(
        "get", close_channel(ChannelNotFoundEntity("NOT_FOUND - no queue 'q'")), "NOT_FOUND - no queue 'q'",
        id="404-channel",
    ),
    pytest.param(
        "get", lose(ConnectionNotImplemented(STREAM_GET)),
        f"connection lost during get: ConnectionNotImplemented: {STREAM_GET}", id="540-closing",
    ),
    pytest.param("get", fail(ConnectionNotImplemented(STREAM_GET)), STREAM_GET, id="540"),
    pytest.param(
        "declare", fail(ConnectionFrameError("FRAME_ERROR - bad frame")), "FRAME_ERROR - bad frame",
        id="501",
    ),
    pytest.param(
        "get", lose(ConnectionCommandInvalid("COMMAND_INVALID - second 'channel.open'")),
        "ConnectionCommandInvalid: COMMAND_INVALID", id="503-closing",
    ),
]

HANDSHAKE_FAILURES = [
    pytest.param(ConnectionRefusedError(111, "Connection refused"), id="refused"),
    pytest.param(ConnectionResetError(104, "Connection reset by peer"), id="reset"),
    pytest.param(IncompatibleProtocolError(), id="incompatible-protocol"),
    pytest.param(asyncio.IncompleteReadError(b"", 7), id="eof"),
    pytest.param(StopAsyncIteration(), id="stop-async-iteration"),
    pytest.param(
        ConnectionClosed(320, "CONNECTION_FORCED - broker forced connection closure"), id="closed-320"
    ),
    pytest.param(_open_refused(541, "INTERNAL_ERROR - vhost 'test' is down"), id="open-541"),
    pytest.param(_open_refused(320, "CONNECTION_FORCED - shutdown"), id="open-320"),
    pytest.param(
        _open_refused(530, VHOST_REFUSED + ": connection limit (5) is reached"), id="open-530-limit"
    ),
]


class TestClassification:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("attempt", [1, 3])
    @pytest.mark.parametrize(("step", "failure", "text"), REFUSALS)
    async def test_refusal_is_an_error_on_any_attempt(self, step, failure, text, attempt):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg())
        broker.hooks[step] = _fails_on(attempt, failure)

        events = await asyncio.wait_for(
            _run_against(trigger, broker, _timeouts(rpc=0.05, connect=0.05)), 2
        )

        assert events[0]["status"] == "error"
        assert text in events[0]["error"]
        assert len(broker.connections) == attempt
        assert all(connection.close_calls == 1 for connection in broker.connections)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exc", HANDSHAKE_FAILURES)
    async def test_failed_handshake_is_an_error_on_the_first_attempt(self, exc):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(), hooks={"connect": fail(exc)})

        events = await _run_against(trigger, broker)

        assert events[0]["status"] == "error"
        assert len(broker.connections) == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("exc", HANDSHAKE_FAILURES)
    async def test_failed_handshake_is_retried_on_a_later_attempt(self, exc, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"third"))
        broker.hooks["connect"] = _fails_on(2, fail(exc))

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(
                _run_against(trigger, broker, _timeouts(connect=0.05)), 2
            )

        assert events[0]["message"]["body"] == "third"
        lines = _retry_lines(caplog)
        assert "(attempt 2)" in lines[1].getMessage()
        assert f"connect failed: {type(exc).__name__}" in lines[1].getMessage()

    @pytest.mark.asyncio
    async def test_connection_lost_in_basic_get_is_retried(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"after"))
        broker.hooks["get"] = on(lose(Exception()), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        assert "connection lost during get: Exception" in _retry_lines(caplog)[0].getMessage()

    @pytest.mark.asyncio
    async def test_connection_lost_between_calls_is_retried(self, caplog):
        """Gone during ``poll_interval``: the next ``basic_get`` finds the channel closed."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", poll_interval=0.05)
        broker = _broker(tail=[None, Msg(body=b"after")])

        async def _lose_while_pausing(connection, channel):
            if broker.calls.count("get") == 1:
                asyncio.get_running_loop().call_later(0.01, connection.lose, asyncio.CancelledError())

        broker.hooks["get"] = _lose_while_pausing

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        assert "connection lost during get" in _retry_lines(caplog)[0].getMessage()
        assert len(broker.connections) == 2

    @pytest.mark.asyncio
    async def test_channel_closed_with_another_code_is_retried(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"after"))
        broker.hooks["get"] = on(close_channel(ChannelPreconditionFailed("PRECONDITION_FAILED")), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        assert "get failed: ChannelPreconditionFailed" in _retry_lines(caplog)[0].getMessage()

    @pytest.mark.asyncio
    async def test_runtime_error_of_a_closed_connection_is_retried(self, caplog):
        """``_step`` reads a call that failed on a closed connection as the lost connection."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"after"))

        async def _closed(connection, channel):
            connection.closing.set_exception(Exception())
            raise RuntimeError("Connection closed")

        broker.hooks["channel"] = on(_closed, 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        assert "connection lost during channel" in _retry_lines(caplog)[0].getMessage()

    @pytest.mark.asyncio
    async def test_call_on_a_closed_channel_is_retried(self, caplog):
        """``ChannelInvalidStateError`` with the connection still alive."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"after"))
        broker.hooks["get"] = on(fail(ChannelInvalidStateError("channel closed")), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        assert "get failed: ChannelInvalidStateError" in _retry_lines(caplog)[0].getMessage()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("step", ["declare", "get"])
    async def test_reset_socket_after_the_connect_is_retried_on_the_first_attempt(self, step, caplog):
        """Only the connect step is an error on the first attempt."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"after"))
        broker.hooks[step] = on(fail(ConnectionResetError(104, "Connection reset by peer")), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        (line,) = _retry_lines(caplog)
        assert f"{step} failed: ConnectionResetError" in line.getMessage()
        assert "(attempt 1)" in line.getMessage()

    @pytest.mark.asyncio
    async def test_call_cancelled_on_a_live_connection_is_retried(self, caplog):
        """A step ended by cancellation with ``closing`` still pending gives no reason."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"after"))
        broker.hooks["get"] = on(fail(asyncio.CancelledError()), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        assert "connection lost during get: no reason given" in _retry_lines(caplog)[0].getMessage()

    @pytest.mark.asyncio
    async def test_cancelled_closing_future_is_a_lost_connection(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"after"))

        async def _cancel_closing(connection, channel):
            connection.closing.cancel()
            await _forever()

        broker.hooks["get"] = on(_cancel_closing, 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        assert "connection lost during get: CancelledError" in _retry_lines(caplog)[0].getMessage()

    @pytest.mark.asyncio
    async def test_any_other_exception_is_an_error(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(), hooks={"get": fail(ValueError("bug"))})

        events = await _run_against(trigger, broker)

        assert events == [{"status": "error", "error": "bug"}]
        assert len(broker.connections) == 1


class TestHeartbeatLoss:
    """aiormq closes a connection whose heartbeat stopped with a bare CancelledError, sets
    it on every ``closing`` future and ends the pending call by cancellation."""

    @staticmethod
    def _heartbeat_lost():
        async def _hook(connection, channel):
            connection.lose(asyncio.CancelledError())
            raise asyncio.CancelledError()
        return _hook

    @pytest.mark.asyncio
    async def test_pull_step_ended_by_cancellation_is_retried(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"after"))
        broker.hooks["get"] = on(self._heartbeat_lost(), 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "after"
        (line,) = _retry_lines(caplog)
        assert "connection lost during get: CancelledError" in line.getMessage()
        assert broker.connections[0].channels[0].closing.done()

    @pytest.mark.asyncio
    async def test_cancelling_run_after_a_heartbeat_loss_passes_on(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = FakeBroker(hooks={"get": on(self._heartbeat_lost(), 1)})

        with _patched(broker, _timeouts(rpc=30)):
            task = asyncio.ensure_future(_collect_events(trigger))
            while broker.calls.count("get") < 2 and not task.done():
                await asyncio.sleep(0.005)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)

        assert [connection.close_calls for connection in broker.connections] == [1, 1]
        await _settle()
        assert _stray_tasks() == []


class TestHomeNodeDown:
    """A 404 naming the queue's home node is a restarting node, not a deleted queue."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("mode", "step", "failure"),
        [
            ("pull", "declare", fail(ChannelNotFoundEntity(HOME_NODE_DOWN))),
            ("pull", "get", close_channel(ChannelNotFoundEntity(HOME_NODE_DOWN))),
            ("push", "consume", close_channel(ChannelNotFoundEntity(HOME_NODE_DOWN))),
        ],
        ids=["declare", "get", "consume"],
    )
    async def test_404_of_a_home_node_is_retried(self, mode, step, failure, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode=mode)
        broker = _broker(Msg(body=b"back"))
        broker.hooks[step] = on(failure, 1)

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "back"
        (line,) = _retry_lines(caplog)
        assert "home node 'rabbit@node-1'" in line.getMessage()

    @pytest.mark.asyncio
    async def test_404_of_a_home_node_closing_the_push_channel_is_retried(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()

        async def _consume(connection, channel):
            async def _later():
                await asyncio.sleep(0.02)
                if connection.attempt == 1:
                    channel.closing.set_exception(ChannelNotFoundEntity(HOME_NODE_DOWN))
                else:
                    await channel.deliver(Msg(body=b"back"))
            asyncio.get_running_loop().create_task(_later())

        broker.hooks["consume"] = _consume

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "back"
        assert "home node" in _retry_lines(caplog)[0].getMessage()


# ---------------------------------------------------------------------------
# Leaving the push consumer
# ---------------------------------------------------------------------------
class TestPushGiveBack:
    @pytest.mark.asyncio
    async def test_match_cancels_then_nacks_the_unread_then_closes(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        first, second, third = Msg(body=b"first"), Msg(body=b"second"), Msg(body=b"third")
        broker = _broker(first, second, third)

        events = await _run_against(trigger, broker)

        assert events[0]["message"]["body"] == "first"
        assert broker.calls == ["connect", "channel", "declare", "consume", "ack", "cancel", "nack", "close"]
        assert broker.acked == [first]
        assert broker.nacked == [(second, True), (third, True)]

    @pytest.mark.asyncio
    async def test_expired_deadline_cancels_then_nacks_the_unread_then_closes(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=0.05,
            filter_data={"filter_headers": {"x-type": "order"}},
            ack_unmatched=True,
        )
        stray = Msg(headers={"x-type": "invoice"})
        unread = [Msg(headers={"x-type": "invoice"}), Msg(headers={"x-type": "invoice"})]
        broker = _broker(stray, *unread)

        def _slow_first(message, msg_filter):
            if not broker.acked:
                time.sleep(0.08)
            return amqp_utils.match(message, msg_filter)

        with patch("airflow_provider_rmq.triggers.rmq.match", _slow_first):
            events = await _run_against(trigger, broker)

        assert events == [{"status": "timeout"}]
        assert broker.calls[-3:] == ["cancel", "nack", "close"]
        assert broker.acked == [stray]
        assert broker.nacked == [(msg, True) for msg in unread]

    @pytest.mark.asyncio
    async def test_cancelled_run_cancels_then_nacks_the_unread_then_closes(self):
        """The sensor's timeout, a cleared task or a stopping triggerer cancel ``run()``: the
        unread deliveries go back by NACK. The match whose ACK the cancellation cut short
        had its frame queued already: it is acknowledged, and no event carries it."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        first, second, third = Msg(body=b"first"), Msg(body=b"second"), Msg(body=b"third")
        broker = _broker(first, second, third, hooks={"ack": hang()})

        with _patched(broker):
            task = asyncio.ensure_future(_collect_events(trigger))
            while "ack" not in broker.calls and not task.done():
                await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)

        assert broker.calls[-3:] == ["cancel", "nack", "close"]
        assert broker.acked == [first]
        assert broker.nacked == [(second, True), (third, True)]
        assert broker.connections[0].close_calls == 1

    @pytest.mark.asyncio
    async def test_cancelled_run_with_an_empty_buffer_cancels_and_closes(self):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()

        with _patched(broker):
            task = asyncio.ensure_future(_collect_events(trigger))
            while "consume" not in broker.calls and not task.done():
                await asyncio.sleep(0.01)
            await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)

        assert broker.calls[-2:] == ["cancel", "close"]
        assert broker.nacked == []

    @pytest.mark.asyncio
    async def test_cancelled_run_holds_the_exit_for_one_rpc_bound(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = _broker(Msg(), Msg(), hooks={"ack": hang(), "cancel": hang()})

        with _patched(broker, _timeouts(rpc=0.2)), caplog.at_level("WARNING"):
            task = asyncio.ensure_future(_collect_events(trigger))
            while "ack" not in broker.calls and not task.done():
                await asyncio.sleep(0.01)
            started = time.monotonic()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 3)
            elapsed = time.monotonic() - started

        assert 0.2 <= elapsed < 1.5
        (line,) = log_lines(caplog, "returning 1 unread deliveries")
        assert "cancel did not complete within 0.2s" in line.getMessage()
        assert broker.connections[0].close_calls == 1

    @pytest.mark.asyncio
    async def test_delivery_arriving_during_the_cancel_is_given_back(self):
        """A delivery the broker sent before ``cancel-ok`` lands in the buffer and goes back."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        first, late = Msg(body=b"first"), Msg(body=b"late")
        broker = _broker(first)

        async def _deliver_before_cancel_ok(connection, channel):
            await channel.deliver(late)

        broker.hooks["cancel"] = _deliver_before_cancel_ok

        events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "first"
        assert broker.nacked == [(late, True)]

    @pytest.mark.asyncio
    async def test_hanging_cancel_holds_the_exit_for_one_rpc_bound(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = _broker(Msg(body=b"first"), Msg(), Msg())
        broker.hooks["cancel"] = hang()

        with caplog.at_level("WARNING"):
            started = time.monotonic()
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.2)), 3)
            elapsed = time.monotonic() - started

        assert events[0]["message"]["body"] == "first"
        assert 0.2 <= elapsed < 1.5
        (line,) = log_lines(caplog, "returning 2 unread deliveries")
        assert "cancel did not complete within 0.2s" in line.getMessage()
        assert broker.nacked == []
        assert broker.connections[0].close_calls == 1

    @pytest.mark.asyncio
    async def test_cancel_and_nack_share_one_rpc_bound(self, caplog):
        """The NACK gets what the cancel left of ``rpc_timeout``, not a bound of its own."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = _broker(Msg(body=b"first"), Msg(), Msg())

        async def _slow_cancel(connection, channel):
            await asyncio.sleep(0.4)

        broker.hooks["cancel"] = _slow_cancel
        broker.hooks["nack"] = hang()

        with caplog.at_level("WARNING"):
            started = time.monotonic()
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.6)), 3)
            elapsed = time.monotonic() - started

        assert events[0]["message"]["body"] == "first"
        # One rpc_timeout for the two of them; a bound of its own for the NACK is 1.0 s.
        assert 0.6 <= elapsed < 0.9
        (line,) = log_lines(caplog, "returning 2 unread deliveries")
        bound = float(re.search(r"nack did not complete within ([0-9.]+)s", line.getMessage()).group(1))
        assert bound < 0.25

    @pytest.mark.asyncio
    async def test_cancel_spending_the_whole_bound_leaves_no_nack(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = _broker(Msg(body=b"first"), Msg(), Msg())

        async def _cancel_past_the_bound(connection, channel):
            # Blocking the loop lets the cancel complete, with nothing left of the bound.
            time.sleep(0.25)  # noqa: ASYNC251

        broker.hooks["cancel"] = _cancel_past_the_bound

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.2)), 3)

        assert events[0]["message"]["body"] == "first"
        assert "nack" not in broker.calls
        (line,) = log_lines(caplog, "returning 2 unread deliveries")
        assert "nack did not complete within 0s" in line.getMessage()

    @pytest.mark.asyncio
    async def test_failed_nack_leaves_the_rest_to_the_close(self, caplog):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = _broker(Msg(body=b"first"), Msg(), Msg())
        broker.hooks["nack"] = lose(Exception())

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "first"
        assert len(log_lines(caplog, "returning 2 unread deliveries")) == 1
        assert broker.connections[0].close_calls == 1


class TestPushDeliveryTakenBeforeTheDeadline:
    """A delivery taken off the buffer before the deadline is disposed of under the full
    ``rpc_timeout``; the deadline then ends the attempt through the give-back."""

    @pytest.mark.asyncio
    async def test_stray_is_returned_and_the_rest_given_back(self):
        """The NACK of a miss and its 0.1 s pause outlast what the deadline left."""
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=0.05,
            filter_data={"filter_headers": {"x-type": "order"}},
        )
        first, second = Msg(headers={"x-type": "invoice"}), Msg(headers={"x-type": "invoice"})
        broker = _broker(first, second)

        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events == [{"status": "timeout"}]
        assert broker.calls[-4:] == ["nack", "cancel", "nack", "close"]
        assert broker.nacked == [(first, True), (second, True)]

    @pytest.mark.asyncio
    async def test_match_is_acked_without_a_false_warning(self, caplog):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=0.05,
        )
        matching = Msg(body=b"order")
        broker = _broker(matching)

        async def _slow_ack(connection, channel):
            await asyncio.sleep(0.15)

        broker.hooks["ack"] = _slow_ack

        with caplog.at_level("WARNING"):
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events[0]["message"]["body"] == "order"
        assert log_lines(caplog, ACK_TEXT) == []
        assert broker.acked == [matching]


# ---------------------------------------------------------------------------
# Pause between attempts and message_wait_timeout
# ---------------------------------------------------------------------------
class TestPauseAndDeadline:
    @pytest.mark.asyncio
    async def test_pause_doubles_to_sixty_and_starts_over_after_a_wait(self, pauses):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", poll_interval=0.02)
        broker = _broker(tail=[None, Msg(body=b"seventh")])
        broker.hooks["connect"] = on(hang(), 1, 2, 3, 4, 6)

        async def _lose_after_an_empty_get(connection, channel):
            # Attempt 5 gets an answer to basic_get, the wait for a message, and loses
            # the connection while it pauses on the empty queue.
            asyncio.get_running_loop().call_later(0.005, connection.lose, asyncio.CancelledError())

        broker.hooks["get"] = on(_lose_after_an_empty_get, 5)

        events = await asyncio.wait_for(
            _run_against(trigger, broker, _timeouts(rpc=30, connect=0.01)), 5
        )

        assert events[0]["message"]["body"] == "seventh"
        assert pauses == [15, 30, 60, 60, 15, 30]

    @pytest.mark.asyncio
    async def test_pull_attempt_that_got_no_answer_to_get_keeps_the_pause_growing(self, pauses):
        """Only a ``basic_get`` that returned is the wait for a message."""
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q")
        broker = _broker(Msg(body=b"fourth"))
        broker.hooks["get"] = on(hang(), 1, 2, 3)

        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.01)), 5)

        assert events[0]["message"]["body"] == "fourth"
        assert pauses == [15, 30, 60]

    @pytest.mark.asyncio
    async def test_push_pause_starts_over_after_a_wait(self, pauses):
        trigger = RMQTrigger(rmq_conn_id="conn", queue_name="q", mode="push")
        broker = FakeBroker()
        broker.hooks["connect"] = on(hang(), 1, 2, 4)

        async def _consume(connection, channel):
            async def _later():
                await asyncio.sleep(0.01)
                if connection.attempt == 3:
                    connection.lose(asyncio.CancelledError())
                else:
                    await channel.deliver(Msg(body=b"fifth"))
            asyncio.get_running_loop().create_task(_later())

        broker.hooks["consume"] = _consume

        events = await asyncio.wait_for(
            _run_against(trigger, broker, _timeouts(rpc=30, connect=0.01)), 5
        )

        assert events[0]["message"]["body"] == "fifth"
        assert pauses == [15, 30, 15, 30]

    @pytest.mark.asyncio
    async def test_pull_mode_ignores_message_wait_timeout(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", message_wait_timeout=0.01, poll_interval=0.05,
        )
        broker = _broker(tail=[None, Msg(body=b"later")])

        events = await asyncio.wait_for(_run_against(trigger, broker), 2)

        assert events[0]["message"]["body"] == "later"

    @pytest.mark.asyncio
    async def test_deadline_running_out_during_the_pause(self, pauses):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=0.15,
        )
        broker = FakeBroker(hooks={"connect": hang()})

        started = time.monotonic()
        events = await asyncio.wait_for(
            _run_against(trigger, broker, _timeouts(rpc=30, connect=0.03)), 2
        )

        assert events == [{"status": "timeout"}]
        assert time.monotonic() - started < 0.5
        (pause,) = pauses
        assert pause < 0.15
        assert broker.calls.count("connect") == 1
        assert all(connection.close_calls == 1 for connection in broker.connections)

    @pytest.mark.asyncio
    async def test_deadline_running_out_between_steps(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=0.05,
        )

        async def _slow_channel(connection, channel):
            # Blocking the loop lets the step complete within its bound but past the
            # deadline, which the next step then finds gone.
            time.sleep(0.07)  # noqa: ASYNC251

        broker = FakeBroker(hooks={"channel": _slow_channel})

        events = await _run_against(trigger, broker)

        assert events == [{"status": "timeout"}]
        assert broker.calls == ["connect", "channel", "close"]

    @pytest.mark.asyncio
    async def test_deadline_cuts_a_step_bound(self):
        """A step is given what the deadline leaves, then the deadline decides."""
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=0.05,
        )
        broker = FakeBroker(hooks={"declare": hang()})

        started = time.monotonic()
        events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)

        assert events == [{"status": "timeout"}]
        assert time.monotonic() - started < 0.5
        assert len(broker.connections) == 1

    @pytest.mark.asyncio
    async def test_overrun_without_a_buffer_is_at_most_the_close(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=0.05,
        )
        broker = FakeBroker(hooks={"close": hang()})

        with patch("airflow_provider_rmq.utils.amqp.CLOSE_TIMEOUT", 0.2):
            started = time.monotonic()
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=30)), 2)
            elapsed = time.monotonic() - started

        assert events == [{"status": "timeout"}]
        # rpc_timeout is 30 s: anything short of it shows the exit added no rpc bound.
        assert 0.25 <= elapsed < 1.0

    @pytest.mark.asyncio
    async def test_overrun_with_a_hanging_cancel_is_at_most_rpc_and_the_close(self):
        trigger = RMQTrigger(
            rmq_conn_id="conn", queue_name="q", mode="push", message_wait_timeout=0.05,
        )
        broker = FakeBroker(hooks={"close": hang(), "cancel": hang()})

        with patch("airflow_provider_rmq.utils.amqp.CLOSE_TIMEOUT", 0.5):
            started = time.monotonic()
            events = await asyncio.wait_for(_run_against(trigger, broker, _timeouts(rpc=0.5)), 5)
            elapsed = time.monotonic() - started

        assert events == [{"status": "timeout"}]
        # Deadline, one rpc_timeout and the close: 1.05 s; a second rpc_timeout is 1.55 s.
        assert 1.05 <= elapsed < 1.45


# ---------------------------------------------------------------------------
# Handshake outcomes on a real aiormq.Connection
# ---------------------------------------------------------------------------
REAL_HANDSHAKE_FAILURES = [
    pytest.param(
        _close_after_start_ok(320, "CONNECTION_FORCED - broker forced connection closure"),
        id="close-320-after-start-ok",
    ),
    pytest.param(_close_on_open(541, "INTERNAL_ERROR - vhost 'test' is down"), id="open-541"),
    pytest.param(_close_on_open(320, "CONNECTION_FORCED - shutdown"), id="open-320"),
    pytest.param(_eof, id="eof"),
]


class TestRealHandshake:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("handler", REAL_HANDSHAKE_FAILURES)
    async def test_failure_on_the_first_attempt_is_an_error(self, handler):
        script = _Script(handler)

        events = await _run_real(RMQTrigger(rmq_conn_id="conn", queue_name="q"), script)

        assert events[0]["status"] == "error"
        assert "connection lost" not in events[0]["error"]
        assert script.accepted == 1

    @pytest.mark.asyncio
    @pytest.mark.parametrize("handler", REAL_HANDSHAKE_FAILURES)
    async def test_failure_on_the_second_attempt_is_retried(self, handler, caplog):
        script = _Script(_silent, handler)

        with caplog.at_level("WARNING"):
            events = await _run_real(RMQTrigger(rmq_conn_id="conn", queue_name="q"), script, 3)

        assert events == []
        second = _retry_lines(caplog)[1].getMessage()
        assert "(attempt 2)" in second
        assert "connect failed" in second

    @pytest.mark.asyncio
    @pytest.mark.parametrize("handlers", [(), (_silent,)], ids=["first", "second"])
    async def test_vhost_refused_is_an_error_on_any_attempt(self, handlers):
        script = _Script(*handlers, _close_on_open(530, VHOST_REFUSED))

        events = await _run_real(RMQTrigger(rmq_conn_id="conn", queue_name="q"), script)

        assert events[0]["status"] == "error"
        assert script.accepted == len(handlers) + 1

    @pytest.mark.asyncio
    async def test_login_refused_on_the_second_attempt_is_an_error(self):
        """A ``Connection.Close`` 403 after ``StartOk`` is a refusal, not a cut handshake."""
        script = _Script(_silent, _close_after_start_ok(403, "ACCESS_REFUSED - Login was refused"))

        events = await _run_real(RMQTrigger(rmq_conn_id="conn", queue_name="q"), script)

        assert events[0]["status"] == "error"
        assert "ACCESS_REFUSED" in events[0]["error"]
        assert script.accepted == 2

    @pytest.mark.asyncio
    async def test_404_of_a_home_node_is_read_off_the_real_exception(self, caplog):
        script = _Script(_channel_close_on_declare(HOME_NODE_DOWN))

        with caplog.at_level("WARNING"):
            events = await _run_real(RMQTrigger(rmq_conn_id="conn", queue_name="q"), script, 2)

        assert events == []
        (line, *_) = _retry_lines(caplog)
        assert "declare failed: ChannelNotFoundEntity" in line.getMessage()
        assert "home node 'rabbit@node-1'" in line.getMessage()

    @pytest.mark.asyncio
    async def test_404_of_a_missing_queue_is_an_error(self):
        script = _Script(_channel_close_on_declare("NOT_FOUND - no queue 'q' in vhost 'test'"))

        events = await _run_real(RMQTrigger(rmq_conn_id="conn", queue_name="q"), script)

        assert events[0]["status"] == "error"
        assert "no queue 'q'" in events[0]["error"]
        assert script.accepted == 1
