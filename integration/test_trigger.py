"""``RMQTrigger.run()`` against a real broker.

The unit suite drives the trigger over fakes of ``aiormq``; these tests prove the broker
behaves the way those fakes say it does: a message is delivered and acknowledged in
both modes, a quorum queue keeps the deliveries the push consumer gave back without
counting them, a deleted queue cancels the consumer and then answers 404, a connection
the broker closes is followed by a new attempt, ``message_wait_timeout`` ends the wait,
and a wrong password is an error on the first attempt.

Every queue is declared through the ``scratch`` fixture and deleted in teardown. The
connections opened here are the trigger's own, named ``rmq-trigger:<queue>``; the one
test that closes a connection from the broker's side finds it by that name.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import pika
import pytest

from airflow_provider_rmq.triggers import rmq as trigger_module
from airflow_provider_rmq.triggers.rmq import RMQTrigger
from airflow_provider_rmq.utils.amqp import CLOSE_TIMEOUT
from airflow_provider_rmq.utils.filters import MessageFilter

from .conftest import BrokerScratch, ManagementApi

#: Headers of a message these tests want, and of one they do not.
WANTED = {"type": "wanted"}
STRAY = {"type": "stray"}

#: Filter data the trigger takes: only messages with :data:`WANTED` headers match.
WANTED_FILTER = MessageFilter(filter_headers=WANTED).serialize()

#: Seconds a trigger is given to fire its event when nothing is meant to slow it down.
EVENT_TIMEOUT = 30.0

#: Seconds a condition on the broker is given to come true.
CONDITION_TIMEOUT = 15.0

#: ``rpc_timeout`` of the connection the retry tests use: it is also the longest turn of
#: the push wait, so a cancelled consumer is noticed within it.
FAST_RPC_TIMEOUT = 2

#: Pause between attempts in the retry tests.
FAST_PAUSE = 0.1

#: Text of the WARNING the trigger writes before every new attempt.
RETRY_LINE = "; retrying queue"


async def first_event(trigger: RMQTrigger, within: float = EVENT_TIMEOUT) -> dict[str, Any]:
    """Run *trigger* until its first event and return the payload.

    The generator is closed afterwards, which is what the triggerer does once an event
    is in hand; ``within`` bounds the whole run so a hanging trigger fails the test.
    """
    events = trigger.run()
    try:
        event = await asyncio.wait_for(events.__anext__(), within)
    finally:
        await events.aclose()
    return event.payload


async def wait_for(condition: Callable[[], Any], within: float = CONDITION_TIMEOUT) -> Any:
    """Poll *condition* until it returns something truthy, and return that.

    The event loop keeps turning between polls, so a trigger running alongside goes on
    with its attempt.

    :raises AssertionError: *within* seconds passed first.
    """
    deadline = time.monotonic() + within
    while True:
        value = condition()
        if value:
            return value
        assert time.monotonic() < deadline, f"condition not met within {within:g}s"
        await asyncio.sleep(0.1)


def retry_lines(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Messages of the WARNINGs the trigger wrote before a new attempt."""
    return [r.getMessage() for r in caplog.records if RETRY_LINE in r.getMessage()]


def publish_conn(monkeypatch: pytest.MonkeyPatch, conn_id: str, url: str) -> str:
    """Publish *url* as the Airflow connection *conn_id* for this test only."""
    monkeypatch.setenv(f"AIRFLOW_CONN_{conn_id.upper()}", url)
    return conn_id


@pytest.fixture
def fast_conn_id(rmq_url: str, monkeypatch: pytest.MonkeyPatch) -> str:
    """Connection to the test broker with ``rpc_timeout`` of :data:`FAST_RPC_TIMEOUT`."""
    separator = "&" if "?" in rmq_url else "?"
    return publish_conn(
        monkeypatch, "rmq_trigger_fast", f"{rmq_url}{separator}rpc_timeout={FAST_RPC_TIMEOUT}"
    )


@pytest.fixture
def fast_pause(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Shrink the pause between attempts to :data:`FAST_PAUSE` seconds."""
    monkeypatch.setattr(trigger_module, "RETRY_PAUSE_MIN", FAST_PAUSE)
    monkeypatch.setattr(trigger_module, "RETRY_PAUSE_MAX", FAST_PAUSE)
    yield


@pytest.fixture
def trigger_log(caplog: pytest.LogCaptureFixture) -> pytest.LogCaptureFixture:
    """``caplog`` recording the WARNINGs of the trigger's ``airflow.task`` logger."""
    caplog.set_level(logging.WARNING, logger="airflow.task")
    return caplog


@pytest.mark.parametrize("mode", ["pull", "push"])
async def test_matching_message_fires_success(
    scratch: BrokerScratch, rmq_conn_id: str, mode: str
) -> None:
    """The event carries body, headers, routing key and exchange; the message is acked."""
    queue = scratch.declare_queue(f"success.{mode}")
    scratch.publish(queue, b'{"id": 1}', headers=WANTED)
    trigger = RMQTrigger(
        rmq_conn_id, queue, filter_data=WANTED_FILTER, poll_interval=0.2, mode=mode
    )

    event = await first_event(trigger)

    assert event == {
        "status": "success",
        "message": {
            "body": '{"id": 1}',
            "headers": WANTED,
            "routing_key": queue,
            "exchange": "",
        },
    }
    assert scratch.depth(queue) == 0


async def test_push_on_quorum_queue_gives_back_the_rest_uncounted(
    scratch: BrokerScratch,
    rmq_conn_id: str,
    pika_channel: pika.adapters.blocking_connection.BlockingChannel,
    trigger_log: pytest.LogCaptureFixture,
) -> None:
    """The deliveries behind the match go back by NACK and carry no delivery count.

    A quorum queue counts a delivery dropped with its connection — the next read of
    such a message has an ``x-delivery-count`` header — but not one NACKed back.
    """
    queue = scratch.declare_queue("quorum.push", arguments={"x-queue-type": "quorum"})
    scratch.publish(queue, b"wanted", headers=WANTED)
    scratch.publish(queue, b"second", headers=STRAY)
    scratch.publish(queue, b"third", headers=STRAY)
    trigger = RMQTrigger(rmq_conn_id, queue, filter_data=WANTED_FILTER, mode="push")

    event = await first_event(trigger)

    assert event["status"] == "success"
    assert event["message"]["body"] == "wanted"
    await wait_for(lambda: scratch.depth(queue) == 2)
    left = []
    for _ in range(2):
        method, properties, body = pika_channel.basic_get(queue, auto_ack=True)
        assert method is not None
        left.append((body, dict(properties.headers or {})))
    assert sorted(body for body, _ in left) == [b"second", b"third"]
    for body, headers in left:
        assert "x-delivery-count" not in headers, (body, headers)
    assert not [r for r in trigger_log.records if "unread deliveries" in r.getMessage()]


async def test_deleted_queue_cancels_the_consumer_then_answers_404(
    scratch: BrokerScratch,
    fast_conn_id: str,
    fast_pause: None,
    trigger_log: pytest.LogCaptureFixture,
) -> None:
    """A queue deleted under the push wait: a WARNING and a retry, then a 404 error."""
    queue = scratch.declare_queue("deleted")
    trigger = RMQTrigger(fast_conn_id, queue, filter_data=WANTED_FILTER, mode="push")
    running = asyncio.ensure_future(first_event(trigger))
    try:
        await wait_for(lambda: scratch.consumers(queue) == 1)
        scratch.delete_queue(queue)
        deleted_at = time.monotonic()
        event = await asyncio.wait_for(running, EVENT_TIMEOUT)
    finally:
        running.cancel()
    elapsed = time.monotonic() - deleted_at

    assert event["status"] == "error"
    assert "NOT_FOUND" in event["error"]
    assert elapsed <= 10.0
    (line,) = retry_lines(trigger_log)
    assert "cancelled by the broker" in line


async def test_connection_closed_by_the_broker_is_retried(
    scratch: BrokerScratch,
    pika_connection: pika.BlockingConnection,
    rmq_management: ManagementApi,
    fast_conn_id: str,
    fast_pause: None,
    trigger_log: pytest.LogCaptureFixture,
) -> None:
    """The broker closes the trigger's connection: a WARNING, a new attempt, a delivery.

    Only the connection named after the scratch queue is closed; the connection of the
    ``scratch`` fixture carries on.
    """
    queue = scratch.declare_queue("forced-close")
    name = f"rmq-trigger:{queue}"
    trigger = RMQTrigger(fast_conn_id, queue, filter_data=WANTED_FILTER, mode="push")
    running = asyncio.ensure_future(first_event(trigger))
    try:
        await wait_for(lambda: scratch.consumers(queue) == 1)
        # The Management API lists a connection once its statistics have been collected.
        await wait_for(lambda: rmq_management.connections_named(name))
        rmq_management.close_connection_named(name, "integration test closes the trigger")
        await wait_for(lambda: retry_lines(trigger_log))
        await wait_for(lambda: scratch.consumers(queue) == 1)
        scratch.publish(queue, b"after", headers=WANTED)
        event = await asyncio.wait_for(running, EVENT_TIMEOUT)
    finally:
        running.cancel()

    assert event["status"] == "success"
    assert event["message"]["body"] == "after"
    (line,) = retry_lines(trigger_log)
    assert "connection lost" in line
    assert pika_connection.is_open
    assert scratch.depth(queue) == 0


async def test_message_wait_timeout_fires_timeout(scratch: BrokerScratch, rmq_conn_id: str) -> None:
    """An empty queue in push mode: the timeout event within the bound and its overrun."""
    queue = scratch.declare_queue("wait-timeout")
    trigger = RMQTrigger(rmq_conn_id, queue, mode="push", message_wait_timeout=3)

    started = time.monotonic()
    event = await first_event(trigger)
    elapsed = time.monotonic() - started

    assert event == {"status": "timeout"}
    assert elapsed >= 2.9
    assert elapsed <= 3.0 + CLOSE_TIMEOUT


async def test_wrong_password_is_an_error_on_the_first_attempt(
    scratch: BrokerScratch,
    rmq_url: str,
    monkeypatch: pytest.MonkeyPatch,
    trigger_log: pytest.LogCaptureFixture,
) -> None:
    """The broker refuses the login: an error event, and no attempt after the first."""
    queue = scratch.declare_queue("wrong-password")
    parts = urlsplit(rmq_url)
    userinfo, _, host = parts.netloc.rpartition("@")
    user = userinfo.partition(":")[0]
    wrong = urlunsplit(parts._replace(netloc=f"{user}:wrong-{uuid.uuid4().hex}@{host}"))
    conn_id = publish_conn(monkeypatch, "rmq_trigger_wrong_password", wrong)
    trigger = RMQTrigger(conn_id, queue, mode="push")

    event = await first_event(trigger)

    assert event["status"] == "error"
    assert retry_lines(trigger_log) == []
