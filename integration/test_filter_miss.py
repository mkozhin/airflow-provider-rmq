"""What a filter miss does to a real queue, on both consuming paths.

The unit suite proves the branches with mocks; these tests prove the broker agrees:
an acknowledged miss leaves the queue and is not dead-lettered, a requeued one comes
back at the pace of the pause, and the batch-oriented paths take exactly one batch per
fetch.

Every queue here is declared through the ``scratch`` fixture, so it carries the run's
prefix and is deleted in teardown. Dead-lettering is routed through the default
exchange, which the broker owns: ``scratch`` keeps a register of queues only, so a
test that needs an exchange of its own has to teach it to delete one first.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import aio_pika
import pika

from airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator
from airflow_provider_rmq.sensors.rmq import RMQSensor
from airflow_provider_rmq.utils.amqp import FilterMissState, match_and_ack
from airflow_provider_rmq.utils.filters import MessageFilter

from .conftest import BrokerScratch

log = logging.getLogger(__name__)

#: Headers of a message these tests want, and of one they do not.
WANTED = {"type": "wanted"}
STRAY = {"type": "stray"}

#: Seconds a queue is given to reach an expected depth. A NACKed message is back in the
#: queue as soon as the broker has processed the frame, so this bounds a wait that
#: normally ends on the first poll.
DEPTH_TIMEOUT = 10.0


def wait_for_depth(scratch: BrokerScratch, queue: str, expected: int) -> int:
    """Return the depth of *queue*, waiting up to :data:`DEPTH_TIMEOUT` for *expected*.

    Depth is the number of ready messages, measured by a passive ``queue_declare``, so
    a message still unacknowledged on some channel is not counted. Returning the depth
    rather than asserting on it keeps the failure message at the call site.
    """
    deadline = time.monotonic() + DEPTH_TIMEOUT
    depth = scratch.depth(queue)
    while depth != expected and time.monotonic() < deadline:
        time.sleep(0.05)
        depth = scratch.depth(queue)
    return depth


def publish_many(
    channel: pika.adapters.blocking_connection.BlockingChannel,
    queue: str,
    bodies: list[bytes],
    headers: dict[str, Any],
) -> None:
    """Publish *bodies* to *queue* on one channel, in order.

    ``BrokerScratch.publish`` opens a channel per message, which costs a round-trip
    each; a test that fills a queue with a hundred and fifty messages uses this instead.
    The queue name comes from ``scratch``, so it is already inside the run's namespace.
    """
    properties = pika.BasicProperties(headers=headers)
    for body in bodies:
        channel.basic_publish(exchange="", routing_key=queue, body=body, properties=properties)


async def consume_once(
    queue: aio_pika.abc.AbstractQueue,
    msg_filter: MessageFilter,
    miss_state: FilterMissState,
) -> bool | None:
    """Take one message off *queue* through ``match_and_ack``.

    :returns: the verdict of ``match_and_ack``, or ``None`` when the queue was empty.
    """
    message = await queue.get(fail=False)
    if message is None:
        return None
    return await match_and_ack(message, msg_filter, miss_state)


async def test_ack_unmatched_takes_the_message_out_of_the_queue(
    scratch: BrokerScratch, aio_channel: aio_pika.abc.AbstractChannel
) -> None:
    """With the parameter on, a rejected message is gone and never comes back."""
    name = scratch.declare_queue("ack-unmatched")
    scratch.publish(name, b"stray", headers=STRAY)
    queue = await aio_channel.declare_queue(name, passive=True)

    miss_state = FilterMissState(name, log, ack_unmatched=True)
    assert await consume_once(queue, MessageFilter(filter_headers=WANTED), miss_state) is False

    assert miss_state.streak == 1
    assert miss_state.dropped == 1
    assert wait_for_depth(scratch, name, 0) == 0

    # A requeued message would be back within the 0.1 s pause; an acknowledged one is
    # not there for the rest of the second either.
    deadline = time.monotonic() + 1.0
    while time.monotonic() < deadline:
        assert await queue.get(fail=False) is None
        await asyncio.sleep(0.1)


async def test_ack_unmatched_does_not_dead_letter(
    scratch: BrokerScratch, aio_channel: aio_pika.abc.AbstractChannel
) -> None:
    """An acknowledgement is not a reject, so the dead-letter exchange stays silent."""
    dead = scratch.declare_queue("dlx.dead")
    source = scratch.declare_queue(
        "dlx.source",
        arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": dead},
    )
    scratch.publish(source, b"stray", headers=STRAY)
    queue = await aio_channel.declare_queue(source, passive=True)

    miss_state = FilterMissState(source, log, ack_unmatched=True)
    assert await consume_once(queue, MessageFilter(filter_headers=WANTED), miss_state) is False

    assert wait_for_depth(scratch, source, 0) == 0
    await asyncio.sleep(0.5)
    assert scratch.depth(dead) == 0

    # The dead-letter route itself works: a reject on the same queue does fill the
    # dead-letter queue, so the empty check above is about the acknowledgement and not
    # about a mistyped argument.
    scratch.publish(source, b"rejected", headers=STRAY)
    rejected = await queue.get(fail=False)
    assert rejected is not None
    await rejected.nack(requeue=False)
    assert wait_for_depth(scratch, dead, 1) == 1


async def test_default_requeues_and_paces_the_redeliveries(
    scratch: BrokerScratch, aio_channel: aio_pika.abc.AbstractChannel
) -> None:
    """Without the parameter the message stays in the queue and comes back paced.

    The bound on redeliveries is one-sided on purpose: it catches a pause that went
    missing, and a broker busy with someone else's load only makes the loop slower.
    """
    name = scratch.declare_queue("requeue-loop")
    scratch.publish(name, b"stray", headers=STRAY)
    queue = await aio_channel.declare_queue(name, passive=True)

    msg_filter = MessageFilter(filter_headers=WANTED)
    miss_state = FilterMissState(name, log, ack_unmatched=False)
    window = 2.0
    started = time.monotonic()
    deliveries = 0
    while time.monotonic() - started < window:
        outcome = await consume_once(queue, msg_filter, miss_state)
        if outcome is None:
            # The message is in flight back to the queue; this is not a redelivery.
            await asyncio.sleep(0.05)
            continue
        assert outcome is False
        deliveries += 1
    elapsed = time.monotonic() - started

    assert deliveries >= 2, "the message was not redelivered at all"
    # The pause sits between deliveries, so the first one is not preceded by it.
    assert deliveries <= 10 * elapsed + 1
    assert miss_state.streak == deliveries
    assert miss_state.dropped == 0
    assert wait_for_depth(scratch, name, 1) == 1


async def test_a_body_that_does_not_read_stays_in_the_queue(
    scratch: BrokerScratch, aio_channel: aio_pika.abc.AbstractChannel
) -> None:
    """An ERROR is not a miss: the broker still holds the message with the parameter on.

    This is the decision where being wrong destroys data, so it is asserted against a
    real queue and not only against a mocked ``nack``: the filter asks for a ``body.*``
    key on a body that is not JSON, and the depth has to stay at one however many turns
    the loop takes.
    """
    name = scratch.declare_queue("unreadable-body")
    scratch.publish(name, b"not json at all", headers=WANTED)
    queue = await aio_channel.declare_queue(name, passive=True)

    msg_filter = MessageFilter(filter_headers={"body.status": "ok"})
    miss_state = FilterMissState(name, log, ack_unmatched=True)
    turns = 0
    while turns < 3:
        outcome = await consume_once(queue, msg_filter, miss_state)
        if outcome is None:
            # The message is on its way back to the queue; this is not a turn.
            await asyncio.sleep(0.05)
            continue
        assert outcome is False
        turns += 1

    assert miss_state.error_streak == 3
    assert miss_state.dropped == 0
    assert wait_for_depth(scratch, name, 1) == 1


def test_sensor_takes_one_message_per_poke(
    scratch: BrokerScratch,
    pika_channel: pika.adapters.blocking_connection.BlockingChannel,
    rmq_conn_id: str,
) -> None:
    """A poke returns on the first match and leaves the rest of the batch in the queue."""
    name = scratch.declare_queue("sensor-batch")
    publish_many(pika_channel, name, [json.dumps({"n": n}).encode() for n in range(3)], WANTED)
    assert wait_for_depth(scratch, name, 3) == 3

    sensor = RMQSensor(
        task_id="sensor_batch",
        queue_name=name,
        rmq_conn_id=rmq_conn_id,
        filter_headers=WANTED,
        poke_batch_size=10,
    )

    assert sensor.poke({}) is True
    assert json.loads(sensor._return_value["body"]) == {"n": 0}
    assert wait_for_depth(scratch, name, 2) == 2

    assert sensor.poke({}) is True
    assert json.loads(sensor._return_value["body"]) == {"n": 1}
    assert wait_for_depth(scratch, name, 1) == 1


def test_operator_drains_the_head_one_batch_per_run(
    scratch: BrokerScratch,
    pika_channel: pika.adapters.blocking_connection.BlockingChannel,
    rmq_conn_id: str,
) -> None:
    """With the parameter on, successive runs walk the queue down to the wanted message.

    The wanted message sits behind a hundred and fifty stray ones, so the first run
    cannot reach it: the operator fetches one batch per run, as it does today. What the
    acknowledgement changes is that the batch is gone afterwards, so the second run
    reaches the tail — without it the same hundred would come back forever.
    """
    name = scratch.declare_queue("operator-drain")
    publish_many(pika_channel, name, [f"stray-{n}".encode() for n in range(150)], STRAY)
    publish_many(pika_channel, name, [b"wanted"], WANTED)
    assert wait_for_depth(scratch, name, 151) == 151

    operator = RMQConsumeOperator(
        task_id="consume_drain",
        queue_name=name,
        rmq_conn_id=rmq_conn_id,
        filter_headers=WANTED,
        max_messages=100,
        ack_unmatched=True,
    )

    assert operator.execute({}) == []
    assert wait_for_depth(scratch, name, 51) == 51

    matched = operator.execute({})
    assert [message["body"] for message in matched] == ["wanted"]
    assert wait_for_depth(scratch, name, 0) == 0
