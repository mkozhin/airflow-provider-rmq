"""What the broker accepts from the provider's declares and QoS on RabbitMQ 4.3+.

These tests prove that the provider's defaults work on a broker that denies the
deprecated features ``transient_nonexcl_queues`` and ``global_qos``:
``RMQQueueManagementOperator`` declares a durable queue by default; ``RMQHook`` declares
an exclusive queue, durable by default or not, and the second declare of the same
exclusive queue on the same connection is accepted as equivalent; a hook built with a
true ``global_qos`` opens its channel, publishes and reads.

Durability of a non-exclusive queue is checked over AMQP alone: the broker compares
``durable`` of an active declare with the existing queue and answers a mismatch with
``406 PRECONDITION_FAILED``, so a declare with ``durable=True`` succeeds and one with
``durable=False`` is refused. An exclusive queue is transient whatever ``durable`` says,
and the broker accepts either value when it is declared again.

Every queue here is named through the ``scratch`` fixture and registered for teardown.
An exclusive queue belongs to the hook's connection and is deleted by the broker when
that connection closes; each test waits for that before it ends, so the teardown finds
nothing left to delete.
"""

from __future__ import annotations

import time

import pika
import pytest
from pika.exceptions import ChannelClosedByBroker

from airflow_provider_rmq.hooks.rmq import RMQHook
from airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator

from .conftest import BrokerScratch

#: Seconds an exclusive queue is given to disappear after its connection closes.
EXCLUSIVE_GONE_TIMEOUT = 5.0


def _redeclare(connection: pika.BlockingConnection, name: str, *, durable: bool) -> None:
    """Actively declare the non-exclusive queue *name* again, on a channel of its own.

    Succeeds only when *durable* matches the existing queue; the broker closes the
    channel with ``406 PRECONDITION_FAILED`` otherwise.
    """
    channel = connection.channel()
    try:
        channel.queue_declare(queue=name, durable=durable)
    finally:
        if channel.is_open:
            channel.close()


def _wait_until_gone(scratch: BrokerScratch, name: str) -> None:
    """Wait until a passive declare of *name* answers ``404 NOT_FOUND``.

    The broker removes an exclusive queue once the owning connection has terminated,
    which can trail the ``connection.close-ok`` the client waits for.
    """
    deadline = time.monotonic() + EXCLUSIVE_GONE_TIMEOUT
    while True:
        try:
            scratch.depth(name)
        except ChannelClosedByBroker as exc:
            assert exc.reply_code == 404
            return
        if time.monotonic() >= deadline:
            pytest.fail(f"exclusive queue {name} still exists after its connection closed")
        time.sleep(0.1)


def test_operator_declares_a_durable_queue_by_default(
    scratch: BrokerScratch,
    pika_connection: pika.BlockingConnection,
    rmq_conn_id: str,
) -> None:
    """``declare_queue`` with default arguments creates a durable queue."""
    name = scratch.register_queue(scratch.name("operator-default"))

    RMQQueueManagementOperator(
        task_id="declare_queue_default",
        rmq_conn_id=rmq_conn_id,
        action="declare_queue",
        queue_name=name,
    ).execute({})

    assert scratch.depth(name) == 0
    _redeclare(pika_connection, name, durable=True)
    with pytest.raises(ChannelClosedByBroker, match="current is 'true'") as excinfo:
        _redeclare(pika_connection, name, durable=False)
    assert excinfo.value.reply_code == 406


def test_hook_declares_an_exclusive_queue_twice_on_one_connection(
    scratch: BrokerScratch, rmq_conn_id: str
) -> None:
    """An exclusive queue with the default ``durable`` is declared, then declared again."""
    name = scratch.register_queue(scratch.name("hook-exclusive-default"))

    with RMQHook(rmq_conn_id=rmq_conn_id) as hook:
        first = hook.queue_declare(name, exclusive=True)
        second = hook.queue_declare(name, exclusive=True)
        assert first.method.queue == name
        assert second.method.queue == name

    _wait_until_gone(scratch, name)


def test_hook_declares_a_transient_exclusive_queue(
    scratch: BrokerScratch, rmq_conn_id: str
) -> None:
    """``durable=False`` with ``exclusive=True`` is a queue the broker accepts."""
    name = scratch.register_queue(scratch.name("hook-exclusive-transient"))

    with RMQHook(rmq_conn_id=rmq_conn_id) as hook:
        result = hook.queue_declare(name, durable=False, exclusive=True)
        assert result.method.queue == name

    _wait_until_gone(scratch, name)


def test_hook_with_global_qos_publishes_and_reads(
    scratch: BrokerScratch, rmq_conn_id: str
) -> None:
    """A hook built with ``global_qos=True`` sets prefetch, publishes and consumes."""
    name = scratch.declare_queue("qos-global")
    bodies = ["one", "two", "three"]

    with RMQHook(
        rmq_conn_id=rmq_conn_id, qos={"prefetch_count": 5, "global_qos": True}
    ) as hook:
        for body in bodies:
            hook.basic_publish(exchange="", routing_key=name, body=body)
        messages = hook.consume_messages(name, max_messages=len(bodies))
        for message in messages:
            hook.ack(message["method"].delivery_tag)

    assert [message["body"] for message in messages] == bodies
    assert scratch.depth(name) == 0
