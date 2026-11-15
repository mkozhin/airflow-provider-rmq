"""What the broker says to ``RMQPublishOperator`` with publisher confirms on.

These tests prove the broker answers a publish with confirms where a message would
otherwise be lost, and that the operator fails the task on that answer: a queue that
does not exist, a binding that does not exist, an exchange that does not exist, and a
queue that refuses the message; a message to an existing queue is delivered. One test drives ``RMQHook`` directly
to prove that the channel it reopens after the broker closed one is in confirms mode.

Every queue here is declared through the ``scratch`` fixture, so it carries the run's
prefix and is deleted in teardown. The tests that need a missing queue or a missing
exchange only name one under the run's prefix and never create it; the one exchange
published to by name is the broker's own ``amq.direct``, with a routing key under the
run's prefix that no queue is bound to.
"""

from __future__ import annotations

import pika
import pytest
from airflow.exceptions import AirflowException
from pika.exceptions import ChannelClosedByBroker, NackError, UnroutableError

from airflow_provider_rmq.hooks.rmq import RMQHook
from airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator

from .conftest import BrokerScratch

#: Built-in direct exchange every vhost has; nothing is bound to the keys used here.
BUILTIN_DIRECT = "amq.direct"


def test_publish_to_an_existing_queue_lands_in_the_queue(
    scratch: BrokerScratch,
    pika_channel: pika.adapters.blocking_connection.BlockingChannel,
    rmq_conn_id: str,
) -> None:
    """A confirmed publish succeeds and the message is in the queue when it returns."""
    name = scratch.declare_queue("publish-ok")

    RMQPublishOperator(
        task_id="publish_ok",
        rmq_conn_id=rmq_conn_id,
        queue_name=name,
        message=["first", "second"],
    ).execute({})

    # The acknowledgement comes after the message is enqueued, so the depth needs no wait.
    assert scratch.depth(name) == 2
    method, _, body = pika_channel.basic_get(queue=name, auto_ack=True)
    assert method is not None
    assert body == b"first"


def test_publish_to_a_missing_queue_fails_with_no_route(
    scratch: BrokerScratch, rmq_conn_id: str
) -> None:
    """``queue_name=`` of a queue nobody declared is returned by the broker."""
    missing = scratch.name("no-such-queue")

    operator = RMQPublishOperator(
        task_id="publish_missing_queue",
        rmq_conn_id=rmq_conn_id,
        queue_name=missing,
        message="lost",
    )
    with pytest.raises(AirflowException, match="312 NO_ROUTE") as excinfo:
        operator.execute({})

    assert "message 1 of 1" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, UnroutableError)


def test_unbound_key_on_a_named_exchange_passes_by_default(
    rmq_prefix: str, rmq_conn_id: str
) -> None:
    """A named exchange is not mandatory by default: an unrouted message is accepted."""
    RMQPublishOperator(
        task_id="publish_unbound_default",
        rmq_conn_id=rmq_conn_id,
        exchange=BUILTIN_DIRECT,
        routing_key=f"{rmq_prefix}unbound",
        message="nobody listens",
    ).execute({})


def test_unbound_key_on_a_named_exchange_fails_when_mandatory(
    rmq_prefix: str, rmq_conn_id: str
) -> None:
    """``mandatory=True`` makes the same unrouted message fail the task."""
    operator = RMQPublishOperator(
        task_id="publish_unbound_mandatory",
        rmq_conn_id=rmq_conn_id,
        exchange=BUILTIN_DIRECT,
        routing_key=f"{rmq_prefix}unbound",
        message="nobody listens",
        mandatory=True,
    )
    with pytest.raises(AirflowException, match="312 NO_ROUTE"):
        operator.execute({})


def test_publish_to_a_missing_exchange_fails_with_not_found(
    scratch: BrokerScratch, rmq_conn_id: str
) -> None:
    """The broker closes the channel with 404, and the task fails on that message."""
    operator = RMQPublishOperator(
        task_id="publish_missing_exchange",
        rmq_conn_id=rmq_conn_id,
        exchange=scratch.name("no-such-exchange"),
        routing_key="any",
        message="lost",
    )
    with pytest.raises(
        AirflowException, match=r"broker closed the channel: 404 NOT_FOUND"
    ) as excinfo:
        operator.execute({})

    assert "message 1 of 1" in str(excinfo.value)
    assert isinstance(excinfo.value.__cause__, ChannelClosedByBroker)


def test_publish_to_a_queue_that_rejects_fails_with_nack(
    scratch: BrokerScratch, rmq_conn_id: str
) -> None:
    """A full queue with ``reject-publish`` overflow nacks the message."""
    name = scratch.declare_queue(
        "reject-publish",
        arguments={"x-max-length": 0, "x-overflow": "reject-publish"},
    )

    operator = RMQPublishOperator(
        task_id="publish_rejected",
        rmq_conn_id=rmq_conn_id,
        queue_name=name,
        message="refused",
    )
    with pytest.raises(AirflowException, match=r"rejected by the broker \(nack\)") as excinfo:
        operator.execute({})

    assert isinstance(excinfo.value.__cause__, NackError)
    assert scratch.depth(name) == 0


def test_channel_reopened_after_not_found_is_in_confirms_mode(
    scratch: BrokerScratch, rmq_conn_id: str
) -> None:
    """After a 404 closes the channel, the hook's next channel still waits for confirms.

    The unrouted publish on the reopened channel raises ``UnroutableError`` only when
    that channel is in confirms mode; without confirms it would return silently.
    """
    with RMQHook(rmq_conn_id=rmq_conn_id, publisher_confirms=True) as hook:
        with pytest.raises(ChannelClosedByBroker) as excinfo:
            hook.basic_publish(
                exchange=scratch.name("no-such-exchange"), routing_key="any", body="lost"
            )
        assert excinfo.value.reply_code == 404

        with pytest.raises(UnroutableError):
            hook.basic_publish(
                exchange="",
                routing_key=scratch.name("no-such-queue"),
                body="lost",
                mandatory=True,
            )
