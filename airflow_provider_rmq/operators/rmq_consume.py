from __future__ import annotations

import logging
from functools import partial
from typing import Any, Callable, Sequence

import pika.exceptions
from airflow.models import BaseOperator

from airflow_provider_rmq.hooks.rmq import RMQHook
from airflow_provider_rmq.utils.amqp import FilterMissState, dispose_miss
from airflow_provider_rmq.utils.filters import FilterVerdict, MessageFilter

log = logging.getLogger("airflow.task")


class RMQConsumeOperator(BaseOperator):
    """Consume messages from a RabbitMQ queue with optional filtering.

    Matching messages are ACKed and returned via XCom.

    A message the filter read and rejected is disposed of according to
    ``ack_unmatched``: NACKed with requeue=True by default, so it stays in the queue for
    another consumer, or ACKed and gone for good when the parameter is on. A message the
    filter could not be evaluated on — a callable that raised, a body that does not read
    as JSON — is always returned to the queue, whatever ``ack_unmatched`` says.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#ff6600"

    def __init__(
        self,
        *,
        queue_name: str,
        rmq_conn_id: str = "rmq_default",
        max_messages: int = 100,
        filter_headers: dict[str, Any] | None = None,
        filter_callable: Callable[[Any, str], bool] | None = None,
        qos: dict | None = None,
        ack_unmatched: bool = False,
        **kwargs,
    ):
        """Create a new RMQConsumeOperator.

        :param queue_name: Name of the RabbitMQ queue to consume from.
        :type queue_name: str
        :param rmq_conn_id: Airflow connection ID for RabbitMQ.
        :type rmq_conn_id: str
        :param max_messages: Maximum number of messages to consume per execution.
        :type max_messages: int
        :param filter_headers: Dict of AMQP headers that a message must match.
        :type filter_headers: dict[str, Any] | None
        :param filter_callable: Callable ``(properties, body) -> bool`` for custom filtering.
        :type filter_callable: Callable[[Any, str], bool] | None
        :param qos: QoS settings dict (``prefetch_size``, ``prefetch_count``, ``global_qos``).
        :type qos: dict | None
        :param ack_unmatched: What to do with a message the filter rejected. ``False``
            (default) returns it to the queue. ``True`` acknowledges it instead: the
            message leaves the broker for good — DLX does not fire on an
            acknowledgement. Use it only on a queue this task owns. Must be a ``bool``:
            the string ``"false"`` is truthy, and the parameter destroys messages.
        :type ack_unmatched: bool
        """
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.rmq_conn_id = rmq_conn_id
        self.max_messages = max_messages
        self.filter_headers = filter_headers
        self.filter_callable = filter_callable
        self.qos = qos

        if not isinstance(ack_unmatched, bool):
            raise ValueError("'ack_unmatched' must be a bool.")  # noqa: TRY004
        self.ack_unmatched = ack_unmatched

    def execute(self, context: Any) -> list[dict[str, Any]]:
        msg_filter = MessageFilter(
            filter_headers=self.filter_headers,
            filter_callable=self.filter_callable,
        )

        miss_state = FilterMissState(
            queue_name=self.queue_name,
            log=log,
            ack_unmatched=self.ack_unmatched,
        )
        matched_messages: list[dict[str, Any]] = []
        errors = 0

        with RMQHook(rmq_conn_id=self.rmq_conn_id, qos=self.qos) as hook:
            try:
                raw_messages = hook.consume_messages(
                    queue_name=self.queue_name,
                    max_messages=self.max_messages,
                    auto_ack=False,
                )
            except pika.exceptions.ChannelClosedByBroker as e:
                log.warning("Queue '%s' is not available: %s", self.queue_name, e)
                return []

            if not raw_messages:
                log.info("Queue '%s' is empty.", self.queue_name)
                return []

            for msg in raw_messages:
                verdict, reason = msg_filter.evaluate(msg["properties"], msg["body"])
                if verdict is FilterVerdict.MATCH:
                    miss_state.on_match()
                    hook.ack(msg["method"].delivery_tag)
                    matched_messages.append({
                        "body": msg["body"],
                        "headers": dict(msg["properties"].headers or {}),
                        "routing_key": msg["method"].routing_key or "",
                        "exchange": msg["method"].exchange or "",
                    })
                elif verdict is FilterVerdict.ERROR:
                    errors += 1
                    miss_state.on_error(reason)
                    hook.nack(msg["method"].delivery_tag, requeue=True)
                else:
                    tag = msg["method"].delivery_tag
                    dispose_miss(
                        miss_state,
                        partial(hook.ack, tag),
                        partial(hook.nack, tag, requeue=True),
                    )

            # `evaluated` is what the filter reached a verdict on: a body it could not
            # read is not evidence about filter_data, and counting it would point the
            # batch line at the wrong thing. `miss_state.dropped` is the batch total
            # too — the state lives exactly one execute(), and execute() reads one batch.
            miss_state.on_batch_done(
                matched=len(matched_messages),
                evaluated=len(raw_messages) - errors,
                dropped_in_batch=miss_state.dropped,
            )

        log.info(
            "Consumed %d matching messages from queue '%s'.",
            len(matched_messages),
            self.queue_name,
        )
        return matched_messages