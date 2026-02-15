from __future__ import annotations

import logging
from typing import Any, Callable, Sequence

import pika.exceptions
from airflow.models import BaseOperator

from apache_airflow_provider_rmq.hooks.rmq import RMQHook
from apache_airflow_provider_rmq.utils.filters import MessageFilter

log = logging.getLogger("airflow.task")


class RMQConsumeOperator(BaseOperator):
    """Consume messages from a RabbitMQ queue with optional filtering.

    Matching messages are ACKed and returned via XCom.
    Non-matching messages are NACKed with requeue=True (their status is not changed).
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
        """
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.rmq_conn_id = rmq_conn_id
        self.max_messages = max_messages
        self.filter_headers = filter_headers
        self.filter_callable = filter_callable
        self.qos = qos

    def execute(self, context: Any) -> list[dict[str, Any]]:
        msg_filter = MessageFilter(
            filter_headers=self.filter_headers,
            filter_callable=self.filter_callable,
        )

        matched_messages: list[dict[str, Any]] = []

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
                if msg_filter.matches(msg["properties"], msg["body"]):
                    hook.ack(msg["method"].delivery_tag)
                    matched_messages.append({
                        "body": msg["body"],
                        "headers": dict(msg["properties"].headers or {}),
                        "routing_key": msg["method"].routing_key,
                        "exchange": msg["method"].exchange,
                    })
                else:
                    hook.nack(msg["method"].delivery_tag, requeue=True)

        log.info(
            "Consumed %d matching messages from queue '%s'.",
            len(matched_messages),
            self.queue_name,
        )
        return matched_messages