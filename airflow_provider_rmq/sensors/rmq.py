from __future__ import annotations

import logging
from typing import Any, Callable, Literal, Sequence

import pika.exceptions
from airflow.exceptions import AirflowSkipException
from airflow.sensors.base import BaseSensorOperator

from airflow_provider_rmq.hooks.rmq import RMQHook
from airflow_provider_rmq.utils.filters import MessageFilter

log = logging.getLogger("airflow.task")


class RMQSensor(BaseSensorOperator):
    """Wait for a message in a RabbitMQ queue matching optional conditions.

    Supports classic poke mode and deferrable mode (via RMQTrigger).
    Matching message is returned via XCom. Non-matching messages are NACKed with requeue.

    .. note::
        ``filter_callable`` is **not supported** in deferrable mode (``deferrable=True``)
        because callables cannot be serialized to the triggerer process. Use dict-based
        ``filter_headers`` instead, or set ``deferrable=False``.

    .. note::
        When using quorum queues (default in RabbitMQ 4.x clusters), non-matching
        messages are NACKed with requeue=True. RabbitMQ 4.0+ enforces a default
        redelivery limit of 20 for quorum queues — after 20 redeliveries the message
        is dead-lettered or dropped. If heavy filtering is expected, consider using
        a dedicated queue or increasing the delivery-limit policy.
    """

    template_fields: Sequence[str] = ("queue_name", "message_wait_timeout")
    ui_color = "#ff6600"

    def __init__(
        self,
        *,
        queue_name: str,
        rmq_conn_id: str = "rmq_default",
        filter_headers: dict[str, Any] | None = None,
        filter_callable: Callable[[Any, str], bool] | None = None,
        deferrable: bool = False,
        poke_batch_size: int = 100,
        mode: Literal["pull", "push"] = "pull",
        message_wait_timeout: float | None = None,
        **kwargs,
    ):
        """Create a new RMQSensor.

        :param queue_name: Name of the RabbitMQ queue to monitor.
        :param rmq_conn_id: Airflow connection ID for RabbitMQ.
        :param filter_headers: Dict of AMQP headers that a message must match.
        :param filter_callable: Callable ``(properties, body) -> bool`` for custom filtering.
            Not supported with ``deferrable=True``.
        :param deferrable: Run in deferrable mode using :class:`RMQTrigger`.
        :param poke_batch_size: Max messages to fetch per poke cycle.
        :param mode: Delivery mode for deferrable trigger — ``"pull"`` (periodic polling,
            default) or ``"push"`` (broker-pushed via ``basic_consume``). Only used when
            ``deferrable=True``.
        :param message_wait_timeout: Maximum seconds to wait for a matching message in push
            mode. ``None`` means wait indefinitely. Only valid when ``mode="push"`` and
            ``deferrable=True``. Note: actual wait may slightly exceed this value while the
            AMQP consumer is cancelled gracefully.
        """
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.rmq_conn_id = rmq_conn_id
        self.filter_headers = filter_headers
        self.filter_callable = filter_callable
        self.deferrable = deferrable
        self.poke_batch_size = poke_batch_size
        self.mode = mode
        self.message_wait_timeout = message_wait_timeout
        self._return_value: dict | None = None

        if self.deferrable and self.filter_callable is not None:
            raise ValueError(
                "Callable-based filters are not supported in deferrable mode "
                "because they cannot be serialized to the triggerer process. "
                "Use dict-based filter_headers instead, or disable deferrable mode."
            )

        if self.message_wait_timeout is not None and self.mode != "push":
            raise ValueError(
                "message_wait_timeout is only supported in push mode (mode='push'). "
                "Either set mode='push' or remove message_wait_timeout."
            )

    def execute(self, context: Any) -> dict | None:
        if self.deferrable:
            self._defer()
            return None  # unreachable, defer raises TaskDeferred
        super().execute(context)
        return self._return_value

    def _defer(self) -> None:
        from airflow_provider_rmq.triggers.rmq import RMQTrigger

        msg_filter = MessageFilter(filter_headers=self.filter_headers)

        timeout = float(self.message_wait_timeout) if self.message_wait_timeout is not None else None
        self.defer(
            trigger=RMQTrigger(
                rmq_conn_id=self.rmq_conn_id,
                queue_name=self.queue_name,
                filter_data=msg_filter.serialize(),
                mode=self.mode,
                message_wait_timeout=timeout,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Any, event: dict[str, Any]) -> dict | None:
        if event.get("status") == "success":
            return event.get("message")
        if event.get("status") == "timeout":
            raise AirflowSkipException(
                f"RMQSensor timed out waiting for message in queue '{self.queue_name}'"
            )
        raise RuntimeError(f"RMQTrigger failed: {event.get('error', 'unknown error')}")

    def poke(self, context: Any) -> bool:
        msg_filter = MessageFilter(
            filter_headers=self.filter_headers,
            filter_callable=self.filter_callable,
        )

        hook = RMQHook(rmq_conn_id=self.rmq_conn_id)
        try:
            try:
                messages = hook.consume_messages(
                    queue_name=self.queue_name,
                    max_messages=self.poke_batch_size,
                    auto_ack=False,
                )
            except pika.exceptions.ChannelClosedByBroker as e:
                log.warning("Queue '%s' is not available: %s", self.queue_name, e)
                return False

            if not messages:
                return False

            for i, msg in enumerate(messages):
                if msg_filter.matches(msg["properties"], msg["body"]):
                    hook.ack(msg["method"].delivery_tag)
                    self._return_value = {
                        "body": msg["body"],
                        "headers": dict(msg["properties"].headers or {}),
                        "routing_key": msg["method"].routing_key or "",
                        "exchange": msg["method"].exchange or "",
                    }
                    # Nack remaining messages
                    for remaining in messages[i + 1:]:
                        hook.nack(remaining["method"].delivery_tag, requeue=True)
                    return True
                else:
                    hook.nack(msg["method"].delivery_tag, requeue=True)
            return False
        finally:
            hook.close()
