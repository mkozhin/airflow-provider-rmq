from __future__ import annotations

import logging
from typing import Any, Callable, Sequence

from airflow.sensors.base import BaseSensorOperator

from apache_airflow_provider_rmq.hooks.rmq import RMQHook
from apache_airflow_provider_rmq.utils.filters import MessageFilter

log = logging.getLogger("airflow.task")


class RMQSensor(BaseSensorOperator):
    """Wait for a message in a RabbitMQ queue matching optional conditions.

    Supports classic poke mode and deferrable mode (via RMQTrigger).
    Matching message is returned via XCom. Non-matching messages are NACKed with requeue.
    """

    template_fields: Sequence[str] = ("queue_name",)
    ui_color = "#ff6600"

    def __init__(
        self,
        *,
        queue_name: str,
        rmq_conn_id: str = "rmq_default",
        filter_headers: dict[str, Any] | None = None,
        filter_callable: Callable[[Any, str], bool] | None = None,
        deferrable: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.rmq_conn_id = rmq_conn_id
        self.filter_headers = filter_headers
        self.filter_callable = filter_callable
        self.deferrable = deferrable
        self._return_value: dict | None = None

        if self.deferrable and self.filter_callable is not None:
            raise ValueError(
                "Callable-based filters are not supported in deferrable mode "
                "because they cannot be serialized to the triggerer process. "
                "Use dict-based filter_headers instead, or disable deferrable mode."
            )

    def execute(self, context: Any) -> dict | None:
        if self.deferrable:
            self._defer()
            return None  # unreachable, defer raises TaskDeferred
        super().execute(context)
        return self._return_value

    def _defer(self) -> None:
        from apache_airflow_provider_rmq.triggers.rmq import RMQTrigger

        msg_filter = MessageFilter(filter_headers=self.filter_headers)

        self.defer(
            trigger=RMQTrigger(
                rmq_conn_id=self.rmq_conn_id,
                queue_name=self.queue_name,
                filter_data=msg_filter.serialize(),
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: Any, event: dict[str, Any]) -> dict | None:
        if event.get("status") == "success":
            return event.get("message")
        raise RuntimeError(f"RMQTrigger failed: {event.get('error', 'unknown error')}")

    def poke(self, context: Any) -> bool:
        msg_filter = MessageFilter(
            filter_headers=self.filter_headers,
            filter_callable=self.filter_callable,
        )

        hook = RMQHook(rmq_conn_id=self.rmq_conn_id)
        try:
            info = hook.queue_info(self.queue_name)
            if not info.get("exists") or info.get("message_count", 0) == 0:
                return False

            messages = hook.consume_messages(
                queue_name=self.queue_name,
                max_messages=info["message_count"],
                auto_ack=False,
            )

            for i, msg in enumerate(messages):
                if msg_filter.matches(msg["properties"], msg["body"]):
                    hook.ack(msg["method"].delivery_tag)
                    self._return_value = {
                        "body": msg["body"],
                        "headers": dict(msg["properties"].headers or {}),
                        "routing_key": msg["method"].routing_key,
                        "exchange": msg["method"].exchange,
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