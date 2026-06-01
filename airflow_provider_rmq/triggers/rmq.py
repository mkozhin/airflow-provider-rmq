from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Literal

import aio_pika
from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow_provider_rmq.utils.amqp import build_amqp_connection, match_and_ack
from airflow_provider_rmq.utils.filters import MessageFilter

log = logging.getLogger("airflow.task")


class RMQTrigger(BaseTrigger):
    """Async trigger that waits for a matching message in a RabbitMQ queue.

    Uses aio_pika for non-blocking AMQP access. Supports two delivery modes:

    - ``mode="pull"`` (default): polls the queue periodically via ``queue.get()``.
      Use ``poll_interval`` to control the wait between attempts.
    - ``mode="push"``: subscribes via ``basic_consume`` (``queue.iterator()``).
      The broker delivers messages instantly as they arrive — no polling delay.
      Use ``message_wait_timeout`` to limit how long the trigger waits for a message.

    Note: callable filters are NOT supported here (cannot be serialized).

    Note:
        When using quorum queues (default in RabbitMQ 4.x clusters), non-matching
        messages are NACKed with requeue=True. RabbitMQ 4.0+ enforces a default
        redelivery limit of 20 for quorum queues — after 20 redeliveries the message
        is dead-lettered or dropped. If heavy filtering is expected, consider using
        a dedicated queue or increasing the delivery-limit policy.
    """

    def __init__(
        self,
        rmq_conn_id: str,
        queue_name: str,
        filter_data: dict[str, Any] | None = None,
        poll_interval: float = 5.0,
        mode: Literal["pull", "push"] = "pull",
        message_wait_timeout: float | None = None,
    ):
        """Create a new RMQTrigger.

        :param rmq_conn_id: Airflow connection ID for RabbitMQ.
        :param queue_name: Name of the RabbitMQ queue to poll.
        :param filter_data: Serialized filter data from :meth:`MessageFilter.serialize`.
        :param poll_interval: Seconds between poll attempts when queue is empty (pull mode only).
        :param mode: Delivery mode — ``"pull"`` (periodic polling) or ``"push"``
            (broker-pushed via ``basic_consume``).
        :param message_wait_timeout: Maximum seconds to wait for a matching message in push
            mode. ``None`` means wait indefinitely (until the sensor-level timeout fires).
            Note: the actual wait may slightly exceed this value while the AMQP consumer
            is cancelled gracefully (``basic_cancel`` round-trip to the broker).
        """
        super().__init__()
        self.rmq_conn_id = rmq_conn_id
        self.queue_name = queue_name
        self.filter_data = filter_data or {}
        self.poll_interval = poll_interval
        self.mode = mode
        self.message_wait_timeout = message_wait_timeout

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "airflow_provider_rmq.triggers.rmq.RMQTrigger",
            {
                "rmq_conn_id": self.rmq_conn_id,
                "queue_name": self.queue_name,
                "filter_data": self.filter_data,
                "poll_interval": self.poll_interval,
                "mode": self.mode,
                "message_wait_timeout": self.message_wait_timeout,
            },
        )

    async def _handle_message(
        self,
        message: Any,
        msg_filter: MessageFilter,
    ) -> dict[str, Any] | None:
        """ACK matching messages, NACK non-matching ones.

        Returns a success payload dict on match, or None if the message did not match.
        """
        matched = await match_and_ack(message, msg_filter)
        if not matched:
            return None
        return {
            "status": "success",
            "message": {
                "body": message.body.decode("utf-8"),
                "headers": dict(message.headers or {}),
                "routing_key": message.routing_key or "",
                "exchange": message.exchange or "",
            },
        }

    async def _run_push(
        self,
        queue: Any,
        msg_filter: MessageFilter,
    ) -> dict[str, Any] | None:
        """Push mode: subscribe via basic_consume and wait for a matching message."""

        async def _consume() -> dict[str, Any] | None:
            async with queue.iterator() as q_iter:
                async for message in q_iter:
                    result = await self._handle_message(message, msg_filter)
                    if result is not None:
                        return result
            return None

        try:
            if self.message_wait_timeout is not None:
                return await asyncio.wait_for(_consume(), timeout=self.message_wait_timeout)
            else:
                return await _consume()
        except asyncio.TimeoutError:
            return {"status": "timeout"}

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Main trigger loop."""
        msg_filter = MessageFilter.deserialize(self.filter_data)

        try:
            conn_info = await asyncio.get_running_loop().run_in_executor(
                None, BaseHook.get_connection, self.rmq_conn_id
            )
            url, ssl_context = build_amqp_connection(conn_info)
            connect_kwargs: dict[str, Any] = {"url": url}
            if ssl_context is not None:
                connect_kwargs["ssl_context"] = ssl_context

            connection = await aio_pika.connect_robust(**connect_kwargs)

            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(self.queue_name, passive=True)

                if self.mode == "push":
                    result = await self._run_push(queue, msg_filter)
                    if result is None:
                        yield TriggerEvent({
                            "status": "error",
                            "error": "Push consumer ended without matching message",
                        })
                    else:
                        yield TriggerEvent(result)
                    return

                # pull mode
                while True:
                    message = await queue.get(fail=False)

                    if message is None:
                        await asyncio.sleep(self.poll_interval)
                        continue

                    result = await self._handle_message(message, msg_filter)
                    if result is not None:
                        yield TriggerEvent(result)
                        return
                    # sleep(0.1) after NACK is handled inside match_and_ack

        except Exception as e:
            log.exception("RMQTrigger encountered an error")
            yield TriggerEvent({"status": "error", "error": str(e)})
