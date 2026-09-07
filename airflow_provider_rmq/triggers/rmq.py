from __future__ import annotations

import asyncio
import logging
from typing import Any, AsyncIterator, Literal

import aio_pika
from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

from airflow_provider_rmq.utils.amqp import FilterMissState, build_amqp_connection, match_and_ack
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
        With ``ack_unmatched`` off, a non-matching message is NACKed with
        requeue=True. RabbitMQ 4.0+ enforces a default redelivery limit of 20 for
        quorum queues (the default in 4.x clusters) — after 20 redeliveries the
        message is dead-lettered or dropped. If heavy filtering is expected, use a
        dedicated queue with ``ack_unmatched=True``, or raise the delivery-limit
        policy. With ``ack_unmatched`` on the message is acknowledged instead and
        never redelivered, so the limit is out of the picture.
    """

    def __init__(
        self,
        rmq_conn_id: str,
        queue_name: str,
        filter_data: dict[str, Any] | None = None,
        poll_interval: float = 5.0,
        mode: Literal["pull", "push"] = "pull",
        message_wait_timeout: float | None = None,
        ack_unmatched: bool = False,
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
        :param ack_unmatched: What to do with a message the filter rejected. ``False``
            (default) returns it to the queue. ``True`` acknowledges it instead: the
            message leaves the broker for good — DLX does not fire on an
            acknowledgement. Use it only on a queue this task owns. Must be a ``bool``:
            the string ``"false"`` is truthy, and the parameter destroys messages.
        """
        super().__init__()
        self.rmq_conn_id = rmq_conn_id
        self.queue_name = queue_name
        self.filter_data = filter_data or {}
        self.poll_interval = poll_interval
        self.mode = mode
        self.message_wait_timeout = message_wait_timeout

        if not isinstance(ack_unmatched, bool):
            raise ValueError("'ack_unmatched' must be a bool.")  # noqa: TRY004
        self.ack_unmatched = ack_unmatched

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
                "ack_unmatched": self.ack_unmatched,
            },
        )

    async def _handle_message(
        self,
        message: Any,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any] | None:
        """ACK a matching message and dispose of the rest by verdict.

        :param message: An ``aio_pika`` message.
        :param msg_filter: Pre-built filter the message is evaluated against.
        :param miss_state: Counters of this ``run()``; its ``ack_unmatched`` decides
            whether a rejected message is acknowledged or returned to the queue. A
            message the filter could not be evaluated on goes back to the queue either
            way.

        Returns a success payload dict on match, or None if the message did not match.
        """
        matched = await match_and_ack(message, msg_filter, miss_state)
        if not matched:
            return None
        return {
            "status": "success",
            "message": {
                # errors="replace": the ACK is already behind us, so a binary payload
                # must not raise here — the event would be gone with nothing to redeliver.
                "body": message.body.decode("utf-8", errors="replace"),
                "headers": dict(message.headers or {}),
                "routing_key": message.routing_key or "",
                "exchange": message.exchange or "",
            },
        }

    async def _run_push(
        self,
        queue: Any,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any] | None:
        """Push mode: subscribe via basic_consume and wait for a matching message."""

        async def _consume() -> dict[str, Any] | None:
            async with queue.iterator() as q_iter:
                async for message in q_iter:
                    result = await self._handle_message(message, msg_filter, miss_state)
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
        # One state per run(): the run of misses spans every delivery this trigger
        # sees, in either mode, so the threshold is reached by a queue full of stray
        # messages and not only by a single batch of them.
        miss_state = FilterMissState(self.queue_name, log, ack_unmatched=self.ack_unmatched)

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
                    result = await self._run_push(queue, msg_filter, miss_state)
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

                    result = await self._handle_message(message, msg_filter, miss_state)
                    if result is not None:
                        yield TriggerEvent(result)
                        return
                    # A rejected message is disposed of inside match_and_ack. With
                    # ack_unmatched off it goes back to the queue and the 0.1 s pause
                    # there paces the redelivery loop. With it on the message is
                    # acknowledged and the loop turns straight away, so turns come
                    # faster than they do with the pause — and every one of them takes
                    # a message out of the queue, so the loop is bounded by the queue
                    # depth and ends, rather than running until the message TTL does.

        except Exception as e:
            log.exception("RMQTrigger encountered an error")
            yield TriggerEvent({"status": "error", "error": str(e)})
