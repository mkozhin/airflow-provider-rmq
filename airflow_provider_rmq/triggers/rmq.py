from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, AsyncIterator, Literal

import aiormq
from aio_pika import IncomingMessage
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import AMQPError, ChannelInvalidStateError
from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from pamqp import commands as spec

from airflow_provider_rmq.utils.amqp import (
    AmqpTimeouts,
    FilterMissState,
    abandon_call,
    build_amqp_connection,
    close_quietly,
    get_amqp_timeouts,
    match,
    match_and_ack,
)
from airflow_provider_rmq.utils.filters import FilterVerdict, MessageFilter

log = logging.getLogger("airflow.task")


class _StepTimeout(Exception):
    """A step of a connection attempt that did not complete within its bound.

    The class is the trigger's own and not :exc:`asyncio.TimeoutError`: from Python 3.11
    that is the built-in :exc:`TimeoutError`, a subclass of :exc:`OSError`, and a step
    that ran out of time would read the same as a refused connection.

    :param step: Name of the step.
    :param timeout: Seconds the step was given.
    """

    def __init__(self, step: str, timeout: float):
        super().__init__(f"{step} did not complete within {timeout:g}s")
        self.step = step
        self.timeout = timeout


class _ConnectionLost(Exception):
    """The connection or the channel of an attempt closed under a running step.

    :param step: Name of the step that was running.
    :param reason: Exception the connection or the channel was closed with, read off
        its ``closing`` future. ``aiormq`` closes a connection whose heartbeat stopped
        with a bare :exc:`asyncio.CancelledError` and one the peer hung up on with a
        bare :exc:`Exception`; a broker that closed the channel itself leaves its reply
        here (``ChannelNotFoundEntity`` for a 404, and so on). ``None`` when the step
        ended by cancellation while ``closing`` was still pending.
    """

    def __init__(self, step: str, reason: BaseException | None):
        super().__init__(f"connection lost during {step}: {_describe(reason)}")
        self.step = step
        self.reason = reason


class _ConsumerCancelled(Exception):
    """The broker cancelled the push consumer: its tag left ``channel.consumers``.

    RabbitMQ sends ``basic.cancel`` when the queue is deleted, when the leader of a
    quorum queue moves, or when the node holding the queue restarts.
    """


def _describe(reason: BaseException | None) -> str:
    """Name a closing reason for a log line: type and text, or just the type."""
    if reason is None:
        return "no reason given"
    text = str(reason)
    return f"{type(reason).__name__}: {text}" if text else type(reason).__name__


def _closing_reason(closing: asyncio.Future) -> BaseException | None:
    """Read the reason off a finished ``closing`` future without raising it.

    ``aiormq`` finishes ``closing`` with the close reason set as its exception, and that
    reason is a :exc:`asyncio.CancelledError` when the heartbeat stopped: ``result()``
    would raise it into the caller as if the caller itself were being cancelled.
    """
    if closing.cancelled():
        return asyncio.CancelledError()
    return closing.exception()


def _drop(task: asyncio.Future) -> None:
    """Cancel ``task`` unless it has finished, and stop watching it."""
    if not task.done():
        task.cancel()
    abandon_call(task)


async def _step(
    awaitable: Any,
    timeout: float,
    name: str,
    closing: asyncio.Future | None = None,
) -> Any:
    """Await one step of a connection attempt within ``timeout`` seconds.

    :param awaitable: The broker call.
    :param timeout: Seconds the step is given.
    :param name: Name of the step, for the exception and the log.
    :param closing: ``closing`` future of the attempt's connection, raced against the
        call. ``None`` for the connect step: a failed handshake finishes ``closing``
        before ``connect()`` returns its own exception, and the race would turn that
        exception into a lost connection.
    :returns: Whatever the call returns.
    :raises _StepTimeout: Neither the call nor ``closing`` finished in time.
    :raises _ConnectionLost: ``closing`` finished first, or the call ended by
        cancellation — the way ``aiormq`` ends a call whose connection went away.
    :raises asyncio.CancelledError: The caller was cancelled.

    A call that is given up on is cancelled and left to finish on its own
    (:func:`~airflow_provider_rmq.utils.amqp.abandon_call`): cancelling an ``aiormq``
    RPC writes a ``channel.close`` to the very socket the call may be stuck on.

    A :exc:`asyncio.CancelledError` out of the wait itself is always the caller's:
    :func:`asyncio.wait` does not pass on what the watched futures ended with. So a
    cancellation of ``run()`` and a lost connection are told apart by where they
    arrive, not by the state of ``closing``, which stays finished for the rest of the
    attempt once the connection is gone.
    """
    task = asyncio.ensure_future(awaitable)
    watched = {task} if closing is None else {task, closing}
    try:
        await asyncio.wait(watched, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    except asyncio.CancelledError:
        _drop(task)
        raise
    if task.done() and not task.cancelled() and task.exception() is None:
        return task.result()
    if closing is not None and closing.done():
        # The call may have failed already, but only as the consequence of the closed
        # connection: the reason worth reporting is the one the connection closed with.
        cause = task.exception() if task.done() and not task.cancelled() else None
        _drop(task)
        raise _ConnectionLost(name, _closing_reason(closing)) from cause
    if task.done():
        if task.cancelled():
            raise _ConnectionLost(name, None)
        return task.result()
    _drop(task)
    raise _StepTimeout(name, timeout)


class RMQTrigger(BaseTrigger):
    """Async trigger that waits for a matching message in a RabbitMQ queue.

    The trigger opens its own ``aiormq`` connection and channel, and every broker call
    on them is bounded in time: the handshake by ``connect_timeout``, every other call
    by ``rpc_timeout`` from the connection's ``extra``. Supports two delivery modes:

    - ``mode="pull"`` (default): polls the queue with ``basic_get``, pausing
      ``poll_interval`` seconds whenever the queue is empty.
    - ``mode="push"``: subscribes with ``basic_consume``. The broker delivers messages
      as they arrive — no polling delay. Use ``message_wait_timeout`` to limit how long
      the trigger waits for a message.

    A matching message is acknowledged before the success event is fired. When that
    acknowledgement cannot be confirmed — it timed out, or the connection went away
    under it — the event is fired all the same with a WARNING: the frame may have
    reached the broker, so the message may come back as a duplicate, but it is never
    lost.

    Note: callable filters are NOT supported here (cannot be serialized).

    Note:
        With ``ack_unmatched`` off, a non-matching message is NACKed with
        requeue=True (``basic.nack``). A quorum queue's ``delivery-limit`` counts only
        ``basic.reject`` and failed deliveries, so a non-matching message goes back to
        the head of the queue and is evaluated again, indefinitely. If heavy filtering
        is expected, use a dedicated queue with ``ack_unmatched=True``: the message is
        then acknowledged instead and never redelivered.
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
        :param mode: Delivery mode — ``"pull"`` (periodic ``basic_get``) or ``"push"``
            (broker-pushed via ``basic_consume``).
        :param message_wait_timeout: Maximum seconds to wait for a matching message in push
            mode, counted from the start of ``run()``. ``None`` means wait indefinitely
            (until the sensor-level timeout fires). The actual wait may exceed this value
            by the time it takes to close the connection.
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


    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Wait for a matching message and fire one event.

        The event is ``{"status": "success", "message": {...}}`` for a matching
        message, ``{"status": "timeout"}`` when ``message_wait_timeout`` ran out in push
        mode, and ``{"status": "error", "error": ...}`` when the attempt failed.
        """
        loop = asyncio.get_running_loop()
        deadline = None
        if self.mode == "push" and self.message_wait_timeout is not None:
            deadline = loop.time() + self.message_wait_timeout
        msg_filter = MessageFilter.deserialize(self.filter_data)
        # One state per run(): the run of misses spans every delivery this trigger
        # sees, in either mode, so the threshold is reached by a queue full of stray
        # messages and not only by a single batch of them.
        miss_state = FilterMissState(self.queue_name, log, ack_unmatched=self.ack_unmatched)

        try:
            conn_info = await loop.run_in_executor(None, BaseHook.get_connection, self.rmq_conn_id)
            url, ssl_context = build_amqp_connection(conn_info)
            timeouts = get_amqp_timeouts(conn_info)
            result = await self._attempt(url, ssl_context, timeouts, deadline, msg_filter, miss_state)
        except Exception as e:
            log.exception("RMQTrigger encountered an error")
            yield TriggerEvent({"status": "error", "error": str(e)})
            return
        yield TriggerEvent(result)

    async def _attempt(
        self,
        url: str,
        ssl_context: Any,
        timeouts: AmqpTimeouts,
        deadline: float | None,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any]:
        """Run one attempt on a connection and a channel of its own.

        The connection object is created before the first ``await``, so the ``finally``
        closes it whatever ends the attempt — a result, a failure, or a cancellation of
        ``run()`` by the triggerer. Closing the connection closes its channel, and the
        broker puts back every delivery the attempt had not acknowledged.
        """
        connection = aiormq.Connection(url, context=ssl_context)
        try:
            await _step(connection.connect(), timeouts.connect, "connect")
            closing = connection.closing
            channel = await _step(
                connection.channel(publisher_confirms=False), timeouts.rpc, "channel", closing
            )
            await _step(
                channel.queue_declare(self.queue_name, passive=True), timeouts.rpc, "declare", closing
            )
            if self.mode == "push":
                return await self._consume(channel, closing, timeouts, deadline, msg_filter, miss_state)
            return await self._poll(channel, closing, timeouts, msg_filter, miss_state)
        finally:
            await close_quietly(connection, f"RMQTrigger connection for queue {self.queue_name!r}", log)

    async def _poll(
        self,
        channel: Any,
        closing: asyncio.Future,
        timeouts: AmqpTimeouts,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any]:
        """Pull mode: ``basic_get`` until a message matches, pausing on an empty queue."""
        while True:
            delivered = await _step(
                channel.basic_get(self.queue_name, no_ack=False), timeouts.rpc, "get", closing
            )
            if isinstance(delivered.delivery, spec.Basic.GetEmpty):
                await asyncio.sleep(self.poll_interval)
                continue
            result = await self._handle(delivered, closing, timeouts, msg_filter, miss_state)
            if result is not None:
                return result
            # A rejected message is disposed of inside match_and_ack. With
            # ack_unmatched off it goes back to the queue and the 0.1 s pause
            # there paces the redelivery loop. With it on the message is
            # acknowledged and the loop turns straight away, so turns come
            # faster than they do with the pause — and every one of them takes
            # a message out of the queue, so the loop is bounded by the queue
            # depth and ends, rather than running until the message TTL does.

    async def _consume(
        self,
        channel: Any,
        closing: asyncio.Future,
        timeouts: AmqpTimeouts,
        deadline: float | None,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any]:
        """Push mode: ``basic_consume`` into a local queue and take deliveries from it.

        The consumer callback only puts a delivery into the local queue; the filter, the
        acknowledgement and every bound run here. The wait is watched in turns of at most
        ``rpc_timeout`` seconds: a delivery is handled, a closed channel ends the
        attempt, and a turn that brought neither looks for the consumer tag in
        ``channel.consumers`` — ``aiormq`` drops the tag when the broker cancels the
        consumer, and nothing else puts a consumer back on this channel.
        """
        loop = asyncio.get_running_loop()
        deliveries: asyncio.Queue[DeliveredMessage] = asyncio.Queue()

        async def _on_delivery(delivered: DeliveredMessage) -> None:
            deliveries.put_nowait(delivered)

        tag = f"rmq-trigger.{uuid.uuid4().hex}"
        await _step(
            channel.basic_consume(self.queue_name, _on_delivery, consumer_tag=tag),
            timeouts.rpc,
            "consume",
            closing,
        )
        while True:
            if not deliveries.empty():
                result = await self._handle(
                    deliveries.get_nowait(), closing, timeouts, msg_filter, miss_state
                )
                if result is not None:
                    return result
                continue
            wait = timeouts.rpc
            if deadline is not None:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    return {"status": "timeout"}
                wait = min(wait, remaining)
            getter = asyncio.ensure_future(deliveries.get())
            try:
                await asyncio.wait(
                    {getter, channel.closing}, timeout=wait, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                if not getter.done():
                    getter.cancel()
            if getter.done() and not getter.cancelled():
                result = await self._handle(getter.result(), closing, timeouts, msg_filter, miss_state)
                if result is not None:
                    return result
                continue
            if channel.closing.done():
                raise _ConnectionLost("wait", _closing_reason(channel.closing))
            if tag not in channel.consumers:
                raise _ConsumerCancelled(
                    f"consumer on queue {self.queue_name!r} was cancelled by the broker"
                )

    async def _handle(
        self,
        delivered: DeliveredMessage,
        closing: asyncio.Future,
        timeouts: AmqpTimeouts,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any] | None:
        """Dispose of one delivery and return the success payload if it matched.

        :param delivered: Delivery from ``basic_get`` or ``basic_consume``.
        :param closing: ``closing`` future of the attempt's connection.
        :param timeouts: Bounds of the attempt; the broker call runs under ``rpc``.
        :param msg_filter: Pre-built filter the message is evaluated against.
        :param miss_state: Counters of this ``run()``; its ``ack_unmatched`` decides
            whether a rejected message is acknowledged or returned to the queue. A
            message the filter could not be evaluated on goes back to the queue either
            way.

        The verdict is reached once, before the broker call, because it decides what a
        failed call means. ``basic.ack`` has no reply: a frame that left for the socket
        before the connection broke may have reached the broker, which then deleted
        the message, and one still queued for writing is dropped with the connection,
        which puts the message back. A failed acknowledgement of a matching message
        therefore still fires the success event, with a WARNING — a duplicate is
        possible, a lost message is not. A failed broker call on any other verdict is
        raised to the caller.
        """
        message = IncomingMessage(delivered)
        verdict = match(message, msg_filter)
        try:
            matched = await _step(
                match_and_ack(message, msg_filter, miss_state, verdict=verdict),
                timeouts.rpc,
                "ack",
                closing,
            )
        except (_StepTimeout, _ConnectionLost, ChannelInvalidStateError, ConnectionError, AMQPError) as exc:
            if verdict[0] is not FilterVerdict.MATCH:
                raise
            log.warning(
                "RMQTrigger: ACK not confirmed for the matching message on queue %r "
                "(conn_id=%r): %s; the message may be delivered again",
                self.queue_name, self.rmq_conn_id, exc,
            )
            matched = True
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
