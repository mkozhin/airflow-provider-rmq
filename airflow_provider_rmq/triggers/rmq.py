from __future__ import annotations

import asyncio
import logging
import re
import uuid
from collections import deque
from dataclasses import dataclass
from typing import Any, AsyncIterator, Literal, cast
from urllib.parse import urlencode

import aiormq
from aio_pika import IncomingMessage
from aiormq.abc import DeliveredMessage
from aiormq.exceptions import (
    AMQPError,
    AuthenticationError,
    ChannelAccessRefused,
    ChannelClosed,
    ChannelInvalidStateError,
    ChannelNotFoundEntity,
    ConnectionNotAllowed,
    ProbableAuthenticationError,
)
from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from pamqp import commands as spec
from pamqp.exceptions import AMQPInternalError

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
from airflow_provider_rmq.utils.backoff import next_backoff
from airflow_provider_rmq.utils.filters import FilterVerdict, MessageFilter

log = logging.getLogger("airflow.task")

#: Seconds of the first pause between attempts, and of the pause that follows an
#: attempt which got as far as waiting for a message.
RETRY_PAUSE_MIN = 15.0
#: Seconds the pause between attempts doubles up to and then stays at.
RETRY_PAUSE_MAX = 60.0

# The 404 RabbitMQ answers with while the node holding a durable classic queue restarts:
# the queue is still there, its node is not, and it comes back.
_HOME_NODE_DOWN = re.compile(r"home node .* is down or inaccessible")
# The reply text of the 530 refusing the user access to a vhost on ``Connection.Open``:
# "access to vhost 'v' refused for user 'u'". The 530 of a vhost connection limit
# starts the same way and goes on with ": connection limit (N) is reached" — that one
# passes once connections are released, so the pattern is anchored at the end.
_VHOST_REFUSED = re.compile(r"access to vhost '.*' refused for user '[^']*'$")


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


class _DeadlineReached(Exception):
    """``message_wait_timeout`` ran out before the next step or pause."""


@dataclass
class _Attempt:
    """Where one attempt is: the retry loop reads it once the attempt has ended.

    :param number: Attempt number within ``run()``, from 1.
    :param deadline: Loop time ``message_wait_timeout`` runs out at, or ``None``.
    :param step: Name of the step the attempt is in or last ran.
    :param waiting: Whether the attempt got as far as waiting for a message.
    """

    number: int
    deadline: float | None
    step: str = "connect"
    waiting: bool = False

    def bound(self, seconds: float, step: str) -> float:
        """Enter ``step`` and return its bound: ``seconds``, cut to what the deadline leaves.

        :raises _DeadlineReached: The deadline has passed.
        """
        self.step = step
        return _within(seconds, self.deadline)


def _within(seconds: float, deadline: float | None) -> float:
    """``seconds``, or what is left until ``deadline`` if that is less.

    :raises _DeadlineReached: Nothing is left until ``deadline``.
    """
    if deadline is None:
        return seconds
    remaining = deadline - asyncio.get_running_loop().time()
    if remaining <= 0:
        raise _DeadlineReached
    return min(seconds, remaining)


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


def _named(url: str, name: str) -> str:
    """Add ``name`` to the connection URL: ``aiormq`` hands it to the broker as
    ``connection_name``, which the Management UI lists the connection under."""
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}{urlencode({'name': name})}"


def _refused(exc: BaseException) -> bool:
    """Whether the broker refused the trigger for good: no attempt can get further.

    A wrong or changed password, no access to the vhost, no access to the queue, and a
    queue that is not there. The 404 of a durable classic queue whose node is restarting
    is not one of them: the queue comes back with its node.

    The reply text of an ``aiormq`` exception is its ``str()``; there is no attribute
    of its own for it.
    """
    if isinstance(
        exc, (ProbableAuthenticationError, AuthenticationError, ConnectionNotAllowed, ChannelAccessRefused)
    ):
        return True
    if isinstance(exc, ChannelNotFoundEntity):
        return not _HOME_NODE_DOWN.search(str(exc))
    if isinstance(exc, AMQPInternalError):
        # aiormq raises it with the frame it got instead of Connection.OpenOk.
        frame = exc.args[1] if len(exc.args) > 1 else None
        if isinstance(frame, spec.Connection.Close):
            if frame.reply_code == 403:
                return True
            return frame.reply_code == 530 and bool(_VHOST_REFUSED.search(frame.reply_text or ""))
    return False


def _retryable(exc: BaseException, attempt: _Attempt) -> bool:
    """Whether a failed attempt is followed by another one, or ends ``run()`` with an error.

    In the order of the checks:

    1. A step out of time is retried.
    2. A refusal (:func:`_refused`) is an error on any attempt — also when it is the
       reason a connection or a channel was closed with.
    3. A connect step that was refused or cut off — the socket refused or reset, the
       handshake ending in EOF, a ``Connection.Close`` in the middle of it — is an error
       on the first attempt and retried on every later one: a broker that took the
       first attempt and is now restarting looks exactly like that.
    4. A lost connection or channel, a consumer the broker cancelled, and a call on a
       connection or a channel that has closed are retried.
    5. Anything else is an error.
    """
    if isinstance(exc, _StepTimeout):
        return True
    if isinstance(exc, _ConnectionLost):
        return exc.reason is None or not _refused(exc.reason)
    if _refused(exc):
        return False
    if attempt.step == "connect" and isinstance(exc, (OSError, EOFError, StopAsyncIteration, AMQPInternalError)):
        return attempt.number > 1
    return isinstance(exc, (_ConsumerCancelled, ConnectionError, ChannelClosed, ChannelInvalidStateError))


def _failure(exc: BaseException, attempt: _Attempt) -> str:
    """Describe a retried failure for the WARNING: the step and what went wrong."""
    if isinstance(exc, (_StepTimeout, _ConnectionLost, _ConsumerCancelled)):
        return str(exc)
    return f"{attempt.step} failed: {_describe(exc)}"


async def _pause(seconds: float) -> None:
    """Wait between two attempts. A cancellation here is always the caller's."""
    await asyncio.sleep(seconds)


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

    Supports two delivery modes:

    - ``mode="pull"`` (default): polls the queue with ``basic_get``, pausing
      ``poll_interval`` seconds whenever the queue is empty.
    - ``mode="push"``: subscribes with ``basic_consume``. The broker delivers messages
      as they arrive — no polling delay. Use ``message_wait_timeout`` to limit how long
      the trigger waits for a message.

    **Attempts.** The trigger talks to the broker over ``aiormq`` directly. Every attempt
    opens a connection and a channel of its own — the connection is named
    ``rmq-trigger:<queue>`` in the Management UI — and closes the connection whatever
    ends it. Every broker call is bounded in time: the handshake by ``connect_timeout``,
    opening the channel, the passive declare of the queue, ``basic_get``,
    ``basic_consume`` and every acknowledgement by ``rpc_timeout``, both from the
    connection's ``extra``. In push mode the wait for a delivery is watched in turns of
    at most ``rpc_timeout`` seconds, each of which also checks the channel and the
    consumer.

    **Retries.** A step out of time, a lost connection or channel (heartbeat, a broker
    restart, a reset socket) and a consumer cancelled by the broker (the queue deleted
    and declared again, a quorum leader moving, a node restarting) end the attempt with
    a WARNING in the task log; the next attempt follows after a pause of 15, 30, then 60
    seconds for every one after that, which starts over at 15 once an attempt has
    reached the wait for a message. The task stays deferred meanwhile and carries on by
    itself once the broker is back. A refused or cut-off handshake is an error on the
    first attempt — a wrong host or port, a broker that is not there — and retried on
    later ones. An error event ends ``run()`` on any attempt for a wrong password, no
    access to the vhost, a 403 or a 404 on the queue (except the 404 of a durable
    queue whose node is restarting, which is retried), and for any other exception.

    **Acknowledgement.** A matching message is acknowledged before the success event is
    fired. When that acknowledgement cannot be confirmed — it timed out, or the
    connection went away under it — the event is fired all the same with a WARNING:
    the frame may have reached the broker, so the message may come back as a
    duplicate, but it is never lost.

    **Leaving the push consumer.** On a match and on ``message_wait_timeout`` the
    consumer is cancelled and every delivery the broker had already sent and the
    trigger had not looked at is NACKed back with ``requeue=True`` before the
    connection closes: a quorum queue counts a delivery dropped with its connection
    against ``delivery-limit``, a NACK it does not. Both get one ``rpc_timeout`` between
    them; if they fail, the connection is closed all the same and the broker puts the
    deliveries back itself.

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
        :param poll_interval: Seconds between ``basic_get`` calls when the queue is empty
            (pull mode only).
        :param mode: Delivery mode — ``"pull"`` (``basic_get`` in a loop) or ``"push"``
            (``basic_consume``, the broker sends messages as they arrive).
        :param message_wait_timeout: Maximum seconds to wait for a matching message in push
            mode, counted from the start of ``run()`` across every attempt and every
            pause between them; ``{"status": "timeout"}`` is fired when it runs out.
            ``None`` means wait indefinitely (until the sensor-level timeout fires). The
            deadline is checked before every step, and every step and pause is cut to
            what it leaves. It may be overrun by at most ``rpc_timeout`` plus
            :data:`~airflow_provider_rmq.utils.amqp.CLOSE_TIMEOUT` seconds: returning
            the unread deliveries to the queue and closing the connection.
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
        mode, and ``{"status": "error", "error": ...}`` when an attempt failed in a way
        that is not retried (see the class docstring). Every other failure is logged as
        a WARNING — the step or the reason, the ``conn_id``, the queue, the attempt
        number and the pause before the next one — and the next attempt follows.
        """
        loop = asyncio.get_running_loop()
        deadline = None
        if self.mode == "push" and self.message_wait_timeout is not None:
            deadline = loop.time() + self.message_wait_timeout
        msg_filter = MessageFilter.deserialize(self.filter_data)
        # One state per run(): the run of misses spans every delivery this trigger
        # sees, in either mode and across attempts, so the threshold is reached by a
        # queue full of stray messages and not only by a single batch of them.
        miss_state = FilterMissState(self.queue_name, log, ack_unmatched=self.ack_unmatched)

        try:
            conn_info = await loop.run_in_executor(None, BaseHook.get_connection, self.rmq_conn_id)
            url, ssl_context = build_amqp_connection(conn_info)
            url = _named(url, f"rmq-trigger:{self.queue_name}")
            timeouts = get_amqp_timeouts(conn_info)
        except Exception as e:
            log.exception("RMQTrigger encountered an error")
            yield TriggerEvent({"status": "error", "error": str(e)})
            return

        pause = 0.0
        number = 0
        while True:
            number += 1
            attempt = _Attempt(number, deadline)
            try:
                result = await self._attempt(url, ssl_context, timeouts, attempt, msg_filter, miss_state)
            except _DeadlineReached:
                result = {"status": "timeout"}
            except Exception as exc:
                if not _retryable(exc, attempt):
                    log.exception("RMQTrigger encountered an error")
                    yield TriggerEvent({"status": "error", "error": str(exc)})
                    return
                pause = next_backoff(0.0 if attempt.waiting else pause, RETRY_PAUSE_MAX, RETRY_PAUSE_MIN)
                try:
                    wait = _within(pause, deadline)
                except _DeadlineReached:
                    yield TriggerEvent({"status": "timeout"})
                    return
                log.warning(
                    "RMQTrigger: %s on conn_id=%r (attempt %d); retrying queue %r in %gs",
                    _failure(exc, attempt), self.rmq_conn_id, number, self.queue_name, wait,
                )
                await _pause(wait)
                continue
            yield TriggerEvent(result)
            return

    async def _attempt(
        self,
        url: str,
        ssl_context: Any,
        timeouts: AmqpTimeouts,
        attempt: _Attempt,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any]:
        """Run one attempt on a connection and a channel of its own.

        The connection object is created before the first ``await``, so the ``finally``
        closes it whatever ends the attempt — a result, a failure, or a cancellation of
        ``run()`` by the triggerer. Closing the connection closes its channel, and the
        broker puts back every delivery the attempt had not acknowledged.

        :raises _DeadlineReached: ``message_wait_timeout`` ran out before a step.
        """
        connection = aiormq.Connection(url, context=ssl_context)
        try:
            # Every bound is taken before its call is made: an expired deadline must
            # stop the call from starting at all.
            bound = attempt.bound(timeouts.connect, "connect")
            await _step(connection.connect(), bound, "connect")
            closing = connection.closing
            bound = attempt.bound(timeouts.rpc, "channel")
            channel = await _step(connection.channel(publisher_confirms=False), bound, "channel", closing)
            bound = attempt.bound(timeouts.rpc, "declare")
            await _step(channel.queue_declare(self.queue_name, passive=True), bound, "declare", closing)
            if self.mode == "push":
                return await self._consume(channel, closing, timeouts, attempt, msg_filter, miss_state)
            return await self._poll(channel, closing, timeouts, attempt, msg_filter, miss_state)
        except RuntimeError as exc:
            # A call on a connection that has closed under it; ChannelInvalidStateError,
            # one of these, is retried as it is.
            if connection.is_closed and not isinstance(exc, ChannelInvalidStateError):
                raise _ConnectionLost(attempt.step, _closing_reason(connection.closing)) from exc
            raise
        finally:
            await close_quietly(connection, f"RMQTrigger connection for queue {self.queue_name!r}", log)

    async def _poll(
        self,
        channel: Any,
        closing: asyncio.Future,
        timeouts: AmqpTimeouts,
        attempt: _Attempt,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any]:
        """Pull mode: ``basic_get`` until a message matches, pausing on an empty queue."""
        attempt.waiting = True
        while True:
            bound = attempt.bound(timeouts.rpc, "get")
            delivered = await _step(channel.basic_get(self.queue_name, no_ack=False), bound, "get", closing)
            if isinstance(delivered.delivery, spec.Basic.GetEmpty):
                await asyncio.sleep(self.poll_interval)
                continue
            result = await self._handle(
                delivered, closing, attempt.bound(timeouts.rpc, "ack"), msg_filter, miss_state
            )
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
        attempt: _Attempt,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any]:
        """Push mode: ``basic_consume`` into a local buffer and take deliveries from it.

        The consumer callback only appends a delivery to the buffer; the filter, the
        acknowledgement and every bound run here. A delivery is taken off the buffer
        only once the deadline has been checked, so one that is not handled is still
        there to be given back. The wait is watched in turns of at most ``rpc_timeout``
        seconds: a delivery is handled, a closed channel ends the attempt, and a turn
        that brought neither looks for the consumer tag in ``channel.consumers`` —
        ``aiormq`` drops the tag when the broker cancels the consumer, and nothing else
        puts a consumer back on this channel.

        A match and an expired deadline end the attempt through :meth:`_give_back`.
        """
        buffer: deque[DeliveredMessage] = deque()
        arrived = asyncio.Event()

        async def _on_delivery(delivered: DeliveredMessage) -> None:
            buffer.append(delivered)
            arrived.set()

        tag = f"rmq-trigger.{uuid.uuid4().hex}"
        bound = attempt.bound(timeouts.rpc, "consume")
        await _step(
            channel.basic_consume(self.queue_name, _on_delivery, consumer_tag=tag), bound, "consume", closing
        )
        attempt.waiting = True
        try:
            while True:
                if buffer:
                    bound = attempt.bound(timeouts.rpc, "ack")
                    result = await self._handle(buffer.popleft(), closing, bound, msg_filter, miss_state)
                    if result is not None:
                        await self._give_back(channel, closing, tag, buffer, timeouts.rpc)
                        return result
                    continue
                bound = attempt.bound(timeouts.rpc, "wait")
                arrived.clear()
                waiter = asyncio.ensure_future(arrived.wait())
                try:
                    await asyncio.wait(
                        {waiter, channel.closing}, timeout=bound, return_when=asyncio.FIRST_COMPLETED
                    )
                finally:
                    if not waiter.done():
                        waiter.cancel()
                if buffer:
                    continue
                if channel.closing.done():
                    raise _ConnectionLost("wait", _closing_reason(channel.closing))
                if tag not in channel.consumers:
                    raise _ConsumerCancelled(
                        f"consumer on queue {self.queue_name!r} was cancelled by the broker"
                    )
        except _DeadlineReached:
            await self._give_back(channel, closing, tag, buffer, timeouts.rpc)
            raise

    async def _give_back(
        self,
        channel: Any,
        closing: asyncio.Future,
        tag: str,
        buffer: deque[DeliveredMessage],
        rpc: float,
    ) -> None:
        """Cancel the consumer and NACK the unread deliveries back to the queue.

        Cancelling first stops new deliveries; the ones the broker sent before its
        ``basic.cancel-ok`` land in the buffer and are NACKed with the rest. The
        cancellation and the NACKs share one ``rpc`` bound: the deadline has been met or
        the result is in hand, and this is what may overrun the deadline. A failure
        changes nothing about the outcome — the connection is closed next, and the
        broker puts back whatever is still unacknowledged.
        """
        loop = asyncio.get_running_loop()
        until = loop.time() + rpc
        try:
            await _step(channel.basic_cancel(tag), rpc, "cancel", closing)
            # Deliveries that arrived before cancel-ok run their callbacks as tasks.
            await asyncio.sleep(0)
            while buffer:
                left = until - loop.time()
                if left <= 0:
                    raise _StepTimeout("nack", rpc)
                # Every delivery of a consumer is a Basic.Deliver.
                delivery = cast(spec.Basic.Deliver, buffer[0].delivery)
                await _step(channel.basic_nack(delivery.delivery_tag, requeue=True), left, "nack", closing)
                buffer.popleft()
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "RMQTrigger: returning %d unread deliveries to queue %r failed: %s; "
                "closing the connection returns them",
                len(buffer), self.queue_name, exc,
            )

    async def _handle(
        self,
        delivered: DeliveredMessage,
        closing: asyncio.Future,
        timeout: float,
        msg_filter: MessageFilter,
        miss_state: FilterMissState,
    ) -> dict[str, Any] | None:
        """Dispose of one delivery and return the success payload if it matched.

        :param delivered: Delivery from ``basic_get`` or ``basic_consume``.
        :param closing: ``closing`` future of the attempt's connection.
        :param timeout: Seconds the broker call is given.
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
                timeout,
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

