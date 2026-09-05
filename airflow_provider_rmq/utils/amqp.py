from __future__ import annotations

import asyncio
import logging
import math
import ssl
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode

from airflow_provider_rmq.utils.filters import FilterVerdict, MessageFilter
from airflow_provider_rmq.utils.ssl import build_ssl_context

log = logging.getLogger(__name__)

AMQP_PORT = 5672
AMQPS_PORT = 5671

#: Seconds between AMQP heartbeat frames. ``aiormq`` allows three missed intervals
#: (``HEARTBEAT_GRACE_MULTIPLIER``) and checks every half-interval, so a broken link
#: surfaces as an exception in about ``3 * DEFAULT_HEARTBEAT`` seconds — some 90 s at
#: the default — and ``connect_robust`` reconnects.
DEFAULT_HEARTBEAT = 30
#: Seconds allowed for establishing an AMQP connection.
DEFAULT_CONNECT_TIMEOUT = 15
#: Seconds allowed for a single AMQP RPC: ``channel()``, declare, bind, publish.
DEFAULT_RPC_TIMEOUT = 30

#: ``extra`` keys that override the defaults above.
HEARTBEAT_KEY = "heartbeat"
CONNECT_TIMEOUT_KEY = "connect_timeout"
RPC_TIMEOUT_KEY = "rpc_timeout"

_MISSING = object()

#: Smallest timeout a connection may ask for. Anything at or below zero would make
#: every call fail before it started.
_MIN_TIMEOUT = 1e-9


@dataclass(frozen=True)
class AmqpTimeouts:
    """Timeouts applied to asynchronous AMQP calls.

    :param connect: Seconds allowed for ``connect_robust``.
    :param rpc: Seconds allowed for a single AMQP RPC.
    """

    connect: float
    rpc: float


def _read_number(
    extras: dict[str, Any],
    key: str,
    default: Any,
    cast: Any,
    minimum: Any,
) -> Any:
    """Read a number from ``extra``, falling back to ``default``.

    A missing key uses the default silently; a present but unusable value
    (non-numeric, not finite, or below ``minimum``) uses the default and logs a WARNING.
    """
    raw = extras.get(key, _MISSING)
    if raw is _MISSING:
        return default
    try:
        value = cast(raw)
    except (TypeError, ValueError, OverflowError):
        # OverflowError belongs here: ``extra_dejson`` parses a 400-digit literal to an
        # int no float can hold, and the cast raises on it. Unhandled it leaves
        # ``build_amqp_connection`` altogether, past the caller that would have written
        # the reason into the connection row. A literal written as 1e400 is read as
        # ``inf``, which the cast takes and the finiteness check below turns down.
        log.warning("RMQ connection extra %r=%r is not a number, using %s", key, raw, default)
        return default
    if not math.isfinite(value):
        # ``float`` reads "inf" and "nan" happily, and both pass the comparison below:
        # an infinite timeout is a call with no bound at all, which is what every bound
        # here exists to prevent.
        log.warning(
            "RMQ connection extra %r=%r is not a finite number, using %s", key, raw, default
        )
        return default
    if value < minimum:
        log.warning(
            "RMQ connection extra %r=%r must be at least %s, using %s",
            key, raw, minimum, default,
        )
        return default
    return value


def _read_heartbeat(extras: dict[str, Any]) -> int:
    """Read the heartbeat interval in seconds from ``extra``.

    ``0`` is accepted as a deliberate opt-out and logged as a WARNING, because it
    turns off broken-link detection entirely. Unusable values fall back to
    :data:`DEFAULT_HEARTBEAT` with a WARNING.

    The interval is read as a number and only then made whole, so that a value between
    zero and one second falls back rather than truncating to zero: an interval too short
    to express is a mistake, and reading it as the opt-out would silently turn off the
    detection the caller was asking to tighten.
    """
    value = _read_number(extras, HEARTBEAT_KEY, DEFAULT_HEARTBEAT, float, 0)
    if 0 < value < 1:
        log.warning(
            "RMQ connection extra %r=%r is shorter than a second, using %s",
            HEARTBEAT_KEY, value, DEFAULT_HEARTBEAT,
        )
        return DEFAULT_HEARTBEAT
    value = int(value)
    if value == 0:
        log.warning(
            "RMQ connection extra %r=0 turns off the AMQP heartbeat: a broken link stays "
            "undetected and the consumer keeps waiting on a zombie connection",
            HEARTBEAT_KEY,
        )
    return value


def get_amqp_timeouts(conn_info: Any) -> AmqpTimeouts:
    """Build call timeouts from an Airflow connection.

    :param conn_info: Airflow Connection object (``airflow.models.Connection``).
    :returns: :class:`AmqpTimeouts` with ``extra`` overrides applied.

    Timeouts come back separately from the URL because they parameterise the
    calls, not the connection string.
    """
    extras = conn_info.extra_dejson
    return AmqpTimeouts(
        connect=_read_number(
            extras, CONNECT_TIMEOUT_KEY, DEFAULT_CONNECT_TIMEOUT, float, _MIN_TIMEOUT
        ),
        rpc=_read_number(extras, RPC_TIMEOUT_KEY, DEFAULT_RPC_TIMEOUT, float, _MIN_TIMEOUT),
    )


def _abandon_call(future: Any) -> None:
    """Stop watching ``future``, whenever it may end.

    A cancelled call takes as long as it takes to notice — an ``aiormq`` RPC answers a
    cancellation by closing its channel, which writes a frame to the socket that may be
    exactly what is stuck — so nothing waits for it and nothing cancels it a second
    time. Its outcome is read once it lands only so a failure of an abandoned call is
    not reported as an exception nobody retrieved.
    """
    future.add_done_callback(lambda done: None if done.cancelled() else done.exception())


async def call_with_timeout(awaitable: Any, timeout: float) -> Any:
    """Await ``awaitable``, raising :exc:`asyncio.TimeoutError` after ``timeout`` seconds.

    :param awaitable: Coroutine or future to await; it is cancelled when the timeout hits.
    :param timeout: Seconds allowed for the call.

    ``timeout`` bounds the caller and nothing else: a call that has not returned by then
    is cancelled and left to finish on its own while the caller gets its
    :exc:`~asyncio.TimeoutError` straight away. Waiting for the cancellation to complete
    would put the bound back in the hands of the call being bounded — cancelling an
    ``aiormq`` RPC makes it close its channel, and a channel closes by writing a frame
    to the same socket the call is stuck on, so the recovery paths that exist to survive
    a silent broker (a bounded ``basic.consume``, a best-effort ``close()``) would hang
    on exactly the connection they are recovering from.

    Cancellation of the *caller* is passed on untouched, including when it lands in the
    same event-loop tick as the timeout. The caller waits on a private future that only
    its own cancellation can reach, so a ``CancelledError`` arriving there is always the
    caller's and is re-raised, while the timeout is read off the inner future instead.
    ``asyncio.wait_for`` on Python below 3.11 collapses the two cases and returns the
    inner result, and a plain ``await`` on the inner future collapses them the other way
    and reports a timeout — either way a consumer task would ignore ``stop()`` and
    reconcile would wait for it forever.
    """
    loop = asyncio.get_running_loop()
    future = asyncio.ensure_future(awaitable)
    waiter = loop.create_future()
    expired = False

    def _on_timeout() -> None:
        nonlocal expired
        if waiter.done():
            return
        expired = True
        # The call is cancelled before the caller is woken, so a call that answers its
        # cancellation at once still gets that tick to do it in.
        future.cancel()
        waiter.set_result(None)

    def _on_done(_future: Any) -> None:
        if not waiter.done():
            waiter.set_result(None)

    timer = loop.call_later(timeout, _on_timeout)
    future.add_done_callback(_on_done)
    try:
        await waiter
    except asyncio.CancelledError:
        future.remove_done_callback(_on_done)
        future.cancel()
        _abandon_call(future)
        raise
    finally:
        timer.cancel()

    future.remove_done_callback(_on_done)
    if not future.done():
        # Already cancelled by the timer; it is left to run its cancellation out.
        _abandon_call(future)
        raise asyncio.TimeoutError()

    try:
        return future.result()
    except asyncio.CancelledError:
        if expired:
            raise asyncio.TimeoutError() from None
        raise


class _PropsShim:
    """Shim to bridge aio_pika message headers to MessageFilter's HasHeaders protocol."""

    __slots__ = ("headers",)

    def __init__(self, headers: dict[str, Any]):
        self.headers = headers


def build_amqp_connection(
    conn_info: Any,
    vhost_override: str | None = None,
) -> tuple[str, ssl.SSLContext | None]:
    """Build aio_pika-compatible AMQP URL and SSL context from Airflow connection.

    :param conn_info: Airflow Connection object (``airflow.models.Connection``).
    :param vhost_override: Optional vhost to use instead of ``conn_info.schema``.
    :returns: ``(url, ssl_context)`` — pass ``ssl_context`` to ``connect_robust()`` if not None.

    The URL carries a ``heartbeat`` query parameter taken from the ``heartbeat``
    key of ``extra`` (the same key the synchronous hook reads) or from
    :data:`DEFAULT_HEARTBEAT`. Heartbeats are what turn a broken link into an
    exception, which is what lets ``connect_robust`` reconnect.
    """
    extras = conn_info.extra_dejson
    ssl_context = build_ssl_context(extras)
    vhost = vhost_override or conn_info.schema or "/"
    port = conn_info.port if conn_info.port else (AMQPS_PORT if ssl_context else AMQP_PORT)
    query = urlencode({HEARTBEAT_KEY: _read_heartbeat(extras)})
    url = (
        f"{'amqps' if ssl_context else 'amqp'}://"
        f"{quote(conn_info.login or 'guest', safe='')}:{quote(conn_info.password or 'guest', safe='')}"
        f"@{conn_info.host or 'localhost'}:{port}/{quote(vhost, safe='')}?{query}"
    )
    return url, ssl_context


def match(message: Any, msg_filter: MessageFilter) -> tuple[FilterVerdict, str | None]:
    """Evaluate message body and headers against filter without any ACK/NACK side-effects.

    :param message: An ``aio_pika`` message (must have ``.body`` and ``.headers``).
    :param msg_filter: Pre-built :class:`~airflow_provider_rmq.utils.filters.MessageFilter`.
    :returns: ``(verdict, reason)`` as returned by
        :meth:`~airflow_provider_rmq.utils.filters.MessageFilter.evaluate` — a
        :class:`~airflow_provider_rmq.utils.filters.FilterVerdict` and, for ``ERROR``,
        the type and text of the exception behind it.

    Non-UTF-8 bytes in the body are replaced with the Unicode replacement character
    (``errors="replace"``) so binary payloads never raise :exc:`UnicodeDecodeError`.
    """
    body_str = message.body.decode("utf-8", errors="replace")
    props = _PropsShim(dict(message.headers or {}))
    return msg_filter.evaluate(props, body_str)


#: Smallest ``streak`` and ``dropped`` value worth a WARNING. Missing the filter is a
#: normal mode of operation — a line per stray message would cost one log record per
#: delivery — so the thresholds start here and then follow the powers of ten.
_MISS_WARN_FLOOR = 100
#: Outcomes without a single failure to evaluate that clear ``error_streak``.
_ERROR_STREAK_RESET_RUN = 100


def _is_threshold(value: int) -> bool:
    """Tell whether ``value`` is a power of ten at or above :data:`_MISS_WARN_FLOOR`."""
    if value < _MISS_WARN_FLOOR:
        return False
    while value % 10 == 0:
        value //= 10
    return value == 1


class FilterMissState:
    """Counters behind the log lines a filter miss produces on one consuming loop.

    The object holds nothing about any single message: no keys, no broker calls, no
    coroutines, so the same class serves the asynchronous and the synchronous paths.
    Its lifetime is the owner's — a consumer task in the watcher, one ``run()`` in the
    trigger, one task run in the operator, the sensor instance across its ``poke`` calls.

    :param queue_name: Queue the counters belong to; it names the queue in every line.
    :param logger: Logger the WARNING goes to, kept on the object as ``log``. It is the
        owner's, because ``FileTaskHandler`` is attached to ``airflow.task`` rather than
        to the root: a module logger would keep the line out of the task log everywhere
        but the watcher.
    :param ack_unmatched: ``True`` when a message the filter rejected is acknowledged
        and gone, ``False`` when it is returned to the queue.

    Counters do not reset one another. ``streak`` is the run of misses since the last
    match, and a match ends it. ``error_streak`` survives a match: alternating
    ``MATCH``/``ERROR`` would otherwise make every failure the first one and put a line
    on every turn of the loop. Only a hundred outcomes in a row without a failure clear
    it. ``dropped`` is cleared by nothing at all — it counts messages deleted for good,
    and a stream of ninety-nine misses and one match never reaches the ``streak``
    threshold while deleting ninety-nine messages out of every hundred.
    """

    __slots__ = (
        "ack_unmatched",
        "clean_run",
        "dropped",
        "error_streak",
        "log",
        "queue_name",
        "streak",
    )

    def __init__(self, queue_name: str, logger: logging.Logger, ack_unmatched: bool = False):
        self.queue_name = queue_name
        self.log = logger
        self.ack_unmatched = ack_unmatched
        self.streak = 0
        self.dropped = 0
        self.error_streak = 0
        self.clean_run = 0

    def on_match(self) -> None:
        """Record a message that passed the filter."""
        self.streak = 0
        self._count_clean_outcome()

    def on_miss(self) -> None:
        """Record a message the filter read and rejected, once it has been disposed of.

        The call comes after the ACK or the NACK, so ``dropped`` counts deletions the
        broker confirmed: an ``ack()`` that raises on a closed channel leaves the
        message in the queue, and a line saying it was deleted would be untrue.
        """
        self.streak += 1
        self._count_clean_outcome()
        if self.ack_unmatched:
            self.dropped += 1
        if _is_threshold(self.streak):
            disposition = (
                "were acknowledged and dropped" if self.ack_unmatched
                else "were returned to the queue"
            )
            self.log.warning(
                "%d messages in a row missed the filter on queue %r and %s — check filter_data",
                self.streak, self.queue_name, disposition,
            )
        # An equal `dropped` says exactly what the line above already said, in the same
        # number: every miss of this state's lifetime is in that run and every one of
        # them was deleted. The totals part ways as soon as a match breaks the run.
        if self.ack_unmatched and _is_threshold(self.dropped) and self.dropped != self.streak:
            self.log.warning(
                "%d messages that did not pass the filter have been dropped on queue %r "
                "(ack_unmatched is on) — check filter_data",
                self.dropped, self.queue_name,
            )

    def on_error(self, reason: str | None) -> None:
        """Record a message the filter could not be evaluated on.

        :param reason: Type and text of the exception behind the failure, as returned by
            :meth:`~airflow_provider_rmq.utils.filters.MessageFilter.evaluate`. It goes
            into the WARNING, so the first line of a series says what failed and not
            merely where.

        The first failure is always logged — it is the diagnostic one — and the series is
        then throttled to the powers of ten from a hundred up. An ``ERROR`` message is
        returned to the queue whatever the subscription asked for, so it comes back every
        0.1 s: a line per turn would put the redelivery loop itself into the log.
        """
        self.error_streak += 1
        self.clean_run = 0
        if self.error_streak == 1 or _is_threshold(self.error_streak):
            self.log.warning(
                "Filter could not be evaluated on queue %r (%d in this series, message "
                "returned to the queue): %s",
                self.queue_name, self.error_streak, reason,
            )

    def on_batch_done(self, matched: int, evaluated: int, dropped_in_batch: int) -> None:
        """Report on a batch the caller evaluated in one go.

        :param matched: Messages of the batch that passed the filter.
        :param evaluated: Messages the filter reached a verdict on.
        :param dropped_in_batch: Messages acknowledged because they missed the filter.

        A batch is the operator's whole task run, so a line here comes out at most once
        per run and needs no threshold: it carries the visibility ``streak`` cannot give
        to a counter that never outlives a single ``max_messages`` batch. A batch that
        matched nothing and dropped everything is one line and not two.
        """
        if evaluated > 0 and matched == 0:
            dropped_clause = (
                f", and {dropped_in_batch} of them were acknowledged and dropped"
                if dropped_in_batch > 0 else ""
            )
            self.log.warning(
                "None of the %d messages read from queue %r passed the filter%s — "
                "check filter_data",
                evaluated, self.queue_name, dropped_clause,
            )
        elif dropped_in_batch > 0:
            self.log.warning(
                "%d of the %d messages read from queue %r did not pass the filter and were "
                "dropped (ack_unmatched is on) — check filter_data",
                dropped_in_batch, evaluated, self.queue_name,
            )

    def _count_clean_outcome(self) -> None:
        """Count an outcome that was not a failure to evaluate, clearing a stale series."""
        self.clean_run += 1
        if self.clean_run >= _ERROR_STREAK_RESET_RUN:
            self.error_streak = 0
            self.clean_run = 0


async def nack_and_sleep(message: Any) -> None:
    """NACK a message with requeue=True and sleep 0.1 s to prevent a hot redelivery loop.

    :param message: An ``aio_pika`` message (must have ``.nack()`` async method).

    The message goes back to the queue and nothing else happens to it: acknowledging a
    message lives in :func:`handle_miss`, so a message the filter failed on cannot be
    routed through the acknowledging branch by mistake.
    """
    await message.nack(requeue=True)
    await asyncio.sleep(0.1)


async def handle_miss(message: Any, miss_state: FilterMissState) -> None:
    """Dispose of a message the filter read and rejected.

    :param message: An ``aio_pika`` message (must have ``.ack()`` and ``.nack()``).
    :param miss_state: Counters of the consuming loop; its ``ack_unmatched`` decides
        between acknowledging the message and returning it to the queue.

    With ``ack_unmatched`` off the message is returned to the queue after the usual
    0.1 s pause. With it on the message is acknowledged and gone for good: dead-lettering
    follows a reject and not an acknowledgement, so no copy is kept anywhere.

    The counters are told after the broker call, so a deletion that did not go through
    is not counted as one.
    """
    if miss_state.ack_unmatched:
        await message.ack()
    else:
        await nack_and_sleep(message)
    miss_state.on_miss()


def handle_miss_sync(hook: Any, delivery_tag: int, miss_state: FilterMissState) -> None:
    """Dispose of a message the filter read and rejected, on a blocking path.

    :param hook: Anything with ``ack(delivery_tag)`` and ``nack(delivery_tag, requeue=)``
        — an :class:`~airflow_provider_rmq.hooks.rmq.RMQHook` in the operator and in the
        sensor. Duck-typed, so the AMQP helpers stay free of a hook import.
    :param delivery_tag: Tag of the delivery to dispose of.
    :param miss_state: Counters of the consuming loop; its ``ack_unmatched`` decides
        between acknowledging the message and returning it to the queue.

    The operator and the sensor share it so the decision behind an acknowledged miss
    lives in one place. There is no pause here: a blocking loop reads one batch per
    fetch and does not come back to the same message inside it.

    The counters are told after the broker call, so a deletion that did not go through
    is not counted as one.
    """
    if miss_state.ack_unmatched:
        hook.ack(delivery_tag)
    else:
        hook.nack(delivery_tag, requeue=True)
    miss_state.on_miss()


async def match_and_ack(
    message: Any,
    msg_filter: MessageFilter,
    miss_state: FilterMissState,
) -> bool:
    """Evaluate message against filter, ACK on match, dispose of the rest by verdict.

    :param message: An ``aio_pika`` message (must have ``.body``, ``.headers``,
        ``.ack()``, ``.nack()`` async methods).
    :param msg_filter: Pre-built :class:`~airflow_provider_rmq.utils.filters.MessageFilter`.
    :param miss_state: Counters of the consuming loop; it also carries the
        ``ack_unmatched`` decision read from the subscription or the task parameter.
    :returns: ``True`` if the message matched and was ACKed, ``False`` otherwise.

    A message the filter rejected goes to :func:`handle_miss`, which acknowledges it when
    ``ack_unmatched`` is on and returns it to the queue otherwise. A message the filter
    could not be evaluated on is always returned to the queue, after which the call
    sleeps 0.1 s to prevent a hot redelivery loop on classic queues.

    Note:
        On quorum queues (RabbitMQ 4.x default), the broker enforces a delivery-limit
        of 20 by default — a message returned to the queue is dead-lettered after 20
        redeliveries. Use dedicated queues per DAG to avoid unintended message loss.
    """
    verdict, reason = match(message, msg_filter)
    if verdict is FilterVerdict.MATCH:
        miss_state.on_match()
        await message.ack()
        return True
    if verdict is FilterVerdict.ERROR:
        miss_state.on_error(reason)
        await nack_and_sleep(message)
        return False
    await handle_miss(message, miss_state)
    return False
