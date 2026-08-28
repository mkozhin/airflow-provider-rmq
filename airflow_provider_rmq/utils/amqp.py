from __future__ import annotations

import asyncio
import logging
import math
import ssl
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode

from airflow_provider_rmq.utils.filters import MessageFilter
from airflow_provider_rmq.utils.ssl import build_ssl_context

log = logging.getLogger(__name__)

AMQP_PORT = 5672
AMQPS_PORT = 5671

#: Seconds between AMQP heartbeat frames. The client declares the connection dead
#: after two missed intervals, so a broken link surfaces as an exception in about
#: ``2 * DEFAULT_HEARTBEAT`` seconds and ``connect_robust`` reconnects.
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
    except (TypeError, ValueError):
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
    """
    value = _read_number(
        extras, HEARTBEAT_KEY, DEFAULT_HEARTBEAT, lambda raw: int(float(raw)), 0
    )
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


def match(message: Any, msg_filter: MessageFilter) -> bool:
    """Evaluate message body and headers against filter without any ACK/NACK side-effects.

    :param message: An ``aio_pika`` message (must have ``.body`` and ``.headers``).
    :param msg_filter: Pre-built :class:`~airflow_provider_rmq.utils.filters.MessageFilter`.
    :returns: ``True`` if the message matches (or there are no filters), ``False`` otherwise.

    Non-UTF-8 bytes in the body are replaced with the Unicode replacement character
    (``errors="replace"``) so binary payloads never raise :exc:`UnicodeDecodeError`.
    """
    body_str = message.body.decode("utf-8", errors="replace")
    props = _PropsShim(dict(message.headers or {}))
    return not msg_filter.has_filters or msg_filter.matches(props, body_str)



def next_backoff(current: float, maximum: float, minimum: float = 0.0) -> float:
    """The pause that follows ``current``: twice as long, and never past ``maximum``.

    :param minimum: Floor for the result, for a backoff that starts counting from zero.
    """
    return min(max(current * 2, minimum), maximum)


async def nack_and_sleep(message: Any) -> None:
    """NACK a message with requeue=True and sleep 0.1 s to prevent a hot redelivery loop.

    :param message: An ``aio_pika`` message (must have ``.nack()`` async method).
    """
    await message.nack(requeue=True)
    await asyncio.sleep(0.1)



async def match_and_ack(message: Any, msg_filter: MessageFilter) -> bool:
    """Evaluate message against filter, ACK on match, NACK+requeue on miss.

    :param message: An ``aio_pika`` message (must have ``.body``, ``.headers``,
        ``.ack()``, ``.nack()`` async methods).
    :param msg_filter: Pre-built :class:`~airflow_provider_rmq.utils.filters.MessageFilter`.
    :returns: ``True`` if the message matched and was ACKed, ``False`` if NACKed.

    After a NACK, sleeps 0.1 s to prevent a hot redelivery loop on classic queues.

    Note:
        On quorum queues (RabbitMQ 4.x default), the broker enforces a delivery-limit
        of 20 by default — non-matching messages are dead-lettered after 20 redeliveries.
        Use dedicated queues per DAG to avoid unintended message loss.
    """
    if match(message, msg_filter):
        await message.ack()
        return True
    await nack_and_sleep(message)
    return False
