from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import socket
import threading
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import aio_pika
import aio_pika.exceptions
import httpx
from aio_pika.connection import make_url
from airflow.exceptions import DagRunAlreadyExists
from airflow.hooks.base import BaseHook
from airflow.models import DagModel
from sqlalchemy.exc import IntegrityError

from airflow_provider_rmq.utils.amqp import (
    DEFAULT_RPC_TIMEOUT,
    AmqpTimeouts,
    build_amqp_connection,
    call_with_timeout,
    get_amqp_timeouts,
    match,
    nack_and_sleep,
    next_backoff,
)
from airflow_provider_rmq.utils.executor import BoundedExecutor
from airflow_provider_rmq.utils.filters import MessageFilter
from airflow_provider_rmq.utils.management import (
    get_current_bindings,
    get_management_url,
    get_queue_consumers,
)
from airflow_provider_rmq.utils.metrics import incr
from airflow_provider_rmq.watcher.models import (
    WatcherSession,
    get_conn_statuses,
    set_consumer_status,
    upsert_conn_status,
)
from airflow_provider_rmq.watcher.orphan_tracker import OrphanTracker
from airflow_provider_rmq.watcher.subscription_builder import _SUB_QUEUE_PREFIX

log = logging.getLogger(__name__)

_RECONNECT_DELAY = 5.0

#: Roles a connection is pooled under. Publishing lives on its own connection because
#: a resource alarm blocks publishing connections by making the broker stop reading
#: their socket — which stalls ``basic.ack`` of every consumer sharing that connection.
_ROLE_CONSUME = "consume"
_ROLE_PUBLISH = "publish"

# ---------------------------------------------------------------------------
# Timeouts — seconds one call is given before the caller stops waiting for it
# ---------------------------------------------------------------------------

#: A best-effort ``close()`` of a connection or channel.
_CLOSE_TIMEOUT = 5.0

#: A cancelled task, before the cycle moves on without it. A task that ignores its
#: cancellation must not hold up the cycle that cancelled it.
_CANCEL_TIMEOUT = 30.0
#: Seconds ``stop()`` waits for cancelled tasks. Shorter than the listener's own
#: ``_STOP_TIMEOUT`` on purpose: the wait and the closing that follows it share that one
#: budget, and a wait that spends all of it leaves connections and the HTTP client open.
_STOP_CANCEL_TIMEOUT = 10.0

#: A blocking database write. The worker stays busy until the call itself returns — a
#: running thread cannot be interrupted — so the timeout buys back the coroutine, not
#: the worker.
_DB_TIMEOUT = 30.0

#: A single ``trigger_dag``. A consumer task awaits it, not the reconcile cycle, so the
#: cycle watchdog never sees it hang: without a timeout of its own the task would sit in
#: ``listening`` while consuming nothing at all.
_TRIGGER_TIMEOUT = 60.0

#: One Management API request, connect and read together.
_MGMT_HTTP_TIMEOUT = 5.0

# ---------------------------------------------------------------------------
# Strike counts — how often a signal has to repeat in a row before it is acted on
# ---------------------------------------------------------------------------
#: A single occurrence of any of these is also what an ordinary hiccup looks like: a
#: slow cycle, a consumer registering late, one unanswered request. Requiring two in a
#: row, with a single good observation clearing the count, puts every verdict at least
#: two reconcile intervals away from the first suspicion — the cheapest count no single
#: bad moment can reach.

#: Publish timeouts on one ``conn_id`` that condemn its publish connection.
_PUBLISH_TIMEOUTS_BEFORE_DROP = 2

#: Negative liveness checks that condemn a consumer.
_NEGATIVE_CHECKS_BEFORE_RESTART = 2

#: Management API failures after which the liveness check switches to the passive-declare
#: probe: a wrong URL or credentials without the ``management`` tag must not disable the
#: watchdog altogether.
_MGMT_FAILURES_BEFORE_FALLBACK = 2

#: Cycles in which a ``conn_id`` has live tasks but not one of them reaches ``listening``.
#: A healthy attach costs a connect and two RPCs, so a connection that never gets there
#: is not answering, and the check has no candidate to prove it with.
_STUCK_CYCLES_BEFORE_DROP = 2

#: Reconcile cycles that must pass before the same conn_id may be recreated again.
#: A real disconnect needs one recreation, so the limit costs nothing there, while a
#: misclassification turns from a continuous loop into a rare, logged event.
_CYCLES_BEFORE_REDROP = 5

# ---------------------------------------------------------------------------
# Vocabularies stored in the database
# ---------------------------------------------------------------------------

#: Values ``rmq_watcher_conn_status.status`` takes: the broker confirmed our consumers,
#: it did not, a negative verdict was held back by the recreation rate limit, or no
#: check has ever reached a verdict on this conn_id.
_CONN_CONNECTED = "connected"
_CONN_ERROR = "error"
_CONN_DEGRADED = "degraded"
_CONN_UNKNOWN = "unknown"

#: Values ``rmq_watcher_subscriptions.consumer_status`` takes: one consumer task is
#: opening its connection, consuming, retrying after an error, or gone.
_SUB_CONNECTING = "connecting"
_SUB_LISTENING = "listening"
_SUB_ERROR = "error"
_SUB_DISCONNECTED = "disconnected"

#: What a trigger attempt ended as. ``triggered`` and ``duplicate`` both mean the DAG
#: run for this delivery exists, so the delivery is acknowledged; ``skipped`` means the
#: DAG cannot run at all and acknowledging it is terminal by design — a NACK would turn
#: a paused DAG into a redelivery accumulator.
_OUTCOME_TRIGGERED = "triggered"
_OUTCOME_SKIPPED = "skipped"
_OUTCOME_DUPLICATE = "duplicate"

#: ``DagRun.run_id`` is a ``String(250)`` validated against this alphabet.
_RUN_ID_MAX_LEN = 250
_RUN_ID_UNSAFE_RE = re.compile(r"[^A-Za-z0-9_.~:+-]")
_RUN_ID_HASH_LEN = 8

#: Backoff for a delivery whose trigger failed: doubling from the first second up to a
#: minute, reset by the next success. The 0.1 s of ``nack_and_sleep`` guards against a
#: hot loop of filter misses and is far too short here — ~10 redeliveries per second
#: burn the default delivery-limit of 20 on a quorum queue in about two seconds, and the
#: message is dead-lettered by the very mechanism meant to keep it.
_TRIGGER_BACKOFF_START = 1.0
_TRIGGER_BACKOFF_MAX = 60.0

#: Backoff for a delivery whose cooldown placeholder could not be published, doubling
#: from the first second up to a minute and reset by the next successful publish. A
#: broker under a resource alarm holds publishes for as long as the alarm lasts, and
#: without a growing pause the same delivery comes back every reconnect delay and burns
#: the quorum-queue delivery limit — after which the broker dead-letters or drops the
#: very message the requeue was meant to keep.
_PUBLISH_BACKOFF_START = 1.0
_PUBLISH_BACKOFF_MAX = 60.0

_FIRE_EXCHANGE = "rmq_watcher.fire"
_FIRE_QUEUE = "rmq_watcher.fire"
_PENDING_QUEUE_PREFIX = "rmq_watcher.pending."
_EXCHANGE_TTL_MS = 28800000  # 8h — safety net against unbounded orphan queue growth


def _consumer_tag(suffix: Any, nonce: str | None = None) -> str:
    """Build the consumer tag this process registers on a queue.

    The tag carries host, pid and subscription id, so a liveness check can pick our
    own consumer out of the queue's consumer list: the same queue legitimately
    carries foreign consumers and the second scheduler replica in HA mode, and a
    plain consumer count cannot tell them apart from ours.

    ``nonce`` distinguishes one attach from the next. A connection whose ``close()``
    never returned may still be registered on the broker, and without the nonce that
    ghost carries the same tag as the task that replaced it and vouches for it.
    """
    tag = f"rmq_watcher.{socket.gethostname()}.{os.getpid()}.{suffix}"
    return f"{tag}.{nonce}" if nonce else tag


def _attach_nonce() -> str:
    """Short random marker identifying a single attach of a consumer to a queue."""
    return uuid.uuid4().hex[:8]


def _safe_run_id(raw: str) -> str:
    """Turn an arbitrary string into a run id Airflow accepts and can store.

    Both halves of a run id come from outside: the producer picks ``message_id`` (a
    shortstr of up to 255 bytes, any characters at all), and a queue or DAG name may
    carry spaces or slashes, while ``DagRun.run_id`` is a validated ``String(250)``.
    A value that needed substitution or truncation carries a short digest of the
    original, so two different messages keep two different run ids instead of
    collapsing into a single DAG run.
    """
    sanitized = _RUN_ID_UNSAFE_RE.sub("_", raw)
    if sanitized == raw and len(sanitized) <= _RUN_ID_MAX_LEN:
        return sanitized
    digest = hashlib.sha256(raw.encode("utf-8", "replace")).hexdigest()[:_RUN_ID_HASH_LEN]
    return f"{sanitized[: _RUN_ID_MAX_LEN - _RUN_ID_HASH_LEN - 1]}_{digest}"


def _build_run_id(queue_name: str, message_id: str | None = None) -> str:
    """Run id for one immediate-mode delivery.

    An AMQP ``message_id`` makes the run id deterministic, so a redelivery of the same
    message lands on the DAG run it already produced instead of starting a second one.
    Without one, the timestamp keeps every delivery distinct — deduplication then rests
    entirely on the producer choosing to set ``message_id``.
    """
    suffix = message_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
    return _safe_run_id(f"rmq__{queue_name}__{suffix}")


def _sync_trigger(dag_id: str, conf: dict, run_id: str) -> str:
    """Synchronous DAG trigger — called via the thread pool from the consumer loop.

    :returns: :data:`_OUTCOME_TRIGGERED` when a DAG run was created,
        :data:`_OUTCOME_DUPLICATE` when this delivery already has one, and
        :data:`_OUTCOME_SKIPPED` when the DAG cannot run.

    Uses a short-lived WatcherSession to avoid polluting Airflow's thread-local
    scoped session. Every other exception propagates, so the caller requeues the
    delivery instead of acknowledging an event that never reached a DAG.
    """
    from airflow.api.common.trigger_dag import trigger_dag  # lazy: not always installed

    with WatcherSession() as session:
        dag_model = (
            session.query(DagModel)
            .filter_by(dag_id=dag_id, is_active=True, is_paused=False)
            .first()
        )
        if not dag_model:
            log.warning(
                "DAG %s not found, inactive or paused — message acked, skipping trigger",
                dag_id,
            )
            return _OUTCOME_SKIPPED

    try:
        # replace_microseconds=False keeps the full timestamp: find_duplicate() matches
        # on execution_date as well as run_id, so a truncated one makes two distinct
        # messages of the same second look like a redelivery of each other.
        trigger_dag(dag_id=dag_id, run_id=run_id, conf=conf, replace_microseconds=False)
    except DagRunAlreadyExists:
        log.info("DAG run %s already exists — redelivery of a handled message", run_id)
        return _OUTCOME_DUPLICATE
    except IntegrityError:
        log.info("DAG run %s already exists (concurrent insert), acking the delivery", run_id)
        return _OUTCOME_DUPLICATE
    return _OUTCOME_TRIGGERED


class _StatusWriter:
    """The one writer of the ``consumer_status`` row of a single subscription.

    A status write is a blocking database call: it runs in a pool worker while the
    caller waits only :data:`_DB_TIMEOUT` for it, and it carries on afterwards. Two
    writes of the same subscription running at once would leave the row holding
    whichever of them a worker happened to commit last — ``listening`` for a
    subscription that has since reported an error, with nothing to correct it, because
    the manager already counts the newer value as stored.

    So one write of a subscription runs at a time. Each caller notes the status it
    wants stored and asks the writer to store; whoever gets there first stores status
    after status until none is left, and the others find the writer taken and return
    on the spot. Of the statuses noted while a write runs only the newest survives — an
    older one it replaced was never going to be the truth about the subscription — and
    no caller ever waits for another caller's write, so a database that has stopped
    answering costs one worker and holds up nothing else.

    A writer is kept for the life of the process, because the row outlives everything
    that writes to it: recovery replaces the task, the state that tracks it and the
    manager itself, and an authority replaced along with them could not rule on the
    write the one before it left running.
    """

    def __init__(self, sub_id: int) -> None:
        self._sub_id = sub_id
        self._lock = threading.Lock()
        self._pending: tuple[str, str | None] | None = None
        self._storing = False
        self._stored: tuple[str, str | None] | None = None

    @property
    def stored(self) -> tuple[str, str | None] | None:
        """The ``(status, last_error)`` last committed to the row, ``None`` while none has been.

        The one honest answer to "does the row already say this?", and the reason it is
        kept here rather than by the caller: a write the caller stopped waiting for still
        reaches the database, so a caller that recorded what it *asked* for would hold a
        marker the row disagrees with, and the guard that skips an unchanged status would
        then suppress every write that could put the two back together.

        The reason it is the pair and not the status alone: the row carries both, and a
        second failure of the same kind changes only the text. Compared on status alone,
        the row keeps naming the cause that has already been dealt with.
        """
        with self._lock:
            return self._stored

    @property
    def has_pending(self) -> bool:
        """Whether a noted status is still waiting to reach the row."""
        with self._lock:
            return self._pending is not None

    def record(self, status: str, last_error: str | None) -> None:
        """Note ``status`` as the one to store next, in place of any still unstored."""
        with self._lock:
            self._pending = (status, last_error)

    def store(self) -> bool:
        """Store noted statuses until none is left. Blocking — belongs in a pool.

        :returns: Whether this call did the storing. ``False`` means a write of this
            subscription is already running and takes the noted status with it, which
            is why the call gives its worker straight back instead of waiting for it.
        """
        with self._lock:
            if self._storing:
                return False
            self._storing = True
        pending: tuple[str, str | None] | None = None
        try:
            while True:
                with self._lock:
                    pending = self._pending
                    self._pending = None
                    if pending is None:
                        self._storing = False
                        return True
                status, last_error = pending
                with WatcherSession() as session:
                    set_consumer_status(session, self._sub_id, status, last_error=last_error)
                    session.commit()
                with self._lock:
                    self._stored = (status, last_error)
                    pending = None
        except BaseException:
            with self._lock:
                # The status this call took on never reached the row, so it goes back to
                # being the one to store next. A subscription that reached its steady
                # state and then went quiet makes no further write to carry it, and its
                # row would say ``connecting`` for as long as the task runs.
                if pending is not None and self._pending is None:
                    self._pending = pending
                self._storing = False
            raise


#: The writer of each subscription, by ``sub_id``: see :class:`_StatusWriter`.
_status_writers: dict[int, _StatusWriter] = {}
_status_writers_lock = threading.Lock()


def _status_writer(sub_id: int) -> _StatusWriter:
    """The writer of subscription ``sub_id``, made the first time it is asked for."""
    with _status_writers_lock:
        writer = _status_writers.get(sub_id)
        if writer is None:
            writer = _status_writers[sub_id] = _StatusWriter(sub_id)
        return writer


def _write_conn_error(conn_id: str, error: str) -> None:
    """Store a failed connection attempt against ``conn_id``. Blocking."""
    with WatcherSession() as session:
        upsert_conn_status(
            session, conn_id, _CONN_ERROR, consumer_count=0, last_error=error
        )
        session.commit()


def _write_conn_status_rows(rows: list[tuple], now: datetime) -> None:
    """Store one status row per conn_id. Blocking.

    A row whose ``status`` is ``None`` carries no verdict — the check produced no data —
    and keeps whatever is already stored, so an unreachable Management API does not
    paint every connection red. A conn_id with nothing stored yet starts at
    :data:`_CONN_UNKNOWN`: the number of tasks the watcher started says nothing about
    the broker, so a connection nobody has verified is reported as unverified rather
    than as healthy.

    The stored statuses are read only when some row actually needs the fallback, so an
    all-verdict cycle costs one write and no extra full-table scan.
    """
    with WatcherSession() as session:
        stored: dict[str, str] | None = None
        for conn_id, count, status, reason, broker_count in rows:
            if status is None:
                if stored is None:
                    stored = {row.conn_id: row.status for row in get_conn_statuses(session)}
                status = stored.get(conn_id, _CONN_UNKNOWN)
            upsert_conn_status(
                session,
                conn_id,
                status,
                consumer_count=count,
                last_error=reason,
                broker_consumer_count=broker_count,
                last_reconcile_at=now,
            )
        session.commit()


class _Backoff:
    """The pause a repeatedly failing step waits out, doubling up to a maximum.

    :param start: The first pause, and the one a success goes back to.
    :param maximum: Longest pause the doubling reaches.
    """

    def __init__(self, start: float, maximum: float) -> None:
        self._start = start
        self._maximum = maximum
        #: The pause the next failure waits out.
        self.seconds = start

    def reset(self) -> None:
        """Back to the first pause — the step got through."""
        self.seconds = self._start

    async def wait(self) -> None:
        """Wait out the current pause, then double it for the failure after this one."""
        await asyncio.sleep(self.seconds)
        self.seconds = next_backoff(self.seconds, self._maximum)


def _error_text(exc: BaseException) -> str:
    """One line naming ``exc``, falling back to its type when it carries no message.

    ``asyncio.TimeoutError`` is raised with no arguments at all, and a ``last_error`` of
    ``""`` tells the operator no more than an empty row would: which call gave up is the
    whole of what there is to say about it.
    """
    return str(exc) or type(exc).__name__


def _new_connection(url: str, ssl_context: Any) -> Any:
    """Build the robust connection to ``url``, before anything is awaited on it.

    Whoever asks for a connection pools this object first and connects it afterwards.
    ``RobustConnection`` runs the connect in a task of its own and hands the caller only
    a future to wait on, so an attempt the caller stops waiting for — a timeout, a
    cancelled consumer task — leaves a live connection to the broker that is in no pool,
    that nothing closes, and that the connection factory goes on reconnecting.
    """
    return aio_pika.RobustConnection(make_url(url), ssl_context=ssl_context)


def _retrieve_outcome(task: asyncio.Task) -> None:
    """Read a finished connect's outcome, so a failure nobody waited for is not reported
    as an exception that was never retrieved."""
    if not task.cancelled():
        task.exception()


async def _await_connected(task: asyncio.Task, timeout: float, what: str) -> None:
    """Give ``task`` — one ``connect()`` of a connection — ``timeout`` seconds to land.

    The bound has to be here rather than in ``connect_robust(timeout=...)``, which covers
    the TCP connect and the AMQP handshake and stops there. What follows it is
    ``ready()``, and that waits for the broker to declare the connection unblocked: a
    broker under a resource alarm sends ``Connection.Blocked`` to every client that
    advertises the capability, as ``aiormq`` does, so a connect made during an alarm does
    not return until the alarm clears — however small the ``timeout`` was.

    A connect the caller gives up on is left running and never cancelled: ``connect()``
    waits on a future its connection factory resolves, and cancelling that wait cancels
    the future itself, after which every later ``connect()`` of the same connection
    raises :exc:`~asyncio.CancelledError` at once. Left alone, the attempt stays in
    flight for the next caller to wait on and the connection becomes usable the moment
    it lands.

    :raises asyncio.TimeoutError: When the connect has not landed in ``timeout`` seconds.
    """
    done, _ = await asyncio.wait({task}, timeout=timeout)
    if not done:
        raise asyncio.TimeoutError(f"{what} did not connect within {timeout:g}s")
    task.result()


async def _close_quietly(closeable: Any, what: str, method: str = "close") -> None:
    """Close ``closeable``, giving up after :data:`_CLOSE_TIMEOUT`.

    Closing is best effort wherever it happens: the caller has already stopped using the
    object and what happens next does not depend on the answer. The failure is logged
    rather than swallowed — a close that fails or never returns is a broker or a socket
    behaving oddly, and that is worth a line.

    :param what: What is being closed, for the log line.
    :param method: Name of the closing coroutine — ``httpx`` spells it ``aclose``.
    """
    try:
        await call_with_timeout(getattr(closeable, method)(), timeout=_CLOSE_TIMEOUT)
    except Exception as exc:
        log.warning("Closing %s failed: %s — continuing without it", what, exc)


@asynccontextmanager
async def _attached(queue: Any, consumer_tag: str, timeout: float) -> AsyncIterator[Any]:
    """Register ``consumer_tag`` on ``queue`` and hand out the iterator of that consumer.

    ``basic.consume`` is performed here instead of inside the iterator's own
    ``__aenter__`` so that the registration is bounded. Unbounded it is the third way
    into the incident this watchdog exists for: the manager would report the
    subscription as attached and hand the liveness check a tag the broker never
    registered, and the connection would read as healthy with nothing consuming.

    Leaving the block cancels the consumer under :data:`_CLOSE_TIMEOUT` — aio_pika
    cancels it with no bound of its own, and ``basic.cancel`` is exactly the call a
    zombie connection never answers, which would leave the cancelled task and its
    channel alive for as long as the process runs.

    :param timeout: Seconds the broker is given to confirm the registration.
    """
    q_iter = queue.iterator(consumer_tag=consumer_tag)
    await call_with_timeout(q_iter.consume(), timeout=timeout)
    try:
        yield q_iter
    finally:
        await _close_quietly(q_iter, f"the consumer {consumer_tag!r}")


def _usable(connection: Any) -> bool:
    """Whether ``connection`` can carry a call right now.

    Two states have to be told apart from a healthy connection, and only one of them
    says ``is_closed``. The other is a reconnect that has not finished: aio_pika's
    factory clears the transport before each attempt it makes, so a connection it is
    still rebuilding — or has stopped rebuilding — reports ``is_closed`` False with
    nothing underneath, and answers every ``channel()`` with
    ``RuntimeError("Connection was not opened")``. Handed that object, a consumer task
    fails on it, pauses, and asks the pool for the same object again for as long as the
    process runs.
    """
    return not connection.is_closed and getattr(connection, "transport", None) is not None


_DELIVERY_FAULT = "_rmq_raised_by_delivery"


def _mark_delivery_fault(exc: BaseException) -> None:
    """Record on ``exc`` that it was raised by the handling of a delivery.

    The mark is what draws the line the cancellation heuristic cannot draw for itself:
    the heuristic reads an exception chain, and the chain a broken connection leaves
    behind is the same one whether the call it interrupted was the queue iterator's
    ``basic.cancel`` or a publish made for a message already taken off the queue. Only
    the place the exception came from tells the two apart, so that is what is recorded
    where it is still known.

    An exception whose class refuses attributes stays unmarked and is judged by the
    chain alone; every exception raised on this path so far accepts one.
    """
    try:
        setattr(exc, _DELIVERY_FAULT, True)
    except (AttributeError, TypeError):
        log.debug(
            "An exception of type %s takes no attribute, so the delivery that raised it "
            "is not recorded on it", type(exc).__name__,
        )


def _raised_while_cancelling(exc: BaseException) -> bool:
    """Whether ``exc`` came out of handling a cancellation.

    aio_pika cancels the consumer from inside ``QueueIterator.__anext__`` while it
    handles the ``CancelledError``, waits for that cancel without a bound of its own and
    catches only a timeout of it. A broker that rejects the pending ``basic.cancel`` —
    which every connection torn down mid-call does — therefore lets its own error out in
    place of the cancellation. A retry loop reading that as a transient fault subscribes
    again, and a task the manager counts as cancelled goes on consuming the queue under
    no supervision: nothing holds it, nothing can cancel it, and it writes status into
    the row its replacement owns.

    ``Task.cancelling()`` would answer this directly and arrived in Python 3.11, so the
    chain the exception carries is what there is to read on 3.10.

    What is read of that chain: ``__cause__`` always, ``__context__`` only while
    ``__suppress_context__`` is unset. ``__cause__`` is the answer to a deliberate
    ``raise ... from ...``, and ``aiormq`` hands a torn-down connection to a pending RPC
    exactly that way — ``raise self._exception from e`` with ``e`` the ``CancelledError``
    the RPC task was stopped with — which is how a rejected ``basic.cancel`` reaches
    ``__anext__``. ``__context__`` carries what was merely being handled when this
    exception was raised, and that is the plain form: an error escaping the
    ``except CancelledError`` block inside ``__anext__``. ``__suppress_context__`` is
    what a ``raise ... from None`` sets, and code that writes it is saying the
    cancellation it was handling is not the cause of what it raises —
    :func:`call_with_timeout` converting an expired call into a ``TimeoutError`` says
    precisely that.

    The chain is all this function reads, so it cannot see where the exception was
    raised: an ``aiormq`` failure carrying a ``CancelledError`` in ``__cause__`` looks
    the same whether it came from ``__anext__`` or from a publish. Callers keep the
    handling of a delivery away from here with :func:`_mark_delivery_fault` instead.
    """
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if isinstance(current, asyncio.CancelledError):
            return True
        seen.add(id(current))
        if current.__cause__ is not None:
            current = current.__cause__
        elif current.__suppress_context__:
            current = None
        else:
            current = current.__context__
    return False


def _ends_as_cancelled(exc: BaseException) -> bool:
    """Whether a consumer loop ends as cancelled on ``exc`` instead of retrying on it.

    An exception the handling of a delivery raised is a transient fault, whatever chain
    it carries: the mark says where it came from, and where it came from is the one
    thing :func:`_raised_while_cancelling` cannot read. Everything else — that is,
    whatever the queue iterator itself raised — is put to the heuristic.
    """
    if getattr(exc, _DELIVERY_FAULT, False):
        return False
    return _raised_while_cancelling(exc)


async def _wait_cancelled(
    tasks: list[asyncio.Task], timeout: float | None = None
) -> set[asyncio.Task]:
    """Wait for already-cancelled ``tasks``, giving up after ``timeout``.

    :param timeout: Seconds to wait; :data:`_CANCEL_TIMEOUT` when not given. Read here
        rather than defaulted in the signature, where it would be bound once at import
        and stop following the constant.

    :returns: The tasks that were still running when the wait gave up.

    A task is free to catch its own ``CancelledError`` and keep going, and the cycle
    that cancelled it has a budget of its own to keep. Waiting without a bound is what
    turns one uncooperative task into a cycle that never ends. The caller keeps what is
    handed back so that a task the recovery walked away from is still accounted for.
    """
    if not tasks:
        return set()
    timeout = _CANCEL_TIMEOUT if timeout is None else timeout
    done, pending = await asyncio.wait(tasks, timeout=timeout)
    for task in done:
        if not task.cancelled():
            task.exception()  # retrieved so it is not reported as never awaited
    if pending:
        log.warning(
            "%d consumer task(s) are still running %.0fs after being cancelled — the "
            "cycle continues without waiting for them",
            len(pending), timeout,
        )
    return pending


class _ConsumerState:
    """In-memory guard: writes consumer_status to DB only when the status actually changes.

    Prevents hot DB writes during reconnect storms (e.g. 20+/min → 2-4/min).

    A ``sub_id`` of ``None`` tracks the status in memory only — the fire consumer runs
    on the shared ``rmq_watcher.fire`` queue and has no row in
    ``rmq_watcher_subscriptions``.
    """

    def __init__(self, sub_id: int | None, executor: BoundedExecutor) -> None:
        self._sub_id = sub_id
        self._executor = executor
        self._status: str | None = None
        #: Tag the task registered on its queue during its current attach, ``None``
        #: while it is not attached. The liveness check asks the broker for this exact
        #: tag rather than recomputing one.
        self.consumer_tag: str | None = None

    @property
    def status(self) -> str | None:
        """Status the task last reported, ``None`` before it reported anything."""
        return self._status

    async def write(
        self,
        status: str,
        last_error: str | None = None,
        executor: BoundedExecutor | None = None,
    ) -> None:
        """Record ``status``, storing it unless the row already holds it.

        :param executor: Pool the blocking write runs in; the consumer pool by default.
            The reconcile cycle passes its own so that a stalled delivery and a stalled
            cycle never share a worker.

        The reported status is the manager's own view of the task and is updated
        whatever the database does: it gates the liveness check, and a subscription
        left out of that check because one write timed out would never be verified
        again for the life of the task.

        The store is a blocking database write, so it runs in a pool under a timeout:
        the consumer task awaits it, and a database that never answers would otherwise
        hold the task open forever. It never propagates either — diagnostics must not
        stop consumption — so a write that does not land is simply tried again by the
        next call.

        What "already stored" means is the writer's to say, not this call's. Every write
        of one subscription goes through that subscription's own writer
        (:class:`_StatusWriter`), which is the only thing that knows what reached the
        row: a write the caller stopped waiting for still gets there, and a status handed
        to a write already running gets there through that one. A caller marking down
        what it *asked* for would end up holding the opposite of what the row says, and
        the guard right below would then suppress every write that could correct it.
        """
        self._status = status
        if self._sub_id is None:
            return
        writer = _status_writer(self._sub_id)
        if (status, last_error) == writer.stored and not writer.has_pending:
            # Skipped only when the row already says this *and* nothing is waiting to be
            # written: a status left behind by a write that failed is the row's only way
            # back to the truth, and returning here would drop it.
            return
        writer.record(status, last_error)
        pool = executor if executor is not None else self._executor
        try:
            await call_with_timeout(pool.run(writer.store), timeout=_DB_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "Cannot store status %r of subscription %s: %s",
                status, self._sub_id, exc,
            )

    async def flush(self, executor: BoundedExecutor) -> None:
        """Store the status an earlier write did not get into the row.

        A write that fails is put back by the writer, and what carries it after that is
        the next write of the same subscription. A subscription whose consumer is
        attached and whose queue is quiet makes none, so without a nudge of its own its
        row would keep the status it had while the database was away.
        """
        if self._sub_id is None:
            return
        writer = _status_writer(self._sub_id)
        if not writer.has_pending:
            return
        try:
            await call_with_timeout(executor.run(writer.store), timeout=_DB_TIMEOUT)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "Cannot store the unwritten status of subscription %s: %s",
                self._sub_id, exc,
            )


class _Checked:
    """Liveness bookkeeping shared by the two kinds of consumer the check judges."""

    negative_checks: int

    def record(self, seen: bool) -> bool:
        """Note what the broker answered about this consumer.

        :param seen: Whether the broker holds this consumer's own tag.
        :returns: Whether the consumer is condemned, i.e. whether
            :data:`_NEGATIVE_CHECKS_BEFORE_RESTART` checks in a row came back negative.

        One positive answer clears the count. A negative count survives being acted on:
        while the recreation rate limit holds a verdict back the condition it describes
        is still there, so the verdict stands again on the first cycle the limit allows
        instead of starting the count from zero.
        """
        if seen:
            self.negative_checks = 0
            return False
        self.negative_checks += 1
        return self.negative_checks >= _NEGATIVE_CHECKS_BEFORE_RESTART


@dataclass
class _ActiveSub(_Checked):
    """Snapshot of a running subscription consumer task."""
    task: asyncio.Task
    sub: dict  # full snapshot of sub at task start time
    state: _ConsumerState  # status the task reports, readable by the manager
    negative_checks: int = 0  # consecutive liveness checks the broker answered negatively


@dataclass
class _FireSub(_Checked):
    """What the manager knows about the fire consumer task, mirroring :class:`_ActiveSub`.

    Its own state gates the liveness check the same way a subscription's does — a fire
    task pausing in its retry loop is not a candidate — and its own counter keeps its
    verdict to itself instead of expressing it through the ``conn_id`` it shares with
    ordinary subscriptions.
    """
    conn_id: str
    state: _ConsumerState
    #: The connection object the task was handed and holds for its whole life. The pool
    #: replaces that object whenever the connect behind it fails, and a task left on the
    #: replaced one answers every call with the failure it holds, so the manager compares
    #: the two and restarts the task when they part.
    connection: Any = None
    negative_checks: int = 0


def _unseen_reason(unseen_subs: int, fire_unseen: bool) -> str:
    """Name what the broker failed to confirm on one ``conn_id``."""
    parts = []
    if unseen_subs:
        parts.append(f"{unseen_subs} subscription(s) unseen by the broker")
    if fire_unseen:
        parts.append("the fire consumer is unseen by the broker")
    return " and ".join(parts)


@dataclass
class _ConnLiveness:
    """Verdict one liveness check reached for a single ``conn_id``.

    :param status: ``connected`` when the broker confirmed our consumers, ``error``
        when it did not, ``degraded`` when a negative verdict was held back by the
        recreation rate limit, and ``None`` when the check produced no data at all.
    :param broker_consumer_count: Consumers the broker reports on our queues, ``None``
        when the check cannot tell (Management API unavailable or not configured).
    :param reason: Human-readable explanation for a non-``connected`` status.
    """
    status: str | None
    broker_consumer_count: int | None
    reason: str | None = None


@dataclass
class _ConnState:
    """Everything the manager remembers about one ``conn_id``.

    One record per conn_id, so a conn_id no subscription mentions any more is forgotten
    with a single ``pop``. A conn_id that comes back later is a new connection and
    starts from a clean slate: a leftover drop cycle would hold off its first legitimate
    recreation, and a leftover verdict would be written into its status row.
    """

    #: Pooled connections by role — :data:`_ROLE_CONSUME`, :data:`_ROLE_PUBLISH`, or both.
    connections: dict[str, Any] = field(default_factory=dict)
    #: The connect of each role that has not landed yet. A caller that stopped waiting
    #: leaves its attempt here rather than starting a second one, so one conn_id costs
    #: the broker one connection however many callers are waiting for it.
    connecting: dict[str, asyncio.Task] = field(default_factory=dict)
    #: Serialises starting a connect of this one conn_id, so that however many callers
    #: want the connection the broker is asked for one. They wait for that attempt side
    #: by side, outside this lock. One lock per conn_id and not one for the manager:
    #: everything it guards concerns a single broker, so a broker that stopped answering
    #: holds up the conn_ids that use it and no others.
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    #: Channel of the publish connection, opened on demand.
    publish_channel: Any = None
    #: Call timeouts read off the Airflow connection's ``extra``.
    timeouts: AmqpTimeouts | None = None
    #: Consecutive publish timeouts — the publish connection's own liveness signal.
    publish_timeouts: int = 0
    #: Cycle the whole connection was last dropped in, ``None`` while it never was.
    last_drop_cycle: int | None = None
    #: Verdict this cycle's liveness check reached, ``None`` while it reached none.
    liveness: _ConnLiveness | None = None
    #: Consecutive Management API failures.
    mgmt_failures: int = 0
    #: Consecutive cycles with live tasks of which not one reaches ``listening``.
    stuck_cycles: int = 0

    def ready(self, role: str) -> Any:
        """The connection of ``role`` a caller can use right now, ``None`` when there is none.

        Pooled is not the same as usable: the connection object is pooled from the moment
        its connect starts, so that an attempt nobody waits for any more is still an
        attempt somebody can close, and until that connect lands every call made through
        it fails. Whoever asks while it is under way waits for that one attempt rather
        than opening a second connection to a broker that is already struggling.

        Nor is "not closed" the same as usable — see :func:`_usable`.
        """
        connection = self.connections.get(role)
        if connection is None or role in self.connecting or not _usable(connection):
            return None
        return connection


class RMQConsumerManager:
    """Manages a pool of asyncio tasks — one per subscription — each consuming one RMQ queue.

    Connection pooling: one ``connect_robust`` connection per ``(conn_id, role)``, where role
    is ``consume`` or ``publish``; multiple subscriptions sharing the same conn_id reuse the
    same consuming connection (each gets its own channel), while cooldown publishing runs on
    a lazily opened publish connection of that conn_id.
    """

    def __init__(
        self,
        executor: BoundedExecutor,
        cycle_executor: BoundedExecutor,
    ) -> None:
        # Two pools, never one: ``executor`` carries what a consumer task waits on,
        # ``cycle_executor`` what the reconcile cycle waits on. Sharing them would let
        # deliveries stuck on the database starve the cycle that is supposed to notice
        # and recover from exactly that. Both are owned by whoever builds the manager
        # and outlive the event loop, so neither is created here.
        self._executor = executor
        self._cycle_executor = cycle_executor
        self._active: dict[int, _ActiveSub] = {}  # sub_id → _ActiveSub
        self._conns: dict[str, _ConnState] = {}  # conn_id → its connections and bookkeeping
        self._cycle_no = 0  # liveness checks performed, i.e. reconcile cycles
        # (management_url, vhost, login) → queue → consumer tags, for the current cycle
        # only. The login is part of the key because the reply is shaped by the rights of
        # whoever asked: a user tagged ``management`` is shown only its own channels.
        self._consumer_cache: dict[tuple[str, str, str], dict[str, set[str]]] = {}
        self._fire_task: asyncio.Task | None = None
        self._fire_state: _FireSub | None = None  # state record of the running fire task
        self._fire_needs_restart = False  # last check found the fire consumer gone
        self._cooldown_tracker = OrphanTracker()  # dag_ids for which pending queues were created
        self._exchange_tracker = OrphanTracker()  # dag_ids for which sub queues/bindings were created
        self._http_client: httpx.AsyncClient | None = None  # Management API client
        # conn_ids the split-cooldown warning last named, so it is logged on change
        self._split_cooldown_warned: set[str] = set()
        #: Cancelled tasks that had not finished when the cycle stopped waiting for them.
        self._abandoned: set[asyncio.Task] = set()

    def _conn(self, conn_id: str) -> _ConnState:
        """The record of ``conn_id``, created empty the first time it is asked for."""
        state = self._conns.get(conn_id)
        if state is None:
            state = self._conns[conn_id] = _ConnState()
        return state

    def _abandon(self, tasks: set[asyncio.Task]) -> None:
        """Keep hold of cancelled ``tasks`` that outlived the wait for them.

        Recovery starts a replacement without them, so nothing else refers to such a
        task and asyncio itself keeps only a weak reference. Holding them here keeps
        each one alive until it really finishes — it lets go of itself the moment it
        does — and counts each one once, as it is abandoned, so
        ``rmq_watcher.tasks_abandoned`` counts tasks: a number that keeps growing means
        the connection they hang on answers nothing at all, which no reconnect of ours
        can mend.
        """
        fresh = {task for task in tasks if not task.done() and task not in self._abandoned}
        if not fresh:
            return
        for task in fresh:
            self._abandoned.add(task)
            task.add_done_callback(self._abandoned.discard)
            incr("rmq_watcher.tasks_abandoned")
        log.warning(
            "%d cancelled consumer task(s) have still not finished — they are kept "
            "until they do (%d in all)",
            len(fresh), len(self._abandoned),
        )

    async def start(self) -> None:
        """Create the shared Management API HTTP client. Connections/tasks are created on demand."""
        self._http_client = httpx.AsyncClient(timeout=_MGMT_HTTP_TIMEOUT)

    async def stop(self) -> None:
        tasks_to_cancel: list[asyncio.Task] = [
            entry.task for entry in self._active.values()
        ]
        if self._fire_task is not None and not self._fire_task.done():
            tasks_to_cancel.append(self._fire_task)

        for task in tasks_to_cancel:
            task.cancel()
        try:
            # A task that answers its cancellation slowly must not spend the whole
            # budget the caller allows this call: what is left of it is what closes the
            # connections, and the broker holds the consumers of an unclosed one until
            # the socket drops — beside the consumers of the loop that replaces this one.
            await _wait_cancelled(tasks_to_cancel, timeout=_STOP_CANCEL_TIMEOUT)
        finally:
            self._fire_task = None
            self._fire_state = None

            for conn_id in list(self._conns):
                await self._drop_connection(conn_id)
            self._active.clear()
            self._conns.clear()

            if self._http_client is not None:
                await _close_quietly(
                    self._http_client, "the Management API client", "aclose"
                )
                self._http_client = None

    async def reconcile(self, subscriptions: list[dict]) -> None:
        """Sync running tasks, connections and infrastructure with the subscription list.

        The order the steps run in is the point. Exchange provisioning goes first, and is
        awaited: exchange-mode queues are created by this provider (unlike ``queue=``
        mode, where the queue is created out-of-band and ``_consume_subscription`` always
        passive-declares it), and a brand-new consumer task would otherwise fail fatally
        on a passive declare against a queue that does not exist yet. Consumer tasks and
        the cooldown infrastructure follow.

        A task that is merely still running proves nothing, so the cycle ends by asking
        the broker which of our consumers it actually holds: one it does not know is
        rebuilt together with its connection, and the status row of every conn_id is
        written from that answer.
        """
        exchange_subs = [s for s in subscriptions if s.get("exchange")]
        await self._provision_exchange_subs(exchange_subs)

        await self._sync_consumer_tasks(subscriptions)
        await self._close_unreferenced_connections({s["conn_id"] for s in subscriptions})
        cooldown_dag_ids = await self._sync_fire_consumer(subscriptions)

        # Orphan check runs unconditionally so that removing a dag_id from an otherwise
        # active set of cooldown subscriptions is still detected even when RMQ provisioning
        # fails (i.e. _provision_cooldown returns early in its except block).
        self._check_orphaned_pending_queues(cooldown_dag_ids)

        # Same unconditional-orphan-check rationale as cooldown above, applied to
        # exchange-mode sub queues/bindings.
        self._check_orphaned_exchange_bindings({s["dag_id"] for s in exchange_subs})

        await self._recover_dead_consumers(subscriptions)

        await self._update_all_conn_counts(subscriptions)

        await self._store_unwritten_statuses()

    async def _store_unwritten_statuses(self) -> None:
        """Carry the statuses a database outage left unwritten into their rows.

        The consumer that writes a status has moved on by the time the database answers
        again, and one whose queue is quiet writes nothing further, so the cycle is what
        gets the row and the subscription back into agreement.
        """
        for entry in list(self._active.values()):
            await entry.state.flush(self._cycle_executor)
        fire = self._fire_state
        if fire is not None:
            await fire.state.flush(self._cycle_executor)

    async def _sync_consumer_tasks(self, subscriptions: list[dict]) -> None:
        """Cancel the tasks of removed subscriptions and start the ones that are missing.

        A subscription gets a fresh task when it has none, when its task has finished —
        a consumer only ever finishes on a fatal error — and when a field the running
        task read at startup has changed, which is what makes an edit in the UI take
        effect within a cycle.
        """
        new_ids = {sub["id"] for sub in subscriptions}

        to_remove = [sid for sid in list(self._active) if sid not in new_ids]
        for sub_id in to_remove:
            self._active[sub_id].task.cancel()
        if to_remove:
            self._abandon(
                await _wait_cancelled(
                    [self._active[sub_id].task for sub_id in to_remove]
                )
            )
            for sub_id in to_remove:
                # Through the state of the task that has just been cancelled: the
                # write it may have left running in a worker is a write of this same
                # row, and both go through the one writer of this subscription.
                entry = self._active.pop(sub_id, None)
                if entry is not None:
                    await entry.state.write(
                        _SUB_DISCONNECTED, executor=self._cycle_executor
                    )

        for sub in subscriptions:
            sub_id = sub["id"]
            entry = self._active.get(sub_id)
            if entry is None or entry.task.done() or self._subs_changed(sub_id, sub):
                if entry is not None and not entry.task.done():
                    entry.task.cancel()
                    self._abandon(await _wait_cancelled([entry.task]))
                task = asyncio.create_task(self._consume_subscription(sub))
                self._active[sub_id] = _ActiveSub(
                    task=task, sub=sub.copy(), state=_ConsumerState(sub_id, self._executor)
                )

    async def _close_unreferenced_connections(self, active_conn_ids: set[str]) -> None:
        """Close and forget every conn_id no subscription mentions any more."""
        for conn_id in [c for c in self._conns if c not in active_conn_ids]:
            await self._drop_connection(conn_id)
            self._conns.pop(conn_id, None)

    async def _sync_fire_consumer(self, subscriptions: list[dict]) -> set[str]:
        """Keep the cooldown infrastructure and its single fire consumer in place.

        :returns: The dag_ids that currently have a cooldown subscription, for the
            orphan check that runs whether or not provisioning succeeded.

        One fire consumer serves every cooldown subscription, and which conn_id carries
        it is decided by sorting rather than by the order the rows came back in: the
        query behind that list has no ORDER BY, PostgreSQL reorders rows an UPDATE
        touched, and every status transition updates exactly these rows. Reading the
        choice off that order would move the fire consumer during the reconnect
        turbulence this feature exists to survive, and each move costs a cancel.

        The task starts on a connection that is connected and answers, never on one
        whose connect is still in flight: an object is pooled from the moment its
        connect starts and reports ``is_closed`` False until it is closed, so a task
        started on it fails every call it makes for as long as it runs, and its own
        retry loop keeps it alive — and therefore out of every restart path.
        """
        cooldown_dag_ids: set[str] = set()
        cooldown_conn_ids: set[str] = set()
        for sub in subscriptions:
            if sub.get("cooldown", 0) > 0:
                cooldown_dag_ids.add(sub["dag_id"])
                cooldown_conn_ids.add(sub["conn_id"])
        fire_conn_id = min(cooldown_conn_ids) if cooldown_conn_ids else None
        self._report_split_cooldown(cooldown_conn_ids, fire_conn_id)

        if cooldown_dag_ids and fire_conn_id is not None:
            await self._provision_cooldown(cooldown_dag_ids, fire_conn_id)
            running = self._fire_state
            moved = running is not None and running.conn_id != fire_conn_id
            # Raw pool read rather than ``ready()``: the question is which object the
            # pool holds, not whether it can be used yet, and a connect in flight must
            # not read as a connection the fire consumer has to be taken off.
            pooled = self._conn(fire_conn_id).connections.get(_ROLE_CONSUME)
            replaced = running is not None and not moved and running.connection is not pooled
            if (moved or replaced) and self._fire_task is not None:
                # The fire consumer holds the connection object it was handed for its
                # whole life. Left running against the old conn_id, or against an object
                # the pool has replaced, it would keep rmq_watcher.fire without a
                # consumer and cooldown DAGs would silently stop firing.
                if moved:
                    log.info(
                        "Cooldown subscriptions moved from conn_id=%r to %r — restarting "
                        "the fire consumer on the new connection",
                        running.conn_id if running else None, fire_conn_id,
                    )
                else:
                    log.warning(
                        "The fire consumer of conn_id=%r holds a connection the pool no "
                        "longer has — restarting it on the pooled one",
                        fire_conn_id,
                    )
                self._fire_task.cancel()
                self._abandon(await _wait_cancelled([self._fire_task]))
                self._fire_task = None
                self._fire_state = None
            if self._fire_task is None or self._fire_task.done():
                connection = self._conn(fire_conn_id).ready(_ROLE_CONSUME)
                if connection is not None:
                    self._launch_fire_task(fire_conn_id, connection)
                else:
                    log.warning(
                        "Fire task cannot start: conn_id=%r has no connection after provisioning",
                        fire_conn_id,
                    )
        elif not cooldown_dag_ids:
            if self._fire_task is not None and not self._fire_task.done():
                self._fire_task.cancel()
                self._abandon(await _wait_cancelled([self._fire_task]))
            self._fire_task = None
            self._fire_state = None

        return cooldown_dag_ids

    def _report_split_cooldown(
        self, cooldown_conn_ids: set[str], fire_conn_id: str | None
    ) -> None:
        """Report cooldown subscriptions spread over more than one broker as an error.

        The pending queues and the fire exchange are declared on ``fire_conn_id`` alone,
        while a matched delivery is published to ``rmq_watcher.pending.{dag_id}`` on the
        conn_id of the subscription that received it. On any other conn_id that queue
        does not exist, the mandatory publish comes back unrouted, the delivery is
        requeued and the DAG never fires — so the configuration is named out loud, once
        per change, rather than failing silently per message.
        """
        split = cooldown_conn_ids if len(cooldown_conn_ids) > 1 else set()
        if split == self._split_cooldown_warned:
            return
        self._split_cooldown_warned = set(split)
        if not split:
            return
        log.error(
            "Cooldown subscriptions span %d connections (%s), and cooldown "
            "infrastructure is provisioned on %r only. Deliveries matched on the other "
            "connection(s) cannot be routed to their pending queue and their DAGs will "
            "not fire — put every cooldown subscription on one conn_id.",
            len(split), ", ".join(sorted(split)), fire_conn_id,
        )

    def _subs_changed(self, sub_id: int, new_sub: dict) -> bool:
        """Compare snapshot of running sub with new sub on fields that affect consumer behaviour."""
        entry = self._active.get(sub_id)
        if entry is None:
            return True
        old = entry.sub
        return (
            old.get("queue_name") != new_sub.get("queue_name")
            or old.get("dag_id") != new_sub.get("dag_id")
            or old.get("cooldown", 0) != new_sub.get("cooldown", 0)
            or old.get("filter_data") != new_sub.get("filter_data")
            or old.get("conn_id") != new_sub.get("conn_id")
        )

    async def _provision_cooldown(
        self, cooldown_dag_ids: set[str], conn_id: str
    ) -> None:
        """Create fire exchange/queue and pending queues for all cooldown DAGs.

        Idempotent — safe to call on every reconcile cycle.
        Error handling: if RMQ is unavailable or permissions are missing, logs ERROR
        and returns without raising so ordinary consumers continue to work.
        """
        try:
            connection = await self._get_or_create_connection(
                conn_id, executor=self._cycle_executor
            )
            rpc_timeout = self._rpc_timeout(conn_id)
            # Use a short-lived channel for setup operations
            setup_channel = await call_with_timeout(connection.channel(), timeout=rpc_timeout)
            try:
                await _ensure_fire_infrastructure(setup_channel, timeout=rpc_timeout)
                for dag_id in cooldown_dag_ids:
                    await _ensure_pending_queue(setup_channel, dag_id, timeout=rpc_timeout)
            finally:
                await _close_quietly(
                    setup_channel, f"the cooldown setup channel of conn_id={conn_id!r}"
                )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.error(
                "Failed to provision cooldown infrastructure (exchange=%r, conn_id=%r): %s. "
                "Ordinary consumers continue. Will retry on next reconcile cycle.",
                _FIRE_EXCHANGE, conn_id, exc,
            )
            return

        # Update tracking: accumulate all dag_ids that ever had cooldown infra provisioned
        self._cooldown_tracker.mark_provisioned(cooldown_dag_ids)

    def _check_orphaned_pending_queues(self, active_cooldown_dag_ids: set[str]) -> None:
        """Log WARNING for pending queues that no longer have an active cooldown subscription.

        Called unconditionally at the end of reconcile() — regardless of whether RMQ
        provisioning succeeded — so orphan detection always fires even when RMQ is down.
        """
        newly_orphaned, restored = self._cooldown_tracker.diff(active_cooldown_dag_ids)

        if newly_orphaned:
            for dag_id in sorted(newly_orphaned):
                log.warning(
                    "Pending queue rmq_watcher.pending.%s is now orphaned (subscription removed). "
                    "The TTL timer continues in RMQ. To clean up manually: "
                    "rabbitmqadmin delete queue name=rmq_watcher.pending.%s",
                    dag_id, dag_id,
                )

        if restored:
            for dag_id in sorted(restored):
                log.info(
                    "Subscription for DAG %r restored — removing from orphaned pending set.",
                    dag_id,
                )

    def _check_orphaned_exchange_bindings(self, active_exchange_dag_ids: set[str]) -> None:
        """Log WARNING for exchange-mode sub queues/bindings that no longer have an active
        ``exchange=`` subscription.

        Called unconditionally at the end of reconcile() — regardless of whether
        ``_provision_exchange_subs`` succeeded — so orphan detection always fires even when
        RMQ or the Management API is down.

        Bindings are NOT unbound automatically when a dag_id becomes orphaned — see
        ADR-0005: auto-unbind was rejected because it can't distinguish a transient
        AST-parse failure (DAG comes back with no loss — the queue TTL bounds growth) from
        a permanent DAG removal/rename (then this is a bounded leak until manual cleanup).
        """
        newly_orphaned, restored = self._exchange_tracker.diff(active_exchange_dag_ids)

        if newly_orphaned:
            for dag_id in sorted(newly_orphaned):
                log.warning(
                    "Sub queue %s%s is now orphaned (exchange= subscription removed). "
                    "Bindings are not removed automatically; the TTL safety net continues in "
                    "RMQ. To clean up manually: rabbitmqadmin delete queue name=%s%s",
                    _SUB_QUEUE_PREFIX, dag_id, _SUB_QUEUE_PREFIX, dag_id,
                )

        if restored:
            for dag_id in sorted(restored):
                log.info(
                    "Exchange subscription for DAG %r restored — removing from orphaned "
                    "sub queue set.",
                    dag_id,
                )

    async def _update_all_conn_counts(self, subscriptions: list[dict]) -> None:
        """Write the status row of every conn_id the subscription list mentions.

        A conn_id whose tasks have all died still gets a row: a row that simply stops
        being updated is indistinguishable from a healthy one, so every conn_id is
        stamped on every cycle whether or not anything is consuming on it.

        ``status`` follows the verdict of the liveness check rather than the number of
        running tasks — a task that is not done proves nothing about the broker. A check
        that produced no data leaves the stored status alone, so an unreachable
        Management API does not paint every connection red; a conn_id no check has ever
        reached a verdict on starts at ``unknown``, while ``last_reconcile_at``
        is written with an explicit value on every cycle: without it SQLAlchemy emits no
        UPDATE at all once the other fields stop changing, and the timestamp freezes.
        """
        counts: dict[str, int] = {}
        for sub in subscriptions:
            conn_id = sub["conn_id"]
            entry = self._active.get(sub["id"])
            alive = 1 if entry is not None and not entry.task.done() else 0
            counts[conn_id] = counts.get(conn_id, 0) + alive

        # The fire consumer is one of our consumers on the broker too, so it counts on
        # this side as well. Both numbers then cover the same set and a conn_id with
        # cooldown subscriptions compares equal on a healthy system.
        fire = self._fire_state
        if (
            fire is not None
            and self._fire_task is not None
            and not self._fire_task.done()
            and fire.conn_id in counts
        ):
            counts[fire.conn_id] += 1

        if not counts:
            return

        # Naive UTC: the view compares this stamp with a naive utcnow() of its own, and
        # mixing naive and aware values raises straight from the template.
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        rows = []
        for conn_id, count in counts.items():
            state = self._conns.get(conn_id)
            verdict = state.liveness if state is not None else None
            rows.append((
                conn_id,
                count,
                verdict.status if verdict is not None else None,
                verdict.reason if verdict is not None else None,
                verdict.broker_consumer_count if verdict is not None else None,
            ))

        try:
            await call_with_timeout(
                self._cycle_executor.run(_write_conn_status_rows, rows, now),
                timeout=_DB_TIMEOUT,
            )
        except Exception as exc:
            log.warning("Cannot write connection status rows: %s", exc)

    def _rpc_timeout(self, conn_id: str) -> float:
        """Seconds allowed for a single AMQP RPC on ``conn_id``.

        The value comes from the connection's ``extra`` and is cached when the
        connection is built; before that the provider default applies.
        """
        state = self._conns.get(conn_id)
        timeouts = state.timeouts if state is not None else None
        return timeouts.rpc if timeouts else DEFAULT_RPC_TIMEOUT

    async def _drop_connection(self, conn_id: str, role: str | None = None) -> None:
        """Close and forget the connection(s) of ``conn_id`` — both roles, or one named.

        The entry leaves the pool before ``close()`` is attempted, so a connection whose
        close hangs is still gone from the cache: a zombie connection answers every call
        with silence, and keeping it would hand the same dead object to the next caller.
        """
        state = self._conn(conn_id)
        roles = (role,) if role is not None else (_ROLE_CONSUME, _ROLE_PUBLISH)
        for pooled_role in roles:
            # A connect still in flight is cancelled with the connection it was building:
            # closing cancels the factory that would have resolved it, so left alone it
            # would wait for an answer that can no longer come.
            task = state.connecting.pop(pooled_role, None)
            if task is not None and not task.done():
                task.cancel()
            connection = state.connections.pop(pooled_role, None)
            if pooled_role == _ROLE_PUBLISH:
                state.publish_channel = None
            if connection is None:
                continue
            await _close_quietly(
                connection, f"the {pooled_role} connection of conn_id={conn_id!r}"
            )
        if role is None:
            # Only a full drop feeds the liveness rate limit: the publish role has its
            # own gate of consecutive publish timeouts, and letting it move this mark
            # would postpone recovery of a consuming connection that went silent at the
            # same time.
            state.last_drop_cycle = self._cycle_no

    def _may_drop_connection(self, conn_id: str) -> bool:
        """Whether ``conn_id`` may be recreated in this cycle.

        The same connection is torn down at most once every
        :data:`_CYCLES_BEFORE_REDROP` cycles, so a verdict that keeps coming back
        negative — a misclassification, or a fault that lives outside the connection —
        surfaces as a rare logged event instead of a silent recreation loop.
        """
        last_cycle = self._conn(conn_id).last_drop_cycle
        return last_cycle is None or self._cycle_no - last_cycle >= _CYCLES_BEFORE_REDROP

    async def _get_publish_channel(self, conn_id: str) -> Any:
        """Return a channel on the publish connection of ``conn_id``, opening it on demand.

        The channel raises on a returned message. A cooldown placeholder is published
        ``mandatory``, and the broker answers an unroutable one — the pending queue is
        not there — with ``basic.return`` followed by an ack. On aio_pika's default
        channel that pair resolves the publish successfully and hands the returned
        message back as its result, so a caller that only watches for exceptions would
        acknowledge the delivery behind an event that reached no queue at all.
        """
        state = self._conn(conn_id)
        channel = state.publish_channel
        if channel is not None and not channel.is_closed:
            return channel
        connection = await self._get_or_create_connection(conn_id, role=_ROLE_PUBLISH)
        channel = await call_with_timeout(
            connection.channel(on_return_raises=True),
            timeout=self._rpc_timeout(conn_id),
        )
        state.publish_channel = channel
        return channel

    async def _get_connection_info(
        self, conn_id: str, executor: BoundedExecutor | None = None
    ) -> Any:
        """Read the Airflow connection of ``conn_id`` off the loop thread, under a timeout.

        ``BaseHook.get_connection`` is a metadata-database query, and a database that
        stopped answering must not decide how long a reconcile cycle lasts.
        """
        pool = executor if executor is not None else self._executor
        return await call_with_timeout(
            pool.run(BaseHook.get_connection, conn_id), timeout=_DB_TIMEOUT
        )

    async def _get_or_create_connection(
        self,
        conn_id: str,
        role: str = _ROLE_CONSUME,
        executor: BoundedExecutor | None = None,
    ) -> Any:
        """Return the pooled connection of ``(conn_id, role)``, building it on demand.

        ``executor`` names the pool the blocking metadata read runs in: a consumer task
        uses the consumer pool, everything the reconcile cycle awaits uses the cycle
        pool, so a stalled delivery and a stalled cycle never share a worker.

        The metadata read stays outside the lock, the lock belongs to this one conn_id
        and it covers starting the connect rather than waiting for it, so what a broker
        that stopped answering costs any caller is one ``connect_timeout``. What the
        three together rule out is a wait that grows with the number of subscriptions:
        inherited by the reconcile cycle's own probes it outlasts the cycle's budget,
        which tears the manager down and cancels the consumer tasks of the healthy
        brokers along with it.

        The connection object is pooled before it is connected and stays pooled while its
        connect is in flight: an attempt nobody waits for any more is still an attempt
        somebody has to close, and the caller that comes next waits on that same attempt
        instead of opening a second connection to a broker that is already struggling.
        A connect that *failed* takes its connection with it — the object holds the
        failure and hands it to every later ``connect()`` — so the next attempt starts
        from a fresh one.
        """
        pool = executor if executor is not None else self._executor
        state = self._conn(conn_id)
        # Fast path: a usable connection is already pooled
        connection = state.ready(role)
        if connection is not None:
            return connection

        conn_info = await self._get_connection_info(conn_id, pool)
        url, ssl_context = build_amqp_connection(conn_info)
        timeouts = get_amqp_timeouts(conn_info)
        try:
            return await self._connect_pooled(conn_id, role, url, ssl_context, timeouts)
        except Exception as exc:
            # The lock is already released: writing the row is a database call, and a
            # database that stopped answering must not decide how long the next caller
            # waits for the broker.
            #
            # The row describes the consuming connection, and only a consume-role failure
            # writes it. Under a resource alarm the publish connection is the one the
            # broker blocks, while consumers of the same conn_id keep working and the
            # broker keeps confirming their tags: a publish failure written here would
            # overwrite that verdict with `error` and no consumers, once per cooldown
            # retry, until the next cycle rewrote it. The publish role reports through
            # its own consecutive-timeout gate and the subscription that publishes.
            if role == _ROLE_CONSUME:
                await self._record_conn_error(conn_id, exc, pool)
            raise

    async def _connect_pooled(
        self,
        conn_id: str,
        role: str,
        url: str,
        ssl_context: Any,
        timeouts: AmqpTimeouts,
    ) -> Any:
        """Connect the pooled connection of ``(conn_id, role)``, one attempt at a time.

        The lock covers starting the attempt and nothing else. What every caller then
        waits for is the one task that attempt created, and they wait for it side by
        side: a broker that answers nothing costs each of them ``connect_timeout`` once,
        not ``connect_timeout`` times the number of callers ahead of them. Held across
        the wait instead, the lock would hand the reconcile cycle a queue that grows with
        the subscriptions of that conn_id until it outlasts the cycle's own budget —
        which tears the manager down and cancels the consumer tasks of every healthy
        broker with it.

        The wait is bounded but the attempt behind it is not: on Python 3.10 a connect
        that times out raises ``asyncio.TimeoutError``, which is not one of aio_pika's
        ``CONNECTION_EXCEPTIONS``, so its reconnect factory logs it, sleeps and tries
        again while ``connect()`` waits on a future that is never resolved.
        """
        state = self._conn(conn_id)
        what = f"the {role} connection of conn_id={conn_id!r}"
        discarded: Any = None
        async with state.lock:
            connection = state.ready(role)
            if connection is not None:
                return connection
            state.timeouts = timeouts
            connection = state.connections.get(role)

            attempt = state.connecting.get(role)
            in_flight = attempt is not None and not attempt.done()
            # A connect still under way keeps its connection: that object is what every
            # waiter is waiting on, and it has no transport until the attempt lands.
            # Once no attempt is left, one without a transport is a reconnect that never
            # finished, and it answers every call with the same failure forever.
            if connection is None or (not in_flight and not _usable(connection)):
                stale = state.connecting.pop(role, None)
                if stale is not None and not stale.done():
                    # It was building the connection being replaced, and the factory that
                    # would have answered it is gone with it.
                    stale.cancel()
                discarded = connection
                connection = _new_connection(url, ssl_context)
                state.connections[role] = connection
                if role == _ROLE_PUBLISH:
                    state.publish_channel = None

            task = state.connecting.get(role)
            if task is None or task.done():
                task = asyncio.ensure_future(connection.connect(timeout=timeouts.connect))
                task.add_done_callback(_retrieve_outcome)
                state.connecting[role] = task

        if discarded is not None and not discarded.is_closed:
            # Closed outside the lock, and closed rather than dropped: the object owns a
            # reconnect task that goes on trying for the life of the process, and the
            # socket under it is the broker's to keep until somebody says otherwise.
            await _close_quietly(discarded, f"the replaced {what}")

        try:
            await _await_connected(task, timeouts.connect, what)
        except asyncio.CancelledError:
            if not task.cancelled():
                # The caller is the one being cancelled — pass it on untouched.
                raise
            # The connect was cancelled under this caller: recovery dropped the
            # connection while the caller was waiting for it. Reported as a cancellation
            # it would end the consumer task through its own ``except CancelledError``
            # branch — silently, with no status and no restart — where the drop it
            # followed is an ordinary transient failure the task retries through.
            raise ConnectionError(f"{what} was dropped while it connected") from None
        except Exception:
            if task.done():
                # The connect failed rather than outstayed its bound. The connection
                # object holds that failure and answers every later ``connect()`` with
                # it, so it goes and the next attempt starts from a fresh one.
                async with state.lock:
                    if state.connections.get(role) is connection:
                        await self._drop_connection(conn_id, role=role)
            raise

        async with state.lock:
            if state.connecting.get(role) is task:
                state.connecting.pop(role, None)
            if state.connections.get(role) is connection:
                return connection
            replacement = state.ready(role)

        # Recovery replaced the pooled connection while this attempt was landing. The
        # object this call built belongs to nobody now, and holding an open connection
        # the pool cannot reach costs the broker a connection for the life of the
        # process.
        log.info("%s was replaced while it connected — closing the connection it built", what)
        await _close_quietly(connection, what)
        if replacement is not None:
            return replacement
        raise ConnectionError(f"{what} was replaced while it connected")

    async def _record_conn_error(
        self, conn_id: str, exc: Exception, pool: BoundedExecutor
    ) -> None:
        """Store a failed connection attempt of ``conn_id``, best effort."""
        try:
            await call_with_timeout(
                pool.run(_write_conn_error, conn_id, _error_text(exc)), timeout=_DB_TIMEOUT
            )
        except Exception as write_exc:
            log.warning(
                "Cannot store the failed connection attempt for conn_id=%r: %s",
                conn_id, write_exc,
            )

    async def _provision_one_exchange_sub(
        self,
        setup_channel: Any,
        exchange: str,
        sub: dict,
        http_client: httpx.AsyncClient,
        rpc_timeout: float,
    ) -> bool:
        """Provision the sub queue + bind-diff for a single exchange-mode subscription.

        Returns ``True`` if ``dag_id`` should be marked provisioned, ``False`` otherwise.
        Raises ``aio_pika.exceptions.ChannelClosed`` (and all its subclasses, e.g.
        ``ChannelPreconditionFailed``, ``ChannelNotFoundEntity``, ``DuplicateConsumerTag``)
        unchanged so the caller can reopen ``setup_channel`` — any broker-side channel
        close (not just PRECONDITION_FAILED) makes the channel unusable for every
        subsequent RPC — every other exception is caught and logged here, isolated to
        this one subscription.
        """
        dag_id = sub["dag_id"]
        conn_id = sub["conn_id"]
        try:
            queue = await _ensure_sub_queue(setup_channel, dag_id, timeout=rpc_timeout)

            conn_info = await self._get_connection_info(conn_id, self._cycle_executor)
            vhost = conn_info.schema or "/"
            management_url = get_management_url(conn_info)
            if management_url is None:
                log.error(
                    "management_url not set on conn_id=%r — skipping "
                    "bind-diff for DAG %r (queue %s%s still declared, will "
                    "retry next cycle)",
                    conn_id, dag_id, _SUB_QUEUE_PREFIX, dag_id,
                )
                return True

            if conn_info.login is None or conn_info.password is None:
                log.error(
                    "conn_id=%r has no login/password set — skipping "
                    "bind-diff for DAG %r (queue %s%s still declared, will "
                    "retry next cycle)",
                    conn_id, dag_id, _SUB_QUEUE_PREFIX, dag_id,
                )
                return False

            auth = (conn_info.login, conn_info.password)
            queue_name = f"{_SUB_QUEUE_PREFIX}{dag_id}"
            try:
                current = await get_current_bindings(
                    http_client, management_url, vhost, queue_name, exchange, auth,
                )
            except Exception as exc:
                log.warning(
                    "Management API bind-diff failed for DAG %r (queue %s, "
                    "exchange %r): %s — skipping bind-diff this cycle, queue "
                    "still declared, retried next cycle",
                    dag_id, queue_name, exchange, exc,
                )
                return True

            desired = set(sub.get("routing_keys") or [])
            await _sync_bindings(queue, exchange, desired, current, timeout=rpc_timeout)
        except asyncio.CancelledError:
            raise
        except aio_pika.exceptions.ChannelClosed:
            raise
        except Exception as exc:
            log.error(
                "Failed to provision sub queue for DAG %r (exchange=%r, "
                "conn_id=%r): %s. Other DAGs in this exchange group "
                "continue. Will retry on next reconcile cycle.",
                dag_id, exchange, conn_id, exc,
            )
            return False
        else:
            return True

    async def _provision_exchange_subs(self, exchange_subs: list[dict]) -> None:
        """Provision exchange/sub-queue infrastructure and sync bindings for all
        ``exchange=`` subscriptions.

        Idempotent — safe to call on every reconcile cycle. Subscriptions are grouped by
        ``(conn_id, exchange)`` so the exchange/AE/log infrastructure is declared once per
        group, and the sub queue + bind-diff is handled once per subscription within the
        group. Errors in one group (connection failure, RMQ unavailable, Management API
        unavailable, conflicting exchange properties) are logged and do not prevent other
        groups — or the ordinary ``queue=`` consumer start-up that follows in
        ``reconcile()`` — from proceeding.
        """
        if not exchange_subs:
            return

        http_client = self._http_client
        if http_client is None:
            log.error(
                "Management API HTTP client not initialized (start() not called?) — "
                "skipping provisioning for %d exchange-mode subscription(s) this cycle",
                len(exchange_subs),
            )
            return

        groups: dict[tuple[str, str], list[dict]] = {}
        for sub in exchange_subs:
            key = (sub["conn_id"], sub["exchange"])
            groups.setdefault(key, []).append(sub)

        for (conn_id, exchange), group in groups.items():
            await self._provision_exchange_group(
                conn_id, exchange, group, http_client
            )

    async def _provision_exchange_group(
        self, conn_id: str, exchange: str, group: list[dict], http_client: httpx.AsyncClient
    ) -> None:
        """Declare one exchange and the sub queue of every subscription behind it.

        The exchange, its ``.unrouted``/``.log`` safety-net queues and the group's setup
        channel are declared once for the whole group; the sub queue and its bind-diff
        follow per subscription. Everything that can go wrong here — an unreachable
        broker, a missing permission, an exchange that already exists with other
        properties — is logged and left for the next cycle: this group is one of several,
        and the ordinary ``queue=`` consumers start regardless.
        """
        try:
            connection = await self._get_or_create_connection(
                conn_id, executor=self._cycle_executor
            )
            rpc_timeout = self._rpc_timeout(conn_id)
            setup_channel = await call_with_timeout(
                connection.channel(), timeout=rpc_timeout
            )
            try:
                await _ensure_exchange_infrastructure(
                    setup_channel, exchange, timeout=rpc_timeout
                )

                for sub in group:
                    dag_id = sub["dag_id"]
                    try:
                        provisioned = await self._provision_one_exchange_sub(
                            setup_channel, exchange, sub, http_client, rpc_timeout,
                        )
                    except asyncio.CancelledError:
                        raise
                    except aio_pika.exceptions.ChannelClosed as exc:
                        if isinstance(exc, aio_pika.exceptions.ChannelPreconditionFailed):
                            log.error(
                                "Sub queue %s%s already exists with conflicting "
                                "properties (PRECONDITION_FAILED) on conn_id=%r: %s. "
                                "This DAG's subscription is not provisioned this "
                                "cycle. Will retry on next reconcile cycle.",
                                _SUB_QUEUE_PREFIX, dag_id, conn_id, exc,
                            )
                        else:
                            log.error(
                                "Broker closed the channel while provisioning sub "
                                "queue %s%s (exchange=%r, conn_id=%r): %s. This DAG's "
                                "subscription is not provisioned this cycle. Will "
                                "retry on next reconcile cycle.",
                                _SUB_QUEUE_PREFIX, dag_id, exchange, conn_id, exc,
                            )
                        # Any ChannelClosed subclass (PRECONDITION_FAILED,
                        # ChannelNotFoundEntity, DuplicateConsumerTag, etc.) closes
                        # the entire broker-side channel — any further RPC on
                        # setup_channel would raise for every remaining DAG in this
                        # group. Open a fresh channel so subsequent subscriptions in
                        # the same (conn_id, exchange) group are not collaterally
                        # broken by this one DAG's conflict.
                        try:
                            setup_channel = await call_with_timeout(
                                connection.channel(), timeout=rpc_timeout
                            )
                        except Exception as reopen_exc:
                            log.error(
                                "Failed to reopen channel on conn_id=%r after "
                                "channel close: %s. Remaining DAGs in this "
                                "exchange group (%r) cannot be provisioned this cycle.",
                                conn_id, reopen_exc, exchange,
                            )
                            break
                        continue
                    if provisioned:
                        self._exchange_tracker.mark_provisioned({dag_id})
            finally:
                await _close_quietly(
                    setup_channel,
                    f"the provisioning channel of conn_id={conn_id!r}",
                )
        except asyncio.CancelledError:
            raise
        except aio_pika.exceptions.ChannelClosed as exc:
            if isinstance(exc, aio_pika.exceptions.ChannelPreconditionFailed):
                log.error(
                    "Exchange %r already exists with conflicting properties "
                    "(PRECONDITION_FAILED) on conn_id=%r: %s. Sub queues for this "
                    "group are not provisioned. Will retry on next reconcile cycle.",
                    exchange, conn_id, exc,
                )
            else:
                log.error(
                    "Broker closed the channel while declaring exchange "
                    "infrastructure (exchange=%r, conn_id=%r): %s. Sub queues for "
                    "this group are not provisioned. Will retry on next reconcile "
                    "cycle.",
                    exchange, conn_id, exc,
                )
            return
        except Exception as exc:
            log.error(
                "Failed to provision exchange infrastructure (exchange=%r, conn_id=%r): "
                "%s. Ordinary consumers continue. Will retry on next reconcile cycle.",
                exchange, conn_id, exc,
            )
            return

    def _state_of(self, sub_id: int) -> _ConsumerState:
        """State record the manager keeps for a subscription.

        The consumer task reports its status into this object, which is what lets the
        manager tell a subscription that believes it is listening from one still
        connecting or backing off after an error.
        """
        entry = self._active.get(sub_id)
        if entry is None:
            # The task is started from the same statement that registers the entry, so
            # this cannot happen; a status written into an object the manager does not
            # hold would leave the subscription out of the liveness check for good.
            log.error(
                "Subscription %s has no state record in the manager — its status is "
                "reported nowhere and the liveness check will skip it",
                sub_id,
            )
            return _ConsumerState(sub_id, self._executor)
        return entry.state

    def _fire_state_of(self) -> _ConsumerState:
        """State record the manager keeps for the fire consumer."""
        if self._fire_state is None:
            log.error(
                "The fire consumer has no state record in the manager — its status is "
                "reported nowhere and the liveness check will skip it"
            )
            return _ConsumerState(None, self._executor)
        return self._fire_state.state

    def _launch_fire_task(self, conn_id: str, connection: Any) -> None:
        """Start the fire consumer on an open connection of ``conn_id``.

        The conn_id is recorded next to the task: the fire consumer keeps the
        connection object it was handed for its whole life, so whoever recreates that
        connection has to know which task is holding the old one.
        """
        self._fire_state = _FireSub(
            conn_id=conn_id,
            state=_ConsumerState(None, self._executor),
            connection=connection,
        )
        self._fire_task = asyncio.create_task(
            self._consume_fire_queue(connection, conn_id)
        )

    def _restart_reason(self, conn_id: str | None) -> str:
        """Why the last liveness check condemned ``conn_id``, for the restart log."""
        state = self._conns.get(conn_id) if conn_id is not None else None
        verdict = state.liveness if state is not None else None
        if verdict is not None and verdict.reason:
            return verdict.reason
        return "the broker does not hold our consumer"

    async def _recover_dead_consumers(self, subscriptions: list[dict]) -> None:
        """Ask the broker what it still holds and rebuild whatever it does not.

        Recovery recreates the connection, not just the task: a zombie connection
        answers every call with silence, so a task restarted on the pooled object hangs
        exactly where its predecessor did. The fire consumer goes down with that
        connection because it holds the object it was handed at startup for its whole
        life and never finishes on its own — left running it would spin on a closed
        connection while ``rmq_watcher.fire`` sits without a consumer.
        """
        to_restart, to_recreate = await self._check_subscription_liveness(subscriptions)
        fire = self._fire_state
        restart_fire = self._fire_needs_restart or (
            fire is not None and fire.conn_id in to_recreate
        )
        if not to_restart and not to_recreate and not restart_fire:
            return

        by_id = {sub["id"]: sub for sub in subscriptions}
        cancelled: list[asyncio.Task] = []

        for sub_id in sorted(to_restart):
            entry = self._active.pop(sub_id, None)
            if entry is None:
                continue
            log.warning(
                "Restarting consumer of subscription %d (queue %r, conn_id=%r): %s",
                sub_id, entry.sub.get("queue_name"), entry.sub.get("conn_id"),
                self._restart_reason(entry.sub.get("conn_id")),
            )
            entry.task.cancel()
            cancelled.append(entry.task)

        fire_conn_id = fire.conn_id if fire is not None else None
        if restart_fire:
            log.warning(
                "Restarting the fire consumer on conn_id=%r: %s",
                fire_conn_id, self._restart_reason(fire_conn_id),
            )
            if self._fire_task is not None:
                self._fire_task.cancel()
                cancelled.append(self._fire_task)
            self._fire_task = None
            self._fire_state = None

        self._abandon(await _wait_cancelled(cancelled))

        for conn_id in sorted(to_recreate):
            await self._drop_connection(conn_id)

        for sub_id in sorted(to_restart):
            sub = by_id.get(sub_id)
            if sub is None:
                continue
            self._active[sub_id] = _ActiveSub(
                task=asyncio.create_task(self._consume_subscription(sub)),
                sub=sub.copy(),
                state=_ConsumerState(sub_id, self._executor),
            )
            incr("rmq_watcher.consumer_restarted")

        if restart_fire and fire_conn_id is not None:
            try:
                connection = await self._get_or_create_connection(
                    fire_conn_id, executor=self._cycle_executor
                )
            except Exception as exc:
                log.warning(
                    "Fire consumer cannot restart: conn_id=%r is unavailable: %s — "
                    "the next reconcile cycle tries again",
                    fire_conn_id, exc,
                )
            else:
                self._launch_fire_task(fire_conn_id, connection)
                incr("rmq_watcher.consumer_restarted")

    async def _check_subscription_liveness(
        self, subscriptions: list[dict]
    ) -> tuple[set[int], set[str]]:
        """Ask the broker whether our consumers are still registered.

        :param subscriptions: The subscription list of the current reconcile cycle.
        :returns: ``(sub_ids to restart, conn_ids to recreate)``.

        Only a subscription whose own state says ``listening`` is examined: one still
        connecting or backing off after an error is being handled by its own retry loop.
        A consumer is alive when its own ``consumer_tag`` is registered on the broker —
        a plain consumer count would be satisfied by a foreign client or by the second
        scheduler replica, which is exactly what masks a zombie of ours.

        A verdict needs :data:`_NEGATIVE_CHECKS_BEFORE_RESTART` negative checks in a row;
        a single positive one clears the counter. A check that produced no data — the
        Management API being unreachable — leaves the counters untouched, whereas an AMQP
        probe that fails *or hangs* counts as negative: silence on an AMQP call is the
        signature of the zombie connection this watchdog exists for.

        A conn_id that offers no candidate at all is judged on its own terms by
        :meth:`_judge_without_candidates`.

        The verdict of each conn_id is kept on its :class:`_ConnState` for the status
        writer.
        """
        self._cycle_no += 1
        for state in self._conns.values():
            state.liveness = None
        self._consumer_cache = {}
        self._fire_needs_restart = False

        candidates, stalled, live_tasks = self._partition_candidates(subscriptions)

        to_restart: set[int] = set()
        to_recreate: set[str] = set()

        for conn_id, subs in candidates.items():
            dead_subs, recreate = await self._judge_candidates(conn_id, subs)
            to_restart |= dead_subs
            if recreate:
                to_recreate.add(conn_id)

        for conn_id, live in live_tasks.items():
            if conn_id in candidates:
                continue
            if await self._judge_without_candidates(
                conn_id, live, stalled.get(conn_id, [])
            ):
                to_recreate.add(conn_id)

        return to_restart, to_recreate

    def _fire_candidate(self) -> _FireSub | None:
        """The fire consumer, when it is attached and can therefore be asked about."""
        fire = self._fire_state
        if (
            fire is None
            or fire.state.consumer_tag is None
            or fire.state.status != _SUB_LISTENING
            or self._fire_task is None
            or self._fire_task.done()
        ):
            return None
        return fire

    def _partition_candidates(
        self, subscriptions: list[dict]
    ) -> tuple[
        dict[str, list[tuple[dict, _ActiveSub]]], dict[str, list[dict]], dict[str, int]
    ]:
        """Sort this cycle's subscriptions by what the broker can be asked about them.

        :returns: ``(candidates, stalled, live_tasks)`` — per conn_id, the subscriptions
            whose own state says ``listening`` together with their entry, those with a
            running task that is not attached, and how many tasks are live in total.

        Every conn_id of the subscription list appears in ``live_tasks``, whether or not
        anything is running on it: a conn_id whose tasks have all died still needs a
        verdict, and the count of zero is that verdict.
        """
        candidates: dict[str, list[tuple[dict, _ActiveSub]]] = {}
        stalled: dict[str, list[dict]] = {}
        live_tasks: dict[str, int] = {}
        for sub in subscriptions:
            conn_id = sub["conn_id"]
            live_tasks.setdefault(conn_id, 0)
            entry = self._active.get(sub["id"])
            if entry is None or entry.task.done():
                continue
            live_tasks[conn_id] += 1
            if entry.state.status != _SUB_LISTENING or entry.state.consumer_tag is None:
                stalled.setdefault(conn_id, []).append(sub)
                continue
            candidates.setdefault(conn_id, []).append((sub, entry))

        fire = self._fire_candidate()
        if fire is not None:
            candidates.setdefault(fire.conn_id, [])
        return candidates, stalled, live_tasks

    async def _judge_candidates(
        self, conn_id: str, subs: list[tuple[dict, _ActiveSub]]
    ) -> tuple[set[int], bool]:
        """Put the attached consumers of one conn_id to the broker and judge the answer.

        :returns: ``(sub_ids to restart, whether to recreate the connection)``.
        """
        state = self._conn(conn_id)
        state.stuck_cycles = 0
        fire = self._fire_candidate()
        fire_here = fire is not None and fire.conn_id == conn_id
        queues = {sub["queue_name"] for sub, _ in subs}
        expected_tags = {entry.state.consumer_tag for _, entry in subs}
        if fire_here and fire is not None:
            queues.add(_FIRE_QUEUE)
            expected_tags.add(fire.state.consumer_tag)

        live_tags, broker_count, reason = await self._probe_consumers(
            conn_id, queues, expected_tags
        )
        if live_tags is None:
            state.liveness = _ConnLiveness(
                status=None, broker_consumer_count=None, reason=reason
            )
            return set(), False

        unseen: set[int] = set()
        dead_subs: set[int] = set()
        for sub, entry in subs:
            seen = entry.state.consumer_tag in live_tags
            if entry.record(seen):
                dead_subs.add(sub["id"])
            if seen:
                continue
            unseen.add(sub["id"])
            log.warning(
                "Broker does not know consumer %s of subscription %d (queue %r, "
                "conn_id=%r) — negative check %d of %d",
                entry.state.consumer_tag, sub["id"], sub["queue_name"], conn_id,
                entry.negative_checks, _NEGATIVE_CHECKS_BEFORE_RESTART,
            )

        fire_unseen = False
        if fire_here and fire is not None:
            seen = fire.state.consumer_tag in live_tags
            if fire.record(seen):
                self._fire_needs_restart = True
            if not seen:
                fire_unseen = True
                log.warning(
                    "Broker does not know the fire consumer on conn_id=%r "
                    "(queue %r) — negative check %d of %d",
                    conn_id, _FIRE_QUEUE, fire.negative_checks,
                    _NEGATIVE_CHECKS_BEFORE_RESTART,
                )

        if not unseen and not fire_unseen:
            state.liveness = _ConnLiveness(
                status=_CONN_CONNECTED, broker_consumer_count=broker_count
            )
            return set(), False

        unseen_reason = _unseen_reason(len(unseen), fire_unseen)
        if not dead_subs:
            # The restart waits for a second negative check in a row, the reported
            # status does not: the broker is holding none of these consumers right
            # now, and a green row would claim the opposite for a whole reconcile
            # interval.
            checks = max(
                [entry.negative_checks for sub, entry in subs if sub["id"] in unseen]
                + ([fire.negative_checks] if fire_unseen and fire is not None else [])
            )
            state.liveness = _ConnLiveness(
                status=_CONN_ERROR,
                broker_consumer_count=broker_count,
                reason=(
                    f"{unseen_reason} — negative check {checks} of "
                    f"{_NEGATIVE_CHECKS_BEFORE_RESTART}, recovery starts once that "
                    f"many checks agree"
                ),
            )
            return set(), False

        if not self._may_drop_connection(conn_id):
            state.liveness = self._held_back_verdict(conn_id, broker_count, unseen_reason)
            return set(), False

        confirmed = sorted(sub["id"] for sub, _ in subs if sub["id"] not in dead_subs)
        if confirmed:
            log.warning(
                "The connection of conn_id=%r is being recreated for %d unseen subscription(s); "
                "subscription(s) %s share it and keep their own retry loop — they "
                "surface the drop as a transient consumer error",
                conn_id, len(dead_subs), confirmed,
            )
        state.liveness = _ConnLiveness(
            status=_CONN_ERROR,
            broker_consumer_count=broker_count,
            reason=reason or "consumer not registered on the broker",
        )
        return dead_subs, True

    async def _judge_without_candidates(
        self, conn_id: str, live: int, stalled: list[dict]
    ) -> bool:
        """Judge a conn_id that offers the broker no consumer of ours to confirm.

        :param live: Running tasks this conn_id has.
        :param stalled: Subscriptions of those tasks, none of which reached ``listening``.
        :returns: Whether the connection is to be recreated.

        With no running task the conn_id is reported as an error outright. With running
        tasks that never reach ``listening`` for :data:`_STUCK_CYCLES_BEFORE_DROP` cycles
        the connection is asked directly, through :meth:`_probe_connection`, whether it
        answers an RPC: silence is the shape of a pooled connection whose ``channel()``
        never returns, where every task retries onto the same silence and the check would
        otherwise see nothing to judge, and the connection is condemned. An answer means
        the tasks are attached to a working connection and are failing after it — a
        trigger that keeps raising leaves a subscription in ``error`` just as surely — so
        the connection stays and the row says so.
        """
        state = self._conn(conn_id)
        if not live:
            state.stuck_cycles = 0
            state.liveness = _ConnLiveness(
                status=_CONN_ERROR,
                broker_consumer_count=None,
                reason="no consumer task of this connection is running",
            )
            return False

        stuck = state.stuck_cycles = state.stuck_cycles + 1
        if stuck < _STUCK_CYCLES_BEFORE_DROP:
            state.liveness = _ConnLiveness(
                status=None, broker_consumer_count=None, reason=None
            )
            return False

        # Nothing said so far about *why* these tasks are not consuming, so ask the
        # connection itself. A task sitting in ``error`` because its trigger keeps
        # failing is attached to a perfectly healthy connection, and condemning that
        # would blame the broker for a database outage.
        answers, probe_reason = await self._probe_connection(
            conn_id, {sub["queue_name"] for sub in stalled}
        )
        if answers:
            # The counter keeps running: nothing here is fixed, and dropping it
            # would leave the next cycle with no verdict to report at all.
            state.liveness = _ConnLiveness(
                status=_CONN_ERROR,
                broker_consumer_count=None,
                reason=(
                    f"{live} task(s) of this connection have not reached 'listening' "
                    f"for {stuck} cycles, and the connection answers an RPC — the "
                    f"fault is downstream of it; see the subscriptions' own errors"
                ),
            )
            log.warning(
                "conn_id=%r has %d running task(s) and not one of them is "
                "consuming after %d cycles, yet the connection answers an RPC — "
                "leaving it in place: what stops these tasks is not the connection",
                conn_id, live, stuck,
            )
            return False

        reason = (
            f"{live} task(s) of this connection have not reached 'listening' for "
            f"{stuck} cycles, and {probe_reason}"
        )
        log.warning(
            "conn_id=%r has %d running task(s) and not one of them is consuming "
            "after %d cycles, and %s — recreating it: a pooled connection that "
            "answers no RPC hands the same silence to every task that retries on it",
            conn_id, live, stuck, probe_reason,
        )
        if not self._may_drop_connection(conn_id):
            state.liveness = self._held_back_verdict(conn_id, None, reason)
            return False
        state.stuck_cycles = 0
        state.liveness = _ConnLiveness(
            status=_CONN_ERROR, broker_consumer_count=None, reason=reason
        )
        return True

    def _held_back_verdict(
        self, conn_id: str, broker_count: int | None, reason: str
    ) -> _ConnLiveness:
        """Verdict for a ``conn_id`` the rate limit keeps from being recreated again.

        Repeated verdicts mean either the check misjudges the connection or the fault
        is not in the connection, so the drop is refused and the row says ``degraded``
        rather than silently looping through recreations.
        """
        log.warning(
            "The connection of conn_id=%r is condemned again after %d cycle(s), sooner "
            "than the %d-cycle limit allows — leaving it in place. Repeated verdicts mean "
            "either the check misjudges it or the fault is not in the connection.",
            conn_id, self._cycle_no - (self._conn(conn_id).last_drop_cycle or 0),
            _CYCLES_BEFORE_REDROP,
        )
        return _ConnLiveness(
            status=_CONN_DEGRADED,
            broker_consumer_count=broker_count,
            reason=(
                f"{reason}, but the connection was already recreated less than "
                f"{_CYCLES_BEFORE_REDROP} cycles ago"
            ),
        )

    async def _probe_consumers(
        self, conn_id: str, queues: set[str], expected_tags: set[str]
    ) -> tuple[set[str] | None, int | None, str | None]:
        """Ask ``conn_id`` which of our consumers the broker currently holds.

        :param conn_id: Airflow connection whose consumers are being verified.
        :param queues: Queue names our consumers of this conn_id are subscribed to.
        :param expected_tags: Consumer tags those subscriptions registered.
        :returns: ``(live tags, consumers the broker reports, reason)``, where live tags
            of ``None`` means the check produced no data and the counters stay untouched.

        With a ``management_url`` the answer comes from ``GET /api/consumers/{vhost}``,
        whose reply is shaped by the rights of the account that asked — a user tagged
        ``management`` is shown its own channels, the whole vhost is visible to
        ``monitoring`` and ``administrator``. It is therefore cached for the cycle under
        the account as well as the broker and vhost, and reused only between conn_ids
        that log in as the same user: several of them often point at one broker, and
        asking it once per conn_id multiplies the same request. Without a
        ``management_url`` — and after
        :data:`_MGMT_FAILURES_BEFORE_FALLBACK` failed Management API calls in a row, which
        is what a wrong URL or credentials without the ``management`` tag look like — the
        probe is a passive declare on a fresh channel: it says nothing about individual
        consumers, so its success vouches for every tag of this conn_id and its failure —
        a raised error or a call that never returns — condemns them all.
        """
        if self._http_client is None:
            return await self._probe_by_passive_declare(conn_id, queues, expected_tags)

        try:
            conn_info = await self._get_connection_info(conn_id, self._cycle_executor)
            management_url = get_management_url(conn_info)
        except Exception as exc:
            log.warning(
                "Cannot read the Airflow connection of conn_id=%r for the liveness "
                "check: %s — liveness unknown this cycle, counters unchanged",
                conn_id, exc,
            )
            return None, None, str(exc)

        if (
            management_url is not None
            and conn_info.login is not None
            and conn_info.password is not None
        ):
            vhost = conn_info.schema or "/"
            cache_key = (management_url, vhost, conn_info.login)
            by_queue = self._consumer_cache.get(cache_key)
            if by_queue is None:
                try:
                    by_queue = await get_queue_consumers(
                        self._http_client,
                        management_url,
                        vhost,
                        (conn_info.login, conn_info.password),
                    )
                except Exception as exc:
                    failures = self._conn(conn_id).mgmt_failures + 1
                    self._conn(conn_id).mgmt_failures = failures
                    if failures < _MGMT_FAILURES_BEFORE_FALLBACK:
                        log.warning(
                            "Management API did not answer the consumer list for "
                            "conn_id=%r: %s — liveness unknown this cycle, counters "
                            "unchanged",
                            conn_id, exc,
                        )
                        return None, None, str(exc)
                    log.warning(
                        "Management API has failed %d times in a row for conn_id=%r "
                        "(%s) — falling back to the AMQP probe so the watchdog keeps "
                        "running while the API stays unusable",
                        failures, conn_id, exc,
                    )
                    return await self._probe_by_passive_declare(
                        conn_id, queues, expected_tags
                    )
                self._consumer_cache[cache_key] = by_queue
            self._conn(conn_id).mgmt_failures = 0
            live_tags: set[str] = set()
            broker_count = 0
            for queue_name in queues:
                queue_tags = by_queue.get(queue_name, set())
                live_tags |= queue_tags
                broker_count += len(queue_tags)
            return live_tags, broker_count, None

        return await self._probe_by_passive_declare(conn_id, queues, expected_tags)

    async def _probe_by_passive_declare(
        self, conn_id: str, queues: set[str], expected_tags: set[str]
    ) -> tuple[set[str], int | None, str | None]:
        """Verify ``conn_id`` by passive-declaring one of its queues on a fresh channel."""
        answers, reason = await self._probe_connection(conn_id, queues)
        if not answers:
            return set(), None, reason
        return set(expected_tags), None, None

    async def _probe_connection(
        self, conn_id: str, queues: set[str]
    ) -> tuple[bool, str | None]:
        """Ask the pooled connection of ``conn_id`` itself whether it answers an RPC.

        :param conn_id: Airflow connection to probe.
        :param queues: Queue names to pick the passive declare's subject from.
        :returns: ``(answers, reason)``; ``reason`` is filled in when it does not.

        A passive ``queue_declare`` on a fresh channel is the one question that reaches
        the connection object rather than the broker's bookkeeping about it: a zombie —
        a connection the broker has long forgotten while the client socket stays open —
        answers it with silence, which the per-call timeout turns into a failure. With
        no queue to name the probe cannot be asked and the connection is left alone.
        """
        if not queues:
            return True, None
        queue_name = min(queues)
        rpc_timeout = self._rpc_timeout(conn_id)
        channel = None
        try:
            connection = await self._get_or_create_connection(
                conn_id, executor=self._cycle_executor
            )
            channel = await call_with_timeout(connection.channel(), timeout=rpc_timeout)
            await call_with_timeout(
                channel.declare_queue(queue_name, passive=True), timeout=rpc_timeout
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            reason = f"passive declare of {queue_name!r} failed: {exc}"
            log.warning(
                "Liveness probe on conn_id=%r failed (%s) — treating the connection as "
                "dead: silence on an AMQP call is what a zombie connection answers with",
                conn_id, reason,
            )
            return False, reason
        finally:
            if channel is not None:
                await _close_quietly(
                    channel, f"the liveness probe channel of conn_id={conn_id!r}"
                )
        return True, None

    async def _consume_subscription(self, sub: dict) -> None:
        sub_id: int = sub["id"]
        dag_id: str = sub["dag_id"]
        queue_name: str = sub["queue_name"]
        conn_id: str = sub["conn_id"]
        cooldown: int = sub.get("cooldown", 0) or 0
        msg_filter = MessageFilter.deserialize(sub.get("filter_data") or {})
        state = self._state_of(sub_id)
        # Kept across reconnects: a broken trigger path stays broken through the
        # reconnect that a NACKed delivery may well cause, and so does a broker that
        # will not accept a publish.
        trigger_backoff = _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX)
        publish_backoff = _Backoff(_PUBLISH_BACKOFF_START, _PUBLISH_BACKOFF_MAX)

        while True:
            await state.write(_SUB_CONNECTING)
            try:
                connection = await self._get_or_create_connection(conn_id)
                rpc_timeout = self._rpc_timeout(conn_id)
                channel = await call_with_timeout(connection.channel(), timeout=rpc_timeout)
                try:
                    queue = await call_with_timeout(
                        channel.declare_queue(queue_name, passive=True),
                        timeout=rpc_timeout,
                    )
                    # No set_qos on purpose: messages that miss the filter are NACKed
                    # with requeue (ADR-0002) and come back to the head of the queue, so
                    # any finite prefetch window fills up with them and consumption stops
                    # for good once the misses reach the window size. The unacked window
                    # stays unbounded.
                    consumer_tag = _consumer_tag(sub_id, _attach_nonce())
                    async with _attached(queue, consumer_tag, rpc_timeout) as q_iter:
                        state.consumer_tag = consumer_tag
                        await state.write(_SUB_LISTENING, last_error=None)
                        log.info(
                            "Subscription %d (DAG %s) is consuming queue %r on conn_id=%r",
                            sub_id, dag_id, queue_name, conn_id,
                        )
                        incr("rmq_watcher.consumer_attached")

                        try:
                            async for message in q_iter:
                                try:
                                    if not match(message, msg_filter):
                                        await nack_and_sleep(message)
                                        continue
                                    if cooldown > 0:
                                        await self._handle_cooldown_delivery(
                                            conn_id, dag_id, cooldown, message,
                                            publish_backoff,
                                        )
                                    else:
                                        await self._handle_immediate_delivery(
                                            sub, message, state, trigger_backoff
                                        )
                                except Exception as delivery_exc:
                                    # Handling a delivery reaches the broker on its own —
                                    # a publish, an ACK, a connection it has to rebuild —
                                    # and a connection torn down under any of those raises
                                    # what a rejected consumer cancellation raises. The
                                    # mark says which of the two this is, so the retry
                                    # loop below keeps the subscription instead of ending
                                    # it as cancelled.
                                    _mark_delivery_fault(delivery_exc)
                                    raise
                        except Exception as exc:
                            if not _ends_as_cancelled(exc):
                                raise
                            log.warning(
                                "Cancelling consumer %s of subscription %d failed: %s — "
                                "the task ends as the cancelled task it is",
                                consumer_tag, sub_id, exc,
                            )
                            raise asyncio.CancelledError from exc
                finally:
                    state.consumer_tag = None
                    await _close_quietly(
                        channel, f"the channel of subscription {sub_id}"
                    )

                # The iterator finished without an exception — the broker cancelled the
                # consumer. Pause before subscribing again so a broker that keeps ending
                # it right away cannot spin this loop.
                await asyncio.sleep(_RECONNECT_DELAY)

            except asyncio.CancelledError:
                return

            except aio_pika.exceptions.ChannelNotFoundEntity as exc:
                # Fatal: queue doesn't exist — exit and wait for reconciliation to restart
                await state.write(_SUB_ERROR, last_error=_error_text(exc))
                log.error(
                    "Queue %r not found for subscription %d (DAG %s): %s",
                    queue_name, sub_id, dag_id, exc,
                )
                return

            except aio_pika.exceptions.ChannelClosed as exc:
                # Recoverable: channel dropped (e.g. queue deleted at runtime)
                await state.write(_SUB_ERROR, last_error=_error_text(exc))
                log.warning(
                    "Channel closed for subscription %d (queue %r): %s — retrying in %ss",
                    sub_id, queue_name, exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

            except Exception as exc:
                # The row says what went wrong, not merely that the task is trying: a
                # registration the broker never confirms leaves the subscription
                # ``connecting`` for as long as the fault lasts, and an operator reading
                # that sees a task starting up rather than one that cannot.
                await state.write(_SUB_ERROR, last_error=_error_text(exc))
                log.warning(
                    "Transient error in consumer %d (queue %r): %s — retrying in %ss",
                    sub_id, queue_name, exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

    async def _handle_cooldown_delivery(
        self,
        conn_id: str,
        dag_id: str,
        cooldown: int,
        message: Any,
        backoff: _Backoff,
    ) -> None:
        """Publish the cooldown placeholder that one matched delivery stands for.

        A publish that cannot go through pauses before the exception leaves the consumer
        loop: the delivery is already back on the queue, and without the pause the broker
        hands it straight back while the publish still cannot go through.
        """
        try:
            await self._publish_pending(conn_id, dag_id, cooldown, message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "Publishing the cooldown placeholder for DAG %s failed: %s — the "
                "delivery is back on the queue, pausing %.1fs before consuming again",
                dag_id, exc, backoff.seconds,
            )
            await backoff.wait()
            raise
        backoff.reset()

    async def _handle_immediate_delivery(
        self, sub: dict, message: Any, state: _ConsumerState, backoff: _Backoff
    ) -> None:
        """Start the DAG run for one matched delivery and acknowledge it afterwards.

        The ACK follows the DAG run, so a failed trigger returns the delivery to the
        queue instead of losing the event, and pauses for a growing interval before the
        next one. The subscription reports the failure as its own status: the connection
        is fine and the iterator keeps running, so nothing else would ever say that this
        subscription stopped starting DAG runs.
        """
        dag_id: str = sub["dag_id"]
        sub_id: int = sub["id"]
        try:
            outcome = await self._trigger_dag(
                dag_id, sub["queue_name"], sub_id, message
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "Triggering DAG %s for subscription %d failed: %s "
                "— requeueing the delivery, pausing %.1fs",
                dag_id, sub_id, exc, backoff.seconds,
            )
            await message.nack(requeue=True)
            await state.write(_SUB_ERROR, last_error=_error_text(exc))
            await backoff.wait()
            return
        backoff.reset()
        await state.write(_SUB_LISTENING, last_error=None)
        await message.ack()
        if outcome == _OUTCOME_TRIGGERED:
            incr("rmq_watcher.dag_triggered")

    async def _publish_pending(
        self, conn_id: str, dag_id: str, cooldown: int, message: Any
    ) -> None:
        """Publish the cooldown placeholder for ``dag_id`` and ACK the delivery behind it.

        The publish runs on the publish connection of ``conn_id``, so a broker blocking
        publishers under a resource alarm leaves the consuming connection — and with it
        every ``basic.ack`` — untouched.

        A broker that *rejects* the publish has done its job: the pending queue is
        declared ``x-max-length=1`` with ``x-overflow=reject-publish``, so a rejection
        means the cooldown for this dag_id is already counting down and a second
        placeholder would add nothing. That is the ordinary case while a cooldown window
        is open, and the delivery is acknowledged — requeueing it would redeliver the
        same message for the whole window and burn the quorum-queue delivery limit on it.

        A broker that *returns* the publish says the opposite: the pending queue is
        missing, so nothing is counting down and nothing will ever fire this window. The
        delivery goes back on the queue and the next reconcile cycle declares the queue
        again. The two answers are told apart by the channel, which is opened to raise on
        a returned message (see :meth:`_get_publish_channel`).

        A publish that fails for any other reason returns the delivery to the queue right
        away: leaving the loop hands back only what is still buffered in the iterator, and
        this message, already taken out of it, would otherwise sit unacknowledged on the
        abandoned channel until the broker's ``consumer_timeout``.
        """
        pending_queue = f"{_PENDING_QUEUE_PREFIX}{dag_id}"
        try:
            channel = await self._get_publish_channel(conn_id)
            await call_with_timeout(
                channel.default_exchange.publish(
                    aio_pika.Message(
                        b"",
                        expiration=str(cooldown * 1000),
                        message_id=str(uuid.uuid4()),
                    ),
                    routing_key=pending_queue,
                ),
                timeout=self._rpc_timeout(conn_id),
            )
        except asyncio.CancelledError:
            raise
        except aio_pika.exceptions.PublishError as exc:
            # The broker returned the message: the pending queue is not there at all, so
            # nothing will ever fire this cooldown window. The delivery goes back on the
            # queue and the next reconcile cycle declares the queue again.
            log.warning(
                "Cooldown placeholder for DAG %s reached no queue (%s) — the delivery "
                "is back on the queue",
                dag_id, exc,
            )
            await self._handle_publish_failure(conn_id, message, exc)
            raise
        except aio_pika.exceptions.DeliveryError:
            log.debug(
                "Cooldown placeholder for DAG %s was rejected by %s — a cooldown window "
                "is already open, acknowledging the delivery",
                dag_id, pending_queue,
            )
            self._conn(conn_id).publish_timeouts = 0
            await message.ack()
            return
        except Exception as exc:
            await self._handle_publish_failure(conn_id, message, exc)
            raise
        self._conn(conn_id).publish_timeouts = 0
        await message.ack()

    async def _handle_publish_failure(
        self, conn_id: str, message: Any, exc: BaseException
    ) -> None:
        """Requeue the delivery and condemn the publish connection after repeated timeouts.

        A publish connection carries no consumers, so the broker-side liveness check never
        sees it and a zombie there would be handed out of the cache forever. Its own probe
        is the publish itself: two timeouts in a row mean the connection is gone, and only
        the publish role is recreated.
        """
        try:
            await message.nack(requeue=True)
        except Exception as nack_exc:
            log.warning(
                "Requeueing the delivery after a failed publish on conn_id=%r failed: %s",
                conn_id, nack_exc,
            )

        state = self._conn(conn_id)
        if not isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            state.publish_timeouts = 0
            return

        count = state.publish_timeouts = state.publish_timeouts + 1
        if count < _PUBLISH_TIMEOUTS_BEFORE_DROP:
            return

        log.warning(
            "Publish timed out %d times in a row on conn_id=%r — recreating its publish "
            "connection; consumers of this conn_id keep their own connection",
            count, conn_id,
        )
        state.publish_timeouts = 0
        await self._drop_connection(conn_id, role=_ROLE_PUBLISH)

    async def _consume_fire_queue(self, connection: Any, conn_id: str) -> None:
        """Consumer for rmq_watcher.fire queue — triggers DAGs after cooldown expires via DLX."""
        state = self._fire_state_of()
        # Kept across reconnects: a broken trigger path stays broken through the
        # reconnect a requeued fire event may well cause.
        trigger_backoff = _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX)
        while True:
            await state.write(_SUB_CONNECTING)
            try:
                rpc_timeout = self._rpc_timeout(conn_id)
                channel = await call_with_timeout(connection.channel(), timeout=rpc_timeout)
                try:
                    queue = await call_with_timeout(
                        channel.declare_queue(_FIRE_QUEUE, passive=True),
                        timeout=rpc_timeout,
                    )
                    fire_tag = _consumer_tag("fire", _attach_nonce())
                    async with _attached(queue, fire_tag, rpc_timeout) as q_iter:
                        state.consumer_tag = fire_tag
                        await state.write(_SUB_LISTENING)
                        log.info(
                            "Cooldown fire consumer is consuming queue %r on conn_id=%r",
                            _FIRE_QUEUE, conn_id,
                        )
                        incr("rmq_watcher.consumer_attached")

                        try:
                            async for message in q_iter:
                                try:
                                    await self._handle_fire_delivery(
                                        message, state, trigger_backoff
                                    )
                                except Exception as delivery_exc:
                                    # Same boundary the subscription loop draws: what the
                                    # handling of a fire event raises is judged as the
                                    # transient fault it is, not as a cancellation the
                                    # chain happens to resemble.
                                    _mark_delivery_fault(delivery_exc)
                                    raise
                        except Exception as exc:
                            if not _ends_as_cancelled(exc):
                                raise
                            log.warning(
                                "Cancelling the fire consumer %s of conn_id=%r failed: "
                                "%s — the task ends as the cancelled task it is",
                                fire_tag, conn_id, exc,
                            )
                            raise asyncio.CancelledError from exc
                finally:
                    state.consumer_tag = None
                    await _close_quietly(
                        channel, f"the fire consumer channel of conn_id={conn_id!r}"
                    )

                # The iterator returned without an exception. A robust iterator does that
                # on a connection closed on purpose, on a channel loss it cannot wait out,
                # and when its own wait for the channel to come back runs out — states
                # that repeat as readily as they arrive. Pause before subscribing again so
                # none of them can spin this loop.
                await asyncio.sleep(_RECONNECT_DELAY)

            except asyncio.CancelledError:
                return

            except aio_pika.exceptions.ChannelNotFoundEntity as exc:
                await state.write(_SUB_ERROR, last_error=_error_text(exc))
                log.error(
                    "Fire queue %r not found: %s — exiting fire consumer, "
                    "will restart on next reconcile cycle.",
                    _FIRE_QUEUE, exc,
                )
                return

            except aio_pika.exceptions.ChannelClosed as exc:
                await state.write(_SUB_ERROR, last_error=_error_text(exc))
                log.warning(
                    "Fire queue channel closed: %s — retrying in %ss",
                    exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

            except Exception as exc:
                await state.write(_SUB_ERROR, last_error=_error_text(exc))
                log.warning(
                    "Transient error in fire consumer: %s — retrying in %ss",
                    exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

    async def _handle_fire_delivery(
        self, message: Any, state: _ConsumerState, backoff: _Backoff
    ) -> None:
        """Start the DAG run one expired cooldown window calls for, then acknowledge it.

        A trigger that fails puts the fire event back on the queue and pauses for a
        growing interval before the next one. The pause is what keeps the event: a fire
        event is the only record that a cooldown window expired, ``rmq_watcher.fire`` is
        declared with no dead-letter exchange, and a redelivery every reconnect delay
        spends the default delivery limit of a quorum queue in about a hundred seconds —
        after which the broker drops the message the requeue was meant to keep.

        The fire consumer reports the failure as its own status for the same reason a
        subscription does: the connection is fine and the iterator keeps running, so
        nothing else would say that expired cooldown windows stopped starting DAG runs.
        """
        dag_id = message.routing_key or ""
        if not dag_id:
            log.warning("Fire queue message has no routing_key — skipping")
            await message.ack()
            return
        if not message.message_id:
            log.warning(
                "Fire queue message has no message_id — skipping (idempotency broken)"
            )
            await message.ack()
            return

        try:
            outcome = await self._trigger_fire_dag(dag_id, message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning(
                "Triggering DAG %s for an expired cooldown window failed: %s — the fire "
                "event is back on the queue, pausing %.1fs",
                dag_id, exc, backoff.seconds,
            )
            await message.nack(requeue=True)
            await state.write(_SUB_ERROR, last_error=_error_text(exc))
            await backoff.wait()
            return

        backoff.reset()
        await state.write(_SUB_LISTENING, last_error=None)
        if outcome == _OUTCOME_TRIGGERED:
            incr("rmq_watcher.dag_triggered")
        # Known limitation: if _sync_trigger returned early because the DAG is
        # paused/inactive, the message is still ACKed here and the fire event is
        # permanently lost. This is intentional — the DLX message has already spent its
        # TTL and re-queuing would cause an infinite loop. Operators should ensure DAGs
        # are active before enabling cooldown subscriptions.
        await message.ack()

    async def _trigger_fire_dag(self, dag_id: str, message: Any) -> str:
        """Start the DAG run one expired cooldown window calls for, and report how it ended.

        The placeholder carries no payload of its own — body and headers are dropped when
        the matched delivery is replaced by it — so its conf names where the run came
        from and nothing else. The run id is derived from the placeholder's message_id,
        which makes a redelivery of the same fire event land on the run it already made.
        """
        conf = {
            "source": "cooldown",
            "dag_id": dag_id,
            "body": "",
            "headers": {},
            "routing_key": dag_id,
            "queue": _FIRE_QUEUE,
            "subscription_id": None,
        }
        run_id = _safe_run_id(f"rmq_cooldown__{dag_id}__{message.message_id}")
        return await call_with_timeout(
            self._executor.run(_sync_trigger, dag_id, conf, run_id),
            timeout=_TRIGGER_TIMEOUT,
        )

    async def _trigger_dag(
        self,
        dag_id: str,
        queue_name: str,
        sub_id: int,
        message: Any,
    ) -> str:
        """Start the DAG run for one delivery and report how it ended."""
        conf = {
            "source": "immediate",
            "body": message.body.decode("utf-8", errors="replace"),
            "headers": dict(message.headers or {}),
            "routing_key": getattr(message, "routing_key", "") or "",
            "queue": queue_name,
            "subscription_id": sub_id,
        }
        run_id = _build_run_id(queue_name, getattr(message, "message_id", None))
        return await call_with_timeout(
            self._executor.run(_sync_trigger, dag_id, conf, run_id),
            timeout=_TRIGGER_TIMEOUT,
        )


async def _ensure_fire_infrastructure(channel: Any, timeout: float) -> None:
    """Declare the fire exchange and queue idempotently.

    - Exchange: rmq_watcher.fire (topic, durable)
    - Queue:    rmq_watcher.fire (durable, binding key=#)
    """
    exchange = await call_with_timeout(
        channel.declare_exchange(
            _FIRE_EXCHANGE,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
        ),
        timeout=timeout,
    )
    queue = await call_with_timeout(
        channel.declare_queue(
            _FIRE_QUEUE,
            durable=True,
        ),
        timeout=timeout,
    )
    await call_with_timeout(queue.bind(exchange, routing_key="#"), timeout=timeout)


async def _ensure_pending_queue(channel: Any, dag_id: str, timeout: float) -> None:
    """Declare the per-DAG pending queue idempotently.

    Queue: rmq_watcher.pending.{dag_id}
      - x-dead-letter-exchange    = rmq_watcher.fire
      - x-dead-letter-routing-key = {dag_id}
      - x-max-length              = 1
      - x-overflow                = reject-publish

    No consumer is attached — messages expire via per-message TTL and are
    dead-lettered to rmq_watcher.fire with routing_key=dag_id.
    """
    queue_name = f"{_PENDING_QUEUE_PREFIX}{dag_id}"
    await call_with_timeout(
        channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": _FIRE_EXCHANGE,
                "x-dead-letter-routing-key": dag_id,
                "x-max-length": 1,
                "x-overflow": "reject-publish",
            },
        ),
        timeout=timeout,
    )


async def _ensure_exchange_infrastructure(
    channel: Any, exchange: str, timeout: float
) -> None:
    """Declare the exchange-mode RMQ infrastructure for a given exchange, idempotently.

    - Exchange ``{exchange}``: topic, durable, ``arguments={"alternate-exchange": "{exchange}.unrouted"}``
    - Exchange ``{exchange}.unrouted``: fanout, durable (alternate-exchange target)
    - Queue ``{exchange}.unrouted``: durable, ``x-message-ttl=_EXCHANGE_TTL_MS`` (8h),
      bound to the fanout exchange above with no routing key
    - Queue ``{exchange}.log``: durable, ``x-message-ttl=_EXCHANGE_TTL_MS`` (8h), bound to
      ``{exchange}`` with routing key ``#`` (catch-all mirror of every routed message)

    All declares are active (no ``passive=True``) — safe to repeat every reconcile cycle,
    same pattern as ``_ensure_fire_infrastructure``. If ``exchange`` already exists with
    different properties (declared outside this provider), RabbitMQ raises
    ``aio_pika.exceptions.ChannelPreconditionFailed`` (reply code 406) — left to the caller
    to catch and log distinctly from generic errors.
    """
    unrouted_exchange_name = f"{exchange}.unrouted"
    log_queue_name = f"{exchange}.log"

    exchange_obj = await call_with_timeout(
        channel.declare_exchange(
            exchange,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
            arguments={"alternate-exchange": unrouted_exchange_name},
        ),
        timeout=timeout,
    )

    unrouted_exchange_obj = await call_with_timeout(
        channel.declare_exchange(
            unrouted_exchange_name,
            type=aio_pika.ExchangeType.FANOUT,
            durable=True,
        ),
        timeout=timeout,
    )
    unrouted_queue = await call_with_timeout(
        channel.declare_queue(
            unrouted_exchange_name,
            durable=True,
            arguments={"x-message-ttl": _EXCHANGE_TTL_MS},
        ),
        timeout=timeout,
    )
    await call_with_timeout(unrouted_queue.bind(unrouted_exchange_obj), timeout=timeout)

    log_queue = await call_with_timeout(
        channel.declare_queue(
            log_queue_name,
            durable=True,
            arguments={"x-message-ttl": _EXCHANGE_TTL_MS},
        ),
        timeout=timeout,
    )
    await call_with_timeout(log_queue.bind(exchange_obj, routing_key="#"), timeout=timeout)


async def _ensure_sub_queue(channel: Any, dag_id: str, timeout: float) -> Any:
    """Declare the per-DAG exchange-mode sub queue idempotently and return it.

    Queue: ``rmq_watcher.sub.{dag_id}`` — durable, ``x-message-ttl=_EXCHANGE_TTL_MS`` (8h).

    Unlike the cooldown pending queue, this queue is actively consumed by a live consumer
    (the same ``_consume_subscription`` used for ``queue=`` mode) — the TTL here is purely a
    safety net against unbounded growth if the subscription becomes orphaned (see
    ADR-0005), not a timer mechanism.
    """
    queue_name = f"{_SUB_QUEUE_PREFIX}{dag_id}"
    return await call_with_timeout(
        channel.declare_queue(
            queue_name,
            durable=True,
            arguments={"x-message-ttl": _EXCHANGE_TTL_MS},
        ),
        timeout=timeout,
    )


async def _sync_bindings(
    queue: Any,
    exchange: str,
    desired: set[str],
    current: set[str],
    timeout: float,
) -> None:
    """Bind/unbind a queue to an exchange so its live bindings match ``desired``.

    Binds every routing key in ``desired - current`` and unbinds every routing key in
    ``current - desired``; logs each change on INFO. No-op when ``desired == current``.
    """
    to_bind = desired - current
    to_unbind = current - desired

    for routing_key in sorted(to_bind):
        await call_with_timeout(queue.bind(exchange, routing_key=routing_key), timeout=timeout)
        log.info(
            "Bound queue %s to exchange %r with routing_key=%r",
            queue.name, exchange, routing_key,
        )

    for routing_key in sorted(to_unbind):
        await call_with_timeout(queue.unbind(exchange, routing_key=routing_key), timeout=timeout)
        log.info(
            "Unbound queue %s from exchange %r with routing_key=%r",
            queue.name, exchange, routing_key,
        )
