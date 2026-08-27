from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import socket
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aio_pika
import aio_pika.exceptions
import httpx
from airflow.exceptions import DagRunAlreadyExists
from airflow.hooks.base import BaseHook
from airflow.models import DagModel
from sqlalchemy.exc import IntegrityError

from airflow_provider_rmq.utils.amqp import (
    DEFAULT_RPC_TIMEOUT,
    AmqpTimeouts,
    build_amqp_connection,
    call_with_timeout as _call_with_timeout,
    get_amqp_timeouts,
    match as _match,
    nack_and_sleep as _nack_and_sleep,
)
from airflow_provider_rmq.utils.executor import BoundedExecutor
from airflow_provider_rmq.utils.filters import MessageFilter
from airflow_provider_rmq.utils.management import (
    get_current_bindings,
    get_management_url,
    get_queue_consumers,
)
from airflow_provider_rmq.utils.metrics import incr as _incr
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

#: Seconds granted to a best-effort ``close()`` of a connection or channel.
_CLOSE_TIMEOUT = 5.0

#: Roles a connection is pooled under. Publishing lives on its own connection because
#: a resource alarm blocks publishing connections by making the broker stop reading
#: their socket — which stalls ``basic.ack`` of every consumer sharing that connection.
_ROLE_CONSUME = "consume"
_ROLE_PUBLISH = "publish"

#: Consecutive publish timeouts on one ``conn_id`` that condemn its publish connection.
_PUBLISH_TIMEOUTS_BEFORE_DROP = 2

#: Consecutive negative liveness checks that condemn a consumer. Two of them put the
#: verdict at least two reconcile intervals away from the first suspicion, so a single
#: slow cycle or a consumer registering late never costs a restart.
_NEGATIVE_CHECKS_BEFORE_RESTART = 2

#: Reconcile cycles that must pass before the same conn_id may be recreated again.
#: A real disconnect needs one recreation, so the limit costs nothing there, while a
#: misclassification turns from a continuous loop into a rare, logged event.
_DROP_COOLDOWN_CYCLES = 5

#: What a trigger attempt ended as. ``triggered`` and ``duplicate`` both mean the DAG
#: run for this delivery exists, so the delivery is acknowledged; ``skipped`` means the
#: DAG cannot run at all and acknowledging it is terminal by design — a NACK would turn
#: a paused DAG into a redelivery accumulator.
#: Status of a ``conn_id`` no liveness check has ever reached a verdict on.
_STATUS_UNKNOWN = "unknown"

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

#: Seconds a cancelled task is given to finish before the cycle moves on without it.
#: A task that ignores its cancellation must not hold up the cycle that cancelled it.
_CANCEL_TIMEOUT = 30.0

#: Consecutive Management API failures on one ``conn_id`` after which the liveness check
#: switches to the passive-declare probe. A single blip stays "no data"; a misconfigured
#: or permanently unreachable API must not disable the watchdog altogether.
_MGMT_FAILURES_BEFORE_FALLBACK = 2

#: Consecutive cycles in which a ``conn_id`` has live tasks but not one of them reaches
#: ``listening``. A healthy attach costs a connect and two RPCs, so a connection that
#: never gets there is not answering, and the check has no candidate to prove it with.
_STUCK_CYCLES_BEFORE_DROP = 2

#: Seconds a blocking database write is given before the caller stops waiting for it.
#: The worker stays busy until the call itself returns — a running thread cannot be
#: interrupted — so the timeout buys back the coroutine, not the worker.
_DB_TIMEOUT = 30.0

#: Seconds a single ``trigger_dag`` is given. A consumer task awaits it, not the
#: reconcile cycle, so the cycle watchdog never sees it hang: without a timeout of its
#: own the task would sit in ``listening`` while consuming nothing at all.
_TRIGGER_TIMEOUT = 60.0

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


def _write_consumer_status(sub_id: int, status: str, last_error: str | None) -> None:
    """Store the status of one subscription. Blocking — belongs in a pool."""
    with WatcherSession() as session:
        set_consumer_status(session, sub_id, status, last_error=last_error)
        session.commit()


def _write_conn_error(conn_id: str, error: str) -> None:
    """Store a failed connection attempt against ``conn_id``. Blocking."""
    with WatcherSession() as session:
        upsert_conn_status(session, conn_id, "error", consumer_count=0, last_error=error)
        session.commit()


def _write_conn_status_rows(rows: list[tuple], now: datetime) -> None:
    """Store one status row per conn_id. Blocking.

    A row whose ``status`` is ``None`` carries no verdict — the check produced no data —
    and keeps whatever is already stored, so an unreachable Management API does not
    paint every connection red. A conn_id with nothing stored yet starts at
    :data:`_STATUS_UNKNOWN`: the number of tasks the watcher started says nothing about
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
                status = stored.get(conn_id, _STATUS_UNKNOWN)
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


async def _wait_cancelled(tasks: list[asyncio.Task]) -> None:
    """Wait for already-cancelled ``tasks``, giving up after :data:`_CANCEL_TIMEOUT`.

    A task is free to catch its own ``CancelledError`` and keep going, and the cycle
    that cancelled it has a budget of its own to keep. Waiting without a bound is what
    turns one uncooperative task into a cycle that never ends.
    """
    if not tasks:
        return
    done, pending = await asyncio.wait(tasks, timeout=_CANCEL_TIMEOUT)
    for task in done:
        if not task.cancelled():
            task.exception()  # retrieved so it is not reported as never awaited
    if pending:
        log.warning(
            "%d consumer task(s) are still running %.0fs after being cancelled — the "
            "cycle continues without waiting for them",
            len(pending), _CANCEL_TIMEOUT,
        )


class _ConsumerState:
    """In-memory guard: writes consumer_status to DB only when the status actually changes.

    Prevents hot DB writes during reconnect storms (e.g. 20+/min → 2-4/min).

    A ``sub_id`` of ``None`` tracks the status in memory only — the fire consumer runs
    on the shared ``rmq_watcher.fire`` queue and has no row in ``rmq_subscriptions``.
    """

    def __init__(self, sub_id: int | None, executor: BoundedExecutor) -> None:
        self._sub_id = sub_id
        self._executor = executor
        self._status: str | None = None
        self._stored_status: str | None = None
        #: Tag the task registered on its queue during its current attach, ``None``
        #: while it is not attached. The liveness check asks the broker for this exact
        #: tag rather than recomputing one.
        self.consumer_tag: str | None = None

    @property
    def status(self) -> str | None:
        """Status the task last reported, ``None`` before it reported anything."""
        return self._status

    async def write(self, status: str, last_error: str | None = None) -> None:
        """Record ``status``, storing it if it differs from the one already stored.

        The reported status is the manager's own view of the task and is updated
        whatever the database does: it gates the liveness check, and a subscription
        left out of that check because one write timed out would never be verified
        again for the life of the task.

        The store is a blocking database write, so it runs in the consumer pool under a
        timeout: the consumer task awaits it, and a database that never answers would
        otherwise hold the task open forever. A write that does not land leaves the
        *stored* marker untouched, so the next call tries again, and it never
        propagates — diagnostics must not stop consumption.
        """
        self._status = status
        if status == self._stored_status:
            return
        if self._sub_id is not None:
            try:
                await _call_with_timeout(
                    self._executor.run(
                        _write_consumer_status, self._sub_id, status, last_error
                    ),
                    timeout=_DB_TIMEOUT,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning(
                    "Cannot store status %r of subscription %s: %s",
                    status, self._sub_id, exc,
                )
                return
        self._stored_status = status


@dataclass
class _ActiveSub:
    """Snapshot of a running subscription consumer task."""
    task: asyncio.Task
    sub: dict  # full snapshot of sub at task start time
    state: _ConsumerState  # status the task reports, readable by the manager
    negative_checks: int = 0  # consecutive liveness checks the broker answered negatively


@dataclass
class _FireSub:
    """What the manager knows about the fire consumer task, mirroring :class:`_ActiveSub`.

    Its own state gates the liveness check the same way a subscription's does — a fire
    task pausing in its retry loop is not a candidate — and its own counter keeps its
    verdict to itself instead of expressing it through the ``conn_id`` it shares with
    ordinary subscriptions.
    """
    conn_id: str
    state: _ConsumerState
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
    :param broker_consumers: Consumers the broker reports on our queues, ``None``
        when the check cannot tell (Management API unavailable or not configured).
    :param reason: Human-readable explanation for a non-``connected`` status.
    """
    status: str | None
    broker_consumers: int | None
    reason: str | None = None


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
        self._connections: dict[tuple[str, str], Any] = {}  # (conn_id, role) → RobustConnection
        self._publish_channels: dict[str, Any] = {}  # conn_id → channel of the publish connection
        self._timeouts: dict[str, AmqpTimeouts] = {}  # conn_id → call timeouts from its extra
        self._publish_timeouts: dict[str, int] = {}  # conn_id → consecutive publish timeouts
        self._last_drop_cycle: dict[str, int] = {}  # conn_id → cycle of its last full drop
        self._cycle_no = 0  # liveness checks performed, i.e. reconcile cycles
        self._liveness: dict[str, _ConnLiveness] = {}  # conn_id → verdict of the last check
        self._mgmt_failures: dict[str, int] = {}  # conn_id → consecutive Management API errors
        self._stuck_cycles: dict[str, int] = {}  # conn_id → cycles with no listening task
        # (management_url, vhost) → queue → consumer tags, for the current cycle only
        self._consumer_cache: dict[tuple[str, str], dict[str, set[str]]] = {}
        self._conn_lock = asyncio.Lock()  # prevents duplicate connections on concurrent starts
        self._fire_task: asyncio.Task | None = None
        self._fire_state: _FireSub | None = None  # state record of the running fire task
        self._fire_needs_restart = False  # last check found the fire consumer gone
        self._cooldown_tracker = OrphanTracker()  # dag_ids for which pending queues were created
        self._exchange_tracker = OrphanTracker()  # dag_ids for which sub queues/bindings were created
        self._http_client: httpx.AsyncClient | None = None  # Management API client
        # conn_ids the split-cooldown warning last named, so it is logged on change
        self._split_cooldown_warned: set[str] = set()

    async def start(self) -> None:
        """Create the shared Management API HTTP client. Connections/tasks are created on demand."""
        self._http_client = httpx.AsyncClient(timeout=5.0)

    async def stop(self) -> None:
        tasks_to_cancel: list[asyncio.Task] = [
            entry.task for entry in self._active.values()
        ]
        if self._fire_task is not None and not self._fire_task.done():
            tasks_to_cancel.append(self._fire_task)

        for task in tasks_to_cancel:
            task.cancel()
        await _wait_cancelled(tasks_to_cancel)

        self._fire_task = None
        self._fire_state = None

        for conn in list(self._connections.values()):
            try:
                await _call_with_timeout(conn.close(), timeout=_CLOSE_TIMEOUT)
            except Exception:
                pass
        self._active.clear()
        self._connections.clear()
        self._publish_channels.clear()
        self._publish_timeouts.clear()

        if self._http_client is not None:
            try:
                await self._http_client.aclose()
            except Exception:
                pass
            self._http_client = None

    async def reconcile(self, subscriptions: list[dict]) -> None:
        """Sync running tasks with the current subscription list.

        Cancels tasks for removed subscriptions, starts tasks for new ones,
        and restarts tasks that exited due to fatal errors (task.done()).
        Also manages cooldown infrastructure (fire exchange/queue, pending queues) and
        exchange-mode infrastructure (exchange/sub queue/bindings).

        A task that is merely still running proves nothing, so the cycle ends by asking
        the broker which of our consumers it actually holds: one it does not know is
        rebuilt together with its connection, and the status row of every conn_id is
        written from that answer.

        Exchange provisioning runs (awaited) before the cancel/start consumer block below:
        exchange-mode queues are created by this provider (unlike ``queue=`` mode, where the
        queue is created out-of-band and ``_consume_subscription`` always passive-declares
        it) — running provisioning first avoids a brand-new consumer task fatally failing a
        passive declare against a queue that doesn't exist yet.
        """
        exchange_subs = [s for s in subscriptions if s.get("exchange")]
        await self._provision_exchange_subs(exchange_subs)

        new_ids = {sub["id"] for sub in subscriptions}

        # cancel tasks for removed subscriptions
        to_remove = [sid for sid in list(self._active) if sid not in new_ids]
        for sub_id in to_remove:
            self._active[sub_id].task.cancel()
        if to_remove:
            await _wait_cancelled([self._active[sub_id].task for sub_id in to_remove])
            for sub_id in to_remove:
                try:
                    await _call_with_timeout(
                        self._cycle_executor.run(
                            _write_consumer_status, sub_id, "disconnected", None
                        ),
                        timeout=_DB_TIMEOUT,
                    )
                except Exception as exc:
                    log.warning(
                        "Cannot mark removed subscription %s disconnected: %s", sub_id, exc
                    )
                self._active.pop(sub_id, None)

        # start tasks for new subscriptions, dead ones, or changed ones (hot-reload)
        for sub in subscriptions:
            sub_id = sub["id"]
            entry = self._active.get(sub_id)
            if entry is None or entry.task.done() or self._subs_changed(sub_id, sub):
                if entry is not None and not entry.task.done():
                    entry.task.cancel()
                    await _wait_cancelled([entry.task])
                task = asyncio.create_task(self._consume_subscription(sub))
                self._active[sub_id] = _ActiveSub(
                    task=task, sub=sub.copy(), state=_ConsumerState(sub_id, self._executor)
                )

        # close connections no longer referenced by any subscription
        active_conn_ids = {sub["conn_id"] for sub in subscriptions}
        for key in [k for k in list(self._connections) if k[0] not in active_conn_ids]:
            conn_id, role = key
            try:
                await _call_with_timeout(self._connections.pop(key).close(), timeout=_CLOSE_TIMEOUT)
            except Exception:
                pass
            if role == _ROLE_PUBLISH:
                self._publish_channels.pop(conn_id, None)

        # Forget everything else remembered per conn_id. A conn_id that comes back later
        # is a new connection and starts from a clean slate: a leftover drop cycle would
        # hold off its first legitimate recreation, and a leftover verdict would be
        # written into its status row.
        self._forget_conn_state(active_conn_ids)

        # manage cooldown infrastructure
        cooldown_dag_ids: set[str] = set()
        cooldown_conn_ids: set[str] = set()
        for sub in subscriptions:
            if sub.get("cooldown", 0) > 0:
                cooldown_dag_ids.add(sub["dag_id"])
                cooldown_conn_ids.add(sub["conn_id"])
        # One fire consumer serves every cooldown subscription, and which conn_id carries
        # it is decided by sorting rather than by the order the rows came back in: the
        # query behind that list has no ORDER BY, PostgreSQL reorders rows an UPDATE
        # touched, and every status transition updates exactly these rows. Reading the
        # choice off that order would move the fire consumer during the reconnect
        # turbulence this feature exists to survive, and each move costs a cancel.
        fire_conn_id = min(cooldown_conn_ids) if cooldown_conn_ids else None
        self._warn_on_split_cooldown(cooldown_conn_ids, fire_conn_id)

        if cooldown_dag_ids and fire_conn_id is not None:
            await self._provision_cooldown(cooldown_dag_ids, fire_conn_id)
            running = self._fire_state
            moved = running is not None and running.conn_id != fire_conn_id
            if moved and self._fire_task is not None:
                # The fire consumer holds the connection object it was handed for its
                # whole life. Left running against the old conn_id it would keep
                # rmq_watcher.fire without a consumer on the new one and cooldown DAGs
                # would silently stop firing.
                log.info(
                    "Cooldown subscriptions moved from conn_id=%r to %r — restarting the "
                    "fire consumer on the new connection",
                    running.conn_id if running else None, fire_conn_id,
                )
                self._fire_task.cancel()
                await _wait_cancelled([self._fire_task])
                self._fire_task = None
                self._fire_state = None
            if self._fire_task is None or self._fire_task.done():
                connection = self._connections.get((fire_conn_id, _ROLE_CONSUME))
                if connection is not None:
                    self._launch_fire_task(fire_conn_id, connection)
                else:
                    log.warning(
                        "Fire task cannot start: connection %s not available after provisioning",
                        fire_conn_id,
                    )
        elif not cooldown_dag_ids:
            if self._fire_task is not None and not self._fire_task.done():
                self._fire_task.cancel()
                await _wait_cancelled([self._fire_task])
            self._fire_task = None
            self._fire_state = None

        # Orphan check runs unconditionally so that removing a dag_id from an otherwise
        # active set of cooldown subscriptions is still detected even when RMQ provisioning
        # fails (i.e. _provision_cooldown returns early in its except block).
        self._check_orphaned_pending_queues(cooldown_dag_ids)

        # Same unconditional-orphan-check rationale as cooldown above, applied to
        # exchange-mode sub queues/bindings.
        active_exchange_dag_ids = {s["dag_id"] for s in exchange_subs}
        self._check_orphaned_exchange_bindings(active_exchange_dag_ids)

        await self._recover_dead_consumers(subscriptions)

        await self._update_all_conn_counts(subscriptions)

    def _warn_on_split_cooldown(
        self, cooldown_conn_ids: set[str], fire_conn_id: str | None
    ) -> None:
        """Warn while cooldown subscriptions are spread over more than one broker.

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

    def _forget_conn_state(self, active_conn_ids: set[str]) -> None:
        """Drop per-conn_id bookkeeping for conn_ids no subscription mentions."""
        for tracked in (
            self._timeouts,
            self._publish_timeouts,
            self._last_drop_cycle,
            self._liveness,
            self._mgmt_failures,
            self._stuck_cycles,
        ):
            for conn_id in [c for c in tracked if c not in active_conn_ids]:
                del tracked[conn_id]

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
            setup_channel = await _call_with_timeout(connection.channel(), timeout=rpc_timeout)
            try:
                await _ensure_fire_infrastructure(setup_channel, timeout=rpc_timeout)
                for dag_id in cooldown_dag_ids:
                    await _ensure_pending_queue(setup_channel, dag_id, timeout=rpc_timeout)
            finally:
                try:
                    await _call_with_timeout(setup_channel.close(), timeout=_CLOSE_TIMEOUT)
                except Exception:
                    pass
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
            verdict = self._liveness.get(conn_id)
            rows.append((
                conn_id,
                count,
                verdict.status if verdict is not None else None,
                verdict.reason if verdict is not None else None,
                verdict.broker_consumers if verdict is not None else None,
            ))

        try:
            await _call_with_timeout(
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
        timeouts = self._timeouts.get(conn_id)
        return timeouts.rpc if timeouts else DEFAULT_RPC_TIMEOUT

    async def _drop_connection(self, conn_id: str, role: str | None = None) -> None:
        """Close and forget the connection(s) of ``conn_id`` — both roles, or one named.

        The entry leaves the pool before ``close()`` is attempted, so a connection whose
        close hangs is still gone from the cache: a zombie connection answers every call
        with silence, and keeping it would hand the same dead object to the next caller.
        """
        roles = (role,) if role is not None else (_ROLE_CONSUME, _ROLE_PUBLISH)
        for pooled_role in roles:
            connection = self._connections.pop((conn_id, pooled_role), None)
            if pooled_role == _ROLE_PUBLISH:
                self._publish_channels.pop(conn_id, None)
            if connection is None:
                continue
            try:
                await _call_with_timeout(connection.close(), timeout=_CLOSE_TIMEOUT)
            except Exception as exc:
                log.warning(
                    "Closing the %s connection of conn_id=%r failed: %s — dropped from the "
                    "pool anyway",
                    pooled_role, conn_id, exc,
                )
        if role is None:
            # Only a full drop feeds the liveness rate limit: the publish role has its
            # own gate of consecutive publish timeouts, and letting it move this mark
            # would postpone recovery of a consuming connection that went silent at the
            # same time.
            self._last_drop_cycle[conn_id] = self._cycle_no

    def _may_drop_connection(self, conn_id: str) -> bool:
        """Whether ``conn_id`` may be recreated in this cycle.

        The same connection is torn down at most once every
        :data:`_DROP_COOLDOWN_CYCLES` cycles, so a verdict that keeps coming back
        negative — a misclassification, or a fault that lives outside the connection —
        surfaces as a rare logged event instead of a silent recreation loop.
        """
        last_cycle = self._last_drop_cycle.get(conn_id)
        return last_cycle is None or self._cycle_no - last_cycle >= _DROP_COOLDOWN_CYCLES

    async def _get_publish_channel(self, conn_id: str) -> Any:
        """Return a channel on the publish connection of ``conn_id``, opening it on demand."""
        channel = self._publish_channels.get(conn_id)
        if channel is not None and not channel.is_closed:
            return channel
        connection = await self._get_or_create_connection(conn_id, role=_ROLE_PUBLISH)
        channel = await _call_with_timeout(
            connection.channel(), timeout=self._rpc_timeout(conn_id)
        )
        self._publish_channels[conn_id] = channel
        return channel

    async def _get_connection_info(
        self, conn_id: str, executor: BoundedExecutor | None = None
    ) -> Any:
        """Read the Airflow connection of ``conn_id`` off the loop thread, under a timeout.

        ``BaseHook.get_connection`` is a metadata-database query, and a database that
        stopped answering must not decide how long a reconcile cycle lasts.
        """
        pool = executor if executor is not None else self._executor
        return await _call_with_timeout(
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
        """
        pool = executor if executor is not None else self._executor
        key = (conn_id, role)
        # Fast path: a live connection is already pooled
        connection = self._connections.get(key)
        if connection is not None and not connection.is_closed:
            return connection

        # Slow path: acquire lock to prevent duplicate connection creation
        async with self._conn_lock:
            connection = self._connections.get(key)
            if connection is not None:
                if not connection.is_closed:
                    return connection
                # A closed connection never revives — replace it with a fresh one.
                del self._connections[key]
                if role == _ROLE_PUBLISH:
                    self._publish_channels.pop(conn_id, None)

            conn_info = await self._get_connection_info(conn_id, pool)
            url, ssl_context = build_amqp_connection(conn_info)
            timeouts = get_amqp_timeouts(conn_info)
            self._timeouts[conn_id] = timeouts
            kwargs: dict[str, Any] = {"url": url, "timeout": timeouts.connect}
            if ssl_context is not None:
                kwargs["ssl_context"] = ssl_context

            try:
                connection = await aio_pika.connect_robust(**kwargs)
                self._connections[key] = connection
            except Exception as exc:
                try:
                    await _call_with_timeout(
                        pool.run(_write_conn_error, conn_id, str(exc)),
                        timeout=_DB_TIMEOUT,
                    )
                except Exception as write_exc:
                    log.warning(
                        "Cannot store the failed connection attempt for conn_id=%r: %s",
                        conn_id, write_exc,
                    )
                raise

            return connection

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
                    "management_url not set on connection %r — skipping "
                    "bind-diff for DAG %r (queue %s%s still declared, will "
                    "retry next cycle)",
                    conn_id, dag_id, _SUB_QUEUE_PREFIX, dag_id,
                )
                return True

            if conn_info.login is None or conn_info.password is None:
                log.error(
                    "Connection %r has no login/password set — skipping "
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
                log.error(
                    "Management API bind-diff failed for DAG %r (queue %s, "
                    "exchange %r): %s — skipping bind-diff this cycle, queue "
                    "still declared",
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
            try:
                connection = await self._get_or_create_connection(
                    conn_id, executor=self._cycle_executor
                )
                rpc_timeout = self._rpc_timeout(conn_id)
                setup_channel = await _call_with_timeout(
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
                                setup_channel = await _call_with_timeout(
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
                    try:
                        await _call_with_timeout(setup_channel.close(), timeout=_CLOSE_TIMEOUT)
                    except Exception:
                        pass
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
                continue
            except Exception as exc:
                log.error(
                    "Failed to provision exchange infrastructure (exchange=%r, conn_id=%r): "
                    "%s. Ordinary consumers continue. Will retry on next reconcile cycle.",
                    exchange, conn_id, exc,
                )
                continue

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
            conn_id=conn_id, state=_ConsumerState(None, self._executor)
        )
        self._fire_task = asyncio.create_task(
            self._consume_fire_queue(connection, conn_id)
        )

    def _restart_reason(self, conn_id: str | None) -> str:
        """Why the last liveness check condemned ``conn_id``, for the restart log."""
        verdict = self._liveness.get(conn_id) if conn_id is not None else None
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

        await _wait_cancelled(cancelled)

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
            _incr("rmq_watcher.consumer_restarted")

        if restart_fire and fire_conn_id is not None:
            try:
                connection = await self._get_or_create_connection(
                    fire_conn_id, executor=self._cycle_executor
                )
            except Exception as exc:
                log.warning(
                    "Fire consumer cannot restart: connection %r is unavailable: %s — "
                    "the next reconcile cycle tries again",
                    fire_conn_id, exc,
                )
            else:
                self._launch_fire_task(fire_conn_id, connection)
                _incr("rmq_watcher.consumer_restarted")

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

        A conn_id that offers no candidate at all is judged on its own terms. With no
        running task it is reported as an error outright. With running tasks that never
        reach ``listening`` for :data:`_STUCK_CYCLES_BEFORE_DROP` cycles the connection
        is asked directly, through :meth:`_probe_connection`, whether it answers an RPC:
        silence is the shape of a pooled connection whose ``channel()`` never returns,
        where every task retries onto the same silence and the check would otherwise see
        nothing to judge, and the connection is condemned. An answer means the tasks are
        attached to a working connection and are failing after it — a trigger that keeps
        raising leaves a subscription in ``error`` just as surely — so the connection
        stays and the row says so.

        The verdict of each conn_id is kept in ``self._liveness`` for the status writer.
        """
        self._cycle_no += 1
        self._liveness = {}
        self._consumer_cache = {}
        self._fire_needs_restart = False

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
            if entry.state.status != "listening" or entry.state.consumer_tag is None:
                stalled.setdefault(conn_id, []).append(sub)
                continue
            candidates.setdefault(conn_id, []).append((sub, entry))

        fire = self._fire_state
        fire_tag = fire.state.consumer_tag if fire is not None else None
        fire_candidate = (
            fire is not None
            and fire_tag is not None
            and self._fire_task is not None
            and not self._fire_task.done()
            and fire.state.status == "listening"
        )
        if fire_candidate and fire is not None:
            candidates.setdefault(fire.conn_id, [])

        to_restart: set[int] = set()
        to_recreate: set[str] = set()

        for conn_id, subs in candidates.items():
            self._stuck_cycles.pop(conn_id, None)
            fire_here = fire_candidate and fire is not None and fire.conn_id == conn_id
            queues = {sub["queue_name"] for sub, _ in subs}
            expected_tags = {entry.state.consumer_tag for _, entry in subs}
            if fire_here:
                queues.add(_FIRE_QUEUE)
                expected_tags.add(fire_tag)

            live_tags, broker_count, reason = await self._probe_consumers(
                conn_id, queues, expected_tags
            )
            if live_tags is None:
                self._liveness[conn_id] = _ConnLiveness(
                    status=None, broker_consumers=None, reason=reason
                )
                continue

            unseen: set[int] = set()
            dead_subs: set[int] = set()
            for sub, entry in subs:
                if entry.state.consumer_tag in live_tags:
                    entry.negative_checks = 0
                    continue
                unseen.add(sub["id"])
                entry.negative_checks += 1
                log.warning(
                    "Broker does not know consumer %s of subscription %d (queue %r, "
                    "conn_id=%r) — negative check %d of %d",
                    entry.state.consumer_tag, sub["id"], sub["queue_name"], conn_id,
                    entry.negative_checks, _NEGATIVE_CHECKS_BEFORE_RESTART,
                )
                if entry.negative_checks >= _NEGATIVE_CHECKS_BEFORE_RESTART:
                    dead_subs.add(sub["id"])

            fire_unseen = False
            if fire_here and fire is not None:
                if fire_tag in live_tags:
                    fire.negative_checks = 0
                else:
                    fire_unseen = True
                    fire.negative_checks += 1
                    log.warning(
                        "Broker does not know the fire consumer on conn_id=%r "
                        "(queue %r) — negative check %d of %d",
                        conn_id, _FIRE_QUEUE, fire.negative_checks,
                        _NEGATIVE_CHECKS_BEFORE_RESTART,
                    )
                    if fire.negative_checks >= _NEGATIVE_CHECKS_BEFORE_RESTART:
                        fire.negative_checks = 0
                        self._fire_needs_restart = True

            if not unseen and not fire_unseen:
                self._liveness[conn_id] = _ConnLiveness(
                    status="connected", broker_consumers=broker_count
                )
                continue

            unseen_reason = _unseen_reason(len(unseen), fire_unseen)
            if not dead_subs:
                # The restart waits for a second negative check in a row, the reported
                # status does not: the broker is holding none of these consumers right
                # now, and a green row would claim the opposite for a whole reconcile
                # interval.
                checks = max(
                    [entry.negative_checks for sub, entry in subs
                     if sub["id"] in unseen]
                    + ([fire.negative_checks] if fire_unseen and fire is not None else [])
                )
                self._liveness[conn_id] = _ConnLiveness(
                    status="error",
                    broker_consumers=broker_count,
                    reason=(
                        f"{unseen_reason} — negative check {checks} of "
                        f"{_NEGATIVE_CHECKS_BEFORE_RESTART}, recovery starts once that "
                        f"many checks agree"
                    ),
                )
                continue

            if not self._may_drop_connection(conn_id):
                self._liveness[conn_id] = self._held_back_verdict(
                    conn_id, broker_count, unseen_reason,
                )
                continue

            confirmed = sorted(
                sub["id"] for sub, _ in subs if sub["id"] not in dead_subs
            )
            if confirmed:
                log.warning(
                    "Connection %r is being recreated for %d unseen subscription(s); "
                    "subscription(s) %s share it and keep their own retry loop — they "
                    "surface the drop as a transient consumer error",
                    conn_id, len(dead_subs), confirmed,
                )
            to_restart |= dead_subs
            to_recreate.add(conn_id)
            self._liveness[conn_id] = _ConnLiveness(
                status="error",
                broker_consumers=broker_count,
                reason=reason or "consumer not registered on the broker",
            )

        for conn_id, live in live_tasks.items():
            if conn_id in candidates:
                continue
            if not live:
                self._stuck_cycles.pop(conn_id, None)
                self._liveness[conn_id] = _ConnLiveness(
                    status="error",
                    broker_consumers=None,
                    reason="no consumer task of this connection is running",
                )
                continue
            stuck = self._stuck_cycles.get(conn_id, 0) + 1
            self._stuck_cycles[conn_id] = stuck
            if stuck < _STUCK_CYCLES_BEFORE_DROP:
                self._liveness[conn_id] = _ConnLiveness(
                    status=None, broker_consumers=None, reason=None
                )
                continue

            # Nothing said so far about *why* these tasks are not consuming, so ask the
            # connection itself. A task sitting in ``error`` because its trigger keeps
            # failing is attached to a perfectly healthy connection, and condemning that
            # would blame the broker for a database outage.
            answers, probe_reason = await self._probe_connection(
                conn_id, {sub["queue_name"] for sub in stalled.get(conn_id, [])}
            )
            if answers:
                # The counter keeps running: nothing here is fixed, and dropping it
                # would leave the next cycle with no verdict to report at all.
                self._liveness[conn_id] = _ConnLiveness(
                    status="error",
                    broker_consumers=None,
                    reason=(
                        f"{live} task(s) of this connection have not reached 'listening' "
                        f"for {stuck} cycles, and the connection answers an RPC — the "
                        f"fault is downstream of it; see the subscriptions' own errors"
                    ),
                )
                log.warning(
                    "Connection %r has %d running task(s) and not one of them is "
                    "consuming after %d cycles, yet the connection answers an RPC — "
                    "leaving it in place: what stops these tasks is not the connection",
                    conn_id, live, stuck,
                )
                continue

            reason = (
                f"{live} task(s) of this connection have not reached 'listening' for "
                f"{stuck} cycles, and {probe_reason}"
            )
            log.warning(
                "Connection %r has %d running task(s) and not one of them is consuming "
                "after %d cycles, and %s — recreating it: a pooled connection that "
                "answers no RPC hands the same silence to every task that retries on it",
                conn_id, live, stuck, probe_reason,
            )
            if not self._may_drop_connection(conn_id):
                self._liveness[conn_id] = self._held_back_verdict(
                    conn_id, None, reason
                )
                continue
            self._stuck_cycles.pop(conn_id, None)
            to_recreate.add(conn_id)
            self._liveness[conn_id] = _ConnLiveness(
                status="error", broker_consumers=None, reason=reason
            )

        return to_restart, to_recreate

    def _held_back_verdict(
        self, conn_id: str, broker_count: int | None, reason: str
    ) -> _ConnLiveness:
        """Verdict for a ``conn_id`` the rate limit keeps from being recreated again.

        Repeated verdicts mean either the check misjudges the connection or the fault
        is not in the connection, so the drop is refused and the row says ``degraded``
        rather than silently looping through recreations.
        """
        log.warning(
            "Connection %r is condemned again after %d cycle(s), sooner than the "
            "%d-cycle limit allows — leaving it in place. Repeated verdicts mean "
            "either the check misjudges it or the fault is not in the connection.",
            conn_id, self._cycle_no - self._last_drop_cycle[conn_id],
            _DROP_COOLDOWN_CYCLES,
        )
        return _ConnLiveness(
            status="degraded",
            broker_consumers=broker_count,
            reason=(
                f"{reason}, but the connection was already recreated less than "
                f"{_DROP_COOLDOWN_CYCLES} cycles ago"
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
        whose reply covers the whole vhost and is therefore cached for the cycle: several
        conn_ids often point at one broker, and asking it once per conn_id multiplies the
        same request. Without a ``management_url`` — and after
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
                "Cannot read connection %r for the liveness check: %s — liveness "
                "unknown this cycle, counters unchanged",
                conn_id, exc,
            )
            return None, None, str(exc)

        if (
            management_url is not None
            and conn_info.login is not None
            and conn_info.password is not None
        ):
            vhost = conn_info.schema or "/"
            cache_key = (management_url, vhost)
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
                    failures = self._mgmt_failures.get(conn_id, 0) + 1
                    self._mgmt_failures[conn_id] = failures
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
            self._mgmt_failures.pop(conn_id, None)
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
            channel = await _call_with_timeout(connection.channel(), timeout=rpc_timeout)
            await _call_with_timeout(
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
                try:
                    await _call_with_timeout(channel.close(), timeout=_CLOSE_TIMEOUT)
                except Exception:
                    pass
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
        trigger_backoff = _TRIGGER_BACKOFF_START
        publish_backoff = _PUBLISH_BACKOFF_START

        while True:
            await state.write("connecting")
            try:
                connection = await self._get_or_create_connection(conn_id)
                rpc_timeout = self._rpc_timeout(conn_id)
                channel = await _call_with_timeout(connection.channel(), timeout=rpc_timeout)
                queue = await _call_with_timeout(
                    channel.declare_queue(queue_name, passive=True), timeout=rpc_timeout
                )
                # No set_qos on purpose: messages that miss the filter are NACKed with
                # requeue (ADR-0002) and come back to the head of the queue, so any finite
                # prefetch window fills up with them and consumption stops for good once
                # the misses reach the window size. The unacked window stays unbounded.
                consumer_tag = _consumer_tag(sub_id, _attach_nonce())
                state.consumer_tag = consumer_tag
                await state.write("listening", last_error=None)
                log.info(
                    "Subscription %d (DAG %s) is consuming queue %r on conn_id=%r",
                    sub_id, dag_id, queue_name, conn_id,
                )
                _incr("rmq_watcher.consumer_reconnect")

                async with queue.iterator(consumer_tag=consumer_tag) as q_iter:
                    async for message in q_iter:
                        if cooldown > 0:
                            # Cooldown mode: match-only check, then publish to pending queue
                            if not _match(message, msg_filter):
                                await _nack_and_sleep(message)
                                continue
                            try:
                                await self._publish_pending(
                                    conn_id, dag_id, cooldown, message
                                )
                            except asyncio.CancelledError:
                                raise
                            except Exception as exc:
                                # The delivery is already back on the queue; pausing
                                # before leaving the iterator keeps the broker from
                                # handing it straight back while the publish still
                                # cannot go through.
                                log.warning(
                                    "Publishing the cooldown placeholder for DAG %s "
                                    "failed: %s — the delivery is back on the queue, "
                                    "pausing %.1fs before consuming again",
                                    dag_id, exc, publish_backoff,
                                )
                                await asyncio.sleep(publish_backoff)
                                publish_backoff = min(
                                    publish_backoff * 2, _PUBLISH_BACKOFF_MAX
                                )
                                raise
                            publish_backoff = _PUBLISH_BACKOFF_START
                        else:
                            # Immediate mode: ACK only once the DAG run exists, so a
                            # failed trigger returns the delivery instead of losing it.
                            if not _match(message, msg_filter):
                                await _nack_and_sleep(message)
                                continue
                            try:
                                outcome = await self._trigger_dag(
                                    dag_id, queue_name, sub_id, message
                                )
                            except asyncio.CancelledError:
                                raise
                            except Exception as exc:
                                log.warning(
                                    "Triggering DAG %s for subscription %d failed: %s "
                                    "— requeueing the delivery, pausing %.1fs",
                                    dag_id, sub_id, exc, trigger_backoff,
                                )
                                await message.nack(requeue=True)
                                # The connection is fine and the iterator keeps running,
                                # so nothing else would ever report that this
                                # subscription stopped starting DAG runs.
                                await state.write("error", last_error=str(exc))
                                await asyncio.sleep(trigger_backoff)
                                trigger_backoff = min(
                                    trigger_backoff * 2, _TRIGGER_BACKOFF_MAX
                                )
                                continue
                            trigger_backoff = _TRIGGER_BACKOFF_START
                            await state.write("listening", last_error=None)
                            await message.ack()
                            if outcome == _OUTCOME_TRIGGERED:
                                _incr("rmq_watcher.dag_triggered")

                # The iterator finished without an exception — the broker cancelled the
                # consumer. Pause before subscribing again so a broker that keeps ending
                # it right away cannot spin this loop.
                state.consumer_tag = None
                await asyncio.sleep(_RECONNECT_DELAY)

            except asyncio.CancelledError:
                return

            except aio_pika.exceptions.ChannelNotFoundEntity as exc:
                # Fatal: queue doesn't exist — exit and wait for reconciliation to restart
                await state.write("error", last_error=str(exc))
                log.error(
                    "Queue %r not found for subscription %d (DAG %s): %s",
                    queue_name, sub_id, dag_id, exc,
                )
                return

            except aio_pika.exceptions.ChannelClosed as exc:
                # Recoverable: channel dropped (e.g. queue deleted at runtime)
                log.warning(
                    "Channel closed for subscription %d (queue %r): %s — retrying in %ss",
                    sub_id, queue_name, exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

            except Exception as exc:
                log.warning(
                    "Transient error in consumer %d (queue %r): %s — retrying in %ss",
                    sub_id, queue_name, exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

    async def _publish_pending(
        self, conn_id: str, dag_id: str, cooldown: int, message: Any
    ) -> None:
        """Publish the cooldown placeholder for ``dag_id`` and ACK the delivery behind it.

        The publish runs on the publish connection of ``conn_id``, so a broker blocking
        publishers under a resource alarm leaves the consuming connection — and with it
        every ``basic.ack`` — untouched.

        A broker that rejects the publish has done its job: the pending queue is declared
        ``x-max-length=1`` with ``x-overflow=reject-publish``, so a rejection means the
        cooldown for this dag_id is already counting down and a second placeholder would
        add nothing. That is the ordinary case while a cooldown window is open, and the
        delivery is acknowledged — requeueing it would redeliver the same message for the
        whole window and burn the quorum-queue delivery limit on it.

        A publish that fails for any other reason returns the delivery to the queue right
        away: leaving the loop hands back only what is still buffered in the iterator, and
        this message, already taken out of it, would otherwise sit unacknowledged on the
        abandoned channel until the broker's ``consumer_timeout``.
        """
        pending_queue = f"{_PENDING_QUEUE_PREFIX}{dag_id}"
        try:
            channel = await self._get_publish_channel(conn_id)
            await _call_with_timeout(
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
            # The message came back unrouted: the pending queue is not there at all.
            await self._handle_publish_failure(conn_id, message, exc)
            raise
        except aio_pika.exceptions.DeliveryError:
            log.debug(
                "Cooldown placeholder for DAG %s was rejected by %s — a cooldown window "
                "is already open, acknowledging the delivery",
                dag_id, pending_queue,
            )
            self._publish_timeouts.pop(conn_id, None)
            await message.ack()
            return
        except Exception as exc:
            await self._handle_publish_failure(conn_id, message, exc)
            raise
        self._publish_timeouts.pop(conn_id, None)
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

        if not isinstance(exc, (asyncio.TimeoutError, TimeoutError)):
            self._publish_timeouts.pop(conn_id, None)
            return

        count = self._publish_timeouts.get(conn_id, 0) + 1
        self._publish_timeouts[conn_id] = count
        if count < _PUBLISH_TIMEOUTS_BEFORE_DROP:
            return

        log.warning(
            "Publish timed out %d times in a row on conn_id=%r — recreating its publish "
            "connection; consumers of this conn_id keep their own connection",
            count, conn_id,
        )
        self._publish_timeouts.pop(conn_id, None)
        await self._drop_connection(conn_id, role=_ROLE_PUBLISH)

    async def _consume_fire_queue(self, connection: Any, conn_id: str) -> None:
        """Consumer for rmq_watcher.fire queue — triggers DAGs after cooldown expires via DLX."""
        state = self._fire_state_of()
        while True:
            await state.write("connecting")
            try:
                rpc_timeout = self._rpc_timeout(conn_id)
                channel = await _call_with_timeout(connection.channel(), timeout=rpc_timeout)
                queue = await _call_with_timeout(
                    channel.declare_queue(_FIRE_QUEUE, passive=True), timeout=rpc_timeout
                )
                fire_tag = _consumer_tag("fire", _attach_nonce())
                state.consumer_tag = fire_tag
                await state.write("listening")
                log.info(
                    "Cooldown fire consumer is consuming queue %r on conn_id=%r",
                    _FIRE_QUEUE, conn_id,
                )
                _incr("rmq_watcher.consumer_reconnect")

                async with queue.iterator(consumer_tag=fire_tag) as q_iter:
                    async for message in q_iter:
                        dag_id = message.routing_key or ""
                        if not dag_id:
                            log.warning(
                                "Fire queue message has no routing_key — skipping"
                            )
                            await message.ack()
                            continue
                        if not message.message_id:
                            log.warning(
                                "Fire queue message has no message_id — skipping (idempotency broken)"
                            )
                            await message.ack()
                            continue
                        run_id = _safe_run_id(
                            f"rmq_cooldown__{dag_id}__{message.message_id}"
                        )
                        conf = {
                            "source": "cooldown",
                            "dag_id": dag_id,
                            "body": "",
                            "headers": {},
                            "routing_key": dag_id,
                            "queue": _FIRE_QUEUE,
                            "subscription_id": None,
                        }
                        outcome = await _call_with_timeout(
                            self._executor.run(_sync_trigger, dag_id, conf, run_id),
                            timeout=_TRIGGER_TIMEOUT,
                        )
                        if outcome == _OUTCOME_TRIGGERED:
                            _incr("rmq_watcher.dag_triggered")
                        # Known limitation: if _sync_trigger returned early because the
                        # DAG is paused/inactive, the message is still ACKed here and the
                        # fire event is permanently lost. This is intentional — the DLX
                        # message has already spent its TTL and re-queuing would cause an
                        # infinite loop. Operators should ensure DAGs are active before
                        # enabling cooldown subscriptions.
                        await message.ack()

            except asyncio.CancelledError:
                return

            except aio_pika.exceptions.ChannelNotFoundEntity as exc:
                await state.write("error", last_error=str(exc))
                log.error(
                    "Fire queue %r not found: %s — exiting fire consumer, "
                    "will restart on next reconcile cycle.",
                    _FIRE_QUEUE, exc,
                )
                return

            except aio_pika.exceptions.ChannelClosed as exc:
                await state.write("error", last_error=str(exc))
                log.warning(
                    "Fire queue channel closed: %s — retrying in %ss",
                    exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

            except Exception as exc:
                await state.write("error", last_error=str(exc))
                log.warning(
                    "Transient error in fire consumer: %s — retrying in %ss",
                    exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

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
        return await _call_with_timeout(
            self._executor.run(_sync_trigger, dag_id, conf, run_id),
            timeout=_TRIGGER_TIMEOUT,
        )


async def _ensure_fire_infrastructure(channel: Any, timeout: float) -> None:
    """Declare the fire exchange and queue idempotently.

    - Exchange: rmq_watcher.fire (topic, durable)
    - Queue:    rmq_watcher.fire (durable, binding key=#)
    """
    exchange = await _call_with_timeout(
        channel.declare_exchange(
            _FIRE_EXCHANGE,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
        ),
        timeout=timeout,
    )
    queue = await _call_with_timeout(
        channel.declare_queue(
            _FIRE_QUEUE,
            durable=True,
        ),
        timeout=timeout,
    )
    await _call_with_timeout(queue.bind(exchange, routing_key="#"), timeout=timeout)


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
    await _call_with_timeout(
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

    exchange_obj = await _call_with_timeout(
        channel.declare_exchange(
            exchange,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
            arguments={"alternate-exchange": unrouted_exchange_name},
        ),
        timeout=timeout,
    )

    unrouted_exchange_obj = await _call_with_timeout(
        channel.declare_exchange(
            unrouted_exchange_name,
            type=aio_pika.ExchangeType.FANOUT,
            durable=True,
        ),
        timeout=timeout,
    )
    unrouted_queue = await _call_with_timeout(
        channel.declare_queue(
            unrouted_exchange_name,
            durable=True,
            arguments={"x-message-ttl": _EXCHANGE_TTL_MS},
        ),
        timeout=timeout,
    )
    await _call_with_timeout(unrouted_queue.bind(unrouted_exchange_obj), timeout=timeout)

    log_queue = await _call_with_timeout(
        channel.declare_queue(
            log_queue_name,
            durable=True,
            arguments={"x-message-ttl": _EXCHANGE_TTL_MS},
        ),
        timeout=timeout,
    )
    await _call_with_timeout(log_queue.bind(exchange_obj, routing_key="#"), timeout=timeout)


async def _ensure_sub_queue(channel: Any, dag_id: str, timeout: float) -> Any:
    """Declare the per-DAG exchange-mode sub queue idempotently and return it.

    Queue: ``rmq_watcher.sub.{dag_id}`` — durable, ``x-message-ttl=_EXCHANGE_TTL_MS`` (8h).

    Unlike the cooldown pending queue, this queue is actively consumed by a live consumer
    (the same ``_consume_subscription`` used for ``queue=`` mode) — the TTL here is purely a
    safety net against unbounded growth if the subscription becomes orphaned (see
    ADR-0005), not a timer mechanism.
    """
    queue_name = f"{_SUB_QUEUE_PREFIX}{dag_id}"
    return await _call_with_timeout(
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
        await _call_with_timeout(queue.bind(exchange, routing_key=routing_key), timeout=timeout)
        log.info(
            "Bound queue %s to exchange %r with routing_key=%r",
            queue.name, exchange, routing_key,
        )

    for routing_key in sorted(to_unbind):
        await _call_with_timeout(queue.unbind(exchange, routing_key=routing_key), timeout=timeout)
        log.info(
            "Unbound queue %s from exchange %r with routing_key=%r",
            queue.name, exchange, routing_key,
        )
