from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import socket
import time
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

#: Workers of the fallback consumer pool. Sized well above the expected number of
#: subscriptions: every matched delivery occupies one worker for the duration of its
#: ``trigger_dag``.
_CONSUMER_POOL_WORKERS = 32

_default_executor: BoundedExecutor | None = None


def _consumer_executor() -> BoundedExecutor:
    """Process-wide pool for managers created without one of their own.

    The listener passes its own pool so that consumer work and cycle work never
    compete for the same workers; a manager built directly (a test, an embedding
    caller) still gets a bounded pool rather than the loop's default executor, which
    dies with the loop and cannot be shut down without risking a deadlock.
    """
    global _default_executor
    if _default_executor is None:
        _default_executor = BoundedExecutor("rmq-consumer", _CONSUMER_POOL_WORKERS)
    return _default_executor

_FIRE_EXCHANGE = "rmq_watcher.fire"
_FIRE_QUEUE = "rmq_watcher.fire"
_PENDING_QUEUE_PREFIX = "rmq_watcher.pending."
_EXCHANGE_TTL_MS = 28800000  # 8h — safety net against unbounded orphan queue growth


def _consumer_tag(suffix: Any) -> str:
    """Build the consumer tag this process registers on a queue.

    The tag carries host, pid and subscription id, so a liveness check can pick our
    own consumer out of the queue's consumer list: the same queue legitimately
    carries foreign consumers and the second scheduler replica in HA mode, and a
    plain consumer count cannot tell them apart from ours.
    """
    return f"rmq_watcher.{socket.gethostname()}.{os.getpid()}.{suffix}"


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


class _ConsumerState:
    """In-memory guard: writes consumer_status to DB only when the status actually changes.

    Prevents hot DB writes during reconnect storms (e.g. 20+/min → 2-4/min).

    A ``sub_id`` of ``None`` tracks the status in memory only — the fire consumer runs
    on the shared ``rmq_watcher.fire`` queue and has no row in ``rmq_subscriptions``.
    """

    def __init__(self, sub_id: int | None) -> None:
        self._sub_id = sub_id
        self._last_status: str | None = None

    @property
    def status(self) -> str | None:
        """Status last written for this subscription, ``None`` before the first write."""
        return self._last_status

    def write(self, status: str, last_error: str | None = None) -> None:
        if status == self._last_status:
            return
        if self._sub_id is not None:
            with WatcherSession() as session:
                set_consumer_status(session, self._sub_id, status, last_error=last_error)
                session.commit()
        self._last_status = status


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

    def __init__(self, executor: BoundedExecutor | None = None) -> None:
        self._executor = executor if executor is not None else _consumer_executor()
        self._active: dict[int, _ActiveSub] = {}  # sub_id → _ActiveSub
        self._connections: dict[tuple[str, str], Any] = {}  # (conn_id, role) → RobustConnection
        self._publish_channels: dict[str, Any] = {}  # conn_id → channel of the publish connection
        self._timeouts: dict[str, AmqpTimeouts] = {}  # conn_id → call timeouts from its extra
        self._publish_timeouts: dict[str, int] = {}  # conn_id → consecutive publish timeouts
        self._last_drop_at: dict[str, float] = {}  # conn_id → monotonic time of last drop
        self._last_drop_cycle: dict[str, int] = {}  # conn_id → cycle of its last full drop
        self._cycle_no = 0  # liveness checks performed, i.e. reconcile cycles
        self._liveness: dict[str, _ConnLiveness] = {}  # conn_id → verdict of the last check
        self._conn_lock = asyncio.Lock()  # prevents duplicate connections on concurrent starts
        self._fire_task: asyncio.Task | None = None
        self._fire_state: _FireSub | None = None  # state record of the running fire task
        self._fire_needs_restart = False  # last check found the fire consumer gone
        self._cooldown_tracker = OrphanTracker()  # dag_ids for which pending queues were created
        self._exchange_tracker = OrphanTracker()  # dag_ids for which sub queues/bindings were created
        self._http_client: httpx.AsyncClient | None = None  # Management API client

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
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)

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
            await asyncio.gather(
                *(self._active[sub_id].task for sub_id in to_remove),
                return_exceptions=True,
            )
            for sub_id in to_remove:
                try:
                    with WatcherSession() as session:
                        set_consumer_status(session, sub_id, "disconnected")
                        session.commit()
                except Exception:
                    pass
                self._active.pop(sub_id, None)

        # start tasks for new subscriptions, dead ones, or changed ones (hot-reload)
        for sub in subscriptions:
            sub_id = sub["id"]
            entry = self._active.get(sub_id)
            if entry is None or entry.task.done() or self._subs_changed(sub_id, sub):
                if entry is not None and not entry.task.done():
                    entry.task.cancel()
                    await asyncio.gather(entry.task, return_exceptions=True)
                task = asyncio.create_task(self._consume_subscription(sub))
                self._active[sub_id] = _ActiveSub(
                    task=task, sub=sub.copy(), state=_ConsumerState(sub_id)
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

        # manage cooldown infrastructure
        cooldown_dag_ids: set[str] = set()
        fire_conn_id: str | None = None
        for sub in subscriptions:
            if sub.get("cooldown", 0) > 0:
                cooldown_dag_ids.add(sub["dag_id"])
                if fire_conn_id is None:
                    fire_conn_id = sub["conn_id"]

        if cooldown_dag_ids and fire_conn_id is not None:
            await self._provision_cooldown(cooldown_dag_ids, fire_conn_id)
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
                await asyncio.gather(self._fire_task, return_exceptions=True)
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

        self._update_all_conn_counts(subscriptions)

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
            connection = await self._get_or_create_connection(conn_id)
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

    def _update_all_conn_counts(self, subscriptions: list[dict]) -> None:
        """Write the status row of every conn_id the subscription list mentions.

        A conn_id whose tasks have all died still gets a row: a row that simply stops
        being updated is indistinguishable from a healthy one, which is how a dead
        watcher showed green for a day.

        ``status`` follows the verdict of the liveness check rather than the number of
        running tasks — a task that is not done proves nothing about the broker. A check
        that produced no data leaves the stored status alone, so an unreachable
        Management API does not paint every connection red, while ``last_reconcile_at``
        is written with an explicit value on every cycle: without it SQLAlchemy emits no
        UPDATE at all once the other fields stop changing, and the timestamp freezes.
        """
        counts: dict[str, int] = {}
        for sub in subscriptions:
            conn_id = sub["conn_id"]
            entry = self._active.get(sub["id"])
            alive = 1 if entry is not None and not entry.task.done() else 0
            counts[conn_id] = counts.get(conn_id, 0) + alive

        if not counts:
            return

        # Naive UTC: the view compares this stamp with a naive utcnow() of its own, and
        # mixing naive and aware values would raise straight from the template.
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        try:
            with WatcherSession() as session:
                stored = {row.conn_id: row.status for row in get_conn_statuses(session)}
                for conn_id, count in counts.items():
                    verdict = self._liveness.get(conn_id)
                    if verdict is not None and verdict.status is not None:
                        status = verdict.status
                    else:
                        status = stored.get(
                            conn_id, "connected" if count else "disconnected"
                        )
                    upsert_conn_status(
                        session,
                        conn_id,
                        status,
                        consumer_count=count,
                        last_error=verdict.reason if verdict is not None else None,
                        broker_consumer_count=(
                            verdict.broker_consumers if verdict is not None else None
                        ),
                        last_reconcile_at=now,
                    )
                session.commit()
        except Exception as exc:
            log.warning("Cannot write connection status rows: %s", exc)

    def _rpc_timeout(self, conn_id: str | None) -> float:
        """Seconds allowed for a single AMQP RPC on ``conn_id``.

        The value comes from the connection's ``extra`` and is cached when the
        connection is built; before that — and for callers that have no conn_id — the
        provider default applies.
        """
        timeouts = self._timeouts.get(conn_id) if conn_id else None
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
        self._last_drop_at[conn_id] = time.monotonic()
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

    async def _get_or_create_connection(self, conn_id: str, role: str = _ROLE_CONSUME) -> Any:
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

            conn_info = await self._executor.run(BaseHook.get_connection, conn_id)
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
                    with WatcherSession() as session:
                        upsert_conn_status(
                            session, conn_id, "error",
                            consumer_count=0, last_error=str(exc),
                        )
                        session.commit()
                except Exception:
                    pass
                raise

            return connection

    async def _provision_one_exchange_sub(
        self,
        setup_channel: Any,
        exchange: str,
        sub: dict,
        http_client: httpx.AsyncClient,
        rpc_timeout: float = DEFAULT_RPC_TIMEOUT,
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

            conn_info = await self._executor.run(BaseHook.get_connection, conn_id)
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
                connection = await self._get_or_create_connection(conn_id)
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
        return entry.state if entry is not None else _ConsumerState(sub_id)

    def _fire_state_of(self) -> _ConsumerState:
        """State record the manager keeps for the fire consumer."""
        return self._fire_state.state if self._fire_state is not None else _ConsumerState(None)

    def _launch_fire_task(self, conn_id: str, connection: Any) -> None:
        """Start the fire consumer on an open connection of ``conn_id``.

        The conn_id is recorded next to the task: the fire consumer keeps the
        connection object it was handed for its whole life, so whoever recreates that
        connection has to know which task is holding the old one.
        """
        self._fire_state = _FireSub(conn_id=conn_id, state=_ConsumerState(None))
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

        if cancelled:
            await asyncio.gather(*cancelled, return_exceptions=True)

        for conn_id in sorted(to_recreate):
            await self._drop_connection(conn_id)

        for sub_id in sorted(to_restart):
            sub = by_id.get(sub_id)
            if sub is None:
                continue
            self._active[sub_id] = _ActiveSub(
                task=asyncio.create_task(self._consume_subscription(sub)),
                sub=sub.copy(),
                state=_ConsumerState(sub_id),
            )
            _incr("rmq_watcher.consumer_restarted")

        if restart_fire and fire_conn_id is not None:
            try:
                connection = await self._get_or_create_connection(fire_conn_id)
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

        The verdict of each conn_id is kept in ``self._liveness`` for the status writer.
        """
        self._cycle_no += 1
        self._liveness = {}
        self._fire_needs_restart = False

        candidates: dict[str, list[dict]] = {}
        for sub in subscriptions:
            entry = self._active.get(sub["id"])
            if entry is None or entry.task.done() or entry.state.status != "listening":
                continue
            candidates.setdefault(sub["conn_id"], []).append(sub)

        fire = self._fire_state
        fire_candidate = (
            fire is not None
            and self._fire_task is not None
            and not self._fire_task.done()
            and fire.state.status == "listening"
        )
        if fire_candidate and fire is not None:
            candidates.setdefault(fire.conn_id, [])

        to_restart: set[int] = set()
        to_recreate: set[str] = set()

        for conn_id, subs in candidates.items():
            fire_here = fire_candidate and fire is not None and fire.conn_id == conn_id
            queues = {sub["queue_name"] for sub in subs}
            expected_tags = {_consumer_tag(sub["id"]) for sub in subs}
            if fire_here:
                queues.add(_FIRE_QUEUE)
                expected_tags.add(_consumer_tag("fire"))

            live_tags, broker_count, reason = await self._probe_consumers(
                conn_id, queues, expected_tags
            )
            if live_tags is None:
                self._liveness[conn_id] = _ConnLiveness(
                    status=None, broker_consumers=None, reason=reason
                )
                continue

            dead_subs: set[int] = set()
            for sub in subs:
                entry = self._active.get(sub["id"])
                if entry is None:
                    continue
                if _consumer_tag(sub["id"]) in live_tags:
                    entry.negative_checks = 0
                    continue
                entry.negative_checks += 1
                log.warning(
                    "Broker does not know consumer %s of subscription %d (queue %r, "
                    "conn_id=%r) — negative check %d of %d",
                    _consumer_tag(sub["id"]), sub["id"], sub["queue_name"], conn_id,
                    entry.negative_checks, _NEGATIVE_CHECKS_BEFORE_RESTART,
                )
                if entry.negative_checks >= _NEGATIVE_CHECKS_BEFORE_RESTART:
                    dead_subs.add(sub["id"])

            if fire_here and fire is not None:
                if _consumer_tag("fire") in live_tags:
                    fire.negative_checks = 0
                else:
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

            if not dead_subs:
                self._liveness[conn_id] = _ConnLiveness(
                    status="connected", broker_consumers=broker_count
                )
                continue

            if not self._may_drop_connection(conn_id):
                held_back = (
                    f"{len(dead_subs)} subscription(s) unseen by the broker, but the "
                    f"connection was already recreated less than "
                    f"{_DROP_COOLDOWN_CYCLES} cycles ago"
                )
                log.warning(
                    "Connection %r is condemned again after %d cycle(s), sooner than the "
                    "%d-cycle limit allows — leaving it in place. Repeated verdicts mean "
                    "either the check misjudges it or the fault is not in the connection.",
                    conn_id, self._cycle_no - self._last_drop_cycle[conn_id],
                    _DROP_COOLDOWN_CYCLES,
                )
                self._liveness[conn_id] = _ConnLiveness(
                    status="degraded", broker_consumers=broker_count, reason=held_back
                )
                continue

            to_restart |= dead_subs
            to_recreate.add(conn_id)
            self._liveness[conn_id] = _ConnLiveness(
                status="error",
                broker_consumers=broker_count,
                reason=reason or "consumer not registered on the broker",
            )

        return to_restart, to_recreate

    async def _probe_consumers(
        self, conn_id: str, queues: set[str], expected_tags: set[str]
    ) -> tuple[set[str] | None, int | None, str | None]:
        """Ask ``conn_id`` which of our consumers the broker currently holds.

        :param conn_id: Airflow connection whose consumers are being verified.
        :param queues: Queue names our consumers of this conn_id are subscribed to.
        :param expected_tags: Consumer tags those subscriptions registered.
        :returns: ``(live tags, consumers the broker reports, reason)``, where live tags
            of ``None`` means the check produced no data and the counters stay untouched.

        With a ``management_url`` the answer comes from ``GET /api/consumers/{vhost}``.
        Without one the probe is a passive declare on a fresh channel: it says nothing
        about individual consumers, so its success vouches for every tag of this conn_id
        and its failure — a raised error or a call that never returns — condemns them all.
        """
        conn_info = None
        management_url = None
        if self._http_client is not None:
            try:
                conn_info = await self._executor.run(BaseHook.get_connection, conn_id)
                management_url = get_management_url(conn_info)
            except Exception as exc:
                log.warning(
                    "Cannot read connection %r for the liveness check: %s — liveness "
                    "unknown this cycle, counters unchanged",
                    conn_id, exc,
                )
                return None, None, str(exc)

        if (
            self._http_client is not None
            and management_url is not None
            and conn_info is not None
            and conn_info.login is not None
            and conn_info.password is not None
        ):
            try:
                by_queue = await get_queue_consumers(
                    self._http_client,
                    management_url,
                    conn_info.schema or "/",
                    (conn_info.login, conn_info.password),
                )
            except Exception as exc:
                log.warning(
                    "Management API did not answer the consumer list for conn_id=%r: %s "
                    "— liveness unknown this cycle, counters unchanged",
                    conn_id, exc,
                )
                return None, None, str(exc)
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
        queue_name = min(queues)
        rpc_timeout = self._rpc_timeout(conn_id)
        channel = None
        try:
            connection = await self._get_or_create_connection(conn_id)
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
            return set(), None, reason
        finally:
            if channel is not None:
                try:
                    await _call_with_timeout(channel.close(), timeout=_CLOSE_TIMEOUT)
                except Exception:
                    pass
        return set(expected_tags), None, None

    async def _consume_subscription(self, sub: dict) -> None:
        sub_id: int = sub["id"]
        dag_id: str = sub["dag_id"]
        queue_name: str = sub["queue_name"]
        conn_id: str = sub["conn_id"]
        cooldown: int = sub.get("cooldown", 0) or 0
        msg_filter = MessageFilter.deserialize(sub.get("filter_data") or {})
        state = self._state_of(sub_id)
        consumer_tag = _consumer_tag(sub_id)
        # Kept across reconnects: a broken trigger path stays broken through the
        # reconnect that a NACKed delivery may well cause.
        trigger_backoff = _TRIGGER_BACKOFF_START

        while True:
            state.write("connecting")
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
                state.write("listening", last_error=None)

                async with queue.iterator(consumer_tag=consumer_tag) as q_iter:
                    async for message in q_iter:
                        if cooldown > 0:
                            # Cooldown mode: match-only check, then publish to pending queue
                            if not _match(message, msg_filter):
                                await _nack_and_sleep(message)
                                continue
                            await self._publish_pending(conn_id, dag_id, cooldown, message)
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
                                await asyncio.sleep(trigger_backoff)
                                trigger_backoff = min(
                                    trigger_backoff * 2, _TRIGGER_BACKOFF_MAX
                                )
                                continue
                            trigger_backoff = _TRIGGER_BACKOFF_START
                            await message.ack()
                            if outcome == _OUTCOME_TRIGGERED:
                                _incr("rmq_watcher.dag_triggered")

                # The iterator finished without an exception — the broker cancelled the
                # consumer. Pause before subscribing again so a broker that keeps ending
                # it right away cannot spin this loop.
                await asyncio.sleep(_RECONNECT_DELAY)

            except asyncio.CancelledError:
                return

            except aio_pika.exceptions.ChannelNotFoundEntity as exc:
                # Fatal: queue doesn't exist — exit and wait for reconciliation to restart
                state.write("error", last_error=str(exc))
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

        A failed publish returns the delivery to the queue right away: leaving the loop
        hands back only what is still buffered in the iterator, and this message, already
        taken out of it, would otherwise sit unacknowledged on the abandoned channel until
        the broker's ``consumer_timeout``.
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

    async def _consume_fire_queue(self, connection: Any, conn_id: str | None = None) -> None:
        """Consumer for rmq_watcher.fire queue — triggers DAGs after cooldown expires via DLX."""
        state = self._fire_state_of()
        while True:
            state.write("connecting")
            try:
                rpc_timeout = self._rpc_timeout(conn_id)
                channel = await _call_with_timeout(connection.channel(), timeout=rpc_timeout)
                queue = await _call_with_timeout(
                    channel.declare_queue(_FIRE_QUEUE, passive=True), timeout=rpc_timeout
                )
                state.write("listening")

                async with queue.iterator(consumer_tag=_consumer_tag("fire")) as q_iter:
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
                        outcome = await self._executor.run(
                            _sync_trigger, dag_id, conf, run_id
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
                state.write("error", last_error=str(exc))
                log.error(
                    "Fire queue %r not found: %s — exiting fire consumer, "
                    "will restart on next reconcile cycle.",
                    _FIRE_QUEUE, exc,
                )
                return

            except aio_pika.exceptions.ChannelClosed as exc:
                state.write("error", last_error=str(exc))
                log.warning(
                    "Fire queue channel closed: %s — retrying in %ss",
                    exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

            except Exception as exc:
                state.write("error", last_error=str(exc))
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
        return await self._executor.run(_sync_trigger, dag_id, conf, run_id)


async def _ensure_fire_infrastructure(
    channel: Any, timeout: float = DEFAULT_RPC_TIMEOUT
) -> None:
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


async def _ensure_pending_queue(
    channel: Any, dag_id: str, timeout: float = DEFAULT_RPC_TIMEOUT
) -> None:
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
    channel: Any, exchange: str, timeout: float = DEFAULT_RPC_TIMEOUT
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


async def _ensure_sub_queue(
    channel: Any, dag_id: str, timeout: float = DEFAULT_RPC_TIMEOUT
) -> Any:
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
    timeout: float = DEFAULT_RPC_TIMEOUT,
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
