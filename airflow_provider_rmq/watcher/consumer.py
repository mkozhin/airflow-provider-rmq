from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import aio_pika
import aio_pika.exceptions
import httpx
from airflow.hooks.base import BaseHook
from airflow.models import DagModel
from sqlalchemy.exc import IntegrityError

from airflow_provider_rmq.utils.amqp import (
    build_amqp_connection,
    match_and_ack,
    match as _match,
    nack_and_sleep as _nack_and_sleep,
)
from airflow_provider_rmq.utils.filters import MessageFilter
from airflow_provider_rmq.utils.management import get_current_bindings, get_management_url
from airflow_provider_rmq.watcher.models import (
    WatcherSession,
    set_consumer_status,
    upsert_conn_status,
)
from airflow_provider_rmq.watcher.orphan_tracker import OrphanTracker

log = logging.getLogger(__name__)

_RECONNECT_DELAY = 5.0

_FIRE_EXCHANGE = "rmq_watcher.fire"
_FIRE_QUEUE = "rmq_watcher.fire"
_PENDING_QUEUE_PREFIX = "rmq_watcher.pending."
_SUB_QUEUE_PREFIX = "rmq_watcher.sub."
_EXCHANGE_TTL_MS = 28800000  # 8h — safety net against unbounded orphan queue growth


def _build_run_id(queue_name: str) -> str:
    return f"rmq__{queue_name}__{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%f')}"


def _sync_trigger(dag_id: str, conf: dict, run_id: str) -> None:
    """Synchronous DAG trigger — called via run_in_executor from the consumer loop.

    Uses a short-lived WatcherSession to avoid polluting Airflow's thread-local
    scoped session. Skips trigger if the DAG is inactive or paused.
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
            return

    try:
        trigger_dag(dag_id=dag_id, run_id=run_id, conf=conf)
    except IntegrityError:
        log.warning("DAG run %s already exists (duplicate run_id), skipping", run_id)


class _ConsumerState:
    """In-memory guard: writes consumer_status to DB only when the status actually changes.

    Prevents hot DB writes during reconnect storms (e.g. 20+/min → 2-4/min).
    """

    def __init__(self, sub_id: int) -> None:
        self._sub_id = sub_id
        self._last_status: str | None = None

    def write(self, status: str, last_error: str | None = None) -> None:
        if status == self._last_status:
            return
        with WatcherSession() as session:
            set_consumer_status(session, self._sub_id, status, last_error=last_error)
            session.commit()
        self._last_status = status


@dataclass
class _ActiveSub:
    """Snapshot of a running subscription consumer task."""
    task: asyncio.Task
    sub: dict  # full snapshot of sub at task start time


class RMQConsumerManager:
    """Manages a pool of asyncio tasks — one per subscription — each consuming one RMQ queue.

    Connection pooling: one ``connect_robust`` connection per ``conn_id``; multiple subscriptions
    sharing the same conn_id reuse the same connection (each gets its own channel).
    """

    def __init__(self) -> None:
        self._active: dict[int, _ActiveSub] = {}  # sub_id → _ActiveSub
        self._connections: dict[str, Any] = {}  # conn_id → RobustConnection
        self._conn_lock = asyncio.Lock()  # prevents duplicate connections on concurrent starts
        self._fire_task: asyncio.Task | None = None
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

        for conn in list(self._connections.values()):
            try:
                await conn.close()
            except Exception:
                pass
        self._active.clear()
        self._connections.clear()

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
                self._active[sub_id] = _ActiveSub(task=task, sub=sub.copy())

        # close connections no longer referenced by any subscription
        active_conn_ids = {sub["conn_id"] for sub in subscriptions}
        for conn_id in [c for c in list(self._connections) if c not in active_conn_ids]:
            try:
                await self._connections.pop(conn_id).close()
            except Exception:
                pass

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
                connection = self._connections.get(fire_conn_id)
                if connection is not None:
                    self._fire_task = asyncio.create_task(
                        self._consume_fire_queue(connection)
                    )
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

        # Orphan check runs unconditionally so that removing a dag_id from an otherwise
        # active set of cooldown subscriptions is still detected even when RMQ provisioning
        # fails (i.e. _provision_cooldown returns early in its except block).
        self._check_orphaned_pending_queues(cooldown_dag_ids)

        # Same unconditional-orphan-check rationale as cooldown above, applied to
        # exchange-mode sub queues/bindings.
        active_exchange_dag_ids = {s["dag_id"] for s in exchange_subs}
        self._check_orphaned_exchange_bindings(active_exchange_dag_ids)

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
            # Use a short-lived channel for setup operations
            setup_channel = await connection.channel()
            try:
                await _ensure_fire_infrastructure(setup_channel)
                for dag_id in cooldown_dag_ids:
                    await _ensure_pending_queue(setup_channel, dag_id)
            finally:
                try:
                    await setup_channel.close()
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
        counts: dict[str, int] = {}
        for sub in subscriptions:
            cid = sub["conn_id"]
            entry = self._active.get(sub["id"])
            if entry and not entry.task.done():
                counts[cid] = counts.get(cid, 0) + 1
        for conn_id, count in counts.items():
            try:
                with WatcherSession() as session:
                    upsert_conn_status(session, conn_id, "connected", consumer_count=count)
                    session.commit()
            except Exception:
                pass

    async def _get_or_create_connection(self, conn_id: str) -> Any:
        # Fast path: connection already exists
        if conn_id in self._connections:
            return self._connections[conn_id]

        # Slow path: acquire lock to prevent duplicate connection creation
        async with self._conn_lock:
            if conn_id in self._connections:
                return self._connections[conn_id]

            loop = asyncio.get_running_loop()
            conn_info = await loop.run_in_executor(None, BaseHook.get_connection, conn_id)
            url, ssl_context = build_amqp_connection(conn_info)
            kwargs: dict[str, Any] = {"url": url}
            if ssl_context is not None:
                kwargs["ssl_context"] = ssl_context

            try:
                connection = await aio_pika.connect_robust(**kwargs)
                self._connections[conn_id] = connection
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

        loop = asyncio.get_running_loop()

        for (conn_id, exchange), group in groups.items():
            try:
                connection = await self._get_or_create_connection(conn_id)
                setup_channel = await connection.channel()
                try:
                    await _ensure_exchange_infrastructure(setup_channel, exchange)

                    for sub in group:
                        dag_id = sub["dag_id"]
                        queue = await _ensure_sub_queue(setup_channel, dag_id)

                        conn_info = await loop.run_in_executor(
                            None, BaseHook.get_connection, conn_id
                        )
                        vhost = conn_info.schema or "/"
                        management_url = get_management_url(conn_info)
                        if management_url is None:
                            log.error(
                                "management_url not set on connection %r — skipping bind-diff "
                                "for DAG %r (queue %s%s still declared, will retry next cycle)",
                                conn_id, dag_id, _SUB_QUEUE_PREFIX, dag_id,
                            )
                            continue

                        auth = (conn_info.login, conn_info.password)
                        queue_name = f"{_SUB_QUEUE_PREFIX}{dag_id}"
                        try:
                            current = await get_current_bindings(
                                http_client, management_url, vhost,
                                queue_name, exchange, auth,
                            )
                        except Exception as exc:
                            log.error(
                                "Management API bind-diff failed for DAG %r (queue %s, "
                                "exchange %r): %s — skipping bind-diff this cycle, queue "
                                "still declared",
                                dag_id, queue_name, exchange, exc,
                            )
                            continue

                        desired = set(sub.get("routing_keys") or [])
                        await _sync_bindings(queue, exchange, desired, current)
                finally:
                    try:
                        await setup_channel.close()
                    except Exception:
                        pass
            except asyncio.CancelledError:
                raise
            except aio_pika.exceptions.ChannelPreconditionFailed as exc:
                log.error(
                    "Exchange %r already exists with conflicting properties "
                    "(PRECONDITION_FAILED) on conn_id=%r: %s. Sub queues for this group are "
                    "not provisioned. Will retry on next reconcile cycle.",
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

            self._exchange_tracker.mark_provisioned({sub["dag_id"] for sub in group})

    async def _consume_subscription(self, sub: dict) -> None:
        sub_id: int = sub["id"]
        dag_id: str = sub["dag_id"]
        queue_name: str = sub["queue_name"]
        conn_id: str = sub["conn_id"]
        cooldown: int = sub.get("cooldown", 0) or 0
        msg_filter = MessageFilter.deserialize(sub.get("filter_data") or {})
        state = _ConsumerState(sub_id)

        while True:
            state.write("connecting")
            try:
                connection = await self._get_or_create_connection(conn_id)
                channel = await connection.channel()
                queue = await channel.declare_queue(queue_name, passive=True)
                state.write("listening", last_error=None)

                async with queue.iterator() as q_iter:
                    async for message in q_iter:
                        if cooldown > 0:
                            # Cooldown mode: match-only check, then publish to pending queue
                            if not _match(message, msg_filter):
                                await _nack_and_sleep(message)
                                continue
                            msg_id = str(uuid.uuid4())
                            pending_queue = f"{_PENDING_QUEUE_PREFIX}{dag_id}"
                            await channel.default_exchange.publish(
                                aio_pika.Message(
                                    b"",
                                    expiration=str(cooldown * 1000),
                                    message_id=msg_id,
                                ),
                                routing_key=pending_queue,
                            )
                            await message.ack()
                        else:
                            # Immediate mode: existing match_and_ack + trigger_dag
                            matched = await match_and_ack(message, msg_filter)
                            if matched:
                                await self._trigger_dag(dag_id, queue_name, sub_id, message)

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

    async def _consume_fire_queue(self, connection: Any) -> None:
        """Consumer for rmq_watcher.fire queue — triggers DAGs after cooldown expires via DLX."""
        while True:
            try:
                channel = await connection.channel()
                queue = await channel.declare_queue(_FIRE_QUEUE, passive=True)

                async with queue.iterator() as q_iter:
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
                        run_id = f"rmq_cooldown__{dag_id}__{message.message_id}"
                        conf = {
                            "source": "cooldown",
                            "dag_id": dag_id,
                            "body": "",
                            "headers": {},
                            "routing_key": dag_id,
                            "queue": _FIRE_QUEUE,
                            "subscription_id": None,
                        }
                        loop = asyncio.get_running_loop()
                        await loop.run_in_executor(
                            None, _sync_trigger, dag_id, conf, run_id
                        )
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
                log.error(
                    "Fire queue %r not found: %s — exiting fire consumer, "
                    "will restart on next reconcile cycle.",
                    _FIRE_QUEUE, exc,
                )
                return

            except aio_pika.exceptions.ChannelClosed as exc:
                log.warning(
                    "Fire queue channel closed: %s — retrying in %ss",
                    exc, _RECONNECT_DELAY,
                )
                await asyncio.sleep(_RECONNECT_DELAY)

            except Exception as exc:
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
    ) -> None:
        conf = {
            "source": "immediate",
            "body": message.body.decode("utf-8", errors="replace"),
            "headers": dict(message.headers or {}),
            "routing_key": getattr(message, "routing_key", "") or "",
            "queue": queue_name,
            "subscription_id": sub_id,
        }
        run_id = _build_run_id(queue_name)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _sync_trigger, dag_id, conf, run_id)


async def _ensure_fire_infrastructure(channel: Any) -> None:
    """Declare the fire exchange and queue idempotently.

    - Exchange: rmq_watcher.fire (topic, durable)
    - Queue:    rmq_watcher.fire (durable, binding key=#)
    """
    exchange = await channel.declare_exchange(
        _FIRE_EXCHANGE,
        type=aio_pika.ExchangeType.TOPIC,
        durable=True,
    )
    queue = await channel.declare_queue(
        _FIRE_QUEUE,
        durable=True,
    )
    await queue.bind(exchange, routing_key="#")


async def _ensure_pending_queue(channel: Any, dag_id: str) -> None:
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
    await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={
            "x-dead-letter-exchange": _FIRE_EXCHANGE,
            "x-dead-letter-routing-key": dag_id,
            "x-max-length": 1,
            "x-overflow": "reject-publish",
        },
    )


async def _ensure_exchange_infrastructure(channel: Any, exchange: str) -> None:
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

    exchange_obj = await channel.declare_exchange(
        exchange,
        type=aio_pika.ExchangeType.TOPIC,
        durable=True,
        arguments={"alternate-exchange": unrouted_exchange_name},
    )

    unrouted_exchange_obj = await channel.declare_exchange(
        unrouted_exchange_name,
        type=aio_pika.ExchangeType.FANOUT,
        durable=True,
    )
    unrouted_queue = await channel.declare_queue(
        unrouted_exchange_name,
        durable=True,
        arguments={"x-message-ttl": _EXCHANGE_TTL_MS},
    )
    await unrouted_queue.bind(unrouted_exchange_obj)

    log_queue = await channel.declare_queue(
        log_queue_name,
        durable=True,
        arguments={"x-message-ttl": _EXCHANGE_TTL_MS},
    )
    await log_queue.bind(exchange_obj, routing_key="#")


async def _ensure_sub_queue(channel: Any, dag_id: str) -> Any:
    """Declare the per-DAG exchange-mode sub queue idempotently and return it.

    Queue: ``rmq_watcher.sub.{dag_id}`` — durable, ``x-message-ttl=_EXCHANGE_TTL_MS`` (8h).

    Unlike the cooldown pending queue, this queue is actively consumed by a live consumer
    (the same ``_consume_subscription`` used for ``queue=`` mode) — the TTL here is purely a
    safety net against unbounded growth if the subscription becomes orphaned (see
    ADR-0005), not a timer mechanism.
    """
    queue_name = f"{_SUB_QUEUE_PREFIX}{dag_id}"
    return await channel.declare_queue(
        queue_name,
        durable=True,
        arguments={"x-message-ttl": _EXCHANGE_TTL_MS},
    )


async def _sync_bindings(
    queue: Any,
    exchange: str,
    desired: set[str],
    current: set[str],
) -> None:
    """Bind/unbind a queue to an exchange so its live bindings match ``desired``.

    Binds every routing key in ``desired - current`` and unbinds every routing key in
    ``current - desired``; logs each change on INFO. No-op when ``desired == current``.
    """
    to_bind = desired - current
    to_unbind = current - desired

    for routing_key in sorted(to_bind):
        await queue.bind(exchange, routing_key=routing_key)
        log.info(
            "Bound queue %s to exchange %r with routing_key=%r",
            queue.name, exchange, routing_key,
        )

    for routing_key in sorted(to_unbind):
        await queue.unbind(exchange, routing_key=routing_key)
        log.info(
            "Unbound queue %s from exchange %r with routing_key=%r",
            queue.name, exchange, routing_key,
        )
