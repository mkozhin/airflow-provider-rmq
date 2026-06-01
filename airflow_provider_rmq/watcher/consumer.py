from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any

import aio_pika
import aio_pika.exceptions
from airflow.hooks.base import BaseHook
from airflow.models import DagModel
from sqlalchemy.exc import IntegrityError

from airflow_provider_rmq.utils.amqp import build_amqp_connection, match_and_ack
from airflow_provider_rmq.utils.filters import MessageFilter
from airflow_provider_rmq.watcher.models import (
    WatcherSession,
    set_consumer_status,
    upsert_conn_status,
)

log = logging.getLogger(__name__)

_RECONNECT_DELAY = 5.0


def _build_run_id(queue_name: str) -> str:
    return f"rmq__{queue_name}__{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}"


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


class RMQConsumerManager:
    """Manages a pool of asyncio tasks — one per subscription — each consuming one RMQ queue.

    Connection pooling: one ``connect_robust`` connection per ``conn_id``; multiple subscriptions
    sharing the same conn_id reuse the same connection (each gets its own channel).
    """

    def __init__(self) -> None:
        self._tasks: dict[int, asyncio.Task] = {}
        self._connections: dict[str, Any] = {}  # conn_id → RobustConnection
        self._conn_lock = asyncio.Lock()  # prevents duplicate connections on concurrent starts
        self._sub_conn_ids: dict[int, str] = {}  # sub_id → conn_id for count tracking

    async def start(self) -> None:
        """No-op: connections and tasks are created on demand."""

    async def stop(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        for conn in list(self._connections.values()):
            try:
                await conn.close()
            except Exception:
                pass
        self._tasks.clear()
        self._connections.clear()
        self._sub_conn_ids.clear()

    async def reconcile(self, subscriptions: list[dict]) -> None:
        """Sync running tasks with the current subscription list.

        Cancels tasks for removed subscriptions, starts tasks for new ones,
        and restarts tasks that exited due to fatal errors (task.done()).
        """
        new_ids = {sub["id"] for sub in subscriptions}

        # cancel tasks for removed subscriptions
        to_remove = [sid for sid in list(self._tasks) if sid not in new_ids]
        for sub_id in to_remove:
            self._tasks[sub_id].cancel()
        if to_remove:
            await asyncio.gather(
                *(self._tasks.pop(sub_id) for sub_id in to_remove),
                return_exceptions=True,
            )
            for sub_id in to_remove:
                self._sub_conn_ids.pop(sub_id, None)

        # start tasks for new subscriptions or dead ones (fatal exit → reconciliation restarts)
        for sub in subscriptions:
            sub_id = sub["id"]
            if sub_id not in self._tasks or self._tasks[sub_id].done():
                task = asyncio.create_task(self._consume_subscription(sub))
                self._tasks[sub_id] = task
                self._sub_conn_ids[sub_id] = sub["conn_id"]

        # close connections no longer referenced by any subscription
        active_conn_ids = {sub["conn_id"] for sub in subscriptions}
        for conn_id in [c for c in list(self._connections) if c not in active_conn_ids]:
            try:
                await self._connections.pop(conn_id).close()
            except Exception:
                pass

        self._update_all_conn_counts(subscriptions)

    def _update_all_conn_counts(self, subscriptions: list[dict]) -> None:
        counts: dict[str, int] = {}
        for sub in subscriptions:
            cid = sub["conn_id"]
            task = self._tasks.get(sub["id"])
            if task and not task.done():
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

    async def _consume_subscription(self, sub: dict) -> None:
        sub_id: int = sub["id"]
        dag_id: str = sub["dag_id"]
        queue_name: str = sub["queue_name"]
        conn_id: str = sub["conn_id"]
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

    async def _trigger_dag(
        self,
        dag_id: str,
        queue_name: str,
        sub_id: int,
        message: Any,
    ) -> None:
        conf = {
            "body": message.body.decode("utf-8"),
            "headers": dict(message.headers or {}),
            "routing_key": getattr(message, "routing_key", "") or "",
            "queue": queue_name,
            "subscription_id": sub_id,
        }
        run_id = _build_run_id(queue_name)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, _sync_trigger, dag_id, conf, run_id)
