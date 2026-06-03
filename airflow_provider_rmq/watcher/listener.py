from __future__ import annotations

import ast
import asyncio
import glob
import logging
import os
import threading
from typing import Any

from airflow.listeners import hookimpl

from airflow_provider_rmq.watcher.consumer import RMQConsumerManager
from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    WatcherSession,
    get_enabled_subscriptions,
    upsert_subscription,
)

log = logging.getLogger(__name__)

_DEFAULT_RECONCILE_INTERVAL = 60


def _parse_rmq_trigger_decorator(node: ast.expr) -> dict | None:
    """Return subscription dict if node is an rmq_trigger(...) call, else None.

    Handles both bare name ``rmq_trigger(...)`` and attribute access
    ``decorators.rmq_trigger(...)``.  Only literal argument values are extracted;
    non-literal expressions are skipped (subscription won't be registered from
    AST scan — user should create it via the UI instead).
    """
    if not isinstance(node, ast.Call):
        return None
    func = node.func
    is_rmq = (
        (isinstance(func, ast.Name) and func.id == "rmq_trigger")
        or (isinstance(func, ast.Attribute) and func.attr == "rmq_trigger")
    )
    if not is_rmq:
        return None

    kwargs: dict = {}
    # positional: rmq_trigger("queue_name")
    if node.args:
        val = node.args[0]
        if isinstance(val, ast.Constant) and isinstance(val.value, str):
            kwargs["queue_name"] = val.value
    # keyword arguments
    for kw in node.keywords:
        if kw.arg not in ("queue", "conn_id", "filter_data"):
            continue
        try:
            value = ast.literal_eval(kw.value)
        except (ValueError, TypeError):
            continue
        if kw.arg == "queue":
            kwargs["queue_name"] = value
        else:
            kwargs[kw.arg] = value

    if "queue_name" not in kwargs:
        return None

    return {
        "queue_name": kwargs["queue_name"],
        "conn_id": kwargs.get("conn_id", "rmq_default"),
        "filter_data": kwargs.get("filter_data", {}),
    }


class RMQWatcherListener:
    """Airflow Listener that runs a background RabbitMQ consumer loop inside the Scheduler process.

    Lifecycle:
    - ``on_starting`` fires when the Scheduler process starts; we spawn a daemon thread
      with its own asyncio event loop.
    - The loop reconciles subscriptions from DAG files (mtime-based scan) and the DB
      every ``reconcile_interval`` seconds, then delegates to ``RMQConsumerManager``.
    - ``before_stopping`` sets a stop event; the loop exits after the current iteration.
    """

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event: threading.Event | None = None
        self._manager: RMQConsumerManager | None = None
        # mtime-based incremental scan state (lives in the daemon thread only)
        self._last_mtimes: dict[str, float] = {}   # filepath → mtime
        self._cached_subs: dict[str, list[dict]] = {}  # filepath → list[sub dict]

    # ------------------------------------------------------------------
    # Listener API
    # ------------------------------------------------------------------

    @hookimpl
    def on_starting(self, component: Any) -> None:
        name = type(component).__name__
        log.info("RMQWatcherListener.on_starting: component=%s", name)
        if "Scheduler" in name:
            self._start()

    @hookimpl
    def before_stopping(self, component: Any) -> None:
        if self._stop_event is not None:
            self._stop_event.set()

    # ------------------------------------------------------------------
    # Thread / event-loop bootstrap
    # ------------------------------------------------------------------

    def _start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            if self._stop_event is None or not self._stop_event.is_set():
                log.warning("RMQ Watcher thread already running — ignoring duplicate on_starting")
                return
            # Previous lifecycle is shutting down — wait briefly then start fresh
            log.info("RMQ Watcher: waiting for previous thread to stop...")
            self._thread.join(timeout=10)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="rmq-watcher",
            daemon=True,
        )
        self._thread.start()

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._main())
            except Exception:
                log.exception("RMQ Watcher loop crashed — restarting in 30s")
            finally:
                loop.close()
            if not self._stop_event.is_set():
                self._stop_event.wait(timeout=30)
        log.info("RMQ Watcher loop stopped")

    async def _main(self) -> None:
        self._manager = RMQConsumerManager()
        await self._manager.start()
        try:
            while not self._stop_event.is_set():
                try:
                    scanned = self._scan_subscriptions()
                    self._sync_to_db(scanned)

                    with WatcherSession() as session:
                        active_subs = [
                            {
                                "id": sub.id,
                                "dag_id": sub.dag_id,
                                "queue_name": sub.queue_name,
                                "conn_id": sub.conn_id,
                                "filter_data": sub.filter_data or {},
                            }
                            for sub in get_enabled_subscriptions(session)
                        ]

                    await self._manager.reconcile(active_subs)
                except Exception:
                    log.exception("Error in RMQ Watcher reconciliation cycle")

                await asyncio.sleep(self._get_reconcile_interval())
        finally:
            await self._manager.stop()

    def _get_reconcile_interval(self) -> int:
        try:
            from airflow.models import Variable
            val = Variable.get("rmq_watcher_reconcile_interval", default_var=None)
            if val is not None:
                return int(val)
        except Exception:
            pass
        return _DEFAULT_RECONCILE_INTERVAL

    # ------------------------------------------------------------------
    # DAG-file scanning (mtime-based incremental)
    # ------------------------------------------------------------------

    def _get_dags_folder(self) -> str:
        try:
            from airflow.configuration import conf as airflow_conf
            return airflow_conf.get("core", "dags_folder")
        except Exception:
            return "/opt/airflow/dags"

    def _scan_subscriptions(self) -> list[dict]:
        """Incrementally scan DAG files for @rmq_trigger subscriptions using mtime.

        On a stable deployment (no file changes) this costs only N mtime syscalls.
        Changed or new files are re-parsed; deleted files are evicted from the cache.
        Memory is bounded to the number of current DAG files on disk.
        """
        dags_folder = self._get_dags_folder()
        current_files = set(glob.glob(f"{dags_folder}/**/*.py", recursive=True))

        # Evict deleted files
        for path in list(self._last_mtimes):
            if path not in current_files:
                self._last_mtimes.pop(path)
                self._cached_subs.pop(path, None)

        # Re-parse changed or new files
        for path in current_files:
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if mtime == self._last_mtimes.get(path):
                continue
            subs = self._extract_subscriptions_from_file(path)
            self._cached_subs[path] = subs
            self._last_mtimes[path] = mtime

        # Flatten all cached subscriptions
        result: list[dict] = []
        for subs in self._cached_subs.values():
            result.extend(subs)
        return result

    def _extract_subscriptions_from_file(self, path: str) -> list[dict]:
        """Extract @rmq_trigger subscriptions from a DAG file via AST parsing.

        AST parsing never executes the file and never acquires the Python import
        lock, so it is safe to call from a background thread inside the Scheduler
        process.  DagBag would acquire the import lock and could deadlock with the
        Scheduler's own import activity, causing heartbeat failures and tasks being
        marked as killed externally.

        Limitation: dag_id is taken from the decorated function name. If the user
        passes an explicit dag_id= to @dag(...), the function name is still used.
        In practice most TaskFlow DAGs use the function name as dag_id.
        """
        try:
            with open(path, encoding="utf-8") as f:
                source = f.read()
            tree = ast.parse(source, filename=path)
        except (SyntaxError, OSError, UnicodeDecodeError) as exc:
            log.warning("Failed to read/parse DAG file %s: %s", path, exc)
            return []

        result: list[dict] = []
        for node in ast.walk(tree):
            if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                continue
            for decorator in node.decorator_list:
                sub = _parse_rmq_trigger_decorator(decorator)
                if sub is not None:
                    sub["dag_id"] = node.name
                    result.append(sub)
        return result

    # ------------------------------------------------------------------
    # DB synchronisation
    # ------------------------------------------------------------------

    def _sync_to_db(self, scanned: list[dict]) -> None:
        """Reconcile dag_file subscriptions in DB with the current scan result.

        - Upserts all subscriptions found in code (source='dag_file').
        - Deletes dag_file subscriptions that no longer exist in code.
        - Never touches ui-sourced subscriptions.
        """
        scanned_keys = {
            (s["dag_id"], s["queue_name"], s.get("conn_id", "rmq_default"))
            for s in scanned
        }

        with WatcherSession() as session:
            existing = (
                session.query(RMQSubscription)
                .filter_by(source="dag_file")
                .all()
            )

            for sub in existing:
                key = (sub.dag_id, sub.queue_name, sub.conn_id)
                if key not in scanned_keys:
                    session.query(RMQSubscription).filter_by(
                        dag_id=sub.dag_id,
                        queue_name=sub.queue_name,
                        conn_id=sub.conn_id,
                        source="dag_file",
                    ).delete()

            for s in scanned:
                upsert_subscription(
                    session,
                    dag_id=s["dag_id"],
                    queue_name=s["queue_name"],
                    conn_id=s.get("conn_id", "rmq_default"),
                    filter_data=s.get("filter_data", {}),
                    source="dag_file",
                )

            session.commit()
