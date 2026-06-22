from __future__ import annotations

import ast
import asyncio
import glob
import logging
import os
import threading
import traceback
from typing import Any

from airflow.listeners import hookimpl

from airflow_provider_rmq.watcher.consumer import RMQConsumerManager
from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    WatcherSession,
    get_enabled_subscriptions,
    upsert_subscription,
)
from airflow_provider_rmq.watcher.subscription_builder import (
    build_subscriptions,
    has_exchange_conflict,
)

log = logging.getLogger(__name__)

_DEFAULT_RECONCILE_INTERVAL = 60


def _extract_dag_id_from_decorators(decorators: list[ast.expr]) -> str | None:
    """Return explicit dag_id from @dag(dag_id='...') or None to fall back to the function name."""
    for dec in decorators:
        if not isinstance(dec, ast.Call):
            continue
        func = dec.func
        is_dag = (
            (isinstance(func, ast.Name) and func.id == "dag")
            or (isinstance(func, ast.Attribute) and func.attr == "dag")
        )
        if not is_dag:
            continue
        for kw in dec.keywords:
            if kw.arg == "dag_id":
                try:
                    value = ast.literal_eval(kw.value)
                    if isinstance(value, str):
                        return value
                except (ValueError, TypeError):
                    log.warning(
                        "rmq_trigger: dag_id= is not a string literal — falling back to function name"
                    )
                break
    return None


_RMQ_TRIGGER_KWARGS = (
    "queue",
    "queues",
    "exchange",
    "routing_keys",
    "routing_key_ids",
    "routing_key_status",
    "conn_id",
    "filter_data",
    "cooldown",
)


def _parse_rmq_trigger_decorator(node: ast.expr, dag_id: str) -> list[dict]:
    """Return list of subscription dicts if node is an rmq_trigger(...) call, else [].

    Handles both bare name ``rmq_trigger(...)`` and attribute access
    ``decorators.rmq_trigger(...)``.  Only literal argument values are extracted;
    non-literal expressions are skipped (subscription won't be registered from
    AST scan — user should create it via the UI instead).

    Validation/construction of the subscription dict is delegated to
    ``build_subscriptions()`` (``subscription_builder.py``) — this function is
    only responsible for the AST-specific part: extracting literal kwarg values
    via ``ast.literal_eval``. Any ``ValueError`` raised by ``build_subscriptions``
    (mutex violation, empty lists, dots in ids/status, negative cooldown, ...)
    is logged as a WARNING and the subscription is skipped — the same graceful
    degradation pattern already used for non-literal values.

    Returns a list:
    - empty list if node is not an rmq_trigger call, required args are missing,
      or validation failed
    - one dict for ``queue=``/``exchange=`` (single subscription)
    - N dicts for ``queues=[...]`` (one per queue in the list)

    ``group_key`` is NOT set here — it is set in _extract_subscriptions_from_file
    where dag_id is known.
    """
    if not isinstance(node, ast.Call):
        return []
    func = node.func
    is_rmq = (
        (isinstance(func, ast.Name) and func.id == "rmq_trigger")
        or (isinstance(func, ast.Attribute) and func.attr == "rmq_trigger")
    )
    if not is_rmq:
        return []

    kwargs: dict = {}
    # positional: rmq_trigger("queue_name")
    if node.args:
        val = node.args[0]
        if isinstance(val, ast.Constant) and isinstance(val.value, str):
            kwargs["queue"] = val.value
    # keyword arguments
    for kw in node.keywords:
        if kw.arg not in _RMQ_TRIGGER_KWARGS:
            continue
        try:
            value = ast.literal_eval(kw.value)
        except (ValueError, TypeError):
            continue
        kwargs[kw.arg] = value

    if "queue" not in kwargs and "queues" not in kwargs and "exchange" not in kwargs:
        return []

    try:
        return build_subscriptions(dag_id=dag_id, **kwargs)
    except ValueError as exc:
        log.warning("rmq_trigger: skipping invalid subscription for dag_id=%s: %s", dag_id, exc)
        return []


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
        job_type = getattr(component, 'job_type', '') or ''
        # In Airflow 2.9+, on_starting fires inside Job.__init__() before super().__init__()
        # sets job_type, so job_type is always None here. The class is renamed from
        # SchedulerJobRunner to Job (ORM model). Detect scheduler via call-stack:
        # scheduler_command.py is present for the scheduler, triggerer_command.py for the triggerer.
        stack_files = [frame.filename for frame in traceback.extract_stack()]
        is_scheduler_stack = any('scheduler_command' in f for f in stack_files)
        is_scheduler = "Scheduler" in name or "Scheduler" in job_type or is_scheduler_stack
        log.info(
            "RMQWatcherListener.on_starting: component=%s (job_type=%s, is_scheduler=%s)",
            name, job_type, is_scheduler,
        )
        if is_scheduler:
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

                    # Exchange/routing_keys metadata is never persisted to the DB (see
                    # plan Technical Details → "Почему миграция БД не нужна") — it only
                    # lives in-memory in the AST scan cache, re-derived every cycle. Build
                    # a lookup keyed the same way as the unique constraint on
                    # RMQSubscription so it can be merged back onto DB rows below.
                    exchange_meta = {
                        (s["dag_id"], s["queue_name"], s.get("conn_id", "rmq_default")): {
                            "exchange": s["exchange"],
                            "routing_keys": s["routing_keys"],
                        }
                        for s in scanned
                        if "exchange" in s
                    }

                    with WatcherSession() as session:
                        active_subs = []
                        for sub in get_enabled_subscriptions(session):
                            entry = {
                                "id": sub.id,
                                "dag_id": sub.dag_id,
                                "queue_name": sub.queue_name,
                                "conn_id": sub.conn_id,
                                "filter_data": sub.filter_data or {},
                                "cooldown": sub.cooldown or 0,
                            }
                            meta = exchange_meta.get(
                                (sub.dag_id, sub.queue_name, sub.conn_id)
                            )
                            if meta is not None:
                                entry.update(meta)
                            active_subs.append(entry)

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

        dag_id is taken from the explicit dag_id= argument of @dag(...) when it is
        a string literal; otherwise falls back to the decorated function name.

        Multiple ``@rmq_trigger(exchange=...)`` decorators on the same DAG are
        not supported (see ``subscription_builder.has_exchange_conflict`` /
        Technical Details → "Стекинг exchange= на одном DAG" in the plan). The
        decorator raises ``ValueError`` for this case (it can abort the
        decorator call); the AST parser cannot abort a DAG import, so it logs a
        WARNING and skips the duplicate, keeping the first one parsed.

        Defense in depth: the whole function body is wrapped in a broad
        ``except Exception`` (in addition to the read/parse-specific except
        below). ``_parse_rmq_trigger_decorator`` already turns validation
        failures from ``build_subscriptions`` into a graceful per-decorator
        WARNING+skip via its own ``except ValueError`` — but if a future bug
        ever lets an unexpected exception type leak past that (e.g. a
        non-str/non-list literal reaching a helper that isn't guarded yet),
        it must still only cost this one DAG file's subscriptions, not crash
        the entire reconcile cycle for every other DAG. ``_scan_subscriptions``
        relies on this function never raising so it can still record the
        file's mtime — without that, a permanently malformed file would be
        re-parsed (and re-crash) on every single cycle forever.
        """
        try:
            with open(path, encoding="utf-8") as f:
                source = f.read()
            tree = ast.parse(source, filename=path)
        except (SyntaxError, OSError, UnicodeDecodeError) as exc:
            log.warning("Failed to read/parse DAG file %s: %s", path, exc)
            return []

        try:
            result: list[dict] = []
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                dag_id = _extract_dag_id_from_decorators(node.decorator_list) or node.name
                dag_subs: list[dict] = []
                for decorator in node.decorator_list:
                    for sub in _parse_rmq_trigger_decorator(decorator, dag_id):
                        if has_exchange_conflict(dag_subs, [sub]):
                            log.warning(
                                "rmq_trigger: dag_id=%s already has an exchange= subscription — "
                                "skipping duplicate (stacking multiple exchange= decorators on one "
                                "DAG is not supported)",
                                dag_id,
                            )
                            continue
                        sub["dag_id"] = dag_id
                        sub["group_key"] = dag_id if sub.get("cooldown", 0) > 0 else None
                        dag_subs.append(sub)
                result.extend(dag_subs)
            return result
        except Exception:
            log.exception(
                "Unexpected error extracting @rmq_trigger subscriptions from DAG file %s — "
                "skipping this file's subscriptions for this cycle",
                path,
            )
            return []

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
                    cooldown=s.get("cooldown", 0) or None,
                    group_key=s.get("group_key"),
                )

            session.commit()
