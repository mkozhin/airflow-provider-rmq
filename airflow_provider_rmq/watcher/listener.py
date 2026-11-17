from __future__ import annotations

import asyncio
import logging
import os
import site
import socket
import subprocess
import sys
import sysconfig
import threading
import time
import traceback
import uuid
from collections.abc import Collection
from concurrent.futures import Future
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from airflow.listeners import hookimpl
from sqlalchemy.orm import Session

import airflow_provider_rmq
from airflow_provider_rmq.utils.amqp import call_with_timeout
from airflow_provider_rmq.utils.backoff import next_backoff
from airflow_provider_rmq.utils.executor import BoundedExecutor
from airflow_provider_rmq.utils.metrics import incr
from airflow_provider_rmq.watcher import runner as _runner
from airflow_provider_rmq.watcher.consumer import RMQConsumerManager
from airflow_provider_rmq.watcher.dag_file_scan import (
    _READABLE_FILE_REMEDY,
    _read_and_parse,
    _read_file_triggers,
)
from airflow_provider_rmq.watcher.dag_folder_listing import (
    DagFolderListingError,
    list_dag_files,
)
from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    WatcherIdentity,
    WatcherSession,
    ensure_heartbeat_table,
    ensure_table_exists,
    get_active_dag_ids,
    get_enabled_subscriptions,
    is_schema_ready,
    prune_heartbeats,
    record_heartbeat_cycle,
    record_heartbeat_deadline,
    record_heartbeat_stop,
    upsert_subscription,
    utcnow,
)
from airflow_provider_rmq.watcher.tunables import (
    CYCLE_TIMEOUT_VAR,
    DEFAULT_RECONCILE_INTERVAL,
    RECONCILE_INTERVAL_VAR,
    VARIABLE_TIMEOUT,
    cycle_timeout,
    read_positive,
)

log = logging.getLogger(__name__)

#: Seconds allowed for one schema-migration attempt.
_MIGRATION_TIMEOUT = 30.0

#: Seconds allowed for one blocking step of the cycle — the DAG-file scan and the two
#: subscription queries. All three go to the cycle pool, and a call that never returns
#: holds its worker until the operating system gives up on the socket, so without a
#: bound of their own they would spend the whole cycle budget and cost the loop, every
#: consumer task and every connection with it. Three steps at this bound still fit
#: inside the smallest cycle budget with room for the reconcile that follows them.
_STEP_TIMEOUT = 60.0

#: Seconds allowed for one heartbeat write — the start record, a new deadline or the
#: mark of a finished pass, together with creating the heartbeat table while it is not
#: known to exist. Every deadline the process records counts this bound twice: once for
#: the rest of the write that records it, once for the write that has to follow.
_HEARTBEAT_TIMEOUT = 10.0

#: Seconds allowed for deleting other runs' heartbeat rows after a recorded pass.
_PRUNE_TIMEOUT = 10.0

#: Seconds :meth:`RMQWatcherListener.mark_stopped` waits for the stop mark to be written.
#: The runner calls it on its way out, and the process does not wait on a database that
#: does not answer for longer than this.
_STOP_MARK_TIMEOUT = 2.0

#: Name of the thread that writes the stop mark.
_STOP_MARK_THREAD_NAME = "rmq-watcher-stop-mark"

#: Cycles to skip after a failed migration attempt: one, then doubling up to the
#: cap (an hour of cycles at the default interval).
_MIGRATION_BACKOFF_START = 1
_MIGRATION_BACKOFF_MAX = 60

#: Trusted synchronisations in a row a dag_file subscription's key may be missing from
#: the scan before its row is deleted. Every scheduler increments on its own, so an
#: installation running more than two of them has to raise this proportionally.
_ABSENT_GRACE_CYCLES = 3

#: Counted misses an enabled dag_file row of an inactive DAG carries before the early
#: deletion applies to it. Two, because a single count is not proof of a miss under
#: concurrent schedulers — the reasoning is where the branch reads it.
_INACTIVE_DAG_MISSES = 2

#: Listings in a row the cycle refuses to read as a statement about the code. The
#: first ``_UNTRUSTED_SCAN_LIMIT`` of them hold the previous parse, the next one gives
#: in: a folder that emptied for real, and a filesystem that is not coming back, both
#: have to become the truth eventually, or the watcher would never sync again. One
#: contiguous run of such listings shares this budget, whatever mix of shapes it takes.
_UNTRUSTED_SCAN_LIMIT = 3

#: Share of the file count the folder is known to have held that has to read for the
#: listing to be a statement about the code. Which failure a file answers with takes no
#: part in the decision: an aborted checkout leaves files truncated (a parse failure), a
#: storage backend losing access leaves them unstattable (an I/O failure), and both say
#: the same thing — what is on disk right now is not the code. A broken file or two among
#: many is about the code and stays trusted; a listing that lost most of what read before
#: is not. The count it is measured against is a high-water mark that only comes down
#: where a shrink has been believed, so a folder unmounting a slice per cycle is compared
#: with what it held rather than with its own previous step.
#:
#: A process that has believed no listing yet holds no such count, and there is nothing
#: else on the filesystem to derive one from: it can refuse a listing that produced
#: nothing readable, and beyond that it has to take what it is given. A scheduler that
#: starts into a half-mounted DAG volume therefore believes it, and the rows whose files
#: are missing count misses from that cycle on — :data:`_ABSENT_GRACE_CYCLES` of them is
#: what stands between such a start and the deletions, where a process with a mark behind
#: it also has this distrust in front of that.
_TRUSTED_SCAN_FRACTION = 0.5

#: Deletions of dag_file rows this process keeps the key of, so that a key the scan
#: brings back is reported instead of silently becoming an enabled row again. The report
#: is about a key returning within a few cycles of its deletion — a redeploy that fixes
#: the DAG file — so the oldest entry makes room for a newer one once the map is this
#: full, rather than the map growing for the life of the process.
_DELETED_ROWS_REMEMBERED = 256

#: Cycles the previous parse of an unreadable DAG file is held for. One, not three:
#: the case the hold exists for is a file caught mid-write, and that resolves within a
#: single cycle. Anything lasting longer is a genuinely broken file, whose DAG Airflow
#: deactivates in 50–110 seconds — after which every delivery on its subscriptions is
#: acked and dropped. Patience here is paid for in messages, so there is exactly as
#: much of it as a file being written needs.
_UNREADABLE_FILE_LIMIT = 1

#: Directories the reason of a distrusted listing names before it only counts the rest:
#: a permission change applied to a whole tree can make many directories unreadable at
#: once, and one log line has room for the first few of them.
_UNREADABLE_DIRS_NAMED = 5

#: Seconds allowed for the manager to stop before the loop is torn down anyway.
_STOP_TIMEOUT = 30.0

#: Module the scheduler starts as the watcher process.
_RUNNER_MODULE = _runner.__name__

#: Program the watcher process runs with ``python -c``, the runner's module name as its
#: argument.
#:
#: Its first step removes the entry ``python -c`` puts in front of :data:`sys.path`,
#: the working directory the scheduler was started in — usually ``AIRFLOW_HOME``. The
#: scheduler's own :data:`sys.path` has no such entry, and only ``sys``, which is built
#: into the interpreter, is imported before the removal, so no module lying in that
#: directory — ``runpy``, ``logging``, a package named like this one — is imported in
#: the watcher process in its place. Under ``-P`` or ``PYTHONSAFEPATH`` (Python 3.11 and
#: later) the entry is not added and nothing is removed. ``python -m`` adds the same
#: entry and searches it for ``runpy`` and for the runner's package before any code of
#: the watcher runs, which is why the process is not started that way.
#:
#: It then appends the directory named by the runner's ``PACKAGE_DIR_ENV``, when that
#: is set, to the end of :data:`sys.path` and runs the module named by its argument as
#: ``__main__``, the way ``python -m`` would.
_BOOTSTRAP = (
    "import sys; "
    "getattr(sys.flags, 'safe_path', False) or sys.path.pop(0); "
    "import os, runpy; "
    f"sys.path.extend(filter(None, [os.environ.get({_runner.PACKAGE_DIR_ENV!r})])); "
    "runpy.run_module(sys.argv[1], run_name='__main__', alter_sys=True)"
)

#: Seconds ``before_stopping`` waits for the watcher process to exit after SIGTERM.
#: The scheduler's own shutdown must not stall behind a watcher that takes its time,
#: so a process still running after this is killed; the deliveries it had not
#: acknowledged go back to their queues on the broker.
_CHILD_STOP_TIMEOUT = 5.0

#: Seconds ``before_stopping`` waits for the watcher process to be reaped after
#: SIGKILL. A process in uninterruptible kernel I/O — a hung NFS mount with the DAG
#: files on it — dies only once that I/O returns, and the scheduler does not wait for
#: it past this.
_CHILD_REAP_TIMEOUT = 2.0

#: Workers of the cycle pool: the reconcile loop's own blocking calls (schema
#: migration, Variable reads, DAG-file scan, subscription queries, heartbeat writes).
_CYCLE_POOL_WORKERS = 4

#: Workers of the consumer pool, handed to the manager. Kept apart from the cycle
#: pool so that a database that stopped answering degrades consumption without
#: also starving the cycle that writes statuses and rebuilds connections.
_CONSUMER_POOL_WORKERS = 32


def _read_settings() -> tuple[int | None, float | None]:
    """Read the watcher tunables from Airflow Variables. Blocking: hits the database.

    Returns ``(reconcile_interval, cycle_timeout)``; either is ``None`` when the
    Variable is unset, holds something that is not a positive number, or sits in a
    database that answers the read with a refusal — Airflow reports all three the same
    way. The caller reads ``None`` as "keep the built-in default".
    """
    return (
        read_positive(RECONCILE_INTERVAL_VAR, int),
        read_positive(CYCLE_TIMEOUT_VAR, float),
    )


def _read_active_subs(exchange_meta: dict) -> list[dict]:
    """Read the enabled subscriptions, merging the in-memory exchange metadata onto them.

    A blocking SQLAlchemy query, so it runs in the cycle pool rather than on the loop
    thread; the exchange/routing_keys pairs are keyed the same way as the unique
    constraint on ``RMQSubscription`` and are looked up here, where the rows are.
    """
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
                "ack_unmatched": bool(sub.ack_unmatched),
            }
            meta = exchange_meta.get((sub.dag_id, sub.queue_name, sub.conn_id))
            if meta is not None:
                entry.update(meta)
            active_subs.append(entry)
    return active_subs


def _write_heartbeat(write: Any, identity: WatcherIdentity, *args: Any) -> None:
    """Run the heartbeat write ``write`` for ``identity`` in a session of its own.

    Blocking. The heartbeat table is created first while this process does not know it
    to exist, so the first write of a run — and the next one after a creation that
    failed — brings the table up itself.
    """
    ensure_heartbeat_table()
    with WatcherSession() as session:
        write(session, identity, *args)


def _run_hostname() -> str:
    """The host name the heartbeat row shows: Airflow's ``get_hostname()``.

    Airflow resolves it through ``[core] hostname_callable``; a callable that fails
    leaves the socket's host name, with a WARNING — the name only labels the row.
    """
    from airflow.utils.net import get_hostname

    try:
        return get_hostname()
    except Exception:
        hostname = socket.gethostname()
        log.warning(
            "RMQ Watcher: Airflow's get_hostname() failed — the heartbeat row shows the "
            "host as %s", hostname, exc_info=True,
        )
        return hostname


def _prune_other_heartbeats(keep_id: str, now: datetime) -> int:
    """Delete other runs' heartbeat rows that have stopped counting at ``now``. Blocking."""
    with WatcherSession() as session:
        return prune_heartbeats(session, keep_id, now)


def _read_active_dag_ids() -> set[str] | None:
    """Return the dag_ids Airflow counts as active, or ``None`` when they cannot be read.

    ``None`` is the caller's cue to treat every DAG as active: an unreachable
    database must not be the thing that speeds a deletion up.

    The read gets a session of its own, opened and closed around this call alone.
    PostgreSQL answers every statement that follows a failed one in the same
    transaction with ``InFailedSqlTransaction``, so sharing the session that writes
    the counters would turn "treat every DAG as active" into "synchronise nothing
    at all" for as long as the ``dag`` table stays unreadable.
    """
    try:
        with WatcherSession() as session:
            return get_active_dag_ids(session)
    except Exception as exc:  # noqa: BLE001 - an unreachable database must not speed a deletion up
        log.warning(
            "RMQ Watcher: cannot read the active DAG ids (%s) — treating every DAG "
            "as active for this synchronisation",
            exc,
        )
        return None


def _absent_cycles_still(observed: int | None) -> Any:
    """Predicate matching a row whose miss counter still reads exactly ``observed``.

    The counter is read once per row and written back under this predicate, so a
    scheduler whose read went stale — the key came back and another scheduler cleared
    the counter, or moved it on — writes nothing instead of overwriting what that one
    committed. ``NULL`` and ``0`` say the same thing about a row and different things
    to SQL, where nothing at all equals ``NULL``, so the value the row was read as
    carries into the predicate unnormalised.
    """
    if observed is None:
        return RMQSubscription.absent_cycles.is_(None)
    return RMQSubscription.absent_cycles == observed


def _sub_key(sub: dict) -> tuple[str, str, str]:
    """The key a scanned subscription is identified by: ``(dag_id, queue_name, conn_id)``.

    The same triple the unique constraint on ``RMQSubscription`` is built from, so a
    scanned dict and a database row are comparable by it. ``conn_id`` carries the
    default the decorator leaves out.
    """
    return (sub["dag_id"], sub["queue_name"], sub.get("conn_id", "rmq_default"))


def _untrusted_listing_reason(
    dags_folder: str,
    current_files: set[str],
    readable: int,
    baseline: int | None,
    failures: list[tuple[str, Exception | str, float | None]],
    unreadable_dirs: Collection[str] = (),
) -> str:
    """Phrase what about this listing of ``dags_folder`` keeps it from being believed.

    The four shapes a distrusted listing takes — a folder that named nothing, one
    whose every file refused, one that read whole but came back smaller than the
    folder is known to have been, and one that both shrank and lost files — followed
    by the directories the walk could not read when there are any, and by the first
    failure behind it when there is one.
    """
    if not current_files:
        what = f"the DAG folder {dags_folder!r} listed no files"
    elif not readable:
        what = (
            f"none of the {len(current_files)} files the DAG folder "
            f"{dags_folder!r} listed could be read"
        )
    elif readable == len(current_files):
        what = (
            f"the DAG folder {dags_folder!r} listed only {readable} files, "
            f"against {baseline} it is known to have held"
        )
    else:
        what = (
            f"only {readable} of the {len(current_files)} files the DAG folder "
            f"{dags_folder!r} listed could be read, against "
            f"{baseline} the folder is known to have held"
        )
    if unreadable_dirs:
        named = sorted(unreadable_dirs)
        what += ", directories that cannot be read: " + ", ".join(
            repr(path) for path in named[:_UNREADABLE_DIRS_NAMED]
        )
        if len(named) > _UNREADABLE_DIRS_NAMED:
            what += f" and {len(named) - _UNREADABLE_DIRS_NAMED} more"
    if failures:
        what += f", first reason: {failures[0][1]!r}"
    return what


def _site_directories() -> set[str]:
    """Directories the interpreter puts on :data:`sys.path` by itself for installed
    packages: the site-packages of the installation and of the user."""
    paths = sysconfig.get_paths()
    found = {paths["purelib"], paths["platlib"]}
    try:
        found.update(site.getsitepackages())
    except AttributeError:
        pass
    user_site = site.getusersitepackages()
    if user_site:
        found.add(user_site)
    return {os.path.realpath(path) for path in found}


def _package_directory_off_site_path() -> str | None:
    """Directory the package is imported from, or ``None`` when it is a site-packages one.

    A package installed with pip lies in a site-packages directory, and the watcher
    process finds it there, as the scheduler does. A package imported from anywhere
    else — the plugins folder, which Airflow appends to :data:`sys.path` only once it
    has initialised — is not importable before Airflow is, so the directory is handed
    to the watcher process for :data:`_BOOTSTRAP` to append.
    """
    package_root = str(Path(airflow_provider_rmq.__file__).parents[1])
    if os.path.realpath(package_root) in _site_directories():
        return None
    return package_root


def _child_command() -> list[str]:
    """Command line of the watcher process: ``python -c`` :data:`_BOOTSTRAP` with the
    runner's module name as its argument.

    The directory of a package found by :func:`_package_directory_off_site_path`
    travels in the runner's ``PACKAGE_DIR_ENV`` and goes to the end of
    :data:`sys.path`, where the scheduler has it, so a module in the plugins folder
    named like one of the standard library or an installed package shadows it in
    neither process. ``PYTHONPATH`` would put it ahead of both.
    """
    return [sys.executable, "-c", _BOOTSTRAP, _RUNNER_MODULE]


class _StepInFlight(Exception):
    """The previous attempt at a cycle step is still occupying a worker of the pool.

    Raised instead of handing the pool a second copy of a call that has not come back:
    the cycle pool has four workers, and one stuck attempt per cycle saturates it in a
    handful of cycles — after which even the pure-filesystem scan cannot start and the
    liveness check stops running altogether.
    """


class _StopRequested(Exception):
    """A stop has been requested of the loop, so the cycle ends where it stands.

    Raised by :meth:`RMQWatcherListener._cycle_step` in place of starting a step, or of
    waiting any further for the one in flight, and by the cycle in place of starting
    the reconcile. The pass ends there and the loop goes on to stop the consumer
    manager, whose bounded stop is what marks the subscription rows disconnected.
    """


class _UntrustedScan(Exception):
    """The scan result is not a statement about the code, so the cycle must not sync it.

    Raised by the scan step when the DAG folder could not be read, was empty or
    unlistable, or when a listing short of ``_TRUSTED_SCAN_FRACTION`` of what the folder
    is known to have held is still young enough to be a hiccup of the environment rather
    than the truth. Skipping ``_sync_to_db`` keeps such a listing from counting a miss
    against any ``dag_file`` row it failed to name, with ``_ABSENT_GRACE_CYCLES`` behind
    that; reading the rows, the liveness check and the status writes carry on regardless.
    """


class RMQWatcherListener:
    """Airflow Listener that runs the RabbitMQ consumer loop in a child process of the scheduler.

    The class plays two parts, one in each process.

    In the scheduler process it only starts and stops the watcher process, and holds
    no thread, event loop or pool there. Airflow forks the scheduler process — the
    Celery send pool, the DAG processor manager — and a fork is only safe from a
    process with a single thread.

    - ``on_starting`` fires in the scheduler's main thread, inside ``Job.__init__``,
      and starts the runner, ``airflow_provider_rmq.watcher.runner``, through
      :data:`_BOOTSTRAP` (see :func:`_child_command`) with :class:`subprocess.Popen`,
      which forks and execs in C with no Python code in between. The scheduler's PID travels in the
      environment, together with a marker that keeps a listener loaded in the
      watcher process from starting another one.
    - ``before_stopping`` sends the watcher process SIGTERM and waits a bounded time
      for it to exit, then kills it, and returns even if the killed process cannot be
      reaped yet.

    In the watcher process the runner calls :meth:`run` from a thread of its own:

    - The loop reconciles subscriptions from DAG files (mtime-based scan) and the DB
      every ``reconcile_interval`` seconds, then delegates to ``RMQConsumerManager``.
    - :meth:`request_stop` sets the stop event and wakes the loop out of its wait, so
      shutdown does not wait out a reconcile interval.
    - The run keeps one row of ``rmq_watcher_heartbeat``: a deadline before the first
      pass, the mark of every pass that finishes within its budget with the next
      deadline, and — through :meth:`mark_stopped` — the mark of an orderly stop.
    """

    def __init__(self) -> None:
        # Watcher process started by ``on_starting`` in the scheduler; ``None`` in every
        # other process and before the start.
        self._child: subprocess.Popen | None = None
        # PID of the process that started the watcher process. A process forked from
        # the scheduler afterwards — a LocalExecutor task supervisor — inherits this
        # listener together with ``_child``, and the two tell it the watcher is the
        # scheduler's, not its own.
        self._owner_pid: int | None = None
        # Authoritative stop signal of the cycle. It exists from construction on, so a
        # stop requested before the loop has started is already recorded when it does.
        self._stop_event = threading.Event()
        # PID of the scheduler process the loop serves, set by :meth:`run` before the
        # loop starts. ``_main`` compares it with the current parent before every pass
        # and stops once they differ.
        self._parent_pid: int
        self._manager: RMQConsumerManager | None = None
        # mtime-based incremental scan state, written by whichever cycle-pool worker runs
        # the "scan" step. One step of a name is in flight at a time (:class:`_StepInFlight`
        # in _cycle_step), which is what keeps two workers out of these dicts at once.
        self._last_mtimes: dict[str, float] = {}   # filepath → mtime
        self._cached_subs: dict[str, list[dict]] = {}  # filepath → list[sub dict]
        self._read_failures: dict[str, int] = {}  # filepath → failed reads in a row
        # Directories of the DAG folder the last walk could not read. The next walk
        # is handed them to report which of them read again, and the log tells a
        # directory that has just become unreadable from one that stays so.
        self._unreadable_dirs: set[str] = set()
        # Listings in a row the cycle would not read as a statement about the code,
        # counted so that the distrust expires at ``_UNTRUSTED_SCAN_LIMIT`` instead of
        # lasting forever: a folder of files nobody can read is a filesystem talking,
        # but the distrust of it still has to end somewhere. Every way out of the
        # distrust — a listing believed, and a listing given in to — puts it back to
        # zero, so each episode gets the whole budget.
        self._untrusted_scan_streak: int = 0
        # Files the folder is known to have held, which :data:`_TRUSTED_SCAN_FRACTION`
        # of the next listing is measured against. It rises with a listing that read
        # more and comes down only where a shrink has been believed, so a folder losing
        # a slice of itself per cycle is measured against what it held rather than
        # against its own previous step. ``None`` until the first listing is believed:
        # a process that has seen none has nothing to compare one against, which is a
        # different statement from a folder known to be empty. Like the caches above it
        # holds what the folder showed, not what a cycle managed to use: a step the
        # cycle stopped waiting for goes on to finish in its pool worker, and the
        # listing it observed is an observation of the folder either way.
        self._trusted_file_count: int | None = None
        # Whether the listing the cycle last took as the truth named files and read
        # none of them. Such a listing is the folder collapsing, and it is refused for
        # the whole budget before it is given in to; once it has been given in to, the
        # same listing every cycle after it is the steady state that was already paid
        # for, and refusing it again would leave the sync step running one cycle in
        # four for the life of the process. It also tells the two zeroes apart: a
        # folder believed to hold nothing readable and a folder believed to be empty
        # both leave the mark at zero, and only the first of them has a collapse behind
        # it.
        self._trusted_listing_read_nothing: bool = False
        # Result of the last scan the cycle trusted. Stands in for the scan of a cycle
        # that raised :class:`_UntrustedScan`, which needs it for the exchange metadata:
        # that metadata lives only in memory and reconcile has no other source for it.
        self._last_good_scan: list[dict] = []
        # Ordinal of the scan the cycle last accepted as trusted, raised by one on
        # every acceptance. The sync step is handed the ordinal of the listing it was
        # given and checks it against this one before committing: a step that outlived
        # its timeout goes on running in its pool worker while later cycles scan, and
        # what such a worker holds is a listing about a state of the folder the process
        # has since read again. Nothing it computed from that listing may reach the
        # database, deletions least of all.
        self._scan_generation = 0
        # dag_file keys this process deleted and the ``enabled`` flag each of them
        # carried, held until the scan brings the key back. A key that comes back
        # arrives as a brand new row with the default flag on it, and that is what the
        # log line reporting it says. Written by the sync step alone, like the dicts
        # above are written by the scan step alone, and bounded by
        # :data:`_DELETED_ROWS_REMEMBERED`: a deleted key is far more often gone for
        # good than back, and an installation generating dag_ids on the fly would
        # otherwise leave one entry per deletion here for the life of the process.
        self._deleted_dag_file_rows: dict[tuple[str, str, str], bool] = {}
        # Both pools are built by :meth:`_ensure_pools` on first use, in the process
        # that runs the loop, and outlive every event loop created after them. The
        # listener is instantiated in every Airflow component that loads the plugin,
        # and a component that never runs the loop never gets a pool.
        self._cycle_pool: BoundedExecutor | None = None
        self._consumer_pool: BoundedExecutor | None = None
        # Tunables, cached between refreshes so that a database outage cannot stall
        # the loop at a point the cycle watchdog does not cover.
        self._reconcile_interval = DEFAULT_RECONCILE_INTERVAL
        self._cycle_timeout_override: float | None = None
        # Blocking cycle steps that are still in a worker, keyed by step name, so the
        # next cycle asks whether the previous attempt returned instead of adding one.
        self._step_attempts: dict[str, Future] = {}
        # Schema migration retry state
        self._migration_backoff = 0          # cycles to skip after the last failure
        self._migration_skip_cycles = 0
        #: Step the current cycle is on, reported when the cycle runs out of budget.
        self._phase = "idle"
        # Loop and wake-up event of the currently running cycle, published as one
        # tuple by every ``_main`` so that ``request_stop`` can never pair the live
        # loop with the previous loop's event.
        self._waker: tuple[asyncio.AbstractEventLoop, asyncio.Event] | None = None
        # The run of the watcher process whose heartbeat row the loop keeps, created by
        # :meth:`run`. ``None`` in every other process and before the run: nothing is
        # written without it.
        self._identity: WatcherIdentity | None = None
        # Whether the start record of this run has been attempted. A restart of
        # ``_main`` inside the process keeps the run, so it does not record a start.
        self._heartbeat_started = False
        # Set by ``_run_loop`` when it restarts ``_main`` after a crash: the deadline the
        # row carries covered a pass of the crashed loop, so the restarted loop records a
        # new one before its first pass.
        self._heartbeat_restarted = False
        # Cycle budget the last recorded deadline was computed with; a pass that runs
        # under a larger one records a new deadline before it starts.
        self._heartbeat_budget: float | None = None
        # The deadline the last heartbeat write carried, which the stop mark repeats.
        self._heartbeat_due_at: datetime | None = None
        # The stop mark is written once per run, by the first :meth:`mark_stopped`.
        self._stop_mark_lock = threading.Lock()
        self._stop_marked = False

    # ------------------------------------------------------------------
    # Listener API
    # ------------------------------------------------------------------

    @hookimpl
    def on_starting(self, component: Any) -> None:
        if _runner.CHILD_ENV_MARKER in os.environ:
            # The watcher process itself, where a plugin load registered the listener
            # again. The loop already runs here; another process would be a second
            # watcher on the same subscriptions.
            log.info(
                "RMQWatcherListener.on_starting in the watcher process — "
                "not starting another one"
            )
            return
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
            "RMQWatcherListener.on_starting: component=%s (job_type=%s, is_scheduler=%s)%s",
            name, job_type, is_scheduler,
            "" if is_scheduler
            else " — watcher not started: only the scheduler starts the watcher process",
        )
        if is_scheduler:
            self._spawn_child()

    @hookimpl
    def before_stopping(self, component: Any) -> None:
        self._stop_child()

    def _spawn_child(self) -> None:
        """Start the watcher process, unless the one started earlier is still running.

        The process gets the scheduler's environment plus the child marker, the
        scheduler's PID and, for a package imported from outside the interpreter's
        site-packages (the plugins folder), that directory — see
        :func:`_child_command`. stdout and stderr are inherited, so the watcher's lines
        land in the scheduler container's log, and every other descriptor of the
        scheduler is closed in the child.

        A process forked from the scheduler after the start inherits this listener
        and starts nothing: the watcher process is the scheduler's. A failure to start
        is logged and the scheduler goes on without the watcher.
        """
        if self._is_a_fork_of_the_owner():
            self._log_left_to_the_owner()
            return
        child = self._child
        if child is not None and child.poll() is None:
            log.warning(
                "RMQ Watcher process %d is already running — ignoring duplicate on_starting",
                child.pid,
            )
            return
        if (
            sys.platform.startswith("linux")
            and threading.current_thread() is not threading.main_thread()
        ):
            # Linux ties the parent-death signal to the thread that started the
            # process, so the watcher is told when this thread exits rather than when
            # the scheduler does; the parent check of every cycle still ends it once
            # the scheduler is gone.
            log.warning(
                "RMQ Watcher process is started from thread %r rather than the main "
                "thread — it is signalled when that thread exits",
                threading.current_thread().name,
            )
        pid = os.getpid()
        package_dir = _package_directory_off_site_path()
        env = dict(os.environ)
        env[_runner.CHILD_ENV_MARKER] = "1"
        env[_runner.PARENT_PID_ENV] = str(pid)
        if package_dir is None:
            env.pop(_runner.PACKAGE_DIR_ENV, None)
        else:
            env[_runner.PACKAGE_DIR_ENV] = package_dir
        self._owner_pid = pid
        try:
            self._child = subprocess.Popen(
                _child_command(),
                env=env,
                stdin=subprocess.DEVNULL,
            )
        except Exception:
            log.exception(
                "RMQ Watcher process could not be started — the scheduler runs without "
                "the watcher"
            )
            return
        log.info(
            "RMQ Watcher process started with PID %d (scheduler PID %d)",
            self._child.pid, pid,
        )

    def _stop_child(self) -> None:
        """Stop the watcher process within a bounded time and log how it ended.

        SIGTERM first, and :data:`_CHILD_STOP_TIMEOUT` for the loop to stop the
        consumer manager and leave; then SIGKILL, and :data:`_CHILD_REAP_TIMEOUT` for
        the kernel to finish the process. A process that is still not reaped after
        that is logged with its PID and left behind: the scheduler's shutdown does not
        depend on it. A process that has exited already — the container's init sends
        SIGTERM to the whole process group, the watcher included — is only reaped.
        """
        if self._is_a_fork_of_the_owner():
            self._log_left_to_the_owner()
            return
        child = self._child
        if child is None:
            return
        self._child = None
        code = child.poll()
        if code is not None:
            log.info("RMQ Watcher process %d had already exited with code %d", child.pid, code)
            return
        child.terminate()
        try:
            code = child.wait(timeout=_CHILD_STOP_TIMEOUT)
        except subprocess.TimeoutExpired:
            log.warning(
                "RMQ Watcher process %d still running %.0fs after SIGTERM — killing it",
                child.pid, _CHILD_STOP_TIMEOUT,
            )
            child.kill()
            try:
                code = child.wait(timeout=_CHILD_REAP_TIMEOUT)
            except subprocess.TimeoutExpired:
                log.error(
                    "RMQ Watcher process %d not reaped %.0fs after SIGKILL — leaving it "
                    "behind, the scheduler goes on stopping",
                    child.pid, _CHILD_REAP_TIMEOUT,
                )
                return
        log.info("RMQ Watcher process %d exited with code %d", child.pid, code)

    def _is_a_fork_of_the_owner(self) -> bool:
        """Whether this process is a fork of the one that started the watcher process.

        Such a process — the task supervisor LocalExecutor forks from the scheduler —
        carries the scheduler's frames on its stack, so ``on_starting`` takes its job
        for the scheduler, and it inherits ``_child``, whose ``poll()`` reads as an
        exit because the process is not its child. It neither starts nor stops a
        watcher process.
        """
        owner = self._owner_pid
        return owner is not None and owner != os.getpid()

    def _log_left_to_the_owner(self) -> None:
        """Log that this fork of the scheduler leaves the watcher process to it."""
        log.info(
            "RMQWatcherListener in process %d, a fork of the scheduler process %d — the "
            "watcher process belongs to the scheduler, nothing to start or stop here",
            os.getpid(), self._owner_pid,
        )

    def request_stop(self) -> None:
        """Ask the loop to end its cycle where it stands and stop the consumer manager.

        Sets the stop event and wakes the loop. A loop waiting between cycles returns
        from the wait at once; a cycle waiting on a blocking step stops waiting for it
        and starts no further step, the reconcile included — the worker running that
        step cannot be interrupted and is left to the process exit. A reconcile that
        has already started runs to its end, under the cycle budget, before the loop
        gets to the manager.

        Safe to call any number of times, from any thread, and before the loop has
        started — :meth:`run` then returns without running a cycle. It is ordinary code
        with no signal-handler restrictions, which is how the runner calls it once the
        signal it waited for has arrived.
        """
        self._stop_event.set()
        self._wake_loop()

    def mark_stopped(self) -> None:
        """Record the orderly stop of this run on its heartbeat row.

        The first call writes ``stopped_at`` from a thread of its own, with a session of
        its own, and waits :data:`_STOP_MARK_TIMEOUT` seconds at most: the loop may be
        in the middle of a reconcile, and the process leaves whether the database
        answered or not. A failed or late write is logged and the stop goes on. Later
        calls write nothing, and so does a call made before :meth:`run` has created the
        run.

        The write carries the run's whole identity, so it lands on a row whose start
        record has not been committed yet, and the heartbeat writes of the loop leave
        ``stopped_at`` as it is. The runner calls this after :meth:`request_stop`, for a
        stop signal that comes while the scheduler is still its parent: an orderly stop
        of the scheduler. A watcher that ends because the scheduler died leaves the row
        unmarked, and its deadline runs out.
        """
        identity = self._identity
        if identity is None:
            return
        with self._stop_mark_lock:
            if self._stop_marked:
                return
            self._stop_marked = True
        stopped_at = utcnow()
        due_at = self._heartbeat_due_at or stopped_at
        try:
            thread = threading.Thread(
                target=self._write_stop_mark,
                args=(identity, due_at, stopped_at),
                name=_STOP_MARK_THREAD_NAME,
                daemon=True,
            )
            thread.start()
        except RuntimeError:
            log.warning(
                "RMQ Watcher: cannot start the thread that marks the stop of run %s",
                identity.id, exc_info=True,
            )
            return
        thread.join(timeout=_STOP_MARK_TIMEOUT)
        if thread.is_alive():
            log.warning(
                "RMQ Watcher: the stop mark of run %s was not written within %.0fs — "
                "stopping without it",
                identity.id, _STOP_MARK_TIMEOUT,
            )

    @staticmethod
    def _write_stop_mark(
        identity: WatcherIdentity, due_at: datetime, stopped_at: datetime
    ) -> None:
        """Write the stop mark of ``identity``, logging a failure. Runs in its own thread."""
        try:
            _write_heartbeat(record_heartbeat_stop, identity, due_at, stopped_at)
        except Exception:
            log.warning(
                "RMQ Watcher: cannot mark the stop of run %s on its heartbeat row",
                identity.id, exc_info=True,
            )

    def _wake_loop(self) -> None:
        """Nudge the running cycle so it sees the stop event without waiting out the
        reconcile interval or the blocking step in flight.

        The threading event is the authoritative signal; this only shortens the wait.
        An :class:`asyncio.Event` may be touched from its own loop alone, and that loop
        may already be closing — hence the guard and the swallowed ``RuntimeError``.
        """
        waker = self._waker
        if waker is None:
            return
        loop, wakeup = waker
        if loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(wakeup.set)
        except RuntimeError:
            pass

    # ------------------------------------------------------------------
    # Loop bootstrap in the watcher process
    # ------------------------------------------------------------------

    def run(self, parent_pid: int) -> None:
        """Run the watcher loop in the calling thread until it is asked to stop.

        :param parent_pid: PID of the scheduler process the loop serves. The loop
            stops on its own once the current parent differs from it.

        Returns once :meth:`request_stop` has been called, or the parent has changed,
        and the loop torn down.

        The run's :class:`WatcherIdentity` is created here, before the loop: every
        restart of the loop inside this process keeps it, and with it the heartbeat row.
        """
        self._parent_pid = parent_pid
        self._identity = WatcherIdentity(
            id=str(uuid.uuid4()),
            hostname=_run_hostname(),
            pid=os.getpid(),
            started_at=utcnow(),
        )
        log.info(
            "RMQ Watcher loop started with the default reconcile interval of %ss and "
            "a cycle budget of %.0fs; the first cycle reads %s and %s and logs any "
            "override it finds",
            self._reconcile_interval, self._cycle_timeout(),
            RECONCILE_INTERVAL_VAR, CYCLE_TIMEOUT_VAR,
        )
        log.info(
            "RMQ Watcher run %s (host %s, PID %d) keeps its heartbeat in "
            "rmq_watcher_heartbeat",
            self._identity.id, self._identity.hostname, self._identity.pid,
        )
        self._run_loop()

    def _ensure_pools(self) -> tuple[BoundedExecutor, BoundedExecutor]:
        """Return the cycle pool and the consumer pool, building whichever is missing.

        Called from the loop thread only — the manager's creation in ``_main`` and
        every :meth:`_cycle_step` — so the first check and the assignment after it
        need no lock. A pool already in place is returned as it is.
        """
        if self._cycle_pool is None:
            self._cycle_pool = BoundedExecutor("rmq-watcher-cycle", _CYCLE_POOL_WORKERS)
        if self._consumer_pool is None:
            self._consumer_pool = BoundedExecutor(
                "rmq-watcher-consumer", _CONSUMER_POOL_WORKERS
            )
        return self._cycle_pool, self._consumer_pool

    def _parent_is_gone(self) -> bool:
        """Whether the scheduler process the loop serves has exited.

        A child whose parent exits is reparented, so its parent PID changes; the
        comparison needs nothing from the parent itself and holds even when the kernel
        signal that normally reports the parent's death was never armed. A parent that
        has changed arms the watcher process's hard deadline before the line saying so
        is logged, so a logging handler that blocks cannot keep the process alive.
        """
        current = os.getppid()
        if current == self._parent_pid:
            return False
        _runner._arm_hard_exit()
        log.error(
            "RMQ Watcher: the scheduler process %d has exited (parent is now %d) — "
            "stopping the watcher", self._parent_pid, current,
        )
        return True

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._main())
            except Exception:
                log.exception("RMQ Watcher loop crashed — restarting in 30s")
                self._heartbeat_restarted = True
            finally:
                # Both thread pools belong to the listener, not to the loop, so the
                # next loop reuses them. loop.shutdown_default_executor() is
                # deliberately not called: on Python 3.10/3.11 it ends in a
                # thread.join() with no timeout that wait_for cannot interrupt, so a
                # single call stuck on the database would freeze this thread for good.
                # The pools' own workers are never joined either: the watcher process
                # leaves through os._exit after a bounded wait for this thread, so a
                # stuck call holds neither the loop nor the process's exit — see
                # BoundedExecutor and the runner.
                loop.close()
            if not self._stop_event.is_set():
                self._stop_event.wait(timeout=30)
        log.info("RMQ Watcher loop stopped")

    async def _main(self) -> None:
        wakeup = asyncio.Event()
        self._waker = (asyncio.get_running_loop(), wakeup)
        cycle_pool, consumer_pool = self._ensure_pools()
        self._manager = RMQConsumerManager(
            executor=consumer_pool, cycle_executor=cycle_pool
        )
        await self._manager.start()
        try:
            while not self._stop_event.is_set():
                if self._parent_is_gone():
                    self._stop_event.set()
                    break
                await self._refresh_settings()
                budget = self._cycle_timeout()
                await self._record_deadline(budget)
                started = time.monotonic()
                try:
                    await call_with_timeout(self._run_cycle(), budget)
                except asyncio.TimeoutError:
                    log.error(
                        "RMQ Watcher cycle exceeded its %.0fs budget in phase %r after "
                        "%.0fs — recreating the event loop",
                        budget, self._phase, time.monotonic() - started,
                    )
                    incr("rmq_watcher.cycle_timed_out")
                    raise
                # Liveness, not health: ``_run_cycle`` logs and swallows the failure of
                # any single step, so every pass that returned counts here, whether or
                # not all of its steps succeeded. Only a pass that ran out of budget
                # does not.
                incr("rmq_watcher.cycle_completed")
                await self._record_cycle(budget)
                await self._wait_for_next_cycle()
        finally:
            await self._stop_manager()

    async def _record_deadline(self, budget: float) -> None:
        """Record the deadline of the pass about to start, when the row needs one.

        :param budget: Seconds the pass is given.

        The run's first pass records the start of the run. A later pass records a new
        deadline when ``_main`` has been restarted after a crash — the crashed loop's
        pass, the consumers' stop and the pause before the restart outlast the deadline
        the row carries — or when its budget is larger than the one the stored deadline
        was computed with. Either leaves ``last_cycle_at`` as it is. The deadline covers
        the rest of this write, the pass and the write that marks it:
        ``now + H + budget + H`` with ``H`` = :data:`_HEARTBEAT_TIMEOUT`.
        """
        identity = self._identity
        if identity is None:
            return
        if not self._heartbeat_started or self._heartbeat_restarted:
            self._heartbeat_started = True
            self._heartbeat_restarted = False
        elif self._heartbeat_budget is None or budget <= self._heartbeat_budget:
            return
        due_at = utcnow() + timedelta(seconds=_HEARTBEAT_TIMEOUT + budget + _HEARTBEAT_TIMEOUT)
        self._heartbeat_due_at = due_at
        if await self._heartbeat_step(
            "heartbeat", _HEARTBEAT_TIMEOUT,
            _write_heartbeat, record_heartbeat_deadline, identity, due_at,
        ):
            self._heartbeat_budget = budget

    async def _record_cycle(self, budget: float) -> None:
        """Mark the pass that has just finished within ``budget`` and prune old rows.

        Nothing is written once a stop has been requested. The next deadline covers
        everything until the next mark: the rest of this write, the prune, the wait
        between passes, the settings read, the next pass and its write —
        ``now + H + P + interval + V + budget + H`` with ``H`` =
        :data:`_HEARTBEAT_TIMEOUT`, ``P`` = :data:`_PRUNE_TIMEOUT` and ``V`` =
        :data:`VARIABLE_TIMEOUT`. Rows of other runs past their deadline and last marked
        more than :data:`~.models.HEARTBEAT_WINDOW` ago are deleted after the mark is
        committed, in a transaction of their own: a failed prune leaves the mark in place.
        """
        identity = self._identity
        # Checked here and not left to the step: once a stop has been requested the
        # deadline the stop mark repeats stays the one the last write carried.
        if identity is None or self._stop_event.is_set():
            return
        now = utcnow()
        due_at = now + timedelta(
            seconds=_HEARTBEAT_TIMEOUT + _PRUNE_TIMEOUT + self._reconcile_interval
            + VARIABLE_TIMEOUT + budget + _HEARTBEAT_TIMEOUT
        )
        self._heartbeat_due_at = due_at
        if not await self._heartbeat_step(
            "heartbeat", _HEARTBEAT_TIMEOUT,
            _write_heartbeat, record_heartbeat_cycle, identity, now, due_at,
        ):
            return
        self._heartbeat_budget = budget
        await self._heartbeat_step(
            "heartbeat_prune", _PRUNE_TIMEOUT,
            _prune_other_heartbeats, identity.id, now,
        )

    async def _heartbeat_step(
        self, name: str, timeout: float, fn: Any, *args: Any
    ) -> bool:
        """Run a heartbeat step through :meth:`_cycle_step`; return whether it succeeded.

        A heartbeat that cannot be written is not a reason to stop consuming: a failure,
        an attempt of the step still in flight and a saturated pool are logged and the
        loop goes on. A stop requested before or during the step ends it quietly.
        """
        try:
            await self._cycle_step(name, fn, *args, timeout=timeout)
        except _StopRequested:
            return False
        except _StepInFlight as exc:
            log.warning("RMQ Watcher: the %r step is skipped — %s", name, exc)
            return False
        except Exception:
            log.warning(
                "RMQ Watcher: the %r step failed or exceeded %ss — the watcher goes on",
                name, timeout, exc_info=True,
            )
            return False
        return True

    async def _wait_for_next_cycle(self) -> None:
        """Wait one reconcile interval, returning at once once the watcher is stopping.

        The wake-up event is cleared afterwards, so it signals "somebody nudged the
        loop just now" rather than latching on the first nudge for the rest of the
        process; whether the watcher keeps going is decided by the stop event alone.
        """
        if self._stop_event.is_set():
            return
        waker = self._waker
        if waker is None:
            return
        wakeup = waker[1]
        try:
            await call_with_timeout(wakeup.wait(), self._reconcile_interval)
        except asyncio.TimeoutError:
            pass
        finally:
            wakeup.clear()

    async def _run_cycle(self) -> None:
        """Run one reconciliation pass: migrate, scan, sync, read subscriptions, reconcile.

        Errors are logged here rather than around the call site, so that the only
        thing the caller's timeout can observe is the timeout itself.
        ``asyncio.TimeoutError`` is a subclass of ``Exception`` (an alias of the
        builtin ``TimeoutError`` from Python 3.11 on), so an ``except Exception``
        wrapped around this call instead of placed inside it would swallow the cycle
        watchdog whole and leave the layer a no-op.

        A stop requested during the pass ends it before the next step, and a blocking
        step in flight is not waited for (:class:`_StopRequested`): the loop goes on to
        stop the consumer manager, and a reconcile is never started once a stop has
        been requested. A reconcile already running when the stop arrives is not
        interrupted and runs to its end, under the cycle budget.
        """
        try:
            self._phase = "migrate"
            await self._ensure_schema()

            self._phase = "scan"
            try:
                scanned = await self._cycle_step("scan", self._scan_subscriptions)
            except _UntrustedScan as exc:
                # The rest of the cycle runs from the database rows, so consumption, the
                # liveness check and the status writes survive a filesystem that looked
                # away — including on a watcher that has no good scan behind it yet, where
                # the substitute is empty and the ui subscriptions still get reconciled.
                log.error(
                    "RMQ Watcher: the scan result cannot be trusted (%s) — skipping the "
                    "sync step, keeping the subscription rows as they are", exc,
                )
                scanned = self._last_good_scan
            else:
                self._last_good_scan = scanned
                self._scan_generation += 1

                self._phase = "sync"
                await self._cycle_step(
                    "sync", self._sync_to_db, scanned, self._scan_generation
                )

            # Exchange/routing_keys metadata is never persisted to the DB (see
            # plan Technical Details → "Почему миграция БД не нужна") — it only
            # lives in-memory in the AST scan cache, re-derived every cycle. Build
            # a lookup keyed the same way as the unique constraint on
            # RMQSubscription so it can be merged back onto DB rows below.
            exchange_meta = {
                _sub_key(s): {
                    "exchange": s["exchange"],
                    "routing_keys": s["routing_keys"],
                }
                for s in scanned
                if "exchange" in s
            }

            self._phase = "read subs"
            active_subs = await self._cycle_step("read subs", _read_active_subs, exchange_meta)

            self._phase = "reconcile"
            self._raise_if_stopping("reconcile")
            await self._manager.reconcile(active_subs)
        except _StopRequested as exc:
            log.info("RMQ Watcher: stop requested — ending this cycle %s", exc)
        except _StepInFlight as exc:
            log.warning("RMQ Watcher: skipping this reconciliation cycle — %s", exc)
        except Exception:
            log.exception("Error in RMQ Watcher reconciliation cycle")

    async def _cycle_step(
        self, name: str, fn: Any, *args: Any, timeout: float | None = None
    ) -> Any:
        """Run one blocking step of the cycle in the cycle pool, bounded and unrepeated.

        :param name: Step name, used for the in-flight bookkeeping and the log line.
        :param fn: Blocking callable to run in the cycle pool.
        :param timeout: Seconds this step is given; the steps of the cycle proper leave
            it out and share :data:`_STEP_TIMEOUT`; the tunable read, the migration and
            the heartbeat steps name one of their own.
        :returns: Whatever ``fn`` returned.
        :raises _StopRequested: A stop has been requested, before the step started or
            while its attempt was running.
        :raises _StepInFlight: The previous attempt at this step has not returned.
        :raises asyncio.TimeoutError: The attempt outlived ``timeout``.

        For the steps of the cycle proper either failure ends the cycle: they feed each
        other, and a reconcile run on a subscription list that could not be read would
        cancel every consumer of a subscription the query simply did not return.

        The bound buys back the coroutine, not the worker — a running thread cannot be
        interrupted — hence the second half: one attempt of a step in flight at a time.
        Resubmitting every cycle would fill the four-worker pool with stuck copies of the
        same call, and the steps that need no database would stop running with it.

        A step is not started once a stop has been requested, and a stop requested while
        the attempt runs ends the wait for it at once (:meth:`_attempt_unless_stopping`):
        the loop is on its way to stopping the consumer manager, and a step holding it
        up for as long as ``timeout`` would spend the time the watcher process gives the
        loop to get there. The attempt stays in the bookkeeping like any other attempt
        the cycle stopped waiting for.
        """
        timeout = _STEP_TIMEOUT if timeout is None else timeout
        self._raise_if_stopping(name)
        cycle_pool, _ = self._ensure_pools()
        attempt = self._step_attempts.get(name)
        if attempt is not None and not attempt.done():
            incr("rmq_watcher.cycle_step_skipped")
            raise _StepInFlight(
                f"the {name!r} step of an earlier cycle is still running in the "
                f"{cycle_pool.name!r} pool "
                f"({cycle_pool.in_flight}/{cycle_pool.max_workers} "
                f"workers busy)"
            )
        attempt = cycle_pool.submit(fn, *args)
        self._step_attempts[name] = attempt
        try:
            return await call_with_timeout(
                self._attempt_unless_stopping(name, attempt), timeout
            )
        except asyncio.TimeoutError:
            incr("rmq_watcher.cycle_step_timed_out")
            log.warning(
                "RMQ Watcher: the %r step did not finish within %ss; its worker stays "
                "busy until the call itself returns",
                name, timeout,
            )
            raise

    def _raise_if_stopping(self, step: str) -> None:
        """Raise :class:`_StopRequested` naming ``step`` once a stop has been requested."""
        if self._stop_event.is_set():
            raise _StopRequested(f"before the {step!r} step")

    async def _attempt_unless_stopping(self, step: str, attempt: Future) -> Any:
        """Await ``attempt``, giving up on it as soon as a stop has been requested.

        :param step: Step name, carried by the :class:`_StopRequested` raised.
        :param attempt: The step's call, submitted to the cycle pool.
        :returns: Whatever the call returned.
        :raises _StopRequested: The stop event was set while the call was running.

        The call is raced against the wake-up event :meth:`request_stop` sets, and the
        stop event decides what a wake-up means: one that comes without a stop leaves
        the call to be awaited to its end, under the caller's bound. Giving up cancels
        the call if it has not started yet; a call already running holds its worker
        until it returns. A cycle run on a loop other than the one ``_main`` published
        its wake-up event for has nothing to race against and awaits the call alone.
        """
        result = asyncio.wrap_future(attempt)
        waker = self._waker
        if waker is None or waker[0] is not asyncio.get_running_loop():
            return await result
        stopping = asyncio.ensure_future(waker[1].wait())
        try:
            await asyncio.wait({result, stopping}, return_when=asyncio.FIRST_COMPLETED)
            if not result.done() and self._stop_event.is_set():
                raise _StopRequested(f"while the {step!r} step was running")
            return await result
        finally:
            stopping.cancel()
            result.cancel()

    async def _stop_manager(self) -> None:
        """Stop the manager, giving up after ``_STOP_TIMEOUT`` seconds.

        Reached while the loop is already being torn down, so a manager that cannot
        finish must not keep the thread from starting a fresh loop.
        """
        try:
            await call_with_timeout(self._manager.stop(), _STOP_TIMEOUT)
        except Exception:
            log.warning(
                "RMQ Watcher: manager.stop() did not finish within %ss — continuing "
                "with loop teardown", _STOP_TIMEOUT, exc_info=True,
            )

    # ------------------------------------------------------------------
    # Tunables and schema migration — blocking calls kept off the loop thread
    # ------------------------------------------------------------------

    def _cycle_timeout(self) -> float:
        """Seconds one cycle may take before the event loop is recreated."""
        return cycle_timeout(self._reconcile_interval, self._cycle_timeout_override)

    async def _refresh_settings(self) -> None:
        """Re-read the tunables, keeping the last known values on a read that did not
        answer.

        ``Variable.get`` talks to the database and the result decides the cycle
        budget, so it has to be read *before* the budget starts counting — outside
        everything the cycle watchdog covers. It therefore runs in the cycle pool
        under a short timeout of its own. A read still stuck in a worker blocks the
        next one from starting, so an unresponsive database costs one worker rather
        than one per cycle, and a changed Variable takes effect on the next cycle.

        The values are held on a read that timed out or threw. A database that answers
        with a refusal is a different case: Airflow swallows a failing secrets backend
        and hands back the very ``None`` an unset Variable gives, so the refusal is
        indistinguishable from nobody having set anything and the built-in defaults take
        effect — an operator's interval is dropped for the duration. That is recorded in
        ``docs/backlog/refusing-database-resets-tunables.md``.
        """
        try:
            interval, cycle_timeout = await self._cycle_step(
                "settings", _read_settings, timeout=VARIABLE_TIMEOUT
            )
        except (_StepInFlight, _StopRequested):
            return
        except Exception:
            log.warning(
                "RMQ Watcher: cannot read tunables from Airflow Variables — keeping "
                "interval=%ss, cycle timeout=%ss",
                self._reconcile_interval, self._cycle_timeout(), exc_info=True,
            )
            return
        previous = (self._reconcile_interval, self._cycle_timeout())
        self._reconcile_interval = (
            interval if interval is not None else DEFAULT_RECONCILE_INTERVAL
        )
        self._cycle_timeout_override = cycle_timeout
        if previous != (self._reconcile_interval, self._cycle_timeout()):
            log.info(
                "RMQ Watcher tunables in effect: reconcile interval %ss, cycle budget "
                "%.0fs", self._reconcile_interval, self._cycle_timeout(),
            )

    async def _ensure_schema(self) -> None:
        """Retry table creation and column migration until the schema is ready.

        The plugin runs the migration once at load; a database unreachable at that
        moment would leave the ORM model describing columns the live table lacks,
        which breaks every status upsert and the Subscriptions page with it. So each
        cycle tries again until the schema reports itself ready.

        The call blocks (``create_all``, ``inspect``, ``ALTER TABLE``) and goes to the
        cycle pool: run inline it would block the loop thread, and a blocked loop
        services no timers — the very watchdog wrapped around this cycle would never
        fire, and AMQP heartbeats would stop leaving the process with it.

        ``ALTER TABLE ... ADD COLUMN`` needs an ACCESS EXCLUSIVE lock and can queue
        behind whoever holds one, hence both the private timeout and the single
        attempt in flight: a timeout does not free the worker, so a retry every cycle
        would fill the pool with stuck attempts and hold that many connections of
        Airflow's shared engine.
        """
        if is_schema_ready():
            return
        if self._migration_skip_cycles > 0:
            self._migration_skip_cycles -= 1
            return

        try:
            await self._cycle_step(
                "migrate", ensure_table_exists, timeout=_MIGRATION_TIMEOUT
            )
        except _StepInFlight:
            return
        except _StopRequested:
            raise
        except Exception:
            log.warning(
                "RMQ Watcher: schema migration attempt failed or exceeded %ss",
                _MIGRATION_TIMEOUT, exc_info=True,
            )
            self._defer_migration()
            return

        if is_schema_ready():
            self._migration_backoff = 0
            self._migration_skip_cycles = 0
            log.info("RMQ Watcher: watcher schema is ready")
        else:
            self._defer_migration()

    def _defer_migration(self) -> None:
        """Hold off the next migration attempt, doubling the wait up to the cap."""
        self._migration_backoff = next_backoff(
            self._migration_backoff,
            _MIGRATION_BACKOFF_MAX,
            minimum=_MIGRATION_BACKOFF_START,
        )
        self._migration_skip_cycles = self._migration_backoff
        log.warning(
            "RMQ Watcher: schema is not ready — retrying the migration in %s cycle(s)",
            self._migration_backoff,
        )

    # ------------------------------------------------------------------
    # DAG-file scanning (mtime-based incremental)
    # ------------------------------------------------------------------

    def _get_dags_folder(self) -> str:
        """Path of the DAG folder, verified to be a source a scan can be trusted from.

        The value comes from ``airflow.settings.DAGS_FOLDER`` — the same path the
        Scheduler parses DAGs from — and every one of the checks below turns a bad
        value into :class:`_UntrustedScan` instead of a scan result:

        - an empty value names no folder at all, which is a broken setting rather
          than a folder that holds no DAG files;
        - a missing directory is not a statement about the code either — it is what a
          git-sync symlink swap looks like for the second it lasts.

        Whether the folder can be listed is the walk's to tell:
        :meth:`_scan_subscriptions` turns away a folder whose root the walk cannot
        read — ``os.path.isdir`` answers ``True`` for a directory with no read
        permission, and such a folder must never pass for an honestly empty one,
        which the scan is required to take as the truth.
        """
        try:
            from airflow import settings as airflow_settings
            dags_folder = airflow_settings.DAGS_FOLDER
        except Exception as exc:
            raise _UntrustedScan(f"the DAG folder setting is unreadable: {exc!r}") from exc

        if not dags_folder:
            raise _UntrustedScan("the DAG folder setting is empty")
        if not os.path.isdir(dags_folder):
            raise _UntrustedScan(f"the DAG folder {dags_folder!r} is not a directory")
        return dags_folder

    def _scan_subscriptions(self) -> list[dict]:
        """Incrementally scan DAG files for @rmq_trigger subscriptions using mtime.

        The files are those Airflow loads (:func:`list_dag_files`): every ``.py`` file
        of the folder, hidden files and directories included, less what an
        ``.airflowignore`` excludes. On a stable deployment (no file changes) this
        costs the walk and N mtime syscalls. Changed or new files are re-parsed;
        deleted files are evicted from the cache. Memory is bounded to the number of
        current DAG files on disk.

        A subdirectory the walk cannot read hides its files, not their absence: every
        file the cache knows under it is accounted for as one more file of the listing
        that did not read, exactly as an unreadable file is. Its previous parse is held
        for ``_UNREADABLE_FILE_LIMIT`` cycles (:meth:`_file_unreadable`), it counts
        against the share of the listing that read, and once the hold runs out its keys
        go the ordinary counted-miss way. The directory itself is reported once, when
        it becomes unreadable, and once more when a walk reads it again.

        :raises _UntrustedScan: The DAG folder is not a source the scan can be trusted
            from — its root or the root's ``.airflowignore`` cannot be read, or
            Airflow's rules cannot list it — or too little of the listing it produced
            could be read for the result to be a statement about the code (see
            :data:`_TRUSTED_SCAN_FRACTION`). Nothing the sync step would count misses
            against is touched before that verdict: the folder is checked first, the
            per-file failures are held back until the whole listing has been tried,
            and the eviction of cached paths the listing does not name waits for the
            verdict too.
        """
        # The streak below counts listings, and a folder that produces none at all — it
        # vanished under a git-sync swap, refuses to be listed, or cannot be listed by
        # Airflow's rules — leaves it untouched: such an outage neither spends the
        # budget nor refills it. Refilling it would let a folder alternating between
        # unlistable and degraded hold the distrust for as long as it keeps alternating,
        # and the sync step, deletions and new subscriptions alike, would never run
        # again on such an installation.
        dags_folder = self._get_dags_folder()
        try:
            listing = list_dag_files(dags_folder, self._unreadable_dirs)
        except DagFolderListingError as exc:
            raise _UntrustedScan(
                f"the DAG folder {dags_folder!r} cannot be listed by Airflow's rules: {exc}"
            ) from exc
        # The walk spells the root exactly as it was given.
        root_error = listing.unreadable_dirs.get(dags_folder)
        if root_error is not None:
            raise _UntrustedScan(
                f"the DAG folder {dags_folder!r} cannot be listed: {root_error!r}"
            ) from root_error
        self._report_unreadable_dirs(listing.unreadable_dirs, listing.recovered)
        self._unreadable_dirs = set(listing.unreadable_dirs)

        current_files = set(listing.files)

        # Re-parse changed or new files. Failures are collected rather than applied as
        # they are met: how much of the listing read is what decides whether the listing
        # is about the code at all, and that is known only once every file has been
        # tried. Applying them first would drop the keys of the installation from a scan
        # the sync step counts misses against.
        failures: list[tuple[str, Exception | str, float | None]] = []
        # Listed paths that give no stat, with their error: nothing tells what they are.
        unexamined: dict[str, Exception] = {}
        for path in current_files:
            try:
                mtime = os.path.getmtime(path)
            except OSError as exc:
                # The file is in the listing but gives no stat — permissions, a stale
                # NFS handle. Nothing is known about its content, so no mtime is kept.
                failures.append((path, exc, None))
                unexamined[path] = exc
                continue
            if mtime == self._last_mtimes.get(path):
                # A stat that answered is evidence the previous failure is over, and
                # the patience the next one gets is measured from here.
                self._read_failures.pop(path, None)
                continue
            try:
                subs = self._extract_subscriptions_from_file(path)
            except OSError as exc:
                failures.append((path, exc, None))
                continue
            if subs is None:
                failures.append((path, "parse failed", mtime))
                continue
            self._read_failures.pop(path, None)
            self._cached_subs[path] = subs
            self._last_mtimes[path] = mtime

        # Files the cache knows under a directory the walk could not read are files of
        # this listing that did not read. A listed ``.py`` path that gives no stat
        # counts as such a directory: it may be a symlinked directory named ``*.py``
        # whose target went out of reach.
        hidden = self._paths_hidden_by(
            {**unexamined, **listing.unreadable_dirs}, current_files
        )
        current_files.update(path for path, _, _ in hidden)
        failures.extend(hidden)

        readable = len(current_files) - len(failures)
        baseline = self._trusted_file_count
        if current_files and not readable and not self._trusted_listing_read_nothing:
            # A folder that named files not one of which could be read says nothing
            # about the code in them, whatever the mark stands at — and it stands at
            # zero for as long as an empty folder is what was last believed, which is
            # a mark every listing clears. The share below is a share of something.
            trusted = False
        elif baseline is None:
            # No listing of this process has been believed yet, so there is nothing to
            # measure this one against and the only listing a cold start can refuse is
            # the one that produced nothing readable at all. What a partial listing
            # costs here is bounded by the miss counter alone — see the note on the
            # cold start in :data:`_TRUSTED_SCAN_FRACTION`.
            trusted = readable > 0
        else:
            trusted = readable >= baseline * _TRUSTED_SCAN_FRACTION

        if trusted:
            self._untrusted_scan_streak = 0
            # The high-water mark, so that a decline slower than the share per cycle is
            # measured against what the folder held rather than against its own previous
            # step: a baseline every trusted listing rewrote would follow a folder
            # unmounting piece by piece all the way down and call every step of it
            # trustworthy.
            self._trusted_file_count = readable if baseline is None else max(baseline, readable)
            self._trusted_listing_read_nothing = bool(current_files) and not readable
        else:
            what = _untrusted_listing_reason(
                dags_folder, current_files, readable, baseline, failures,
                listing.unreadable_dirs,
            )
            self._untrusted_scan_streak += 1
            if self._untrusted_scan_streak <= _UNTRUSTED_SCAN_LIMIT:
                raise _UntrustedScan(
                    f"{what} ({self._untrusted_scan_streak} of "
                    f"{_UNTRUSTED_SCAN_LIMIT} in a row)"
                )
            log.warning(
                "RMQ Watcher: %s — %d times in a row now, so the listing is taken as "
                "the truth and the keys missing from it go the counted-miss way",
                what, self._untrusted_scan_streak,
            )
            # Giving in is believing the shrink, so the mark comes down to it and the
            # streak starts over. Both halves matter: a mark left where it was would
            # distrust every listing that follows for the rest of the process — an
            # installation whose DAG folder is legitimately empty would log this line
            # every cycle forever — and a streak left spent would hand the next collapse
            # no grace at all.
            self._untrusted_scan_streak = 0
            self._trusted_file_count = readable
            self._trusted_listing_read_nothing = bool(current_files) and not readable

        for path, reason, mtime in failures:
            self._file_unreadable(path, reason, mtime)

        # Evict files the listing no longer names. Every dict keyed by path is drained,
        # not only the one holding mtimes: an I/O failure drops the mtime and keeps the
        # other two, and a file deleted in that state would otherwise stay in them for
        # the process's life.
        for path in self._known_paths():
            if path not in current_files:
                self._last_mtimes.pop(path, None)
                self._cached_subs.pop(path, None)
                self._read_failures.pop(path, None)

        # Flatten all cached subscriptions
        result: list[dict] = []
        for subs in self._cached_subs.values():
            result.extend(subs)
        return result

    def _known_paths(self) -> set[str]:
        """Every path the scan cache holds an entry for, in any of its dicts."""
        return set(self._last_mtimes) | set(self._cached_subs) | set(self._read_failures)

    def _paths_hidden_by(
        self, hidden_by: dict[str, Exception], current_files: set[str]
    ) -> list[tuple[str, Exception | str, float | None]]:
        """The failures of the cached files under a directory the walk could not read.

        Such a file is a file of the listing that did not read: it gets the error of
        the first directory of ``hidden_by`` it sits under and no mtime, since nothing
        is known about its content. Files already in ``current_files`` are not
        repeated. The result is in path order.
        """
        if not hidden_by:
            return []
        prefixes = [
            (os.path.join(dir_path, ""), dir_error)
            for dir_path, dir_error in hidden_by.items()
        ]
        hidden: list[tuple[str, Exception | str, float | None]] = []
        for path in sorted(self._known_paths() - current_files):
            for prefix, dir_error in prefixes:
                if path.startswith(prefix):
                    hidden.append((path, dir_error, None))
                    break
        return hidden

    def _report_unreadable_dirs(
        self, unreadable_dirs: dict[str, Exception], recovered: set[str]
    ) -> None:
        """Log the directories of this walk that cannot be read, and those read again.

        A directory gets a WARNING in the walk that first finds it unreadable and a
        DEBUG line in every walk after it that still does; the files under it get
        their own lines from :meth:`_file_unreadable`. A directory of the previous
        walk that this one read without error gets an INFO line. One that left the
        set any other way — deleted, excluded by an ``.airflowignore``, or under a
        parent that cannot be read itself — is not reported: nothing is known about
        whether it reads.
        """
        for dir_path, dir_error in sorted(unreadable_dirs.items()):
            if dir_path in self._unreadable_dirs:
                log.debug(
                    "RMQ Watcher: DAG folder directory %s still cannot be read (%r)",
                    dir_path, dir_error,
                )
            else:
                log.warning(
                    "RMQ Watcher: DAG folder directory %s cannot be read (%r) — the DAG "
                    "files the watcher knows under it count as unreadable files until "
                    "it reads again",
                    dir_path, dir_error,
                )
        for dir_path in sorted(recovered):
            log.info("RMQ Watcher: DAG folder directory %s is readable again", dir_path)

    def _file_unreadable(
        self, path: str, reason: Exception | str, mtime: float | None
    ) -> None:
        """Account for one failed read of ``path`` and decide what its cache now holds.

        The previous parse is kept for ``_UNREADABLE_FILE_LIMIT`` cycles — long enough
        for a file caught mid-write to finish being written, so its keys stay in the
        scan and neither the miss counter nor the orphan tracker notices. Once patience
        runs out (or there was never a parse to keep) the file counts as having no
        subscriptions and its keys go the ordinary counted-miss way.

        ``mtime`` carries the whole difference between the two kinds of failure. A
        parse failure passes the file's mtime and it is recorded, so a permanently
        malformed file is re-parsed once per edit rather than once per cycle. An I/O
        failure passes ``None`` and the remembered mtime is dropped instead: nothing is
        known about the content, and keeping the old value would make the empty cache
        stick — once access came back with the content unchanged, ``mtime ==
        self._last_mtimes.get(path)`` would skip the re-read and the file would count
        as subscription-less until its next edit. Re-reading an unreadable file every
        cycle costs one failed syscall, orders of magnitude less than the parse the
        mtime bookkeeping exists to avoid.
        """
        streak = self._read_failures.get(path, 0) + 1
        self._read_failures[path] = streak

        if path in self._cached_subs and streak <= _UNREADABLE_FILE_LIMIT:
            log.warning(
                "RMQ Watcher: DAG file %s did not read (%s) — holding its previous "
                "parse for this cycle (%d of %d)",
                path, reason, streak, _UNREADABLE_FILE_LIMIT,
            )
            return

        # A file whose mtime is dropped is re-read every cycle, so a permanently
        # unreadable one reaches this line for as long as it stays in the folder. The
        # streak that first ran out of patience is worth a WARNING; the ones after it
        # repeat a state the operator has already been told about.
        log.log(
            logging.WARNING if streak <= _UNREADABLE_FILE_LIMIT + 1 else logging.DEBUG,
            "RMQ Watcher: DAG file %s did not read (%s) %d cycle(s) in a row — "
            "dropping its subscriptions",
            path, reason, streak,
        )
        self._cached_subs[path] = []
        if mtime is None:
            self._last_mtimes.pop(path, None)
        else:
            self._last_mtimes[path] = mtime

    def _extract_subscriptions_from_file(self, path: str) -> list[dict] | None:
        """Extract @rmq_trigger subscriptions from a DAG file via AST parsing.

        Reading and parsing the file (``dag_file_scan._read_and_parse``) and reading
        the parsed file (``dag_file_scan._read_file_triggers``) are done by
        ``dag_file_scan.py``; this method maps their outcomes onto what the scan
        records for the file and logs what the operator needs to know.

        AST parsing never executes the file and imports nothing, so no DAG code runs
        in the watcher process. The scan runs in a worker of the cycle pool under a
        step timeout, and that timeout buys back the coroutine, not the worker. DagBag
        would execute every file there — its imports and module-level calls — and a
        file that hangs on import would keep the worker for as long as it hangs while
        holding the import lock of every module it is in the middle of importing,
        blocking any other thread of the process that imports one of them. Parsing
        the source leaves nothing to wait on but the file read itself.

        The file is refused whole: if ``_read_file_triggers`` reports any problem
        with it — its docstring lists what the scan refuses — not one subscription
        of the file is registered, and an ERROR names the file, every problem with
        its location and reason, and how to write the file
        (``dag_file_scan._READABLE_FILE_REMEDY``).
        The decorator raises on the same file at import time, which takes every DAG
        of the file out of Airflow; a subscription kept for a neighbouring DAG would
        consume for a DAG Airflow no longer runs. Rows the file registered before go
        the counted-miss way, like those of a removed decorator.

        Defense in depth: the reading of the parsed file is wrapped in a broad
        ``except Exception``. A bug that lets an unexpected exception out of it must
        cost this one DAG file's subscriptions for the cycle, not crash the entire
        reconcile cycle for every other DAG, so it answers ``None``.

        :returns: The file's subscriptions — an empty list for a file read and parsed
            but refused — or ``None`` when the content was read but did not yield a
            parse (content that does not decode, that the parser refuses in any of
            its ways, or anything the broad guard around the reading catches).
        :raises OSError: The file gave no access at all. The two answers are kept
            apart because ``_scan_subscriptions`` records the file's mtime only
            after a failed parse: a parse happened means the content was read and
            its mtime says something about it, which is what keeps a permanently
            malformed file from being re-parsed on every single cycle forever. An
            ``open`` that never answered says nothing about the content, and a
            remembered mtime would be a lie.
        """
        try:
            tree = _read_and_parse(path)
        except OSError:
            raise
        except Exception as exc:  # noqa: BLE001 - the caller's contract is OSError alone
            # Every way the parser has of refusing the content it was handed lands
            # here: a syntax error, source that does not decode, the ``ValueError:
            # source code string cannot contain null bytes`` of a file caught
            # mid-write or of a binary one under a ``.py`` name, the
            # ``RecursionError`` of a deeply nested one. All of them say the same
            # thing to the caller — the content was read and did not yield a parse —
            # and the one exception this method may raise is the ``OSError`` of a
            # file that gave no access, which a caller counting misses reads
            # differently.
            log.warning("Failed to parse DAG file %s: %r", path, exc)
            return None

        try:
            read = _read_file_triggers(tree)
        except Exception:
            log.exception(
                "Unexpected error extracting @rmq_trigger subscriptions from DAG file %s — "
                "skipping this file's subscriptions for this cycle",
                path,
            )
            return None

        if read.problems:
            log.error(
                "rmq_trigger: DAG file %s cannot be read statically, so no subscription "
                "of this file is registered. %s\n%s",
                path,
                _READABLE_FILE_REMEDY,
                "\n".join(f"  - {problem}" for problem in read.problems),
            )
            return []
        return read.subscriptions

    # ------------------------------------------------------------------
    # DB synchronisation
    # ------------------------------------------------------------------

    def _delete_absent_sub(
        self,
        session: Session,
        sub: RMQSubscription,
        observed: int | None,
        deleted_now: list[tuple[tuple[str, str, str], bool]],
        pending_logs: list[tuple[int, str, tuple]],
        *,
        report: str,
        only_enabled: bool = False,
    ) -> bool:
        """Delete one dag_file row, but only while it still reads the way it was read.

        Another scheduler may have seen the key come back and cleared the counter
        between this cycle's read and its write; the condition on the counter leaves
        that row alone instead of deleting it and letting the next scan recreate it
        with a fresh ``id`` and ``enabled=True``.

        ``only_enabled`` puts ``enabled`` into the same predicate, for the deletion an
        enabled row of an inactive DAG gets ahead of the grace period. That deletion is
        owed entirely to the consumer such a row keeps — one that acks and drops every
        delivery — so an operator switching the row off in the window between the read
        and the write leaves a row with no consumer, which is a row the full grace
        period covers.

        A row that really went is appended to ``deleted_now`` as its key and the
        ``enabled`` flag this cycle read on it, and the line about it — built around
        ``report``, the sentence naming why it went — to ``pending_logs``. The flag is
        an observation and is reported as one: only the early deletion puts ``enabled``
        into the ``DELETE`` predicate, so on the grace-period path an operator's toggle
        between the read and the write leaves the recorded flag naming what the cycle
        saw rather than what the row held when it went. The caller takes both
        off those lists once the transaction holding the deletion is committed, so
        that the record making the scan report a recreated key and the line saying the
        row went alike stand for a deletion the database kept. Returns whether the row
        went, so that the caller says what happened rather than what it set out to do.
        """
        key = (sub.dag_id, sub.queue_name, sub.conn_id)
        was_enabled = bool(sub.enabled)
        criteria = [_absent_cycles_still(observed)]
        if only_enabled:
            criteria.append(RMQSubscription.enabled.is_(True))
        deleted = session.query(RMQSubscription).filter_by(
            dag_id=sub.dag_id,
            queue_name=sub.queue_name,
            conn_id=sub.conn_id,
            source="dag_file",
        ).filter(*criteria).delete()
        if deleted:
            deleted_now.append((key, was_enabled))
            pending_logs.append((
                logging.INFO,
                "RMQ Watcher: dag_file subscription %s/%s (conn_id=%s) %s",
                (*key, report),
            ))
        return bool(deleted)

    def _count_absent_cycle(
        self,
        session: Session,
        sub: RMQSubscription,
        observed: int | None,
        pending_logs: list[tuple[int, str, tuple]],
    ) -> None:
        """Write one more miss onto ``sub``, while its counter still reads ``observed``.

        The counter is the whole of what stands between a key that went missing for a
        moment and a row that gets deleted, and every scheduler writes it from a value
        it read some milliseconds earlier. A plain assignment would carry that value
        over a recovery another scheduler has already committed — the key came back in
        its scan, the upsert cleared the counter — and put the row back at a count from
        which the next miss deletes it, an intervening successful scan notwithstanding.
        The write therefore matches on what was read, and a scheduler holding a stale
        value writes nothing.

        The line reporting the count goes to ``pending_logs`` for the caller to emit
        once the transaction is committed, because the count is one statement of a
        transaction that goes on to upsert every scanned key. The line about a write
        the predicate refused is emitted here: it says the row was left alone, which
        is what the whole transaction leaves it either way.
        """
        counted = observed + 1 if observed else 1
        updated = session.query(RMQSubscription).filter_by(
            dag_id=sub.dag_id,
            queue_name=sub.queue_name,
            conn_id=sub.conn_id,
            source="dag_file",
        ).filter(_absent_cycles_still(observed)).update(
            {"absent_cycles": counted}, synchronize_session=False
        )
        if updated:
            pending_logs.append((
                logging.WARNING,
                ("RMQ Watcher: dag_file subscription %s/%s (conn_id=%s) missing from "
                 "the scan (%s of %s allowed) — keeping the row for now"),
                (sub.dag_id, sub.queue_name, sub.conn_id, counted, _ABSENT_GRACE_CYCLES),
            ))
        else:
            log.info(
                "RMQ Watcher: dag_file subscription %s/%s (conn_id=%s) missing from the "
                "scan, but its miss counter moved since this cycle read it — leaving "
                "the row to the scheduler that wrote it",
                sub.dag_id, sub.queue_name, sub.conn_id,
            )

    def _remember_deleted_row(self, key: tuple[str, str, str], was_enabled: bool) -> None:
        """Record one committed deletion, dropping the oldest record to stay in bounds."""
        self._deleted_dag_file_rows.pop(key, None)
        self._deleted_dag_file_rows[key] = was_enabled
        while len(self._deleted_dag_file_rows) > _DELETED_ROWS_REMEMBERED:
            self._deleted_dag_file_rows.pop(next(iter(self._deleted_dag_file_rows)))

    def _sync_to_db(self, scanned: list[dict], generation: int | None = None) -> None:
        """Reconcile dag_file subscriptions in DB with the current scan result.

        :param scanned: Subscriptions the scan found in the DAG files.
        :param generation: :attr:`_scan_generation` ``scanned`` was accepted as, or
            ``None`` from a caller that has no scan sequence behind it. A pass whose
            ordinal no longer matches the listener's commits nothing: the folder has
            been read again since, and every deletion below was decided against a
            listing that reading superseded.

        - Upserts all subscriptions found in code (source='dag_file').
        - Counts the scans a dag_file row's key was missing from, in
          ``absent_cycles``. :data:`_ABSENT_GRACE_CYCLES` misses are survivable and the
          one after them deletes the row. Until then the row lives an entirely ordinary
          life: same ``id``, same consumer task, same ``enabled``.
        - Deletes an enabled row earlier — from its third miss on — when Airflow no
          longer counts its ``dag_id`` as active, because a delivery on such a
          subscription is acknowledged and dropped. A disabled row has no consumer and
          therefore always gets the full grace period.
        - Reports a key that comes back after this watcher deleted its row: what the
          scan recreates is a new row carrying the default ``enabled``, so a
          subscription an operator had switched off starts consuming again — with a
          WARNING naming it and the flag the deleting cycle read on the row. The
          deletions it is matched against are the ones this process made and still
          remembers, so the
          line follows the memory of the process rather than the row: a scheduler
          restarted in between, or another scheduler recreating what this one deleted,
          leaves the recreation to the miss counter's own INFO lines.
        - Never touches ui-sourced subscriptions.
        """
        scanned_keys = {_sub_key(s) for s in scanned}
        active_dag_ids: set[str] | None = None
        # Deletions this cycle made and keys it saw come back. Both are applied to the
        # map of deleted keys once the transaction carrying them is committed: a commit
        # that fails leaves neither a record of a row that is still there, nor a hole
        # where the report of a key's return has yet to be made.
        deleted_now: list[tuple[tuple[str, str, str], bool]] = []
        returned_now: list[tuple[str, str, str]] = []
        # Lines about writes this transaction is making, each of them a level, a format
        # string and the arguments filling it. They are emitted from the same place,
        # after the commit, so that a line claiming a row was deleted, a miss counted
        # or a counter cleared stands for a write the database kept.
        pending_logs: list[tuple[int, str, tuple]] = []

        with WatcherSession() as session:
            existing = (
                session.query(RMQSubscription)
                .filter_by(source="dag_file")
                .all()
            )
            existing_keys = {(s.dag_id, s.queue_name, s.conn_id) for s in existing}

            # The active dag_ids are read before the loop writes anything, and only
            # when a row of this cycle could reach the short circuit at all — on the
            # overwhelmingly common cycle every key is in the scan and the query behind
            # them, an unindexed full scan of Airflow's ``dag`` table, is never run.
            # The read gets a session of its own, and a pool one connection wide hands
            # that session the connection this one is already writing through: closing
            # it rolls the connection back, so anything this cycle had flushed by then
            # would be gone from the database while the code above went on believing it
            # deleted a row.
            # The window is bounded on both sides: a row that has already counted
            # _ABSENT_GRACE_CYCLES misses goes on the grace period alone, before the
            # branch this read serves is reached, so its presence is no reason to run
            # the query — and no reason to let the query's latency stand in front of a
            # deletion that asks nothing of it.
            if any(
                sub.enabled
                and _INACTIVE_DAG_MISSES <= (sub.absent_cycles or 0) < _ABSENT_GRACE_CYCLES
                and (sub.dag_id, sub.queue_name, sub.conn_id) not in scanned_keys
                for sub in existing
            ):
                active_dag_ids = _read_active_dag_ids()

            for sub in existing:
                key = (sub.dag_id, sub.queue_name, sub.conn_id)
                if key in scanned_keys:
                    if sub.absent_cycles:
                        pending_logs.append((
                            logging.INFO,
                            ("RMQ Watcher: dag_file subscription %s/%s (conn_id=%s) is "
                             "back in the scan after %s miss(es) — miss counter cleared"),
                            (sub.dag_id, sub.queue_name, sub.conn_id, sub.absent_cycles),
                        ))
                    continue

                # ``NULL`` and ``0`` are the same statement about a row that has never
                # missed a scan, and the arithmetic here reads them as one; the writes
                # below take the value as the row carries it, because SQL matches
                # ``NULL`` on nothing at all.
                observed = sub.absent_cycles
                current = observed or 0

                if current + 1 > _ABSENT_GRACE_CYCLES:
                    if not self._delete_absent_sub(
                        session, sub, observed, deleted_now, pending_logs,
                        report=(
                            f"missing from {current + 1} trusted scans in a row "
                            "— deleted"
                        ),
                    ):
                        log.info(
                            "RMQ Watcher: dag_file subscription %s/%s (conn_id=%s) missing "
                            "from %s trusted scans in a row, but its miss counter moved "
                            "since this cycle read it — leaving the row to the scheduler "
                            "that wrote it",
                            sub.dag_id, sub.queue_name, sub.conn_id, current + 1,
                        )
                    continue

                # The early deletion asks for :data:`_INACTIVE_DAG_MISSES` counted
                # misses, because one count is not proof that a scan missed the key.
                # Every scheduler writes this counter from a value it read milliseconds
                # earlier, and the column carries neither a version nor an ordering: a
                # miss written after another scheduler's successful scan cleared the
                # counter reads exactly like one written before it, so a key that is
                # there can leave a count of 1 behind. A second count is written
                # against a counter that a recovery in between would have cleared, so
                # it says what this deletion needs said — the key was missing.
                if (
                    sub.enabled
                    and current >= _INACTIVE_DAG_MISSES
                    and active_dag_ids is not None
                    and sub.dag_id not in active_dag_ids
                ):
                    if not self._delete_absent_sub(
                        session, sub, observed, deleted_now, pending_logs,
                        only_enabled=True,
                        report=(
                            f"missing from the scan and DAG {sub.dag_id} is no longer "
                            "active — deleted without waiting out the grace period"
                        ),
                    ):
                        log.info(
                            "RMQ Watcher: dag_file subscription %s/%s (conn_id=%s) missing "
                            "from the scan and DAG %s is no longer active, but the row "
                            "changed since this cycle read it — its miss counter, or the "
                            "enabled flag an operator turned off — so it keeps the grace "
                            "period",
                            sub.dag_id, sub.queue_name, sub.conn_id, sub.dag_id,
                        )
                    continue

                self._count_absent_cycle(session, sub, observed, pending_logs)

            for s in scanned:
                key = _sub_key(s)
                was_enabled = self._deleted_dag_file_rows.get(key)
                if was_enabled is not None:
                    returned_now.append(key)
                    if key not in existing_keys:
                        pending_logs.append((
                            logging.WARNING,
                            ("RMQ Watcher: dag_file subscription %s/%s (conn_id=%s) is in "
                             "the scan again after this watcher deleted it — it is "
                             "recreated as a new row carrying the default enabled flag, "
                             "and the cycle that deleted it read the row as %s"),
                            (*key, "enabled" if was_enabled else "disabled"),
                        ))
                upsert_subscription(
                    session,
                    dag_id=s["dag_id"],
                    queue_name=s["queue_name"],
                    conn_id=key[2],
                    filter_data=s.get("filter_data", {}),
                    source="dag_file",
                    cooldown=s.get("cooldown", 0) or None,
                    ack_unmatched=bool(s.get("ack_unmatched", False)),
                    group_key=s.get("group_key"),
                )

            if generation is not None and generation != self._scan_generation:
                session.rollback()
                incr("rmq_watcher.sync_superseded")
                log.warning(
                    "RMQ Watcher: discarding the synchronisation of scan %s — scan %s "
                    "has been accepted while it ran, so its deletions were decided "
                    "against a listing the process has already read past",
                    generation, self._scan_generation,
                )
                return

            session.commit()

            for level, msg, args in pending_logs:
                log.log(level, msg, *args)
            for key in returned_now:
                self._deleted_dag_file_rows.pop(key, None)
            for key, was_enabled in deleted_now:
                self._remember_deleted_row(key, was_enabled)
