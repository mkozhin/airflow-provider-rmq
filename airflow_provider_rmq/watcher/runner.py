"""Entry point of the watcher process, the module ``airflow_provider_rmq.watcher.runner``.

The scheduler starts this module as a child process of its own — ``python -c`` the
listener's bootstrap, which runs it as ``__main__`` — and the watcher loop runs here,
in a fresh interpreter that shares no threads, locks or sockets with the scheduler.
Airflow forks the scheduler process — the Celery send pool and the DAG
processor manager — and a fork is only safe from a process with a single thread, so
the loop and its pools live in this process and the scheduler keeps none of them.

The process is shaped around one rule: a stop signal never runs Python code. SIGTERM
and SIGINT are blocked before anything else happens, the mask is inherited by every
thread started after it, and one thread at a time collects them — the watch thread
while Airflow is being prepared, the main thread after that — with
:func:`signal.sigtimedwait` where the platform has it, as Linux does, and elsewhere,
macOS for one, by polling :func:`signal.sigpending` and taking a pending one with
:func:`signal.sigwait`, and acts on it in ordinary code. A handler could interrupt a
thread that holds a lock — a :class:`threading.Event`, the buffer of stdout — and
deadlock on it; a blocked signal cannot, and one that arrives before either thread
waits stays pending in the kernel until one asks for it, so it is never lost.

SIGHUP, SIGQUIT, SIGUSR1 and SIGUSR2 are ignored from the same first step on. The
process is in the scheduler's process group, and an init that sends a signal it
receives to the whole group, as dumb-init with ``DUMB_INIT_SETSID=1`` does, sends it
here too: a ``docker kill -s USR2`` that asks the scheduler for its executor dump would
otherwise end the watcher, which is not started again.

This module defines every environment variable the two processes share, so the
contract between them lives in one place: the listener in the scheduler sets them when
it starts this process, and they are read here, by the listener's bootstrap and by a
listener loaded in this process.

The scheduler hands its own PID over in the environment, so the value is fixed before
this process exists and cannot race the scheduler's exit. On Linux the kernel is asked
to deliver SIGTERM once the scheduler thread that started this process exits, and the
parent PID is compared with the handed-over one right after that request, which
catches a scheduler that exited before it was made. The watch thread repeats the
comparison while Airflow is being prepared, and once the loop runs the main thread
repeats it between two waits for a stop signal and the loop before every pass; on
other systems those comparisons alone notice the scheduler's death.

Before the loop thread starts, the main thread imports Airflow and brings it to the
state the scheduler is in when its jobs run: the providers' configuration is loaded,
the metadata database is reached directly, and the plugins are loaded together with
the timetables, operator links and dependencies they register. Airflow loads each of
those on first use and none of them under a lock, and the first use here would
otherwise be several pool workers triggering DAG runs at once. The main thread is where
the scheduler loads them too, so a plugin that calls what Python allows in the main
thread alone — :func:`signal.signal`, :func:`signal.set_wakeup_fd` — loads here as it
does there. The loop thread starts only once the preparation has returned, so the
plugins are loaded before the loop runs and never while it does.

The preparation has no time limit of its own — a plugin that hangs on import, a table
creation that waits for a lock — so a thread of its own, ``rmq-watcher-prepare-watch``,
waits for a stop signal from the moment the preparation starts, and compares the parent
PID with the scheduler's every :data:`_SIGNAL_POLL_INTERVAL`. A stop signal or the
scheduler's death while the preparation is still running ends the process at once with
code 0: nothing has started yet that needs stopping. A scheduler that died leaves no
one to follow its SIGTERM with a SIGKILL, and this is what keeps a hung preparation
from outliving it. The watch thread ends once the preparation has returned, before the
loop thread starts, and from then on the main thread alone takes the stop signals.

The process leaves through :func:`os._exit`, after a bounded wait for the loop thread:
the interpreter's own exit joins every pool worker with no timeout, and a call stuck on
the database in one of them would keep the process alive for as long as the operating
system keeps its socket open. Logging is shut down within a bounded time too, since a
thread left behind may hold a handler's lock, and the lines written past such a thread
go straight to stderr.

Every call on the way out may still block for good — a write to a stderr pipe nobody
reads, a handler's lock, a database socket that never answers — and once the scheduler
has died no one is left to send SIGKILL. So the first thing the process does once it
has decided to leave, before it writes or logs a line, is to have the kernel end it
:data:`_HARD_EXIT_DEADLINE` seconds later with SIGALRM, which no thread blocks and no
handler of the process catches: whatever its threads are stuck in, the process is gone
by then. A plugin may set a SIGALRM handler of its own on import, and until the
preparation of Airflow has returned such a handler stays in force, so a stop the watch
thread acts on during the preparation has this deadline only when no plugin did.

Only the standard library is imported at module level. Airflow and the listener are
imported by :func:`main` after the signal mask is in place, so the threads Airflow
starts on import already have it.
"""
from __future__ import annotations

import ctypes
import logging
import os
import signal
import sys
import threading
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow_provider_rmq.watcher.listener import RMQWatcherListener

# Named explicitly: run as a program this module is ``__main__``, and the watcher's
# lines belong under the package's logger hierarchy whichever way it was started.
log = logging.getLogger("airflow_provider_rmq.watcher.runner")

#: Environment variable set in the watcher process. A listener that finds it does not
#: start another process: the watcher runs in this one already.
CHILD_ENV_MARKER = "AIRFLOW_PROVIDER_RMQ_WATCHER_CHILD"

#: Environment variable carrying the PID of the scheduler process that started the
#: watcher, as a decimal string.
PARENT_PID_ENV = "AIRFLOW_PROVIDER_RMQ_WATCHER_PARENT_PID"

#: Environment variable carrying the directory the package is imported from, set when
#: that directory is not one of the interpreter's site-packages — the plugins folder.
#: The listener's bootstrap appends it to the end of :data:`sys.path` before it runs
#: this module, where the scheduler has the plugins folder too.
PACKAGE_DIR_ENV = "AIRFLOW_PROVIDER_RMQ_WATCHER_PACKAGE_DIR"

#: Seconds the main thread waits for the loop thread once the loop has been asked to
#: stop. The loop finishes the step it is on and then stops the consumer manager, which
#: is bounded by the listener's ``_STOP_TIMEOUT``; this is longer, so it leaves the
#: manager all of its budget and some room for the teardown around it. A loop thread
#: still running after it is left behind by the process exit.
_EXIT_JOIN_TIMEOUT = 45.0

#: Signals that stop the watcher. Blocked in every thread of the process and collected
#: by one thread at a time: the watch thread while Airflow is being prepared, the main
#: thread after that.
_STOP_SIGNALS = frozenset({signal.SIGTERM, signal.SIGINT})

#: Signals the process ignores (:func:`_ignore_group_signals`). The default action of
#: each ends a process, and each reaches this one whenever it is sent to the
#: scheduler's process group: the scheduler dumps its executor's state on SIGUSR2 and
#: its threads' stacks on SIGQUIT and runs on, and the watcher runs on with it.
_IGNORED_SIGNALS = (signal.SIGHUP, signal.SIGQUIT, signal.SIGUSR1, signal.SIGUSR2)

#: Seconds one wait for a stop signal lasts: before the watch thread checks whether the
#: scheduler is still its parent and whether the preparation has finished, and before
#: the main thread checks whether the scheduler is still its parent and the loop thread
#: still running.
_SIGNAL_POLL_INTERVAL = 1.0

#: Name of the thread that waits for a stop signal while the main thread prepares
#: Airflow.
_WATCH_THREAD_NAME = "rmq-watcher-prepare-watch"

#: Seconds the process gives :func:`logging.shutdown` on its way out.
_LOGGING_SHUTDOWN_TIMEOUT = 2.0

#: Name of the thread :func:`logging.shutdown` runs in on the way out.
_LOGGING_SHUTDOWN_THREAD_NAME = "rmq-watcher-logging-shutdown"

#: Whole seconds from the decision to leave until the kernel ends the process with
#: SIGALRM (:func:`_arm_hard_exit`). Longer than everything the way out waits for on
#: purpose — the loop thread, the closing of the database connections, the logging
#: shutdown — so a way out that runs as it should ends in :func:`os._exit` before it.
_HARD_EXIT_DEADLINE = int(_EXIT_JOIN_TIMEOUT) + 15

#: Seconds between two looks at the pending signals on a platform without
#: :func:`signal.sigtimedwait`. A fraction of the scheduler's 5-second stop timeout, so
#: a SIGTERM reaches the loop with most of that budget left.
_SIGNAL_PENDING_POLL = 0.1

#: ``PR_SET_PDEATHSIG`` from ``<linux/prctl.h>``.
_PR_SET_PDEATHSIG = 1

#: Functions of ``airflow.plugins_manager`` that :func:`_prepare_airflow` calls, in
#: this order. The first loads the plugins; each of the others collects one kind of
#: class the plugins register, which Airflow reads when it deserializes a DAG. A name
#: the installed Airflow lacks is skipped.
_PLUGIN_INITIALISERS = (
    "ensure_plugins_loaded",
    "initialize_timetables_plugins",
    "initialize_extra_operators_links_plugins",
    "initialize_ti_deps_plugins",
    "initialize_priority_weight_strategy_plugins",
)


def _report(message: str) -> None:
    """Write a line straight to file descriptor 2, the process's stderr.

    :func:`os.write` takes no lock of Python's — neither the buffer of
    :data:`sys.stderr` nor a logging handler's, which a thread still running may hold —
    so the line goes out from any thread at any moment: before Airflow configures
    logging, from the watch thread that is about to end the process, and past a loop
    thread left behind. stderr is the scheduler's, inherited by this process, so the
    line lands in the same container log as everything the watcher logs. A write into a
    pipe nobody drains blocks all the same; every caller on the way out arms the hard
    deadline (:func:`_arm_hard_exit`) first, and the one line written before that — a
    failed parent-death request — is written with the watch thread already running, so
    such a write cannot keep the process alive.
    """
    data = f"RMQ Watcher process {os.getpid()}: {message}\n".encode(errors="backslashreplace")
    try:
        while data:
            data = data[os.write(2, data):]
    except OSError:
        pass


def _make_sigalrm_fatal() -> None:
    """Give SIGALRM its default action, which ends the process, and unblock it here.

    An ignored signal stays ignored across the exec that started this process, the
    blocked mask is inherited through it too, and a plugin may set a handler on import;
    any of them would turn :func:`_arm_hard_exit` into nothing. Called from the main
    thread — right after the stop signals are blocked, so every later thread inherits
    the mask, and again once the preparation of Airflow has returned.

    Both calls come before the process has decided to leave, so a real-time timer found
    armed is not the hard deadline but one a plugin set, and it is cancelled: under the
    default action it would end a watcher that runs as it should. The handover keeps
    SIGALRM blocked in this thread, and passes through ``SIG_IGN``, which discards a
    SIGALRM still pending, before ``SIG_DFL``: such a signal neither reaches a plugin's
    handler nor ends the process once unblocked.
    """
    signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
    signal.setitimer(signal.ITIMER_REAL, 0)
    signal.signal(signal.SIGALRM, signal.SIG_IGN)
    signal.signal(signal.SIGALRM, signal.SIG_DFL)
    signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGALRM})


def _ignore_group_signals() -> None:
    """Set the disposition of every signal in :data:`_IGNORED_SIGNALS` to ``SIG_IGN``.

    ``SIG_IGN`` runs no code of the process: the kernel discards the signal on arrival,
    whatever thread it would reach, so none stays pending either. A disposition belongs
    to the process, so it holds in every thread whatever its mask. It is kept across
    exec, so a program this process starts — a ``_cmd`` option of Airflow's
    configuration — begins with these signals ignored, and one that sets a handler of
    its own receives them as usual. Blocking them instead would carry them over exec
    blocked, where even such a handler never runs.
    """
    for signum in _IGNORED_SIGNALS:
        signal.signal(signum, signal.SIG_IGN)


def _arm_hard_exit() -> None:
    """Have the kernel end the process :data:`_HARD_EXIT_DEADLINE` seconds from now.

    Called first thing once the process has decided to leave — a stop signal taken, the
    scheduler's death noticed, the loop ended or failed, the setup raised — before any
    line is written or logged, from whichever thread made the decision: the real-time
    timer of :func:`signal.setitimer` is process-wide. SIGALRM is neither blocked nor
    handled (:func:`_make_sigalrm_fatal`), so its default action ends the process
    whatever its threads are blocked in. The listener in the scheduler sees such an
    exit as code -14.

    A timer already armed is left as it is: the deadline counts from the first decision
    to leave, and a later call does not touch it.
    """
    if signal.getitimer(signal.ITIMER_REAL)[0] > 0:
        return
    signal.setitimer(signal.ITIMER_REAL, _HARD_EXIT_DEADLINE)


def _set_parent_death_signal() -> str | None:
    """Ask the kernel to send this process SIGTERM once its parent thread exits.

    ``prctl(PR_SET_PDEATHSIG)``, which Linux alone has; on other systems nothing is
    requested and the parent checks alone — the watch thread's, the main thread's and
    the loop's — end the process once the scheduler is gone. The signal is tied to the
    thread that started this process, which is the scheduler's main thread. A request
    the kernel refuses, or a C library without ``prctl``, leaves the process with the
    parent checks alone.

    Returns the line that reports a failed request, or ``None``. The caller writes it
    once the watch thread runs (:func:`_prepare_under_watch`): a write to a stderr that
    blocks stalls the main thread, and the watch thread is what still ends the process
    on a stop or the scheduler's death.
    """
    if not sys.platform.startswith("linux"):
        return None
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        result = libc.prctl(_PR_SET_PDEATHSIG, int(signal.SIGTERM), 0, 0, 0)
    except (OSError, AttributeError) as exc:
        return f"cannot request the parent-death signal ({exc}) — relying on the parent checks"
    if result != 0:
        errno = ctypes.get_errno()
        return (
            f"prctl(PR_SET_PDEATHSIG) failed: {os.strerror(errno)} — relying on the parent "
            "checks"
        )
    return None


def _wait_for_stop_signal(timeout: float) -> int | None:
    """Wait up to ``timeout`` seconds for a blocked stop signal and take it.

    Returns the number of the signal taken, or ``None`` once the time is up. Where the
    platform has :func:`signal.sigtimedwait` the kernel does the waiting. Elsewhere the
    pending signals are looked at every :data:`_SIGNAL_PENDING_POLL` seconds, and one
    found there is taken with :func:`signal.sigwait`, which returns at once for a
    signal already pending. Either way the signal is consumed in ordinary code and no
    handler runs.
    """
    sigtimedwait = getattr(signal, "sigtimedwait", None)
    if sigtimedwait is not None:
        received = sigtimedwait(_STOP_SIGNALS, timeout)
        return None if received is None else received.si_signo
    deadline = time.monotonic() + timeout
    while not signal.sigpending() & _STOP_SIGNALS:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return None
        time.sleep(min(_SIGNAL_PENDING_POLL, remaining))
    return int(signal.sigwait(_STOP_SIGNALS))


def _prepare_airflow() -> None:
    """Bring the imported Airflow to the state the scheduler runs its jobs in.

    Runs in the main thread, after ``import airflow`` and before the loop thread
    exists:

    - the providers' configuration, which the ``airflow scheduler`` command loads
      before it starts, so an option a provider defines reads the same here;
    - direct access to the metadata database, which the scheduler forces on Airflow
      versions that read ``[core] database_access_isolation`` lazily;
    - the plugins and the classes they register (:data:`_PLUGIN_INITIALISERS`), which
      Airflow otherwise loads on first use from whichever pool worker deserializes a
      DAG first, with no lock against a second worker doing the same.
    """
    from airflow import plugins_manager
    from airflow.providers_manager import ProvidersManager

    ProvidersManager().initialize_providers_configuration()

    try:
        from airflow.api_internal.internal_api_call import InternalApiConfig
    except ImportError:
        pass
    else:
        force_direct_access = getattr(InternalApiConfig, "force_database_direct_access", None)
        if force_direct_access is not None:
            force_direct_access()

    for name in _PLUGIN_INITIALISERS:
        initialise = getattr(plugins_manager, name, None)
        if initialise is not None:
            initialise()


def _watch_preparation(parent: int, finished: threading.Event) -> None:
    """Wait for a stop signal and watch the parent until ``finished`` is set.

    Runs in the watch thread while the main thread prepares Airflow. After every wait
    that ends without a signal it compares its parent PID with ``parent``. A stop signal
    or a parent change ends the process at once, with code 0, through :func:`os._exit`:
    nothing that needs stopping has started, and the main thread may be anywhere in
    Airflow's import, holding any lock, logging's among them — so the line saying why
    goes out through :func:`_report`, and logging is not shut down. The hard deadline
    (:func:`_arm_hard_exit`) is armed before that line, which a stalled stderr could
    otherwise keep from ever being written.

    Returns once ``finished`` is set and the wait under way has ended, which leaves any
    later signal pending for the main thread. A signal that last wait takes ends the
    process like any other, so no signal is taken and then dropped.
    """
    while not finished.is_set():
        received = _wait_for_stop_signal(_SIGNAL_POLL_INTERVAL)
        if received is not None:
            _arm_hard_exit()
            _report(
                f"received {signal.Signals(received).name} while Airflow was being prepared "
                "— exiting"
            )
            os._exit(0)
        if os.getppid() != parent:
            _arm_hard_exit()
            _report(
                f"the scheduler process {parent} exited while Airflow was being prepared "
                "— exiting"
            )
            os._exit(0)


def _prepare_under_watch(
    parent: int, notice: str | None = None
) -> type[RMQWatcherListener]:
    """Import and prepare Airflow in the main thread while the watch thread waits for a stop.

    The main thread imports Airflow, runs :func:`_prepare_airflow` and imports the
    listener, as the scheduler loads Airflow and its plugins in its own main thread, so a
    plugin may call what Python allows there alone — :func:`signal.signal`,
    :func:`signal.set_wakeup_fd`. Meanwhile :func:`_watch_preparation` runs in the watch
    thread and ends the process on a stop signal or the scheduler's death. Returns the
    listener class once the preparation has returned and the watch thread has ended, so
    from then on the main thread alone takes the stop signals. An exception the
    preparation raises is raised here, once the watch thread has ended too. Either way
    SIGALRM is made fatal again first (:func:`_make_sigalrm_fatal`), in case a plugin
    set a handler or a timer for it.

    ``notice``, a line for stderr, is written through :func:`_report` once the watch
    thread runs, so a stderr that blocks the write leaves the process still ending on a
    stop signal or the scheduler's death.
    """
    finished = threading.Event()
    watch = threading.Thread(
        target=_watch_preparation, args=(parent, finished), name=_WATCH_THREAD_NAME,
        daemon=True,
    )
    watch.start()
    try:
        if notice is not None:
            _report(notice)
        # Importing Airflow configures its settings, the ORM and logging.
        import airflow  # noqa: F401

        _prepare_airflow()

        from airflow_provider_rmq.watcher.listener import RMQWatcherListener
    finally:
        finished.set()
        watch.join()
        _make_sigalrm_fatal()
    return RMQWatcherListener


def _shut_down_logging() -> None:
    """Flush and close the logging handlers, waiting :data:`_LOGGING_SHUTDOWN_TIMEOUT` at most.

    :func:`logging.shutdown` takes the lock of every handler in turn, and a thread the
    process leaves behind — the loop thread past its join timeout, a pool worker stuck
    in a call — may hold one for good, inside a handler whose write never returns. The
    shutdown runs in a daemon thread of its own, so such a lock costs this wait and not
    the exit, which nothing else would bring about once the scheduler has died. The
    handlers write every record out as they take it, so a shutdown cut short loses at
    most what one of them still buffers.
    """
    thread = threading.Thread(
        target=logging.shutdown, name=_LOGGING_SHUTDOWN_THREAD_NAME, daemon=True
    )
    thread.start()
    thread.join(timeout=_LOGGING_SHUTDOWN_TIMEOUT)
    if thread.is_alive():
        _report(
            f"logging did not shut down within {_LOGGING_SHUTDOWN_TIMEOUT:.0f}s — exiting "
            "without it"
        )


def _run_watcher(listener_class: type[RMQWatcherListener], parent: int) -> int:
    """Run the loop in its thread until a stop signal arrives, the scheduler dies or the
    loop ends by itself.

    The main thread logs nothing between the start of the loop thread and its wait, and
    on a stop it arms the hard deadline and asks the loop to stop before it logs why: a
    logging handler that blocks costs at most the deadline, never the stop itself.
    Returns the exit code: 0 after a stop, 1 when the loop raised out of its thread.
    """
    listener = listener_class()
    loop_failed = threading.Event()

    def run_loop() -> None:
        try:
            log.info(
                "RMQ Watcher process %d started for the scheduler process %d",
                os.getpid(), parent,
            )
            listener.run(parent)
        except BaseException:
            _arm_hard_exit()
            log.exception("RMQ Watcher loop failed — exiting")
            loop_failed.set()

    thread = threading.Thread(target=run_loop, name="rmq-watcher", daemon=True)
    thread.start()

    # The loop thread also ends on its own when the loop sees the parent change.
    while thread.is_alive():
        received = _wait_for_stop_signal(_SIGNAL_POLL_INTERVAL)
        if received is not None:
            _arm_hard_exit()
            listener.request_stop()
            log.info(
                "RMQ Watcher process received %s — stopping the watcher",
                signal.Signals(received).name,
            )
            break
        current = os.getppid()
        if current != parent:
            _arm_hard_exit()
            listener.request_stop()
            log.error(
                "RMQ Watcher process: the scheduler process %d has exited (parent is now "
                "%d) — stopping the watcher", parent, current,
            )
            break

    # The process leaves from here on, whichever way the loop ended.
    _arm_hard_exit()
    thread.join(timeout=_EXIT_JOIN_TIMEOUT)
    if thread.is_alive():
        # Not logged: the loop thread may be stuck holding a logging handler's lock.
        _report(
            f"the loop is still running {_EXIT_JOIN_TIMEOUT:.0f}s after the stop — exiting "
            "without it"
        )
        return 0
    if loop_failed.is_set():
        return 1
    _close_database_connections()
    return 0


def _close_database_connections() -> None:
    """Close the metadata-database connections the process holds idle.

    Airflow registers the same call as an at-exit hook, which :func:`os._exit` skips;
    closing them here ends each session on the database side cleanly. Called only
    once the loop thread has returned, so no call of the loop is using the engine. A
    connection still held by a worker stuck in a call is not in the pool and is left
    to the process exit. A close that never returns is ended by the hard deadline
    armed before it.
    """
    from airflow import settings

    try:
        settings.dispose_orm()
    except Exception:
        log.warning("RMQ Watcher process could not close its database connections", exc_info=True)


def main() -> None:
    """Run the watcher loop until a stop signal arrives or the scheduler exits.

    Never returns: the process ends through :func:`os._exit` with code 0 after a stop,
    the one that comes while Airflow is being prepared included, 1 when setting up or
    running the watcher raised, and 2 when the scheduler's PID is missing from the
    environment — or, when the way out blocks past :data:`_HARD_EXIT_DEADLINE`, by
    SIGALRM.
    """
    # First, before any import that could start a thread: every thread inherits the
    # mask of the thread that created it.
    signal.pthread_sigmask(signal.SIG_BLOCK, _STOP_SIGNALS)
    _ignore_group_signals()
    _make_sigalrm_fatal()

    try:
        parent = int(os.environ[PARENT_PID_ENV])
    except (KeyError, ValueError):
        _arm_hard_exit()
        _report(f"{PARENT_PID_ENV} does not hold the scheduler's PID — not starting the watcher")
        os._exit(2)

    notice = _set_parent_death_signal()
    # The parent-death signal only covers a parent that exits after the request above,
    # so a parent that is already gone is caught here.
    if os.getppid() != parent:
        _arm_hard_exit()
        _report(f"the scheduler process {parent} exited before the watcher started")
        os._exit(0)

    code = 1
    try:
        try:
            listener_class = _prepare_under_watch(parent, notice)
            code = _run_watcher(listener_class, parent)
        except BaseException:
            _arm_hard_exit()
            log.exception("RMQ Watcher process failed — exiting")
            code = 1
        _arm_hard_exit()
        _shut_down_logging()
    finally:
        os._exit(code)


if __name__ == "__main__":
    main()
