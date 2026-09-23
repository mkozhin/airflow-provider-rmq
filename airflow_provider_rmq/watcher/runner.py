"""Entry point of the watcher process: ``python -m airflow_provider_rmq.watcher.runner``.

The scheduler starts this module as a child process of its own, and the watcher loop
runs here, in a fresh interpreter that shares no threads, locks or sockets with the
scheduler. Airflow forks the scheduler process — the Celery send pool and the DAG
processor manager — and a fork is only safe from a process with a single thread, so
the loop and its pools live in this process and the scheduler keeps none of them.

The process is shaped around one rule: a signal never runs Python code. SIGTERM and
SIGINT are blocked before anything else happens, the mask is inherited by every thread
started after it, and the main thread collects them with :func:`signal.sigtimedwait`
and asks the loop to stop in ordinary code. A handler could interrupt a thread that
holds a lock — a :class:`threading.Event`, the buffer of stdout — and deadlock on it;
a blocked signal cannot, and one that arrives before the loop is running stays pending
in the kernel until the main thread asks for it, so it is never lost.

The scheduler hands its own PID over in the environment, so the value is fixed before
this process exists and cannot race the scheduler's exit. The kernel is asked to
deliver SIGTERM once the scheduler thread that started this process exits, and the
parent PID is compared with the handed-over one right after that request, which
catches a scheduler that exited before it was made. The loop repeats the comparison
before every pass.

The process leaves through :func:`os._exit`, after a bounded wait for the loop thread:
the interpreter's own exit joins every pool worker with no timeout, and a call stuck on
the database in one of them would keep the process alive for as long as the operating
system keeps its socket open.

Only the standard library is imported at module level. Airflow and the listener are
imported inside :func:`main`, after the signal mask is in place, so the threads Airflow
starts on import already have it.
"""
from __future__ import annotations

import ctypes
import logging
import os
import signal
import sys
import threading

log = logging.getLogger(__name__)

#: Environment variable set in the watcher process. A listener that finds it does not
#: start another process: the watcher runs in this one already.
CHILD_ENV_MARKER = "AIRFLOW_PROVIDER_RMQ_WATCHER_CHILD"

#: Environment variable carrying the PID of the scheduler process that started the
#: watcher, as a decimal string.
PARENT_PID_ENV = "AIRFLOW_PROVIDER_RMQ_WATCHER_PARENT_PID"

#: Seconds the main thread waits for the loop thread once the loop has been asked to
#: stop. The loop finishes the step it is on and then stops the consumer manager, which
#: is bounded by the listener's ``_STOP_TIMEOUT`` of 30 seconds, so this leaves the
#: manager all of its budget and some room for the teardown around it. A loop thread
#: still running after it is left behind by the process exit.
_EXIT_JOIN_TIMEOUT = 45.0

#: Signals that stop the watcher. Blocked in every thread of the process and collected
#: by the main thread alone.
_STOP_SIGNALS = frozenset({signal.SIGTERM, signal.SIGINT})

#: Seconds one :func:`signal.sigtimedwait` call waits before the main thread checks
#: whether the loop thread is still running.
_SIGNAL_POLL_INTERVAL = 1.0

#: ``PR_SET_PDEATHSIG`` from ``<linux/prctl.h>``.
_PR_SET_PDEATHSIG = 1


def _report(message: str) -> None:
    """Write a line to stderr, for the steps that run before Airflow configures logging.

    stderr is the scheduler's, inherited by this process, so the line lands in the same
    container log as everything the watcher logs later.
    """
    print(f"RMQ Watcher process {os.getpid()}: {message}", file=sys.stderr, flush=True)


def _set_parent_death_signal() -> None:
    """Ask the kernel to send this process SIGTERM once its parent thread exits.

    Linux only: ``prctl(PR_SET_PDEATHSIG)`` has no counterpart on other systems, and
    there the call does nothing — the loop's own parent check before every pass still
    stops the watcher. The signal is tied to the thread that started this process, which
    is the scheduler's main thread. A request the kernel refuses is reported and the
    process carries on with the loop's parent check alone.
    """
    if not sys.platform.startswith("linux"):
        return
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        result = libc.prctl(_PR_SET_PDEATHSIG, int(signal.SIGTERM), 0, 0, 0)
    except (OSError, AttributeError) as exc:
        _report(f"cannot request the parent-death signal ({exc}) — relying on the loop's parent check")
        return
    if result != 0:
        errno = ctypes.get_errno()
        _report(
            f"prctl(PR_SET_PDEATHSIG) failed: {os.strerror(errno)} — relying on the loop's "
            "parent check"
        )


def main() -> None:
    """Run the watcher loop until a stop signal arrives or the scheduler exits.

    Never returns: the process ends through :func:`os._exit` with code 0 after a stop,
    1 when setting up or running the watcher raised, and 2 when the scheduler's PID is
    missing from the environment.
    """
    # First, before any import that could start a thread: every thread inherits the
    # mask of the thread that created it.
    signal.pthread_sigmask(signal.SIG_BLOCK, _STOP_SIGNALS)

    try:
        parent = int(os.environ[PARENT_PID_ENV])
    except (KeyError, ValueError):
        _report(f"{PARENT_PID_ENV} does not hold the scheduler's PID — not starting the watcher")
        os._exit(2)

    _set_parent_death_signal()
    # The parent-death signal only covers a parent that exits after the request above,
    # so a parent that is already gone is caught here.
    if os.getppid() != parent:
        _report(f"the scheduler process {parent} exited before the watcher started")
        os._exit(0)

    try:
        # Importing Airflow configures its settings, the ORM and logging.
        import airflow  # noqa: F401

        from airflow_provider_rmq.watcher.listener import RMQWatcherListener

        log.info("RMQ Watcher process %d started for the scheduler process %d", os.getpid(), parent)
        listener = RMQWatcherListener()
        thread = threading.Thread(
            target=listener.run, args=(parent,), name="rmq-watcher", daemon=True
        )
        thread.start()

        # The loop thread ends on its own when the loop sees the parent change.
        while thread.is_alive():
            received = signal.sigtimedwait(_STOP_SIGNALS, _SIGNAL_POLL_INTERVAL)
            if received is not None:
                log.info(
                    "RMQ Watcher process received %s — stopping the watcher",
                    signal.Signals(received.si_signo).name,
                )
                listener.request_stop()
                break

        thread.join(timeout=_EXIT_JOIN_TIMEOUT)
        if thread.is_alive():
            log.warning(
                "RMQ Watcher loop still running %.0fs after the stop — exiting without it",
                _EXIT_JOIN_TIMEOUT,
            )
    except BaseException:
        log.exception("RMQ Watcher process failed — exiting")
        logging.shutdown()
        os._exit(1)

    logging.shutdown()
    os._exit(0)


if __name__ == "__main__":
    main()
