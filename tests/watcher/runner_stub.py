"""Test entry point for the watcher process: the real runner around a loop that does nothing.

``tests/watcher/test_runner_process.py`` starts this file as a separate interpreter —
as a script, or as the module ``runner_stub`` in place of the runner — and drives it
with real signals. It runs :func:`airflow_provider_rmq.watcher.runner.main` unchanged,
with these points of the process replaced:

- the listener's ``_main`` appends ``loop_started`` to the marker file, then prints
  ``RUNNER_STUB start`` with what the loop thread finds on arrival — the signals
  blocked in it, whether Airflow's plugins and the timetables they register are loaded,
  and the names of the plugins — and waits for the stop event, so the loop touches
  neither the database nor the broker. The marker comes first, so a test that signals
  the process on the ``start`` line always finds ``loop_started`` ahead of
  ``request_stop``. Once stopped the loop lingers a moment, the way the real loop stops
  its consumers;
- the listener's ``run`` appends ``loop_ended`` to the marker file once it returns;
- ``request_stop`` appends ``request_stop`` to the marker file before stopping the loop;
- ``os._exit`` appends ``exit <code>`` to the marker file and prints
  ``RUNNER_STUB exit <code>`` before the process ends;
- ``signal.pthread_sigmask`` prints ``RUNNER_STUB runner_mask airflow_imported=<bool>``
  on the runner's call, saying whether Airflow was imported by then;
- the runner's ``_prepare_airflow`` step prints ``RUNNER_STUB prepare`` with the name
  of the thread it runs in and the signals blocked there before it does its work.

The listener is patched from inside the runner's own ``_prepare_airflow`` step, once
the runner has imported Airflow, so nothing here imports Airflow ahead of the runner.

The stub turns core dumps off for itself, so a signal whose default action dumps core
leaves no file in the working directory.

Every line of stdout that belongs to the stub starts with ``RUNNER_STUB``; the rest is
Airflow's and the runner's logging. The marker file is named by
``RMQ_STUB_MARKER_FILE``.

Knobs, all read from the environment:

- ``RMQ_STUB_NO_PREMASK`` — leave the blocking of the stop signals to the runner alone
  instead of blocking them first thing here.
- ``RMQ_STUB_HOLD`` — after the ``masked`` marker, wait for a line on stdin before
  importing anything else, and report whether SIGTERM is pending once it arrives.
- ``RMQ_STUB_CALL_ON_STARTING`` — from inside the loop, send ``on_starting`` for a
  scheduler job through Airflow's listener manager, the way a plugin load in the
  watcher process would reach the listener.
- ``RMQ_STUB_HANG_PREPARE`` — make ``_prepare_airflow`` hang for good once Airflow is
  imported, the way a plugin that never finishes loading would, after printing
  ``RUNNER_STUB prepare hung``.
- ``RMQ_STUB_STALL_STDERR`` — at the start of ``_prepare_airflow``, point file
  descriptor 2 at a pipe that is full and never read, so every later write to stderr
  blocks for good; prints ``RUNNER_STUB stderr stalled``.
- ``RMQ_STUB_HANG_EXIT`` — make the runner's ``_close_database_connections`` hang for
  good after printing ``RUNNER_STUB close hung``, the way a database that never
  answers the close would.
- ``RMQ_STUB_HARD_EXIT_DEADLINE`` — whole seconds to use as the runner's
  ``_HARD_EXIT_DEADLINE``.
"""
# The signal mask comes first, before any import beyond these: every thread inherits
# the mask of the thread that starts it, and the runner blocks the same two signals as
# its own first step.
import os
import signal
import sys
from typing import NoReturn

_MASK = {signal.SIGTERM, signal.SIGINT}
if not os.environ.get("RMQ_STUB_NO_PREMASK"):
    signal.pthread_sigmask(signal.SIG_BLOCK, _MASK)
    print("RUNNER_STUB masked", flush=True)

import resource

resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

_MARKER_FILE = os.environ["RMQ_STUB_MARKER_FILE"]

#: Seconds the loop lingers after the stop event, standing in for the consumers' stop.
_LINGER_AFTER_STOP = 0.5


def _mark(line: str) -> None:
    with open(_MARKER_FILE, "a", encoding="utf-8") as marker:
        marker.write(line + "\n")
        marker.flush()
        os.fsync(marker.fileno())


if os.environ.get("RMQ_STUB_HOLD"):
    sys.stdin.readline()
    pending = signal.SIGTERM in signal.sigpending()
    print(f"RUNNER_STUB released pending_sigterm={pending}", flush=True)

import asyncio
import threading

from airflow_provider_rmq.watcher import runner

if set(runner._STOP_SIGNALS) != _MASK:
    print(f"RUNNER_STUB mask differs from the runner's: {runner._STOP_SIGNALS}", flush=True)
    os._exit(3)

if os.environ.get("RMQ_STUB_HARD_EXIT_DEADLINE"):
    runner._HARD_EXIT_DEADLINE = int(os.environ["RMQ_STUB_HARD_EXIT_DEADLINE"])

_real_exit = os._exit


def _exit(status: int) -> NoReturn:
    _mark(f"exit {status}")
    print(f"RUNNER_STUB exit {status}", flush=True)
    _real_exit(status)


os._exit = _exit

_real_sigmask = signal.pthread_sigmask


def _sigmask(how, mask):
    print(f"RUNNER_STUB runner_mask airflow_imported={'airflow' in sys.modules}", flush=True)
    signal.pthread_sigmask = _real_sigmask
    return _real_sigmask(how, mask)


signal.pthread_sigmask = _sigmask


class SchedulerJob:
    """Component whose class name makes the listener take it for the scheduler."""

    job_type = "SchedulerJob"


def _call_on_starting() -> None:
    from airflow.listeners.listener import get_listener_manager

    get_listener_manager().hook.on_starting(component=SchedulerJob())
    print("RUNNER_STUB on_starting done", flush=True)


def _patch_listener() -> None:
    from airflow import plugins_manager

    from airflow_provider_rmq.watcher.listener import RMQWatcherListener

    real_request_stop = RMQWatcherListener.request_stop
    real_run = RMQWatcherListener.run

    def request_stop(self) -> None:
        _mark("request_stop")
        real_request_stop(self)

    def run(self, parent_pid: int) -> None:
        real_run(self, parent_pid)
        _mark("loop_ended")

    async def main(self) -> None:
        wakeup = asyncio.Event()
        self._waker = (asyncio.get_running_loop(), wakeup)
        plugins_loaded = (
            plugins_manager.plugins is not None and plugins_manager.timetable_classes is not None
        )
        plugin_names = ",".join(sorted(str(p.name) for p in plugins_manager.plugins or []))
        _mark("loop_started")
        print(
            f"RUNNER_STUB start blocked={_blocked_signals()} plugins_loaded={plugins_loaded} "
            f"plugins={plugin_names}",
            flush=True,
        )
        if os.environ.get("RMQ_STUB_CALL_ON_STARTING"):
            _call_on_starting()
        while not self._stop_event.is_set():
            await wakeup.wait()
            wakeup.clear()
        await asyncio.sleep(_LINGER_AFTER_STOP)

    RMQWatcherListener.request_stop = request_stop  # type: ignore[method-assign]
    RMQWatcherListener.run = run  # type: ignore[method-assign]
    RMQWatcherListener._main = main  # type: ignore[method-assign]


_real_prepare_airflow = runner._prepare_airflow


def _blocked_signals() -> str:
    return ",".join(
        sorted(signal.Signals(sig).name for sig in signal.pthread_sigmask(signal.SIG_BLOCK, []))
    )


#: Read end of the stalled stderr pipe, kept open and never read.
_held_fds: list[int] = []


def _stall_stderr() -> None:
    read_end, write_end = os.pipe()
    os.set_blocking(write_end, False)
    # Large writes first, then single bytes: a pipe takes a write of up to a page whole
    # or not at all, so the last few free bytes only go in one at a time.
    for chunk in (b"x" * 4096, b"x"):
        try:
            while True:
                os.write(write_end, chunk)
        except BlockingIOError:
            pass
    os.set_blocking(write_end, True)
    os.dup2(write_end, 2)
    _held_fds.append(read_end)
    print("RUNNER_STUB stderr stalled", flush=True)


def _hang_closing() -> None:
    print("RUNNER_STUB close hung", flush=True)
    threading.Event().wait()


if os.environ.get("RMQ_STUB_HANG_EXIT"):
    runner._close_database_connections = _hang_closing


def _prepare_airflow() -> None:
    print(
        f"RUNNER_STUB prepare thread={threading.current_thread().name} "
        f"blocked={_blocked_signals()}",
        flush=True,
    )
    if os.environ.get("RMQ_STUB_STALL_STDERR"):
        _stall_stderr()
    if os.environ.get("RMQ_STUB_HANG_PREPARE"):
        print("RUNNER_STUB prepare hung", flush=True)
        threading.Event().wait()
    _real_prepare_airflow()
    _patch_listener()


runner._prepare_airflow = _prepare_airflow

runner.main()
