"""Test entry point for the watcher process: the real runner around a loop that does nothing.

``tests/watcher/test_runner_process.py`` starts this file as a separate interpreter and
drives it with real signals. It runs :func:`airflow_provider_rmq.watcher.runner.main`
unchanged, with three points of the process replaced:

- the listener's ``_main`` prints ``RUNNER_STUB start`` and waits for the stop event,
  so the loop touches neither the database nor the broker;
- ``request_stop`` appends ``request_stop`` to the marker file before stopping the loop;
- ``os._exit`` appends ``exit <code>`` to the marker file and prints
  ``RUNNER_STUB exit <code>`` before the process ends.

Every line of stdout that belongs to the stub starts with ``RUNNER_STUB``; the rest is
Airflow's and the runner's logging. The marker file is named by
``RMQ_STUB_MARKER_FILE``.

Knobs, all read from the environment:

- ``RMQ_STUB_HOLD`` — after the ``masked`` marker, wait for a line on stdin before
  importing anything else, and report whether SIGTERM is pending once it arrives.
- ``RMQ_STUB_CALL_ON_STARTING`` — from inside the loop, send ``on_starting`` for a
  scheduler job through Airflow's listener manager, the way a plugin load in the
  watcher process would reach the listener, and report whether the plugins were loaded
  before the manager was asked for and how many watcher listeners it holds.
- ``RMQ_STUB_LOG_POPEN`` — append ``popen <argv>`` to the marker file for every
  :class:`subprocess.Popen` the process creates.
"""
# The signal mask comes first, before any import beyond these: every thread inherits
# the mask of the thread that starts it, and the runner blocks the same two signals as
# its own first step.
import os
import signal
import sys
from typing import NoReturn

_MASK = {signal.SIGTERM, signal.SIGINT}
signal.pthread_sigmask(signal.SIG_BLOCK, _MASK)
print("RUNNER_STUB masked", flush=True)

_MARKER_FILE = os.environ["RMQ_STUB_MARKER_FILE"]


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
import subprocess

from airflow_provider_rmq.watcher import runner

if set(runner._STOP_SIGNALS) != _MASK:
    print(f"RUNNER_STUB mask differs from the runner's: {runner._STOP_SIGNALS}", flush=True)
    os._exit(3)

if os.environ.get("RMQ_STUB_LOG_POPEN"):
    _real_popen_init = subprocess.Popen.__init__

    def _logging_popen_init(self, args, *rest, **kwargs):
        _mark(f"popen {args!r}")
        _real_popen_init(self, args, *rest, **kwargs)

    subprocess.Popen.__init__ = _logging_popen_init  # type: ignore[method-assign]

_real_exit = os._exit


def _exit(status: int) -> NoReturn:
    _mark(f"exit {status}")
    print(f"RUNNER_STUB exit {status}", flush=True)
    _real_exit(status)


os._exit = _exit

# Airflow before the listener, in the order the runner imports them: with
# ``core.lazy_load_plugins`` off, importing Airflow loads the plugin, which imports the
# listener module in turn, and a listener import that started first would still be
# half done at that point.
import airflow  # noqa: F401

from airflow_provider_rmq.watcher.listener import RMQWatcherListener

_real_request_stop = RMQWatcherListener.request_stop


def _request_stop(self) -> None:
    _mark("request_stop")
    _real_request_stop(self)


class SchedulerJob:
    """Component whose class name makes the listener take it for the scheduler."""

    job_type = "SchedulerJob"


def _call_on_starting() -> None:
    from airflow import plugins_manager
    from airflow.listeners.listener import get_listener_manager

    plugins_loaded = plugins_manager.plugins is not None
    manager = get_listener_manager()
    watchers = sum(
        isinstance(plugin, RMQWatcherListener) for plugin in manager.pm.get_plugins()
    )
    manager.hook.on_starting(component=SchedulerJob())
    print(
        f"RUNNER_STUB on_starting done plugins_loaded_before={plugins_loaded} "
        f"watcher_listeners={watchers}",
        flush=True,
    )


async def _main(self) -> None:
    wakeup = asyncio.Event()
    self._waker = (asyncio.get_running_loop(), wakeup)
    print("RUNNER_STUB start", flush=True)
    if os.environ.get("RMQ_STUB_CALL_ON_STARTING"):
        _call_on_starting()
    while not self._stop_event.is_set():
        await wakeup.wait()
        wakeup.clear()


RMQWatcherListener.request_stop = _request_stop  # type: ignore[method-assign]
RMQWatcherListener._main = _main  # type: ignore[method-assign]

runner.main()
