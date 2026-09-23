"""Unit tests for the watcher process entry point.

``main()`` runs here with every process-wide effect replaced: the signal mask, the
kernel's parent-death request, ``sigtimedwait``, ``logging.shutdown`` and ``os._exit``.
The pytest process keeps its signals, and ``os._exit`` raises :class:`_Exited` instead
of ending it, so a test reads the exit code off the exception.
"""
from __future__ import annotations

import errno
import logging
import os
import signal
import subprocess
import sys
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from airflow_provider_rmq.watcher import listener as listener_module
from airflow_provider_rmq.watcher import runner

_PARENT = 4242


class _Exited(Exception):
    """Raised by the replaced ``os._exit``; carries the exit code."""

    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


def _raise_exited(code: int) -> None:
    raise _Exited(code)


class _FakeListener:
    """Stand-in for ``RMQWatcherListener`` whose loop runs until it is asked to stop.

    :param stops_on_request: Whether :meth:`request_stop` ends :meth:`run`. A listener
        that ignores it models a loop stuck in a step; the test releases it through
        :attr:`release` once it is done.
    :param returns_at_once: Whether :meth:`run` returns without waiting, as the loop
        does once it sees the parent change.
    """

    def __init__(self, stops_on_request: bool = True, returns_at_once: bool = False) -> None:
        self.stops_on_request = stops_on_request
        self.returns_at_once = returns_at_once
        self.release = threading.Event()
        self.run_args: tuple | None = None
        self.run_thread_name: str | None = None
        self.stop_requests = 0

    def run(self, parent_pid: int | None = None) -> None:
        self.run_args = (parent_pid,)
        self.run_thread_name = threading.current_thread().name
        if self.returns_at_once:
            return
        self.release.wait(timeout=10)

    def request_stop(self) -> None:
        self.stop_requests += 1
        if self.stops_on_request:
            self.release.set()


def _signal_after(calls: int, signo: int = signal.SIGTERM):
    """A ``sigtimedwait`` replacement that times out ``calls`` times, then reports ``signo``."""
    state = {"calls": 0}

    def fake(signals, timeout):
        state["calls"] += 1
        if state["calls"] <= calls:
            threading.Event().wait(0.01)
            return None
        return SimpleNamespace(si_signo=signo)

    return fake


def _never_signalled(signals, timeout):
    threading.Event().wait(0.01)


@pytest.fixture
def process(monkeypatch):
    """Replace the process-wide effects of ``main()`` and return the mocks.

    The environment carries a valid parent PID and ``getppid`` agrees with it; tests
    change either to take another path.
    """
    mocks = SimpleNamespace(
        sigmask=MagicMock(name="pthread_sigmask"),
        pdeathsig=MagicMock(name="_set_parent_death_signal"),
        getppid=MagicMock(name="getppid", return_value=_PARENT),
        exit=MagicMock(name="_exit", side_effect=_raise_exited),
        shutdown=MagicMock(name="logging.shutdown"),
        sigtimedwait=MagicMock(name="sigtimedwait", side_effect=_signal_after(0)),
    )
    monkeypatch.setenv(runner.PARENT_PID_ENV, str(_PARENT))
    monkeypatch.setattr(runner.signal, "pthread_sigmask", mocks.sigmask)
    monkeypatch.setattr(runner.signal, "sigtimedwait", mocks.sigtimedwait)
    monkeypatch.setattr(runner, "_set_parent_death_signal", mocks.pdeathsig)
    monkeypatch.setattr(runner.os, "getppid", mocks.getppid)
    monkeypatch.setattr(runner.os, "_exit", mocks.exit)
    monkeypatch.setattr(runner.logging, "shutdown", mocks.shutdown)
    return mocks


def _run_main() -> int:
    with pytest.raises(_Exited) as info:
        runner.main()
    return info.value.code


def _use_listener(monkeypatch, fake) -> MagicMock:
    factory = MagicMock(name="RMQWatcherListener", return_value=fake)
    monkeypatch.setattr(listener_module, "RMQWatcherListener", factory)
    return factory


# ---------------------------------------------------------------------------
# Module surface
# ---------------------------------------------------------------------------


class TestModuleSurface:
    def test_environment_names(self):
        assert runner.CHILD_ENV_MARKER == "AIRFLOW_PROVIDER_RMQ_WATCHER_CHILD"
        assert runner.PARENT_PID_ENV == "AIRFLOW_PROVIDER_RMQ_WATCHER_PARENT_PID"

    def test_exit_join_outlasts_the_manager_stop_budget(self):
        assert runner._EXIT_JOIN_TIMEOUT > listener_module._STOP_TIMEOUT

    def test_module_import_loads_neither_airflow_nor_the_listener(self):
        """The signal mask has to be in place before anything that can start a thread
        is imported, so importing the module itself must stay within the stdlib."""
        code = (
            "import sys\n"
            "import airflow_provider_rmq.watcher.runner\n"
            "print('airflow' in sys.modules, "
            "'airflow_provider_rmq.watcher.listener' in sys.modules)\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=60, check=True
        )
        assert result.stdout.split() == ["False", "False"]


# ---------------------------------------------------------------------------
# _set_parent_death_signal
# ---------------------------------------------------------------------------


class TestSetParentDeathSignal:
    def test_requests_sigterm_through_prctl(self, monkeypatch):
        monkeypatch.setattr(runner.sys, "platform", "linux")
        libc = MagicMock()
        libc.prctl.return_value = 0
        cdll = MagicMock(return_value=libc)
        monkeypatch.setattr(runner.ctypes, "CDLL", cdll)

        runner._set_parent_death_signal()

        cdll.assert_called_once_with(None, use_errno=True)
        libc.prctl.assert_called_once_with(1, int(signal.SIGTERM), 0, 0, 0)

    def test_refused_request_is_reported_without_raising(self, monkeypatch, capsys):
        monkeypatch.setattr(runner.sys, "platform", "linux")
        libc = MagicMock()
        libc.prctl.return_value = -1
        monkeypatch.setattr(runner.ctypes, "CDLL", MagicMock(return_value=libc))
        monkeypatch.setattr(runner.ctypes, "get_errno", MagicMock(return_value=errno.EINVAL))

        runner._set_parent_death_signal()

        err = capsys.readouterr().err
        assert "PR_SET_PDEATHSIG" in err
        assert "Invalid argument" in err

    def test_unloadable_libc_is_reported_without_raising(self, monkeypatch, capsys):
        monkeypatch.setattr(runner.sys, "platform", "linux")
        monkeypatch.setattr(runner.ctypes, "CDLL", MagicMock(side_effect=OSError("no libc")))

        runner._set_parent_death_signal()

        assert "no libc" in capsys.readouterr().err

    def test_does_nothing_off_linux(self, monkeypatch):
        monkeypatch.setattr(runner.sys, "platform", "darwin")
        cdll = MagicMock()
        monkeypatch.setattr(runner.ctypes, "CDLL", cdll)

        runner._set_parent_death_signal()

        cdll.assert_not_called()


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMainStartup:
    def test_signals_are_blocked_before_anything_else(self, process, monkeypatch):
        fake = _FakeListener()
        factory = _use_listener(monkeypatch, fake)
        order = MagicMock()
        order.attach_mock(process.sigmask, "sigmask")
        order.attach_mock(process.pdeathsig, "pdeathsig")
        order.attach_mock(process.getppid, "getppid")
        order.attach_mock(factory, "listener")

        assert _run_main() == 0

        names = [c[0] for c in order.mock_calls]
        assert names[0] == "sigmask"
        assert names.index("sigmask") < names.index("pdeathsig") < names.index("getppid")
        assert names.index("getppid") < names.index("listener")
        process.sigmask.assert_called_once_with(
            signal.SIG_BLOCK, frozenset({signal.SIGTERM, signal.SIGINT})
        )

    def test_missing_parent_pid_exits_with_code_2(self, process, monkeypatch, capsys):
        monkeypatch.delenv(runner.PARENT_PID_ENV)
        factory = _use_listener(monkeypatch, _FakeListener())

        assert _run_main() == 2

        process.sigmask.assert_called_once()
        process.pdeathsig.assert_not_called()
        factory.assert_not_called()
        assert runner.PARENT_PID_ENV in capsys.readouterr().err

    def test_malformed_parent_pid_exits_with_code_2(self, process, monkeypatch):
        monkeypatch.setenv(runner.PARENT_PID_ENV, "not-a-pid")
        factory = _use_listener(monkeypatch, _FakeListener())

        assert _run_main() == 2

        process.pdeathsig.assert_not_called()
        factory.assert_not_called()

    def test_parent_gone_before_setup_exits_without_starting_the_loop(
        self, process, monkeypatch, capsys
    ):
        process.getppid.return_value = 1
        factory = _use_listener(monkeypatch, _FakeListener())

        assert _run_main() == 0

        process.pdeathsig.assert_called_once()
        factory.assert_not_called()
        process.sigtimedwait.assert_not_called()
        assert str(_PARENT) in capsys.readouterr().err

    def test_initialisation_failure_exits_with_code_1(self, process, monkeypatch, caplog):
        factory = MagicMock(side_effect=RuntimeError("metadata database unreachable"))
        monkeypatch.setattr(listener_module, "RMQWatcherListener", factory)

        with caplog.at_level(logging.ERROR, logger=runner.__name__):
            assert _run_main() == 1

        process.shutdown.assert_called_once()
        assert any(
            r.levelno == logging.ERROR and r.exc_info and "metadata database unreachable"
            in str(r.exc_info[1])
            for r in caplog.records
        )


class TestMainLoop:
    def test_signal_requests_stop_and_joins_the_loop(self, process, monkeypatch, caplog):
        fake = _FakeListener()
        _use_listener(monkeypatch, fake)
        process.sigtimedwait.side_effect = _signal_after(2)

        with caplog.at_level(logging.INFO, logger=runner.__name__):
            assert _run_main() == 0

        assert fake.run_args == (_PARENT,)
        assert fake.run_thread_name == "rmq-watcher"
        assert fake.stop_requests == 1
        assert process.sigtimedwait.call_count == 3
        waited_for, timeout = process.sigtimedwait.call_args.args
        assert set(waited_for) == {signal.SIGTERM, signal.SIGINT}
        assert timeout == runner._SIGNAL_POLL_INTERVAL
        process.shutdown.assert_called_once()
        assert any("received SIGTERM" in r.getMessage() for r in caplog.records)

    def test_sigint_stops_the_loop_too(self, process, monkeypatch):
        fake = _FakeListener()
        _use_listener(monkeypatch, fake)
        process.sigtimedwait.side_effect = _signal_after(0, signal.SIGINT)

        assert _run_main() == 0

        assert fake.stop_requests == 1

    def test_loop_ending_on_its_own_exits_without_a_signal(self, process, monkeypatch):
        fake = _FakeListener(returns_at_once=True)
        _use_listener(monkeypatch, fake)
        process.sigtimedwait.side_effect = _never_signalled

        assert _run_main() == 0

        assert fake.run_args == (_PARENT,)
        assert fake.stop_requests == 0
        process.shutdown.assert_called_once()

    def test_loop_outliving_the_join_is_left_behind(self, process, monkeypatch, caplog):
        fake = _FakeListener(stops_on_request=False)
        _use_listener(monkeypatch, fake)
        monkeypatch.setattr(runner, "_EXIT_JOIN_TIMEOUT", 0.05)
        try:
            with caplog.at_level(logging.WARNING, logger=runner.__name__):
                assert _run_main() == 0
        finally:
            fake.release.set()

        assert fake.stop_requests == 1
        assert any(
            r.levelno == logging.WARNING and "still running" in r.getMessage()
            for r in caplog.records
        )
        process.shutdown.assert_called_once()

    def test_failure_while_waiting_exits_with_code_1(self, process, monkeypatch):
        fake = _FakeListener()
        _use_listener(monkeypatch, fake)
        process.sigtimedwait.side_effect = OSError("sigtimedwait failed")
        try:
            assert _run_main() == 1
        finally:
            fake.release.set()

        process.shutdown.assert_called_once()


def test_running_as_main_module_calls_main():
    """``python -m airflow_provider_rmq.watcher.runner`` enters :func:`main`.

    Without the scheduler's PID in the environment ``main()`` leaves with code 2 before
    importing Airflow, which is enough to show the module runs it.
    """
    env = {k: v for k, v in os.environ.items() if k != runner.PARENT_PID_ENV}
    result = subprocess.run(
        [sys.executable, "-m", "airflow_provider_rmq.watcher.runner"],
        env=env, capture_output=True, text=True, timeout=60, check=False,
    )
    assert result.returncode == 2
    assert runner.PARENT_PID_ENV in result.stderr
