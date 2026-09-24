"""Unit tests for the watcher process entry point.

``main()`` runs here with every process-wide effect replaced: the signal mask, the
signals ignored, the disposition of SIGALRM, the kernel's parent-death request, the preparation of Airflow,
the signal wait, the closing of the database connections, ``logging.shutdown`` and
``os._exit``; the conftest replaces the real-time timer of ``signal.setitimer`` for
every test. The pytest
process keeps its signals, its plugins and its ORM, and ``os._exit`` raises
:class:`_Exited` instead of ending it, so a test reads the exit code off the exception.

The process waits for a stop signal twice: in the watch thread while the main thread
prepares Airflow, and in the main thread once the loop runs. The replaced signal waits
of the loop tests report a signal only in the second, which begins once ``main()`` has
created the listener.
"""
from __future__ import annotations

import builtins
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
    :param raises: Exception :meth:`run` raises instead of returning.

    :attr:`returned` is set once :meth:`run` has finished, whichever way it ended.
    """

    def __init__(
        self,
        stops_on_request: bool = True,
        returns_at_once: bool = False,
        raises: BaseException | None = None,
    ) -> None:
        self.stops_on_request = stops_on_request
        self.returns_at_once = returns_at_once
        self.raises = raises
        self.release = threading.Event()
        self.returned = threading.Event()
        self.run_args: tuple | None = None
        self.run_thread_name: str | None = None
        self.stop_requests = 0

    def run(self, parent_pid: int) -> None:
        self.run_args = (parent_pid,)
        self.run_thread_name = threading.current_thread().name
        try:
            if self.raises is not None:
                raise self.raises
            if self.returns_at_once:
                return
            self.release.wait(timeout=10)
            # A little work after the stop, the way the loop stops the consumers:
            # a main thread that did not wait for it would exit first.
            threading.Event().wait(0.05)
        finally:
            self.returned.set()

    def request_stop(self) -> None:
        self.stop_requests += 1
        if self.stops_on_request:
            self.release.set()


def _loop_phase() -> bool:
    """Whether ``main()`` has left the preparation: it creates the listener right after.

    The main thread creates the listener only once the watch thread has ended, so the
    answer never changes under a wait of the watch thread.
    """
    factory = listener_module.RMQWatcherListener
    return isinstance(factory, MagicMock) and factory.called


class _SignalAfter:
    """A ``sigtimedwait`` replacement that times out while ``main()`` prepares Airflow
    and ``calls`` times after that, then reports ``signo``.

    :attr:`loop_calls` counts the calls made once the loop runs.
    """

    def __init__(self, calls: int, signo: int = signal.SIGTERM) -> None:
        self.calls = calls
        self.signo = signo
        self.loop_calls = 0

    def __call__(self, signals, timeout):
        if _loop_phase():
            self.loop_calls += 1
            if self.loop_calls > self.calls:
                return SimpleNamespace(si_signo=self.signo)
        threading.Event().wait(0.01)
        return None


def _never_signalled(signals, timeout):
    threading.Event().wait(0.01)


def _failing_in_the_loop(error: BaseException):
    """A ``sigtimedwait`` replacement that times out while ``main()`` prepares Airflow
    and raises ``error`` once the loop runs."""

    def sigtimedwait(signals, timeout):
        if _loop_phase():
            raise error
        threading.Event().wait(0.01)

    return sigtimedwait


@pytest.fixture
def process(monkeypatch):
    """Replace the process-wide effects of ``main()`` and return the mocks.

    The environment carries a valid parent PID and ``getppid`` agrees with it; tests
    change either to take another path.
    """
    mocks = SimpleNamespace(
        sigmask=MagicMock(name="pthread_sigmask"),
        ignore=MagicMock(name="_ignore_group_signals"),
        sigalrm=MagicMock(name="_make_sigalrm_fatal"),
        pdeathsig=MagicMock(name="_set_parent_death_signal", return_value=None),
        prepare=MagicMock(name="_prepare_airflow"),
        close_db=MagicMock(name="_close_database_connections"),
        getppid=MagicMock(name="getppid", return_value=_PARENT),
        exit=MagicMock(name="_exit", side_effect=_raise_exited),
        shutdown=MagicMock(name="logging.shutdown"),
        sigtimedwait=MagicMock(name="sigtimedwait", side_effect=_SignalAfter(0)),
    )
    monkeypatch.setenv(runner.PARENT_PID_ENV, str(_PARENT))
    monkeypatch.setattr(runner.signal, "pthread_sigmask", mocks.sigmask)
    monkeypatch.setattr(runner.signal, "sigtimedwait", mocks.sigtimedwait, raising=False)
    monkeypatch.setattr(runner, "_ignore_group_signals", mocks.ignore)
    monkeypatch.setattr(runner, "_make_sigalrm_fatal", mocks.sigalrm)
    monkeypatch.setattr(runner, "_set_parent_death_signal", mocks.pdeathsig)
    monkeypatch.setattr(runner, "_prepare_airflow", mocks.prepare)
    monkeypatch.setattr(runner, "_close_database_connections", mocks.close_db)
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
        assert runner.PACKAGE_DIR_ENV == "AIRFLOW_PROVIDER_RMQ_WATCHER_PACKAGE_DIR"

    def test_the_logger_is_the_package_one_under_python_m_too(self):
        assert runner.log.name == "airflow_provider_rmq.watcher.runner"

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
    @pytest.fixture(autouse=True)
    def _on_linux(self, monkeypatch):
        monkeypatch.setattr(runner.sys, "platform", "linux")

    def test_off_linux_nothing_is_requested(self, monkeypatch, capfd):
        monkeypatch.setattr(runner.sys, "platform", "darwin")
        cdll = MagicMock(name="CDLL")
        monkeypatch.setattr(runner.ctypes, "CDLL", cdll)

        assert runner._set_parent_death_signal() is None

        cdll.assert_not_called()
        assert capfd.readouterr().err == ""

    def test_requests_sigterm_through_prctl(self, monkeypatch, capfd):
        libc = MagicMock()
        libc.prctl.return_value = 0
        cdll = MagicMock(return_value=libc)
        monkeypatch.setattr(runner.ctypes, "CDLL", cdll)

        assert runner._set_parent_death_signal() is None

        cdll.assert_called_once_with(None, use_errno=True)
        libc.prctl.assert_called_once_with(1, int(signal.SIGTERM), 0, 0, 0)
        assert capfd.readouterr().err == ""

    def test_refused_request_is_returned_without_being_written(self, monkeypatch, capfd):
        libc = MagicMock()
        libc.prctl.return_value = -1
        monkeypatch.setattr(runner.ctypes, "CDLL", MagicMock(return_value=libc))
        monkeypatch.setattr(runner.ctypes, "get_errno", MagicMock(return_value=errno.EINVAL))

        notice = runner._set_parent_death_signal()

        assert notice is not None
        assert "PR_SET_PDEATHSIG" in notice
        assert "Invalid argument" in notice
        assert "relying on the parent checks" in notice
        assert capfd.readouterr().err == ""

    def test_unloadable_libc_is_returned_without_being_written(self, monkeypatch, capfd):
        monkeypatch.setattr(runner.ctypes, "CDLL", MagicMock(side_effect=OSError("no libc")))

        notice = runner._set_parent_death_signal()

        assert notice is not None and "no libc" in notice
        assert capfd.readouterr().err == ""

    def test_libc_without_prctl_is_returned_without_being_written(self, monkeypatch, capfd):
        monkeypatch.setattr(runner.ctypes, "CDLL", MagicMock(return_value=MagicMock(spec=[])))

        notice = runner._set_parent_death_signal()

        assert notice is not None
        assert "cannot request the parent-death signal" in notice
        assert "prctl" in notice
        assert capfd.readouterr().err == ""


# ---------------------------------------------------------------------------
# _wait_for_stop_signal
# ---------------------------------------------------------------------------


class _Clock:
    """``time.monotonic`` and ``time.sleep`` replacements: a sleep moves the clock."""

    def __init__(self) -> None:
        self.now = 100.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class TestWaitForStopSignal:
    def test_sigtimedwait_takes_the_signal_where_the_platform_has_it(self, monkeypatch):
        sigtimedwait = MagicMock(return_value=SimpleNamespace(si_signo=signal.SIGINT))
        monkeypatch.setattr(runner.signal, "sigtimedwait", sigtimedwait, raising=False)

        assert runner._wait_for_stop_signal(1.0) == signal.SIGINT

        sigtimedwait.assert_called_once_with(runner._STOP_SIGNALS, 1.0)

    def test_sigtimedwait_timing_out_returns_none(self, monkeypatch):
        monkeypatch.setattr(
            runner.signal, "sigtimedwait", MagicMock(return_value=None), raising=False
        )

        assert runner._wait_for_stop_signal(1.0) is None

    @pytest.fixture
    def without_sigtimedwait(self, monkeypatch):
        """A platform like macOS: no ``sigtimedwait``, a mocked clock, and the
        pending set and ``sigwait`` replaced."""
        monkeypatch.delattr(runner.signal, "sigtimedwait", raising=False)
        clock = _Clock()
        monkeypatch.setattr(runner.time, "monotonic", clock.monotonic)
        monkeypatch.setattr(runner.time, "sleep", clock.sleep)
        mocks = SimpleNamespace(
            clock=clock,
            sigpending=MagicMock(name="sigpending", return_value=set()),
            sigwait=MagicMock(name="sigwait", return_value=signal.SIGTERM),
        )
        monkeypatch.setattr(runner.signal, "sigpending", mocks.sigpending)
        monkeypatch.setattr(runner.signal, "sigwait", mocks.sigwait)
        return mocks

    def test_a_pending_signal_is_taken_with_sigwait(self, without_sigtimedwait):
        mocks = without_sigtimedwait
        mocks.sigpending.side_effect = [set(), set(), {signal.SIGTERM}]

        assert runner._wait_for_stop_signal(1.0) == signal.SIGTERM

        mocks.sigwait.assert_called_once_with(runner._STOP_SIGNALS)
        assert mocks.clock.sleeps == [runner._SIGNAL_PENDING_POLL] * 2

    def test_a_signal_already_pending_is_taken_without_sleeping(self, without_sigtimedwait):
        mocks = without_sigtimedwait
        mocks.sigpending.return_value = {signal.SIGINT}
        mocks.sigwait.return_value = signal.SIGINT

        assert runner._wait_for_stop_signal(1.0) == signal.SIGINT

        assert mocks.clock.sleeps == []

    def test_nothing_pending_returns_none_once_the_time_is_up(self, without_sigtimedwait):
        mocks = without_sigtimedwait

        assert runner._wait_for_stop_signal(0.25) is None

        mocks.sigwait.assert_not_called()
        assert sum(mocks.clock.sleeps) == pytest.approx(0.25)
        assert max(mocks.clock.sleeps) <= runner._SIGNAL_PENDING_POLL

    def test_a_pending_signal_other_than_the_stop_ones_is_left_alone(
        self, without_sigtimedwait
    ):
        mocks = without_sigtimedwait
        mocks.sigpending.return_value = {signal.SIGUSR1}

        assert runner._wait_for_stop_signal(0.25) is None

        mocks.sigwait.assert_not_called()

    def test_real_signals_without_sigtimedwait(self):
        """The polling path against the kernel: a SIGTERM the process sends itself
        while the signal is blocked is found pending and consumed by ``sigwait``, and
        no handler runs — the default action would end the process."""
        code = (
            "import os, signal\n"
            "from airflow_provider_rmq.watcher import runner\n"
            "if hasattr(signal, 'sigtimedwait'):\n"
            "    del signal.sigtimedwait\n"
            "signal.pthread_sigmask(signal.SIG_BLOCK, runner._STOP_SIGNALS)\n"
            "print(runner._wait_for_stop_signal(0.2))\n"
            "os.kill(os.getpid(), signal.SIGTERM)\n"
            "print(int(runner._wait_for_stop_signal(5.0)))\n"
            "print(signal.SIGTERM in signal.sigpending())\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.split() == ["None", str(int(signal.SIGTERM)), "False"]


# ---------------------------------------------------------------------------
# _prepare_airflow and _close_database_connections
# ---------------------------------------------------------------------------


class TestIgnoreGroupSignals:
    def test_the_signals_the_scheduler_takes_without_stopping_are_ignored(self):
        """SIGUSR2 and SIGQUIT are the ones the scheduler handles and runs on; SIGHUP
        and SIGUSR1 reach the process group the same way. None of them is a stop signal
        or SIGALRM, whose dispositions are the runner's other steps."""
        assert set(runner._IGNORED_SIGNALS) == {
            signal.SIGHUP, signal.SIGQUIT, signal.SIGUSR1, signal.SIGUSR2
        }
        assert not set(runner._IGNORED_SIGNALS) & (runner._STOP_SIGNALS | {signal.SIGALRM})

    def test_each_signal_sent_afterwards_leaves_the_process_running(self):
        """Against the kernel: every ignored signal is sent to the process itself,
        which is delivered before ``kill`` returns, and the process runs on; the stop
        signals and SIGALRM keep the dispositions they had."""
        code = (
            "import os, resource, signal\n"
            "from airflow_provider_rmq.watcher import runner\n"
            "resource.setrlimit(resource.RLIMIT_CORE, (0, 0))\n"
            "before = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT,\n"
            "                                             signal.SIGALRM)}\n"
            "runner._ignore_group_signals()\n"
            "for signum in runner._IGNORED_SIGNALS:\n"
            "    os.kill(os.getpid(), signum)\n"
            "print(all(signal.getsignal(s) is signal.SIG_IGN for s in runner._IGNORED_SIGNALS),\n"
            "      all(signal.getsignal(s) is h for s, h in before.items()))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.split() == ["True", "True"]


class TestPrepareAirflow:
    @pytest.fixture
    def airflow_parts(self, monkeypatch):
        from airflow import plugins_manager, providers_manager
        from airflow.api_internal import internal_api_call

        order = MagicMock()
        providers = MagicMock(name="ProvidersManager")
        monkeypatch.setattr(providers_manager, "ProvidersManager", providers)
        order.attach_mock(providers.return_value.initialize_providers_configuration, "providers")
        for name in runner._PLUGIN_INITIALISERS:
            fn = MagicMock(name=name)
            monkeypatch.setattr(plugins_manager, name, fn, raising=False)
            order.attach_mock(fn, name)
        direct = MagicMock(name="force_database_direct_access")
        monkeypatch.setattr(
            internal_api_call.InternalApiConfig, "force_database_direct_access", direct,
            raising=False,
        )
        order.attach_mock(direct, "direct")
        return SimpleNamespace(order=order, plugins_manager=plugins_manager)

    def test_providers_configuration_then_direct_access_then_plugins(self, airflow_parts):
        runner._prepare_airflow()

        names = [c[0] for c in airflow_parts.order.mock_calls]
        assert names == ["providers", "direct", *runner._PLUGIN_INITIALISERS]

    def test_initialisers_the_installed_airflow_lacks_are_skipped(
        self, airflow_parts, monkeypatch
    ):
        monkeypatch.delattr(
            airflow_parts.plugins_manager, "initialize_priority_weight_strategy_plugins"
        )
        from airflow.api_internal.internal_api_call import InternalApiConfig

        monkeypatch.delattr(InternalApiConfig, "force_database_direct_access")

        runner._prepare_airflow()

        names = [c[0] for c in airflow_parts.order.mock_calls]
        assert names == ["providers", *runner._PLUGIN_INITIALISERS[:-1]]

    def test_every_initialiser_exists_in_the_installed_airflow(self):
        from airflow import plugins_manager

        assert all(hasattr(plugins_manager, name) for name in runner._PLUGIN_INITIALISERS)


class TestCloseDatabaseConnections:
    def test_disposes_the_orm(self, monkeypatch):
        from airflow import settings

        dispose = MagicMock(name="dispose_orm")
        monkeypatch.setattr(settings, "dispose_orm", dispose)

        runner._close_database_connections()

        dispose.assert_called_once_with()

    def test_a_failure_is_logged_and_swallowed(self, monkeypatch, caplog):
        from airflow import settings

        monkeypatch.setattr(
            settings, "dispose_orm", MagicMock(side_effect=RuntimeError("connection reset"))
        )

        with caplog.at_level(logging.WARNING, logger=runner.log.name):
            runner._close_database_connections()

        assert any(
            r.levelno == logging.WARNING and "database connections" in r.getMessage()
            for r in caplog.records
        )


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


class TestMainStartup:
    def test_signals_are_blocked_before_anything_else(self, process, monkeypatch):
        fake = _FakeListener()
        factory = _use_listener(monkeypatch, fake)
        order = MagicMock()
        order.attach_mock(process.sigmask, "sigmask")
        order.attach_mock(process.ignore, "ignore")
        order.attach_mock(process.sigalrm, "sigalrm")
        order.attach_mock(process.pdeathsig, "pdeathsig")
        order.attach_mock(process.getppid, "getppid")
        order.attach_mock(process.prepare, "prepare")
        order.attach_mock(factory, "listener")

        assert _run_main() == 0

        names = [c[0] for c in order.mock_calls]
        assert names[:3] == ["sigmask", "ignore", "sigalrm"]
        process.ignore.assert_called_once_with()
        assert names.index("sigalrm") < names.index("pdeathsig") < names.index("getppid")
        assert names.index("getppid") < names.index("listener")
        # SIGALRM is made fatal again once the preparation, where a plugin may set a
        # handler for it, has returned.
        sigalrm_calls = [i for i, name in enumerate(names) if name == "sigalrm"]
        assert len(sigalrm_calls) == 2
        assert names.index("prepare") < sigalrm_calls[1] < names.index("listener")
        process.sigmask.assert_called_once_with(
            signal.SIG_BLOCK, frozenset({signal.SIGTERM, signal.SIGINT})
        )

    def test_airflow_is_imported_only_after_the_mask(self, process, monkeypatch):
        """Airflow starts threads on import, and a thread takes the mask of the thread
        that starts it — so no ``import airflow`` may come before the mask."""
        _use_listener(monkeypatch, _FakeListener())
        events = []
        process.sigmask.side_effect = lambda *args: events.append("sigmask")
        real_import = builtins.__import__

        def recording_import(name, *args, **kwargs):
            if name == "airflow" or name.startswith("airflow."):
                events.append(f"import {name}")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", recording_import)

        assert _run_main() == 0

        assert events[0] == "sigmask"
        assert "import airflow" in events

    def test_airflow_is_prepared_before_the_loop_thread_starts(self, process, monkeypatch):
        fake = _FakeListener()
        _use_listener(monkeypatch, fake)
        prepared_before_loop = []
        real_run = fake.run

        def run(parent_pid: int) -> None:
            prepared_before_loop.append(process.prepare.called)
            real_run(parent_pid)

        fake.run = run  # type: ignore[method-assign]

        assert _run_main() == 0

        process.prepare.assert_called_once_with()
        assert prepared_before_loop == [True]

    def test_the_start_is_logged_with_both_pids(self, process, monkeypatch, caplog):
        _use_listener(monkeypatch, _FakeListener())

        with caplog.at_level(logging.INFO, logger=runner.log.name):
            assert _run_main() == 0

        started = [
            r for r in caplog.records
            if r.getMessage()
            == f"RMQ Watcher process {os.getpid()} started for the scheduler process {_PARENT}"
        ]
        # Logged from the loop thread: the main thread goes from starting it straight
        # to its signal wait.
        assert [r.threadName for r in started] == ["rmq-watcher"]

    def test_missing_parent_pid_exits_with_code_2(self, process, monkeypatch, capfd):
        monkeypatch.delenv(runner.PARENT_PID_ENV)
        factory = _use_listener(monkeypatch, _FakeListener())

        assert _run_main() == 2

        process.sigmask.assert_called_once()
        process.pdeathsig.assert_not_called()
        factory.assert_not_called()
        assert runner.PARENT_PID_ENV in capfd.readouterr().err

    def test_malformed_parent_pid_exits_with_code_2(self, process, monkeypatch):
        monkeypatch.setenv(runner.PARENT_PID_ENV, "not-a-pid")
        factory = _use_listener(monkeypatch, _FakeListener())

        assert _run_main() == 2

        process.pdeathsig.assert_not_called()
        factory.assert_not_called()

    def test_parent_gone_before_setup_exits_without_starting_the_loop(
        self, process, monkeypatch, capfd
    ):
        process.getppid.return_value = 1
        factory = _use_listener(monkeypatch, _FakeListener())

        assert _run_main() == 0

        process.pdeathsig.assert_called_once()
        factory.assert_not_called()
        process.sigtimedwait.assert_not_called()
        assert str(_PARENT) in capfd.readouterr().err

    def test_initialisation_failure_exits_with_code_1(self, process, monkeypatch, caplog):
        factory = MagicMock(side_effect=RuntimeError("metadata database unreachable"))
        monkeypatch.setattr(listener_module, "RMQWatcherListener", factory)

        with caplog.at_level(logging.ERROR, logger=runner.log.name):
            assert _run_main() == 1

        process.shutdown.assert_called_once()
        assert any(
            r.levelno == logging.ERROR and r.exc_info and "metadata database unreachable"
            in str(r.exc_info[1])
            for r in caplog.records
        )

    def test_a_failing_preparation_exits_with_code_1(self, process, monkeypatch, caplog):
        factory = _use_listener(monkeypatch, _FakeListener())
        process.prepare.side_effect = RuntimeError("providers configuration unreadable")

        with caplog.at_level(logging.ERROR, logger=runner.log.name):
            assert _run_main() == 1

        factory.assert_not_called()
        assert any(r.levelno == logging.ERROR and r.exc_info for r in caplog.records)

    def test_system_exit_during_setup_exits_with_code_1(self, process, monkeypatch, caplog):
        """``SystemExit`` is not an ``Exception``; a setup step that raises it still
        ends in the logged exit with code 1."""
        factory = MagicMock(side_effect=SystemExit(3))
        monkeypatch.setattr(listener_module, "RMQWatcherListener", factory)

        with caplog.at_level(logging.ERROR, logger=runner.log.name):
            assert _run_main() == 1

        process.shutdown.assert_called_once()
        assert any(
            r.levelno == logging.ERROR and r.exc_info and isinstance(r.exc_info[1], SystemExit)
            for r in caplog.records
        )


class _PreparationAbandoned(Exception):
    """Raised by a hung preparation once the watch thread has ended the process.

    A real process runs no further than :func:`os._exit`; the test only has to get the
    main thread out of the preparation it would have been left in.
    """


#: Marks a test whose ``os._exit`` ends the watch thread with :class:`SystemExit`, which
#: pytest reports as an exception of that thread.
_EXIT_ENDS_THE_WATCH_THREAD = pytest.mark.filterwarnings(
    "ignore::pytest.PytestUnhandledThreadExceptionWarning"
)


def _watch_alive() -> bool:
    return any(
        t.name == runner._WATCH_THREAD_NAME and t.is_alive() for t in threading.enumerate()
    )


class TestMainPreparation:
    """Airflow is imported and prepared in the main thread while the watch thread
    waits for a stop signal and watches the parent."""

    @pytest.fixture
    def hung_preparation(self, process):
        """A preparation that does not return, the way a plugin that hangs on import
        would leave it, and an ``os._exit`` that tells which thread called it.

        Returns the list of ``(code, thread name, preparation hung, logging shut down)``
        of every ``os._exit`` call. From the watch thread the call ends that thread and
        lets the preparation go, which raises :class:`_PreparationAbandoned`.
        """
        released = threading.Event()
        exits: list[tuple[int, str, bool, bool]] = []

        def prepare() -> None:
            released.wait(timeout=30)
            raise _PreparationAbandoned

        def exit_(code: int) -> None:
            exits.append(
                (code, threading.current_thread().name, not released.is_set(),
                 process.shutdown.called)
            )
            if threading.current_thread() is threading.main_thread():
                raise _Exited(code)
            released.set()
            # Ends the watch thread quietly, the way os._exit ends the process.
            raise SystemExit

        process.prepare.side_effect = prepare
        process.exit.side_effect = exit_
        yield exits
        released.set()

    def test_airflow_is_prepared_in_the_main_thread(self, process, monkeypatch):
        _use_listener(monkeypatch, _FakeListener())
        threads = []
        process.prepare.side_effect = lambda: threads.append(threading.current_thread())

        assert _run_main() == 0

        assert threads == [threading.main_thread()]

    def test_a_plugin_may_call_main_thread_only_signal_functions(self, process, monkeypatch):
        """A plugin that sets a signal handler or the wakeup fd on import loads as it
        does in the scheduler's main thread; off the main thread both raise."""
        _use_listener(monkeypatch, _FakeListener())
        done = []

        def plugin_import() -> None:
            signal.signal(signal.SIGUSR1, signal.getsignal(signal.SIGUSR1))
            signal.set_wakeup_fd(signal.set_wakeup_fd(-1))
            done.append(True)

        process.prepare.side_effect = plugin_import

        assert _run_main() == 0

        assert done == [True]

    def test_the_watch_thread_runs_during_the_preparation_and_ends_before_the_loop(
        self, process, monkeypatch
    ):
        fake = _FakeListener()
        factory = _use_listener(monkeypatch, fake)
        watching = {}
        process.prepare.side_effect = lambda: watching.update(preparation=_watch_alive())

        def create_listener() -> _FakeListener:
            watching["listener"] = _watch_alive()
            return fake

        factory.side_effect = create_listener

        assert _run_main() == 0

        assert watching == {"preparation": True, "listener": False}

    @_EXIT_ENDS_THE_WATCH_THREAD
    def test_a_stop_signal_during_a_hung_preparation_exits_at_once(
        self, process, monkeypatch, hung_preparation, capfd
    ):
        factory = _use_listener(monkeypatch, _FakeListener())
        signals = iter([None, SimpleNamespace(si_signo=signal.SIGTERM)])

        def sigtimedwait(waited_for, timeout):
            threading.Event().wait(0.01)
            return next(signals)

        process.sigtimedwait.side_effect = sigtimedwait

        _run_main()

        assert hung_preparation[0] == (0, runner._WATCH_THREAD_NAME, True, False), (
            "the watch thread did not end the process while the preparation hung"
        )
        factory.assert_not_called()
        process.close_db.assert_not_called()
        assert "received SIGTERM while Airflow was being prepared" in capfd.readouterr().err

    @_EXIT_ENDS_THE_WATCH_THREAD
    def test_the_scheduler_dying_during_a_hung_preparation_exits(
        self, process, monkeypatch, hung_preparation, capfd
    ):
        """No parent-death signal comes — as on macOS, or where the kernel refused
        it — and the watch thread notices the new parent between two waits."""
        factory = _use_listener(monkeypatch, _FakeListener())
        process.sigtimedwait.side_effect = _never_signalled
        parents = iter([_PARENT, _PARENT, 1])
        process.getppid.side_effect = lambda: next(parents, 1)

        _run_main()

        assert hung_preparation[0] == (0, runner._WATCH_THREAD_NAME, True, False)
        factory.assert_not_called()
        err = capfd.readouterr().err
        assert f"the scheduler process {_PARENT} exited while Airflow was being prepared" in err

    @_EXIT_ENDS_THE_WATCH_THREAD
    def test_a_platform_without_sigtimedwait_stops_during_a_hung_preparation(
        self, process, monkeypatch, hung_preparation, capfd
    ):
        factory = _use_listener(monkeypatch, _FakeListener())
        monkeypatch.delattr(runner.signal, "sigtimedwait", raising=False)
        monkeypatch.setattr(runner.signal, "sigpending", MagicMock(return_value={signal.SIGINT}))
        sigwait = MagicMock(return_value=signal.SIGINT)
        monkeypatch.setattr(runner.signal, "sigwait", sigwait)

        _run_main()

        assert hung_preparation[0] == (0, runner._WATCH_THREAD_NAME, True, False)
        sigwait.assert_called_once_with(runner._STOP_SIGNALS)
        factory.assert_not_called()
        assert "received SIGINT while Airflow was being prepared" in capfd.readouterr().err

    def test_a_failed_parent_death_request_is_written_once_the_watch_thread_runs(
        self, process, monkeypatch, capfd
    ):
        _use_listener(monkeypatch, _FakeListener())
        process.pdeathsig.return_value = "prctl(PR_SET_PDEATHSIG) failed: Invalid argument"
        watching = []
        real_report = runner._report

        def report(message: str) -> None:
            watching.append(_watch_alive())
            real_report(message)

        monkeypatch.setattr(runner, "_report", report)

        assert _run_main() == 0

        assert watching[0] is True
        assert "prctl(PR_SET_PDEATHSIG) failed: Invalid argument" in capfd.readouterr().err

    @_EXIT_ENDS_THE_WATCH_THREAD
    def test_a_stop_signal_ends_the_process_while_that_line_blocks(
        self, process, monkeypatch
    ):
        """stderr is a pipe nobody reads, so writing the failed parent-death request
        never returns; the watch thread still takes the stop signal and ends the
        process."""
        factory = _use_listener(monkeypatch, _FakeListener())
        process.pdeathsig.return_value = "prctl(PR_SET_PDEATHSIG) failed: Invalid argument"
        released = threading.Event()
        exits: list[tuple[int, str]] = []

        def report(message: str) -> None:
            if message.startswith("prctl"):
                released.wait(timeout=30)
                raise _PreparationAbandoned

        def exit_(code: int) -> None:
            exits.append((code, threading.current_thread().name))
            if threading.current_thread() is threading.main_thread():
                raise _Exited(code)
            released.set()
            raise SystemExit

        monkeypatch.setattr(runner, "_report", report)
        process.exit.side_effect = exit_
        signals = iter([None, SimpleNamespace(si_signo=signal.SIGTERM)])

        def sigtimedwait(waited_for, timeout):
            threading.Event().wait(0.01)
            return next(signals)

        process.sigtimedwait.side_effect = sigtimedwait

        try:
            _run_main()
        finally:
            released.set()

        assert exits[0] == (0, runner._WATCH_THREAD_NAME)
        process.prepare.assert_not_called()
        factory.assert_not_called()

    def test_an_exception_of_the_preparation_is_logged_once_the_watch_thread_ended(
        self, process, monkeypatch, caplog
    ):
        _use_listener(monkeypatch, _FakeListener())
        error = RuntimeError("plugin import failed")
        process.prepare.side_effect = error
        watching_at_exit = []

        def exit_(code: int) -> None:
            watching_at_exit.append(_watch_alive())
            _raise_exited(code)

        process.exit.side_effect = exit_

        with caplog.at_level(logging.ERROR, logger=runner.log.name):
            assert _run_main() == 1

        assert watching_at_exit == [False]
        assert any(
            r.levelno == logging.ERROR and "process failed" in r.getMessage()
            and r.exc_info and r.exc_info[1] is error
            for r in caplog.records
        )
        process.shutdown.assert_called_once()


class TestWatchPreparation:
    """``_watch_preparation`` called directly, in the test's thread."""

    def test_a_stop_signal_ends_the_process_with_code_0(self, process, capfd):
        process.sigtimedwait.side_effect = [None, SimpleNamespace(si_signo=signal.SIGTERM)]

        with pytest.raises(_Exited) as info:
            runner._watch_preparation(_PARENT, threading.Event())

        assert info.value.code == 0
        process.shutdown.assert_not_called()
        assert "received SIGTERM while Airflow was being prepared" in capfd.readouterr().err

    def test_a_parent_change_ends_the_process_with_code_0(self, process, capfd):
        process.sigtimedwait.side_effect = None
        process.sigtimedwait.return_value = None
        process.getppid.side_effect = [_PARENT, 1]

        with pytest.raises(_Exited) as info:
            runner._watch_preparation(_PARENT, threading.Event())

        assert info.value.code == 0
        assert process.sigtimedwait.call_count == 2
        assert f"the scheduler process {_PARENT} exited" in capfd.readouterr().err

    def test_a_finished_preparation_returns_after_the_wait_under_way(self, process):
        finished = threading.Event()

        def sigtimedwait(waited_for, timeout):
            finished.set()

        process.sigtimedwait.side_effect = sigtimedwait

        runner._watch_preparation(_PARENT, finished)

        process.sigtimedwait.assert_called_once_with(
            runner._STOP_SIGNALS, runner._SIGNAL_POLL_INTERVAL
        )
        process.exit.assert_not_called()

    def test_a_signal_taken_as_the_preparation_finishes_still_ends_the_process(
        self, process, capfd
    ):
        """The preparation returns while the watch thread is in a wait, and that wait
        takes a stop signal: the signal ends the process instead of being dropped."""
        finished = threading.Event()

        def sigtimedwait(waited_for, timeout):
            finished.set()
            return SimpleNamespace(si_signo=signal.SIGTERM)

        process.sigtimedwait.side_effect = sigtimedwait

        with pytest.raises(_Exited) as info:
            runner._watch_preparation(_PARENT, finished)

        assert info.value.code == 0
        assert "received SIGTERM" in capfd.readouterr().err

    def test_nothing_is_waited_for_once_the_preparation_has_finished(self, process):
        """A signal arriving after the watch thread ended stays pending for the main
        thread's wait."""
        finished = threading.Event()
        finished.set()

        runner._watch_preparation(_PARENT, finished)

        process.sigtimedwait.assert_not_called()
        process.getppid.assert_not_called()

    def test_real_signals_in_a_thread_other_than_the_main_one(self):
        """Against the kernel: with the stop signals blocked process-wide, a watch thread
        takes a SIGTERM the process sends itself while the main thread sets a signal
        handler and then hangs, and ends the process with code 0; no handler runs."""
        code = (
            "import os, signal, threading\n"
            "from airflow_provider_rmq.watcher import runner\n"
            "signal.pthread_sigmask(signal.SIG_BLOCK, runner._STOP_SIGNALS)\n"
            "handled = []\n"
            "finished = threading.Event()\n"
            "threading.Thread(target=runner._watch_preparation,\n"
            "                 args=(os.getppid(), finished), daemon=True).start()\n"
            "signal.signal(signal.SIGTERM, lambda *a: handled.append(1) or os._exit(5))\n"
            "os.kill(os.getpid(), signal.SIGTERM)\n"
            "threading.Event().wait(30)\n"
            "os._exit(4)\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert "received SIGTERM while Airflow was being prepared" in result.stderr


class TestReport:
    def test_the_line_goes_straight_to_file_descriptor_2(self, monkeypatch, capfd):
        """``sys.stderr`` is not used: its buffer's lock may be held by a thread the
        process leaves behind."""
        broken = MagicMock(name="stderr")
        broken.write.side_effect = AssertionError("sys.stderr used")
        monkeypatch.setattr(runner.sys, "stderr", broken)

        runner._report("going — now")

        assert capfd.readouterr().err == f"RMQ Watcher process {os.getpid()}: going — now\n"

    def test_a_closed_stderr_is_ignored(self, monkeypatch):
        monkeypatch.setattr(runner.os, "write", MagicMock(side_effect=OSError(9, "EBADF")))

        runner._report("nobody listening")


class TestShutDownLogging:
    def test_logging_is_shut_down(self, monkeypatch, capfd):
        shutdown = MagicMock(name="logging.shutdown")
        monkeypatch.setattr(runner.logging, "shutdown", shutdown)

        runner._shut_down_logging()

        shutdown.assert_called_once_with()
        assert capfd.readouterr().err == ""

    def test_a_shutdown_that_does_not_return_is_given_up(self, monkeypatch, capfd):
        release = threading.Event()
        monkeypatch.setattr(runner.logging, "shutdown", lambda: release.wait(timeout=30))
        monkeypatch.setattr(runner, "_LOGGING_SHUTDOWN_TIMEOUT", 0.1)
        try:
            runner._shut_down_logging()
        finally:
            release.set()

        assert "logging did not shut down" in capfd.readouterr().err

    def test_a_handler_lock_held_by_a_thread_left_behind_does_not_block_the_exit(self):
        """Against the real :func:`logging.shutdown`: a thread holds a handler's lock
        for good, and the process still gets to its exit."""
        code = (
            "import logging, os, sys, threading\n"
            "from airflow_provider_rmq.watcher import runner\n"
            "handler = logging.StreamHandler(sys.stdout)\n"
            "logging.getLogger().addHandler(handler)\n"
            "held = threading.Event()\n"
            "def hold():\n"
            "    handler.acquire()\n"
            "    held.set()\n"
            "    threading.Event().wait()\n"
            "threading.Thread(target=hold, daemon=True).start()\n"
            "held.wait()\n"
            "runner._LOGGING_SHUTDOWN_TIMEOUT = 0.5\n"
            "runner._shut_down_logging()\n"
            "print('returned', flush=True)\n"
            "os._exit(0)\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=30,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.split() == ["returned"]
        assert "logging did not shut down" in result.stderr


# ---------------------------------------------------------------------------
# Hard exit deadline
# ---------------------------------------------------------------------------


class _Recorder(logging.Handler):
    def __init__(self, recorded: list[str]) -> None:
        super().__init__(level=logging.INFO)
        self.recorded = recorded

    def emit(self, record: logging.LogRecord) -> None:
        self.recorded.append(f"log {record.getMessage()}")


@pytest.fixture
def events(process, monkeypatch):
    """One list, in order, of the alarm, the lines written and logged, the steps of the
    way out and the stop requests of a :class:`_FakeListener` built with
    :func:`_recording_listener`."""
    recorded: list[str] = []

    def setitimer(which: int, seconds: float, interval: float = 0.0) -> tuple[float, float]:
        assert which == signal.ITIMER_REAL
        recorded.append(f"alarm {seconds}")
        return 0.0, 0.0

    real_report = runner._report

    def report(message: str) -> None:
        recorded.append(f"report {message}")
        real_report(message)

    def exit_(code: int) -> None:
        recorded.append(f"exit {code}")
        _raise_exited(code)

    monkeypatch.setattr(runner.signal, "setitimer", setitimer)
    monkeypatch.setattr(runner.signal, "getitimer", lambda which: (0.0, 0.0))
    monkeypatch.setattr(runner, "_report", report)
    process.exit.side_effect = exit_
    process.prepare.side_effect = lambda: recorded.append("prepare")
    process.close_db.side_effect = lambda: recorded.append("close_db")
    process.shutdown.side_effect = lambda: recorded.append("shutdown")
    handler = _Recorder(recorded)
    level = runner.log.level
    runner.log.addHandler(handler)
    runner.log.setLevel(logging.INFO)
    yield recorded
    runner.log.removeHandler(handler)
    runner.log.setLevel(level)


def _recording_listener(recorded: list[str], **kwargs) -> _FakeListener:
    fake = _FakeListener(**kwargs)
    real_request_stop = fake.request_stop

    def request_stop() -> None:
        recorded.append("request_stop")
        real_request_stop()

    fake.request_stop = request_stop  # type: ignore[method-assign]
    return fake


def _first(recorded: list[str], prefix: str) -> int:
    return next(i for i, item in enumerate(recorded) if item.startswith(prefix))


_ARMED = f"alarm {runner._HARD_EXIT_DEADLINE}"


class TestHardExitDeadline:
    def test_the_deadline_outlasts_every_bounded_wait_of_the_way_out(self):
        assert isinstance(runner._HARD_EXIT_DEADLINE, int)
        assert runner._HARD_EXIT_DEADLINE > (
            runner._EXIT_JOIN_TIMEOUT + runner._LOGGING_SHUTDOWN_TIMEOUT
        )

    def test_arming_sets_the_alarm_to_the_deadline(self, alarm_calls):
        runner._arm_hard_exit()

        assert alarm_calls == [runner._HARD_EXIT_DEADLINE]

    def test_an_alarm_already_armed_is_left_as_it_is(self, alarm_calls):
        runner._arm_hard_exit()
        runner._arm_hard_exit()

        assert alarm_calls == [runner._HARD_EXIT_DEADLINE]

    def test_an_alarm_already_armed_keeps_its_fraction_of_a_second(self, monkeypatch):
        """The timer is read, not re-armed: a remainder of 7.4 s stays 7.4 s rather
        than being rounded to whole seconds and set again, which would move the
        deadline on every call."""
        setitimer = MagicMock(name="setitimer")
        monkeypatch.setattr(runner.signal, "getitimer", lambda which: (7.4, 0.0))
        monkeypatch.setattr(runner.signal, "setitimer", setitimer)

        runner._arm_hard_exit()

        setitimer.assert_not_called()

    def test_the_deadline_is_not_moved_by_later_calls(self):
        """Against the kernel: calls made later leave the timer where the first put it."""
        code = (
            "import signal, time\n"
            "from airflow_provider_rmq.watcher import runner\n"
            "signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})\n"
            "runner._HARD_EXIT_DEADLINE = 5\n"
            "runner._arm_hard_exit()\n"
            "time.sleep(0.3)\n"
            "runner._arm_hard_exit()\n"
            "print(signal.getitimer(signal.ITIMER_REAL)[0])\n"
            "signal.setitimer(signal.ITIMER_REAL, 0)\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
            check=True,
        )

        assert 0 < float(result.stdout) < 4.8

    def test_the_alarm_ends_a_process_whose_threads_are_all_blocked(self):
        """Against the kernel: SIGALRM inherited as ignored and blocked, a watch-like
        thread arms the deadline and then blocks on a pipe nobody writes, the main
        thread blocks too — and the process still ends, by SIGALRM, once the deadline
        is up."""
        code = (
            "import os, signal, threading\n"
            "from airflow_provider_rmq.watcher import runner\n"
            "signal.pthread_sigmask(signal.SIG_BLOCK, runner._STOP_SIGNALS | {signal.SIGALRM})\n"
            "signal.signal(signal.SIGALRM, signal.SIG_IGN)\n"
            "runner._make_sigalrm_fatal()\n"
            "runner._HARD_EXIT_DEADLINE = 1\n"
            "def stuck():\n"
            "    runner._arm_hard_exit()\n"
            "    os.read(os.pipe()[0], 1)\n"
            "threading.Thread(target=stuck, daemon=True).start()\n"
            "threading.Event().wait(30)\n"
            "os._exit(4)\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
            check=False,
        )

        assert result.returncode == -signal.SIGALRM, result.stderr

    def test_a_timer_a_plugin_armed_is_cancelled_and_a_pending_alarm_dropped(self):
        """Against the kernel: a plugin set a SIGALRM handler, armed a real-time timer
        and left a SIGALRM pending while blocked. Once SIGALRM is made fatal the
        plugin's handler never runs, the pending signal does not end the process and
        neither does the timer when its time comes."""
        code = (
            "import os, signal, time\n"
            "from airflow_provider_rmq.watcher import runner\n"
            "ran = []\n"
            "signal.signal(signal.SIGALRM, lambda signum, frame: ran.append(signum))\n"
            "signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})\n"
            "os.kill(os.getpid(), signal.SIGALRM)\n"
            "signal.setitimer(signal.ITIMER_REAL, 0.3)\n"
            "runner._make_sigalrm_fatal()\n"
            "time.sleep(0.8)\n"
            "print(ran, signal.getsignal(signal.SIGALRM) is signal.SIG_DFL,\n"
            "      signal.SIGALRM in signal.pthread_sigmask(signal.SIG_BLOCK, []))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=60,
            check=False,
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.split() == ["[]", "True", "False"]

    def test_a_stop_signal_in_the_watch_thread_arms_before_the_report(self, process, events):
        events.clear()
        process.sigtimedwait.side_effect = [SimpleNamespace(si_signo=signal.SIGTERM)]

        with pytest.raises(_Exited):
            runner._watch_preparation(_PARENT, threading.Event())

        assert events[0] == _ARMED
        assert events[1].startswith("report received SIGTERM")
        assert events[2] == "exit 0"

    def test_a_parent_change_in_the_watch_thread_arms_before_the_report(
        self, process, events
    ):
        events.clear()
        process.sigtimedwait.side_effect = None
        process.sigtimedwait.return_value = None
        process.getppid.side_effect = [1]

        with pytest.raises(_Exited):
            runner._watch_preparation(_PARENT, threading.Event())

        assert events[0] == _ARMED
        assert events[1].startswith(f"report the scheduler process {_PARENT} exited")
        assert events[2] == "exit 0"

    def test_a_stop_signal_in_the_main_wait_arms_then_stops_then_logs(
        self, process, monkeypatch, events
    ):
        _use_listener(monkeypatch, _recording_listener(events))
        process.sigtimedwait.side_effect = _SignalAfter(1)

        assert _run_main() == 0

        armed = _first(events, "alarm")
        assert _first(events, "prepare") < armed, "armed before any stop"
        assert events[armed] == _ARMED
        assert events[armed + 1] == "request_stop"
        assert events[armed + 2].startswith("log RMQ Watcher process received SIGTERM")
        assert armed < _first(events, "close_db") < _first(events, "shutdown")
        assert events[-1] == "exit 0"

    def test_a_parent_change_in_the_main_wait_arms_then_stops_then_logs(
        self, process, monkeypatch, events
    ):
        """No parent-death signal comes — macOS, or a refused request — and the loop
        thread is stuck short of its own parent check: the main thread notices the new
        parent between two waits."""
        fake = _recording_listener(events)
        _use_listener(monkeypatch, fake)
        process.sigtimedwait.side_effect = _never_signalled
        process.getppid.side_effect = lambda: 1 if _loop_phase() else _PARENT

        assert _run_main() == 0

        armed = _first(events, "alarm")
        assert events[armed] == _ARMED
        assert events[armed + 1] == "request_stop"
        assert events[armed + 2] == (
            f"log RMQ Watcher process: the scheduler process {_PARENT} has exited "
            "(parent is now 1) — stopping the watcher"
        )
        assert fake.returned.is_set()
        assert events[-1] == "exit 0"

    def test_a_loop_ending_on_its_own_arms_before_the_way_out(
        self, process, monkeypatch, events
    ):
        _use_listener(monkeypatch, _recording_listener(events, returns_at_once=True))
        process.sigtimedwait.side_effect = _never_signalled

        assert _run_main() == 0

        assert _first(events, "alarm") < _first(events, "close_db") < _first(events, "shutdown")
        assert "request_stop" not in events

    def test_a_loop_that_raises_arms_before_the_line_saying_so(
        self, process, monkeypatch, events
    ):
        _use_listener(monkeypatch, _recording_listener(events, raises=RuntimeError("boom")))
        process.sigtimedwait.side_effect = _never_signalled

        assert _run_main() == 1

        armed = _first(events, "alarm")
        assert armed < _first(events, "log RMQ Watcher loop failed")

    def test_a_failing_setup_arms_before_the_line_saying_so(
        self, process, monkeypatch, events
    ):
        _use_listener(monkeypatch, _FakeListener())
        process.prepare.side_effect = RuntimeError("plugin import failed")

        assert _run_main() == 1

        assert _first(events, "alarm") < _first(events, "log RMQ Watcher process failed")
        assert _first(events, "log RMQ Watcher process failed") < _first(events, "shutdown")

    def test_a_missing_parent_pid_arms_before_the_report(self, monkeypatch, events):
        monkeypatch.delenv(runner.PARENT_PID_ENV)

        assert _run_main() == 2

        assert events[0] == _ARMED
        assert events[1].startswith(f"report {runner.PARENT_PID_ENV}")

    def test_a_parent_gone_before_setup_arms_before_the_report(self, process, events):
        process.getppid.return_value = 1

        assert _run_main() == 0

        assert events[0] == _ARMED
        assert events[1].startswith(f"report the scheduler process {_PARENT} exited before")

    def test_nothing_is_armed_while_the_watcher_runs(self, process, monkeypatch, events):
        """The alarm is the way out's alone: the preparation and the loop run with none
        pending."""
        fake = _recording_listener(events)
        _use_listener(monkeypatch, fake)
        armed_while_running = []
        real_run = fake.run

        def run(parent_pid: int) -> None:
            armed_while_running.append(any(e.startswith("alarm") for e in events))
            real_run(parent_pid)

        fake.run = run  # type: ignore[method-assign]
        process.sigtimedwait.side_effect = _SignalAfter(2)

        assert _run_main() == 0

        assert armed_while_running == [False]

    def test_the_exit_is_reached_when_logging_cannot_be_shut_down(
        self, process, monkeypatch
    ):
        """The thread :func:`logging.shutdown` runs in cannot even start; the process
        still leaves through :func:`os._exit`, with the loop's code, rather than through
        the interpreter's own exit, which joins every thread left behind."""
        _use_listener(monkeypatch, _FakeListener())
        monkeypatch.setattr(
            runner, "_shut_down_logging",
            MagicMock(side_effect=RuntimeError("can't start new thread")),
        )

        assert _run_main() == 0

        process.exit.assert_called_once_with(0)


class TestMainLoop:
    def test_signal_requests_stop_and_joins_the_loop(self, process, monkeypatch, caplog):
        fake = _FakeListener()
        _use_listener(monkeypatch, fake)
        waits = _SignalAfter(2)
        process.sigtimedwait.side_effect = waits

        returned_at_exit = []

        def exit_after_the_loop(code: int) -> None:
            returned_at_exit.append(fake.returned.is_set())
            _raise_exited(code)

        process.exit.side_effect = exit_after_the_loop

        with caplog.at_level(logging.INFO, logger=runner.log.name):
            assert _run_main() == 0

        assert fake.run_args == (_PARENT,)
        assert fake.run_thread_name == "rmq-watcher"
        assert fake.stop_requests == 1
        assert returned_at_exit == [True], "the process left before the loop returned"
        assert waits.loop_calls == 3
        waited_for, timeout = process.sigtimedwait.call_args.args
        assert set(waited_for) == {signal.SIGTERM, signal.SIGINT}
        assert timeout == runner._SIGNAL_POLL_INTERVAL
        process.shutdown.assert_called_once()
        process.close_db.assert_called_once_with()
        assert any("received SIGTERM" in r.getMessage() for r in caplog.records)
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_a_platform_without_sigtimedwait_stops_on_a_pending_signal(
        self, process, monkeypatch, caplog
    ):
        """macOS has no ``sigtimedwait``: the main thread finds SIGTERM among the
        pending signals, takes it with ``sigwait`` and stops the loop the same way."""
        fake = _FakeListener()
        _use_listener(monkeypatch, fake)
        monkeypatch.delattr(runner.signal, "sigtimedwait", raising=False)
        loop_looks = []

        def sigpending():
            if not _loop_phase():
                return set()
            loop_looks.append(1)
            return {signal.SIGTERM} if len(loop_looks) >= 3 else set()

        sigwait = MagicMock(return_value=signal.SIGTERM)
        monkeypatch.setattr(runner.signal, "sigpending", sigpending)
        monkeypatch.setattr(runner.signal, "sigwait", sigwait)
        monkeypatch.setattr(runner, "_SIGNAL_PENDING_POLL", 0.01)

        with caplog.at_level(logging.INFO, logger=runner.log.name):
            assert _run_main() == 0

        assert fake.stop_requests == 1
        assert fake.returned.is_set()
        sigwait.assert_called_once_with(runner._STOP_SIGNALS)
        process.close_db.assert_called_once_with()
        assert any("received SIGTERM" in r.getMessage() for r in caplog.records)

    def test_sigint_stops_the_loop_too(self, process, monkeypatch):
        fake = _FakeListener()
        _use_listener(monkeypatch, fake)
        process.sigtimedwait.side_effect = _SignalAfter(0, signal.SIGINT)

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
        process.close_db.assert_called_once_with()

    def test_loop_outliving_the_join_is_left_behind(self, process, monkeypatch, capfd):
        fake = _FakeListener(stops_on_request=False)
        _use_listener(monkeypatch, fake)
        monkeypatch.setattr(runner, "_EXIT_JOIN_TIMEOUT", 0.05)
        try:
            assert _run_main() == 0
        finally:
            fake.release.set()

        assert fake.stop_requests == 1
        assert "the loop is still running 0s after the stop" in capfd.readouterr().err
        process.shutdown.assert_called_once()
        process.close_db.assert_not_called()

    def test_a_loop_left_behind_holding_a_handler_lock_does_not_block_the_exit(
        self, process, monkeypatch, capfd
    ):
        """The loop thread is stuck inside a logging handler and holds its lock past the
        join; the report of it and the shutdown of logging take no such lock and the
        process still reaches its exit."""
        handler = logging.Handler()
        holding = threading.Event()
        release = threading.Event()

        class _StuckInAHandler(_FakeListener):
            def run(self, parent_pid: int) -> None:
                with handler.lock:
                    holding.set()
                    release.wait(timeout=30)

        _use_listener(monkeypatch, _StuckInAHandler())
        process.sigtimedwait.side_effect = _SignalAfter(0)
        monkeypatch.setattr(runner, "_EXIT_JOIN_TIMEOUT", 0.05)
        monkeypatch.setattr(runner, "_LOGGING_SHUTDOWN_TIMEOUT", 0.1)

        def shutdown() -> None:
            with handler.lock:
                pass

        process.shutdown.side_effect = shutdown
        try:
            assert _run_main() == 0
            assert holding.is_set()
        finally:
            release.set()

        err = capfd.readouterr().err
        assert "the loop is still running" in err
        assert "logging did not shut down" in err

    def test_failure_while_waiting_exits_with_code_1(self, process, monkeypatch, caplog):
        fake = _FakeListener()
        _use_listener(monkeypatch, fake)
        process.sigtimedwait.side_effect = _failing_in_the_loop(OSError("sigtimedwait failed"))
        try:
            with caplog.at_level(logging.ERROR, logger=runner.log.name):
                assert _run_main() == 1
        finally:
            fake.release.set()

        process.shutdown.assert_called_once()
        assert any(
            r.levelno == logging.ERROR and r.exc_info and "sigtimedwait failed"
            in str(r.exc_info[1])
            for r in caplog.records
        )

    def test_keyboard_interrupt_while_waiting_exits_with_code_1(
        self, process, monkeypatch, caplog
    ):
        fake = _FakeListener()
        _use_listener(monkeypatch, fake)
        process.sigtimedwait.side_effect = _failing_in_the_loop(KeyboardInterrupt())
        try:
            with caplog.at_level(logging.ERROR, logger=runner.log.name):
                assert _run_main() == 1
        finally:
            fake.release.set()

        assert any(
            r.levelno == logging.ERROR and r.exc_info
            and isinstance(r.exc_info[1], KeyboardInterrupt)
            for r in caplog.records
        )

    @pytest.mark.parametrize(
        "error", [RuntimeError("event loop unavailable"), SystemExit(3)],
        ids=["exception", "system-exit"],
    )
    def test_a_loop_that_raises_exits_with_code_1(self, process, monkeypatch, caplog, error):
        """An exception out of :meth:`run` ends the thread, not the process: the main
        thread sees it gone and leaves with 1 rather than the 0 of a stopped loop."""
        fake = _FakeListener(raises=error)
        _use_listener(monkeypatch, fake)
        process.sigtimedwait.side_effect = _never_signalled

        with caplog.at_level(logging.ERROR, logger=runner.log.name):
            assert _run_main() == 1

        assert fake.returned.is_set()
        process.shutdown.assert_called_once()
        process.close_db.assert_not_called()
        assert any(
            r.levelno == logging.ERROR and "loop failed" in r.getMessage() and r.exc_info
            and r.exc_info[1] is error
            for r in caplog.records
        )


def test_running_as_main_module_calls_main():
    """``python -m airflow_provider_rmq.watcher.runner`` enters :func:`main`.

    Without the scheduler's PID in the environment ``main()`` leaves with code 2 before
    importing Airflow, which is enough to show the module runs it.
    """
    env = {k: v for k, v in os.environ.items() if k != runner.PARENT_PID_ENV}
    result = subprocess.run(
        [sys.executable, "-m", runner.__name__],
        env=env, capture_output=True, text=True, timeout=60, check=False,
    )
    assert result.returncode == 2
    assert runner.PARENT_PID_ENV in result.stderr
