"""Integration tests of the watcher process: the real runner in a real interpreter.

Each test starts ``tests/watcher/runner_stub.py`` — :func:`runner.main` around a loop
that only waits for the stop event — with :class:`subprocess.Popen` and drives it with
real signals: the signal mask, the watch thread of the preparation, ``sigtimedwait``, the
parent-death signal and ``os._exit`` all run as they do in production. The stub
imports Airflow against a throwaway ``AIRFLOW_HOME`` and a SQLite file inside it, and
the loop never reaches the database or a broker.

The stub reports through ``RUNNER_STUB ...`` lines on stdout, read by a thread that
lets a test wait for a given line, and through a marker file that outlives the process.
Every process a test starts is killed and reaped when the test ends, whatever its
outcome.
"""
from __future__ import annotations

import ctypes
import functools
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from airflow_provider_rmq.watcher import runner

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="these tests read /proc and call prctl, which Linux alone has",
)

_STUB = Path(__file__).with_name("runner_stub.py")
_REPO_ROOT = Path(__file__).resolve().parents[2]

#: Seconds a stub gets to reach its loop: the runner imports Airflow on the way.
_START_TIMEOUT = 60.0
#: Seconds a stub gets to leave once it has been asked to.
_EXIT_TIMEOUT = 30.0

#: Whole seconds of the hard exit deadline in the tests that let it expire.
_SHORT_DEADLINE = 3

#: ``PR_SET_CHILD_SUBREAPER`` from ``<linux/prctl.h>``.
_PR_SET_CHILD_SUBREAPER = 36


class _Output:
    """Lines of a process's stdout, collected by a thread as they arrive.

    The stream reaches its end once every process holding its write end has exited,
    which is what :meth:`wait_closed` waits for; the reading thread closes it there.
    """

    def __init__(self, stream) -> None:
        self._lines: list[str] = []
        self._closed = False
        self._cond = threading.Condition()
        self._thread = threading.Thread(
            target=self._read, args=(stream,), name="runner-stub-output", daemon=True
        )
        self._thread.start()

    def _read(self, stream) -> None:
        with stream:
            for line in stream:
                with self._cond:
                    self._lines.append(line.rstrip("\n"))
                    self._cond.notify_all()
        with self._cond:
            self._closed = True
            self._cond.notify_all()

    @property
    def text(self) -> str:
        with self._cond:
            return "\n".join(self._lines)

    def wait_for(self, prefix: str, timeout: float) -> str:
        """Return the first line starting with ``prefix``, waiting for it if need be."""

        def found() -> str | None:
            return next((line for line in self._lines if line.startswith(prefix)), None)

        with self._cond:
            self._cond.wait_for(lambda: found() is not None or self._closed, timeout)
            line = found()
        assert line is not None, f"no line starting with {prefix!r}; output:\n{self.text}"
        return line

    def wait_closed(self, timeout: float) -> None:
        with self._cond:
            closed = self._cond.wait_for(lambda: self._closed, timeout)
        assert closed, f"stdout still open after {timeout}s; output:\n{self.text}"


def _children_of(pid: int) -> list[int]:
    """PIDs of the live processes whose parent is ``pid``, read from ``/proc``."""
    children = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            stat = (entry / "stat").read_text()
        except OSError:
            continue
        # The command name in parentheses may hold spaces; the fields after it do not.
        state, ppid = stat.rsplit(")", 1)[1].split()[:2]
        if int(ppid) == pid and state != "Z":
            children.append(int(entry.name))
    return children


def _set_child_subreaper(enabled: bool) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(_PR_SET_CHILD_SUBREAPER, int(enabled), 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "prctl(PR_SET_CHILD_SUBREAPER) failed")


@pytest.fixture
def stub_env(tmp_path):
    """Environment of a stub: this process's, with a throwaway Airflow home.

    The watcher's own variables are left out, so each test sets what it needs.
    """
    home = tmp_path / "airflow_home"
    home.mkdir()
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in (runner.CHILD_ENV_MARKER, runner.PARENT_PID_ENV)
        and not key.startswith("RMQ_STUB_")
    }
    python_path = env.get("PYTHONPATH")
    env.update(
        AIRFLOW_HOME=str(home),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{home / 'airflow.db'}",
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__DAGS_FOLDER=str(home / "dags"),
        AIRFLOW__CORE__PLUGINS_FOLDER=str(home / "plugins"),
        PYTHONPATH=str(_REPO_ROOT) + (os.pathsep + python_path if python_path else ""),
        RMQ_STUB_MARKER_FILE=str(tmp_path / "marker"),
    )
    return env


@pytest.fixture
def marker(stub_env):
    """Lines of the stub's marker file; empty while the file does not exist."""
    path = Path(stub_env["RMQ_STUB_MARKER_FILE"])

    def read() -> list[str]:
        return path.read_text().splitlines() if path.exists() else []

    return read


@pytest.fixture
def processes():
    """Processes a test starts, killed and reaped when it ends.

    ``popen`` holds :class:`subprocess.Popen` objects; ``orphans`` holds PIDs of
    processes whose parent was killed and that this process reaps as their subreaper.
    A test that reaps an orphan itself removes it from ``orphans``; one still listed is
    signalled only while it is still this process's unreaped child, so a PID the system
    has handed to an unrelated process since is never touched.
    """
    started = {"popen": [], "orphans": []}
    yield started
    for proc in started["popen"]:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=_EXIT_TIMEOUT)
        if proc.stdin is not None:
            proc.stdin.close()
    for pid in started["orphans"]:
        try:
            reaped, _ = os.waitpid(pid, os.WNOHANG)
        except ChildProcessError:
            continue
        if reaped == 0:
            os.kill(pid, signal.SIGKILL)
            os.waitpid(pid, 0)


def _start_stub(
    env: dict[str, str], processes, stdin=subprocess.DEVNULL, start_new_session=False
):
    proc = subprocess.Popen(
        [sys.executable, str(_STUB)],
        env=env,
        cwd=_REPO_ROOT,
        stdin=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        start_new_session=start_new_session,
    )
    processes["popen"].append(proc)
    return proc, _Output(proc.stdout)


def _exit_code(proc: subprocess.Popen, output: _Output) -> int:
    try:
        return proc.wait(timeout=_EXIT_TIMEOUT)
    except subprocess.TimeoutExpired:
        pytest.fail(f"process still running {_EXIT_TIMEOUT}s later; output:\n{output.text}")


def _fields(line: str) -> dict[str, str]:
    """``key=value`` fields of a ``RUNNER_STUB`` line."""
    return dict(item.split("=", 1) for item in line.split() if "=" in item)


class TestStopBySignal:
    def test_process_starts_the_loop_and_leaves_with_code_0_on_sigterm(
        self, stub_env, marker, processes
    ):
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        proc, output = _start_stub(stub_env, processes)

        output.wait_for("RUNNER_STUB masked", _START_TIMEOUT)
        start = output.wait_for("RUNNER_STUB start", _START_TIMEOUT)
        proc.send_signal(signal.SIGTERM)

        assert _exit_code(proc, output) == 0
        # The main thread waits for the loop, which lingers after the stop, and leaves
        # only once it has returned.
        assert marker() == ["loop_started", "request_stop", "loop_ended", "exit 0"]
        assert "RMQ Watcher process received SIGTERM" in output.text
        assert (
            f"RMQ Watcher process {proc.pid} started for the scheduler process {os.getpid()}"
            in output.text
        )
        # Airflow's plugins and what they register are loaded before the loop thread
        # exists, not on first use from a pool worker.
        assert _fields(start)["plugins_loaded"] == "True"
        lines = output.text.splitlines()
        assert lines.index("RUNNER_STUB masked") < lines.index(start)

    def test_the_runner_alone_masks_the_signals_before_importing_airflow(
        self, stub_env, marker, processes
    ):
        """With no mask of the stub's own in front of it, the loop thread still has
        both stop signals blocked, and Airflow was not imported when the runner set the
        mask — so no thread Airflow starts on import can take a stop signal either."""
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        stub_env["RMQ_STUB_NO_PREMASK"] = "1"
        proc, output = _start_stub(stub_env, processes)

        start = output.wait_for("RUNNER_STUB start", _START_TIMEOUT)
        proc.send_signal(signal.SIGTERM)

        assert _exit_code(proc, output) == 0
        assert "RUNNER_STUB masked" not in output.text
        assert _fields(output.wait_for("RUNNER_STUB runner_mask", 0)) == {
            "airflow_imported": "False"
        }
        prepare = _fields(output.wait_for("RUNNER_STUB prepare", 0))
        assert prepare["thread"] == "MainThread"
        assert set(prepare["blocked"].split(",")) >= {"SIGTERM", "SIGINT"}
        assert set(_fields(start)["blocked"].split(",")) >= {"SIGTERM", "SIGINT"}
        assert marker() == ["loop_started", "request_stop", "loop_ended", "exit 0"]

    def test_sigterm_arriving_before_the_loop_starts_is_not_lost(
        self, stub_env, marker, processes
    ):
        """The stub holds after blocking the signals and before importing anything
        else, the test sends SIGTERM into that window and only then lets it go on: the
        signal waits in the kernel until the runner asks for it, which it does as soon
        as the preparation of Airflow is under way, and the loop never starts."""
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        stub_env["RMQ_STUB_HOLD"] = "1"
        proc, output = _start_stub(stub_env, processes, stdin=subprocess.PIPE)

        output.wait_for("RUNNER_STUB masked", _START_TIMEOUT)
        proc.send_signal(signal.SIGTERM)
        proc.stdin.write("go\n")
        proc.stdin.close()

        assert output.wait_for("RUNNER_STUB released", _START_TIMEOUT).endswith(
            "pending_sigterm=True"
        )
        assert _exit_code(proc, output) == 0
        assert marker() == ["exit 0"]
        assert "received SIGTERM while Airflow was being prepared" in output.text


class TestPreparationInTheMainThread:
    def test_a_plugin_that_sets_a_signal_handler_on_import_is_loaded(
        self, stub_env, marker, processes
    ):
        """A plugin that calls :func:`signal.signal` and :func:`signal.set_wakeup_fd`
        on import — which Python allows in the main thread alone — loads in the watcher
        process as it does in the scheduler, and SIGTERM still stops the loop."""
        plugins = Path(stub_env["AIRFLOW__CORE__PLUGINS_FOLDER"])
        plugins.mkdir()
        (plugins / "rmq_signal_plugin.py").write_text(
            "import signal\n"
            "from airflow.plugins_manager import AirflowPlugin\n"
            "signal.signal(signal.SIGUSR1, signal.getsignal(signal.SIGUSR1))\n"
            "signal.set_wakeup_fd(signal.set_wakeup_fd(-1))\n"
            "class RmqSignalPlugin(AirflowPlugin):\n"
            "    name = 'rmq_signal_plugin'\n"
        )
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        proc, output = _start_stub(stub_env, processes)

        start = output.wait_for("RUNNER_STUB start", _START_TIMEOUT)
        proc.send_signal(signal.SIGTERM)

        assert _exit_code(proc, output) == 0
        assert "rmq_signal_plugin" in _fields(start)["plugins"].split(","), output.text
        assert marker() == ["loop_started", "request_stop", "loop_ended", "exit 0"]


class TestStopDuringPreparation:
    def test_sigterm_during_a_hung_preparation_exits_at_once(
        self, stub_env, marker, processes
    ):
        """The preparation never returns, the way a plugin that hangs on import would
        leave it; SIGTERM still ends the process with code 0, well within the time the
        scheduler gives it before SIGKILL, and the loop never starts."""
        from airflow_provider_rmq.watcher import listener as listener_module

        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        stub_env["RMQ_STUB_HANG_PREPARE"] = "1"
        proc, output = _start_stub(stub_env, processes)

        output.wait_for("RUNNER_STUB prepare hung", _START_TIMEOUT)
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)

        assert _exit_code(proc, output) == 0
        assert time.monotonic() - started < listener_module._CHILD_STOP_TIMEOUT
        assert marker() == ["exit 0"]
        assert "received SIGTERM while Airflow was being prepared" in output.text


class TestSignalsToTheProcessGroup:
    """The stub runs in a session of its own, so its process group stands in for the
    scheduler's and a signal sent to the group reaches the stub alone, the way an init
    that forwards a signal to the scheduler's whole group reaches the watcher."""

    _GROUP_SIGNALS = (signal.SIGHUP, signal.SIGQUIT, signal.SIGUSR1, signal.SIGUSR2)

    def test_signals_the_scheduler_runs_on_leave_the_running_watcher_running(
        self, stub_env, marker, processes
    ):
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        proc, output = _start_stub(stub_env, processes, start_new_session=True)

        output.wait_for("RUNNER_STUB start", _START_TIMEOUT)
        for signum in self._GROUP_SIGNALS:
            os.killpg(proc.pid, signum)

        assert proc.poll() is None
        os.killpg(proc.pid, signal.SIGTERM)
        assert _exit_code(proc, output) == 0
        assert marker() == ["loop_started", "request_stop", "loop_ended", "exit 0"]
        assert "RMQ Watcher process received SIGTERM" in output.text

    def test_signals_the_scheduler_runs_on_leave_a_hung_preparation_running(
        self, stub_env, marker, processes
    ):
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        stub_env["RMQ_STUB_HANG_PREPARE"] = "1"
        proc, output = _start_stub(stub_env, processes, start_new_session=True)

        output.wait_for("RUNNER_STUB prepare hung", _START_TIMEOUT)
        for signum in self._GROUP_SIGNALS:
            os.killpg(proc.pid, signum)

        assert proc.poll() is None
        os.killpg(proc.pid, signal.SIGTERM)
        assert _exit_code(proc, output) == 0
        assert marker() == ["exit 0"]
        assert "received SIGTERM while Airflow was being prepared" in output.text


class TestHardExitDeadline:
    """A way out that blocks for good is ended by the kernel's SIGALRM once the
    deadline armed at the stop is up. The stub shortens the deadline to a few seconds."""

    def test_a_database_close_that_never_returns_is_ended_by_the_deadline(
        self, stub_env, marker, processes
    ):
        """The loop stops as asked, then closing the database connections hangs: the
        process is ended by SIGALRM within the deadline of the SIGTERM."""
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        stub_env["RMQ_STUB_HANG_EXIT"] = "1"
        stub_env["RMQ_STUB_HARD_EXIT_DEADLINE"] = str(_SHORT_DEADLINE)
        proc, output = _start_stub(stub_env, processes)

        output.wait_for("RUNNER_STUB start", _START_TIMEOUT)
        started = time.monotonic()
        proc.send_signal(signal.SIGTERM)

        assert _exit_code(proc, output) == -signal.SIGALRM, output.text
        elapsed = time.monotonic() - started
        assert _SHORT_DEADLINE - 1 <= elapsed < _SHORT_DEADLINE + 5
        output.wait_for("RUNNER_STUB close hung", 0)
        assert "RMQ Watcher process received SIGTERM" in output.text
        assert marker() == ["loop_started", "request_stop", "loop_ended"]

    def test_a_stalled_stderr_during_a_hung_preparation_is_ended_by_the_deadline(
        self, stub_env, marker, processes
    ):
        """stderr is a full pipe nobody reads and the preparation hangs: the watch thread
        takes SIGTERM and blocks writing the line saying so, and SIGALRM ends the
        process all the same."""
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        stub_env["RMQ_STUB_STALL_STDERR"] = "1"
        stub_env["RMQ_STUB_HANG_PREPARE"] = "1"
        stub_env["RMQ_STUB_HARD_EXIT_DEADLINE"] = str(_SHORT_DEADLINE)
        proc, output = _start_stub(stub_env, processes)

        output.wait_for("RUNNER_STUB stderr stalled", _START_TIMEOUT)
        output.wait_for("RUNNER_STUB prepare hung", _START_TIMEOUT)
        proc.send_signal(signal.SIGTERM)

        assert _exit_code(proc, output) == -signal.SIGALRM, output.text
        assert "received SIGTERM while Airflow was being prepared" not in output.text
        assert marker() == []


class TestParentDeath:
    @pytest.fixture
    def subreaper(self):
        """Make this process the reaper of the orphans its descendants leave.

        Requested ahead of ``processes``, so it is still in place while that fixture
        reaps the orphans.
        """
        _set_child_subreaper(True)
        yield
        _set_child_subreaper(False)

    @staticmethod
    def _start_through_an_intermediate(stub_env, processes):
        """Start the stub from an intermediate process that hands over its own PID as
        the scheduler's, and return the intermediate, the output both share and the
        stub's PID. The stub is listed among the orphans this process reaps."""
        intermediate_code = (
            "import os, subprocess, sys, threading\n"
            "env = dict(os.environ)\n"
            f"env[{runner.PARENT_PID_ENV!r}] = str(os.getpid())\n"
            f"child = subprocess.Popen([sys.executable, {str(_STUB)!r}], env=env, "
            "stdin=subprocess.DEVNULL)\n"
            "print(f'RUNNER_STUB_INTERMEDIATE child={child.pid}', flush=True)\n"
            "threading.Event().wait()\n"
        )
        intermediate = subprocess.Popen(
            [sys.executable, "-c", intermediate_code],
            env=stub_env,
            cwd=_REPO_ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        processes["popen"].append(intermediate)
        # The stub writes to the same pipe, so the output closes once both have exited.
        output = _Output(intermediate.stdout)
        stub_pid = int(output.wait_for("RUNNER_STUB_INTERMEDIATE", _START_TIMEOUT).split("=")[1])
        processes["orphans"].append(stub_pid)
        return intermediate, output, stub_pid

    def test_killing_the_parent_stops_the_watcher_process(
        self, subreaper, stub_env, marker, processes
    ):
        """The intermediate process is killed with SIGKILL, which runs none of its
        code. The stub is told by the kernel alone: the loop here never checks its
        parent."""
        intermediate, output, stub_pid = self._start_through_an_intermediate(
            stub_env, processes
        )
        output.wait_for("RUNNER_STUB start", _START_TIMEOUT)

        intermediate.kill()
        intermediate.wait(timeout=_EXIT_TIMEOUT)
        output.wait_closed(_EXIT_TIMEOUT)

        assert marker() == ["loop_started", "request_stop", "loop_ended", "exit 0"]
        assert "RMQ Watcher process received SIGTERM" in output.text
        _, status = os.waitpid(stub_pid, 0)
        processes["orphans"].remove(stub_pid)
        assert os.waitstatus_to_exitcode(status) == 0

    def test_killing_the_parent_during_a_hung_preparation_stops_the_watcher_process(
        self, subreaper, stub_env, marker, processes
    ):
        """The preparation never returns and the loop never starts; the scheduler dies
        with SIGKILL, so no one is left to follow a SIGTERM with SIGKILL. The watcher
        process still ends on its own, with code 0."""
        stub_env["RMQ_STUB_HANG_PREPARE"] = "1"
        intermediate, output, stub_pid = self._start_through_an_intermediate(
            stub_env, processes
        )
        output.wait_for("RUNNER_STUB prepare hung", _START_TIMEOUT)

        intermediate.kill()
        intermediate.wait(timeout=_EXIT_TIMEOUT)
        output.wait_closed(_EXIT_TIMEOUT)

        assert marker() == ["exit 0"]
        assert "while Airflow was being prepared — exiting" in output.text
        _, status = os.waitpid(stub_pid, 0)
        processes["orphans"].remove(stub_pid)
        assert os.waitstatus_to_exitcode(status) == 0

    def test_killing_the_parent_while_the_way_out_hangs_leaves_no_orphan(
        self, subreaper, stub_env, marker, processes
    ):
        """The scheduler dies with SIGKILL, so no one is left to follow its SIGTERM with
        SIGKILL, and the watcher's way out then hangs on the database: SIGALRM ends it."""
        stub_env["RMQ_STUB_HANG_EXIT"] = "1"
        stub_env["RMQ_STUB_HARD_EXIT_DEADLINE"] = str(_SHORT_DEADLINE)
        intermediate, output, stub_pid = self._start_through_an_intermediate(
            stub_env, processes
        )
        output.wait_for("RUNNER_STUB start", _START_TIMEOUT)

        intermediate.kill()
        intermediate.wait(timeout=_EXIT_TIMEOUT)
        output.wait_closed(_EXIT_TIMEOUT)

        _, status = os.waitpid(stub_pid, 0)
        processes["orphans"].remove(stub_pid)
        assert os.waitstatus_to_exitcode(status) == -signal.SIGALRM, output.text
        output.wait_for("RUNNER_STUB close hung", 0)
        assert marker() == ["loop_started", "request_stop", "loop_ended"]


class TestNoGrandchild:
    def test_on_starting_in_the_watcher_process_starts_nothing(
        self, stub_env, marker, processes
    ):
        """``on_starting`` for a scheduler job, sent through Airflow's listener manager
        from inside the watcher process, reaches the plugin's listener and starts no
        process of its own."""
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        stub_env[runner.CHILD_ENV_MARKER] = "1"
        stub_env["RMQ_STUB_CALL_ON_STARTING"] = "1"
        proc, output = _start_stub(stub_env, processes)

        output.wait_for("RUNNER_STUB on_starting done", _START_TIMEOUT)
        children = _children_of(proc.pid)
        proc.send_signal(signal.SIGTERM)

        assert "RMQWatcherListener.on_starting in the watcher process" in output.text
        assert children == []
        assert _exit_code(proc, output) == 0
        assert marker() == ["loop_started", "request_stop", "loop_ended", "exit 0"]


class TestStartedByTheListener:
    @pytest.mark.parametrize("in_site", [True, False], ids=["site-packages", "plugins"])
    def test_the_listener_starts_and_stops_a_real_watcher_process(
        self, stub_env, marker, processes, monkeypatch, tmp_path, in_site
    ):
        """The whole path in the scheduler: ``_spawn_child`` builds the command line
        and the environment and starts the process, the runner inside it reaches the
        loop, and ``before_stopping`` stops it with SIGTERM well within its timeout —
        for a package in site-packages and for one in the plugins folder.

        The scheduler's working directory holds ``runpy.py``, ``logging.py`` and a
        package named like this one, and the watcher process imports none of them.

        The stub takes the runner's place as the module the listener starts;
        ``PYTHONPATH`` makes it importable. Its stdout goes to a pipe instead of this
        process's, so the test can wait for its lines.
        """
        from airflow_provider_rmq.watcher import listener as listener_module

        cwd = tmp_path / "scheduler_cwd"
        (cwd / "airflow_provider_rmq").mkdir(parents=True)
        for relative in ("runpy.py", "logging.py", "airflow_provider_rmq/__init__.py"):
            (cwd / relative).write_text(
                f"print('SHADOWED {relative}', flush=True)\nraise SystemExit(3)\n"
            )
        monkeypatch.chdir(cwd)
        env = dict(stub_env)
        env["PYTHONPATH"] = str(_STUB.parent)
        monkeypatch.setattr(listener_module, "_RUNNER_MODULE", _STUB.stem)
        package_root = os.path.realpath(_REPO_ROOT)
        monkeypatch.setattr(
            listener_module,
            "_site_directories",
            lambda: {package_root} if in_site else set(),
        )
        monkeypatch.setattr(
            listener_module.subprocess,
            "Popen",
            functools.partial(
                subprocess.Popen, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True
            ),
        )
        listener = listener_module.RMQWatcherListener()

        with patch.dict(os.environ, env, clear=True):
            listener._spawn_child()
        child = listener._child
        assert child is not None
        processes["popen"].append(child)
        output = _Output(child.stdout)
        assert child.args[1] == "-c"

        output.wait_for("RUNNER_STUB start", _START_TIMEOUT)
        started = time.monotonic()
        listener.before_stopping(None)
        elapsed = time.monotonic() - started

        assert child.returncode == 0, output.text
        assert elapsed < listener_module._CHILD_STOP_TIMEOUT
        assert marker() == ["loop_started", "request_stop", "loop_ended", "exit 0"]
        assert "SHADOWED" not in output.text
