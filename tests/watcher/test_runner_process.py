"""Integration tests of the watcher process: the real runner in a real interpreter.

Each test starts ``tests/watcher/runner_stub.py`` — :func:`runner.main` around a loop
that only waits for the stop event — with :class:`subprocess.Popen` and drives it with
real signals: the signal mask, ``sigtimedwait``, the parent-death signal and
``os._exit`` all run as they do in production. The stub imports Airflow against a
throwaway ``AIRFLOW_HOME`` and a SQLite file inside it, and the loop never reaches the
database or a broker.

The stub reports through ``RUNNER_STUB ...`` lines on stdout, read by a thread that
lets a test wait for a given line, and through a marker file that outlives the process.
Every process a test starts is killed and reaped when the test ends, whatever its
outcome.
"""
from __future__ import annotations

import ctypes
import os
import signal
import subprocess
import sys
import threading
from pathlib import Path

import pytest

from airflow_provider_rmq.watcher import runner

pytestmark = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="the watcher process relies on Linux signals and prctl",
)

_STUB = Path(__file__).with_name("runner_stub.py")
_REPO_ROOT = Path(__file__).resolve().parents[2]

#: Seconds a stub gets to reach its loop: the runner imports Airflow on the way.
_START_TIMEOUT = 60.0
#: Seconds a stub gets to leave once it has been asked to.
_EXIT_TIMEOUT = 30.0

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
    """
    started: dict[str, list] = {"popen": [], "orphans": []}
    yield started
    for proc in started["popen"]:
        if proc.poll() is None:
            proc.kill()
        proc.wait(timeout=_EXIT_TIMEOUT)
        if proc.stdin is not None:
            proc.stdin.close()
    for pid in started["orphans"]:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        try:
            os.waitpid(pid, 0)
        except ChildProcessError:
            pass


def _start_stub(env: dict[str, str], processes, stdin=subprocess.DEVNULL):
    proc = subprocess.Popen(
        [sys.executable, str(_STUB)],
        env=env,
        cwd=_REPO_ROOT,
        stdin=stdin,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    processes["popen"].append(proc)
    return proc, _Output(proc.stdout)


def _exit_code(proc: subprocess.Popen, output: _Output) -> int:
    try:
        return proc.wait(timeout=_EXIT_TIMEOUT)
    except subprocess.TimeoutExpired:
        pytest.fail(f"process still running {_EXIT_TIMEOUT}s later; output:\n{output.text}")


class TestStopBySignal:
    def test_process_starts_the_loop_and_leaves_with_code_0_on_sigterm(
        self, stub_env, marker, processes
    ):
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        proc, output = _start_stub(stub_env, processes)

        output.wait_for("RUNNER_STUB masked", _START_TIMEOUT)
        output.wait_for("RUNNER_STUB start", _START_TIMEOUT)
        proc.send_signal(signal.SIGTERM)

        assert _exit_code(proc, output) == 0
        assert marker() == ["request_stop", "exit 0"]
        assert "RMQ Watcher process received SIGTERM" in output.text
        lines = output.text.splitlines()
        assert lines.index("RUNNER_STUB masked") < lines.index("RUNNER_STUB start")

    def test_sigterm_arriving_before_the_loop_starts_is_not_lost(
        self, stub_env, marker, processes
    ):
        """The stub holds after blocking the signals and before importing anything
        else, the test sends SIGTERM into that window and only then lets it go on: the
        signal waits in the kernel until the runner asks for it."""
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
        assert marker() == ["request_stop", "exit 0"]


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

    def test_killing_the_parent_stops_the_watcher_process(
        self, subreaper, stub_env, marker, processes
    ):
        """An intermediate process starts the stub with its own PID as the scheduler's
        and is killed with SIGKILL, which runs none of its code. The stub is told by
        the kernel alone: the loop here never checks its parent."""
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
        output.wait_for("RUNNER_STUB start", _START_TIMEOUT)

        intermediate.kill()
        intermediate.wait(timeout=_EXIT_TIMEOUT)
        output.wait_closed(_EXIT_TIMEOUT)

        assert marker() == ["request_stop", "exit 0"]
        assert "RMQ Watcher process received SIGTERM" in output.text
        _, status = os.waitpid(stub_pid, 0)
        assert os.waitstatus_to_exitcode(status) == 0


class TestNoGrandchild:
    @pytest.mark.parametrize(
        "lazy_load_plugins", ["True", "False"], ids=["lazy-plugins", "plugins-on-import"]
    )
    def test_on_starting_in_the_watcher_process_starts_nothing(
        self, stub_env, marker, processes, lazy_load_plugins
    ):
        """``on_starting`` for a scheduler job, sent through Airflow's listener manager
        from inside the watcher process, reaches the plugin's listener and starts no
        process of its own."""
        stub_env[runner.PARENT_PID_ENV] = str(os.getpid())
        stub_env[runner.CHILD_ENV_MARKER] = "1"
        stub_env["RMQ_STUB_CALL_ON_STARTING"] = "1"
        stub_env["RMQ_STUB_LOG_POPEN"] = "1"
        stub_env["AIRFLOW__CORE__LAZY_LOAD_PLUGINS"] = lazy_load_plugins
        proc, output = _start_stub(stub_env, processes)

        done = output.wait_for("RUNNER_STUB on_starting done", _START_TIMEOUT)
        children = _children_of(proc.pid)
        proc.send_signal(signal.SIGTERM)

        fields = dict(item.split("=") for item in done.split()[3:])
        assert fields["plugins_loaded_before"] == str(lazy_load_plugins == "False")
        assert int(fields["watcher_listeners"]) >= 1
        assert children == []
        # Airflow may run short-lived tools of its own on the way; none of them is the
        # watcher's entry point.
        spawned = [line for line in marker() if line.startswith("popen")]
        assert not [line for line in spawned if "airflow_provider_rmq.watcher.runner" in line]
        assert "RMQWatcherListener.on_starting in the watcher process" in output.text
        assert _exit_code(proc, output) == 0
        assert [line for line in marker() if not line.startswith("popen")] == [
            "request_stop",
            "exit 0",
        ]
