from __future__ import annotations

import ast
import asyncio
import contextlib
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

import airflow_provider_rmq
from airflow_provider_rmq.utils.amqp import call_with_timeout
from airflow_provider_rmq.utils.executor import BoundedExecutor
from airflow_provider_rmq.watcher import listener as listener_module
from airflow_provider_rmq.watcher.dag_file_scan import _READABLE_FILE_REMEDY
from airflow_provider_rmq.watcher.dag_folder_listing import (
    DagFolderListing,
    DagFolderListingError,
    list_dag_files,
)
from airflow_provider_rmq.watcher.listener import (
    CYCLE_TIMEOUT_VAR,
    RECONCILE_INTERVAL_VAR,
    RMQWatcherListener,
    _ABSENT_GRACE_CYCLES,
    _CHILD_REAP_TIMEOUT,
    _CHILD_STOP_TIMEOUT,
    _DELETED_ROWS_REMEMBERED,
    _INACTIVE_DAG_MISSES,
    _UNREADABLE_FILE_LIMIT,
    _UNTRUSTED_SCAN_LIMIT,
    _UntrustedScan,
    _read_active_subs,
    _read_settings,
)
from airflow_provider_rmq.watcher import models
from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    RMQWatcherHeartbeat,
    WatcherBase,
    WatcherIdentity,
    ensure_heartbeat_table,
    record_heartbeat_start,
    upsert_subscription,
)
from airflow_provider_rmq.watcher.runner import (
    CHILD_ENV_MARKER,
    PACKAGE_DIR_ENV,
    PARENT_PID_ENV,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_LISTENER_LOG = "airflow_provider_rmq.watcher.listener"


def _listing(*files: str) -> DagFolderListing:
    """A walk of the DAG folder that listed ``files`` and read every directory."""
    return DagFolderListing(set(files), {}, set())


def _make_session_ctx(existing_subs=None):
    """Return (ctx, session) where ctx is a mock context manager for WatcherSession."""
    session = MagicMock()
    session.query.return_value.filter_by.return_value.all.return_value = (
        existing_subs if existing_subs is not None else []
    )
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=session)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, session


# ---------------------------------------------------------------------------
# on_starting / before_stopping
# ---------------------------------------------------------------------------

class SchedulerJobRunner:
    """Airflow component the watcher starts in — recognised by its class name."""


class GunicornWebServer:
    """Airflow component the watcher stays out of, recognised the same way."""


def _blocked_by(release: threading.Event, result=None, calls: list | None = None):
    """A blocking call that holds its worker until ``release`` is set.

    :param result: What the call returns once it is released.
    :param calls: Collects one entry per invocation, for tests that count attempts.
    """
    def blocked(*_args, **_kwargs):
        if calls is not None:
            calls.append(1)
        release.wait(timeout=5)
        return result

    return blocked


class TestListenerLifecycle:
    def test_on_starting_with_scheduler_starts_the_watcher_process(self):
        listener = RMQWatcherListener()
        with patch.object(listener, "_spawn_child") as mock_start:
            listener.on_starting(SchedulerJobRunner())
        mock_start.assert_called_once()

    def test_on_starting_with_webserver_ignores(self):
        listener = RMQWatcherListener()
        with patch.object(listener, "_spawn_child") as mock_start:
            listener.on_starting(GunicornWebServer())
        mock_start.assert_not_called()

    def test_before_stopping_noop_when_not_started(self):
        listener = RMQWatcherListener()
        # No watcher process behind the listener — must not raise
        listener.before_stopping(MagicMock())

    def test_scheduler_component_name_matches(self):
        # Regression: ensure the substring check works for Airflow 2.9+ class name
        assert "Scheduler" in "SchedulerJobRunner"

    def test_on_starting_with_job_type_scheduler_starts_the_watcher_process(self):
        """Airflow 2.9+: component class is 'Job' but job_type='SchedulerJob'."""
        class Job:
            job_type = "SchedulerJob"

        listener = RMQWatcherListener()
        with patch.object(listener, "_spawn_child") as mock_start:
            listener.on_starting(Job())
        mock_start.assert_called_once()

    def test_on_starting_with_job_type_triggerer_ignores(self):
        """Triggerer job имеет job_type='TriggererJob' — не должен запускать watcher."""
        class Job:
            job_type = "TriggererJob"

        listener = RMQWatcherListener()
        with patch.object(listener, "_spawn_child") as mock_start:
            listener.on_starting(Job())
        mock_start.assert_not_called()

    def test_on_starting_airflow29_scheduler_command_in_stack(self):
        """Airflow 2.9+: component=Job(job_type=None), определяем шедулер по стеку вызовов."""
        class Job:
            job_type = None

        fake_frame = MagicMock()
        fake_frame.filename = "/opt/airflow/airflow/cli/commands/scheduler_command.py"

        listener = RMQWatcherListener()
        with patch("airflow_provider_rmq.watcher.listener.traceback.extract_stack",
                   return_value=[fake_frame]), \
             patch.object(listener, "_spawn_child") as mock_start:
            listener.on_starting(Job())
        mock_start.assert_called_once()

    def test_on_starting_airflow29_triggerer_command_not_scheduler(self):
        """Airflow 2.9+: component=Job(job_type=None) из triggerer — не запускает watcher."""
        class Job:
            job_type = None

        fake_frame = MagicMock()
        fake_frame.filename = "/opt/airflow/airflow/cli/commands/triggerer_command.py"

        listener = RMQWatcherListener()
        with patch("airflow_provider_rmq.watcher.listener.traceback.extract_stack",
                   return_value=[fake_frame]), \
             patch.object(listener, "_spawn_child") as mock_start:
            listener.on_starting(Job())
        mock_start.assert_not_called()

    def test_run_loop_restarts_after_crash(self):
        """L3: _run_loop должен перезапускать _main() после исключения."""
        listener = RMQWatcherListener()
        listener._stop_event = _instant_stop_event()
        call_count = {"n": 0}

        async def mock_main():
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("simulated crash")
            # На второй итерации останавливаем цикл
            listener._stop_event.set()

        with patch.object(listener, "_main", side_effect=mock_main):
            listener._run_loop()

        assert call_count["n"] == 2


# ---------------------------------------------------------------------------
# _scan_subscriptions — mtime-based incremental scanning
# ---------------------------------------------------------------------------

class TestScanSubscriptions:
    def _listener(self):
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        return listener

    def test_scan_subscriptions_first_run_parses_all_files(self):
        listener = self._listener()
        files = ["/dags/dag1.py", "/dags/dag2.py"]

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files", return_value=_listing(*files)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]) as mock_ex:
            listener._scan_subscriptions()

        assert mock_ex.call_count == 2

    def test_scan_subscriptions_unchanged_files_not_reparsed(self):
        listener = self._listener()
        files = ["/dags/dag1.py"]

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files", return_value=_listing(*files)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]) as mock_ex:
            listener._scan_subscriptions()  # first run
            listener._scan_subscriptions()  # same mtime — should NOT re-parse

        assert mock_ex.call_count == 1

    def test_scan_subscriptions_changed_file_reparsed(self):
        listener = self._listener()
        files = ["/dags/dag1.py"]
        mtime_values = iter([1000.0, 2000.0])

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files", return_value=_listing(*files)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=mtime_values), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]) as mock_ex:
            listener._scan_subscriptions()  # mtime=1000
            listener._scan_subscriptions()  # mtime=2000 → re-parse

        assert mock_ex.call_count == 2

    def test_scan_subscriptions_deleted_file_removed_from_cache(self):
        listener = self._listener()
        kept = "/dags/dag1.py"
        deleted = "/dags/dag2.py"

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing(kept, deleted)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]):
            listener._scan_subscriptions()

        assert deleted in listener._last_mtimes
        listener._read_failures[deleted] = 1

        # Second scan: one of the two files is gone, the other is still there
        with patch("airflow_provider_rmq.watcher.listener.list_dag_files", return_value=_listing(kept)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]):
            listener._scan_subscriptions()

        assert deleted not in listener._last_mtimes
        assert deleted not in listener._cached_subs
        assert deleted not in listener._read_failures
        assert kept in listener._last_mtimes

    def test_scan_subscriptions_finds_decorated_dags(self):
        listener = self._listener()
        expected = {
            "dag_id": "orders_dag",
            "queue_name": "orders",
            "conn_id": "rmq_default",
            "filter_data": {"filter_headers": {"type": "new_order"}},
        }

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing("/dags/orders.py")), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[expected]):
            result = listener._scan_subscriptions()

        assert expected in result

    def test_scan_subscriptions_ignores_dags_without_attribute(self):
        listener = self._listener()

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing("/dags/plain.py")), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]):
            result = listener._scan_subscriptions()

        assert result == []

    def test_extract_subscriptions_raises_on_ioerror(self):
        # No access to the file at all: the caller has to tell this apart from a
        # file that was read and did not parse, so the OSError propagates.
        listener = RMQWatcherListener()
        with pytest.raises(OSError):
            listener._extract_subscriptions_from_file("/nonexistent/broken.py")

    def test_mtime_recorded_once_patience_over_a_failed_parse_runs_out(self):
        # Regression for the permanent-DoS scenario: a DAG file whose parse keeps
        # failing must be re-parsed once per edit, not once per cycle. The parse
        # happened, so its mtime says something about the content and is recorded —
        # after the hold that a file caught mid-write is entitled to. The second file
        # is what keeps the listing itself a statement about the code, so the per-file
        # bookkeeping is what the cycles below exercise.
        listener = self._listener()
        file = "/dags/broken.py"
        healthy = "/dags/healthy.py"
        files = [file, healthy]

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files", return_value=_listing(*files)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file",
                          return_value=[{"dag_id": "d", "queue_name": "q"}]):
            listener._scan_subscriptions()

        def fake_extract(path):
            return None if path == file else [{"dag_id": "h", "queue_name": "hq"}]

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files", return_value=_listing(*files)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=2000.0), \
             patch.object(
                 listener, "_extract_subscriptions_from_file", side_effect=fake_extract
             ) as mock_ex:
            for _ in range(_UNREADABLE_FILE_LIMIT + 2):
                listener._scan_subscriptions()

        assert listener._last_mtimes[file] == 2000.0
        assert listener._cached_subs[file] == []
        # One call per cycle of the hold, one when patience ran out, none after:
        # the recorded mtime stops the re-parse.
        assert [c for c in mock_ex.call_args_list if c == call(file)] == \
            [call(file)] * (_UNREADABLE_FILE_LIMIT + 1)

    def test_a_file_the_parser_refuses_does_not_escape_the_scan(self, tmp_path):
        """The scan step answers with a result or with `_UntrustedScan` and nothing else.

        A ``ValueError`` out of ``ast.parse`` would leave the whole cycle — the sync,
        the consumer relaunches, the liveness check and the status writes — undone, on
        every cycle for as long as the file sits in the folder.
        """
        (tmp_path / "good.py").write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q')\n"
            "@dag(dag_id='good')\n"
            "def good(): pass\n"
        )
        (tmp_path / "binary.py").write_bytes(b"\x00\x01\x02\n")
        listener = RMQWatcherListener()

        with patch("airflow.settings.DAGS_FOLDER", str(tmp_path)):
            result = listener._scan_subscriptions()

        assert [s["dag_id"] for s in result] == ["good"]


# ---------------------------------------------------------------------------
# An unreadable DAG file holds its previous parse for a bounded number of cycles
# ---------------------------------------------------------------------------

#: The DAG file the cases below put through failures, and a successful parse of it.
_HELD_FILE = "/dags/orders.py"
_HELD_SUBS = [{"dag_id": "orders", "queue_name": "q"}]

#: A second file that always reads. Its presence is what keeps the listing a statement
#: about the code: a scan in which *every* file failed is distrusted as a whole, so
#: without it the per-file behaviour below could never be reached.
_HEALTHY_FILE = "/dags/other.py"
_HEALTHY_SUBS = [{"dag_id": "other", "queue_name": "oq"}]

#: Default for the ``result`` argument below: a successful parse. A literal default
#: cannot say it, because ``None`` is itself a meaningful answer.
_PARSED = object()


def _held_subs_of(result):
    """The scanned subscriptions that came from ``_HELD_FILE``."""
    return [s for s in result if s["dag_id"] == "orders"]


class TestUnreadableFileHold:
    def _listener(self):
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        return listener

    def _scan(self, listener, *, mtime=1000.0, mtime_error=None,
              result=_PARSED, error=None):
        """One scan cycle; returns ``(result, extraction mock)``.

        Only ``_HELD_FILE`` is subject to the arguments — ``_HEALTHY_FILE`` always
        stats and parses.
        """
        if result is _PARSED:
            result = list(_HELD_SUBS)

        def fake_getmtime(path):
            if path != _HELD_FILE:
                return 1000.0
            if mtime_error is not None:
                raise mtime_error
            return mtime

        def fake_extract(path):
            if path != _HELD_FILE:
                return list(_HEALTHY_SUBS)
            if error is not None:
                raise error
            return None if result is None else list(result)

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing(_HELD_FILE, _HEALTHY_FILE)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=fake_getmtime), \
             patch.object(listener, "_extract_subscriptions_from_file",
                          side_effect=fake_extract) as mock_ex:
            return listener._scan_subscriptions(), mock_ex

    def test_a_failed_parse_keeps_the_previous_subscriptions_for_one_cycle(self, caplog):
        listener = self._listener()
        self._scan(listener)

        with caplog.at_level(logging.WARNING):
            result, _ = self._scan(listener, mtime=2000.0, result=None)

        assert _held_subs_of(result) == _HELD_SUBS
        assert listener._last_mtimes[_HELD_FILE] == 1000.0
        assert "holding its previous parse" in caplog.text

    def test_a_file_that_healed_is_parsed_again_and_its_streak_is_cleared(self):
        listener = self._listener()
        self._scan(listener)
        self._scan(listener, mtime=2000.0, result=None)
        assert listener._read_failures[_HELD_FILE] == 1

        result, _ = self._scan(listener, mtime=2000.0)

        assert _held_subs_of(result) == _HELD_SUBS
        assert _HELD_FILE not in listener._read_failures
        assert listener._last_mtimes[_HELD_FILE] == 2000.0

    def test_a_stat_that_answers_again_gives_the_next_failure_its_full_patience(self):
        """The hold is one cycle of patience, not one per file for the process's life."""
        listener = self._listener()
        self._scan(listener)
        self._scan(listener, mtime_error=OSError("stale handle"))
        assert listener._read_failures[_HELD_FILE] == 1

        # The content did not change, so the file takes the unchanged-mtime fast path —
        # which is still a stat that answered.
        self._scan(listener, mtime=1000.0)
        assert _HELD_FILE not in listener._read_failures

        result, _ = self._scan(listener, mtime_error=OSError("stale handle"))
        assert _held_subs_of(result) == _HELD_SUBS

    def test_a_stat_that_keeps_failing_drops_the_remembered_mtime(self):
        listener = self._listener()
        self._scan(listener)

        for _ in range(_UNREADABLE_FILE_LIMIT):
            self._scan(listener, mtime_error=OSError("stale handle"))
            assert listener._cached_subs[_HELD_FILE] == _HELD_SUBS

        result, _ = self._scan(listener, mtime_error=OSError("stale handle"))
        assert _held_subs_of(result) == []
        assert listener._cached_subs[_HELD_FILE] == []
        assert _HELD_FILE not in listener._last_mtimes

        # Access is back and the content never changed: the very same mtime must
        # still lead to a re-read, or the file would count as subscription-less
        # until somebody edits it.
        result, mock_ex = self._scan(listener, mtime=1000.0)
        assert mock_ex.call_args_list == [call(_HELD_FILE)]
        assert _held_subs_of(result) == _HELD_SUBS

    def test_an_open_that_keeps_failing_drops_the_remembered_mtime(self):
        listener = self._listener()
        self._scan(listener)

        for _ in range(_UNREADABLE_FILE_LIMIT):
            self._scan(listener, mtime=2000.0, error=PermissionError("denied"))
            assert listener._cached_subs[_HELD_FILE] == _HELD_SUBS

        result, _ = self._scan(listener, mtime=2000.0, error=PermissionError("denied"))
        assert _held_subs_of(result) == []
        assert _HELD_FILE not in listener._last_mtimes

        # Permissions come back with the content untouched — mtime is still 2000.
        result, mock_ex = self._scan(listener, mtime=2000.0)
        assert mock_ex.call_args_list == [call(_HELD_FILE)]
        assert _held_subs_of(result) == _HELD_SUBS

    def test_a_file_that_stays_unreadable_is_reported_a_bounded_number_of_times(self, caplog):
        """Its mtime is dropped, so it is re-read every cycle — and would be re-logged."""
        listener = self._listener()
        self._scan(listener)
        for _ in range(_UNREADABLE_FILE_LIMIT + 1):
            self._scan(listener, mtime_error=OSError("stale handle"))

        caplog.clear()
        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG):
            for _ in range(5):
                self._scan(listener, mtime_error=OSError("stale handle"))

        assert [r for r in caplog.records if "dropping its subscriptions" in r.getMessage()] == []

    def test_content_the_parser_refuses_outright_answers_none(self, tmp_path):
        """A file caught mid-write, or a binary one under a .py name.

        ``ast.parse`` answers a null byte in the source with ``ValueError``, not with
        ``SyntaxError``, and the caller's whole contract is that the only thing this
        method raises is the ``OSError`` of a file that gave no access: anything else
        escapes the scan step and takes the reconcile cycle — consumer relaunches,
        liveness check and status writes with it — down every cycle the file is there.
        """
        dag_file = tmp_path / "half_written.py"
        dag_file.write_bytes(b"from airflow.decorators import dag\n\x00\x00\x00\n")
        listener = RMQWatcherListener()

        assert listener._extract_subscriptions_from_file(str(dag_file)) is None

    def test_content_the_parser_refuses_outright_is_logged(self, tmp_path, caplog):
        dag_file = tmp_path / "half_written.py"
        dag_file.write_bytes(b"x = 1\n\x00\n")
        listener = RMQWatcherListener()

        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG):
            listener._extract_subscriptions_from_file(str(dag_file))

        assert any("Failed to parse DAG file" in r.getMessage() for r in caplog.records), \
            [r.getMessage() for r in caplog.records]

    def test_undecodable_content_answers_none(self, tmp_path):
        dag_file = tmp_path / "latin1.py"
        dag_file.write_bytes(b"x = '\xff\xfe not utf-8'\n")
        listener = RMQWatcherListener()

        assert listener._extract_subscriptions_from_file(str(dag_file)) is None

    def test_undecodable_content_is_logged(self, tmp_path, caplog):
        dag_file = tmp_path / "latin1.py"
        dag_file.write_bytes(b"x = '\xff\xfe not utf-8'\n")
        listener = RMQWatcherListener()

        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG):
            listener._extract_subscriptions_from_file(str(dag_file))

        assert any("Failed to parse DAG file" in r.getMessage() for r in caplog.records), \
            [r.getMessage() for r in caplog.records]

    def test_source_is_decoded_the_way_an_import_decodes_it(self, tmp_path):
        """Bytes are handed to the parser as they are, so the BOM, a PEP 263
        declaration and non-UTF-8 bytes inside a comment (which Python's own import
        accepts) read exactly as the DAG processor reads them."""
        trigger = (
            b"from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            b"from airflow.decorators import dag\n"
            b"@rmq_trigger(queue='q')\n"
            b"@dag(dag_id='d')\n"
            b"def d(): pass\n"
        )
        variants = {
            "bom.py": b"\xef\xbb\xbf" + trigger,
            "latin1.py": b"# -*- coding: latin-1 -*-\nNAME = '\xe9'\n" + trigger,
            "comment.py": b"# \xff\xfe not utf-8\n" + trigger,
        }
        listener = RMQWatcherListener()
        for name, content in variants.items():
            dag_file = tmp_path / name
            dag_file.write_bytes(content)
            result = listener._extract_subscriptions_from_file(str(dag_file))
            assert [s["queue_name"] for s in result] == ["q"], name

    def test_a_parse_failure_and_an_io_failure_leave_different_mtimes(self):
        parsed, io = self._listener(), self._listener()
        self._scan(parsed)
        self._scan(io)

        for _ in range(_UNREADABLE_FILE_LIMIT + 1):
            self._scan(parsed, mtime=2000.0, result=None)
            self._scan(io, mtime=2000.0, error=OSError("no access"))

        # Content was read and did not parse — its mtime says something about it.
        assert parsed._last_mtimes[_HELD_FILE] == 2000.0
        # Nothing is known about the content — a remembered mtime would be a lie.
        assert _HELD_FILE not in io._last_mtimes

    def test_a_file_unreadable_from_the_very_first_cycle_gets_no_hold(self):
        parsed, io = self._listener(), self._listener()

        assert _held_subs_of(self._scan(parsed, result=None)[0]) == []
        assert parsed._cached_subs[_HELD_FILE] == []
        assert parsed._last_mtimes[_HELD_FILE] == 1000.0

        assert _held_subs_of(self._scan(io, error=OSError("no access"))[0]) == []
        assert io._cached_subs[_HELD_FILE] == []
        assert _HELD_FILE not in io._last_mtimes

    def test_a_deleted_file_leaves_nothing_behind_even_after_an_io_failure(self):
        """The I/O branch drops the mtime, and eviction reads more than that dict."""
        listener = self._listener()
        self._scan(listener)
        for _ in range(_UNREADABLE_FILE_LIMIT + 1):
            self._scan(listener, mtime_error=OSError("stale handle"))
        assert _HELD_FILE not in listener._last_mtimes
        assert _HELD_FILE in listener._cached_subs

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing(_HEALTHY_FILE)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0):
            listener._scan_subscriptions()

        assert _HELD_FILE not in listener._cached_subs
        assert _HELD_FILE not in listener._read_failures


# ---------------------------------------------------------------------------
# A listing in which no file could be read is not a statement about the code
# ---------------------------------------------------------------------------

class TestEveryFileUnreadable:
    _FILES = (_HELD_FILE, _HEALTHY_FILE)

    def _listener(self):
        """A listener whose cache holds one parse of every file of ``_FILES``."""
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing(*self._FILES)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file",
                          return_value=list(_HELD_SUBS)):
            listener._scan_subscriptions()
        return listener

    def _scan_all_stale(self, listener):
        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing(*self._FILES)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=OSError("stale handle")):
            return listener._scan_subscriptions()

    def test_a_listing_nobody_can_stat_raises_untrusted_scan(self):
        listener = self._listener()
        cached = {path: list(subs) for path, subs in listener._cached_subs.items()}

        with pytest.raises(_UntrustedScan, match="none of the 2 files"):
            self._scan_all_stale(listener)

        assert listener._cached_subs == cached
        assert listener._read_failures == {}

    def test_the_distrust_ends_and_the_keys_go_the_counted_miss_way(self):
        listener = self._listener()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_all_stale(listener)

        # Giving in still spends the per-file hold before the subscriptions go.
        assert self._scan_all_stale(listener) == _HELD_SUBS * len(self._FILES)
        assert self._scan_all_stale(listener) == []

    def test_one_file_that_still_reads_makes_the_listing_trustworthy(self):
        listener = self._listener()

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing(*self._FILES)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=lambda path: 2000.0 if path == _HEALTHY_FILE
                   else (_ for _ in ()).throw(OSError("stale handle"))), \
             patch.object(listener, "_extract_subscriptions_from_file",
                          return_value=list(_HEALTHY_SUBS)):
            listener._scan_subscriptions()

        assert listener._read_failures == {_HELD_FILE: 1}

    def test_a_listing_nobody_can_read_is_reported_and_then_left_alone(self, caplog):
        """A filesystem that is not coming back is a state told once, not every minute.

        The listing is believed, its files count as having no subscriptions, and from
        there the folder is in a steady state: a WARNING per cycle on it would drown the
        line that says a listing has just collapsed.
        """
        listener = self._listener()
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_all_stale(listener)
        self._scan_all_stale(listener)   # believed; the per-file hold is spent here
        self._scan_all_stale(listener)   # the subscriptions of both files go

        caplog.clear()
        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG):
            for _ in range(5):
                assert self._scan_all_stale(listener) == []

        assert [r.getMessage() for r in caplog.records] == []
        assert listener._untrusted_scan_streak == 0

    def _scan_all_unparseable(self, listener):
        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing(*self._FILES)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=2000.0), \
             patch.object(listener, "_extract_subscriptions_from_file",
                          return_value=None):
            return listener._scan_subscriptions()

    def test_a_listing_of_files_that_all_fail_to_parse_is_distrusted_too(self):
        """An aborted checkout truncates files: they read, they do not parse.

        A folder in which nothing parses any more is the filesystem mid-swap, not a
        statement that the installation has no subscriptions — and Airflow deactivates
        those DAGs within a minute or two, which is what would otherwise put every
        enabled row on the short-circuit path to deletion.
        """
        listener = self._listener()
        cached = {path: list(subs) for path, subs in listener._cached_subs.items()}

        with pytest.raises(_UntrustedScan, match="none of the 2 files"):
            self._scan_all_unparseable(listener)

        assert listener._cached_subs == cached
        assert listener._read_failures == {}

    def test_the_distrust_of_an_unparseable_listing_ends_at_the_limit(self):
        listener = self._listener()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_all_unparseable(listener)

        assert self._scan_all_unparseable(listener) == _HELD_SUBS * len(self._FILES)
        assert self._scan_all_unparseable(listener) == []

    def test_a_healthy_scan_resets_the_streak(self):
        listener = self._listener()
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_all_stale(listener)

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing(*self._FILES)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0):
            listener._scan_subscriptions()

        assert listener._untrusted_scan_streak == 0
        with pytest.raises(_UntrustedScan):
            self._scan_all_stale(listener)


# ---------------------------------------------------------------------------
# A listing that lost most of what read before is not a statement about the code
# ---------------------------------------------------------------------------

class TestCollapsedListing:
    """Partial failures: a slow NFS backend, a subtree that lost its permissions.

    The common shape of a filesystem in trouble is not that every file refuses at
    once — it is that most of them do, which the sync step would otherwise read as
    the installation having lost its subscriptions.
    """

    _FILES = tuple(f"/dags/dag{n}.py" for n in range(4))

    def _subs_of(self, path):
        return [{"dag_id": path, "queue_name": "q"}]

    def _listener(self):
        """A listener whose cache holds one parse of every file of ``_FILES``."""
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        self._scan(listener)
        return listener

    def _scan(self, listener, *, files=None, unreadable=(), mtime=1000.0):
        files = list(self._FILES if files is None else files)
        unreadable = set(unreadable)

        def fake_getmtime(path):
            if path in unreadable:
                raise OSError("stale handle")
            return mtime

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files", return_value=_listing(*files)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=fake_getmtime), \
             patch.object(listener, "_extract_subscriptions_from_file",
                          side_effect=self._subs_of):
            return listener._scan_subscriptions()

    def test_a_listing_that_mostly_stopped_reading_is_distrusted(self):
        listener = self._listener()
        cached = {path: list(subs) for path, subs in listener._cached_subs.items()}

        with pytest.raises(_UntrustedScan, match="only 1 of the 4 files"):
            self._scan(listener, unreadable=self._FILES[1:], mtime=2000.0)

        assert listener._cached_subs == cached
        assert listener._read_failures == {}

    def test_a_broken_file_among_many_leaves_the_listing_trusted(self):
        """One file is about the code, and the code is what the scan is there to read."""
        listener = self._listener()

        result = self._scan(listener, unreadable=self._FILES[:1], mtime=2000.0)

        assert len(result) == len(self._FILES)
        assert listener._read_failures == {self._FILES[0]: 1}

    def test_the_distrust_of_a_collapsed_listing_ends_at_the_limit(self):
        listener = self._listener()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan(listener, unreadable=self._FILES[1:], mtime=2000.0)

        # Giving in still spends the per-file hold before the subscriptions go.
        held = self._scan(listener, unreadable=self._FILES[1:], mtime=2000.0)
        assert len(held) == len(self._FILES)
        dropped = self._scan(listener, unreadable=self._FILES[1:], mtime=2000.0)
        assert [s["dag_id"] for s in dropped] == [self._FILES[0]]

    def test_a_decline_slower_than_the_share_is_distrusted_too(self):
        """The share is measured against what the folder held, not against last cycle.

        A count every trusted listing rewrote would follow a folder unmounting piece by
        piece all the way down and call every step of the way trustworthy, while the
        sync step counted misses against everything that had gone.
        """
        listener = self._listener()

        assert len(self._scan(listener, files=self._FILES[:3], mtime=2000.0)) == 3
        assert len(self._scan(listener, files=self._FILES[:2], mtime=3000.0)) == 2
        with pytest.raises(_UntrustedScan, match="listed only 1 files"):
            self._scan(listener, files=self._FILES[:1], mtime=4000.0)

    def test_giving_in_to_a_shrink_leaves_the_next_collapse_its_whole_grace(self):
        """Believing a listing is the end of one episode of distrust, not of them all.

        A folder that legitimately loses most of its files is given in to after the
        limit — and the next collapse, of a filesystem that really is failing, has to
        meet the same limit rather than a budget the shrink before it spent.
        """
        listener = self._listener()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan(listener, files=self._FILES[:1], mtime=2000.0)
        assert len(self._scan(listener, files=self._FILES[:1], mtime=2000.0)) == 1
        assert listener._untrusted_scan_streak == 0

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan(listener, files=self._FILES[:1],
                           unreadable=self._FILES[:1], mtime=3000.0)

    def test_a_folder_that_grew_is_trusted(self):
        """The share is measured against what the folder held, not against the listing."""
        listener = self._listener()
        grown = list(self._FILES) + [f"/dags/new{n}.py" for n in range(10)]

        result = self._scan(listener, files=grown, mtime=2000.0)

        assert len(result) == len(grown)
        assert listener._untrusted_scan_streak == 0


# ---------------------------------------------------------------------------
# An empty listing is distrusted for a bounded number of cycles
# ---------------------------------------------------------------------------

class TestEmptyScanDistrust:
    def _warm_listener(self, files=("/dags/dag1.py",)):
        """A listener whose cache holds one parse of every file in ``files``."""
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        subs = [{"dag_id": "d", "queue_name": "q"}]
        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing(*files)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=subs):
            listener._scan_subscriptions()
        return listener

    def _scan_empty(self, listener):
        with patch("airflow_provider_rmq.watcher.listener.list_dag_files", return_value=_listing()):
            return listener._scan_subscriptions()

    def test_empty_listing_between_two_full_ones_is_untrusted(self):
        listener = self._warm_listener()
        mtimes = dict(listener._last_mtimes)
        cached = {path: list(subs) for path, subs in listener._cached_subs.items()}

        with pytest.raises(_UntrustedScan, match="listed no files"):
            self._scan_empty(listener)

        assert listener._last_mtimes == mtimes
        assert listener._cached_subs == cached

    def test_emptiness_becomes_the_truth_after_the_limit(self):
        listener = self._warm_listener()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)

        assert self._scan_empty(listener) == []
        assert listener._last_mtimes == {}
        assert listener._cached_subs == {}

        # A folder that stays empty stays believed: nothing is known any more, so
        # the distrust has nothing to compare the emptiness against.
        assert self._scan_empty(listener) == []

    def test_a_non_empty_listing_resets_the_streak(self):
        listener = self._warm_listener()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)
        assert listener._untrusted_scan_streak == _UNTRUSTED_SCAN_LIMIT

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing("/dags/dag1.py")), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0):
            listener._scan_subscriptions()

        assert listener._untrusted_scan_streak == 0
        # The whole grace period is available again.
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)

    def test_empty_listing_on_a_cold_cache_is_distrusted_too(self):
        """A scheduler restarting into a folder git-sync has not populated yet.

        The folder exists and lists, so nothing above this check objects, and the
        cache of a process that has just started remembers no files to compare the
        emptiness against — believing it at once would count a miss against every
        row in the table.
        """
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan, match="listed no files"):
                self._scan_empty(listener)

    def test_an_installation_with_no_dag_files_at_all_stays_workable(self):
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)

        assert self._scan_empty(listener) == []
        assert self._scan_empty(listener) == []

    def test_a_believed_empty_folder_buys_no_trust_for_a_listing_nothing_read_of(self):
        """A share of zero is met by everything, an unreadable listing included.

        The mark an empty folder leaves once it is believed is zero, and files that
        appear in it and refuse every read are the folder collapsing like any other:
        the whole budget stands between such a listing and the miss counters of the
        rows in the table.
        """
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)
        assert self._scan_empty(listener) == []

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing("/dags/dag1.py")), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=OSError("stale handle")):
            for _ in range(_UNTRUSTED_SCAN_LIMIT):
                with pytest.raises(_UntrustedScan, match="none of the 1 files"):
                    listener._scan_subscriptions()

    def test_an_io_failure_that_emptied_the_mtimes_does_not_buy_trust(self):
        """The distrust cannot be keyed on a dict a failing filesystem erases.

        What it is keyed on is the count of the last scan the cycle trusted, which a
        stat that never answers cannot raise — the run of failures below leaves the
        remembered mtimes empty, and the file reading again is what ends that run and
        gives the emptiness after it the whole budget once more.
        """
        listener = self._warm_listener()
        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing("/dags/dag1.py")), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=OSError("stale handle")):
            for _ in range(_UNTRUSTED_SCAN_LIMIT + _UNREADABLE_FILE_LIMIT + 1):
                try:
                    listener._scan_subscriptions()
                except _UntrustedScan:
                    pass
        assert listener._last_mtimes == {}

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing("/dags/dag1.py")), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]):
            listener._scan_subscriptions()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan, match="listed no files"):
                self._scan_empty(listener)

    def _fail_the_folder(self, listener):
        listener._get_dags_folder = MagicMock(
            side_effect=_UntrustedScan("the DAG folder cannot be listed")
        )
        with pytest.raises(_UntrustedScan, match="cannot be listed"):
            listener._scan_subscriptions()
        listener._get_dags_folder = MagicMock(return_value="/dags")

    def test_a_folder_level_outage_leaves_the_budget_where_it_was(self):
        """The budget is spent by listings, and an outage produces none of them.

        A folder that vanished under a git-sync swap, or lost its read permission, is
        distrusted without any limit — and the listings before and after it are one run
        as far as the budget goes.
        """
        listener = self._warm_listener()
        for _ in range(5):
            self._fail_the_folder(listener)

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan, match="listed no files"):
                self._scan_empty(listener)

    def test_a_folder_flickering_between_unlistable_and_empty_still_converges(self):
        """The bound on the distrust has to hold whatever the folder alternates between.

        An outage that refilled the budget would let this pattern hold the distrust for
        as long as it lasts, and the sync step — the deletions and every new
        subscription with them — would never run again on such an installation.
        """
        listener = self._warm_listener()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            self._fail_the_folder(listener)
            with pytest.raises(_UntrustedScan, match="listed no files"):
                self._scan_empty(listener)

        self._fail_the_folder(listener)
        assert self._scan_empty(listener) == []

    def test_a_believed_emptiness_is_reported_and_then_left_alone(self, caplog):
        """An installation with no DAG files at all is a supported, quiet one."""
        listener = self._warm_listener()
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)
        self._scan_empty(listener)   # the emptiness is believed, with a WARNING

        caplog.clear()
        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG):
            for _ in range(5):
                assert self._scan_empty(listener) == []

        assert [r.getMessage() for r in caplog.records] == []
        assert listener._untrusted_scan_streak == 0

    def test_a_file_appearing_in_an_empty_folder_gets_the_whole_grace_back(self):
        """The folder is known to hold one file again, and losing it is news again."""
        listener = self._warm_listener()
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)
        self._scan_empty(listener)

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing("/dags/dag1.py")), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=2000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]):
            listener._scan_subscriptions()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan, match="listed no files"):
                self._scan_empty(listener)

    def test_the_warning_names_the_folder_when_emptiness_is_accepted(self, caplog):
        listener = self._warm_listener()
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)

        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG):
            self._scan_empty(listener)

        assert "/dags" in caplog.text


# ---------------------------------------------------------------------------
# _get_dags_folder — the DAG folder is checked before the scan is believed
# ---------------------------------------------------------------------------

class TestGetDagsFolder:
    def test_readable_folder_is_returned(self, tmp_path):
        listener = RMQWatcherListener()
        (tmp_path / "dag1.py").write_text("")

        with patch("airflow.settings.DAGS_FOLDER", str(tmp_path)):
            assert listener._get_dags_folder() == str(tmp_path)

    def test_empty_folder_is_returned_too(self, tmp_path):
        # An honestly empty directory is the truth about the code and must reach the walk.
        listener = RMQWatcherListener()

        with patch("airflow.settings.DAGS_FOLDER", str(tmp_path)):
            assert listener._get_dags_folder() == str(tmp_path)

    def test_unreadable_setting_raises_untrusted_scan(self, monkeypatch):
        import airflow.settings

        listener = RMQWatcherListener()
        monkeypatch.delattr(airflow.settings, "DAGS_FOLDER")

        with pytest.raises(_UntrustedScan, match="unreadable"):
            listener._get_dags_folder()

    def test_empty_setting_raises_untrusted_scan(self):
        listener = RMQWatcherListener()

        with patch("airflow.settings.DAGS_FOLDER", ""), \
             pytest.raises(_UntrustedScan, match="empty"):
            listener._get_dags_folder()

    def test_empty_setting_keeps_the_walk_from_running(self):
        # A setting that names no folder is turned away before the folder is walked.
        listener = RMQWatcherListener()

        with patch("airflow.settings.DAGS_FOLDER", ""), \
             patch("airflow_provider_rmq.watcher.listener.list_dag_files") as mock_listing, \
             pytest.raises(_UntrustedScan):
            listener._scan_subscriptions()

        mock_listing.assert_not_called()

    def test_missing_directory_raises_untrusted_scan(self, tmp_path):
        listener = RMQWatcherListener()
        missing = str(tmp_path / "swapped-away")

        with patch("airflow.settings.DAGS_FOLDER", missing), \
             pytest.raises(_UntrustedScan, match="not a directory"):
            listener._get_dags_folder()

    @pytest.mark.skipif(
        os.geteuid() == 0,
        reason="root reads a directory whatever its mode says, so there is no unlistable folder to build",
    )
    @pytest.mark.parametrize("trailing", ["", os.sep], ids=["plain", "trailing-separator"])
    def test_unlistable_directory_raises_untrusted_scan(self, tmp_path, trailing):
        # os.path.isdir says True for a directory with no read permission, and the
        # walk lists no file of it: the folder is unreadable, not empty, and the scan
        # turns it away on the walk's report of its root.
        folder = tmp_path / "dags"
        folder.mkdir()
        (folder / "dag1.py").write_text("")
        folder.chmod(0o111)
        listener = RMQWatcherListener()
        dags_folder = str(folder) + trailing

        try:
            assert os.path.isdir(dags_folder)
            listing = list_dag_files(dags_folder)
            assert listing.files == set()
            assert set(listing.unreadable_dirs) == {dags_folder}
            with patch("airflow.settings.DAGS_FOLDER", dags_folder):
                assert listener._get_dags_folder() == dags_folder
                # The distrust has no limit: an unreadable folder never becomes truth.
                for _ in range(5):
                    with pytest.raises(_UntrustedScan, match="cannot be listed"):
                        listener._scan_subscriptions()
            assert listener._untrusted_scan_streak == 0
            assert listener._unreadable_dirs == set()
        finally:
            folder.chmod(0o755)

    @pytest.mark.skipif(
        os.geteuid() == 0,
        reason="root reads a directory whatever its mode says, so there is no unlistable folder to build",
    )
    def test_unlistable_directory_raises_with_a_warm_cache_too(self, tmp_path):
        folder = tmp_path / "dags"
        folder.mkdir()
        (folder / "dag1.py").write_text("")
        listener = RMQWatcherListener()

        with patch("airflow.settings.DAGS_FOLDER", str(folder)):
            listener._scan_subscriptions()
            assert listener._last_mtimes

            folder.chmod(0o111)
            try:
                for _ in range(5):
                    with pytest.raises(_UntrustedScan, match="cannot be listed"):
                        listener._scan_subscriptions()
            finally:
                folder.chmod(0o755)

    @pytest.mark.skipif(
        os.geteuid() == 0,
        reason="root reads a directory whatever its mode says, so there is no unlistable folder to build",
    )
    def test_untrusted_folder_evicts_nothing_from_the_cache(self, tmp_path):
        folder = tmp_path / "dags"
        folder.mkdir()
        dag_file = folder / "dag1.py"
        dag_file.write_text("")
        listener = RMQWatcherListener()

        with patch("airflow.settings.DAGS_FOLDER", str(folder)), \
             patch.object(listener, "_extract_subscriptions_from_file",
                          return_value=[{"dag_id": "d", "queue_name": "q"}]):
            listener._scan_subscriptions()

        mtimes = dict(listener._last_mtimes)
        cached = {path: list(subs) for path, subs in listener._cached_subs.items()}
        assert str(dag_file) in mtimes

        folder.chmod(0o111)
        try:
            with patch("airflow.settings.DAGS_FOLDER", str(folder)), \
                 pytest.raises(_UntrustedScan):
                listener._scan_subscriptions()
        finally:
            folder.chmod(0o755)

        assert listener._last_mtimes == mtimes
        assert listener._cached_subs == cached


# ---------------------------------------------------------------------------
# _scan_subscriptions on a real DAG folder — what the walk lists, and the
# directories it cannot read
# ---------------------------------------------------------------------------

_needs_permissions = pytest.mark.skipif(
    os.geteuid() == 0,
    reason="root reads a directory whatever its mode says, so there is no unreadable one to build",
)


def _dag_source(dag_id: str) -> str:
    """A DAG file with one ``@rmq_trigger`` subscription, for the DAG ``dag_id``."""
    return (
        "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
        "from airflow.decorators import dag\n"
        f"@rmq_trigger(queue='q_{dag_id}')\n"
        f"@dag(dag_id='{dag_id}')\n"
        "def d(): pass\n"
    )


def _put_dag(path: Path, dag_id: str) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_dag_source(dag_id))
    return str(path)


def _dag_ids(result: list[dict]) -> set[str]:
    return {sub["dag_id"] for sub in result}


class TestScanOfTheDagFolder:
    """The walk decides which files the scan reads, and a directory it cannot read
    holds the previous parse of the files under it the way an unreadable file does."""

    @pytest.fixture
    def folder(self, tmp_path):
        path = tmp_path / "dags"
        path.mkdir()
        for dag_id in ("a", "b", "c"):
            _put_dag(path / f"{dag_id}.py", dag_id)
        return path

    @pytest.fixture
    def chmod(self):
        """``chmod`` that gives the path its access back after the test."""
        changed: list[Path] = []

        def _chmod(path: Path, mode: int) -> None:
            changed.append(path)
            os.chmod(path, mode)

        yield _chmod
        for path in reversed(changed):
            os.chmod(path, 0o755)

    @staticmethod
    def _scan(listener, dags_folder) -> list[dict]:
        with patch("airflow.settings.DAGS_FOLDER", str(dags_folder)):
            return listener._scan_subscriptions()

    @staticmethod
    def _warnings(caplog, text: str) -> list[logging.LogRecord]:
        return [
            r for r in caplog.records
            if r.levelno == logging.WARNING and text in r.getMessage()
        ]

    @_needs_permissions
    def test_a_subdirectory_turning_unreadable_holds_the_previous_parse(
        self, folder, chmod, caplog
    ):
        held = _put_dag(folder / "sub" / "s.py", "s")
        listener = RMQWatcherListener()
        assert _dag_ids(self._scan(listener, folder)) == {"a", "b", "c", "s"}

        chmod(folder / "sub", 0)
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LISTENER_LOG), \
             patch.object(listener, "_file_unreadable",
                          wraps=listener._file_unreadable) as spy:
            result = self._scan(listener, folder)

        assert _dag_ids(result) == {"a", "b", "c", "s"}
        assert held in listener._cached_subs
        assert listener._untrusted_scan_streak == 0
        # The file counts in the listing once, as one file that did not read.
        assert [c.args[0] for c in spy.call_args_list] == [held]
        dir_warnings = self._warnings(caplog, "cannot be read")
        assert len(dir_warnings) == 1
        assert str(folder / "sub") in dir_warnings[0].getMessage()
        hold_warnings = self._warnings(caplog, "holding its previous parse")
        assert len(hold_warnings) == 1
        assert held in hold_warnings[0].getMessage()

    @_needs_permissions
    def test_the_next_cycle_under_an_unreadable_subdirectory_drops_its_subscriptions(
        self, folder, chmod
    ):
        held = _put_dag(folder / "sub" / "s.py", "s")
        listener = RMQWatcherListener()
        self._scan(listener, folder)
        chmod(folder / "sub", 0)

        for _ in range(_UNREADABLE_FILE_LIMIT):
            assert "s" in _dag_ids(self._scan(listener, folder))
        result = self._scan(listener, folder)

        assert _dag_ids(result) == {"a", "b", "c"}
        assert listener._cached_subs[held] == []
        assert listener._untrusted_scan_streak == 0

    @_needs_permissions
    def test_a_subdirectory_reading_again_is_reported_and_its_files_reread(
        self, folder, chmod, caplog
    ):
        held = _put_dag(folder / "sub" / "s.py", "s")
        listener = RMQWatcherListener()
        self._scan(listener, folder)
        chmod(folder / "sub", 0)
        for _ in range(_UNREADABLE_FILE_LIMIT + 1):
            self._scan(listener, folder)
        assert "s" not in _dag_ids(self._scan(listener, folder))

        os.chmod(folder / "sub", 0o755)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=_LISTENER_LOG):
            result = self._scan(listener, folder)

        assert "s" in _dag_ids(result)
        assert held in listener._last_mtimes
        assert held not in listener._read_failures
        assert listener._unreadable_dirs == set()
        infos = [
            r.getMessage() for r in caplog.records
            if r.levelno == logging.INFO and "readable again" in r.getMessage()
        ]
        assert infos == [
            f"RMQ Watcher: DAG folder directory {folder / 'sub'} is readable again"
        ]

    @_needs_permissions
    def test_a_directory_unreadable_from_the_start_blocks_nothing(
        self, folder, chmod, caplog
    ):
        _put_dag(folder / "lost+found" / "x.py", "x")
        chmod(folder / "lost+found", 0)
        listener = RMQWatcherListener()

        with caplog.at_level(logging.DEBUG, logger=_LISTENER_LOG):
            first = self._scan(listener, folder)
            second = self._scan(listener, folder)

        assert _dag_ids(first) == _dag_ids(second) == {"a", "b", "c"}
        assert listener._untrusted_scan_streak == 0
        dir_warnings = self._warnings(caplog, "cannot be read")
        assert [r.getMessage().split(" cannot")[0] for r in dir_warnings] == [
            f"RMQ Watcher: DAG folder directory {folder / 'lost+found'}"
        ]
        assert self._warnings(caplog, "did not read") == []
        assert any(
            r.levelno == logging.DEBUG and "still cannot be read" in r.getMessage()
            for r in caplog.records
        )

    @_needs_permissions
    def test_a_large_unreadable_subtree_makes_the_listing_untrusted(
        self, folder, chmod, caplog
    ):
        for n in range(6):
            _put_dag(folder / "big" / f"s{n}.py", f"s{n}")
        listener = RMQWatcherListener()
        self._scan(listener, folder)
        cached = {path: list(subs) for path, subs in listener._cached_subs.items()}

        chmod(folder / "big", 0)
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LISTENER_LOG), \
             pytest.raises(_UntrustedScan, match="directories that cannot be read") as exc:
            self._scan(listener, folder)

        assert repr(str(folder / "big")) in str(exc.value)
        assert listener._cached_subs == cached
        # The directory is reported in the cycle that finds it, distrusted or not, and
        # only at DEBUG in the cycles after it.
        assert len(self._warnings(caplog, "cannot be read")) == 1
        assert listener._unreadable_dirs == {str(folder / "big")}
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger=_LISTENER_LOG), \
             pytest.raises(_UntrustedScan):
            self._scan(listener, folder)
        assert self._warnings(caplog, "cannot be read") == []
        assert any(
            r.levelno == logging.DEBUG and "still cannot be read" in r.getMessage()
            for r in caplog.records
        )

    @_needs_permissions
    def test_a_large_subtree_that_stays_unreadable_ends_in_the_miss_count(
        self, folder, chmod
    ):
        for n in range(6):
            _put_dag(folder / "big" / f"s{n}.py", f"s{n}")
        big_ids = {f"s{n}" for n in range(6)}
        listener = RMQWatcherListener()
        self._scan(listener, folder)
        chmod(folder / "big", 0)

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan(listener, folder)
        # The budget runs out: the listing is believed, and the files under the
        # directory hold their previous parse for the cycles an unreadable file does.
        given_in = self._scan(listener, folder)
        assert listener._untrusted_scan_streak == 0
        assert big_ids <= _dag_ids(given_in)
        for _ in range(_UNREADABLE_FILE_LIMIT - 1):
            assert big_ids <= _dag_ids(self._scan(listener, folder))

        result = self._scan(listener, folder)

        assert _dag_ids(result) == {"a", "b", "c"}
        assert listener._untrusted_scan_streak == 0
        assert listener._unreadable_dirs == {str(folder / "big")}

    @_needs_permissions
    def test_an_unreadable_ignore_file_of_the_root_is_an_untrusted_scan(
        self, folder, chmod
    ):
        listener = RMQWatcherListener()
        self._scan(listener, folder)
        ignore_file = folder / ".airflowignore"
        ignore_file.write_text("nothing\n")
        chmod(ignore_file, 0)
        listener._untrusted_scan_streak = 2

        with pytest.raises(_UntrustedScan, match="cannot be listed"):
            self._scan(listener, folder)

        # Like a folder that cannot be listed at all: the streak neither grows nor
        # restarts, and the root is not reported as a directory of the folder.
        assert listener._untrusted_scan_streak == 2
        assert listener._unreadable_dirs == set()

    def test_a_listing_error_is_an_untrusted_scan(self, folder):
        os.symlink(folder, folder / "loop")
        listener = RMQWatcherListener()
        listener._untrusted_scan_streak = 2

        with pytest.raises(_UntrustedScan, match="cannot be listed by Airflow's rules"):
            self._scan(listener, folder)

        assert listener._untrusted_scan_streak == 2

    def test_a_listing_error_carries_its_cause(self):
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   side_effect=DagFolderListingError("bad pattern")), \
             pytest.raises(_UntrustedScan, match="bad pattern") as exc:
            listener._scan_subscriptions()

        assert isinstance(exc.value.__cause__, DagFolderListingError)

    def test_a_file_under_airflowignore_gives_no_subscription(self, folder):
        (folder / ".airflowignore").write_text("copies\n")
        _put_dag(folder / "copies" / "a_copy.py", "a_copy")
        listener = RMQWatcherListener()

        assert _dag_ids(self._scan(listener, folder)) == {"a", "b", "c"}

    def test_a_file_in_a_hidden_directory_gives_its_subscription(self, folder):
        _put_dag(folder / ".hidden" / "h.py", "h")
        _put_dag(folder / ".dotted.py", "dotted")
        listener = RMQWatcherListener()

        assert _dag_ids(self._scan(listener, folder)) == {"a", "b", "c", "h", "dotted"}

    @_needs_permissions
    def test_a_trailing_slash_on_the_folder_matches_paths_under_an_unreadable_directory(
        self, folder, chmod
    ):
        _put_dag(folder / "sub" / "s.py", "s")
        listener = RMQWatcherListener()
        dags_folder = os.path.join(str(folder), "")
        self._scan(listener, dags_folder)
        cached = set(listener._cached_subs)

        chmod(folder / "sub", 0)
        result = self._scan(listener, dags_folder)

        assert "s" in _dag_ids(result)
        assert set(listener._cached_subs) == cached

    @_needs_permissions
    def test_a_symlinked_directory_whose_target_turns_unreachable_holds_its_files(
        self, folder, tmp_path, chmod, caplog
    ):
        outside = tmp_path / "outside"
        _put_dag(outside / "target" / "t.py", "t")
        os.symlink(outside / "target", folder / "link")
        listener = RMQWatcherListener()
        assert "t" in _dag_ids(self._scan(listener, folder))
        held = str(folder / "link" / "t.py")
        assert held in listener._cached_subs

        chmod(outside, 0)
        with caplog.at_level(logging.WARNING, logger=_LISTENER_LOG):
            result = self._scan(listener, folder)

        assert "t" in _dag_ids(result)
        assert str(folder / "link") in listener._unreadable_dirs
        assert len(self._warnings(caplog, "holding its previous parse")) == 1

    @_needs_permissions
    @pytest.mark.parametrize("where", ["", "sub"])
    def test_a_py_entry_that_cannot_be_examined_is_an_error_of_that_file_alone(
        self, folder, tmp_path, chmod, caplog, where
    ):
        """``is_file()`` fails on a ``.py`` symlink into a directory without access."""
        locked = tmp_path / "locked"
        _put_dag(locked / "x.py", "x")
        _put_dag(folder / "sub" / "s.py", "s")
        entry = folder / where / "x.py"
        os.symlink(locked / "x.py", entry)
        listener = RMQWatcherListener()
        assert _dag_ids(self._scan(listener, folder)) == {"a", "b", "c", "s", "x"}

        chmod(locked, 0)
        caplog.clear()
        with caplog.at_level(logging.WARNING, logger=_LISTENER_LOG):
            result = self._scan(listener, folder)

        assert _dag_ids(result) == {"a", "b", "c", "s", "x"}
        assert listener._unreadable_dirs == set()
        assert listener._untrusted_scan_streak == 0
        assert self._warnings(caplog, "cannot be read") == []
        hold_warnings = self._warnings(caplog, "holding its previous parse")
        assert [str(entry) in r.getMessage() for r in hold_warnings] == [True]

    @_needs_permissions
    def test_a_child_left_behind_by_a_parent_turning_unreadable_is_not_reported_read(
        self, folder, chmod, caplog
    ):
        _put_dag(folder / "sub" / "inner" / "i.py", "i")
        chmod(folder / "sub" / "inner", 0)
        listener = RMQWatcherListener()
        self._scan(listener, folder)
        assert listener._unreadable_dirs == {str(folder / "sub" / "inner")}

        chmod(folder / "sub", 0)
        caplog.clear()
        with caplog.at_level(logging.INFO, logger=_LISTENER_LOG):
            self._scan(listener, folder)

        assert listener._unreadable_dirs == {str(folder / "sub")}
        assert not any("readable again" in r.getMessage() for r in caplog.records)


class TestFilesUnderAnUnreadableDirectory:
    """The cache's paths under a directory the walk could not read join the listing
    as files that did not read; built on a mocked walk, independent of the uid."""

    @staticmethod
    def _warm(listener, paths):
        for path in paths:
            listener._cached_subs[path] = [{"dag_id": os.path.basename(path)}]
            listener._last_mtimes[path] = 1.0

    def test_only_paths_under_the_directory_are_held_and_each_once(self):
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        held = ["/dags/sub/s.py", "/dags/sub/deeper/d.py"]
        sibling = "/dags/sub_other/o.py"
        self._warm(listener, [*held, sibling])
        error = PermissionError(13, "denied", "/dags/sub")
        listing = DagFolderListing({"/dags/a.py"}, {"/dags/sub": error}, set())

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=listing), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=5.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]), \
             patch.object(listener, "_file_unreadable",
                          wraps=listener._file_unreadable) as spy:
            listener._scan_subscriptions()

        assert sorted(c.args[0] for c in spy.call_args_list) == sorted(held)
        assert all(c.args[1] is error for c in spy.call_args_list)
        assert all(c.args[2] is None for c in spy.call_args_list)
        for path in held:
            assert path in listener._cached_subs
        # A sibling whose name starts with the directory's is not under it.
        assert sibling not in listener._cached_subs

    def test_paths_under_a_listed_py_entry_that_gives_no_stat_are_held(self):
        """A symlinked directory named ``*.py`` whose target went out of reach is
        listed as a ``.py`` file; the files known under it are held, not evicted."""
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        under = "/dags/pkg.py/d.py"
        self._warm(listener, ["/dags/a.py", under])
        error = PermissionError(13, "denied", "/dags/pkg.py")

        def getmtime(path):
            if path == "/dags/pkg.py":
                raise error
            return 1.0

        with patch("airflow_provider_rmq.watcher.listener.list_dag_files",
                   return_value=_listing("/dags/a.py", "/dags/pkg.py")), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=getmtime), \
             patch.object(listener, "_file_unreadable",
                          wraps=listener._file_unreadable) as spy:
            listener._scan_subscriptions()

        assert sorted(c.args[0] for c in spy.call_args_list) == ["/dags/pkg.py", under]
        assert under in listener._cached_subs
        assert listener._unreadable_dirs == set()


class TestUntrustedListingReason:
    def test_the_reason_names_the_directories_that_cannot_be_read(self):
        reason = listener_module._untrusted_listing_reason(
            "/dags", {"/dags/a.py", "/dags/sub/s.py"}, 1, 4, [],
            ["/dags/sub"],
        )

        assert "directories that cannot be read: '/dags/sub'" in reason

    def test_a_long_list_of_directories_is_cut_to_a_count(self):
        dirs = [f"/dags/d{n}" for n in range(listener_module._UNREADABLE_DIRS_NAMED + 3)]

        reason = listener_module._untrusted_listing_reason(
            "/dags", {"/dags/a.py"}, 1, 4, [], dirs,
        )

        assert reason.count("'/dags/d") == listener_module._UNREADABLE_DIRS_NAMED
        assert reason.endswith(" and 3 more")

    def test_a_listing_with_every_directory_read_names_none(self):
        reason = listener_module._untrusted_listing_reason(
            "/dags", {"/dags/a.py"}, 1, 4, [],
        )

        assert "directories" not in reason


# ---------------------------------------------------------------------------
# _extract_subscriptions_from_file — интеграционные тесты с реальными файлами
# ---------------------------------------------------------------------------

class TestExtractSubscriptionsFromFile:
    def test_explicit_dag_id_used_over_function_name(self, tmp_path):
        dag_file = tmp_path / "my_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q1')\n"
            "@dag(dag_id='explicit_name')\n"
            "def get_params_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["dag_id"] == "explicit_name"
        assert result[0]["queue_name"] == "q1"

    def test_extraction_succeeds_for_dag_that_would_fail_at_runtime_import(self, tmp_path):
        """AST parsing never executes the file (see this method's own docstring:
        "AST parsing never executes the file and never acquires the Python import
        lock"). A DAG file that would raise if Airflow's DagBag actually imported
        it — so the DAG never registers in DagModel — is still scanned
        successfully here and its @rmq_trigger subscription still gets recorded.
        This is the mechanism behind the dag-not-found badge's "dag_file
        subscription whose DAG fails at runtime import" acceptance scenario
        (see the dag-not-found-badge plan, Task 4)."""
        dag_file = tmp_path / "broken_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q1')\n"
            "@dag(dag_id='broken_dag')\n"
            "def get_broken_dag(): pass\n"
            "\n"
            "raise RuntimeError('boom — this module blows up on a real import')\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["dag_id"] == "broken_dag"
        assert result[0]["queue_name"] == "q1"

    def test_fallback_to_function_name_when_no_dag_id(self, tmp_path):
        dag_file = tmp_path / "my_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q2')\n"
            "@dag(schedule_interval=None)\n"
            "def my_function(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["dag_id"] == "my_function"

    def test_dag_id_resolved_from_module_level_constant(self, tmp_path):
        # Direct regression test for the real donstroy_pipeline_spark_all.py bug:
        # dag_id= referencing a module-level string constant assigned earlier in
        # the file must resolve to the constant's value, NOT to the decorated
        # function's name.
        dag_file = tmp_path / "my_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "DAG_ID = 'runtime_name'\n"
            "@rmq_trigger(queue='q3')\n"
            "@dag(dag_id=DAG_ID)\n"
            "def variable_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["dag_id"] == "runtime_name"

    def test_empty_string_module_constant_falls_back_to_function_name(self, tmp_path):
        # Regression: a module-level constant DAG_ID = "" referenced via
        # dag_id=DAG_ID must fall back to the function name (matching
        # Airflow's own dag_id or f.__name__), NOT silently register a
        # subscription with dag_id="".
        dag_file = tmp_path / "my_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "DAG_ID = ''\n"
            "@rmq_trigger(queue='q3')\n"
            "@dag(dag_id=DAG_ID)\n"
            "def empty_const_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["dag_id"] == "empty_const_dag"

    def test_nested_function_with_literal_dag_id_still_resolves(self, tmp_path):
        # A nested/factory function using a LITERAL dag_id (not a name
        # reference) must still resolve correctly — the nested-function
        # guard only refuses module-constant lookups (which could be
        # shadowed by a local binding), not string literals, which need no
        # scope information at all. Guards against a future "simplification"
        # that blanket-skips all nested FunctionDefs.
        dag_file = tmp_path / "factory_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "\n"
            "def make_dag():\n"
            "    @rmq_trigger(queue='q')\n"
            "    @dag(dag_id='literal_id')\n"
            "    def data_proc(): pass\n"
            "\n"
            "    return data_proc\n"
            "\n"
            "built_dag = make_dag()\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["dag_id"] == "literal_id"

    def test_positional_literal_dag_id_from_file(self, tmp_path):
        # @dag("real_id") — positional dag_id, decorated function has a
        # DIFFERENT name, so a name-fallback would silently register the
        # wrong dag_id.
        dag_file = tmp_path / "my_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q')\n"
            "@dag('real_id')\n"
            "def get_params_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["dag_id"] == "real_id"

    def test_positional_constant_dag_id_from_file(self, tmp_path):
        # DAG_ID referenced positionally (@dag(DAG_ID)) rather than via
        # dag_id=DAG_ID — exercises the same module-constant resolution as
        # the keyword form, through the full file pipeline.
        dag_file = tmp_path / "my_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "DAG_ID = 'real_id'\n"
            "@rmq_trigger(queue='q')\n"
            "@dag(DAG_ID)\n"
            "def variable_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["dag_id"] == "real_id"

    def test_use_before_definition_isolated_across_multiple_functions(self, tmp_path):
        # Two functions, each relying on a DIFFERENT module constant defined
        # at a different file position. early_dag uses FIRST_DAG (defined
        # before it) and resolves fine. late_dag uses SECOND_DAG (defined
        # between the two functions, i.e. after early_dag but before
        # late_dag) — it must resolve for late_dag only; if the position
        # filter leaked across functions (e.g. used the full/final constant
        # map for every function instead of one filtered per function's own
        # line), early_dag would incorrectly see SECOND_DAG too.
        dag_file = tmp_path / "my_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "FIRST_DAG = 'first_id'\n"
            "@rmq_trigger(queue='q1')\n"
            "@dag(dag_id=FIRST_DAG)\n"
            "def early_dag(): pass\n"
            "SECOND_DAG = 'second_id'\n"
            "@rmq_trigger(queue='q2')\n"
            "@dag(dag_id=SECOND_DAG)\n"
            "def late_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        by_queue = {sub["queue_name"]: sub for sub in result}
        assert len(result) == 2
        assert by_queue["q1"]["dag_id"] == "first_id"
        assert by_queue["q2"]["dag_id"] == "second_id"

    def test_exchange_subscription_gets_correct_queue_name_and_group_key(self, tmp_path):
        dag_file = tmp_path / "exchange_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'])\n"
            "@dag(dag_id='jetstat_dag')\n"
            "def jetstat_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        sub = result[0]
        assert sub["dag_id"] == "jetstat_dag"
        assert sub["queue_name"] == "rmq_watcher.sub.jetstat_dag"
        assert sub["exchange"] == "jetstat.airflow"
        assert sub["routing_keys"] == ["abc123.*"]
        # cooldown defaults to 0 → group_key is None, same rule as queue= mode
        assert sub["group_key"] is None

    def test_two_exchange_decorators_on_same_function_refuse_the_file(self, tmp_path, caplog):
        # The decorator raises on the second exchange= call at import, so the scan
        # keeps neither of them.
        dag_file = tmp_path / "double_exchange_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(exchange='exchange.one', routing_keys=['a.b'])\n"
            "@rmq_trigger(exchange='exchange.two', routing_keys=['c.d'])\n"
            "@dag(dag_id='double_exchange_dag')\n"
            "def double_exchange_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING):
            result = listener._extract_subscriptions_from_file(str(dag_file))

        assert result == []
        errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
        assert len(errors) == 1
        assert "double_exchange_dag" in errors[0]
        assert "second exchange=" in errors[0]

    def test_unexpected_exception_during_extraction_answers_none_not_raises(self, tmp_path):
        # Regression: if _read_file_triggers (or build_subscriptions) ever lets
        # an unexpected exception type leak past its own handling,
        # _extract_subscriptions_from_file must still not
        # raise — one malformed DAG file must never crash the whole reconcile
        # cycle for every other DAG (see _main's broad except Exception,
        # which would otherwise skip _sync_to_db/reconcile() for ALL DAGs).
        dag_file = tmp_path / "broken_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q')\n"
            "@dag(dag_id='broken_dag')\n"
            "def broken_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        with patch(
            "airflow_provider_rmq.watcher.listener._read_file_triggers",
            side_effect=AttributeError("boom"),
        ):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result is None

    def test_unexpected_exception_during_extraction_logs_and_does_not_propagate(
        self, tmp_path, caplog
    ):
        dag_file = tmp_path / "broken_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q')\n"
            "@dag(dag_id='broken_dag')\n"
            "def broken_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.ERROR), patch(
            "airflow_provider_rmq.watcher.listener._read_file_triggers",
            side_effect=TypeError("boom"),
        ):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result is None
        assert any(str(dag_file) in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _extract_subscriptions_from_file — unresolved dag_id contract (Task 3)
# ---------------------------------------------------------------------------


def _errors(caplog) -> list[str]:
    """Messages of the ERROR records ``caplog`` captured."""
    return [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]


class TestExtractSubscriptionsUnresolvedDagId:
    def test_nested_function_does_not_use_module_constants(self, tmp_path, caplog):
        # A DAG declared inside a factory function: the local reassignment of
        # DAG_ID shadows the module-level constant of the same name. Passing
        # the module-level map to a nested function's decorators would
        # resolve to the WRONG value ("module-id" instead of "local-id") —
        # the guard must refuse to resolve at all instead.
        dag_file = tmp_path / "factory_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "DAG_ID = 'module-id'\n"
            "\n"
            "def make_dag():\n"
            "    DAG_ID = 'local-id'\n"
            "\n"
            "    @rmq_trigger(queue='q')\n"
            "    @dag(dag_id=DAG_ID)\n"
            "    def data_proc(): pass\n"
            "\n"
            "    return data_proc\n"
            "\n"
            "built_dag = make_dag()\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result == []
        assert any("data_proc" in m for m in _errors(caplog))

    def test_unresolvable_dag_id_with_rmq_trigger_refuses_with_an_error(self, tmp_path, caplog):
        dag_file = tmp_path / "unresolvable_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q')\n"
            "@dag(dag_id=f'prefix_{1+1}')\n"
            "def unresolvable_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result == []
        errors = _errors(caplog)
        assert len(errors) == 1
        assert str(dag_file) in errors[0]
        assert "unresolvable_dag" in errors[0]
        assert "cannot statically resolve dag_id" in errors[0]

    def test_unresolvable_dag_id_error_does_not_send_the_operator_to_the_ui(
        self, tmp_path, caplog
    ):
        # Such a DAG does not import, so a subscription made by hand on the
        # Subscriptions page would consume for a DAG Airflow does not run.
        dag_file = tmp_path / "unresolvable_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q')\n"
            "@dag(dag_id=f'prefix_{1+1}')\n"
            "def unresolvable_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING):
            listener._extract_subscriptions_from_file(str(dag_file))
        assert "UI" not in caplog.text
        assert "Subscriptions page" not in caplog.text
        errors = _errors(caplog)
        assert len(errors) == 1
        assert "cannot statically resolve dag_id" in errors[0]

    def test_kwargs_unpacking_refuses_with_an_error(self, tmp_path, caplog):
        dag_file = tmp_path / "kwargs_unpacking_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "DAG_KWARGS = {'schedule': None}\n"
            "@rmq_trigger(queue='q')\n"
            "@dag(**DAG_KWARGS)\n"
            "def kwargs_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result == []
        errors = _errors(caplog)
        assert len(errors) == 1
        assert str(dag_file) in errors[0]
        assert "kwargs_dag" in errors[0]

    def test_unresolvable_dag_id_without_rmq_trigger_stays_silent(self, tmp_path, caplog):
        dag_file = tmp_path / "no_trigger_dag.py"
        dag_file.write_text(
            "from airflow.decorators import dag\n"
            "@dag(dag_id=f'prefix_{1+1}')\n"
            "def no_trigger_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result == []
        assert not any("no_trigger_dag" in r.message for r in caplog.records)

    def test_use_before_definition_not_visible_to_earlier_function(self, tmp_path, caplog):
        # DAG_ID is assigned AFTER the decorated function in the file — real
        # Python would NameError at import time here, so the static resolver
        # must not treat it as resolvable either.
        dag_file = tmp_path / "use_before_def.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q')\n"
            "@dag(dag_id=DAG_ID)\n"
            "def early_dag(): pass\n"
            "DAG_ID = 'too_late'\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result == []
        assert any("early_dag" in m for m in _errors(caplog))

    def test_unresolvable_function_refuses_every_subscription_of_the_file(
        self, tmp_path, caplog
    ):
        # The decorator breaks the import of the whole file, so the resolvable
        # neighbour's DAG is gone from Airflow too and must not keep a consumer.
        dag_file = tmp_path / "mixed_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "CONST = 'resolved_id'\n"
            "@rmq_trigger(queue='q1')\n"
            "@dag(dag_id=CONST)\n"
            "def resolvable_dag(): pass\n"
            "\n"
            "@rmq_trigger(queue='q2')\n"
            "@dag(dag_id=f'prefix_{1+1}')\n"
            "def unresolvable_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result == []
        errors = _errors(caplog)
        assert len(errors) == 1
        assert "unresolvable_dag" in errors[0]
        assert "'resolvable_dag'" not in errors[0]

    def test_exchange_subscription_with_resolved_constant(self, tmp_path):
        dag_file = tmp_path / "exchange_const_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "DAG_ID = 'real_id'\n"
            "@rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'])\n"
            "@dag(dag_id=DAG_ID)\n"
            "def some_function(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        sub = result[0]
        assert sub["dag_id"] == "real_id"
        assert sub["queue_name"] == "rmq_watcher.sub.real_id"

    def test_aliased_dag_decorator_not_recognized_refuses_with_an_error(self, tmp_path, caplog):
        # `dag` imported under an alias is not matched by the `is_dag`
        # predicate (Name(id="dag")/Attribute(attr="dag") only) — no
        # recognized @dag(...) call exists among the decorators at all, so
        # the dag_id is unresolved and the file is refused, NOT registered
        # under the function name.
        dag_file = tmp_path / "aliased_dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag as airflow_dag\n"
            "DAG_ID = 'real_id'\n"
            "@rmq_trigger(queue='q')\n"
            "@airflow_dag(dag_id=DAG_ID)\n"
            "def aliased_function(): pass\n"
        )
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result == []
        assert any("aliased_function" in m for m in _errors(caplog))


# ---------------------------------------------------------------------------
# _extract_subscriptions_from_file — a file is read whole or refused whole
# ---------------------------------------------------------------------------

_IMPORTS = (
    "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
    "from airflow.decorators import dag\n"
)

#: A DAG the scan reads on its own; the cases below put something beside it.
_READABLE_DAG = (
    "@rmq_trigger(queue='good_q')\n"
    "@dag(dag_id='good')\n"
    "def good(): pass\n"
)


class TestExtractSubscriptionsFileRefusal:
    @staticmethod
    def _extract(tmp_path, source: str, caplog=None):
        dag_file = tmp_path / "dag.py"
        dag_file.write_text(source)
        listener = RMQWatcherListener()
        if caplog is None:
            return listener._extract_subscriptions_from_file(str(dag_file))
        with caplog.at_level(logging.WARNING):
            return listener._extract_subscriptions_from_file(str(dag_file))

    def test_file_constant_conn_id_is_read(self, tmp_path):
        result = self._extract(tmp_path, _IMPORTS + (
            "RMQ_CONN = 'rmq_prod'\n"
            "@rmq_trigger(queue='q', conn_id=RMQ_CONN)\n"
            "@dag(dag_id='d')\n"
            "def d(): pass\n"
        ))
        assert [(s["queue_name"], s["conn_id"]) for s in result] == [("q", "rmq_prod")]

    def test_positional_arguments_through_constants_are_read(self, tmp_path):
        result = self._extract(tmp_path, _IMPORTS + (
            "QUEUE = 'orders'\n"
            "RMQ_CONN = 'rmq_prod'\n"
            "FILTER = {'body.type': 'paid'}\n"
            "@rmq_trigger(QUEUE, None, RMQ_CONN, FILTER, 30)\n"
            "@dag(dag_id='d')\n"
            "def d(): pass\n"
        ))
        assert len(result) == 1
        assert result[0]["queue_name"] == "orders"
        assert result[0]["conn_id"] == "rmq_prod"
        assert result[0]["filter_data"] == {"body.type": "paid"}
        assert result[0]["cooldown"] == 30
        assert result[0]["group_key"] == "d"

    def test_call_conn_id_registers_nothing_and_logs_an_error(self, tmp_path, caplog):
        dag_file_source = _IMPORTS + (
            "@rmq_trigger(queue='q', conn_id=get_conn())\n"
            "@dag(dag_id='d')\n"
            "def d(): pass\n"
        )
        assert self._extract(tmp_path, dag_file_source, caplog) == []
        errors = _errors(caplog)
        assert len(errors) == 1
        assert str(tmp_path / "dag.py") in errors[0]
        assert "dag_id='d'" in errors[0]
        assert "conn_id cannot be read" in errors[0]
        assert "a function call" in errors[0]

    def test_stacked_readable_and_unreadable_calls_register_nothing(self, tmp_path, caplog):
        result = self._extract(tmp_path, _IMPORTS + (
            "@rmq_trigger(queue='a')\n"
            "@rmq_trigger(queue='b', cooldown=COOLDOWN)\n"
            "@dag(dag_id='d')\n"
            "def d(): pass\n"
        ), caplog)
        assert result == []
        assert "cooldown cannot be read" in _errors(caplog)[0]

    def test_stacked_call_the_builder_refuses_registers_nothing(self, tmp_path, caplog):
        result = self._extract(tmp_path, _IMPORTS + (
            "@rmq_trigger(queue='a')\n"
            "@rmq_trigger(queue='b', cooldown=-1)\n"
            "@dag(dag_id='d')\n"
            "def d(): pass\n"
        ), caplog)
        assert result == []
        assert "'cooldown' must be >= 0" in _errors(caplog)[0]

    def test_one_unreadable_dag_refuses_its_readable_neighbour(self, tmp_path, caplog):
        result = self._extract(tmp_path, _IMPORTS + _READABLE_DAG + (
            "@rmq_trigger(queue='bad_q', filter_data=load_filter())\n"
            "@dag(dag_id='bad')\n"
            "def bad(): pass\n"
        ), caplog)
        assert result == []
        errors = _errors(caplog)
        assert len(errors) == 1
        assert "dag_id='bad'" in errors[0]
        assert "dag_id='good'" not in errors[0]

    @pytest.mark.parametrize(
        ("beside", "expected"),
        [
            pytest.param(
                "from airflow import DAG\nrmq_trigger(queue='b')(DAG('B'))\n",
                "line 7: the name rmq_trigger is used other than as a decorator call",
                id="direct-call-on-a-dag",
            ),
            pytest.param(
                "from airflow_provider_rmq.watcher.decorators import rmq_trigger as rt\n"
                "@rt(queue='b')\n@dag(dag_id='b')\ndef b(): pass\n",
                "rmq_trigger is imported as 'rt'",
                id="alias-used",
            ),
            pytest.param(
                "from airflow_provider_rmq.watcher.decorators import rmq_trigger as rt\n",
                "rmq_trigger is imported as 'rt'",
                id="alias-unused",
            ),
            pytest.param(
                "from airflow_provider_rmq.watcher import decorators\n"
                "rt = decorators.rmq_trigger\n",
                "the attribute .rmq_trigger is used other than as a decorator call",
                id="attribute-alias",
            ),
            pytest.param(
                "from functools import partial\n"
                "RMQ = partial(rmq_trigger, conn_id='rmq_prod')\n",
                "the name rmq_trigger is used other than as a decorator call",
                id="partial",
            ),
            pytest.param(
                "@dag(dag_id='b')\n@rmq_trigger(queue='b')\ndef b(): pass\n",
                "placed below @dag(...)",
                id="rmq-trigger-below-dag",
            ),
            pytest.param(
                "from common_rmq import rmq_trigger\n"
                "@rmq_trigger(queue='b')\n@dag(dag_id='b')\ndef b(): pass\n",
                "rmq_trigger is not bound by one absolute import alone in the scope the "
                "decorator runs in, not rmq_trigger of airflow_provider_rmq.watcher.decorators",
                id="second-import-from-another-module",
            ),
            pytest.param(
                "from common_rmq import *\n",
                "the file has 'from ... import *'",
                id="wildcard-import",
            ),
        ],
    )
    def test_a_mention_the_scan_cannot_follow_refuses_the_file(
        self, tmp_path, caplog, beside, expected
    ):
        result = self._extract(tmp_path, _IMPORTS + _READABLE_DAG + beside, caplog)
        assert result == []
        errors = _errors(caplog)
        assert len(errors) == 1
        assert expected in errors[0]

    def test_rmq_trigger_of_another_module_refuses_the_file(self, tmp_path, caplog):
        result = self._extract(tmp_path, (
            "from common_rmq import rmq_trigger\n"
            "from airflow.decorators import dag\n"
        ) + _READABLE_DAG, caplog)
        assert result == []
        errors = _errors(caplog)
        assert len(errors) == 1
        assert "rmq_trigger is common_rmq.rmq_trigger" in errors[0]

    def test_rmq_trigger_below_dag_is_named_in_the_error(self, tmp_path, caplog):
        self._extract(tmp_path, _IMPORTS + (
            "@dag(dag_id='b')\n"
            "@rmq_trigger(queue='b')\n"
            "def b(): pass\n"
        ), caplog)
        assert "placed below @dag(...)" in _errors(caplog)[0]

    @pytest.mark.parametrize(
        "source",
        [
            pytest.param(
                "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
                "from airflow.decorators import dag\n"
                "@rmq_trigger(queue='q')\n@dag(dag_id='d')\ndef d(): pass\n",
                id="import-under-own-name",
            ),
            pytest.param(
                "from airflow_provider_rmq.watcher import decorators\n"
                "from airflow.decorators import dag\n"
                "@decorators.rmq_trigger(queue='q')\n@dag(dag_id='d')\ndef d(): pass\n",
                id="attribute-in-decorator-list",
            ),
            pytest.param(
                "from airflow_provider_rmq.watcher.decorators import rmq_trigger as rmq_trigger\n"
                "from airflow.decorators import dag\n"
                "@rmq_trigger(queue='q')\n@dag(dag_id='d')\ndef d(): pass\n",
                id="import-as-own-name",
            ),
        ],
    )
    def test_forms_the_scan_follows_are_read(self, tmp_path, caplog, source):
        result = self._extract(tmp_path, source, caplog)
        assert [s["queue_name"] for s in result] == ["q"]
        assert _errors(caplog) == []

    def test_nested_factory_and_module_if_with_literals_read_as_before(self, tmp_path, caplog):
        result = self._extract(tmp_path, _IMPORTS + (
            "import os\n"
            "def make_dag():\n"
            "    @rmq_trigger(queue='nested_q', cooldown=10)\n"
            "    @dag(dag_id='nested')\n"
            "    def nested(): pass\n"
            "    return nested()\n"
            "make_dag()\n"
            "if os.environ.get('ENV') != 'test':\n"
            "    @rmq_trigger(queues=['a', 'b'])\n"
            "    @dag(dag_id='under_if')\n"
            "    def under_if(): pass\n"
            "    under_if()\n"
        ), caplog)
        assert sorted((s["dag_id"], s["queue_name"]) for s in result) == [
            ("nested", "nested_q"),
            ("under_if", "a"),
            ("under_if", "b"),
        ]
        assert _errors(caplog) == []

    def test_a_refused_file_answers_an_empty_list_not_none(self, tmp_path):
        result = self._extract(tmp_path, _IMPORTS + (
            "@rmq_trigger(queue=QUEUE)\n"
            "@dag(dag_id='d')\n"
            "def d(): pass\n"
        ))
        assert result == []

    def test_a_refused_file_is_not_read_again_while_its_mtime_stands(self, tmp_path, caplog):
        """The file was read and parsed, so its mtime is kept like any parsed file's:
        the ERROR is written once, not on every cycle, and its rows go the
        counted-miss way."""
        (tmp_path / "dag.py").write_text(_IMPORTS + (
            "@rmq_trigger(queue=QUEUE)\n"
            "@dag(dag_id='d')\n"
            "def d(): pass\n"
        ))
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value=str(tmp_path))
        with patch.object(
            listener_module, "_read_and_parse", wraps=listener_module._read_and_parse
        ) as parse, caplog.at_level(logging.WARNING):
            listener._scan_subscriptions()
            listener._scan_subscriptions()

        assert parse.call_count == 1
        errors = _errors(caplog)
        assert len(errors) == 1
        assert "cannot be read statically" in errors[0]
        assert _READABLE_FILE_REMEDY in errors[0]


# ---------------------------------------------------------------------------
# _sync_to_db
# ---------------------------------------------------------------------------

def _existing_sub(
    dag_id="d",
    queue_name="q",
    conn_id="rmq_default",
    *,
    enabled=True,
    absent_cycles=None,
):
    """Build a stand-in for one dag_file row of RMQSubscription.

    ``absent_cycles`` and ``enabled`` are set explicitly on purpose: an unset attribute
    of a MagicMock is another MagicMock, which is neither ``None`` nor a number and
    would walk into the miss-counter arithmetic.
    """
    sub = MagicMock()
    sub.dag_id = dag_id
    sub.queue_name = queue_name
    sub.conn_id = conn_id
    sub.source = "dag_file"
    sub.enabled = enabled
    sub.absent_cycles = absent_cycles
    return sub


def _delete_mock(session):
    """The delete() bound to the conditional query the miss counter builds."""
    return session.query.return_value.filter_by.return_value.filter.return_value.delete


def _update_mock(session):
    """The update() the miss counter writes through, conditional on the value read."""
    return session.query.return_value.filter_by.return_value.filter.return_value.update


def _counted_miss(session):
    """The count the miss counter wrote this synchronisation, or ``None`` if it did not.

    The counter goes to the database as an UPDATE matched on the value the row was
    read as, so what a row now carries is what that statement wrote — the attribute on
    the object the cycle read is not where the count lives.
    """
    update = _update_mock(session)
    if not update.call_args_list:
        return None
    return update.call_args.args[0]["absent_cycles"]


@contextlib.contextmanager
def _sync_env(ctx, active_dag_ids):
    """Patch everything _sync_to_db reaches outside itself.

    ``active_dag_ids`` is a set, or an exception instance to raise from
    ``get_active_dag_ids``. It is always given explicitly: the session helper only
    configures ``.filter_by(...).all()``, so an unpatched ``get_active_dag_ids`` would
    quietly collect an empty set out of a MagicMock and make every DAG read as
    inactive.
    """
    with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
         patch("airflow_provider_rmq.watcher.listener.upsert_subscription") as mock_up, \
         patch("airflow_provider_rmq.watcher.listener.get_active_dag_ids") as mock_active:
        if isinstance(active_dag_ids, Exception):
            mock_active.side_effect = active_dag_ids
        else:
            mock_active.return_value = active_dag_ids
        yield mock_up


class TestSyncToDb:
    def test_sync_to_db_upserts_dag_file_subscriptions(self):
        listener = RMQWatcherListener()
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "c", "filter_data": {}}]

        ctx, session = _make_session_ctx(existing_subs=[])

        with _sync_env(ctx, {"d"}) as mock_up:
            listener._sync_to_db(scanned)

        mock_up.assert_called_once()
        call_kwargs = mock_up.call_args
        assert call_kwargs.kwargs["dag_id"] == "d"
        assert call_kwargs.kwargs["source"] == "dag_file"

    def test_sync_to_db_passes_ack_unmatched_to_upsert(self):
        listener = RMQWatcherListener()
        scanned = [{
            "dag_id": "d",
            "queue_name": "q",
            "conn_id": "c",
            "filter_data": {},
            "ack_unmatched": True,
        }]

        ctx, _session = _make_session_ctx(existing_subs=[])

        with _sync_env(ctx, {"d"}) as mock_up:
            listener._sync_to_db(scanned)

        assert mock_up.call_args.kwargs["ack_unmatched"] is True

    def test_sync_to_db_writes_false_when_the_scan_carries_no_value(self):
        listener = RMQWatcherListener()
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "c", "filter_data": {}}]

        ctx, _session = _make_session_ctx(existing_subs=[])

        with _sync_env(ctx, {"d"}) as mock_up:
            listener._sync_to_db(scanned)

        assert mock_up.call_args.kwargs["ack_unmatched"] is False

    def test_one_miss_on_active_dag_keeps_the_row_and_counts_it(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="old_dag")
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"old_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert _counted_miss(session) == 1

    def test_row_is_deleted_one_miss_past_the_grace_period(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="old_dag", absent_cycles=_ABSENT_GRACE_CYCLES)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"old_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_called_once()

    def test_last_miss_inside_the_grace_period_still_keeps_the_row(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="old_dag", absent_cycles=_ABSENT_GRACE_CYCLES - 1)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"old_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert _counted_miss(session) == _ABSENT_GRACE_CYCLES

    def test_enabled_sub_of_inactive_dag_survives_the_first_miss(self):
        """A broken DAG file drops the key and is_active at once — one shared miss must survive."""
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="gone_dag", enabled=True, absent_cycles=None)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert _counted_miss(session) == 1

    def test_enabled_sub_of_inactive_dag_survives_the_second_miss(self):
        """One count can be a race between schedulers rather than a miss.

        A miss written from a value read before another scheduler's successful scan
        lands after that scan's reset and is indistinguishable from a real one, so the
        early deletion asks for a count that no single such write can produce.
        """
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="gone_dag", enabled=True, absent_cycles=1)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert _counted_miss(session) == 2

    def test_enabled_sub_of_inactive_dag_is_deleted_on_the_third_miss(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="gone_dag", enabled=True, absent_cycles=_INACTIVE_DAG_MISSES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_called_once()

    def test_disabled_sub_of_inactive_dag_gets_the_full_grace_period(self):
        """No consumer, nothing to lose — and the flag is exactly what must not be lost."""
        listener = RMQWatcherListener()
        ctx, session = _make_session_ctx(existing_subs=[])

        for miss in range(1, _ABSENT_GRACE_CYCLES + 1):
            existing = _existing_sub(dag_id="gone_dag", enabled=False, absent_cycles=miss - 1)
            ctx, session = _make_session_ctx(existing_subs=[existing])
            with _sync_env(ctx, {"other_dag"}):
                listener._sync_to_db([])
            _delete_mock(session).assert_not_called()
            assert _counted_miss(session) == miss

        existing = _existing_sub(
            dag_id="gone_dag", enabled=False, absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])
        with _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])
        _delete_mock(session).assert_called_once()

    def test_failure_to_read_active_dag_ids_does_not_speed_deletion_up(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="gone_dag", enabled=True, absent_cycles=_INACTIVE_DAG_MISSES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, RuntimeError("db is down")):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert _counted_miss(session) == _INACTIVE_DAG_MISSES + 1

    def test_paused_dag_counts_as_active_and_keeps_the_grace_period(self):
        """get_active_dag_ids reports a paused DAG as active (ADR 0006), so no shortcut."""
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="paused_dag", enabled=True, absent_cycles=_INACTIVE_DAG_MISSES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"paused_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert _counted_miss(session) == _INACTIVE_DAG_MISSES + 1

    def test_key_back_in_scan_is_reported_and_leaves_deletion_alone(self, caplog):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="d", queue_name="q", absent_cycles=2)
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default", "filter_data": {}}]
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with caplog.at_level(logging.INFO), _sync_env(ctx, {"d"}) as mock_up:
            listener._sync_to_db(scanned)

        _delete_mock(session).assert_not_called()
        # the counter itself is cleared by upsert_subscription, which is patched here
        mock_up.assert_called_once()
        assert any("is back in the scan" in r.getMessage() for r in caplog.records)

    def test_delete_is_conditional_on_the_counter_that_was_read(self):
        """A scheduler that read a stale counter must not delete a row another one revived."""
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="old_dag", absent_cycles=_ABSENT_GRACE_CYCLES)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"old_dag"}):
            listener._sync_to_db([])

        clause = session.query.return_value.filter_by.return_value.filter.call_args.args[0]
        # str() on the expression renders the value as a bind parameter, which reads the
        # same whatever the value is — the value itself is the whole point here.
        assert clause.right.value == _ABSENT_GRACE_CYCLES

    def test_the_short_circuit_delete_is_conditional_on_its_counter_too(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="gone_dag", enabled=True, absent_cycles=_INACTIVE_DAG_MISSES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_called_once()
        clause = session.query.return_value.filter_by.return_value.filter.call_args.args[0]
        assert clause.right.value == _INACTIVE_DAG_MISSES

    def test_a_counted_miss_is_logged_as_a_warning(self, caplog):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="old_dag", queue_name="q")
        ctx, _ = _make_session_ctx(existing_subs=[existing])

        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"old_dag"}):
            listener._sync_to_db([])

        assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and
                "old_dag/q" in r.getMessage() and "missing from the scan" in r.getMessage()]

    def test_the_grace_period_deletion_is_logged(self, caplog):
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="old_dag", queue_name="q", absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, _ = _make_session_ctx(existing_subs=[existing])

        with caplog.at_level(logging.INFO,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"old_dag"}):
            listener._sync_to_db([])

        assert [r.getMessage() for r in caplog.records if r.levelno == logging.INFO and
                "trusted scans in a row — deleted" in r.getMessage()]

    def test_a_key_recreated_after_a_deletion_is_reported(self, caplog):
        """The flip the backlog item is about: a row comes back consuming, silently.

        ``upsert_subscription`` preserves ``enabled`` for a row that exists and creates
        a new one otherwise, so a key that returns after its row went carries the
        default flag — and the operator who switched it off has to hear about it.
        """
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="d", queue_name="q", enabled=False, absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, _ = _make_session_ctx(existing_subs=[existing])
        with _sync_env(ctx, {"d"}):
            listener._sync_to_db([])

        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default",
                    "filter_data": {}}]
        ctx, _ = _make_session_ctx(existing_subs=[])
        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"d"}):
            listener._sync_to_db(scanned)

        assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and
                "d/q" in r.getMessage() and "read the row as disabled" in r.getMessage()]

    def test_the_reported_flag_is_named_as_what_the_cycle_read(self, caplog):
        """The grace-period DELETE matches on the counter, not on ``enabled``.

        An operator toggling the row between this cycle's read and its write changes
        what goes, so the line says what the cycle saw instead of asserting what the
        row held.
        """
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="d", queue_name="q", enabled=False, absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, _ = _make_session_ctx(existing_subs=[existing])
        with _sync_env(ctx, {"d"}):
            listener._sync_to_db([])

        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default",
                    "filter_data": {}}]
        ctx, _ = _make_session_ctx(existing_subs=[])
        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"d"}):
            listener._sync_to_db(scanned)

        reported = [r.getMessage() for r in caplog.records
                    if "is in the scan again" in r.getMessage()]
        assert reported
        assert "the cycle that deleted it read the row as disabled" in reported[0]

    def test_a_key_that_never_went_is_not_reported_as_recreated(self, caplog):
        listener = RMQWatcherListener()
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default",
                    "filter_data": {}}]
        ctx, _ = _make_session_ctx(existing_subs=[])

        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"d"}):
            listener._sync_to_db(scanned)

        assert [r.getMessage() for r in caplog.records
                if "is in the scan again" in r.getMessage()] == []

    def test_a_row_another_scheduler_kept_is_not_reported_as_recreated(self):
        """The conditional DELETE that removed nothing has no recreate to report."""
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="d", queue_name="q", absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])
        _delete_mock(session).return_value = 0

        with _sync_env(ctx, {"d"}):
            listener._sync_to_db([])

        assert listener._deleted_dag_file_rows == {}

    def test_a_deletion_is_remembered_only_once_it_is_committed(self):
        """A transaction that never landed leaves no record of a row still in the table.

        The record is what makes the next scan report a recreation, and a row the
        database still holds is not going to be recreated by anything.
        """
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="d", queue_name="q", absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])
        _delete_mock(session).return_value = 1
        session.commit.side_effect = RuntimeError("the database went away")

        with _sync_env(ctx, {"d"}), pytest.raises(RuntimeError):
            listener._sync_to_db([])

        assert listener._deleted_dag_file_rows == {}

    def test_a_deletion_is_logged_only_once_it_is_committed(self, caplog):
        """A line saying a row went, for a row that is still in the table.

        The deletion is one statement of a transaction that goes on to upsert every
        scanned key, and a failure anywhere in it takes the deletion back — so the
        line about the row is written from what the commit kept.
        """
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="d", queue_name="q", absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])
        _delete_mock(session).return_value = 1
        session.commit.side_effect = RuntimeError("the database went away")

        with caplog.at_level(logging.INFO,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"d"}), pytest.raises(RuntimeError):
            listener._sync_to_db([])

        messages = [r.getMessage() for r in caplog.records]
        assert not any("deleted" in m for m in messages), messages

    def test_a_returning_key_is_forgotten_only_once_the_upsert_is_committed(self):
        """The report is owed until the row it is about has actually been recreated."""
        listener = RMQWatcherListener()
        listener._deleted_dag_file_rows[("d", "q", "rmq_default")] = False
        ctx, session = _make_session_ctx(existing_subs=[])
        session.commit.side_effect = RuntimeError("the database went away")
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default",
                    "filter_data": {}}]

        with _sync_env(ctx, {"d"}), pytest.raises(RuntimeError):
            listener._sync_to_db(scanned)

        assert listener._deleted_dag_file_rows == {("d", "q", "rmq_default"): False}

    def test_a_counted_miss_is_logged_only_once_it_is_committed(self, caplog):
        """The count is one statement of a transaction that goes on to upsert.

        A failure anywhere in that transaction takes the count back, and the warning
        is what an operator has to read the counter's progress from.
        """
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="d", queue_name="q")
        ctx, session = _make_session_ctx(existing_subs=[existing])
        _update_mock(session).return_value = 1
        session.commit.side_effect = RuntimeError("the database went away")

        with caplog.at_level(logging.INFO,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"d"}), pytest.raises(RuntimeError):
            listener._sync_to_db([])

        messages = [r.getMessage() for r in caplog.records]
        assert not any("keeping the row for now" in m for m in messages), messages

    def test_a_cleared_miss_counter_is_logged_only_once_it_is_committed(self, caplog):
        """The counter is cleared by the upsert, which is part of the same transaction."""
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="d", queue_name="q", absent_cycles=2)
        ctx, session = _make_session_ctx(existing_subs=[existing])
        session.commit.side_effect = RuntimeError("the database went away")
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default",
                    "filter_data": {}}]

        with caplog.at_level(logging.INFO,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"d"}), pytest.raises(RuntimeError):
            listener._sync_to_db(scanned)

        messages = [r.getMessage() for r in caplog.records]
        assert not any("miss counter cleared" in m for m in messages), messages

    def test_a_recreated_key_is_reported_only_once_it_is_committed(self, caplog):
        """The row the warning is about is created by the upsert this commit carries."""
        listener = RMQWatcherListener()
        listener._deleted_dag_file_rows[("d", "q", "rmq_default")] = False
        ctx, session = _make_session_ctx(existing_subs=[])
        session.commit.side_effect = RuntimeError("the database went away")
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default",
                    "filter_data": {}}]

        with caplog.at_level(logging.INFO,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"d"}), pytest.raises(RuntimeError):
            listener._sync_to_db(scanned)

        messages = [r.getMessage() for r in caplog.records]
        assert not any("is in the scan again" in m for m in messages), messages

    def test_a_synchronisation_of_a_superseded_scan_commits_nothing(self):
        """The listing this pass decided against is one the process has read past.

        A sync step that outlived its timeout keeps running in its pool worker, and
        the deletions it computed answer a question about the folder that a later
        cycle has already asked again.
        """
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="d", queue_name="q", enabled=False, absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])
        _delete_mock(session).return_value = 1
        listener._scan_generation = 7

        with _sync_env(ctx, {"d"}):
            listener._sync_to_db([], 6)

        session.commit.assert_not_called()
        session.rollback.assert_called_once()
        assert listener._deleted_dag_file_rows == {}

    def test_a_synchronisation_of_the_current_scan_commits(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="d", queue_name="q", absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])
        _delete_mock(session).return_value = 1
        listener._scan_generation = 7

        with _sync_env(ctx, {"d"}):
            listener._sync_to_db([], 7)

        session.commit.assert_called_once()
        assert listener._deleted_dag_file_rows == {("d", "q", "rmq_default"): True}

    def test_the_deleted_keys_a_process_remembers_are_bounded(self):
        """Deleted keys mostly never come back, and dag_ids can be generated per run.

        What the map is for is a key returning within a few cycles of its deletion, so
        the oldest record makes room rather than the map growing for the life of the
        process.
        """
        listener = RMQWatcherListener()
        existing = [
            _existing_sub(dag_id=f"d{n}", queue_name="q",
                          absent_cycles=_ABSENT_GRACE_CYCLES)
            for n in range(_DELETED_ROWS_REMEMBERED + 10)
        ]
        ctx, session = _make_session_ctx(existing_subs=existing)
        _delete_mock(session).return_value = 1

        with _sync_env(ctx, {s.dag_id for s in existing}):
            listener._sync_to_db([])

        assert len(listener._deleted_dag_file_rows) == _DELETED_ROWS_REMEMBERED
        assert ("d0", "q", "rmq_default") not in listener._deleted_dag_file_rows
        newest = f"d{_DELETED_ROWS_REMEMBERED + 9}"
        assert (newest, "q", "rmq_default") in listener._deleted_dag_file_rows

    def test_the_short_circuit_deletion_is_logged(self, caplog):
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="gone_dag", queue_name="q", absent_cycles=_INACTIVE_DAG_MISSES
        )
        ctx, _ = _make_session_ctx(existing_subs=[existing])

        with caplog.at_level(logging.INFO,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])

        assert [r.getMessage() for r in caplog.records if r.levelno == logging.INFO and
                "is no longer active — deleted" in r.getMessage()]

    def test_failure_to_read_active_dag_ids_is_logged(self, caplog):
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="gone_dag", enabled=True, absent_cycles=_INACTIVE_DAG_MISSES
        )
        ctx, _ = _make_session_ctx(existing_subs=[existing])

        with caplog.at_level(logging.WARNING,
                             logger=_LISTENER_LOG), \
                _sync_env(ctx, RuntimeError("db is down")):
            listener._sync_to_db([])

        assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and
                "cannot read the active DAG ids" in r.getMessage()]

    def test_the_active_dag_ids_are_not_read_when_every_key_is_in_the_scan(self):
        """An unindexed full scan of Airflow's dag table, on every cycle, for nothing."""
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="d", queue_name="q")
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default", "filter_data": {}}]
        ctx, _ = _make_session_ctx(existing_subs=[existing])

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription"), \
             patch("airflow_provider_rmq.watcher.listener.get_active_dag_ids") as mock_active:
            listener._sync_to_db(scanned)

        mock_active.assert_not_called()

    def test_the_active_dag_ids_are_not_read_for_a_row_the_grace_period_deletes(self):
        """The grace period asks nothing of the DAG table, and waits for nothing."""
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="gone_dag", queue_name="q", enabled=True,
            absent_cycles=_ABSENT_GRACE_CYCLES,
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription"), \
             patch("airflow_provider_rmq.watcher.listener.get_active_dag_ids") as mock_active:
            listener._sync_to_db([])

        mock_active.assert_not_called()
        _delete_mock(session).assert_called_once()

    def test_the_active_dag_ids_are_read_at_most_once_per_synchronisation(self):
        listener = RMQWatcherListener()
        existing = [
            _existing_sub(dag_id="a", queue_name="q", enabled=True,
                          absent_cycles=_INACTIVE_DAG_MISSES),
            _existing_sub(dag_id="b", queue_name="q", enabled=True,
                          absent_cycles=_INACTIVE_DAG_MISSES),
        ]
        ctx, _ = _make_session_ctx(existing_subs=existing)

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription"), \
             patch("airflow_provider_rmq.watcher.listener.get_active_dag_ids") as mock_active:
            mock_active.return_value = {"a", "b"}
            listener._sync_to_db([])

        mock_active.assert_called_once()

    def test_the_active_dag_id_read_gets_a_session_of_its_own(self):
        """A failed read poisons the transaction it ran in on PostgreSQL."""
        listener = RMQWatcherListener()
        existing = _existing_sub(
            dag_id="gone_dag", enabled=True, absent_cycles=_INACTIVE_DAG_MISSES
        )
        write_ctx, write_session = _make_session_ctx(existing_subs=[existing])
        read_ctx, read_session = _make_session_ctx(existing_subs=[])

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession",
                   side_effect=[write_ctx, read_ctx]), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription"), \
             patch("airflow_provider_rmq.watcher.listener.get_active_dag_ids") as mock_active:
            mock_active.return_value = set()
            listener._sync_to_db([])

        mock_active.assert_called_once_with(read_session)
        assert read_session is not write_session
        write_session.commit.assert_called_once()

    def test_deletion_is_scoped_to_dag_file_rows(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="old_dag", absent_cycles=_ABSENT_GRACE_CYCLES)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"old_dag"}):
            listener._sync_to_db([])

        session.query.return_value.filter_by.assert_any_call(
            dag_id="old_dag", queue_name="q", conn_id="rmq_default", source="dag_file",
        )

    def test_sync_to_db_does_not_delete_subscription_still_in_scan(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="d", queue_name="q")
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default", "filter_data": {}}]
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"d"}):
            listener._sync_to_db(scanned)

        # Subscription is still in scan → must NOT be deleted
        _delete_mock(session).assert_not_called()


# ---------------------------------------------------------------------------
# _extract_subscriptions_from_file — group_key и cooldown
# ---------------------------------------------------------------------------

class TestExtractSubscriptionsGroupKeyAndCooldown:
    def test_single_queue_cooldown_zero_group_key_is_none(self, tmp_path):
        dag_file = tmp_path / "dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q', cooldown=0)\n"
            "@dag(dag_id='my_dag')\n"
            "def my_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["group_key"] is None
        assert result[0]["cooldown"] == 0

    def test_single_queue_with_cooldown_group_key_is_dag_id(self, tmp_path):
        dag_file = tmp_path / "dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queue='q', cooldown=300)\n"
            "@dag(dag_id='my_dag')\n"
            "def my_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 1
        assert result[0]["group_key"] == "my_dag"
        assert result[0]["cooldown"] == 300

    def test_queues_list_with_cooldown_creates_multiple_entries(self, tmp_path):
        dag_file = tmp_path / "dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queues=['orders', 'payments'], cooldown=60)\n"
            "@dag(dag_id='multi_queue_dag')\n"
            "def multi_queue_dag(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 2
        queue_names = {r["queue_name"] for r in result}
        assert queue_names == {"orders", "payments"}
        assert all(r["dag_id"] == "multi_queue_dag" for r in result)
        assert all(r["cooldown"] == 60 for r in result)
        assert all(r["group_key"] == "multi_queue_dag" for r in result)

    def test_queues_list_no_cooldown_group_key_is_none(self, tmp_path):
        dag_file = tmp_path / "dag.py"
        dag_file.write_text(
            "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
            "from airflow.decorators import dag\n"
            "@rmq_trigger(queues=['a', 'b'])\n"
            "@dag(dag_id='dag_no_cooldown')\n"
            "def dag_no_cooldown(): pass\n"
        )
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file(str(dag_file))
        assert len(result) == 2
        assert all(r["group_key"] is None for r in result)
        assert all(r["cooldown"] == 0 for r in result)


# ---------------------------------------------------------------------------
# _sync_to_db — передача cooldown и group_key в upsert_subscription
# ---------------------------------------------------------------------------

class TestSyncToDbCooldownAndGroupKey:
    def test_sync_to_db_passes_cooldown_and_group_key(self):
        listener = RMQWatcherListener()
        scanned = [
            {
                "dag_id": "d",
                "queue_name": "q",
                "conn_id": "rmq_default",
                "filter_data": {},
                "cooldown": 300,
                "group_key": "d",
            }
        ]
        ctx, session = _make_session_ctx(existing_subs=[])
        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription") as mock_up:
            listener._sync_to_db(scanned)

        mock_up.assert_called_once()
        call_kwargs = mock_up.call_args.kwargs
        assert call_kwargs["cooldown"] == 300
        assert call_kwargs["group_key"] == "d"

    def test_sync_to_db_cooldown_zero_stored_as_none(self):
        """cooldown=0 is normalized to None in DB (nullable int column)."""
        listener = RMQWatcherListener()
        scanned = [
            {
                "dag_id": "d",
                "queue_name": "q",
                "conn_id": "rmq_default",
                "filter_data": {},
                "cooldown": 0,
                "group_key": None,
            }
        ]
        ctx, session = _make_session_ctx(existing_subs=[])
        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription") as mock_up:
            listener._sync_to_db(scanned)

        call_kwargs = mock_up.call_args.kwargs
        assert call_kwargs["cooldown"] is None
        assert call_kwargs["group_key"] is None

    def test_sync_to_db_missing_cooldown_defaults_to_none(self):
        """Scanned sub without 'cooldown' key → upsert_subscription receives cooldown=None."""
        listener = RMQWatcherListener()
        scanned = [
            {
                "dag_id": "d",
                "queue_name": "q",
                "conn_id": "rmq_default",
                "filter_data": {},
                # no cooldown key — old-style dict
            }
        ]
        ctx, session = _make_session_ctx(existing_subs=[])
        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription") as mock_up:
            listener._sync_to_db(scanned)

        call_kwargs = mock_up.call_args.kwargs
        assert call_kwargs["cooldown"] is None


# ---------------------------------------------------------------------------
# active_subs projection — cooldown field (from _main)
# ---------------------------------------------------------------------------

class TestActiveSubs:
    def test_active_subs_cooldown_null_becomes_zero(self):
        """Sub with cooldown=NULL in DB → active_subs gets cooldown=0, not TypeError.

        Note: this test reimplements the active_subs projection formula inline rather than
        driving a real _main iteration (which would require a full async listener setup).
        It validates the formula logic — specifically that `s.cooldown or 0` converts NULL
        to 0 without raising TypeError. If the formula in _main changes, update this test.
        """
        listener = RMQWatcherListener()
        listener._stop_event = __import__("threading").Event()

        sub_mock = MagicMock()
        sub_mock.id = 1
        sub_mock.dag_id = "d"
        sub_mock.queue_name = "q"
        sub_mock.conn_id = "rmq_default"
        sub_mock.filter_data = {}
        sub_mock.cooldown = None  # NULL in DB

        calls = {"n": 0}

        async def run_once():
            # Replicate the active_subs projection from _main
            subs = [sub_mock]
            active_subs = [
                {
                    "id": s.id,
                    "dag_id": s.dag_id,
                    "queue_name": s.queue_name,
                    "conn_id": s.conn_id,
                    "filter_data": s.filter_data or {},
                    "cooldown": s.cooldown or 0,
                }
                for s in subs
            ]
            calls["n"] += 1
            return active_subs

        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(run_once())
        loop.close()

        assert calls["n"] == 1
        assert result[0]["cooldown"] == 0

    def test_active_subs_cooldown_value_preserved(self):
        """Sub with cooldown=120 in DB → active_subs gets cooldown=120."""
        sub_mock = MagicMock()
        sub_mock.cooldown = 120

        active_sub = {"cooldown": sub_mock.cooldown or 0}
        assert active_sub["cooldown"] == 120


# ---------------------------------------------------------------------------
# _main — merging exchange/routing_keys metadata into active_subs (Task 3)
# ---------------------------------------------------------------------------

def _make_db_sub(dag_id, queue_name, conn_id="rmq_default", cooldown=0, filter_data=None):
    sub = MagicMock()
    sub.id = 1
    sub.dag_id = dag_id
    sub.queue_name = queue_name
    sub.conn_id = conn_id
    sub.filter_data = filter_data or {}
    sub.cooldown = cooldown
    return sub


class TestReadActiveSubs:
    """The projection `_read_active_subs` builds is the only dict a consumer sees —
    a column missing from it never reaches the consumer task, whatever the row says."""

    def _read(self, rows):
        ctx, _session = _make_session_ctx()
        with patch(
            "airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx
        ), patch(
            "airflow_provider_rmq.watcher.listener.get_enabled_subscriptions",
            return_value=rows,
        ):
            return _read_active_subs({})

    def test_ack_unmatched_from_the_row_reaches_the_entry(self):
        row = _make_db_sub("d", "q")
        row.ack_unmatched = True
        assert self._read([row])[0]["ack_unmatched"] is True

    def test_row_without_a_value_reads_as_false(self):
        row = _make_db_sub("d", "q")
        row.ack_unmatched = None
        assert self._read([row])[0]["ack_unmatched"] is False

    def test_ack_unmatched_false_on_the_row_reaches_the_entry(self):
        row = _make_db_sub("d", "q")
        row.ack_unmatched = False
        assert self._read([row])[0]["ack_unmatched"] is False


class TestMainExchangeMetaMerge:
    """Drives a single `_main()` reconcile iteration end-to-end with mocked
    DB/manager/scan, asserting the in-memory exchange/routing_keys merge
    described in the plan (Task 3) — the DB row itself never carries this
    metadata (see Technical Details → "Почему миграция БД не нужна")."""

    def _run_one_iteration(self, listener, scanned, db_subs):
        """Run `_main()` for exactly one reconcile cycle and capture the
        `active_subs` list passed to `RMQConsumerManager.reconcile`."""
        listener._stop_event = threading.Event()
        listener._parent_pid = os.getppid()
        listener._scan_subscriptions = MagicMock(return_value=scanned)
        listener._sync_to_db = MagicMock()

        ctx, session = _make_session_ctx()

        captured = {}

        manager = MagicMock()
        manager.start = AsyncMock()
        manager.stop = AsyncMock()

        async def fake_reconcile(active_subs):
            captured["active_subs"] = active_subs
            # Stop the loop after the first iteration so _main returns.
            listener._stop_event.set()

        manager.reconcile = AsyncMock(side_effect=fake_reconcile)

        with patch(
            "airflow_provider_rmq.watcher.listener.RMQConsumerManager",
            return_value=manager,
        ), patch(
            "airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx
        ), patch(
            "airflow_provider_rmq.watcher.listener.get_enabled_subscriptions",
            return_value=db_subs,
        ), patch(
            "airflow_provider_rmq.watcher.listener.is_schema_ready", return_value=True
        ), patch(
            "airflow_provider_rmq.watcher.listener._read_settings",
            return_value=(None, None),
        ), patch(
            "asyncio.sleep", new=AsyncMock()
        ):
            asyncio.run(listener._main())

        return captured["active_subs"]

    def test_exchange_db_row_gets_merged_metadata(self):
        """DB row matching a scanned exchange subscription gets exchange/routing_keys merged in."""
        listener = RMQWatcherListener()
        scanned = [
            {
                "dag_id": "jetstat_dag",
                "queue_name": "rmq_watcher.sub.jetstat_dag",
                "conn_id": "rmq_default",
                "filter_data": {},
                "cooldown": 0,
                "exchange": "jetstat.airflow",
                "routing_keys": ["abc123.succeeded"],
            }
        ]
        db_sub = _make_db_sub("jetstat_dag", "rmq_watcher.sub.jetstat_dag")

        active_subs = self._run_one_iteration(listener, scanned, [db_sub])

        assert len(active_subs) == 1
        entry = active_subs[0]
        assert entry["exchange"] == "jetstat.airflow"
        assert entry["routing_keys"] == ["abc123.succeeded"]
        # Core queue-consumption fields are still present and correct.
        assert entry["dag_id"] == "jetstat_dag"
        assert entry["queue_name"] == "rmq_watcher.sub.jetstat_dag"
        assert entry["conn_id"] == "rmq_default"

    def test_plain_queue_db_row_not_affected(self):
        """A regular queue= subscription has no scanned exchange counterpart → no exchange key."""
        listener = RMQWatcherListener()
        scanned = [
            {
                "dag_id": "plain_dag",
                "queue_name": "orders",
                "conn_id": "rmq_default",
                "filter_data": {},
                "cooldown": 0,
            }
        ]
        db_sub = _make_db_sub("plain_dag", "orders")

        active_subs = self._run_one_iteration(listener, scanned, [db_sub])

        assert len(active_subs) == 1
        entry = active_subs[0]
        assert "exchange" not in entry
        assert "routing_keys" not in entry
        assert entry["dag_id"] == "plain_dag"
        assert entry["queue_name"] == "orders"

    def test_ui_subscription_never_gets_exchange_metadata(self):
        """A UI-sourced subscription has no entry in `scanned` at all (it isn't
        produced by AST parsing), so it can never match `exchange_meta` and
        never gets exchange/routing_keys merged in."""
        listener = RMQWatcherListener()
        scanned: list[dict] = []  # nothing scanned from DAG files this cycle
        db_sub = _make_db_sub("ui_dag", "ui_queue")

        active_subs = self._run_one_iteration(listener, scanned, [db_sub])

        assert len(active_subs) == 1
        entry = active_subs[0]
        assert "exchange" not in entry
        assert "routing_keys" not in entry
        assert entry["dag_id"] == "ui_dag"
        assert entry["queue_name"] == "ui_queue"

    def test_exchange_meta_keyed_by_conn_id_does_not_cross_match(self):
        """Same dag_id/queue_name but different conn_id must not be merged —
        the lookup key includes conn_id, matching the unique constraint on
        RMQSubscription."""
        listener = RMQWatcherListener()
        scanned = [
            {
                "dag_id": "d",
                "queue_name": "rmq_watcher.sub.d",
                "conn_id": "other_conn",
                "filter_data": {},
                "cooldown": 0,
                "exchange": "ex",
                "routing_keys": ["k"],
            }
        ]
        db_sub = _make_db_sub("d", "rmq_watcher.sub.d", conn_id="rmq_default")

        active_subs = self._run_one_iteration(listener, scanned, [db_sub])

        assert len(active_subs) == 1
        assert "exchange" not in active_subs[0]


# ---------------------------------------------------------------------------
# Cycle watchdog, tunables cache, schema-migration retry and the thread pools
# (Task 4)
# ---------------------------------------------------------------------------

async def _never_returns(*args, **kwargs):
    """Stand-in for a call that hangs: it outlives any timeout under test."""
    await asyncio.sleep(30)


def _fake_manager():
    """Consumer manager stand-in whose async methods return immediately."""
    manager = MagicMock()
    manager.start = AsyncMock()
    manager.stop = AsyncMock()
    manager.reconcile = AsyncMock()
    return manager


def _instant_stop_event():
    """Real stop event whose inter-loop wait returns at once.

    ``_run_loop`` waits 30 seconds between loops; a restart test would otherwise
    spend that long doing nothing.
    """
    event = threading.Event()
    event.wait = lambda timeout=None: event.is_set()
    return event


def _cycle_listener():
    """Listener with the scan and the DB sync stubbed and no sleep between cycles."""
    listener = RMQWatcherListener()
    listener._stop_event = threading.Event()
    listener._parent_pid = os.getppid()
    listener._scan_subscriptions = MagicMock(return_value=[])
    listener._sync_to_db = MagicMock()
    listener._reconcile_interval = 0
    # The tunables are set by each test; the read itself has its own tests.
    listener._refresh_settings = AsyncMock()
    return listener


@contextlib.contextmanager
def _cycle_patches(manager, db_subs=()):
    """Patch everything a cycle reaches outside the listener itself."""
    ctx, _ = _make_session_ctx()
    with patch(
        "airflow_provider_rmq.watcher.listener.RMQConsumerManager", return_value=manager
    ) as manager_cls, patch(
        "airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx
    ), patch(
        "airflow_provider_rmq.watcher.listener.get_enabled_subscriptions",
        return_value=list(db_subs),
    ), patch(
        "airflow_provider_rmq.watcher.listener.is_schema_ready", return_value=True
    ), patch(
        "airflow_provider_rmq.watcher.listener._read_settings", return_value=(None, None)
    ):
        yield manager_cls


class TestCycleWorkOffTheLoop:
    """The cycle's blocking steps belong in the cycle pool, not on the loop thread.

    A blocked loop services no timers, so the very watchdog wrapped around the cycle
    would never fire — and the AMQP heartbeats sent from that loop would stop with it.
    """

    @pytest.mark.asyncio
    async def test_scan_sync_and_subscription_read_leave_the_loop_thread(self):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        loop_thread = threading.current_thread()
        threads = {}

        def record(name, result=None):
            def call(*args):
                threads[name] = threading.current_thread()
                return result
            return call

        listener._scan_subscriptions = record("scan", [])
        listener._sync_to_db = record("sync")

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.get_enabled_subscriptions",
            side_effect=record("read subs", []),
        ):
            await listener._run_cycle()

        assert set(threads) == {"scan", "sync", "read subs"}
        assert all(thread is not loop_thread for thread in threads.values())
        manager.reconcile.assert_awaited_once_with([])

    @pytest.mark.asyncio
    async def test_settings_read_leaves_the_loop_thread(self):
        listener = RMQWatcherListener()
        loop_thread = threading.current_thread()
        seen = []

        def read():
            seen.append(threading.current_thread())
            return (120, None)

        with patch(
            "airflow_provider_rmq.watcher.listener._read_settings", side_effect=read
        ):
            await listener._refresh_settings()

        assert seen and seen[0] is not loop_thread
        assert listener._reconcile_interval == 120

    @pytest.mark.asyncio
    async def test_a_blocking_scan_trips_the_cycle_watchdog(self):
        """A step stuck inside a worker still costs the cycle its budget."""
        listener = _cycle_listener()
        listener._cycle_timeout_override = 0.1
        manager = _fake_manager()
        release = threading.Event()
        listener._scan_subscriptions = _blocked_by(release, result=[])
        try:
            with _cycle_patches(manager), pytest.raises(asyncio.TimeoutError):
                await listener._main()
        finally:
            release.set()

        assert listener._phase == "scan"
        manager.reconcile.assert_not_awaited()


class TestCycleStepsAreBounded:
    """The cycle's blocking steps run under a bound of their own, and only one attempt
    of each is ever in flight.

    Without both, a metadata database that stopped answering costs the whole cycle
    budget every time round: the loop is recreated, every consumer task is cancelled and
    every connection closed — and each such cycle leaves one more worker of the
    four-worker cycle pool stuck for good, until even the filesystem scan cannot start
    and the liveness check stops running at all.
    """

    @pytest.mark.asyncio
    async def test_a_stuck_step_ends_the_cycle_without_spending_its_budget(self, caplog):
        listener = _cycle_listener()
        listener._cycle_timeout_override = 30
        manager = _fake_manager()
        listener._manager = manager
        release = threading.Event()
        listener._scan_subscriptions = _blocked_by(release, result=[])
        try:
            with _cycle_patches(manager), patch(
                "airflow_provider_rmq.watcher.listener._STEP_TIMEOUT", 0.05
            ), patch("airflow_provider_rmq.watcher.listener.incr") as incr, \
                    caplog.at_level(
                        logging.WARNING,
                        logger=_LISTENER_LOG,
                    ):
                await listener._run_cycle()
        finally:
            release.set()

        manager.reconcile.assert_not_awaited()
        incr.assert_any_call("rmq_watcher.cycle_step_timed_out")
        assert any("did not finish" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_a_step_still_in_a_worker_is_not_submitted_again(self, caplog):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        release = threading.Event()
        starts = []
        listener._scan_subscriptions = _blocked_by(release, result=[], calls=starts)
        try:
            with _cycle_patches(manager), patch(
                "airflow_provider_rmq.watcher.listener._STEP_TIMEOUT", 0.05
            ), patch("airflow_provider_rmq.watcher.listener.incr") as incr, \
                    caplog.at_level(
                        logging.WARNING,
                        logger=_LISTENER_LOG,
                    ):
                await listener._run_cycle()
                await listener._run_cycle()
                await listener._run_cycle()
        finally:
            release.set()

        assert starts == [1], "one attempt of a step at a time, whatever the cycle count"
        incr.assert_any_call("rmq_watcher.cycle_step_skipped")
        assert any("still running" in r.getMessage() for r in caplog.records)
        manager.reconcile.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_step_that_returns_lets_the_next_cycle_try_again(self):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager

        with _cycle_patches(manager):
            await listener._run_cycle()
            await listener._run_cycle()

        assert listener._scan_subscriptions.call_count == 2
        assert manager.reconcile.await_count == 2

    @pytest.mark.asyncio
    async def test_a_subscription_read_that_times_out_never_reaches_reconcile(self):
        """Reconciling against a list the query could not deliver would cancel every
        consumer whose subscription simply did not come back."""
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        release = threading.Event()

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener._STEP_TIMEOUT", 0.05
        ), patch(
            "airflow_provider_rmq.watcher.listener.get_enabled_subscriptions",
            side_effect=_blocked_by(release, result=[]),
        ):
            try:
                await listener._run_cycle()
            finally:
                release.set()

        manager.reconcile.assert_not_awaited()


class TestUntrustedScan:
    """A scan the cycle cannot trust skips the sync step and nothing else.

    Skipping `_sync_to_db` is what keeps a result that says nothing about the code from
    counting a miss against every dag_file row whose key it failed to name — while
    consumption, the liveness check and the status writes keep running from the
    database rows.
    """

    @pytest.mark.asyncio
    async def test_untrusted_scan_skips_sync_but_reaches_reconcile(self):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        listener._scan_subscriptions = MagicMock(side_effect=_UntrustedScan("folder gone"))

        with _cycle_patches(manager):
            await listener._run_cycle()

        listener._sync_to_db.assert_not_called()
        manager.reconcile.assert_awaited_once_with([])

    @pytest.mark.asyncio
    async def test_exchange_meta_comes_from_the_last_good_scan(self):
        """The exchange flag lives only in the scan cache, and reconcile has no other
        source for it: an untrusted cycle borrows the last trusted result."""
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        scanned = [{
            "dag_id": "d", "queue_name": "q", "conn_id": "rmq_default",
            "exchange": "ex", "routing_keys": ["rk"],
        }]
        db_subs = [_make_db_sub("d", "q")]

        with _cycle_patches(manager, db_subs=db_subs):
            listener._scan_subscriptions = MagicMock(return_value=scanned)
            await listener._run_cycle()
            listener._scan_subscriptions = MagicMock(side_effect=_UntrustedScan("empty listing"))
            await listener._run_cycle()

        assert listener._last_good_scan == scanned
        entry = manager.reconcile.await_args.args[0][0]
        assert entry["exchange"] == "ex"
        assert entry["routing_keys"] == ["rk"]

    @pytest.mark.asyncio
    async def test_untrusted_scan_without_any_good_scan_still_reconciles(self):
        """A watcher that never had a trusted scan keeps consuming: the ui
        subscriptions in the database have nothing to do with the DAG files."""
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        listener._scan_subscriptions = MagicMock(side_effect=_UntrustedScan("no dags folder"))
        db_subs = [_make_db_sub("d", "q")]

        with _cycle_patches(manager, db_subs=db_subs):
            await listener._run_cycle()

        listener._sync_to_db.assert_not_called()
        entry = manager.reconcile.await_args.args[0][0]
        assert "exchange" not in entry

    @pytest.mark.asyncio
    async def test_error_is_logged_with_the_reason(self, caplog):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        listener._scan_subscriptions = MagicMock(
            side_effect=_UntrustedScan("the DAG folder cannot be listed")
        )

        with _cycle_patches(manager), caplog.at_level(
            logging.ERROR, logger=_LISTENER_LOG
        ):
            await listener._run_cycle()

        messages = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any("the DAG folder cannot be listed" in m for m in messages)

    @pytest.mark.asyncio
    async def test_a_step_in_flight_scan_ends_the_cycle_before_reconcile(self, caplog):
        """Only `_UntrustedScan` substitutes the previous result; the other two ways the
        scan step ends still end the cycle before reconcile."""
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        listener._last_good_scan = [{"dag_id": "d", "queue_name": "q"}]
        release = threading.Event()
        listener._scan_subscriptions = _blocked_by(release, result=[])
        try:
            with _cycle_patches(manager), patch(
                "airflow_provider_rmq.watcher.listener._STEP_TIMEOUT", 0.05
            ), caplog.at_level(
                logging.WARNING, logger=_LISTENER_LOG
            ):
                await listener._run_cycle()   # times out, leaving the attempt in flight
                await listener._run_cycle()   # _StepInFlight
        finally:
            release.set()

        assert listener._last_good_scan == [{"dag_id": "d", "queue_name": "q"}]
        listener._sync_to_db.assert_not_called()
        manager.reconcile.assert_not_awaited()
        assert any("did not finish" in r.getMessage() for r in caplog.records)
        assert any("still running" in r.getMessage() for r in caplog.records)


class TestCycleWatchdog:
    @pytest.mark.asyncio
    async def test_cycle_timeout_reaches_the_caller_of_main(self):
        """The cycle's own `except Exception` must not see the watchdog's timeout.

        ``asyncio.TimeoutError`` is an ``Exception``, so a handler wrapped around the
        timeout instead of placed inside the cycle would swallow it and leave the
        whole layer a no-op — the loop would never be recreated.
        """
        listener = _cycle_listener()
        listener._cycle_timeout_override = 0.05
        manager = _fake_manager()
        manager.reconcile = _never_returns

        with _cycle_patches(manager), pytest.raises(asyncio.TimeoutError):
            await listener._main()

        assert listener._phase == "reconcile"
        manager.stop.assert_awaited()   # the manager is torn down on the way out

    @pytest.mark.asyncio
    async def test_cycle_timeout_reports_the_phase_and_counts_a_metric(self, caplog):
        listener = _cycle_listener()
        listener._cycle_timeout_override = 0.05
        manager = _fake_manager()
        manager.reconcile = _never_returns

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.incr"
        ) as incr, caplog.at_level(
            logging.ERROR, logger=_LISTENER_LOG
        ), pytest.raises(asyncio.TimeoutError):
            await listener._main()

        incr.assert_called_once_with("rmq_watcher.cycle_timed_out")
        assert any("reconcile" in record.getMessage() for record in caplog.records)

    @pytest.mark.asyncio
    async def test_ordinary_cycle_error_is_logged_and_swallowed(self):
        """Moving the handler inside the cycle must not change what it handles."""
        listener = _cycle_listener()
        manager = _fake_manager()
        manager.reconcile = AsyncMock(side_effect=RuntimeError("broker is gone"))
        listener._manager = manager

        with _cycle_patches(manager):
            await listener._run_cycle()   # no exception escapes

        manager.reconcile.assert_awaited()

    def test_hung_cycle_recreates_the_event_loop(self):
        listener = RMQWatcherListener()
        listener._stop_event = _instant_stop_event()
        loops = []

        async def fake_main():
            loops.append(asyncio.get_running_loop())
            if len(loops) == 1:
                raise asyncio.TimeoutError()
            listener._stop_event.set()

        with patch.object(listener, "_main", side_effect=fake_main):
            listener._run_loop()

        assert len(loops) == 2
        assert loops[0] is not loops[1]

    def test_loop_restart_reuses_the_pools_and_spares_the_default_executor(self):
        listener = RMQWatcherListener()
        listener._stop_event = _instant_stop_event()
        seen = []

        async def fake_main():
            # Reaches the pools the way the real ``_main`` does.
            seen.append(listener._ensure_pools())
            if len(seen) == 1:
                raise asyncio.TimeoutError()
            listener._stop_event.set()

        with patch.object(listener, "_main", side_effect=fake_main), patch.object(
            asyncio.base_events.BaseEventLoop, "shutdown_default_executor"
        ) as shutdown:
            listener._run_loop()

        (cycle_first, consumer_first), (cycle_second, consumer_second) = seen
        assert isinstance(cycle_first, BoundedExecutor)
        assert isinstance(consumer_first, BoundedExecutor)
        assert cycle_second is cycle_first
        assert consumer_second is consumer_first
        shutdown.assert_not_called()

    def test_a_call_stuck_in_the_pool_does_not_block_the_next_loop(self):
        """A worker still occupied by a call the loop gave up on must not hold the
        loop thread — the pool outlives the loop by design."""
        listener = RMQWatcherListener()
        listener._stop_event = _instant_stop_event()
        release = threading.Event()
        rounds = []
        escaped = []

        async def fake_main():
            rounds.append(1)
            cycle_pool, _ = listener._ensure_pools()
            if len(rounds) == 1:
                # The timeout is what ends this loop; ``_run_loop`` swallows whatever
                # escapes, so the test keeps its own record of what that was.
                try:
                    await call_with_timeout(
                        asyncio.wrap_future(cycle_pool.submit(release.wait, 5)),
                        0.05,
                    )
                except BaseException as exc:
                    escaped.append(exc)
                    raise
            listener._stop_event.set()

        try:
            with patch.object(listener, "_main", side_effect=fake_main):
                listener._run_loop()
        finally:
            release.set()

        assert len(rounds) == 2
        assert len(escaped) == 1 and isinstance(escaped[0], asyncio.TimeoutError)

    @pytest.mark.asyncio
    async def test_hung_manager_stop_does_not_block_teardown(self):
        listener = _cycle_listener()
        manager = _fake_manager()
        manager.stop = _never_returns
        listener._manager = manager

        with patch("airflow_provider_rmq.watcher.listener._STOP_TIMEOUT", 0.05):
            await listener._stop_manager()   # returns instead of hanging

    @pytest.mark.asyncio
    async def test_manager_gets_the_consumer_pool(self):
        """Cycle work and consumer work must never share workers."""
        listener = _cycle_listener()
        manager = _fake_manager()

        async def stop_after_first(subs):
            listener._stop_event.set()

        manager.reconcile = AsyncMock(side_effect=stop_after_first)

        with _cycle_patches(manager) as manager_cls:
            await listener._main()

        cycle_pool, consumer_pool = listener._ensure_pools()
        assert manager_cls.call_args.kwargs["executor"] is consumer_pool
        assert manager_cls.call_args.kwargs["cycle_executor"] is cycle_pool
        assert consumer_pool is not cycle_pool

    @pytest.mark.asyncio
    async def test_busy_consumer_pool_leaves_the_cycle_pool_free(self):
        listener = RMQWatcherListener()
        listener._consumer_pool = BoundedExecutor("test-consumer", 2)
        release = threading.Event()
        for _ in range(2):
            listener._consumer_pool.submit(release.wait, 5)
        cycle_pool, _ = listener._ensure_pools()
        try:
            assert await cycle_pool.run(lambda: "cycle work") == "cycle work"
        finally:
            release.set()


class TestPoolsOnFirstUse:
    """The listener is built in every Airflow component that loads the plugin, and only
    the process that runs the loop may end up with the pools' threads."""

    def test_a_new_listener_holds_no_pool(self):
        listener = RMQWatcherListener()

        assert listener._cycle_pool is None
        assert listener._consumer_pool is None

    def test_first_use_builds_both_pools_and_later_uses_get_the_same_ones(self):
        listener = RMQWatcherListener()

        cycle_pool, consumer_pool = listener._ensure_pools()
        again = listener._ensure_pools()

        assert isinstance(cycle_pool, BoundedExecutor)
        assert isinstance(consumer_pool, BoundedExecutor)
        assert cycle_pool is not consumer_pool
        assert cycle_pool.name == "rmq-watcher-cycle"
        assert consumer_pool.name == "rmq-watcher-consumer"
        assert again[0] is cycle_pool and again[1] is consumer_pool
        assert listener._cycle_pool is cycle_pool
        assert listener._consumer_pool is consumer_pool

    def test_a_pool_already_in_place_is_kept(self):
        listener = RMQWatcherListener()
        own = BoundedExecutor("test-cycle", 1)
        listener._cycle_pool = own

        cycle_pool, consumer_pool = listener._ensure_pools()

        assert cycle_pool is own
        assert isinstance(consumer_pool, BoundedExecutor)

    @pytest.mark.asyncio
    async def test_a_cycle_run_directly_needs_no_pool_prepared(self):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager

        with _cycle_patches(manager):
            await listener._run_cycle()

        listener._scan_subscriptions.assert_called_once()
        manager.reconcile.assert_awaited_once()
        assert listener._cycle_pool is not None


class TestRunAndRequestStop:
    """The entry points the runner drives the loop through."""

    def test_the_stop_event_exists_from_construction(self):
        listener = RMQWatcherListener()

        assert isinstance(listener._stop_event, threading.Event)
        assert not listener._stop_event.is_set()

    def test_run_records_the_parent_and_runs_the_loop_in_the_calling_thread(self, caplog):
        listener = RMQWatcherListener()
        ran_in = []

        with caplog.at_level(
            logging.INFO, logger=_LISTENER_LOG
        ), patch.object(
            listener, "_run_loop", side_effect=lambda: ran_in.append(threading.current_thread())
        ):
            listener.run(parent_pid=4242)

        assert ran_in == [threading.current_thread()]
        assert listener._parent_pid == 4242
        messages = [record.getMessage() for record in caplog.records]
        started = [m for m in messages if "RMQ Watcher loop started" in m]
        assert started, messages
        assert re.search(r"default reconcile interval of 60s", started[0]), started[0]
        assert re.search(r"cycle budget of 300s", started[0]), started[0]
        assert RECONCILE_INTERVAL_VAR in started[0]
        assert CYCLE_TIMEOUT_VAR in started[0]

    def test_a_stop_requested_before_run_returns_without_a_cycle(self):
        listener = RMQWatcherListener()
        main = MagicMock()

        listener.request_stop()
        started = time.monotonic()
        with patch.object(listener, "_main", main):
            listener.run(parent_pid=os.getppid())

        main.assert_not_called()
        assert time.monotonic() - started < 1.0

    def test_request_stop_twice_is_harmless(self):
        listener = RMQWatcherListener()
        loop = MagicMock()
        loop.is_closed.return_value = False
        listener._waker = (loop, MagicMock())

        listener.request_stop()
        listener.request_stop()

        assert listener._stop_event.is_set()
        assert loop.call_soon_threadsafe.call_count == 2

    def test_request_stop_without_a_loop_only_sets_the_event(self):
        listener = RMQWatcherListener()

        listener.request_stop()
        listener.request_stop()

        assert listener._stop_event.is_set()

    @pytest.mark.asyncio
    async def test_request_stop_wakes_the_loop_out_of_its_wait(self):
        listener = _cycle_listener()
        listener._reconcile_interval = 30
        manager = _fake_manager()
        parked = asyncio.Event()

        async def reconcile(subs):
            parked.set()

        manager.reconcile = AsyncMock(side_effect=reconcile)

        with _cycle_patches(manager):
            task = asyncio.ensure_future(listener._main())
            await call_with_timeout(parked.wait(), 5)
            started = time.monotonic()
            listener.request_stop()
            await call_with_timeout(task, 5)

        assert time.monotonic() - started < 1.0
        manager.stop.assert_awaited()


class TestParentCheck:
    """A loop whose scheduler has exited stops on its own, pass by pass."""

    @pytest.mark.asyncio
    async def test_a_changed_parent_stops_the_loop_before_the_next_pass(self, caplog):
        listener = _cycle_listener()
        listener._parent_pid = 4242
        manager = _fake_manager()

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.os.getppid", side_effect=[4242, 1]
        ), caplog.at_level(
            logging.ERROR, logger=_LISTENER_LOG
        ):
            await call_with_timeout(listener._main(), 5)

        manager.reconcile.assert_awaited_once()
        manager.stop.assert_awaited()
        assert listener._stop_event.is_set()
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any("4242" in m and "exited" in m for m in errors), errors

    def test_a_changed_parent_arms_the_hard_exit_before_the_line_saying_so(
        self, monkeypatch
    ):
        """The watcher process is on its way out: a logging handler that blocks on the
        error line must not keep it alive once the scheduler has died."""
        from airflow_provider_rmq.watcher import runner

        listener = _cycle_listener()
        listener._parent_pid = 4242
        events: list[str] = []
        monkeypatch.setattr(runner, "_arm_hard_exit", lambda: events.append("armed"))

        class _Recorder(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                events.append(f"log {record.getMessage()}")

        handler = _Recorder(level=logging.ERROR)
        listener_log = logging.getLogger(_LISTENER_LOG)
        listener_log.addHandler(handler)
        try:
            with patch("airflow_provider_rmq.watcher.listener.os.getppid", return_value=1):
                assert listener._parent_is_gone() is True
        finally:
            listener_log.removeHandler(handler)

        assert events[0] == "armed"
        assert events[1].startswith("log RMQ Watcher: the scheduler process 4242 has exited")

    def test_an_unchanged_parent_arms_nothing(self, alarm_calls):
        listener = _cycle_listener()
        listener._parent_pid = os.getppid()

        assert listener._parent_is_gone() is False
        assert alarm_calls == []

    @pytest.mark.asyncio
    async def test_a_parent_gone_before_the_first_pass_runs_no_cycle(self):
        listener = _cycle_listener()
        listener._parent_pid = 4242
        manager = _fake_manager()

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.os.getppid", return_value=1
        ):
            await call_with_timeout(listener._main(), 5)

        manager.reconcile.assert_not_awaited()
        listener._scan_subscriptions.assert_not_called()
        assert listener._stop_event.is_set()

    def test_run_stops_once_the_parent_differs(self):
        """The PID handed to ``run`` is the one the loop compares against."""
        listener = _cycle_listener()
        manager = _fake_manager()

        with _cycle_patches(manager):
            listener.run(parent_pid=os.getppid() + 1)

        manager.reconcile.assert_not_awaited()
        assert listener._stop_event.is_set()


class TestCycleCompletedMetric:
    """``rmq_watcher.cycle_completed`` says the loop is alive and turning."""

    @staticmethod
    def _completed(incr):
        return [c for c in incr.call_args_list if c == call("rmq_watcher.cycle_completed")]

    @pytest.mark.asyncio
    async def test_every_pass_that_returned_counts(self):
        listener = _cycle_listener()
        manager = _fake_manager()
        passes = []

        async def stop_after_second(subs):
            passes.append(1)
            if len(passes) == 2:
                listener._stop_event.set()

        manager.reconcile = AsyncMock(side_effect=stop_after_second)

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.incr"
        ) as incr:
            await call_with_timeout(listener._main(), 5)

        assert len(self._completed(incr)) == 2

    @pytest.mark.asyncio
    async def test_a_pass_with_a_failed_step_still_counts(self, caplog):
        listener = _cycle_listener()
        manager = _fake_manager()

        def broken_scan():
            listener._stop_event.set()
            raise RuntimeError("scan blew up")

        listener._scan_subscriptions = MagicMock(side_effect=broken_scan)

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.incr"
        ) as incr, caplog.at_level(
            logging.ERROR, logger=_LISTENER_LOG
        ):
            await call_with_timeout(listener._main(), 5)

        assert len(self._completed(incr)) == 1
        assert any(
            "Error in RMQ Watcher reconciliation cycle" in r.getMessage()
            for r in caplog.records
        )

    @pytest.mark.asyncio
    async def test_a_pass_that_ran_out_of_budget_does_not_count(self):
        listener = _cycle_listener()
        listener._cycle_timeout_override = 0.05
        manager = _fake_manager()
        manager.reconcile = _never_returns

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.incr"
        ) as incr, pytest.raises(asyncio.TimeoutError):
            await listener._main()

        assert self._completed(incr) == []
        incr.assert_any_call("rmq_watcher.cycle_timed_out")


class TestCycleTunables:
    def test_cycle_timeout_never_drops_below_the_floor(self):
        listener = RMQWatcherListener()
        listener._reconcile_interval = 60
        assert listener._cycle_timeout() == 300

    def test_cycle_timeout_scales_with_the_interval(self):
        listener = RMQWatcherListener()
        listener._reconcile_interval = 600
        assert listener._cycle_timeout() == 1800

    def test_variable_overrides_the_computed_budget(self):
        listener = RMQWatcherListener()
        listener._reconcile_interval = 600
        listener._cycle_timeout_override = 42.0
        assert listener._cycle_timeout() == 42.0

    @pytest.mark.asyncio
    async def test_refresh_reads_both_variables(self):
        listener = RMQWatcherListener()
        with patch(
            "airflow_provider_rmq.watcher.listener._read_settings",
            return_value=(120, 45.0),
        ):
            await listener._refresh_settings()

        assert listener._reconcile_interval == 120
        assert listener._cycle_timeout() == 45.0

    @pytest.mark.asyncio
    async def test_every_cycle_re_reads_the_tunables(self):
        """One read per cycle, so a changed Variable takes effect on the next one."""
        listener = RMQWatcherListener()
        with patch(
            "airflow_provider_rmq.watcher.listener._read_settings",
            return_value=(120, None),
        ) as read:
            await listener._refresh_settings()
            await listener._refresh_settings()
            await listener._refresh_settings()

        assert read.call_count == 3
        assert listener._reconcile_interval == 120

    @pytest.mark.asyncio
    async def test_hung_variable_read_keeps_the_last_known_interval(self):
        listener = RMQWatcherListener()
        listener._reconcile_interval = 17
        release = threading.Event()
        blocked = _blocked_by(release, result=(999, None))

        try:
            with patch(
                "airflow_provider_rmq.watcher.listener._read_settings", side_effect=blocked
            ), patch("airflow_provider_rmq.watcher.listener._VARIABLE_TIMEOUT", 0.05):
                await listener._refresh_settings()
                assert listener._reconcile_interval == 17
                assert listener._cycle_timeout() == 300

                # the previous read still holds a worker — no second one is started
                await listener._refresh_settings()
        finally:
            release.set()

        assert listener._reconcile_interval == 17

    @pytest.mark.asyncio
    async def test_unset_variable_falls_back_to_the_default_interval(self):
        listener = RMQWatcherListener()
        listener._reconcile_interval = 17
        with patch(
            "airflow_provider_rmq.watcher.listener._read_settings", return_value=(None, None)
        ):
            await listener._refresh_settings()

        assert listener._reconcile_interval == 60
        assert listener._cycle_timeout_override is None


class TestReadSettings:
    """The reader itself: both Variable names, both casts and both reject branches.

    Every caller of it is patched out in the tests above, so a typo in a Variable name
    would leave the whole suite green while the operator's override stopped working.
    """

    @contextlib.contextmanager
    def _variables(self, values: dict):
        variable = MagicMock()
        variable.get.side_effect = lambda name, default_var=None: values.get(
            name, default_var
        )
        module = MagicMock()
        module.Variable = variable
        with patch.dict(sys.modules, {"airflow.models": module}):
            yield variable

    def test_unset_variables_read_as_no_override(self):
        with self._variables({}) as variable:
            assert _read_settings() == (None, None)
        asked = {c.args[0] for c in variable.get.call_args_list}
        assert asked == {RECONCILE_INTERVAL_VAR, CYCLE_TIMEOUT_VAR}

    def test_values_are_read_and_cast(self):
        with self._variables({
            RECONCILE_INTERVAL_VAR: "120",
            CYCLE_TIMEOUT_VAR: "45.5",
        }):
            interval, budget = _read_settings()

        assert interval == 120 and isinstance(interval, int)
        assert budget == 45.5 and isinstance(budget, float)

    def test_a_value_that_is_not_a_number_is_ignored_with_a_warning(self, caplog):
        with caplog.at_level(
            logging.WARNING, logger=_LISTENER_LOG
        ), self._variables({RECONCILE_INTERVAL_VAR: "soon"}):
            interval, _ = _read_settings()

        assert interval is None
        assert any(
            RECONCILE_INTERVAL_VAR in r.getMessage() and "not a number" in r.getMessage()
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    @pytest.mark.parametrize("raw", ["0", "-30"])
    def test_a_non_positive_value_is_ignored_with_a_warning(self, caplog, raw):
        with caplog.at_level(
            logging.WARNING, logger=_LISTENER_LOG
        ), self._variables({CYCLE_TIMEOUT_VAR: raw}):
            _, budget = _read_settings()

        assert budget is None
        assert any(
            CYCLE_TIMEOUT_VAR in r.getMessage() and "must be positive" in r.getMessage()
            for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    @pytest.mark.parametrize("raw", ["inf", "1e400", "nan", "-inf"])
    def test_a_value_that_is_not_finite_is_ignored_with_a_warning(self, caplog, raw):
        """``float`` reads these happily and they pass a plain positivity check: an
        infinite cycle budget is a watchdog that never fires, and a NaN one compares
        False against everything, which leaves the timers in no order at all."""
        with caplog.at_level(
            logging.WARNING, logger=_LISTENER_LOG
        ), self._variables({CYCLE_TIMEOUT_VAR: raw}):
            _, budget = _read_settings()

        assert budget is None
        assert any(
            CYCLE_TIMEOUT_VAR in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_one_bad_variable_does_not_hide_the_other(self):
        with self._variables({
            RECONCILE_INTERVAL_VAR: "nope",
            CYCLE_TIMEOUT_VAR: "900",
        }):
            assert _read_settings() == (None, 900.0)


class TestSchemaMigrationRetry:
    @pytest.mark.asyncio
    async def test_migration_is_retried_until_the_schema_is_ready(self):
        listener = _cycle_listener()
        ready = {"value": False}
        calls = []

        def migrate():
            calls.append(1)
            ready["value"] = len(calls) >= 2

        with patch(
            "airflow_provider_rmq.watcher.listener.ensure_table_exists", side_effect=migrate
        ), patch(
            "airflow_provider_rmq.watcher.listener.is_schema_ready",
            side_effect=lambda: ready["value"],
        ):
            await listener._ensure_schema()
            assert len(calls) == 1
            assert listener._migration_skip_cycles == 1   # backoff armed

            listener._migration_skip_cycles = 0
            await listener._ensure_schema()
            assert len(calls) == 2
            assert listener._migration_skip_cycles == 0   # backoff cleared on success

            await listener._ensure_schema()
            assert len(calls) == 2, "a ready schema must not be migrated again"

    @pytest.mark.asyncio
    async def test_failed_migration_backs_off_instead_of_retrying_every_cycle(self):
        listener = _cycle_listener()
        calls = []

        with patch(
            "airflow_provider_rmq.watcher.listener.ensure_table_exists",
            side_effect=lambda: calls.append(1),
        ), patch(
            "airflow_provider_rmq.watcher.listener.is_schema_ready", return_value=False
        ):
            await listener._ensure_schema()      # attempt 1 → wait 1 cycle
            await listener._ensure_schema()      # skipped
            assert len(calls) == 1
            await listener._ensure_schema()      # attempt 2 → wait 2 cycles
            assert len(calls) == 2
            assert listener._migration_skip_cycles == 2

    @pytest.mark.asyncio
    async def test_only_one_migration_attempt_is_ever_in_flight(self):
        """A timeout does not free the worker, so retrying every cycle would fill the
        cycle pool with stuck attempts."""
        listener = _cycle_listener()
        release = threading.Event()
        calls = []
        blocked = _blocked_by(release, calls=calls)

        try:
            with patch(
                "airflow_provider_rmq.watcher.listener.ensure_table_exists",
                side_effect=blocked,
            ), patch(
                "airflow_provider_rmq.watcher.listener.is_schema_ready", return_value=False
            ), patch("airflow_provider_rmq.watcher.listener._MIGRATION_TIMEOUT", 0.05):
                await listener._ensure_schema()
                for _ in range(5):
                    listener._migration_skip_cycles = 0   # even with backoff cleared
                    await listener._ensure_schema()
        finally:
            release.set()

        assert len(calls) == 1

    @pytest.mark.asyncio
    async def test_hung_migration_gives_up_without_spending_the_cycle_budget(self):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        release = threading.Event()

        try:
            with _cycle_patches(manager), patch(
                "airflow_provider_rmq.watcher.listener.is_schema_ready", return_value=False
            ), patch(
                "airflow_provider_rmq.watcher.listener.ensure_table_exists",
                side_effect=_blocked_by(release),
            ), patch("airflow_provider_rmq.watcher.listener._MIGRATION_TIMEOUT", 0.05):
                started = time.monotonic()
                await listener._run_cycle()
                elapsed = time.monotonic() - started
        finally:
            release.set()

        assert elapsed < 1.0
        manager.reconcile.assert_awaited(), "the cycle must go on without the migration"


class TestGracefulStop:
    """Shutdown must not wait out a reconcile interval that has just started."""

    @pytest.mark.asyncio
    async def test_a_cycle_that_stopped_the_watcher_does_not_wait_at_all(self):
        """The stop event may be set while the cycle runs — the wait must see it.

        The waker is a live loop and a real event, so an instant return says the stop
        event was read and not that there was nothing to wait on. The interval is short
        enough that a wait would show up as a failure rather than as a hang.
        """
        listener = _cycle_listener()
        listener._reconcile_interval = 2
        listener._waker = (asyncio.get_running_loop(), asyncio.Event())
        listener._stop_event.set()

        started = time.monotonic()
        await listener._wait_for_next_cycle()

        assert time.monotonic() - started < 1.0

    def test_request_stop_survives_a_loop_that_is_already_closed(self):
        """A closed loop is left alone: the call itself is what the guard prevents.

        The ``RuntimeError`` swallowed a couple of lines below would keep the event
        untouched either way, so the observation is the call, not its effect.
        """
        listener = RMQWatcherListener()
        loop = asyncio.new_event_loop()
        loop.close()
        loop.call_soon_threadsafe = MagicMock()
        listener._waker = (loop, MagicMock())

        listener.request_stop()

        assert listener._stop_event.is_set()
        loop.call_soon_threadsafe.assert_not_called()

    def test_request_stop_survives_a_loop_closing_under_it(self):
        """``is_closed`` can still say False when the loop closes a moment later.

        The nudge is made and it raises; stopping must go through all the same.
        """
        listener = RMQWatcherListener()
        loop = MagicMock()
        loop.is_closed.return_value = False
        loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
        listener._waker = (loop, MagicMock())

        listener.request_stop()

        loop.call_soon_threadsafe.assert_called_once()
        assert listener._stop_event.is_set()


class TestStopMidCycle:
    """A stop that comes during a cycle ends it where it stands.

    The watcher process gives the loop a bounded time to stop the consumer manager once
    it has asked for the stop, and a cycle that went on through its remaining steps —
    each one allowed a minute — would spend that time before the manager got any of it.
    """

    @staticmethod
    def _started(release: threading.Event, started: threading.Event, result=None):
        """A blocking call that reports its start and then holds its worker until
        ``release`` is set."""
        def blocked(*_args, **_kwargs):
            started.set()
            release.wait(timeout=5)
            return result

        return blocked

    @pytest.mark.asyncio
    async def test_a_stop_before_the_scan_runs_no_later_step_and_no_reconcile(self, caplog):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        listener._ensure_schema = AsyncMock(side_effect=listener.request_stop)
        read_subs = MagicMock(return_value=[])

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.get_enabled_subscriptions", read_subs
        ), caplog.at_level(logging.DEBUG, logger=_LISTENER_LOG):
            await listener._run_cycle()

        listener._scan_subscriptions.assert_not_called()
        listener._sync_to_db.assert_not_called()
        read_subs.assert_not_called()
        manager.reconcile.assert_not_awaited()
        assert any(
            "stop requested" in r.getMessage() and r.levelno == logging.INFO
            for r in caplog.records
        )
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    @pytest.mark.asyncio
    async def test_a_stop_after_the_subscription_read_never_starts_the_reconcile(self):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager

        def read_then_stop(*_args, **_kwargs):
            listener._stop_event.set()
            return []

        with _cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.get_enabled_subscriptions",
            side_effect=read_then_stop,
        ):
            await listener._run_cycle()

        manager.reconcile.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_stop_during_a_step_ends_the_cycle_without_waiting_for_the_worker(
        self, caplog
    ):
        listener = _cycle_listener()
        listener._reconcile_interval = 30
        manager = _fake_manager()
        release = threading.Event()
        started = threading.Event()
        listener._scan_subscriptions = self._started(release, started, result=[])

        try:
            with _cycle_patches(manager), patch(
                "airflow_provider_rmq.watcher.listener.incr"
            ) as incr, caplog.at_level(logging.DEBUG, logger=_LISTENER_LOG):
                task = asyncio.ensure_future(listener._main())
                await call_with_timeout(asyncio.to_thread(started.wait, 5), 5)
                stopped_at = time.monotonic()
                listener.request_stop()
                await call_with_timeout(task, 5)
                elapsed = time.monotonic() - stopped_at

            assert not release.is_set(), "the worker is still in the call"
            assert elapsed < 1.0
            listener._sync_to_db.assert_not_called()
            manager.reconcile.assert_not_awaited()
            manager.stop.assert_awaited_once()
            assert call("rmq_watcher.cycle_step_timed_out") not in incr.call_args_list
            assert not [r for r in caplog.records if r.levelno >= logging.ERROR]
            assert any("stop requested" in r.getMessage() for r in caplog.records)
        finally:
            release.set()

    @pytest.mark.asyncio
    async def test_a_stop_from_another_thread_ends_the_wait_for_a_step(self):
        """The runner's main thread is where the stop comes from in production."""
        listener = _cycle_listener()
        listener._waker = (asyncio.get_running_loop(), asyncio.Event())
        release = threading.Event()
        started = threading.Event()

        def stop_once_started():
            started.wait(timeout=5)
            listener.request_stop()

        stopper = threading.Thread(target=stop_once_started)
        stopper.start()
        try:
            with pytest.raises(listener_module._StopRequested):
                await call_with_timeout(
                    listener._cycle_step("scan", self._started(release, started)), 5
                )
            assert not release.is_set()
        finally:
            release.set()
            stopper.join(timeout=5)

    @pytest.mark.asyncio
    async def test_a_wake_up_without_a_stop_leaves_the_step_to_finish(self):
        listener = _cycle_listener()
        wakeup = asyncio.Event()
        listener._waker = (asyncio.get_running_loop(), wakeup)
        release = threading.Event()
        started = threading.Event()

        step = asyncio.ensure_future(
            listener._cycle_step("scan", self._started(release, started, result=["done"]))
        )
        try:
            await call_with_timeout(asyncio.to_thread(started.wait, 5), 5)
            wakeup.set()
            await asyncio.sleep(0.05)
            assert not step.done(), "a wake-up alone is not a stop"
        finally:
            release.set()

        assert await call_with_timeout(step, 5) == ["done"]

    @pytest.mark.asyncio
    async def test_a_stop_during_the_migration_neither_defers_it_nor_runs_the_scan(
        self, caplog
    ):
        listener = _cycle_listener()
        manager = _fake_manager()
        listener._manager = manager
        listener._waker = (asyncio.get_running_loop(), asyncio.Event())
        release = threading.Event()
        started = threading.Event()

        try:
            with _cycle_patches(manager), patch(
                "airflow_provider_rmq.watcher.listener.is_schema_ready", return_value=False
            ), patch(
                "airflow_provider_rmq.watcher.listener.ensure_table_exists",
                side_effect=self._started(release, started),
            ), caplog.at_level(logging.DEBUG, logger=_LISTENER_LOG):
                cycle = asyncio.ensure_future(listener._run_cycle())
                await call_with_timeout(asyncio.to_thread(started.wait, 5), 5)
                listener.request_stop()
                await call_with_timeout(cycle, 5)
        finally:
            release.set()

        assert listener._migration_backoff == 0
        assert listener._migration_skip_cycles == 0
        listener._scan_subscriptions.assert_not_called()
        manager.reconcile.assert_not_awaited()
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    @pytest.mark.asyncio
    async def test_a_reconcile_under_way_runs_to_its_end_before_the_manager_stops(self):
        listener = _cycle_listener()
        listener._reconcile_interval = 30
        manager = _fake_manager()
        order = []

        async def reconcile(subs):
            listener.request_stop()
            await asyncio.sleep(0.05)
            order.append("reconcile finished")

        manager.reconcile = AsyncMock(side_effect=reconcile)
        manager.stop = AsyncMock(side_effect=lambda: order.append("manager stopped"))

        with _cycle_patches(manager):
            await call_with_timeout(listener._main(), 5)

        assert order == ["reconcile finished", "manager stopped"]


# ---------------------------------------------------------------------------
# Watcher process: start, stop and the scheduler's own threads
# ---------------------------------------------------------------------------


def _fake_child(pid: int = 4321, *, running: bool = True) -> MagicMock:
    """A ``Popen`` stand-in: ``poll`` says whether the process is still running."""
    child = MagicMock(spec=subprocess.Popen)
    child.pid = pid
    child.poll.return_value = None if running else 0
    return child


@contextlib.contextmanager
def _popen(*children):
    """Patch ``Popen`` in the listener to hand out ``children`` one per call."""
    with patch(
        "airflow_provider_rmq.watcher.listener.subprocess.Popen",
        side_effect=list(children) or [_fake_child()],
    ) as popen:
        yield popen


_PACKAGE_ROOT = str(Path(airflow_provider_rmq.__file__).parents[1])


def _python_with_safe_path() -> str | None:
    """An interpreter of Python 3.11 or later, which knows ``PYTHONSAFEPATH``, or
    ``None`` when neither this one nor one on ``PATH`` is."""
    if sys.version_info >= (3, 11):
        return sys.executable
    for name in ("python3.14", "python3.13", "python3.12", "python3.11", "python3"):
        found = shutil.which(name)
        if found is None:
            continue
        probe = subprocess.run(
            [found, "-c", "import sys; print(sys.version_info >= (3, 11))"],
            capture_output=True, text=True, timeout=60, check=False,
        )
        if probe.stdout.strip() == "True":
            return found
    return None


@contextlib.contextmanager
def _package_in_site_packages(in_site: bool):
    """Make the package's directory one of the interpreter's site-packages, or none."""
    found = {os.path.realpath(_PACKAGE_ROOT)} if in_site else set()
    with patch(
        "airflow_provider_rmq.watcher.listener._site_directories", return_value=found
    ):
        yield


@contextlib.contextmanager
def _watch_for_threads_and_loops():
    """Record every thread started and every event loop created inside the block.

    Yields a dict whose ``threads`` lists the names of the threads started and whose
    ``loops`` counts the event loops created. Only what the calling thread does is
    recorded: a thread a previous test left behind may start threads or loops of its
    own while the block runs, which is not the scheduler gaining one. Loops are counted
    at construction, so every way of creating one — ``asyncio.new_event_loop``,
    ``asyncio.run``, a policy — is seen. The set of live threads is compared too: a
    thread started and finished inside the block shows up in ``threads``, one still
    running afterwards shows up in ``new_live_threads``. Only threads that appeared are
    counted there, for the same reason.
    """
    seen: dict = {"threads": [], "loops": 0, "new_live_threads": set()}
    caller = threading.current_thread()
    real_start = threading.Thread.start
    real_loop_init = asyncio.BaseEventLoop.__init__

    def recording_start(thread, *args, **kwargs):
        if threading.current_thread() is caller:
            seen["threads"].append(thread.name)
        return real_start(thread, *args, **kwargs)

    def recording_loop_init(loop, *args, **kwargs):
        if threading.current_thread() is caller:
            seen["loops"] += 1
        return real_loop_init(loop, *args, **kwargs)

    before = set(threading.enumerate())
    with patch.object(threading.Thread, "start", recording_start), patch.object(
        asyncio.BaseEventLoop, "__init__", recording_loop_init
    ):
        yield seen
    seen["new_live_threads"] = {
        thread.name for thread in set(threading.enumerate()) - before
    }


class TestWatcherProcessStart:
    """``on_starting`` in the scheduler starts the watcher process and nothing else."""

    def test_the_runner_module_is_started_with_the_scheduler_interpreter(self):
        """A package in site-packages starts through the bootstrap too, with no
        directory to append — not even one left in the scheduler's environment."""
        listener = RMQWatcherListener()
        with patch.dict(os.environ, {PACKAGE_DIR_ENV: "/stale"}), _package_in_site_packages(
            True
        ), _popen() as popen:
            listener.on_starting(SchedulerJobRunner())

        popen.assert_called_once()
        assert popen.call_args.args[0] == [
            sys.executable, "-c", listener_module._BOOTSTRAP,
            "airflow_provider_rmq.watcher.runner",
        ]
        assert PACKAGE_DIR_ENV not in popen.call_args.kwargs["env"]
        kwargs = popen.call_args.kwargs
        assert kwargs["stdin"] is subprocess.DEVNULL
        # stdout and stderr are inherited, so the watcher logs to the scheduler's
        # container log; every other descriptor is closed by the default.
        assert "stdout" not in kwargs and "stderr" not in kwargs
        assert kwargs.get("close_fds", True) is True

    def test_the_environment_carries_the_marker_and_the_scheduler_pid(self):
        listener = RMQWatcherListener()
        with patch.dict(os.environ, {"RMQ_TEST_INHERITED": "yes"}), _popen() as popen:
            listener.on_starting(SchedulerJobRunner())

        env = popen.call_args.kwargs["env"]
        assert env[CHILD_ENV_MARKER] == "1"
        assert env[PARENT_PID_ENV] == str(os.getpid())
        assert env["RMQ_TEST_INHERITED"] == "yes"
        assert CHILD_ENV_MARKER not in os.environ, "the scheduler's own environment stays as it is"

    def test_a_package_outside_site_packages_starts_through_the_bootstrap(self):
        """A package in the plugins folder is not importable before Airflow is: the
        bootstrap appends its directory to the path and runs the runner module."""
        listener = RMQWatcherListener()
        with _package_in_site_packages(False), _popen() as popen:
            listener.on_starting(SchedulerJobRunner())

        assert popen.call_args.args[0] == [
            sys.executable, "-c", listener_module._BOOTSTRAP,
            "airflow_provider_rmq.watcher.runner",
        ]
        assert popen.call_args.kwargs["env"][PACKAGE_DIR_ENV] == _PACKAGE_ROOT
        assert os.path.isdir(os.path.join(_PACKAGE_ROOT, "airflow_provider_rmq"))

    @pytest.mark.parametrize("in_site", [True, False], ids=["site-packages", "plugins"])
    @pytest.mark.parametrize("existing", ["/opt/extra", None], ids=["set", "unset"])
    def test_pythonpath_is_passed_on_unchanged(self, existing, in_site):
        """An entry on ``PYTHONPATH`` would precede the standard library and
        site-packages in the watcher process alone."""
        listener = RMQWatcherListener()
        with patch.dict(os.environ, {}), _package_in_site_packages(in_site), _popen() as popen:
            os.environ.pop("PYTHONPATH", None)
            if existing is not None:
                os.environ["PYTHONPATH"] = existing
            listener.on_starting(SchedulerJobRunner())

        assert popen.call_args.kwargs["env"].get("PYTHONPATH") == existing

    def test_the_bootstrap_appends_the_directory_and_runs_its_argument_as_main(
        self, tmp_path
    ):
        """The bootstrap in a real interpreter: the directory ends :data:`sys.path`,
        after the standard library, and the module named by the argument runs as
        ``__main__``. A module in the directory named like one of the standard library
        — a custom secrets backend in ``plugins/secrets.py`` — does not shadow it."""
        plugins = tmp_path / "plugins"
        plugins.mkdir()
        (plugins / "secrets.py").write_text("raise ImportError('plugins/secrets.py shadowed')\n")
        (plugins / "rmq_bootstrap_probe.py").write_text(
            "import os, sys, sysconfig\n"
            "import secrets\n"
            "if __name__ == '__main__':\n"
            f"    plugins = os.environ[{PACKAGE_DIR_ENV!r}]\n"
            "    stdlib = sys.path.index(sysconfig.get_paths()['stdlib'])\n"
            "    print(sys.path.index(plugins) > stdlib, sys.path[-1] == plugins)\n"
        )
        env = dict(os.environ, **{PACKAGE_DIR_ENV: str(plugins)})
        result = subprocess.run(
            [sys.executable, "-c", listener_module._BOOTSTRAP, "rmq_bootstrap_probe"],
            env=env, capture_output=True, text=True, timeout=60, check=False,
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.split() == ["True", "True"]

    @pytest.mark.parametrize("in_site", [True, False], ids=["site-packages", "plugins"])
    def test_the_bootstrap_imports_nothing_from_the_working_directory(
        self, tmp_path, in_site
    ):
        """The command line the listener starts, in a real interpreter, from a working
        directory holding ``runpy.py``, ``logging.py`` and a package named like this
        one: none of them is imported, and :data:`sys.path` is the interpreter's own
        without the working directory — plus the package directory at its end for a
        package outside site-packages.

        A ``sitecustomize`` on ``PYTHONPATH`` prints :data:`sys.path` from ``os._exit``,
        which the runner reaches with code 2 when the scheduler's PID is missing.
        """
        hooks = tmp_path / "hooks"
        hooks.mkdir()
        (hooks / "sitecustomize.py").write_text(
            "import os, sys\n"
            "_real_exit = os._exit\n"
            "def _exit(code):\n"
            "    print('SYS_PATH', repr(sys.path), flush=True)\n"
            "    _real_exit(code)\n"
            "os._exit = _exit\n"
        )
        cwd = tmp_path / "cwd"
        (cwd / "airflow_provider_rmq" / "watcher").mkdir(parents=True)
        shadows = {
            "runpy.py": "runpy",
            "logging.py": "logging",
            "airflow_provider_rmq/__init__.py": "airflow_provider_rmq",
            "airflow_provider_rmq/watcher/__init__.py": "airflow_provider_rmq.watcher",
            "airflow_provider_rmq/watcher/runner.py": "airflow_provider_rmq.watcher.runner",
        }
        for relative, name in shadows.items():
            (cwd / relative).write_text(
                f"print('SHADOWED {name}', flush=True)\nraise SystemExit(3)\n"
            )
        env = {
            k: v for k, v in os.environ.items()
            if k not in (PARENT_PID_ENV, PACKAGE_DIR_ENV)
        }
        env["PYTHONPATH"] = str(hooks)
        if not in_site:
            env[PACKAGE_DIR_ENV] = _PACKAGE_ROOT
        reference = subprocess.run(
            [sys.executable, "-c", "import sys; print(repr(sys.path[1:]))"],
            cwd=cwd, env=env, capture_output=True, text=True, timeout=60, check=True,
        )

        result = subprocess.run(
            listener_module._child_command(),
            cwd=cwd, env=env, capture_output=True, text=True, timeout=60, check=False,
        )

        assert "SHADOWED" not in result.stdout + result.stderr
        assert result.returncode == 2, result.stderr
        assert PARENT_PID_ENV in result.stderr
        line = next(line for line in result.stdout.splitlines() if line.startswith("SYS_PATH"))
        path = ast.literal_eval(line.split(" ", 1)[1])
        expected = ast.literal_eval(reference.stdout.strip())
        assert path == expected + ([] if in_site else [_PACKAGE_ROOT])
        assert "" not in path and str(cwd) not in path

    def test_under_safe_path_the_bootstrap_removes_nothing(self, tmp_path):
        """With ``PYTHONSAFEPATH`` (Python 3.11 and later) ``python -c`` puts no working
        directory on :data:`sys.path`, and the bootstrap leaves the path whole."""
        python = _python_with_safe_path()
        if python is None:
            pytest.skip("no Python 3.11 or later to run the bootstrap under PYTHONSAFEPATH")
        plugins = tmp_path / "plugins"
        plugins.mkdir()
        (plugins / "rmq_bootstrap_probe.py").write_text(
            "import sys\nprint(repr(sys.path))\n"
        )
        env = dict(os.environ, PYTHONSAFEPATH="1", **{PACKAGE_DIR_ENV: str(plugins)})
        env.pop("PYTHONPATH", None)
        reference = subprocess.run(
            [python, "-c", "import sys; print(repr(sys.path))"],
            env=env, capture_output=True, text=True, timeout=60, check=True,
        )

        result = subprocess.run(
            [python, "-c", listener_module._BOOTSTRAP, "rmq_bootstrap_probe"],
            env=env, capture_output=True, text=True, timeout=60, check=False,
        )

        assert result.returncode == 0, result.stderr
        expected = ast.literal_eval(reference.stdout.strip())
        assert ast.literal_eval(result.stdout.strip()) == expected + [str(plugins)]

    def test_site_directories_hold_the_interpreters_site_packages(self):
        import sysconfig

        found = listener_module._site_directories()

        assert os.path.realpath(sysconfig.get_paths()["purelib"]) in found
        assert os.path.realpath(sys.prefix) not in found

    def test_the_runner_module_name_comes_from_the_runner_itself(self):
        from airflow_provider_rmq.watcher import runner

        assert listener_module._RUNNER_MODULE == runner.__name__

    def test_the_start_is_logged_with_the_child_pid(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(logging.INFO, logger=_LISTENER_LOG), _popen(_fake_child(777)):
            listener.on_starting(SchedulerJobRunner())

        assert any(
            "process started with PID 777" in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_a_component_other_than_the_scheduler_starts_no_process(self):
        listener = RMQWatcherListener()
        with _popen() as popen:
            listener.on_starting(GunicornWebServer())

        popen.assert_not_called()
        assert listener._child is None

    def test_the_child_marker_keeps_a_listener_from_starting_another_process(self, caplog):
        listener = RMQWatcherListener()
        with patch.dict(os.environ, {CHILD_ENV_MARKER: "1"}), caplog.at_level(
            logging.INFO, logger=_LISTENER_LOG
        ), _popen() as popen:
            listener.on_starting(SchedulerJobRunner())

        popen.assert_not_called()
        assert listener._child is None
        messages = [r.getMessage() for r in caplog.records]
        assert messages == [
            "RMQWatcherListener.on_starting in the watcher process — not starting another one"
        ]

    def test_a_duplicate_start_with_the_process_alive_is_ignored(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING, logger=_LISTENER_LOG), _popen(
            _fake_child(111), _fake_child(222)
        ) as popen:
            listener.on_starting(SchedulerJobRunner())
            listener.on_starting(SchedulerJobRunner())

        assert popen.call_count == 1
        assert listener._child.pid == 111
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any("already running" in m and "111" in m for m in warnings), warnings

    def test_a_process_that_has_exited_is_replaced(self):
        listener = RMQWatcherListener()
        with _popen(_fake_child(111, running=False), _fake_child(222)) as popen:
            listener.on_starting(SchedulerJobRunner())
            listener.on_starting(SchedulerJobRunner())

        assert popen.call_count == 2
        assert listener._child.pid == 222

    def test_a_start_from_another_thread_warns_and_still_starts(self, caplog, monkeypatch):
        monkeypatch.setattr(sys, "platform", "linux")
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING, logger=_LISTENER_LOG), _popen() as popen:
            thread = threading.Thread(target=listener._spawn_child, name="not-main")
            thread.start()
            thread.join(timeout=5)

        popen.assert_called_once()
        warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
        assert any("not-main" in m and "main thread" in m for m in warnings), warnings

    def test_a_start_from_the_main_thread_does_not_warn(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING, logger=_LISTENER_LOG), _popen():
            listener._spawn_child()

        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_the_process_that_started_the_watcher_is_its_owner(self):
        listener = RMQWatcherListener()
        with _popen():
            listener.on_starting(SchedulerJobRunner())

        assert listener._owner_pid == os.getpid()

    def test_a_fork_of_the_scheduler_starts_no_process(self, caplog):
        """A LocalExecutor task supervisor is forked from the scheduler: its stack
        holds the scheduler's frames and it inherits this listener with ``_child``,
        whose ``poll()`` there reads as an exit. It must not start a watcher of its own."""
        listener = RMQWatcherListener()
        listener._owner_pid = os.getpid() + 1
        inherited = _fake_child(111, running=False)
        listener._child = inherited
        with caplog.at_level(logging.INFO, logger=_LISTENER_LOG), _popen() as popen:
            listener.on_starting(SchedulerJobRunner())

        popen.assert_not_called()
        assert listener._child is inherited
        messages = [r.getMessage() for r in caplog.records]
        assert any(
            f"a fork of the scheduler process {os.getpid() + 1}" in m for m in messages
        ), messages

    def test_a_fork_of_the_scheduler_leaves_the_watcher_process_alone(self):
        listener = RMQWatcherListener()
        listener._owner_pid = os.getpid() + 1
        inherited = _fake_child(111)
        listener._child = inherited

        listener.before_stopping(MagicMock())

        inherited.poll.assert_not_called()
        inherited.terminate.assert_not_called()
        inherited.kill.assert_not_called()
        assert listener._child is inherited

    def test_a_failed_start_still_marks_the_owner(self):
        """A fork taken after a failed start is still not the scheduler."""
        listener = RMQWatcherListener()
        with patch(
            "airflow_provider_rmq.watcher.listener.subprocess.Popen",
            side_effect=OSError("no such interpreter"),
        ):
            listener.on_starting(SchedulerJobRunner())

        assert listener._owner_pid == os.getpid()

    def test_off_linux_the_watcher_process_is_started_too(self, caplog, monkeypatch):
        monkeypatch.setattr(sys, "platform", "darwin")
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING, logger=_LISTENER_LOG), _popen() as popen:
            listener.on_starting(SchedulerJobRunner())

        popen.assert_called_once()
        assert listener._child is not None
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_off_linux_a_start_from_another_thread_does_not_warn(self, caplog, monkeypatch):
        """The warning is about the parent-death signal, which only Linux has."""
        monkeypatch.setattr(sys, "platform", "darwin")
        listener = RMQWatcherListener()
        with caplog.at_level(logging.WARNING, logger=_LISTENER_LOG), _popen() as popen:
            thread = threading.Thread(target=listener._spawn_child, name="not-main")
            thread.start()
            thread.join(timeout=5)

        popen.assert_called_once()
        assert not [r for r in caplog.records if r.levelno >= logging.WARNING]

    def test_a_popen_failure_is_logged_and_the_scheduler_carries_on(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(logging.ERROR, logger=_LISTENER_LOG), patch(
            "airflow_provider_rmq.watcher.listener.subprocess.Popen",
            side_effect=OSError("no such interpreter"),
        ):
            listener.on_starting(SchedulerJobRunner())

        assert listener._child is None
        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert errors and "could not be started" in errors[0].getMessage()
        listener.before_stopping(MagicMock())


class TestWatcherProcessStop:
    """``before_stopping`` ends the watcher process within a bounded time."""

    def test_a_process_that_exits_on_sigterm_is_not_killed(self, caplog):
        listener = RMQWatcherListener()
        child = _fake_child()
        child.wait.return_value = 0
        listener._child = child

        with caplog.at_level(logging.INFO, logger=_LISTENER_LOG):
            listener.before_stopping(MagicMock())

        child.terminate.assert_called_once()
        child.kill.assert_not_called()
        child.wait.assert_called_once_with(timeout=_CHILD_STOP_TIMEOUT)
        assert any(
            "4321 exited with code 0" in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]
        assert listener._child is None

    def test_a_process_still_running_after_the_timeout_is_killed(self, caplog):
        listener = RMQWatcherListener()
        child = _fake_child()
        child.wait.side_effect = [subprocess.TimeoutExpired("runner", _CHILD_STOP_TIMEOUT), -9]
        listener._child = child

        with caplog.at_level(logging.INFO, logger=_LISTENER_LOG):
            listener.before_stopping(MagicMock())

        child.terminate.assert_called_once()
        child.kill.assert_called_once()
        assert child.wait.call_args_list == [
            call(timeout=_CHILD_STOP_TIMEOUT), call(timeout=_CHILD_REAP_TIMEOUT),
        ]
        messages = [(r.levelno, r.getMessage()) for r in caplog.records]
        assert any(
            level == logging.WARNING and "killing" in m for level, m in messages
        ), messages
        assert any("exited with code -9" in m for _, m in messages), messages

    def test_a_process_not_reaped_after_sigkill_is_left_behind(self, caplog):
        listener = RMQWatcherListener()
        child = _fake_child(999)
        child.wait.side_effect = subprocess.TimeoutExpired("runner", 1)
        listener._child = child

        with caplog.at_level(logging.ERROR, logger=_LISTENER_LOG):
            listener.before_stopping(MagicMock())

        child.kill.assert_called_once()
        assert child.wait.call_count == 2
        errors = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert errors and "999" in errors[0] and "not reaped" in errors[0], errors

    def test_a_process_that_exited_already_is_only_reaped(self, caplog):
        """The container's init sends SIGTERM to the whole process group on
        ``docker stop``, so the runner may be gone before the scheduler stops."""
        listener = RMQWatcherListener()
        child = _fake_child(running=False)
        listener._child = child

        with caplog.at_level(logging.INFO, logger=_LISTENER_LOG):
            listener.before_stopping(MagicMock())

        child.terminate.assert_not_called()
        child.kill.assert_not_called()
        assert any(
            "already exited with code 0" in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]

    def test_before_stopping_without_a_process_does_nothing(self):
        listener = RMQWatcherListener()

        listener.before_stopping(MagicMock())

        assert listener._child is None

    def test_a_second_before_stopping_does_not_signal_again(self):
        listener = RMQWatcherListener()
        child = _fake_child()
        child.wait.return_value = 0
        listener._child = child

        listener.before_stopping(MagicMock())
        listener.before_stopping(MagicMock())

        child.terminate.assert_called_once()

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
    def test_a_real_process_stops_on_sigterm(self):
        listener = RMQWatcherListener()
        child = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"],
            stdin=subprocess.DEVNULL,
        )
        try:
            listener._child = child

            started = time.monotonic()
            listener.before_stopping(MagicMock())

            assert time.monotonic() - started < _CHILD_STOP_TIMEOUT
            assert child.returncode == -15
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()

    @pytest.mark.skipif(sys.platform == "win32", reason="POSIX signals")
    def test_a_real_process_ignoring_sigterm_is_killed_in_bounded_time(self):
        listener = RMQWatcherListener()
        child = subprocess.Popen(
            [
                sys.executable, "-c",
                (
                    "import signal, time\n"
                    "signal.signal(signal.SIGTERM, signal.SIG_IGN)\n"
                    "print('ready', flush=True)\n"
                    "time.sleep(60)\n"
                ),
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
        )
        try:
            assert child.stdout.readline().strip() == b"ready"
            listener._child = child

            started = time.monotonic()
            with patch("airflow_provider_rmq.watcher.listener._CHILD_STOP_TIMEOUT", 0.5):
                listener.before_stopping(MagicMock())

            assert time.monotonic() - started < 0.5 + _CHILD_REAP_TIMEOUT + 1.0
            assert child.returncode == -9
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()


class TestNothingOfThePluginRunsInTheScheduler:
    """Airflow forks the scheduler process — the Celery send pool, the DAG processor
    manager — and a fork is only safe from a process with a single thread. The
    listener in the scheduler starts and stops the watcher process and leaves no
    thread, event loop or pool of its own behind."""

    @staticmethod
    def _assert_nothing_of_the_plugin(listener, seen):
        assert seen["threads"] == [], seen["threads"]
        assert seen["new_live_threads"] == set(), seen["new_live_threads"]
        assert seen["loops"] == 0
        assert listener._waker is None
        assert listener._cycle_pool is None
        assert listener._consumer_pool is None
        assert listener._manager is None

    def test_on_starting_in_the_scheduler_leaves_the_process_as_it_was(self):
        listener = RMQWatcherListener()
        with _watch_for_threads_and_loops() as seen, _popen() as popen:
            listener.on_starting(SchedulerJobRunner())

        popen.assert_called_once()
        assert listener._child is not None
        self._assert_nothing_of_the_plugin(listener, seen)

    def test_a_start_a_stop_and_a_start_again_leave_the_process_as_it_was(self):
        listener = RMQWatcherListener()
        first, second = _fake_child(111), _fake_child(222)
        first.wait.return_value = 0
        with _watch_for_threads_and_loops() as seen, _popen(first, second) as popen:
            listener.on_starting(SchedulerJobRunner())
            listener.before_stopping(SchedulerJobRunner())
            listener.on_starting(SchedulerJobRunner())

        assert popen.call_count == 2
        first.terminate.assert_called_once()
        assert listener._child is second
        self._assert_nothing_of_the_plugin(listener, seen)


class TestLifecycleDiagnostics:
    def test_unrecognised_component_logs_why_the_watcher_is_not_started(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(
            logging.INFO, logger=_LISTENER_LOG
        ), patch.object(listener, "_spawn_child") as start:
            listener.on_starting(GunicornWebServer())

        start.assert_not_called()
        messages = [record.getMessage() for record in caplog.records]
        assert len(messages) == 1, "the reason belongs in the existing record"
        assert "watcher not started" in messages[0]

    def test_scheduler_component_is_logged_without_a_reason(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(
            logging.INFO, logger=_LISTENER_LOG
        ), patch.object(listener, "_spawn_child"):
            listener.on_starting(SchedulerJobRunner())

        assert "watcher not started" not in caplog.records[0].getMessage()

    @pytest.mark.asyncio
    async def test_an_override_is_logged_once_the_first_cycle_has_read_it(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(
            logging.INFO, logger=_LISTENER_LOG
        ), patch(
            "airflow_provider_rmq.watcher.listener._read_settings",
            return_value=(120, 999.0),
        ):
            await listener._refresh_settings()
            await listener._refresh_settings()

        effective = [
            r.getMessage() for r in caplog.records if "tunables in effect" in r.getMessage()
        ]
        assert len(effective) == 1, effective
        assert "120" in effective[0] and "999" in effective[0]


# ---------------------------------------------------------------------------
# End-to-end scenarios of the backlog item, on a real session (Task 8)
# ---------------------------------------------------------------------------

#: DAG file the scan finds one subscription in.
_DAG_WITH_TRIGGER = (
    "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
    "from airflow.decorators import dag\n"
    "@rmq_trigger(queue='q')\n"
    "@dag(dag_id='d')\n"
    "def d(): pass\n"
)

#: The same DAG with the decorator taken off — a scan of it is a statement that the
#: subscription is gone from the code.
_DAG_WITHOUT_TRIGGER = (
    "from airflow.decorators import dag\n"
    "@dag(dag_id='d')\n"
    "def d(): pass\n"
)

#: The same DAG with the decorator's queue made unreadable — the scan refuses the file.
_DAG_WITH_UNREADABLE_TRIGGER = (
    "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
    "from airflow.decorators import dag\n"
    "@rmq_trigger(queue='q', conn_id=get_conn())\n"
    "@dag(dag_id='d')\n"
    "def d(): pass\n"
)

#: A file caught mid-write: readable, unparseable.
_DAG_HALF_WRITTEN = "from airflow.decorators import dag\n@dag(dag_id='d'\n"


def _write_dag_file(path, source: str, mtime: float) -> None:
    """Write a DAG file and stamp its mtime, which drives the incremental scan."""
    path.write_text(source)
    os.utime(path, (mtime, mtime))


@contextlib.contextmanager
def _db(engine):
    """Short-lived session on the engine the cycle writes to."""
    s = sessionmaker(bind=engine)()
    try:
        yield s
    finally:
        s.close()


def _snapshot(engine, dag_id="d", queue_name="q"):
    """Values of the dag_file row that the scenarios below assert on, or ``None``."""
    with _db(engine) as session:
        row = (
            session.query(RMQSubscription)
            .filter_by(dag_id=dag_id, queue_name=queue_name)
            .one_or_none()
        )
        if row is None:
            return None
        return {"id": row.id, "enabled": row.enabled, "absent_cycles": row.absent_cycles}


def _disable_subscription(engine, dag_id="d", queue_name="q") -> None:
    """Turn the row off the way the Subscriptions page's button does."""
    with _db(engine) as session:
        session.query(RMQSubscription).filter_by(
            dag_id=dag_id, queue_name=queue_name
        ).update({"enabled": False})
        session.commit()


def _add_ui_subscription(engine, dag_id="d", queue_name="ui_q") -> None:
    """Row the Subscriptions page created, which no scan is a statement about."""
    with _db(engine) as session:
        session.add(RMQSubscription(
            dag_id=dag_id, queue_name=queue_name, conn_id="rmq_default",
            source="ui", enabled=True,
        ))
        session.commit()


def _record_dag(engine, dag_id="d", *, is_active: bool) -> None:
    """Give Airflow's dag table a row, which is what the short circuit consults."""
    from airflow.models import DagModel

    with _db(engine) as session:
        session.add(DagModel(dag_id=dag_id, is_active=is_active, is_paused=False))
        session.commit()


@pytest.fixture
def e2e_listener(dagmodel_engine, tmp_path, monkeypatch):
    """Listener whose scan reads ``tmp_path`` and whose sync writes a real database."""
    monkeypatch.setattr("airflow.settings.DAGS_FOLDER", str(tmp_path), raising=False)
    listener = RMQWatcherListener()
    listener._stop_event = threading.Event()
    listener._reconcile_interval = 0
    listener._refresh_settings = AsyncMock()
    listener._manager = _fake_manager()
    with patch(
        "airflow_provider_rmq.watcher.listener.WatcherSession",
        sessionmaker(bind=dagmodel_engine),
    ), patch(
        "airflow_provider_rmq.watcher.listener.is_schema_ready", return_value=True
    ), patch(
        "airflow_provider_rmq.watcher.listener._read_settings", return_value=(None, None)
    ):
        try:
            yield listener
        finally:
            # The cases below put work on the pools the cycle builds on first use;
            # a case that never reached them leaves nothing to shut down.
            for pool in (listener._cycle_pool, listener._consumer_pool):
                if pool is not None:
                    pool.shutdown()


class TestScanMissEndToEnd:
    """The scenarios of the backlog item, driven through whole cycles.

    Only a real session shows what these are about: SQL does not call ``NULL`` and
    ``0`` the same thing, so the condition on the DELETE is right or wrong here and
    nowhere else, and ``enabled`` survives precisely because the row keeps its ``id``.
    """

    @pytest.mark.asyncio
    async def test_disabled_row_survives_an_empty_listing_between_two_full_scans(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _disable_subscription(dagmodel_engine)
        before = _snapshot(dagmodel_engine)

        dag_file.unlink()
        await e2e_listener._run_cycle()

        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=2000)
        await e2e_listener._run_cycle()

        after = _snapshot(dagmodel_engine)
        assert after is not None
        assert after["id"] == before["id"]
        assert after["enabled"] is False
        # The sync step never ran on the empty cycle, so the miss was never counted.
        assert after["absent_cycles"] is None

    @pytest.mark.asyncio
    async def test_disabled_row_survives_a_syntax_error_in_its_file(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _disable_subscription(dagmodel_engine)
        before = _snapshot(dagmodel_engine)

        _write_dag_file(dag_file, _DAG_HALF_WRITTEN, mtime=2000)
        await e2e_listener._run_cycle()

        after = _snapshot(dagmodel_engine)
        assert after is not None
        assert after["id"] == before["id"]
        assert after["enabled"] is False
        assert after["absent_cycles"] is None

    @pytest.mark.asyncio
    async def test_a_broken_file_never_costs_the_disabled_row_its_flag(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        """The path on which deleting at the first miss would bring the defect back.

        A broken DAG file makes Airflow drop ``is_active`` within a minute or two, so
        both signals fail together — and the row the operator switched off has to come
        through it still switched off. The whole folder is unparseable here, which the
        cycle distrusts to the limit before the per-file hold and the miss counter get
        their turn.
        """
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=False)
        _disable_subscription(dagmodel_engine)
        before = _snapshot(dagmodel_engine)

        _write_dag_file(dag_file, _DAG_HALF_WRITTEN, mtime=2000)
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            await e2e_listener._run_cycle()   # the listing is not believed
            assert _snapshot(dagmodel_engine) == before

        await e2e_listener._run_cycle()   # believed; the previous parse is held
        assert _snapshot(dagmodel_engine) == before
        await e2e_listener._run_cycle()   # patience runs out, the key is gone

        after = _snapshot(dagmodel_engine)
        assert after is not None
        assert after["id"] == before["id"]
        assert after["enabled"] is False
        assert after["absent_cycles"] == 1

    @pytest.mark.asyncio
    async def test_removed_decorator_on_an_active_dag_waits_out_the_grace_period(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=True)

        _write_dag_file(dag_file, _DAG_WITHOUT_TRIGGER, mtime=2000)
        for miss in range(1, _ABSENT_GRACE_CYCLES + 1):
            await e2e_listener._run_cycle()
            row = _snapshot(dagmodel_engine)
            assert row is not None, f"deleted after {miss} miss(es)"
            assert row["absent_cycles"] == miss

        await e2e_listener._run_cycle()
        assert _snapshot(dagmodel_engine) is None

    @pytest.mark.asyncio
    async def test_an_unreadable_decorator_goes_the_way_of_a_removed_one(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        """A file the scan refuses answers no subscriptions, and the rows it
        registered before count misses exactly like those of a removed decorator."""
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=True)

        _write_dag_file(dag_file, _DAG_WITH_UNREADABLE_TRIGGER, mtime=2000)
        for miss in range(1, _ABSENT_GRACE_CYCLES + 1):
            await e2e_listener._run_cycle()
            row = _snapshot(dagmodel_engine)
            assert row is not None, f"deleted after {miss} miss(es)"
            assert row["absent_cycles"] == miss

        await e2e_listener._run_cycle()
        assert _snapshot(dagmodel_engine) is None

    @pytest.mark.asyncio
    async def test_removed_decorator_on_an_inactive_dag_deletes_on_the_third_miss(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        """An enabled row of a DAG Airflow no longer runs still gets two misses free.

        Its consumer acks and drops every delivery, hence the hurry; one shared hiccup
        of the filesystem has to be survivable, and one count can be a race between
        two schedulers rather than a miss at all, hence two.
        """
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=False)

        _write_dag_file(dag_file, _DAG_WITHOUT_TRIGGER, mtime=2000)
        for miss in range(1, _INACTIVE_DAG_MISSES + 1):
            await e2e_listener._run_cycle()
            row = _snapshot(dagmodel_engine)
            assert row is not None, f"deleted after {miss} miss(es)"
            assert row["enabled"] is True
            assert row["absent_cycles"] == miss

        await e2e_listener._run_cycle()
        assert _snapshot(dagmodel_engine) is None

    @pytest.mark.asyncio
    async def test_a_row_under_the_counter_still_reaches_reconcile(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        """The counter is not a visibility flag: the row keeps its consumer task."""
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=True)

        _write_dag_file(dag_file, _DAG_WITHOUT_TRIGGER, mtime=2000)
        await e2e_listener._run_cycle()

        assert _snapshot(dagmodel_engine)["absent_cycles"] == 1
        active_subs = e2e_listener._manager.reconcile.await_args.args[0]
        assert [(s["dag_id"], s["queue_name"]) for s in active_subs] == [("d", "q")]

    @pytest.mark.asyncio
    async def test_a_dag_the_scheduler_never_parsed_takes_the_short_circuit(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        """No row in ``dag`` at all reads as inactive, exactly like ``is_active=False``."""
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()

        _write_dag_file(dag_file, _DAG_WITHOUT_TRIGGER, mtime=2000)
        for miss in range(1, _INACTIVE_DAG_MISSES + 1):
            await e2e_listener._run_cycle()
            assert _snapshot(dagmodel_engine)["absent_cycles"] == miss

        await e2e_listener._run_cycle()
        assert _snapshot(dagmodel_engine) is None

    @pytest.mark.asyncio
    async def test_a_ui_row_survives_the_grace_period_deletion(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=True)
        _add_ui_subscription(dagmodel_engine)
        before = _snapshot(dagmodel_engine, queue_name="ui_q")

        _write_dag_file(dag_file, _DAG_WITHOUT_TRIGGER, mtime=2000)
        for _ in range(_ABSENT_GRACE_CYCLES + 1):
            await e2e_listener._run_cycle()

        assert _snapshot(dagmodel_engine) is None
        after = _snapshot(dagmodel_engine, queue_name="ui_q")
        assert after == before

    @pytest.mark.asyncio
    async def test_a_ui_row_survives_the_short_circuit_deletion(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=False)
        _add_ui_subscription(dagmodel_engine)
        before = _snapshot(dagmodel_engine, queue_name="ui_q")

        _write_dag_file(dag_file, _DAG_WITHOUT_TRIGGER, mtime=2000)
        for _ in range(_INACTIVE_DAG_MISSES + 1):
            await e2e_listener._run_cycle()

        assert _snapshot(dagmodel_engine) is None
        after = _snapshot(dagmodel_engine, queue_name="ui_q")
        assert after == before

    @pytest.mark.asyncio
    async def test_a_folder_that_empties_for_good_eventually_deletes_the_row(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        """The one path on which both counters run one after the other.

        The empty listing is distrusted for :data:`_UNTRUSTED_SCAN_LIMIT` cycles, the
        one after them evicts the cache, and only then do the trusted misses start
        counting towards the deletion.
        """
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=True)
        before = _snapshot(dagmodel_engine)

        dag_file.unlink()
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            await e2e_listener._run_cycle()
        assert _snapshot(dagmodel_engine) == before

        for miss in range(1, _ABSENT_GRACE_CYCLES + 1):
            await e2e_listener._run_cycle()
            row = _snapshot(dagmodel_engine)
            assert row is not None, f"deleted after {miss} trusted miss(es)"
            assert row["absent_cycles"] == miss

        await e2e_listener._run_cycle()
        assert _snapshot(dagmodel_engine) is None

    @pytest.mark.asyncio
    async def test_only_a_trusted_scan_moves_the_scan_ordinal(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        """A listing the cycle refuses to read is not a statement about the folder.

        The ordinal is what a sync step checks its own listing against, so a cycle
        that kept the rows as they are must leave a sync still running alone.
        """
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        assert e2e_listener._scan_generation == 1

        _write_dag_file(dag_file, _DAG_HALF_WRITTEN, mtime=2000)
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            await e2e_listener._run_cycle()

        assert e2e_listener._scan_generation == 1

    @pytest.mark.asyncio
    async def test_a_sync_a_later_scan_overtook_deletes_nothing(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        """The defect the miss counter exists to close, reached through a stuck worker.

        A sync step that outlives its timeout goes on running in its pool worker while
        the cycles carry on. The next cycle reads the folder again — the key is back —
        and is refused the sync step, because the earlier attempt has not returned. The
        listing that worker holds is two cycles old, and the deletion it decided on is
        one the grace-period branch asks nothing about ``enabled`` before making, so a
        row an operator switched off is what it would take with it.
        """
        gate = threading.Event()
        sync = e2e_listener._sync_to_db

        def sync_that_outlives_its_timeout(*args):
            gate.wait(timeout=5)
            return sync(*args)

        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=True)
        _disable_subscription(dagmodel_engine)
        with _db(dagmodel_engine) as session:
            session.query(RMQSubscription).filter_by(dag_id="d").update(
                {"absent_cycles": _ABSENT_GRACE_CYCLES}
            )
            session.commit()
        before = _snapshot(dagmodel_engine)

        e2e_listener._sync_to_db = sync_that_outlives_its_timeout
        _write_dag_file(dag_file, _DAG_WITHOUT_TRIGGER, mtime=2000)
        with patch("airflow_provider_rmq.watcher.listener._STEP_TIMEOUT", 0.5):
            await e2e_listener._run_cycle()   # the sync is given up on, its worker is not
            _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=3000)
            await e2e_listener._run_cycle()   # the key is back, and this scan is trusted

        gate.set()
        e2e_listener._step_attempts["sync"].result(timeout=5)

        assert _snapshot(dagmodel_engine) == before
        assert e2e_listener._deleted_dag_file_rows == {}


# ---------------------------------------------------------------------------
# Two schedulers writing the same row
# ---------------------------------------------------------------------------


def _add_dag_file_subscription(
    engine, dag_id="d", queue_name="q", *, enabled=True, absent_cycles=None
):
    """Row of a dag_file subscription in whatever state a scenario starts from."""
    with _db(engine) as session:
        session.add(RMQSubscription(
            dag_id=dag_id, queue_name=queue_name, conn_id="rmq_default",
            source="dag_file", enabled=enabled, absent_cycles=absent_cycles,
        ))
        session.commit()


def _reader_another_scheduler_writes_inside(engine, values, *, active):
    """Active-dag read with another scheduler's committed write landing inside it.

    Every one of these races lives in the window between this cycle reading a row and
    writing it, and the active-dag read is the point inside that window a test can
    hold open. ``active`` is what Airflow answers about ``d`` once the write is in.
    """
    def read():
        with _db(engine) as session:
            session.query(RMQSubscription).filter_by(dag_id="d").update(values)
            session.commit()
        return {"d"} if active else set()
    return read


def _another_scheduler_writes_before_the_delete(listener, engine, values):
    """Land another scheduler's committed write between the row's read and its DELETE.

    The grace-period deletion asks Airflow nothing, so the hook a race against it needs
    is the deletion itself: the write goes in on its own connection and the statement
    that follows is the real one, matched on the value this cycle read.
    """
    original = listener._delete_absent_sub

    def delete(*args, **kwargs):
        with _db(engine) as session:
            session.query(RMQSubscription).filter_by(dag_id="d").update(values)
            session.commit()
        return original(*args, **kwargs)

    return delete


@pytest.fixture
def two_process_engine(tmp_path):
    """File-backed engine whose sessions each get a connection of their own.

    A row committed by another scheduler mid-cycle is what these cases turn on, and
    it needs a database two connections can hold transactions on at once: the
    in-memory engine the scenarios above share is one connection wide, where a commit
    from the second session would carry the first one's unfinished work with it.
    """
    from airflow.models import DagModel

    engine = create_engine(f"sqlite:///{tmp_path / 'watcher.db'}", poolclass=NullPool)
    WatcherBase.metadata.create_all(engine)
    DagModel.__table__.create(engine, checkfirst=True)
    yield engine
    engine.dispose()


class TestConcurrentSchedulerWrites:
    """Every row is written from a value read some milliseconds earlier.

    Both writes the miss counter makes are matched on that value, so a scheduler
    holding a stale one writes nothing rather than undoing what another scheduler
    committed in between.
    """

    @staticmethod
    def _sync(listener, engine, read_active):
        with patch("airflow_provider_rmq.watcher.listener.WatcherSession",
                   sessionmaker(bind=engine)), \
             patch("airflow_provider_rmq.watcher.listener._read_active_dag_ids",
                   read_active):
            listener._sync_to_db([])

    def test_a_row_switched_off_in_the_window_keeps_the_full_grace_period(
        self, two_process_engine, caplog
    ):
        """The early deletion is owed to the consumer, and a row switched off has none.

        An operator pressing the button on the Subscriptions page while the cycle is
        between its read and its write leaves a disabled row, and a disabled row gets
        the whole grace period — the flag is the very thing the grace period is there
        to keep. The line about the row says what happened to it: a deletion the
        predicate refused is not a deletion, and a log claiming one would be the only
        record anybody has of the cycle.
        """
        listener = RMQWatcherListener()
        _add_dag_file_subscription(
            two_process_engine, enabled=True, absent_cycles=_INACTIVE_DAG_MISSES
        )

        with caplog.at_level(
            logging.INFO, logger=_LISTENER_LOG
        ):
            self._sync(
                listener, two_process_engine,
                _reader_another_scheduler_writes_inside(
                    two_process_engine, {"enabled": False}, active=False
                ),
            )

        row = _snapshot(two_process_engine)
        assert row is not None
        assert row["enabled"] is False
        assert listener._deleted_dag_file_rows == {}
        messages = [r.getMessage() for r in caplog.records]
        assert not any("deleted" in m for m in messages), messages
        assert any("keeps the grace period" in m for m in messages), messages

    def test_a_deletion_the_recovery_refused_is_not_reported_as_done(
        self, two_process_engine, caplog
    ):
        """The row outlived its last miss, and the line about it says so.

        The counter another scheduler cleared inside the window is what the ``DELETE``
        is matched on, so it matches nothing — and the observability the counted miss
        replaced silent deletion with is worth only what its lines are true about.
        """
        listener = RMQWatcherListener()
        _add_dag_file_subscription(
            two_process_engine, enabled=True, absent_cycles=_ABSENT_GRACE_CYCLES
        )

        listener._delete_absent_sub = _another_scheduler_writes_before_the_delete(
            listener, two_process_engine, {"absent_cycles": None}
        )

        with caplog.at_level(
            logging.INFO, logger=_LISTENER_LOG
        ):
            self._sync(listener, two_process_engine, lambda: {"d"})

        assert _snapshot(two_process_engine) is not None
        assert listener._deleted_dag_file_rows == {}
        messages = [r.getMessage() for r in caplog.records]
        assert not any("deleted" in m for m in messages), messages
        assert any(
            "its miss counter moved since this cycle read it" in m for m in messages
        ), messages

    def test_a_count_committed_inside_the_window_loses_to_the_scan(
        self, two_process_engine
    ):
        """The key is in this cycle's scan, so the count another scheduler wrote goes.

        The reset is written the same way the counter is: a statement the database
        runs, not an attribute the session diffs against the value it read. A session
        that read the counter as ``NULL`` sees no change in assigning ``None``, and
        the count that landed in between would survive a whole cycle of the key being
        there — leaving the row one miss from deletion.
        """
        listener = RMQWatcherListener()
        _add_dag_file_subscription(two_process_engine, enabled=True)

        def upsert_after_another_scheduler_counted(session, **kwargs):
            with _db(two_process_engine) as other:
                other.query(RMQSubscription).filter_by(dag_id="d").update(
                    {"absent_cycles": 1}
                )
                other.commit()
            return upsert_subscription(session, **kwargs)

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession",
                   sessionmaker(bind=two_process_engine)), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription",
                   upsert_after_another_scheduler_counted):
            listener._sync_to_db([{"dag_id": "d", "queue_name": "q", "filter_data": {}}])

        assert _snapshot(two_process_engine)["absent_cycles"] is None

    def test_a_recovery_another_scheduler_committed_is_not_overwritten(
        self, two_process_engine
    ):
        """The key came back in that scheduler's scan, and its upsert cleared the count."""
        listener = RMQWatcherListener()
        _add_dag_file_subscription(two_process_engine, enabled=True, absent_cycles=2)

        self._sync(
            listener, two_process_engine,
            _reader_another_scheduler_writes_inside(
                two_process_engine, {"absent_cycles": None}, active=True
            ),
        )

        assert _snapshot(two_process_engine)["absent_cycles"] is None

    def test_a_stale_count_cannot_delete_the_row_the_recovery_saved(
        self, two_process_engine
    ):
        """What the overwritten recovery costs is the row, one miss later.

        A count written from a stale read puts the row back where the next miss
        deletes it, and the successful scan in between — a whole cycle of another
        scheduler seeing the key — leaves no trace at all.
        """
        listener = RMQWatcherListener()
        _add_dag_file_subscription(
            two_process_engine, enabled=True, absent_cycles=_ABSENT_GRACE_CYCLES - 1
        )

        self._sync(
            listener, two_process_engine,
            _reader_another_scheduler_writes_inside(
                two_process_engine, {"absent_cycles": None}, active=True
            ),
        )
        self._sync(listener, two_process_engine, lambda: {"d"})

        row = _snapshot(two_process_engine)
        assert row is not None
        assert row["absent_cycles"] == 1

    def test_a_count_this_race_leaves_behind_cannot_delete_the_row_alone(
        self, two_process_engine
    ):
        """One count is not proof of a miss, so the early deletion asks for two.

        A cycle that read the counter as ``NULL`` writes its miss after another
        scheduler's successful scan committed the reset, and on a counter that already
        reads ``NULL`` that reset is a write the database has nothing to do about:
        nothing the later write could be matched against is left behind. The count
        that lands is therefore about a key that is there, and no compare-and-swap can
        tell it from one about a key that is not — the column carries no version and
        no ordering.
        """
        listener = RMQWatcherListener()
        _add_dag_file_subscription(two_process_engine, enabled=True)
        count_the_miss = listener._count_absent_cycle

        def count_after_another_scheduler_scanned(session, sub, observed, pending_logs):
            with _db(two_process_engine) as other:
                upsert_subscription(
                    other, dag_id="d", queue_name="q", source="dag_file"
                )
                other.commit()
            return count_the_miss(session, sub, observed, pending_logs)

        listener._count_absent_cycle = count_after_another_scheduler_scanned
        self._sync(listener, two_process_engine, lambda: set())
        listener._count_absent_cycle = count_the_miss
        assert _snapshot(two_process_engine)["absent_cycles"] == 1

        # One real miss, on a DAG Airflow no longer runs — the pair the short circuit
        # used to delete on.
        self._sync(listener, two_process_engine, lambda: set())
        row = _snapshot(two_process_engine)
        assert row is not None, "one real miss deleted the row"
        assert row["absent_cycles"] == _INACTIVE_DAG_MISSES

        self._sync(listener, two_process_engine, lambda: set())
        assert _snapshot(two_process_engine) is None

    def test_a_deletion_already_flushed_survives_the_active_dag_read(
        self, dagmodel_engine
    ):
        """The active-dag read runs before the loop writes, and this is why.

        Its session is one of its own, and a pool one connection wide — an in-memory
        SQLite, which is what an installation on it has — hands that session the
        connection this cycle is writing through. Closing it rolls the connection
        back, taking with it every statement the cycle had already flushed, while the
        cycle goes on to commit and to remember deletions the database no longer has.
        """
        listener = RMQWatcherListener()
        _record_dag(dagmodel_engine, dag_id="b", is_active=False)
        _add_dag_file_subscription(
            dagmodel_engine, dag_id="a", absent_cycles=_ABSENT_GRACE_CYCLES
        )
        _add_dag_file_subscription(
            dagmodel_engine, dag_id="b", absent_cycles=_INACTIVE_DAG_MISSES
        )

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession",
                   sessionmaker(bind=dagmodel_engine)):
            listener._sync_to_db([])

        assert _snapshot(dagmodel_engine, dag_id="a") is None
        assert _snapshot(dagmodel_engine, dag_id="b") is None
        assert set(listener._deleted_dag_file_rows) == {
            ("a", "q", "rmq_default"), ("b", "q", "rmq_default"),
        }


# ---------------------------------------------------------------------------
# Heartbeat of the watcher-process run
# ---------------------------------------------------------------------------

_H = listener_module._HEARTBEAT_TIMEOUT
_P = listener_module._PRUNE_TIMEOUT
_V = listener_module._VARIABLE_TIMEOUT


@pytest.fixture
def heartbeat_db(schema_engine):
    """The engine standing in for Airflow's, with the listener's sessions bound to it.

    The heartbeat table is not created here: the first heartbeat write creates it.
    """
    with patch(
        "airflow_provider_rmq.watcher.listener.WatcherSession",
        sessionmaker(bind=schema_engine),
    ):
        yield schema_engine


def _heartbeat_rows(engine) -> dict[str, dict]:
    """The heartbeat rows by id, as plain values."""
    with sessionmaker(bind=engine)() as session:
        return {
            row.id: {
                "hostname": row.hostname,
                "pid": row.pid,
                "started_at": row.started_at,
                "last_cycle_at": row.last_cycle_at,
                "due_at": row.due_at,
                "stopped_at": row.stopped_at,
            }
            for row in session.query(RMQWatcherHeartbeat).all()
        }


def _the_row(engine) -> dict:
    rows = _heartbeat_rows(engine)
    assert len(rows) == 1, rows
    return next(iter(rows.values()))


def _heartbeat_listener() -> RMQWatcherListener:
    """A cycle listener with a run identity, as :meth:`RMQWatcherListener.run` sets it."""
    listener = _cycle_listener()
    listener._identity = WatcherIdentity(
        id="run-1", hostname="worker-1", pid=4321, started_at=listener_module._utcnow()
    )
    return listener


def _stop_on(listener, passes: int):
    """A reconcile stand-in that asks for the stop during pass number ``passes``."""
    seen = []

    async def reconcile(subs):
        seen.append(1)
        if len(seen) >= passes:
            listener._stop_event.set()

    return AsyncMock(side_effect=reconcile)


def _stop_after_waits(listener, waits: int, on_wait=None):
    """A between-passes wait that asks for the stop on wait number ``waits``."""
    seen = []

    async def wait():
        seen.append(1)
        if on_wait is not None:
            on_wait()
        if len(seen) >= waits:
            listener._stop_event.set()

    return AsyncMock(side_effect=wait)


@contextlib.contextmanager
def _heartbeat_cycle_patches(manager):
    """Patch everything a cycle reaches outside the listener, leaving the sessions to
    :func:`heartbeat_db`."""
    with patch(
        "airflow_provider_rmq.watcher.listener.RMQConsumerManager", return_value=manager
    ), patch(
        "airflow_provider_rmq.watcher.listener.get_enabled_subscriptions", return_value=[]
    ), patch(
        "airflow_provider_rmq.watcher.listener.is_schema_ready", return_value=True
    ), patch(
        "airflow_provider_rmq.watcher.listener._read_settings", return_value=(None, None)
    ):
        yield


def _heartbeat_warnings(caplog) -> list[str]:
    return [
        r.getMessage() for r in caplog.records
        if r.levelno >= logging.WARNING and r.name == _LISTENER_LOG
    ]


def _spy_writes(monkeypatch) -> list[str]:
    """Record the name of every heartbeat write the listener runs, then run it."""
    names: list[str] = []
    real = listener_module._write_heartbeat

    def write(fn, identity, *args):
        names.append(fn.__name__)
        return real(fn, identity, *args)

    monkeypatch.setattr(listener_module, "_write_heartbeat", write)
    return names


class TestHeartbeatRecords:
    """The run's row: a deadline before the first pass, a mark after every pass that
    finished within its budget."""

    @pytest.mark.asyncio
    async def test_the_start_row_carries_the_deadline_of_the_overridden_budget(
        self, heartbeat_db, caplog
    ):
        listener = _heartbeat_listener()
        # The real settings read, so the budget comes from rmq_watcher_cycle_timeout.
        del listener._refresh_settings
        manager = _fake_manager()
        manager.reconcile = _stop_on(listener, 1)
        before = listener_module._utcnow()

        with _heartbeat_cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener._read_settings",
            return_value=(None, 900.0),
        ), caplog.at_level(logging.INFO, logger=_LISTENER_LOG):
            await call_with_timeout(listener._main(), 5)
        after = listener_module._utcnow()

        row = _the_row(heartbeat_db)
        assert listener._cycle_timeout() == 900.0
        span = timedelta(seconds=_H + 900.0 + _H)
        assert before + span <= row["due_at"] <= after + span
        assert row["hostname"] == "worker-1"
        assert row["pid"] == 4321
        assert row["started_at"] == listener._identity.started_at
        assert row["last_cycle_at"] is None, "a pass the stop cut short is not marked"
        assert row["stopped_at"] is None
        assert _heartbeat_warnings(caplog) == []

    @pytest.mark.asyncio
    async def test_a_larger_budget_records_a_new_deadline_before_the_pass(
        self, heartbeat_db, monkeypatch
    ):
        listener = _heartbeat_listener()
        del listener._refresh_settings
        manager = _fake_manager()
        rows_seen = []

        async def reconcile(subs):
            rows_seen.append((listener_module._utcnow(), _the_row(heartbeat_db)))
            if len(rows_seen) == 2:
                listener._stop_event.set()

        manager.reconcile = AsyncMock(side_effect=reconcile)
        # The settings read puts the default interval back; the passes follow at once.
        listener._wait_for_next_cycle = AsyncMock()
        writes = _spy_writes(monkeypatch)

        with _heartbeat_cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener._read_settings",
            side_effect=[(None, 300.0), (None, 900.0)],
        ):
            await call_with_timeout(listener._main(), 5)

        assert writes == [
            "record_heartbeat_start", "record_heartbeat_cycle", "record_heartbeat_due",
        ]
        (_, during_first), (second_seen_at, during_second) = rows_seen
        assert during_first["last_cycle_at"] is None
        # The mark of the first pass stays; the deadline is the larger budget's, taken
        # between that mark and the second reconcile.
        first_mark = during_second["last_cycle_at"]
        assert first_mark is not None
        assert _the_row(heartbeat_db)["last_cycle_at"] == first_mark
        span = timedelta(seconds=_H + 900.0 + _H)
        assert first_mark + span <= during_second["due_at"] <= second_seen_at + span

    @pytest.mark.asyncio
    async def test_an_unchanged_or_smaller_budget_records_no_deadline_before_the_pass(
        self, heartbeat_db, monkeypatch
    ):
        listener = _heartbeat_listener()
        del listener._refresh_settings
        manager = _fake_manager()
        manager.reconcile = _stop_on(listener, 3)
        # The settings read puts the default interval back; the passes follow at once.
        listener._wait_for_next_cycle = AsyncMock()
        writes = _spy_writes(monkeypatch)

        with _heartbeat_cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener._read_settings",
            side_effect=[(None, 600.0), (None, 600.0), (None, 300.0)],
        ):
            await call_with_timeout(listener._main(), 5)

        assert writes == [
            "record_heartbeat_start", "record_heartbeat_cycle", "record_heartbeat_cycle",
        ]

    @pytest.mark.asyncio
    async def test_a_pass_within_its_budget_moves_the_mark_and_the_deadline(
        self, heartbeat_db
    ):
        listener = _heartbeat_listener()
        listener._cycle_timeout_override = 300.0
        listener._reconcile_interval = 7
        manager = _fake_manager()
        marks = []
        listener._wait_for_next_cycle = _stop_after_waits(
            listener, 2, on_wait=lambda: marks.append(_the_row(heartbeat_db))
        )
        before = listener_module._utcnow()

        with _heartbeat_cycle_patches(manager):
            await call_with_timeout(listener._main(), 5)

        first, second = marks
        assert before <= first["last_cycle_at"] <= second["last_cycle_at"]
        for mark in marks:
            assert mark["due_at"] - mark["last_cycle_at"] == timedelta(
                seconds=_H + _P + 7 + _V + 300.0 + _H
            )
            assert mark["stopped_at"] is None

    @pytest.mark.asyncio
    async def test_a_pass_that_ran_out_of_budget_marks_nothing(self, heartbeat_db):
        listener = _heartbeat_listener()
        listener._cycle_timeout_override = 0.05
        manager = _fake_manager()
        manager.reconcile = _never_returns

        with _heartbeat_cycle_patches(manager), pytest.raises(asyncio.TimeoutError):
            await listener._main()

        row = _the_row(heartbeat_db)
        assert row["last_cycle_at"] is None
        assert row["due_at"] is not None

    def test_a_restart_of_main_keeps_the_run_and_marks_no_stop(
        self, heartbeat_db, monkeypatch
    ):
        """``_main`` crashes after its first mark and the loop restarts it: the run, its
        row and its start time stay, no second start is recorded, and a crash is not an
        orderly stop."""
        listener = _cycle_listener()
        listener._stop_event = _instant_stop_event()
        manager = _fake_manager()
        waits = []

        async def wait():
            waits.append(1)
            if len(waits) == 1:
                raise RuntimeError("the loop blew up")
            listener._stop_event.set()

        listener._wait_for_next_cycle = AsyncMock(side_effect=wait)
        writes = _spy_writes(monkeypatch)

        with _heartbeat_cycle_patches(manager):
            listener.run(parent_pid=os.getppid())

        assert writes == [
            "record_heartbeat_start", "record_heartbeat_cycle", "record_heartbeat_cycle",
        ]
        rows = _heartbeat_rows(heartbeat_db)
        assert list(rows) == [listener._identity.id]
        row = rows[listener._identity.id]
        assert row["started_at"] == listener._identity.started_at
        assert row["pid"] == os.getpid()
        assert row["last_cycle_at"] is not None
        assert row["stopped_at"] is None

    @pytest.mark.asyncio
    async def test_old_rows_of_other_runs_are_pruned_after_the_mark(self, heartbeat_db):
        listener = _heartbeat_listener()
        now = listener_module._utcnow()
        ensure_heartbeat_table()
        with sessionmaker(bind=heartbeat_db)() as session:
            for run_id, age in (("old", timedelta(days=2)), ("recent", timedelta(hours=2))):
                record_heartbeat_start(
                    session,
                    WatcherIdentity(id=run_id, hostname="h", pid=1, started_at=now - age),
                    now - age,
                )
        manager = _fake_manager()
        listener._wait_for_next_cycle = _stop_after_waits(listener, 1)

        with _heartbeat_cycle_patches(manager):
            await call_with_timeout(listener._main(), 5)

        assert set(_heartbeat_rows(heartbeat_db)) == {"run-1", "recent"}

    def test_nothing_is_written_without_a_run(self):
        """``_main`` driven without :meth:`run` — every process but the watcher's —
        writes no heartbeat."""
        listener = _cycle_listener()
        manager = _fake_manager()
        manager.reconcile = _stop_on(listener, 1)
        write = MagicMock()

        with _cycle_patches(manager), patch.object(
            listener_module, "_write_heartbeat", write
        ):
            asyncio.run(listener._main())

        write.assert_not_called()


class TestHeartbeatStopMark:
    """An orderly stop marks the row from a thread of its own, bounded."""

    @pytest.mark.asyncio
    async def test_the_mark_is_written_while_a_reconcile_still_runs(self, heartbeat_db):
        listener = _heartbeat_listener()
        manager = _fake_manager()
        entered = asyncio.Event()
        release = asyncio.Event()

        async def reconcile(subs):
            entered.set()
            await release.wait()

        manager.reconcile = AsyncMock(side_effect=reconcile)

        def stop() -> None:
            listener.request_stop()
            listener.mark_stopped()

        with _heartbeat_cycle_patches(manager):
            task = asyncio.ensure_future(listener._main())
            await call_with_timeout(entered.wait(), 5)
            await asyncio.to_thread(stop)
            during = _the_row(heartbeat_db)
            assert not task.done(), "the reconcile is still running"
            release.set()
            await call_with_timeout(task, 5)

        assert during["stopped_at"] is not None
        after = _the_row(heartbeat_db)
        assert after["stopped_at"] == during["stopped_at"]
        assert after["last_cycle_at"] is None, "no mark once the stop has been requested"
        assert after["due_at"] == during["due_at"]

    def test_a_hung_write_is_not_waited_for_past_the_bound(self, monkeypatch, caplog):
        assert listener_module._STOP_MARK_TIMEOUT <= 2.0
        monkeypatch.setattr(listener_module, "_STOP_MARK_TIMEOUT", 0.1)
        release = threading.Event()
        monkeypatch.setattr(listener_module, "_write_heartbeat", _blocked_by(release))
        listener = _heartbeat_listener()

        try:
            with caplog.at_level(logging.WARNING, logger=_LISTENER_LOG):
                started = time.monotonic()
                listener.mark_stopped()
                elapsed = time.monotonic() - started
        finally:
            release.set()

        assert elapsed < 1.0
        assert any("was not written within" in m for m in _heartbeat_warnings(caplog))

    def test_a_failed_write_is_logged_and_the_stop_goes_on(self, monkeypatch, caplog):
        monkeypatch.setattr(
            listener_module, "_write_heartbeat",
            MagicMock(side_effect=RuntimeError("database is gone")),
        )
        listener = _heartbeat_listener()

        with caplog.at_level(logging.WARNING, logger=_LISTENER_LOG):
            listener.mark_stopped()

        assert any("cannot mark the stop" in m for m in _heartbeat_warnings(caplog))

    def test_a_second_call_writes_nothing(self, heartbeat_db, monkeypatch):
        writes = _spy_writes(monkeypatch)
        listener = _heartbeat_listener()

        listener.mark_stopped()
        first = _the_row(heartbeat_db)
        listener.mark_stopped()

        assert writes == ["record_heartbeat_stop"]
        assert _the_row(heartbeat_db) == first
        assert first["stopped_at"] is not None
        assert first["pid"] == 4321

    def test_a_call_before_run_writes_nothing(self, monkeypatch):
        write = MagicMock()
        monkeypatch.setattr(listener_module, "_write_heartbeat", write)
        listener = RMQWatcherListener()

        listener.mark_stopped()

        write.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_stop_before_the_start_record_submits_none(
        self, heartbeat_db, monkeypatch, caplog
    ):
        listener = _heartbeat_listener()
        manager = _fake_manager()
        writes = _spy_writes(monkeypatch)

        async def refresh_then_stop():
            listener.request_stop()
            await asyncio.to_thread(listener.mark_stopped)

        listener._refresh_settings = AsyncMock(side_effect=refresh_then_stop)

        with _heartbeat_cycle_patches(manager), caplog.at_level(
            logging.INFO, logger=_LISTENER_LOG
        ):
            await call_with_timeout(listener._main(), 5)

        assert writes == ["record_heartbeat_stop"]
        manager.reconcile.assert_not_awaited()
        row = _the_row(heartbeat_db)
        assert row["stopped_at"] is not None
        assert row["last_cycle_at"] is None
        assert _heartbeat_warnings(caplog) == []

    @pytest.mark.asyncio
    async def test_a_stop_during_the_start_record_ends_with_stopped_at(
        self, heartbeat_db, monkeypatch, caplog
    ):
        listener = _heartbeat_listener()
        manager = _fake_manager()
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        real_start = listener_module.record_heartbeat_start

        def slow_start(*args):
            entered.set()
            release.wait(timeout=5)
            try:
                real_start(*args)
            finally:
                finished.set()

        monkeypatch.setattr(listener_module, "record_heartbeat_start", slow_start)

        def stop() -> None:
            listener.request_stop()
            listener.mark_stopped()

        try:
            with _heartbeat_cycle_patches(manager), caplog.at_level(
                logging.INFO, logger=_LISTENER_LOG
            ):
                task = asyncio.ensure_future(listener._main())
                await call_with_timeout(asyncio.to_thread(entered.wait, 5), 5)
                await asyncio.to_thread(stop)
                await call_with_timeout(task, 5)
        finally:
            release.set()
        assert await asyncio.to_thread(finished.wait, 5)

        row = _the_row(heartbeat_db)
        assert row["stopped_at"] is not None, "the start record does not clear the mark"
        manager.reconcile.assert_not_awaited()
        assert _heartbeat_warnings(caplog) == []

    @pytest.mark.asyncio
    async def test_a_pass_mark_committed_after_the_stop_mark_keeps_it(
        self, heartbeat_db, monkeypatch, caplog
    ):
        listener = _heartbeat_listener()
        manager = _fake_manager()
        entered = threading.Event()
        release = threading.Event()
        finished = threading.Event()
        real_cycle = listener_module.record_heartbeat_cycle

        def slow_cycle(*args):
            entered.set()
            release.wait(timeout=5)
            try:
                real_cycle(*args)
            finally:
                finished.set()

        monkeypatch.setattr(listener_module, "record_heartbeat_cycle", slow_cycle)

        def stop() -> None:
            listener.request_stop()
            listener.mark_stopped()

        try:
            with _heartbeat_cycle_patches(manager), caplog.at_level(
                logging.INFO, logger=_LISTENER_LOG
            ):
                task = asyncio.ensure_future(listener._main())
                await call_with_timeout(asyncio.to_thread(entered.wait, 5), 5)
                await asyncio.to_thread(stop)
                await call_with_timeout(task, 5)
        finally:
            release.set()
        assert await asyncio.to_thread(finished.wait, 5)

        row = _the_row(heartbeat_db)
        assert row["stopped_at"] is not None
        assert row["last_cycle_at"] is not None
        assert _heartbeat_warnings(caplog) == [], "a stop is not a failed heartbeat"


class TestHeartbeatFailures:
    """A heartbeat that cannot be written never costs the loop its pass."""

    @staticmethod
    def _completed(incr):
        return [c for c in incr.call_args_list if c == call("rmq_watcher.cycle_completed")]

    @pytest.mark.asyncio
    async def test_a_failed_mark_neither_ends_the_loop_nor_the_metric(
        self, heartbeat_db, monkeypatch, caplog
    ):
        listener = _heartbeat_listener()
        manager = _fake_manager()
        listener._wait_for_next_cycle = _stop_after_waits(listener, 2)
        monkeypatch.setattr(
            listener_module, "record_heartbeat_cycle",
            MagicMock(side_effect=RuntimeError("database is gone")),
        )

        with _heartbeat_cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.incr"
        ) as incr, caplog.at_level(logging.WARNING, logger=_LISTENER_LOG):
            await call_with_timeout(listener._main(), 5)

        assert manager.reconcile.await_count == 2
        assert len(self._completed(incr)) == 2
        failures = [m for m in _heartbeat_warnings(caplog) if "'heartbeat' step" in m]
        assert len(failures) == 2

    @pytest.mark.asyncio
    async def test_a_failed_prune_keeps_the_mark(self, heartbeat_db, monkeypatch, caplog):
        listener = _heartbeat_listener()
        manager = _fake_manager()
        listener._wait_for_next_cycle = _stop_after_waits(listener, 2)
        monkeypatch.setattr(
            listener_module, "prune_heartbeats",
            MagicMock(side_effect=RuntimeError("lock wait timeout")),
        )

        with _heartbeat_cycle_patches(manager), patch(
            "airflow_provider_rmq.watcher.listener.incr"
        ) as incr, caplog.at_level(logging.WARNING, logger=_LISTENER_LOG):
            await call_with_timeout(listener._main(), 5)

        assert manager.reconcile.await_count == 2
        assert len(self._completed(incr)) == 2
        assert _the_row(heartbeat_db)["last_cycle_at"] is not None
        assert any("'heartbeat_prune' step" in m for m in _heartbeat_warnings(caplog))

    @pytest.mark.asyncio
    async def test_a_hung_write_is_not_submitted_again(
        self, heartbeat_db, monkeypatch, caplog
    ):
        monkeypatch.setattr(listener_module, "_HEARTBEAT_TIMEOUT", 0.1)
        listener = _heartbeat_listener()
        manager = _fake_manager()
        listener._wait_for_next_cycle = _stop_after_waits(listener, 3)
        release = threading.Event()
        calls: list = []
        monkeypatch.setattr(
            listener_module, "record_heartbeat_cycle", _blocked_by(release, calls=calls)
        )

        try:
            with _heartbeat_cycle_patches(manager), patch(
                "airflow_provider_rmq.watcher.listener.incr"
            ) as incr, caplog.at_level(logging.WARNING, logger=_LISTENER_LOG):
                await call_with_timeout(listener._main(), 5)
        finally:
            release.set()

        assert calls == [1]
        assert manager.reconcile.await_count == 3
        assert len(self._completed(incr)) == 3
        warnings = _heartbeat_warnings(caplog)
        assert sum("heartbeat not written" in m for m in warnings) == 2
        assert any("did not finish within 0.1s" in m for m in warnings)

    @pytest.mark.asyncio
    async def test_a_failed_table_creation_in_the_start_step_is_retried_by_the_mark(
        self, heartbeat_db, monkeypatch, caplog
    ):
        listener = _heartbeat_listener()
        manager = _fake_manager()
        listener._wait_for_next_cycle = _stop_after_waits(listener, 1)
        creations = []

        def create():
            creations.append(1)
            if len(creations) == 1:
                raise RuntimeError("permission denied")
            models.ensure_heartbeat_table()

        monkeypatch.setattr(listener_module, "ensure_heartbeat_table", create)

        with _heartbeat_cycle_patches(manager), caplog.at_level(
            logging.WARNING, logger=_LISTENER_LOG
        ):
            await call_with_timeout(listener._main(), 5)

        manager.reconcile.assert_awaited_once()
        assert any("'heartbeat' step" in m for m in _heartbeat_warnings(caplog))
        assert len(creations) == 2
        assert _the_row(heartbeat_db)["last_cycle_at"] is not None

    @pytest.mark.asyncio
    async def test_a_hung_table_creation_in_the_start_step_is_bounded(
        self, heartbeat_db, monkeypatch, caplog
    ):
        """The table creation of the start step hangs: the step gives up at its bound,
        the pass starts, and the mark after it creates the table once the hung call has
        returned."""
        monkeypatch.setattr(listener_module, "_HEARTBEAT_TIMEOUT", 0.1)
        listener = _heartbeat_listener()
        manager = _fake_manager()
        listener._wait_for_next_cycle = _stop_after_waits(listener, 1)
        release = threading.Event()
        creations = []

        def create():
            creations.append(1)
            if len(creations) == 1:
                release.wait(timeout=5)
                raise RuntimeError("lock wait timeout")
            models.ensure_heartbeat_table()

        monkeypatch.setattr(listener_module, "ensure_heartbeat_table", create)

        async def reconcile(subs):
            release.set()
            attempt = listener._step_attempts["heartbeat"]
            await call_with_timeout(asyncio.wrap_future(attempt), 5)

        manager.reconcile = AsyncMock(side_effect=reconcile)

        try:
            with _heartbeat_cycle_patches(manager), caplog.at_level(
                logging.WARNING, logger=_LISTENER_LOG
            ):
                await call_with_timeout(listener._main(), 5)
        finally:
            release.set()

        manager.reconcile.assert_awaited_once()
        assert any("did not finish within 0.1s" in m for m in _heartbeat_warnings(caplog))
        assert len(creations) == 2
        assert _the_row(heartbeat_db)["last_cycle_at"] is not None
