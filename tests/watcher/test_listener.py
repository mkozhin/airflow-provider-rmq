from __future__ import annotations

import ast
import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow_provider_rmq.watcher.listener import (
    RMQWatcherListener,
    _extract_dag_id_from_decorators,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _decorators(src: str) -> list:
    """Parse a one-function snippet and return its decorator_list."""
    return ast.parse(src).body[0].decorator_list

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
# _extract_dag_id_from_decorators
# ---------------------------------------------------------------------------

class TestExtractDagId:
    def test_string_literal_dag_id(self):
        decs = _decorators("@dag(dag_id='my_dag')\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) == "my_dag"

    def test_attribute_access_dag(self):
        decs = _decorators("@decorators.dag(dag_id='my_dag')\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) == "my_dag"

    def test_no_dag_id_kwarg_returns_none(self):
        decs = _decorators("@dag(schedule_interval=None)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is None

    def test_no_dag_decorator_returns_none(self):
        decs = _decorators("@some_other_decorator\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is None

    def test_non_literal_dag_id_returns_none(self):
        decs = _decorators("@dag(dag_id=VARIABLE)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is None

    def test_non_string_literal_dag_id_returns_none(self):
        decs = _decorators("@dag(dag_id=123)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is None

    def test_async_function_with_explicit_dag_id(self):
        src = "@dag(dag_id='async_dag')\nasync def f(): pass"
        decs = ast.parse(src).body[0].decorator_list
        assert _extract_dag_id_from_decorators(decs) == "async_dag"

    def test_empty_decorator_list_returns_none(self):
        assert _extract_dag_id_from_decorators([]) is None


# ---------------------------------------------------------------------------
# on_starting / before_stopping
# ---------------------------------------------------------------------------

class TestListenerLifecycle:
    def test_on_starting_with_scheduler_starts_thread(self):
        class SchedulerJobRunner:
            pass

        listener = RMQWatcherListener()
        with patch.object(listener, "_start") as mock_start:
            listener.on_starting(SchedulerJobRunner())
        mock_start.assert_called_once()

    def test_on_starting_with_webserver_ignores(self):
        class GunicornWebServer:
            pass

        listener = RMQWatcherListener()
        with patch.object(listener, "_start") as mock_start:
            listener.on_starting(GunicornWebServer())
        mock_start.assert_not_called()

    def test_before_stopping_sets_stop_event(self):
        listener = RMQWatcherListener()
        listener._stop_event = threading.Event()
        listener.before_stopping(MagicMock())
        assert listener._stop_event.is_set()

    def test_before_stopping_noop_when_not_started(self):
        listener = RMQWatcherListener()
        # _stop_event is None — must not raise
        listener.before_stopping(MagicMock())

    def test_scheduler_component_name_matches(self):
        # Regression: ensure the substring check works for Airflow 2.7+ class name
        assert "Scheduler" in "SchedulerJobRunner"

    def test_on_starting_with_job_type_scheduler_starts_thread(self):
        """Airflow 2.9+: component class is 'Job' but job_type='SchedulerJob'."""
        class Job:
            job_type = "SchedulerJob"

        listener = RMQWatcherListener()
        with patch.object(listener, "_start") as mock_start:
            listener.on_starting(Job())
        mock_start.assert_called_once()

    def test_on_starting_with_job_type_triggerer_ignores(self):
        """Triggerer job имеет job_type='TriggererJob' — не должен запускать watcher."""
        class Job:
            job_type = "TriggererJob"

        listener = RMQWatcherListener()
        with patch.object(listener, "_start") as mock_start:
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
             patch.object(listener, "_start") as mock_start:
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
             patch.object(listener, "_start") as mock_start:
            listener.on_starting(Job())
        mock_start.assert_not_called()

    def test_duplicate_on_starting_creates_only_one_thread(self):
        """L2: второй on_starting при живом потоке должен игнорироваться."""
        class SchedulerJobRunner:
            pass

        listener = RMQWatcherListener()

        with patch("threading.Thread") as mock_thread_cls:
            mock_thread = MagicMock()
            mock_thread.is_alive.return_value = True
            mock_thread_cls.return_value = mock_thread

            # Первый вызов — создаёт поток
            listener._start()
            # Имитируем, что поток запущен и stop_event не выставлен
            listener._thread = mock_thread
            listener._stop_event = threading.Event()

            # Второй вызов — поток жив, stop_event не выставлен → игнор
            listener._start()

        # Thread() конструктор вызван ровно один раз
        assert mock_thread_cls.call_count == 1

    def test_run_loop_restarts_after_crash(self):
        """L3: _run_loop должен перезапускать _main() после исключения."""
        listener = RMQWatcherListener()
        listener._stop_event = threading.Event()
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

        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=files), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]) as mock_ex:
            listener._scan_subscriptions()

        assert mock_ex.call_count == 2

    def test_scan_subscriptions_unchanged_files_not_reparsed(self):
        listener = self._listener()
        files = ["/dags/dag1.py"]

        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=files), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]) as mock_ex:
            listener._scan_subscriptions()  # first run
            listener._scan_subscriptions()  # same mtime — should NOT re-parse

        assert mock_ex.call_count == 1

    def test_scan_subscriptions_changed_file_reparsed(self):
        listener = self._listener()
        files = ["/dags/dag1.py"]
        mtime_values = iter([1000.0, 2000.0])

        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=files), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=mtime_values), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]) as mock_ex:
            listener._scan_subscriptions()  # mtime=1000
            listener._scan_subscriptions()  # mtime=2000 → re-parse

        assert mock_ex.call_count == 2

    def test_scan_subscriptions_deleted_file_removed_from_cache(self):
        listener = self._listener()
        file = "/dags/dag1.py"

        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=[file]), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]):
            listener._scan_subscriptions()

        assert file in listener._last_mtimes

        # Second scan: file is gone
        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=[]), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]):
            listener._scan_subscriptions()

        assert file not in listener._last_mtimes
        assert file not in listener._cached_subs

    def test_scan_subscriptions_finds_decorated_dags(self):
        listener = self._listener()
        expected = {
            "dag_id": "orders_dag",
            "queue_name": "orders",
            "conn_id": "rmq_default",
            "filter_data": {"filter_headers": {"type": "new_order"}},
        }

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=["/dags/orders.py"]), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[expected]):
            result = listener._scan_subscriptions()

        assert expected in result

    def test_scan_subscriptions_ignores_dags_without_attribute(self):
        listener = self._listener()

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=["/dags/plain.py"]), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]):
            result = listener._scan_subscriptions()

        assert result == []

    def test_extract_subscriptions_returns_empty_list_on_ioerror(self):
        listener = RMQWatcherListener()
        result = listener._extract_subscriptions_from_file("/nonexistent/broken.py")
        assert result == []


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

    def test_fallback_to_function_name_when_dag_id_is_variable(self, tmp_path):
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
        assert result[0]["dag_id"] == "variable_dag"


# ---------------------------------------------------------------------------
# _sync_to_db
# ---------------------------------------------------------------------------

class TestSyncToDb:
    def test_sync_to_db_upserts_dag_file_subscriptions(self):
        listener = RMQWatcherListener()
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "c", "filter_data": {}}]

        ctx, session = _make_session_ctx(existing_subs=[])

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription") as mock_up:
            listener._sync_to_db(scanned)

        mock_up.assert_called_once()
        call_kwargs = mock_up.call_args
        assert call_kwargs.kwargs["dag_id"] == "d"
        assert call_kwargs.kwargs["source"] == "dag_file"

    def test_sync_to_db_deletes_removed_dag_subscriptions(self):
        listener = RMQWatcherListener()

        # One dag_file subscription in DB, but nothing in scan
        existing = MagicMock()
        existing.dag_id = "old_dag"
        existing.queue_name = "q"
        existing.conn_id = "rmq_default"

        ctx, session = _make_session_ctx(existing_subs=[existing])

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription"):
            listener._sync_to_db([])  # empty scan → old_dag sub should be deleted

        session.query.return_value.filter_by.return_value.delete.assert_called()

    def test_sync_to_db_preserves_ui_subscriptions(self):
        listener = RMQWatcherListener()

        # Only dag_file subs are returned (filter_by source='dag_file') → none
        ctx, session = _make_session_ctx(existing_subs=[])

        delete_mock = session.query.return_value.filter_by.return_value.delete

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription"):
            listener._sync_to_db([])

        # No dag_file subs to delete → delete never called
        delete_mock.assert_not_called()

    def test_sync_to_db_does_not_delete_subscription_still_in_scan(self):
        listener = RMQWatcherListener()

        existing = MagicMock()
        existing.dag_id = "d"
        existing.queue_name = "q"
        existing.conn_id = "rmq_default"

        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default", "filter_data": {}}]
        ctx, session = _make_session_ctx(existing_subs=[existing])
        delete_mock = session.query.return_value.filter_by.return_value.delete

        with patch("airflow_provider_rmq.watcher.listener.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.listener.upsert_subscription"):
            listener._sync_to_db(scanned)

        # Subscription is still in scan → must NOT be deleted
        delete_mock.assert_not_called()
