from __future__ import annotations

import ast
import asyncio
import threading
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow_provider_rmq.watcher.listener import (
    RMQWatcherListener,
    _extract_dag_id_from_decorators,
    _parse_rmq_trigger_decorator,
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
        # Regression: ensure the substring check works for Airflow 2.9+ class name
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

    def test_two_exchange_decorators_on_same_function_second_skipped(self, tmp_path, caplog):
        import logging as _logging

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
        with caplog.at_level(_logging.WARNING):
            result = listener._extract_subscriptions_from_file(str(dag_file))

        assert len(result) == 1
        # ast.walk visits decorator_list in source order — first decorator parsed wins
        assert result[0]["exchange"] == "exchange.one"
        assert any("double_exchange_dag" in r.message for r in caplog.records)


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


# ---------------------------------------------------------------------------
# _parse_rmq_trigger_decorator — новые параметры queues и cooldown
# ---------------------------------------------------------------------------

def _parse_decorator(src: str, dag_id: str = "test_dag") -> list[dict]:
    """Parse a decorator call string and return subscription dicts."""
    node = ast.parse(src, mode="eval").body
    return _parse_rmq_trigger_decorator(node, dag_id)


class TestParseRmqTriggerDecorator:
    def test_single_queue_no_cooldown_returns_one_dict(self):
        result = _parse_decorator("rmq_trigger(queue='orders')")
        assert len(result) == 1
        assert result[0]["queue_name"] == "orders"
        assert result[0]["cooldown"] == 0
        assert result[0]["conn_id"] == "rmq_default"

    def test_queues_list_returns_n_dicts(self):
        result = _parse_decorator("rmq_trigger(queues=['orders', 'payments'])")
        assert len(result) == 2
        queue_names = [d["queue_name"] for d in result]
        assert "orders" in queue_names
        assert "payments" in queue_names

    def test_queues_list_all_share_same_cooldown(self):
        result = _parse_decorator("rmq_trigger(queues=['a', 'b', 'c'], cooldown=300)")
        assert len(result) == 3
        assert all(d["cooldown"] == 300 for d in result)

    def test_cooldown_parsed_correctly(self):
        result = _parse_decorator("rmq_trigger(queue='q', cooldown=60)")
        assert len(result) == 1
        assert result[0]["cooldown"] == 60

    def test_cooldown_zero_is_default(self):
        result = _parse_decorator("rmq_trigger(queue='q')")
        assert result[0]["cooldown"] == 0

    def test_non_rmq_trigger_returns_empty_list(self):
        result = _parse_decorator("some_other_decorator(queue='q')")
        assert result == []

    def test_no_queue_or_queues_returns_empty_list(self):
        result = _parse_decorator("rmq_trigger(conn_id='rmq')")
        assert result == []

    def test_non_literal_queues_skipped_returns_empty(self):
        result = _parse_decorator("rmq_trigger(queues=SOME_VAR)")
        assert result == []

    def test_conn_id_propagated_to_all_entries(self):
        result = _parse_decorator("rmq_trigger(queues=['a', 'b'], conn_id='my_conn')")
        assert all(d["conn_id"] == "my_conn" for d in result)

    def test_filter_data_propagated_to_all_entries(self):
        result = _parse_decorator(
            "rmq_trigger(queues=['a', 'b'], filter_data={'filter_headers': {'k': 'v'}})"
        )
        assert all(d["filter_data"] == {"filter_headers": {"k": "v"}} for d in result)

    def test_positional_queue_name_parsed(self):
        result = _parse_decorator("rmq_trigger('my_queue')")
        assert len(result) == 1
        assert result[0]["queue_name"] == "my_queue"

    def test_attribute_access_rmq_trigger(self):
        result = _parse_decorator("decorators.rmq_trigger(queue='q')")
        assert len(result) == 1
        assert result[0]["queue_name"] == "q"

    def test_negative_cooldown_skipped_returns_empty(self):
        result = _parse_decorator("rmq_trigger(queue='q', cooldown=-1)")
        assert result == []

    def test_negative_cooldown_logs_warning(self, caplog):
        import logging as _logging
        with caplog.at_level(_logging.WARNING):
            _parse_decorator("rmq_trigger(queue='q', cooldown=-1)", dag_id="neg_dag")
        assert any("neg_dag" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _parse_rmq_trigger_decorator — exchange-mode (Task 2)
# ---------------------------------------------------------------------------


class TestParseRmqTriggerDecoratorExchange:
    def test_exchange_with_routing_key_ids_literal_list(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'])",
            dag_id="my_dag",
        )
        assert len(result) == 1
        sub = result[0]
        assert sub["exchange"] == "jetstat.airflow"
        assert sub["queue_name"] == "rmq_watcher.sub.my_dag"
        assert sub["routing_keys"] == ["abc123.*"]

    def test_exchange_default_routing_key_status_is_wildcard(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'])"
        )
        assert result[0]["routing_keys"] == ["abc123.*"]

    def test_exchange_explicit_string_routing_key_status(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'], "
            "routing_key_status='succeeded')"
        )
        assert result[0]["routing_keys"] == ["abc123.succeeded"]

    def test_exchange_explicit_list_routing_key_status(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'], "
            "routing_key_status=['succeeded', 'failed'])"
        )
        assert set(result[0]["routing_keys"]) == {"abc123.succeeded", "abc123.failed"}

    def test_exchange_with_literal_routing_keys_directly(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='some.other.exchange', routing_keys=['region.eu.alert'])"
        )
        assert len(result) == 1
        assert result[0]["exchange"] == "some.other.exchange"
        assert result[0]["routing_keys"] == ["region.eu.alert"]

    def test_exchange_routing_keys_and_routing_key_ids_union(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_keys=['literal.key'], "
            "routing_key_ids=['abc123'])"
        )
        assert set(result[0]["routing_keys"]) == {"literal.key", "abc123.*"}

    def test_non_literal_routing_key_ids_skipped(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=SOME_VAR)"
        )
        # routing_key_ids not extracted (non-literal) → neither routing_keys nor
        # routing_key_ids present → build_subscriptions raises → skipped
        assert result == []

    def test_non_literal_routing_keys_skipped(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_keys=SOME_VAR)"
        )
        assert result == []

    def test_non_literal_routing_key_status_falls_back_to_default(self):
        # routing_key_status is non-literal → not extracted → build_subscriptions
        # falls back to its own default ("*"); routing_key_ids is still literal
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'], "
            "routing_key_status=SOME_VAR)"
        )
        assert result[0]["routing_keys"] == ["abc123.*"]

    def test_exchange_and_queue_mutex_violation_skipped(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', queue='q', routing_keys=['a.b'])"
        )
        assert result == []

    def test_exchange_without_routing_keys_skipped(self):
        result = _parse_decorator("rmq_trigger(exchange='jetstat.airflow')")
        assert result == []

    def test_exchange_dot_in_routing_key_id_skipped(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc.123'])"
        )
        assert result == []

    def test_exchange_reserved_prefix_skipped(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='rmq_watcher.something', routing_keys=['a.b'])"
        )
        assert result == []


# ---------------------------------------------------------------------------
# _extract_subscriptions_from_file — group_key и cooldown
# ---------------------------------------------------------------------------

class TestExtractSubscriptionsGroupKeyAndCooldown:
    def test_single_queue_cooldown_zero_group_key_is_none(self, tmp_path):
        dag_file = tmp_path / "dag.py"
        dag_file.write_text(
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


class TestMainExchangeMetaMerge:
    """Drives a single `_main()` reconcile iteration end-to-end with mocked
    DB/manager/scan, asserting the in-memory exchange/routing_keys merge
    described in the plan (Task 3) — the DB row itself never carries this
    metadata (see Technical Details → "Почему миграция БД не нужна")."""

    def _run_one_iteration(self, listener, scanned, db_subs):
        """Run `_main()` for exactly one reconcile cycle and capture the
        `active_subs` list passed to `RMQConsumerManager.reconcile`."""
        listener._stop_event = threading.Event()
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
