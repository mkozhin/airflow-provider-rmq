from __future__ import annotations

import ast
import asyncio
import contextlib
import glob
import logging
import os
import re
import sys
import threading
import time
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest
from sqlalchemy.orm import sessionmaker

from airflow_provider_rmq.utils.amqp import call_with_timeout
from airflow_provider_rmq.utils.executor import BoundedExecutor
from airflow_provider_rmq.watcher.listener import (
    CYCLE_TIMEOUT_VAR,
    RECONCILE_INTERVAL_VAR,
    RMQWatcherListener,
    _ABSENT_GRACE_CYCLES,
    _UNRESOLVED_DAG_ID,
    _UNREADABLE_FILE_LIMIT,
    _DELETED_ROWS_REMEMBERED,
    _UNTRUSTED_SCAN_LIMIT,
    _UntrustedScan,
    _collect_module_constants,
    _extract_dag_id_from_decorators,
    _is_rmq_trigger_call,
    _parse_rmq_trigger_decorator,
    _read_settings,
)
from airflow_provider_rmq.watcher.models import RMQSubscription


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
# _collect_module_constants
# ---------------------------------------------------------------------------


def _constants(src: str) -> dict:
    """Parse a module-level source snippet and return its constant map."""
    tree = ast.parse(src)
    return _collect_module_constants(tree)[0]


class TestCollectModuleConstantsBaseCases:
    def test_single_string_constant_resolves_with_correct_lineno(self):
        result = _constants("DAG_ID = 'my_dag'\n")
        assert result == {"DAG_ID": ("my_dag", 1)}

    def test_single_string_constant_lineno_matches_assignment_line(self):
        result = _constants("\n\nDAG_ID = 'my_dag'\n")
        assert result["DAG_ID"] == ("my_dag", 3)

    def test_annotated_assignment_resolves(self):
        result = _constants("DAG_ID: str = 'my_dag'\n")
        assert result == {"DAG_ID": ("my_dag", 1)}

    def test_non_string_constant_ignored(self):
        result = _constants("DAG_ID = 123\n")
        assert result == {}

    def test_non_literal_rhs_ignored(self):
        result = _constants("DAG_ID = some_func()\n")
        assert result == {}


class TestCollectModuleConstantsReassignment:
    """All of these forms poison the constant → empty map."""

    def test_re_literal_assignment(self):
        result = _constants("DAG_ID = 'first'\nDAG_ID = 'second'\n")
        assert result == {}

    def test_aug_assign(self):
        result = _constants("DAG_ID = 'first'\nDAG_ID += '_daily'\n")
        assert result == {}

    def test_tuple_unpacking(self):
        result = _constants("DAG_ID, Q = 'real', 'q'\n")
        assert result == {}

    def test_chained_assignment(self):
        result = _constants("DAG_ID = OTHER = 'real'\n")
        assert result == {}

    def test_re_import(self):
        result = _constants("DAG_ID = 'real'\nfrom settings import DAG_ID\n")
        assert result == {}

    def test_import_as(self):
        result = _constants("DAG_ID = 'real'\nimport x as DAG_ID\n")
        assert result == {}

    def test_def_with_same_name(self):
        result = _constants("DAG_ID = 'real'\ndef DAG_ID(): pass\n")
        assert result == {}

    def test_class_with_same_name(self):
        result = _constants("DAG_ID = 'real'\nclass DAG_ID: pass\n")
        assert result == {}

    def test_del(self):
        result = _constants("DAG_ID = 'real'\ndel DAG_ID\n")
        assert result == {}

    def test_top_level_for_target(self):
        result = _constants("DAG_ID = 'real'\nfor DAG_ID in range(3): pass\n")
        assert result == {}

    def test_with_as(self):
        result = _constants("DAG_ID = 'real'\nwith open('x') as DAG_ID: pass\n")
        assert result == {}

    def test_conditional_reassignment_inside_if(self):
        result = _constants("DAG_ID = 'old'\nif True:\n    DAG_ID = 'new'\n")
        assert result == {}


class TestCollectModuleConstantsExoticBindingForms:
    """Module-scope binding forms that earlier, less complete enumerations
    of binding forms missed — all must poison the constant."""

    @pytest.mark.parametrize(
        "src",
        [
            "DAG_ID = 'real'\ntry:\n    pass\nexcept Exception as DAG_ID:\n    pass\n",
            "DAG_ID = 'real'\ndef f():\n    global DAG_ID\n    DAG_ID = 'local'\n",
            "DAG_ID = 'real'\nif (DAG_ID := 'walrus'):\n    pass\n",
            "DAG_ID = 'real'\nmatch 1:\n    case DAG_ID:\n        pass\n",
            "DAG_ID = 'real'\nmatch [1, 2]:\n    case [*DAG_ID]:\n        pass\n",
            "DAG_ID = 'real'\nmatch {'k': 1}:\n    case {'k': _, **DAG_ID}:\n        pass\n",
            "DAG_ID = 'real'\nif True:\n    for DAG_ID in range(3):\n        pass\n",
        ],
        ids=[
            "except_as",
            "global_plus_function_assign",
            "walrus_in_if",
            "match_as",
            "match_star",
            "match_mapping_rest",
            "nested_for_inside_if",
        ],
    )
    def test_exotic_binding_forms_poison_constant(self, src):
        assert _constants(src) == {}


class TestCollectModuleConstantsWildcardImport:
    def test_wildcard_import_disables_resolution_entirely(self):
        result = _constants("DAG_ID = 'local'\nfrom settings import *\n")
        assert result == {}

    def test_wildcard_import_disables_unrelated_constant_too(self):
        # With only one constant name in the file, "DAG_ID absent" is
        # ambiguous between "wildcard poisons file-wide" and "wildcard only
        # poisons names that plausibly collide with its exports". A second,
        # entirely unrelated constant name proves it's the former: the
        # wildcard import disables resolution for the WHOLE file
        # unconditionally, not just for names that happen to collide.
        result = _constants(
            "DAG_ID = 'local'\nUNRELATED_NAME = 'other'\nfrom settings import *\n"
        )
        assert result == {}


class TestCollectModuleConstantsWalrusInNestedScope:
    """Cases that only _outer_parts (traversing the enclosing-scope parts of
    a scope-boundary node) catches — a naive scope-pruned walk that skipped
    these entirely would miss the walrus poisoning."""

    def test_walrus_inside_comprehension(self):
        result = _constants("DAG_ID = 'real'\nxs = [(DAG_ID := i) for i in range(3)]\n")
        assert result == {}

    def test_walrus_in_parameter_default(self):
        result = _constants(
            "DAG_ID = 'real'\ndef g(x=(DAG_ID := 'other')): pass\n"
        )
        assert result == {}

    def test_walrus_in_decorator_expression(self):
        result = _constants(
            "DAG_ID = 'real'\n@deco(DAG_ID := 'n')\ndef f(): pass\n"
        )
        assert result == {}


class TestCollectModuleConstantsForeignScopeDoesNotPoison:
    """Binding a same-named identifier in a FOREIGN (nested) scope must NOT
    poison the module-level constant — this is the payoff of the
    scope-aware walk over a scope-blind ast.walk."""

    def test_same_named_function_parameter(self):
        result = _constants("DAG_NAME = 'real'\ndef process(DAG_NAME): pass\n")
        assert result == {"DAG_NAME": ("real", 1)}

    def test_class_body_attribute(self):
        result = _constants("DAG_NAME = 'real'\nclass C:\n    DAG_NAME = 'b'\n")
        assert result == {"DAG_NAME": ("real", 1)}

    def test_local_variable_inside_function(self):
        result = _constants("DAG_NAME = 'real'\ndef f():\n    DAG_NAME = 'loc'\n")
        assert result == {"DAG_NAME": ("real", 1)}

    def test_comprehension_target(self):
        result = _constants("DAG_NAME = 'real'\nys = [DAG_NAME for DAG_NAME in range(3)]\n")
        assert result == {"DAG_NAME": ("real", 1)}

    def test_lambda_parameter(self):
        result = _constants("DAG_NAME = 'real'\nf = lambda DAG_NAME: DAG_NAME\n")
        assert result == {"DAG_NAME": ("real", 1)}


class TestCollectModuleConstantsDonstroyRegression:
    def test_module_constant_used_as_parameter_default_value_resolves(self):
        # Matches the real donstroy_pipeline_spark_all.py shape: the module
        # constant is referenced (Name/Load, not a binding) as a parameter
        # default value elsewhere in the file — it must still resolve.
        src = (
            "DAG_NAME = 'donstroy_pipeline_spark_all'\n"
            "def create_cluster(x, dag_name: str = DAG_NAME): pass\n"
        )
        result = _constants(src)
        assert result == {"DAG_NAME": ("donstroy_pipeline_spark_all", 1)}


class TestCollectModuleConstantsPep695:
    # PEP 695 `type X = ...` binds the alias name via an ordinary ast.Name
    # in Store context, already covered by the first poison branch — no
    # separate handling needed. Raw 3.12 syntax cannot appear directly in
    # this file's body since CI also runs 3.10/3.11 (SyntaxError at parse
    # time), so the snippet is parsed from a string instead.
    @pytest.mark.skipif(sys.version_info < (3, 12), reason="PEP 695 `type` statement requires 3.12+")
    def test_pep695_type_alias_with_same_name_poisons_constant(self):
        src = "DAG_ID = 'real'\ntype DAG_ID = int\n"
        result = _constants(src)
        assert result == {}


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

    def test_no_dag_decorator_returns_unresolved(self):
        decs = _decorators("@some_other_decorator\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is _UNRESOLVED_DAG_ID

    def test_non_literal_dag_id_without_constant_returns_unresolved(self):
        decs = _decorators("@dag(dag_id=VARIABLE)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is _UNRESOLVED_DAG_ID

    def test_non_string_literal_dag_id_returns_unresolved(self):
        decs = _decorators("@dag(dag_id=123)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is _UNRESOLVED_DAG_ID

    def test_async_function_with_explicit_dag_id(self):
        src = "@dag(dag_id='async_dag')\nasync def f(): pass"
        decs = ast.parse(src).body[0].decorator_list
        assert _extract_dag_id_from_decorators(decs) == "async_dag"

    def test_empty_decorator_list_returns_unresolved(self):
        assert _extract_dag_id_from_decorators([]) is _UNRESOLVED_DAG_ID


class TestExtractDagIdConstants:
    def test_variable_dag_id_resolved_via_constants(self):
        decs = _decorators("@dag(dag_id=DAG_ID)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs, {"DAG_ID": "real_id"}) == "real_id"

    def test_variable_dag_id_not_in_constants_returns_unresolved(self):
        decs = _decorators("@dag(dag_id=DAG_ID)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs, {"OTHER": "x"}) is _UNRESOLVED_DAG_ID

    def test_empty_string_constant_returns_none(self):
        # Regression: DAG_ID = "" (a str, so it IS collected into `constants`
        # by _collect_module_constants) referenced via dag_id=DAG_ID must
        # fall back to the function name (None), not be returned verbatim as
        # "" — matching the falsy rule already applied to the literal branch.
        decs = _decorators("@dag(dag_id=DAG_ID)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs, {"DAG_ID": ""}) is None

    @pytest.mark.parametrize(
        "literal_src",
        ["''", "None", "False", "0", "0.0", "[]", "{}", "()"],
    )
    def test_falsy_literal_dag_id_returns_none(self, literal_src):
        decs = _decorators(f"@dag(dag_id={literal_src})\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is None


class TestExtractDagIdPositional:
    def test_positional_string_literal_dag_id(self):
        decs = _decorators("@dag('my_dag')\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) == "my_dag"

    def test_positional_variable_dag_id_resolved_via_constants(self):
        decs = _decorators("@dag(DAG_NAME)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs, {"DAG_NAME": "resolved"}) == "resolved"

    def test_positional_unresolvable_dag_id_returns_unresolved(self):
        decs = _decorators("@dag(SOME_VAR)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is _UNRESOLVED_DAG_ID

    def test_positional_empty_string_dag_id_returns_none(self):
        decs = _decorators("@dag('')\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is None

    def test_positional_truthy_non_string_literal_returns_unresolved(self):
        decs = _decorators("@dag(123)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is _UNRESOLVED_DAG_ID

    def test_positional_and_keyword_dag_id_conflict_returns_unresolved(self):
        # @dag("a", dag_id="b") is invalid Python at call time (TypeError:
        # multiple values for argument 'dag_id') — must not resolve to
        # either value.
        decs = _decorators("@dag('a', dag_id='b')\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is _UNRESOLVED_DAG_ID


class TestExtractDagIdUnpacking:
    def test_dict_unpacking_returns_unresolved(self):
        decs = _decorators('@dag(**{"dag_id": "real"})\ndef f(): pass')
        assert _extract_dag_id_from_decorators(decs) is _UNRESOLVED_DAG_ID

    def test_name_unpacking_returns_unresolved(self):
        decs = _decorators("@dag(**DAG_KWARGS)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is _UNRESOLVED_DAG_ID

    def test_star_args_returns_unresolved(self):
        decs = _decorators("@dag(*ARGS)\ndef f(): pass")
        assert _extract_dag_id_from_decorators(decs) is _UNRESOLVED_DAG_ID


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
    def test_on_starting_with_scheduler_starts_thread(self):
        listener = RMQWatcherListener()
        with patch.object(listener, "_start") as mock_start:
            listener.on_starting(SchedulerJobRunner())
        mock_start.assert_called_once()

    def test_on_starting_with_webserver_ignores(self):
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
        kept = "/dags/dag1.py"
        deleted = "/dags/dag2.py"

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=[kept, deleted]), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=[]):
            listener._scan_subscriptions()

        assert deleted in listener._last_mtimes
        listener._read_failures[deleted] = 1

        # Second scan: one of the two files is gone, the other is still there
        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=[kept]), \
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

        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=files), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime", return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file",
                          return_value=[{"dag_id": "d", "queue_name": "q"}]):
            listener._scan_subscriptions()

        def fake_extract(path):
            return None if path == file else [{"dag_id": "h", "queue_name": "hq"}]

        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=files), \
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

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=[_HELD_FILE, _HEALTHY_FILE]), \
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
                             logger="airflow_provider_rmq.watcher.listener"):
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
                             logger="airflow_provider_rmq.watcher.listener"):
            listener._extract_subscriptions_from_file(str(dag_file))

        assert any("Failed to parse DAG file" in r.getMessage() for r in caplog.records), \
            [r.getMessage() for r in caplog.records]

    def test_undecodable_content_answers_none(self, tmp_path):
        dag_file = tmp_path / "latin1.py"
        dag_file.write_bytes(b"# \xff\xfe not utf-8\n")
        listener = RMQWatcherListener()

        assert listener._extract_subscriptions_from_file(str(dag_file)) is None

    def test_undecodable_content_is_logged(self, tmp_path, caplog):
        dag_file = tmp_path / "latin1.py"
        dag_file.write_bytes(b"# \xff\xfe not utf-8\n")
        listener = RMQWatcherListener()

        with caplog.at_level(logging.WARNING,
                             logger="airflow_provider_rmq.watcher.listener"):
            listener._extract_subscriptions_from_file(str(dag_file))

        assert any("Failed to decode DAG file" in r.getMessage() for r in caplog.records), \
            [r.getMessage() for r in caplog.records]

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

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=[_HEALTHY_FILE]), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0):
            listener._scan_subscriptions()

        assert _HELD_FILE not in listener._cached_subs
        assert _HELD_FILE not in listener._read_failures


# ---------------------------------------------------------------------------
# A listing in which no file could be read is not a statement about the code
# ---------------------------------------------------------------------------

class TestEveryFileUnreadable:
    def _listener(self, files=(_HELD_FILE, _HEALTHY_FILE)):
        """A listener whose cache holds one parse of every file in ``files``."""
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        listener._files = list(files)
        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=list(files)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file",
                          return_value=list(_HELD_SUBS)):
            listener._scan_subscriptions()
        return listener

    def _scan_all_stale(self, listener):
        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=list(listener._files)), \
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
        assert self._scan_all_stale(listener) == _HELD_SUBS * len(listener._files)
        assert self._scan_all_stale(listener) == []

    def test_one_file_that_still_reads_makes_the_listing_trustworthy(self):
        listener = self._listener()

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=list(listener._files)), \
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
                             logger="airflow_provider_rmq.watcher.listener"):
            for _ in range(5):
                assert self._scan_all_stale(listener) == []

        assert [r.getMessage() for r in caplog.records] == []
        assert listener._untrusted_scan_streak == 0

    def _scan_all_unparseable(self, listener):
        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=list(listener._files)), \
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

        assert self._scan_all_unparseable(listener) == _HELD_SUBS * len(listener._files)
        assert self._scan_all_unparseable(listener) == []

    def test_a_healthy_scan_resets_the_streak(self):
        listener = self._listener()
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_all_stale(listener)

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=list(listener._files)), \
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

        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=files), \
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
        """The share is measured against the last trusted scan, not against the listing."""
        listener = self._listener()
        grown = list(self._FILES) + [f"/dags/new{n}.py" for n in range(10)]

        result = self._scan(listener, files=grown, mtime=2000.0)

        assert len(result) == len(grown)
        assert listener._untrusted_scan_streak == 0


# ---------------------------------------------------------------------------
# An empty glob is distrusted for a bounded number of cycles
# ---------------------------------------------------------------------------

class TestEmptyScanDistrust:
    def _warm_listener(self, files=("/dags/dag1.py",)):
        """A listener whose cache holds one parse of every file in ``files``."""
        listener = RMQWatcherListener()
        listener._get_dags_folder = MagicMock(return_value="/dags")
        subs = [{"dag_id": "d", "queue_name": "q"}]
        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=list(files)), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0), \
             patch.object(listener, "_extract_subscriptions_from_file", return_value=subs):
            listener._scan_subscriptions()
        return listener

    def _scan_empty(self, listener):
        with patch("airflow_provider_rmq.watcher.listener.glob.glob", return_value=[]):
            return listener._scan_subscriptions()

    def test_empty_glob_between_two_full_ones_is_untrusted(self):
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

    def test_a_non_empty_glob_resets_the_streak(self):
        listener = self._warm_listener()

        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)
        assert listener._untrusted_scan_streak == _UNTRUSTED_SCAN_LIMIT

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=["/dags/dag1.py"]), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   return_value=1000.0):
            listener._scan_subscriptions()

        assert listener._untrusted_scan_streak == 0
        # The whole grace period is available again.
        for _ in range(_UNTRUSTED_SCAN_LIMIT):
            with pytest.raises(_UntrustedScan):
                self._scan_empty(listener)

    def test_empty_glob_on_a_cold_cache_is_distrusted_too(self):
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

    def test_an_io_failure_that_emptied_the_mtimes_does_not_buy_trust(self):
        """The distrust cannot be keyed on a dict a failing filesystem erases.

        What it is keyed on is the count of the last scan the cycle trusted, which a
        stat that never answers cannot raise — the run of failures below leaves the
        remembered mtimes empty, and the file reading again is what ends that run and
        gives the emptiness after it the whole budget once more.
        """
        listener = self._warm_listener()
        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=["/dags/dag1.py"]), \
             patch("airflow_provider_rmq.watcher.listener.os.path.getmtime",
                   side_effect=OSError("stale handle")):
            for _ in range(_UNTRUSTED_SCAN_LIMIT + _UNREADABLE_FILE_LIMIT + 1):
                try:
                    listener._scan_subscriptions()
                except _UntrustedScan:
                    pass
        assert listener._last_mtimes == {}

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=["/dags/dag1.py"]), \
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
                             logger="airflow_provider_rmq.watcher.listener"):
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

        with patch("airflow_provider_rmq.watcher.listener.glob.glob",
                   return_value=["/dags/dag1.py"]), \
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
                             logger="airflow_provider_rmq.watcher.listener"):
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
        # An honestly empty directory is the truth about the code and must reach glob.
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

    def test_empty_setting_keeps_glob_from_running(self):
        # glob.glob("/**/*.py", recursive=True) is a walk of the whole filesystem
        # from the root inside a worker of the cycle pool.
        listener = RMQWatcherListener()

        with patch("airflow.settings.DAGS_FOLDER", ""), \
             patch("airflow_provider_rmq.watcher.listener.glob.glob") as mock_glob, \
             pytest.raises(_UntrustedScan):
            listener._scan_subscriptions()

        mock_glob.assert_not_called()

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
    def test_unlistable_directory_raises_untrusted_scan(self, tmp_path):
        # os.path.isdir says True for a directory with no read permission and glob
        # returns [] for it, so the explicit listability check is the only thing
        # telling it apart from an honestly empty folder.
        folder = tmp_path / "dags"
        folder.mkdir()
        (folder / "dag1.py").write_text("")
        folder.chmod(0o111)
        listener = RMQWatcherListener()

        try:
            assert os.path.isdir(str(folder))
            assert glob.glob(os.path.join(str(folder), "**", "*.py"), recursive=True) == []
            with patch("airflow.settings.DAGS_FOLDER", str(folder)):
                # The distrust has no limit: an unreadable folder never becomes truth.
                for _ in range(5):
                    with pytest.raises(_UntrustedScan, match="cannot be listed"):
                        listener._get_dags_folder()
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

    def test_two_exchange_decorators_on_same_function_second_skipped(self, tmp_path, caplog):
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

        assert len(result) == 1
        # ast.walk visits decorator_list in source order — first decorator parsed wins
        assert result[0]["exchange"] == "exchange.one"
        assert any("double_exchange_dag" in r.message for r in caplog.records)

    def test_unexpected_exception_during_extraction_answers_none_not_raises(self, tmp_path):
        # Regression: if _parse_rmq_trigger_decorator (or build_subscriptions)
        # ever lets an unexpected exception type leak past its own
        # except ValueError, _extract_subscriptions_from_file must still not
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
            "airflow_provider_rmq.watcher.listener._parse_rmq_trigger_decorator",
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
            "airflow_provider_rmq.watcher.listener._parse_rmq_trigger_decorator",
            side_effect=TypeError("boom"),
        ):
            result = listener._extract_subscriptions_from_file(str(dag_file))
        assert result is None
        assert any(str(dag_file) in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# _extract_subscriptions_from_file — unresolved dag_id contract (Task 3)
# ---------------------------------------------------------------------------


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
        assert any("data_proc" in r.message for r in caplog.records)

    def test_unresolvable_dag_id_with_rmq_trigger_skips_and_warns(self, tmp_path, caplog):
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
        assert any(
            r.levelname == "WARNING"
            and str(dag_file) in r.message
            and "unresolvable_dag" in r.message
            and "UI" in r.message
            for r in caplog.records
        )

    def test_kwargs_unpacking_skips_and_warns(self, tmp_path, caplog):
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
        assert any(
            r.levelname == "WARNING"
            and str(dag_file) in r.message
            and "kwargs_dag" in r.message
            and "UI" in r.message
            for r in caplog.records
        )

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
        assert any("early_dag" in r.message for r in caplog.records)

    def test_unresolvable_function_does_not_drop_other_subscriptions_in_same_file(
        self, tmp_path, caplog
    ):
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
        assert len(result) == 1
        assert result[0]["dag_id"] == "resolved_id"
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert len(warnings) == 1
        assert "unresolvable_dag" in warnings[0].message

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

    def test_aliased_dag_decorator_not_recognized_skips_and_warns(self, tmp_path, caplog):
        # `dag` imported under an alias is not matched by the `is_dag`
        # predicate (Name(id="dag")/Attribute(attr="dag") only) — no
        # recognized @dag(...) call exists among the decorators at all, so
        # this must yield _UNRESOLVED_DAG_ID (skip + warn), NOT silent
        # registration under the function name.
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
        assert any("aliased_function" in r.message for r in caplog.records)


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

    def test_one_miss_on_active_dag_keeps_the_row_and_counts_it(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="old_dag")
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"old_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert existing.absent_cycles == 1

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
        assert existing.absent_cycles == _ABSENT_GRACE_CYCLES

    def test_enabled_sub_of_inactive_dag_survives_the_first_miss(self):
        """A broken DAG file drops the key and is_active at once — one shared miss must survive."""
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="gone_dag", enabled=True, absent_cycles=None)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert existing.absent_cycles == 1

    def test_enabled_sub_of_inactive_dag_is_deleted_on_the_second_miss(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="gone_dag", enabled=True, absent_cycles=1)
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
            assert existing.absent_cycles == miss

        existing = _existing_sub(
            dag_id="gone_dag", enabled=False, absent_cycles=_ABSENT_GRACE_CYCLES
        )
        ctx, session = _make_session_ctx(existing_subs=[existing])
        with _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])
        _delete_mock(session).assert_called_once()

    def test_failure_to_read_active_dag_ids_does_not_speed_deletion_up(self):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="gone_dag", enabled=True, absent_cycles=1)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, RuntimeError("db is down")):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert existing.absent_cycles == 2

    def test_paused_dag_counts_as_active_and_keeps_the_grace_period(self):
        """get_active_dag_ids reports a paused DAG as active (ADR 0006), so no shortcut."""
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="paused_dag", enabled=True, absent_cycles=1)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"paused_dag"}):
            listener._sync_to_db([])

        _delete_mock(session).assert_not_called()
        assert existing.absent_cycles == 2

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
        existing = _existing_sub(dag_id="gone_dag", enabled=True, absent_cycles=1)
        ctx, session = _make_session_ctx(existing_subs=[existing])

        with _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])

        clause = session.query.return_value.filter_by.return_value.filter.call_args.args[0]
        assert clause.right.value == 1

    def test_a_counted_miss_is_logged_as_a_warning(self, caplog):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="old_dag", queue_name="q")
        ctx, _ = _make_session_ctx(existing_subs=[existing])

        with caplog.at_level(logging.WARNING,
                             logger="airflow_provider_rmq.watcher.listener"), \
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
                             logger="airflow_provider_rmq.watcher.listener"), \
                _sync_env(ctx, {"old_dag"}):
            listener._sync_to_db([])

        assert [r.getMessage() for r in caplog.records if r.levelno == logging.INFO and
                "trusted scans in a row — deleting" in r.getMessage()]

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
                             logger="airflow_provider_rmq.watcher.listener"), \
                _sync_env(ctx, {"d"}):
            listener._sync_to_db(scanned)

        assert [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING and
                "d/q" in r.getMessage() and "deleted one was disabled" in r.getMessage()]

    def test_a_key_that_never_went_is_not_reported_as_recreated(self, caplog):
        listener = RMQWatcherListener()
        scanned = [{"dag_id": "d", "queue_name": "q", "conn_id": "rmq_default",
                    "filter_data": {}}]
        ctx, _ = _make_session_ctx(existing_subs=[])

        with caplog.at_level(logging.WARNING,
                             logger="airflow_provider_rmq.watcher.listener"), \
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
        existing = _existing_sub(dag_id="gone_dag", queue_name="q", absent_cycles=1)
        ctx, _ = _make_session_ctx(existing_subs=[existing])

        with caplog.at_level(logging.INFO,
                             logger="airflow_provider_rmq.watcher.listener"), \
                _sync_env(ctx, {"other_dag"}):
            listener._sync_to_db([])

        assert [r.getMessage() for r in caplog.records if r.levelno == logging.INFO and
                "is no longer active — deleting" in r.getMessage()]

    def test_failure_to_read_active_dag_ids_is_logged(self, caplog):
        listener = RMQWatcherListener()
        existing = _existing_sub(dag_id="gone_dag", enabled=True, absent_cycles=1)
        ctx, _ = _make_session_ctx(existing_subs=[existing])

        with caplog.at_level(logging.WARNING,
                             logger="airflow_provider_rmq.watcher.listener"), \
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

    def test_the_active_dag_ids_are_read_at_most_once_per_synchronisation(self):
        listener = RMQWatcherListener()
        existing = [
            _existing_sub(dag_id="a", queue_name="q", enabled=True, absent_cycles=1),
            _existing_sub(dag_id="b", queue_name="q", enabled=True, absent_cycles=1),
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
        existing = _existing_sub(dag_id="gone_dag", enabled=True, absent_cycles=1)
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
# _is_rmq_trigger_call
# ---------------------------------------------------------------------------


def _expr(src: str) -> ast.expr:
    """Parse a single expression string and return its AST node."""
    return ast.parse(src, mode="eval").body


class TestIsRmqTriggerCall:
    def test_bare_call_returns_true(self):
        assert _is_rmq_trigger_call(_expr("rmq_trigger(queue='orders')")) is True

    def test_attribute_access_call_returns_true(self):
        assert _is_rmq_trigger_call(_expr("decorators.rmq_trigger(queue='orders')")) is True

    def test_unrelated_call_returns_false(self):
        assert _is_rmq_trigger_call(_expr("some_other_call(queue='orders')")) is False

    def test_non_call_node_returns_false(self):
        # A bare `@some_name` decorator is an ast.Name, not an ast.Call.
        assert _is_rmq_trigger_call(_expr("some_name")) is False


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
        with caplog.at_level(logging.WARNING):
            _parse_decorator("rmq_trigger(queue='q', cooldown=-1)", dag_id="neg_dag")
        assert any("neg_dag" in r.message for r in caplog.records)

    def test_string_cooldown_skipped_returns_empty(self):
        # A typo'd literal like cooldown="abc" passes ast.literal_eval (it's a
        # valid string literal) but is semantically invalid — must be skipped
        # gracefully, not raise an uncaught TypeError out of this function.
        result = _parse_decorator("rmq_trigger(queue='q', cooldown='abc')")
        assert result == []

    def test_string_cooldown_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            _parse_decorator("rmq_trigger(queue='q', cooldown='abc')", dag_id="str_cooldown_dag")
        assert any("str_cooldown_dag" in r.message for r in caplog.records)

    def test_list_cooldown_skipped_returns_empty(self):
        result = _parse_decorator("rmq_trigger(queue='q', cooldown=[1, 2])")
        assert result == []

    def test_float_cooldown_skipped_returns_empty(self):
        result = _parse_decorator("rmq_trigger(queue='q', cooldown=1.5)")
        assert result == []

    def test_bool_cooldown_skipped_returns_empty(self):
        result = _parse_decorator("rmq_trigger(queue='q', cooldown=True)")
        assert result == []

    def test_queues_with_non_string_items_skipped_returns_empty(self):
        result = _parse_decorator("rmq_trigger(queues=[1, 2, 3])")
        assert result == []

    def test_queues_with_mixed_string_and_int_items_skipped_returns_empty(self):
        result = _parse_decorator("rmq_trigger(queues=['a', 2])")
        assert result == []

    def test_queues_with_non_string_items_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            _parse_decorator("rmq_trigger(queues=[1, 2, 3])", dag_id="bad_queues_dag")
        assert any("bad_queues_dag" in r.message for r in caplog.records)


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

    def test_exchange_empty_routing_key_status_list_skipped(self):
        # routing_key_status=[] collapses the routing_key_ids cross-product to
        # an empty set — build_subscriptions raises ValueError, which must be
        # caught and turned into a graceful skip, not propagate.
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'], "
            "routing_key_status=[])"
        )
        assert result == []

    def test_non_string_exchange_skipped_returns_empty(self):
        # exchange=123 is a valid ast.literal_eval() result (an int literal)
        # but semantically invalid — must be turned into a graceful WARNING+
        # skip by build_subscriptions raising ValueError, not propagate as an
        # uncaught AttributeError from exchange.startswith(...). An uncaught
        # exception here would crash the whole reconcile cycle, not just this
        # DAG (see _extract_subscriptions_from_file's broad except Exception).
        result = _parse_decorator("rmq_trigger(exchange=123, routing_keys=['a.b'])")
        assert result == []

    def test_non_string_exchange_logs_warning(self, caplog):
        with caplog.at_level(logging.WARNING):
            _parse_decorator(
                "rmq_trigger(exchange=123, routing_keys=['a.b'])", dag_id="bad_exchange_dag"
            )
        assert any("bad_exchange_dag" in r.message for r in caplog.records)

    def test_non_str_non_list_routing_key_status_skipped_returns_empty(self):
        # routing_key_status=123 must not reach list(routing_key_status) and
        # raise an uncaught TypeError.
        result = _parse_decorator(
            "rmq_trigger(exchange='ex', routing_key_ids=['abc'], routing_key_status=123)"
        )
        assert result == []

    def test_routing_keys_as_plain_string_skipped_returns_empty(self):
        # Forgetting list brackets (routing_keys="literal.string" instead of
        # routing_keys=["literal.string"]) must not silently expand into
        # one-character wildcard routing keys — build_subscriptions rejects
        # non-list routing_keys with ValueError, which is caught here.
        result = _parse_decorator(
            "rmq_trigger(exchange='ex', routing_keys='literal.string')"
        )
        assert result == []

    def test_routing_key_ids_as_plain_string_skipped_returns_empty(self):
        result = _parse_decorator(
            "rmq_trigger(exchange='jetstat.airflow', "
            "routing_key_ids='670f877702775c2de8325b1f')"
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
                        logger="airflow_provider_rmq.watcher.listener",
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
                        logger="airflow_provider_rmq.watcher.listener",
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

    `_sync_to_db` deletes every dag_file row whose key the scan did not return, so a
    result that says nothing about the code must not reach it — while consumption, the
    liveness check and the status writes keep running from the database rows.
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
            listener._scan_subscriptions = MagicMock(side_effect=_UntrustedScan("empty glob"))
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
            logging.ERROR, logger="airflow_provider_rmq.watcher.listener"
        ):
            await listener._run_cycle()

        messages = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
        assert any("the DAG folder cannot be listed" in m for m in messages)

    @pytest.mark.asyncio
    async def test_a_step_in_flight_scan_behaves_as_before(self, caplog):
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
                logging.WARNING, logger="airflow_provider_rmq.watcher.listener"
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
            logging.ERROR, logger="airflow_provider_rmq.watcher.listener"
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
            seen.append((listener._cycle_pool, listener._consumer_pool))
            if len(seen) == 1:
                raise asyncio.TimeoutError()
            listener._stop_event.set()

        with patch.object(listener, "_main", side_effect=fake_main), patch.object(
            asyncio.base_events.BaseEventLoop, "shutdown_default_executor"
        ) as shutdown:
            listener._run_loop()

        assert seen[0] == seen[1]
        shutdown.assert_not_called()

    def test_a_call_stuck_in_the_pool_does_not_block_the_next_loop(self):
        """A worker still occupied by a call the loop gave up on must not hold the
        watcher thread — the pool outlives the loop by design."""
        listener = RMQWatcherListener()
        listener._stop_event = _instant_stop_event()
        release = threading.Event()
        rounds = []

        async def fake_main():
            rounds.append(1)
            if len(rounds) == 1:
                await call_with_timeout(
                    asyncio.wrap_future(listener._cycle_pool.submit(release.wait, 5)),
                    0.05,
                )
            listener._stop_event.set()

        try:
            with patch.object(listener, "_main", side_effect=fake_main):
                listener._run_loop()
        finally:
            release.set()

        assert len(rounds) == 2

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

        assert manager_cls.call_args.kwargs["executor"] is listener._consumer_pool
        assert listener._consumer_pool is not listener._cycle_pool

    @pytest.mark.asyncio
    async def test_busy_consumer_pool_leaves_the_cycle_pool_free(self):
        listener = RMQWatcherListener()
        listener._consumer_pool = BoundedExecutor("test-consumer", 2)
        release = threading.Event()
        for _ in range(2):
            listener._consumer_pool.submit(release.wait, 5)
        try:
            assert await listener._cycle_pool.run(lambda: "cycle work") == "cycle work"
        finally:
            release.set()


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
            logging.WARNING, logger="airflow_provider_rmq.watcher.listener"
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
            logging.WARNING, logger="airflow_provider_rmq.watcher.listener"
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
            logging.WARNING, logger="airflow_provider_rmq.watcher.listener"
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
    async def test_before_stopping_wakes_the_loop_instead_of_waiting_out_the_interval(self):
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
            listener.before_stopping(MagicMock())
            await call_with_timeout(task, 5)

        assert time.monotonic() - started < 1.0
        assert listener._stop_event.is_set()
        manager.stop.assert_awaited()

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

    def test_before_stopping_survives_a_loop_that_is_already_closed(self):
        """A closed loop is left alone: the call itself is what the guard prevents.

        The ``RuntimeError`` swallowed a couple of lines below would keep the event
        untouched either way, so the observation is the call, not its effect.
        """
        listener = RMQWatcherListener()
        listener._stop_event = threading.Event()
        loop = asyncio.new_event_loop()
        loop.close()
        loop.call_soon_threadsafe = MagicMock()
        listener._waker = (loop, MagicMock())

        listener.before_stopping(MagicMock())

        assert listener._stop_event.is_set()
        loop.call_soon_threadsafe.assert_not_called()

    def test_before_stopping_survives_a_loop_closing_under_it(self):
        """``is_closed`` can still say False when the loop closes a moment later.

        The nudge is made and it raises; stopping must go through all the same.
        """
        listener = RMQWatcherListener()
        listener._stop_event = threading.Event()
        loop = MagicMock()
        loop.is_closed.return_value = False
        loop.call_soon_threadsafe.side_effect = RuntimeError("Event loop is closed")
        listener._waker = (loop, MagicMock())

        listener.before_stopping(MagicMock())

        loop.call_soon_threadsafe.assert_called_once()
        assert listener._stop_event.is_set()

    def test_before_stopping_joins_the_watcher_thread(self):
        listener = RMQWatcherListener()
        listener._stop_event = threading.Event()
        thread = MagicMock()
        thread.is_alive.return_value = True
        listener._thread = thread

        listener.before_stopping(MagicMock())

        thread.join.assert_called_once()
        assert thread.join.call_args.kwargs["timeout"] > 0


class TestLifecycleDiagnostics:
    def test_unrecognised_component_logs_why_the_watcher_is_not_started(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.listener"
        ), patch.object(listener, "_start") as start:
            listener.on_starting(GunicornWebServer())

        start.assert_not_called()
        messages = [record.getMessage() for record in caplog.records]
        assert len(messages) == 1, "the reason belongs in the existing record"
        assert "watcher not started" in messages[0]

    def test_scheduler_component_is_logged_without_a_reason(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.listener"
        ), patch.object(listener, "_start"):
            listener.on_starting(SchedulerJobRunner())

        assert "watcher not started" not in caplog.records[0].getMessage()

    def test_thread_start_is_logged_with_the_default_interval_and_budget(self, caplog):
        """The thread starts before the first Variable read, so the record has to say
        that the numbers in it are the built-in defaults."""
        listener = RMQWatcherListener()
        with caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.listener"
        ), patch("threading.Thread", return_value=MagicMock()):
            listener._start()

        messages = [record.getMessage() for record in caplog.records]
        started = [m for m in messages if "RMQ Watcher thread started" in m]
        assert started, messages
        assert re.search(r"default reconcile interval of 60s", started[0]), started[0]
        assert re.search(r"cycle budget of 300s", started[0]), started[0]
        assert RECONCILE_INTERVAL_VAR in started[0]
        assert CYCLE_TIMEOUT_VAR in started[0]

    @pytest.mark.asyncio
    async def test_an_override_is_logged_once_the_first_cycle_has_read_it(self, caplog):
        listener = RMQWatcherListener()
        with caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.listener"
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

    def test_a_thread_that_ignores_the_stop_signal_is_logged(self, caplog):
        """The scheduler's own shutdown must not stall behind the watcher, so the join
        gives up — and says so, or a thread left behind would be invisible."""
        listener = RMQWatcherListener()
        listener._stop_event = threading.Event()
        thread = MagicMock()
        thread.is_alive.return_value = True
        listener._thread = thread

        with caplog.at_level(
            logging.WARNING, logger="airflow_provider_rmq.watcher.listener"
        ):
            listener.before_stopping(MagicMock())

        thread.join.assert_called_once()
        assert any(
            "still running" in r.getMessage() for r in caplog.records
        ), [r.getMessage() for r in caplog.records]


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
            # Every listener builds two thread pools in its constructor, and the cases
            # below actually put work on them.
            listener._cycle_pool.shutdown()
            listener._consumer_pool.shutdown()


class TestScanMissEndToEnd:
    """The scenarios of the backlog item, driven through whole cycles.

    Only a real session shows what these are about: SQL does not call ``NULL`` and
    ``0`` the same thing, so the condition on the DELETE is right or wrong here and
    nowhere else, and ``enabled`` survives precisely because the row keeps its ``id``.
    """

    @pytest.mark.asyncio
    async def test_disabled_row_survives_an_empty_glob_between_two_full_scans(
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
    async def test_removed_decorator_on_an_inactive_dag_deletes_on_the_second_miss(
        self, e2e_listener, dagmodel_engine, tmp_path
    ):
        """An enabled row of a DAG Airflow no longer runs still gets one miss for free.

        Its consumer acks and drops every delivery, hence the hurry; one shared hiccup
        of the filesystem still has to be survivable, hence not the first miss.
        """
        dag_file = tmp_path / "d.py"
        _write_dag_file(dag_file, _DAG_WITH_TRIGGER, mtime=1000)
        await e2e_listener._run_cycle()
        _record_dag(dagmodel_engine, is_active=False)

        _write_dag_file(dag_file, _DAG_WITHOUT_TRIGGER, mtime=2000)
        await e2e_listener._run_cycle()

        row = _snapshot(dagmodel_engine)
        assert row is not None
        assert row["enabled"] is True
        assert row["absent_cycles"] == 1

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
        await e2e_listener._run_cycle()
        assert _snapshot(dagmodel_engine)["absent_cycles"] == 1

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
        await e2e_listener._run_cycle()
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
