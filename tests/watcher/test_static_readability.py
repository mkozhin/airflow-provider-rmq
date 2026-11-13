"""The decorator's check that the watcher's scan reads its call as the decorator does.

``_verify_static_readability`` is exercised on DAG files written to ``tmp_path``
and never imported: the call site (path and line) and the subscriptions the
decorator built are passed in directly.
"""

from __future__ import annotations

import glob
import os
import textwrap

import pytest

from airflow_provider_rmq.watcher.dag_file_scan import (
    RMQTriggerNotStaticError,
    _same_value,
    _verify_static_readability,
    _watcher_scans,
)
from airflow_provider_rmq.watcher.subscription_builder import build_subscriptions

_HEADER = (
    "from airflow.decorators import dag\n"
    "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
)


@pytest.fixture
def dags_folder(tmp_path):
    folder = tmp_path / "dags"
    folder.mkdir()
    return folder


def _write(folder, name: str, body: str, *, header: bool = True) -> str:
    path = folder / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text((_HEADER if header else "") + textwrap.dedent(body), encoding="utf-8")
    return str(path)


def _line_of(path: str, needle: str, nth: int = 0) -> int:
    """1-based number of the ``nth`` line of ``path`` containing ``needle``."""
    with open(path, encoding="utf-8-sig", errors="replace") as f:
        hits = [no for no, line in enumerate(f, start=1) if needle in line]
    return hits[nth]


def _verify(path: str, dag_id: str, dags_folder, *, lineno: int | None = None, **kwargs):
    """Run the check for a call that received ``kwargs`` on the DAG ``dag_id``."""
    _verify_static_readability(
        path,
        lineno if lineno is not None else _line_of(path, "@rmq_trigger"),
        dag_id,
        build_subscriptions(dag_id=dag_id, **kwargs),
        str(dags_folder),
    )


# ---------------------------------------------------------------------------
# _same_value
# ---------------------------------------------------------------------------


class TestSameValue:
    @pytest.mark.parametrize(
        ("a", "b"),
        [(1, 1.0), (1, True), (1.0, True), (0, False), ("1", 1), (None, 0), ([1], (1,))],
        ids=["int_float", "int_bool", "float_bool", "zero_false", "str_int",
             "none_zero", "list_tuple"],
    )
    def test_values_of_different_types_differ(self, a, b):
        assert not _same_value(a, b)
        assert not _same_value(b, a)

    @pytest.mark.parametrize("value", [1, 1.5, True, None, "q", [1, "a"], (1, 2), {1, 2}])
    def test_value_equals_itself(self, value):
        assert _same_value(value, value)

    def test_dict_keys_compare_with_their_type(self):
        assert not _same_value({1: "x"}, {"1": "x"})
        assert not _same_value({1: "x"}, {True: "x"})
        assert not _same_value({1: "x"}, {1.0: "x"})

    def test_dict_with_mixed_key_types_equals_itself(self):
        assert _same_value({1: "a", "b": "c"}, {"b": "c", 1: "a"})

    def test_dict_with_mixed_key_types_differs_on_a_value(self):
        assert not _same_value({1: "a", "b": "c"}, {1: "a", "b": "d"})

    def test_dicts_of_different_size_differ(self):
        assert not _same_value({"a": 1}, {"a": 1, "b": 2})

    def test_nested_containers_compare_element_by_element(self):
        assert _same_value({"a": [1, {"b": (2, "c")}]}, {"a": [1, {"b": (2, "c")}]})
        assert not _same_value({"a": [1, {"b": 2}]}, {"a": [1, {"b": True}]})
        assert not _same_value({"a": [1, {"b": 2}]}, {"a": [1, {"b": 2.0}]})
        assert not _same_value([[1, 2]], [[1, 2, 3]])
        assert not _same_value([1, 2], [2, 1])

    def test_sets_compare_members_with_their_type(self):
        assert _same_value({1, 2}, {2, 1})
        assert not _same_value({1}, {True})
        assert not _same_value({1, 2}, {1, 3})


# ---------------------------------------------------------------------------
# _watcher_scans
# ---------------------------------------------------------------------------


class TestWatcherScans:
    @pytest.mark.parametrize(
        "rel",
        ["a.py", "sub/b.py", "sub/deeper/c.py"],
    )
    def test_python_file_inside_the_folder_is_scanned(self, dags_folder, rel):
        assert _watcher_scans(str(dags_folder / rel), str(dags_folder))

    @pytest.mark.parametrize(
        "rel",
        [".x.py", "sub/.y.py", ".hidden/a.py", "sub/.hidden/b.py", "a.txt", "a.pyc",
         "sub/a.py.bak"],
    )
    def test_hidden_or_non_python_file_is_not_scanned(self, dags_folder, rel):
        assert not _watcher_scans(str(dags_folder / rel), str(dags_folder))

    def test_file_outside_the_folder_is_not_scanned(self, dags_folder, tmp_path):
        assert not _watcher_scans(str(tmp_path / "outside.py"), str(dags_folder))
        assert not _watcher_scans(str(tmp_path / "dags2" / "a.py"), str(dags_folder))
        assert not _watcher_scans(str(tmp_path / "dags.py"), str(dags_folder))

    def test_folder_with_trailing_separator_or_dots_in_its_own_path(self, tmp_path):
        folder = tmp_path / ".airflow" / "dags"
        assert _watcher_scans(str(folder / "a.py"), str(folder) + os.sep)
        assert _watcher_scans(str(folder / "sub" / ".." / "a.py"), str(folder))

    def test_agrees_with_the_watcher_glob(self, tmp_path, dags_folder):
        for rel in [
            "dags/a.py", "dags/sub/b.py", "dags/sub/deeper/c.py",
            "dags/.x.py", "dags/sub/.y.py", "dags/.hidden/a.py", "dags/sub/.hidden/b.py",
            "dags/a.txt", "dags/sub/notes.md",
            "outside.py", "dags2/a.py",
        ]:
            path = tmp_path / rel
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("")

        listed = set(glob.glob(os.path.join(str(dags_folder), "**", "*.py"), recursive=True))
        everything = {
            os.path.join(root, name)
            for root, _dirs, names in os.walk(tmp_path)
            for name in names
        }
        scanned = {p for p in everything if _watcher_scans(p, str(dags_folder))}

        assert scanned == listed
        assert len(listed) == 3


# ---------------------------------------------------------------------------
# _verify_static_readability: calls that pass
# ---------------------------------------------------------------------------


class TestReadableCallPasses:
    def test_literal_arguments(self, dags_folder):
        path = _write(dags_folder, "lit.py", """
            @rmq_trigger(queue="orders", conn_id="rmq_prod", filter_data={"n": 1}, cooldown=30)
            @dag(dag_id="lit", schedule=None)
            def lit():
                pass
        """)
        _verify(path, "lit", dags_folder, queue="orders", conn_id="rmq_prod",
                filter_data={"n": 1}, cooldown=30)

    def test_constants_of_the_file(self, dags_folder):
        path = _write(dags_folder, "const.py", """
            DAG_ID = "const"
            RMQ_CONN = "rmq_prod"
            Q1 = "a"
            PAID = "paid"
            OPTS = {"cooldown": 10, "ack_unmatched": True}

            @rmq_trigger(None, [Q1, "b"], RMQ_CONN, {"body.type": PAID}, **OPTS)
            @dag(dag_id=DAG_ID, schedule=None)
            def const():
                pass
        """)
        _verify(path, "const", dags_folder, queues=["a", "b"], conn_id="rmq_prod",
                filter_data={"body.type": "paid"}, cooldown=10, ack_unmatched=True)

    def test_file_starting_with_a_bom(self, dags_folder):
        path = dags_folder / "bom.py"
        path.write_bytes(b"\xef\xbb\xbf" + (_HEADER + textwrap.dedent("""
            @rmq_trigger(queue="q")
            @dag(dag_id="bom", schedule=None)
            def bom():
                pass
        """)).encode())
        _verify(str(path), "bom", dags_folder, queue="q")

    def test_file_in_a_subdirectory(self, dags_folder):
        path = _write(dags_folder, "team/sub.py", """
            @rmq_trigger(queue="q")
            @dag(dag_id="sub", schedule=None)
            def sub():
                pass
        """)
        _verify(path, "sub", dags_folder, queue="q")


# ---------------------------------------------------------------------------
# _verify_static_readability: finding the call
# ---------------------------------------------------------------------------


class TestFindingTheCall:
    _MULTILINE = """
        @rmq_trigger(
            queue="orders",
            conn_id="rmq_prod",
        )
        @dag(dag_id="multi", schedule=None)
        def multi():
            pass
    """

    @pytest.mark.parametrize("offset", [0, 1, 2, 3], ids=["open", "queue", "conn_id", "close"])
    def test_any_line_of_a_multiline_call(self, dags_folder, offset):
        path = _write(dags_folder, "multi.py", self._MULTILINE)
        lineno = _line_of(path, "@rmq_trigger(") + offset
        _verify(path, "multi", dags_folder, lineno=lineno, queue="orders", conn_id="rmq_prod")

    def test_line_outside_every_call_is_not_found(self, dags_folder):
        path = _write(dags_folder, "multi.py", self._MULTILINE)
        with pytest.raises(RMQTriggerNotStaticError, match="does not recognize"):
            _verify(path, "multi", dags_folder, lineno=_line_of(path, "def multi"),
                    queue="orders", conn_id="rmq_prod")

    def test_each_stacked_call_is_checked_against_its_own_line(self, dags_folder):
        path = _write(dags_folder, "stack.py", """
            @rmq_trigger(queue="first")
            @rmq_trigger(queue="second", cooldown=5)
            @dag(dag_id="stack", schedule=None)
            def stack():
                pass
        """)
        first = _line_of(path, "@rmq_trigger", 0)
        second = _line_of(path, "@rmq_trigger", 1)
        _verify(path, "stack", dags_folder, lineno=first, queue="first")
        _verify(path, "stack", dags_folder, lineno=second, queue="second", cooldown=5)
        with pytest.raises(RMQTriggerNotStaticError, match="queue_name"):
            _verify(path, "stack", dags_folder, lineno=second, queue="first")

    def test_nested_factory(self, dags_folder):
        path = _write(dags_folder, "nested.py", """
            def build():
                @rmq_trigger(queue="nested_q")
                @dag(dag_id="nested", schedule=None)
                def nested():
                    pass
                return nested()

            build()
        """)
        _verify(path, "nested", dags_folder, queue="nested_q")

    def test_dag_under_a_module_level_if(self, dags_folder):
        path = _write(dags_folder, "cond.py", """
            ENABLED = True

            if ENABLED:
                @rmq_trigger(queue="cond_q")
                @dag(dag_id="cond", schedule=None)
                def cond():
                    pass

                cond()
        """)
        _verify(path, "cond", dags_folder, queue="cond_q")


# ---------------------------------------------------------------------------
# _verify_static_readability: refusals
# ---------------------------------------------------------------------------

_SIMPLE = """
    @rmq_trigger(queue="q")
    @dag(dag_id="simple", schedule=None)
    def simple():
        pass
"""


class TestFileTheWatcherDoesNotScan:
    def test_file_outside_dags_folder(self, tmp_path, dags_folder):
        path = _write(tmp_path / "plugins", "factory.py", _SIMPLE)
        with pytest.raises(RMQTriggerNotStaticError, match="does not scan") as exc:
            _verify(path, "simple", dags_folder, queue="q")
        assert str(dags_folder) in str(exc.value)

    def test_file_in_a_hidden_directory(self, dags_folder):
        path = _write(dags_folder, ".hidden/simple.py", _SIMPLE)
        with pytest.raises(RMQTriggerNotStaticError, match="does not scan"):
            _verify(path, "simple", dags_folder, queue="q")

    def test_file_name_starting_with_a_dot(self, dags_folder):
        path = _write(dags_folder, ".simple.py", _SIMPLE)
        with pytest.raises(RMQTriggerNotStaticError, match="does not scan"):
            _verify(path, "simple", dags_folder, queue="q")

    def test_check_runs_before_the_file_is_read(self, tmp_path, dags_folder):
        missing = str(tmp_path / "elsewhere" / "missing.py")
        with pytest.raises(RMQTriggerNotStaticError, match="does not scan"):
            _verify(missing, "simple", dags_folder, lineno=1, queue="q")


class TestFileTheWatcherCannotRead:
    def test_file_that_does_not_open(self, dags_folder):
        missing = str(dags_folder / "missing.py")
        with pytest.raises(RMQTriggerNotStaticError, match="cannot read this file") as exc:
            _verify(missing, "simple", dags_folder, lineno=1, queue="q")
        assert "FileNotFoundError" in str(exc.value)

    def test_file_that_does_not_decode(self, dags_folder):
        path = dags_folder / "latin.py"
        path.write_bytes(_HEADER.encode() + b"X = '\xff'\n" + textwrap.dedent(_SIMPLE).encode())
        with pytest.raises(RMQTriggerNotStaticError, match="cannot read this file") as exc:
            _verify(str(path), "simple", dags_folder, queue="q")
        assert "SyntaxError" in str(exc.value)


class TestFileProblemRefusesTheCall:
    """Any problem the watcher refuses the file for stops the import with its list."""

    def test_unused_alias_elsewhere_in_the_file(self, dags_folder):
        path = _write(dags_folder, "alias.py", textwrap.dedent("""
            from airflow_provider_rmq.watcher.decorators import rmq_trigger as rt
        """) + textwrap.dedent(_SIMPLE))
        with pytest.raises(RMQTriggerNotStaticError, match="refuses all of its subscriptions") as exc:
            _verify(path, "simple", dags_folder, queue="q")
        assert "imported as 'rt'" in str(exc.value)

    def test_unreadable_call_in_a_factory_the_file_never_calls(self, dags_folder):
        path = _write(dags_folder, "factory.py", textwrap.dedent(_SIMPLE) + textwrap.dedent("""
            def unused():
                @rmq_trigger(queue=get_queue())
                @dag(dag_id="other", schedule=None)
                def other():
                    pass
                return other()
        """))
        with pytest.raises(RMQTriggerNotStaticError, match="refuses all of its subscriptions") as exc:
            _verify(path, "simple", dags_folder, queue="q")
        assert "dag_id='other'" in str(exc.value)
        assert "queue cannot be read" in str(exc.value)

    def test_unreadable_argument_of_this_call(self, dags_folder):
        path = _write(dags_folder, "call.py", """
            @rmq_trigger(queue="q", conn_id=get_conn())
            @dag(dag_id="call", schedule=None)
            def call():
                pass
        """)
        with pytest.raises(RMQTriggerNotStaticError, match="refuses all of its subscriptions") as exc:
            _verify(path, "call", dags_folder, queue="q", conn_id="rmq_prod")
        assert "conn_id cannot be read" in str(exc.value)

    def test_dag_id_that_cannot_be_resolved(self, dags_folder):
        path = _write(dags_folder, "fstr.py", """
            ENV = "prod"

            @rmq_trigger(queue="q")
            @dag(dag_id=f"{ENV}_dag", schedule=None)
            def fstr():
                pass
        """)
        with pytest.raises(RMQTriggerNotStaticError, match="cannot statically resolve dag_id"):
            _verify(path, "prod_dag", dags_folder, queue="q")


class TestCallNotFound:
    def test_rmq_trigger_re_exported_under_another_name(self, dags_folder):
        path = _write(dags_folder, "reexport.py", """
            from helper import trig

            @trig(queue="q")
            @dag(dag_id="reexport", schedule=None)
            def reexport():
                pass
        """, header=False)
        with pytest.raises(RMQTriggerNotStaticError, match="does not recognize") as exc:
            _verify(path, "reexport", dags_folder, lineno=_line_of(path, "@trig"), queue="q")
        assert "@rmq_trigger(...)" in str(exc.value)
        assert "above @dag(...)" in str(exc.value)


class TestDagIdDiffers:
    def test_static_dag_id_is_not_the_dag_decorated(self, dags_folder):
        path = _write(dags_folder, "simple.py", _SIMPLE)
        with pytest.raises(RMQTriggerNotStaticError, match="dag_id='simple'") as exc:
            _verify(path, "renamed", dags_folder, queue="q")
        assert "'renamed'" in str(exc.value)


class TestValuesDiffer:
    def test_different_conn_id(self, dags_folder):
        path = _write(dags_folder, "conn.py", """
            RMQ_CONN = "rmq_prod"

            @rmq_trigger(queue="q", conn_id=RMQ_CONN)
            @dag(dag_id="conn", schedule=None)
            def conn():
                pass
        """)
        with pytest.raises(RMQTriggerNotStaticError, match="other arguments") as exc:
            _verify(path, "conn", dags_folder, queue="q", conn_id="rmq_other")
        message = str(exc.value)
        assert "conn_id" in message
        assert "'rmq_prod'" in message
        assert "'rmq_other'" in message
        assert "queue_name" not in message

    def test_int_against_bool_in_filter_data(self, dags_folder):
        path = _write(dags_folder, "typed.py", """
            @rmq_trigger(queue="q", filter_data={"body.flag": 1})
            @dag(dag_id="typed", schedule=None)
            def typed():
                pass
        """)
        with pytest.raises(RMQTriggerNotStaticError, match="filter_data"):
            _verify(path, "typed", dags_folder, queue="q", filter_data={"body.flag": True})

    def test_different_number_of_subscriptions(self, dags_folder):
        path = _write(dags_folder, "many.py", """
            QUEUES = ["a", "b"]

            @rmq_trigger(queues=QUEUES)
            @dag(dag_id="many", schedule=None)
            def many():
                pass
        """)
        with pytest.raises(RMQTriggerNotStaticError, match="builds 2 subscription"):
            _verify(path, "many", dags_folder, queues=["a", "b", "c"])

    def test_row_keys_the_watcher_adds_are_not_compared(self, dags_folder):
        path = _write(dags_folder, "cool.py", """
            @rmq_trigger(queue="q", cooldown=30)
            @dag(dag_id="cool", schedule=None)
            def cool():
                pass
        """)
        runtime = build_subscriptions(dag_id="cool", queue="q", cooldown=30)
        for sub in runtime:
            sub["dag_id"] = "cool"
            sub["group_key"] = "something else"
        _verify_static_readability(
            path, _line_of(path, "@rmq_trigger"), "cool", runtime, str(dags_folder)
        )
