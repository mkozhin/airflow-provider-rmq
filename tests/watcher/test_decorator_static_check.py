"""The decorator's static-readability check on DAG files imported through ``DagBag``.

``@rmq_trigger`` checks its call against the watcher's static reading of the
file while the file is imported, so these tests need a real call site: each DAG
file is written to a ``DAGS_FOLDER`` under ``tmp_path`` and loaded by Airflow's
``DagBag``. A failing check becomes an import error of the whole file, which is
how Airflow shows it as "Broken DAG".
"""

from __future__ import annotations

import importlib
import sys
import textwrap
from pathlib import Path

import pytest

_HEADER = (
    "from datetime import datetime\n"
    "\n"
    "from airflow.decorators import dag\n"
    "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
    "\n"
    "START = datetime(2024, 1, 1)\n"
)

STEP_3 = "cannot read every @rmq_trigger of this file statically"
STEP_4 = "rmq_trigger is called through a name the RMQ watcher does not recognize"
STEP_6 = "the RMQ watcher reads other arguments from the file"
NOT_SCANNED = "in a file the RMQ watcher does not scan"


@pytest.fixture
def dags_folder(tmp_path, monkeypatch):
    """A ``DAGS_FOLDER`` under ``tmp_path``, set on ``airflow.settings``."""
    from airflow import settings

    folder = tmp_path / "dags"
    folder.mkdir()
    monkeypatch.setattr(settings, "DAGS_FOLDER", str(folder))
    return folder


@pytest.fixture
def lib(tmp_path, monkeypatch):
    """Write importable modules to a folder outside ``DAGS_FOLDER``.

    Returns ``write(module_name, source)``; the modules leave ``sys.modules``
    after the test.
    """
    folder = tmp_path / "lib"
    folder.mkdir()
    monkeypatch.syspath_prepend(str(folder))
    written: list[str] = []

    def write(module: str, source: str) -> None:
        (folder / f"{module}.py").write_text(textwrap.dedent(source), encoding="utf-8")
        importlib.invalidate_caches()
        written.append(module)

    yield write
    for module in written:
        sys.modules.pop(module, None)


def _write(folder: Path, name: str, body: str) -> Path:
    path = folder / name
    path.write_text(_HEADER + textwrap.dedent(body), encoding="utf-8")
    return path


def _load(folder: Path):
    from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=str(folder), include_examples=False, safe_mode=False)


def _import_error(bag, path: Path) -> str:
    errors = {Path(p).name: text for p, text in bag.import_errors.items()}
    assert path.name in errors, f"{path.name} imported without an error"
    return errors[path.name]


def _subs(bag, dag_id: str) -> list[dict]:
    return bag.dags[dag_id]._rmq_subscriptions


class TestReadableFilesImport:
    def test_literals(self, dags_folder):
        _write(dags_folder, "literals.py", """
            @rmq_trigger(
                queue="orders",
                conn_id="rmq_prod",
                filter_data={"filter_headers": {"type": "paid"}},
                cooldown=30,
            )
            @dag(dag_id="literal_dag", schedule=None, start_date=START)
            def literal_dag():
                pass

            literal_dag()
        """)
        bag = _load(dags_folder)

        assert not bag.import_errors
        [sub] = _subs(bag, "literal_dag")
        assert (sub["queue_name"], sub["conn_id"], sub["cooldown"]) == ("orders", "rmq_prod", 30)

    def test_constants(self, dags_folder):
        _write(dags_folder, "constants.py", """
            QUEUE = "orders"
            Q1 = "payments"
            Q2 = "refunds"
            RMQ_CONN = "rmq_prod"
            PAID = "paid"
            RMQ_OPTS = {"conn_id": "rmq_eu", "cooldown": 10}

            @rmq_trigger(QUEUE, None, RMQ_CONN, {"filter_headers": {"type": PAID}})
            @rmq_trigger(queues=[Q1, Q2], **RMQ_OPTS)
            @dag(dag_id="constant_dag", schedule=None, start_date=START)
            def constant_dag():
                pass

            constant_dag()
        """)
        bag = _load(dags_folder)

        assert not bag.import_errors
        subs = _subs(bag, "constant_dag")
        assert [(s["queue_name"], s["conn_id"]) for s in subs] == [
            ("payments", "rmq_eu"),
            ("refunds", "rmq_eu"),
            ("orders", "rmq_prod"),
        ]
        assert subs[2]["filter_data"] == {"filter_headers": {"type": "paid"}}

    def test_nested_factory(self, dags_folder):
        _write(dags_folder, "nested.py", """
            def make():
                @rmq_trigger(queue="nested.q")
                @dag(dag_id="nested_dag", schedule=None, start_date=START)
                def nested_dag():
                    pass

                return nested_dag()

            NESTED = make()
        """)
        bag = _load(dags_folder)

        assert not bag.import_errors
        assert _subs(bag, "nested_dag")[0]["queue_name"] == "nested.q"

    def test_dag_under_module_level_if(self, dags_folder):
        _write(dags_folder, "under_if.py", """
            if True:
                @rmq_trigger(queue="if.q")
                @dag(dag_id="if_dag", schedule=None, start_date=START)
                def if_dag():
                    pass

                if_dag()
        """)
        bag = _load(dags_folder)

        assert not bag.import_errors
        assert _subs(bag, "if_dag")[0]["queue_name"] == "if.q"

    def test_file_in_a_subfolder(self, dags_folder):
        (dags_folder / "team").mkdir()
        _write(dags_folder / "team", "sub.py", """
            @rmq_trigger(queue="team.q")
            @dag(dag_id="team_dag", schedule=None, start_date=START)
            def team_dag():
                pass

            team_dag()
        """)
        bag = _load(dags_folder)

        assert not bag.import_errors
        assert "team_dag" in bag.dags


class TestUnreadableFilesFailToImport:
    """Every form the watcher's scan refuses breaks the import with the file's problems."""

    @pytest.mark.parametrize(
        ("name", "body", "reason"),
        [
            pytest.param(
                "fstring.py",
                """
                ENV = "dev"

                @rmq_trigger(queue=f"{ENV}.orders")
                @dag(dag_id="fstring_dag", schedule=None, start_date=START)
                def fstring_dag():
                    pass

                fstring_dag()
                """,
                "queue cannot be read — an f-string",
                id="f-string",
            ),
            pytest.param(
                "call.py",
                """
                def get_conn():
                    return "rmq_prod"

                @rmq_trigger(queue="orders", conn_id=get_conn())
                @dag(dag_id="call_dag", schedule=None, start_date=START)
                def call_dag():
                    pass

                call_dag()
                """,
                "conn_id cannot be read — a function call",
                id="call",
            ),
            pytest.param(
                "imported.py",
                """
                from rmq_static_check_consts import RMQ_CONN

                @rmq_trigger(queue="orders", conn_id=RMQ_CONN)
                @dag(dag_id="imported_dag", schedule=None, start_date=START)
                def imported_dag():
                    pass

                imported_dag()
                """,
                "conn_id cannot be read — 'RMQ_CONN'",
                id="imported-constant",
            ),
            pytest.param(
                "mutated.py",
                """
                FILTER = {"filter_headers": {"type": "paid"}}
                FILTER["filter_headers"]["type"] = "refund"

                @rmq_trigger(queue="orders", filter_data=FILTER)
                @dag(dag_id="mutated_dag", schedule=None, start_date=START)
                def mutated_dag():
                    pass

                mutated_dag()
                """,
                "filter_data cannot be read — 'FILTER'",
                id="mutated-constant",
            ),
            pytest.param(
                "loop.py",
                """
                for QUEUE_NAME in ["loop.q"]:
                    @rmq_trigger(queue=QUEUE_NAME)
                    @dag(dag_id="loop_dag", schedule=None, start_date=START)
                    def loop_dag():
                        pass

                    loop_dag()
                """,
                "queue cannot be read — 'QUEUE_NAME'",
                id="decorator-in-loop",
            ),
            pytest.param(
                "imported_dag_id.py",
                """
                from rmq_static_check_consts import DAG_ID

                @rmq_trigger(queue="orders")
                @dag(dag_id=DAG_ID, schedule=None, start_date=START)
                def imported_dag_id():
                    pass

                imported_dag_id()
                """,
                "cannot statically resolve dag_id",
                id="dag-id-from-import",
            ),
            pytest.param(
                "direct.py",
                """
                from airflow import DAG

                with DAG("direct_dag", schedule=None, start_date=START) as direct:
                    pass

                rmq_trigger(queue="direct.q")(direct)
                """,
                "line 13: the name rmq_trigger is used other than as a decorator call",
                id="direct-call-on-dag",
            ),
            pytest.param(
                "alias_used.py",
                """
                from airflow_provider_rmq.watcher.decorators import rmq_trigger as rt

                @rt(queue="orders")
                @dag(dag_id="alias_dag", schedule=None, start_date=START)
                def alias_dag():
                    pass

                alias_dag()
                """,
                "rmq_trigger is imported as 'rt'",
                id="alias-used",
            ),
            pytest.param(
                "alias_unused.py",
                """
                from airflow_provider_rmq.watcher.decorators import rmq_trigger as rt

                @rmq_trigger(queue="orders")
                @dag(dag_id="alias_unused_dag", schedule=None, start_date=START)
                def alias_unused_dag():
                    pass

                alias_unused_dag()
                """,
                "rmq_trigger is imported as 'rt'",
                id="alias-unused-next-to-literal-call",
            ),
            pytest.param(
                "partial.py",
                """
                from functools import partial

                RMQ = partial(rmq_trigger, conn_id="rmq_prod")

                @RMQ(queue="orders")
                @dag(dag_id="partial_dag", schedule=None, start_date=START)
                def partial_dag():
                    pass

                partial_dag()
                """,
                "line 10: the name rmq_trigger is used other than as a decorator call",
                id="partial",
            ),
            pytest.param(
                "stacked.py",
                """
                def get_queue():
                    return "b"

                @rmq_trigger(queue="a")
                @rmq_trigger(queue=get_queue())
                @dag(dag_id="stacked_dag", schedule=None, start_date=START)
                def stacked_dag():
                    pass

                stacked_dag()
                """,
                "queue cannot be read — a function call",
                id="stacked-readable-and-unreadable",
            ),
            pytest.param(
                "unknown_key.py",
                """
                @rmq_trigger(queue="orders")
                @dag(dag_id="known_dag", schedule=None, start_date=START)
                def known_dag():
                    pass

                known_dag()

                def never_called():
                    @rmq_trigger(queue="orders", queue_name="typo")
                    @dag(dag_id="typo_dag", schedule=None, start_date=START)
                    def typo_dag():
                        pass

                    return typo_dag()
                """,
                "queue_name cannot be read — rmq_trigger has no such parameter",
                id="unknown-key",
            ),
        ],
    )
    def test_import_error_lists_the_files_problems(self, dags_folder, lib, name, body, reason):
        lib("rmq_static_check_consts", """
            RMQ_CONN = "rmq_prod"
            DAG_ID = "imported_dag_id"
        """)
        path = _write(dags_folder, name, body)
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in error
        assert STEP_3 in error
        assert reason in error
        assert not bag.dags

    def test_re_exported_under_another_name(self, dags_folder, lib):
        lib("rmq_static_check_helper", """
            from airflow_provider_rmq.watcher.decorators import rmq_trigger as trig
        """)
        path = _write(dags_folder, "reexport.py", """
            from rmq_static_check_helper import trig

            @trig(queue="orders")
            @dag(dag_id="reexport_dag", schedule=None, start_date=START)
            def reexport_dag():
                pass

            reexport_dag()
        """)
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in error
        assert STEP_4 in error
        assert not bag.dags


class TestConstantChangedBeforeTheFactoryCall:
    def test_rebound_constant(self, dags_folder):
        path = _write(dags_folder, "rebound.py", """
            QUEUE = "a.q"

            @rmq_trigger(queue=QUEUE)
            @dag(dag_id="rebound_dag", schedule=None, start_date=START)
            def rebound_dag():
                pass

            QUEUE = "b.q"
            rebound_dag()
        """)
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert STEP_3 in error
        assert "queue cannot be read — 'QUEUE'" in error
        assert not bag.dags

    def test_constant_changed_behind_the_scans_back(self, dags_folder):
        """A change the scan cannot see still differs from what the scan reads."""
        path = _write(dags_folder, "hidden_change.py", """
            FILTER = {"filter_headers": {"type": "paid"}}

            @rmq_trigger(queue="orders", filter_data=FILTER)
            @dag(dag_id="hidden_change_dag", schedule=None, start_date=START)
            def hidden_change_dag():
                pass

            globals()["FILTER"]["filter_headers"]["type"] = "refund"
            hidden_change_dag()
        """)
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in error
        assert STEP_6 in error
        assert "subscription 1, filter_data: the watcher reads" in error
        assert not bag.dags


class TestCallSiteOutsideTheScannedFiles:
    def test_factory_in_a_module_outside_dags_folder(self, dags_folder, lib):
        lib("rmq_static_check_factories", """
            from datetime import datetime

            from airflow.decorators import dag
            from airflow_provider_rmq.watcher.decorators import rmq_trigger


            def build():
                @rmq_trigger(queue="lib.q")
                @dag(dag_id="lib_dag", schedule=None, start_date=datetime(2024, 1, 1))
                def lib_dag():
                    pass

                return lib_dag()
        """)
        path = _write(dags_folder, "uses_factory.py", """
            from rmq_static_check_factories import build

            LIB_DAG = build()
        """)
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in error
        assert NOT_SCANNED in error
        assert "rmq_static_check_factories.py" in error
        assert not bag.dags

    def test_file_in_a_hidden_folder(self, dags_folder):
        """``DagBag`` loads a hidden folder the watcher's glob skips."""
        hidden = dags_folder / ".hidden"
        hidden.mkdir()
        path = _write(hidden, "hidden.py", """
            @rmq_trigger(queue="orders")
            @dag(dag_id="hidden_dag", schedule=None, start_date=START)
            def hidden_dag():
                pass

            hidden_dag()
        """)
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert NOT_SCANNED in error


class TestWholeFileFails:
    def test_one_unreadable_dag_breaks_its_readable_neighbour(self, dags_folder):
        path = _write(dags_folder, "two_dags.py", """
            def get_conn():
                return "rmq_prod"

            @rmq_trigger(queue="good.q")
            @dag(dag_id="good_dag", schedule=None, start_date=START)
            def good_dag():
                pass

            good_dag()

            @rmq_trigger(queue="bad.q", conn_id=get_conn())
            @dag(dag_id="bad_dag", schedule=None, start_date=START)
            def bad_dag():
                pass

            bad_dag()
        """)
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert STEP_3 in error
        assert "bad_dag" in error
        assert "good_dag" not in bag.dags
        assert "bad_dag" not in bag.dags

    def test_invalid_literal_fails_with_the_builders_error(self, dags_folder):
        path = _write(dags_folder, "negative_cooldown.py", """
            @rmq_trigger(queue="orders", cooldown=-1)
            @dag(dag_id="negative_dag", schedule=None, start_date=START)
            def negative_dag():
                pass

            negative_dag()
        """)
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert "ValueError: 'cooldown' must be >= 0." in error
        assert "RMQTriggerNotStaticError" not in error
        assert not bag.dags
