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
import zipfile
from pathlib import Path
from unittest.mock import patch

import pytest

_HEADER = (
    "from datetime import datetime\n"
    "\n"
    "from airflow.decorators import dag\n"
    "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
    "\n"
    "START = datetime(2024, 1, 1)\n"
)

FILE_REFUSED = "cannot read every @rmq_trigger of this file statically"
CALL_NOT_FOUND = "rmq_trigger is called through a name the RMQ watcher does not recognize"
VALUES_DIFFER = "the RMQ watcher reads other arguments from the file"
NOT_SCANNED = "in a file the RMQ watcher does not scan"


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
                "stacked_exchange.py",
                """
                @rmq_trigger(exchange="jetstat.airflow", routing_keys=["a.b"])
                @rmq_trigger(exchange="jetstat.other", routing_keys=["c.d"])
                @dag(dag_id="stacked_exchange_dag", schedule=None, start_date=START)
                def stacked_exchange_dag():
                    pass

                stacked_exchange_dag()
                """,
                "second exchange=",
                id="stacked-exchange",
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
        assert FILE_REFUSED in error
        assert reason in error
        assert not bag.dags

    def test_direct_call_on_a_dag_fails_with_type_error(self, dags_folder):
        path = _write(dags_folder, "direct.py", """
            from airflow import DAG

            with DAG("direct_dag", schedule=None, start_date=START) as direct:
                pass

            rmq_trigger(queue="direct.q")(direct)
        """)
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert "TypeError" in error
        assert "applied to the DAG 'direct_dag' itself" in error
        assert not bag.dags

    def test_same_named_wrapper_of_another_module(self, dags_folder, lib):
        lib("rmq_static_check_common", """
            import functools

            from airflow_provider_rmq.watcher import decorators

            rmq_trigger = functools.partial(decorators.rmq_trigger, conn_id="rmq_prod")
        """)
        path = dags_folder / "wrapper.py"
        path.write_text(textwrap.dedent("""
            from datetime import datetime

            from airflow.decorators import dag
            from rmq_static_check_common import rmq_trigger

            @rmq_trigger(queue="orders")
            @dag(dag_id="wrapper_dag", schedule=None, start_date=datetime(2024, 1, 1))
            def wrapper_dag():
                pass

            wrapper_dag()
        """), encoding="utf-8")
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in error
        assert FILE_REFUSED in error
        assert "rmq_trigger is rmq_static_check_common.rmq_trigger" in error
        assert not bag.dags

    def test_same_named_dag_wrapper_of_another_module(self, dags_folder, lib):
        lib("rmq_static_check_company", """
            from airflow.decorators import dag as airflow_dag

            def dag(dag_id, **kwargs):
                return airflow_dag(dag_id=dag_id, **kwargs)
        """)
        path = dags_folder / "dag_wrapper.py"
        path.write_text(textwrap.dedent("""
            from datetime import datetime

            from airflow_provider_rmq.watcher.decorators import rmq_trigger
            from rmq_static_check_company import dag

            @rmq_trigger(queue="orders")
            @dag("dag_wrapper_dag", schedule=None, start_date=datetime(2024, 1, 1))
            def dag_wrapper_dag():
                pass

            dag_wrapper_dag()
        """), encoding="utf-8")
        bag = _load(dags_folder)

        error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in error
        assert FILE_REFUSED in error
        assert "dag is rmq_static_check_company.dag, not Airflow's dag decorator" in error
        assert not bag.dags

    def test_dag_file_inside_a_zip(self, dags_folder):
        archive = dags_folder / "pkg.zip"
        with zipfile.ZipFile(archive, "w") as zf:
            zf.writestr("zipped_dag.py", _HEADER + textwrap.dedent("""
                @rmq_trigger(queue="orders")
                @dag(dag_id="zipped_dag", schedule=None, start_date=START)
                def zipped_dag():
                    pass

                zipped_dag()
            """))
        bag = _load(dags_folder)

        assert len(bag.import_errors) == 1
        [error] = bag.import_errors.values()
        assert "RMQTriggerNotStaticError" in error
        assert "cannot read this file" in error
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
        assert CALL_NOT_FOUND in error
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
        assert FILE_REFUSED in error
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
        assert VALUES_DIFFER in error
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


class TestOneParsePerFile:
    def test_a_file_with_many_dags_is_parsed_once(self, dags_folder):
        from airflow_provider_rmq.watcher import dag_file_scan

        count = 20
        _write(dags_folder, "many.py", "".join(f"""
            @rmq_trigger(queue="q{n}", cooldown={n})
            @dag(dag_id="many_{n}", schedule=None, start_date=START)
            def many_{n}():
                pass

            many_{n}()
        """ for n in range(count)))
        with patch.object(
            dag_file_scan, "_read_and_parse", wraps=dag_file_scan._read_and_parse
        ) as parse:
            bag = _load(dags_folder)

        assert not bag.import_errors
        assert len(bag.dags) == count
        assert parse.call_count == 1


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
        assert FILE_REFUSED in error
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


def _watcher_errors(caplog) -> list[str]:
    return [
        r.getMessage()
        for r in caplog.records
        if r.levelname == "ERROR" and r.name == "airflow_provider_rmq.watcher.listener"
    ]


class TestWatcherAndDecoratorReadTheSameFile:
    """One DAG file seen by both processes: the watcher's scan and the DAG import."""

    @staticmethod
    def _scan(path: Path, caplog) -> list[dict]:
        import logging

        from airflow_provider_rmq.watcher.listener import RMQWatcherListener

        with caplog.at_level(logging.WARNING):
            result = RMQWatcherListener()._extract_subscriptions_from_file(str(path))
        assert result is not None
        return result

    def test_constants_give_the_watcher_the_decorators_subscriptions(self, dags_folder, caplog):
        path = _write(dags_folder, "shared.py", """
            QUEUE = "orders"
            RMQ_CONN = "rmq_prod"
            FILTER = {"filter_headers": {"type": "paid"}}
            RMQ_OPTS = {"conn_id": "rmq_eu", "cooldown": 10}

            @rmq_trigger(QUEUE, None, RMQ_CONN, FILTER, 30)
            @rmq_trigger(queues=["payments", "refunds"], **RMQ_OPTS)
            @dag(dag_id="shared_dag", schedule=None, start_date=START)
            def shared_dag():
                pass

            shared_dag()
        """)
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert not bag.import_errors
        assert _watcher_errors(caplog) == []
        imported = _subs(bag, "shared_dag")
        stored_fields = [
            {k: v for k, v in row.items() if k not in ("dag_id", "group_key")}
            for row in scanned
        ]
        by_queue = sorted(stored_fields, key=lambda s: s["queue_name"])
        assert by_queue == sorted(imported, key=lambda s: s["queue_name"])
        assert {row["dag_id"] for row in scanned} == {"shared_dag"}
        orders = next(s for s in by_queue if s["queue_name"] == "orders")
        assert (orders["conn_id"], orders["filter_data"], orders["cooldown"]) == (
            "rmq_prod",
            {"filter_headers": {"type": "paid"}},
            30,
        )

    def test_an_unreadable_argument_fails_in_both_processes(self, dags_folder, caplog):
        path = _write(dags_folder, "broken.py", """
            import os

            @rmq_trigger(queue="good.q")
            @dag(dag_id="good_dag", schedule=None, start_date=START)
            def good_dag():
                pass

            good_dag()

            @rmq_trigger(queue="bad.q", conn_id=os.environ.get("RMQ_CONN", "rmq_prod"))
            @dag(dag_id="bad_dag", schedule=None, start_date=START)
            def bad_dag():
                pass

            bad_dag()
        """)
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert scanned == []
        [error_log] = _watcher_errors(caplog)
        assert str(path) in error_log
        assert "conn_id cannot be read" in error_log

        import_error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in import_error
        assert FILE_REFUSED in import_error
        assert "conn_id cannot be read" in import_error
        assert not bag.dags

    def test_a_dag_wrapper_that_renames_the_dag_fails_in_both_processes(
        self, dags_folder, lib, caplog
    ):
        lib("rmq_static_check_prefixing", """
            from airflow.decorators import dag as airflow_dag

            def dag(dag_id, **kwargs):
                return airflow_dag(dag_id="prod__" + dag_id, **kwargs)
        """)
        path = dags_folder / "prefixed.py"
        path.write_text(textwrap.dedent("""
            from datetime import datetime

            from airflow_provider_rmq.watcher.decorators import rmq_trigger
            from rmq_static_check_prefixing import dag

            START = datetime(2024, 1, 1)

            @rmq_trigger(queue="orders")
            @dag("orders_dag", schedule=None, start_date=START)
            def orders_dag():
                pass

            orders_dag()

            @rmq_trigger(queue="payments")
            @dag("payments_dag", schedule=None, start_date=START)
            def payments_dag():
                pass

            payments_dag()
        """), encoding="utf-8")
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert scanned == []
        [error_log] = _watcher_errors(caplog)
        assert str(path) in error_log
        assert "dag is rmq_static_check_prefixing.dag" in error_log

        import_error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in import_error
        assert FILE_REFUSED in import_error
        assert "dag is rmq_static_check_prefixing.dag" in import_error
        assert not bag.dags

    def test_dag_imported_from_airflow_models_dag_is_read_by_both(self, dags_folder, caplog):
        path = dags_folder / "models_dag.py"
        path.write_text(textwrap.dedent("""
            from datetime import datetime

            import airflow.decorators
            from airflow.models.dag import dag
            from airflow_provider_rmq.watcher.decorators import rmq_trigger

            START = datetime(2024, 1, 1)

            @rmq_trigger(queue="orders")
            @dag(dag_id="models_dag", schedule=None, start_date=START)
            def models_dag():
                pass

            models_dag()

            @rmq_trigger(queue="payments")
            @airflow.decorators.dag(dag_id="decorators_dag", schedule=None, start_date=START)
            def decorators_dag():
                pass

            decorators_dag()
        """), encoding="utf-8")
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert not bag.import_errors
        assert _watcher_errors(caplog) == []
        assert sorted((r["dag_id"], r["queue_name"]) for r in scanned) == [
            ("decorators_dag", "payments"), ("models_dag", "orders"),
        ]
        assert [s["queue_name"] for s in _subs(bag, "models_dag")] == ["orders"]
        assert [s["queue_name"] for s in _subs(bag, "decorators_dag")] == ["payments"]

    @pytest.mark.parametrize(
        ("before", "after"),
        [
            pytest.param(
                """
                def on_failure(context):
                    dag = context["dag"]
                    print(dag.dag_id)
                """,
                "",
                id="local-in-a-callback",
            ),
            pytest.param(
                """
                def describe(dag):
                    return dag.dag_id
                """,
                "",
                id="parameter",
            ),
            pytest.param("", "dag = orders_dag()\n", id="factory-result-below"),
            pytest.param(
                "",
                """
                from airflow import DAG

                with DAG("other_dag", schedule=None, start_date=START) as dag:
                    pass
                """,
                id="with-dag-as-dag-below",
            ),
        ],
    )
    def test_a_binding_of_dag_the_decorator_cannot_see_is_read_by_both(
        self, dags_folder, caplog, before, after
    ):
        path = _write(
            dags_folder,
            "local_dag.py",
            textwrap.dedent(before)
            + textwrap.dedent("""
                from airflow.decorators import task

                @rmq_trigger(queue="orders")
                @dag(dag_id="orders_dag", schedule=None, start_date=START)
                def orders_dag():
                    @task
                    def report(dag=None):
                        return dag

                    report()

                orders_dag()
            """)
            + textwrap.dedent(after),
        )
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert not bag.import_errors
        assert _watcher_errors(caplog) == []
        assert [(r["dag_id"], r["queue_name"]) for r in scanned] == [("orders_dag", "orders")]
        assert [s["queue_name"] for s in _subs(bag, "orders_dag")] == ["orders"]

    @pytest.mark.parametrize(
        "after",
        [
            pytest.param("dag = create_dag()\n", id="factory-result-assigned-to-dag"),
            pytest.param(
                """
                FACTORY_DAG = create_dag()

                from airflow import DAG

                with DAG("legacy_dag", schedule=None, start_date=START) as dag:
                    pass
                """,
                id="with-dag-as-dag-below-the-factory-call",
            ),
        ],
    )
    def test_a_module_binding_of_dag_below_a_factory_call_is_read_by_both(
        self, dags_folder, caplog, after
    ):
        path = _write(
            dags_folder,
            "factory_dag.py",
            textwrap.dedent("""
                def create_dag():
                    @rmq_trigger(queue="orders")
                    @dag(dag_id="factory_dag", schedule=None, start_date=START)
                    def factory_dag():
                        pass

                    return factory_dag()

            """)
            + textwrap.dedent(after),
        )
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert not bag.import_errors
        assert _watcher_errors(caplog) == []
        assert [(r["dag_id"], r["queue_name"]) for r in scanned] == [("factory_dag", "orders")]
        assert [s["queue_name"] for s in _subs(bag, "factory_dag")] == ["orders"]

    def test_a_module_rebinding_of_dag_above_a_factory_call_fails_in_both_processes(
        self, dags_folder, caplog
    ):
        path = _write(dags_folder, "late_factory.py", """
            def create_dag():
                @rmq_trigger(queue="orders")
                @dag(dag_id="factory_dag", schedule=None, start_date=START)
                def factory_dag():
                    pass

                return factory_dag()

            dag = dag
            FACTORY_DAG = create_dag()
        """)
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert scanned == []
        [error_log] = _watcher_errors(caplog)
        assert "dag is not bound by one absolute import alone" in error_log

        import_error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in import_error
        assert FILE_REFUSED in import_error
        assert "dag is not bound by one absolute import alone" in import_error
        assert not bag.dags

    def test_a_factory_handed_on_before_a_module_binding_of_dag_fails_in_both_processes(
        self, dags_folder, caplog
    ):
        path = _write(dags_folder, "registry.py", """
            from airflow import DAG

            FACTORIES = []

            def create_orders_dag():
                @rmq_trigger(queue="orders")
                @dag(dag_id="orders_dag", schedule=None, start_date=START)
                def orders_dag():
                    pass

                return orders_dag()

            FACTORIES.append(create_orders_dag)

            with DAG("legacy_dag", schedule=None, start_date=START) as dag:
                pass

            for factory in FACTORIES:
                factory()
        """)
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert scanned == []
        [error_log] = _watcher_errors(caplog)
        assert "dag is not bound by one absolute import alone" in error_log

        assert "TypeError" in _import_error(bag, path)
        assert not bag.dags

    @pytest.mark.parametrize(
        ("body", "import_failure"),
        [
            pytest.param(
                """
                def outer():
                    def create_orders_dag():
                        @rmq_trigger(queue="orders")
                        @dag(dag_id="orders_dag", schedule=None, start_date=START)
                        def orders_dag():
                            pass

                        return orders_dag()

                    return create_orders_dag

                create = outer()
                dag = None
                create()
                """,
                "TypeError",
                id="two-level-factory",
            ),
            pytest.param(
                """
                def create_orders_dags():
                    @rmq_trigger(queue="orders")
                    @dag(dag_id="orders_dag", schedule=None, start_date=START)
                    def orders_dag():
                        pass

                    yield orders_dag()

                made = create_orders_dags()
                dag = None
                list(made)
                """,
                "TypeError",
                id="generator-factory",
            ),
            pytest.param(
                """
                from airflow import DAG

                FACTORIES = []

                def register(factory):
                    FACTORIES.append(factory)
                    return factory

                @register
                def create_orders_dag():
                    @rmq_trigger(queue="orders")
                    @dag(dag_id="orders_dag", schedule=None, start_date=START)
                    def orders_dag():
                        pass

                    return orders_dag()

                create_orders_dag()

                with DAG("legacy_dag", schedule=None, start_date=START) as dag:
                    pass

                for factory in FACTORIES:
                    factory()
                """,
                "RMQTriggerNotStaticError",
                id="registered-factory",
            ),
        ],
    )
    def test_a_factory_whose_call_may_not_run_the_dag_def_fails_in_both_processes(
        self, dags_folder, caplog, body, import_failure
    ):
        path = _write(dags_folder, "deferred_factory.py", body)
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert scanned == []
        [error_log] = _watcher_errors(caplog)
        assert "dag is not bound by one absolute import alone" in error_log

        assert import_failure in _import_error(bag, path)
        assert not bag.dags

    def test_a_factory_its_file_never_calls_with_dag_rebound_below_fails_in_both_processes(
        self, dags_folder, caplog, monkeypatch
    ):
        monkeypatch.syspath_prepend(str(dags_folder))
        monkeypatch.delitem(sys.modules, "factories", raising=False)
        factories = _write(dags_folder, "factories.py", """
            from airflow import DAG

            def create_orders_dag():
                @rmq_trigger(queue="orders")
                @dag(dag_id="orders_dag", schedule=None, start_date=START)
                def orders_dag():
                    pass

                return orders_dag()

            with DAG("legacy_dag", schedule=None, start_date=START) as dag:
                pass
        """)
        caller = _write(dags_folder, "caller.py", """
            from factories import create_orders_dag

            create_orders_dag()
        """)
        try:
            bag = _load(dags_folder)
        finally:
            sys.modules.pop("factories", None)
        scanned = self._scan(factories, caplog)

        assert scanned == []
        [error_log] = _watcher_errors(caplog)
        assert "dag is not bound by one absolute import alone" in error_log

        assert "TypeError" in _import_error(bag, caller)
        assert sorted(bag.dags) == ["legacy_dag"]

    def test_a_decorator_without_wraps_below_dag_fails_in_both_processes(
        self, dags_folder, caplog
    ):
        path = _write(dags_folder, "no_wraps.py", """
            def no_wraps(func):
                def inner():
                    return func()

                return inner

            @rmq_trigger(queue="orders")
            @dag(schedule=None, start_date=START)
            @no_wraps
            def my_dag():
                pass

            my_dag()

            @rmq_trigger(queue="payments")
            @dag(dag_id="neighbour", schedule=None, start_date=START)
            def neighbour():
                pass

            neighbour()
        """)
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert scanned == []
        [error_log] = _watcher_errors(caplog)
        assert "@no_wraps sits below @rmq_trigger(...)" in error_log

        import_error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in import_error
        assert FILE_REFUSED in import_error
        assert "@no_wraps sits below @rmq_trigger(...)" in import_error
        assert not bag.dags

    @pytest.mark.parametrize(
        ("before", "after"),
        [
            pytest.param("dag = dag\n", "", id="module-rebinding-above"),
            pytest.param(
                "",
                """
                def swap():
                    global dag
                    dag = None
                """,
                id="global-in-a-function",
            ),
        ],
    )
    def test_a_rebinding_of_dag_the_decorator_sees_fails_in_both_processes(
        self, dags_folder, caplog, before, after
    ):
        path = _write(
            dags_folder,
            "rebound_dag.py",
            before
            + textwrap.dedent("""
                @rmq_trigger(queue="orders")
                @dag(dag_id="orders_dag", schedule=None, start_date=START)
                def orders_dag():
                    pass

                orders_dag()
            """)
            + textwrap.dedent(after),
        )
        bag = _load(dags_folder)
        scanned = self._scan(path, caplog)

        assert scanned == []
        [error_log] = _watcher_errors(caplog)
        assert "dag is not bound by one absolute import alone" in error_log

        import_error = _import_error(bag, path)
        assert "RMQTriggerNotStaticError" in import_error
        assert FILE_REFUSED in import_error
        assert "dag is not bound by one absolute import alone" in import_error
        assert not bag.dags
