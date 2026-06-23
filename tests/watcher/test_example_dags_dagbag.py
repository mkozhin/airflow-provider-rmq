"""Regression test: load docs/example_dags/ through the real Airflow DagBag.

Unlike tests/watcher/test_decorators.py (which exercises the decorator in
isolation via MagicMock with a pre-set ``.dag_id``), this test reproduces the
actual import path Airflow uses for DAG files: ``DagBag`` executes each
``.py`` file as a real Python module. That is the only way to catch bugs like
the one fixed in Task 1 — ``@rmq_trigger`` reading ``.dag_id`` off an
unvalidated object — because the AST-based subscription scanner in
``listener.py`` (``_extract_subscriptions_from_file``) never imports DAG
files at all; it only parses source text.

Note: ``docs/example_dags/rmq_dlq_setup.py`` has a pre-existing, unrelated
import error (``TypeError: unsupported operand type(s) for >>: 'list' and
'list'`` from chaining two task-list bitshifts), but it never mentions
``rmq_trigger`` in its source, so it is naturally excluded by the discovery
filter below and needs no special-casing here. See the "Post-Completion"
section of ``docs/plans/20260623-rmq-trigger-taskflow-dag-fix.md``.
"""
from __future__ import annotations

from pathlib import Path

import pytest

EXAMPLE_DAGS_DIR = Path(__file__).resolve().parents[2] / "docs" / "example_dags"


def _rmq_trigger_example_dag_files() -> list[Path]:
    """Every example DAG file whose source mentions ``rmq_trigger``.

    This naturally picks up the two draft example DAGs
    (``rmq_watcher_cooldown_multi_queue.py``, ``rmq_watcher_jetstat_exchange.py``)
    that are untracked in the repo as of this plan, in addition to the
    already-committed ``rmq_watcher_triggered_dag.py`` — all three must import
    cleanly.
    """
    return sorted(
        path
        for path in EXAMPLE_DAGS_DIR.glob("*.py")
        if "rmq_trigger" in path.read_text(encoding="utf-8")
    )


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=str(EXAMPLE_DAGS_DIR), include_examples=False, safe_mode=False)


class TestExampleDagsImportCleanly:
    def test_rmq_trigger_example_dag_files_exist(self):
        # Sanity check on the discovery glob itself: if example DAGs get
        # renamed/moved this test should fail loudly instead of silently
        # checking zero files.
        files = _rmq_trigger_example_dag_files()
        names = {path.name for path in files}
        assert "rmq_watcher_triggered_dag.py" in names
        assert len(files) >= 1

    def test_no_import_errors_for_rmq_trigger_dag_files(self, dagbag):
        rmq_trigger_files = {path.name for path in _rmq_trigger_example_dag_files()}

        errored_files = {Path(path).name for path in dagbag.import_errors}

        unexpected_errors = errored_files & rmq_trigger_files
        assert not unexpected_errors, (
            f"@rmq_trigger example DAG(s) failed to import via real DagBag: "
            f"{sorted(unexpected_errors)}. Errors: "
            f"{ {k: v for k, v in dagbag.import_errors.items() if Path(k).name in unexpected_errors} }"
        )

    def test_rmq_trigger_dags_actually_registered(self, dagbag):
        """Each @rmq_trigger example DAG file must produce at least one DAG.

        A file with zero import errors but also zero registered DAGs would
        be a silent false negative for this regression test (e.g. the file
        not being picked up by DagBag at all).
        """
        dag_file_paths = {dag.fileloc for dag in dagbag.dags.values()}
        rmq_trigger_files = _rmq_trigger_example_dag_files()

        for path in rmq_trigger_files:
            assert any(
                Path(p).name == path.name for p in dag_file_paths
            ), f"{path.name} produced no DAGs in the DagBag"
