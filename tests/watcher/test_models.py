from __future__ import annotations

import logging
from datetime import datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.orm import sessionmaker

from airflow_provider_rmq.watcher import models
from airflow_provider_rmq.watcher.models import (
    RMQConnStatus,
    RMQSubscription,
    WatcherBase,
    delete_subscriptions_for_dag,
    ensure_table_exists,
    get_active_dag_ids,
    get_conn_statuses,
    get_enabled_subscriptions,
    set_consumer_status,
    upsert_conn_status,
    upsert_subscription,
)


def _naive(stamp: str) -> datetime:
    """Naive UTC timestamp, in the shape the watcher writes it."""
    return datetime.fromisoformat(stamp)


#: Table the migration has to bring up to date: rmq_watcher_conn_status without the
#: two diagnostic columns.
_conn_status_ddl_without_diagnostics = """
CREATE TABLE rmq_watcher_conn_status (
    conn_id VARCHAR(250) NOT NULL PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    consumer_count INTEGER NOT NULL,
    last_error TEXT,
    updated_at DATETIME NOT NULL
)
"""


@pytest.fixture(scope="function")
def session():
    """SQLite in-memory session with fresh schema per test."""
    engine = create_engine("sqlite:///:memory:")
    WatcherBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.close()
    WatcherBase.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def schema_engine(monkeypatch):
    """Engine standing in for Airflow's, with the migration flag reset."""
    engine = create_engine("sqlite:///:memory:")
    monkeypatch.setattr("airflow.settings.engine", engine, raising=False)
    monkeypatch.setattr(models, "_schema_ready", False)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def session_with_dagmodel():
    """SQLite in-memory session with watcher schema plus Airflow's DagModel table."""
    from airflow.models import DagModel

    engine = create_engine("sqlite:///:memory:")
    WatcherBase.metadata.create_all(engine)
    DagModel.__table__.create(engine, checkfirst=True)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.close()
    WatcherBase.metadata.drop_all(engine)


# ---------------------------------------------------------------------------

class TestCreateSubscription:
    def test_create_subscription(self, session):
        sub = upsert_subscription(session, dag_id="my_dag", queue_name="q1")
        session.commit()
        assert sub.id is not None
        assert sub.dag_id == "my_dag"
        assert sub.queue_name == "q1"
        assert sub.conn_id == "rmq_default"
        assert sub.enabled is True
        assert sub.consumer_status == "connecting"
        assert sub.trigger_mode == "any"


class TestUpsert:
    def test_upsert_updates_existing(self, session):
        upsert_subscription(session, dag_id="d", queue_name="q", conn_id="c1", filter_data={"filter_headers": {"k": "v1"}})
        session.commit()

        upsert_subscription(session, dag_id="d", queue_name="q", conn_id="c1", filter_data={"filter_headers": {"k": "v2"}})
        session.commit()

        rows = session.query(RMQSubscription).all()
        assert len(rows) == 1
        assert rows[0].filter_data == {"filter_headers": {"k": "v2"}}

    def test_unique_constraint_dag_queue_conn(self, session):
        session.add(RMQSubscription(dag_id="d", queue_name="q", conn_id="c", source="ui"))
        session.commit()
        session.add(RMQSubscription(dag_id="d", queue_name="q", conn_id="c", source="ui"))
        with pytest.raises(IntegrityError):
            session.commit()

    def test_same_queue_different_conn_id_allowed(self, session):
        upsert_subscription(session, dag_id="d", queue_name="q", conn_id="c1")
        upsert_subscription(session, dag_id="d", queue_name="q", conn_id="c2")
        session.commit()
        assert session.query(RMQSubscription).count() == 2


class TestDelete:
    def test_delete_subscriptions_for_dag(self, session):
        upsert_subscription(session, dag_id="dag_a", queue_name="q1")
        upsert_subscription(session, dag_id="dag_a", queue_name="q2")
        upsert_subscription(session, dag_id="dag_b", queue_name="q1")
        session.commit()

        deleted = delete_subscriptions_for_dag(session, "dag_a")
        session.commit()

        assert deleted == 2
        remaining = session.query(RMQSubscription).all()
        assert len(remaining) == 1
        assert remaining[0].dag_id == "dag_b"

    def test_delete_only_dag_file_source(self, session):
        upsert_subscription(session, dag_id="d", queue_name="q", source="dag_file")
        session.add(RMQSubscription(dag_id="d", queue_name="q2", conn_id="rmq_default", source="ui"))
        session.commit()

        delete_subscriptions_for_dag(session, "d")
        session.commit()

        remaining = session.query(RMQSubscription).all()
        assert len(remaining) == 1
        assert remaining[0].source == "ui"


class TestUpsertEnabled:
    def test_dag_file_upsert_preserves_disabled(self, session):
        """M2: dag_file reconcile не должен перетирать enabled=False, выставленный через UI."""
        sub = upsert_subscription(session, dag_id="d", queue_name="q", source="dag_file", enabled=True)
        session.commit()
        sub.enabled = False
        session.commit()

        upsert_subscription(session, dag_id="d", queue_name="q", source="dag_file", enabled=True)
        session.commit()

        row = session.query(RMQSubscription).filter_by(dag_id="d", queue_name="q").one()
        assert row.enabled is False

    def test_ui_upsert_updates_enabled(self, session):
        """M2: ui источник должен обновлять enabled."""
        sub = upsert_subscription(session, dag_id="d", queue_name="q", source="ui", enabled=False)
        session.commit()

        upsert_subscription(session, dag_id="d", queue_name="q", source="ui", enabled=True)
        session.commit()

        row = session.query(RMQSubscription).filter_by(dag_id="d", queue_name="q").one()
        assert row.enabled is True

    def test_new_dag_file_subscription_uses_enabled_arg(self, session):
        """Новая запись от dag_file должна брать enabled из аргумента."""
        upsert_subscription(session, dag_id="d", queue_name="q", source="dag_file", enabled=False)
        session.commit()

        row = session.query(RMQSubscription).filter_by(dag_id="d", queue_name="q").one()
        assert row.enabled is False


class TestGetEnabled:
    def test_get_enabled_subscriptions_filters_disabled(self, session):
        upsert_subscription(session, dag_id="d1", queue_name="q1", enabled=True)
        upsert_subscription(session, dag_id="d2", queue_name="q2", enabled=False)
        session.commit()

        result = get_enabled_subscriptions(session)
        assert len(result) == 1
        assert result[0].dag_id == "d1"


class TestConsumerStatus:
    def test_set_consumer_status_updates_field(self, session):
        sub = upsert_subscription(session, dag_id="d", queue_name="q")
        session.commit()

        set_consumer_status(session, sub.id, "listening")
        session.commit()

        refreshed = session.query(RMQSubscription).filter_by(id=sub.id).one()
        assert refreshed.consumer_status == "listening"
        assert refreshed.last_error is None

    def test_set_consumer_status_sets_last_error(self, session):
        sub = upsert_subscription(session, dag_id="d", queue_name="q")
        session.commit()

        set_consumer_status(session, sub.id, "error", last_error="queue not found")
        session.commit()

        refreshed = session.query(RMQSubscription).filter_by(id=sub.id).one()
        assert refreshed.consumer_status == "error"
        assert refreshed.last_error == "queue not found"

    def test_set_consumer_status_of_a_subscription_that_is_gone_is_a_no_op(self, session):
        """The write is an ``UPDATE ... WHERE id = ...``, so the row a deleted
        subscription took with it matches nothing. The reconcile cycle finishes the row
        of a subscription it has already let go of, and a deleted one has to cost it a
        no-op rather than an error."""
        set_consumer_status(session, 4242, "disconnected")
        session.commit()

        assert session.query(RMQSubscription).filter_by(id=4242).one_or_none() is None


class TestConnStatus:
    def test_upsert_conn_status_creates_and_updates(self, session):
        upsert_conn_status(session, "rmq_default", "connected", consumer_count=3)
        session.commit()

        row = session.query(RMQConnStatus).filter_by(conn_id="rmq_default").one()
        assert row.status == "connected"
        assert row.consumer_count == 3

        upsert_conn_status(session, "rmq_default", "disconnected", consumer_count=0, last_error="timeout")
        session.commit()

        assert session.query(RMQConnStatus).count() == 1
        row = session.query(RMQConnStatus).filter_by(conn_id="rmq_default").one()
        assert row.status == "disconnected"
        assert row.last_error == "timeout"

    def test_writes_and_updates_diagnostic_columns(self, session):
        upsert_conn_status(
            session, "rmq_default", "connected", consumer_count=3,
            broker_consumer_count=2, last_reconcile_at=_naive("2026-08-27T10:00:00"),
        )
        session.commit()

        row = session.query(RMQConnStatus).filter_by(conn_id="rmq_default").one()
        assert row.broker_consumer_count == 2
        assert row.last_reconcile_at == _naive("2026-08-27T10:00:00")

        upsert_conn_status(
            session, "rmq_default", "connected", consumer_count=3,
            broker_consumer_count=3, last_reconcile_at=_naive("2026-08-27T10:05:00"),
        )
        session.commit()

        row = session.query(RMQConnStatus).filter_by(conn_id="rmq_default").one()
        assert row.broker_consumer_count == 3
        assert row.last_reconcile_at == _naive("2026-08-27T10:05:00")

    def test_omitted_diagnostic_args_keep_stored_values(self, session):
        upsert_conn_status(
            session, "c", "connected", consumer_count=1,
            broker_consumer_count=1, last_reconcile_at=_naive("2026-08-27T10:00:00"),
        )
        session.commit()

        upsert_conn_status(session, "c", "connected", consumer_count=1)
        session.commit()

        row = session.query(RMQConnStatus).filter_by(conn_id="c").one()
        assert row.broker_consumer_count == 1
        assert row.last_reconcile_at == _naive("2026-08-27T10:00:00")

    def test_explicit_none_records_absence_of_data(self, session):
        upsert_conn_status(
            session, "c", "connected", consumer_count=1,
            broker_consumer_count=1, last_reconcile_at=_naive("2026-08-27T10:00:00"),
        )
        session.commit()

        upsert_conn_status(
            session, "c", "connected", consumer_count=1,
            broker_consumer_count=None, last_reconcile_at=None,
        )
        session.commit()

        row = session.query(RMQConnStatus).filter_by(conn_id="c").one()
        assert row.broker_consumer_count is None
        assert row.last_reconcile_at is None

    def test_unchanged_status_still_moves_last_reconcile_at(self, session):
        """A steady-state cycle changes nothing else — the timestamp must still move."""
        first = _naive("2026-08-27T10:00:00")
        second = _naive("2026-08-27T10:00:30")
        upsert_conn_status(
            session, "c", "connected", consumer_count=2, last_reconcile_at=first,
        )
        session.commit()

        upsert_conn_status(
            session, "c", "connected", consumer_count=2, last_reconcile_at=second,
        )
        session.commit()
        session.expire_all()

        row = session.query(RMQConnStatus).filter_by(conn_id="c").one()
        assert row.last_reconcile_at == second

    def test_get_conn_statuses_returns_all(self, session):
        upsert_conn_status(session, "conn_a", "connected", consumer_count=1)
        upsert_conn_status(session, "conn_b", "disconnected", consumer_count=0)
        session.commit()

        result = get_conn_statuses(session)
        assert len(result) == 2
        conn_ids = {r.conn_id for r in result}
        assert conn_ids == {"conn_a", "conn_b"}


class TestIsolation:
    def test_watcher_base_isolated_from_airflow_base(self):
        table_names = set(WatcherBase.metadata.tables.keys())
        assert all(name.startswith("rmq_watcher_") for name in table_names), (
            f"WatcherBase contains non-watcher tables: {table_names}"
        )
        assert "rmq_watcher_subscriptions" in table_names
        assert "rmq_watcher_conn_status" in table_names
        assert len(table_names) == 2


class TestGetActiveDagIds:
    def test_returns_only_active_dag_ids(self, session_with_dagmodel):
        from airflow.models import DagModel

        session = session_with_dagmodel
        session.add(DagModel(dag_id="active_dag", is_active=True, is_paused=False))
        session.commit()

        result = get_active_dag_ids(session)
        assert result == {"active_dag"}

    def test_excludes_inactive_dag_ids(self, session_with_dagmodel):
        from airflow.models import DagModel

        session = session_with_dagmodel
        session.add(DagModel(dag_id="active_dag", is_active=True, is_paused=False))
        session.add(DagModel(dag_id="inactive_dag", is_active=False, is_paused=False))
        session.commit()

        result = get_active_dag_ids(session)
        assert result == {"active_dag"}

    def test_empty_table_returns_empty_set(self, session_with_dagmodel):
        session = session_with_dagmodel
        result = get_active_dag_ids(session)
        assert result == set()

    def test_paused_dag_still_included(self, session_with_dagmodel):
        """Locks in the intentional non-filtering of is_paused — see ADR 0006."""
        from airflow.models import DagModel

        session = session_with_dagmodel
        session.add(DagModel(dag_id="paused_dag", is_active=True, is_paused=True))
        session.commit()

        result = get_active_dag_ids(session)
        assert result == {"paused_dag"}


class TestSchemaMigration:
    def test_adds_columns_missing_from_legacy_table(self, schema_engine):
        with schema_engine.begin() as conn:
            conn.execute(text(_conn_status_ddl_without_diagnostics))

        ensure_table_exists()

        columns = {c["name"] for c in inspect(schema_engine).get_columns("rmq_watcher_conn_status")}
        assert "broker_consumer_count" in columns
        assert "last_reconcile_at" in columns
        assert models.is_schema_ready() is True

    def test_migrated_table_accepts_diagnostic_writes(self, schema_engine):
        with schema_engine.begin() as conn:
            conn.execute(text(_conn_status_ddl_without_diagnostics))
        ensure_table_exists()

        session = sessionmaker(bind=schema_engine)()
        try:
            upsert_conn_status(
                session, "c", "connected", consumer_count=1,
                broker_consumer_count=1, last_reconcile_at=_naive("2026-08-27T10:00:00"),
            )
            session.commit()
            row = session.query(RMQConnStatus).filter_by(conn_id="c").one()
            assert row.broker_consumer_count == 1
        finally:
            session.close()

    @pytest.mark.parametrize("dialect_name", ["postgresql", "mysql", "sqlite"])
    def test_the_ddl_is_built_from_the_target_dialect(self, dialect_name):
        """The migration claims to be dialect-independent because it compiles each
        column with the engine's own dialect rather than hard-coding a type name; a
        literal ``DATETIME`` would be wrong on PostgreSQL."""
        from sqlalchemy.dialects import registry

        dialect = registry.load(dialect_name)()
        table = models.RMQConnStatus.__table__
        rendered = {
            column.name: column.type.compile(dialect) for column in table.columns
        }

        assert rendered["broker_consumer_count"].upper().startswith("INTEGER")
        assert "CHAR" not in rendered["last_reconcile_at"].upper()
        if dialect_name == "postgresql":
            assert rendered["last_reconcile_at"].upper() == "TIMESTAMP WITHOUT TIME ZONE"
        assert all(value for value in rendered.values())

    @pytest.mark.parametrize("dialect_name", ["postgresql", "sqlite"])
    def test_the_migration_renders_its_ddl_with_the_engine_dialect(self, dialect_name):
        """The claim above is about the migration, so put the migration to it. A DDL
        built from a hard-coded type name or ``str(column.type)`` would render
        ``DATETIME`` on PostgreSQL, where ``ALTER TABLE ... ADD COLUMN x DATETIME``
        fails with `type "datetime" does not exist` — the migration then never completes
        and the Subscriptions page keeps showing the not-migrated notice."""
        from sqlalchemy.dialects import registry

        statements: list[str] = []

        class _Conn:
            def execute(self, clause):
                statements.append(str(clause))

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        class _Engine:
            dialect = registry.load(dialect_name)()

            def begin(self):
                return _Conn()

        class _Inspector:
            def get_columns(self, table_name):
                # Every column reads as missing, so each one is rendered into DDL.
                return []

        engine = _Engine()
        # ``inspect`` is imported inside the function, so the patch goes to its source.
        with patch("sqlalchemy.inspect", return_value=_Inspector()):
            complete = models._add_missing_columns(engine)

        assert complete is True
        ddl = "\n".join(statements)
        assert "last_reconcile_at" in ddl
        if dialect_name == "postgresql":
            assert "TIMESTAMP WITHOUT TIME ZONE" in ddl
            assert "DATETIME" not in ddl.upper()
        else:
            assert "DATETIME" in ddl.upper()

    def test_repeated_call_on_current_schema_is_safe(self, schema_engine):
        ensure_table_exists()
        assert models.is_schema_ready() is True

        # a fresh process would re-run the whole thing against the same database
        models._schema_ready = False
        ensure_table_exists()
        assert models.is_schema_ready() is True

    def test_ready_schema_short_circuits(self, schema_engine, monkeypatch):
        calls = []
        monkeypatch.setattr(
            models, "_add_missing_columns", lambda engine: calls.append(engine) or True
        )

        ensure_table_exists()
        ensure_table_exists()

        assert len(calls) == 1

    def test_failed_migration_is_retried_until_it_succeeds(self, schema_engine, monkeypatch):
        attempts = []

        def flaky(engine):
            attempts.append(engine)
            return len(attempts) > 1

        monkeypatch.setattr(models, "_add_missing_columns", flaky)

        ensure_table_exists()
        assert models.is_schema_ready() is False

        ensure_table_exists()
        assert models.is_schema_ready() is True
        assert len(attempts) == 2

        ensure_table_exists()
        assert len(attempts) == 2

    def test_failing_alter_warns_and_leaves_schema_not_ready(self, schema_engine, monkeypatch, caplog):
        class _BlindInspector:
            """Reports every column as missing, so each ALTER hits a duplicate."""

            def get_columns(self, table_name):
                return []

        monkeypatch.setattr("sqlalchemy.inspect", lambda engine: _BlindInspector())

        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.watcher.models"):
            ensure_table_exists()

        assert models.is_schema_ready() is False
        assert any("failed to add column" in r.message for r in caplog.records)

    def test_unreadable_table_warns_and_leaves_schema_not_ready(self, schema_engine, monkeypatch, caplog):
        class _BrokenInspector:
            def get_columns(self, table_name):
                raise OperationalError("SELECT 1", {}, Exception("no such table"))

        monkeypatch.setattr("sqlalchemy.inspect", lambda engine: _BrokenInspector())

        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.watcher.models"):
            ensure_table_exists()

        assert models.is_schema_ready() is False
        assert any("cannot inspect" in r.message for r in caplog.records)

    def test_database_failure_propagates_and_leaves_schema_not_ready(self, monkeypatch):
        engine = create_engine("sqlite:////nonexistent-dir/rmq.db")
        monkeypatch.setattr("airflow.settings.engine", engine, raising=False)
        monkeypatch.setattr(models, "_schema_ready", False)

        with pytest.raises(OperationalError):
            ensure_table_exists()

        assert models.is_schema_ready() is False
