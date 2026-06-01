from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from airflow_provider_rmq.watcher.models import (
    RMQConnStatus,
    RMQSubscription,
    WatcherBase,
    delete_subscriptions_for_dag,
    get_conn_statuses,
    get_enabled_subscriptions,
    set_consumer_status,
    upsert_conn_status,
    upsert_subscription,
)


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
