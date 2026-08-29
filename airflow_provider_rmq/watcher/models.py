from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.sql import func

log = logging.getLogger(__name__)

WatcherBase = declarative_base()


class RMQSubscription(WatcherBase):
    """One RMQ-queue → DAG subscription."""

    __tablename__ = "rmq_watcher_subscriptions"
    __table_args__ = (
        UniqueConstraint("dag_id", "queue_name", "conn_id", name="uq_rmq_sub_dag_queue_conn"),
    )

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(250), nullable=False)
    queue_name = Column(String(250), nullable=False)
    conn_id = Column(String(250), nullable=False, default="rmq_default")
    filter_data = Column(JSON, nullable=True)
    source = Column(String(10), nullable=False)           # 'dag_file' | 'ui'
    enabled = Column(Boolean, nullable=False, default=True)
    consumer_status = Column(String(20), nullable=False, default="connecting")
    last_error = Column(Text, nullable=True)
    trigger_mode = Column(String(20), nullable=False, default="any")
    group_key = Column(String(250), nullable=True)
    cooldown = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())


class RMQConnStatus(WatcherBase):
    """Connection-level status — one row per conn_id."""

    __tablename__ = "rmq_watcher_conn_status"

    conn_id = Column(String(250), primary_key=True)
    # "unknown" until a liveness check reaches a verdict on this conn_id
    status = Column(String(20), nullable=False, default="unknown")
    # how many consumer tasks the watcher itself has started
    consumer_count = Column(Integer, nullable=False, default=0)
    # how many consumers the broker reports for our queues; NULL means "no data"
    broker_consumer_count = Column(Integer, nullable=True)
    last_error = Column(Text, nullable=True)
    # wall-clock time of the last reconcile cycle, written explicitly by the
    # watcher so that an UPDATE is emitted even when nothing else changed
    last_reconcile_at = Column(DateTime, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())


#: Marker for an argument the caller left out, which ``None`` cannot express here:
#: ``None`` is itself a meaningful value for the nullable columns below.
_UNSET: Any = object()


def _make_session_factory():
    from airflow.settings import engine
    return sessionmaker(bind=engine)


WatcherSession: sessionmaker = _make_session_factory()


_schema_ready = False


def is_schema_ready() -> bool:
    """Whether the schema has been fully created and migrated in this process."""
    return _schema_ready


def _add_missing_columns(engine) -> bool:
    """Add columns the model declares but the live table lacks.

    Columns are added as nullable regardless of the model, because a NOT NULL
    column cannot be attached to a table that already holds rows. Returns True
    when every table matches the model; a False result keeps the schema marked
    as not ready so that the next call tries again.
    """
    from sqlalchemy import inspect as sa_inspect
    from sqlalchemy import text

    complete = True
    inspector = sa_inspect(engine)
    for table in WatcherBase.metadata.sorted_tables:
        try:
            present = {col["name"] for col in inspector.get_columns(table.name)}
        except Exception:
            log.warning("RMQ Watcher: cannot inspect %s", table.name, exc_info=True)
            complete = False
            continue
        for column in table.columns:
            if column.name in present:
                continue
            col_type = column.type.compile(engine.dialect)
            # no IF NOT EXISTS: SQLite rejects it outright, and the inspector
            # above already told us the column is absent
            ddl = f"ALTER TABLE {table.name} ADD COLUMN {column.name} {col_type}"
            try:
                with engine.begin() as conn:
                    conn.execute(text(ddl))
            except Exception:
                log.warning(
                    "RMQ Watcher: failed to add column %s.%s", table.name, column.name,
                    exc_info=True,
                )
                complete = False
            else:
                log.info("RMQ Watcher: added column %s.%s", table.name, column.name)
    return complete


def ensure_table_exists() -> None:
    """Create rmq_watcher_* tables and add columns missing from an older schema.

    Safe to call multiple times: once a call completes without errors the
    schema is marked ready and further calls return immediately. Until then
    every call retries, so a caller that runs periodically recovers from a
    database that was unreachable earlier.
    """
    global _schema_ready
    if _schema_ready:
        return
    from airflow.settings import engine
    WatcherBase.metadata.create_all(engine, checkfirst=True)
    if _add_missing_columns(engine):
        _schema_ready = True


# ---------------------------------------------------------------------------
# CRUD helpers — callers provide the session; these do not commit
# ---------------------------------------------------------------------------

def upsert_subscription(
    session: Session,
    dag_id: str,
    queue_name: str,
    conn_id: str = "rmq_default",
    filter_data: dict[str, Any] | None = None,
    source: str = "dag_file",
    enabled: bool = True,
    trigger_mode: str = "any",
    group_key: str | None = None,
    cooldown: int | None = None,
) -> RMQSubscription:
    """Insert or update a subscription. Caller must commit."""
    sub = (
        session.query(RMQSubscription)
        .filter_by(dag_id=dag_id, queue_name=queue_name, conn_id=conn_id)
        .first()
    )
    if sub is None:
        sub = RMQSubscription(
            dag_id=dag_id,
            queue_name=queue_name,
            conn_id=conn_id,
            enabled=enabled,
        )
        session.add(sub)
    elif source == "ui":
        sub.enabled = enabled
    # dag_file source: preserve current enabled value from DB
    sub.filter_data = filter_data or {}
    sub.source = source
    sub.trigger_mode = trigger_mode
    sub.group_key = group_key
    sub.cooldown = cooldown
    return sub


def delete_subscriptions_for_dag(session: Session, dag_id: str) -> int:
    """Delete all dag_file subscriptions for a DAG. Returns deleted count."""
    deleted = (
        session.query(RMQSubscription)
        .filter_by(dag_id=dag_id, source="dag_file")
        .delete()
    )
    return deleted


def get_enabled_subscriptions(session: Session) -> list[RMQSubscription]:
    """Return all enabled subscriptions."""
    return session.query(RMQSubscription).filter_by(enabled=True).all()


def set_consumer_status(
    session: Session,
    sub_id: int,
    status: str,
    last_error: str | None = None,
) -> None:
    """Update consumer_status and last_error for a subscription. Caller must commit."""
    session.query(RMQSubscription).filter_by(id=sub_id).update(
        {"consumer_status": status, "last_error": last_error}
    )


def upsert_conn_status(
    session: Session,
    conn_id: str,
    status: str,
    consumer_count: int,
    last_error: str | None = None,
    broker_consumer_count: int | None = _UNSET,
    last_reconcile_at: datetime | None = _UNSET,
) -> RMQConnStatus:
    """Insert or update connection status. Caller must commit.

    ``broker_consumer_count`` and ``last_reconcile_at`` distinguish two cases:
    an omitted argument keeps whatever the row holds, while an explicit None
    records "no data" — the broker could not be asked.
    """
    row = session.query(RMQConnStatus).filter_by(conn_id=conn_id).first()
    if row is None:
        row = RMQConnStatus(conn_id=conn_id)
        session.add(row)
    row.status = status
    row.consumer_count = consumer_count
    row.last_error = last_error
    if broker_consumer_count is not _UNSET:
        row.broker_consumer_count = broker_consumer_count
    if last_reconcile_at is not _UNSET:
        row.last_reconcile_at = last_reconcile_at
    return row


def get_conn_statuses(session: Session) -> list[RMQConnStatus]:
    """Return all connection status rows."""
    return session.query(RMQConnStatus).all()


def get_active_dag_ids(session: Session) -> set[str]:
    """Return dag_ids of all Airflow DAGs currently known to be active."""
    from airflow.models import DagModel
    # is_paused is intentionally NOT filtered here — see ADR 0006. A paused
    # DAG intentionally appears in the result.
    return {
        row[0]
        for row in session.query(DagModel.dag_id)
        .filter(DagModel.is_active.is_(True))
        .all()
    }
