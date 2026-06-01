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
    status = Column(String(20), nullable=False, default="disconnected")
    consumer_count = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())


def _make_session_factory():
    from airflow.settings import engine
    return sessionmaker(bind=engine)


WatcherSession: sessionmaker = _make_session_factory()


def ensure_table_exists() -> None:
    """Create rmq_watcher_* tables if they don't exist. Safe to call multiple times."""
    from airflow.settings import engine
    WatcherBase.metadata.create_all(engine, checkfirst=True)


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
        )
        session.add(sub)
    sub.filter_data = filter_data or {}
    sub.source = source
    sub.enabled = enabled
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
) -> RMQConnStatus:
    """Insert or update connection status. Caller must commit."""
    row = session.query(RMQConnStatus).filter_by(conn_id=conn_id).first()
    if row is None:
        row = RMQConnStatus(conn_id=conn_id)
        session.add(row)
    row.status = status
    row.consumer_count = consumer_count
    row.last_error = last_error
    return row


def get_conn_statuses(session: Session) -> list[RMQConnStatus]:
    """Return all connection status rows."""
    return session.query(RMQConnStatus).all()
