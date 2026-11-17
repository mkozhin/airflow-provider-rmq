from __future__ import annotations

import logging
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
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
    case,
    inspect,
    or_,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, declarative_base, sessionmaker
from sqlalchemy.sql import func

log = logging.getLogger(__name__)

WatcherBase = declarative_base()


def utcnow() -> datetime:
    """The current time as naive UTC, the form every watcher timestamp is stored in."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def naive_utc(value: datetime) -> datetime:
    """``value`` as naive UTC, the form every watcher timestamp is stored in.

    A database driver may hand back an aware datetime; comparing it with a naive one
    raises ``TypeError``, so an aware value is converted first.
    """
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


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
    # whether a message this subscription's filter rejected is acknowledged and gone
    # instead of returned to the queue; NULL reads as False. Nullable because
    # _add_missing_columns attaches a column without NOT NULL whatever the model says,
    # and a NOT NULL model would give a new install a different schema than a migrated
    # one
    ack_unmatched = Column(Boolean, nullable=True)
    # how many trusted scans in a row failed to mention this row's key; NULL and 0
    # both mean "the key is there". Written by the watcher's _sync_to_db and reset
    # by upsert_subscription; readers of the row ignore it
    absent_cycles = Column(Integer, nullable=True)
    created_at = Column(DateTime, nullable=False, default=func.now())
    updated_at = Column(DateTime, nullable=False, default=func.now(), onupdate=func.now())

    # Display-only, not columns: the Subscriptions page fills them per row before
    # handing the row to its template, which reads the same two names off a grouped
    # row and off a single one. Declared here so the shape holds for a row the page
    # renders by any other path, where the values are simply the empty ones.
    shared_queue_names = ()
    shared_acking = False


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


#: Declarative base of the watcher-process heartbeat table. Kept apart from
#: :data:`WatcherBase` so that :func:`ensure_table_exists`, which every Airflow process
#: runs from the plugin's ``on_load``, never issues DDL for it: only the watcher process
#: and the webserver create it, through :func:`ensure_heartbeat_table`.
HeartbeatBase = declarative_base()


@dataclass(frozen=True)
class WatcherIdentity:
    """One run of a watcher process: the key of its heartbeat row."""

    id: str
    hostname: str
    pid: int
    started_at: datetime


class RMQWatcherHeartbeat(HeartbeatBase):
    """Heartbeat of one watcher-process run — one row per :class:`WatcherIdentity`.

    Times are naive UTC, written by the watcher process. ``due_at`` is the moment by
    which this run, under the settings it works with, owes its next mark; a row whose
    ``due_at`` has passed without a ``stopped_at`` belongs to a run that is not working.
    """

    __tablename__ = "rmq_watcher_heartbeat"

    id = Column(String(36), primary_key=True)
    hostname = Column(String(500), nullable=False)
    pid = Column(Integer, nullable=False)
    started_at = Column(DateTime, nullable=False)
    # when the run recorded its last cycle that finished within its budget
    last_cycle_at = Column(DateTime, nullable=True)
    due_at = Column(DateTime, nullable=False)
    # set by an orderly stop only
    stopped_at = Column(DateTime, nullable=True)


#: Marker for an argument the caller left out, which ``None`` cannot express here:
#: ``None`` is itself a meaningful value for the nullable columns below. Callers pass
#: it to say "keep what the row holds".
KEEP: Any = object()


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
    """Create the :data:`WatcherBase` tables and add columns missing from an older schema.

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


_heartbeat_ready = False
_heartbeat_lock = threading.Lock()


def is_heartbeat_table_ready() -> bool:
    """Whether the heartbeat table is known to exist in this process."""
    return _heartbeat_ready


def ensure_heartbeat_table() -> None:
    """Create ``rmq_watcher_heartbeat`` if it is missing.

    Safe to call multiple times and from several threads: the threads of one process
    create the table one at a time, and once a call succeeds the table is marked ready
    and further calls return immediately. A creation that fails while the table exists
    afterwards — another process created it between the check and the ``CREATE`` —
    counts as a success. Any other failure propagates and leaves the flag unset, so the
    next call tries again.
    """
    global _heartbeat_ready
    if _heartbeat_ready:
        return
    from airflow.settings import engine
    with _heartbeat_lock:
        if _heartbeat_ready:
            return
        try:
            HeartbeatBase.metadata.create_all(engine, checkfirst=True)
        except Exception:
            if not inspect(engine).has_table(RMQWatcherHeartbeat.__tablename__):
                raise
        _heartbeat_ready = True


# ---------------------------------------------------------------------------
# CRUD helpers — callers provide the session; these do not commit
# ---------------------------------------------------------------------------

def _clear_absent_cycles(session: Session, sub: RMQSubscription) -> None:
    """Clear the miss counter of an existing row, as a statement the database runs.

    The caller writes the row from a source that says the key is there, so whatever
    misses stood behind it are over. Assigning the attribute would leave the decision
    to the session's own diff, and the session compares against the value it read some
    milliseconds earlier: one that read ``NULL`` and assigns ``None`` has nothing to
    flush, so a count another scheduler committed in between would survive the write
    and the next miss would delete the row — a successful scan notwithstanding.

    The ``IS NOT NULL`` predicate keeps the ordinary case a no-op the database
    decides on, and the loaded value is expired so that the object the caller gets
    back reads the counter from the row rather than from the stale load.
    """
    session.query(RMQSubscription).filter(
        RMQSubscription.id == sub.id,
        RMQSubscription.absent_cycles.isnot(None),
    ).update({"absent_cycles": None}, synchronize_session=False)
    session.expire(sub, ["absent_cycles"])


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
    ack_unmatched: bool = False,
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
        # a row that has just been created has missed no scan
    else:
        if source == "ui":
            sub.enabled = enabled
        # dag_file source: preserve current enabled value from DB
        _clear_absent_cycles(session, sub)
    sub.filter_data = filter_data or {}
    sub.source = source
    sub.trigger_mode = trigger_mode
    sub.group_key = group_key
    sub.cooldown = cooldown
    sub.ack_unmatched = ack_unmatched
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
    broker_consumer_count: int | None = KEEP,
    last_reconcile_at: datetime | None = KEEP,
) -> RMQConnStatus:
    """Insert or update connection status. Caller must commit.

    ``broker_consumer_count`` and ``last_reconcile_at`` distinguish two cases:
    an omitted argument keeps whatever the row holds, while an explicit None
    records "no data" — the broker could not be asked. ``last_error`` takes the same
    sentinel, for the caller that keeps a stored status: the text belongs to that
    status, and clearing it leaves a red row with nothing saying why.
    """
    row = session.query(RMQConnStatus).filter_by(conn_id=conn_id).first()
    if row is None:
        row = RMQConnStatus(conn_id=conn_id)
        session.add(row)
    row.status = status
    row.consumer_count = consumer_count
    if last_error is not KEEP:
        row.last_error = last_error
    if broker_consumer_count is not KEEP:
        row.broker_consumer_count = broker_consumer_count
    if last_reconcile_at is not KEEP:
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


# ---------------------------------------------------------------------------
# Heartbeat writes — each call is its own transaction and commits it
# ---------------------------------------------------------------------------

#: How long a row past its deadline keeps counting after its last mark: the verdict
#: and the Subscriptions page read such rows for this long, and a living watcher
#: deletes other runs' rows once it is over.
HEARTBEAT_WINDOW = timedelta(days=1)

#: Most heartbeat rows :func:`get_heartbeats` returns. A Scheduler that keeps
#: restarting adds a row per restart; the rows within their deadline come first, so
#: the limit only drops the oldest of the others.
HEARTBEAT_ROWS_LIMIT = 50


def _identity_values(identity: WatcherIdentity) -> dict[str, Any]:
    return {
        "hostname": identity.hostname,
        "pid": identity.pid,
        "started_at": identity.started_at,
    }


def _update_heartbeat(session: Session, row_id: str, values: dict[str, Any]) -> int:
    """UPDATE the row ``row_id`` with ``values``; return how many rows matched."""
    return (
        session.query(RMQWatcherHeartbeat)
        .filter(RMQWatcherHeartbeat.id == row_id)
        .update(values, synchronize_session=False)
    )


def _upsert_heartbeat(session: Session, identity: WatcherIdentity, **fields: Any) -> None:
    """Write ``fields`` and the identity onto the row of ``identity`` and commit.

    Columns outside ``fields`` keep what the row holds; a row that does not exist yet
    is inserted with them empty. An INSERT that fails on ``IntegrityError`` while the
    row exists — another writer committed it in between — is rolled back and the same
    values go in as an UPDATE, so the write lands on that row instead of being lost.
    An ``IntegrityError`` with no row behind it is a failed write and propagates.
    """
    values = {**_identity_values(identity), **fields}
    if _update_heartbeat(session, identity.id, values) == 0:
        try:
            session.execute(
                RMQWatcherHeartbeat.__table__.insert().values(id=identity.id, **values)
            )
        except IntegrityError:
            session.rollback()
            if _update_heartbeat(session, identity.id, values) == 0:
                raise
    session.commit()


def record_heartbeat_deadline(
    session: Session, identity: WatcherIdentity, due_at: datetime,
) -> None:
    """Record a deadline for the run's next mark without a completed cycle. Commits.

    The start of a run is recorded this way too. ``last_cycle_at`` and ``stopped_at``
    keep what the row already holds.
    """
    _upsert_heartbeat(session, identity, due_at=due_at)


def record_heartbeat_cycle(
    session: Session, identity: WatcherIdentity, last_cycle_at: datetime, due_at: datetime,
) -> None:
    """Record a cycle that finished within its budget and the next deadline. Commits.

    ``stopped_at`` keeps what the row already holds.
    """
    _upsert_heartbeat(session, identity, last_cycle_at=last_cycle_at, due_at=due_at)


def record_heartbeat_stop(
    session: Session, identity: WatcherIdentity, due_at: datetime, stopped_at: datetime,
) -> None:
    """Record an orderly stop of a watcher run. Commits.

    The write carries the full identity, so the mark lands even on a row the start
    record has not committed yet.
    """
    _upsert_heartbeat(session, identity, due_at=due_at, stopped_at=stopped_at)


def _last_mark_column():
    """A row's latest mark: ``stopped_at``, else ``last_cycle_at``, else ``started_at``."""
    return func.coalesce(
        RMQWatcherHeartbeat.stopped_at,
        RMQWatcherHeartbeat.last_cycle_at,
        RMQWatcherHeartbeat.started_at,
    )


def prune_heartbeats(session: Session, keep_id: str, now: datetime) -> int:
    """Delete other runs' rows that are past their deadline and were last marked more
    than :data:`HEARTBEAT_WINDOW` before ``now``. Commits.

    The row ``keep_id`` stays whatever its age, and so does a row whose deadline is
    still ahead. Returns the deleted count.
    """
    deleted = (
        session.query(RMQWatcherHeartbeat)
        .filter(
            RMQWatcherHeartbeat.id != keep_id,
            RMQWatcherHeartbeat.due_at < now,
            _last_mark_column() < now - HEARTBEAT_WINDOW,
        )
        .delete(synchronize_session=False)
    )
    session.commit()
    return deleted


def get_heartbeats(session: Session, now: datetime) -> list[RMQWatcherHeartbeat]:
    """Return the rows that count at ``now``.

    Every row whose deadline is still ahead (``due_at >= now``), then the rows past
    their deadline that were last marked within :data:`HEARTBEAT_WINDOW` of ``now``,
    latest mark first — at most :data:`HEARTBEAT_ROWS_LIMIT` rows in all.
    """
    current = case((RMQWatcherHeartbeat.due_at >= now, 0), else_=1)
    return (
        session.query(RMQWatcherHeartbeat)
        .filter(
            or_(
                RMQWatcherHeartbeat.due_at >= now,
                _last_mark_column() >= now - HEARTBEAT_WINDOW,
            )
        )
        .order_by(current, _last_mark_column().desc())
        .limit(HEARTBEAT_ROWS_LIMIT)
        .all()
    )
