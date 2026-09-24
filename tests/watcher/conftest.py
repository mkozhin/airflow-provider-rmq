from __future__ import annotations

import signal

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from airflow_provider_rmq.watcher import models
from airflow_provider_rmq.watcher.models import WatcherBase

#: rmq_watcher_subscriptions as an installation that has not run the migration yet has
#: it: the columns of the first release, without the miss counter and without the
#: unmatched-ack flag. The table the migration has to bring up to date, and the table
#: the Subscriptions page has to survive reading.
SUBSCRIPTIONS_DDL_PRE_MIGRATION = """
CREATE TABLE rmq_watcher_subscriptions (
    id INTEGER NOT NULL PRIMARY KEY,
    dag_id VARCHAR(250) NOT NULL,
    queue_name VARCHAR(250) NOT NULL,
    conn_id VARCHAR(250) NOT NULL,
    filter_data JSON,
    source VARCHAR(10) NOT NULL,
    enabled BOOLEAN NOT NULL,
    consumer_status VARCHAR(20) NOT NULL,
    last_error TEXT,
    trigger_mode VARCHAR(20) NOT NULL,
    group_key VARCHAR(250),
    cooldown INTEGER,
    created_at DATETIME NOT NULL,
    updated_at DATETIME NOT NULL,
    CONSTRAINT uq_rmq_sub_dag_queue_conn UNIQUE (dag_id, queue_name, conn_id)
)
"""


@pytest.fixture(autouse=True)
def alarm_calls(monkeypatch):
    """Replace the real-time timer of :func:`signal.setitimer` in every test here and
    return the seconds it was set to.

    The watcher process arms a SIGALRM deadline once it decides to leave, the loop's
    parent check among the places that do; a real timer in the pytest process would end
    the whole run once it fired. The replacement keeps the value it was last set to and
    :func:`signal.getitimer` reports it, with no time passing; the other timers are the
    real ones.
    """
    calls: list[float] = []
    armed = [0.0]
    real_setitimer = signal.setitimer
    real_getitimer = signal.getitimer

    def setitimer(which: int, seconds: float, interval: float = 0.0) -> tuple[float, float]:
        if which != signal.ITIMER_REAL:
            return real_setitimer(which, seconds, interval)
        calls.append(seconds)
        previous = armed[0]
        armed[0] = float(seconds)
        return previous, 0.0

    def getitimer(which: int) -> tuple[float, float]:
        if which != signal.ITIMER_REAL:
            return real_getitimer(which)
        return armed[0], 0.0

    monkeypatch.setattr(signal, "setitimer", setitimer)
    monkeypatch.setattr(signal, "getitimer", getitimer)
    return calls


def _memory_engine():
    """In-memory SQLite engine that is one connection wide and shared across threads.

    ``StaticPool`` is what makes the database visible outside the thread that created
    it: SQLAlchemy hands an in-memory SQLite a ``SingletonThreadPool`` by default, so
    every worker of the watcher's cycle pool would open a connection of its own and
    find an empty database behind it. One shared connection also needs
    ``check_same_thread=False`` from the sqlite3 driver.
    """
    return create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )


@pytest.fixture(scope="function")
def session():
    """SQLite in-memory session with fresh schema per test."""
    engine = _memory_engine()
    WatcherBase.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session()
    yield s
    s.close()
    WatcherBase.metadata.drop_all(engine)


@pytest.fixture(scope="function")
def schema_engine(monkeypatch):
    """Engine standing in for Airflow's, with the migration flag reset."""
    engine = _memory_engine()
    monkeypatch.setattr("airflow.settings.engine", engine, raising=False)
    monkeypatch.setattr(models, "_schema_ready", False)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def dagmodel_engine():
    """In-memory engine carrying the watcher schema plus Airflow's DagModel table.

    ``get_active_dag_ids`` reads the ``dag`` table, and a test that leaves it out gets
    a ``no such table: dag`` swallowed by the "a failed read means every DAG is
    active" wrapper — which turns both branches of the short circuit green for the
    wrong reason.
    """
    from airflow.models import DagModel

    engine = _memory_engine()
    WatcherBase.metadata.create_all(engine)
    DagModel.__table__.create(engine, checkfirst=True)
    yield engine
    engine.dispose()


@pytest.fixture(scope="function")
def session_with_dagmodel(dagmodel_engine):
    """SQLite in-memory session with watcher schema plus Airflow's DagModel table."""
    s = sessionmaker(bind=dagmodel_engine)()
    yield s
    s.close()
