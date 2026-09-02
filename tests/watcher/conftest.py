from __future__ import annotations

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from airflow_provider_rmq.watcher import models
from airflow_provider_rmq.watcher.models import WatcherBase


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
