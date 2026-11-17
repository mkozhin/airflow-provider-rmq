"""Tests for the watcher banner on Airflow's home page: the cached verdict it answers
from, the alert Airflow asks and renders, and its installation in the webserver.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import flask
import pytest
from markupsafe import Markup
from sqlalchemy import inspect
from sqlalchemy.orm import sessionmaker

from airflow_provider_rmq.watcher import dashboard_alert, liveness, models
from airflow_provider_rmq.watcher.listener import _VARIABLE_TIMEOUT
from airflow_provider_rmq.watcher.liveness import LivenessState, assess_liveness
from airflow_provider_rmq.watcher.models import (
    RMQConnStatus,
    RMQSubscription,
    RMQWatcherHeartbeat,
    WatcherBase,
)
from airflow_provider_rmq.watcher.tunables import DEFAULT_RECONCILE_INTERVAL, MIN_CYCLE_TIMEOUT

#: Reference moment of the tests, naive UTC like the times the watcher writes.
T0 = datetime(2026, 11, 17, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)

#: The reconcile interval and the cycle budget with no Variable set.
I = DEFAULT_RECONCILE_INTERVAL
B = MIN_CYCLE_TIMEOUT

_LOGGER = "airflow_provider_rmq.watcher.dashboard_alert"


class Clock:
    """Stand-in for :func:`time.monotonic` that moves only when told to."""

    def __init__(self) -> None:
        self.now = 1000.0

    def monotonic(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture(autouse=True)
def empty_cache(monkeypatch):
    """Every test starts with a cache that holds nothing."""
    monkeypatch.setattr(dashboard_alert, "_cache", (float("-inf"), None))


@pytest.fixture
def clock(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(dashboard_alert, "time", SimpleNamespace(monotonic=clock.monotonic))
    return clock


@pytest.fixture
def db(schema_engine, monkeypatch):
    """The metadata database of the webserver: the watcher tables without the heartbeat
    table, Variables unset, and ``now`` fixed at :data:`T0`."""
    WatcherBase.metadata.create_all(schema_engine)
    monkeypatch.setattr(dashboard_alert, "WatcherSession", sessionmaker(bind=schema_engine))
    monkeypatch.setattr(
        "airflow_provider_rmq.watcher.views.read_positive", lambda name, cast: None
    )
    monkeypatch.setattr(dashboard_alert, "_utcnow", lambda: T0)
    return schema_engine


def _add(engine, *objects) -> None:
    with sessionmaker(bind=engine)() as session:
        session.add_all(objects)
        session.commit()


def _subscription(enabled: bool = True) -> RMQSubscription:
    return RMQSubscription(
        dag_id="dag", queue_name="q", conn_id="rmq_default", source="ui", enabled=enabled
    )


def _stamp(age_seconds: float) -> RMQConnStatus:
    return RMQConnStatus(
        conn_id="rmq_default", last_reconcile_at=T0 - timedelta(seconds=age_seconds)
    )


def _alive_heartbeat() -> RMQWatcherHeartbeat:
    return RMQWatcherHeartbeat(
        id="run-1",
        hostname="scheduler-1",
        pid=4242,
        started_at=T0 - timedelta(minutes=10),
        last_cycle_at=T0 - timedelta(minutes=1),
        due_at=T0 + timedelta(minutes=5),
    )


def _has_heartbeat_table(engine) -> bool:
    return inspect(engine).has_table("rmq_watcher_heartbeat")


def _refusing_ensure(monkeypatch, failures: int):
    """Make the heartbeat table creation fail ``failures`` times, then work; return the
    list of call outcomes."""
    calls: list[str] = []

    def ensure() -> None:
        if len([c for c in calls if c == "failed"]) < failures:
            calls.append("failed")
            raise RuntimeError("permission denied for schema public")
        calls.append("created")
        models.ensure_heartbeat_table()

    monkeypatch.setattr(dashboard_alert, "ensure_heartbeat_table", ensure)
    return calls


# ---------------------------------------------------------------------------
# Reading the verdict
# ---------------------------------------------------------------------------


class TestReadVerdict:
    def test_a_creation_that_failed_at_install_is_retried_on_the_next_miss(
        self, db, clock, monkeypatch
    ):
        calls = _refusing_ensure(monkeypatch, failures=1)
        monkeypatch.setattr("airflow.settings.DASHBOARD_UIALERTS", [])

        dashboard_alert.install_dashboard_alert()
        assert calls == ["failed"]
        assert not _has_heartbeat_table(db)

        dashboard_alert.cached_verdict()

        assert calls == ["failed", "created"]
        assert _has_heartbeat_table(db)
        assert models.is_heartbeat_table_ready()

    def test_creation_stops_once_the_table_exists(self, db, clock, monkeypatch):
        calls = _refusing_ensure(monkeypatch, failures=0)

        dashboard_alert.cached_verdict()
        clock.advance(dashboard_alert._CACHE_SECONDS)
        dashboard_alert.cached_verdict()

        assert calls == ["created"]

    def test_without_the_table_a_stale_stamp_is_down(self, db, clock, monkeypatch, caplog):
        _refusing_ensure(monkeypatch, failures=99)
        _add(db, _subscription(), _stamp(age_seconds=3600))

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            verdict = dashboard_alert.cached_verdict()

        assert verdict is not None
        assert verdict.state is LivenessState.DOWN
        assert verdict.processes == ()
        assert not _has_heartbeat_table(db)
        assert "last_reconcile_at alone" in caplog.text

    def test_without_the_table_a_fresh_stamp_is_unconfirmed(self, db, clock, monkeypatch):
        _refusing_ensure(monkeypatch, failures=99)
        _add(db, _subscription(), _stamp(age_seconds=30))

        verdict = dashboard_alert.cached_verdict()

        assert verdict.state is LivenessState.UNCONFIRMED

    def test_a_stamp_one_interval_one_budget_and_ten_seconds_old_is_fresh(self, db, clock):
        _add(db, _subscription(), _stamp(age_seconds=I + B + 10))

        assert dashboard_alert.cached_verdict().state is LivenessState.UNCONFIRMED

    def test_a_stamp_past_the_whole_threshold_is_stale(self, db, clock):
        threshold = I + B + _VARIABLE_TIMEOUT + dashboard_alert._RECONCILE_TAIL
        _add(db, _subscription(), _stamp(age_seconds=threshold + 1))

        assert dashboard_alert.cached_verdict().state is LivenessState.DOWN

    def test_a_stamp_at_the_threshold_is_still_fresh(self, db, clock):
        threshold = I + B + _VARIABLE_TIMEOUT + dashboard_alert._RECONCILE_TAIL
        _add(db, _subscription(), _stamp(age_seconds=threshold))

        assert dashboard_alert.cached_verdict().state is LivenessState.UNCONFIRMED

    def test_an_aware_stamp_is_compared_as_utc(self, db, monkeypatch):
        with sessionmaker(bind=db)() as session:
            monkeypatch.setattr(
                session,
                "query",
                lambda *a: SimpleNamespace(
                    scalar=lambda: (T0 - timedelta(seconds=30)).replace(tzinfo=timezone.utc)
                ),
            )
            assert dashboard_alert._reconcile_is_fresh(session, T0) is True

    def test_the_stamp_and_threshold_are_not_read_while_a_run_is_alive(
        self, db, clock, monkeypatch
    ):
        models.ensure_heartbeat_table()
        _add(db, _subscription(), _alive_heartbeat())

        def refuse():
            raise AssertionError("the threshold is read only without an alive run")

        monkeypatch.setattr(dashboard_alert, "_stale_after", refuse)

        verdict = dashboard_alert.cached_verdict()

        assert verdict.state is LivenessState.OK
        assert [p.id for p in verdict.processes] == ["run-1"]

    def test_no_enabled_subscription_is_unused(self, db, clock):
        _add(db, _subscription(enabled=False), _stamp(age_seconds=3600))

        assert dashboard_alert.cached_verdict().state is LivenessState.UNUSED

    def test_rows_older_than_the_window_are_not_read(self, db, clock):
        models.ensure_heartbeat_table()
        old = _alive_heartbeat()
        old.last_cycle_at = T0 - dashboard_alert.HEARTBEAT_WINDOW - timedelta(minutes=1)
        old.due_at = old.last_cycle_at + timedelta(minutes=5)
        _add(db, _subscription(), old)

        verdict = dashboard_alert.cached_verdict()

        assert verdict.state is LivenessState.DOWN
        assert verdict.processes == ()


class TestCache:
    def test_the_database_is_not_read_again_within_the_period(self, clock, monkeypatch):
        reads: list[int] = []

        def read():
            reads.append(1)
            return assess_liveness([], T0, True, False)

        monkeypatch.setattr(dashboard_alert, "_read_verdict", read)

        first = dashboard_alert.cached_verdict()
        clock.advance(dashboard_alert._CACHE_SECONDS - 0.1)
        second = dashboard_alert.cached_verdict()

        assert len(reads) == 1
        assert second is first

        clock.advance(0.1)
        dashboard_alert.cached_verdict()

        assert len(reads) == 2

    def test_a_failed_read_hides_the_banner_and_warns_once_per_period(
        self, clock, monkeypatch, caplog
    ):
        def broken():
            raise RuntimeError("could not connect to server")

        monkeypatch.setattr(dashboard_alert, "WatcherSession", broken)

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            assert dashboard_alert.cached_verdict() is None
            clock.advance(dashboard_alert._CACHE_SECONDS - 1)
            assert dashboard_alert.cached_verdict() is None
            warnings = [r for r in caplog.records if "failed to read" in r.getMessage()]
            assert len(warnings) == 1

            clock.advance(1)
            assert dashboard_alert.cached_verdict() is None
            warnings = [r for r in caplog.records if "failed to read" in r.getMessage()]
            assert len(warnings) == 2

    def test_the_cache_entry_is_one_immutable_pair(self, clock, monkeypatch):
        verdict = assess_liveness([], T0, True, False)
        monkeypatch.setattr(dashboard_alert, "_read_verdict", lambda: verdict)

        dashboard_alert.cached_verdict()

        assert dashboard_alert._cache == (clock.now + dashboard_alert._CACHE_SECONDS, verdict)
        assert isinstance(dashboard_alert._cache, tuple)


# ---------------------------------------------------------------------------
# The alert
# ---------------------------------------------------------------------------


def _row(**overrides):
    values = {
        "id": "run-1",
        "hostname": "scheduler-1",
        "pid": 4242,
        "started_at": T0 - timedelta(minutes=10),
        "last_cycle_at": T0 - timedelta(minutes=1),
        "due_at": T0 + timedelta(minutes=5),
        "stopped_at": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _verdict(state: LivenessState):
    if state is LivenessState.UNUSED:
        return assess_liveness([], T0, False, False)
    if state is LivenessState.OK:
        return assess_liveness([_row()], T0, True, False)
    if state is LivenessState.STARTING:
        return assess_liveness([_row(last_cycle_at=None)], T0, True, False)
    if state is LivenessState.UNCONFIRMED:
        return assess_liveness([], T0, True, True)
    return assess_liveness(
        [_row(stopped_at=T0 - timedelta(minutes=2))], T0, True, False
    )


@pytest.fixture
def app():
    """An application serving the Subscriptions endpoint under the name FAB gives it."""
    app = flask.Flask(__name__)
    app.add_url_rule(
        "/rmq-watcher/subscriptions",
        endpoint="RMQWatcherView.subscriptions",
        view_func=lambda: "",
    )
    return app


@pytest.fixture
def alert():
    return dashboard_alert._make_alert()


def _answer_with(monkeypatch, verdict) -> None:
    monkeypatch.setattr(dashboard_alert, "cached_verdict", lambda: verdict)


class TestAlert:
    def test_the_alert_is_a_uialert_shown_to_every_user(self, alert):
        from airflow.www.utils import UIAlert

        assert isinstance(alert, UIAlert)
        assert alert.roles is None

    @pytest.mark.parametrize("state", list(LivenessState))
    def test_should_show_is_true_only_for_down(self, app, alert, monkeypatch, state):
        _answer_with(monkeypatch, _verdict(state))

        with app.test_request_context("/home"):
            shown = alert.should_show(SimpleNamespace())

        assert shown is (state is LivenessState.DOWN)

    def test_should_show_takes_the_appbuilder_in_any_form(self, app, alert, monkeypatch):
        _answer_with(monkeypatch, _verdict(LivenessState.DOWN))

        with app.test_request_context("/home"):
            assert alert.should_show() is True
            assert alert.should_show(SimpleNamespace(), extra=1) is True
            assert alert.should_show(appbuilder=SimpleNamespace()) is True

    def test_down_renders_as_an_error_with_a_real_link(self, app, alert, monkeypatch):
        _answer_with(monkeypatch, _verdict(LivenessState.DOWN))

        with app.test_request_context("/home", base_url="http://airflow.local/airflow"):
            alert.should_show(SimpleNamespace())
            message = alert.message
            category = alert.category
            rendered = flask.render_template_string("{{ m }}", m=message)

        assert isinstance(message, Markup)
        assert category == "error"
        link = '<a href="/airflow/rmq-watcher/subscriptions">RMQ Subscriptions</a>'
        assert link in message
        assert link in rendered
        assert "stopped at 2026-11-17 11:58:00 UTC" in message

    def test_the_substituted_time_is_escaped(self, app, alert, monkeypatch):
        _answer_with(monkeypatch, _verdict(LivenessState.DOWN))
        monkeypatch.setattr(liveness, "_format_time", lambda value: "<b>noon</b>")

        with app.test_request_context("/home"):
            alert.should_show(SimpleNamespace())
            message = alert.message

        assert "&lt;b&gt;noon&lt;/b&gt;" in message
        assert "<b>" not in message
        assert '<a href="/rmq-watcher/subscriptions">' in message

    def test_without_the_endpoint_the_link_is_the_page_path(self, alert, monkeypatch):
        _answer_with(monkeypatch, _verdict(LivenessState.DOWN))

        with flask.Flask(__name__).test_request_context("/home"):
            alert.should_show(SimpleNamespace())
            message = alert.message

        assert f'<a href="{liveness.SUBSCRIPTIONS_PATH}">' in message

    def test_message_and_category_read_the_request_and_not_the_database(
        self, app, alert, monkeypatch
    ):
        _answer_with(monkeypatch, _verdict(LivenessState.DOWN))

        with app.test_request_context("/home"):
            alert.should_show(SimpleNamespace())

            def refuse():
                raise AssertionError("rendering must not read the verdict again")

            monkeypatch.setattr(dashboard_alert, "cached_verdict", refuse)
            assert alert.message
            assert alert.category == "error"

    def test_a_cache_that_expires_between_decision_and_render_keeps_the_text(
        self, app, alert, clock, monkeypatch
    ):
        verdicts = iter([_verdict(LivenessState.DOWN), _verdict(LivenessState.OK)])
        monkeypatch.setattr(dashboard_alert, "_read_verdict", lambda: next(verdicts))

        with app.test_request_context("/home"):
            assert alert.should_show(SimpleNamespace()) is True
            decided = alert.message

            clock.advance(dashboard_alert._CACHE_SECONDS + 1)
            assert dashboard_alert.cached_verdict().state is LivenessState.OK

            assert alert.message == decided
            assert alert.category == "error"

    def test_a_request_without_a_decision_renders_nothing(self, app, alert):
        with app.test_request_context("/home"):
            assert alert.message == Markup("")
            assert alert.category == "info"
        assert alert.message == Markup("")
        assert alert.category == "info"

    def test_a_failed_read_hides_the_banner_without_an_exception(
        self, app, alert, clock, monkeypatch
    ):
        def broken():
            raise RuntimeError("could not connect to server")

        monkeypatch.setattr(dashboard_alert, "WatcherSession", broken)

        with app.test_request_context("/home"):
            assert alert.should_show(SimpleNamespace()) is False
            assert alert.message == Markup("")
            assert alert.category == "info"

    def test_any_failure_of_the_decision_hides_the_banner(
        self, app, alert, monkeypatch, caplog
    ):
        def broken():
            raise RuntimeError("unexpected")

        monkeypatch.setattr(dashboard_alert, "cached_verdict", broken)

        with app.test_request_context("/home"), caplog.at_level(
            logging.WARNING, logger=_LOGGER
        ):
            assert alert.should_show(SimpleNamespace()) is False

        assert "failed to decide on the watcher banner" in caplog.text


# ---------------------------------------------------------------------------
# Installation
# ---------------------------------------------------------------------------


@pytest.fixture
def no_ddl(monkeypatch):
    calls: list[int] = []
    monkeypatch.setattr(dashboard_alert, "ensure_heartbeat_table", lambda: calls.append(1))
    return calls


def _ours(alerts):
    return [a for a in alerts if getattr(a, dashboard_alert._MARKER, False) is True]


class TestInstall:
    def test_one_instance_is_added_and_the_existing_alerts_are_kept(
        self, monkeypatch, no_ddl
    ):
        from airflow import settings

        existing = object()
        monkeypatch.setattr(settings, "DASHBOARD_UIALERTS", [existing])

        dashboard_alert.install_dashboard_alert()
        dashboard_alert.install_dashboard_alert()

        alerts = settings.DASHBOARD_UIALERTS
        assert alerts[0] is existing
        assert len(alerts) == 2
        assert len(_ours(alerts)) == 1

    def test_a_tuple_from_local_settings_is_kept_in_a_new_list(self, monkeypatch, no_ddl):
        from airflow import settings

        first, second = object(), object()
        monkeypatch.setattr(settings, "DASHBOARD_UIALERTS", (first, second))

        dashboard_alert.install_dashboard_alert()

        alerts = settings.DASHBOARD_UIALERTS
        assert isinstance(alerts, list)
        assert alerts[:2] == [first, second]
        assert len(_ours(alerts)) == 1

    def test_installation_creates_the_heartbeat_table(self, schema_engine, monkeypatch):
        monkeypatch.setattr("airflow.settings.DASHBOARD_UIALERTS", [])

        dashboard_alert.install_dashboard_alert()

        assert _has_heartbeat_table(schema_engine)

    def test_a_failed_creation_still_installs_the_banner(self, monkeypatch, caplog):
        from airflow import settings

        def refuse():
            raise RuntimeError("permission denied")

        monkeypatch.setattr(dashboard_alert, "ensure_heartbeat_table", refuse)
        monkeypatch.setattr(settings, "DASHBOARD_UIALERTS", [])

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            dashboard_alert.install_dashboard_alert()

        assert len(_ours(settings.DASHBOARD_UIALERTS)) == 1
        assert "failed to create the rmq_watcher_heartbeat table" in caplog.text
