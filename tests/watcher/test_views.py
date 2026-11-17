"""Tests for RMQWatcherView — business logic, mocked DB and template rendering.

Flask-AppBuilder's ``@has_access`` is patched to a no-op before the views
module is (re)loaded, so view methods can be called directly in tests without
a logged-in user or a full Airflow web-app context.
"""
from __future__ import annotations

import importlib
import logging
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask
from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker

from airflow_provider_rmq.watcher.liveness import LivenessState, assess_liveness
from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    RMQWatcherHeartbeat,
    WatcherBase,
    ensure_heartbeat_table,
)
from tests.watcher.conftest import SUBSCRIPTIONS_DDL_PRE_MIGRATION

# ---------------------------------------------------------------------------
# Patch has_access → identity before loading views, so the class is built
# without the access check. reload() handles the case where the module was
# already cached from a previous import in this pytest session.
# ---------------------------------------------------------------------------
import flask_appbuilder.security.decorators as _fab_sec_dec  # noqa: E402

_orig_has_access = _fab_sec_dec.has_access
_fab_sec_dec.has_access = lambda f: f  # no-op for tests

if "airflow_provider_rmq.watcher.views" in sys.modules:
    importlib.reload(sys.modules["airflow_provider_rmq.watcher.views"])

from airflow_provider_rmq.watcher.views import (  # noqa: E402
    DEFAULT_RECONCILE_INTERVAL,
    LIVENESS_UNAVAILABLE_MESSAGE,
    SCHEMA_OUTDATED_MESSAGE,
    SHARED_QUEUE_ACK_WARNING,
    SHARED_QUEUE_WARNING,
    SUBSCRIPTIONS_SCHEMA_OUTDATED_MESSAGE,
    UNCONFIRMED_NOTE,
    ConnStatusRow,
    RMQWatcherView,
    SubscriptionGroup,
    _build_conn_status_rows,
    _group_subscriptions,
    _read_liveness,
    _reconcile_interval,
    _stale_after,
)

_fab_sec_dec.has_access = _orig_has_access  # restore for the rest of the session


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sub(
    *,
    id: int = 1,
    dag_id: str = "my_dag",
    queue_name: str = "my_queue",
    conn_id: str = "rmq_default",
    source: str = "ui",
    enabled: bool = True,
    consumer_status: str = "listening",
    last_error: str | None = None,
    group_key: str | None = None,
    cooldown: int | None = None,
    ack_unmatched: bool = False,
) -> MagicMock:
    sub = MagicMock()
    sub.id = id
    sub.dag_id = dag_id
    sub.queue_name = queue_name
    sub.conn_id = conn_id
    sub.source = source
    sub.enabled = enabled
    sub.consumer_status = consumer_status
    sub.last_error = last_error
    sub.group_key = group_key
    sub.cooldown = cooldown
    sub.ack_unmatched = ack_unmatched
    return sub


def _make_conn_status_row(
    *,
    conn_id: str = "rmq_default",
    status: str = "connected",
    consumer_count: int = 1,
    broker_consumer_count: int | None = 1,
    last_error: str | None = None,
    last_reconcile_at: datetime | None = None,
) -> SimpleNamespace:
    """Real (non-MagicMock) stand-in for an RMQConnStatus row."""
    return SimpleNamespace(
        conn_id=conn_id,
        status=status,
        consumer_count=consumer_count,
        broker_consumer_count=broker_consumer_count,
        last_error=last_error,
        last_reconcile_at=last_reconcile_at,
    )


def _naive_utc_ago(seconds: float) -> datetime:
    """Naive-UTC timestamp `seconds` in the past — the form the watcher writes."""
    return datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=seconds)


#: Reference moment of the liveness tests, naive UTC like the times the watcher writes.
_T0 = datetime(2026, 11, 17, 12, 0, 0, tzinfo=timezone.utc).replace(tzinfo=None)


def _session_ctx(first_sub=None, subs=None):
    """Return (ctx, session) mock pair for WatcherSession."""
    session = MagicMock()
    query = session.query.return_value
    query.order_by.return_value.all.return_value = subs or []
    query.filter_by.return_value.first.return_value = first_sub
    ctx = MagicMock()
    ctx.__enter__.return_value = session
    ctx.__exit__.return_value = False
    return ctx, session


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def app():
    a = Flask(__name__)
    a.config["TESTING"] = True
    a.config["SECRET_KEY"] = "test-secret"
    a.config["WTF_CSRF_ENABLED"] = False
    return a


@pytest.fixture
def view():
    v = RMQWatcherView()
    v.render_template = MagicMock(return_value="<html>mocked</html>")
    return v


@pytest.fixture(autouse=True)
def unused_liveness(monkeypatch):
    """Every page request reads a verdict of no enabled subscriptions and no runs.

    The watcher liveness is read in a session of its own; a test about the page's
    other blocks keeps its session mocks to the blocks it is about. Tests of the
    liveness block set their own verdict over this one.
    """
    monkeypatch.setattr(
        "airflow_provider_rmq.watcher.views._read_liveness",
        lambda: assess_liveness([], _T0, False, False),
    )


# ---------------------------------------------------------------------------
# GET /subscriptions — list page
# ---------------------------------------------------------------------------

class TestSubscriptionsList:
    def test_subscriptions_list_returns_200(self, app, view):
        """subscriptions() renders the template (i.e. would return 200)."""
        ctx, _ = _session_ctx()
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[]), \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
            result = view.subscriptions()

        assert result == "<html>mocked</html>"
        view.render_template.assert_called_once()
        args = view.render_template.call_args
        assert args.args[0] == "rmq_watcher/subscriptions.html"

    def test_subscriptions_list_shows_conn_status(self, app, view):
        """render_template receives conn_statuses from DB."""
        cs = _make_conn_status_row()
        ctx, _ = _session_ctx()
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[cs]), \
             patch("airflow_provider_rmq.watcher.views._reconcile_interval", return_value=60), \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
            view.subscriptions()

        kwargs = view.render_template.call_args.kwargs
        rows = kwargs["conn_statuses"]
        assert [r.conn_id for r in rows] == [cs.conn_id]
        assert rows[0].status == cs.status
        assert kwargs["conn_status_error"] is None

    def test_subscriptions_list_shows_consumer_status_badge(self, app, view):
        """render_template receives the subscriptions list (badges are rendered from it)."""
        sub = _make_sub(consumer_status="error")
        ctx, _ = _session_ctx(subs=[sub])
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[]), \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
            view.subscriptions()

        kwargs = view.render_template.call_args.kwargs
        assert sub in kwargs["subscriptions"]

    def test_subscriptions_list_passes_active_dag_ids(self, app, view):
        """render_template receives active_dag_ids as returned by the
        lookup — this is only about the view passing the value through, not
        about the badge appearing in HTML (that's covered in Task 2)."""
        sub = _make_sub(dag_id="ghost_dag")
        ctx, _ = _session_ctx(subs=[sub])
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[]), \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value={"other_dag"}):
            view.subscriptions()

        kwargs = view.render_template.call_args.kwargs
        assert kwargs["active_dag_ids"] == {"other_dag"}

    def test_subscriptions_list_active_dag_ids_none_on_lookup_failure(self, app, view, caplog):
        """A failing dag-lookup must not propagate: subscriptions() still
        renders, active_dag_ids is None, and the failure is logged."""
        ctx, _ = _session_ctx()
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[]), \
             patch(
                 "airflow_provider_rmq.watcher.views.get_active_dag_ids",
                 side_effect=RuntimeError("boom"),
             ), \
             caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.watcher.views"):
            result = view.subscriptions()

        assert result == "<html>mocked</html>"
        kwargs = view.render_template.call_args.kwargs
        assert kwargs["active_dag_ids"] is None
        warning_records = [
            r
            for r in caplog.records
            if r.levelno == logging.WARNING
            and r.name == "airflow_provider_rmq.watcher.views"
        ]
        assert len(warning_records) == 1
        assert warning_records[0].exc_info is not None

    def test_subscriptions_list_dag_lookup_uses_separate_session(self, app, view):
        """The mock-based tests above cannot distinguish correct session
        isolation from an implementation that shares the `subs` session and
        calls rollback() on failure — both would pass those tests
        identically. This asserts the isolation itself: WatcherSession is
        opened twice, with two distinct session objects, and the main
        session's rollback() is never invoked.

        ``side_effect`` is a callable (not a list of two context managers) —
        a list would raise StopIteration if the view ever opened a session a
        third time.
        """
        contexts = []

        def _new_ctx(*args, **kwargs):
            session = MagicMock()
            query = session.query.return_value
            query.order_by.return_value.all.return_value = []
            ctx = MagicMock()
            ctx.__enter__.return_value = session
            ctx.__exit__.return_value = False
            contexts.append(ctx)
            return ctx

        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", side_effect=_new_ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[]), \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids") as mock_get_active:
            view.subscriptions()

        assert len(contexts) == 2
        main_session = contexts[0].__enter__.return_value
        dag_session = contexts[1].__enter__.return_value
        assert main_session is not dag_session
        mock_get_active.assert_called_once_with(dag_session)
        main_session.rollback.assert_not_called()


# ---------------------------------------------------------------------------
# POST /subscriptions/create
# ---------------------------------------------------------------------------

class TestCreateSubscription:
    def test_create_subscription_post_valid(self, app, view):
        """Valid POST inserts subscription and redirects."""
        ctx, session = _session_ctx()
        mock_redirect = MagicMock(return_value="redirect-response")
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={"dag_id": "dag1", "queue_name": "q1", "conn_id": "rmq_default"},
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", mock_redirect), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            result = view.create()

        mock_upsert.assert_called_once()
        call_kwargs = mock_upsert.call_args.kwargs
        assert call_kwargs["dag_id"] == "dag1"
        assert call_kwargs["queue_name"] == "q1"
        assert call_kwargs["source"] == "ui"
        session.commit.assert_called_once()
        assert result == "redirect-response"

    def test_create_subscription_post_invalid(self, app, view):
        """POST without dag_id renders form with error (no redirect)."""
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={"dag_id": "", "queue_name": "q1"},
        ), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession"):
            view.create()

        mock_flash.assert_called_once()
        flash_args = mock_flash.call_args.args
        assert "required" in flash_args[0]
        view.render_template.assert_called_once_with(
            "rmq_watcher/subscription_form.html",
            sub=None,
            is_dag_file=False,
            ack_unmatched_checked=False,
        )

    def test_create_subscription_post_invalid_includes_is_dag_file_false(self, app, view):
        """V1: render_template при ошибке валидации получает is_dag_file=False."""
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={"dag_id": "", "queue_name": "q1"},
        ), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession"):
            view.create()

        kwargs = view.render_template.call_args.kwargs
        assert kwargs.get("is_dag_file") is False

    def test_create_subscription_get_renders_form(self, app, view):
        """GET renders empty form."""
        with app.test_request_context("/subscriptions/create", method="GET"):
            view.create()

        view.render_template.assert_called_once_with(
            "rmq_watcher/subscription_form.html",
            sub=None,
            is_dag_file=False,
            ack_unmatched_checked=False,
        )


# ---------------------------------------------------------------------------
# POST /subscriptions/<id>/delete
# ---------------------------------------------------------------------------

class TestDeleteSubscription:
    def test_delete_ui_subscription(self, app, view):
        """DELETE ui subscription removes it from DB."""
        sub = _make_sub(source="ui")
        ctx, session = _session_ctx(first_sub=sub)
        mock_redirect = MagicMock(return_value="redirect-response")
        with app.test_request_context("/subscriptions/1/delete", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", mock_redirect), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            result = view.delete(1)

        session.delete.assert_called_once_with(sub)
        session.commit.assert_called_once()
        assert result == "redirect-response"

    def test_delete_dag_file_subscription_rejected(self, app, view):
        """DELETE dag_file subscription shows error flash and does NOT delete."""
        sub = _make_sub(source="dag_file")
        ctx, session = _session_ctx(first_sub=sub)
        mock_flash = MagicMock()
        with app.test_request_context("/subscriptions/1/delete", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.delete(1)

        session.delete.assert_not_called()
        mock_flash.assert_called_once()
        flash_category = mock_flash.call_args.args[1]
        assert flash_category == "error"


# ---------------------------------------------------------------------------
# POST /subscriptions/<id>/toggle
# ---------------------------------------------------------------------------

class TestToggleSubscription:
    def test_toggle_subscription_enabled_to_disabled(self, app, view):
        """toggle() inverts the enabled flag and commits."""
        sub = _make_sub(enabled=True)
        ctx, session = _session_ctx(first_sub=sub)
        with app.test_request_context("/subscriptions/1/toggle", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.toggle(1)

        assert sub.enabled is False
        session.commit.assert_called_once()

    def test_toggle_subscription_disabled_to_enabled(self, app, view):
        """toggle() inverts False → True."""
        sub = _make_sub(enabled=False)
        ctx, session = _session_ctx(first_sub=sub)
        with app.test_request_context("/subscriptions/1/toggle", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.toggle(1)

        assert sub.enabled is True
        session.commit.assert_called_once()


# ---------------------------------------------------------------------------
# _group_subscriptions helper
# ---------------------------------------------------------------------------

class TestGroupSubscriptions:
    def test_no_group_key_returns_ungrouped(self):
        """Subs without group_key are returned as-is."""
        sub = _make_sub(group_key=None)
        result = _group_subscriptions([sub])
        assert result == [sub]

    def test_shared_group_key_creates_one_group(self):
        """Two subs with the same group_key produce one SubscriptionGroup."""
        sub1 = _make_sub(id=1, queue_name="q1", group_key="my_dag", cooldown=300)
        sub2 = _make_sub(id=2, queue_name="q2", group_key="my_dag", cooldown=300)
        result = _group_subscriptions([sub1, sub2])
        assert len(result) == 1
        grp = result[0]
        assert isinstance(grp, SubscriptionGroup)
        assert set(grp.queue_names) == {"q1", "q2"}
        assert grp.sub_ids == [1, 2]
        assert grp.cooldown == 300

    def test_mixed_grouped_and_ungrouped(self):
        """Mix of group_key and no group_key returns both."""
        sub_single = _make_sub(id=10, queue_name="solo", group_key=None)
        sub_g1 = _make_sub(id=1, queue_name="q1", group_key="dag_g")
        sub_g2 = _make_sub(id=2, queue_name="q2", group_key="dag_g")
        result = _group_subscriptions([sub_single, sub_g1, sub_g2])
        assert len(result) == 2
        assert result[0] is sub_single  # ungrouped come first
        assert isinstance(result[1], SubscriptionGroup)

    def test_group_disabled_if_any_sub_disabled(self):
        """Group.enabled is False if any member is disabled."""
        sub1 = _make_sub(id=1, queue_name="q1", group_key="dag_g", enabled=True)
        sub2 = _make_sub(id=2, queue_name="q2", group_key="dag_g", enabled=False)
        result = _group_subscriptions([sub1, sub2])
        assert result[0].enabled is False

    def test_group_ack_unmatched_if_any_sub_acks(self):
        """Group.ack_unmatched is True as soon as one member acknowledges — the
        opposite of `enabled`, which the group carries only while every member has
        it. A group row has to name the setting when a single one of its queues
        deletes messages."""
        sub1 = _make_sub(id=1, queue_name="q1", group_key="dag_g", ack_unmatched=False)
        sub2 = _make_sub(id=2, queue_name="q2", group_key="dag_g", ack_unmatched=True)
        result = _group_subscriptions([sub1, sub2])
        assert result[0].ack_unmatched is True

    def test_group_ack_unmatched_false_when_no_sub_acks(self):
        sub1 = _make_sub(id=1, queue_name="q1", group_key="dag_g", ack_unmatched=False)
        sub2 = _make_sub(id=2, queue_name="q2", group_key="dag_g", ack_unmatched=False)
        result = _group_subscriptions([sub1, sub2])
        assert result[0].ack_unmatched is False

    def test_group_listening_count(self):
        """listening_count counts only 'listening' statuses."""
        sub1 = _make_sub(id=1, queue_name="q1", group_key="dag_g", consumer_status="listening")
        sub2 = _make_sub(id=2, queue_name="q2", group_key="dag_g", consumer_status="connecting")
        grp = _group_subscriptions([sub1, sub2])[0]
        assert grp.listening_count == 1
        assert grp.total_count == 2
        assert grp.display_status == "1/2 listening"

    def test_group_first_non_null_last_error(self):
        """Group captures first non-null last_error."""
        sub1 = _make_sub(id=1, queue_name="q1", group_key="dag_g", last_error=None)
        sub2 = _make_sub(id=2, queue_name="q2", group_key="dag_g", last_error="timeout")
        grp = _group_subscriptions([sub1, sub2])[0]
        assert grp.last_error == "timeout"


# ---------------------------------------------------------------------------
# POST /subscriptions/create — multi-queue and cooldown
# ---------------------------------------------------------------------------

class TestCreateMultiQueue:
    def test_create_multiple_queues(self, app, view):
        """POST with multiple queue_name values creates a subscription for each."""
        ctx, session = _session_ctx()
        mock_redirect = MagicMock(return_value="redirect-response")
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={
                "dag_id": "dag1",
                "queue_name": ["q1", "q2"],
                "conn_id": "rmq_default",
                "cooldown": "300",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", mock_redirect), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            result = view.create()

        assert mock_upsert.call_count == 2
        queues_called = {c.kwargs["queue_name"] for c in mock_upsert.call_args_list}
        assert queues_called == {"q1", "q2"}
        for call in mock_upsert.call_args_list:
            assert call.kwargs["cooldown"] == 300
            assert call.kwargs["group_key"] == "dag1"
        assert result == "redirect-response"

    def test_create_cooldown_zero_sets_group_key_none(self, app, view):
        """cooldown=0 creates subscription with group_key=None."""
        ctx, session = _session_ctx()
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={"dag_id": "dag1", "queue_name": "q1", "cooldown": "0"},
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.create()

        call_kwargs = mock_upsert.call_args.kwargs
        assert call_kwargs["group_key"] is None
        assert call_kwargs["cooldown"] is None

    def test_create_no_queue_name_shows_error(self, app, view):
        """POST without any queue_name flashes error."""
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={"dag_id": "dag1"},
        ), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession"):
            view.create()

        mock_flash.assert_called_once()
        assert "required" in mock_flash.call_args.args[0]

    def test_create_negative_cooldown_shows_error(self, app, view):
        """POST with cooldown=-1 flashes an error."""
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={"dag_id": "dag1", "queue_name": "q1", "cooldown": "-1"},
        ), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession"):
            view.create()

        mock_flash.assert_called_once()
        assert "non-negative" in mock_flash.call_args.args[0]


# ---------------------------------------------------------------------------
# edit() — single subscription edit
# ---------------------------------------------------------------------------

class TestEditSubscription:
    def test_edit_post_with_cooldown_sets_group_key(self, app, view):
        """POST edit with cooldown>0 sets group_key=dag_id and cooldown on the sub."""
        sub = _make_sub(id=1, source="ui", dag_id="my_dag", queue_name="q1", cooldown=None)
        ctx, session = _session_ctx(first_sub=sub)
        with app.test_request_context(
            "/subscriptions/1/edit",
            method="POST",
            data={
                "dag_id": "my_dag",
                "queue_name": "q1",
                "conn_id": "rmq_default",
                "cooldown": "300",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit(1)

        assert sub.cooldown == 300
        assert sub.group_key == "my_dag"
        assert sub.enabled is True

    def test_edit_post_with_cooldown_zero_clears_group_key(self, app, view):
        """POST edit with cooldown=0 sets group_key=None and cooldown=None on the sub."""
        sub = _make_sub(id=1, source="ui", dag_id="my_dag", queue_name="q1", cooldown=300)
        sub.group_key = "my_dag"
        ctx, session = _session_ctx(first_sub=sub)
        with app.test_request_context(
            "/subscriptions/1/edit",
            method="POST",
            data={
                "dag_id": "my_dag",
                "queue_name": "q1",
                "conn_id": "rmq_default",
                "cooldown": "0",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit(1)

        assert sub.cooldown is None
        assert sub.group_key is None

    def test_edit_post_negative_cooldown_shows_error(self, app, view):
        """POST edit with cooldown=-1 flashes an error and re-renders form."""
        sub = _make_sub(id=1, source="ui")
        ctx, session = _session_ctx(first_sub=sub)
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/1/edit",
            method="POST",
            data={
                "dag_id": "my_dag",
                "queue_name": "q1",
                "conn_id": "rmq_default",
                "cooldown": "-1",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash):
            view.edit(1)

        mock_flash.assert_called_once()
        assert "non-negative" in mock_flash.call_args.args[0]

    def test_edit_get_renders_form(self, app, view):
        """GET edit renders subscription_form.html with the sub."""
        sub = _make_sub(id=1, source="ui")
        ctx, session = _session_ctx(first_sub=sub)
        with app.test_request_context("/subscriptions/1/edit", method="GET"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx):
            result = view.edit(1)

        assert result == "<html>mocked</html>"
        view.render_template.assert_called_once()
        assert view.render_template.call_args.args[0] == "rmq_watcher/subscription_form.html"


# ---------------------------------------------------------------------------
# Group endpoints: delete_group, toggle_group, edit_group
# ---------------------------------------------------------------------------

def _session_ctx_with_filter_all(subs_by_group: list):
    """Session ctx where filter_by(...).all() and filter_by(...).order_by(...).all() return subs_by_group."""
    session = MagicMock()
    query = session.query.return_value
    # Support chaining: filter_by(...).all() and filter_by(...).order_by(...).all()
    filter_by_result = query.filter_by.return_value
    filter_by_result.all.return_value = subs_by_group
    filter_by_result.order_by.return_value.all.return_value = subs_by_group
    filter_by_result.first.return_value = subs_by_group[0] if subs_by_group else None
    query.order_by.return_value.all.return_value = subs_by_group
    ctx = MagicMock()
    ctx.__enter__.return_value = session
    ctx.__exit__.return_value = False
    return ctx, session


class TestDeleteGroup:
    def test_delete_group_removes_all_subs(self, app, view):
        """DELETE group removes all subscriptions in the group."""
        sub1 = _make_sub(id=1, queue_name="q1", source="ui", group_key="my_dag")
        sub2 = _make_sub(id=2, queue_name="q2", source="ui", group_key="my_dag")
        ctx, session = _session_ctx_with_filter_all([sub1, sub2])
        mock_redirect = MagicMock(return_value="redirect-response")
        with app.test_request_context("/subscriptions/group/my_dag/delete", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", mock_redirect), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            result = view.delete_group("my_dag")

        assert session.delete.call_count == 2
        session.commit.assert_called_once()
        assert result == "redirect-response"

    def test_delete_group_rejects_dag_file(self, app, view):
        """Cannot delete a group that has dag_file source."""
        sub1 = _make_sub(id=1, queue_name="q1", source="dag_file", group_key="my_dag")
        ctx, session = _session_ctx_with_filter_all([sub1])
        mock_flash = MagicMock()
        with app.test_request_context("/subscriptions/group/my_dag/delete", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.delete_group("my_dag")

        session.delete.assert_not_called()
        mock_flash.assert_called_once()
        assert mock_flash.call_args.args[1] == "error"


class TestToggleGroup:
    def test_toggle_group_all_enabled_disables_all(self, app, view):
        """If all enabled → toggle disables all."""
        sub1 = _make_sub(id=1, queue_name="q1", enabled=True, group_key="dag_g")
        sub2 = _make_sub(id=2, queue_name="q2", enabled=True, group_key="dag_g")
        ctx, session = _session_ctx_with_filter_all([sub1, sub2])
        with app.test_request_context("/subscriptions/group/dag_g/toggle", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.toggle_group("dag_g")

        assert sub1.enabled is False
        assert sub2.enabled is False
        session.commit.assert_called_once()

    def test_toggle_group_any_disabled_enables_all(self, app, view):
        """If any sub is disabled → toggle enables all."""
        sub1 = _make_sub(id=1, queue_name="q1", enabled=True, group_key="dag_g")
        sub2 = _make_sub(id=2, queue_name="q2", enabled=False, group_key="dag_g")
        ctx, session = _session_ctx_with_filter_all([sub1, sub2])
        with app.test_request_context("/subscriptions/group/dag_g/toggle", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.toggle_group("dag_g")

        assert sub1.enabled is True
        assert sub2.enabled is True


class TestEditGroup:
    def test_edit_group_post_updates_cooldown_on_all(self, app, view):
        """POST edit_group updates cooldown for all subs in group."""
        sub1 = _make_sub(id=1, queue_name="q1", source="ui", group_key="dag_g", cooldown=300)
        sub2 = _make_sub(id=2, queue_name="q2", source="ui", group_key="dag_g", cooldown=300)
        ctx, session = _session_ctx_with_filter_all([sub1, sub2])
        mock_redirect = MagicMock(return_value="redirect-response")
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={
                "dag_id": "dag_g",
                "queue_name": ["q1", "q2"],
                "conn_id": "rmq_default",
                "cooldown": "600",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", mock_redirect), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            result = view.edit_group("dag_g")

        assert mock_upsert.call_count == 2
        for call in mock_upsert.call_args_list:
            assert call.kwargs["cooldown"] == 600
            assert call.kwargs["group_key"] == "dag_g"
            assert call.kwargs["enabled"] is True
        assert result == "redirect-response"

    def test_edit_group_post_deletes_removed_queue(self, app, view):
        """All existing subs are deleted before re-creating; only kept queues
        are upserted, so removing a queue effectively drops it."""
        sub1 = _make_sub(id=1, queue_name="q1", source="ui", group_key="dag_g")
        sub2 = _make_sub(id=2, queue_name="q2", source="ui", group_key="dag_g")
        ctx, session = _session_ctx_with_filter_all([sub1, sub2])
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={
                "dag_id": "dag_g",
                "queue_name": ["q1"],  # q2 removed
                "conn_id": "rmq_default",
                "cooldown": "300",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit_group("dag_g")

        # Both subs are deleted first (delete-all strategy)
        deleted_subs = [call.args[0] for call in session.delete.call_args_list]
        assert sub1 in deleted_subs
        assert sub2 in deleted_subs
        session.flush.assert_called_once()
        # Only q1 is upserted (q2 was removed from the form)
        assert mock_upsert.call_count == 1
        assert mock_upsert.call_args.kwargs["queue_name"] == "q1"

    def test_edit_group_post_conn_id_change_deletes_old_rows(self, app, view):
        """Changing conn_id must not leave orphaned rows for the old conn_id.

        Before the fix, subs with the same queue_name but old conn_id were
        skipped in the delete loop (because queue_name was still in new_queue_names),
        and upsert_subscription would insert a new row keyed on the new conn_id,
        leaving both old and new rows active — causing duplicate consumers.
        """
        sub1 = _make_sub(id=1, queue_name="q1", conn_id="rmq_old", source="ui", group_key="dag_g")
        sub2 = _make_sub(id=2, queue_name="q2", conn_id="rmq_old", source="ui", group_key="dag_g")
        ctx, session = _session_ctx_with_filter_all([sub1, sub2])
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={
                "dag_id": "dag_g",
                "queue_name": ["q1", "q2"],  # same queues, different conn_id
                "conn_id": "rmq_new",
                "cooldown": "300",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit_group("dag_g")

        # Both old rows must be deleted first
        deleted_subs = [call.args[0] for call in session.delete.call_args_list]
        assert sub1 in deleted_subs
        assert sub2 in deleted_subs
        session.flush.assert_called_once()
        # Both queues upserted with the new conn_id
        assert mock_upsert.call_count == 2
        upserted_conn_ids = {call.kwargs["conn_id"] for call in mock_upsert.call_args_list}
        assert upserted_conn_ids == {"rmq_new"}


# ---------------------------------------------------------------------------
# Real-template-rendering harness — jinja2.Environment built directly (NOT
# the full Flask-AppBuilder context). This is the only way to prove the
# dag-not-found badge actually appears in rendered HTML, not just that it
# reaches render_template()'s kwargs. Reused by the Task 3 regression test
# for the separate-DAG-session isolation guarantee.
# ---------------------------------------------------------------------------

_TEMPLATES_DIR = Path(__file__).resolve().parents[2] / "airflow_provider_rmq" / "watcher" / "templates"

_subscriptions_jinja_env = Environment(
    loader=ChoiceLoader(
        [
            FileSystemLoader(str(_TEMPLATES_DIR)),
            DictLoader({"rmq_watcher/base.html": "{% block content %}{% endblock %}"}),
        ]
    ),
)
_subscriptions_jinja_env.globals["get_flashed_messages"] = lambda **kw: []
_subscriptions_jinja_env.globals["url_for"] = lambda *a, **kw: "#"
_subscriptions_jinja_env.globals["csrf_token"] = lambda: "test-token"
# `| tojson` is a jinja2 builtin filter (verified on 3.1.6) — no registration needed.


def _render_subscriptions_html(rows, **ctx):
    """Render subscriptions.html for real through a standalone jinja2
    Environment.

    ``base_template`` must resolve via the DictLoader above — the template's
    first line is ``{% extends base_template %}``, and ``base_template`` is a
    context variable Flask-AppBuilder normally injects at render time, not a
    literal filename. Every call passes it by default; callers may override
    or omit it via ``ctx`` (e.g. to test a context missing other keys).
    """
    template = _subscriptions_jinja_env.get_template("rmq_watcher/subscriptions.html")
    render_ctx = {
        "base_template": "rmq_watcher/base.html",
        "subscriptions": rows,
        "conn_statuses": [],
    }
    render_ctx.update(ctx)
    return template.render(**render_ctx)


def _badge_present(html: str, text: str = "⚠ dag not found") -> bool:
    """True iff a warning badge whose visible text matches ``text`` is rendered.

    The text has to sit inside the ``label label-warning`` span rather than in an
    attribute such as ``title``. Whitespace-normalized so it is robust to line
    wrapping in the template.
    """
    normalized = re.sub(r"\s+", " ", html)
    return (
        re.search(
            rf'<span class="label label-warning"[^>]*>\s*{text}\s*</span>',
            normalized,
        )
        is not None
    )


def _render_subscription_form_html(**ctx):
    """Render subscription_form.html for real through the same standalone
    environment, so what the checkbox actually emits is what gets asserted."""
    template = _subscriptions_jinja_env.get_template("rmq_watcher/subscription_form.html")
    render_ctx = {
        "base_template": "rmq_watcher/base.html",
        "sub": None,
        "is_dag_file": False,
    }
    render_ctx.update(ctx)
    return template.render(**render_ctx)


def _form_field(html: str, name: str) -> str:
    """The ``<input>`` tag of the form field called ``name``."""
    normalized = re.sub(r"\s+", " ", html)
    match = re.search(rf'<input[^>]*name="{name}"[^>]*>', normalized)
    assert match is not None, f"the form carries no input named {name}"
    return match.group(0)


def _make_group(
    *,
    dag_id: str = "my_dag",
    conn_id: str = "rmq_default",
    source: str = "ui",
    cooldown: int = 300,
    group_key: str = "my_dag",
    queue_names: list[str] | None = None,
    sub_ids: list[int] | None = None,
    enabled: bool = True,
    ack_unmatched: bool = False,
    statuses: list[str] | None = None,
    last_error: str | None = None,
) -> SubscriptionGroup:
    """Real (non-MagicMock) SubscriptionGroup fixture for the grouped
    template branch."""
    return SubscriptionGroup(
        dag_id=dag_id,
        conn_id=conn_id,
        source=source,
        cooldown=cooldown,
        group_key=group_key,
        queue_names=queue_names if queue_names is not None else ["q1", "q2"],
        sub_ids=sub_ids if sub_ids is not None else [1, 2],
        enabled=enabled,
        ack_unmatched=ack_unmatched,
        statuses=statuses if statuses is not None else ["listening", "listening"],
        last_error=last_error,
    )


def _make_single_row(
    *,
    id: int = 1,
    dag_id: str = "my_dag",
    queue_name: str = "my_queue",
    conn_id: str = "rmq_default",
    source: str = "ui",
    enabled: bool = True,
    consumer_status: str = "listening",
    last_error: str | None = None,
    cooldown: int | None = None,
    ack_unmatched: bool = False,
    group_key: str | None = None,
) -> SimpleNamespace:
    """Real (non-MagicMock) single-row fixture matching the attributes the
    template's single-queue branch reads.

    Deliberately not an unflushed ``RMQSubscription(...)``: SQLAlchemy
    ``Column(default=...)`` only applies on INSERT, so an unflushed instance
    has ``enabled``/``source``/``consumer_status`` all ``None``, which would
    route it into the wrong template branches. Also deliberately has no
    ``is_group`` attribute, matching the real model — the template's
    ``item.is_group is defined`` check relies on that to route it here.
    """
    return SimpleNamespace(
        id=id,
        dag_id=dag_id,
        queue_name=queue_name,
        conn_id=conn_id,
        source=source,
        enabled=enabled,
        consumer_status=consumer_status,
        last_error=last_error,
        cooldown=cooldown,
        ack_unmatched=ack_unmatched,
        group_key=group_key,
    )


class TestSubscriptionsTemplateRendering:
    def test_grouped_row_unknown_dag_id_shows_badge(self):
        row = _make_group(dag_id="ghost_dag", group_key="ghost_dag")
        html = _render_subscriptions_html([row], active_dag_ids={"other_dag"})
        assert _badge_present(html)

    def test_single_row_unknown_dag_id_shows_badge(self):
        """Kept separate from the grouped-row test above — this is what
        would catch a copy-paste mistake between the two template branches."""
        row = _make_single_row(dag_id="ghost_dag")
        html = _render_subscriptions_html([row], active_dag_ids={"other_dag"})
        assert _badge_present(html)

    def test_known_dag_id_no_badge(self):
        grouped = _make_group(dag_id="known_dag", group_key="known_dag")
        single = _make_single_row(dag_id="known_dag_2")
        html = _render_subscriptions_html(
            [grouped, single], active_dag_ids={"known_dag", "known_dag_2"}
        )
        assert not _badge_present(html)

    def test_active_dag_ids_none_no_badge_for_any_row(self):
        grouped = _make_group(dag_id="ghost_dag_1", group_key="ghost_dag_1")
        single = _make_single_row(dag_id="ghost_dag_2")
        html = _render_subscriptions_html([grouped, single], active_dag_ids=None)
        assert not _badge_present(html)

    def test_active_dag_ids_missing_from_context_renders_without_badge(self):
        """Regression guard for the `is defined` check in the template:
        without it, Undefined's __iter__ makes
        `item.dag_id not in active_dag_ids` evaluate True on every row, and
        the badge would silently appear everywhere instead of nowhere."""
        grouped = _make_group(dag_id="ghost_dag_1", group_key="ghost_dag_1")
        single = _make_single_row(dag_id="ghost_dag_2")
        html = _render_subscriptions_html([grouped, single])  # no active_dag_ids kwarg
        assert not _badge_present(html)

    def test_disabled_subscription_unknown_dag_id_still_shows_badge(self):
        """Per the plan's Technical Details ('Disabled-подписка с неизвестным
        dag_id бейдж получает'): enabled=False means 'not currently
        listening', not 'row is correct' — a disabled subscription with an
        unknown dag_id must still get the badge, so the problem is visible
        before the subscription is ever re-enabled."""
        grouped = _make_group(dag_id="ghost_dag_1", group_key="ghost_dag_1", enabled=False)
        single = _make_single_row(dag_id="ghost_dag_2", enabled=False)
        html = _render_subscriptions_html([grouped, single], active_dag_ids={"other_dag"})
        assert _badge_present(html)

    def test_the_create_button_is_gone_while_the_notice_stands(self):
        """The form behind the button writes through the mapping the notice is about.

        Its POST goes to ``upsert_subscription``, whose query selects every column the
        ORM maps — so on the schema the notice describes the button leads to a 500,
        offered from the very page that has just said the table cannot be read.
        """
        html = _render_subscriptions_html([], subscriptions_error="cannot be read")
        assert "Create subscription" not in html

    def test_the_create_button_is_there_when_the_table_reads(self):
        html = _render_subscriptions_html([_make_single_row(dag_id="d")])
        assert "Create subscription" in html

    def test_active_dag_ids_empty_set_shows_badge_on_all_rows(self):
        """set() (lookup succeeded, genuinely zero active DAGs) is a
        different sentinel than None (lookup failed) per the plan's
        Technical Details — an empty-but-successful result means every
        subscription is dangling, so the badge must appear on every row."""
        grouped = _make_group(dag_id="ghost_dag_1", group_key="ghost_dag_1")
        single = _make_single_row(dag_id="ghost_dag_2")
        html = _render_subscriptions_html([grouped, single], active_dag_ids=set())
        assert _badge_present(html)


# ---------------------------------------------------------------------------
# Session-isolation regression test — REAL ORM objects, real in-memory
# SQLite session, combined with the render harness above. The mock-based
# isolation tests in TestSubscriptionsList (dag_lookup_uses_separate_session,
# active_dag_ids_none_on_lookup_failure) prove the view *calls* things in the
# right shape, but a MagicMock session can't reproduce SQLAlchemy's real
# detach/expire semantics — an implementation that shares one session and
# calls rollback() on failure would pass those mock tests unchanged, yet
# raise DetachedInstanceError against a real database. Only a real Session
# can distinguish the two.
# ---------------------------------------------------------------------------

class TestSubscriptionsDagLookupSessionIsolationReal:
    def test_real_subs_survive_dag_lookup_failure_for_template_render(self, app, view):
        engine = create_engine("sqlite:///:memory:")
        WatcherBase.metadata.create_all(engine)
        RealSession = sessionmaker(bind=engine)

        # Seed data in a throwaway session, fully separate from the one the
        # view will use to read it.
        seed = RealSession()
        seed.add(
            RMQSubscription(
                dag_id="ghost_dag",
                queue_name="q1",
                conn_id="rmq_default",
                source="ui",
                enabled=True,
            )
        )
        seed.commit()
        seed.close()

        try:
            with app.test_request_context("/subscriptions"), \
                 patch("airflow_provider_rmq.watcher.views.WatcherSession", RealSession), \
                 patch(
                     "airflow_provider_rmq.watcher.views.get_active_dag_ids",
                     side_effect=RuntimeError("boom"),
                 ):
                view.subscriptions()

            kwargs = view.render_template.call_args.kwargs
            assert kwargs["active_dag_ids"] is None

            # `subs` are real, detached RMQSubscription instances read from a
            # session that has since been closed (not rolled back). Feeding
            # them through the real Jinja harness must not raise
            # DetachedInstanceError when the template reads item.dag_id.
            html = _render_subscriptions_html(
                kwargs["subscriptions"],
                active_dag_ids=kwargs["active_dag_ids"],
                conn_statuses=kwargs["conn_statuses"],
            )
            assert "ghost_dag" in html
        finally:
            WatcherBase.metadata.drop_all(engine)


# ---------------------------------------------------------------------------
# The page against a database whose rmq_watcher_subscriptions is older than
# the provider — the state an installation is in until the watcher loop in
# the scheduler runs its migration.
# ---------------------------------------------------------------------------

#: One subscription row, written through raw SQL so it lands in whichever
#: shape of the table the test built.
_seed_subscription_sql = """
INSERT INTO rmq_watcher_subscriptions (
    dag_id, queue_name, conn_id, source, enabled, consumer_status,
    trigger_mode, created_at, updated_at
) VALUES (
    'my_dag', 'q1', 'rmq_default', 'dag_file', 1, 'listening',
    'per_message', '2026-09-02 10:00:00', '2026-09-02 10:00:00'
)
"""


class TestSubscriptionsSchemaOutdated:
    """The Subscriptions table is read through the ORM, so a column the
    provider maps and the database does not have makes the query itself
    fail. The page has to survive that the way the Connections block does.
    """

    @staticmethod
    def _legacy_engine():
        """Engine whose subscriptions table predates the columns later releases
        added, with the rest of the watcher schema current."""
        engine = create_engine("sqlite:///:memory:")
        WatcherBase.metadata.create_all(engine)
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE rmq_watcher_subscriptions"))
            conn.execute(text(SUBSCRIPTIONS_DDL_PRE_MIGRATION))
            conn.execute(text(_seed_subscription_sql))
        return engine

    def test_outdated_schema_yields_notice_not_error(self, app, view, caplog):
        engine = self._legacy_engine()
        RealSession = sessionmaker(bind=engine)
        try:
            with app.test_request_context("/subscriptions"), \
                 patch("airflow_provider_rmq.watcher.views.WatcherSession", RealSession), \
                 patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value={"my_dag"}), \
                 caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.watcher.views"):
                result = view.subscriptions()

            assert result == "<html>mocked</html>"
            kwargs = view.render_template.call_args.kwargs
            assert kwargs["subscriptions_error"] == SUBSCRIPTIONS_SCHEMA_OUTDATED_MESSAGE
            assert "rmq_watcher_subscriptions" in kwargs["subscriptions_error"]
            assert kwargs["subscriptions"] == []
            assert any(r.exc_info is not None for r in caplog.records)
        finally:
            engine.dispose()

    def test_connections_block_still_reads_after_the_failed_query(self, app, view):
        """The failed read leaves the session usable: the Connections block
        is read from the same session afterwards and gets no notice of its
        own."""
        engine = self._legacy_engine()
        RealSession = sessionmaker(bind=engine)
        try:
            with app.test_request_context("/subscriptions"), \
                 patch("airflow_provider_rmq.watcher.views.WatcherSession", RealSession), \
                 patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
                view.subscriptions()

            kwargs = view.render_template.call_args.kwargs
            assert kwargs["conn_status_error"] is None
            assert kwargs["conn_statuses"] == []
        finally:
            engine.dispose()

    def test_the_failed_query_is_rolled_back(self, app, view):
        """SQLite goes on answering after a failed statement; PostgreSQL does not.

        Only the rollback keeps the Connections block below readable there, and
        SQLite cannot show its absence — so the call itself is what is asserted.
        """
        engine = self._legacy_engine()
        RealSession = sessionmaker(bind=engine)
        sessions = []

        def spied_session():
            session = RealSession()
            session.rollback = MagicMock(wraps=session.rollback)
            sessions.append(session)
            return session

        try:
            with app.test_request_context("/subscriptions"), \
                 patch("airflow_provider_rmq.watcher.views.WatcherSession", spied_session), \
                 patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
                view.subscriptions()

            sessions[0].rollback.assert_called_once()
        finally:
            engine.dispose()

    def test_a_rollback_that_fails_too_leaves_the_page_standing(self, app, view):
        """A connection broken enough to refuse the rollback is the case for this."""
        engine = self._legacy_engine()
        RealSession = sessionmaker(bind=engine)

        def failing_rollback_session():
            session = RealSession()
            session.rollback = MagicMock(side_effect=OSError("connection is gone"))
            return session

        try:
            with app.test_request_context("/subscriptions"), \
                 patch("airflow_provider_rmq.watcher.views.WatcherSession", failing_rollback_session), \
                 patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
                result = view.subscriptions()

            assert result == "<html>mocked</html>"
            kwargs = view.render_template.call_args.kwargs
            assert kwargs["subscriptions_error"] == SUBSCRIPTIONS_SCHEMA_OUTDATED_MESSAGE
        finally:
            engine.dispose()

    def test_notice_names_the_subscriptions_table_in_html(self, app, view):
        """Rendered for real: the operator reads about the table that is
        actually behind, and the empty-state text does not claim there are
        no subscriptions."""
        engine = self._legacy_engine()
        RealSession = sessionmaker(bind=engine)
        try:
            with app.test_request_context("/subscriptions"), \
                 patch("airflow_provider_rmq.watcher.views.WatcherSession", RealSession), \
                 patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
                view.subscriptions()

            kwargs = view.render_template.call_args.kwargs
            html = _render_subscriptions_html(
                kwargs["subscriptions"],
                subscriptions_error=kwargs["subscriptions_error"],
                conn_statuses=kwargs["conn_statuses"],
                conn_status_error=kwargs["conn_status_error"],
                active_dag_ids=kwargs["active_dag_ids"],
            )
            assert "rmq_watcher_subscriptions" in html
            assert "No subscriptions yet." not in html
            assert SCHEMA_OUTDATED_MESSAGE not in html
        finally:
            engine.dispose()

    def test_current_schema_renders_the_table(self, app, view):
        """The guard costs the healthy page nothing: rows are read and
        rendered, and no notice is raised."""
        engine = create_engine("sqlite:///:memory:")
        WatcherBase.metadata.create_all(engine)
        with engine.begin() as conn:
            conn.execute(text(_seed_subscription_sql))
        RealSession = sessionmaker(bind=engine)
        try:
            with app.test_request_context("/subscriptions"), \
                 patch("airflow_provider_rmq.watcher.views.WatcherSession", RealSession), \
                 patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value={"my_dag"}):
                view.subscriptions()

            kwargs = view.render_template.call_args.kwargs
            assert kwargs["subscriptions_error"] is None
            assert [s.dag_id for s in kwargs["subscriptions"]] == ["my_dag"]

            html = _render_subscriptions_html(
                kwargs["subscriptions"],
                subscriptions_error=kwargs["subscriptions_error"],
                conn_statuses=kwargs["conn_statuses"],
                active_dag_ids=kwargs["active_dag_ids"],
            )
            assert "my_dag" in html
            assert "rmq_watcher_subscriptions" not in html
        finally:
            engine.dispose()


# ---------------------------------------------------------------------------
# Connection liveness on the Subscriptions page — age of the last reconcile
# cycle, broker consumer count and the schema notice.
# ---------------------------------------------------------------------------

class TestReconcileIntervalFromVariable:
    def test_variable_value_is_used(self):
        with patch("airflow.models.Variable") as variable:
            variable.get.return_value = "120"
            assert _reconcile_interval() == 120
        variable.get.assert_called_once()
        assert variable.get.call_args.args[0] == "rmq_watcher_reconcile_interval"

    def test_unset_variable_falls_back_to_default(self):
        with patch("airflow.models.Variable") as variable:
            variable.get.return_value = None
            assert _reconcile_interval() == DEFAULT_RECONCILE_INTERVAL

    def test_unavailable_variable_falls_back_to_default(self, caplog):
        """A database that cannot be reached must not take the page down —
        the staleness check simply assumes the built-in interval."""
        with patch("airflow.models.Variable") as variable, \
             caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.watcher.views"):
            variable.get.side_effect = RuntimeError("no database")
            assert _reconcile_interval() == DEFAULT_RECONCILE_INTERVAL

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert warnings[0].exc_info is not None

    def test_non_numeric_variable_falls_back_to_default(self):
        with patch("airflow.models.Variable") as variable:
            variable.get.return_value = "not-a-number"
            assert _reconcile_interval() == DEFAULT_RECONCILE_INTERVAL

    def test_non_positive_variable_falls_back_to_default(self):
        """Zero would make every row stale, a negative one would make none."""
        with patch("airflow.models.Variable") as variable:
            variable.get.return_value = "0"
            assert _reconcile_interval() == DEFAULT_RECONCILE_INTERVAL


class TestStaleThreshold:
    """The page must flag exactly what the loop treats as late, and not earlier.

    A stamp is written at the end of a cycle and the loop then waits one interval,
    so the oldest stamp a correctly working watcher produces is one interval plus
    one whole cycle budget.
    """

    @staticmethod
    def _variables(**values):
        """Patch Airflow Variables, answering by name."""
        def get(name, default_var=None):
            return values.get(name, default_var)
        variable = MagicMock()
        variable.get.side_effect = get
        return patch("airflow.models.Variable", variable)

    def test_defaults_allow_an_interval_plus_the_built_in_budget(self):
        with self._variables():
            assert _stale_after() == 60 + 300

    def test_longer_interval_grows_both_terms(self):
        """At 120s the budget is three intervals, not the 300s floor."""
        with self._variables(rmq_watcher_reconcile_interval="120"):
            assert _stale_after() == 120 + 360

    def test_cycle_timeout_variable_is_honoured(self):
        with self._variables(rmq_watcher_cycle_timeout="45"):
            assert _stale_after() == 60 + 45

    def test_healthy_cycle_within_budget_is_not_flagged(self):
        """A cycle that takes 90s on a 60s interval is well inside the budget the
        watchdog allows, so its stamp must not raise the warning badge."""
        with self._variables():
            threshold = _stale_after()
        row = _build_conn_status_rows(
            [_make_conn_status_row(last_reconcile_at=_naive_utc_ago(150))], threshold
        )[0]
        assert row.is_stale is False

    def test_unreadable_cycle_timeout_falls_back_to_the_built_in_budget(self, caplog):
        variable = MagicMock()
        variable.get.side_effect = [None, RuntimeError("no database")]
        with patch("airflow.models.Variable", variable), \
             caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.watcher.views"):
            assert _stale_after() == 60 + 300
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "rmq_watcher_cycle_timeout" in warnings[0].getMessage()


class TestConnStatusRowBuilding:
    def test_fresh_reconcile_is_not_stale(self):
        row = _build_conn_status_rows(
            [_make_conn_status_row(last_reconcile_at=_naive_utc_ago(10))], 360
        )[0]
        assert row.is_stale is False
        assert 5 <= row.age_seconds <= 30
        assert row.age_display.endswith("s")

    def test_reconcile_older_than_threshold_is_stale(self):
        row = _build_conn_status_rows(
            [_make_conn_status_row(last_reconcile_at=_naive_utc_ago(400))], 360
        )[0]
        assert row.is_stale is True
        assert row.age_display == "6m"

    def test_just_under_threshold_is_not_stale(self):
        """A cycle that spent nearly its whole budget is doing what it is allowed
        to do, and must not be flagged — otherwise a watcher the loop itself
        considers healthy shows a warning on every row."""
        rows = _build_conn_status_rows(
            [_make_conn_status_row(last_reconcile_at=_naive_utc_ago(350))], 360
        )
        assert rows[0].is_stale is False

    def test_null_reconcile_time_reads_as_no_data(self):
        row = _build_conn_status_rows([_make_conn_status_row(last_reconcile_at=None)], 360)[0]
        assert row.age_seconds is None
        assert row.is_stale is False
        assert row.age_display == "—"

    def test_naive_timestamp_does_not_raise(self):
        """The watcher writes naive UTC; the comparison must be made in the
        same form, so a naive value is the normal case, not an error."""
        row = _build_conn_status_rows(
            [_make_conn_status_row(last_reconcile_at=_naive_utc_ago(90))], 60
        )[0]
        assert row.age_seconds == pytest.approx(90, abs=10)

    def test_aware_timestamp_does_not_raise(self):
        """A driver that hands back an aware datetime must not produce a
        TypeError from mixing it with the naive 'now'."""
        aware = datetime.now(timezone.utc) - timedelta(seconds=120)
        row = _build_conn_status_rows([_make_conn_status_row(last_reconcile_at=aware)], 60)[0]
        assert row.age_seconds == pytest.approx(120, abs=10)
        assert row.is_stale is True

    def test_future_timestamp_clamps_to_zero(self):
        """Clock skew between the scheduler and the webserver must not show a
        negative age."""
        future = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=120)
        row = _build_conn_status_rows([_make_conn_status_row(last_reconcile_at=future)], 60)[0]
        assert row.age_seconds == 0.0
        assert row.is_stale is False

    def test_hour_scale_age_display(self):
        row = _build_conn_status_rows(
            [_make_conn_status_row(last_reconcile_at=_naive_utc_ago(3 * 3600 + 25 * 60))], 60
        )[0]
        assert row.age_display == "3h 25m"

    def test_matching_counts_are_no_mismatch(self):
        row = _build_conn_status_rows(
            [_make_conn_status_row(consumer_count=2, broker_consumer_count=2)], 60
        )[0]
        assert row.counts_mismatch is False

    def test_differing_counts_are_a_mismatch(self):
        row = _build_conn_status_rows(
            [_make_conn_status_row(consumer_count=2, broker_consumer_count=0)], 60
        )[0]
        assert row.counts_mismatch is True

    def test_null_broker_count_is_no_mismatch(self):
        """NULL means 'the broker could not be asked', which is not evidence
        of a disagreement."""
        row = _build_conn_status_rows(
            [_make_conn_status_row(consumer_count=2, broker_consumer_count=None)], 60
        )[0]
        assert row.broker_consumer_count is None
        assert row.counts_mismatch is False


class TestConnectionsTableRendering:
    def test_fresh_reconcile_shows_age_without_badge(self):
        rows = _build_conn_status_rows(
            [_make_conn_status_row(last_reconcile_at=_naive_utc_ago(15))], 60
        )
        html = _render_subscriptions_html([], conn_statuses=rows)
        assert "ago" in html
        assert not _badge_present(html, r"⚠ [^<]*ago")

    def test_stale_reconcile_shows_badge(self):
        rows = _build_conn_status_rows(
            [_make_conn_status_row(last_reconcile_at=_naive_utc_ago(3600))], 60
        )
        html = _render_subscriptions_html([], conn_statuses=rows)
        assert _badge_present(html, r"⚠ [^<]*ago")

    def test_null_reconcile_time_renders_dash(self):
        """A row written before the migration added the column holds NULL —
        the page must render, not blow up on the missing value."""
        rows = _build_conn_status_rows([_make_conn_status_row(last_reconcile_at=None)], 60)
        html = _render_subscriptions_html([], conn_statuses=rows)
        assert "rmq_default" in html
        assert not _badge_present(html, r"⚠ [^<]*ago")
        assert "—" in html

    def test_naive_timestamp_renders_without_type_error(self):
        rows = _build_conn_status_rows(
            [_make_conn_status_row(last_reconcile_at=_naive_utc_ago(45))], 60
        )
        html = re.sub(r"\s+", " ", _render_subscriptions_html([], conn_statuses=rows))
        assert re.search(r"\d+s ago", html), html

    def test_broker_count_shown_next_to_consumer_count(self):
        rows = _build_conn_status_rows(
            [_make_conn_status_row(consumer_count=3, broker_consumer_count=3)], 60
        )
        html = re.sub(r"\s+", " ", _render_subscriptions_html([], conn_statuses=rows))
        assert "Broker consumers" in html
        # Equal counts: both cells carry the number and neither carries a badge.
        assert re.search(r"<td>\s*3\s*</td>\s*<td>\s*3\s*</td>", html), html
        assert "label-warning" not in html
        assert not _badge_present(html, r"⚠ \d+")

    def test_count_mismatch_is_marked(self):
        rows = _build_conn_status_rows(
            [_make_conn_status_row(consumer_count=3, broker_consumer_count=1)], 60
        )
        html = _render_subscriptions_html([], conn_statuses=rows)
        assert _badge_present(html, r"⚠ \d+")

    def test_null_broker_count_renders_dash(self):
        rows = _build_conn_status_rows(
            [_make_conn_status_row(consumer_count=3, broker_consumer_count=None)], 60
        )
        html = _render_subscriptions_html([], conn_statuses=rows)
        assert not _badge_present(html, r"⚠ \d+")
        assert "—" in html

    def test_schema_notice_replaces_the_table(self):
        html = _render_subscriptions_html(
            [], conn_statuses=[], conn_status_error=SCHEMA_OUTDATED_MESSAGE
        )
        assert "rmq_watcher_conn_status" in html
        assert "No connections established yet." not in html


class TestConnStatusBadges:
    def test_degraded_status_renders_its_own_badge(self):
        """A verdict the drop rate limit held back is neither green nor an error."""
        rows = _build_conn_status_rows(
            [_make_conn_status_row(status="degraded", last_error="held back")], 60
        )
        html = re.sub(r"\s+", " ", _render_subscriptions_html([], conn_statuses=rows))
        assert "degraded" in html
        assert "label-warning" in html
        assert "label-success" not in html

    def test_unknown_status_renders_as_a_neutral_label(self):
        """A conn_id no check has reached a verdict on must not read as healthy."""
        rows = _build_conn_status_rows([_make_conn_status_row(status="unknown")], 60)
        html = re.sub(r"\s+", " ", _render_subscriptions_html([], conn_statuses=rows))
        assert "unknown" in html
        assert "label-default" in html
        assert "label-success" not in html


class TestConnStatusReadFailure:
    def test_unreadable_conn_status_yields_notice_not_error(self, app, view, caplog):
        """An incomplete schema (the migration has not run yet in this
        database) must degrade to a message on the page instead of a
        rendering error — the subscriptions themselves still render."""
        sub = _make_sub(dag_id="my_dag")
        ctx, _ = _session_ctx(subs=[sub])
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch(
                 "airflow_provider_rmq.watcher.views.get_conn_statuses",
                 side_effect=RuntimeError("no such column: broker_consumer_count"),
             ), \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value={"my_dag"}), \
             caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.watcher.views"):
            result = view.subscriptions()

        assert result == "<html>mocked</html>"
        kwargs = view.render_template.call_args.kwargs
        assert kwargs["conn_status_error"] == SCHEMA_OUTDATED_MESSAGE
        assert kwargs["conn_statuses"] == []
        assert sub in kwargs["subscriptions"]
        assert any(r.exc_info is not None for r in caplog.records)

    def test_notice_renders_through_the_real_template(self, app, view):
        """Proves the notice reaches HTML: the view's own kwargs, fed to the
        real template, must produce the message and no Connections table."""
        row = _make_single_row(dag_id="my_dag")
        # _group_subscriptions reads group_key; the template branch fixture
        # only carries what the template itself reads
        row.group_key = None
        ctx, _ = _session_ctx(subs=[row])
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch(
                 "airflow_provider_rmq.watcher.views.get_conn_statuses",
                 side_effect=RuntimeError("no such column: last_reconcile_at"),
             ), \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value={"my_dag"}):
            view.subscriptions()

        kwargs = view.render_template.call_args.kwargs
        html = _render_subscriptions_html(
            kwargs["subscriptions"],
            conn_statuses=kwargs["conn_statuses"],
            conn_status_error=kwargs["conn_status_error"],
            active_dag_ids=kwargs["active_dag_ids"],
        )
        assert "rmq_watcher_conn_status" in html
        assert "Broker consumers" not in html
        assert "my_dag" in html

    def test_no_interval_lookup_when_there_are_no_rows(self, app, view):
        """Reading the interval is a database round trip; with no connection
        rows to measure, the page must not make it."""
        ctx, _ = _session_ctx()
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[]), \
             patch("airflow_provider_rmq.watcher.views._reconcile_interval") as interval, \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
            view.subscriptions()

        interval.assert_not_called()
        assert view.render_template.call_args.kwargs["conn_statuses"] == []


class TestConnStatusRowDataclass:
    def test_age_display_seconds(self):
        assert ConnStatusRow(
            conn_id="c", status="connected", consumer_count=1,
            broker_consumer_count=1, last_error=None, age_seconds=42.7, is_stale=False,
        ).age_display == "42s"

    def test_age_display_minutes(self):
        assert ConnStatusRow(
            conn_id="c", status="connected", consumer_count=1,
            broker_consumer_count=1, last_error=None, age_seconds=605.0, is_stale=True,
        ).age_display == "10m"


# ---------------------------------------------------------------------------
# ack_unmatched — the form field, the page column and the shared-queue badge
# ---------------------------------------------------------------------------

def _session_ctx_for_sharing(*, first_sub=None, group_subs=None, enabled_rows=()):
    """Session ctx whose ``filter_by`` answers both shapes the view uses.

    ``filter_by(enabled=True).all()`` is the read behind the shared-queue warning;
    every other ``filter_by`` is the handler's own lookup of the row or the group.
    """
    session = MagicMock()
    query = session.query.return_value

    def _filter_by(**kwargs):
        result = MagicMock()
        if kwargs.get("enabled") is True:
            result.all.return_value = list(enabled_rows)
        else:
            rows = list(group_subs or [])
            result.all.return_value = rows
            result.order_by.return_value.all.return_value = rows
            result.first.return_value = first_sub
        return result

    query.filter_by.side_effect = _filter_by
    ctx = MagicMock()
    ctx.__enter__.return_value = session
    ctx.__exit__.return_value = False
    return ctx, session


def _warning_flashes(mock_flash) -> list[str]:
    """Messages the handler flashed under the "warning" category."""
    return [
        call.args[0]
        for call in mock_flash.call_args_list
        if len(call.args) > 1 and call.args[1] == "warning"
    ]


def _render_rows_with_sharing(rows, **ctx):
    """Render the page the way subscriptions() builds it — the badge is decided in
    _group_subscriptions over the very rows being rendered, so a test cannot hand the
    template a context the view would never produce."""
    return _render_subscriptions_html(
        _group_subscriptions(rows),
        shared_queue_warning=SHARED_QUEUE_WARNING,
        shared_queue_ack_warning=SHARED_QUEUE_ACK_WARNING,
        **ctx,
    )


def _shared_queue_badges(html: str) -> list[str]:
    """Visible text of every shared-queue badge on the page."""
    normalized = re.sub(r"\s+", " ", html)
    return [
        text.strip()
        for text in re.findall(
            r'<span class="label label-warning"[^>]*>\s*(⚠ shared queue[^<]*)</span>',
            normalized,
        )
    ]


def _ack_column_marks(html: str) -> int:
    """How many rows show the Ack-unmatched column as on."""
    normalized = re.sub(r"\s+", " ", html)
    return len(re.findall(r'<span class="label label-info"[^>]*>on</span>', normalized))


class TestAckUnmatchedFormField:
    def test_create_without_the_field_stores_false(self, app, view):
        ctx, _ = _session_ctx()
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={"dag_id": "dag1", "queue_name": "q1", "conn_id": "rmq_default"},
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.create()

        assert mock_upsert.call_args.kwargs["ack_unmatched"] is False

    def test_create_with_on_stores_true(self, app, view):
        ctx, _ = _session_ctx()
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={
                "dag_id": "dag1",
                "queue_name": "q1",
                "conn_id": "rmq_default",
                "ack_unmatched": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.create()

        assert mock_upsert.call_args.kwargs["ack_unmatched"] is True

    def test_edit_stores_the_value(self, app, view):
        sub = _make_sub(id=1, source="ui", ack_unmatched=False)
        ctx, _ = _session_ctx(first_sub=sub)
        with app.test_request_context(
            "/subscriptions/1/edit",
            method="POST",
            data={
                "dag_id": "my_dag",
                "queue_name": "q1",
                "conn_id": "rmq_default",
                "cooldown": "0",
                "enabled": "on",
                "ack_unmatched": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit(1)

        assert sub.ack_unmatched is True

    def test_edit_without_the_field_clears_the_value(self, app, view):
        sub = _make_sub(id=1, source="ui", ack_unmatched=True)
        ctx, _ = _session_ctx(first_sub=sub)
        with app.test_request_context(
            "/subscriptions/1/edit",
            method="POST",
            data={
                "dag_id": "my_dag",
                "queue_name": "q1",
                "conn_id": "rmq_default",
                "cooldown": "0",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit(1)

        assert sub.ack_unmatched is False

    def test_edit_of_a_dag_file_row_leaves_the_value_alone(self, app, view):
        """The value belongs to @rmq_trigger on a dag_file row: a write from the
        form would be overwritten by the next scan anyway, so it is assigned the
        way cooldown is — inside the `not is_dag_file` branch, unlike `enabled`."""
        sub = _make_sub(id=1, source="dag_file", ack_unmatched=False)
        ctx, _ = _session_ctx(first_sub=sub)
        with app.test_request_context(
            "/subscriptions/1/edit",
            method="POST",
            data={"enabled": "on", "ack_unmatched": "on"},
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit(1)

        assert sub.ack_unmatched is False

    def test_edit_group_changing_only_filter_data_keeps_the_value(self, app, view):
        """edit_group deletes the rows and re-creates them through
        upsert_subscription, which assigns ack_unmatched unconditionally — without
        the parameter a save of any other field would clear it for the whole group."""
        sub1 = _make_sub(
            id=1, queue_name="q1", source="ui", group_key="dag_g", ack_unmatched=True
        )
        sub2 = _make_sub(
            id=2, queue_name="q2", source="ui", group_key="dag_g", ack_unmatched=True
        )
        ctx, _ = _session_ctx_with_filter_all([sub1, sub2])
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={
                "dag_id": "dag_g",
                "queue_name": ["q1", "q2"],
                "conn_id": "rmq_default",
                "cooldown": "300",
                "enabled": "on",
                "ack_unmatched": "on",
                "filter_data": '{"filter_headers": {"type": "invoice"}}',
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit_group("dag_g")

        assert mock_upsert.call_count == 2
        assert all(
            call.kwargs["ack_unmatched"] is True for call in mock_upsert.call_args_list
        )

    def test_edit_group_draws_a_mixed_group_unchecked(self, app, view):
        """A group whose rows disagree has no mixed state to show, and the save
        levels every row to the one value the form carries. The box is therefore
        drawn off: a save of any other field cannot switch deletion on for the row
        that runs without it. Ticking it deliberately still levels the group up."""
        rep = _make_sub(
            id=1, queue_name="q1", source="ui", group_key="dag_g", ack_unmatched=True
        )
        other = _make_sub(
            id=2, queue_name="q2", source="ui", group_key="dag_g", ack_unmatched=False
        )
        ctx, _ = _session_ctx_with_filter_all([rep, other])
        with app.test_request_context("/subscriptions/group/dag_g/edit", method="GET"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx):
            view.edit_group("dag_g")

        kwargs = view.render_template.call_args.kwargs
        assert kwargs["sub"] is rep
        assert kwargs["ack_unmatched_checked"] is False
        assert kwargs["ack_unmatched_mixed"] is True

        ctx, _ = _session_ctx_with_filter_all([rep, other])
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={
                "dag_id": "dag_g",
                "queue_name": ["q1", "q2"],
                "conn_id": "rmq_default",
                "cooldown": "300",
                "enabled": "on",
                "ack_unmatched": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit_group("dag_g")

        assert [call.kwargs["ack_unmatched"] for call in mock_upsert.call_args_list] == [
            True,
            True,
        ]

    def test_edit_group_does_not_take_the_box_from_the_first_queue(self, app, view):
        """The first row by name acknowledges and the second does not. The box
        follows the group, not the representative: it is off, because the save
        writes one value to both rows and the second row keeps its messages."""
        first = _make_sub(
            id=1, queue_name="q1", source="ui", group_key="dag_g", ack_unmatched=True
        )
        second = _make_sub(
            id=2, queue_name="q2", source="ui", group_key="dag_g", ack_unmatched=False
        )
        ctx, _ = _session_ctx_with_filter_all([first, second])
        with app.test_request_context("/subscriptions/group/dag_g/edit", method="GET"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx):
            view.edit_group("dag_g")

        kwargs = view.render_template.call_args.kwargs
        assert kwargs["sub"] is first
        assert kwargs["ack_unmatched_checked"] is False
        assert kwargs["ack_unmatched_mixed"] is True

    def test_edit_group_of_an_agreeing_group_is_not_mixed(self, app, view):
        rows = [
            _make_sub(
                id=1, queue_name="q1", source="ui", group_key="dag_g", ack_unmatched=True
            ),
            _make_sub(
                id=2, queue_name="q2", source="ui", group_key="dag_g", ack_unmatched=True
            ),
        ]
        ctx, _ = _session_ctx_with_filter_all(rows)
        with app.test_request_context("/subscriptions/group/dag_g/edit", method="GET"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx):
            view.edit_group("dag_g")

        kwargs = view.render_template.call_args.kwargs
        assert kwargs["ack_unmatched_checked"] is True
        assert kwargs["ack_unmatched_mixed"] is False

    def test_edit_group_of_a_group_that_acknowledges_nothing_draws_the_box_off(
        self, app, view
    ):
        rows = [
            _make_sub(
                id=1,
                queue_name="q1",
                source="ui",
                group_key="dag_g",
                ack_unmatched=False,
            ),
            _make_sub(
                id=2,
                queue_name="q2",
                source="ui",
                group_key="dag_g",
                ack_unmatched=False,
            ),
        ]
        ctx, _ = _session_ctx_with_filter_all(rows)
        with app.test_request_context("/subscriptions/group/dag_g/edit", method="GET"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx):
            view.edit_group("dag_g")

        kwargs = view.render_template.call_args.kwargs
        assert kwargs["ack_unmatched_checked"] is False
        assert kwargs["ack_unmatched_mixed"] is False


    def test_create_error_keeps_the_submitted_box(self, app, view):
        """A save that trips on cooldown redraws the form from the submitted value,
        so the intent the user expressed survives the error."""
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={
                "dag_id": "dag1",
                "queue_name": "q1",
                "conn_id": "rmq_default",
                "cooldown": "-1",
                "ack_unmatched": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession"), \
             patch("airflow_provider_rmq.watcher.views.flash"):
            view.create()

        assert view.render_template.call_args.kwargs["ack_unmatched_checked"] is True

    def test_edit_error_redraws_the_box_from_the_form_not_from_the_row(
        self, app, view
    ):
        """The user cleared the box and tripped on cooldown. Redrawing it from the
        row would put permanent deletion back on, and the next successful submit
        would keep it — so the error page carries the submitted value instead."""
        sub = _make_sub(id=1, source="ui", ack_unmatched=True)
        ctx, _ = _session_ctx(first_sub=sub)
        with app.test_request_context(
            "/subscriptions/1/edit",
            method="POST",
            data={
                "dag_id": "my_dag",
                "queue_name": "q1",
                "conn_id": "rmq_default",
                "cooldown": "-1",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"):
            view.edit(1)

        assert view.render_template.call_args.kwargs["ack_unmatched_checked"] is False
        assert sub.ack_unmatched is True

    def test_edit_error_keeps_a_submitted_box_on(self, app, view):
        sub = _make_sub(id=1, source="ui", ack_unmatched=False)
        ctx, _ = _session_ctx(first_sub=sub)
        with app.test_request_context(
            "/subscriptions/1/edit",
            method="POST",
            data={
                "dag_id": "my_dag",
                "queue_name": "q1",
                "conn_id": "rmq_default",
                "cooldown": "0",
                "enabled": "on",
                "ack_unmatched": "on",
                "filter_data": "{not json",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"):
            view.edit(1)

        assert view.render_template.call_args.kwargs["ack_unmatched_checked"] is True

    def test_edit_get_draws_the_box_from_the_row(self, app, view):
        sub = _make_sub(id=1, source="ui", ack_unmatched=True)
        ctx, _ = _session_ctx(first_sub=sub)
        with app.test_request_context("/subscriptions/1/edit", method="GET"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx):
            view.edit(1)

        assert view.render_template.call_args.kwargs["ack_unmatched_checked"] is True

    def test_edit_group_error_redraws_the_box_from_the_form(self, app, view):
        """Every row of the group acknowledges, the user cleared the box and tripped
        on filter_data: the error page offers the box off, not the group's value."""
        rows = [
            _make_sub(
                id=1, queue_name="q1", source="ui", group_key="dag_g", ack_unmatched=True
            ),
            _make_sub(
                id=2, queue_name="q2", source="ui", group_key="dag_g", ack_unmatched=True
            ),
        ]
        ctx, _ = _session_ctx_with_filter_all(rows)
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={
                "dag_id": "dag_g",
                "queue_name": ["q1", "q2"],
                "conn_id": "rmq_default",
                "cooldown": "300",
                "enabled": "on",
                "filter_data": "{not json",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash"):
            view.edit_group("dag_g")

        assert view.render_template.call_args.kwargs["ack_unmatched_checked"] is False

    def test_saving_a_mixed_group_as_rendered_does_not_level_it_up(self, app, view):
        """The form of a mixed group is rendered with the box off, so submitting it
        unchanged — a cooldown edit, say — writes False to every row. The rows that
        acknowledged stop deleting; no row starts."""
        rows = [
            _make_sub(
                id=1, queue_name="q1", source="ui", group_key="dag_g", ack_unmatched=True
            ),
            _make_sub(
                id=2,
                queue_name="q2",
                source="ui",
                group_key="dag_g",
                ack_unmatched=False,
            ),
        ]
        ctx, _ = _session_ctx_with_filter_all(rows)
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={
                "dag_id": "dag_g",
                "queue_name": ["q1", "q2"],
                "conn_id": "rmq_default",
                "cooldown": "600",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription") as mock_upsert, \
             patch("airflow_provider_rmq.watcher.views.flash"), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit_group("dag_g")

        assert [call.kwargs["ack_unmatched"] for call in mock_upsert.call_args_list] == [
            False,
            False,
        ]


class TestSharedQueueBadge:
    def test_two_enabled_rows_on_one_queue_both_get_the_badge(self):
        rows = [
            _make_single_row(id=1, dag_id="a", queue_name="shared_q"),
            _make_single_row(id=2, dag_id="b", queue_name="shared_q"),
            _make_single_row(id=3, dag_id="c", queue_name="own_q"),
        ]
        badges = _shared_queue_badges(_render_rows_with_sharing(rows))
        assert badges == ["⚠ shared queue", "⚠ shared queue"]

    def test_a_single_row_on_its_own_queue_gets_no_badge(self):
        rows = [_make_single_row(id=1, dag_id="a", queue_name="own_q")]
        assert _shared_queue_badges(_render_rows_with_sharing(rows)) == []

    def test_a_disabled_neighbour_does_not_raise_the_badge(self):
        """A disabled row has no consumer, so nothing competes for the queue."""
        rows = [
            _make_single_row(id=1, dag_id="a", queue_name="shared_q"),
            _make_single_row(id=2, dag_id="b", queue_name="shared_q", enabled=False),
        ]
        assert _shared_queue_badges(_render_rows_with_sharing(rows)) == []

    def test_the_same_queue_on_another_conn_id_is_not_shared(self):
        rows = [
            _make_single_row(id=1, dag_id="a", queue_name="q", conn_id="rmq_a"),
            _make_single_row(id=2, dag_id="b", queue_name="q", conn_id="rmq_b"),
        ]
        assert _shared_queue_badges(_render_rows_with_sharing(rows)) == []

    def test_one_acknowledging_row_makes_the_badge_harsher_on_both(self):
        """Severity belongs to the pair, not to the row it is shown on: the
        subscription with the default setting is the one losing its messages."""
        rows = [
            _make_single_row(
                id=1, dag_id="a", queue_name="shared_q", ack_unmatched=True
            ),
            _make_single_row(id=2, dag_id="b", queue_name="shared_q"),
        ]
        badges = _shared_queue_badges(_render_rows_with_sharing(rows))
        assert badges == [
            "⚠ shared queue — messages deleted",
            "⚠ shared queue — messages deleted",
        ]

    def test_grouped_row_names_the_shared_queue_and_shows_the_column(self):
        """The grouped branch renders SubscriptionGroup, not a subscription row —
        without ack_unmatched on the dataclass the column would be empty and the
        badge could never turn harsh."""
        rows = [
            _make_single_row(
                id=1,
                dag_id="dag_g",
                queue_name="q1",
                group_key="dag_g",
                cooldown=300,
                ack_unmatched=True,
            ),
            _make_single_row(
                id=2, dag_id="dag_g", queue_name="q2", group_key="dag_g", cooldown=300
            ),
            _make_single_row(id=3, dag_id="other", queue_name="q1"),
        ]
        html = _render_rows_with_sharing(rows)
        badges = _shared_queue_badges(html)
        assert badges == [
            "⚠ shared queue — messages deleted",
            "⚠ shared queue: q1 — messages deleted",
        ]
        assert _ack_column_marks(html) == 1

    def test_group_of_three_queues_names_only_the_shared_one(self):
        rows = [
            _make_single_row(
                id=i, dag_id="dag_g", queue_name=q, group_key="dag_g", cooldown=300
            )
            for i, q in enumerate(["q1", "q2", "q3"], start=1)
        ]
        rows.append(_make_single_row(id=4, dag_id="other", queue_name="q2"))
        badges = _shared_queue_badges(_render_rows_with_sharing(rows))
        assert badges == ["⚠ shared queue", "⚠ shared queue: q2"]

    def test_a_disabled_row_on_a_shared_queue_is_not_badged(self):
        """A disabled row runs no consumer, so it competes with nobody — only the two
        enabled rows of the pair are on the queue."""
        rows = [
            _make_single_row(id=1, dag_id="a", queue_name="shared_q"),
            _make_single_row(id=2, dag_id="b", queue_name="shared_q"),
            _make_single_row(id=3, dag_id="c", queue_name="shared_q", enabled=False),
        ]
        assert _shared_queue_badges(_render_rows_with_sharing(rows)) == [
            "⚠ shared queue",
            "⚠ shared queue",
        ]

    def test_a_row_the_view_did_not_annotate_renders_without_a_badge(self):
        """The badge reads an attribute the row may not carry — a template rendered
        outside subscriptions() has to drop the badge rather than break the page."""
        rows = [_make_single_row(id=1, dag_id="a", queue_name="q")]
        grouped = _make_group(dag_id="dag_g", group_key="dag_g")
        html = _render_subscriptions_html([grouped] + rows)
        assert _shared_queue_badges(html) == []

    def test_the_view_passes_the_badge_wordings_to_the_template(self, app, view):
        """The badge and the flash say the same thing because they are the same two
        constants, handed to the template rather than copied into it."""
        rows = [
            _make_single_row(id=1, dag_id="a", queue_name="shared_q", ack_unmatched=True),
            _make_single_row(id=2, dag_id="b", queue_name="shared_q"),
        ]
        ctx, _ = _session_ctx(subs=rows)
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[]), \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
            view.subscriptions()

        kwargs = view.render_template.call_args.kwargs
        assert kwargs["shared_queue_warning"] == SHARED_QUEUE_WARNING
        assert kwargs["shared_queue_ack_warning"] == SHARED_QUEUE_ACK_WARNING
        assert [row.shared_queue_names for row in kwargs["subscriptions"]] == [
            ["shared_q"],
            ["shared_q"],
        ]
        assert all(row.shared_acking for row in kwargs["subscriptions"])


class TestSharedQueueFlash:
    def test_toggle_warns_when_the_queue_already_has_an_acknowledging_consumer(
        self, app, view
    ):
        """The one-click Enable is the busiest of the five paths and goes through
        no form at all, so the warning has to be tested against it rather than
        against edit, where it would pass without covering this route."""
        sub = _make_sub(id=1, dag_id="b", queue_name="shared_q", enabled=False)
        neighbour = _make_sub(
            id=2, dag_id="a", queue_name="shared_q", ack_unmatched=True
        )
        ctx, _ = _session_ctx_for_sharing(
            first_sub=sub, enabled_rows=[sub, neighbour]
        )
        mock_flash = MagicMock()
        with app.test_request_context("/subscriptions/1/toggle", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.toggle(1)

        warnings = _warning_flashes(mock_flash)
        assert len(warnings) == 1
        assert "shared_q" in warnings[0]
        assert SHARED_QUEUE_WARNING in warnings[0]
        assert SHARED_QUEUE_ACK_WARNING in warnings[0]

    def test_toggle_off_does_not_warn(self, app, view):
        """Two enabled rows on the queue, so the query behind the warning does report
        sharing — what keeps the flash away is the guard on the direction of the toggle,
        and nothing else."""
        sub = _make_sub(id=1, dag_id="b", queue_name="shared_q", enabled=True)
        neighbours = [
            _make_sub(id=2, dag_id="a", queue_name="shared_q"),
            _make_sub(id=3, dag_id="c", queue_name="shared_q"),
        ]
        ctx, _ = _session_ctx_for_sharing(first_sub=sub, enabled_rows=neighbours)
        mock_flash = MagicMock()
        with app.test_request_context("/subscriptions/1/toggle", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.toggle(1)

        assert _warning_flashes(mock_flash) == []

    def test_a_read_that_fails_after_the_commit_does_not_lose_the_save(self, app, view):
        """The commit has landed by the time the warning is looked up, so a connection
        that drops in between costs an advisory line, not the toggle. Raising here would
        answer a saved change with a 500 and invite a retry that toggles it straight
        back."""
        sub = _make_sub(id=1, dag_id="b", queue_name="shared_q", enabled=False)
        ctx, session = _session_ctx_for_sharing(first_sub=sub)

        def _filter_by(**kwargs):
            if kwargs.get("enabled") is True:
                raise OperationalError("SELECT", {}, Exception("connection lost"))
            result = MagicMock()
            result.first.return_value = sub
            return result

        session.query.return_value.filter_by.side_effect = _filter_by
        mock_flash = MagicMock()
        with app.test_request_context("/subscriptions/1/toggle", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            assert view.toggle(1) == "redirect"

        session.commit.assert_called_once()
        assert any("Could not check" in m for m in _warning_flashes(mock_flash))

    def test_toggle_group_warns_the_same_way(self, app, view):
        sub1 = _make_sub(
            id=1, dag_id="dag_g", queue_name="q1", enabled=False, group_key="dag_g"
        )
        sub2 = _make_sub(
            id=2, dag_id="dag_g", queue_name="q2", enabled=False, group_key="dag_g"
        )
        neighbour = _make_sub(id=3, dag_id="a", queue_name="q2", ack_unmatched=True)
        ctx, _ = _session_ctx_for_sharing(
            group_subs=[sub1, sub2], enabled_rows=[sub1, sub2, neighbour]
        )
        mock_flash = MagicMock()
        with app.test_request_context("/subscriptions/group/dag_g/toggle", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.toggle_group("dag_g")

        warnings = _warning_flashes(mock_flash)
        assert len(warnings) == 1
        assert "q2" in warnings[0]
        assert SHARED_QUEUE_ACK_WARNING in warnings[0]

    def test_toggle_group_off_does_not_warn(self, app, view):
        """Disabling a group takes its queues off the broker; the rows the query still
        reports as shared are not a reason to warn about the save that removed them."""
        sub1 = _make_sub(
            id=1, dag_id="dag_g", queue_name="q1", enabled=True, group_key="dag_g"
        )
        sub2 = _make_sub(
            id=2, dag_id="dag_g", queue_name="q2", enabled=True, group_key="dag_g"
        )
        neighbours = [
            _make_sub(id=3, dag_id="a", queue_name="q2", ack_unmatched=True),
            _make_sub(id=4, dag_id="c", queue_name="q2"),
        ]
        ctx, _ = _session_ctx_for_sharing(
            group_subs=[sub1, sub2], enabled_rows=neighbours
        )
        mock_flash = MagicMock()
        with app.test_request_context("/subscriptions/group/dag_g/toggle", method="POST"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.toggle_group("dag_g")

        assert _warning_flashes(mock_flash) == []

    def test_create_warns_on_a_queue_a_neighbour_already_listens_to(self, app, view):
        existing = _make_sub(id=1, dag_id="a", queue_name="shared_q")
        created = _make_sub(id=2, dag_id="b", queue_name="shared_q")
        ctx, _ = _session_ctx_for_sharing(enabled_rows=[existing, created])
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={
                "dag_id": "b",
                "queue_name": "shared_q",
                "conn_id": "rmq_default",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription"), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.create()

        warnings = _warning_flashes(mock_flash)
        assert len(warnings) == 1
        assert "shared_q" in warnings[0]
        assert SHARED_QUEUE_ACK_WARNING not in warnings[0]

    def test_create_of_a_disabled_subscription_does_not_warn(self, app, view):
        """A row saved disabled starts no consumer, so it takes nothing from the
        neighbour — even though the queue is shared by the rows the query returns."""
        existing = _make_sub(id=1, dag_id="a", queue_name="shared_q")
        created = _make_sub(id=2, dag_id="b", queue_name="shared_q")
        ctx, _ = _session_ctx_for_sharing(enabled_rows=[existing, created])
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/create",
            method="POST",
            data={"dag_id": "b", "queue_name": "shared_q", "conn_id": "rmq_default"},
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription"), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.create()

        assert _warning_flashes(mock_flash) == []

    def test_edit_warns_on_a_queue_a_neighbour_already_listens_to(self, app, view):
        sub = _make_sub(id=1, dag_id="b", queue_name="shared_q", source="ui")
        neighbour = _make_sub(id=2, dag_id="a", queue_name="shared_q")
        ctx, _ = _session_ctx_for_sharing(first_sub=sub, enabled_rows=[sub, neighbour])
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/1/edit",
            method="POST",
            data={
                "dag_id": "b",
                "queue_name": "shared_q",
                "conn_id": "rmq_default",
                "cooldown": "0",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit(1)

        warnings = _warning_flashes(mock_flash)
        assert len(warnings) == 1
        assert "shared_q" in warnings[0]

    def test_edit_group_warns_on_a_queue_a_neighbour_already_listens_to(self, app, view):
        sub1 = _make_sub(id=1, dag_id="dag_g", queue_name="q1", source="ui", group_key="dag_g")
        sub2 = _make_sub(id=2, dag_id="dag_g", queue_name="q2", source="ui", group_key="dag_g")
        neighbour = _make_sub(id=3, dag_id="a", queue_name="q1")
        ctx, _ = _session_ctx_for_sharing(
            group_subs=[sub1, sub2], enabled_rows=[sub1, sub2, neighbour]
        )
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={
                "dag_id": "dag_g",
                "queue_name": ["q1", "q2"],
                "conn_id": "rmq_default",
                "cooldown": "300",
                "enabled": "on",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription"), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit_group("dag_g")

        warnings = _warning_flashes(mock_flash)
        assert len(warnings) == 1
        assert "q1" in warnings[0]

    def test_edit_group_saved_disabled_does_not_warn(self, app, view):
        sub1 = _make_sub(id=1, dag_id="dag_g", queue_name="q1", source="ui", group_key="dag_g")
        sub2 = _make_sub(id=2, dag_id="dag_g", queue_name="q2", source="ui", group_key="dag_g")
        neighbour = _make_sub(id=3, dag_id="a", queue_name="q1")
        ctx, _ = _session_ctx_for_sharing(
            group_subs=[sub1, sub2], enabled_rows=[sub1, sub2, neighbour]
        )
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={
                "dag_id": "dag_g",
                "queue_name": ["q1", "q2"],
                "conn_id": "rmq_default",
                "cooldown": "300",
            },
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.upsert_subscription"), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit_group("dag_g")

        assert _warning_flashes(mock_flash) == []

    def test_edit_group_of_a_dag_file_group_warns_too(self, app, view):
        """A dag_file group is saved through the branch that only flips `enabled`;
        enabling it puts the queue back on the broker just as the UI branch does."""
        sub1 = _make_sub(
            id=1, dag_id="dag_g", queue_name="q1", source="dag_file",
            group_key="dag_g", enabled=False,
        )
        sub2 = _make_sub(
            id=2, dag_id="dag_g", queue_name="q2", source="dag_file",
            group_key="dag_g", enabled=False,
        )
        neighbour = _make_sub(id=3, dag_id="a", queue_name="q1", ack_unmatched=True)
        ctx, _ = _session_ctx_for_sharing(
            group_subs=[sub1, sub2], enabled_rows=[sub1, sub2, neighbour]
        )
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit",
            method="POST",
            data={"enabled": "on"},
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit_group("dag_g")

        warnings = _warning_flashes(mock_flash)
        assert len(warnings) == 1
        assert "q1" in warnings[0]
        assert SHARED_QUEUE_ACK_WARNING in warnings[0]

    def test_edit_group_of_a_dag_file_group_saved_off_does_not_warn(self, app, view):
        sub1 = _make_sub(
            id=1, dag_id="dag_g", queue_name="q1", source="dag_file", group_key="dag_g"
        )
        sub2 = _make_sub(
            id=2, dag_id="dag_g", queue_name="q2", source="dag_file", group_key="dag_g"
        )
        neighbours = [
            _make_sub(id=3, dag_id="a", queue_name="q1"),
            _make_sub(id=4, dag_id="c", queue_name="q1"),
        ]
        ctx, _ = _session_ctx_for_sharing(
            group_subs=[sub1, sub2], enabled_rows=neighbours
        )
        mock_flash = MagicMock()
        with app.test_request_context(
            "/subscriptions/group/dag_g/edit", method="POST", data={},
        ), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.flash", mock_flash), \
             patch("airflow_provider_rmq.watcher.views.redirect", return_value="redirect"), \
             patch("airflow_provider_rmq.watcher.views.url_for", return_value="/subs"):
            view.edit_group("dag_g")

        assert _warning_flashes(mock_flash) == []


class TestAckUnmatchedFormTemplate:
    def test_the_checkbox_is_editable_on_a_ui_row(self):
        html = _render_subscription_form_html(
            sub=_make_single_row(ack_unmatched=True),
            is_dag_file=False,
            ack_unmatched_checked=True,
        )
        field = _form_field(html, "ack_unmatched")
        assert "checked" in field
        assert "disabled" not in field

    def test_the_checkbox_is_unchecked_when_the_row_does_not_acknowledge(self):
        html = _render_subscription_form_html(
            sub=_make_single_row(ack_unmatched=False),
            is_dag_file=False,
            ack_unmatched_checked=False,
        )
        assert "checked" not in _form_field(html, "ack_unmatched")

    def test_a_group_form_says_the_value_is_levelled(self):
        """The save writes one value to every row — the form has to say so, because
        a mixed group has no mixed state to show."""
        rows = [_make_single_row(id=1, queue_name="q1"), _make_single_row(id=2, queue_name="q2")]
        html = _render_subscription_form_html(
            sub=rows[0], is_dag_file=False, group_subs=rows
        )
        assert "stands for the whole group" in html

    def test_a_mixed_group_draws_the_box_off_even_from_an_acknowledging_row(self):
        """The representative row acknowledges and the group is mixed: the box is
        off, because a save writes one value to every row and the box that is on
        would take the second row's messages with it."""
        rows = [
            _make_single_row(id=1, queue_name="q1", ack_unmatched=True),
            _make_single_row(id=2, queue_name="q2", ack_unmatched=False),
        ]
        html = _render_subscription_form_html(
            sub=rows[0],
            is_dag_file=False,
            group_subs=rows,
            ack_unmatched_checked=False,
            ack_unmatched_mixed=True,
        )
        assert "checked" not in _form_field(html, "ack_unmatched")

    def test_a_group_that_acknowledges_everywhere_draws_the_box_on(self):
        rows = [
            _make_single_row(id=1, queue_name="q1", ack_unmatched=True),
            _make_single_row(id=2, queue_name="q2", ack_unmatched=True),
        ]
        html = _render_subscription_form_html(
            sub=rows[0],
            is_dag_file=False,
            group_subs=rows,
            ack_unmatched_checked=True,
            ack_unmatched_mixed=False,
        )
        assert "checked" in _form_field(html, "ack_unmatched")

    def test_a_mixed_group_is_told_its_rows_disagree(self):
        rows = [
            _make_single_row(id=1, queue_name="q1", ack_unmatched=False),
            _make_single_row(id=2, queue_name="q2", ack_unmatched=True),
        ]
        html = _render_subscription_form_html(
            sub=rows[0],
            is_dag_file=False,
            group_subs=rows,
            ack_unmatched_checked=False,
            ack_unmatched_mixed=True,
        )
        assert "rows disagree today" in html

    def test_a_mixed_group_with_the_box_ticked_is_told_what_the_save_turns_on(self):
        """An error re-render redraws the box from what was submitted, so a mixed
        group can be shown with the box on. The notice has to follow the box: saying
        it is drawn off would promise the opposite of what the next save writes."""
        rows = [
            _make_single_row(id=1, queue_name="q1", ack_unmatched=False),
            _make_single_row(id=2, queue_name="q2", ack_unmatched=True),
        ]
        html = _render_subscription_form_html(
            sub=rows[0],
            is_dag_file=False,
            group_subs=rows,
            ack_unmatched_checked=True,
            ack_unmatched_mixed=True,
        )
        text = re.sub(r"\s+", " ", html)
        assert "checked" in _form_field(html, "ack_unmatched")
        assert "rows disagree today" in text
        assert "the box is off" not in text
        assert "switches permanent deletion on for every queue in the group" in text

    def test_a_mixed_group_with_the_box_off_says_the_save_cannot_turn_it_on(self):
        rows = [
            _make_single_row(id=1, queue_name="q1", ack_unmatched=False),
            _make_single_row(id=2, queue_name="q2", ack_unmatched=True),
        ]
        html = _render_subscription_form_html(
            sub=rows[0],
            is_dag_file=False,
            group_subs=rows,
            ack_unmatched_checked=False,
            ack_unmatched_mixed=True,
        )
        text = re.sub(r"\s+", " ", html)
        assert "cannot turn it on for a queue that runs without it" in text
        assert "switches permanent deletion on for every queue in the group" not in text

    def test_an_agreeing_group_is_not_told_about_a_disagreement(self):
        rows = [
            _make_single_row(id=1, queue_name="q1", ack_unmatched=True),
            _make_single_row(id=2, queue_name="q2", ack_unmatched=True),
        ]
        html = _render_subscription_form_html(
            sub=rows[0],
            is_dag_file=False,
            group_subs=rows,
            ack_unmatched_checked=True,
            ack_unmatched_mixed=False,
        )
        assert "rows disagree today" not in html

    def test_a_single_row_form_does_not_mention_the_group(self):
        html = _render_subscription_form_html(
            sub=_make_single_row(), is_dag_file=False
        )
        assert "stands for the whole group" not in html

    def test_the_checkbox_is_disabled_on_a_dag_file_row(self):
        """The value belongs to the decorator there, and a disabled checkbox is
        not submitted at all — the form cannot even offer a write the next scan
        would undo."""
        html = _render_subscription_form_html(
            sub=_make_single_row(ack_unmatched=True),
            is_dag_file=True,
            ack_unmatched_checked=True,
        )
        assert "disabled" in _form_field(html, "ack_unmatched")
        assert "@rmq_trigger(ack_unmatched=True)" in html

    def test_the_enabled_checkbox_stays_editable_on_a_dag_file_row(self):
        html = _render_subscription_form_html(
            sub=_make_single_row(), is_dag_file=True
        )
        assert "disabled" not in _form_field(html, "enabled")


# ---------------------------------------------------------------------------
# Watcher processes block and the statuses it vouches for
# ---------------------------------------------------------------------------

def _heartbeat(**overrides) -> SimpleNamespace:
    """A heartbeat row of a run alive at :data:`_T0` with a completed cycle."""
    values = {
        "id": "run-1",
        "hostname": "scheduler-1",
        "pid": 4242,
        "started_at": _T0 - timedelta(minutes=10),
        "last_cycle_at": _T0 - timedelta(minutes=1),
        "due_at": _T0 + timedelta(minutes=5),
        "stopped_at": None,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _dead_heartbeat(**overrides) -> SimpleNamespace:
    values = {
        "id": "run-0",
        "hostname": "scheduler-2",
        "pid": 77,
        "started_at": _T0 - timedelta(hours=3),
        "last_cycle_at": _T0 - timedelta(hours=2),
        "due_at": _T0 - timedelta(hours=1),
    }
    values.update(overrides)
    return _heartbeat(**values)


def _liveness(state: LivenessState):
    """A verdict in ``state``, assessed at :data:`_T0`."""
    if state is LivenessState.UNUSED:
        return assess_liveness([], _T0, False, False)
    if state is LivenessState.OK:
        return assess_liveness([_heartbeat()], _T0, True, False)
    if state is LivenessState.STARTING:
        return assess_liveness([_heartbeat(last_cycle_at=None)], _T0, True, False)
    if state is LivenessState.UNCONFIRMED:
        return assess_liveness([], _T0, True, True)
    return assess_liveness([_dead_heartbeat()], _T0, True, False)


def _page_kwargs(app, view, monkeypatch, verdict, *, subs=(), conn_statuses=()):
    """Request the page with ``verdict`` as the liveness; return its template kwargs."""
    monkeypatch.setattr(
        "airflow_provider_rmq.watcher.views._read_liveness", lambda: verdict
    )
    ctx, _ = _session_ctx(subs=list(subs))
    with app.test_request_context("/subscriptions"), \
         patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
         patch(
             "airflow_provider_rmq.watcher.views.get_conn_statuses",
             return_value=list(conn_statuses),
         ), \
         patch("airflow_provider_rmq.watcher.views._stale_after", return_value=360.0), \
         patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=None):
        view.subscriptions()
    return view.render_template.call_args.kwargs


def _render_page(kwargs) -> str:
    """Feed the view's own kwargs to the real template."""
    return _render_subscriptions_html(
        kwargs["subscriptions"],
        **{k: v for k, v in kwargs.items() if k != "subscriptions"},
    )


def _normalized(html: str) -> str:
    return re.sub(r"\s+", " ", html)


def _process_rows_html(html: str) -> list[str]:
    """The ``<tr>`` rows of the Watcher processes table."""
    normalized = _normalized(html)
    block = normalized.split("<h4>Watcher processes</h4>", 1)[1].split("<h4>Connections</h4>", 1)[0]
    return re.findall(r"<tr[^>]*>.*?</tr>", block.split("<tbody>", 1)[1]) if "<tbody>" in block else []


class TestWatcherProcessesBlock:
    def test_one_alive_process_is_listed(self, app, view, monkeypatch):
        kwargs = _page_kwargs(app, view, monkeypatch, _liveness(LivenessState.OK))

        [row] = kwargs["watcher_processes"]
        assert (row.hostname, row.pid, row.state) == ("scheduler-1", 4242, "alive")
        assert row.started_display == "2026-11-17 11:50:00"
        assert row.last_mark_display == "2026-11-17 11:59:00"
        assert row.age_display == "1m"
        assert row.due_display == "2026-11-17 12:05:00"
        assert not row.is_dead
        assert kwargs["liveness_state"] == "ok"
        assert kwargs["liveness_error"] is None

    def test_several_processes_are_listed_latest_mark_first(self, app, view, monkeypatch):
        verdict = assess_liveness(
            [
                _dead_heartbeat(),
                _heartbeat(),
                _heartbeat(
                    id="run-2",
                    hostname="scheduler-3",
                    pid=5,
                    last_cycle_at=_T0 - timedelta(minutes=30),
                    stopped_at=_T0 - timedelta(minutes=20),
                ),
            ],
            _T0,
            True,
            False,
        )
        kwargs = _page_kwargs(app, view, monkeypatch, verdict)

        rows = kwargs["watcher_processes"]
        assert [(r.hostname, r.state) for r in rows] == [
            ("scheduler-1", "alive"),
            ("scheduler-3", "stopped"),
            ("scheduler-2", "dead"),
        ]
        assert [r.is_dead for r in rows] == [False, False, True]
        assert rows[1].due_display == "—"
        assert rows[1].last_mark_display == "2026-11-17 11:40:00"
        assert rows[2].age_display == "2h 0m"

    def test_the_dead_row_is_highlighted_and_the_alive_one_is_not(self, app, view, monkeypatch):
        verdict = assess_liveness([_heartbeat(), _dead_heartbeat()], _T0, True, False)
        html = _render_page(_page_kwargs(app, view, monkeypatch, verdict))

        alive_row, dead_row = _process_rows_html(html)
        assert "scheduler-1" in alive_row and 'class="danger"' not in alive_row
        assert "● alive" in alive_row
        assert "scheduler-2" in dead_row and dead_row.startswith('<tr class="danger">')
        assert "✕ dead" in dead_row

    def test_a_starting_process_renders_its_own_label(self, app, view, monkeypatch):
        html = _render_page(
            _page_kwargs(app, view, monkeypatch, _liveness(LivenessState.STARTING))
        )

        [row] = _process_rows_html(html)
        assert "○ starting" in row

    def test_no_runs_render_a_notice(self, app, view, monkeypatch):
        html = _render_page(
            _page_kwargs(app, view, monkeypatch, _liveness(LivenessState.UNCONFIRMED))
        )

        assert _process_rows_html(html) == []
        assert "No watcher process has recorded a heartbeat in the last day." in html

    @pytest.mark.parametrize("state", list(LivenessState))
    def test_the_unconfirmed_note_is_shown_only_for_unconfirmed(
        self, app, view, monkeypatch, state
    ):
        html = _render_page(_page_kwargs(app, view, monkeypatch, _liveness(state)))

        assert (UNCONFIRMED_NOTE in html) is (state is LivenessState.UNCONFIRMED)


class TestStatusesUnderTheVerdict:
    @pytest.mark.parametrize("state", list(LivenessState))
    def test_statuses_are_masked_and_the_banner_shown_only_for_down(
        self, app, view, monkeypatch, state
    ):
        kwargs = _page_kwargs(app, view, monkeypatch, _liveness(state))

        is_down = state is LivenessState.DOWN
        assert kwargs["mask_statuses"] is is_down
        assert (kwargs["liveness_banner"] is not None) is is_down

    def test_down_renders_the_banner_at_the_top_and_every_status_as_unknown(
        self, app, view, monkeypatch
    ):
        subs = [
            _make_single_row(id=1, dag_id="single_dag", consumer_status="listening"),
            _make_single_row(id=2, dag_id="off_dag", enabled=False, consumer_status="listening"),
            _make_single_row(id=3, dag_id="grp_dag", queue_name="q1", group_key="grp"),
            _make_single_row(id=4, dag_id="grp_dag", queue_name="q2", group_key="grp"),
        ]
        conn = _make_conn_status_row(last_reconcile_at=_naive_utc_ago(30))
        kwargs = _page_kwargs(
            app, view, monkeypatch, _liveness(LivenessState.DOWN),
            subs=subs, conn_statuses=[conn],
        )
        html = _normalized(_render_page(kwargs))

        banner = re.search(r'<div class="alert alert-danger">(.*?)</div>', html)
        assert banner is not None
        assert html.index(banner.group(0)) < html.index("<h4>Watcher processes</h4>")
        assert "RMQ watcher has not completed a cycle since 2026-11-17 10:00:00 UTC" in banner.group(1)
        assert '<a href="/rmq-watcher/subscriptions">RMQ Subscriptions</a>' in banner.group(1)

        assert 'title="watcher is not running — last recorded: connected"' in html
        assert 'title="watcher is not running — last recorded: listening"' in html
        assert 'title="watcher is not running — last recorded: 2/2 listening"' in html
        assert html.count("? unknown") == 3
        assert "● connected" not in html
        assert '<span class="label label-success">listening</span>' not in html
        assert "2/2 listening </span>" not in html
        assert '<span class="label label-default">disabled</span>' in html

    @pytest.mark.parametrize(
        "state", [LivenessState.OK, LivenessState.STARTING, LivenessState.UNCONFIRMED]
    )
    def test_statuses_are_shown_as_recorded_unless_down(self, app, view, monkeypatch, state):
        subs = [
            _make_single_row(id=1, dag_id="single_dag", consumer_status="listening"),
            _make_single_row(id=3, dag_id="grp_dag", queue_name="q1", group_key="grp"),
            _make_single_row(id=4, dag_id="grp_dag", queue_name="q2", group_key="grp"),
        ]
        conn = _make_conn_status_row(last_reconcile_at=_naive_utc_ago(30))
        kwargs = _page_kwargs(
            app, view, monkeypatch, _liveness(state), subs=subs, conn_statuses=[conn]
        )
        html = _normalized(_render_page(kwargs))

        assert "? unknown" not in html
        assert "alert-danger" not in html
        assert "● connected" in html
        assert '<span class="label label-success">listening</span>' in html
        assert "2/2 listening </span>" in html


class TestLivenessReadFailure:
    def test_a_failed_read_returns_nothing_and_warns(self, monkeypatch, caplog):
        def broken(session, now):
            raise OperationalError("SELECT", {}, Exception("server closed the connection"))

        monkeypatch.setattr(
            "airflow_provider_rmq.watcher.dashboard_alert.read_liveness", broken
        )
        ctx, _ = _session_ctx()
        with patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.watcher.views"):
            assert _read_liveness() is None

        [record] = [r for r in caplog.records if r.name == "airflow_provider_rmq.watcher.views"]
        assert record.levelno == logging.WARNING
        assert record.exc_info is not None

    def test_the_page_stands_with_a_notice_and_recorded_statuses(self, app, view, monkeypatch):
        sub = _make_single_row(dag_id="my_dag", consumer_status="listening")
        kwargs = _page_kwargs(app, view, monkeypatch, None, subs=[sub])

        assert kwargs["liveness_error"] == LIVENESS_UNAVAILABLE_MESSAGE
        assert kwargs["watcher_processes"] == []
        assert kwargs["mask_statuses"] is False
        assert kwargs["liveness_banner"] is None
        html = _normalized(_render_page(kwargs))
        assert LIVENESS_UNAVAILABLE_MESSAGE in html
        assert '<span class="label label-success">listening</span>' in html
        assert "my_dag" in html

    def test_a_heartbeat_row_in_the_database_reads_as_an_alive_process(
        self, schema_engine, monkeypatch
    ):
        WatcherBase.metadata.create_all(schema_engine)
        RealSession = sessionmaker(bind=schema_engine)
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with RealSession() as seed:
            seed.add(
                RMQSubscription(
                    dag_id="d", queue_name="q", conn_id="rmq_default", source="ui", enabled=True
                )
            )
            seed.commit()
        ensure_heartbeat_table()
        with RealSession() as seed:
            seed.add(
                RMQWatcherHeartbeat(
                    id="run-1",
                    hostname="scheduler-1",
                    pid=4242,
                    started_at=now - timedelta(minutes=10),
                    last_cycle_at=now - timedelta(minutes=1),
                    due_at=now + timedelta(minutes=5),
                )
            )
            seed.commit()
        monkeypatch.setattr("airflow_provider_rmq.watcher.views.WatcherSession", RealSession)

        verdict = _read_liveness()

        assert verdict is not None
        assert verdict.state is LivenessState.OK
        assert [(p.hostname, p.pid) for p in verdict.processes] == [("scheduler-1", 4242)]
