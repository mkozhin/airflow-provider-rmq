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
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask
from jinja2 import ChoiceLoader, DictLoader, Environment, FileSystemLoader
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from airflow_provider_rmq.watcher.models import RMQSubscription, WatcherBase

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

from airflow_provider_rmq.watcher.views import RMQWatcherView, SubscriptionGroup, _group_subscriptions  # noqa: E402

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
    return sub


def _make_conn_status(conn_id: str = "rmq_default", status: str = "connected") -> MagicMock:
    cs = MagicMock()
    cs.conn_id = conn_id
    cs.status = status
    cs.consumer_count = 1
    cs.last_error = None
    return cs


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
        cs = _make_conn_status()
        ctx, _ = _session_ctx()
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[cs]), \
             patch("airflow_provider_rmq.watcher.views.get_active_dag_ids", return_value=set()):
            view.subscriptions()

        kwargs = view.render_template.call_args.kwargs
        assert kwargs["conn_statuses"] == [cs]

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
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
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
            "rmq_watcher/subscription_form.html", sub=None, is_dag_file=False
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
            "rmq_watcher/subscription_form.html", sub=None, is_dag_file=False
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


def _badge_present(html: str) -> bool:
    """True iff the dag-not-found badge markup is present: the
    ``label label-warning`` span AND its visible text nested inside it (not
    just "dag not found" hiding in e.g. the ``title`` attribute).
    Whitespace-normalized so it's robust to line wrapping in the template.
    """
    normalized = re.sub(r"\s+", " ", html)
    return (
        re.search(
            r'<span class="label label-warning"[^>]*>\s*⚠ dag not found\s*</span>',
            normalized,
        )
        is not None
    )


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
