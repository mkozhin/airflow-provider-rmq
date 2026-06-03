"""Tests for RMQWatcherView — business logic, mocked DB and template rendering.

Flask-AppBuilder's ``@has_access`` is patched to a no-op before the views
module is (re)loaded, so view methods can be called directly in tests without
a logged-in user or a full Airflow web-app context.
"""
from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest
from flask import Flask

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

from airflow_provider_rmq.watcher.views import RMQWatcherView  # noqa: E402

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
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[]):
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
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[cs]):
            view.subscriptions()

        kwargs = view.render_template.call_args.kwargs
        assert kwargs["conn_statuses"] == [cs]

    def test_subscriptions_list_shows_consumer_status_badge(self, app, view):
        """render_template receives the subscriptions list (badges are rendered from it)."""
        sub = _make_sub(consumer_status="error")
        ctx, _ = _session_ctx(subs=[sub])
        with app.test_request_context("/subscriptions"), \
             patch("airflow_provider_rmq.watcher.views.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.views.get_conn_statuses", return_value=[]):
            view.subscriptions()

        kwargs = view.render_template.call_args.kwargs
        assert sub in kwargs["subscriptions"]


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
