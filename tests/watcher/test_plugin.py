"""Tests for RMQWatcherPlugin registration."""
from __future__ import annotations

from airflow_provider_rmq.watcher.plugin import RMQWatcherPlugin


def test_plugin_name():
    assert RMQWatcherPlugin.name == "rmq_watcher"


def test_plugin_has_listener():
    assert len(RMQWatcherPlugin.listeners) == 1
    assert type(RMQWatcherPlugin.listeners[0]).__name__ == "RMQWatcherListener"


def test_plugin_has_appbuilder_view():
    views = RMQWatcherPlugin.appbuilder_views
    assert len(views) == 1
    assert views[0]["name"] == "Subscriptions"
    assert views[0]["category"] == "RabbitMQ Watcher"
    assert type(views[0]["view"]).__name__ == "RMQWatcherView"


def test_plugin_has_blueprint_with_templates():
    import os
    bps = RMQWatcherPlugin.flask_blueprints
    assert len(bps) == 1
    bp = bps[0]
    assert bp.name == "rmq_watcher"
    for tmpl in ["subscriptions.html", "subscription_form.html"]:
        path = os.path.join(bp.template_folder, "rmq_watcher", tmpl)
        assert os.path.isfile(path), f"template missing from blueprint: {tmpl}"


def test_rmq_trigger_importable_from_watcher():
    from airflow_provider_rmq.watcher import rmq_trigger
    assert callable(rmq_trigger)
