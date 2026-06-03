"""Tests for RMQWatcherPlugin registration."""
from __future__ import annotations

from unittest.mock import patch

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
    assert views[0]["category"] == "RabbitMQ"
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


def test_on_load_does_not_raise_on_ensure_table_error():
    """P1: исключение в ensure_table_exists не должно всплывать из on_load."""
    with patch(
        "airflow_provider_rmq.watcher.plugin.ensure_table_exists",
        side_effect=Exception("DB unavailable"),
    ):
        RMQWatcherPlugin.on_load()  # не должен бросить исключение
