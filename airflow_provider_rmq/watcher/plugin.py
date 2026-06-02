from __future__ import annotations

import os

from flask import Blueprint
from airflow.plugins_manager import AirflowPlugin

from airflow_provider_rmq.watcher.listener import RMQWatcherListener
from airflow_provider_rmq.watcher.models import ensure_table_exists
from airflow_provider_rmq.watcher.views import RMQWatcherView

_bp = Blueprint(
    "rmq_watcher",
    __name__,
    template_folder=os.path.join(os.path.dirname(__file__), "templates"),
)


class RMQWatcherPlugin(AirflowPlugin):
    name = "rmq_watcher"
    listeners = [RMQWatcherListener()]
    flask_blueprints = [_bp]
    appbuilder_views = [
        {
            "name": "Subscriptions",
            "category": "RabbitMQ Watcher",
            "view": RMQWatcherView(),
        }
    ]

    @classmethod
    def on_load(cls, *args, **kwargs):
        """Create rmq_watcher_* tables when the plugin is first loaded."""
        ensure_table_exists()
