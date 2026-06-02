from __future__ import annotations

from airflow.plugins_manager import AirflowPlugin

from airflow_provider_rmq.watcher.listener import RMQWatcherListener
from airflow_provider_rmq.watcher.models import ensure_table_exists
from airflow_provider_rmq.watcher.views import RMQWatcherView


class RMQWatcherPlugin(AirflowPlugin):
    name = "rmq_watcher"
    listeners = [RMQWatcherListener()]
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
