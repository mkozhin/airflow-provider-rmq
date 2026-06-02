try:
    from airflow_provider_rmq._version import version as __version__
except ImportError:
    __version__ = "0.0.0-dev"


def get_provider_info():
    return {
        "package-name": "airflow-provider-rmq",
        "name": "RabbitMQ",
        "description": "`RabbitMQ <https://www.rabbitmq.com/>`__ provider for Apache Airflow. "
            "Reactively trigger DAGs from queue messages via the RMQ Watcher Plugin — "
            "no polling, no worker slots. "
            "Also includes hooks, operators, sensors, and deferrable triggers.",
        "versions": [__version__],
        "integrations": [
            {
                "integration-name": "RabbitMQ",
                "external-doc-url": "https://www.rabbitmq.com/docs",
                "tags": ["service"],
            },
        ],
        "operators": [
            {
                "integration-name": "RabbitMQ",
                "python-modules": [
                    "airflow_provider_rmq.operators.rmq_publish",
                    "airflow_provider_rmq.operators.rmq_consume",
                    "airflow_provider_rmq.operators.rmq_management",
                ],
            },
        ],
        "sensors": [
            {
                "integration-name": "RabbitMQ",
                "python-modules": ["airflow_provider_rmq.sensors.rmq"],
            },
        ],
        "hooks": [
            {
                "integration-name": "RabbitMQ",
                "python-modules": ["airflow_provider_rmq.hooks.rmq"],
            },
        ],
        "triggers": [
            {
                "integration-name": "RabbitMQ",
                "python-modules": ["airflow_provider_rmq.triggers.rmq"],
            },
        ],
        "connection-types": [
            {
                "hook-class-name": "airflow_provider_rmq.hooks.rmq.RMQHook",
                "connection-type": "amqp",
            },
        ],
        "plugins": [
            {
                "name": "rmq_watcher",
                "plugin-class": "airflow_provider_rmq.watcher.plugin.RMQWatcherPlugin",
            },
        ],
    }
