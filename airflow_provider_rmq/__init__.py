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
        "config": {
            "rmq_watcher": {
                "description": "Options for the RMQ Watcher Plugin.",
                "options": {
                    "grant_op_access": {
                        "description": (
                            "Whether the Op role holds the permissions of the "
                            "Subscriptions page: can_read, can_create, can_edit and "
                            "can_delete on the resource RMQ Subscriptions plus "
                            "menu_access on Subscriptions and on RabbitMQ. The webserver "
                            "gives the role those permissions at every start while this "
                            "is true, and takes the same six back while it is false.\n"
                        ),
                        "version_added": "2.4.0",
                        "type": "boolean",
                        "example": None,
                        "default": "True",
                    },
                },
            },
        },
    }
