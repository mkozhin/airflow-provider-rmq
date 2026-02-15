__version__ = "1.0.0"


def get_provider_info():
    return {
        "package-name": "apache-airflow-provider-rmq",
        "name": "RabbitMQ Airflow Provider",
        "description": "RabbitMQ provider for Apache Airflow with publish, consume, sensor, and management operators.",
        "connection-types": [
            {
                "hook-class-name": "apache_airflow_provider_rmq.hooks.rmq.RMQHook",
                "connection-type": "amqp",
            }
        ],
        "versions": [__version__],
    }
