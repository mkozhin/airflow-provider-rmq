"""Example DAG 1: Basic publish and consume.

Demonstrates the simplest use case — declare a queue,
publish a message, wait for it with a sensor, consume it, and clean up.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

from apache_airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator
from apache_airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from apache_airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator
from apache_airflow_provider_rmq.sensors.rmq import RMQSensor

RMQ_CONN_ID = "rmq_default"
QUEUE_NAME = "example_basic"

with DAG(
    dag_id="rmq_example_basic",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq"],
    doc_md="""
    ### Basic RabbitMQ Example
    1. Creates a durable queue
    2. Publishes a JSON message
    3. Waits for the message with RMQSensor
    4. Consumes and prints the message
    5. Deletes the queue
    """,
) as dag:
    create_queue = RMQQueueManagementOperator(
        task_id="create_queue",
        action="declare_queue",
        queue_name=QUEUE_NAME,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    publish = RMQPublishOperator(
        task_id="publish_message",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_NAME,
        message={"event": "order_created", "order_id": 42},
    )

    wait = RMQSensor(
        task_id="wait_for_message",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        poke_interval=5,
        timeout=60,
    )

    consume = RMQConsumeOperator(
        task_id="consume_messages",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        max_messages=10,
    )

    cleanup = RMQQueueManagementOperator(
        task_id="delete_queue",
        action="delete_queue",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
    )

    create_queue >> publish >> wait >> consume >> cleanup
