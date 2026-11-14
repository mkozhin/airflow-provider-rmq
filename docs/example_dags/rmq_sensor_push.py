"""Example DAG: Deferrable sensor in push mode.

Demonstrates RMQSensor with ``mode="push"`` — the triggerer subscribes to the
queue via AMQP ``basic_consume`` and the broker delivers messages instantly as
they arrive, without polling delay.

When to use push vs pull
------------------------
* **push** — prefer when low latency matters (payment events, real-time alerts)
  or when the queue is mostly idle and polling would waste connections.
* **pull** (default) — prefer when simplicity is more important than latency,
  or when filter logic causes many NACKs (see quorum queue note below).

Quorum queue note
-----------------
At the default ``ack_unmatched=False`` a rejected message is NACKed with
``requeue=True`` (``basic.nack``). A quorum queue's ``delivery-limit`` counts
only ``basic.reject`` and failed deliveries, so a rejected message goes back to
the head of the queue and is evaluated again, indefinitely. If heavy filtering
is expected, use a dedicated queue for the specific message type with
``ack_unmatched=True`` on it, where a rejected message is acknowledged and
never redelivered.

message_wait_timeout
--------------------
The optional ``message_wait_timeout`` sets how long (in seconds) the trigger
waits for a matching message before giving up with a ``RuntimeError``. The
actual wait may slightly exceed this value while the AMQP consumer is cancelled
gracefully (``basic_cancel`` round-trip to the broker).
``None`` (default) means wait indefinitely — the sensor's own ``timeout``
parameter still applies at the DAG level.
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.decorators import dag, task

from airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator
from airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator
from airflow_provider_rmq.sensors.rmq import RMQSensor

log = logging.getLogger("airflow.task")

RMQ_CONN_ID = "rmq_default"
QUEUE_NAME = "example_push"


@dag(
    dag_id="rmq_sensor_push",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "rmq", "push", "deferrable"],
    doc_md="""
    ### Push Mode Sensor Example

    Uses `deferrable=True` with `mode="push"` — the broker delivers messages
    instantly via `basic_consume` instead of periodic polling.

    Key differences from the deferrable pull mode:
    - No `poll_interval` delay — message is processed as soon as it arrives.
    - Optional `message_wait_timeout` limits how long to wait for a message.
    - `filter_callable` is NOT supported (same as all deferrable modes).
    """,
)
def rmq_sensor_push():
    setup = RMQQueueManagementOperator(
        task_id="setup_queue",
        action="declare_queue",
        queue_name=QUEUE_NAME,
        durable=True,
        rmq_conn_id=RMQ_CONN_ID,
    )

    # Publish a non-matching message first to demonstrate filtering
    publish_noise = RMQPublishOperator(
        task_id="publish_noise",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_NAME,
        message={"event": "page_view"},
        headers={"x-type": "analytics"},
    )

    publish_order = RMQPublishOperator(
        task_id="publish_order",
        rmq_conn_id=RMQ_CONN_ID,
        queue_name=QUEUE_NAME,
        message={"event": "order_created", "order_id": 42},
        headers={"x-type": "order"},
        delivery_mode=2,
    )

    # Push-mode sensor: broker delivers messages instantly.
    # message_wait_timeout=60 — give up after 60 s if no matching message arrives.
    wait_for_order = RMQSensor(
        task_id="wait_for_order",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
        filter_headers={"x-type": "order"},
        deferrable=True,
        mode="push",
        message_wait_timeout=60,
        timeout=120,
    )

    @task
    def process_message(message: dict) -> dict:
        """Log and process the message received by the sensor."""
        log.info("Push sensor matched message: %s", message)
        return message

    cleanup = RMQQueueManagementOperator(
        task_id="cleanup_queue",
        action="delete_queue",
        queue_name=QUEUE_NAME,
        rmq_conn_id=RMQ_CONN_ID,
    )

    setup >> publish_noise >> publish_order >> wait_for_order
    processed = process_message(wait_for_order.output)
    processed >> cleanup


rmq_sensor_push()
