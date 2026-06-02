"""Example DAG: reactive launch via RMQ Watcher Plugin.

RMQ Watcher Plugin inverts the usual sensor pattern: instead of a DAG
waiting for a message, a message in the queue *causes* the DAG to start.
No polling, no ``deferred`` task slots — the Scheduler process itself listens
to RabbitMQ via AMQP ``basic_consume`` in a background thread.

How it works
------------
1. ``@rmq_trigger`` adds a ``_rmq_subscriptions`` attribute to the DAG object.
2. The Scheduler's ``RMQWatcherListener.on_starting`` callback launches a
   background asyncio loop (``RMQConsumerManager``) that opens one
   ``connect_robust`` connection per ``conn_id``.
3. Every 60 s (configurable via Airflow Variable ``rmq_watcher_reconcile_interval``)
   the reconciliation loop re-reads DAG files and syncs subscriptions to DB.
4. When a matching message arrives the background loop calls ``trigger_dag()``
   directly (no HTTP roundtrip) and acks the message.

``conf`` passed to the DAG run
-------------------------------
::

    {
        "body":            "<UTF-8 decoded message body>",
        "headers":         {"event": "trigger", ...},   # AMQP headers
        "routing_key":     "watcher.demo",               # exchange routing key
        "queue":           "watcher-demo",
        "subscription_id": 7,                            # rmq_watcher_subscriptions.id
    }

Best-practice: dedicated queue
--------------------------------
Use a queue dedicated to this DAG (e.g. ``orders.airflow-trigger`` separate from
``orders``). When the Watcher is the sole consumer there is no risk of NACK
hot-loops on quorum queues or interference with other services.

HA note
-------
In multi-scheduler deployments every active scheduler runs its own consumer.
This may cause duplicate DAG runs. Set ``max_active_runs=1`` on the DAG as a
lightweight mitigation; full distributed lock support is planned for a future
release.
"""
from __future__ import annotations

import logging

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from airflow_provider_rmq.watcher.decorators import rmq_trigger

log = logging.getLogger(__name__)


@rmq_trigger(
    queue="watcher-demo",
    conn_id="rmq_default",
    # Only messages that carry the AMQP header ``event: trigger`` will start
    # this DAG; all others are NACKed with requeue=True.
    filter_data={"filter_headers": {"event": "trigger"}},
)
@dag(
    dag_id="rmq_watcher_demo",
    # The DAG also runs on a daily schedule — both trigger sources are fully
    # independent.  When started by the Watcher, dag_run.conf holds the
    # message payload; when started by the scheduler conf is empty ({}).
    schedule="@daily",
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    tags=["rmq", "watcher", "example"],
    doc_md=__doc__,
)
def rmq_watcher_demo():
    @task
    def process_event(**context):
        conf = context["dag_run"].conf or {}

        if conf:
            log.info("Triggered by RMQ message:")
            log.info("  body        = %s", conf.get("body"))
            log.info("  headers     = %s", conf.get("headers"))
            log.info("  routing_key = %s", conf.get("routing_key"))
            log.info("  queue       = %s", conf.get("queue"))
        else:
            log.info("Triggered by scheduler (no message payload)")

    process_event()


rmq_watcher_demo()
