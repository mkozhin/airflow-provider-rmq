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

``conf`` passed to the DAG run (immediate trigger, cooldown=0)
---------------------------------------------------------------
::

    {
        "source":          "immediate",
        "body":            "<UTF-8 decoded message body>",
        "headers":         {"event": "trigger", ...},   # AMQP headers
        "routing_key":     "watcher.demo",               # exchange routing key
        "queue":           "watcher-demo",
        "subscription_id": 7,                            # rmq_watcher_subscriptions.id
    }

``conf`` passed to the DAG run (cooldown trigger)
-------------------------------------------------
When ``cooldown > 0`` the original message data is lost in the DLX chain —
only metadata survives::

    {
        "source":          "cooldown",
        "dag_id":          "<dag_id>",
        "body":            "",          # always empty — original body not available
        "headers":         {},          # always empty — original headers not available
        "routing_key":     "<dag_id>",
        "queue":           "rmq_watcher.fire",
        "subscription_id": None,
    }

Multi-queue and cooldown
------------------------
Use ``queues=[...]`` to watch several queues with one decorator call.  A message
from *any* of those queues starts the DAG.  Combine with ``cooldown=N`` (seconds)
to collapse bursts: only one DAG run is triggered per cooldown window, no matter
how many messages arrive.

Cooldown mechanism (DLX pattern, no Airflow DB writes)
``````````````````````````````````````````````````````
* **rmq_watcher.pending.{dag_id}** — a durable queue with ``x-max-length=1``,
  ``x-overflow=reject-publish``, and the dead-letter exchange set to
  ``rmq_watcher.fire``.  The first matching message publishes a TTL-bearer
  message here; subsequent messages are silently rejected by the broker while
  the timer is active.  **No consumer is attached** to this queue.
* **rmq_watcher.fire** — a topic exchange and same-named durable queue.  When
  the TTL expires the bearer message is dead-lettered here with ``routing_key``
  set to the dag_id.  The fire consumer reads this queue and calls
  ``trigger_dag()``.
* Cooldown changes (e.g. from a DAG-file edit) take effect on the next
  reconcile cycle, not instantly; already-published pending messages keep the
  old TTL.

Limitations
-----------
* **All ``cooldown``-subscriptions for one dag_id share one timer.** Stacking
  multiple ``@rmq_trigger`` with ``cooldown > 0`` on the same DAG shares the
  pending queue and the cooldown window.  This is intentional.
* **All cooldown DAGs must share one conn_id/vhost.**  The ``rmq_watcher.*``
  infrastructure queues are created in the vhost of the first cooldown
  connection found.  Mixing conn_ids with cooldown is unsupported.
* **Orphaned pending queues are not deleted automatically.**  If a cooldown
  subscription is removed the corresponding ``rmq_watcher.pending.{dag_id}``
  queue remains in RabbitMQ.  A WARNING is logged on the first reconcile cycle
  after removal.  Manual cleanup: ``rabbitmqadmin delete queue name=rmq_watcher.pending.<dag_id>``.

Best-practice: dedicated queue
--------------------------------
Use a queue dedicated to this DAG (e.g. ``orders.airflow-trigger`` separate from
``orders``). When the Watcher is the sole consumer there is no risk of NACK
hot-loops on quorum queues or interference with other services.

HA note
-------
In multi-scheduler deployments every active scheduler runs its own consumer.
For immediate triggers (cooldown=0) this may cause duplicate DAG runs — set
``max_active_runs=1`` as a lightweight mitigation.  For cooldown triggers the
DLX mechanism naturally deduplicates: the pending queue has ``x-max-length=1``
so only one timer fires, and the run_id is derived from the message_id set at
publish time, making it idempotent across redeliveries.
"""
from __future__ import annotations

import logging

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from airflow_provider_rmq.watcher.decorators import rmq_trigger

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Example 1: single queue, immediate trigger (original behaviour)
# ---------------------------------------------------------------------------

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
            log.info("  source      = %s", conf.get("source"))
            log.info("  body        = %s", conf.get("body"))
            log.info("  headers     = %s", conf.get("headers"))
            log.info("  routing_key = %s", conf.get("routing_key"))
            log.info("  queue       = %s", conf.get("queue"))
        else:
            log.info("Triggered by scheduler (no message payload)")

    process_event()


rmq_watcher_demo()


# ---------------------------------------------------------------------------
# Example 2: multiple queues with cooldown
#
# Messages arriving in *either* "orders" or "payments" start the DAG, but at
# most once every 5 minutes (300 s).  All messages in the cooldown window are
# ACKed; the DAG receives a synthetic conf (body and headers are empty because
# the original message data is not forwarded through the DLX chain).
# ---------------------------------------------------------------------------

@rmq_trigger(
    queues=["orders", "payments"],
    cooldown=300,           # trigger at most once per 5 minutes
    conn_id="rmq_default",
)
@dag(
    dag_id="rmq_watcher_demo_cooldown",
    schedule=None,          # event-driven only
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    tags=["rmq", "watcher", "example", "cooldown"],
)
def rmq_watcher_demo_cooldown():
    @task
    def process_batch(**context):
        conf = context["dag_run"].conf or {}

        source = conf.get("source", "scheduler")
        if source == "cooldown":
            # Triggered by the DLX fire mechanism after the cooldown window.
            # Note: body and headers are always empty for cooldown triggers.
            log.info("Triggered by cooldown mechanism (DLX)")
            log.info("  dag_id = %s", conf.get("dag_id"))
        else:
            log.info("Triggered by scheduler or immediate trigger")

    process_batch()


rmq_watcher_demo_cooldown()
