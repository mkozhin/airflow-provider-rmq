"""Example DAG: debounce two queues with a cooldown delay.

Subscribes one DAG to two independent queues (``orders`` and ``payments``).
A message from *either* queue starts the cooldown timer; any further messages
from *either* queue while the timer is running are silently ACKed. The DAG is
triggered exactly once per cooldown window, no matter how many messages
arrived during it.

Why debounce instead of triggering on every message
-----------------------------------------------------
Use this pattern when the DAG re-reads the *current* state from its own
source of truth (a database query, an API call, ...) rather than processing
each individual message. A burst of N events should still result in a single
DAG run, not N runs.

``conf`` passed to the DAG run
-------------------------------
Because ``cooldown > 0`` is set on both queues, every trigger goes through
the DLX timer mechanism — ``conf["source"]`` is always ``"cooldown"`` and the
original message body/headers are not forwarded::

    {
        "source":          "cooldown",
        "dag_id":          "rmq_watcher_orders_payments_cooldown",
        "body":            "",   # always empty for cooldown triggers
        "headers":         {},   # always empty for cooldown triggers
        "routing_key":     "rmq_watcher_orders_payments_cooldown",
        "queue":           "rmq_watcher.fire",
        "subscription_id": None,
    }

There is no way to tell from ``conf`` which of the two queues fired first —
if that matters, subscribe to each queue in its own DAG instead of sharing
one ``queues=[...]`` call.

RabbitMQ permissions
---------------------
``cooldown > 0`` requires configure/write/read on the ``rmq_watcher.*``
resource pattern in addition to read access on ``orders``/``payments``::

    rabbitmqctl set_permissions -p <vhost> <user> \\
        "^(rmq_watcher\\..*|orders|payments)$" \\
        "^(rmq_watcher\\..*|orders|payments)$" \\
        "^(rmq_watcher\\..*|orders|payments)$"

Gotcha: statically readable arguments
--------------------------------------
The watcher reads ``@rmq_trigger(...)`` from this file's source via ``ast``
and never executes the file, so each argument must be something the source
alone says: a literal, or a constant assigned a literal exactly once at the
top level of this file, above the DAG function. ``COOLDOWN = 300`` followed
by ``cooldown=COOLDOWN`` is read, and so is ``queues=[ORDERS_Q,
PAYMENTS_Q]`` with two string constants; a list or dict constant is read
only if the file uses it nowhere but in ``rmq_trigger`` arguments. An
imported constant, a function call or an f-string is not read: the
decorator then fails the import of this file with
``RMQTriggerNotStaticError`` (a Broken DAG in the Airflow UI), and the
watcher registers no subscription of the file and logs an ERROR naming the
parameter and the reason. ``dag_id`` in ``@dag(...)`` follows the same rule
and must be a string. See "How it works" in the README.
"""
from __future__ import annotations

import logging

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from airflow_provider_rmq.watcher.decorators import rmq_trigger

log = logging.getLogger(__name__)


@rmq_trigger(
    queues=["orders", "payments"],
    cooldown=300,  # collapse bursts: trigger at most once every 5 minutes
    conn_id="rmq_default",
)
@dag(
    dag_id="rmq_watcher_orders_payments_cooldown",
    schedule=None,           # event-driven only — no scheduler-triggered runs
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    tags=["rmq", "watcher", "example", "cooldown"],
    doc_md=__doc__,
)
def rmq_watcher_orders_payments_cooldown():
    @task
    def reconcile_latest_state(**context):
        conf = context["dag_run"].conf or {}

        if conf.get("source") == "cooldown":
            log.info(
                "Cooldown window elapsed for dag_id=%s — re-reading current "
                "state from source of truth (orders + payments)",
                conf.get("dag_id"),
            )
            # conf["body"]/conf["headers"] are always empty here — do not
            # rely on message payload, query your own data source instead.
        else:
            # Manual trigger via UI/API, or schedule!=None in a different setup.
            log.info("Triggered without an RMQ message (source=%s)", conf.get("source"))

    reconcile_latest_state()


rmq_watcher_orders_payments_cooldown()
