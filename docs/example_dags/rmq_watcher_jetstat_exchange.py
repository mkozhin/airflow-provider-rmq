"""Example DAG: exchange-mode trigger on a single Jetstat report's success.

Subscribes directly to the ``jetstat.airflow`` topic exchange instead of a
pre-existing, manually-bound queue. The provider owns the RMQ infrastructure
end-to-end: it declares the exchange, a dedicated ``rmq_watcher.sub.{dag_id}``
queue, and keeps that queue's bindings in sync with the routing keys declared
below — no manual queue creation, no external YAML route table.

This DAG watches exactly one report id and only its ``succeeded`` status —
useful when a DAG's downstream work depends on one specific upstream report
having finished successfully, as opposed to ``queue=``/``queues=`` which
would require Jetstat (or whatever publishes to the exchange) to know about
this DAG's queue ahead of time.

Routing key shape
------------------
``routing_key_ids`` is expanded against ``routing_key_status`` as an
``id × status`` cross-product — ``"<id>.<status>"``. With a single id and a
single status this resolves to one routing key:
``"670f877702775c2de8325b1f.succeeded"``.

To react to *any* terminal status instead of only success, either drop
``routing_key_status`` (defaults to ``"*"``, matching any status segment) and
branch on ``conf["routing_key"]`` inside the DAG, or list every status
explicitly: ``routing_key_status=["succeeded", "failed"]``.

``conf`` passed to the DAG run
-------------------------------
Exchange-mode triggers reuse the same "immediate" shape as a plain
``queue=`` subscription — ``"queue"`` is always
``rmq_watcher.sub.{dag_id}`` (not the exchange name), and ``"routing_key"``
is the actual matched routing key::

    {
        "source":          "immediate",
        "body":            "<UTF-8 decoded report payload>",
        "headers":         {...},
        "routing_key":     "670f877702775c2de8325b1f.succeeded",
        "queue":           "rmq_watcher.sub.rmq_watcher_jetstat_report_succeeded",
        "subscription_id": 12,
    }

Connection requirement: ``management_url``
---------------------------------------------
Exchange-mode needs a Management HTTP API endpoint to read current bindings
(plain AMQP has no "show my bindings" operation). Add it to the same Airflow
Connection used for AMQP, as a connection ``extra``::

    {
        "management_url": "https://rabbitmq.example.com"
    }

RabbitMQ permissions
---------------------
In addition to the ``rmq_watcher\\..*`` pattern required for cooldown, the
Airflow RMQ user needs configure/write on ``jetstat.airflow`` and its
``.unrouted``/``.log`` safety-net queues, plus read on ``jetstat.airflow``
and ``jetstat.airflow.unrouted``::

    rabbitmqctl set_permissions -p <vhost> <user> \\
        "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted|\\.log)?)$" \\
        "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted|\\.log)?)$" \\
        "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted)?)$"

Gotcha: literal arguments only
-------------------------------
``@rmq_trigger(...)`` is re-parsed from the DAG file's source via ``ast`` on
every reconcile cycle — only literal values are recognised. Referencing a
module-level variable (e.g. a ``REPORT_ID`` constant) for ``routing_key_ids``
is silently ignored and the subscription is skipped. Write the report id
directly as a string literal in the decorator call, as below.
"""
from __future__ import annotations

import logging

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

from airflow_provider_rmq.watcher.decorators import rmq_trigger

log = logging.getLogger(__name__)


@rmq_trigger(
    exchange="jetstat.airflow",
    routing_key_ids=["670f877702775c2de8325b1f"],
    routing_key_status="succeeded",  # only the success event starts the DAG
    conn_id="rmq_default",
)
@dag(
    dag_id="rmq_watcher_jetstat_report_succeeded",
    schedule=None,           # event-driven only
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    tags=["rmq", "watcher", "example", "exchange", "jetstat"],
    doc_md=__doc__,
)
def rmq_watcher_jetstat_report_succeeded():
    @task
    def process_report(**context):
        conf = context["dag_run"].conf or {}
        routing_key = conf.get("routing_key", "")
        report_id, _, status = routing_key.partition(".")

        log.info("Jetstat report finished: report_id=%s status=%s", report_id, status)
        log.info("  body  = %s", conf.get("body"))
        log.info("  queue = %s", conf.get("queue"))  # rmq_watcher.sub.{dag_id}

    process_report()


rmq_watcher_jetstat_report_succeeded()
