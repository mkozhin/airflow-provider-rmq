from __future__ import annotations

from typing import Any


def rmq_trigger(
    queue: str | None = None,
    queues: list[str] | None = None,
    conn_id: str = "rmq_default",
    filter_data: dict[str, Any] | None = None,
    cooldown: int = 0,
):
    """Attach an RMQ subscription to a DAG.

    The decorator appends one or more entries to ``dag._rmq_subscriptions``.
    Stacking multiple ``@rmq_trigger`` calls on the same DAG produces multiple
    entries (one consumer per queue). The DAG object itself is returned unchanged.

    :param queue: Name of a single RabbitMQ queue to watch.  Mutually exclusive
        with ``queues``.
    :param queues: List of RabbitMQ queue names to watch.  One subscription entry
        is created per queue; all share the same ``conn_id``, ``filter_data``,
        and ``cooldown``.  Mutually exclusive with ``queue``.
    :param conn_id: Airflow connection ID for the RabbitMQ broker
        (default: ``"rmq_default"``).
    :param filter_data: Optional message filter in the format returned by
        ``MessageFilter.serialize()``: ``{"filter_headers": {...}}``.
        ``None`` (default) means no filter — any message triggers the DAG.
        Flat dicts are not normalised; pass the exact format.
    :param cooldown: Seconds to wait before triggering the DAG after the first
        matching message (default ``0`` — immediate trigger).  When ``cooldown > 0``
        the DLX pattern is used: the first message publishes a TTL bearer to
        ``rmq_watcher.pending.{dag_id}`` (``x-max-length=1``); subsequent
        messages in the window are silently rejected by the broker.  After the
        TTL expires the bearer is dead-lettered to ``rmq_watcher.fire`` and the
        DAG is triggered exactly once.

    Cooldown limitations
    --------------------
    * **conf body/headers are empty for cooldown triggers.**  The original
      message data is not forwarded through the DLX chain; ``conf["body"]``
      and ``conf["headers"]`` are always ``""`` / ``{}`` for cooldown-triggered
      DAG runs.  Only ``conf["source"] == "cooldown"`` and ``conf["dag_id"]``
      are reliable.
    * **Cooldown changes take effect on the next reconcile cycle**, not
      instantly.  Already-published pending messages keep the old TTL.
    * **All cooldown subscriptions for one dag_id share one pending queue and
      one timer.**  Stacking multiple ``@rmq_trigger(cooldown > 0)`` on the
      same DAG shares the cooldown window.  This is intentional.
    * **All cooldown DAGs must use one conn_id/vhost.**  The
      ``rmq_watcher.*`` infrastructure queues are created in the vhost of the
      first cooldown connection found.  Mixing conn_ids with cooldown is not
      supported.
    * **No consumer is ever attached to ``rmq_watcher.pending.{dag_id}``.**
      The queue acts as a pure timer — its only purpose is to expire the
      message and dead-letter it to ``rmq_watcher.fire``.
    * **Orphaned pending queues are not deleted automatically** when a
      cooldown subscription is removed.  A WARNING is logged on the first
      reconcile after removal.  Manual cleanup command::

          rabbitmqadmin delete queue name=rmq_watcher.pending.<dag_id>

    ``group_key`` is NOT set by the decorator; it is set by the listener where
    the dag_id is known (``group_key = dag_id`` when ``cooldown > 0``, else
    ``None``).

    Best practice: use a dedicated queue per DAG trigger (e.g.
    ``orders.airflow-trigger`` separate from ``orders``) to avoid interference
    with other consumers on the same queue.
    """
    if queue is not None and queues is not None:
        raise ValueError("Specify either 'queue' or 'queues', not both.")
    if queue is None and queues is None:
        raise ValueError("Either 'queue' or 'queues' must be specified.")
    if cooldown < 0:
        raise ValueError("'cooldown' must be >= 0.")

    if queues is not None:
        subscriptions = [
            {
                "queue_name": q,
                "conn_id": conn_id,
                "filter_data": filter_data or {},
                "cooldown": cooldown,
            }
            for q in queues
        ]
    else:
        subscriptions = [
            {
                "queue_name": queue,
                "conn_id": conn_id,
                "filter_data": filter_data or {},
                "cooldown": cooldown,
            }
        ]

    def decorator(dag):
        if not hasattr(dag, "_rmq_subscriptions"):
            dag._rmq_subscriptions = []
        dag._rmq_subscriptions.extend(subscriptions)
        return dag

    return decorator
