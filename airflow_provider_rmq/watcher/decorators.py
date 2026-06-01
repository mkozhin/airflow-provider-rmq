from __future__ import annotations

from typing import Any


def rmq_trigger(
    queue: str,
    conn_id: str = "rmq_default",
    filter_data: dict[str, Any] | None = None,
):
    """Attach an RMQ subscription to a DAG.

    The decorator appends an entry to ``dag._rmq_subscriptions``. Stacking
    multiple ``@rmq_trigger`` calls on the same DAG produces multiple entries
    (one consumer per queue). The DAG object itself is returned unchanged.

    ``filter_data`` must be in the format returned by ``MessageFilter.serialize()``:
    ``{"filter_headers": {...}}``. Passing ``None`` means no filter — any message
    triggers the DAG. Flat dicts are not normalised; pass the exact format.

    Best practice: use a dedicated queue per DAG trigger (e.g.
    ``orders.airflow-trigger`` separate from ``orders``) to avoid interference
    with other consumers on the same queue.
    """
    subscription = {
        "queue_name": queue,
        "conn_id": conn_id,
        "filter_data": filter_data if filter_data is not None else {},
    }

    def decorator(dag):
        if not hasattr(dag, "_rmq_subscriptions"):
            dag._rmq_subscriptions = []
        dag._rmq_subscriptions.append(subscription)
        return dag

    return decorator
