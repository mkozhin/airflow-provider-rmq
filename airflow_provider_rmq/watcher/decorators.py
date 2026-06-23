from __future__ import annotations

import functools
from typing import Any

from airflow_provider_rmq.watcher.subscription_builder import (
    build_subscriptions,
    has_exchange_conflict,
)


def rmq_trigger(
    queue: str | None = None,
    queues: list[str] | None = None,
    conn_id: str = "rmq_default",
    filter_data: dict[str, Any] | None = None,
    cooldown: int = 0,
    *,
    exchange: str | None = None,
    routing_keys: list[str] | None = None,
    routing_key_ids: list[str] | None = None,
    routing_key_status: str | list[str] = "*",
):
    """Attach an RMQ subscription to a DAG.

    The decorator appends one or more entries to ``dag._rmq_subscriptions``.
    Stacking multiple ``@rmq_trigger`` calls on the same DAG produces multiple
    entries (one consumer per queue). The DAG object itself is returned unchanged.

    :param queue: Name of a single RabbitMQ queue to watch.  Mutually exclusive
        with ``queues``/``exchange``.  This is the first positional parameter,
        so ``@rmq_trigger("some.name")`` is always treated as ``queue="some.name"``
        — never as ``exchange=``.  Exchange-mode requires the explicit
        ``exchange=`` keyword; there is no positional shortcut for it.
    :param queues: List of RabbitMQ queue names to watch.  One subscription entry
        is created per queue; all share the same ``conn_id``, ``filter_data``,
        and ``cooldown``.  Mutually exclusive with ``queue``/``exchange``.
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
    :param exchange: Name of a topic exchange to subscribe to.  Mutually
        exclusive with ``queue``/``queues``.  When given, the provider owns
        the RMQ infrastructure end-to-end: it declares the exchange
        (idempotent, active declare), declares a dedicated queue
        ``rmq_watcher.sub.{dag_id}`` (one shared queue per DAG, TTL 8h —
        insurance against unbounded growth if the subscription becomes
        orphaned, not a timer mechanism), and binds/unbinds that queue to
        exactly the routing keys currently declared in the decorator
        (recomputed every reconcile cycle against RabbitMQ's actual binding
        state via the Management HTTP API).  Requires at least one of
        ``routing_keys``/``routing_key_ids``.  ``exchange`` must not start
        with ``"rmq_watcher."`` (reserved for cooldown/fire infrastructure).
        **Stacking multiple ``@rmq_trigger(exchange=...)`` on the same DAG is
        not supported** and raises ``ValueError`` — they would all resolve to
        the same ``rmq_watcher.sub.{dag_id}`` queue; use one decorator call
        with the union of routing keys instead.
    :param routing_keys: Literal topic routing keys of any shape, used as-is.
        Can be combined with ``routing_key_ids``; the final routing key set
        is the union of both (see ADR-0004). Only valid together with
        ``exchange``.
    :param routing_key_ids: Jetstat-shaped routing keys: each ``id`` is
        expanded to ``f"{id}.{status}"`` for every entry in
        ``routing_key_status``. Can be combined with ``routing_keys``; the
        final routing key set is the union of both (see ADR-0004). Only valid
        together with ``exchange``. Neither ``routing_key_ids`` entries nor
        ``routing_key_status`` entries may contain ``"."``.
    :param routing_key_status: Status segment(s) to cross with
        ``routing_key_ids``. A single ``str`` or a ``list[str]``. Defaults to
        ``"*"`` — the AMQP topic wildcard segment, matching any status.

    Example (exchange-mode, Jetstat id × status cross-product)::

        @rmq_trigger(
            exchange="jetstat.airflow",
            routing_key_ids=["670f877702775c2de8325b1f"],
            routing_key_status="succeeded",  # defaults to "*" = any status
        )

    Example (exchange-mode, literal routing keys of any shape)::

        @rmq_trigger(exchange="some.other.exchange", routing_keys=["region.eu.alert"])

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

    Decorator order and DAG types
    ------------------------------
    ``@rmq_trigger`` supports two ways of decorating a DAG:

    * **A ready ``DAG`` instance** — the classic ``with DAG(...) as dag:``
      style. Subscriptions are attached immediately, at decoration time.
    * **An uncalled TaskFlow factory from ``@dag(...)``** — the style used
      throughout this repository's examples and README::

          @rmq_trigger(queue="orders")
          @dag(dag_id="my_dag", schedule=None)
          def my_dag():
              ...

          my_dag()

      ``airflow.decorators.dag(...)`` returns a plain factory function, not a
      ``DAG`` — the real ``DAG`` only exists after the factory is called
      (``my_dag()`` at the end of the file). In this case ``@rmq_trigger``
      wraps the factory and defers attaching subscriptions until the wrapper
      itself is called, then returns the resulting ``DAG``.

    **``@rmq_trigger`` must be the outermost decorator, placed above
    ``@dag(...)``, never below it.** Putting it below (i.e.
    ``@dag(...)`` then ``@rmq_trigger(...)`` then the function) makes
    ``@rmq_trigger`` decorate a plain function instead of a DAG/factory,
    which raises ``TypeError`` at decoration time with a hint about the
    correct order.
    """

    def _attach(dag_obj):
        new_subs = build_subscriptions(
            dag_id=dag_obj.dag_id,
            queue=queue,
            queues=queues,
            exchange=exchange,
            routing_keys=routing_keys,
            routing_key_ids=routing_key_ids,
            routing_key_status=routing_key_status,
            conn_id=conn_id,
            filter_data=filter_data,
            cooldown=cooldown,
        )

        if not hasattr(dag_obj, "_rmq_subscriptions"):
            dag_obj._rmq_subscriptions = []

        if has_exchange_conflict(dag_obj._rmq_subscriptions, new_subs):
            raise ValueError(
                "Multiple @rmq_trigger(exchange=...) decorators on DAG "
                f"{dag_obj.dag_id!r} are not supported — they would all resolve "
                f"to the same 'rmq_watcher.sub.{dag_obj.dag_id}' queue. Use one "
                "decorator call with the union of routing keys instead."
            )

        dag_obj._rmq_subscriptions.extend(new_subs)
        return dag_obj

    def decorator(dag_or_factory):
        if hasattr(dag_or_factory, "dag_id"):
            return _attach(dag_or_factory)

        if not callable(dag_or_factory):
            raise TypeError(
                "@rmq_trigger must decorate a DAG instance or a callable "
                f"@dag(...) factory function, got {type(dag_or_factory)!r}."
            )

        @functools.wraps(dag_or_factory)
        def wrapper(*args, **kwargs):
            result = dag_or_factory(*args, **kwargs)
            if not hasattr(result, "dag_id"):
                raise TypeError(
                    "@rmq_trigger wrapped a callable that did not produce a DAG "
                    f"instance (got {type(result)!r}). Make sure @rmq_trigger is "
                    "the outermost decorator, placed above @dag(...), not below it."
                )
            return _attach(result)

        return wrapper

    return decorator
