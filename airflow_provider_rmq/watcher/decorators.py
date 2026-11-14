from __future__ import annotations

import functools
import sys
from typing import Any

from airflow_provider_rmq.watcher.dag_file_scan import (
    RMQTriggerNotStaticError,
    _verify_static_readability,
)
from airflow_provider_rmq.watcher.subscription_builder import (
    build_subscriptions,
    has_exchange_conflict,
)

__all__ = ["RMQTriggerNotStaticError", "rmq_trigger"]


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
    ack_unmatched: bool = False,
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
        ``cooldown``, and ``ack_unmatched``.  Mutually exclusive with
        ``queue``/``exchange``.
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
        not supported** — they would all resolve to the same
        ``rmq_watcher.sub.{dag_id}`` queue; use one decorator call with the
        union of routing keys instead. The watcher refuses such a file, and
        the decorator raises ``RMQTriggerNotStaticError`` (a ``ValueError``)
        naming the second call when the ``@dag(...)`` factory is called (see
        "Decorator order and DAG types" below) — during DAG file import,
        before the DAG is registered.
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
    :param ack_unmatched: What to do with a message the subscription's filter
        rejected.  ``False`` (default) returns it to the queue, so another
        consumer can take it and this one keeps seeing it again.  ``True``
        acknowledges it instead: the message leaves the broker for good — it is
        not dead-lettered, DLX does not fire on an acknowledgement.  Use it on
        a queue that belongs to this subscription alone; on a queue shared with
        another consumer it deletes messages meant for that consumer.

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
      lowest ``conn_id`` among the cooldown subscriptions.  Mixing conn_ids
      with cooldown is not supported: the watcher logs it as an ERROR, and
      deliveries on the other conn_ids never fire their DAG.
    * **No consumer is ever attached to ``rmq_watcher.pending.{dag_id}``.**
      The queue acts as a pure timer — its only purpose is to expire the
      message and dead-letter it to ``rmq_watcher.fire``.
    * **Orphaned pending queues are not deleted automatically** when a
      cooldown subscription is removed.  A WARNING is logged on the first
      reconcile after removal.  Manual cleanup command::

          rabbitmqadmin delete queue name=rmq_watcher.pending.<dag_id>

    ``group_key`` is NOT set by the decorator; the watcher's DAG-file scan
    (``dag_file_scan._read_function_triggers``) sets it where the dag_id is
    known (``group_key = dag_id`` when ``cooldown > 0``, else ``None``).

    Best practice: use a dedicated queue per DAG trigger (e.g.
    ``orders.airflow-trigger`` separate from ``orders``) to avoid interference
    with other consumers on the same queue.

    Decorator order and DAG types
    ------------------------------
    The watcher finds subscriptions by reading DAG files without executing
    them, and it recognizes one form only: ``@rmq_trigger(...)`` as a
    decorator call above ``@dag(...)`` on a function of a file it scans::

          @rmq_trigger(queue="orders")
          @dag(dag_id="my_dag", schedule=None)
          def my_dag():
              ...

          my_dag()

    ``airflow.decorators.dag(...)`` returns a plain factory function, not a
    ``DAG`` — the real ``DAG`` only exists after the factory is called
    (``my_dag()`` at the end of the file). ``@rmq_trigger`` wraps the factory
    and attaches subscriptions when the wrapper itself is called, then returns
    the resulting ``DAG``. Applied to a ready ``DAG`` instance
    (``rmq_trigger(...)(dag)``, the ``with DAG(...) as dag:`` style), the
    decorator raises ``TypeError`` at once: the watcher does not see such a
    call.

    **``@rmq_trigger`` must be the outermost decorator, placed above
    ``@dag(...)``, never below it.** Putting it below (i.e.
    ``@dag(...)`` then ``@rmq_trigger(...)`` then the function) makes
    ``@rmq_trigger`` wrap the plain function instead of the factory; calling
    the factory then raises ``TypeError`` with a hint about the correct order.

    **``dag_id`` and every argument must be statically readable.** The
    watcher's scan reads ``dag_id`` of ``@dag(...)`` and the arguments of
    ``@rmq_trigger(...)``, positional or keyword, from the file's source:
    literals; constants assigned a literal exactly once at the top level of the
    file, above a module-level decorated function; list, tuple and dict
    displays built from them; ``**OPTS`` of a dict constant. A list or dict
    constant counts only when the file mentions it nowhere but in
    ``rmq_trigger`` arguments. Imported names, calls, attributes, f-strings
    and concatenation are not read. When the DAG is built, the decorator runs
    the watcher's own reading of the file against its call site and raises
    ``RMQTriggerNotStaticError`` (importable from
    ``airflow_provider_rmq.watcher``), a ``ValueError`` that fails the import
    of the whole file, when:

    * the file is not one the watcher scans — a ``*.py`` file inside
      ``DAGS_FOLDER`` that ``.airflowignore`` does not exclude, the files
      Airflow loads; this is the file of the ``rmq_trigger(...)`` call, so a
      factory module under an ignored directory fails the DAG file that
      imports it;
    * any ``@rmq_trigger`` of the file cannot be read, ``rmq_trigger`` is
      mentioned other than as a decorator call on a function, is not imported
      from ``airflow_provider_rmq.watcher.decorators`` (a same-named wrapper
      of another module, a relative import, ``from ... import *``), its
      ``@dag(...)`` is not Airflow's decorator imported from
      ``airflow.decorators`` or ``airflow.models.dag`` (a same-named ``dag`` of
      another module is refused even when it keeps the ``dag_id``), a
      decorator other than ``rmq_trigger(...)`` and ``dag(...)`` sits below
      the first ``@rmq_trigger(...)`` of a function, or a ``dag_id`` cannot be
      resolved;
    * the scan does not find this call, resolves another ``dag_id``, or reads
      other values than the decorator received.

    The watcher, for its part, refuses every subscription of a file whose
    ``@rmq_trigger`` calls it cannot all read, and logs an ERROR, so no DAG
    runs on subscriptions other than the ones its file declares.
    ``DagBag(dag_folder=...)`` in tests and CI needs ``DAGS_FOLDER`` pointing
    at the same folder, or every DAG with ``@rmq_trigger`` fails to import.
    """
    # The call site of rmq_trigger(...): the file and line the watcher's scan has
    # to find this call at, checked once the decorated DAG exists.
    caller = sys._getframe(1)
    call_file, call_line = caller.f_code.co_filename, caller.f_lineno
    del caller

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
            ack_unmatched=ack_unmatched,
        )

        # DAGS_FOLDER of the Airflow process importing the DAG file, read when
        # the DAG is built.
        from airflow import settings

        _verify_static_readability(
            call_file, call_line, dag_obj.dag_id, new_subs, settings.DAGS_FOLDER
        )

        if not hasattr(dag_obj, "_rmq_subscriptions"):
            dag_obj._rmq_subscriptions = []

        # Unreachable while the static check above runs: it refuses a file with
        # a second exchange= subscription on one DAG. This check keeps that
        # invariant in the DAG's own subscriptions for a call the static check
        # lets through, such as one made with the check replaced in tests.
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
            raise TypeError(
                f"rmq_trigger(...) was applied to the DAG {dag_or_factory.dag_id!r} "
                "itself. The RMQ watcher reads subscriptions only from the "
                "decorator form @rmq_trigger(...) placed above @dag(...) on a "
                "function of a DAG file, so it would never register this one; "
                "declare the DAG with @dag(...) and put @rmq_trigger(...) above it."
            )

        if not callable(dag_or_factory):
            raise TypeError(
                "@rmq_trigger must decorate a callable @dag(...) factory function, "
                f"got {type(dag_or_factory)!r}."
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
