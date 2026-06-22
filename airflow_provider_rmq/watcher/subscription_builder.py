"""Shared subscription dict construction for ``@rmq_trigger``.

Both the decorator (``decorators.py``) and the AST parser (``listener.py``)
need to validate the same ``queue``/``queues``/``exchange`` arguments and
build the same shape of subscription dict, from two different extraction
paths (real Python values vs. ``ast.literal_eval`` results). This module is
the single place where those rules live — see
``docs/plans/20260619-exchange-trigger-subscriptions.md`` Technical Details
→ "Валидация декоратора" for the full rationale.
"""

from __future__ import annotations

from typing import Any

_RESERVED_EXCHANGE_PREFIX = "rmq_watcher."
_SUB_QUEUE_PREFIX = "rmq_watcher.sub."


def _normalize_status(routing_key_status: str | list[str]) -> list[str]:
    if isinstance(routing_key_status, str):
        return [routing_key_status]
    if not isinstance(routing_key_status, list):
        raise ValueError("'routing_key_status' must be a string or a list of strings.")
    return list(routing_key_status)


def build_subscriptions(
    *,
    dag_id: str,
    queue: str | None = None,
    queues: list[str] | None = None,
    exchange: str | None = None,
    routing_keys: list[str] | None = None,
    routing_key_ids: list[str] | None = None,
    routing_key_status: str | list[str] = "*",
    conn_id: str = "rmq_default",
    filter_data: dict[str, Any] | None = None,
    cooldown: int = 0,
) -> list[dict]:
    """Validate ``@rmq_trigger`` arguments and build subscription dicts.

    Exactly one of ``queue``, ``queues``, ``exchange`` must be specified.

    For ``exchange=``, at least one of ``routing_keys``/``routing_key_ids``
    must be given. The final routing key set is the union of the literal
    ``routing_keys`` and ``[f"{id_}.{status}" for id_ in routing_key_ids for
    status in statuses]`` (see ADR-0004). ``routing_key_ids``/
    ``routing_key_status`` segments must not contain ``.`` — a dot would
    silently turn a two-segment routing key into a three-segment one and
    change the meaning of the default ``*`` wildcard. Literal ``routing_keys``
    entries are not subject to that check (dots there are an intentionally
    written topic pattern) but must still be non-empty strings.

    Returns a list of dicts with keys ``{"queue_name", "conn_id",
    "filter_data", "cooldown"}`` (plus ``{"exchange", "routing_keys"}`` for
    exchange-mode) — one entry per queue in ``queues``, or a single entry for
    ``queue=``/``exchange=``.
    """
    modes_given = sum(x is not None for x in (queue, queues, exchange))
    if modes_given != 1:
        raise ValueError("Specify exactly one of 'queue', 'queues', or 'exchange'.")

    if not isinstance(cooldown, int) or isinstance(cooldown, bool):
        raise ValueError("'cooldown' must be an int.")
    if cooldown < 0:
        raise ValueError("'cooldown' must be >= 0.")

    if queue is not None and not isinstance(queue, str):
        raise ValueError("'queue' must be a string.")
    if queues is not None:
        if not isinstance(queues, list):
            raise ValueError("'queues' must be a list of strings.")
        if len(queues) == 0:
            raise ValueError("'queues' must not be an empty list.")
        for q in queues:
            if not isinstance(q, str) or q == "":
                raise ValueError("Each entry in 'queues' must be a non-empty string.")

    filter_data = filter_data or {}

    if exchange is not None:
        if not isinstance(exchange, str):
            raise ValueError("'exchange' must be a string.")
        if exchange == "":
            raise ValueError("'exchange' must be a non-empty string.")
        if exchange.startswith(_RESERVED_EXCHANGE_PREFIX):
            raise ValueError(
                f"'exchange' must not start with {_RESERVED_EXCHANGE_PREFIX!r} — "
                "that namespace is reserved for rmq_watcher's own cooldown/fire "
                "infrastructure (rmq_watcher.fire / rmq_watcher.pending.* / "
                "rmq_watcher.sub.*)."
            )

        if not routing_keys and not routing_key_ids:
            raise ValueError(
                "When 'exchange' is specified, at least one of 'routing_keys' "
                "or 'routing_key_ids' must be given."
            )

        literal_keys: list[str] = []
        if routing_keys is not None:
            if not isinstance(routing_keys, list):
                raise ValueError("'routing_keys' must be a list of strings.")
            if len(routing_keys) == 0:
                raise ValueError("'routing_keys' must not be an empty list.")
            for key in routing_keys:
                if not isinstance(key, str) or key == "":
                    raise ValueError("Each entry in 'routing_keys' must be a non-empty string.")
            literal_keys = list(routing_keys)

        from_ids: list[str] = []
        if routing_key_ids is not None:
            if not isinstance(routing_key_ids, list):
                raise ValueError("'routing_key_ids' must be a list of strings.")
            if len(routing_key_ids) == 0:
                raise ValueError("'routing_key_ids' must not be an empty list.")
            statuses = _normalize_status(routing_key_status)
            for id_ in routing_key_ids:
                if not isinstance(id_, str) or id_ == "":
                    raise ValueError("Each entry in 'routing_key_ids' must be a non-empty string.")
                if "." in id_:
                    raise ValueError(
                        f"'routing_key_ids' entry {id_!r} must not contain '.' — "
                        "a dot would change the segment count of the resulting "
                        "routing key."
                    )
            for status in statuses:
                if not isinstance(status, str) or status == "":
                    raise ValueError("Each entry in 'routing_key_status' must be a non-empty string.")
                if "." in status:
                    raise ValueError(
                        f"'routing_key_status' entry {status!r} must not contain '.' — "
                        "a dot would change the segment count of the resulting "
                        "routing key."
                    )
            from_ids = [f"{id_}.{status}" for id_ in routing_key_ids for status in statuses]

        # Union, preserving first-seen order, de-duplicated.
        final_keys = list(dict.fromkeys(literal_keys + from_ids))

        if not final_keys:
            raise ValueError(
                "Resulting routing key set is empty after expansion — check "
                "'routing_key_status' is not an empty list."
            )

        return [
            {
                "queue_name": f"{_SUB_QUEUE_PREFIX}{dag_id}",
                "conn_id": conn_id,
                "filter_data": filter_data,
                "cooldown": cooldown,
                "exchange": exchange,
                "routing_keys": final_keys,
            }
        ]

    if queues is not None:
        return [
            {
                "queue_name": q,
                "conn_id": conn_id,
                "filter_data": filter_data,
                "cooldown": cooldown,
            }
            for q in queues
        ]

    return [
        {
            "queue_name": queue,
            "conn_id": conn_id,
            "filter_data": filter_data,
            "cooldown": cooldown,
        }
    ]


def has_exchange_conflict(existing: list[dict], new: list[dict]) -> bool:
    """Return True if both ``existing`` and ``new`` contain an exchange-mode entry.

    Stacking multiple ``@rmq_trigger(exchange=...)`` calls on the same DAG is
    not supported — they would all resolve to the same
    ``rmq_watcher.sub.{dag_id}`` queue, and merging exchange metadata in
    memory would silently drop routing keys from all but the last-parsed
    decorator. Pure function over lists of dicts, no ``dag``/AST dependency.
    """
    existing_has_exchange = any("exchange" in sub for sub in existing)
    new_has_exchange = any("exchange" in sub for sub in new)
    return existing_has_exchange and new_has_exchange
