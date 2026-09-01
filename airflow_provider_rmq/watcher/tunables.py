"""Names, defaults and timing rules of the Airflow Variables that tune the watcher.

The reconcile interval and the cycle budget are read by two processes that share
nothing else: the scheduler runs the loop, and the webserver renders the Subscriptions
page and needs the same numbers to tell a fresh status row from a late one. The names,
their defaults and the arithmetic over them therefore have one home that both read, and
the view does not reach into the listener for them. The yes-or-no switch of the page's
access is read by the webserver alone and lives here for the same reason: the name of a
watcher Variable and the rules for reading it belong together.
"""
from __future__ import annotations

import logging
import math
from collections.abc import Callable
from typing import Any

log = logging.getLogger(__name__)

#: Seconds between reconcile cycles when the Variable below is unset.
DEFAULT_RECONCILE_INTERVAL = 60

#: Airflow Variables holding the watcher tunables.
RECONCILE_INTERVAL_VAR = "rmq_watcher_reconcile_interval"
CYCLE_TIMEOUT_VAR = "rmq_watcher_cycle_timeout"
GRANT_OP_ACCESS_VAR = "rmq_watcher_grant_op_access"

#: Spellings :func:`read_flag` accepts, compared case-insensitively.
_TRUE_WORDS = frozenset({"1", "true", "yes", "on"})
_FALSE_WORDS = frozenset({"0", "false", "no", "off"})

#: A cycle may take this many reconcile intervals, but never less than
#: ``MIN_CYCLE_TIMEOUT`` seconds. The budget is generous on purpose: hitting it
#: cancels every consumer task and pauses consumption on every conn_id for the
#: 30s loop-restart delay, while the per-call AMQP timeouts catch a stuck network
#: operation far earlier and only for the subscription that owns it.
CYCLE_TIMEOUT_FACTOR = 3
MIN_CYCLE_TIMEOUT = 300


def cycle_timeout(interval: float, override: float | None = None) -> float:
    """Seconds one cycle may take before the loop is considered stuck.

    :param interval: Seconds between cycles.
    :param override: The value of :data:`CYCLE_TIMEOUT_VAR`, when it holds a usable one.
    """
    if override is not None:
        return float(override)
    return float(max(interval * CYCLE_TIMEOUT_FACTOR, MIN_CYCLE_TIMEOUT))


def stale_after(interval: float, budget: float) -> float:
    """Seconds past which a ``last_reconcile_at`` stamp says the loop is late.

    The stamp is written once per cycle, at its end, and the loop then waits
    ``interval`` before starting the next one, so in a healthy watcher the age of the
    stamp reaches one interval plus the length of a cycle. A cycle is allowed to run for
    ``budget`` seconds, which makes ``interval + budget`` the oldest stamp a watcher that
    is doing exactly what it is configured to do can produce.

    :param budget: Seconds one cycle may take — :func:`cycle_timeout`.
    """
    return interval + budget


def read_positive(name: str, cast: Callable[[str], Any]) -> Any:
    """Read Airflow Variable ``name`` as a positive number.

    :param cast: What to read the raw string as — ``int`` for the interval, ``float``
        for the budget.
    :returns: The value, or ``None`` when there is no usable one: the Variable is unset,
        holds something that is not a number, holds one that is not finite, or holds a
        number at or below zero. Both readers take ``None`` as "keep the built-in
        default", and every unusable case is logged as it is found.

    A database that cannot answer reads as an unset Variable: the value comes from
    :meth:`Variable.get`, which swallows a secrets backend's failure and hands back
    ``None``, so both readers take the built-in default. Only a read that hangs is told
    apart — the caller that runs this one under a timeout of its own keeps the values it
    already has.

    Blocking — it queries the metadata database.
    """
    from airflow.models import Variable

    raw = Variable.get(name, default_var=None)
    if raw is None:
        return None
    try:
        value = cast(raw)
    except (TypeError, ValueError):
        log.warning("RMQ Watcher: Variable %s=%r is not a number — ignoring", name, raw)
        return None
    if not math.isfinite(value):
        # ``float`` reads "inf" and "nan" happily, and both pass every comparison below:
        # an infinite cycle budget is a watchdog that never fires, and a NaN one compares
        # False against everything, which leaves the timers in no order at all.
        log.warning("RMQ Watcher: Variable %s=%r is not a finite number — ignoring", name, raw)
        return None
    if value <= 0:
        log.warning("RMQ Watcher: Variable %s=%r must be positive — ignoring", name, raw)
        return None
    return value


def read_flag(name: str, default: bool) -> bool:
    """Read Airflow Variable ``name`` as a yes-or-no answer.

    ``1``, ``true``, ``yes`` and ``on`` read as true; ``0``, ``false``, ``no`` and
    ``off`` read as false. Case and surrounding whitespace do not matter.

    :param default: What an unset Variable and a value spelled in none of the words
        above read as. The latter is logged.
    :raises: The first exception a secrets backend raised, and only when no backend
        answered at all. The backends are asked in Airflow's own order — one that failed
        is logged and the next is asked, so a value the metadata database holds is
        reached across an outage of the custom backend in front of it — but a read every
        backend refused raises rather than returning ``default``, which is where this
        parting from :meth:`Variable.get` lies: that one hands back the very ``None`` an
        unset Variable gives, so a caller acting on the answer would take an unreachable
        database for an administrator who set nothing and let the default decide in his
        place. Raising leaves that decision with the caller, which knows what it is
        about to do.

    Blocking — it queries the secrets backends, the metadata database among them.
    """
    from airflow.configuration import ensure_secrets_loaded
    from airflow.utils.log.secrets_masker import mask_secret

    raw = None
    failure: Exception | None = None
    for backend in ensure_secrets_loaded():
        try:
            raw = backend.get_variable(key=name)
        except Exception as exc:  # noqa: BLE001 - carried, and raised when no one answers
            if failure is None:
                failure = exc
            log.warning(
                "RMQ Watcher: secrets backend %s could not be asked for Variable %s: "
                "%s — asking the next backend",
                type(backend).__name__,
                name,
                exc,
            )
            continue
        if raw is not None:
            break
    if raw is None:
        if failure is not None:
            raise failure
        return default
    # The value is put through the masker the way ``Variable.get`` puts it, so a name
    # Airflow counts as sensitive is redacted in the warning below as it is everywhere
    # else.
    mask_secret(raw, name)
    word = raw.strip().lower()
    if word in _TRUE_WORDS:
        return True
    if word in _FALSE_WORDS:
        return False
    log.warning(
        "RMQ Watcher: Variable %s=%r is not a yes-or-no value — reading it as %s",
        name,
        raw,
        default,
    )
    return default
