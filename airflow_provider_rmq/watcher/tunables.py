"""Names and defaults of the Airflow Variables that tune the watcher.

The reconcile interval is read by two processes that share nothing else: the
scheduler runs the loop, and the webserver renders the Subscriptions page and needs
the same number to tell a fresh status row from a stale one. Keeping the names here
lets the view read them without importing the listener, whose module chain pulls
``aio_pika`` and ``httpx`` into a process that never opens a connection.
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


def read_positive(name: str, cast: Callable[[str], Any]) -> Any:
    """Read Airflow Variable ``name`` as a positive number.

    :param cast: What to read the raw string as — ``int`` for the interval, ``float``
        for the budget.
    :returns: The value, or ``None`` when there is no usable one: the Variable is unset,
        holds something that is not a number, holds one that is not finite, or holds a
        number at or below zero. Both readers take ``None`` as "keep the built-in
        default", and every unusable case is logged as it is found.

    A database that cannot answer raises: the loop keeps the values it already has, the
    view falls back to the default, and each says so in its own terms.

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
