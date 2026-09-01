"""Names, defaults and timing rules of the settings that tune the watcher.

The reconcile interval and the cycle budget are read by two processes that share
nothing else: the scheduler runs the loop, and the webserver renders the Subscriptions
page and needs the same numbers to tell a fresh status row from a late one. The names,
their defaults and the arithmetic over them therefore have one home that both read, and
the view does not reach into the listener for them. The yes-or-no switch of the page's
access is read by the webserver alone and lives here for the same reason: the name of a
watcher setting and the rules for reading it belong together.

That switch is the one setting here that is not an Airflow Variable. It governs a
permission of the Op role, and Airflow gives that role create, edit and delete on
Variables, so a switch kept in the metadata database is one the restricted role can
turn back on for itself. Airflow configuration is written in ``airflow.cfg`` or in the
environment of the webserver process, which no role reaches through the web UI, and
that is the property the switch needs.
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

#: The Airflow configuration option governing the Subscriptions page access of the Op
#: role — ``[rmq_watcher] grant_op_access`` in ``airflow.cfg``, or the environment
#: variable ``AIRFLOW__RMQ_WATCHER__GRANT_OP_ACCESS``.
GRANT_OP_ACCESS_SECTION = "rmq_watcher"
GRANT_OP_ACCESS_OPTION = "grant_op_access"

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


def read_flag(section: str, option: str, default: bool) -> bool:
    """Read Airflow configuration option ``[section] option`` as a yes-or-no answer.

    ``1``, ``t`` and ``true`` read as true; ``0``, ``f`` and ``false`` read as false.
    Case, surrounding whitespace and a trailing ``#`` comment do not matter. These are
    Airflow's own spellings for every boolean option it has, and the option is read
    through Airflow's own parser so that it answers to exactly them.

    :param default: What an option nobody set reads as.
    :raises AirflowConfigException: The option holds a value in none of the spellings
        above. A switch whose value cannot be read is not an instruction, and the caller
        is told so rather than handed ``default`` in its place, because it is the caller
        that knows what the answer decides.

    The value comes from the environment and from ``airflow.cfg``, and from nowhere
    else: the ``_cmd`` and ``_secret`` indirections that would reach a shell command or
    a secrets backend are honoured only for the options Airflow lists as sensitive, and
    the watcher's is not one of them. The read therefore touches no network and no
    database, which is what makes it safe to run while the webserver is still building
    its application — there is nothing here that can hang.
    """
    from airflow.configuration import conf

    return conf.getboolean(section, option, fallback=default)
