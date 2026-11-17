"""Whether any watcher process confirms that subscriptions are being consumed.

The verdict is read from the heartbeat rows (:class:`~.models.RMQWatcherHeartbeat`),
one per run of a watcher process. A row is alive while it carries no ``stopped_at`` and
its ``due_at`` has not passed: the run owes its next mark by ``due_at``, so a row past
it belongs to a run that is not working. An alive row with a completed cycle confirms
the work. With no alive row, a fresh ``last_reconcile_at`` stamp still shows work going
on — a watcher of a provider release that writes no heartbeat, or the last watcher
having stopped moments ago — and only when that stamp is stale as well does nothing
confirm the work.

The functions here are pure: the caller reads the rows and the database facts, and
gets back a verdict that holds plain values, safe to cache and to render after the
session that read them is gone. The question answered is whether *any* watcher
works, not whether every scheduler has one: a dead watcher of one replica while
others consume is visible in the verdict's process list and does not make it ``down``.
"""
from __future__ import annotations

import enum
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from markupsafe import Markup

from airflow_provider_rmq.watcher.models import naive_utc

#: Path of the Subscriptions page, which every banner text links to for the details.
SUBSCRIPTIONS_PATH = "/rmq-watcher/subscriptions"


class LivenessState(enum.Enum):
    """Verdict on whether subscriptions are being consumed.

    ``UNUSED`` — no enabled subscriptions, nothing to consume. ``OK`` — an alive run
    has completed a cycle. ``STARTING`` — runs are alive, none has completed a cycle
    yet. ``UNCONFIRMED`` — no alive run, but a fresh ``last_reconcile_at`` stamp.
    ``DOWN`` — no alive run and no fresh stamp: nothing confirms the work.
    """

    UNUSED = "unused"
    OK = "ok"
    STARTING = "starting"
    UNCONFIRMED = "unconfirmed"
    DOWN = "down"


class ProcessState(enum.Enum):
    """State of one watcher run, as the Subscriptions page lists it.

    ``ALIVE`` — within its deadline with a completed cycle. ``STARTING`` — within its
    deadline, no cycle completed yet. ``DEAD`` — past its deadline without an orderly
    stop. ``STOPPED`` — stopped in an orderly way.
    """

    ALIVE = "alive"
    STARTING = "starting"
    DEAD = "dead"
    STOPPED = "stopped"


@dataclass(frozen=True)
class ProcessRow:
    """One watcher run as assessed, detached from the session that read it.

    Times are naive UTC. ``last_mark`` is ``stopped_at``, else ``last_cycle_at``, else
    ``started_at``; ``last_mark_age_seconds`` is its age at the moment of assessment,
    never negative.
    """

    hostname: str
    pid: int
    started_at: datetime
    due_at: datetime
    stopped_at: datetime | None
    last_mark: datetime
    last_mark_age_seconds: float
    state: ProcessState


@dataclass(frozen=True)
class LivenessVerdict:
    """Outcome of :func:`assess_liveness`.

    ``processes`` lists every assessed run, latest mark first. ``latest_mark`` is the
    latest mark among them (``None`` with no rows), and ``latest_is_stop`` tells
    whether that mark is an orderly stop.
    """

    state: LivenessState
    processes: tuple[ProcessRow, ...]
    latest_mark: datetime | None
    latest_is_stop: bool


def _optional_naive_utc(value: datetime | None) -> datetime | None:
    return None if value is None else naive_utc(value)


def _assess_row(row: Any, now: datetime) -> ProcessRow:
    started_at = naive_utc(row.started_at)
    last_cycle_at = _optional_naive_utc(row.last_cycle_at)
    due_at = naive_utc(row.due_at)
    stopped_at = _optional_naive_utc(row.stopped_at)
    last_mark = stopped_at or last_cycle_at or started_at
    if stopped_at is not None:
        state = ProcessState.STOPPED
    elif now > due_at:
        state = ProcessState.DEAD
    elif last_cycle_at is not None:
        state = ProcessState.ALIVE
    else:
        state = ProcessState.STARTING
    return ProcessRow(
        hostname=row.hostname,
        pid=row.pid,
        started_at=started_at,
        due_at=due_at,
        stopped_at=stopped_at,
        last_mark=last_mark,
        last_mark_age_seconds=max((now - last_mark).total_seconds(), 0.0),
        state=state,
    )


def assess_liveness(
    heartbeats: Iterable[Any],
    now: datetime,
    has_enabled_subscriptions: bool,
    has_fresh_reconcile: Callable[[], bool],
) -> LivenessVerdict:
    """Decide whether any watcher run confirms that subscriptions are consumed.

    The states are checked in order and the first that matches is the verdict:
    ``UNUSED`` without enabled subscriptions, ``OK`` with an alive run that has
    completed a cycle, ``STARTING`` with alive runs none of which has, ``UNCONFIRMED``
    with no alive run but a fresh ``last_reconcile_at``, ``DOWN`` otherwise. A run is
    alive while it has no ``stopped_at`` and ``now`` is at or before its ``due_at``.

    :param heartbeats: Heartbeat rows — :class:`~.models.RMQWatcherHeartbeat` or any
        object with the same attributes.
    :param now: The moment of assessment; naive UTC, an aware value is converted.
    :param has_enabled_subscriptions: Whether any subscription is enabled.
    :param has_fresh_reconcile: Answers whether the latest ``last_reconcile_at`` is
        fresh. It is called only when no run is alive, so the stamp is read only when
        the verdict depends on it.
    """
    now = naive_utc(now)
    processes = tuple(
        sorted(
            (_assess_row(row, now) for row in heartbeats),
            key=lambda process: process.last_mark,
            reverse=True,
        )
    )
    latest = processes[0] if processes else None
    alive = [p for p in processes if p.state in (ProcessState.ALIVE, ProcessState.STARTING)]

    if not has_enabled_subscriptions:
        state = LivenessState.UNUSED
    elif any(p.state is ProcessState.ALIVE for p in alive):
        state = LivenessState.OK
    elif alive:
        state = LivenessState.STARTING
    else:
        fresh = has_fresh_reconcile()
        state = LivenessState.UNCONFIRMED if fresh else LivenessState.DOWN

    return LivenessVerdict(
        state=state,
        processes=processes,
        latest_mark=latest.last_mark if latest else None,
        latest_is_stop=latest is not None and latest.stopped_at is not None,
    )


def masks_statuses(verdict: LivenessVerdict) -> bool:
    """Whether the recorded subscription statuses are shown as unknown.

    Only under ``DOWN``: nothing confirms that a watcher keeps them current.
    """
    return verdict.state is LivenessState.DOWN


_NOT_RUNNING = (
    "RMQ watcher is not running — subscriptions are not consumed and "
    "event-triggered DAGs do not run."
)
_STOPPED = (
    "RMQ watcher stopped at {time} and no Scheduler runs it — "
    "subscriptions are not consumed."
)
_NO_CYCLE = (
    "RMQ watcher has not completed a cycle since {time} — subscriptions are not "
    "consumed and event-triggered DAGs do not run."
)
_DETAILS = ' See <a href="{link}">RMQ Subscriptions</a> for details.'


def _format_time(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S UTC")


def banner_text(verdict: LivenessVerdict, link: str = SUBSCRIPTIONS_PATH) -> Markup | None:
    """The banner for a ``DOWN`` verdict, or ``None`` for any other.

    The text names neither hostname nor PID — every user of the UI sees the banner —
    and links to the Subscriptions page, where the runs are listed. The template is
    markup and the substituted values are escaped.

    :param link: URL of the Subscriptions page; the web UI passes the one it resolves
        under its own base URL.
    """
    if verdict.state is not LivenessState.DOWN:
        return None
    if verdict.latest_mark is None:
        template = _NOT_RUNNING
    elif verdict.latest_is_stop:
        template = _STOPPED
    else:
        template = _NO_CYCLE
    time = _format_time(verdict.latest_mark) if verdict.latest_mark else ""
    return Markup(template + _DETAILS).format(time=time, link=link)
