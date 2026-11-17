from __future__ import annotations

from datetime import timedelta, timezone

import pytest
from markupsafe import Markup

from airflow_provider_rmq.watcher.liveness import (
    LivenessState,
    ProcessState,
    assess_liveness,
    banner_text,
    masks_statuses,
)
from airflow_provider_rmq.watcher.models import (
    HEARTBEAT_ROWS_LIMIT,
    WatcherIdentity,
    get_heartbeats,
    record_heartbeat_cycle,
    record_heartbeat_stop,
)
from tests.watcher.conftest import HEARTBEAT_T0, heartbeat_row

#: Reference moment of the tests, naive UTC like the times the watcher writes.
T0 = HEARTBEAT_T0

#: URL of the Subscriptions page the banner texts link to.
LINK = "/rmq-watcher/subscriptions"


def _row(**overrides):
    """A heartbeat row of a run started at :data:`T0` that has completed no cycle and
    owes its first mark ten minutes later."""
    values = {"started_at": T0, "last_cycle_at": None, "due_at": T0 + timedelta(minutes=10)}
    values.update(overrides)
    return heartbeat_row(**values)


def _banner(verdict):
    return banner_text(verdict, LINK)


def _assess(rows, now, *, enabled=True, fresh=False):
    answer = fresh if callable(fresh) else (lambda: fresh)
    return assess_liveness(
        rows, now, has_enabled_subscriptions=enabled, has_fresh_reconcile=answer
    )


def _cycled(row_id="run-1", *, cycle=T0 + timedelta(minutes=1), due=None, **kwargs):
    kwargs.setdefault("hostname", row_id)
    return _row(
        id=row_id,
        last_cycle_at=cycle,
        due_at=due if due is not None else cycle + timedelta(minutes=10),
        **kwargs,
    )


class TestStates:
    def test_unused_without_enabled_subscriptions(self):
        verdict = _assess([], T0, enabled=False, fresh=False)
        assert verdict.state is LivenessState.UNUSED

    def test_unused_wins_over_dead_rows(self):
        dead = _cycled(due=T0)
        verdict = _assess([dead], T0 + timedelta(hours=1), enabled=False)
        assert verdict.state is LivenessState.UNUSED
        assert verdict.processes[0].state is ProcessState.DEAD

    def test_ok_with_alive_row_that_completed_a_cycle(self):
        verdict = _assess([_cycled()], T0 + timedelta(minutes=2))
        assert verdict.state is LivenessState.OK
        assert verdict.processes[0].state is ProcessState.ALIVE

    def test_starting_with_alive_rows_without_a_cycle(self):
        rows = [_row(id="a", hostname="a"), _row(id="b", hostname="b", started_at=T0 + timedelta(seconds=5))]
        verdict = _assess(rows, T0 + timedelta(minutes=1))
        assert verdict.state is LivenessState.STARTING
        assert {p.state for p in verdict.processes} == {ProcessState.STARTING}

    def test_unconfirmed_without_alive_rows_and_fresh_reconcile(self):
        verdict = _assess([], T0, fresh=True)
        assert verdict.state is LivenessState.UNCONFIRMED

    def test_down_without_alive_rows_and_stale_reconcile(self):
        verdict = _assess([], T0, fresh=False)
        assert verdict.state is LivenessState.DOWN

    def test_row_is_alive_exactly_at_due_at(self):
        row = _cycled(due=T0 + timedelta(minutes=5))
        assert _assess([row], T0 + timedelta(minutes=5)).state is LivenessState.OK
        late = _assess([row], T0 + timedelta(minutes=5, microseconds=1))
        assert late.state is LivenessState.DOWN
        assert late.processes[0].state is ProcessState.DEAD

    @pytest.mark.parametrize("due_offset", [timedelta(hours=-1), timedelta(0), timedelta(hours=1)])
    def test_stopped_row_is_never_alive(self, due_offset):
        now = T0 + timedelta(minutes=3)
        row = _cycled(stopped_at=T0 + timedelta(minutes=2), due=now + due_offset)
        verdict = _assess([row], now)
        assert verdict.state is LivenessState.DOWN
        assert verdict.processes[0].state is ProcessState.STOPPED

    def test_one_alive_row_with_a_cycle_gives_ok_among_dead_ones(self):
        now = T0 + timedelta(hours=1)
        rows = [
            _cycled("dead-1", due=T0 + timedelta(minutes=10)),
            _cycled("stopped", stopped_at=T0 + timedelta(minutes=20)),
            _cycled("alive", cycle=now - timedelta(minutes=1)),
            _row(id="starting", hostname="starting", started_at=now - timedelta(seconds=30), due_at=now + timedelta(minutes=5)),
        ]
        verdict = _assess(rows, now)
        assert verdict.state is LivenessState.OK
        states = {p.hostname: p.state for p in verdict.processes}
        assert states == {
            "dead-1": ProcessState.DEAD,
            "stopped": ProcessState.STOPPED,
            "alive": ProcessState.ALIVE,
            "starting": ProcessState.STARTING,
        }

    def test_reconcile_is_read_only_without_alive_rows(self):
        calls = []

        def fresh():
            calls.append(1)
            return True

        assert _assess([_cycled()], T0 + timedelta(minutes=2), fresh=fresh).state is LivenessState.OK
        assert _assess([_row()], T0 + timedelta(minutes=1), fresh=fresh).state is LivenessState.STARTING
        assert _assess([], T0, enabled=False, fresh=fresh).state is LivenessState.UNUSED
        assert calls == []
        assert _assess([], T0, fresh=fresh).state is LivenessState.UNCONFIRMED
        assert calls == [1]

    def test_processes_are_ordered_latest_mark_first(self):
        rows = [
            _row(id="old", hostname="old", started_at=T0),
            _cycled("newer", cycle=T0 + timedelta(minutes=3)),
            _cycled("newest", stopped_at=T0 + timedelta(minutes=4)),
        ]
        verdict = _assess(rows, T0 + timedelta(minutes=5))
        assert [p.hostname for p in verdict.processes] == ["newest", "newer", "old"]
        assert verdict.latest_mark == T0 + timedelta(minutes=4)
        assert verdict.latest_is_stop is True

    def test_last_mark_and_its_age(self):
        row = _cycled(cycle=T0 + timedelta(minutes=1))
        process = _assess([row], T0 + timedelta(minutes=3)).processes[0]
        assert process.last_mark == T0 + timedelta(minutes=1)
        assert process.last_mark_age_seconds == 120.0

    def test_age_of_a_mark_from_the_future_is_zero(self):
        row = _row(started_at=T0 + timedelta(seconds=30))
        assert _assess([row], T0).processes[0].last_mark_age_seconds == 0.0

    def test_aware_times_are_compared_as_naive_utc(self):
        aware = timezone(timedelta(hours=3))
        row = _row(
            started_at=(T0 + timedelta(hours=3)).replace(tzinfo=aware),
            last_cycle_at=(T0 + timedelta(hours=3, minutes=1)).replace(tzinfo=aware),
            due_at=(T0 + timedelta(hours=3, minutes=5)).replace(tzinfo=aware),
        )
        verdict = _assess([row], (T0 + timedelta(minutes=2)).replace(tzinfo=timezone.utc))
        assert verdict.state is LivenessState.OK
        assert verdict.processes[0].last_mark == T0 + timedelta(minutes=1)


class TestMasksStatuses:
    @pytest.mark.parametrize("state", list(LivenessState))
    def test_only_down_masks(self, state):
        rows_and_args = {
            LivenessState.UNUSED: ([], {"enabled": False}),
            LivenessState.OK: ([_cycled()], {}),
            LivenessState.STARTING: ([_row()], {}),
            LivenessState.UNCONFIRMED: ([], {"fresh": True}),
            LivenessState.DOWN: ([], {}),
        }
        rows, kwargs = rows_and_args[state]
        verdict = _assess(rows, T0 + timedelta(minutes=2), **kwargs)
        assert verdict.state is state
        assert masks_statuses(verdict) is (state is LivenessState.DOWN)


class TestBannerText:
    def test_no_banner_unless_down(self):
        assert _banner(_assess([_cycled()], T0 + timedelta(minutes=2))) is None
        assert _banner(_assess([], T0, fresh=True)) is None
        assert _banner(_assess([_row()], T0)) is None
        assert _banner(_assess([], T0, enabled=False)) is None

    def test_not_running_without_rows(self):
        text = _banner(_assess([], T0))
        assert isinstance(text, Markup)
        assert text.startswith("RMQ watcher is not running — subscriptions are not consumed")
        assert f'<a href="{LINK}">' in text

    def test_stopped_when_the_latest_row_stopped(self):
        rows = [_cycled(stopped_at=T0 + timedelta(minutes=7, seconds=5))]
        text = _banner(_assess(rows, T0 + timedelta(minutes=30)))
        assert text.startswith(
            "RMQ watcher stopped at 2026-11-17 12:07:05 UTC and no Scheduler runs it"
        )
        assert f'<a href="{LINK}">' in text

    def test_no_cycle_since_the_latest_mark(self):
        rows = [
            _cycled("old", stopped_at=T0 + timedelta(minutes=2)),
            _cycled("dead", cycle=T0 + timedelta(minutes=4), due=T0 + timedelta(minutes=9)),
        ]
        text = _banner(_assess(rows, T0 + timedelta(minutes=30)))
        assert text.startswith(
            "RMQ watcher has not completed a cycle since 2026-11-17 12:04:00 UTC"
        )

    def test_no_cycle_since_start_when_none_completed(self):
        row = _row(started_at=T0, due_at=T0 + timedelta(minutes=6))
        text = _banner(_assess([row], T0 + timedelta(minutes=7)))
        assert "has not completed a cycle since 2026-11-17 12:00:00 UTC" in text

    def test_text_names_neither_hostname_nor_pid(self):
        row = _cycled(hostname="secret-host.internal", pid=98765, due=T0 + timedelta(minutes=5))
        text = _banner(_assess([row], T0 + timedelta(hours=1)))
        assert "secret-host" not in text
        assert "98765" not in text

    def test_link_is_markup_and_substituted_link_is_escaped(self):
        text = banner_text(_assess([], T0), link='/x?a=1&b="2"')
        assert '<a href="/x?a=1&amp;b=&#34;2&#34;">RMQ Subscriptions</a>' in text


class TestScenarios:
    """Sequences of the verdict as the watcher fleet changes."""

    def test_sole_watcher_killed(self):
        # kill -9 right after a cycle: the row stays as written, no stopped_at
        cycle = T0 + timedelta(minutes=10)
        due = cycle + timedelta(minutes=7)
        row = _cycled(cycle=cycle, due=due)
        assert _assess([row], due, fresh=True).state is LivenessState.OK
        assert _assess([row], due + timedelta(seconds=1), fresh=True).state is (
            LivenessState.UNCONFIRMED
        )
        verdict = _assess([row], due + timedelta(minutes=5), fresh=False)
        assert verdict.state is LivenessState.DOWN
        assert masks_statuses(verdict)
        assert "has not completed a cycle since 2026-11-17 12:10:00 UTC" in _banner(verdict)

    def test_scheduler_stopped(self):
        # docker stop: SIGTERM while the parent is alive marks stopped_at
        cycle = T0 + timedelta(minutes=10)
        row = _cycled(cycle=cycle, stopped_at=cycle + timedelta(seconds=20))
        soon = _assess([row], cycle + timedelta(minutes=1), fresh=True)
        assert soon.state is LivenessState.UNCONFIRMED
        assert _banner(soon) is None
        later = _assess([row], cycle + timedelta(minutes=10), fresh=False)
        assert later.state is LivenessState.DOWN
        assert "stopped at 2026-11-17 12:10:20 UTC" in _banner(later)

    def test_scheduler_restarted(self):
        # the previous run died without a stop mark; the new run starts, then cycles
        old = _cycled("old", cycle=T0, due=T0 + timedelta(minutes=7))
        restart = T0 + timedelta(minutes=3)
        new = _row(id="new", hostname="new", started_at=restart, due_at=restart + timedelta(minutes=6))
        starting = _assess([old, new], restart + timedelta(minutes=5))
        assert starting.state is LivenessState.STARTING
        new_cycled = _row(
            id="new",
            hostname="new",
            started_at=restart,
            last_cycle_at=restart + timedelta(minutes=5),
            due_at=restart + timedelta(minutes=12),
        )
        running = _assess([old, new_cycled], restart + timedelta(minutes=8))
        assert running.state is LivenessState.OK
        states = {p.hostname: p.state for p in running.processes}
        assert states == {"old": ProcessState.DEAD, "new": ProcessState.ALIVE}

    def test_webserver_upgraded_before_scheduler(self):
        verdict = _assess([], T0, fresh=True)
        assert verdict.state is LivenessState.UNCONFIRMED
        assert verdict.processes == ()
        assert not masks_statuses(verdict)

    def test_no_rows_and_stale_reconcile(self):
        verdict = _assess([], T0, fresh=False)
        assert verdict.state is LivenessState.DOWN
        assert _banner(verdict).startswith("RMQ watcher is not running")


class TestRowsReadFromDatabase:
    def test_rows_from_get_heartbeats(self, heartbeat_session):
        identity = WatcherIdentity(id="db-run", hostname="host", pid=17, started_at=T0)
        cycle = T0 + timedelta(minutes=1)
        record_heartbeat_cycle(heartbeat_session, identity, cycle, cycle + timedelta(minutes=7))
        rows = get_heartbeats(heartbeat_session, T0 + timedelta(minutes=2))
        heartbeat_session.close()

        verdict = _assess(rows, T0 + timedelta(minutes=2))
        assert verdict.state is LivenessState.OK
        process = verdict.processes[0]
        assert (process.hostname, process.pid) == ("host", 17)
        assert process.last_mark == cycle
        past_due = (cycle + timedelta(minutes=8)).replace(tzinfo=timezone.utc)
        assert _assess(rows, past_due).state is LivenessState.DOWN

    def test_an_alive_run_counts_behind_many_newer_stopped_runs(self, heartbeat_session):
        now = T0 + timedelta(minutes=30)
        due = now + timedelta(minutes=5)
        alive = WatcherIdentity(id="alive", hostname="host", pid=1, started_at=T0)
        record_heartbeat_cycle(heartbeat_session, alive, T0 + timedelta(minutes=1), due)
        for index in range(HEARTBEAT_ROWS_LIMIT + 5):
            stopped = WatcherIdentity(
                id=f"stopped-{index}", hostname="host", pid=100 + index,
                started_at=T0 + timedelta(minutes=2),
            )
            record_heartbeat_stop(
                heartbeat_session, stopped, due, now - timedelta(seconds=index + 1),
            )
        rows = get_heartbeats(heartbeat_session, now)
        heartbeat_session.close()

        verdict = _assess(rows, now, fresh=False)

        assert verdict.state is LivenessState.OK
        alive_rows = [p for p in verdict.processes if p.state is ProcessState.ALIVE]
        assert [p.pid for p in alive_rows] == [1]
