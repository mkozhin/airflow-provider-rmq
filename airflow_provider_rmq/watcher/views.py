from __future__ import annotations

import logging
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from flask import flash, redirect, request, url_for
from flask_appbuilder import BaseView, expose
from flask_appbuilder.security.decorators import has_access

from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    WatcherSession,
    get_active_dag_ids,
    get_conn_statuses,
    upsert_subscription,
)
from airflow_provider_rmq.watcher.subscription_form import parse_cooldown, parse_filter_data
from airflow_provider_rmq.watcher.tunables import (
    CYCLE_TIMEOUT_VAR,
    DEFAULT_RECONCILE_INTERVAL,
    RECONCILE_INTERVAL_VAR,
    cycle_timeout,
    read_positive,
    stale_after,
)

log = logging.getLogger(__name__)


#: Shown instead of the Subscriptions table when the subscription rows cannot be read.
#: The read fails for one reason far more often than any other — a table that does not
#: yet carry this version's columns — but a database that is down or a role without the
#: rights fail the same way, so the notice names the likely cause and points at the log
#: for the actual one instead of stating a diagnosis it does not have.
SUBSCRIPTIONS_SCHEMA_OUTDATED_MESSAGE = (
    "Subscriptions are unavailable: the rmq_watcher_subscriptions table could "
    "not be read. Most often this means it does not yet match this version of "
    "the provider, and the watcher loop in the scheduler migrates it on one of "
    "its next cycles; the webserver log carries the actual error."
)


# ------------------------------------------------------------------
# Group helpers
# ------------------------------------------------------------------

@dataclass
class SubscriptionGroup:
    """Logical group of subscriptions sharing the same group_key."""
    dag_id: str
    conn_id: str
    source: str
    cooldown: int
    group_key: str
    queue_names: list[str] = field(default_factory=list)
    sub_ids: list[int] = field(default_factory=list)
    enabled: bool = True
    #: True when at least one member acknowledges what its filter rejected. Aggregated
    #: by OR, unlike `enabled`: the group's row has to name the setting as soon as a
    #: single queue of it deletes messages
    ack_unmatched: bool = False
    #: Queues of this group another enabled subscription of the same conn_id also
    #: listens to. The badge names them, so the list is built here rather than in the
    #: template
    shared_queue_names: list[str] = field(default_factory=list)
    #: True when a subscription on one of those shared queues acknowledges its misses
    shared_acking: bool = False
    statuses: list[str] = field(default_factory=list)
    last_error: str | None = None
    is_group: bool = field(default=True, init=False)

    @property
    def listening_count(self) -> int:
        return sum(1 for s in self.statuses if s == "listening")

    @property
    def total_count(self) -> int:
        return len(self.statuses)

    @property
    def display_status(self) -> str:
        return f"{self.listening_count}/{self.total_count} listening"


def _group_subscriptions(subs: list[RMQSubscription]) -> list[Any]:
    """Group subscriptions by group_key where applicable.

    Returns a mixed list of SubscriptionGroup (for grouped) and
    RMQSubscription (for ungrouped, i.e. group_key is NULL). Every item carries
    ``shared_queue_names`` and ``shared_acking``, so the template asks a row whether
    it is on a shared queue instead of computing the answer itself.
    """
    shared, acking = _queue_sharing(subs)
    grouped: dict[str, SubscriptionGroup] = {}
    ungrouped: list[RMQSubscription] = []

    for sub in subs:
        pair = (sub.queue_name, sub.conn_id)
        # A disabled row runs no consumer, so it competes for nothing — the badge
        # belongs to the rows that are actually on the queue.
        is_shared = bool(sub.enabled) and pair in shared
        if sub.group_key:
            key = sub.group_key
            if key not in grouped:
                grouped[key] = SubscriptionGroup(
                    dag_id=sub.dag_id,
                    conn_id=sub.conn_id,
                    source=sub.source,
                    cooldown=sub.cooldown or 0,
                    group_key=sub.group_key,
                )
            g = grouped[key]
            g.queue_names.append(sub.queue_name)
            g.sub_ids.append(sub.id)
            g.statuses.append(sub.consumer_status)
            # Propagate: group is disabled if any sub is disabled
            if not sub.enabled:
                g.enabled = False
            # Propagate: group acknowledges unmatched if any sub does
            if sub.ack_unmatched:
                g.ack_unmatched = True
            if is_shared:
                g.shared_queue_names.append(sub.queue_name)
                if pair in acking:
                    g.shared_acking = True
            # First non-null last_error
            if g.last_error is None and sub.last_error:
                g.last_error = sub.last_error
        else:
            sub.shared_queue_names = [sub.queue_name] if is_shared else []
            sub.shared_acking = is_shared and pair in acking
            ungrouped.append(sub)

    result: list[Any] = []
    # Ungrouped first (preserves ordering for single-queue subs)
    result.extend(ungrouped)
    result.extend(grouped.values())
    return result


# ------------------------------------------------------------------
# Shared-queue helpers
# ------------------------------------------------------------------

#: Text of the page badge and of the flash for a queue carried by more than one
#: enabled subscription on the same conn_id. Two subscriptions on one queue are safe
#: only while their filters are strictly disjoint: the broker splits deliveries
#: round-robin between the competing consumers, so a message suiting both — every
#: message, when filter_data is empty — is acknowledged by whichever consumer got it
#: and the other subscription's DAG does not run.
SHARED_QUEUE_WARNING = (
    "The broker splits deliveries round-robin between competing consumers, so a "
    "message that suits both filters reaches only one of them and the other "
    "subscription's DAG does not run. Fan-out is built from separate queues and "
    "bindings, not from two subscriptions on one queue."
)

#: Added to the notice above when one of the subscriptions on the shared queue
#: acknowledges what its filter rejected.
SHARED_QUEUE_ACK_WARNING = (
    "One of these subscriptions acknowledges unmatched messages: a message that only "
    "the neighbour's filter accepts is deleted the moment it lands on the "
    "acknowledging consumer. The neighbour stays listening and reports no error, and "
    "its DAG never runs."
)


def _queue_sharing(
    subs: Iterable[RMQSubscription],
) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    """Split enabled subscriptions into the ``(queue_name, conn_id)`` pairs that are
    shared and the ones an acknowledging subscription listens to.

    Severity belongs to the pair, not to the row it is shown on: a subscription with
    the default setting is just as affected by an acknowledging neighbour as the
    neighbour is by it, so both rows of a shared queue carry the same text.
    """
    enabled = [s for s in subs if s.enabled]
    counts = Counter((s.queue_name, s.conn_id) for s in enabled)
    shared = {pair for pair, n in counts.items() if n > 1}
    acking = {(s.queue_name, s.conn_id) for s in enabled if s.ack_unmatched}
    return shared, acking


def _flash_shared_queues(session, pairs: Iterable[tuple[str, str]]) -> None:
    """Warn about every pair among ``pairs`` that more than one enabled subscription
    now listens to.

    Called after the commit, because the answer depends on the rows as they stand
    once the save has landed. The form cannot ask beforehand: subscription_form.html
    renders dag_id, the queue list, conn_id, cooldown and filter_data out of the row,
    so a confirmation round trip would redraw them from the database and lose the very
    input it is confirming — and toggle/toggle_group have no form at all.
    """
    wanted = set(pairs)
    if not wanted:
        return
    shared, acking = _queue_sharing(
        session.query(RMQSubscription).filter_by(enabled=True).all()
    )
    for queue_name, conn_id in sorted(wanted & shared):
        message = (
            f"Queue '{queue_name}' on connection '{conn_id}' is listened to by more "
            f"than one enabled subscription. {SHARED_QUEUE_WARNING}"
        )
        if (queue_name, conn_id) in acking:
            message = f"{message} {SHARED_QUEUE_ACK_WARNING}"
        flash(message, "warning")


# ------------------------------------------------------------------
# Connection status helpers
# ------------------------------------------------------------------

#: Shown instead of the Connections table when the status rows cannot be read.
SCHEMA_OUTDATED_MESSAGE = (
    "Connection status is unavailable: the rmq_watcher_conn_status table does "
    "not yet match this version of the provider. The watcher loop in the "
    "scheduler migrates it on one of its next cycles."
)


@dataclass
class ConnStatusRow:
    """One row of the Connections table with its liveness already evaluated."""

    conn_id: str
    status: str
    consumer_count: int
    broker_consumer_count: int | None
    last_error: str | None
    age_seconds: float | None
    is_stale: bool

    @property
    def counts_mismatch(self) -> bool:
        """Broker reports a different number of consumers than the watcher started."""
        return (
            self.broker_consumer_count is not None
            and self.broker_consumer_count != self.consumer_count
        )

    @property
    def age_display(self) -> str:
        """Age of the last reconcile cycle, or an em dash when there is none."""
        if self.age_seconds is None:
            return "—"
        return format_duration(self.age_seconds)


def format_duration(seconds: float) -> str:
    """Render a number of seconds the way the Connections block shows durations."""
    whole = int(seconds)
    if whole < 60:
        return f"{whole}s"
    if whole < 3600:
        return f"{whole // 60}m"
    return f"{whole // 3600}h {(whole % 3600) // 60}m"


def _reconcile_interval() -> int:
    """Reconcile interval the watcher runs on, in seconds.

    The view lives in the webserver process and cannot see the listener's
    state, so it reads the same Airflow Variable through the same parser the
    listener reads it with. Anything that goes wrong — an unreachable database,
    an unset or unusable Variable — leaves the built-in default, which is what
    the loop falls back to as well.
    """
    try:
        interval = read_positive(RECONCILE_INTERVAL_VAR, int)
    except Exception:
        log.warning(
            "Failed to read Airflow Variable %s — assuming the default "
            "reconcile interval of %ss for the staleness check",
            RECONCILE_INTERVAL_VAR,
            DEFAULT_RECONCILE_INTERVAL,
            exc_info=True,
        )
        return DEFAULT_RECONCILE_INTERVAL
    return interval if interval is not None else DEFAULT_RECONCILE_INTERVAL


def _stale_after() -> float:
    """Age of ``last_reconcile_at`` past which a conn_id is flagged, in seconds.

    The page flags what the loop itself treats as late, and not a second earlier: a
    stamp one interval plus one full cycle budget old belongs to a watcher that is
    running exactly as configured. Both numbers come from the same Airflow Variables
    the loop reads, through the same parser; an unreachable database or an unusable
    Variable leaves the built-in defaults, which is what the loop falls back to as well.
    """
    interval = _reconcile_interval()
    try:
        override = read_positive(CYCLE_TIMEOUT_VAR, float)
    except Exception:
        log.warning(
            "Failed to read Airflow Variable %s — assuming the built-in cycle "
            "budget for the staleness check",
            CYCLE_TIMEOUT_VAR,
            exc_info=True,
        )
        override = None
    return stale_after(interval, cycle_timeout(interval, override))


def _build_conn_status_rows(
    conn_statuses: list[Any], stale_seconds: float
) -> list[ConnStatusRow]:
    """Wrap the status rows for display, computing the age of each cycle.

    ``last_reconcile_at`` is written by the Airflow process as naive UTC, so
    the comparison is made in naive UTC as well. A value carrying a timezone
    is normalized first: mixing naive and aware datetimes raises TypeError,
    and an error in the middle of rendering is a worse outcome than an
    approximate age. NULL — a freshly migrated row the watcher has not stamped
    yet — counts as "no data" and is shown as an em dash.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows: list[ConnStatusRow] = []
    for cs in conn_statuses:
        last = cs.last_reconcile_at
        age_seconds: float | None = None
        is_stale = False
        if last is not None:
            if last.tzinfo is not None:
                last = last.astimezone(timezone.utc).replace(tzinfo=None)
            age_seconds = max((now - last).total_seconds(), 0.0)
            is_stale = age_seconds > stale_seconds
        rows.append(
            ConnStatusRow(
                conn_id=cs.conn_id,
                status=cs.status,
                consumer_count=cs.consumer_count,
                broker_consumer_count=cs.broker_consumer_count,
                last_error=cs.last_error,
                age_seconds=age_seconds,
                is_stale=is_stale,
            )
        )
    return rows


class RMQWatcherView(BaseView):
    route_base = "/rmq-watcher"
    default_view = "subscriptions"
    class_permission_name = "RMQ Subscriptions"
    base_permissions = ["can_read", "can_edit", "can_create", "can_delete"]
    method_permission_name = {
        "subscriptions": "read",
        "create": "create",
        "edit": "edit",
        "delete": "delete",
        "toggle": "edit",
        "delete_group": "delete",
        "toggle_group": "edit",
        "edit_group": "edit",
    }

    @expose("/subscriptions")
    @has_access
    def subscriptions(self):
        subscriptions_error = None
        with WatcherSession() as session:
            try:
                subs = session.query(RMQSubscription).order_by(RMQSubscription.dag_id).all()
            except Exception:
                log.warning(
                    "Failed to read rmq_watcher_subscriptions — the Subscriptions "
                    "block is replaced with a notice for this request",
                    exc_info=True,
                )
                try:
                    # a backend that aborts the transaction on a failed statement
                    # would refuse every later read in this session otherwise; no
                    # row has been loaded yet, so nothing is expired by this
                    session.rollback()
                except Exception:
                    # a connection broken enough to refuse the rollback still has to
                    # leave the page standing — the reads that follow will fail into
                    # their own notices
                    log.warning("Rollback after the failed read failed too", exc_info=True)
                subs = []
                subscriptions_error = SUBSCRIPTIONS_SCHEMA_OUTDATED_MESSAGE
            try:
                conn_statuses = get_conn_statuses(session)
            except Exception:
                log.warning(
                    "Failed to read rmq_watcher_conn_status — the Connections "
                    "block is replaced with a schema notice for this request",
                    exc_info=True,
                )
                conn_statuses = None
            rows = _group_subscriptions(subs)

        conn_status_error = None
        conn_status_rows: list[ConnStatusRow] = []
        stale_seconds = 0.0
        if conn_statuses is None:
            conn_status_error = SCHEMA_OUTDATED_MESSAGE
        elif conn_statuses:
            # reading the tunables is a database round trip, so it happens only
            # when there is a row whose age they could be measured against
            stale_seconds = _stale_after()
            conn_status_rows = _build_conn_status_rows(conn_statuses, stale_seconds)

        # Separate session, opened only after the one above has closed:
        # render_template() below reads `rows`/`subs` as detached objects, and
        # rollback() on any session at this point would expire them and raise
        # DetachedInstanceError in Jinja — so it must never be called here.
        try:
            with WatcherSession() as dag_session:
                active_dag_ids = get_active_dag_ids(dag_session)
        except Exception:
            log.warning(
                "Failed to look up known Airflow dag_ids for the "
                "'dag not found' badge — badge disabled for this request",
                exc_info=True,
            )
            active_dag_ids = None

        return self.render_template(
            "rmq_watcher/subscriptions.html",
            subscriptions=rows,
            subscriptions_error=subscriptions_error,
            conn_statuses=conn_status_rows,
            conn_status_error=conn_status_error,
            stale_after_display=format_duration(stale_seconds),
            active_dag_ids=active_dag_ids,
            shared_queue_warning=SHARED_QUEUE_WARNING,
            shared_queue_ack_warning=SHARED_QUEUE_ACK_WARNING,
        )

    @expose("/subscriptions/create", methods=["GET", "POST"])
    @has_access
    def create(self):
        if request.method == "POST":
            dag_id = request.form.get("dag_id", "").strip()
            queue_names_raw = request.form.getlist("queue_name")
            queue_names = [q.strip() for q in queue_names_raw if q.strip()]
            conn_id = request.form.get("conn_id", "rmq_default").strip() or "rmq_default"
            filter_data_raw = request.form.get("filter_data", "").strip()
            enabled = request.form.get("enabled") == "on"
            ack_unmatched = request.form.get("ack_unmatched") == "on"
            cooldown_raw = request.form.get("cooldown", "0").strip()

            if not dag_id or not queue_names:
                flash("dag_id and at least one queue_name are required", "error")
                return self.render_template(
                    "rmq_watcher/subscription_form.html",
                    sub=None,
                    is_dag_file=False,
                    ack_unmatched_checked=ack_unmatched,
                )

            try:
                cooldown = parse_cooldown(cooldown_raw)
            except ValueError:
                flash("cooldown must be a non-negative integer", "error")
                return self.render_template(
                    "rmq_watcher/subscription_form.html",
                    sub=None,
                    is_dag_file=False,
                    ack_unmatched_checked=ack_unmatched,
                )

            try:
                filter_data = parse_filter_data(filter_data_raw)
            except ValueError:
                flash("filter_data must be valid JSON", "error")
                return self.render_template(
                    "rmq_watcher/subscription_form.html",
                    sub=None,
                    is_dag_file=False,
                    ack_unmatched_checked=ack_unmatched,
                )

            group_key = dag_id if cooldown > 0 else None

            with WatcherSession() as session:
                for queue_name in queue_names:
                    upsert_subscription(
                        session,
                        dag_id=dag_id,
                        queue_name=queue_name,
                        conn_id=conn_id,
                        filter_data=filter_data,
                        source="ui",
                        enabled=enabled,
                        cooldown=cooldown if cooldown > 0 else None,
                        group_key=group_key,
                        ack_unmatched=ack_unmatched,
                    )
                session.commit()
                flash(f"Subscription for DAG '{dag_id}' created", "success")
                if enabled:
                    _flash_shared_queues(session, [(q, conn_id) for q in queue_names])

            return redirect(url_for("RMQWatcherView.subscriptions"))

        return self.render_template(
            "rmq_watcher/subscription_form.html",
            sub=None,
            is_dag_file=False,
            ack_unmatched_checked=False,
        )

    @expose("/subscriptions/<int:sub_id>/edit", methods=["GET", "POST"])
    @has_access
    def edit(self, sub_id: int):
        with WatcherSession() as session:
            sub = session.query(RMQSubscription).filter_by(id=sub_id).first()
            if sub is None:
                flash("Subscription not found", "error")
                return redirect(url_for("RMQWatcherView.subscriptions"))

            is_dag_file = sub.source == "dag_file"

            if request.method == "POST":
                enabled = request.form.get("enabled") == "on"
                sub.enabled = enabled

                if not is_dag_file:
                    # ack_unmatched is assigned the way cooldown is, inside this
                    # branch: for a dag_file row the value belongs to @rmq_trigger,
                    # and a write from the form would be overwritten by the next scan.
                    # enabled stands outside it because it is the one field the UI
                    # owns on a dag_file row as well
                    ack_unmatched = request.form.get("ack_unmatched") == "on"
                    # the error paths below redraw the box from this value rather than
                    # from the row: a save that trips on cooldown or filter_data must
                    # not put permanent deletion back on for a user who cleared it
                    dag_id = request.form.get("dag_id", sub.dag_id).strip()
                    queue_name = request.form.get("queue_name", sub.queue_name).strip()
                    conn_id = (
                        request.form.get("conn_id", sub.conn_id).strip() or "rmq_default"
                    )
                    filter_data_raw = request.form.get("filter_data", "").strip()
                    cooldown_raw = request.form.get("cooldown", "0").strip()

                    if not dag_id or not queue_name:
                        flash("dag_id and queue_name are required", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html",
                            sub=sub,
                            ack_unmatched_checked=ack_unmatched,
                        )

                    try:
                        cooldown = parse_cooldown(cooldown_raw)
                    except ValueError:
                        flash("cooldown must be a non-negative integer", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html",
                            sub=sub,
                            ack_unmatched_checked=ack_unmatched,
                        )

                    try:
                        filter_data = parse_filter_data(filter_data_raw)
                    except ValueError:
                        flash("filter_data must be valid JSON", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html",
                            sub=sub,
                            ack_unmatched_checked=ack_unmatched,
                        )

                    sub.dag_id = dag_id
                    sub.queue_name = queue_name
                    sub.conn_id = conn_id
                    sub.filter_data = filter_data
                    sub.cooldown = cooldown if cooldown > 0 else None
                    sub.group_key = dag_id if cooldown > 0 else None
                    sub.ack_unmatched = ack_unmatched

                session.commit()
                flash("Subscription updated", "success")
                if sub.enabled:
                    _flash_shared_queues(session, [(sub.queue_name, sub.conn_id)])
                return redirect(url_for("RMQWatcherView.subscriptions"))

            return self.render_template(
                "rmq_watcher/subscription_form.html",
                sub=sub,
                is_dag_file=is_dag_file,
                ack_unmatched_checked=bool(sub.ack_unmatched),
            )

    @expose("/subscriptions/<int:sub_id>/delete", methods=["POST"])
    @has_access
    def delete(self, sub_id: int):
        with WatcherSession() as session:
            sub = session.query(RMQSubscription).filter_by(id=sub_id).first()
            if sub is None:
                flash("Subscription not found", "error")
                return redirect(url_for("RMQWatcherView.subscriptions"))

            if sub.source == "dag_file":
                flash(
                    "Cannot delete a dag_file subscription from UI — "
                    "remove @rmq_trigger from the DAG file",
                    "error",
                )
                return redirect(url_for("RMQWatcherView.subscriptions"))

            session.delete(sub)
            session.commit()

        flash("Subscription deleted", "success")
        return redirect(url_for("RMQWatcherView.subscriptions"))

    @expose("/subscriptions/<int:sub_id>/toggle", methods=["POST"])
    @has_access
    def toggle(self, sub_id: int):
        with WatcherSession() as session:
            sub = session.query(RMQSubscription).filter_by(id=sub_id).first()
            if sub is None:
                flash("Subscription not found", "error")
                return redirect(url_for("RMQWatcherView.subscriptions"))

            sub.enabled = not sub.enabled
            session.commit()
            if sub.enabled:
                _flash_shared_queues(session, [(sub.queue_name, sub.conn_id)])

        return redirect(url_for("RMQWatcherView.subscriptions"))

    # ------------------------------------------------------------------
    # Group-level endpoints (act on all subs sharing a group_key)
    # ------------------------------------------------------------------

    @expose("/subscriptions/group/<group_key>/delete", methods=["POST"])
    @has_access
    def delete_group(self, group_key: str):
        with WatcherSession() as session:
            subs = (
                session.query(RMQSubscription)
                .filter_by(group_key=group_key)
                .all()
            )
            if not subs:
                flash("Group not found", "error")
                return redirect(url_for("RMQWatcherView.subscriptions"))

            if any(s.source == "dag_file" for s in subs):
                flash(
                    "Cannot delete a dag_file subscription group from UI — "
                    "remove @rmq_trigger from the DAG file",
                    "error",
                )
                return redirect(url_for("RMQWatcherView.subscriptions"))

            for sub in subs:
                session.delete(sub)
            session.commit()

        flash(f"Group '{group_key}' deleted ({len(subs)} subscription(s))", "success")
        return redirect(url_for("RMQWatcherView.subscriptions"))

    @expose("/subscriptions/group/<group_key>/toggle", methods=["POST"])
    @has_access
    def toggle_group(self, group_key: str):
        with WatcherSession() as session:
            subs = (
                session.query(RMQSubscription)
                .filter_by(group_key=group_key)
                .all()
            )
            if not subs:
                flash("Group not found", "error")
                return redirect(url_for("RMQWatcherView.subscriptions"))

            # If all enabled → disable all; else enable all
            all_enabled = all(s.enabled for s in subs)
            for sub in subs:
                sub.enabled = not all_enabled
            session.commit()
            if not all_enabled:
                _flash_shared_queues(session, [(s.queue_name, s.conn_id) for s in subs])

        return redirect(url_for("RMQWatcherView.subscriptions"))

    @expose("/subscriptions/group/<group_key>/edit", methods=["GET", "POST"])
    @has_access
    def edit_group(self, group_key: str):
        with WatcherSession() as session:
            subs = (
                session.query(RMQSubscription)
                .filter_by(group_key=group_key)
                .order_by(RMQSubscription.queue_name)
                .all()
            )
            if not subs:
                flash("Group not found", "error")
                return redirect(url_for("RMQWatcherView.subscriptions"))

            is_dag_file = any(s.source == "dag_file" for s in subs)
            # Representative sub for display defaults
            rep = subs[0]
            # A save writes one value to every row, so the box carries the value
            # the whole group already agrees on: a group whose rows disagree is
            # drawn off, and a save that only touched cooldown or filter_data
            # cannot switch permanent deletion on for a row that runs without it.
            # The disagreement itself is named under the checkbox.
            ack_unmatched_all = all(s.ack_unmatched for s in subs)
            ack_unmatched_mixed = (
                any(s.ack_unmatched for s in subs) and not ack_unmatched_all
            )

            if request.method == "POST":
                enabled = request.form.get("enabled") == "on"

                if not is_dag_file:
                    # the group is rewritten row by row through upsert_subscription,
                    # which assigns ack_unmatched unconditionally — without passing it
                    # here the save would clear the setting for the whole group. The
                    # error paths below redraw the box from this value, so a save that
                    # trips elsewhere keeps the setting the user submitted
                    ack_unmatched = request.form.get("ack_unmatched") == "on"
                    dag_id = request.form.get("dag_id", rep.dag_id).strip()
                    queue_names_raw = request.form.getlist("queue_name")
                    queue_names = [q.strip() for q in queue_names_raw if q.strip()]
                    conn_id = (
                        request.form.get("conn_id", rep.conn_id).strip() or "rmq_default"
                    )
                    filter_data_raw = request.form.get("filter_data", "").strip()
                    cooldown_raw = request.form.get("cooldown", "0").strip()

                    if not dag_id or not queue_names:
                        flash("dag_id and at least one queue_name are required", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html",
                            sub=rep,
                            is_dag_file=is_dag_file,
                            group_subs=subs,
                            ack_unmatched_checked=ack_unmatched,
                            ack_unmatched_mixed=ack_unmatched_mixed,
                        )

                    try:
                        cooldown = parse_cooldown(cooldown_raw)
                    except ValueError:
                        flash("cooldown must be a non-negative integer", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html",
                            sub=rep,
                            is_dag_file=is_dag_file,
                            group_subs=subs,
                            ack_unmatched_checked=ack_unmatched,
                            ack_unmatched_mixed=ack_unmatched_mixed,
                        )

                    try:
                        filter_data = parse_filter_data(filter_data_raw)
                    except ValueError:
                        flash("filter_data must be valid JSON", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html",
                            sub=rep,
                            is_dag_file=is_dag_file,
                            group_subs=subs,
                            ack_unmatched_checked=ack_unmatched,
                            ack_unmatched_mixed=ack_unmatched_mixed,
                        )

                    new_group_key = dag_id if cooldown > 0 else None

                    # Delete ALL existing subs in the group so that stale rows
                    # cannot accumulate when conn_id or dag_id changes while the
                    # queue names stay the same. upsert_subscription will
                    # re-create every needed row after the flush.
                    for sub in subs:
                        session.delete(sub)
                    session.flush()

                    # Insert / update all queues fresh
                    for qname in queue_names:
                        upsert_subscription(
                            session,
                            dag_id=dag_id,
                            queue_name=qname,
                            conn_id=conn_id,
                            filter_data=filter_data,
                            source="ui",
                            enabled=enabled,
                            cooldown=cooldown if cooldown > 0 else None,
                            group_key=new_group_key,
                            ack_unmatched=ack_unmatched,
                        )

                    session.commit()
                    flash("Group updated", "success")
                    if enabled:
                        _flash_shared_queues(
                            session, [(q, conn_id) for q in queue_names]
                        )
                    return redirect(url_for("RMQWatcherView.subscriptions"))

                # dag_file: only toggle enabled
                for sub in subs:
                    sub.enabled = enabled
                session.commit()
                flash("Group updated", "success")
                if enabled:
                    _flash_shared_queues(
                        session, [(s.queue_name, s.conn_id) for s in subs]
                    )
                return redirect(url_for("RMQWatcherView.subscriptions"))

            return self.render_template(
                "rmq_watcher/subscription_form.html",
                sub=rep,
                is_dag_file=is_dag_file,
                group_subs=subs,
                ack_unmatched_checked=ack_unmatched_all,
                ack_unmatched_mixed=ack_unmatched_mixed,
            )
