from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

from flask import flash, redirect, request, url_for
from flask_appbuilder import BaseView, expose
from flask_appbuilder.security.decorators import has_access

from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    WatcherSession,
    get_conn_statuses,
    upsert_subscription,
)
from airflow_provider_rmq.watcher.subscription_form import parse_cooldown, parse_filter_data

log = logging.getLogger(__name__)


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
    RMQSubscription (for ungrouped, i.e. group_key is NULL).
    """
    grouped: dict[str, SubscriptionGroup] = {}
    ungrouped: list[RMQSubscription] = []

    for sub in subs:
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
            # First non-null last_error
            if g.last_error is None and sub.last_error:
                g.last_error = sub.last_error
        else:
            ungrouped.append(sub)

    result: list[Any] = []
    # Ungrouped first (preserves ordering for single-queue subs)
    result.extend(ungrouped)
    result.extend(grouped.values())
    return result


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
        with WatcherSession() as session:
            subs = session.query(RMQSubscription).order_by(RMQSubscription.dag_id).all()
            conn_statuses = get_conn_statuses(session)
            rows = _group_subscriptions(subs)
        return self.render_template(
            "rmq_watcher/subscriptions.html",
            subscriptions=rows,
            conn_statuses=conn_statuses,
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
            cooldown_raw = request.form.get("cooldown", "0").strip()

            if not dag_id or not queue_names:
                flash("dag_id and at least one queue_name are required", "error")
                return self.render_template(
                    "rmq_watcher/subscription_form.html", sub=None, is_dag_file=False
                )

            try:
                cooldown = parse_cooldown(cooldown_raw)
            except ValueError:
                flash("cooldown must be a non-negative integer", "error")
                return self.render_template(
                    "rmq_watcher/subscription_form.html", sub=None, is_dag_file=False
                )

            try:
                filter_data = parse_filter_data(filter_data_raw)
            except ValueError:
                flash("filter_data must be valid JSON", "error")
                return self.render_template(
                    "rmq_watcher/subscription_form.html", sub=None, is_dag_file=False
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
                    )
                session.commit()

            flash(f"Subscription for DAG '{dag_id}' created", "success")
            return redirect(url_for("RMQWatcherView.subscriptions"))

        return self.render_template("rmq_watcher/subscription_form.html", sub=None, is_dag_file=False)

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
                            "rmq_watcher/subscription_form.html", sub=sub
                        )

                    try:
                        cooldown = parse_cooldown(cooldown_raw)
                    except ValueError:
                        flash("cooldown must be a non-negative integer", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html", sub=sub
                        )

                    try:
                        filter_data = parse_filter_data(filter_data_raw)
                    except ValueError:
                        flash("filter_data must be valid JSON", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html", sub=sub
                        )

                    sub.dag_id = dag_id
                    sub.queue_name = queue_name
                    sub.conn_id = conn_id
                    sub.filter_data = filter_data
                    sub.cooldown = cooldown if cooldown > 0 else None
                    sub.group_key = dag_id if cooldown > 0 else None

                session.commit()
                flash("Subscription updated", "success")
                return redirect(url_for("RMQWatcherView.subscriptions"))

            return self.render_template(
                "rmq_watcher/subscription_form.html",
                sub=sub,
                is_dag_file=is_dag_file,
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

            if request.method == "POST":
                enabled = request.form.get("enabled") == "on"

                if not is_dag_file:
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
                        )

                    session.commit()
                    flash("Group updated", "success")
                    return redirect(url_for("RMQWatcherView.subscriptions"))

                # dag_file: only toggle enabled
                for sub in subs:
                    sub.enabled = enabled
                session.commit()
                flash("Group updated", "success")
                return redirect(url_for("RMQWatcherView.subscriptions"))

            return self.render_template(
                "rmq_watcher/subscription_form.html",
                sub=rep,
                is_dag_file=is_dag_file,
                group_subs=subs,
            )
