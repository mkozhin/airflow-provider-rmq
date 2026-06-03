from __future__ import annotations

import json
import logging

from flask import flash, redirect, request, url_for
from flask_appbuilder import BaseView, expose
from flask_appbuilder.security.decorators import has_access

from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    WatcherSession,
    get_conn_statuses,
    upsert_subscription,
)

log = logging.getLogger(__name__)


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
    }

    @expose("/subscriptions")
    @has_access
    def subscriptions(self):
        with WatcherSession() as session:
            subs = session.query(RMQSubscription).order_by(RMQSubscription.dag_id).all()
            conn_statuses = get_conn_statuses(session)
        return self.render_template(
            "rmq_watcher/subscriptions.html",
            subscriptions=subs,
            conn_statuses=conn_statuses,
        )

    @expose("/subscriptions/create", methods=["GET", "POST"])
    @has_access
    def create(self):
        if request.method == "POST":
            dag_id = request.form.get("dag_id", "").strip()
            queue_name = request.form.get("queue_name", "").strip()
            conn_id = request.form.get("conn_id", "rmq_default").strip() or "rmq_default"
            filter_data_raw = request.form.get("filter_data", "").strip()
            enabled = request.form.get("enabled") == "on"

            if not dag_id or not queue_name:
                flash("dag_id and queue_name are required", "error")
                return self.render_template("rmq_watcher/subscription_form.html", sub=None, is_dag_file=False)

            try:
                filter_data = json.loads(filter_data_raw) if filter_data_raw else {}
            except json.JSONDecodeError:
                flash("filter_data must be valid JSON", "error")
                return self.render_template("rmq_watcher/subscription_form.html", sub=None, is_dag_file=False)

            with WatcherSession() as session:
                upsert_subscription(
                    session,
                    dag_id=dag_id,
                    queue_name=queue_name,
                    conn_id=conn_id,
                    filter_data=filter_data,
                    source="ui",
                    enabled=enabled,
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

                    if not dag_id or not queue_name:
                        flash("dag_id and queue_name are required", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html", sub=sub
                        )

                    try:
                        filter_data = json.loads(filter_data_raw) if filter_data_raw else {}
                    except json.JSONDecodeError:
                        flash("filter_data must be valid JSON", "error")
                        return self.render_template(
                            "rmq_watcher/subscription_form.html", sub=sub
                        )

                    sub.dag_id = dag_id
                    sub.queue_name = queue_name
                    sub.conn_id = conn_id
                    sub.filter_data = filter_data

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
