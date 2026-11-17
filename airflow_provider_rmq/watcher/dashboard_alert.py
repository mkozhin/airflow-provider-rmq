"""The red banner on Airflow's home page when no watcher confirms its work.

The banner is Airflow's own ``UIAlert``, added to ``airflow.settings.DASHBOARD_UIALERTS``
in the webserver. On every request of the home page Airflow asks each alert
``should_show`` and then renders the ``message`` and ``category`` of those that answered
yes, within the same request. The alert here answers from a verdict cached for
:data:`_CACHE_SECONDS` per webserver worker, keeps the verdict and its text in
:data:`flask.g` for that request, and renders only what it kept: the decision and the
text of one request come from the same verdict even when the cache expires in between,
and rendering never reaches the database.

``airflow.www.utils`` is imported only by :func:`install_dashboard_alert`, which runs in
the webserver: the plugin module that imports this one is loaded by every Airflow
process, the scheduler among them.
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from flask import g, has_app_context, url_for
from markupsafe import Markup
from sqlalchemy import exists
from sqlalchemy.orm import Session
from sqlalchemy.sql import func
from werkzeug.routing import BuildError

from airflow_provider_rmq.watcher.listener import _VARIABLE_TIMEOUT
from airflow_provider_rmq.watcher.liveness import (
    SUBSCRIPTIONS_PATH,
    LivenessVerdict,
    assess_liveness,
    banner_text,
)
from airflow_provider_rmq.watcher.models import (
    RMQConnStatus,
    RMQSubscription,
    WatcherSession,
    ensure_heartbeat_table,
    get_heartbeats,
    is_heartbeat_table_ready,
)
from airflow_provider_rmq.watcher.views import _stale_after

log = logging.getLogger(__name__)

#: Seconds a verdict is reused by one webserver worker before the database is read again.
_CACHE_SECONDS = 30.0

#: Seconds of a cycle's work after its ``last_reconcile_at`` stamp: writing the
#: subscription statuses and the heartbeat. A stamp is fresh while it is younger than the
#: page's staleness threshold plus the Variable read and this tail.
_RECONCILE_TAIL = 60.0

#: How far back heartbeat rows are read: a living watcher prunes other runs' rows past
#: this age, and the Subscriptions page lists the runs of the same window.
HEARTBEAT_WINDOW = timedelta(days=1)

#: Endpoint of the Subscriptions page, which the banner links to.
_SUBSCRIPTIONS_ENDPOINT = "RMQWatcherView.subscriptions"

#: Name under which the per-request snapshot is kept in :data:`flask.g`.
_G_ATTR = "rmq_watcher_liveness_alert"

#: Attribute that marks the alert this module installs, so a second installation in the
#: same process finds the first one in the list.
_MARKER = "is_rmq_watcher_liveness_alert"

#: ``(expires_at, verdict)`` on the :func:`time.monotonic` clock. ``None`` as the verdict
#: means the last read failed and the banner stays hidden until the next read. The pair
#: is replaced as a whole, so a reader without the lock sees either the old pair or the
#: new one.
_cache: tuple[float, LivenessVerdict | None] = (float("-inf"), None)
_cache_lock = threading.Lock()


def _utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


def _reconcile_is_fresh(session: Session, now: datetime) -> bool:
    """Whether the latest ``last_reconcile_at`` of any conn_id is still fresh at ``now``.

    Fresh means at most the page's staleness threshold plus :data:`_VARIABLE_TIMEOUT`
    plus :data:`_RECONCILE_TAIL` old. No stamp at all is not fresh.
    """
    latest = session.query(func.max(RMQConnStatus.last_reconcile_at)).scalar()
    if latest is None:
        return False
    threshold = _stale_after() + _VARIABLE_TIMEOUT + _RECONCILE_TAIL
    return (now - _naive_utc(latest)).total_seconds() <= threshold


def read_liveness(session: Session, now: datetime) -> LivenessVerdict:
    """Read the facts the verdict needs and assess it.

    The heartbeat table is created first while this process has not yet seen it exist.
    When that fails the verdict is assessed without heartbeat rows, from the
    ``last_reconcile_at`` stamp alone, so a failed DDL does not hide a watcher that is
    down. The stamp and the staleness threshold are read only when no run is alive.

    Blocking — it queries the metadata database; a failed read propagates.
    """
    if not is_heartbeat_table_ready():
        try:
            ensure_heartbeat_table()
        except Exception:
            log.warning(
                "RMQ Watcher: failed to create the rmq_watcher_heartbeat table — the "
                "watcher liveness is judged by last_reconcile_at alone",
                exc_info=True,
            )
    rows = get_heartbeats(session, now - HEARTBEAT_WINDOW) if is_heartbeat_table_ready() else []
    has_enabled = session.query(exists().where(RMQSubscription.enabled.is_(True))).scalar()
    return assess_liveness(
        rows,
        now,
        has_enabled_subscriptions=bool(has_enabled),
        has_fresh_reconcile=lambda: _reconcile_is_fresh(session, now),
    )


def _read_verdict() -> LivenessVerdict | None:
    """A fresh verdict, or ``None`` when the database cannot give one."""
    try:
        with WatcherSession() as session:
            return read_liveness(session, _utcnow())
    except Exception:
        log.warning(
            "RMQ Watcher: failed to read the watcher liveness — the home page shows no "
            "watcher banner for the next %ss",
            int(_CACHE_SECONDS),
            exc_info=True,
        )
        return None


def cached_verdict() -> LivenessVerdict | None:
    """The verdict of this worker's cache, read again once it is :data:`_CACHE_SECONDS` old.

    One request re-reads at a time; the others wait for it and take its result, so an
    expired cache costs one round of queries and not one per waiting request. A failed
    read is cached as ``None`` for the same period, which also limits its WARNING to one
    per period.
    """
    global _cache
    expires_at, verdict = _cache
    if time.monotonic() < expires_at:
        return verdict
    with _cache_lock:
        expires_at, verdict = _cache
        if time.monotonic() < expires_at:
            return verdict
        verdict = _read_verdict()
        _cache = (time.monotonic() + _CACHE_SECONDS, verdict)
        return verdict


@dataclass(frozen=True)
class _Snapshot:
    """What one request decided: the verdict, and the banner's text and category."""

    verdict: LivenessVerdict | None
    message: Markup | None
    category: str


def _subscriptions_link() -> str:
    """URL of the Subscriptions page under the web UI's own base URL.

    An application without the page's endpoint, or a call outside a request, gets the
    page's path as the provider registers it.
    """
    try:
        return url_for(_SUBSCRIPTIONS_ENDPOINT)
    except (BuildError, RuntimeError):
        return SUBSCRIPTIONS_PATH


def _take_snapshot() -> _Snapshot:
    verdict = cached_verdict()
    message = banner_text(verdict, link=_subscriptions_link()) if verdict else None
    snapshot = _Snapshot(
        verdict=verdict,
        message=message,
        category="error" if message is not None else "info",
    )
    setattr(g, _G_ATTR, snapshot)
    return snapshot


def _request_snapshot() -> _Snapshot | None:
    if not has_app_context():
        return None
    return g.get(_G_ATTR)


def _make_alert() -> Any:
    """Define the alert class on Airflow's ``UIAlert`` and return its one instance."""
    from airflow.www.utils import UIAlert

    class _WatcherLivenessAlert(UIAlert):
        """The watcher banner: shown to every user while the verdict is ``DOWN``.

        ``UIAlert.__init__`` assigns ``message`` and ``category``; here both are
        read-only properties over the request's snapshot, so the constructor is not
        called.
        """

        roles = None

        def __init__(self) -> None:
            setattr(self, _MARKER, True)

        @property
        def message(self) -> Markup:
            snapshot = _request_snapshot()
            if snapshot is None or snapshot.message is None:
                return Markup("")
            return snapshot.message

        @property
        def category(self) -> str:
            snapshot = _request_snapshot()
            return snapshot.category if snapshot is not None else "info"

        def should_show(self, *args: Any, **kwargs: Any) -> bool:
            """Take the request's snapshot and show the banner only for ``DOWN``.

            Airflow passes the appbuilder positionally; nothing here needs it. Any
            failure hides the banner, because the home page must render whatever
            happens to the plugin.
            """
            try:
                return _take_snapshot().message is not None
            except Exception:
                log.warning(
                    "RMQ Watcher: failed to decide on the watcher banner — it is not "
                    "shown for this request",
                    exc_info=True,
                )
                return False

    return _WatcherLivenessAlert()


def install_dashboard_alert() -> None:
    """Add the watcher banner to ``airflow.settings.DASHBOARD_UIALERTS``.

    Creates the heartbeat table first (a failure is a WARNING; the first cache miss tries
    again). The list is rebound rather than appended to, since ``airflow_local_settings``
    may set it as a tuple; an alert this module already installed in the list is kept and
    no second one is added.
    """
    try:
        ensure_heartbeat_table()
    except Exception:
        log.warning(
            "RMQ Watcher: failed to create the rmq_watcher_heartbeat table at webserver "
            "start — the first banner check tries again",
            exc_info=True,
        )
    from airflow import settings

    current = list(settings.DASHBOARD_UIALERTS)
    if any(getattr(alert, _MARKER, False) is True for alert in current):
        return
    settings.DASHBOARD_UIALERTS = [*current, _make_alert()]
