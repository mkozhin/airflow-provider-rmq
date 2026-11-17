"""The red banner on Airflow's home page when no watcher confirms its work.

The banner is Airflow's own ``UIAlert``, added to ``airflow.settings.DASHBOARD_UIALERTS``
in the webserver. On every request of the home page Airflow asks each alert
``should_show`` and then renders the ``message`` and ``category`` of those that answered
yes, within the same request. The alert here answers from a verdict cached for
:data:`_CACHE_SECONDS` per webserver worker, keeps the banner text it decided in
:data:`flask.g` for that request, and renders only what it kept: the decision and the
text of one request come from the same verdict even when the cache expires in between,
and rendering never reaches the database.

The verdict is read by :func:`~.views.read_liveness`, the same read the Subscriptions
page makes. The plugin imports this module only in the webserver, from the blueprint
callback that installs the banner, and ``airflow.www.utils`` is imported only by
:func:`install_dashboard_alert`.
"""
from __future__ import annotations

import functools
import logging
import threading
import time
from typing import Any

from flask import g, has_app_context
from markupsafe import Markup

from airflow_provider_rmq.watcher.liveness import LivenessVerdict, banner_text
from airflow_provider_rmq.watcher.models import WatcherSession, utcnow
from airflow_provider_rmq.watcher.views import read_liveness, subscriptions_link

log = logging.getLogger(__name__)

#: Seconds a verdict is reused by one webserver worker before the database is read again.
_CACHE_SECONDS = 30.0

#: Name under which the banner text decided for the request is kept in :data:`flask.g`.
_G_ATTR = "rmq_watcher_liveness_alert"

#: ``(expires_at, verdict)`` on the :func:`time.monotonic` clock. ``None`` as the verdict
#: means the last read failed and the banner stays hidden until the next read. The pair
#: is replaced as a whole, so a reader without the lock sees either the old pair or the
#: new one.
_cache: tuple[float, LivenessVerdict | None] = (float("-inf"), None)
_cache_lock = threading.Lock()


def _read_verdict() -> LivenessVerdict | None:
    """A fresh verdict, or ``None`` when the database cannot give one."""
    try:
        with WatcherSession() as session:
            return read_liveness(session, utcnow())
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


def _decide() -> Markup | None:
    """The banner text for this request, or ``None`` for no banner; kept in :data:`flask.g`."""
    verdict = cached_verdict()
    message = banner_text(verdict, link=subscriptions_link()) if verdict else None
    setattr(g, _G_ATTR, message)
    return message


@functools.cache
def _alert_class() -> type:
    """The alert class, defined on Airflow's ``UIAlert`` once per process.

    One class per process lets a second installation recognise the alert the first one
    added by its type.
    """
    from airflow.www.utils import UIAlert

    class _WatcherLivenessAlert(UIAlert):
        """The watcher banner: shown to every user while the verdict is ``DOWN``.

        ``UIAlert.__init__`` assigns ``message``; here it is a read-only property over
        the text :meth:`should_show` decided for the request, so the constructor is not
        called.
        """

        roles = None
        category = "error"

        def __init__(self) -> None:
            pass

        @property
        def message(self) -> Markup:
            decided = g.get(_G_ATTR) if has_app_context() else None
            return decided or Markup("")

        def should_show(self, *args: Any, **kwargs: Any) -> bool:
            """Decide the banner for the request; show it only for ``DOWN``.

            Airflow passes the appbuilder positionally; nothing here needs it. Any
            failure hides the banner, because the home page must render whatever
            happens to the plugin.
            """
            try:
                return _decide() is not None
            except Exception:
                log.warning(
                    "RMQ Watcher: failed to decide on the watcher banner — it is not "
                    "shown for this request",
                    exc_info=True,
                )
                return False

    return _WatcherLivenessAlert


def install_dashboard_alert() -> None:
    """Add the watcher banner to ``airflow.settings.DASHBOARD_UIALERTS``.

    The list is rebound rather than appended to, since ``airflow_local_settings`` may
    set it as a tuple; an alert this module already installed in the list is kept and
    no second one is added. Nothing here reads the database: the first request of the
    home page reads the verdict, and creates the heartbeat table on the way.
    """
    from airflow import settings

    alert_class = _alert_class()
    current = list(settings.DASHBOARD_UIALERTS)
    if any(isinstance(alert, alert_class) for alert in current):
        return
    settings.DASHBOARD_UIALERTS = [*current, alert_class()]
