from __future__ import annotations

import logging

log = logging.getLogger(__name__)


def incr(metric: str) -> None:
    """Bump a statsd counter. Metrics never affect control flow, so failures are silent."""
    try:
        from airflow.stats import Stats
        Stats.incr(metric)
    except Exception:
        log.debug("RMQ Watcher: cannot report metric %s", metric, exc_info=True)
