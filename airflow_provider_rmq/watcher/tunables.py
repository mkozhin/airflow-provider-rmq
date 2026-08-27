"""Names and defaults of the Airflow Variables that tune the watcher.

The reconcile interval is read by two processes that share nothing else: the
scheduler runs the loop, and the webserver renders the Subscriptions page and needs
the same number to tell a fresh status row from a stale one. Keeping the names here
lets the view read them without importing the listener, whose module chain pulls
``aio_pika`` and ``httpx`` into a process that never opens a connection.
"""
from __future__ import annotations

#: Seconds between reconcile cycles when the Variable below is unset.
DEFAULT_RECONCILE_INTERVAL = 60

#: Airflow Variables holding the watcher tunables.
RECONCILE_INTERVAL_VAR = "rmq_watcher_reconcile_interval"
CYCLE_TIMEOUT_VAR = "rmq_watcher_cycle_timeout"
