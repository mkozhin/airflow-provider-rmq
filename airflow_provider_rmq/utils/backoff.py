"""The arithmetic of a growing pause, shared by everything that retries.

The watcher waits out a growing interval in three unrelated places — a delivery whose
trigger failed, a publish the broker will not take, and a schema migration counted in
reconcile cycles rather than seconds — and the doubling is the same in all of them.
"""
from __future__ import annotations


def next_backoff(current: float, maximum: float, minimum: float = 0.0) -> float:
    """The pause that follows ``current``: twice as long, and never past ``maximum``.

    :param minimum: Floor for the result, for a backoff that starts counting from zero.
    """
    return min(max(current * 2, minimum), maximum)
