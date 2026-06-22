from __future__ import annotations


class OrphanTracker:
    """Tracks dag_ids that ever had some piece of RMQ infrastructure provisioned for them,
    and detects when a previously-provisioned dag_id drops out of (or returns to) the
    currently active set.

    Pure set algebra — no dependency on aio_pika/httpx/Airflow. Used by
    ``RMQConsumerManager`` for both cooldown pending-queue orphan detection and exchange-mode
    sub-queue/binding orphan detection; each call site keeps its own instance (independent
    state) and its own WARNING/INFO log text — this class only computes the diff.
    """

    def __init__(self) -> None:
        self._provisioned: set[str] = set()  # dag_ids that ever had infra provisioned
        self._orphaned: set[str] = set()  # dag_ids currently known to be orphaned (already warned)

    def mark_provisioned(self, dag_ids: set[str]) -> None:
        """Record that infrastructure was (successfully) provisioned for these dag_ids."""
        self._provisioned.update(dag_ids)

    def diff(self, active_ids: set[str]) -> tuple[set[str], set[str]]:
        """Compare the currently active dag_ids against everything ever provisioned.

        :param active_ids: dag_ids with a currently active subscription.
        :returns: ``(newly_orphaned, restored)`` — ``newly_orphaned`` are dag_ids that just
            dropped out of ``active_ids`` for the first time (not already known-orphaned);
            ``restored`` are previously-orphaned dag_ids that are back in ``active_ids``.
            Internal state is updated so repeated calls with an unchanged ``active_ids``
            return empty sets.
        """
        orphaned = self._provisioned - active_ids
        newly_orphaned = orphaned - self._orphaned
        if newly_orphaned:
            self._orphaned.update(newly_orphaned)

        restored = self._orphaned & active_ids
        if restored:
            self._orphaned -= restored

        return newly_orphaned, restored
