from __future__ import annotations

from airflow_provider_rmq.watcher.orphan_tracker import OrphanTracker


class TestOrphanTracker:
    def test_diff_reports_newly_orphaned_dag_id(self):
        tracker = OrphanTracker()
        tracker.mark_provisioned({"dag_a", "dag_b"})

        newly_orphaned, restored = tracker.diff({"dag_a"})

        assert newly_orphaned == {"dag_b"}
        assert restored == set()

    def test_empty_diff_on_repeated_unchanged_active_set(self):
        tracker = OrphanTracker()
        tracker.mark_provisioned({"dag_a", "dag_b"})

        first = tracker.diff({"dag_a"})
        assert first == ({"dag_b"}, set())

        # Second call with the same active set must not re-report dag_b
        second = tracker.diff({"dag_a"})
        assert second == (set(), set())

    def test_restored_when_dag_id_returns_to_active_set(self):
        tracker = OrphanTracker()
        tracker.mark_provisioned({"dag_a", "dag_b"})

        tracker.diff({"dag_a"})  # dag_b becomes orphaned
        newly_orphaned, restored = tracker.diff({"dag_a", "dag_b"})

        assert newly_orphaned == set()
        assert restored == {"dag_b"}

    def test_restored_dag_id_can_become_orphaned_again(self):
        tracker = OrphanTracker()
        tracker.mark_provisioned({"dag_a"})

        tracker.diff(set())  # dag_a orphaned
        tracker.diff({"dag_a"})  # dag_a restored

        newly_orphaned, restored = tracker.diff(set())
        assert newly_orphaned == {"dag_a"}
        assert restored == set()

    def test_no_orphans_when_nothing_provisioned(self):
        tracker = OrphanTracker()
        newly_orphaned, restored = tracker.diff(set())
        assert newly_orphaned == set()
        assert restored == set()

    def test_mark_provisioned_is_cumulative(self):
        tracker = OrphanTracker()
        tracker.mark_provisioned({"dag_a"})
        tracker.mark_provisioned({"dag_b"})

        newly_orphaned, _ = tracker.diff(set())
        assert newly_orphaned == {"dag_a", "dag_b"}

    def test_independent_instances_do_not_share_state(self):
        tracker1 = OrphanTracker()
        tracker2 = OrphanTracker()

        tracker1.mark_provisioned({"dag_a"})
        tracker2.mark_provisioned({"dag_b"})

        newly_orphaned_1, _ = tracker1.diff(set())
        newly_orphaned_2, _ = tracker2.diff(set())

        assert newly_orphaned_1 == {"dag_a"}
        assert newly_orphaned_2 == {"dag_b"}
