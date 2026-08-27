from __future__ import annotations

import logging
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from airflow_provider_rmq.utils.metrics import incr


class TestIncr:
    """Metrics never affect control flow, so a broken statsd path must stay silent."""

    def test_the_counter_reaches_airflow_stats(self):
        stats = MagicMock()
        with patch.dict(sys.modules, {"airflow.stats": SimpleNamespace(Stats=stats)}):
            incr("rmq_watcher.dag_triggered")

        stats.incr.assert_called_once_with("rmq_watcher.dag_triggered")

    def test_a_failing_stats_backend_is_swallowed(self, caplog):
        stats = MagicMock()
        stats.incr.side_effect = RuntimeError("statsd is unreachable")
        with caplog.at_level(
            logging.DEBUG, logger="airflow_provider_rmq.utils.metrics"
        ), patch.dict(sys.modules, {"airflow.stats": SimpleNamespace(Stats=stats)}):
            incr("rmq_watcher.dag_triggered")

        assert any("cannot report metric" in r.getMessage() for r in caplog.records)

    def test_an_unimportable_stats_module_is_swallowed(self, caplog):
        with caplog.at_level(
            logging.DEBUG, logger="airflow_provider_rmq.utils.metrics"
        ), patch.dict(sys.modules, {"airflow.stats": None}):
            incr("rmq_watcher.cycle_timeout")

        assert any("cannot report metric" in r.getMessage() for r in caplog.records)
