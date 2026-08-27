from __future__ import annotations

import logging
import threading

import pytest

from airflow_provider_rmq.utils.executor import BoundedExecutor


class TestBoundedExecutor:
    @pytest.mark.asyncio
    async def test_run_returns_the_call_result(self):
        pool = BoundedExecutor("test", 2)
        try:
            assert await pool.run(lambda value: value * 2, 21) == 42
        finally:
            pool.shutdown()

    @pytest.mark.asyncio
    async def test_run_propagates_the_call_error(self):
        pool = BoundedExecutor("test", 2)

        def boom():
            raise RuntimeError("database is gone")

        try:
            with pytest.raises(RuntimeError, match="database is gone"):
                await pool.run(boom)
        finally:
            pool.shutdown()

    def test_saturation_is_logged_instead_of_queueing_silently(self, caplog):
        """A stuck call keeps its worker — cancellation cannot free it — so a pool
        with every worker busy must say so rather than look merely slow."""
        pool = BoundedExecutor("test", 1)
        release = threading.Event()
        try:
            with caplog.at_level(
                logging.WARNING, logger="airflow_provider_rmq.utils.executor"
            ):
                pool.submit(release.wait, 5)
                assert caplog.records == []
                pool.submit(release.wait, 5)

            assert any("saturated" in record.getMessage() for record in caplog.records)
        finally:
            release.set()
            pool.shutdown()

    def test_in_flight_drops_back_when_the_call_returns(self):
        pool = BoundedExecutor("test", 2)
        try:
            future = pool.submit(lambda: "done")
            assert future.result(timeout=5) == "done"
            # the release callback runs on the worker thread, so give it a moment
            deadline = threading.Event()
            deadline.wait(0.1)
            assert pool.in_flight == 0
        finally:
            pool.shutdown()

    def test_submitted_future_survives_a_caller_that_gave_up(self):
        """The raw future is a concurrent one on purpose: a cycle that timed out can
        still ask on a later cycle whether the call finally returned."""
        pool = BoundedExecutor("test", 1)
        release = threading.Event()
        future = pool.submit(release.wait, 5)
        try:
            assert future.done() is False
            release.set()
            future.result(timeout=5)
            assert future.done() is True
        finally:
            release.set()
            pool.shutdown()
