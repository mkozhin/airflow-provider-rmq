from __future__ import annotations

import logging
import threading
import time

import pytest

from airflow_provider_rmq.utils.executor import BoundedExecutor


def _wait_until(condition, timeout: float = 5.0) -> None:
    """Poll ``condition`` until it holds, failing the test if it never does."""
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition never became true"
        time.sleep(0.005)


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
            # The release callback runs on the worker thread, a moment after the
            # result is available, so poll for it rather than guessing a delay.
            _wait_until(lambda: pool.in_flight == 0)
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

    def test_the_in_flight_counter_survives_concurrent_submits(self):
        """``submit`` runs on the loop thread and the release on whichever worker
        finished, so the counter has to be taken under a lock — an unguarded
        read-modify-write drifts and takes the saturation warning with it."""
        pool = BoundedExecutor("test", 8)
        submitters = 8
        per_thread = 200
        start = threading.Barrier(submitters)

        def submit_many():
            start.wait(timeout=5)
            for _ in range(per_thread):
                pool.submit(lambda: None)

        try:
            threads = [threading.Thread(target=submit_many) for _ in range(submitters)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=30)
            _wait_until(lambda: pool.in_flight == 0)
        finally:
            pool.shutdown()
