from __future__ import annotations

import logging
import threading
import time
from contextlib import suppress

import pytest

from airflow_provider_rmq.utils.executor import BoundedExecutor, PoolSaturated


def _wait_until(condition, timeout: float = 5.0) -> None:
    """Poll ``condition`` until it holds, failing the test if it never does."""
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition never became true"
        time.sleep(0.005)


@pytest.fixture
def pool(request):
    """Bounded pool, released when the test ends.

    The worker count comes from indirect parametrization and defaults to two.
    """
    pool = BoundedExecutor("test", getattr(request, "param", 2))
    yield pool
    pool.shutdown()


class TestBoundedExecutor:
    @pytest.mark.asyncio
    async def test_run_returns_the_call_result(self, pool):
        assert await pool.run(lambda value: value * 2, 21) == 42

    @pytest.mark.asyncio
    async def test_run_propagates_the_call_error(self, pool):
        def boom():
            raise RuntimeError("database is gone")

        with pytest.raises(RuntimeError, match="database is gone"):
            await pool.run(boom)

    @pytest.mark.parametrize("pool", [1], indirect=True)
    def test_saturation_is_logged_instead_of_queueing_silently(self, pool, caplog):
        """A stuck call keeps its worker — cancellation cannot free it — so a pool
        with every worker busy must say so rather than look merely slow."""
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

    def test_in_flight_drops_back_when_the_call_returns(self, pool):
        future = pool.submit(lambda: "done")
        assert future.result(timeout=5) == "done"
        # The release callback runs on the worker thread, a moment after the
        # result is available, so poll for it rather than guessing a delay.
        _wait_until(lambda: pool.in_flight == 0)

    @pytest.mark.parametrize("pool", [1], indirect=True)
    def test_submitted_future_survives_a_caller_that_gave_up(self, pool):
        """The raw future is a concurrent one on purpose: a cycle that timed out can
        still ask on a later cycle whether the call finally returned."""
        release = threading.Event()
        future = pool.submit(release.wait, 5)
        try:
            assert future.done() is False
            release.set()
            future.result(timeout=5)
            assert future.done() is True
        finally:
            release.set()

    @pytest.mark.parametrize("pool", [8], indirect=True)
    def test_the_in_flight_counter_survives_concurrent_submits(self, pool):
        """``submit`` runs on the loop thread and the release on whichever worker
        finished, so the counter has to be taken under a lock — an unguarded
        read-modify-write drifts and takes the saturation warning with it."""
        submitters = 8
        per_thread = 200
        start = threading.Barrier(submitters)

        def submit_many():
            start.wait(timeout=5)
            for _ in range(per_thread):
                with suppress(PoolSaturated):
                    pool.submit(lambda: None)

        threads = [threading.Thread(target=submit_many) for _ in range(submitters)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
        _wait_until(lambda: pool.in_flight == 0)


class TestPoolCapacity:
    """The pool holds a bounded number of calls, running and waiting together.

    A queued call keeps the arguments it was handed — a message body among them — and
    a caller that gave up cannot take it back out of the queue. Without a bound, every
    retry made while the database is unreachable adds one more.
    """

    @pytest.mark.parametrize("pool", [1], indirect=True)
    def test_a_full_pool_refuses_the_call(self, pool):
        release = threading.Event()
        try:
            accepted = 0
            refused = 0
            for _ in range(50):
                try:
                    pool.submit(release.wait, 5)
                except PoolSaturated:
                    refused += 1
                else:
                    accepted += 1

            assert accepted == pool.capacity
            assert refused == 50 - pool.capacity
            assert pool.in_flight == pool.capacity
        finally:
            release.set()

    @pytest.mark.parametrize("pool", [1], indirect=True)
    def test_a_call_the_caller_gave_up_on_holds_its_place_until_the_pool_reaches_it(self, pool):
        """Cancelling the future does not lift the call out of the queue, so the place
        it takes is given back when a worker gets to it — and the call itself is then
        skipped."""
        release = threading.Event()
        ran = []
        try:
            running = pool.submit(release.wait, 5)
            queued = pool.submit(ran.append, "payload")

            assert queued.cancel() is True
            assert pool.in_flight == 2, "the cancelled call is still held by the pool"

            release.set()
            running.result(timeout=5)
            _wait_until(lambda: pool.in_flight == 0)
            assert ran == [], "a cancelled call must not run"
        finally:
            release.set()

    @pytest.mark.parametrize("pool", [1], indirect=True)
    def test_a_refused_call_reaches_the_caller_as_an_error(self, pool):
        release = threading.Event()
        try:
            for _ in range(pool.capacity):
                pool.submit(release.wait, 5)

            with pytest.raises(PoolSaturated, match="is full"):
                pool.submit(release.wait, 5)
        finally:
            release.set()
