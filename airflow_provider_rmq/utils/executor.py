from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

log = logging.getLogger(__name__)


class BoundedExecutor:
    """A named thread pool that reports saturation instead of queueing silently.

    The pool is created outside any event loop and never bound to one, so the same
    pool serves every event loop the watcher thread creates. A call already running
    in a worker keeps that worker until it returns — ``Future.cancel()`` refuses a
    running task and CPython cannot interrupt a thread — so a blocked call (a hung
    database connection waiting out the OS TCP timeout) costs a worker for as long
    as it lasts. That makes saturation a real failure mode, which is why it is
    logged rather than left to look like ordinary slowness.

    :param name: Pool name, used as the worker thread prefix and in log messages.
    :param max_workers: Upper bound on concurrently running calls.
    """

    def __init__(self, name: str, max_workers: int) -> None:
        self.name = name
        self.max_workers = max_workers
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=name)
        self._in_flight = 0

    @property
    def in_flight(self) -> int:
        """Number of calls handed to the pool that have not returned yet."""
        return self._in_flight

    def submit(self, fn: Callable[..., Any], *args: Any) -> Future:
        """Hand ``fn(*args)`` to the pool and return the raw future.

        The future is a :class:`concurrent.futures.Future`, not an asyncio one: it
        outlives the event loop, so a caller that gave up waiting can still ask on a
        later cycle whether the call finally returned.
        """
        if self._in_flight >= self.max_workers:
            log.warning(
                "RMQ Watcher thread pool %r is saturated: %s/%s workers busy, %r queued "
                "behind them",
                self.name, self._in_flight, self.max_workers,
                getattr(fn, "__name__", fn),
            )
        self._in_flight += 1
        future = self._pool.submit(fn, *args)
        future.add_done_callback(self._release)
        return future

    async def run(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Await ``fn(*args)`` running in the pool."""
        return await asyncio.wrap_future(self.submit(fn, *args))

    def shutdown(self) -> None:
        """Release the pool without waiting for calls that are still running."""
        self._pool.shutdown(wait=False)

    def _release(self, _future: Future) -> None:
        self._in_flight -= 1
