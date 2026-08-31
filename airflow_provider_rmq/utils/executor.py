from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any

log = logging.getLogger(__name__)

#: Calls a saturated pool keeps waiting for a worker, per worker it has.
_QUEUED_PER_WORKER = 2


class PoolSaturated(RuntimeError):
    """Raised by :meth:`BoundedExecutor.submit` when the pool has no room left.

    The caller decides what a refused call means: a status write gives up and leaves the
    stored value for the next attempt, a delivery goes back on the queue and is tried
    again after a pause.
    """


class BoundedExecutor:
    """A named thread pool that refuses work instead of queueing it without limit.

    The pool is created outside any event loop and never bound to one, so the same
    pool serves every event loop the watcher thread creates. A call already running
    in a worker keeps that worker until it returns — ``Future.cancel()`` refuses a
    running task and CPython cannot interrupt a thread — so a blocked call (a hung
    database connection waiting out the OS TCP timeout) costs a worker for as long
    as it lasts. That makes saturation a real failure mode, which is why it is
    logged rather than left to look like ordinary slowness.

    Behind the workers the pool holds at most ``max_workers * _QUEUED_PER_WORKER``
    waiting calls, and :class:`PoolSaturated` is raised once that is full. The bound is
    what keeps a database outage from turning into a memory leak: a caller that stopped
    waiting cancels its future, but the queued work item — and the message payload it
    closes over — stays in the pool's queue until a worker is free to pick it up, so an
    unbounded queue grows for as long as the outage lasts.

    Interpreter exit still waits for every worker: ``concurrent.futures.thread``
    registers an at-exit hook that joins each thread with no timeout, and nothing
    calls :meth:`shutdown` in the normal course of things. A call blocked on the
    database therefore holds up process exit for as long as the operating system
    keeps its socket open — the loop thread is bought back by the timeouts around
    each call, the interpreter's own teardown is not.

    :param name: Pool name, used as the worker thread prefix and in log messages.
    :param max_workers: Upper bound on concurrently running calls.
    """

    def __init__(self, name: str, max_workers: int) -> None:
        self.name = name
        self.max_workers = max_workers
        #: Calls the pool accepts in total — running plus waiting for a worker.
        self.capacity = max_workers * (1 + _QUEUED_PER_WORKER)
        self._pool = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix=name)
        self._lock = threading.Lock()
        self._in_flight = 0

    @property
    def in_flight(self) -> int:
        """Calls the pool is holding — running in a worker or waiting for one."""
        with self._lock:
            return self._in_flight

    def submit(self, fn: Callable[..., Any], *args: Any) -> Future:
        """Hand ``fn(*args)`` to the pool and return the future of that call.

        The future is a :class:`concurrent.futures.Future`, not an asyncio one: it
        outlives the event loop, so a caller that gave up waiting can still ask on a
        later cycle whether the call finally returned. Cancelling it before a worker
        picks the call up means the call is skipped when its turn comes.

        :raises PoolSaturated: When :attr:`capacity` calls are already in the pool.

        The pool runs a wrapper rather than ``fn`` itself, so the count of held calls
        drops when the pool is done with the call — the moment a worker takes the item
        off the queue and either runs it or finds it cancelled. Counting the caller's
        own future instead would drop the count the instant a caller gave up, while the
        queued item and its arguments are still sitting in the pool.

        The counter is taken under a lock: :meth:`submit` runs on the loop thread and
        the release runs on whichever worker finished, so an unguarded increment and
        decrement would drift apart and the saturation warning with them.
        """
        with self._lock:
            in_flight = self._in_flight
            if in_flight >= self.capacity:
                raise PoolSaturated(
                    f"RMQ Watcher thread pool {self.name!r} is full: {in_flight} call(s) "
                    f"held on {self.max_workers} worker(s), no room for "
                    f"{getattr(fn, '__name__', fn)!r}"
                )
            self._in_flight = in_flight + 1
        if in_flight >= self.max_workers:
            log.warning(
                "RMQ Watcher thread pool %r is saturated: %s/%s workers busy, %r queued "
                "behind them",
                self.name, in_flight, self.max_workers,
                getattr(fn, "__name__", fn),
            )

        future: Future = Future()

        def call() -> None:
            try:
                if not future.set_running_or_notify_cancel():
                    return
                try:
                    future.set_result(fn(*args))
                except BaseException as exc:  # noqa: BLE001 - handed to the caller
                    future.set_exception(exc)
            finally:
                self._release()

        try:
            self._pool.submit(call)
        except BaseException:
            self._release()
            raise
        return future

    async def run(self, fn: Callable[..., Any], *args: Any) -> Any:
        """Await ``fn(*args)`` running in the pool."""
        return await asyncio.wrap_future(self.submit(fn, *args))

    def shutdown(self) -> None:
        """Release the pool without waiting for calls that are still running."""
        self._pool.shutdown(wait=False)

    def _release(self) -> None:
        with self._lock:
            self._in_flight -= 1
