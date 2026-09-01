from __future__ import annotations

import asyncio
import base64
import contextlib
import inspect
import logging
import os
import re
import threading
import time
from contextlib import ExitStack, suppress
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import uuid4

import aio_pika
import aio_pika.exceptions
import aiormq
import httpx
import pytest

from airflow_provider_rmq.utils.amqp import (
    DEFAULT_RPC_TIMEOUT,
    AmqpTimeouts,
    call_with_timeout,
)
from airflow_provider_rmq.utils.executor import BoundedExecutor

from airflow_provider_rmq.watcher.consumer import (
    RMQConsumerManager,
    _ActiveSub,
    _Backoff,
    _ConnLiveness,
    _ConsumerState,
    _CYCLES_BEFORE_REDROP,
    _DagNotReady,
    _NOT_READY_LIMIT,
    _StatusWriter,
    _FireSub,
    _RECONNECT_DELAY,
    _STUCK_CYCLES_BEFORE_DROP,
    _ROLE_CONSUME,
    _ROLE_PUBLISH,
    _answer_misses,
    _attach_nonce,
    _attached,
    KEEP,
    _cancelled_by_broker,
    _conn_status_lock,
    _consumer_tag,
    _raised_while_cancelling,
    _build_run_id,
    _safe_run_id,
    _sync_trigger,
    _wait_cancelled,
    _write_conn_error,
    _write_conn_status_rows,
    _status_writer,
    _status_writers,
    _OUTCOME_DUPLICATE,
    _OUTCOME_SKIPPED,
    _OUTCOME_TRIGGERED,
    _RUN_ID_MAX_LEN,
    _TRIGGER_BACKOFF_MAX,
    _TRIGGER_BACKOFF_START,
    _ensure_fire_infrastructure,
    _ensure_pending_queue,
    _ensure_exchange_infrastructure,
    _ensure_sub_queue,
    _sync_bindings,
    _FIRE_EXCHANGE,
    _FIRE_QUEUE,
    _PENDING_QUEUE_PREFIX,
    _PUBLISH_BACKOFF_START,
    _SUB_ERROR,
    _SUB_LISTENING,
    _SUB_QUEUE_PREFIX,
    _EXCHANGE_TTL_MS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

#: Alphabet Airflow accepts for DagRun.run_id (scheduler.allowed_run_id_pattern).
_RUN_ID_UNSAFE = re.compile(r"[^A-Za-z0-9_.~:+-]")

def _make_fake_message(
    body: bytes = b"hello", headers: dict | None = None, message_id: str | None = None
):
    msg = MagicMock()
    msg.body = body
    msg.headers = headers or {}
    msg.routing_key = "rk"
    msg.exchange = ""
    msg.message_id = message_id
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    return msg


class _QueueIterCtx:
    """Async context manager that yields ``messages``.

    After the last one it blocks until cancelled, the way a consumer waits for the next
    delivery — unless ``ends`` is set, which is what the broker cancelling our consumer
    looks like from inside the loop.
    """

    def __init__(self, messages: list, ends: bool = False):
        self._messages = messages
        self._ends = ends
        self._pos = 0
        self.consumed = False
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def consume(self):
        """Register the consumer, the way aio_pika's iterator does on demand."""
        self.consumed = True

    async def close(self):
        """Cancel the consumer, the way the manager does when it leaves the loop."""
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._pos < len(self._messages):
            msg = self._messages[self._pos]
            self._pos += 1
            return msg
        if self._ends:
            raise StopAsyncIteration
        await asyncio.Future()  # block until cancelled → raises CancelledError


class _QueueIterFailingCancel:
    """Iterator whose consumer cancellation fails, the way aio_pika's does.

    ``QueueIterator.__anext__`` cancels the consumer from inside its own handling of the
    CancelledError, waits for that cancel without a bound and catches only a timeout of
    it. A broker that rejects the pending ``basic.cancel`` — which every connection torn
    down mid-call does — therefore leaves ``__anext__`` with its own error in place of
    the cancellation, carrying the CancelledError as context.

    :param through_task_wrapper: Hand that failure over the way ``aiormq``'s RPC wrapper
        does — ``raise self._exception from e``, with ``e`` the ``CancelledError`` the
        RPC task was stopped with — so the error carries a cancellation in ``__cause__``
        as well as in ``__context__``.
    """

    def __init__(self, error: BaseException, through_task_wrapper: bool = False):
        self._error = error
        self._through_task_wrapper = through_task_wrapper
        self.consumed = False
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    async def consume(self):
        self.consumed = True

    async def close(self):
        self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            if not self._through_task_wrapper:
                raise self._error
            try:
                raise asyncio.CancelledError()  # the RPC task the teardown stopped
            except asyncio.CancelledError as rpc_stopped:
                raise self._error from rpc_stopped


def _queue_failing_cancel(error: BaseException):
    """Queue whose iterator fails the cancellation of its consumer with ``error``."""
    queue = MagicMock()
    queue.iterator.return_value = _QueueIterFailingCancel(error)
    return queue


async def _wait_for(condition, timeout: float = 2.0) -> None:
    """Wait until ``condition`` holds, failing the test when it does not in ``timeout``."""
    deadline = time.monotonic() + timeout
    while not condition():
        assert time.monotonic() < deadline, "condition never held"
        await asyncio.sleep(0.01)


def _make_push_queue(messages: list = ()):
    queue = MagicMock()
    queue.iterator.return_value = _QueueIterCtx(list(messages))
    return queue


def _ending_queue():
    """Queue whose iterator ends at once, as it does when the broker cancels us."""
    queue = MagicMock()
    queue.iterator.return_value = _QueueIterCtx([], ends=True)
    return queue


class _FakeExecutor:
    """Stand-in for the manager's thread pool.

    ``run`` awaits the handler the test supplies instead of handing the call to a
    worker thread, so the test sees the function and arguments the manager offloaded.
    """

    def __init__(self, handler):
        self._handler = handler

    async def run(self, fn, *args):
        return await self._handler(fn, *args)


def _offloading_executor() -> _FakeExecutor:
    """Executor stand-in for whole-scenario tests.

    A trigger reports a started DAG run, a status write goes nowhere, and everything
    else — reading the Airflow connection, for one — runs as the manager offloaded it.
    """

    async def handler(fn, *args):
        if fn is _sync_trigger:
            return _OUTCOME_TRIGGERED
        if isinstance(getattr(fn, "__self__", None), _StatusWriter):
            return None   # the subscription's status writer; nothing to store here
        return fn(*args)

    return _FakeExecutor(handler)



_CONSUMER_MODULE = "airflow_provider_rmq.watcher.consumer"


@contextlib.contextmanager
def _record_consumer_sleeps(on_delay, block: bool = False):
    """Collect the pauses ``consumer.py`` takes, ignoring every other module's sleep.

    ``consumer.asyncio`` is the asyncio module itself, so patching its ``sleep``
    patches it process-wide; the caller's frame tells whose pause this is.

    :param block: Leave ``consumer.py`` inside its pause instead of letting it out, so a
        test can reach the loop while it is waiting.
    """
    real_sleep = asyncio.sleep

    async def fake_sleep(delay, *args, **kwargs):
        caller = inspect.currentframe().f_back
        if caller is not None and caller.f_globals.get("__name__") == _CONSUMER_MODULE:
            on_delay(delay)
            if block:
                await asyncio.Future()
        return await real_sleep(0)

    with patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new=fake_sleep):
        yield


def _warmup_lines(log) -> list:
    """``(level, rendered text)`` of every warmup line a delivery handler wrote."""
    return [(c.args[0], c.args[1] % c.args[2:]) for c in log.log.call_args_list]


def _pooled_connections(manager) -> list:
    """Every connection the manager holds, whatever conn_id or role it is pooled under."""
    return [
        conn for state in manager._conns.values() for conn in state.connections.values()
    ]


def _test_pool(name: str = "test-pool") -> BoundedExecutor:
    """Real bounded pool for a test. The manager takes its pools from its caller."""
    return BoundedExecutor(name, 4)


def _make_manager(cycle_executor=None) -> RMQConsumerManager:
    """Build a manager the way the listener does, with a pool for each role."""
    return RMQConsumerManager(
        executor=_test_pool("test-consumer"),
        cycle_executor=(
            cycle_executor if cycle_executor is not None else _test_pool("test-cycle")
        ),
    )


@pytest.fixture(autouse=True)
def fresh_status_writers():
    """Start each test with no writer holding anything for any subscription.

    Writers live as long as the process, because the row each one owns outlives every
    object that writes to it (see :class:`_StatusWriter`), so one test would otherwise
    hand the next one a write of its own that is still running.
    """
    _status_writers.clear()
    yield
    _status_writers.clear()


def _mock_session():
    """Return a MagicMock usable as `with WatcherSession() as session:`."""
    ctx = MagicMock()
    session = MagicMock()
    ctx.__enter__ = MagicMock(return_value=session)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx, session


def _patch_watcher_session():
    """Context manager that patches WatcherSession to a no-op."""
    ctx, _ = _mock_session()
    return patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx)


def _sub(
    id: int = 1,
    dag_id: str = "test_dag",
    queue_name: str = "q",
    conn_id: str = "rmq_default",
    filter_data: dict | None = None,
    cooldown: int = 0,
) -> dict:
    return {
        "id": id,
        "dag_id": dag_id,
        "queue_name": queue_name,
        "conn_id": conn_id,
        "filter_data": filter_data or {},
        "cooldown": cooldown,
    }


def _exchange_sub(
    id: int = 1,
    dag_id: str = "test_dag",
    exchange: str = "jetstat.airflow",
    routing_keys: list[str] | None = None,
    conn_id: str = "rmq_default",
    filter_data: dict | None = None,
    cooldown: int = 0,
) -> dict:
    return {
        "id": id,
        "dag_id": dag_id,
        "queue_name": f"{_SUB_QUEUE_PREFIX}{dag_id}",
        "conn_id": conn_id,
        "filter_data": filter_data or {},
        "cooldown": cooldown,
        "exchange": exchange,
        "routing_keys": routing_keys if routing_keys is not None else ["a.succeeded"],
    }


# ---------------------------------------------------------------------------
# Tests for the timeout budgets
# ---------------------------------------------------------------------------

class TestStopBudgets:
    def test_cancel_wait_leaves_room_for_the_closing_that_follows_it(self):
        """The stop budget covers the wait, the status pass and the closing.

        The closing costs the two roles of one conn_id — a broker that has gone silent
        answers no ``close`` at all and each one runs to its full timeout — because
        ``stop()`` closes every connection at once. Summing it per connection instead
        would put the arithmetic over the caller's bound at three pooled conn_ids.
        """
        from airflow_provider_rmq.watcher import listener
        from airflow_provider_rmq.watcher.consumer import (
            _CLOSE_TIMEOUT,
            _STOP_CANCEL_TIMEOUT,
            _STOP_STATUS_TIMEOUT,
        )

        assert (
            _STOP_CANCEL_TIMEOUT + _STOP_STATUS_TIMEOUT + 2 * _CLOSE_TIMEOUT
            < listener._STOP_TIMEOUT
        )

    @pytest.mark.asyncio
    async def test_connections_that_answer_no_close_do_not_cost_one_timeout_each(self):
        """A stop is what a fresh event loop waits on, and the loop that replaces this
        one opens its own connections: everything this manager holds has to be reached
        inside the caller's bound, not just what fits before it runs out."""
        manager = _make_manager()
        blocked = []
        for index in range(8):
            connection = AsyncMock()
            connection.is_closed = False
            connection.close = _hanging_call
            manager._conn(f"rmq_{index}").connections[_ROLE_CONSUME] = connection
            blocked.append(connection)
        client = MagicMock()
        client.aclose = AsyncMock(side_effect=_never_returns)
        manager._http_client = client

        with patch("airflow_provider_rmq.watcher.consumer._CLOSE_TIMEOUT", 0.1):
            started = time.monotonic()
            await manager.stop()
            elapsed = time.monotonic() - started

        assert elapsed < 0.4, (
            f"the closes ran one after another: {elapsed:.2f}s for {len(blocked)} "
            f"connections that answer nothing"
        )
        client.aclose.assert_awaited_once()
        assert manager._http_client is None


# ---------------------------------------------------------------------------
# Tests for _sync_trigger
# ---------------------------------------------------------------------------

def _patch_sync_trigger_deps(dag_model=None):
    """Returns patch stack for _sync_trigger: WatcherSession + DagModel + trigger_dag."""
    ctx, session = _mock_session()
    session.query.return_value.filter_by.return_value.first.return_value = dag_model

    ws_patch = patch(
        "airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx
    )
    td_patch = patch("airflow.api.common.trigger_dag.trigger_dag")
    return ws_patch, td_patch


def _dag_run_already_exists(run_id: str = "run_id"):
    from airflow.exceptions import DagRunAlreadyExists

    return DagRunAlreadyExists(
        MagicMock(), datetime(2026, 8, 27, 12, 0, 0, tzinfo=timezone.utc), run_id
    )


class TestSyncTrigger:
    def test_trigger_dag_uses_watcher_session(self):
        fake_dag = MagicMock()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=fake_dag)
        with ws_patch as mock_ws, td_patch:
            outcome = _sync_trigger("my_dag", {}, "run_id_1")
        mock_ws.assert_called()
        assert outcome == _OUTCOME_TRIGGERED

    def test_trigger_dag_skips_inactive_dag(self):
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=None)
        with ws_patch, td_patch as mock_td:
            outcome = _sync_trigger("missing_dag", {}, "run_id")
        mock_td.assert_not_called()
        assert outcome == _OUTCOME_SKIPPED

    def test_trigger_dag_skips_paused_dag(self):
        # filter_by includes is_paused=False; paused DAGs return None from .first()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=None)
        with ws_patch, td_patch as mock_td:
            outcome = _sync_trigger("paused_dag", {}, "run_id")
        mock_td.assert_not_called()
        assert outcome == _OUTCOME_SKIPPED

    def test_trigger_dag_handles_integrity_error(self):
        from sqlalchemy.exc import IntegrityError

        fake_dag = MagicMock()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=fake_dag)
        with ws_patch, td_patch as mock_td:
            mock_td.side_effect = IntegrityError("dup", {}, None)
            outcome = _sync_trigger("dag", {}, "run_id")
        assert outcome == _OUTCOME_DUPLICATE

    def test_redelivery_is_reported_as_duplicate(self):
        """Airflow raises DagRunAlreadyExists before the INSERT, not an IntegrityError."""
        fake_dag = MagicMock()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=fake_dag)
        with ws_patch, td_patch as mock_td:
            mock_td.side_effect = _dag_run_already_exists("rmq__q__mid")
            outcome = _sync_trigger("dag", {}, "rmq__q__mid")
        assert outcome == _OUTCOME_DUPLICATE

    def test_other_exceptions_propagate(self):
        """MultipleResultsFound and friends are trigger failures, not duplicates."""
        from sqlalchemy.orm.exc import MultipleResultsFound

        fake_dag = MagicMock()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=fake_dag)
        with ws_patch, td_patch as mock_td:
            mock_td.side_effect = MultipleResultsFound("two rows")
            with pytest.raises(MultipleResultsFound):
                _sync_trigger("dag", {}, "run_id")

    @pytest.mark.parametrize(
        "text", ["Dag id dag not found", "Dag id dag not found in DagModel"]
    )
    def test_a_dag_with_no_serialized_version_is_told_apart(self, text):
        """Airflow raises DagNotFound while its DAG processor has yet to fill
        serialized_dag, in either of the two wordings it has for it. The delivery
        handler acts on that differently from a trigger that failed, so it arrives
        as an exception of its own."""
        from airflow.exceptions import DagNotFound

        fake_dag = MagicMock()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=fake_dag)
        with ws_patch, td_patch as mock_td:
            mock_td.side_effect = DagNotFound(text)
            with pytest.raises(_DagNotReady) as raised:
                _sync_trigger("dag", {}, "run_id")
        assert str(raised.value) == text

    def test_microseconds_are_kept(self):
        """find_duplicate() also matches on execution_date: truncating it to whole
        seconds makes two distinct messages of the same second look like a redelivery."""
        fake_dag = MagicMock()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=fake_dag)
        with ws_patch, td_patch as mock_td:
            _sync_trigger("dag", {}, "run_id")
        assert mock_td.call_args.kwargs["replace_microseconds"] is False


# ---------------------------------------------------------------------------
# Tests for run_id construction
# ---------------------------------------------------------------------------

class TestBuildRunId:
    def test_message_id_makes_the_run_id_deterministic(self):
        first = _build_run_id("orders", "msg-42")
        second = _build_run_id("orders", "msg-42")
        assert first == second == "rmq__orders__msg-42"

    def test_without_message_id_every_delivery_gets_its_own_run_id(self):
        first = _build_run_id("orders")
        second = _build_run_id("orders")
        assert first.startswith("rmq__orders__")
        assert first != second

    def test_unsafe_characters_are_replaced_and_stay_distinguishable(self):
        first = _build_run_id("my queue", "a b")
        second = _build_run_id("my queue", "a/b")
        assert _RUN_ID_UNSAFE.search(first) is None
        assert _RUN_ID_UNSAFE.search(second) is None
        assert first != second

    def test_long_parts_fit_the_column_and_stay_distinguishable(self):
        queue = "q" * 300
        first = _build_run_id(queue, "x" * 200 + "-1")
        second = _build_run_id(queue, "x" * 200 + "-2")
        assert len(first) <= _RUN_ID_MAX_LEN
        assert len(second) <= _RUN_ID_MAX_LEN
        assert _RUN_ID_UNSAFE.search(first) is None
        assert first != second

    def test_the_digest_of_a_shortened_run_id_is_wide_enough(self):
        """A queue name long enough to fill the prefix leaves nothing of the message id
        in it, so the digest is the only thing telling two deliveries apart. Eight hex
        characters are 32 bits, whose birthday threshold is about 77 000 runs — and two
        deliveries sharing a run id collapse into one DAG run that is acknowledged as a
        duplicate, which loses the second event without a word."""
        run_id = _build_run_id("q" * 300, "x" * 200)

        assert len(run_id) == _RUN_ID_MAX_LEN
        digest = run_id.rsplit("_", 1)[1]
        assert len(digest) >= 32, "the digest carries at least 128 bits"
        int(digest, 16)  # it is a digest, not the tail of the queue name

    def test_cooldown_run_id_of_a_long_dag_id_fits_the_column(self):
        dag_id = "very_long_dag_" + "n" * 240
        run_id = _safe_run_id(f"rmq_cooldown__{dag_id}__{uuid4()}")
        assert len(run_id) <= _RUN_ID_MAX_LEN
        assert _RUN_ID_UNSAFE.search(run_id) is None

    def test_short_clean_value_is_left_alone(self):
        assert _safe_run_id("rmq_cooldown__dag__uuid-1") == "rmq_cooldown__dag__uuid-1"


# ---------------------------------------------------------------------------
# Tests for _ConsumerState (in-memory status guard)
# ---------------------------------------------------------------------------

class TestConsumerState:
    @pytest.mark.asyncio
    async def test_state_guard_skips_duplicate_status_write(self):
        ctx, _ = _mock_session()
        with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status") as mock_set:
            state = _ConsumerState(sub_id=1, executor=_test_pool())
            await state.write("listening")
            await state.write("listening")  # duplicate — should be skipped
            assert mock_set.call_count == 1

    @pytest.mark.asyncio
    async def test_state_guard_writes_on_status_change(self):
        ctx, _ = _mock_session()
        with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status") as mock_set:
            state = _ConsumerState(sub_id=1, executor=_test_pool())
            await state.write("connecting")
            await state.write("listening")
            await state.write("error")
            assert mock_set.call_count == 3

    @pytest.mark.asyncio
    async def test_last_error_cleared_on_successful_connect(self):
        writes = []
        ctx, _ = _mock_session()

        def capture(session, sub_id, status, last_error=None):
            writes.append((status, last_error))

        with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status", side_effect=capture):
            state = _ConsumerState(sub_id=1, executor=_test_pool())
            await state.write("error", last_error="broker refused the connection")
            await state.write("connecting")
            await state.write("listening", last_error=None)

        assert writes == [
            ("error", "broker refused the connection"),
            ("connecting", None),
            ("listening", None),
        ], "leaving error must clear the reason in the row"

    @pytest.mark.asyncio
    async def test_status_write_runs_in_the_consumer_pool(self):
        """The write blocks on the database, so it must not run on the loop thread."""
        pool = BoundedExecutor("test-status", 2)
        threads = []
        ctx, _ = _mock_session()

        def capture(session, sub_id, status, last_error=None):
            threads.append(threading.current_thread())

        try:
            with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=capture):
                state = _ConsumerState(sub_id=1, executor=pool)
                await state.write("listening")
        finally:
            pool.shutdown()

        assert threads and threads[0] is not threading.current_thread()

    @pytest.mark.asyncio
    async def test_hanging_status_write_does_not_stall_the_consumer(self):
        """A database that never answers must cost the write, not the subscription."""
        release = threading.Event()
        pool = BoundedExecutor("test-status-hang", 1)
        ctx, _ = _mock_session()

        def hang(session, sub_id, status, last_error=None):
            release.wait(5)

        try:
            with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=hang), \
                 patch("airflow_provider_rmq.watcher.consumer._DB_TIMEOUT", 0.05), \
                 patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
                state = _ConsumerState(sub_id=1, executor=pool)
                await state.write("listening")
        finally:
            release.set()
            pool.shutdown()

        assert mock_log.warning.called
        # The manager's own view of the task moves even when the write does not land:
        # a subscription dropped out of the liveness gate by a failed write would never
        # be verified again.
        assert state.status == "listening"
        # Nothing reached the row, so the next call tries the write again.
        assert _status_writer(1).stored is None


# ---------------------------------------------------------------------------
# Tests for RMQConsumerManager
# ---------------------------------------------------------------------------

@pytest.fixture
async def manager():
    """Manager with both pools, whose Management API client is closed on teardown.

    Tests assign ``manager._http_client`` themselves; closing it here rather than as
    the last statement of each test means an assertion that fails earlier does not
    leak the client.
    """
    mgr = _make_manager()
    try:
        yield mgr
    finally:
        client = mgr._http_client
        if client is not None:
            await client.aclose()


async def _run_then_cancel(coro, timeout: float = 1.0):
    """Run a coroutine as a task, then cancel it, then await completion."""
    task = asyncio.create_task(coro)
    try:
        await asyncio.wait_for(asyncio.shield(task), timeout=timeout)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    return task


class TestReconcile:
    @pytest.mark.asyncio
    async def test_reconcile_starts_new_consumer(self, manager):
        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_sub(id=1)])
            assert 1 in manager._active
            assert not manager._active[1].task.done()
            manager._active[1].task.cancel()
            await asyncio.gather(*[e.task for e in manager._active.values()], return_exceptions=True)

    @pytest.mark.asyncio
    async def test_reconcile_cancels_removed_consumer(self, manager):
        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_sub(id=1)])
            task = manager._active[1].task
            assert not task.done()

            await manager.reconcile([])  # remove sub 1
            assert 1 not in manager._active
            assert task.done()

    @pytest.mark.asyncio
    async def test_stop_cancels_all_tasks(self, manager):
        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_sub(id=1), _sub(id=2)])
            tasks = [e.task for e in manager._active.values()]
            assert all(not t.done() for t in tasks)

        await manager.stop()
        assert all(t.done() for t in tasks)


class TestConsumeSubscription:
    @pytest.mark.asyncio
    async def test_matching_message_triggers_dag(self, manager):
        msg = _make_fake_message(b"order payload")
        queue = _make_push_queue([msg])
        connection = _make_live_connection(queue=queue)

        triggered = asyncio.Event()

        async def mock_trigger(dag_id, queue_name, sub_id, message):
            triggered.set()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag", side_effect=mock_trigger), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(filter_data={}))
            )
            await asyncio.wait_for(triggered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert triggered.is_set()

    @pytest.mark.asyncio
    async def test_non_matching_message_nacked(self, manager):
        msg = _make_fake_message(b"payment", headers={"type": "payment"})
        queue = _make_push_queue([msg])
        connection = _make_live_connection(queue=queue)

        nacked = asyncio.Event()
        original_nack = msg.nack

        async def capture_nack(*args, **kwargs):
            nacked.set()

        msg.nack = capture_nack

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag") as mock_td, \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(
                manager._consume_subscription(
                    _sub(filter_data={"filter_headers": {"type": "order"}})
                )
            )
            await asyncio.wait_for(nacked.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        mock_td.assert_not_called()

    @pytest.mark.asyncio
    async def test_multiple_messages_only_matching_triggers(self, manager):
        """3 messages, only the one with matching header triggers DAG."""
        msg1 = _make_fake_message(b"p1", headers={"type": "payment"})
        msg2 = _make_fake_message(b"o1", headers={"type": "order"})
        msg3 = _make_fake_message(b"p2", headers={"type": "payment"})
        queue = _make_push_queue([msg1, msg2, msg3])
        connection = _make_live_connection(queue=queue)

        trigger_count = 0
        triggered_once = asyncio.Event()

        async def mock_trigger(*args, **kwargs):
            nonlocal trigger_count
            trigger_count += 1
            triggered_once.set()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag", side_effect=mock_trigger), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(
                manager._consume_subscription(
                    _sub(filter_data={"filter_headers": {"type": "order"}})
                )
            )
            await asyncio.wait_for(triggered_once.wait(), timeout=2.0)
            # Give remaining messages a tick to process
            await asyncio.sleep(0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert trigger_count == 1

    @pytest.mark.asyncio
    async def test_non_matching_nack_has_sleep(self, manager):
        """Non-matching message: nack is called AND asyncio.sleep(0.1) follows."""
        msg = _make_fake_message(b"x", headers={"type": "other"})
        queue = _make_push_queue([msg])
        connection = _make_live_connection(queue=queue)

        sleep_called = asyncio.Event()

        async def mock_amqp_sleep(delay):
            if delay == 0.1:
                sleep_called.set()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag"), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", side_effect=mock_amqp_sleep):
            task = asyncio.create_task(
                manager._consume_subscription(
                    _sub(filter_data={"filter_headers": {"type": "order"}})
                )
            )
            await asyncio.wait_for(sleep_called.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert sleep_called.is_set()

    @pytest.mark.asyncio
    async def test_missing_queue_fatal_no_retry(self, manager):
        connection = AsyncMock()
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelNotFoundEntity("no such queue")
        )
        connection.channel = AsyncMock(return_value=channel)

        status_writes = []

        async def capture_write(self_arg, status, last_error=None):
            status_writes.append(status)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write", capture_write):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            # Task should exit on its own (fatal error, no retry)
            await asyncio.wait_for(task, timeout=2.0)

        assert "error" in status_writes
        assert task.done()
        assert not task.cancelled()

    @pytest.mark.asyncio
    async def test_connection_error_retries(self, manager):
        call_count = 0
        connected = asyncio.Event()

        async def mock_get_conn(conn_id):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("Connection refused")
            connected.set()
            connection = AsyncMock()
            channel = AsyncMock()
            channel.declare_queue = AsyncMock(return_value=_make_push_queue())
            connection.channel = AsyncMock(return_value=channel)
            return connection

        with patch.object(manager, "_get_or_create_connection", side_effect=mock_get_conn), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            await asyncio.wait_for(connected.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_channel_closed_recovers_with_retry(self, manager):
        call_count = 0
        declared = asyncio.Event()

        async def mock_declare(queue_name, passive):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aio_pika.exceptions.ChannelClosed("channel closed")
            declared.set()
            return _make_push_queue()

        connection = AsyncMock()
        channel = AsyncMock()
        channel.declare_queue = mock_declare
        connection.channel = AsyncMock(return_value=channel)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            await asyncio.wait_for(declared.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_rmq_unavailable_at_start_retries(self, manager):
        call_count = 0
        connected = asyncio.Event()

        async def mock_get_conn(conn_id):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aio_pika.exceptions.AMQPConnectionError("broker down")
            connected.set()
            connection = AsyncMock()
            channel = AsyncMock()
            channel.declare_queue = AsyncMock(return_value=_make_push_queue())
            connection.channel = AsyncMock(return_value=channel)
            return connection

        with patch.object(manager, "_get_or_create_connection", side_effect=mock_get_conn), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            await asyncio.wait_for(connected.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_two_subs_same_conn_id_share_one_connection(self, manager):
        connection = AsyncMock()
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=_make_push_queue())
        connection.channel = AsyncMock(return_value=channel)

        connection.is_closed = False

        conn_info = MagicMock()
        conn_info.extra_dejson = {}
        conn_info.schema = "/"
        conn_info.port = None
        conn_info.login = "guest"
        conn_info.password = "guest"
        conn_info.host = "localhost"

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=connection) as mock_connect, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=_mock_session()[0]):
            sub1 = _sub(id=1, conn_id="same_conn")
            sub2 = _sub(id=2, conn_id="same_conn")
            task1 = asyncio.create_task(manager._consume_subscription(sub1))
            task2 = asyncio.create_task(manager._consume_subscription(sub2))
            # Give both tasks time to reach and complete _get_or_create_connection
            await asyncio.sleep(0.05)
            task1.cancel()
            task2.cancel()
            await asyncio.gather(task1, task2, return_exceptions=True)

        assert mock_connect.call_count == 1

    @pytest.mark.asyncio
    async def test_a_failed_consumer_cancel_still_ends_the_task(self, manager):
        """A cancel the broker rejects leaves aio_pika's own error in place of the
        CancelledError. Read as a transient fault it would send the task around the
        loop again, and it would consume the queue with nothing holding it: the manager
        has already dropped it, so neither reconcile nor stop can reach it, while it
        writes status into the row its replacement owns."""
        queue = _queue_failing_cancel(
            aio_pika.exceptions.AMQPConnectionError("connection reset by peer")
        )
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))
        q_iter = queue.iterator.return_value

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            await _wait_for(lambda: q_iter.consumed)
            task.cancel()
            outcome = await asyncio.wait_for(
                asyncio.gather(task, return_exceptions=True), timeout=2.0
            )

        # The task ends the way a cancelled consumer does — off the ``CancelledError``
        # branch, which returns — rather than going around the loop for a second
        # consumer on the queue it was cancelled off.
        assert outcome[0] is None
        assert task.done()
        assert queue.iterator.call_count == 1

    @pytest.mark.asyncio
    async def test_the_same_error_outside_a_cancellation_still_retries(self, manager):
        """The task stops on the cancelled path alone. The identical error raised while
        nothing is cancelling it is the transient fault it has always been, and the
        subscription is retried."""
        attempts = 0
        retried = asyncio.Event()

        async def failing_declare(queue_name, passive):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise aio_pika.exceptions.AMQPConnectionError("connection reset by peer")
            retried.set()
            return _make_push_queue()

        channel = _make_live_channel()
        channel.declare_queue = failing_declare
        connection = _make_live_connection(channel=channel)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep",
                   new_callable=AsyncMock):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            await asyncio.wait_for(retried.wait(), timeout=2.0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        assert attempts >= 2


# ---------------------------------------------------------------------------
# Telling a cancellation apart from a fault the handling of a delivery raised
# ---------------------------------------------------------------------------

async def _error_out_of_iterator(q_iter) -> BaseException:
    """The exception ``q_iter`` lets out when the cancellation of its consumer fails."""
    task = asyncio.create_task(q_iter.__anext__())
    await asyncio.sleep(0)
    task.cancel()
    try:
        await task
    except BaseException as exc:  # noqa: BLE001 - the object under test
        return exc
    raise AssertionError("the iterator returned instead of raising")


async def _timeout_out_of_call_with_timeout() -> BaseException:
    """The ``TimeoutError`` a call that outran its bound hands its caller."""
    try:
        await call_with_timeout(asyncio.Future(), timeout=0.01)
    except asyncio.TimeoutError as exc:
        return exc
    raise AssertionError("the call returned instead of timing out")


class TestTellingCancellationFromDeliveryFaults:
    """What each shape of exception chain means to a consumer loop.

    The loop has two answers to an exception that reached it: end as the cancelled task
    it is, or report the fault and subscribe again. Reading the first as the second
    leaves a consumer on the queue with nothing holding it; reading the second as the
    first ends the task silently, with no status written and no restart, and the row
    keeps saying ``listening`` until the next reconcile cycle.
    """

    @pytest.mark.asyncio
    async def test_an_error_out_of_a_cancellation_handler_is_a_cancellation(self):
        """Form 1 — the plain shape ``QueueIterator.__anext__`` produces: a foreign
        error raised inside an active ``except CancelledError``, carrying the
        cancellation as its context."""
        exc = await _error_out_of_iterator(
            _QueueIterFailingCancel(
                aio_pika.exceptions.AMQPConnectionError("connection reset by peer")
            )
        )

        assert exc.__cause__ is None
        assert isinstance(exc.__context__, asyncio.CancelledError)
        assert not exc.__suppress_context__
        assert _raised_while_cancelling(exc)

    def test_a_cancellederror_is_recognised_without_reading_the_chain(self):
        """Form 4 — a bare ``raise`` inside a cancellation handler re-raises that very
        object, and it is answered by the type test at the top of the walk."""
        exc = asyncio.CancelledError()

        assert exc.__cause__ is None
        assert exc.__context__ is None
        assert _raised_while_cancelling(exc)

    def test_an_error_that_disowns_the_cancellation_it_handled_is_not_one(self):
        """Form 2 — ``raise ... from None`` inside a cancellation handler. The
        cancellation is in ``__context__`` because Python puts it there, and
        ``__suppress_context__`` is the raiser saying it is not the cause."""
        try:
            try:
                raise asyncio.CancelledError()
            except asyncio.CancelledError:
                raise ConnectionError("connection was dropped while it connected") from None
        except ConnectionError as raised:
            exc = raised

        assert isinstance(exc.__context__, asyncio.CancelledError)
        assert exc.__suppress_context__
        assert not _raised_while_cancelling(exc)

    @pytest.mark.asyncio
    async def test_a_call_that_outran_its_bound_is_not_a_cancellation(self):
        """Form 2 as ``call_with_timeout`` writes it: the expired call is cancelled, and
        the caller is handed ``TimeoutError`` with that cancellation disowned."""
        exc = await _timeout_out_of_call_with_timeout()

        assert isinstance(exc.__context__, asyncio.CancelledError)
        assert exc.__suppress_context__
        assert not _raised_while_cancelling(exc)

    @pytest.mark.asyncio
    async def test_a_failed_cancel_reaching_the_iterator_through_aiormq_is_a_cancellation(self):
        """Form 5 — the same ``basic.cancel`` failure as form 1, delivered by
        ``aiormq``'s RPC wrapper instead of thrown directly, so it carries the
        ``CancelledError`` in ``__cause__``."""
        exc = await _error_out_of_iterator(
            _QueueIterFailingCancel(
                aio_pika.exceptions.AMQPConnectionError("connection reset by peer"),
                through_task_wrapper=True,
            )
        )

        assert isinstance(exc.__cause__, asyncio.CancelledError)
        assert exc.__suppress_context__
        assert _raised_while_cancelling(exc)

    def test_the_chain_of_a_delivery_fault_is_the_chain_of_a_cancellation(self):
        """Form 3 — what ``aiormq`` hands a publish whose connection was torn down
        mid-RPC. Its chain is form 5's, so no reading of the chain can tell the two
        apart; what does is the frame the exception was raised in, which each consumer
        loop remembers for itself and the two classes below put to those loops."""
        try:
            try:
                raise asyncio.CancelledError()
            except asyncio.CancelledError as rpc_stopped:
                raise aio_pika.exceptions.AMQPConnectionError(
                    "connection reset by peer"
                ) from rpc_stopped
        except aio_pika.exceptions.AMQPConnectionError as raised:
            exc = raised

        assert _raised_while_cancelling(exc), "the chain alone cannot tell it from form 5"


class TestDeliveryFaultsKeepTheSubscription:
    """A cooldown subscription whose delivery handling failed reports it and retries.

    Everything the handling of a delivery does reaches the broker or the metadata
    database on its own — a publish, the channel it needs, the Airflow connection behind
    that channel — and a failure of any of them arrives in the same shapes a rejected
    consumer cancellation does. Ending the task on one of those leaves the row reading
    ``listening`` with no consumer behind it until the next reconcile cycle.
    """

    async def _run_until_resubscribed(self, manager, queue, publish_channel):
        """Run the cooldown subscription until it attaches a second time.

        :returns: The statuses the subscription wrote, in order.
        """
        writes: list[tuple] = []

        async def record_write(self, status, last_error=None):
            writes.append((status, last_error))

        with _record_consumer_sleeps(lambda delay: None), \
             patch.object(manager, "_get_publish_channel", side_effect=publish_channel), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write",
                   new=record_write):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(cooldown=300))
            )
            try:
                await _wait_for(lambda: queue.iterator.call_count >= 2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
        return writes

    @pytest.mark.asyncio
    async def test_a_fault_of_one_task_does_not_disarm_the_cancellation_of_another(self):
        """The exception object two tasks of one connection are handed is the same one.

        ``aiormq``'s ``FutureStore.reject_all`` gives every pending RPC of a torn-down
        connection one instance, so whatever a task records on that object is read by
        every other task of that connection. Here A's placeholder publish fails with it
        while A is handling a delivery, and B is being cancelled at that moment with the
        broker rejecting its ``basic.cancel``, which lets the same object out of B's
        iterator. B must end cancelled: a B that reads it as a delivery fault of its own
        attaches again and consumes with nothing holding it, writing status into the row
        its replacement owns.
        """
        shared = aio_pika.exceptions.AMQPConnectionError("connection reset by peer")
        manager = _make_manager()
        queue_a = _make_push_queue([_make_fake_message(b"order")])
        queue_b = _queue_failing_cancel(shared)
        queues = {"orders": queue_a, "events": queue_b}
        channel = _make_live_channel()
        channel.declare_queue = AsyncMock(side_effect=lambda name, **kwargs: queues[name])
        connection = _make_live_connection(channel=channel)

        async def publish_channel_of_a_torn_down_connection(conn_id):
            try:
                raise asyncio.CancelledError()  # the RPC task the teardown stopped
            except asyncio.CancelledError as rpc_stopped:
                raise shared from rpc_stopped

        writes: list[tuple] = []

        async def record_write(self, status, last_error=None):
            writes.append((status, last_error))

        with _record_consumer_sleeps(lambda delay: None), \
             patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_get_publish_channel",
                          side_effect=publish_channel_of_a_torn_down_connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write",
                   new=record_write):
            a = asyncio.create_task(
                manager._consume_subscription(
                    _sub(id=1, queue_name="orders", cooldown=300)
                )
            )
            b = asyncio.create_task(
                manager._consume_subscription(_sub(id=2, queue_name="events"))
            )
            try:
                # A meets the fault and answers it as one: it reports and attaches again
                await _wait_for(lambda: queue_a.iterator.call_count >= 2)
                await _wait_for(lambda: queue_b.iterator.return_value.consumed)
                b.cancel()
                # The loop ends quietly on its own cancellation, so what says B took it
                # as one is that it stopped — a B that read the object as a fault of its
                # own goes on running, and no exception says so anywhere
                done, _ = await asyncio.wait({b}, timeout=2.0)
                assert done, "B kept running after its own cancellation"
            finally:
                a.cancel()
                b.cancel()
                await asyncio.wait({a, b}, timeout=2.0)

        assert _SUB_ERROR in [status for status, _ in writes], (
            "A read the object as the fault of the delivery it was handling"
        )
        assert queue_b.iterator.call_count == 1, (
            "B was cancelled, so it must not attach again"
        )

    @pytest.mark.asyncio
    async def test_a_publish_that_outran_its_bound_reports_error_and_retries(self):
        """The placeholder publish did not fit in ``rpc_timeout``, so the caller is
        handed a ``TimeoutError`` with the cancellation of the expired call disowned."""
        manager = _make_manager()
        queue = _make_push_queue([_make_fake_message(b"order")])
        connection = _make_live_connection(queue=queue)

        async def timing_out_channel(conn_id):
            return await call_with_timeout(asyncio.Future(), timeout=0.01)

        with patch.object(manager, "_get_or_create_connection", return_value=connection):
            writes = await self._run_until_resubscribed(
                manager, queue, timing_out_channel
            )

        assert _SUB_ERROR in [status for status, _ in writes]

    @pytest.mark.asyncio
    async def test_a_publish_connection_dropped_mid_rpc_reports_error_and_retries(self):
        """The publish connection was torn down while ``channel()`` was pending, so
        ``aiormq`` hands the caller its own error with the ``CancelledError`` of the
        stopped RPC task as the cause — the chain of a genuine cancellation."""
        manager = _make_manager()
        queue = _make_push_queue([_make_fake_message(b"order")])
        connection = _make_live_connection(queue=queue)

        async def dropped_channel(conn_id):
            try:
                raise asyncio.CancelledError()  # the RPC task the teardown stopped
            except asyncio.CancelledError as rpc_stopped:
                raise aio_pika.exceptions.AMQPConnectionError(
                    "connection reset by peer"
                ) from rpc_stopped

        with patch.object(manager, "_get_or_create_connection", return_value=connection):
            writes = await self._run_until_resubscribed(
                manager, queue, dropped_channel
            )

        assert _SUB_ERROR in [status for status, _ in writes]

    @pytest.mark.asyncio
    async def test_a_publish_connection_stuck_on_the_metadata_db_reports_error_and_retries(self):
        """Rebuilding the publish connection reads its Airflow connection first, and a
        metadata database that stopped answering ends that read on ``_DB_TIMEOUT``."""
        manager = _make_manager()
        queue = _make_push_queue([_make_fake_message(b"order")])
        connection = _make_live_connection(queue=queue)
        release = threading.Event()

        def blocking_get_connection(conn_id):
            release.wait(5)
            return _make_conn_info()

        build_publish_connection = manager._get_or_create_connection

        async def by_role(conn_id, role=_ROLE_CONSUME, executor=None):
            if role == _ROLE_PUBLISH:
                return await build_publish_connection(conn_id, role, executor)
            return connection

        writes: list[tuple] = []

        async def record_write(self, status, last_error=None):
            writes.append((status, last_error))

        try:
            with _record_consumer_sleeps(lambda delay: None), \
                 patch("airflow_provider_rmq.watcher.consumer._DB_TIMEOUT", 0.05), \
                 patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                       side_effect=blocking_get_connection), \
                 patch.object(manager, "_get_or_create_connection", side_effect=by_role), \
                 patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write",
                       new=record_write):
                task = asyncio.create_task(
                    manager._consume_subscription(_sub(cooldown=300))
                )
                try:
                    await _wait_for(lambda: queue.iterator.call_count >= 2, timeout=5.0)
                finally:
                    task.cancel()
                    await asyncio.gather(task, return_exceptions=True)
        finally:
            release.set()

        assert _SUB_ERROR in [status for status, _ in writes]

    @pytest.mark.asyncio
    async def test_a_failed_cancel_of_the_consumer_itself_still_ends_the_task(self):
        """What the iterator raises is still judged by the chain: a cancel the broker
        rejected ends the task as the cancelled task it is, and writes no status."""
        manager = _make_manager()
        queue = _queue_failing_cancel(
            aio_pika.exceptions.AMQPConnectionError("connection reset by peer")
        )
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))
        q_iter = queue.iterator.return_value
        writes: list[tuple] = []

        async def record_write(self, status, last_error=None):
            writes.append((status, last_error))

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write",
                   new=record_write):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(cooldown=300))
            )
            await _wait_for(lambda: q_iter.consumed)
            task.cancel()
            outcome = await asyncio.wait_for(
                asyncio.gather(task, return_exceptions=True), timeout=2.0
            )

        assert outcome[0] is None
        assert queue.iterator.call_count == 1
        assert _SUB_ERROR not in [status for status, _ in writes]

# ---------------------------------------------------------------------------
# Tests for C5 (binary body) and C3 (status reset on reconcile removal)
# ---------------------------------------------------------------------------

class TestTriggerDagBinaryBody:
    @pytest.mark.asyncio
    async def test_binary_body_does_not_raise(self):
        """C5: невалидный UTF-8 в теле сообщения не должен бросать исключение."""
        manager = _make_manager()
        msg = _make_fake_message(body=b"\xff\xfe invalid utf-8")

        conf_result = {}

        async def capture_executor(func, *args):
            conf_result.update(args[1])  # conf is the 2nd argument of _sync_trigger
            return None

        manager._executor = _FakeExecutor(capture_executor)
        with patch("airflow_provider_rmq.watcher.consumer.WatcherSession",
                   return_value=_mock_session()[0]):
            await manager._trigger_dag("dag", "q", 1, msg)

        assert isinstance(conf_result.get("body"), str)

    @pytest.mark.asyncio
    async def test_binary_body_replaced_chars(self):
        """C5: невалидные байты заменяются replacement char, результат — строка."""
        manager = _make_manager()
        msg = _make_fake_message(body=b"\xff\xfe")

        conf_result = {}

        async def capture_executor(func, *args):
            conf_result.update(args[1])
            return None

        manager._executor = _FakeExecutor(capture_executor)
        await manager._trigger_dag("dag", "q", 1, msg)

        assert isinstance(conf_result["body"], str)
        assert "�" in conf_result["body"]


class TestReconcileStatusReset:
    @pytest.mark.asyncio
    async def test_reconcile_sets_disconnected_on_removal(self):
        """C3: при удалении подписки из reconcile статус должен сбрасываться в disconnected."""
        manager = _make_manager()

        async def blocking_consume(sub):
            await asyncio.Future()

        set_status_calls = []

        def mock_set_consumer_status(session, sub_id, status, last_error=None):
            set_status_calls.append((sub_id, status))

        ctx, session = _mock_session()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"), \
             patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=mock_set_consumer_status):
            await manager.reconcile([_sub(id=1)])
            await manager.reconcile([])  # удаляем подписку

        assert any(sub_id == 1 and status == "disconnected"
                   for sub_id, status in set_status_calls)


# ---------------------------------------------------------------------------
# Tests for Task 3: RMQ infrastructure (fire exchange/queue, pending queues)
# ---------------------------------------------------------------------------

class TestEnsureFireInfrastructure:
    @pytest.mark.asyncio
    async def test_declares_topic_exchange(self):
        """_ensure_fire_infrastructure declares a topic exchange named rmq_watcher.fire."""
        channel = AsyncMock()
        exchange_mock = AsyncMock()
        channel.declare_exchange = AsyncMock(return_value=exchange_mock)
        queue_mock = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue_mock)

        await _ensure_fire_infrastructure(channel, timeout=DEFAULT_RPC_TIMEOUT)

        channel.declare_exchange.assert_called_once_with(
            _FIRE_EXCHANGE,
            type=aio_pika.ExchangeType.TOPIC,
            durable=True,
        )

    @pytest.mark.asyncio
    async def test_declares_durable_queue_and_binds(self):
        """_ensure_fire_infrastructure declares a durable queue and binds it with '#'."""
        channel = AsyncMock()
        exchange_mock = AsyncMock()
        channel.declare_exchange = AsyncMock(return_value=exchange_mock)
        queue_mock = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue_mock)

        await _ensure_fire_infrastructure(channel, timeout=DEFAULT_RPC_TIMEOUT)

        channel.declare_queue.assert_called_once_with(_FIRE_QUEUE, durable=True)
        queue_mock.bind.assert_called_once_with(exchange_mock, routing_key="#")

    @pytest.mark.asyncio
    async def test_idempotent_no_exception_on_second_call(self):
        """_ensure_fire_infrastructure is idempotent — second call must not raise."""
        channel = AsyncMock()
        exchange_mock = AsyncMock()
        channel.declare_exchange = AsyncMock(return_value=exchange_mock)
        queue_mock = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue_mock)

        await _ensure_fire_infrastructure(channel, timeout=DEFAULT_RPC_TIMEOUT)
        await _ensure_fire_infrastructure(channel, timeout=DEFAULT_RPC_TIMEOUT)

        assert channel.declare_exchange.call_count == 2
        assert channel.declare_queue.call_count == 2


class TestEnsurePendingQueue:
    @pytest.mark.asyncio
    async def test_declares_queue_with_correct_x_arguments(self):
        """_ensure_pending_queue declares queue with DLX and x-max-length=1 arguments."""
        channel = AsyncMock()
        queue_mock = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue_mock)

        dag_id = "my_dag"
        await _ensure_pending_queue(channel, dag_id, timeout=DEFAULT_RPC_TIMEOUT)

        expected_name = f"{_PENDING_QUEUE_PREFIX}{dag_id}"
        channel.declare_queue.assert_called_once_with(
            expected_name,
            durable=True,
            arguments={
                "x-dead-letter-exchange": _FIRE_EXCHANGE,
                "x-dead-letter-routing-key": dag_id,
                "x-max-length": 1,
                "x-overflow": "reject-publish",
            },
        )

    @pytest.mark.asyncio
    async def test_no_consumer_attached(self):
        """_ensure_pending_queue must NOT start consuming from the pending queue."""
        channel = AsyncMock()
        queue_mock = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue_mock)

        await _ensure_pending_queue(channel, "my_dag", timeout=DEFAULT_RPC_TIMEOUT)

        # Confirm that iterator/consume was never called on the queue
        queue_mock.iterator.assert_not_called()
        queue_mock.consume.assert_not_called()

    @pytest.mark.asyncio
    async def test_queue_name_contains_dag_id(self):
        """Pending queue name must include the dag_id."""
        channel = AsyncMock()
        queue_mock = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue_mock)

        await _ensure_pending_queue(channel, "special_dag_123", timeout=DEFAULT_RPC_TIMEOUT)

        call_args = channel.declare_queue.call_args
        assert "special_dag_123" in call_args[0][0]


class TestSubsChanged:
    def test_no_change_returns_false(self):
        manager = _make_manager()
        sub = _sub(id=1, cooldown=300, filter_data={"k": "v"}, conn_id="c1")
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=sub.copy(), state=_ConsumerState(1, _test_pool()))
        assert manager._subs_changed(1, sub) is False

    def test_cooldown_change_returns_true(self):
        manager = _make_manager()
        old_sub = _sub(id=1, cooldown=300)
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy(), state=_ConsumerState(1, _test_pool()))
        new_sub = _sub(id=1, cooldown=600)
        assert manager._subs_changed(1, new_sub) is True

    def test_filter_data_change_returns_true(self):
        manager = _make_manager()
        old_sub = _sub(id=1, filter_data={"type": "order"})
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy(), state=_ConsumerState(1, _test_pool()))
        new_sub = _sub(id=1, filter_data={"type": "payment"})
        assert manager._subs_changed(1, new_sub) is True

    def test_conn_id_change_returns_true(self):
        manager = _make_manager()
        old_sub = _sub(id=1, conn_id="conn_a")
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy(), state=_ConsumerState(1, _test_pool()))
        new_sub = _sub(id=1, conn_id="conn_b")
        assert manager._subs_changed(1, new_sub) is True

    def test_missing_sub_id_returns_true(self):
        manager = _make_manager()
        assert manager._subs_changed(999, _sub(id=999)) is True

    def test_exchange_and_routing_keys_change_does_not_restart_task(self):
        """Changing exchange=/routing_keys= with queue_name/dag_id/cooldown/filter_data/conn_id
        unchanged must NOT be treated as a change — only bind-diff in
        _provision_exchange_subs reacts to it, the consumer task/queue stay the same."""
        manager = _make_manager()
        old_sub = _exchange_sub(id=1, exchange="jetstat.airflow", routing_keys=["a.succeeded"])
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy(), state=_ConsumerState(1, _test_pool()))

        new_sub = _exchange_sub(id=1, exchange="jetstat.airflow", routing_keys=["b.failed", "c.*"])
        assert manager._subs_changed(1, new_sub) is False

    def test_exchange_name_change_does_not_restart_task(self):
        """Changing exchange= itself (queue_name/dag_id/cooldown/filter_data/conn_id unchanged)
        also does not restart the task — same rationale as routing_keys change."""
        manager = _make_manager()
        old_sub = _exchange_sub(id=1, exchange="jetstat.airflow")
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy(), state=_ConsumerState(1, _test_pool()))

        new_sub = _exchange_sub(id=1, exchange="some.other.exchange")
        assert manager._subs_changed(1, new_sub) is False


class TestHotReload:
    @pytest.mark.asyncio
    async def test_cooldown_change_restarts_task(self):
        """reconcile: sub with changed cooldown causes task to be cancelled and restarted."""
        manager = _make_manager()
        started = asyncio.Event()
        start_count = [0]

        async def blocking_consume(sub):
            start_count[0] += 1
            started.set()
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_sub(id=1, cooldown=300)])
            old_task = manager._active[1].task
            # Give the event loop a tick so the task body can start
            await asyncio.sleep(0)
            assert not old_task.done()

            started.clear()
            await manager.reconcile([_sub(id=1, cooldown=600)])
            new_task = manager._active[1].task

            # old task was cancelled, new task is different
            assert old_task.done()
            assert new_task is not old_task
            assert manager._active[1].sub["cooldown"] == 600

            # Wait for the new task to start
            await asyncio.wait_for(started.wait(), timeout=1.0)
            new_task.cancel()
            await asyncio.gather(new_task, return_exceptions=True)

        assert start_count[0] == 2

    @pytest.mark.asyncio
    async def test_unchanged_sub_does_not_restart_task(self):
        """reconcile: sub with no change to cooldown/filter/conn_id keeps same task."""
        manager = _make_manager()

        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_sub(id=1, cooldown=300)])
            old_task = manager._active[1].task

            await manager.reconcile([_sub(id=1, cooldown=300)])
            new_task = manager._active[1].task

            assert old_task is new_task  # same task, not restarted
            old_task.cancel()
            await asyncio.gather(old_task, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_filter_data_change_restarts_task(self):
        """reconcile: changed filter_data causes task restart."""
        manager = _make_manager()
        start_calls = []

        async def blocking_consume(sub):
            start_calls.append(dict(sub.get("filter_data", {})))
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_sub(id=1, filter_data={"type": "order"})])
            old_task = manager._active[1].task

            await manager.reconcile([_sub(id=1, filter_data={"type": "payment"})])
            new_task = manager._active[1].task

            assert old_task is not new_task
            assert old_task.done()
            new_task.cancel()
            await asyncio.gather(new_task, return_exceptions=True)


class TestProvisionCooldown:
    @pytest.mark.asyncio
    async def test_provision_cooldown_error_does_not_raise(self):
        """_provision_cooldown catches exceptions and logs ERROR without re-raising."""
        manager = _make_manager()

        async def bad_get_conn(conn_id):
            raise ConnectionError("broker unavailable")

        with patch.object(manager, "_get_or_create_connection", side_effect=bad_get_conn):
            # Must not raise — errors are logged and swallowed
            await manager._provision_cooldown({"my_dag"}, "rmq_default")

    @pytest.mark.asyncio
    async def test_provision_cooldown_creates_fire_infra_and_pending(self):
        """_provision_cooldown calls _ensure_fire_infrastructure and _ensure_pending_queue."""
        manager = _make_manager()

        setup_channel = AsyncMock()
        setup_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        queue_mock = AsyncMock()
        setup_channel.declare_queue = AsyncMock(return_value=queue_mock)

        connection = AsyncMock()
        connection.channel = AsyncMock(return_value=setup_channel)

        with patch.object(manager, "_get_or_create_connection", return_value=connection):
            await manager._provision_cooldown({"dag_a", "dag_b"}, "rmq_default")

        # Verify fire infrastructure was declared
        fire_exchange_calls = [
            c for c in setup_channel.declare_exchange.call_args_list
            if c[0][0] == _FIRE_EXCHANGE
        ]
        assert fire_exchange_calls, "rmq_watcher.fire exchange not declared"

        # Verify pending queues for both dag_ids were declared
        declared_queue_names = [
            c[0][0] for c in setup_channel.declare_queue.call_args_list
        ]
        assert f"{_PENDING_QUEUE_PREFIX}dag_a" in declared_queue_names
        assert f"{_PENDING_QUEUE_PREFIX}dag_b" in declared_queue_names

        # Verify the short-lived setup channel was closed after provisioning
        setup_channel.close.assert_awaited_once()

    def test_orphaned_warning_logged_on_new_orphan(self):
        """_check_orphaned_pending_queues logs WARNING when a dag_id becomes orphaned."""
        manager = _make_manager()
        manager._cooldown_tracker.mark_provisioned({"orphaned_dag"})

        with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            manager._check_orphaned_pending_queues({"active_dag"})
            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            assert any("orphaned_dag" in m for m in warning_messages)

    def test_no_duplicate_orphan_warning(self):
        """_check_orphaned_pending_queues only warns once per new orphan dag_id."""
        manager = _make_manager()
        manager._cooldown_tracker.mark_provisioned({"orphaned_dag"})
        manager._check_orphaned_pending_queues({"active_dag"})  # already warned once

        with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            manager._check_orphaned_pending_queues({"active_dag"})
            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            # No new orphan warning since orphaned_dag was already reported
            assert not any("orphaned_dag" in m and "orphaned" in m.lower()
                           for m in warning_messages)

    @pytest.mark.asyncio
    async def test_orphan_warning_fires_when_last_cooldown_sub_removed_via_reconcile(self):
        """reconcile() logs orphan WARNING even when ALL cooldown subscriptions are removed at once.

        Regression test: before the fix, _provision_cooldown was only called when
        cooldown_dag_ids was non-empty, so the orphan tracking was silently skipped
        when the last cooldown sub was removed.
        """
        manager = _make_manager()

        async def blocking_consume(sub):
            await asyncio.Future()

        async def blocking_fire(connection, conn_id):
            await asyncio.Future()

        # First reconcile: add one cooldown subscription to populate _cooldown_dag_ids
        connection = AsyncMock()
        setup_channel = AsyncMock()
        setup_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        setup_channel.declare_queue = AsyncMock(return_value=AsyncMock())
        connection.channel = AsyncMock(return_value=setup_channel)
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue", side_effect=blocking_fire), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_get_or_create_connection", return_value=connection):
            await manager.reconcile([_sub(id=1, cooldown=300)])
            await asyncio.sleep(0)
            # _cooldown_tracker should now have "test_dag" provisioned (default dag_id in _sub())
            assert "test_dag" in manager._cooldown_tracker._provisioned

        # Cancel running tasks for clean state
        await manager.stop()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection

        # Second reconcile: remove ALL cooldown subscriptions (empty list)
        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue", side_effect=blocking_fire), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager.reconcile([])
            await asyncio.sleep(0)

            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            assert any("test_dag" in m and "orphaned" in m.lower() for m in warning_messages), (
                "Expected orphan WARNING for test_dag when last cooldown subscription removed, "
                f"but got: {warning_messages}"
            )

    @pytest.mark.asyncio
    async def test_orphan_warning_fires_when_provision_fails_and_dag_removed(self):
        """reconcile() fires the orphan WARNING for a removed dag even when RMQ is down.

        The orphan check runs outside _provision_cooldown, which returns early when the
        broker is unreachable: a subscription removed during an outage is still noticed.
        """
        manager = _make_manager()

        async def blocking_consume(sub):
            await asyncio.Future()

        # dag_a and dag_b both have cooldown infrastructure of their own
        manager._cooldown_tracker.mark_provisioned({"dag_a", "dag_b"})

        # Now: only dag_a remains, dag_b was removed; RMQ is down
        async def fail_get_conn(conn_id):
            raise ConnectionError("broker down")

        with patch.object(manager, "_get_or_create_connection", side_effect=fail_get_conn), \
             patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager.reconcile([_sub(id=1, dag_id="dag_a", cooldown=300)])
            await asyncio.sleep(0)

            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            assert any("dag_b" in m and "orphaned" in m.lower() for m in warning_messages), (
                "Expected orphan WARNING for dag_b even though RMQ provisioning failed, "
                f"but got: {warning_messages}"
            )

        await manager.stop()


# ---------------------------------------------------------------------------
# Tests for Task 4: Cooldown logic in _consume_subscription
# ---------------------------------------------------------------------------

class TestCooldownConsume:
    """Tests for cooldown>0 path in _consume_subscription."""

    def _make_channel_with_queue(self, messages: list):
        """Return (channel, connection) where the channel yields ``messages``."""
        channel = _make_live_channel(queue=_make_push_queue(messages))
        return channel, _make_live_connection(channel=channel)

    @pytest.mark.asyncio
    async def test_cooldown_zero_calls_trigger_dag(self, manager):
        """cooldown=0: matching message triggers DAG via _trigger_dag."""
        msg = _make_fake_message(b"order")
        channel, connection = self._make_channel_with_queue([msg])

        triggered = asyncio.Event()

        async def mock_trigger(dag_id, queue_name, sub_id, message):
            triggered.set()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag", side_effect=mock_trigger), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(cooldown=0))
            )
            await asyncio.wait_for(triggered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert triggered.is_set()

    @pytest.mark.asyncio
    async def test_cooldown_zero_does_not_publish_to_pending(self, manager):
        """cooldown=0: publish to pending queue must NOT be called."""
        msg = _make_fake_message(b"order")
        channel, connection = self._make_channel_with_queue([msg])

        triggered = asyncio.Event()

        async def mock_trigger(*args, **kwargs):
            triggered.set()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag", side_effect=mock_trigger), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(cooldown=0))
            )
            await asyncio.wait_for(triggered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        channel.default_exchange.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_cooldown_positive_publishes_to_pending(self, manager):
        """cooldown=300: matching message publishes to pending queue, does NOT call trigger_dag."""
        msg = _make_fake_message(b"order")
        channel, connection = self._make_channel_with_queue([msg])
        published = asyncio.Event()

        async def capture_publish(amqp_msg, routing_key):
            published.set()

        channel.default_exchange.publish = capture_publish

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag") as mock_td, \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(cooldown=300))
            )
            await asyncio.wait_for(published.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        mock_td.assert_not_called()
        assert published.is_set()
        msg.ack.assert_awaited()

    @pytest.mark.asyncio
    async def test_cooldown_pending_routing_key_contains_dag_id(self, manager):
        """cooldown>0: routing_key for pending publish must be rmq_watcher.pending.{dag_id}."""
        msg = _make_fake_message(b"order")
        channel, connection = self._make_channel_with_queue([msg])

        publish_kwargs = {}
        published = asyncio.Event()

        async def capture_publish(amqp_msg, routing_key):
            publish_kwargs["routing_key"] = routing_key
            published.set()

        channel.default_exchange.publish = capture_publish

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag"), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(dag_id="my_dag", cooldown=300))
            )
            await asyncio.wait_for(published.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert publish_kwargs["routing_key"] == "rmq_watcher.pending.my_dag"

    @pytest.mark.asyncio
    async def test_cooldown_pending_message_has_expiration(self, manager):
        """cooldown>0: the placeholder must carry a TTL the broker actually receives."""
        msg = _make_fake_message(b"order")
        channel, connection = self._make_channel_with_queue([msg])

        published_msg = {}
        published = asyncio.Event()

        async def capture_publish(amqp_msg, routing_key):
            published_msg["msg"] = amqp_msg
            published.set()

        channel.default_exchange.publish = capture_publish

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag"), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(cooldown=300))
            )
            await asyncio.wait_for(published.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # ``properties`` is what aio_pika hands the broker, and the only place the
        # expiration is encoded: reading it here is what makes the assertion about the
        # wire and not about the attribute the constructor stored.
        assert published_msg["msg"].properties.expiration == "300000"

    @pytest.mark.asyncio
    async def test_cooldown_nonmatching_message_nacked(self, manager):
        """cooldown>0: non-matching message is NACKed, publish NOT called."""
        msg = _make_fake_message(b"payment", headers={"type": "payment"})
        channel, connection = self._make_channel_with_queue([msg])

        nacked = asyncio.Event()
        original_nack = msg.nack

        async def capture_nack(*args, **kwargs):
            nacked.set()

        msg.nack = capture_nack

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag") as mock_td, \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(
                manager._consume_subscription(
                    _sub(cooldown=300, filter_data={"filter_headers": {"type": "order"}})
                )
            )
            await asyncio.wait_for(nacked.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        mock_td.assert_not_called()
        channel.default_exchange.publish.assert_not_called()
        assert nacked.is_set()

    @pytest.mark.asyncio
    async def test_cooldown_none_treated_as_zero(self, manager):
        """cooldown=None in sub dict is treated as 0 (immediate trigger_dag)."""
        msg = _make_fake_message(b"order")
        channel, connection = self._make_channel_with_queue([msg])

        triggered = asyncio.Event()

        async def mock_trigger(*args, **kwargs):
            triggered.set()

        sub = _sub()
        sub["cooldown"] = None  # simulate NULL from DB

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag", side_effect=mock_trigger), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(
                manager._consume_subscription(sub)
            )
            await asyncio.wait_for(triggered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert triggered.is_set()
        channel.default_exchange.publish.assert_not_called()


class TestImmediateSourceConf:
    """Verify that _trigger_dag passes source='immediate' in conf."""

    @pytest.mark.asyncio
    async def test_trigger_dag_conf_has_source_immediate(self):
        """_trigger_dag must include source='immediate' in conf for immediate triggers."""
        manager = _make_manager()
        msg = _make_fake_message(b"hello")

        captured_conf = {}

        async def capture_executor(func, *args):
            # args = (dag_id, conf, run_id)
            if len(args) >= 2 and isinstance(args[1], dict):
                captured_conf.update(args[1])
            return None

        manager._executor = _FakeExecutor(capture_executor)
        await manager._trigger_dag("my_dag", "my_queue", 1, msg)

        assert captured_conf.get("source") == "immediate"


# ---------------------------------------------------------------------------
# Tests for Task 5: _consume_fire_queue
# ---------------------------------------------------------------------------

def _make_fire_message(routing_key: str = "my_dag", message_id: str = "uuid-123"):
    """Create a fake DLX fire-queue message with routing_key and message_id."""
    msg = MagicMock()
    msg.routing_key = routing_key
    msg.message_id = message_id
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    return msg


class TestConsumeFireQueue:
    """Tests for _consume_fire_queue — DAG trigger after DLX TTL expires."""

    @pytest.mark.asyncio
    async def test_a_fire_trigger_that_keeps_failing_pauses_longer_each_time(self):
        """A fire event is the only record that a cooldown window expired.

        A trigger that fails requeues it, and without a growing pause the broker hands
        it straight back: at one redelivery per reconnect delay the default delivery
        limit of a quorum queue is spent in under two minutes, after which the event is
        dead-lettered — and ``rmq_watcher.fire`` is declared with no dead-letter exchange,
        so the DAG run it stood for is simply gone. The sibling paths grew a backoff for
        exactly this.
        """
        manager = _make_manager()
        msg = _make_fire_message()
        queue = MagicMock()
        # The same event handed back again and again, the way a requeue does.
        queue.iterator.side_effect = lambda **kw: _QueueIterCtx([msg] * 5)
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        delays: list = []
        real_sleep = asyncio.sleep

        with patch.object(manager, "_trigger_fire_dag",
                          side_effect=RuntimeError("the metadata database is gone")), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             _record_consumer_sleeps(delays.append):
            task = asyncio.create_task(
                manager._consume_fire_queue(connection, "rmq_default")
            )
            deadline = time.monotonic() + 5
            while len([d for d in delays if d != _RECONNECT_DELAY]) < 3:
                assert time.monotonic() < deadline, delays
                await real_sleep(0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        trigger_pauses = [d for d in delays if d != _RECONNECT_DELAY]
        assert trigger_pauses[:3] == [
            _TRIGGER_BACKOFF_START,
            _TRIGGER_BACKOFF_START * 2,
            _TRIGGER_BACKOFF_START * 4,
        ], delays
        assert msg.nack.await_count >= 3

    @pytest.mark.asyncio
    async def test_a_fire_trigger_that_goes_through_clears_the_pause(self):
        outcomes = [RuntimeError("db"), _OUTCOME_TRIGGERED, RuntimeError("db")]
        messages = [_make_fire_message(), _make_fire_message(), _make_fire_message()]
        queue = MagicMock()
        queue.iterator.side_effect = lambda **kw: _QueueIterCtx(list(messages))
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))
        manager = _make_manager()

        async def trigger(*args, **kwargs):
            outcome = outcomes.pop(0) if outcomes else _OUTCOME_TRIGGERED
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        delays: list = []
        real_sleep = asyncio.sleep

        with patch.object(manager, "_trigger_fire_dag", side_effect=trigger), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             _record_consumer_sleeps(delays.append):
            task = asyncio.create_task(
                manager._consume_fire_queue(connection, "rmq_default")
            )
            deadline = time.monotonic() + 5
            while len([d for d in delays if d != _RECONNECT_DELAY]) < 2:
                assert time.monotonic() < deadline, delays
                await real_sleep(0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        trigger_pauses = [d for d in delays if d != _RECONNECT_DELAY]
        assert trigger_pauses[:2] == [_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_START], (
            "a fire event that went through starts the next pause from scratch"
        )

    @pytest.mark.asyncio
    async def test_fire_consumer_triggers_dag_with_routing_key(self):
        """_consume_fire_queue triggers _sync_trigger with dag_id from routing_key."""
        manager = _make_manager()
        msg = _make_fire_message(routing_key="orders_dag", message_id="abc-123")
        connection = _make_live_connection(queue=_make_push_queue([msg]))

        triggered_calls = []
        triggered = asyncio.Event()

        async def mock_executor(func, *args):
            # args = (dag_id, conf, run_id) for _sync_trigger
            triggered_calls.append(args)
            triggered.set()
            return None

        original_run = manager._executor
        manager._executor = _FakeExecutor(mock_executor)
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
            await asyncio.wait_for(triggered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            manager._executor = original_run

        assert len(triggered_calls) == 1
        dag_id_arg, conf_arg, run_id_arg = triggered_calls[0]
        assert dag_id_arg == "orders_dag"
        assert conf_arg["source"] == "cooldown"
        assert conf_arg["dag_id"] == "orders_dag"
        assert conf_arg["queue"] == _FIRE_QUEUE
        assert conf_arg["subscription_id"] is None

    @pytest.mark.asyncio
    async def test_fire_consumer_run_id_contains_dag_id_and_message_id(self):
        """run_id must be rmq_cooldown__{dag_id}__{message.message_id}."""
        manager = _make_manager()
        msg = _make_fire_message(routing_key="my_dag", message_id="fixed-uuid-42")
        connection = _make_live_connection(queue=_make_push_queue([msg]))

        captured_run_id = {}
        triggered = asyncio.Event()

        async def mock_executor(func, *args):
            captured_run_id["run_id"] = args[2]
            triggered.set()
            return None

        original_run = manager._executor
        manager._executor = _FakeExecutor(mock_executor)
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
            await asyncio.wait_for(triggered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            manager._executor = original_run

        assert captured_run_id["run_id"] == "rmq_cooldown__my_dag__fixed-uuid-42"

    @pytest.mark.asyncio
    async def test_fire_consumer_run_id_of_a_long_dag_id_fits_the_column(self):
        """A long dag_id plus a UUID overruns DagRun.run_id, and the DataError it
        raises matches no classification branch — the cooldown event would never fire."""
        manager = _make_manager()
        dag_id = "orders_" + "x" * 240
        msg = _make_fire_message(routing_key=dag_id, message_id=str(uuid4()))
        connection = _make_live_connection(queue=_make_push_queue([msg]))

        captured_run_id = {}
        triggered = asyncio.Event()

        async def mock_executor(func, *args):
            captured_run_id["run_id"] = args[2]
            triggered.set()
            return _OUTCOME_TRIGGERED

        manager._executor = _FakeExecutor(mock_executor)
        task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
        try:
            await asyncio.wait_for(triggered.wait(), timeout=2.0)
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

        run_id = captured_run_id["run_id"]
        assert len(run_id) <= _RUN_ID_MAX_LEN
        assert _RUN_ID_UNSAFE.search(run_id) is None

    @pytest.mark.asyncio
    async def test_fire_consumer_acks_after_trigger(self):
        """_consume_fire_queue ACKs the message after successful _sync_trigger."""
        manager = _make_manager()
        msg = _make_fire_message(routing_key="my_dag")
        connection = _make_live_connection(queue=_make_push_queue([msg]))

        acked = asyncio.Event()
        original_ack = msg.ack

        async def capture_ack(*args, **kwargs):
            acked.set()

        msg.ack = capture_ack

        async def mock_executor(func, *args):
            return None

        original_run = manager._executor
        manager._executor = _FakeExecutor(mock_executor)
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
            await asyncio.wait_for(acked.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            manager._executor = original_run

        assert acked.is_set()

    @pytest.mark.asyncio
    async def test_fire_consumer_acks_on_integrity_error_duplicate(self):
        """Duplicate delivery (IntegrityError in _sync_trigger) → still ACK, no re-raise.

        Note: this test patches _sync_trigger to raise IntegrityError directly from the
        executor. In production _sync_trigger catches IntegrityError internally and returns
        normally, so the real code path tested here is "executor raises → Exception branch
        retries". The test verifies the message is eventually ACKed after the mock_executor
        suppresses the error and returns, allowing the fire consumer to proceed to ack().
        """
        from sqlalchemy.exc import IntegrityError as SaIntegrityError

        manager = _make_manager()
        msg = _make_fire_message(routing_key="my_dag", message_id="dup-uuid")
        connection = _make_live_connection(queue=_make_push_queue([msg]))

        acked = asyncio.Event()

        async def capture_ack(*args, **kwargs):
            acked.set()

        msg.ack = capture_ack

        # _sync_trigger runs in the thread pool; simulate it raising IntegrityError
        def sync_trigger_raises_integrity(dag_id, conf, run_id):
            raise SaIntegrityError("dup run_id", {}, None)

        async def mock_executor(func, *args):
            # func is _sync_trigger; call it synchronously to trigger the IntegrityError path
            try:
                func(*args)
            except SaIntegrityError:
                pass  # _sync_trigger already handles IntegrityError internally

        original_run = manager._executor
        manager._executor = _FakeExecutor(mock_executor)
        try:
            with patch("airflow_provider_rmq.watcher.consumer._sync_trigger",
                       side_effect=sync_trigger_raises_integrity):
                task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
                await asyncio.wait_for(acked.wait(), timeout=2.0)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        finally:
            manager._executor = original_run

        assert acked.is_set()

    @pytest.mark.asyncio
    async def test_fire_consumer_generic_exception_retries(self):
        """Generic Exception in fire consumer → logs warning and retries (no exit)."""
        manager = _make_manager()
        recovered = asyncio.Event()
        call_count = 0

        original_channel = None

        async def channel_factory():
            nonlocal call_count
            call_count += 1
            ch = AsyncMock()
            if call_count == 1:
                # First channel: declare_queue raises a generic RuntimeError
                ch.declare_queue = AsyncMock(side_effect=RuntimeError("transient failure"))
            else:
                # Second channel: succeed and signal recovery
                recovered.set()
                ch.declare_queue = AsyncMock(return_value=_make_push_queue([]))
            return ch

        connection = AsyncMock()
        connection.channel = channel_factory

        with patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
            await asyncio.wait_for(recovered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert call_count >= 2, "Fire consumer should have retried after generic Exception"

    @pytest.mark.asyncio
    async def test_fire_consumer_skips_message_with_empty_routing_key(self):
        """Message with no routing_key is ACKed and skipped (no trigger_dag)."""
        manager = _make_manager()
        msg = _make_fire_message(routing_key="", message_id="no-rk")
        connection = _make_live_connection(queue=_make_push_queue([msg]))

        trigger_called = asyncio.Event()
        acked = asyncio.Event()

        original_ack = msg.ack

        async def capture_ack(*args, **kwargs):
            acked.set()

        msg.ack = capture_ack

        async def fail_if_called(func, *args):
            trigger_called.set()
            return None

        original_run = manager._executor
        manager._executor = _FakeExecutor(fail_if_called)
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
            await asyncio.wait_for(acked.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            manager._executor = original_run

        assert acked.is_set()
        assert not trigger_called.is_set()

    @pytest.mark.asyncio
    async def test_fire_consumer_skips_message_with_missing_message_id(self):
        """Message with no message_id is ACKed and skipped (no trigger_dag) — idempotency guard."""
        manager = _make_manager()
        msg = _make_fire_message(routing_key="my_dag", message_id=None)
        connection = _make_live_connection(queue=_make_push_queue([msg]))

        trigger_called = asyncio.Event()
        acked = asyncio.Event()

        async def capture_ack(*args, **kwargs):
            acked.set()

        msg.ack = capture_ack

        async def fail_if_called(func, *args):
            trigger_called.set()
            return None

        original_run = manager._executor
        manager._executor = _FakeExecutor(fail_if_called)
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
            await asyncio.wait_for(acked.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            manager._executor = original_run

        assert acked.is_set()
        assert not trigger_called.is_set()

    @pytest.mark.asyncio
    async def test_fire_consumer_channel_not_found_exits(self):
        """ChannelNotFoundEntity → fire consumer exits (fatal, no retry)."""
        manager = _make_manager()
        connection = AsyncMock()
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelNotFoundEntity("no such queue")
        )
        connection.channel = AsyncMock(return_value=channel)

        task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
        # Task should exit on its own — fatal error, no retry
        await asyncio.wait_for(task, timeout=2.0)

        assert task.done()
        assert not task.cancelled()

    @pytest.mark.asyncio
    async def test_fire_consumer_channel_closed_retries(self):
        """ChannelClosed → fire consumer retries after delay."""
        manager = _make_manager()
        call_count = 0
        recovered = asyncio.Event()

        async def mock_declare(name, passive=False, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise aio_pika.exceptions.ChannelClosed("dropped")
            recovered.set()
            return _make_push_queue([])

        connection = AsyncMock()
        channel = AsyncMock()
        channel.declare_queue = mock_declare
        connection.channel = AsyncMock(return_value=channel)

        with patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
            await asyncio.wait_for(recovered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        assert call_count >= 2

    @pytest.mark.asyncio
    async def test_the_fire_iterator_ending_pauses_before_resubscribing(self):
        """The fire iterator returns on its own when the connection was closed on
        purpose, when the channel it lost cannot be waited out and when its own wait for
        that channel runs out. Every one of those states repeats as readily as it
        arrives, so without a pause the loop reattaches to ``rmq_watcher.fire`` as fast
        as the broker can answer — the sibling subscription loop waits out the same
        interval for the same reason."""
        manager = _make_manager()
        queue = _ending_queue()
        connection = _make_live_connection(queue=queue)

        delays: list = []
        paused = asyncio.Event()

        def note(delay):
            delays.append(delay)
            paused.set()

        with patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             _record_consumer_sleeps(note):
            task = asyncio.create_task(
                manager._consume_fire_queue(connection, "rmq_default")
            )
            await asyncio.wait_for(paused.wait(), timeout=2.0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        assert delays[0] == _RECONNECT_DELAY
        assert queue.iterator.call_count >= 1

    @pytest.mark.asyncio
    async def test_cancelling_the_fire_task_inside_a_pause_ends_it_quietly(self):
        """Reconcile cancels the fire task whenever it drops the connection under it,
        and so does ``stop()``. A cancellation that arrives while the loop is waiting
        ends it where it stands: the task returns instead of letting the CancelledError
        out as a consumer that died of its own accord."""
        manager = _make_manager()
        channel = _make_live_channel(queue=_ending_queue())
        connection = _make_live_connection(channel=channel)
        # The connection refuses a second attach, so the loop is waiting when the
        # cancellation reaches it whichever pause it took.
        connection.channel = AsyncMock(
            side_effect=[channel, RuntimeError("connection reset by peer")]
        )

        paused = asyncio.Event()

        with patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             _record_consumer_sleeps(lambda delay: paused.set(), block=True):
            task = asyncio.create_task(
                manager._consume_fire_queue(connection, "rmq_default")
            )
            await asyncio.wait_for(paused.wait(), timeout=2.0)
            task.cancel()
            outcome = await asyncio.wait_for(
                asyncio.gather(task, return_exceptions=True), timeout=2.0
            )

        assert outcome[0] is None
        assert task.done()
        assert not task.cancelled()

    @pytest.mark.asyncio
    async def test_reconcile_starts_fire_task_when_cooldown_sub_added(self):
        """reconcile starts _fire_task when first cooldown subscription appears."""
        manager = _make_manager()

        async def blocking_consume(sub):
            await asyncio.Future()

        async def blocking_fire(conn, conn_id=None):
            await asyncio.Future()

        connection = _make_live_connection()

        # Pre-populate _connections so reconcile can find it after _provision_cooldown
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue", side_effect=blocking_fire), \
             patch.object(manager, "_provision_cooldown"), \
             patch.object(manager, "_update_all_conn_counts"):
            await manager.reconcile([_sub(id=1, cooldown=300)])
            await asyncio.sleep(0)  # let tasks start

            assert manager._fire_task is not None
            assert not manager._fire_task.done()

            manager._fire_task.cancel()
            manager._active[1].task.cancel()
            await asyncio.gather(
                manager._fire_task,
                manager._active[1].task,
                return_exceptions=True,
            )

    @pytest.mark.asyncio
    async def test_reconcile_stops_fire_task_when_all_cooldown_subs_removed(self):
        """reconcile cancels _fire_task when all cooldown subscriptions are removed."""
        manager = _make_manager()

        async def blocking_consume(sub):
            await asyncio.Future()

        async def blocking_fire(conn, conn_id=None):
            await asyncio.Future()

        connection = _make_live_connection()

        # Pre-populate _connections so reconcile can find it after _provision_cooldown
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue", side_effect=blocking_fire), \
             patch.object(manager, "_provision_cooldown"), \
             patch.object(manager, "_update_all_conn_counts"):
            # Start with a cooldown subscription — fire task should start
            await manager.reconcile([_sub(id=1, cooldown=300)])
            await asyncio.sleep(0)
            fire_task = manager._fire_task
            assert fire_task is not None and not fire_task.done()

            # Remove all cooldown subscriptions — fire task should stop
            await manager.reconcile([])
            assert fire_task.done()
            assert manager._fire_task is None

    @pytest.mark.asyncio
    async def test_a_fire_delivery_that_failed_reports_error_and_consumes_again(self):
        """The handling of a fire event reaches the broker and the metadata database on
        its own, and a connection torn down under it arrives in the shape of a rejected
        consumer cancellation — ``aiormq`` hands every pending RPC its own error with the
        ``CancelledError`` of the stopped RPC task as the cause. Read as a cancellation,
        the fire task would return silently with no status written, and cooldown DAGs
        would stop firing while the row still said ``listening``."""
        manager = _make_manager()
        queue = _make_push_queue([_make_fake_message(b"fire")])
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))
        writes: list[tuple] = []

        async def record_write(self, status, last_error=None):
            writes.append((status, last_error))

        async def dropped_mid_rpc(message, state, backoff):
            try:
                raise asyncio.CancelledError()  # the RPC task the teardown stopped
            except asyncio.CancelledError as rpc_stopped:
                raise aio_pika.exceptions.AMQPConnectionError(
                    "connection reset by peer"
                ) from rpc_stopped

        with _record_consumer_sleeps(lambda delay: None), \
             patch.object(manager, "_handle_fire_delivery", side_effect=dropped_mid_rpc), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write",
                   new=record_write):
            task = asyncio.create_task(
                manager._consume_fire_queue(connection, "rmq_default")
            )
            try:
                await _wait_for(lambda: queue.iterator.call_count >= 2)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

        assert _SUB_ERROR in [status for status, _ in writes]

    @pytest.mark.asyncio
    async def test_a_failed_consumer_cancel_still_ends_the_fire_task(self):
        """A cancel the broker rejects leaves aio_pika's own error in place of the
        CancelledError. Read as a transient fault it would send the fire consumer around
        the loop again, and a second one would consume rmq_watcher.fire with nothing
        holding it — one expired cooldown window would reach whichever got there first,
        and neither reconcile nor stop could reach the one the manager has dropped."""
        manager = _make_manager()
        queue = _queue_failing_cancel(
            aio_pika.exceptions.AMQPConnectionError("connection reset by peer")
        )
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))
        q_iter = queue.iterator.return_value

        with patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(
                manager._consume_fire_queue(connection, "rmq_default")
            )
            await _wait_for(lambda: q_iter.consumed)
            task.cancel()
            outcome = await asyncio.wait_for(
                asyncio.gather(task, return_exceptions=True), timeout=2.0
            )

        assert outcome[0] is None
        assert task.done()
        assert queue.iterator.call_count == 1

    @pytest.mark.asyncio
    async def test_a_fault_of_one_task_does_not_disarm_the_cancellation_of_the_fire_consumer(self):
        """The exception object every task of one connection is handed is a single one.

        ``aiormq``'s ``FutureStore.reject_all`` gives every pending RPC of a torn-down
        connection one instance, so whatever a task records on that object is read by
        every other task of that connection. Here a subscription's placeholder publish
        fails with it while that subscription is handling a delivery, and the fire
        consumer is cancelled at that moment with the broker rejecting its
        ``basic.cancel``, which lets the very same object out of its iterator. The fire
        consumer must end cancelled: whether the delivery it was handling failed is a
        fact of its own frame, not of the object it is holding. Reading a neighbour's
        fault as its own, it would attach to ``rmq_watcher.fire`` again with nothing
        holding it, and an expired cooldown window would reach whichever of the two
        consumers got there first.
        """
        shared = aio_pika.exceptions.AMQPConnectionError("connection reset by peer")
        manager = _make_manager()
        # The second attach of the subscription has no delivery and waits there, which
        # holds the loop still while the test drives the fire consumer.
        sub_queue = MagicMock()
        sub_queue.iterator = MagicMock(side_effect=[
            _QueueIterCtx([_make_fake_message(b"order")]), _QueueIterCtx([]),
        ])
        # The broker rejects the cancellation of the first attach and takes the one of
        # any attach after it, so a fire consumer that went around the loop again ends
        # on the cancellation this test finishes with instead of running on.
        cancel_rejected = _QueueIterFailingCancel(shared)
        fire_queue = MagicMock()
        fire_queue.iterator = MagicMock(side_effect=[cancel_rejected, _QueueIterCtx([])])
        queues = {"orders": sub_queue, _FIRE_QUEUE: fire_queue}
        channel = _make_live_channel()
        channel.declare_queue = AsyncMock(side_effect=lambda name, **kwargs: queues[name])
        connection = _make_live_connection(channel=channel)

        async def publish_channel_of_a_torn_down_connection(conn_id):
            try:
                raise asyncio.CancelledError()  # the RPC task the teardown stopped
            except asyncio.CancelledError as rpc_stopped:
                raise shared from rpc_stopped

        writes: list[tuple] = []

        async def record_write(self, status, last_error=None):
            writes.append((status, last_error))

        with _record_consumer_sleeps(lambda delay: None), \
             patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_get_publish_channel",
                          side_effect=publish_channel_of_a_torn_down_connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write",
                   new=record_write):
            subscription = asyncio.create_task(
                manager._consume_subscription(
                    _sub(id=1, queue_name="orders", cooldown=300)
                )
            )
            fire = asyncio.create_task(
                manager._consume_fire_queue(connection, "rmq_default")
            )
            try:
                # The subscription meets the fault and answers it as one: it reports
                # and attaches again, leaving its own reading on the shared object
                await _wait_for(lambda: sub_queue.iterator.call_count >= 2)
                assert _SUB_ERROR in [status for status, _ in writes], (
                    "the subscription read the object as the fault of the delivery it "
                    "was handling"
                )
                await _wait_for(lambda: cancel_rejected.consumed)
                fire.cancel()
                # The loop ends quietly on its own cancellation, so what says the fire
                # consumer took it as one is that it stopped — a consumer that read the
                # object as a fault of its own goes on running, and no exception says so
                done, _ = await asyncio.wait({fire}, timeout=2.0)
                assert done, "the fire consumer kept running after its own cancellation"
            finally:
                subscription.cancel()
                fire.cancel()
                await asyncio.wait({subscription, fire}, timeout=2.0)

        assert fire_queue.iterator.call_count == 1, (
            "the fire consumer was cancelled, so it must not attach again"
        )
        assert sub_queue.iterator.call_count == 2, (
            "the subscription reported the fault and attached once more, and the attach "
            "that found nothing to consume is where it waits"
        )

    @pytest.mark.asyncio
    async def test_reconcile_warns_when_fire_connection_unavailable_after_provisioning(self):
        """reconcile() logs WARNING and leaves _fire_task unset if the connection for
        fire_conn_id is still missing from self._connections after _provision_cooldown
        runs (e.g. provisioning failed to establish/store the connection)."""
        manager = _make_manager()

        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_provision_cooldown"), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            # _connections has no entry for "rmq_default" — simulates provisioning
            # failing to establish/store the connection.
            await manager.reconcile([_sub(id=1, cooldown=300)])
            await asyncio.sleep(0)

            assert manager._fire_task is None
            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            assert any(
                "rmq_default" in m and "has no connection" in m for m in warning_messages
            ), (
                f"Expected warning about unavailable connection, got: {warning_messages}"
            )

        manager._active[1].task.cancel()
        await asyncio.gather(manager._active[1].task, return_exceptions=True)


# ---------------------------------------------------------------------------
# Tests for Task 5: exchange-mode infrastructure declares
# ---------------------------------------------------------------------------

class TestEnsureExchangeInfrastructure:
    @pytest.mark.asyncio
    async def test_declares_topic_exchange_with_alternate_exchange(self):
        """_ensure_exchange_infrastructure declares {exchange} as topic with
        alternate-exchange={exchange}.unrouted."""
        channel = AsyncMock()
        channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        channel.declare_queue = AsyncMock(return_value=AsyncMock())

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow", timeout=DEFAULT_RPC_TIMEOUT)

        exchange_calls = {c[0][0]: c for c in channel.declare_exchange.call_args_list}
        assert "jetstat.airflow" in exchange_calls
        exchange_call = exchange_calls["jetstat.airflow"]
        assert exchange_call.kwargs["type"] == aio_pika.ExchangeType.TOPIC
        assert exchange_call.kwargs["durable"] is True
        assert exchange_call.kwargs["arguments"] == {"alternate-exchange": "jetstat.airflow.unrouted"}

    @pytest.mark.asyncio
    async def test_declares_unrouted_fanout_exchange(self):
        """_ensure_exchange_infrastructure declares {exchange}.unrouted as a durable fanout."""
        channel = AsyncMock()
        channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        channel.declare_queue = AsyncMock(return_value=AsyncMock())

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow", timeout=DEFAULT_RPC_TIMEOUT)

        exchange_calls = {c[0][0]: c for c in channel.declare_exchange.call_args_list}
        assert "jetstat.airflow.unrouted" in exchange_calls
        exchange_call = exchange_calls["jetstat.airflow.unrouted"]
        assert exchange_call.kwargs["type"] == aio_pika.ExchangeType.FANOUT
        assert exchange_call.kwargs["durable"] is True

    @pytest.mark.asyncio
    async def test_declares_unrouted_queue_with_ttl_and_binds_to_fanout(self):
        """_ensure_exchange_infrastructure declares {exchange}.unrouted queue with
        x-message-ttl and binds it to the fanout exchange with no routing key."""
        channel = AsyncMock()
        unrouted_exchange_obj = AsyncMock()
        log_exchange_obj = AsyncMock()
        channel.declare_exchange = AsyncMock(
            side_effect=[log_exchange_obj, unrouted_exchange_obj]
        )
        unrouted_queue = AsyncMock()
        log_queue = AsyncMock()
        channel.declare_queue = AsyncMock(side_effect=[unrouted_queue, log_queue])

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow", timeout=DEFAULT_RPC_TIMEOUT)

        queue_calls = {c[0][0]: c for c in channel.declare_queue.call_args_list}
        assert "jetstat.airflow.unrouted" in queue_calls
        queue_call = queue_calls["jetstat.airflow.unrouted"]
        assert queue_call.kwargs["durable"] is True
        assert queue_call.kwargs["arguments"] == {"x-message-ttl": _EXCHANGE_TTL_MS}
        unrouted_queue.bind.assert_called_once_with(unrouted_exchange_obj)

    @pytest.mark.asyncio
    async def test_declares_log_queue_with_ttl_and_catchall_binding(self):
        """_ensure_exchange_infrastructure declares {exchange}.log queue with x-message-ttl
        and binds it to {exchange} with routing_key='#'."""
        channel = AsyncMock()
        exchange_obj = AsyncMock()
        unrouted_exchange_obj = AsyncMock()
        channel.declare_exchange = AsyncMock(side_effect=[exchange_obj, unrouted_exchange_obj])
        unrouted_queue = AsyncMock()
        log_queue = AsyncMock()
        channel.declare_queue = AsyncMock(side_effect=[unrouted_queue, log_queue])

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow", timeout=DEFAULT_RPC_TIMEOUT)

        queue_calls = {c[0][0]: c for c in channel.declare_queue.call_args_list}
        assert "jetstat.airflow.log" in queue_calls
        queue_call = queue_calls["jetstat.airflow.log"]
        assert queue_call.kwargs["durable"] is True
        assert queue_call.kwargs["arguments"] == {"x-message-ttl": _EXCHANGE_TTL_MS}
        log_queue.bind.assert_called_once_with(exchange_obj, routing_key="#")

    @pytest.mark.asyncio
    async def test_idempotent_no_exception_on_second_call(self):
        """_ensure_exchange_infrastructure is idempotent — second call must not raise."""
        channel = AsyncMock()
        channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        channel.declare_queue = AsyncMock(return_value=AsyncMock())

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow", timeout=DEFAULT_RPC_TIMEOUT)
        await _ensure_exchange_infrastructure(channel, "jetstat.airflow", timeout=DEFAULT_RPC_TIMEOUT)

        assert channel.declare_exchange.call_count == 4
        assert channel.declare_queue.call_count == 4


class TestEnsureSubQueue:
    @pytest.mark.asyncio
    async def test_declares_queue_with_ttl_argument(self):
        """_ensure_sub_queue declares rmq_watcher.sub.{dag_id} with x-message-ttl."""
        channel = AsyncMock()
        queue_mock = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue_mock)

        result = await _ensure_sub_queue(channel, "my_dag", timeout=DEFAULT_RPC_TIMEOUT)

        channel.declare_queue.assert_called_once_with(
            f"{_SUB_QUEUE_PREFIX}my_dag",
            durable=True,
            arguments={"x-message-ttl": _EXCHANGE_TTL_MS},
        )
        assert result is queue_mock

    @pytest.mark.asyncio
    async def test_queue_name_contains_dag_id(self):
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=AsyncMock())

        await _ensure_sub_queue(channel, "special_dag_123", timeout=DEFAULT_RPC_TIMEOUT)

        call_args = channel.declare_queue.call_args
        assert "special_dag_123" in call_args[0][0]
        assert call_args[0][0] == f"{_SUB_QUEUE_PREFIX}special_dag_123"

    @pytest.mark.asyncio
    async def test_idempotent_no_exception_on_second_call(self):
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=AsyncMock())

        await _ensure_sub_queue(channel, "my_dag", timeout=DEFAULT_RPC_TIMEOUT)
        await _ensure_sub_queue(channel, "my_dag", timeout=DEFAULT_RPC_TIMEOUT)

        assert channel.declare_queue.call_count == 2


class TestPreconditionFailedRecognition:
    @pytest.mark.asyncio
    async def test_precondition_failed_logged_distinctly_and_does_not_break_other_groups(self):
        """A ChannelPreconditionFailed from declare_exchange (conflicting exchange properties)
        is logged as a distinct conflict, and other groups still get provisioned."""
        manager = _make_manager()
        manager._http_client = AsyncMock()

        good_setup_channel = AsyncMock()
        good_setup_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        good_setup_channel.declare_queue = AsyncMock(return_value=AsyncMock())
        good_connection = AsyncMock()
        good_connection.channel = AsyncMock(return_value=good_setup_channel)

        bad_setup_channel = AsyncMock()
        bad_setup_channel.declare_exchange = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelPreconditionFailed("PRECONDITION_FAILED")
        )
        bad_connection = AsyncMock()
        bad_connection.channel = AsyncMock(return_value=bad_setup_channel)

        async def fake_get_conn(conn_id, **kwargs):
            return bad_connection if conn_id == "bad_conn" else good_connection

        conn_info = MagicMock()
        conn_info.extra_dejson = {"management_url": "https://mq.example.com"}
        conn_info.schema = "/"
        conn_info.login = "guest"
        conn_info.password = "guest"

        with patch.object(manager, "_get_or_create_connection", side_effect=fake_get_conn), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock(return_value=set())), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="dag_bad", exchange="conflicting.exchange",
                               conn_id="bad_conn"),
                _exchange_sub(id=2, dag_id="dag_good", exchange="jetstat.airflow",
                              conn_id="good_conn"),
            ])

        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("PRECONDITION_FAILED" in m or "conflicting" in m.lower()
                   for m in error_messages), error_messages
        assert any("conflicting.exchange" in m for m in error_messages)
        # The good group must still be provisioned despite the bad group's failure
        assert "dag_good" in manager._exchange_tracker._provisioned
        assert "dag_bad" not in manager._exchange_tracker._provisioned


# ---------------------------------------------------------------------------
# Tests for Task 5: _check_orphaned_exchange_bindings
# ---------------------------------------------------------------------------

class TestCheckOrphanedExchangeBindings:
    def test_warning_logged_on_new_orphan(self):
        manager = _make_manager()
        manager._exchange_tracker.mark_provisioned({"orphaned_dag"})

        with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            manager._check_orphaned_exchange_bindings({"active_dag"})
            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            assert any("orphaned_dag" in m for m in warning_messages)

    def test_no_duplicate_warning_on_repeated_cycles(self):
        manager = _make_manager()
        manager._exchange_tracker.mark_provisioned({"orphaned_dag"})
        manager._check_orphaned_exchange_bindings({"active_dag"})  # first warning

        with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            manager._check_orphaned_exchange_bindings({"active_dag"})
            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            assert not any("orphaned_dag" in m for m in warning_messages)

    def test_info_logged_when_subscription_restored(self):
        manager = _make_manager()
        manager._exchange_tracker.mark_provisioned({"my_dag"})
        manager._check_orphaned_exchange_bindings(set())  # orphaned

        with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            manager._check_orphaned_exchange_bindings({"my_dag"})  # restored
            info_messages = [str(c) for c in mock_log.info.call_args_list]
            assert any("my_dag" in m for m in info_messages)


# ---------------------------------------------------------------------------
# Tests for Task 5: _sync_bindings
# ---------------------------------------------------------------------------

class TestSyncBindings:
    @pytest.mark.asyncio
    async def test_binds_only_missing_keys(self):
        queue = AsyncMock()
        queue.name = "rmq_watcher.sub.my_dag"

        await _sync_bindings(queue, "jetstat.airflow", {"a.succeeded", "b.failed"}, {"a.succeeded"}, timeout=DEFAULT_RPC_TIMEOUT)

        queue.bind.assert_called_once_with("jetstat.airflow", routing_key="b.failed")
        queue.unbind.assert_not_called()

    @pytest.mark.asyncio
    async def test_unbinds_only_stale_keys(self):
        queue = AsyncMock()
        queue.name = "rmq_watcher.sub.my_dag"

        await _sync_bindings(queue, "jetstat.airflow", {"a.succeeded"}, {"a.succeeded", "old.key"}, timeout=DEFAULT_RPC_TIMEOUT)

        queue.unbind.assert_called_once_with("jetstat.airflow", routing_key="old.key")
        queue.bind.assert_not_called()

    @pytest.mark.asyncio
    async def test_noop_when_desired_equals_current(self):
        queue = AsyncMock()
        queue.name = "rmq_watcher.sub.my_dag"

        await _sync_bindings(queue, "jetstat.airflow", {"a.succeeded"}, {"a.succeeded"}, timeout=DEFAULT_RPC_TIMEOUT)

        queue.bind.assert_not_called()
        queue.unbind.assert_not_called()

    @pytest.mark.asyncio
    async def test_binds_and_unbinds_together(self):
        queue = AsyncMock()
        queue.name = "rmq_watcher.sub.my_dag"

        await _sync_bindings(
            queue, "jetstat.airflow",
            desired={"new.key"}, current={"old.key"},
            timeout=DEFAULT_RPC_TIMEOUT,
        )

        queue.bind.assert_called_once_with("jetstat.airflow", routing_key="new.key")
        queue.unbind.assert_called_once_with("jetstat.airflow", routing_key="old.key")


# ---------------------------------------------------------------------------
# Tests for Task 5: _provision_exchange_subs
# ---------------------------------------------------------------------------

class TestProvisionExchangeSubs:
    def _conn_info(self, management_url: str | None = "https://mq.example.com"):
        conn_info = MagicMock()
        conn_info.extra_dejson = {"management_url": management_url} if management_url else {}
        conn_info.schema = "/"
        conn_info.login = "guest"
        conn_info.password = "guest"
        return conn_info

    def _make_setup(self):
        """Build (connection, setup_channel, sub_queue_mock) with declare_queue returning a
        distinct AsyncMock per queue name, so that bind/unbind assertions on the sub queue
        are not polluted by the .unrouted/.log infrastructure queues declared by
        _ensure_exchange_infrastructure."""
        queues_by_name: dict[str, AsyncMock] = {}

        async def declare_queue(name, **kwargs):
            if name not in queues_by_name:
                q = AsyncMock()
                q.name = name
                queues_by_name[name] = q
            return queues_by_name[name]

        setup_channel = AsyncMock()
        setup_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        setup_channel.declare_queue = AsyncMock(side_effect=declare_queue)
        connection = AsyncMock()
        connection.channel = AsyncMock(return_value=setup_channel)
        sub_queue_mock = AsyncMock()
        sub_queue_mock.name = f"{_SUB_QUEUE_PREFIX}test_dag"
        queues_by_name[f"{_SUB_QUEUE_PREFIX}test_dag"] = sub_queue_mock
        return connection, setup_channel, sub_queue_mock

    @pytest.mark.asyncio
    async def test_happy_path_declares_and_binds_and_marks_provisioned(self):
        manager = _make_manager()
        manager._http_client = AsyncMock()
        connection, setup_channel, queue_mock = self._make_setup()
        conn_info = self._conn_info()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock(return_value={"old.key"})) as mock_get_bindings:
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="test_dag", exchange="jetstat.airflow",
                              routing_keys=["a.succeeded", "old.key"]),
            ])

        mock_get_bindings.assert_awaited_once()
        # bind-diff: desired={"a.succeeded","old.key"} vs current={"old.key"} → bind a.succeeded
        queue_mock.bind.assert_called_once_with("jetstat.airflow", routing_key="a.succeeded")
        queue_mock.unbind.assert_not_called()
        assert "test_dag" in manager._exchange_tracker._provisioned
        setup_channel.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_missing_management_url_skips_bind_diff_but_declares_queue(self):
        manager = _make_manager()
        manager._http_client = AsyncMock()
        connection, setup_channel, queue_mock = self._make_setup()
        conn_info = self._conn_info(management_url=None)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock()) as mock_get_bindings, \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            # Must not raise despite missing management_url
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="test_dag", exchange="jetstat.airflow"),
            ])

        mock_get_bindings.assert_not_awaited()
        queue_mock.bind.assert_not_called()
        queue_mock.unbind.assert_not_called()
        # Sub queue declare still happened (alongside .unrouted/.log infra queues)
        declared_names = [c.args[0] for c in setup_channel.declare_queue.call_args_list]
        assert f"{_SUB_QUEUE_PREFIX}test_dag" in declared_names
        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("management_url" in m for m in error_messages)
        # mark_provisioned still happens for the group — queue itself was successfully declared
        assert "test_dag" in manager._exchange_tracker._provisioned

    @pytest.mark.asyncio
    async def test_missing_login_logs_error_and_returns_false_without_raising(self):
        manager = _make_manager()
        manager._http_client = AsyncMock()
        connection, setup_channel, queue_mock = self._make_setup()
        conn_info = self._conn_info()
        conn_info.login = None

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock()) as mock_get_bindings, \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            # Must not raise despite conn_info.login being None
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="test_dag", exchange="jetstat.airflow"),
            ])

        mock_get_bindings.assert_not_awaited()
        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("login" in m or "password" in m for m in error_messages)
        # Auth fields missing → provisioning of this DAG is treated as failed.
        assert "test_dag" not in manager._exchange_tracker._provisioned

    @pytest.mark.asyncio
    async def test_missing_password_logs_error_and_returns_false_without_raising(self):
        manager = _make_manager()
        manager._http_client = AsyncMock()
        connection, setup_channel, queue_mock = self._make_setup()
        conn_info = self._conn_info()
        conn_info.password = None

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock()) as mock_get_bindings, \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            # Must not raise despite conn_info.password being None
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="test_dag", exchange="jetstat.airflow"),
            ])

        mock_get_bindings.assert_not_awaited()
        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("login" in m or "password" in m for m in error_messages)
        assert "test_dag" not in manager._exchange_tracker._provisioned

    @pytest.mark.asyncio
    async def test_management_api_error_logged_and_skipped_does_not_affect_other_groups(self):
        manager = _make_manager()
        manager._http_client = AsyncMock()

        conn_info = self._conn_info()

        connection_a, setup_channel_a, queue_a = self._make_setup()
        connection_b, setup_channel_b, queue_b = self._make_setup()

        async def fake_get_conn(conn_id, **kwargs):
            return connection_a if conn_id == "conn_a" else connection_b

        call_count = 0

        async def flaky_get_bindings(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.ConnectError("connection refused")
            return set()

        with patch.object(manager, "_get_or_create_connection", side_effect=fake_get_conn), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   side_effect=flaky_get_bindings), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="dag_a", exchange="exchange_a", conn_id="conn_a"),
                _exchange_sub(id=2, dag_id="dag_b", exchange="exchange_b", conn_id="conn_b"),
            ])

        warnings = [str(c) for c in mock_log.warning.call_args_list]
        assert any("dag_a" in m for m in warnings), (
            "a Management API call that failed this cycle is retried, not acted on"
        )
        # dag_a's bind-diff failed, but the queue is still declared (group is "provisioned")
        assert "dag_a" in manager._exchange_tracker._provisioned
        # dag_b's group is entirely unaffected by dag_a's Management API error
        assert "dag_b" in manager._exchange_tracker._provisioned
        queue_b.bind.assert_not_called()  # desired == current == empty set

    @pytest.mark.asyncio
    async def test_get_connection_called_via_the_cycle_pool(self):
        """BaseHook.get_connection is a metadata-database query: it must leave the
        coroutine, and on a reconcile-cycle path it must go to the cycle pool so a stuck
        delivery and a stuck cycle never queue behind each other."""
        manager = _make_manager()
        manager._http_client = AsyncMock()
        connection, setup_channel, queue_mock = self._make_setup()
        conn_info = self._conn_info()

        cycle_calls = []
        consumer_calls = []

        async def capture_cycle(func, *args):
            cycle_calls.append((func, args))
            return func(*args)

        async def capture_consumer(func, *args):
            consumer_calls.append((func, args))
            return func(*args)

        manager._cycle_executor = _FakeExecutor(capture_cycle)
        manager._executor = _FakeExecutor(capture_consumer)
        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info) as mock_get_connection, \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock(return_value=set())):
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="test_dag", exchange="jetstat.airflow"),
            ])

        assert any(func is mock_get_connection for func, _ in cycle_calls), (
            "BaseHook.get_connection must be invoked through the cycle pool"
        )
        assert consumer_calls == [], "the cycle must not borrow a consumer worker"

    @pytest.mark.asyncio
    async def test_empty_exchange_subs_is_noop(self):
        manager = _make_manager()
        manager._http_client = AsyncMock()

        with patch.object(manager, "_get_or_create_connection") as mock_get_conn:
            await manager._provision_exchange_subs([])

        mock_get_conn.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_http_client_logs_error_and_does_not_raise(self):
        """If start() was never called, self._http_client is None — must not crash,
        just log ERROR and skip provisioning for this cycle."""
        manager = _make_manager()
        assert manager._http_client is None

        with patch.object(manager, "_get_or_create_connection") as mock_get_conn, \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="test_dag", exchange="jetstat.airflow"),
            ])

        mock_get_conn.assert_not_called()
        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("http client" in m.lower() or "start()" in m for m in error_messages)

    @pytest.mark.asyncio
    async def test_connection_error_logged_and_does_not_raise(self):
        manager = _make_manager()
        manager._http_client = AsyncMock()

        async def fail_get_conn(conn_id):
            raise ConnectionError("broker unavailable")

        with patch.object(manager, "_get_or_create_connection", side_effect=fail_get_conn), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            # Must not raise
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="test_dag", exchange="jetstat.airflow"),
            ])

        assert "test_dag" not in manager._exchange_tracker._provisioned
        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("jetstat.airflow" in m for m in error_messages)

    @pytest.mark.asyncio
    async def test_groups_by_conn_id_and_exchange_one_declare_per_group(self):
        """Two subscriptions sharing (conn_id, exchange) declare the exchange infra once."""
        manager = _make_manager()
        manager._http_client = AsyncMock()
        connection, setup_channel, queue_mock = self._make_setup()
        conn_info = self._conn_info()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock(return_value=set())):
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="dag_one", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
                _exchange_sub(id=2, dag_id="dag_two", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
            ])

        # declare_exchange is called 3x per _ensure_exchange_infrastructure invocation
        # (exchange, .unrouted) — should happen exactly once for the shared group
        exchange_call_names = [c[0][0] for c in setup_channel.declare_exchange.call_args_list]
        assert exchange_call_names.count("jetstat.airflow") == 1
        assert "dag_one" in manager._exchange_tracker._provisioned
        assert "dag_two" in manager._exchange_tracker._provisioned

    @pytest.mark.asyncio
    async def test_per_subscription_failure_does_not_block_mark_provisioned_for_others(self):
        """If _ensure_sub_queue/bind-diff raises for the second DAG in a shared-exchange
        group, the first DAG's already-successful provisioning must still be recorded —
        otherwise it would never trip the orphan-detection safety net later (see
        Implementation agent finding 1, code review iteration 2)."""
        manager = _make_manager()
        manager._http_client = AsyncMock()
        connection, setup_channel, queue_mock = self._make_setup()
        conn_info = self._conn_info()

        async def declare_queue(name, **kwargs):
            if name == f"{_SUB_QUEUE_PREFIX}dag_two":
                raise aio_pika.exceptions.ChannelPreconditionFailed(
                    "406", "PRECONDITION_FAILED - inequivalent arg"
                )
            return await setup_channel._declare_queue_orig(name, **kwargs)

        setup_channel._declare_queue_orig = setup_channel.declare_queue
        setup_channel.declare_queue = AsyncMock(side_effect=declare_queue)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock(return_value=set())), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="dag_one", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
                _exchange_sub(id=2, dag_id="dag_two", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
            ])

        # dag_one succeeded earlier in the loop — must be marked provisioned despite
        # dag_two's failure later in the same group.
        assert "dag_one" in manager._exchange_tracker._provisioned
        assert "dag_two" not in manager._exchange_tracker._provisioned
        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("dag_two" in m and "PRECONDITION_FAILED" in m for m in error_messages)

    @pytest.mark.asyncio
    async def test_channel_reopened_after_precondition_failed_so_third_dag_still_succeeds(self):
        """In real AMQP, PRECONDITION_FAILED closes the entire broker-side channel —
        ChannelPreconditionFailed is a ChannelClosed subclass, and aiormq raises for ANY
        further RPC on a closed channel. This test simulates that: once dag_two's declare
        raises ChannelPreconditionFailed on the first channel, that SAME channel object is
        made permanently unusable (any further call raises ChannelInvalidStateError-like
        error). dag_three must still succeed, proving the code requests a fresh channel via
        connection.channel() instead of reusing the dead one (see code review iteration 4)."""
        manager = _make_manager()
        manager._http_client = AsyncMock()
        conn_info = self._conn_info()

        first_connection, first_setup_channel, _ = self._make_setup()
        second_connection, second_setup_channel, queue_three = self._make_setup()
        # second_setup_channel's declare_queue already resolves dag_three's queue via
        # _make_setup's generic declare_queue side_effect.

        channel_call_count = 0

        async def connection_channel():
            nonlocal channel_call_count
            channel_call_count += 1
            return first_setup_channel if channel_call_count == 1 else second_setup_channel

        connection = AsyncMock()
        connection.channel = AsyncMock(side_effect=connection_channel)

        first_channel_dead = False

        async def declare_queue_first_channel(name, **kwargs):
            nonlocal first_channel_dead
            if first_channel_dead:
                # Broker-side channel is closed — any further RPC on it raises.
                raise aio_pika.exceptions.ChannelInvalidStateError(
                    "channel closed due to prior PRECONDITION_FAILED"
                )
            if name == f"{_SUB_QUEUE_PREFIX}dag_two":
                first_channel_dead = True
                raise aio_pika.exceptions.ChannelPreconditionFailed(
                    "406", "PRECONDITION_FAILED - inequivalent arg"
                )
            return await first_setup_channel._declare_queue_orig(name, **kwargs)

        first_setup_channel._declare_queue_orig = first_setup_channel.declare_queue
        first_setup_channel.declare_queue = AsyncMock(side_effect=declare_queue_first_channel)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock(return_value=set())), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="dag_one", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
                _exchange_sub(id=2, dag_id="dag_two", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
                _exchange_sub(id=3, dag_id="dag_three", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
            ])

        # A fresh channel was requested after the PRECONDITION_FAILED (2 total: original +
        # reopened one for dag_three).
        assert channel_call_count == 2
        assert "dag_one" in manager._exchange_tracker._provisioned
        assert "dag_two" not in manager._exchange_tracker._provisioned
        assert "dag_three" in manager._exchange_tracker._provisioned
        # dag_three's queue was declared on the SECOND (fresh) channel, not the dead one.
        declared_on_second = [
            c.args[0] for c in second_setup_channel.declare_queue.call_args_list
        ]
        assert f"{_SUB_QUEUE_PREFIX}dag_three" in declared_on_second
        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("dag_two" in m and "PRECONDITION_FAILED" in m for m in error_messages)

    @pytest.mark.asyncio
    async def test_channel_reopened_after_other_channel_closed_subtype(self):
        """ChannelPreconditionFailed is not the only exception that closes the broker-side
        channel — ChannelNotFoundEntity and DuplicateConsumerTag are sibling ChannelClosed
        subclasses (NOT subclasses of ChannelPreconditionFailed) raised by declare_queue/
        queue.bind/queue.unbind on a broker-rejected operation (e.g. a permissions gap
        raising ChannelAccessRefused in real AMQP). This test uses ChannelNotFoundEntity to
        prove the channel-reopen path is not narrowly scoped to ChannelPreconditionFailed
        (see code review phase 1 iteration 5 / phase 4 finding)."""
        manager = _make_manager()
        manager._http_client = AsyncMock()
        conn_info = self._conn_info()

        first_connection, first_setup_channel, _ = self._make_setup()
        second_connection, second_setup_channel, queue_three = self._make_setup()

        channel_call_count = 0

        async def connection_channel():
            nonlocal channel_call_count
            channel_call_count += 1
            return first_setup_channel if channel_call_count == 1 else second_setup_channel

        connection = AsyncMock()
        connection.channel = AsyncMock(side_effect=connection_channel)

        first_channel_dead = False

        async def declare_queue_first_channel(name, **kwargs):
            nonlocal first_channel_dead
            if first_channel_dead:
                raise aio_pika.exceptions.ChannelInvalidStateError(
                    "channel closed due to prior ChannelClosed subtype"
                )
            if name == f"{_SUB_QUEUE_PREFIX}dag_two":
                first_channel_dead = True
                raise aio_pika.exceptions.ChannelNotFoundEntity(
                    "404", "NOT_FOUND - no queue 'rmq_watcher.sub.dag_two' in vhost '/'"
                )
            return await first_setup_channel._declare_queue_orig(name, **kwargs)

        first_setup_channel._declare_queue_orig = first_setup_channel.declare_queue
        first_setup_channel.declare_queue = AsyncMock(side_effect=declare_queue_first_channel)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                   new=AsyncMock(return_value=set())), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._provision_exchange_subs([
                _exchange_sub(id=1, dag_id="dag_one", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
                _exchange_sub(id=2, dag_id="dag_two", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
                _exchange_sub(id=3, dag_id="dag_three", exchange="jetstat.airflow",
                              conn_id="rmq_default"),
            ])

        # A fresh channel was requested after the non-PRECONDITION_FAILED ChannelClosed
        # subtype (2 total: original + reopened one for dag_three).
        assert channel_call_count == 2
        assert "dag_one" in manager._exchange_tracker._provisioned
        assert "dag_two" not in manager._exchange_tracker._provisioned
        assert "dag_three" in manager._exchange_tracker._provisioned
        declared_on_second = [
            c.args[0] for c in second_setup_channel.declare_queue.call_args_list
        ]
        assert f"{_SUB_QUEUE_PREFIX}dag_three" in declared_on_second
        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("dag_two" in m for m in error_messages)
        # Must NOT use the PRECONDITION_FAILED-specific wording for this subtype.
        assert not any(
            "dag_two" in m and "PRECONDITION_FAILED" in m for m in error_messages
        )


# ---------------------------------------------------------------------------
# Tests for Task 5: reconcile() ordering — exchange provisioning before consumer start
# ---------------------------------------------------------------------------

class TestReconcileExchangeProvisioningOrder:
    @pytest.mark.asyncio
    async def test_exchange_provisioning_awaited_before_consumer_task_created(self):
        """reconcile() must await _provision_exchange_subs before creating the
        asyncio.create_task for a new exchange-mode subscription — otherwise the new
        consumer task's passive declare could race against an unprovisioned queue."""
        manager = _make_manager()
        call_order = []

        async def fake_provision(exchange_subs):
            if exchange_subs:
                call_order.append("provision")

        async def blocking_consume(sub):
            call_order.append("consume_start")
            await asyncio.Future()

        with patch.object(manager, "_provision_exchange_subs", side_effect=fake_provision), \
             patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_exchange_sub(id=1)])
            await asyncio.sleep(0)  # let the new consumer task body start

            manager._active[1].task.cancel()
            await asyncio.gather(manager._active[1].task, return_exceptions=True)

        assert call_order == ["provision", "consume_start"]

    @pytest.mark.asyncio
    async def test_exchange_provisioning_called_with_only_exchange_subs(self):
        """_provision_exchange_subs receives only the subset of subscriptions that declare
        exchange=, not plain queue= subscriptions."""
        manager = _make_manager()
        received = []

        async def fake_provision(exchange_subs):
            received.extend(exchange_subs)

        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_provision_exchange_subs", side_effect=fake_provision), \
             patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_sub(id=1), _exchange_sub(id=2, dag_id="exchange_dag")])

            for entry in manager._active.values():
                entry.task.cancel()
            await asyncio.gather(*(e.task for e in manager._active.values()), return_exceptions=True)

        assert len(received) == 1
        assert received[0]["dag_id"] == "exchange_dag"


# ---------------------------------------------------------------------------
# Tests for Task 5: start()/stop() manage the Management API HTTP client
# ---------------------------------------------------------------------------

class TestHttpClientLifecycle:
    @pytest.mark.asyncio
    async def test_start_creates_http_client(self):
        manager = _make_manager()
        assert manager._http_client is None
        await manager.start()
        try:
            assert manager._http_client is not None
            assert isinstance(manager._http_client, httpx.AsyncClient)
        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_http_client(self):
        manager = _make_manager()
        await manager.start()
        client = manager._http_client
        with patch.object(client, "aclose", new=AsyncMock()) as mock_aclose:
            await manager.stop()
        mock_aclose.assert_awaited_once()
        assert manager._http_client is None


# ---------------------------------------------------------------------------
# Tests for connection health, roles, timeouts and consumer tags
# ---------------------------------------------------------------------------

def _make_live_connection(channel=None, queue=None):
    """AsyncMock connection that reports itself open.

    ``queue`` is shorthand for the common case: a live channel that passive-declares
    its way to that queue.
    """
    if queue is not None:
        channel = _make_live_channel(queue=queue)
    connection = AsyncMock()
    connection.is_closed = False
    if channel is not None:
        connection.channel = AsyncMock(return_value=channel)
    return connection


def _make_live_channel(queue=None):
    channel = AsyncMock()
    channel.is_closed = False
    channel.default_exchange = AsyncMock()
    channel.default_exchange.publish = AsyncMock()
    if queue is not None:
        channel.declare_queue = AsyncMock(return_value=queue)
    return channel


def _make_conn_info():
    conn_info = MagicMock()
    conn_info.extra_dejson = {}
    conn_info.schema = "/"
    conn_info.port = None
    conn_info.login = "guest"
    conn_info.password = "guest"
    conn_info.host = "localhost"
    return conn_info


def _hanging_call(*args, **kwargs):
    """Coroutine that never resolves — models a zombie connection swallowing an RPC."""
    return asyncio.Future()


def _fast_timeouts(manager) -> None:
    manager._conn("rmq_default").timeouts = AmqpTimeouts(connect=0.05, rpc=0.05)


class _BlockedBroker:
    """An ``aiormq`` connection that completes its handshake and stops there.

    ``Connection.ready()`` waits out a ``Connection.Blocked`` frame, which the broker
    sends to every client advertising the capability — ``aiormq`` does — for as long as
    a resource alarm lasts.
    """

    def __init__(self) -> None:
        self.closing = asyncio.get_event_loop().create_future()
        self.closed = False

    async def ready(self) -> None:
        await asyncio.Event().wait()

    async def close(self, exc=None) -> None:
        self.closed = True
        if not self.closing.done():
            self.closing.set_result(None)

    @property
    def is_closed(self) -> bool:
        return self.closed


@contextlib.contextmanager
def _blocked_broker():
    """Patch the socket-level connect, leaving every ``aio_pika`` path real."""
    opened = []

    async def connect(url, **kwargs):
        opened.append(_BlockedBroker())
        return opened[-1]

    with patch.object(aiormq, "connect", connect):
        yield opened


class TestConnectionSetup:
    """Building a connection is bounded, cancellable, and costs one conn_id at a time.

    ``connect_robust(timeout=...)`` bounds the TCP connect and the AMQP handshake only.
    What follows is ``ready()``, which waits for the broker to declare the connection
    unblocked, and a broker under a resource alarm keeps every connection blocked until
    the alarm clears — so an unbounded connect is a connect that can never come back.
    The bound therefore belongs here, and the wait for it outside the lock that starts
    the attempt.
    """

    @pytest.mark.asyncio
    async def test_a_connect_the_broker_never_finishes_is_bounded(self, manager):
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": 0.2}

        with _blocked_broker() as opened, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer._write_conn_error"), \
             _patch_watcher_session():
            started = time.monotonic()
            with pytest.raises(asyncio.TimeoutError) as caught:
                await asyncio.wait_for(
                    manager._get_or_create_connection("rmq_default"), timeout=5
                )
            waited = time.monotonic() - started

            await manager._drop_connection("rmq_default")

        assert "did not connect" in str(caught.value), (
            "the timeout must be the one this call sets, not the test's own"
        )
        assert waited < 2, f"the connect was not bounded: {waited:.1f}s"
        assert len(opened) == 1

    @pytest.mark.asyncio
    async def test_a_connect_that_timed_out_is_pooled_and_not_repeated(self, manager):
        """The attempt stays in the pool: it is a live connection to the broker, and one
        that is in no pool is one nothing can ever close."""
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": 0.2}

        with _blocked_broker() as opened, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer._write_conn_error"), \
             _patch_watcher_session():
            for _ in range(3):
                with pytest.raises(asyncio.TimeoutError):
                    await manager._get_or_create_connection("rmq_default")

            state = manager._conn("rmq_default")
            assert state.connections[_ROLE_CONSUME] is not None
            assert len(opened) == 1, "each attempt opened another connection to the broker"

            await manager._drop_connection("rmq_default")
            assert _pooled_connections(manager) == []
            assert state.connecting == {}

    @pytest.mark.asyncio
    async def test_a_cancelled_caller_leaves_its_connection_in_the_pool(self, manager):
        """Cancelling a consumer task mid-connect is routine — recovery, a subscription
        edit and ``stop()`` all do it — and must not leave a broker connection behind."""
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": 30}

        with _blocked_broker() as opened, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             _patch_watcher_session():
            task = asyncio.create_task(manager._get_or_create_connection("rmq_default"))
            while _ROLE_CONSUME not in manager._conn("rmq_default").connecting:
                await asyncio.sleep(0.01)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

            state = manager._conn("rmq_default")
            assert state.connections[_ROLE_CONSUME] is not None, (
                "the connection the cancelled caller opened is in no pool and nothing "
                "will ever close it"
            )
            assert len(opened) == 1

            connect_task = state.connecting[_ROLE_CONSUME]
            await manager.stop()
            assert _pooled_connections(manager) == []
            assert connect_task.cancelled() or connect_task.done()

    @pytest.mark.asyncio
    async def test_one_broker_that_stopped_answering_holds_up_only_its_own_conn_id(self):
        """The reconcile cycle's own calls must not queue behind every consumer task.

        A manager-wide lock makes that wait grow with the number of subscriptions, and
        once it passes the cycle's budget the manager is torn down and the consumer tasks
        of every conn_id — the healthy ones included — are cancelled with it.
        """
        manager = _make_manager()
        stuck_conn_ids = [f"broker_{i}" for i in range(10)]
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": 0.1}

        def new_connection(*args, **kwargs):
            connection = _make_live_connection()
            connection.connect = _hanging_call
            return connection

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   side_effect=new_connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer._write_conn_error"), \
             _patch_watcher_session():
            stuck = [
                asyncio.ensure_future(manager._get_or_create_connection(conn_id))
                for conn_id in stuck_conn_ids
            ]
            await asyncio.sleep(0)  # every one of them is inside the slow path

            started = time.monotonic()
            with pytest.raises(asyncio.TimeoutError):
                await manager._get_or_create_connection(
                    "cycle_probe", executor=manager._cycle_executor
                )
            waited = time.monotonic() - started
            await asyncio.gather(*stuck, return_exceptions=True)

        assert waited < 0.5, (
            f"the cycle waited {waited:.2f}s on {len(stuck_conn_ids)} conn_ids it does "
            f"not share a broker with — the wait grows with every subscription"
        )

    @pytest.mark.asyncio
    async def test_callers_of_one_conn_id_wait_for_the_same_attempt_side_by_side(self):
        """Waiting for the connect under the lock costs each caller the full
        connect_timeout in turn, and most deployments put every subscription on one
        conn_id. The reconcile cycle needs that same lock, so the wait it inherits grows
        with the number of subscriptions until it outlasts the cycle's budget — and the
        manager is torn down over one broker that stopped answering.
        """
        manager = _make_manager()
        callers = 6
        connect_timeout = 0.3
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": connect_timeout}

        with _blocked_broker() as opened, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer._write_conn_error"), \
             _patch_watcher_session():
            started = time.monotonic()
            outcomes = await asyncio.gather(
                *(manager._get_or_create_connection("rmq_default")
                  for _ in range(callers)),
                return_exceptions=True,
            )
            waited = time.monotonic() - started
            await manager._drop_connection("rmq_default")

        assert all(isinstance(o, asyncio.TimeoutError) for o in outcomes)
        assert len(opened) == 1, "the broker was asked for more than one connection"
        assert waited < connect_timeout * 2, (
            f"{callers} callers of one conn_id took {waited:.2f}s, about "
            f"{waited / connect_timeout:.0f} connect_timeouts — they are queueing "
            f"rather than waiting for the one attempt together"
        )

    @pytest.mark.asyncio
    async def test_a_drop_under_a_waiting_caller_is_a_transient_failure(self):
        """Recovery drops a connection while consumer tasks wait for its connect, and
        the shared connect task is cancelled with it. Reported as a cancellation, that
        would end each waiting task through its own ``except CancelledError`` branch —
        no status written, no restart counted, consumption stopped until the next
        cycle notices the task is done."""
        manager = _make_manager()
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": 30}

        with _blocked_broker(), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer._write_conn_error"), \
             _patch_watcher_session():
            waiter = asyncio.create_task(manager._get_or_create_connection("rmq_default"))
            state = manager._conn("rmq_default")
            await _wait_for(lambda: _ROLE_CONSUME in state.connecting)

            await manager._drop_connection("rmq_default")

            with pytest.raises(ConnectionError):
                await asyncio.wait_for(waiter, timeout=2.0)

    @pytest.mark.asyncio
    async def test_cancelling_the_caller_itself_stays_a_cancellation(self):
        """The waiter's own cancellation is the one case that must pass through
        untouched: recovery, a subscription edit and stop() all cancel consumer tasks,
        and a task that swallowed that would ignore stop()."""
        manager = _make_manager()
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": 30}

        with _blocked_broker(), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             _patch_watcher_session():
            waiter = asyncio.create_task(manager._get_or_create_connection("rmq_default"))
            state = manager._conn("rmq_default")
            await _wait_for(lambda: _ROLE_CONSUME in state.connecting)

            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter

            await manager._drop_connection("rmq_default")

    @pytest.mark.asyncio
    async def test_a_connection_whose_reconnect_never_finished_is_replaced(self, manager):
        """Observed in production: aio_pika's reconnect factory clears the transport
        before each attempt, and a reconnect that never finishes leaves the object in
        the pool reporting is_closed False with nothing under it. Every channel() on it
        raises RuntimeError("Connection was not opened"), so a consumer task handed that
        object fails, pauses, and is handed the same object again — for days, until the
        process is restarted."""
        broken = _make_live_connection()
        broken.transport = None
        fresh = _make_live_connection()
        state = manager._conn("rmq_default")
        state.connections[_ROLE_CONSUME] = broken

        assert state.ready(_ROLE_CONSUME) is None, "an object with no transport is not usable"

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=fresh) as new_connection, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()):
            result = await manager._get_or_create_connection("rmq_default")

        assert result is fresh
        assert state.connections[_ROLE_CONSUME] is fresh
        assert new_connection.call_count == 1
        broken.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_connect_in_flight_keeps_its_connection(self, manager):
        """A connection has no transport until its connect lands, and that object is
        what every waiter is waiting on. Replacing it mid-attempt would open a second
        connection to a broker that is already struggling."""
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": 0.2}

        with _blocked_broker() as opened, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info), \
             patch("airflow_provider_rmq.watcher.consumer._write_conn_error"), \
             _patch_watcher_session():
            for _ in range(3):
                with pytest.raises(asyncio.TimeoutError):
                    await manager._get_or_create_connection("rmq_default")

            assert len(opened) == 1, "the attempt in flight was replaced"
            await manager._drop_connection("rmq_default")

    @pytest.mark.asyncio
    async def test_a_connection_replaced_while_it_connected_is_closed(self, manager):
        """Recovery is free to replace the pooled connection while a connect is landing.
        The object this attempt built then belongs to nobody, and an open connection the
        pool cannot reach costs the broker one for the life of the process."""
        landed = asyncio.Event()
        built = _make_live_connection()
        replacement = _make_live_connection()

        async def lands_on_cue(timeout=None):
            await landed.wait()

        built.connect = lands_on_cue
        state = manager._conn("rmq_default")

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=built), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()):
            task = asyncio.create_task(manager._get_or_create_connection("rmq_default"))
            await _wait_for(lambda: _ROLE_CONSUME in state.connecting)
            state.connections[_ROLE_CONSUME] = replacement
            landed.set()
            result = await asyncio.wait_for(task, timeout=2.0)

        assert result is replacement
        built.close.assert_awaited()
        assert state.connections[_ROLE_CONSUME] is replacement

    @pytest.mark.asyncio
    async def test_a_failed_connect_leaves_the_connection_that_replaced_it_alone(
        self, manager
    ):
        """A connect that failed takes its own connection out of the pool. Whichever
        object the pool holds by then may be another caller's, and dropping that one
        would tear down a healthy connection the moment an old attempt reports back."""
        failed = asyncio.Event()
        built = _make_live_connection()
        replacement = _make_live_connection()

        async def fails_on_cue(timeout=None):
            await failed.wait()
            raise aio_pika.exceptions.AMQPConnectionError("connection refused")

        built.connect = fails_on_cue
        state = manager._conn("rmq_default")

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=built), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()), \
             patch("airflow_provider_rmq.watcher.consumer._write_conn_error"), \
             _patch_watcher_session():
            task = asyncio.create_task(manager._get_or_create_connection("rmq_default"))
            await _wait_for(lambda: _ROLE_CONSUME in state.connecting)
            state.connections[_ROLE_CONSUME] = replacement
            failed.set()
            with pytest.raises(aio_pika.exceptions.AMQPConnectionError):
                await asyncio.wait_for(task, timeout=2.0)

        assert state.connections[_ROLE_CONSUME] is replacement
        replacement.close.assert_not_awaited()


class TestConnectionPool:
    @pytest.mark.asyncio
    async def test_closed_connection_is_replaced(self, manager):
        closed = AsyncMock()
        closed.is_closed = True
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = closed
        fresh = _make_live_connection()

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=fresh) as mock_connect, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()):
            result = await manager._get_or_create_connection("rmq_default")

        assert result is fresh
        assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is fresh
        assert closed not in _pooled_connections(manager)
        assert mock_connect.call_count == 1

    @pytest.mark.asyncio
    async def test_live_connection_is_reused(self, manager):
        live = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = live

        with patch("airflow_provider_rmq.watcher.consumer._new_connection") as mock_connect:
            first = await manager._get_or_create_connection("rmq_default")
            second = await manager._get_or_create_connection("rmq_default")

        assert first is live and second is live
        mock_connect.assert_not_called()

    @pytest.mark.asyncio
    async def test_connect_gets_the_connect_timeout_from_extra(self, manager):
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": 3}
        connection = _make_live_connection()

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info):
            await manager._get_or_create_connection("rmq_default")

        assert connection.connect.await_args.kwargs["timeout"] == 3

    @pytest.mark.asyncio
    async def test_consume_and_publish_roles_are_separate_connections(self, manager):
        consume_conn = _make_live_connection()
        publish_conn = _make_live_connection()
        created = [consume_conn, publish_conn]

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   side_effect=lambda *args, **kwargs: created.pop(0)), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()):
            first = await manager._get_or_create_connection("rmq_default")
            second = await manager._get_or_create_connection("rmq_default", role=_ROLE_PUBLISH)

        assert first is consume_conn
        assert second is publish_conn
        assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is consume_conn
        assert manager._conn("rmq_default").connections[_ROLE_PUBLISH] is publish_conn

    @pytest.mark.asyncio
    async def test_drop_connection_removes_both_roles_when_close_hangs(self, manager):
        consume_conn = _make_live_connection()
        consume_conn.close = _hanging_call
        publish_conn = _make_live_connection()
        publish_conn.close = _hanging_call
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = consume_conn
        manager._conn("rmq_default").connections[_ROLE_PUBLISH] = publish_conn
        manager._conn("rmq_default").publish_channel = _make_live_channel()

        with patch("airflow_provider_rmq.watcher.consumer._CLOSE_TIMEOUT", 0.05), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._drop_connection("rmq_default")

        assert _pooled_connections(manager) == []
        assert manager._conn("rmq_default").publish_channel is None
        assert mock_log.warning.call_args_list, "a close that never returns must be logged"

    @pytest.mark.asyncio
    async def test_drop_connection_with_publish_role_keeps_consume(self, manager):
        consume_conn = _make_live_connection()
        publish_conn = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = consume_conn
        manager._conn("rmq_default").connections[_ROLE_PUBLISH] = publish_conn

        await manager._drop_connection("rmq_default", role=_ROLE_PUBLISH)

        assert _pooled_connections(manager) == [consume_conn]
        publish_conn.close.assert_awaited_once()
        consume_conn.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_reconcile_keeps_connections_of_active_subscriptions(self, manager):
        """Regression: pool keys are tuples, so comparing them with plain conn_id strings
        would close the connection under every running consumer on each cycle."""
        connection = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection

        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_sub(id=1)])

        assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is connection
        connection.close.assert_not_awaited()

        manager._active[1].task.cancel()
        await asyncio.gather(manager._active[1].task, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_reconcile_closes_connections_of_removed_conn_id(self, manager):
        connection = _make_live_connection()
        manager._conn("gone_conn").connections[_ROLE_CONSUME] = connection

        with patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([])

        assert _pooled_connections(manager) == []
        connection.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_fire_task_starts_on_pooled_consume_connection(self, manager):
        """Regression: the fire task reads the pool by ``(conn_id, role)``; a plain
        conn_id lookup returns nothing and cooldown DAGs stop being triggered."""
        setup_channel = _make_live_channel(queue=AsyncMock())
        setup_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        connection = _make_live_connection(channel=setup_channel)

        async def blocking_consume(sub):
            await asyncio.Future()

        async def blocking_fire(conn, conn_id=None):
            await asyncio.Future()

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()), \
             patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue", side_effect=blocking_fire), \
             patch.object(manager, "_update_all_conn_counts"):
            await manager.reconcile([_sub(id=1, cooldown=300)])
            await asyncio.sleep(0)

            assert manager._fire_task is not None
            assert not manager._fire_task.done()

        await manager.stop()

    @pytest.mark.asyncio
    async def test_fire_task_waits_for_a_connect_that_has_not_landed(self, manager):
        """A connection is pooled from the moment its connect starts and reports
        ``is_closed`` False until it is closed, so being in the pool does not say the
        fire consumer can run on it. Started on one whose connect is still in flight,
        the task fails every call it makes and its retry loop keeps it alive — and
        therefore out of every restart path, leaving rmq_watcher.fire without a
        consumer for good."""
        state = manager._conn("rmq_default")
        state.connections[_ROLE_CONSUME] = _make_live_connection()
        never_lands = asyncio.get_running_loop().create_future()
        state.connecting[_ROLE_CONSUME] = asyncio.ensure_future(never_lands)

        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_provision_cooldown"), \
             patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue") as fire, \
             patch.object(manager, "_update_all_conn_counts"):
            await manager.reconcile([_sub(id=1, cooldown=300)])
            await asyncio.sleep(0)

            assert manager._fire_task is None
            fire.assert_not_called()

        never_lands.cancel()
        await manager.stop()

    @pytest.mark.asyncio
    async def test_fire_task_restarts_when_the_pool_replaces_its_connection(self, manager):
        """The fire consumer holds the connection object it was handed for its whole
        life. A connect that failed takes its object with it — the object answers every
        later call with that failure — so a task left on the replaced one spins in its
        retry loop while the pool is healthy again."""
        first = _make_live_connection()
        replacement = _make_live_connection()
        state = manager._conn("rmq_default")
        state.connections[_ROLE_CONSUME] = first
        handed: list = []

        async def blocking_fire(conn, conn_id=None):
            handed.append(conn)
            await asyncio.Future()

        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_provision_cooldown"), \
             patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue", side_effect=blocking_fire), \
             patch.object(manager, "_update_all_conn_counts"):
            await manager.reconcile([_sub(id=1, cooldown=300)])
            await asyncio.sleep(0)
            first_task = manager._fire_task
            assert handed == [first]

            state.connections[_ROLE_CONSUME] = replacement
            await manager.reconcile([_sub(id=1, cooldown=300)])
            await asyncio.sleep(0)

        assert first_task.done()
        assert manager._fire_task is not None
        assert manager._fire_task is not first_task
        assert handed == [first, replacement]
        assert manager._fire_state.connection is replacement

        await manager.stop()


class TestPublishConnection:
    def _cooldown_manager(self, publish_channel):
        manager = _make_manager()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_PUBLISH] = _make_live_connection()
        manager._conn("rmq_default").publish_channel = publish_channel
        return manager

    @pytest.mark.asyncio
    async def test_cooldown_publishes_on_publish_connection(self):
        msg = _make_fake_message(b"order")
        queue = _make_push_queue([msg])
        consume_channel = _make_live_channel(queue=queue)
        publish_channel = _make_live_channel()
        consume_conn = _make_live_connection(channel=consume_channel)
        publish_conn = _make_live_connection(channel=publish_channel)

        manager = _make_manager()
        published = asyncio.Event()

        async def capture_publish(amqp_msg, routing_key):
            published.set()

        publish_channel.default_exchange.publish = capture_publish

        async def get_conn(conn_id, role=_ROLE_CONSUME):
            return publish_conn if role == _ROLE_PUBLISH else consume_conn

        with patch.object(manager, "_get_or_create_connection", side_effect=get_conn), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = asyncio.create_task(manager._consume_subscription(_sub(cooldown=300)))
            await asyncio.wait_for(published.wait(), timeout=2.0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        assert published.is_set()
        consume_channel.default_exchange.publish.assert_not_called()
        msg.ack.assert_awaited()

    @pytest.mark.asyncio
    async def test_two_deliveries_opening_a_publish_channel_at_once_keep_one(self):
        """Opening one takes two awaits, so a cooldown delivery of another subscription
        on this conn_id can open one in between. ``Channel.__del__`` writes no
        ``channel.close`` frame, so the one that is not pooled stays open on the broker
        until the publish connection itself goes."""
        manager = _make_manager()
        opened: list = []
        opening = asyncio.Event()

        async def channel(**kwargs):
            opening.set()
            # Both callers are past the cache read before either has a channel
            await asyncio.sleep(0)
            new = _make_live_channel()
            opened.append(new)
            return new

        connection = _make_live_connection()
        connection.channel = channel
        with patch.object(manager, "_get_or_create_connection", return_value=connection):
            first, second = await asyncio.gather(
                manager._get_publish_channel("rmq_default"),
                manager._get_publish_channel("rmq_default"),
            )

        assert len(opened) == 2, "both callers found the cache empty"
        assert first is second, "the pooled channel is the one every later caller reads"
        loser = next(c for c in opened if c is not first)
        loser.close.assert_awaited()
        assert manager._conn("rmq_default").publish_channel is first

    @pytest.mark.asyncio
    async def test_publish_failure_requeues_the_delivery(self):
        publish_channel = _make_live_channel()
        publish_channel.default_exchange.publish = AsyncMock(
            side_effect=aio_pika.exceptions.AMQPError("publish refused")
        )
        manager = self._cooldown_manager(publish_channel)
        msg = _make_fake_message(b"order")

        with pytest.raises(aio_pika.exceptions.AMQPError):
            await manager._publish_pending("rmq_default", "my_dag", 300, msg)

        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_rejected_placeholder_ends_the_delivery(self):
        """The pending queue is declared x-max-length=1 with x-overflow=reject-publish,
        so a broker that nacks the publish is saying the cooldown window for this dag_id
        is already open. That is the ordinary case while a window runs: requeueing would
        redeliver the same message for the whole window and burn the quorum-queue
        delivery limit on it."""
        publish_channel = _make_live_channel()
        publish_channel.default_exchange.publish = AsyncMock(
            side_effect=aio_pika.exceptions.DeliveryError(None, MagicMock())
        )
        manager = self._cooldown_manager(publish_channel)
        msg = _make_fake_message(b"order")

        await manager._publish_pending("rmq_default", "my_dag", 300, msg)

        msg.ack.assert_awaited_once()
        msg.nack.assert_not_awaited()
        assert manager._conn("rmq_default").publish_timeouts == 0

    @pytest.mark.asyncio
    async def test_a_rejected_placeholder_does_not_stop_the_consumer(self):
        """Every matching message while a cooldown window is open is rejected, so a
        rejection leaking out of _publish_pending would tear the consumer loop down and
        redeliver forever."""
        first = _make_fake_message(b"order", message_id="m1")
        second = _make_fake_message(b"order", message_id="m2")
        done = asyncio.Event()
        second.ack = AsyncMock(side_effect=lambda: done.set())
        publish_channel = _make_live_channel()
        publish_channel.default_exchange.publish = AsyncMock(
            side_effect=aio_pika.exceptions.DeliveryError(None, MagicMock())
        )
        manager = self._cooldown_manager(publish_channel)
        consume_channel = _make_live_channel(queue=_make_push_queue([first, second]))
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = _make_live_connection(
            channel=consume_channel
        )

        with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            task = asyncio.create_task(
                manager._consume_subscription(_sub(cooldown=300))
            )
            try:
                await asyncio.wait_for(done.wait(), timeout=2.0)
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

        first.ack.assert_awaited_once()
        second.ack.assert_awaited_once()
        assert not any(
            "Transient error in consumer" in str(c.args[0])
            for c in mock_log.warning.call_args_list
        ), mock_log.warning.call_args_list

    @pytest.mark.asyncio
    async def test_an_unrouted_placeholder_is_still_a_failure(self):
        """A returned message means the pending queue is not there at all — that is a
        real fault and the delivery goes back to the queue."""
        publish_channel = _make_live_channel()
        publish_channel.default_exchange.publish = AsyncMock(
            side_effect=aio_pika.exceptions.PublishError.__new__(
                aio_pika.exceptions.PublishError
            )
        )
        manager = self._cooldown_manager(publish_channel)
        msg = _make_fake_message(b"order")

        with pytest.raises(aio_pika.exceptions.PublishError):
            await manager._publish_pending("rmq_default", "my_dag", 300, msg)

        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_two_publish_timeouts_recreate_only_publish_connection(self):
        publish_channel = _make_live_channel()
        publish_channel.default_exchange.publish = _hanging_call
        manager = self._cooldown_manager(publish_channel)
        consume_conn = manager._conn("rmq_default").connections[_ROLE_CONSUME]
        publish_conn = manager._conn("rmq_default").connections[_ROLE_PUBLISH]
        _fast_timeouts(manager)

        for _ in range(2):
            with pytest.raises(asyncio.TimeoutError):
                await manager._publish_pending(
                    "rmq_default", "my_dag", 300, _make_fake_message(b"order")
                )

        assert _pooled_connections(manager) == [consume_conn]
        assert manager._conn("rmq_default").publish_channel is None
        publish_conn.close.assert_awaited_once()
        consume_conn.close.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_single_publish_timeout_keeps_publish_connection(self):
        publish_channel = _make_live_channel()
        publish_channel.default_exchange.publish = _hanging_call
        manager = self._cooldown_manager(publish_channel)
        _fast_timeouts(manager)

        with pytest.raises(asyncio.TimeoutError):
            await manager._publish_pending(
                "rmq_default", "my_dag", 300, _make_fake_message(b"order")
            )

        assert _ROLE_PUBLISH in manager._conn("rmq_default").connections
        assert manager._conn("rmq_default").publish_timeouts == 1

    @pytest.mark.asyncio
    async def test_successful_publish_resets_the_timeout_counter(self):
        publish_channel = _make_live_channel()
        manager = self._cooldown_manager(publish_channel)
        manager._conn("rmq_default").publish_timeouts = 1

        await manager._publish_pending(
            "rmq_default", "my_dag", 300, _make_fake_message(b"order")
        )

        assert manager._conn("rmq_default").publish_timeouts == 0

    @pytest.mark.asyncio
    async def test_hanging_publish_does_not_hang_the_consumer_task(self):
        msg = _make_fake_message(b"order")
        queue = _make_push_queue([msg])
        consume_channel = _make_live_channel(queue=queue)
        publish_channel = _make_live_channel()
        publish_channel.default_exchange.publish = _hanging_call
        manager = _make_manager()
        manager._conn("rmq_default").publish_channel = publish_channel
        _fast_timeouts(manager)

        consume_conn = _make_live_connection(channel=consume_channel)
        publish_conn = _make_live_connection(channel=publish_channel)

        async def get_conn(conn_id, role=_ROLE_CONSUME):
            return publish_conn if role == _ROLE_PUBLISH else consume_conn

        nacked = asyncio.Event()

        async def capture_nack(*args, **kwargs):
            nacked.set()

        msg.nack = capture_nack

        with patch.object(manager, "_get_or_create_connection", side_effect=get_conn), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(manager._consume_subscription(_sub(cooldown=300)))
            await asyncio.wait_for(nacked.wait(), timeout=2.0)
            assert not task.done()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


class TestAmqpCallTimeouts:
    @pytest.mark.asyncio
    async def test_hanging_channel_is_transient_and_task_retries(self, manager):
        connection = _make_live_connection()
        connection.channel = _hanging_call
        _fast_timeouts(manager)
        retried = asyncio.Event()
        attempts = 0

        async def counting_get_conn(conn_id, role=_ROLE_CONSUME):
            nonlocal attempts
            attempts += 1
            if attempts >= 2:
                retried.set()
            return connection

        with patch.object(manager, "_get_or_create_connection", side_effect=counting_get_conn), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            await asyncio.wait_for(retried.wait(), timeout=2.0)
            assert not task.done()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        assert attempts >= 2

    @pytest.mark.asyncio
    async def test_hanging_declare_does_not_hang_provisioning(self, manager):
        setup_channel = _make_live_channel()
        setup_channel.declare_exchange = _hanging_call
        connection = _make_live_connection(channel=setup_channel)
        _fast_timeouts(manager)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await asyncio.wait_for(
                manager._provision_cooldown({"my_dag"}, "rmq_default"), timeout=2.0
            )

        assert mock_log.error.call_args_list, "a declare that never returns must be logged"

    @pytest.mark.asyncio
    async def test_hanging_declare_does_not_hang_reconcile(self, manager):
        setup_channel = _make_live_channel()
        setup_channel.declare_exchange = _hanging_call
        connection = _make_live_connection(channel=setup_channel)
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection
        _fast_timeouts(manager)

        async def blocking_consume(sub):
            await asyncio.Future()

        async def blocking_fire(conn, conn_id=None):
            await asyncio.Future()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue", side_effect=blocking_fire), \
             patch.object(manager, "_update_all_conn_counts"):
            await asyncio.wait_for(
                manager.reconcile([_sub(id=1, cooldown=300)]), timeout=2.0
            )

        await manager.stop()

    @pytest.mark.asyncio
    async def test_hanging_exchange_provisioning_does_not_hang_reconcile(self, manager):
        setup_channel = _make_live_channel()
        setup_channel.declare_exchange = _hanging_call
        connection = _make_live_connection(channel=setup_channel)
        _fast_timeouts(manager)
        await manager.start()

        async def blocking_consume(sub):
            await asyncio.Future()

        try:
            with patch.object(manager, "_get_or_create_connection", return_value=connection), \
                 patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
                 patch.object(manager, "_update_all_conn_counts"), \
                 patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
                await asyncio.wait_for(
                    manager.reconcile([_exchange_sub(id=1)]), timeout=2.0
                )
            assert mock_log.error.call_args_list
        finally:
            await manager.stop()


class TestConsumerTag:
    def test_tag_is_stable_and_distinguishes_subscriptions(self):
        assert _consumer_tag(1) == _consumer_tag(1)
        assert _consumer_tag(1) != _consumer_tag(2)
        assert _consumer_tag(1) != _consumer_tag("fire")
        assert str(os.getpid()) in _consumer_tag(1)
        assert _consumer_tag(1).startswith("rmq_watcher.")

    def test_nonce_separates_one_attach_from_the_next(self):
        """A ghost consumer the broker still lists under the previous attach must not
        vouch for the task that replaced it."""
        assert _consumer_tag(1, "aaaa") != _consumer_tag(1, "bbbb")
        assert _consumer_tag(1, "aaaa").startswith(_consumer_tag(1) + ".")
        assert _attach_nonce() != _attach_nonce()

    @pytest.mark.asyncio
    async def test_subscription_consumes_under_its_own_tag(self, manager):
        queue = _make_push_queue()
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            task = await _run_then_cancel(manager._consume_subscription(_sub(id=7)), timeout=0.2)

        tag = queue.iterator.call_args.kwargs["consumer_tag"]
        assert tag.startswith(_consumer_tag(7) + ".")
        assert manager._active.get(7) is None or manager._active[7].state.consumer_tag == tag
        assert task.done()

    @pytest.mark.asyncio
    async def test_the_recorded_tag_is_the_one_the_iterator_registered(self, manager):
        """The liveness check looks the recorded tag up in the broker's answer, so a
        state carrying a nonce of its own would report every healthy consumer as gone and
        recreate its connection every other cycle."""
        queue = _make_push_queue()
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"):
            await manager.reconcile([_sub(id=1)])
            await _wait_for_status(manager._active[1], "listening")
            recorded = manager._active[1].state.consumer_tag
            await _drain(manager)

        assert recorded == queue.iterator.call_args.kwargs["consumer_tag"]

    @pytest.mark.asyncio
    async def test_fire_consumer_uses_the_fire_tag(self, manager):
        queue = _make_push_queue()
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        await _run_then_cancel(manager._consume_fire_queue(connection, "rmq_default"), timeout=0.2)

        assert queue.iterator.call_args.kwargs["consumer_tag"].startswith(
            _consumer_tag("fire") + "."
        )


class TestSubscriptionStateVisibleToManager:
    @pytest.mark.asyncio
    async def test_state_moves_from_connecting_to_listening(self, manager):
        queue = _make_push_queue()
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([_sub(id=1)])
            await _wait_for_status(manager._active[1], "listening")

            manager._active[1].task.cancel()
            await asyncio.gather(manager._active[1].task, return_exceptions=True)

    @pytest.mark.asyncio
    async def test_state_reports_connecting_while_the_broker_is_unreachable(self, manager):
        real_sleep = asyncio.sleep

        async def failing_get_conn(conn_id, role=_ROLE_CONSUME):
            raise ConnectionError("broker down")

        async def instant_sleep(delay):
            await real_sleep(0)

        with patch.object(manager, "_get_or_create_connection", side_effect=failing_get_conn), \
             _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"), \
             patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep", side_effect=instant_sleep):
            await manager.reconcile([_sub(id=1)])
            await _wait_for_status(manager._active[1], "connecting")

            manager._active[1].task.cancel()
            await asyncio.gather(manager._active[1].task, return_exceptions=True)


class TestConsumptionKeepsGoing:
    @pytest.mark.asyncio
    async def test_run_of_non_matching_messages_does_not_stop_consumption(self, manager):
        misses = [_make_fake_message(b"p", headers={"type": "payment"}) for _ in range(3)]
        hit = _make_fake_message(b"o", headers={"type": "order"})
        queue = _make_push_queue([*misses, hit])
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        triggered: list = []
        got_hit = asyncio.Event()

        async def mock_trigger(dag_id, queue_name, sub_id, message):
            triggered.append(message)
            got_hit.set()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_trigger_dag", side_effect=mock_trigger), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(
                manager._consume_subscription(
                    _sub(filter_data={"filter_headers": {"type": "order"}})
                )
            )
            await asyncio.wait_for(got_hit.wait(), timeout=2.0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        assert triggered == [hit]
        for miss in misses:
            miss.nack.assert_awaited_with(requeue=True)

    @pytest.mark.asyncio
    async def test_iterator_ending_pauses_before_resubscribing(self, manager):
        queue = _ending_queue()
        connection = _make_live_connection(queue=queue)

        delays: list = []
        paused = asyncio.Event()

        def note(delay):
            delays.append(delay)
            paused.set()

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             _record_consumer_sleeps(note):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            await asyncio.wait_for(paused.wait(), timeout=2.0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        assert delays[0] == _RECONNECT_DELAY
        assert queue.iterator.call_count >= 1

    @pytest.mark.asyncio
    async def test_a_publish_that_keeps_failing_pauses_longer_each_time(self, manager):
        """A broker under a resource alarm holds publishes for as long as the alarm
        lasts. Without a growing pause the requeued delivery comes back every reconnect
        delay and burns the quorum-queue delivery limit, after which the broker drops the
        very message the requeue was meant to keep."""
        queue = MagicMock()
        queue.iterator.side_effect = lambda **kw: _QueueIterCtx([_make_fake_message(b"o")])
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        delays: list = []
        real_sleep = asyncio.sleep

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_publish_pending",
                          side_effect=asyncio.TimeoutError("alarm")), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             _record_consumer_sleeps(delays.append):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(cooldown=300))
            )
            while len([d for d in delays if d != _RECONNECT_DELAY]) < 3:
                await real_sleep(0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        publish_pauses = [d for d in delays if d != _RECONNECT_DELAY]
        assert publish_pauses[:3] == [
            _PUBLISH_BACKOFF_START,
            _PUBLISH_BACKOFF_START * 2,
            _PUBLISH_BACKOFF_START * 4,
        ], delays

    @pytest.mark.asyncio
    async def test_a_publish_that_goes_through_clears_the_pause(self, manager):
        outcomes = [asyncio.TimeoutError("alarm"), None, asyncio.TimeoutError("alarm")]
        queue = MagicMock()
        queue.iterator.side_effect = lambda **kw: _QueueIterCtx(
            [_make_fake_message(b"o"), _make_fake_message(b"o")]
        )
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        async def publish(*args, **kwargs):
            outcome = outcomes.pop(0) if outcomes else None
            if outcome is not None:
                raise outcome

        delays: list = []
        real_sleep = asyncio.sleep

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_publish_pending", side_effect=publish), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             _record_consumer_sleeps(delays.append):
            task = asyncio.create_task(
                manager._consume_subscription(_sub(cooldown=300))
            )
            while len([d for d in delays if d != _RECONNECT_DELAY]) < 2:
                await real_sleep(0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        publish_pauses = [d for d in delays if d != _RECONNECT_DELAY]
        assert publish_pauses[:2] == [_PUBLISH_BACKOFF_START, _PUBLISH_BACKOFF_START], (
            "a publish that went through starts the next pause from scratch"
        )


# ---------------------------------------------------------------------------
# Tests for the broker-side liveness check
# ---------------------------------------------------------------------------

def _state_with(status: str | None, sub_id: int | None = 1) -> _ConsumerState:
    """Build a state record that already reports ``status`` and is attached to a queue."""
    state = _ConsumerState(sub_id, _test_pool())
    state._status = status
    state.consumer_tag = _consumer_tag(sub_id if sub_id is not None else "fire")
    return state


async def _wait_for_status(entry, status: str, timeout: float = 5.0) -> None:
    """Wait until a subscription task reports ``status``.

    The status write goes through a thread pool, so the task reaches it a few event
    loop turns after reconcile returns rather than after a fixed delay.
    """
    deadline = time.monotonic() + timeout
    while entry.state.status != status:
        assert time.monotonic() < deadline, (
            f"status stayed {entry.state.status!r}, never became {status!r}"
        )
        await asyncio.sleep(0.01)


def _register_active(
    manager, sub: dict, status: str | None = "listening", real_task: bool = False
) -> _ActiveSub:
    """Put a running subscription into the manager the way reconcile does.

    ``real_task`` hands the entry a task that never returns on its own instead of a
    mock: a test that lets the manager cancel and await the task needs a real one.
    """
    entry = _ActiveSub(
        task=_registered_task(real_task),
        sub=sub.copy(),
        state=_state_with(status, sub["id"]),
    )
    manager._active[sub["id"]] = entry
    return entry


def _register_fire(
    manager,
    conn_id: str = "rmq_default",
    status: str | None = "listening",
    real_task: bool = False,
):
    """Put a running fire consumer into the manager, mirroring :func:`_register_active`."""
    manager._fire_task = _registered_task(real_task)
    manager._fire_state = _FireSub(
        conn_id=conn_id,
        state=_state_with(status, None),
        # The connection the task holds is the one the pool has, as it is for a task
        # the manager started itself; a fire consumer holding any other object is what
        # :meth:`_sync_fire_consumer` restarts.
        connection=manager._conn(conn_id).connections.get(_ROLE_CONSUME),
    )
    return manager._fire_state


def _pool_consume_connection(manager, connection, *attached, conn_id: str = "rmq_default"):
    """Pool ``connection`` for ``conn_id`` and register the ``attached`` states on it.

    A consumer registers its tag on the object the pool handed it, and the liveness
    check compares the two before it lets a probe of that object vouch for the tag.
    """
    manager._conn(conn_id).connections[_ROLE_CONSUME] = connection
    for state in attached:
        state.connection = connection
    return connection


def _registered_task(real: bool):
    """Task object for a registered consumer — a real never-ending one, or a mock."""
    if real:
        return asyncio.create_task(_never_returns())
    task = MagicMock()
    task.done.return_value = False
    return task


def _consumer_entry(tag: str, queue: str) -> dict:
    return {"consumer_tag": tag, "queue": {"name": queue, "vhost": "/"}}


def _mgmt_client(payload, requested: list | None = None, status_code: int = 200):
    """httpx client answering the Management API consumer listing from ``payload``."""
    def handler(request: httpx.Request) -> httpx.Response:
        if requested is not None:
            requested.append(str(request.url))
        return httpx.Response(status_code, json=payload)

    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def _mgmt_conn_info(url: str = "https://mb.example", login: str = "guest"):
    conn_info = _make_conn_info()
    conn_info.extra_dejson = {"management_url": url}
    conn_info.login = login
    return conn_info


def _patch_mgmt_connection():
    return patch(
        "airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
        return_value=_mgmt_conn_info("https://mb.example"),
    )


def _patch_mgmt_connection_per_login(
    logins: dict[str, str], url: str = "https://mb.example"
):
    """Patch the connection read so every conn_id of ``logins`` logs in as its own user."""
    return patch(
        "airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
        side_effect=lambda conn_id: _mgmt_conn_info(url, logins[conn_id]),
    )


def _basic_auth_login(request: httpx.Request) -> str:
    """Login the Management API request authenticated as."""
    raw = request.headers["Authorization"].split(" ", 1)[1]
    return base64.b64decode(raw).decode().split(":", 1)[0]


class TestSubscriptionLiveness:
    @pytest.mark.asyncio
    async def test_missing_tag_twice_condemns_the_subscription(self, manager):
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            first = await manager._check_liveness([sub])
            second = await manager._check_liveness([sub])

        assert first == (set(), set())
        assert second == ({7}, {"rmq_default"})
        assert manager._conn("rmq_default").liveness.status == "error"

    @pytest.mark.asyncio
    async def test_the_first_negative_check_is_reported_immediately(self, manager):
        """The restart waits for a second negative check; the status row does not. A
        conn_id whose consumer the broker has just denied must not read 'connected' for
        the whole reconcile interval that the second check is waiting out."""
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            first = await manager._check_liveness([sub])

        assert first == (set(), set())
        verdict = manager._conn("rmq_default").liveness
        assert verdict.status == "error"
        assert verdict.broker_consumer_count == 0
        assert "unseen by the broker" in verdict.reason
        assert "negative check 1 of 2" in verdict.reason

    @pytest.mark.asyncio
    async def test_one_negative_check_restarts_nothing(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert entry.negative_checks == 1

    @pytest.mark.asyncio
    async def test_foreign_consumer_does_not_vouch_for_ours(self, manager):
        """Regression: an HA replica or a foreign client keeps the consumer count
        non-zero while our own consumer is a zombie."""
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = _mgmt_client([
            _consumer_entry("some.other.client", "orders"),
            _consumer_entry("rmq_watcher.otherhost.999.7", "orders"),
        ])

        with _patch_mgmt_connection():
            await manager._check_liveness([sub])
            result = await manager._check_liveness([sub])

        assert result == ({7}, {"rmq_default"})
        assert manager._conn("rmq_default").liveness.broker_consumer_count == 2

    @pytest.mark.asyncio
    async def test_our_tag_among_foreign_ones_counts_as_alive(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.negative_checks = 1
        manager._http_client = _mgmt_client([
            _consumer_entry("some.other.client", "orders"),
            _consumer_entry(_consumer_tag(7), "orders"),
        ])

        with _patch_mgmt_connection():
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert entry.negative_checks == 0
        assert manager._conn("rmq_default").liveness.status == "connected"
        assert manager._conn("rmq_default").liveness.broker_consumer_count == 2

    @pytest.mark.asyncio
    async def test_cluster_alarm_does_not_mute_the_verdict(self, manager):
        """Regression: reading node alarms would silence the watchdog for every conn_id
        for as long as the cluster alarm lasts — including connections that died then."""
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        requested: list[str] = []
        manager._http_client = _mgmt_client(
            [_consumer_entry("publisher.blocked.by.alarm", "orders")], requested=requested,
        )

        with _patch_mgmt_connection():
            await manager._check_liveness([sub])
            result = await manager._check_liveness([sub])

        assert result == ({7}, {"rmq_default"})
        assert requested and all("/api/nodes" not in url for url in requested)

    @pytest.mark.asyncio
    async def test_management_api_failure_is_no_data(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        manager._http_client = _mgmt_client({"error": "unavailable"}, status_code=503)

        with _patch_mgmt_connection(), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            first = await manager._check_liveness([sub])

        assert first == (set(), set())
        assert entry.negative_checks == 0
        assert manager._conn("rmq_default").liveness.status is None
        assert manager._conn("rmq_default").liveness.broker_consumer_count is None
        assert mock_log.warning.called

    @pytest.mark.asyncio
    async def test_repeated_management_api_failures_fall_back_to_the_amqp_probe(self, manager):
        """A permanently unusable Management API (wrong URL, credentials without the
        management tag) must not disable the watchdog: after the second failure in a row
        the check asks the broker over AMQP instead."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        manager._http_client = _mgmt_client({"error": "unauthorized"}, status_code=401)
        probe = AsyncMock(
            return_value=({entry.state.consumer_tag}, None, None, time.monotonic())
        )

        with _patch_mgmt_connection(), \
             patch.object(manager, "_probe_by_passive_declare", probe):
            await manager._check_liveness([sub])
            assert probe.await_count == 0, "a single failure is still 'no data'"
            await manager._check_liveness([sub])

        assert probe.await_count == 1
        assert manager._conn("rmq_default").liveness.status == "connected"
        assert entry.negative_checks == 0

    @pytest.mark.asyncio
    async def test_one_management_request_serves_conn_ids_of_one_account(self, manager):
        """Several conn_ids often point at one broker with one account, and the answer
        such a conn_id gets is the answer the next one would — asking once per conn_id
        multiplies the same call."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        b = _sub(id=8, queue_name="events", conn_id="conn_b")
        entry_a = _register_active(manager, a)
        entry_b = _register_active(manager, b)
        requested: list[str] = []
        manager._http_client = _mgmt_client(
            [
                _consumer_entry(entry_a.state.consumer_tag, "orders"),
                _consumer_entry(entry_b.state.consumer_tag, "events"),
            ],
            requested=requested,
        )

        with _patch_mgmt_connection_per_login({"conn_a": "shared", "conn_b": "shared"}):
            result = await manager._check_liveness([a, b])

        assert result == (set(), set())
        assert len(requested) == 1, requested
        assert manager._conn("conn_a").liveness.status == "connected"
        assert manager._conn("conn_b").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_an_api_that_does_not_answer_is_asked_once_per_cycle(self, manager):
        """A request that failed is an answer about the API, and every conn_id keyed
        the same way would meet the same one. Asking again per conn_id spends another
        _MGMT_HTTP_TIMEOUT of the cycle on a call already known not to come back."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        b = _sub(id=8, queue_name="events", conn_id="conn_b")
        _register_active(manager, a)
        _register_active(manager, b)
        requested: list[str] = []
        manager._http_client = _mgmt_client(
            {"error": "unauthorized"}, requested=requested, status_code=401
        )

        with _patch_mgmt_connection_per_login({"conn_a": "shared", "conn_b": "shared"}):
            result = await manager._check_liveness([a, b])

        assert result == (set(), set())
        assert len(requested) == 1, requested
        assert manager._conn("conn_a").liveness.status is None
        assert manager._conn("conn_b").liveness.status is None
        assert manager._conn("conn_a").mgmt_failures == 1, (
            "the count is each conn_id's own — it decides when this conn_id stops "
            "waiting for the API and asks its own connection instead"
        )
        assert manager._conn("conn_b").mgmt_failures == 1

    @pytest.mark.asyncio
    async def test_two_logins_on_one_vhost_each_get_their_own_answer(self, manager):
        """``GET /api/consumers/{vhost}`` is answered according to the rights of the
        account that asked: a user tagged ``management`` is shown only its own channels.
        Judging one login by the reply fetched for another finds its tags missing and
        condemns a perfectly healthy consumer, cycle after cycle."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        b = _sub(id=8, queue_name="events", conn_id="conn_b")
        entry_a = _register_active(manager, a)
        entry_b = _register_active(manager, b)
        requested: list[str] = []
        visible = {
            "user_a": [_consumer_entry(entry_a.state.consumer_tag, "orders")],
            "user_b": [_consumer_entry(entry_b.state.consumer_tag, "events")],
        }

        def handler(request: httpx.Request) -> httpx.Response:
            login = _basic_auth_login(request)
            requested.append(login)
            return httpx.Response(200, json=visible[login])

        manager._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        with _patch_mgmt_connection_per_login({"conn_a": "user_a", "conn_b": "user_b"}):
            result = await manager._check_liveness([a, b])

        assert result == (set(), set())
        assert sorted(requested) == ["user_a", "user_b"], requested
        assert manager._conn("conn_a").liveness.status == "connected"
        assert manager._conn("conn_b").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_the_consumer_cache_lives_for_one_cycle_only(self, manager):
        """The cached answer saves one request per conn_id of the same account inside a
        cycle. Kept across cycles it would hand every later check the consumer list of
        the first one, and a consumer that has since disappeared would go on vouching
        for itself forever."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        requested: list[str] = []
        manager._http_client = _mgmt_client(
            [_consumer_entry(entry.state.consumer_tag, "orders")], requested=requested
        )

        with _patch_mgmt_connection():
            await manager._check_liveness([sub])
            await manager._check_liveness([sub])

        assert len(requested) == 2, requested

    @pytest.mark.asyncio
    async def test_a_listening_state_without_a_tag_is_not_a_candidate(self, manager):
        """A state that says ``listening`` before the tag is recorded has nothing to look
        for in the broker's answer: probing there compares against ``None`` and condemns
        a consumer that is attaching normally."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.state.consumer_tag = None
        requested: list[str] = []
        manager._http_client = _mgmt_client([], requested=requested)

        with _patch_mgmt_connection():
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert requested == [], "a subscription with no tag of its own is not probed"
        assert entry.negative_checks == 0

    @pytest.mark.asyncio
    async def test_status_gate_reads_the_manager_record_not_the_sub_dict(self, manager):
        """Regression: the subscription dicts reconcile receives carry no status, so a
        gate built on them would be silently always empty (or always true)."""
        sub = _sub(id=7, queue_name="orders")
        sub["consumer_status"] = "listening"
        _register_active(manager, sub, status="connecting")
        requested: list[str] = []
        manager._http_client = _mgmt_client([], requested=requested)

        with _patch_mgmt_connection():
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert requested == [], "a subscription that is not listening is not probed"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["connecting", "error", None])
    async def test_subscription_outside_listening_is_not_a_candidate(self, manager, status):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub, status=status)
        requested: list[str] = []
        manager._http_client = _mgmt_client([], requested=requested)

        with _patch_mgmt_connection():
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert requested == [], "a subscription that is not listening is not probed"
        assert entry.negative_checks == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["connecting", "error", None])
    async def test_a_conn_id_that_never_reaches_listening_is_condemned(self, manager, status):
        """A connection whose ``channel()`` never returns keeps every task of its
        conn_id in 'connecting', so the check has no candidate to put to the broker.
        This layer is what still reaches a verdict on it, and the row says so."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub, status=status)
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection(), \
             patch.object(manager, "_get_or_create_connection", new_callable=AsyncMock,
                          side_effect=OSError("connection refused")):
            first = await manager._check_liveness([sub])
            assert first == (set(), set())
            assert manager._conn("rmq_default").liveness.status is None
            second = await manager._check_liveness([sub])

        assert second == (set(), {"rmq_default"}), (
            "the connection itself must be recreated, not the task that retries on it"
        )
        assert entry.negative_checks == 0, "the subscription itself is not condemned"
        assert manager._conn("rmq_default").liveness.status == "error"
        reason = manager._conn("rmq_default").liveness.reason
        assert "listening" in reason
        assert "passive declare" in reason, "the row must say what was observed"

    @pytest.mark.asyncio
    async def test_a_connection_that_answers_is_not_condemned_for_stalled_tasks(
        self, manager
    ):
        """A trigger that keeps failing leaves its subscription in 'error' for as long as
        the Airflow database is down, on an AMQP connection that is perfectly healthy.
        Dropping it there would blame the broker for a database outage."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub, status="error")
        healthy = _make_live_connection(channel=_make_live_channel())
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection(), \
             patch.object(manager, "_get_or_create_connection", new_callable=AsyncMock,
                          return_value=healthy):
            await manager._check_liveness([sub])
            second = await manager._check_liveness([sub])
            third = await manager._check_liveness([sub])

        assert second == (set(), set()), "the connection answers, so it stays"
        assert third == (set(), set()), "and it keeps staying, cycle after cycle"
        assert entry.negative_checks == 0
        verdict = manager._conn("rmq_default").liveness
        assert verdict.status == "error"
        assert "downstream" in verdict.reason
        assert "answers an RPC" in verdict.reason

    @pytest.mark.asyncio
    async def test_a_candidate_appearing_restarts_the_stuck_counter(self, manager):
        """A conn_id that reaches ``listening`` in between has proved itself; carrying
        the earlier cycles forward would condemn it on the first cycle it is merely slow
        to attach again."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub, status="connecting")
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection(), \
             patch.object(manager, "_get_or_create_connection", new_callable=AsyncMock,
                          side_effect=OSError("connection refused")):
            await manager._check_liveness([sub])
            assert manager._conn("rmq_default").stuck_cycles == 1

            # The task attaches, and the broker confirms it.
            entry.state._status = "listening"
            manager._http_client = _mgmt_client(
                [_consumer_entry(entry.state.consumer_tag, "orders")]
            )
            await manager._check_liveness([sub])
            assert manager._conn("rmq_default").stuck_cycles == 0

            # It drops back out of listening: the count starts from one again, so this
            # cycle alone cannot condemn the connection.
            entry.state._status = "connecting"
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert manager._conn("rmq_default").stuck_cycles == 1

    @pytest.mark.asyncio
    async def test_a_conn_id_with_no_running_task_is_not_green(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.task.done.return_value = True
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert manager._conn("rmq_default").liveness.status == "error"

    @pytest.mark.asyncio
    async def test_finished_task_is_not_a_candidate(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.task.done.return_value = True
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            await manager._check_liveness([sub])
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert entry.negative_checks == 0


class TestLivenessDropRateLimit:
    @pytest.mark.asyncio
    async def test_connection_is_not_recreated_faster_than_the_limit(self, manager):
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection(), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._check_liveness([sub])
            condemned = await manager._check_liveness([sub])
            await manager._drop_connection("rmq_default")  # what reconcile does next
            held_back = await manager._check_liveness([sub])

        assert condemned == ({7}, {"rmq_default"})
        assert held_back == (set(), set())
        assert manager._conn("rmq_default").liveness.status == "degraded"
        assert manager._conn("rmq_default").liveness.reason
        assert any(
            "sooner than" in str(c.args[0]) for c in mock_log.warning.call_args_list
        ), "holding a condemned connection in place must be logged"

    @pytest.mark.asyncio
    async def test_connection_may_be_recreated_again_after_the_cooldown(self, manager):
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            await manager._check_liveness([sub])
            await manager._check_liveness([sub])
            await manager._drop_connection("rmq_default")
            for _ in range(_CYCLES_BEFORE_REDROP - 1):
                assert await manager._check_liveness([sub]) == (set(), set())
            again = await manager._check_liveness([sub])

        assert again == ({7}, {"rmq_default"})

    @pytest.mark.asyncio
    async def test_publish_role_drop_does_not_delay_consumer_recovery(self, manager):
        """The publish role has its own gate of consecutive timeouts; letting it move the
        limit would postpone recovery of a consuming connection that died at the same time."""
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._conn("rmq_default").connections[_ROLE_PUBLISH] = _make_live_connection()
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            await manager._check_liveness([sub])
            await manager._drop_connection("rmq_default", role=_ROLE_PUBLISH)
            result = await manager._check_liveness([sub])

        assert result == ({7}, {"rmq_default"})


class TestFireTaskLiveness:
    @pytest.mark.asyncio
    async def test_fire_task_pausing_after_an_error_is_not_a_candidate(self, manager):
        sub = _sub(id=7, queue_name="orders", cooldown=300)
        _register_active(manager, sub)
        fire = _register_fire(manager, status="error")
        manager._http_client = _mgmt_client([_consumer_entry(_consumer_tag(7), "orders")])

        with _patch_mgmt_connection():
            await manager._check_liveness([sub])
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert fire.negative_checks == 0
        assert manager._fire_needs_restart is False
        assert manager._active[7].negative_checks == 0

    @pytest.mark.asyncio
    async def test_dead_fire_task_condemns_only_itself(self, manager):
        sub = _sub(id=7, queue_name="orders", cooldown=300)
        _register_active(manager, sub)
        _register_fire(manager)
        manager._http_client = _mgmt_client([_consumer_entry(_consumer_tag(7), "orders")])

        with _patch_mgmt_connection():
            await manager._check_liveness([sub])
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert manager._fire_needs_restart is True
        assert manager._active[7].negative_checks == 0
        # The broker holds no fire consumer on this conn_id, so the row must not claim
        # otherwise while the restart is arranged.
        assert manager._conn("rmq_default").liveness.status == "error"
        assert "fire consumer" in manager._conn("rmq_default").liveness.reason

    @pytest.mark.asyncio
    async def test_a_dead_fire_task_alone_on_its_conn_id_condemns_the_connection(
        self, manager
    ):
        """Nothing else on this conn_id can be asked about: its subscription tasks are
        in their own retry loop, and a conn_id that has a candidate is skipped by the
        judge that would otherwise condemn a connection its tasks cannot consume on. So
        the fire consumer's verdict is the connection's — without that, the restart is
        handed the same pooled object every cycle."""
        sub = _sub(id=7, queue_name="orders", cooldown=300)
        _register_active(manager, sub, status="connecting")
        _register_fire(manager)
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            first = await manager._check_liveness([sub])
            second = await manager._check_liveness([sub])

        assert first == (set(), set())
        assert second == (set(), {"rmq_default"})
        assert manager._fire_needs_restart is True
        assert manager._conn("rmq_default").liveness.status == "error"

    @pytest.mark.asyncio
    async def test_the_fire_consumer_alone_on_its_conn_id_restarts_on_a_fresh_one(self):
        """The recovery that follows the verdict: the pooled connection is dropped, so
        the relaunched fire task asks the pool for a connection it has to build."""
        manager = _make_manager()
        sub = _sub(id=7, queue_name="orders", cooldown=300)
        _register_active(manager, sub, status="connecting", real_task=True)
        _register_fire(manager, real_task=True)
        old_conn = _make_live_connection()
        fresh = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = old_conn
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection(), \
             patch.object(manager, "_consume_fire_queue", side_effect=_never_returns), \
             patch.object(manager, "_get_or_create_connection",
                          new_callable=AsyncMock, return_value=fresh):
            await manager._recover_dead_consumers([sub])
            await manager._recover_dead_consumers([sub])

        assert old_conn not in _pooled_connections(manager)
        old_conn.close.assert_awaited()
        assert manager._fire_state is not None
        assert manager._fire_state.connection is fresh
        await _drain(manager)
        await manager._http_client.aclose()

    @pytest.mark.asyncio
    async def test_live_fire_tag_clears_its_counter(self, manager):
        sub = _sub(id=7, queue_name="orders", cooldown=300)
        _register_active(manager, sub)
        fire = _register_fire(manager)
        fire.negative_checks = 1
        manager._http_client = _mgmt_client([
            _consumer_entry(_consumer_tag(7), "orders"),
            _consumer_entry(_consumer_tag("fire"), _FIRE_QUEUE),
        ])

        with _patch_mgmt_connection():
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert fire.negative_checks == 0
        assert manager._fire_needs_restart is False

    @pytest.mark.asyncio
    async def test_fire_task_state_is_reported_by_the_running_task(self, manager):
        queue = _make_push_queue()
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))
        manager._fire_state = _FireSub(conn_id="rmq_default", state=_ConsumerState(None, _test_pool()))

        task = asyncio.create_task(manager._consume_fire_queue(connection, "rmq_default"))
        await _wait_for_status(manager._fire_state, "listening")
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


class TestLivenessAmqpProbe:
    def _hanging_declare_connection(self):
        channel = _make_live_channel()
        channel.declare_queue = _hanging_call
        return _make_live_connection(channel=channel)

    @pytest.mark.asyncio
    async def test_hanging_passive_declare_condemns_the_subscription(self, manager):
        """A connection that answers nothing at all is dead, not undecided: silence on
        an AMQP call counts as a negative verdict rather than as missing data."""
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = None
        _fast_timeouts(manager)
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = self._hanging_declare_connection()

        with patch("airflow_provider_rmq.watcher.consumer._CLOSE_TIMEOUT", 0.05):
            first = await manager._check_liveness([sub])
            second = await manager._check_liveness([sub])

        assert first == (set(), set())
        assert second == ({7}, {"rmq_default"})
        assert manager._conn("rmq_default").liveness.status == "error"

    @pytest.mark.asyncio
    async def test_failing_passive_declare_condemns_the_subscription(self, manager):
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = None
        _fast_timeouts(manager)
        channel = _make_live_channel()
        channel.declare_queue = AsyncMock(side_effect=ConnectionError("gone"))
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = _make_live_connection(channel=channel)

        await manager._check_liveness([sub])
        result = await manager._check_liveness([sub])

        assert result == ({7}, {"rmq_default"})

    @pytest.mark.asyncio
    async def test_a_refused_passive_declare_leaves_the_connection_alone(self, manager):
        """A broker that closes the channel has answered — only a live connection
        carries a close frame back — and what it says is about the queue it names. The
        verdict covers every consumer of the conn_id, so reading a deleted queue as a
        dead connection condemns the healthy subscriptions beside it."""
        gone = _sub(id=7, queue_name="deleted")
        alive = _sub(id=8, queue_name="orders")
        entry_gone = _register_active(manager, gone)
        entry_alive = _register_active(manager, alive)
        manager._http_client = None
        _fast_timeouts(manager)
        channel = _make_live_channel()
        channel.declare_queue = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelNotFoundEntity("404 NOT_FOUND")
        )
        _pool_consume_connection(
            manager, _make_live_connection(channel=channel), entry_gone.state,
            entry_alive.state,
        )

        result = await manager._check_liveness([gone, alive])

        assert result == (set(), set())
        assert entry_alive.negative_checks == 0
        assert manager._conn("rmq_default").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_successful_passive_declare_keeps_the_subscription(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.negative_checks = 1
        manager._http_client = None
        _fast_timeouts(manager)
        channel = _make_live_channel(queue=MagicMock())
        _pool_consume_connection(
            manager, _make_live_connection(channel=channel), entry.state
        )

        result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert entry.negative_checks == 0
        assert manager._conn("rmq_default").liveness.status == "connected"
        assert channel.declare_queue.await_args.kwargs["passive"] is True
        channel.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_amqp_probe_is_used_without_an_http_client_and_reconcile_still_runs(self, manager):
        """``_http_client is None`` (start() not called) must fall back to the AMQP probe
        instead of raising through reconcile."""
        sub = _sub(id=7, queue_name="orders")
        channel = _make_live_channel(queue=MagicMock())
        connection = _make_live_connection(channel=channel)
        manager._http_client = None
        _fast_timeouts(manager)
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection

        async def blocking_consume(sub_arg):
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([sub])
            manager._active[7].state = _state_with("listening", 7)
            result = await manager._check_liveness([sub])

            manager._active[7].task.cancel()
            await asyncio.gather(manager._active[7].task, return_exceptions=True)

        assert result == (set(), set())
        assert channel.declare_queue.await_count == 1


class TestAnAnswerThatArrivesAfterAReattach:
    """The registration a probe was asked about is replaced while the probe is pending.

    A consumer task reattaches on its own, and every attach registers a tag of its own,
    so the answer coming back is about a registration the task no longer holds. Read as
    an answer about the current one it denies a healthy consumer — and one prior negative
    check is all it takes for that to drop its connection.
    """

    def _probe_answering(self, tags, reattach=None):
        """A passive-declare probe answering ``tags``, reattaching a consumer while it runs."""
        async def probe(conn_id, queues, expected):
            taken_at = time.monotonic()
            if reattach is not None:
                reattach()
            return set(tags(expected)), None, None, taken_at

        return probe

    @pytest.mark.asyncio
    async def test_an_answer_about_the_previous_tag_neither_confirms_nor_denies(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.negative_checks = 1  # a denial here would condemn the subscription
        manager._http_client = None
        probe = self._probe_answering(
            lambda expected: expected,
            reattach=lambda: setattr(
                entry.state, "consumer_tag", _consumer_tag(7, "reattached")
            ),
        )

        with patch.object(manager, "_probe_by_passive_declare", side_effect=probe):
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert entry.negative_checks == 1, "an answer that does not apply is no evidence"
        assert entry.state.denied_tag is None, "the tag it holds now was never asked about"
        assert manager._conn("rmq_default").liveness.status is None

    @pytest.mark.asyncio
    async def test_a_dropped_answer_leaves_the_row_alone(self, manager):
        """The verdict is not written either: a false 'error' suppresses every
        'listening' the subscription reports until another probe confirms it."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        manager._http_client = None
        probe = self._probe_answering(
            lambda expected: expected,
            reattach=lambda: setattr(
                entry.state, "consumer_tag", _consumer_tag(7, "reattached")
            ),
        )
        stored = []

        def write(session, sub_id, status, last_error=None):
            stored.append(status)

        with patch.object(manager, "_probe_by_passive_declare", side_effect=probe), \
             _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await manager._check_liveness([sub])
            await entry.state.write(_SUB_LISTENING, last_error=None)
            await manager._store_unwritten_statuses()

        assert stored == [_SUB_LISTENING]

    @pytest.mark.asyncio
    async def test_an_answer_about_the_tag_it_still_holds_condemns_it(self, manager):
        """Dropping an answer that does not apply must leave a dead consumer condemnable:
        a registration the task still holds is judged exactly as it always was."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.negative_checks = 1
        manager._http_client = None
        probe = self._probe_answering(lambda expected: set())

        with patch.object(manager, "_probe_by_passive_declare", side_effect=probe):
            result = await manager._check_liveness([sub])

        assert result == ({7}, {"rmq_default"})
        assert entry.state.denied_tag == entry.state.consumer_tag

    @pytest.mark.asyncio
    async def test_the_subscription_that_did_not_reattach_is_still_judged(self, manager):
        """One dropped answer is one subscription's, not the cycle's."""
        reattached = _sub(id=7, queue_name="orders")
        held = _sub(id=8, queue_name="events")
        entry_a = _register_active(manager, reattached)
        entry_b = _register_active(manager, held)
        entry_a.negative_checks = entry_b.negative_checks = 1
        manager._http_client = None
        probe = self._probe_answering(
            lambda expected: set(),
            reattach=lambda: setattr(
                entry_a.state, "consumer_tag", _consumer_tag(7, "reattached")
            ),
        )

        with patch.object(manager, "_probe_by_passive_declare", side_effect=probe):
            result = await manager._check_liveness([reattached, held])

        assert result == ({8}, {"rmq_default"})
        assert entry_a.negative_checks == 1
        assert entry_b.negative_checks == 2

    @pytest.mark.asyncio
    async def test_a_fire_consumer_that_reattached_keeps_its_counter(self, manager):
        sub = _sub(id=7, queue_name="orders", cooldown=30)
        entry = _register_active(manager, sub)
        fire = _register_fire(manager)
        fire.negative_checks = 1
        manager._http_client = None
        probe = self._probe_answering(
            lambda expected: {entry.state.consumer_tag},
            reattach=lambda: setattr(
                fire.state, "consumer_tag", _consumer_tag("fire", "reattached")
            ),
        )

        with patch.object(manager, "_probe_by_passive_declare", side_effect=probe):
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert fire.negative_checks == 1
        assert manager._fire_needs_restart is False

    @pytest.mark.asyncio
    async def test_a_fire_consumer_that_kept_its_tag_is_still_condemned(self, manager):
        sub = _sub(id=7, queue_name="orders", cooldown=30)
        entry = _register_active(manager, sub)
        fire = _register_fire(manager)
        fire.negative_checks = 1
        manager._http_client = None
        probe = self._probe_answering(lambda expected: {entry.state.consumer_tag})

        with patch.object(manager, "_probe_by_passive_declare", side_effect=probe):
            await manager._check_liveness([sub])

        assert fire.negative_checks == 2
        assert manager._fire_needs_restart is True


class TestAnAnswerOlderThanTheRegistrationItJudges:
    """The answer was taken before the registration it would be applied to existed.

    One Management API snapshot serves every conn_id of the cycle that logs in as the
    same account, so the moment it speaks for can precede the question about a later
    conn_id by the whole judging of an earlier one. A consumer that reattached in that
    gap holds a tag the snapshot was taken too early to hold — which is not evidence
    against it.
    """

    def test_a_matching_tag_registered_after_the_answer_is_still_not_judged_by_it(self):
        """The tag comparison passes here: the registration held now is the one the
        question was asked about. What the answer misses is its moment."""
        state = _state_with(_SUB_LISTENING, 7)
        taken_at = time.monotonic()
        state.consumer_tag = _consumer_tag(7, "reattached")

        assert _answer_misses(state, state.consumer_tag, taken_at) is not None
        assert _answer_misses(state, state.consumer_tag, time.monotonic()) is None

    @pytest.mark.asyncio
    async def test_a_snapshot_older_than_the_reattach_does_not_condemn(self, manager):
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        b = _sub(id=8, queue_name="events", conn_id="conn_b")
        entry_a = _register_active(manager, a)
        entry_b = _register_active(manager, b)
        entry_b.negative_checks = 1  # a denial here would condemn the subscription
        listed = [
            _consumer_entry(entry_a.state.consumer_tag, "orders"),
            _consumer_entry(entry_b.state.consumer_tag, "events"),
        ]

        def handler(request: httpx.Request) -> httpx.Response:
            # The subscription of conn_b reattaches while the snapshot is being taken —
            # long before conn_b is the one being judged.
            entry_b.state.consumer_tag = _consumer_tag(8, "reattached")
            return httpx.Response(200, json=listed)

        manager._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        with _patch_mgmt_connection_per_login({"conn_a": "shared", "conn_b": "shared"}):
            result = await manager._check_liveness([a, b])

        assert result == (set(), set()), "the reattached consumer is healthy"
        assert entry_b.negative_checks == 1, "an answer that does not apply is no evidence"
        assert entry_b.state.denied_tag is None, "the tag it holds now is not in that snapshot"
        assert manager._conn("conn_b").liveness.status is None
        assert manager._conn("conn_a").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_a_registration_the_snapshot_covers_is_condemned_as_before(self, manager):
        """Dropping an answer taken too early must leave a dead consumer condemnable: a
        registration that existed when the snapshot was taken and is missing from it is
        denied, and the second negative check drops its connection."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        b = _sub(id=8, queue_name="events", conn_id="conn_b")
        entry_a = _register_active(manager, a)
        entry_b = _register_active(manager, b)
        entry_b.negative_checks = 1
        manager._http_client = _mgmt_client(
            [_consumer_entry(entry_a.state.consumer_tag, "orders")]
        )

        with _patch_mgmt_connection_per_login({"conn_a": "shared", "conn_b": "shared"}):
            result = await manager._check_liveness([a, b])

        assert result == ({8}, {"conn_b"})
        assert entry_b.negative_checks == 2
        assert entry_b.state.denied_tag == entry_b.state.consumer_tag
        assert manager._conn("conn_a").liveness.status == "connected"
        assert manager._conn("conn_b").liveness.status == "error"


class TestASubscriptionBetweenTwoAttaches:
    """The attach that made a subscription a candidate ends while the cycle runs.

    A pass of a consumer task clears its tag, closes its channel and pauses before it
    attaches again, and the status write that says so comes only once the pause is over
    — so for those seconds the state reads ``listening`` while holding no tag at all.
    The broker can be asked nothing about such a subscription, and a probe that names no
    tag of ours is not the broker denying it.
    """

    @staticmethod
    def _pool_for(manager, conn_id: str, entry) -> None:
        """Pool a connection that answers a passive declare, with ``entry`` attached to it."""
        manager._conn(conn_id).timeouts = AmqpTimeouts(connect=0.05, rpc=0.05)
        _pool_consume_connection(
            manager,
            _make_live_connection(queue=MagicMock()),
            entry.state,
            conn_id=conn_id,
        )

    @staticmethod
    def _detach(entry) -> None:
        """End ``entry``'s attach the way the consumer task's ``finally`` ends it."""
        entry.state.consumer_tag = None
        entry.state.connection = None

    @pytest.mark.asyncio
    async def test_a_subscription_holding_no_tag_is_not_denied(self, manager):
        """conn_a is probed first, and the subscription of conn_b ends its attach while
        that probe runs — before conn_b is asked about at all."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        b = _sub(id=8, queue_name="events", conn_id="conn_b")
        entry_a = _register_active(manager, a)
        entry_b = _register_active(manager, b)
        entry_b.negative_checks = 1  # a denial here would condemn the subscription
        manager._http_client = None
        self._pool_for(manager, "conn_a", entry_a)
        self._pool_for(manager, "conn_b", entry_b)
        pooled_a = manager._conn("conn_a").connections[_ROLE_CONSUME]
        pooled_a.channel.return_value.declare_queue = AsyncMock(
            side_effect=lambda *a, **kw: self._detach(entry_b) or MagicMock()
        )

        result = await manager._check_liveness([a, b])

        assert result == (set(), set())
        assert entry_b.negative_checks == 1, "a subscription holding no tag is asked nothing"
        assert entry_b.state.denied_tag is None
        assert manager._conn("conn_b").liveness.status is None
        assert manager._conn("conn_a").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_the_row_of_a_subscription_holding_no_tag_is_left_alone(self, manager):
        """A verdict naming consumer ``None`` is what an operator reads as a broker that
        dropped the subscription, and it suppresses every ``listening`` reported after
        it until a probe confirms the consumer again."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        b = _sub(id=8, queue_name="events", conn_id="conn_b")
        entry_a = _register_active(manager, a)
        entry_b = _register_active(manager, b)
        manager._http_client = None
        self._pool_for(manager, "conn_a", entry_a)
        self._pool_for(manager, "conn_b", entry_b)
        pooled_a = manager._conn("conn_a").connections[_ROLE_CONSUME]
        pooled_a.channel.return_value.declare_queue = AsyncMock(
            side_effect=lambda *args, **kw: self._detach(entry_b) or MagicMock()
        )
        stored = []

        def write(session, sub_id, status, last_error=None):
            stored.append((sub_id, status, last_error))

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await manager._check_liveness([a, b])
            await manager._store_unwritten_statuses()

        assert stored == []

    @pytest.mark.asyncio
    async def test_a_snapshot_naming_no_tag_of_a_detached_subscription_is_not_a_denial(
        self, manager
    ):
        """The Management API tier reaches the same subscription by another route: the
        snapshot is taken after the attach ended, so its moment is no objection."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        b = _sub(id=8, queue_name="events", conn_id="conn_b")
        entry_a = _register_active(manager, a)
        entry_b = _register_active(manager, b)
        entry_b.negative_checks = 1
        listed = [
            _consumer_entry(entry_a.state.consumer_tag, "orders"),
            _consumer_entry(entry_b.state.consumer_tag, "events"),
        ]
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if len(requests) == 1:  # conn_a's snapshot; conn_b's is taken after it
                self._detach(entry_b)
            return httpx.Response(200, json=listed)

        manager._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        with _patch_mgmt_connection_per_login({"conn_a": "a", "conn_b": "b"}):
            result = await manager._check_liveness([a, b])

        assert result == (set(), set())
        assert entry_b.negative_checks == 1
        assert entry_b.state.denied_tag is None
        assert manager._conn("conn_b").liveness.status is None

    @pytest.mark.asyncio
    async def test_the_consumer_that_kept_its_tag_is_condemned_beside_it(self, manager):
        """Skipping a subscription that holds no tag is one subscription's business: the
        consumer sharing its connection and still holding a tag the broker does not have
        reaches its second negative check and takes the connection down."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        gone = _sub(id=8, queue_name="events", conn_id="conn_b")
        dead = _sub(id=9, queue_name="alerts", conn_id="conn_b")
        entry_a = _register_active(manager, a)
        entry_gone = _register_active(manager, gone)
        entry_dead = _register_active(manager, dead)
        entry_dead.negative_checks = 1
        listed = [
            _consumer_entry(entry_a.state.consumer_tag, "orders"),
            _consumer_entry(entry_gone.state.consumer_tag, "events"),
        ]
        requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            requests.append(request)
            if len(requests) == 1:
                self._detach(entry_gone)
            return httpx.Response(200, json=listed)

        manager._http_client = httpx.AsyncClient(transport=httpx.MockTransport(handler))

        with _patch_mgmt_connection_per_login({"conn_a": "a", "conn_b": "b"}):
            result = await manager._check_liveness([a, gone, dead])

        assert result == ({9}, {"conn_b"})
        assert entry_dead.negative_checks == 2
        assert entry_dead.state.denied_tag == entry_dead.state.consumer_tag
        assert entry_gone.negative_checks == 0
        assert manager._conn("conn_b").liveness.status == "error"

    @pytest.mark.asyncio
    async def test_a_conn_id_left_with_nothing_to_ask_is_not_reported_connected(self, manager):
        """The fire consumer is the only candidate its conn_id has, and it detaches while
        the other conn_id is judged. A probe with no queue to name is answered without
        reaching the broker, so there is nothing for it to confirm."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        entry_a = _register_active(manager, a)
        manager._http_client = None
        self._pool_for(manager, "conn_a", entry_a)
        manager._conn("conn_b").connections[_ROLE_CONSUME] = _make_live_connection(
            queue=MagicMock()
        )
        fire = _register_fire(manager, conn_id="conn_b")
        probed = []
        pooled_a = manager._conn("conn_a").connections[_ROLE_CONSUME]
        pooled_a.channel.return_value.declare_queue = AsyncMock(
            side_effect=lambda name, **kw: probed.append(name)
            or setattr(fire.state, "consumer_tag", None)
            or MagicMock()
        )
        pooled_b = manager._conn("conn_b").connections[_ROLE_CONSUME]
        pooled_b.channel.return_value.declare_queue = AsyncMock(
            side_effect=lambda name, **kw: probed.append(name) or MagicMock()
        )

        result = await manager._check_liveness([a])

        assert result == (set(), set())
        assert probed == ["orders"], "conn_b has no queue left to name the probe with"
        assert manager._conn("conn_b").liveness.status is None
        assert manager._conn("conn_a").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_the_next_cycle_judges_the_conn_id_it_left_no_verdict_for(self, manager):
        """A conn_id every consumer of which detached mid-cycle reaches a judge again:
        the next partition sorts its task as stalled, and the stuck-cycle counter that
        eventually condemns a connection nothing consumes on starts running."""
        a = _sub(id=7, queue_name="orders", conn_id="conn_a")
        b = _sub(id=8, queue_name="events", conn_id="conn_b")
        entry_a = _register_active(manager, a)
        entry_b = _register_active(manager, b)
        manager._http_client = None
        self._pool_for(manager, "conn_a", entry_a)
        self._pool_for(manager, "conn_b", entry_b)
        pooled_a = manager._conn("conn_a").connections[_ROLE_CONSUME]
        pooled_a.channel.return_value.declare_queue = AsyncMock(
            side_effect=lambda *args, **kw: self._detach(entry_b) or MagicMock()
        )

        first = await manager._check_liveness([a, b])
        assert first == (set(), set())
        assert manager._conn("conn_b").liveness.status is None
        assert manager._conn("conn_b").stuck_cycles == 0

        second = await manager._check_liveness([a, b])

        assert second == (set(), set())
        assert manager._conn("conn_b").stuck_cycles == 1, (
            "the subscription is stalled, and the judge for a stalled conn_id counts it"
        )


def _attached_channel(consumer_tag: str):
    """A live channel laid out the way ``aio_pika`` lays one out, holding ``consumer_tag``.

    ``aio_pika``'s channel wraps an ``aiormq`` one, and that is where the map of
    consumers the client believes it has registered lives.
    """
    channel = _make_live_channel(queue=MagicMock())
    channel._channel = SimpleNamespace(
        channel=SimpleNamespace(consumers={consumer_tag: object()})
    )
    return channel


async def _server_cancels(channel, consumer_tag: str) -> None:
    """Hand ``channel`` the ``basic.cancel`` a broker sends when it drops our consumer.

    The frame goes to ``aiormq``'s own handler, which is what the client does with it:
    the tag is dropped and nothing else happens — no exception, no callback, and the
    connection and the channel stay open.
    """
    await aiormq.Channel._on_cancel_frame(
        channel._channel.channel,
        aiormq.spec.Basic.Cancel(consumer_tag=consumer_tag),
    )


class TestServerSideCancel:
    """The broker cancels a consumer of ours and leaves everything else standing.

    A deleted queue, a quorum-queue or stream leader change, the node hosting a classic
    queue restarting under a client connected to another node: the broker sends
    ``basic.cancel``, the connection and the channel stay open, and every probe of that
    connection goes on succeeding while nothing consumes.
    """

    def _cancelled_sub(self, manager, http_client=None):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        channel = _attached_channel(entry.state.consumer_tag)
        entry.state.channel = channel
        manager._http_client = http_client
        _fast_timeouts(manager)
        _pool_consume_connection(
            manager, _make_live_connection(channel=channel), entry.state
        )
        return sub, entry, channel

    @pytest.mark.asyncio
    async def test_a_server_cancelled_consumer_is_condemned_on_the_passive_declare_tier(
        self, manager
    ):
        """The passive declare keeps succeeding — it is the same open connection — so
        without the client's own answer the subscription would read 'listening' with no
        consumer behind it for as long as the process runs."""
        sub, entry, channel = self._cancelled_sub(manager)
        await _server_cancels(channel, entry.state.consumer_tag)

        first = await manager._check_liveness([sub])
        second = await manager._check_liveness([sub])

        assert first == (set(), set())
        assert manager._conn("rmq_default").liveness.status == "error"
        assert second == ({7}, {"rmq_default"})
        assert channel.declare_queue.await_count == 2

    @pytest.mark.asyncio
    async def test_a_cancelled_consumer_is_condemned_while_the_api_says_nothing(
        self, manager
    ):
        """A probe that brings no data does not silence the channel the client holds.
        The tag is gone from that channel's own consumer map, which is an answer about
        this registration and needs no request — reading it as 'no data' leaves a green
        row over a consumer the broker has already dropped."""
        sub, entry, channel = self._cancelled_sub(
            manager,
            http_client=_mgmt_client({"error": "unauthorized"}, status_code=401),
        )
        await _server_cancels(channel, entry.state.consumer_tag)

        with _patch_mgmt_connection():
            result = await manager._check_liveness([sub])

        assert result == (set(), set()), "one negative check restarts nothing"
        assert entry.negative_checks == 1
        verdict = manager._conn("rmq_default").liveness
        assert verdict.status == "error"
        assert "cancelled" in verdict.reason

    @pytest.mark.asyncio
    async def test_a_registered_consumer_keeps_its_counters_while_the_api_says_nothing(
        self, manager
    ):
        """The channel answers only for the registrations it has lost. One it still
        carries is a registration nothing could ask about this cycle, and its counters
        must stay where they were."""
        sub, entry, _channel = self._cancelled_sub(
            manager,
            http_client=_mgmt_client({"error": "unauthorized"}, status_code=401),
        )

        with _patch_mgmt_connection():
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert entry.negative_checks == 0
        assert manager._conn("rmq_default").liveness.status is None

    @pytest.mark.asyncio
    async def test_a_registered_consumer_is_still_vouched_for(self, manager):
        """The tier must keep vouching for healthy subscriptions: refusing to would
        restart every one of them every second cycle for the life of the process."""
        sub, entry, _channel = self._cancelled_sub(manager)

        result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert entry.negative_checks == 0
        assert manager._conn("rmq_default").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_a_server_cancelled_consumer_is_condemned_on_the_management_tier(
        self, manager
    ):
        """The Management API is asked about the tag, but the answer can be stale; the
        client's own channel is the one that cannot be."""
        sub, entry, channel = self._cancelled_sub(
            manager,
            http_client=_mgmt_client(
                [{"queue": {"name": "orders"}, "consumer_tag": _consumer_tag(7)}]
            ),
        )
        await _server_cancels(channel, entry.state.consumer_tag)

        with _patch_mgmt_connection():
            first = await manager._check_liveness([sub])
            second = await manager._check_liveness([sub])

        assert first == (set(), set())
        assert second == ({7}, {"rmq_default"})

    @pytest.mark.asyncio
    async def test_a_delivery_finishing_after_the_cancel_does_not_take_the_verdict_back(
        self, manager
    ):
        """The broker goes on handing over what it had already buffered for a consumer
        it has just cancelled, so the trigger runs, the delivery is acknowledged and the
        subscription reports 'listening' — after the check wrote the verdict that
        condemned the registration. The registration is what the row is about."""
        sub, entry, channel = self._cancelled_sub(manager)
        await _server_cancels(channel, entry.state.consumer_tag)
        stored = []

        def write(session, sub_id, status, last_error=None):
            stored.append(status)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await manager._check_liveness([sub])
            await entry.state.write(_SUB_LISTENING, last_error=None)
            await manager._store_unwritten_statuses()

        assert stored == ["error"]
        assert entry.state.status == _SUB_LISTENING, (
            "the reported status keeps the subscription a candidate of the check"
        )

    @pytest.mark.asyncio
    async def test_a_delivery_on_a_registration_the_channel_holds_reports_listening(
        self, manager
    ):
        """The refusal is about the lost registration and nothing else: a consumer the
        channel still carries is what every ordinary delivery reports through."""
        _sub_row, entry, _channel = self._cancelled_sub(manager)
        stored = []

        def write(session, sub_id, status, last_error=None):
            stored.append(status)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await entry.state.write(_SUB_LISTENING, last_error=None)

        assert stored == [_SUB_LISTENING]

    @pytest.mark.asyncio
    async def test_a_channel_that_cannot_be_read_leaves_the_verdict_to_the_probe(
        self, manager
    ):
        """A channel not yet opened, one being restored, or one a later library version
        lays out differently answers nothing, and nothing is what it must cost."""
        sub, _entry, channel = self._cancelled_sub(manager)
        channel._channel = None

        result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert manager._conn("rmq_default").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_a_server_cancelled_fire_consumer_is_condemned(self, manager):
        sub = _sub(id=7, queue_name="orders", cooldown=30)
        _register_active(manager, sub)
        fire = _register_fire(manager)
        channel = _attached_channel(fire.state.consumer_tag)
        fire.state.channel = channel
        manager._http_client = None
        _fast_timeouts(manager)
        fire.connection = _pool_consume_connection(
            manager, _make_live_connection(channel=channel), manager._active[7].state
        )
        await _server_cancels(channel, fire.state.consumer_tag)

        await manager._check_liveness([sub])
        await manager._check_liveness([sub])

        assert manager._fire_needs_restart is True
        assert manager._conn("rmq_default").liveness.status == "error"


class TestUnreadableAirflowConnection:
    """The liveness check cannot read the Airflow connection of a conn_id.

    A connection renamed or deleted in the UI while subscriptions still name it, or a
    secrets backend that keeps refusing: the check would otherwise produce no data for
    every cycle there is, leaving the stored ``connected`` and the ``listening`` rows
    standing behind it.
    """

    def _manager_with_unreadable_row(self, manager, connection):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        manager._http_client = _mgmt_client([])
        _fast_timeouts(manager)
        _pool_consume_connection(manager, connection, entry.state)
        return sub

    @pytest.mark.asyncio
    async def test_an_unreadable_row_falls_back_to_the_amqp_probe_and_condemns(
        self, manager
    ):
        channel = _make_live_channel()
        channel.declare_queue = AsyncMock(side_effect=ConnectionError("gone"))
        sub = self._manager_with_unreadable_row(manager, _make_live_connection(channel=channel))

        with patch(
            "airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
            side_effect=RuntimeError("connection 'rmq_default' is not defined"),
        ):
            first = await manager._check_liveness([sub])
            unknown = manager._conn("rmq_default").liveness.status
            second = await manager._check_liveness([sub])
            after_fallback = manager._conn("rmq_default").liveness.status
            third = await manager._check_liveness([sub])

        assert (first, unknown) == ((set(), set()), None)
        assert (second, after_fallback) == ((set(), set()), "error")
        assert third == ({7}, {"rmq_default"})

    @pytest.mark.asyncio
    async def test_the_fallback_still_vouches_for_a_pooled_connection_that_answers(
        self, manager
    ):
        """The AMQP probe needs no metadata row, so an unreadable one must not by itself
        condemn subscriptions running on a connection that is perfectly healthy."""
        channel = _make_live_channel(queue=MagicMock())
        sub = self._manager_with_unreadable_row(manager, _make_live_connection(channel=channel))

        with patch(
            "airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
            side_effect=RuntimeError("connection 'rmq_default' is not defined"),
        ):
            await manager._check_liveness([sub])
            second = await manager._check_liveness([sub])

        assert second == (set(), set())
        assert manager._conn("rmq_default").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_a_row_that_can_be_read_again_clears_the_counter(self, manager):
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = _mgmt_client([])
        manager._conn("rmq_default").conn_read_failures = 1

        with _patch_mgmt_connection():
            await manager._check_liveness([sub])

        assert manager._conn("rmq_default").conn_read_failures == 0


# ---------------------------------------------------------------------------
# Tests for recovery: rebuilding condemned consumers and writing honest statuses
# ---------------------------------------------------------------------------

async def _never_returns(*args, **kwargs):
    await asyncio.Future()


async def _drain(manager) -> None:
    """Cancel everything the manager is running so the test leaves no pending tasks."""
    tasks = [entry.task for entry in manager._active.values()]
    if manager._fire_task is not None:
        tasks.append(manager._fire_task)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@contextlib.contextmanager
def _conn_status_in_flight():
    """Hold ``rmq_watcher_conn_status`` the way a write stuck in the database holds it.

    The write that took the turn is inside its transaction for the length of the block;
    every other writer of those rows meets it there.
    """
    _conn_status_lock.acquire()
    try:
        yield
    finally:
        _conn_status_lock.release()


def _patch_status_writer(upsert, stored: dict[str, str] | None = None):
    """Patch the three names ``_update_all_conn_counts`` writes its row through."""
    rows = [
        SimpleNamespace(conn_id=conn_id, status=status)
        for conn_id, status in (stored or {}).items()
    ]
    return (
        _patch_watcher_session(),
        patch("airflow_provider_rmq.watcher.consumer.get_conn_statuses", return_value=rows),
        patch("airflow_provider_rmq.watcher.consumer.upsert_conn_status", upsert),
    )


async def _write_statuses(manager, subscriptions: list[dict], stored: dict[str, str] | None = None):
    """Run the status writer against patched storage and return the calls it made."""
    upsert = MagicMock()
    with ExitStack() as stack:
        for patcher in _patch_status_writer(upsert, stored):
            stack.enter_context(patcher)
        await manager._update_all_conn_counts(subscriptions)
    return upsert


class TestRecoverDeadConsumers:
    @pytest.mark.asyncio
    async def test_condemned_subscription_is_rebuilt_on_a_fresh_connection(self, manager):
        """Regression on the zombie connection: restarting the task alone would hand the
        new consumer the same silent connection object out of the pool."""
        sub = _sub(id=7, queue_name="orders")
        queue = _make_push_queue()
        zombie = _make_live_connection(channel=_make_live_channel(queue=queue))
        fresh = _make_live_connection(channel=_make_live_channel(queue=queue))
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   side_effect=[zombie, fresh]) as connect:
            await manager.reconcile([sub])
            await _wait_for_status(manager._active[7], "listening")
            first_task = manager._active[7].task

            await manager.reconcile([sub])  # first negative check
            assert manager._active[7].task is first_task

            await manager.reconcile([sub])  # second negative check → rebuild
            await _wait_for_status(manager._active[7], "listening")

            assert connect.call_count == 2
            assert manager._active[7].task is not first_task
            assert first_task.done()
            assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is fresh
            assert zombie not in _pooled_connections(manager)
            zombie.close.assert_awaited()

            await _drain(manager)

    @pytest.mark.asyncio
    async def test_live_subscription_is_left_alone(self, manager):
        sub = _sub(id=7, queue_name="orders")
        queue = _make_push_queue()
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=connection) as connect:
            await manager.reconcile([sub])
            await _wait_for_status(manager._active[7], "listening")
            first_task = manager._active[7].task
            # The broker answers with the tag the task actually registered.
            manager._http_client = _mgmt_client(
                [_consumer_entry(manager._active[7].state.consumer_tag, "orders")]
            )

            await manager.reconcile([sub])
            await manager.reconcile([sub])

            assert manager._active[7].task is first_task
            assert connect.call_count == 1
            assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is connection
            connection.close.assert_not_awaited()

            await _drain(manager)

    @pytest.mark.asyncio
    async def test_confirmed_siblings_of_a_dropped_connection_are_named_in_the_log(
        self, manager
    ):
        """The drop closes the connection under the subscriptions the check confirmed;
        they are not restarted and recover through their own retry loop, so the log has
        to say whose connection went away under them."""
        dead = _sub(id=7, queue_name="orders")
        alive = _sub(id=8, queue_name="events")
        _register_active(manager, dead, real_task=True)
        alive_entry = _register_active(manager, alive, real_task=True)
        manager._http_client = _mgmt_client(
            [_consumer_entry(alive_entry.state.consumer_tag, "events")]
        )

        with _patch_mgmt_connection(), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._check_liveness([dead, alive])
            to_restart, to_recreate = await manager._check_liveness(
                [dead, alive]
            )

        assert to_restart == {7}
        assert to_recreate == {"rmq_default"}
        assert any(
            "share it and keep their own retry loop" in str(c.args[0])
            and c.args[3] == [8]
            for c in mock_log.warning.call_args_list
        ), mock_log.warning.call_args_list
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_restart_is_logged_and_counted(self, manager):
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub, real_task=True)
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection(), \
             patch.object(manager, "_consume_subscription", side_effect=_never_returns), \
             patch("airflow_provider_rmq.watcher.consumer.incr") as incr, \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager._recover_dead_consumers([sub])
            await manager._recover_dead_consumers([sub])

        assert incr.call_args_list == [call("rmq_watcher.consumer_restarted")]
        assert any(
            "Restarting consumer of subscription" in str(c.args[0])
            for c in mock_log.warning.call_args_list
        )
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_fire_task_on_the_dropped_conn_id_is_cancelled_and_started_again(self, manager):
        """The fire task holds the connection object it was handed at startup, so a
        connection recreated under it leaves it spinning on a closed one.

        The broker confirms the fire consumer here, so nothing about the fire task itself
        asks for a restart: the only thing that takes it down is its conn_id being
        condemned for the subscriptions that share it."""
        sub = _sub(id=7, queue_name="orders", cooldown=300)
        _register_active(manager, sub, real_task=True)
        fire = _register_fire(manager, real_task=True)
        old_fire_task = manager._fire_task
        old_conn = _make_live_connection()
        fresh = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = old_conn
        manager._http_client = _mgmt_client(
            [_consumer_entry(fire.state.consumer_tag, _FIRE_QUEUE)]
        )

        with _patch_mgmt_connection(), \
             patch.object(manager, "_consume_subscription", side_effect=_never_returns), \
             patch.object(manager, "_consume_fire_queue", side_effect=_never_returns), \
             patch.object(manager, "_get_or_create_connection",
                          new_callable=AsyncMock, return_value=fresh):
            await manager._recover_dead_consumers([sub])
            assert manager._fire_task is old_fire_task, (
                "one negative check on a subscription must not touch the fire task"
            )
            await manager._recover_dead_consumers([sub])

        assert fire.negative_checks == 0, "the broker holds the fire consumer"
        assert manager._fire_needs_restart is False, (
            "nothing but the condemned conn_id may explain this restart"
        )
        assert old_fire_task.done()
        assert manager._fire_task is not None and manager._fire_task is not old_fire_task
        assert not manager._fire_task.done()
        assert manager._fire_state is not None
        assert manager._fire_state.conn_id == "rmq_default"
        assert old_conn not in _pooled_connections(manager)
        old_conn.close.assert_awaited()
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_fire_task_failing_alone_does_not_touch_the_subscriptions(self, manager):
        sub = _sub(id=7, queue_name="orders", cooldown=300)
        entry = _register_active(manager, sub, real_task=True)
        _register_fire(manager, real_task=True)
        old_fire_task = manager._fire_task
        connection = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection
        manager._http_client = _mgmt_client([_consumer_entry(_consumer_tag(7), "orders")])

        with _patch_mgmt_connection(), \
             patch.object(manager, "_consume_subscription", side_effect=_never_returns), \
             patch.object(manager, "_consume_fire_queue", side_effect=_never_returns), \
             patch.object(manager, "_get_or_create_connection",
                          new_callable=AsyncMock, return_value=connection):
            await manager._recover_dead_consumers([sub])
            await manager._recover_dead_consumers([sub])

        assert manager._active[7] is entry, "the subscription must not be restarted"
        assert not entry.task.done()
        assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is connection
        connection.close.assert_not_awaited()
        assert old_fire_task.done()
        assert manager._fire_task is not old_fire_task
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_rate_limited_verdict_leaves_the_connection_in_place(self, manager):
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub, real_task=True)
        first_conn = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = first_conn
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection(), \
             patch.object(manager, "_consume_subscription", side_effect=_never_returns):
            await manager._recover_dead_consumers([sub])
            await manager._recover_dead_consumers([sub])  # rebuild happens here
            assert first_conn not in _pooled_connections(manager)

            second_conn = _make_live_connection()
            manager._conn("rmq_default").connections[_ROLE_CONSUME] = second_conn
            manager._active[7].state = _state_with("listening", 7)
            rebuilt_task = manager._active[7].task

            await manager._recover_dead_consumers([sub])
            await manager._recover_dead_consumers([sub])

        assert manager._conn("rmq_default").liveness.status == "degraded"
        assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is second_conn
        second_conn.close.assert_not_awaited()
        assert manager._active[7].task is rebuilt_task

        upsert = await _write_statuses(manager, [sub])
        assert upsert.call_args.args[2] == "degraded"
        await _drain(manager)


class TestConsumersOnAReplacedConnection:
    """A task whose consumer is registered on a connection object the pool has replaced.

    ``RobustConnection`` clears its transport before every reconnect attempt, so the
    object reports itself open with nothing underneath. The channel close that follows
    the broken link never wakes the queue iterator, and closing an object whose
    transport is already gone resolves nothing either, so the task neither ends nor
    reports anything: it holds a registration the broker dropped, for as long as the
    process runs.
    """

    def _blocked_on_a_replaced_connection(self, manager):
        """The pooled connection of a listening subscription, emptied by its reconnect."""
        connection = manager._conn("rmq_default").connections[_ROLE_CONSUME]
        connection.transport = None
        return connection

    @pytest.mark.asyncio
    async def test_a_probe_on_a_fresh_connection_does_not_confirm_the_old_tags(self, manager):
        """The probe builds a connection where the pool has none usable, and a declare
        that succeeds on the new object says nothing about tags registered on the one it
        replaced: vouching for them is what reports a subscription as consuming while
        the broker holds no consumer of it at all."""
        sub = _sub(id=7, queue_name="orders")
        blocked = _make_live_connection(channel=_make_live_channel(queue=_make_push_queue()))
        blocked.close = _hanging_call
        fresh = _make_live_connection(channel=_make_live_channel(queue=MagicMock()))
        manager._http_client = None  # no management_url: the probe is a passive declare

        with patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()), \
             _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch("airflow_provider_rmq.watcher.consumer._CLOSE_TIMEOUT", 0.05), \
             patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   side_effect=[blocked, fresh]) as connect:
            await manager.reconcile([sub])
            await _wait_for_status(manager._active[7], "listening")
            orphan = manager._active[7].task
            self._blocked_on_a_replaced_connection(manager)

            await manager.reconcile([sub])

            assert connect.call_count == 2
            assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is fresh
            assert manager._active[7].task is orphan  # blocked on the replaced object
            assert manager._active[7].negative_checks == 1
            assert manager._conn("rmq_default").liveness.status == "error"

            await _drain(manager)

    @pytest.mark.asyncio
    async def test_a_probe_run_after_the_replacement_confirms_nothing_either(self, manager):
        """The pool can be moved on before the check runs — a cooldown connection
        provisioned earlier in the cycle rebuilds it — and the probe then starts and
        finishes on the replacement. Nothing about it changed while it ran, and it still
        holds none of the registrations it is being asked to vouch for."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.state.connection = _make_live_connection()  # the object the pool left behind
        manager._http_client = None
        _fast_timeouts(manager)
        replacement = _pool_consume_connection(
            manager, _make_live_connection(channel=_make_live_channel(queue=MagicMock()))
        )

        first = await manager._check_liveness([sub])
        second = await manager._check_liveness([sub])

        assert first == (set(), set())
        assert entry.negative_checks == 2
        assert second == ({7}, {"rmq_default"})
        assert manager._conn("rmq_default").liveness.status == "error"
        assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is replacement

    @pytest.mark.asyncio
    async def test_a_fire_consumer_on_a_replaced_connection_is_not_vouched_for(self, manager):
        """The fire consumer holds the object it was handed for its whole life, so it is
        the object it holds that the declare has to have been answered by."""
        sub = _sub(id=7, queue_name="orders", cooldown=30)
        entry = _register_active(manager, sub)
        manager._http_client = None
        _fast_timeouts(manager)
        _pool_consume_connection(
            manager,
            _make_live_connection(channel=_make_live_channel(queue=MagicMock())),
            entry.state,
        )
        fire = _register_fire(manager)
        fire.connection = _make_live_connection()  # the object the pool left behind

        await manager._check_liveness([sub])
        await manager._check_liveness([sub])

        assert entry.negative_checks == 0, "this subscription is on the pooled connection"
        assert fire.negative_checks == 2
        assert manager._fire_needs_restart is True

    @pytest.mark.asyncio
    async def test_the_orphaned_subscription_is_restarted_on_the_pooled_connection(self, manager):
        """The task is on an object nothing can reach any more; only the manager
        comparing what it holds with what the pool holds ever ends it."""
        sub = _sub(id=7, queue_name="orders")
        blocked = _make_live_connection(channel=_make_live_channel(queue=_make_push_queue()))
        blocked.close = _hanging_call
        fresh = _make_live_connection(channel=_make_live_channel(queue=_make_push_queue()))
        manager._http_client = None

        with patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()), \
             _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch("airflow_provider_rmq.watcher.consumer._CLOSE_TIMEOUT", 0.05), \
             patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   side_effect=[blocked, fresh]) as connect:
            await manager.reconcile([sub])
            await _wait_for_status(manager._active[7], "listening")
            orphan = manager._active[7].task
            self._blocked_on_a_replaced_connection(manager)

            await manager.reconcile([sub])  # the probe replaces the pooled connection
            await manager.reconcile([sub])  # the task left on the replaced one is restarted

            assert manager._active[7].task is not orphan
            assert orphan.done()
            await _wait_for_status(manager._active[7], "listening")
            assert manager._active[7].state.connection is fresh
            assert connect.call_count == 2

            await _drain(manager)

    @pytest.mark.asyncio
    async def test_a_subscription_on_the_pooled_connection_is_left_alone(self, manager):
        """The comparison is between objects, so a task on the connection the pool
        actually holds is never restarted by it."""
        sub = _sub(id=7, queue_name="orders")
        connection = _make_live_connection(channel=_make_live_channel(queue=_make_push_queue()))
        manager._http_client = None

        with patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()), \
             _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=connection) as connect:
            await manager.reconcile([sub])
            await _wait_for_status(manager._active[7], "listening")
            first_task = manager._active[7].task

            await manager.reconcile([sub])
            await manager.reconcile([sub])

            assert manager._active[7].task is first_task
            assert connect.call_count == 1

            await _drain(manager)

    @pytest.mark.asyncio
    async def test_a_subscription_between_attaches_is_left_to_its_own_retry_loop(self, manager):
        """A task connecting or backing off after an error is between connections by
        definition, and restarting it would cut its retry short every cycle."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub, status="error", real_task=True)
        entry.state.connection = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = _make_live_connection()

        assert manager._attached_to_replaced(entry, "rmq_default") is False

        await _drain(manager)


class TestTheRowOfAConsumerTheBrokerDenies:
    """What the subscriptions page says while the restart is still being waited for.

    The restart waits for a second negative check, and the recreation rate limit can
    hold it back for several cycles more. The report does not wait with it: a
    subscription reported as consuming beside a broker that holds no consumer of it is
    the report an incident lives behind.
    """

    @staticmethod
    def _captured_statuses(calls) -> list[tuple]:
        return [(c.args[1], c.args[2]) for c in calls]

    @pytest.mark.asyncio
    async def test_the_first_negative_check_writes_the_subscription_row(self, manager):
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = _mgmt_client([])  # the broker holds no consumer of ours
        written = MagicMock()

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status", written):
            await manager._check_liveness([sub])
            await manager._store_unwritten_statuses()

        assert self._captured_statuses(written.call_args_list) == [(7, "error")]
        assert "does not hold consumer" in written.call_args.kwargs["last_error"]

    @pytest.mark.asyncio
    async def test_a_verdict_held_back_by_the_rate_limit_still_writes_the_row(self, manager):
        """The drop is refused for several cycles; the row must not go back to claiming
        a consumer for all of them."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.negative_checks = 1
        manager._conn("rmq_default").last_drop_cycle = 0
        manager._http_client = _mgmt_client([])
        written = MagicMock()

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status", written):
            result = await manager._check_liveness([sub])
            await manager._store_unwritten_statuses()

        assert result == (set(), set())  # nothing restarted, nothing recreated
        assert manager._conn("rmq_default").liveness.status == "degraded"
        assert self._captured_statuses(written.call_args_list) == [(7, "error")]

    @pytest.mark.asyncio
    async def test_a_consumer_the_broker_confirms_again_gets_its_row_back(self, manager):
        """One negative answer must not leave the row red for the life of the task: the
        check is the only thing that hears the broker about a quiet subscription."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        written = MagicMock()

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status", written):
            manager._http_client = _mgmt_client([])
            await manager._check_liveness([sub])
            await manager._store_unwritten_statuses()
            await manager._http_client.aclose()

            manager._http_client = _mgmt_client(
                [_consumer_entry(entry.state.consumer_tag, "orders")]
            )
            await manager._check_liveness([sub])
            await manager._store_unwritten_statuses()

        assert self._captured_statuses(written.call_args_list) == [
            (7, "error"), (7, "listening")
        ]

    @pytest.mark.asyncio
    async def test_a_confirmed_consumer_costs_the_row_no_write(self, manager):
        """The check writes the row only where it has something to put right."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        manager._http_client = _mgmt_client(
            [_consumer_entry(entry.state.consumer_tag, "orders")]
        )
        written = MagicMock()

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status", written):
            await manager._check_liveness([sub])
            await manager._store_unwritten_statuses()

        written.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_delivery_does_not_take_the_brokers_verdict_back(self, manager):
        """The broker is the authority the row reports, and it can deny a registration
        the client's own channel still carries: a half-open connection, a proxy that
        dropped one direction, a node that let the consumer go while the client heard
        nothing. Whatever the broker had already buffered for that consumer keeps
        arriving, and every delivery reports ``listening`` behind the verdict."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.state.channel = _attached_channel(entry.state.consumer_tag)
        manager._http_client = _mgmt_client([])  # the broker holds no consumer of ours
        written = MagicMock()

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status", written):
            await manager._check_liveness([sub])
            await entry.state.write(_SUB_LISTENING, last_error=None)
            await manager._store_unwritten_statuses()

        assert not _cancelled_by_broker(entry.state), (
            "the premise: the channel still carries the tag the broker denies"
        )
        assert self._captured_statuses(written.call_args_list) == [(7, "error")]
        assert entry.state.status == _SUB_LISTENING, (
            "the reported status keeps the subscription a candidate of the check"
        )

    @pytest.mark.asyncio
    async def test_a_delivery_reports_again_once_the_broker_confirms(self, manager):
        """The refusal lasts exactly as long as the denial behind it.

        The check has nothing to take back once it has confirmed the consumer, so a
        trigger failure reported after that is the delivery's own to put right — and a
        denial left standing would keep the row red with nothing else able to write it.
        """
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.state.channel = _attached_channel(entry.state.consumer_tag)
        written = MagicMock()

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status", written):
            manager._http_client = _mgmt_client([])
            await manager._check_liveness([sub])
            await manager._store_unwritten_statuses()
            await manager._http_client.aclose()

            manager._http_client = _mgmt_client(
                [_consumer_entry(entry.state.consumer_tag, "orders")]
            )
            await manager._check_liveness([sub])
            await manager._store_unwritten_statuses()
            assert entry.negative_checks == 0

            await entry.state.write(_SUB_ERROR, last_error="triggering the DAG failed")
            await entry.state.write(_SUB_LISTENING, last_error=None)

        assert self._captured_statuses(written.call_args_list) == [
            (7, "error"), (7, "listening"), (7, "error"), (7, "listening")
        ]

    @pytest.mark.asyncio
    async def test_a_fresh_attach_reports_over_the_verdict_of_the_one_before(self, manager):
        """The verdict is about one registration. A task that attached again registered
        a tag of its own, which no earlier answer speaks for — held back by that answer,
        the row would stay red for as long as the task runs."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        manager._http_client = _mgmt_client([])
        written = MagicMock()

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status", written):
            await manager._check_liveness([sub])
            entry.state.consumer_tag = _consumer_tag(7, _attach_nonce())
            await entry.state.write(_SUB_LISTENING, last_error=None)
            await manager._store_unwritten_statuses()

        assert self._captured_statuses(written.call_args_list) == [(7, "listening")]

    @pytest.mark.asyncio
    async def test_a_restarted_consumer_writes_over_the_verdict_that_condemned_it(self, manager):
        """Both go through the one writer of this subscription, so the newest of them is
        what the row holds — never a verdict landing behind the task it restarted."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub, real_task=True)
        entry.negative_checks = 1
        manager._http_client = _mgmt_client([])
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = _make_live_connection()
        fresh = _make_live_connection(channel=_make_live_channel(queue=_make_push_queue()))
        stored: list[str] = []

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=fresh), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=lambda session, sub_id, status, last_error=None:
                   stored.append(status)):
            await manager._recover_dead_consumers([sub])
            await _wait_for_status(manager._active[7], "listening")
            await manager._store_unwritten_statuses()

        assert stored[-1] == "listening"
        assert not _status_writer(7).has_pending
        await _drain(manager)


class TestFireTaskFollowsItsConnId:
    @pytest.mark.asyncio
    async def test_cooldown_moving_to_another_conn_id_restarts_the_fire_task(self, manager):
        """The fire consumer holds the connection object it was handed for its whole
        life; left on the old conn_id it would leave rmq_watcher.fire without a consumer
        on the new one and cooldown DAGs would stop firing with nothing to show for it."""
        connection = _make_live_connection(channel=_make_live_channel())
        manager._conn("old_conn").connections[_ROLE_CONSUME] = connection
        manager._conn("new_conn").connections[_ROLE_CONSUME] = connection
        _register_fire(manager, "old_conn", real_task=True)
        first_task = manager._fire_task

        with patch.object(manager, "_consume_subscription", side_effect=_never_returns), \
             patch.object(manager, "_consume_fire_queue", side_effect=_never_returns), \
             patch.object(manager, "_provision_cooldown", new_callable=AsyncMock), \
             patch.object(manager, "_recover_dead_consumers", new_callable=AsyncMock), \
             patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock):
            await manager.reconcile([_sub(id=1, conn_id="new_conn", cooldown=300)])

        assert manager._fire_state.conn_id == "new_conn"
        assert manager._fire_task is not first_task
        assert first_task.cancelled() or first_task.done()
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_a_fire_task_on_the_same_conn_id_is_left_running(self, manager):
        connection = _make_live_connection(channel=_make_live_channel())
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection
        _register_fire(manager, "rmq_default", real_task=True)
        first_task = manager._fire_task

        with patch.object(manager, "_consume_subscription", side_effect=_never_returns), \
             patch.object(manager, "_provision_cooldown", new_callable=AsyncMock), \
             patch.object(manager, "_recover_dead_consumers", new_callable=AsyncMock), \
             patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock):
            await manager.reconcile([_sub(id=1, cooldown=300)])

        assert manager._fire_task is first_task
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_the_fire_conn_id_does_not_follow_the_row_order(self, manager):
        """The subscription list comes from a query with no ORDER BY, and PostgreSQL
        reorders rows an UPDATE touched — which every consumer-status transition does.
        Reading the fire consumer's conn_id off that order would cancel and restart it
        during exactly the reconnect turbulence cooldown exists to survive."""
        connection = _make_live_connection(channel=_make_live_channel())
        for conn_id in ("conn_a", "conn_b"):
            manager._conn(conn_id).connections[_ROLE_CONSUME] = connection
        subs = [
            _sub(id=1, dag_id="dag_a", conn_id="conn_b", cooldown=300),
            _sub(id=2, dag_id="dag_b", conn_id="conn_a", cooldown=300),
        ]

        with patch.object(manager, "_consume_subscription", side_effect=_never_returns), \
             patch.object(manager, "_consume_fire_queue", side_effect=_never_returns), \
             patch.object(manager, "_provision_cooldown", new_callable=AsyncMock), \
             patch.object(manager, "_recover_dead_consumers", new_callable=AsyncMock), \
             patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock):
            await manager.reconcile(subs)
            first_task = manager._fire_task
            assert manager._fire_state.conn_id == "conn_a"

            await manager.reconcile(list(reversed(subs)))

        assert manager._fire_state.conn_id == "conn_a"
        assert manager._fire_task is first_task, (
            "the order the rows came back in must not move the fire consumer"
        )
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_cooldown_split_over_two_conn_ids_is_named_out_loud(self, manager):
        """Pending queues are declared on the chosen conn_id alone, while a matched
        delivery is published to the pending queue of its own conn_id — on any other
        broker that queue is missing, the mandatory publish comes back unrouted and the
        DAG never fires. Full validation is deferred, so at least say it."""
        connection = _make_live_connection(channel=_make_live_channel())
        for conn_id in ("conn_a", "conn_b"):
            manager._conn(conn_id).connections[_ROLE_CONSUME] = connection
        subs = [
            _sub(id=1, dag_id="dag_a", conn_id="conn_a", cooldown=300),
            _sub(id=2, dag_id="dag_b", conn_id="conn_b", cooldown=300),
        ]

        with patch.object(manager, "_consume_subscription", side_effect=_never_returns), \
             patch.object(manager, "_consume_fire_queue", side_effect=_never_returns), \
             patch.object(manager, "_provision_cooldown", new_callable=AsyncMock), \
             patch.object(manager, "_recover_dead_consumers", new_callable=AsyncMock), \
             patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await manager.reconcile(subs)
            await manager.reconcile(subs)

        split = [
            c for c in mock_log.error.call_args_list
            if "span" in str(c.args[0])
        ]
        assert len(split) == 1, "logged when the configuration appears, not every cycle"
        assert split[0].args[2] == "conn_a, conn_b"
        assert split[0].args[3] == "conn_a", "the chosen conn_id is named"
        await _drain(manager)


class TestPerConnState:
    @pytest.mark.asyncio
    async def test_state_of_a_conn_id_that_left_the_list_is_forgotten(self, manager):
        """A conn_id that comes back later is a new connection: a leftover drop cycle
        would suppress its first legitimate recreation, and a leftover verdict would be
        written into its status row."""
        manager._conn("gone").timeouts = AmqpTimeouts(connect=1.0, rpc=1.0)
        manager._conn("gone").publish_timeouts = 1
        manager._conn("gone").last_drop_cycle = 3
        manager._conn("gone").liveness = _ConnLiveness(status="error", broker_consumer_count=0)
        manager._conn("gone").mgmt_failures = 2
        manager._conn("gone").stuck_cycles = 1
        manager._conn("kept").timeouts = AmqpTimeouts(connect=1.0, rpc=1.0)

        with patch.object(manager, "_recover_dead_consumers", new_callable=AsyncMock), \
             patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock), \
             patch.object(manager, "_consume_subscription", side_effect=_never_returns):
            await manager.reconcile([_sub(id=1, conn_id="kept")])

        assert "gone" not in manager._conns
        assert "kept" in manager._conns
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_a_conn_id_that_returns_may_be_recreated_at_once(self, manager):
        manager._conn("rmq_default").last_drop_cycle = 0
        manager._cycle_no = 1
        assert manager._may_drop_connection("rmq_default") is False

        with patch.object(manager, "_recover_dead_consumers", new_callable=AsyncMock), \
             patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock):
            await manager.reconcile([])

        assert manager._may_drop_connection("rmq_default") is True


class TestCycleCallsUseTheCyclePool:
    """Every connection the reconcile cycle builds reads its Airflow connection through
    the cycle pool. Borrowing a consumer worker there is what lets deliveries stalled on
    the metadata database starve the cycle that rebuilds connections and writes statuses.
    """

    @staticmethod
    def _refusing_get_connection(manager):
        return patch.object(
            manager, "_get_or_create_connection", new_callable=AsyncMock,
            side_effect=OSError("connection refused"),
        )

    @pytest.mark.asyncio
    async def test_provisioning_cooldown_uses_the_cycle_pool(self, manager):
        with self._refusing_get_connection(manager) as get_conn:
            await manager._provision_cooldown({"dag_a"}, "rmq_default")

        assert get_conn.await_args.kwargs["executor"] is manager._cycle_executor

    @pytest.mark.asyncio
    async def test_provisioning_exchange_subs_uses_the_cycle_pool(self, manager):
        manager._http_client = AsyncMock()
        with self._refusing_get_connection(manager) as get_conn:
            await manager._provision_exchange_subs([_exchange_sub(id=1)])

        assert get_conn.await_args.kwargs["executor"] is manager._cycle_executor

    @pytest.mark.asyncio
    async def test_the_liveness_probe_uses_the_cycle_pool(self, manager):
        with self._refusing_get_connection(manager) as get_conn:
            answers, reason = await manager._probe_connection("rmq_default", {"orders"})

        assert answers is False and reason
        assert get_conn.await_args.kwargs["executor"] is manager._cycle_executor

    @pytest.mark.asyncio
    async def test_restarting_the_fire_consumer_uses_the_cycle_pool(self, manager):
        _register_fire(manager, real_task=True)
        with patch.object(manager, "_check_liveness", new_callable=AsyncMock,
                          return_value=(set(), {"rmq_default"})), \
             self._refusing_get_connection(manager) as get_conn:
            await manager._recover_dead_consumers([])

        assert get_conn.await_args.kwargs["executor"] is manager._cycle_executor
        await _drain(manager)


class TestCancelledTasksDoNotStallTheCycle:
    @pytest.mark.asyncio
    async def test_a_task_that_ignores_its_cancel_is_left_behind(self, manager):
        """A task free to catch its own CancelledError must not hold the cycle that
        cancelled it — the cycle has a budget of its own to keep."""
        real_sleep = asyncio.sleep

        async def stubborn():
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                await real_sleep(0.3)   # keeps going well past its own cancellation

        task = asyncio.create_task(stubborn())
        await real_sleep(0)
        task.cancel()

        with patch("airflow_provider_rmq.watcher.consumer._CANCEL_TIMEOUT", 0.05), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            await asyncio.wait_for(_wait_cancelled([task]), timeout=2.0)

        assert not task.done(), "the task outlived the wait, which is the point"
        assert any(
            "still running" in str(c.args[0]) for c in mock_log.warning.call_args_list
        ), mock_log.warning.call_args_list
        await task

    @pytest.mark.asyncio
    async def test_a_cancelled_task_that_finishes_is_awaited(self, manager):
        task = asyncio.create_task(_never_returns())
        await asyncio.sleep(0)
        task.cancel()

        await asyncio.wait_for(_wait_cancelled([task]), timeout=2.0)

        assert task.done()


class TestConnStatusRows:
    @pytest.mark.asyncio
    async def test_a_conn_id_with_no_verdict_and_no_row_starts_unknown(self, manager):
        """A brand-new conn_id must not read as healthy: the number of tasks the
        watcher started says nothing about what the broker holds."""
        sub = _sub(id=7)
        _register_active(manager, sub)

        upsert = await _write_statuses(manager, [sub], stored={})

        assert upsert.call_args.args[2] == "unknown"
        assert upsert.call_args.kwargs["consumer_count"] == 1

    @pytest.mark.asyncio
    async def test_the_stored_statuses_are_read_only_when_a_row_needs_them(self, manager):
        """A full-table read on every cycle buys nothing when every row has a verdict."""
        sub = _sub(id=7)
        _register_active(manager, sub)
        manager._conn("rmq_default").liveness = _ConnLiveness(
            status="connected", broker_consumer_count=1
        )
        read = MagicMock(return_value=[])

        with ExitStack() as stack:
            stack.enter_context(_patch_watcher_session())
            stack.enter_context(
                patch("airflow_provider_rmq.watcher.consumer.get_conn_statuses", read)
            )
            stack.enter_context(
                patch("airflow_provider_rmq.watcher.consumer.upsert_conn_status", MagicMock())
            )
            await manager._update_all_conn_counts([sub])

        read.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_fire_task_counts_on_the_watcher_side_too(self, manager):
        """The fire consumer is one of our consumers on the broker, so counting it on
        only one side leaves every cooldown conn_id permanently one apart."""
        sub = _sub(id=7, cooldown=300)
        _register_active(manager, sub, real_task=True)
        _register_fire(manager, "rmq_default", real_task=True)
        manager._conn("rmq_default").liveness = _ConnLiveness(
            status="connected", broker_consumer_count=2
        )

        upsert = await _write_statuses(manager, [sub])

        assert upsert.call_args.kwargs["consumer_count"] == 2
        assert upsert.call_args.kwargs["broker_consumer_count"] == 2
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_negative_verdict_is_not_written_as_connected(self, manager):
        sub = _sub(id=7)
        _register_active(manager, sub)
        manager._conn("rmq_default").liveness = _ConnLiveness(
            status="error", broker_consumer_count=0, reason="consumer not registered",
        )

        upsert = await _write_statuses(manager, [sub], stored={"rmq_default": "connected"})

        assert upsert.call_args.args[2] == "error"
        assert upsert.call_args.kwargs["broker_consumer_count"] == 0
        assert upsert.call_args.kwargs["last_error"] == "consumer not registered"

    @pytest.mark.asyncio
    async def test_no_data_keeps_the_stored_status_but_still_stamps_the_cycle(self, manager):
        sub = _sub(id=7)
        _register_active(manager, sub)
        manager._conn("rmq_default").liveness = _ConnLiveness(
            status=None, broker_consumer_count=None, reason="management API unreachable",
        )

        upsert = await _write_statuses(manager, [sub], stored={"rmq_default": "connected"})

        assert upsert.call_args.args[2] == "connected"
        assert upsert.call_args.kwargs["broker_consumer_count"] is None
        assert upsert.call_args.kwargs["last_reconcile_at"] is not None

    @pytest.mark.asyncio
    async def test_no_data_keeps_the_text_that_explains_the_stored_status(self, manager):
        """The row keeps a status the cycle could not verify, so it must keep the
        reason with it: an 'error' row whose Last Error is blank tells the operator
        less than the cycle before it did, and the two are written together."""
        sub = _sub(id=7)
        _register_active(manager, sub)
        manager._conn("rmq_default").liveness = _ConnLiveness(
            status=None, broker_consumer_count=None, reason=None,
        )

        upsert = await _write_statuses(manager, [sub], stored={"rmq_default": "error"})

        assert upsert.call_args.args[2] == "error"
        assert upsert.call_args.kwargs["last_error"] is KEEP

    @pytest.mark.asyncio
    async def test_a_positive_verdict_clears_the_text_of_the_one_before_it(self, manager):
        """``connected`` carries no reason, and leaving the previous one in place would
        keep an old failure on a green row for the life of the process."""
        sub = _sub(id=7)
        _register_active(manager, sub)
        manager._conn("rmq_default").liveness = _ConnLiveness(
            status="connected", broker_consumer_count=1,
        )

        upsert = await _write_statuses(manager, [sub], stored={"rmq_default": "error"})

        assert upsert.call_args.args[2] == "connected"
        assert upsert.call_args.kwargs["last_error"] is None

    @pytest.mark.asyncio
    async def test_conn_id_without_a_single_live_task_is_still_written(self, manager):
        """Every conn_id of the subscription list gets a row on every cycle. A row
        that simply stops being updated is indistinguishable from a healthy one."""
        sub = _sub(id=7)

        upsert = await _write_statuses(manager, [sub], stored={"rmq_default": "connected"})

        assert upsert.call_count == 1
        assert upsert.call_args.args[1] == "rmq_default"
        assert upsert.call_args.kwargs["consumer_count"] == 0
        assert upsert.call_args.kwargs["last_reconcile_at"] is not None

    @pytest.mark.asyncio
    async def test_confirmed_liveness_is_written_with_the_broker_count(self, manager):
        sub = _sub(id=7)
        _register_active(manager, sub)
        manager._conn("rmq_default").liveness = _ConnLiveness(status="connected", broker_consumer_count=2)

        upsert = await _write_statuses(manager, [sub])

        assert upsert.call_args.args[2] == "connected"
        assert upsert.call_args.kwargs["consumer_count"] == 1
        assert upsert.call_args.kwargs["broker_consumer_count"] == 2

    @pytest.mark.asyncio
    async def test_status_write_failure_is_logged_and_swallowed(self, manager):
        sub = _sub(id=7)
        upsert = MagicMock(side_effect=RuntimeError("db is gone"))

        with ExitStack() as stack:
            for patcher in _patch_status_writer(upsert):
                stack.enter_context(patcher)
            with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
                await manager._update_all_conn_counts([sub])

        assert mock_log.warning.called


class TestConnStatusWriteOrder:
    """Two writes of ``rmq_watcher_conn_status`` at once, and which of them the rows keep.

    The caller stops waiting after ``_DB_TIMEOUT`` and the worker carries on, so a write
    a slow database is still holding meets the write of the cycle behind it. Committed
    in that order, a snapshot taken before an outage lands on top of the one that
    reported it and stays there — the page reads green, and the manager counts the newer
    value as stored. The write that arrives second is therefore dropped rather than
    queued behind the first: waiting for it would cost a worker per cycle.
    """

    @staticmethod
    def _row(status: str) -> tuple:
        return ("rmq_default", 1, status, None, 1)

    @staticmethod
    def _now() -> datetime:
        """The naive UTC stamp the cycle takes, which the view compares its own with."""
        return datetime.now(timezone.utc).replace(tzinfo=None)

    def test_one_write_of_the_rows_runs_at_a_time(self):
        upsert = MagicMock()

        with ExitStack() as stack:
            for patcher in _patch_status_writer(upsert):
                stack.enter_context(patcher)
            with _conn_status_in_flight():
                _write_conn_status_rows([self._row("connected")], self._now())

        assert upsert.call_args_list == []

    def test_a_write_the_rows_are_free_for_goes_through(self):
        """The drop is about the write in the database and nothing else: the gate is
        given back the moment one write returns."""
        upsert = MagicMock()

        with ExitStack() as stack:
            for patcher in _patch_status_writer(upsert):
                stack.enter_context(patcher)
            _write_conn_status_rows([self._row("error")], self._now())
            _write_conn_status_rows([self._row("connected")], self._now())

        assert [c.args[2] for c in upsert.call_args_list] == ["error", "connected"]

    def test_a_write_that_raises_gives_the_rows_back(self):
        """A database that answers with an error is not a write still running, and the
        cycle behind it must not be dropped for the life of the process."""
        upsert = MagicMock(side_effect=[RuntimeError("db is gone"), None])

        with ExitStack() as stack:
            for patcher in _patch_status_writer(upsert):
                stack.enter_context(patcher)
            with pytest.raises(RuntimeError):
                _write_conn_status_rows([self._row("error")], self._now())
            _write_conn_status_rows([self._row("connected")], self._now())

        assert [c.args[2] for c in upsert.call_args_list] == ["error", "connected"]

    @pytest.mark.asyncio
    async def test_a_stuck_write_leaves_the_cycle_pool_free(self, manager):
        """The starvation this drop exists for: a database holding one write past
        ``_DB_TIMEOUT`` keeps its worker, and cycles waiting their turn behind it would
        take one worker each until the pool that reads metadata and probes the broker
        has none left — with recovery stopping exactly when it is needed."""
        sub = _sub(id=7)
        _register_active(manager, sub)
        upsert = MagicMock()
        pool = manager._cycle_executor

        with ExitStack() as stack:
            for patcher in _patch_status_writer(upsert):
                stack.enter_context(patcher)
            stack.enter_context(patch(f"{_CONSUMER_MODULE}._DB_TIMEOUT", 0.2))
            with _conn_status_in_flight():
                for _ in range(pool.max_workers + 1):
                    await manager._update_all_conn_counts([sub])
                probe = await asyncio.wait_for(pool.run(lambda: "free"), timeout=2)

        assert probe == "free", "the cycle pool has no worker left to read metadata with"
        assert upsert.call_args_list == []
        assert pool.in_flight == 0

    @pytest.mark.asyncio
    async def test_a_failed_connect_takes_the_same_turn_as_the_cycle(self, manager):
        """Both writers reach the same rows, so an ``error`` left running by a failed
        connect and the snapshot of the cycle behind it would land in whatever order
        their transactions finish in."""
        written = []

        async def handler(fn, *args):
            return fn(*args)

        manager._executor = _FakeExecutor(handler)
        upsert = MagicMock(side_effect=lambda session, conn_id, status, **kwargs: written.append(status))
        with _patch_watcher_session(), \
             patch(f"{_CONSUMER_MODULE}.upsert_conn_status", upsert), \
             _conn_status_in_flight():
            await manager._store_conn_error(
                "rmq_default", ConnectionError("refused"), manager._executor
            )

        assert written == []


class TestFailedConnectionAttempt:
    """A connect that never succeeds is the only thing that can report itself."""

    def test_write_conn_error_stores_the_failure_against_the_conn_id(self):
        upsert = MagicMock()
        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.upsert_conn_status", upsert):
            _write_conn_error("rmq_default", "connection refused")

        assert upsert.call_args.args[1:] == ("rmq_default", "error")
        assert upsert.call_args.kwargs["consumer_count"] == 0
        assert upsert.call_args.kwargs["last_error"] == "connection refused"

    @pytest.mark.asyncio
    async def test_a_failed_connect_is_stored_and_still_raised(self, manager):
        written = []

        async def handler(fn, *args):
            written.append((fn, args))
            return fn(*args) if fn is not _write_conn_error else None

        manager._executor = _FakeExecutor(handler)
        with patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()), \
             patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   side_effect=ConnectionError("refused")), \
             pytest.raises(ConnectionError):
            await manager._get_or_create_connection("rmq_default")

        assert any(fn is _write_conn_error for fn, _ in written), written

    @pytest.mark.asyncio
    async def test_a_database_that_cannot_store_the_failure_is_logged_not_raised(
        self, manager
    ):
        """Two faults at once — broker down and database down — must still surface the
        broker one to the caller."""
        async def handler(fn, *args):
            if fn is _write_conn_error:
                raise RuntimeError("metadata db is gone")
            return fn(*args)

        manager._executor = _FakeExecutor(handler)
        with patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()), \
             patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   side_effect=ConnectionError("refused")), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log, \
             pytest.raises(ConnectionError):
            await manager._get_or_create_connection("rmq_default")

        assert any(
            "cannot store the failed connection attempt" in str(c.args[0]).lower()
            for c in mock_log.warning.call_args_list
        ), mock_log.warning.call_args_list


class TestCycleWritesOffTheLoop:
    """Reconcile's own database writes belong in the cycle pool.

    They are awaited by the reconcile cycle, not by a consumer task, so sharing the
    consumer pool would let deliveries stuck on the database starve the very cycle
    that is meant to notice and recover from that.
    """

    @pytest.mark.asyncio
    async def test_conn_status_rows_leave_the_loop_thread(self, manager):
        sub = _sub(id=7)
        _register_active(manager, sub)
        threads = []
        upsert = MagicMock(
            side_effect=lambda *a, **k: threads.append(threading.current_thread())
        )
        pool = BoundedExecutor("test-cycle", 2)
        manager._cycle_executor = pool

        try:
            with ExitStack() as stack:
                for patcher in _patch_status_writer(upsert):
                    stack.enter_context(patcher)
                await manager._update_all_conn_counts([sub])
        finally:
            pool.shutdown()

        assert threads and threads[0] is not threading.current_thread()

    @pytest.mark.asyncio
    async def test_reconcile_writes_go_to_the_cycle_pool_not_the_consumer_one(self, manager):
        sub = _sub(id=7)
        _register_active(manager, sub, real_task=True)
        cycle_calls = []
        consumer_calls = []

        async def cycle_handler(fn, *args):
            cycle_calls.append(fn)
            return True

        async def consumer_handler(fn, *args):
            consumer_calls.append(fn)
            return True

        manager._cycle_executor = _FakeExecutor(cycle_handler)
        manager._executor = _FakeExecutor(consumer_handler)

        await manager.reconcile([])   # the subscription is gone — mark it disconnected
        writer = _status_writer(7)
        assert any(getattr(fn, "__self__", None) is writer for fn in cycle_calls), cycle_calls
        assert writer._pending == ("disconnected", None)

        # A cycle with subscriptions on it writes the status row of every conn_id, and
        # that write belongs in the cycle pool as well.
        cycle_calls.clear()
        still_there = _sub(id=8)
        _register_active(manager, still_there, real_task=True)
        with patch.object(manager, "_recover_dead_consumers", new_callable=AsyncMock):
            await manager.reconcile([still_there])

        assert any(fn.__name__ == "_write_conn_status_rows" for fn in cycle_calls), cycle_calls
        assert consumer_calls == [], "the cycle must never borrow a consumer worker"
        await _drain(manager)

    @pytest.mark.asyncio
    async def test_a_hung_status_write_does_not_stall_reconcile(self, manager):
        release = threading.Event()
        pool = BoundedExecutor("test-cycle-hang", 1)
        manager._cycle_executor = pool

        def hang(*args, **kwargs):
            release.wait(5)

        try:
            with ExitStack() as stack:
                for patcher in _patch_status_writer(MagicMock(side_effect=hang)):
                    stack.enter_context(patcher)
                with patch("airflow_provider_rmq.watcher.consumer._DB_TIMEOUT", 0.05), \
                     patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
                    await manager._update_all_conn_counts([_sub(id=7)])
        finally:
            release.set()
            pool.shutdown()

        assert mock_log.warning.called


# ---------------------------------------------------------------------------
# Tests for at-least-once handling of immediate-mode deliveries
# ---------------------------------------------------------------------------

async def _consume_until(manager, sub, messages, done, timeout: float = 2.0):
    """Run ``_consume_subscription`` over ``messages`` until ``done`` is set."""
    connection = _make_live_connection(queue=_make_push_queue(messages))
    with patch.object(manager, "_get_or_create_connection", return_value=connection), \
         patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
        task = asyncio.create_task(manager._consume_subscription(sub))
        try:
            await asyncio.wait_for(done.wait(), timeout=timeout)
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


class TestHungTrigger:
    @pytest.mark.asyncio
    async def test_a_trigger_stuck_in_the_pool_becomes_an_ordinary_failure(self, manager):
        """A consumer task awaits its own trigger, so the cycle watchdog never sees it.

        Without a timeout of its own the subscription would sit in ``listening`` while
        consuming nothing at all. The timeout buys the coroutine back, not the worker:
        the thread stays busy until the blocked call returns, which the test asserts
        rather than pretends otherwise.
        """
        release = threading.Event()
        done = asyncio.Event()
        delays = []
        statuses = []
        msg = _make_fake_message(b"order", message_id="m1")
        msg.nack = AsyncMock()
        # Two workers: one is swallowed by the stuck trigger, the other still carries
        # the status writes the subscription makes while reacting to it.
        pool = BoundedExecutor("test-trigger-hang", 2)
        manager._executor = pool

        def hang(dag_id, conf, run_id):
            release.wait(5)
            return _OUTCOME_TRIGGERED

        def record(delay):
            delays.append(delay)
            done.set()

        connection = _make_live_connection(queue=_make_push_queue([msg]))
        try:
            with patch.object(manager, "_get_or_create_connection", return_value=connection), \
                 patch("airflow_provider_rmq.watcher.consumer._sync_trigger", hang), \
                 patch("airflow_provider_rmq.watcher.consumer._TRIGGER_TIMEOUT", 0.05), \
                 _patch_watcher_session(), \
                 patch(
                     "airflow_provider_rmq.watcher.consumer.set_consumer_status",
                     lambda session, sub_id, status, last_error=None: (
                         statuses.append(status)
                     ),
                 ), \
                 _record_consumer_sleeps(record):
                task = asyncio.create_task(manager._consume_subscription(_sub()))
                try:
                    await asyncio.wait_for(done.wait(), timeout=3.0)
                    assert pool.in_flight >= 1   # the worker is still held by the call
                finally:
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
        finally:
            release.set()
            pool.shutdown()

        msg.nack.assert_awaited_with(requeue=True)
        msg.ack.assert_not_awaited()
        assert delays[0] == _TRIGGER_BACKOFF_START
        assert statuses[-1] == "error"


class TestImmediateDeliveryAcknowledgement:
    @pytest.mark.asyncio
    async def test_ack_comes_after_the_trigger(self, manager):
        """The delivery is acknowledged only once the DAG run exists."""
        order = []
        done = asyncio.Event()
        msg = _make_fake_message(b"order", message_id="m1")

        async def ack():
            order.append("ack")
            done.set()

        msg.ack = ack

        async def handler(fn, *args):
            order.append("trigger")
            return _OUTCOME_TRIGGERED

        manager._executor = _FakeExecutor(handler)
        await _consume_until(manager, _sub(), [msg], done)

        assert order == ["trigger", "ack"]
        msg.nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_successful_trigger_reports_the_metric(self, manager):
        done = asyncio.Event()
        msg = _make_fake_message(b"order", message_id="m1")
        msg.ack = AsyncMock(side_effect=lambda: done.set())

        async def handler(fn, *args):
            return _OUTCOME_TRIGGERED

        manager._executor = _FakeExecutor(handler)
        with patch("airflow_provider_rmq.watcher.consumer.incr") as incr:
            await _consume_until(manager, _sub(), [msg], done)

        assert call("rmq_watcher.dag_triggered") in incr.call_args_list

    @pytest.mark.asyncio
    async def test_failed_trigger_requeues_without_acking(self, manager):
        done = asyncio.Event()
        msg = _make_fake_message(b"order", message_id="m1")

        async def nack(requeue=False):
            done.set()

        msg.nack = AsyncMock(side_effect=nack)

        async def handler(fn, *args):
            raise RuntimeError("airflow metadata db is down")

        manager._executor = _FakeExecutor(handler)
        with patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep",
                   new_callable=AsyncMock):
            await _consume_until(manager, _sub(), [msg], done)

        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_repeated_failures_grow_the_pause_up_to_the_cap(self, manager):
        """A steady trigger failure must not turn into a hot redelivery loop: the
        default delivery-limit of a quorum queue is 20, which 0.1 s pauses burn in
        seconds."""
        delays = []
        done = asyncio.Event()
        messages = [_make_fake_message(b"x", message_id=f"m{i}") for i in range(8)]

        def record(delay):
            delays.append(delay)
            if len(delays) >= 8:
                done.set()

        async def handler(fn, *args):
            raise RuntimeError("boom")

        manager._executor = _FakeExecutor(handler)
        with _record_consumer_sleeps(record):
            await _consume_until(manager, _sub(), messages, done)

        assert delays[:8] == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0]
        assert delays[0] == _TRIGGER_BACKOFF_START
        assert delays[-1] == _TRIGGER_BACKOFF_MAX

    @pytest.mark.asyncio
    async def test_backoff_resets_after_a_success(self, manager):
        delays = []
        done = asyncio.Event()
        messages = [_make_fake_message(b"x", message_id=f"m{i}") for i in range(4)]
        outcomes = iter([RuntimeError("boom"), RuntimeError("boom"), None, RuntimeError("boom")])

        def record(delay):
            delays.append(delay)
            if len(delays) >= 3:
                done.set()

        async def handler(fn, *args):
            outcome = next(outcomes)
            if isinstance(outcome, Exception):
                raise outcome
            return _OUTCOME_TRIGGERED

        manager._executor = _FakeExecutor(handler)
        with _record_consumer_sleeps(record):
            await _consume_until(manager, _sub(), messages, done)

        assert delays[:3] == [1.0, 2.0, 1.0]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("outcome", [_OUTCOME_DUPLICATE, _OUTCOME_SKIPPED])
    async def test_duplicate_and_skipped_deliveries_are_acked(self, manager, outcome):
        """A redelivery of a handled message and a paused DAG both end the delivery:
        requeueing either one would build an accumulator of redeliveries."""
        done = asyncio.Event()
        msg = _make_fake_message(b"order", message_id="m1")
        msg.ack = AsyncMock(side_effect=lambda: done.set())

        async def handler(fn, *args):
            return outcome

        manager._executor = _FakeExecutor(handler)
        with patch("airflow_provider_rmq.watcher.consumer.incr") as incr:
            await _consume_until(manager, _sub(), [msg], done)

        msg.ack.assert_awaited_once()
        msg.nack.assert_not_awaited()
        assert call("rmq_watcher.dag_triggered") not in incr.call_args_list

    @pytest.mark.asyncio
    async def test_a_series_of_filter_misses_keeps_consumption_going(self, manager):
        """Every miss is requeued, and the delivery behind them is still processed —
        a single missing NACK branch would stall the subscription for good."""
        misses = [
            _make_fake_message(b"noise", headers={"type": "other"}, message_id=f"n{i}")
            for i in range(25)
        ]
        hit = _make_fake_message(b"order", headers={"type": "order"}, message_id="m1")
        done = asyncio.Event()
        hit.ack = AsyncMock(side_effect=lambda: done.set())

        async def handler(fn, *args):
            return _OUTCOME_TRIGGERED

        manager._executor = _FakeExecutor(handler)
        with patch("airflow_provider_rmq.watcher.consumer.asyncio.sleep",
                   new_callable=AsyncMock):
            await _consume_until(
                manager,
                _sub(filter_data={"filter_headers": {"type": "order"}}),
                [*misses, hit],
                done,
            )

        for miss in misses:
            miss.nack.assert_awaited_once_with(requeue=True)
            miss.ack.assert_not_awaited()
        hit.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_run_id_carries_the_message_id(self, manager):
        captured = {}
        done = asyncio.Event()
        msg = _make_fake_message(b"order", message_id="msg-42")
        msg.ack = AsyncMock(side_effect=lambda: done.set())

        async def handler(fn, *args):
            captured["run_id"] = args[2]
            return _OUTCOME_TRIGGERED

        manager._executor = _FakeExecutor(handler)
        await _consume_until(manager, _sub(queue_name="orders"), [msg], done)

        assert captured["run_id"] == "rmq__orders__msg-42"


class TestADagWithoutASerializedVersion:
    """A trigger answering ``DagNotFound`` says the DAG processor is still warming up.

    The listener starts with the scheduler, minutes before ``serialized_dag`` is
    filled, and for that stretch the connection, the channel and the registration are
    all in place: the delivery waits for the rest of Airflow. The row keeps saying
    ``listening`` until the wait outlasts the bound the handler holds it to.
    """

    @staticmethod
    def _state(manager) -> _ConsumerState:
        """State whose row writes are recorded instead of reaching a database."""
        state = _ConsumerState(1, manager._executor)
        state.write = AsyncMock()
        return state

    @staticmethod
    async def _deliver(manager, state, backoff, error, count: int = 1) -> list:
        """Hand the handler ``count`` deliveries whose trigger raises ``error``."""
        async def handler(fn, *args):
            if error is None:
                return _OUTCOME_TRIGGERED
            raise error

        manager._executor = _FakeExecutor(handler)
        messages = [_make_fake_message(b"order", message_id=f"m{i}") for i in range(count)]
        for message in messages:
            await manager._handle_immediate_delivery(_sub(), message, state, backoff)
        return messages

    @staticmethod
    def _statuses(write) -> list:
        return [c.args[0] for c in write.await_args_list]

    @pytest.mark.asyncio
    async def test_the_delivery_goes_back_and_the_row_reads_listening(self, manager):
        """The row says what the consumer is doing: holding its registration and taking
        every delivery offered. Nothing about the warmup is a fault of the subscription,
        and ``listening`` is also what keeps it a candidate of the liveness check."""
        state = self._state(manager)
        with patch(f"{_CONSUMER_MODULE}.log") as log, \
             patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new_callable=AsyncMock):
            messages = await self._deliver(
                manager,
                state,
                _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX),
                _DagNotReady("Dag id test_dag not found"),
            )

        messages[0].nack.assert_awaited_once_with(requeue=True)
        messages[0].ack.assert_not_awaited()
        assert self._statuses(state.write) == [_SUB_LISTENING]
        assert state.write.await_args.kwargs["last_error"] is None
        [(level, text)] = _warmup_lines(log)
        assert level == logging.INFO
        assert "not serialized yet" in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize("count", [10, 20])
    async def test_every_tenth_delivery_in_a_row_is_a_warning(self, manager, count):
        """A quorum queue can spend the delivery-limit of the message before the
        streak reaches its bound, and the log line is then the only trace left — which
        is why it says which DAG, how long it has been waiting and that the delivery
        went back, and why it says it at a level a filtered log keeps."""
        state = self._state(manager)
        with patch(f"{_CONSUMER_MODULE}.log") as log, \
             patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new_callable=AsyncMock):
            await self._deliver(
                manager,
                state,
                _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX),
                _DagNotReady("Dag id test_dag not found"),
                count=count,
            )

        lines = _warmup_lines(log)
        assert [
            n for n, (level, _) in enumerate(lines, start=1)
            if level == logging.WARNING
        ] == list(range(10, count + 1, 10))
        text = lines[9][1]
        assert "test_dag" in text
        assert "not serialized yet" in text
        assert "10 deliveries in a row" in text
        assert "back on the queue" in text
        assert state.not_ready_streak == count
        assert set(self._statuses(state.write)) == {_SUB_LISTENING}

    @pytest.mark.asyncio
    async def test_a_warmup_longer_than_the_bound_reaches_the_row(self, manager):
        state = self._state(manager)
        with patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new_callable=AsyncMock):
            await self._deliver(
                manager,
                state,
                _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX),
                _DagNotReady("Dag id test_dag not found"),
                count=_NOT_READY_LIMIT - 1,
            )
            assert _SUB_ERROR not in self._statuses(state.write)

            await self._deliver(
                manager,
                state,
                _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX),
                _DagNotReady("Dag id test_dag not found"),
            )

        assert state.write.await_args.args[0] == _SUB_ERROR
        last_error = state.write.await_args.kwargs["last_error"]
        # The row names the condition an operator can act on. Airflow's own "Dag id X
        # not found" is true of an unparsed DAG and of a deleted one alike, and a row
        # carrying it alone sends whoever reads it looking for a DAG that is there.
        assert "no serialized version" in last_error
        assert "DAG processor has not parsed it" in last_error
        assert "Dag id test_dag not found" in last_error

    @pytest.mark.asyncio
    async def test_a_delivery_that_went_through_starts_the_count_again(self, manager):
        state = self._state(manager)
        state.not_ready_streak = _NOT_READY_LIMIT
        backoff = _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX)
        with patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new_callable=AsyncMock):
            await self._deliver(manager, state, backoff, None)
            await self._deliver(
                manager, state, backoff, _DagNotReady("Dag id test_dag not found")
            )

        assert state.not_ready_streak == 1
        assert _SUB_ERROR not in self._statuses(state.write)

    @pytest.mark.asyncio
    async def test_any_other_trigger_failure_still_reaches_the_row_at_once(self, manager):
        """The silence belongs to the one exception that is not the subscription's
        fault: everything else is reported on the first delivery."""
        state = self._state(manager)
        with patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new_callable=AsyncMock):
            messages = await self._deliver(
                manager,
                state,
                _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX),
                RuntimeError("airflow metadata db is down"),
            )

        messages[0].nack.assert_awaited_once_with(requeue=True)
        assert state.write.await_args.args[0] == _SUB_ERROR
        assert state.not_ready_streak == 0

    @pytest.mark.asyncio
    async def test_a_reattach_starts_the_count_again(self, manager):
        """The count measures the deliveries of one attach.

        A state lives as long as the task and outlives every reconnect inside it, so a
        streak carried across a reattach would report the first delivery of the next
        warmup as the failure of the one before it.
        """
        first = [
            _make_fake_message(b"order", message_id=f"m{i}")
            for i in range(_NOT_READY_LIMIT - 1)
        ]
        last = _make_fake_message(b"order", message_id="last")
        nacked = asyncio.Event()
        last.nack = AsyncMock(side_effect=lambda requeue=False: nacked.set())

        queue = MagicMock()
        queue.iterator.side_effect = [
            _QueueIterCtx(first, ends=True),
            _QueueIterCtx([last]),
        ]
        connection = _make_live_connection(queue=queue)

        async def handler(fn, *args):
            raise _DagNotReady("Dag id test_dag not found")

        manager._executor = _FakeExecutor(handler)
        statuses = []

        async def record(self, status, last_error=None, executor=None):
            statuses.append(status)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(_ConsumerState, "write", record), \
             patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new_callable=AsyncMock):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            # The task reports into the state record the manager holds for it, the way
            # reconcile hands it one before the loop takes its first turn.
            entry = _ActiveSub(
                task=task, sub=_sub(), state=_ConsumerState(1, manager._executor)
            )
            manager._active[1] = entry
            try:
                await asyncio.wait_for(nacked.wait(), timeout=3.0)
            finally:
                task.cancel()
                with suppress(asyncio.CancelledError):
                    await task

        assert _SUB_ERROR not in statuses
        assert entry.state.not_ready_streak == 1

    @pytest.mark.asyncio
    async def test_a_failure_of_another_kind_starts_the_count_again(self, manager):
        """The bound counts deliveries answered that way one after another.

        A warmup broken in the middle by an unrelated failure — one trigger meeting a
        metadata database that is down — is a fresh warmup afterwards. Counting the two
        stretches together would report the subscription over a warmup shorter than the
        bound, which is the very thing the bound is chosen to sit above.
        """
        state = self._state(manager)
        backoff = _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX)
        warming_up = _DagNotReady("Dag id test_dag not found")
        with patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new_callable=AsyncMock):
            await self._deliver(manager, state, backoff, warming_up, count=5)
            assert state.not_ready_streak == 5

            await self._deliver(
                manager, state, backoff, RuntimeError("airflow metadata db is down")
            )
            assert state.not_ready_streak == 0

            await self._deliver(manager, state, backoff, warming_up)

        assert state.not_ready_streak == 1

    @pytest.mark.asyncio
    async def test_the_bound_outlasts_the_warmup_it_is_meant_to_sit_out(self, manager):
        """The number is the whole of the choice: it decides what the row can be trusted
        to say.

        A bound below the warmup of a healthy install reddens a subscription that is
        merely waiting for the DAG processor — the failure this silence exists to stop.
        Far above it the row keeps saying ``listening`` while a DAG that will never be
        serialized takes the deliveries. The pauses the handler waits out are what turns
        the count into time, and the observed warmup to sit out is 13.5 minutes.
        """
        assert _NOT_READY_LIMIT == 25
        state = self._state(manager)
        delays: list = []
        with _record_consumer_sleeps(delays.append):
            await self._deliver(
                manager,
                state,
                _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX),
                _DagNotReady("Dag id test_dag not found"),
                count=_NOT_READY_LIMIT,
            )

        assert state.write.await_args.args[0] == _SUB_ERROR
        silence = sum(delays[:-1])   # the pauses before the delivery that reports
        assert 15 * 60 < silence < 25 * 60, silence

    @pytest.mark.asyncio
    async def test_a_warmup_takes_the_subscription_back_into_the_check(self, manager):
        """An ``error`` left by an unrelated failure does not outlast the next delivery.

        A scheduler that has just come up is where both halves meet: one trigger that
        failed against a metadata database still under load leaves the row reading
        ``error``, and the deliveries after it go through the warmup. Carrying that
        ``error`` on would report a fault of a subscription that has none, and would
        leave it out of :func:`_still_attached` for the length of the warmup — nobody
        would ask the broker about a registration that had meanwhile died.
        """
        state = self._state(manager)
        state._status = _SUB_ERROR

        with patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new_callable=AsyncMock):
            await self._deliver(
                manager,
                state,
                _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX),
                _DagNotReady("Dag id test_dag not found"),
            )

        assert self._statuses(state.write) == [_SUB_LISTENING]

    @pytest.mark.asyncio
    async def test_the_conn_id_row_stays_green_through_the_warmup(self, manager):
        """The silence keeps a whole connection from being reported as stuck.

        A subscription outside ``listening`` is no candidate of the liveness check, and
        a conn_id left without candidates is judged on its own terms: after
        ``_STUCK_CYCLES_BEFORE_DROP`` cycles its row is coloured too. A warmup that
        reddened the subscription would therefore redden the row every subscription of
        that connection shares — the half of the fix an operator sees first.
        """
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)   # listening, as the warmup leaves it
        manager._http_client = _mgmt_client(
            [_consumer_entry(entry.state.consumer_tag, "orders")]
        )

        with _patch_mgmt_connection():
            for _ in range(_STUCK_CYCLES_BEFORE_DROP + 1):
                assert await manager._check_liveness([sub]) == (set(), set())

        assert manager._conn("rmq_default").liveness.status == "connected"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error, candidate",
        [
            (_DagNotReady("Dag id test_dag not found"), True),
            (RuntimeError("airflow metadata db is down"), False),
        ],
        ids=["warming-up", "trigger-failure"],
    )
    async def test_the_liveness_check_keeps_asking_about_a_warming_up_subscription(
        self, manager, error, candidate
    ):
        """A row that says ``listening`` is what keeps the subscription in the check.

        The check asks the broker for the tag of every candidate; a subscription it
        counts as stalled is asked about no more, and a whole connection whose
        subscriptions are all stalled is reported as stuck.
        """
        sub = _sub(id=1)
        message = _make_fake_message(b"order", message_id="m1")
        nacked = asyncio.Event()
        message.nack = AsyncMock(side_effect=lambda requeue=False: nacked.set())
        connection = _make_live_connection(queue=_make_push_queue([message]))

        def trigger(dag_id, conf, run_id):
            raise error

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch(f"{_CONSUMER_MODULE}._sync_trigger", trigger), \
             _record_consumer_sleeps(lambda delay: None), \
             _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_provision_cooldown"):
            await manager.reconcile([sub])
            entry = manager._active[1]
            try:
                await asyncio.wait_for(nacked.wait(), timeout=3.0)
                await _wait_for(
                    lambda: (entry.state.status == _SUB_ERROR) is not candidate
                )
                candidates, stalled, _ = manager._partition_candidates([sub])
            finally:
                entry.task.cancel()
                await asyncio.gather(entry.task, return_exceptions=True)

        assert [s["id"] for s, _ in candidates.get("rmq_default", [])] == (
            [1] if candidate else []
        )
        assert [s["id"] for s in stalled.get("rmq_default", [])] == (
            [] if candidate else [1]
        )


class TestReconnectDiagnostics:
    """A subscription attaching to the broker is the one routine event worth a log line.

    It is what tells a healthy consumer from one that stopped reconnecting: a watcher
    that writes nothing at all leaves the two looking exactly alike in the log.
    """

    @pytest.mark.asyncio
    async def test_subscription_attach_is_logged_and_counted(self, manager, caplog):
        connection = _make_live_connection(queue=_make_push_queue([]))

        with patch.object(
            manager, "_get_or_create_connection", return_value=connection
        ), patch(
            "airflow_provider_rmq.watcher.consumer._ConsumerState.write"
        ), patch(
            "airflow_provider_rmq.watcher.consumer.incr"
        ) as incr, caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.consumer"
        ):
            await _run_then_cancel(
                manager._consume_subscription(
                    _sub(queue_name="orders", conn_id="rmq_prod")
                ),
                timeout=0.1,
            )

        incr.assert_any_call("rmq_watcher.consumer_attached")
        messages = [record.getMessage() for record in caplog.records]
        assert any(
            "orders" in message and "rmq_prod" in message for message in messages
        ), messages

    @pytest.mark.asyncio
    async def test_every_reconnect_is_counted_again(self, manager):
        """The metric measures reconnects, so a second attach must show up as a second
        increment — a once-only log would hide a subscription flapping."""
        connection = _make_live_connection(queue=_ending_queue())
        attaches: list = []

        def count(metric, *args, **kwargs):
            if metric == "rmq_watcher.consumer_attached":
                attaches.append(metric)

        with patch.object(
            manager, "_get_or_create_connection", return_value=connection
        ), patch(
            "airflow_provider_rmq.watcher.consumer._ConsumerState.write"
        ), patch(
            "airflow_provider_rmq.watcher.consumer._RECONNECT_DELAY", 0.01
        ), patch(
            "airflow_provider_rmq.watcher.consumer.incr", side_effect=count
        ):
            task = asyncio.create_task(manager._consume_subscription(_sub()))
            deadline = time.monotonic() + 5
            while len(attaches) < 2:
                assert time.monotonic() < deadline, attaches
                await asyncio.sleep(0)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

        assert len(attaches) > 1

    @pytest.mark.asyncio
    async def test_fire_consumer_attach_is_logged_and_counted(self, manager, caplog):
        connection = _make_live_connection(queue=_make_push_queue([]))

        with patch(
            "airflow_provider_rmq.watcher.consumer._ConsumerState.write"
        ), patch(
            "airflow_provider_rmq.watcher.consumer.incr"
        ) as incr, caplog.at_level(
            logging.INFO, logger="airflow_provider_rmq.watcher.consumer"
        ):
            await _run_then_cancel(
                manager._consume_fire_queue(connection, "rmq_prod"), timeout=0.1
            )

        incr.assert_any_call("rmq_watcher.consumer_attached")
        messages = [record.getMessage() for record in caplog.records]
        assert any(
            _FIRE_QUEUE in message and "rmq_prod" in message for message in messages
        ), messages


# ---------------------------------------------------------------------------
# End-to-end recovery scenarios of the 2026-08-26 incident
# ---------------------------------------------------------------------------

class TestIncidentRecovery:
    """Whole-chain checks: detection, connection rebuild and consumption afterwards.

    The unit tests above pin down each layer on its own; these drive the scenarios of
    the 2026-08-26 incident — a connection that answers nothing while reporting itself
    open, a broker blocking publishers under an alarm — through reconcile end to end.
    """

    @pytest.mark.asyncio
    async def test_silent_connection_is_rebuilt_and_consumption_resumes(self, manager):
        """A connection that is attached, then silent, with ``is_closed`` still False.

        Two cycles condemn the subscription, the connection is recreated, and the
        delivery waiting on the queue is triggered and acked.
        """
        sub = _sub(id=7, queue_name="orders")

        zombie_channel = _make_live_channel(queue=_make_push_queue([]))
        zombie = _make_live_connection()
        opened: list[int] = []

        async def silent_channel():
            opened.append(1)
            if len(opened) == 1:
                return zombie_channel
            await asyncio.Future()  # every later call is swallowed

        zombie.channel = silent_channel
        zombie.close = _hanging_call

        acked = asyncio.Event()
        msg = _make_fake_message(b"order", message_id="m1")
        msg.ack = AsyncMock(side_effect=lambda: acked.set())
        fresh = _make_live_connection(channel=_make_live_channel(queue=_make_push_queue([msg])))

        manager._executor = _offloading_executor()
        manager._http_client = _mgmt_client([])  # the broker holds no consumer of ours

        with _patch_mgmt_connection(), _patch_watcher_session(), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch("airflow_provider_rmq.watcher.consumer._CLOSE_TIMEOUT", 0.05), \
             patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   side_effect=[zombie, fresh]) as connect:
            await manager.reconcile([sub])
            await _wait_for_status(manager._active[7], "listening")
            silent_task = manager._active[7].task

            await manager.reconcile([sub])  # one negative check changes nothing
            assert manager._active[7].task is silent_task
            assert not acked.is_set()

            await manager.reconcile([sub])  # second negative check → rebuild
            await asyncio.wait_for(acked.wait(), timeout=2.0)

            assert connect.call_count == 2
            assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is fresh
            assert zombie not in _pooled_connections(manager)
            assert manager._active[7].task is not silent_task
            assert silent_task.done()
            msg.ack.assert_awaited_once()

            await _drain(manager)

    @pytest.mark.asyncio
    async def test_unreachable_broker_turns_the_stored_status_to_error(self, manager):
        """A connection the broker does not answer is written as ``error``: a row that
        stays green is what lets an outage pass unnoticed."""
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = None
        _fast_timeouts(manager)
        channel = _make_live_channel()
        channel.declare_queue = AsyncMock(side_effect=ConnectionError("broker is gone"))
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = _make_live_connection(
            channel=channel
        )

        await manager._check_liveness([sub])
        await manager._check_liveness([sub])
        upsert = await _write_statuses(manager, [sub], stored={"rmq_default": "connected"})

        assert manager._conn("rmq_default").liveness.status == "error"
        assert upsert.call_args.args[2] == "error"

    @pytest.mark.asyncio
    async def test_zombie_publish_connection_heals_and_cooldown_fires_again(self, manager):
        """A publish connection carries no consumers, so the broker-side watchdog cannot
        see it: the publish itself is its probe, and the next delivery must go out on a
        connection built to replace it — no process restart involved."""
        _fast_timeouts(manager)
        zombie_channel = _make_live_channel()
        zombie_channel.default_exchange.publish = _hanging_call
        zombie_publish = _make_live_connection(channel=zombie_channel)
        consume_conn = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = consume_conn
        manager._conn("rmq_default").connections[_ROLE_PUBLISH] = zombie_publish
        manager._conn("rmq_default").publish_channel = zombie_channel

        for _ in range(2):
            with pytest.raises(asyncio.TimeoutError):
                await manager._publish_pending(
                    "rmq_default", "my_dag", 300, _make_fake_message(b"order")
                )

        assert _ROLE_PUBLISH not in manager._conn("rmq_default").connections
        assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is consume_conn
        consume_conn.close.assert_not_awaited()

        fresh_channel = _make_live_channel()
        fresh_publish = _make_live_connection(channel=fresh_channel)
        msg = _make_fake_message(b"order")

        with patch("airflow_provider_rmq.watcher.consumer._new_connection",
                   return_value=fresh_publish), \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()):
            await manager._publish_pending("rmq_default", "my_dag", 300, msg)

        assert manager._conn("rmq_default").connections[_ROLE_PUBLISH] is fresh_publish
        fresh_channel.default_exchange.publish.assert_awaited_once()
        msg.ack.assert_awaited_once()
        msg.nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_memory_alarm_stays_inside_the_publish_connection(self, manager):
        """Under a resource alarm the broker stops reading from publishing connections.

        With publish split off, deliveries and their acks keep flowing on the consuming
        connection and our consumer tag stays registered, so the watchdog leaves that
        connection alone while the alarm lasts.
        """
        _fast_timeouts(manager)
        blocked_channel = _make_live_channel()
        blocked_channel.default_exchange.publish = _hanging_call
        manager._conn("rmq_default").connections[_ROLE_PUBLISH] = _make_live_connection(
            channel=blocked_channel
        )
        manager._conn("rmq_default").publish_channel = blocked_channel

        acked = asyncio.Event()
        msg = _make_fake_message(b"order", message_id="m1")
        msg.ack = AsyncMock(side_effect=lambda: acked.set())
        consume_conn = _make_live_connection(
            channel=_make_live_channel(queue=_make_push_queue([msg]))
        )
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = consume_conn

        manager._executor = _offloading_executor()
        sub = _sub(id=7, queue_name="orders")
        entry = _ActiveSub(
            task=asyncio.create_task(manager._consume_subscription(sub)),
            sub=sub.copy(),
            state=_ConsumerState(7, manager._executor),
        )
        manager._active[7] = entry

        await asyncio.wait_for(acked.wait(), timeout=2.0)

        # A cooldown publish of the same conn_id is still stuck behind the alarm.
        with pytest.raises(asyncio.TimeoutError):
            await manager._publish_pending(
                "rmq_default", "my_dag", 300, _make_fake_message(b"order")
            )

        manager._http_client = _mgmt_client(
            [_consumer_entry(entry.state.consumer_tag, "orders")]
        )
        with _patch_mgmt_connection():
            await manager._check_liveness([sub])
            result = await manager._check_liveness([sub])

        assert result == (set(), set())
        assert manager._conn("rmq_default").liveness.status == "connected"
        assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is consume_conn
        consume_conn.close.assert_not_awaited()
        assert not entry.task.done()

        await _drain(manager)


class TestUnroutableCooldownPlaceholder:
    """A cooldown placeholder can reach no queue at all.

    ``rmq_watcher.pending.{dag_id}`` is created by provisioning; a broker restored from
    an older definition, or an operator deleting the queue, leaves the placeholder
    unroutable. The broker then returns the message and acknowledges the publish, and
    on a channel opened with aio_pika's defaults that pair resolves the publish
    successfully — the returned message *is* its result. Acknowledging the delivery
    behind it would drop the event with nothing to fire the DAG afterwards.
    """

    def _publish_connection(self, opened: dict):
        """Connection whose channel answers a mandatory publish the way aiormq does."""

        async def channel(on_return_raises: bool = False, **kwargs):
            opened["on_return_raises"] = on_return_raises
            frame = aiormq.spec.Basic.Return(
                reply_code=312,
                reply_text="NO_ROUTE",
                exchange="",
                routing_key="rmq_watcher.pending.my_dag",
            )
            returned = SimpleNamespace(delivery=frame, header=None, body=b"")

            async def publish(message, routing_key, **_):
                if on_return_raises:
                    raise aio_pika.exceptions.PublishError(returned, frame)
                return returned      # aiormq hands the returned message back as a result

            return SimpleNamespace(
                is_closed=False,
                close=AsyncMock(),
                default_exchange=SimpleNamespace(publish=publish),
            )

        connection = AsyncMock()
        connection.is_closed = False
        connection.channel = channel
        return connection

    @pytest.mark.asyncio
    async def test_a_returned_placeholder_does_not_end_the_delivery(self, manager):
        opened: dict = {}
        connection = self._publish_connection(opened)
        manager._conn("rmq_default").connections[_ROLE_PUBLISH] = connection
        msg = _make_fake_message(b"order")

        with pytest.raises(aio_pika.exceptions.PublishError):
            await manager._publish_pending("rmq_default", "my_dag", 300, msg)

        msg.ack.assert_not_awaited()
        msg.nack.assert_awaited_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_the_publish_channel_raises_on_a_returned_message(self, manager):
        opened: dict = {}
        connection = self._publish_connection(opened)
        manager._conn("rmq_default").connections[_ROLE_PUBLISH] = connection

        await manager._get_publish_channel("rmq_default")

        assert opened["on_return_raises"] is True


class TestStatusWriteOrder:
    """A status write the caller stopped waiting for still reaches the database.

    The row must end up holding the newest status all the same: a stale ``listening``
    landing after ``error`` is the incident's own symptom — a subscription reported
    green while it consumes nothing — and the manager, counting the newer value as
    stored, would never write again.
    """

    @pytest.mark.asyncio
    async def test_a_late_write_does_not_overwrite_a_newer_one(self):
        pool = BoundedExecutor("test-status-order", 4)
        state = _ConsumerState(sub_id=7, executor=pool)
        stored = []
        release = threading.Event()

        def write(session, sub_id, status, last_error=None):
            if status == "listening":
                release.wait(5)      # the write the caller has already timed out on
            stored.append(status)

        ctx, _ = _mock_session()
        try:
            with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write), \
                 patch("airflow_provider_rmq.watcher.consumer._DB_TIMEOUT", 0.2):
                await state.write("listening")     # times out, still running
                await state.write("error")         # newer, waits behind it
                release.set()
                deadline = time.monotonic() + 5
                while len(stored) < 2 and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
        finally:
            release.set()
            pool.shutdown()

        assert stored[-1] == "error", stored

    @pytest.mark.asyncio
    async def test_a_write_from_a_replaced_state_cannot_overtake_the_new_one(self):
        """Recovery replaces the state along with the task it tracks.

        The write the old state left in a worker is a write to the same row, so it must
        not land on top of what the state that replaced it has to say — a numbering that
        starts afresh with each state could not rule on it at all.
        """
        pool = BoundedExecutor("test-status-cross-state", 4)
        old = _ConsumerState(sub_id=7, executor=pool)
        new = _ConsumerState(sub_id=7, executor=pool)
        stored = []
        release = threading.Event()
        entered = threading.Event()

        def write(session, sub_id, status, last_error=None):
            if status == "listening":
                entered.set()
                release.wait(5)   # the write the caller has already timed out on
            stored.append(status)

        ctx, _ = _mock_session()
        try:
            with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write), \
                 patch("airflow_provider_rmq.watcher.consumer._DB_TIMEOUT", 0.2):
                await old.write("listening")
                assert entered.is_set()
                await new.write("error")
                release.set()
                deadline = time.monotonic() + 5
                while len(stored) < 2 and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
        finally:
            release.set()
            pool.shutdown()

        assert stored[-1] == "error", stored

    def test_a_status_a_newer_one_replaced_is_gone_even_if_the_newer_one_fails(self):
        """It is the newer status that retires the older one, not the newer write.

        A failed ``error`` write that left ``listening`` free to land afterwards would
        restore the false-green row the writer exists to prevent. The failed status is
        the one tried again, and the one it replaced is gone for good.
        """
        writer = _StatusWriter(7)
        stored = []
        attempts = []

        def write(session, sub_id, status, last_error=None):
            attempts.append(status)
            if len(attempts) == 1:
                raise RuntimeError("database write failed")
            stored.append(status)

        writer.record_if_needed("listening", None)   # the write the caller timed out on
        writer.record_if_needed("error", "gone")     # the status that replaced it

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            with pytest.raises(RuntimeError):
                writer.store()
            writer.store()

        assert attempts == ["error", "error"]
        assert stored == ["error"]

    @pytest.mark.asyncio
    async def test_the_cycle_carries_a_dropped_status_into_the_row(self, manager):
        """Nothing else would: the consumer that wrote the status has moved on, and one
        whose queue is quiet makes no further write to carry it."""
        entry = _register_active(manager, _sub(id=7))
        _status_writer(7).record_if_needed("listening", None)
        stored = []

        def write(session, sub_id, status, last_error=None):
            stored.append(status)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await manager._store_unwritten_statuses()

        assert stored == ["listening"]
        assert not _status_writer(7).has_pending
        assert entry.state.status == "listening"

    def test_a_status_that_did_not_reach_the_row_is_stored_by_the_next_call(self):
        """A subscription that reached its steady state and went quiet writes nothing
        further, so a status dropped on a database outage would leave the row saying
        what it said while the database was away — for as long as the task runs."""
        writer = _StatusWriter(7)
        stored = []
        attempts = []

        def write(session, sub_id, status, last_error=None):
            attempts.append(status)
            if len(attempts) == 1:
                raise RuntimeError("database is away")
            stored.append(status)

        writer.record_if_needed("listening", None)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            with pytest.raises(RuntimeError):
                writer.store()
            assert writer.has_pending, "the status the outage dropped is gone"
            writer.store()

        assert stored == ["listening"]
        assert writer.stored == ("listening", None)
        assert not writer.has_pending

    @pytest.mark.asyncio
    async def test_a_status_left_unwritten_is_not_skipped_by_a_later_matching_one(self):
        """The guard skips a write the row already carries. A status left behind by a
        write that failed is the row's only way back to the truth, so a later status
        equal to what the row says must not return early and strand it — the reconcile
        cycle would then commit the stale one over a subscription that recovered."""
        state = _ConsumerState(7, _test_pool("test-stale-pending"))
        stored = []
        fail_on_error = True

        def write(session, sub_id, status, last_error=None):
            if status == "error" and fail_on_error:
                raise RuntimeError("metadata database is away")
            stored.append((status, last_error))

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await state.write("listening", last_error=None)
            await state.write("error", last_error="trigger failed")   # dropped by the outage
            fail_on_error = False
            await state.write("listening", last_error=None)           # equals the stored one

        assert stored == [("listening", None), ("listening", None)]
        assert not _status_writer(7).has_pending, "the stale error is still waiting"

    @pytest.mark.asyncio
    async def test_a_new_reason_under_an_unchanged_status_reaches_the_row(self):
        """The row carries the status and the reason. A second failure of the same kind
        changes only the text, and an operator reading a cause that has already been
        dealt with looks in the wrong place."""
        state = _ConsumerState(7, _test_pool("test-error-text"))
        stored = []

        def write(session, sub_id, status, last_error=None):
            stored.append((status, last_error))

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await state.write("error", last_error="metadata DB refused")
            await state.write("error", last_error="DAG run limit reached")

        assert stored == [
            ("error", "metadata DB refused"),
            ("error", "DAG run limit reached"),
        ]

    def test_a_write_finding_the_writer_busy_gives_its_worker_straight_back(self):
        """A status write must never queue behind another one.

        The pool cannot interrupt a running call, so a write waiting for one to finish
        holds a second worker for as long as the database is stuck — enough of them and
        the pool that carries the DAG triggers is full.
        """
        pool = BoundedExecutor("test-status-busy", 2)
        writer = _StatusWriter(7)
        release = threading.Event()
        entered = threading.Event()
        commits = []

        def write(session, sub_id, status, last_error=None):
            commits.append(status)
            entered.set()
            release.wait(5)

        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write):
                writer.record_if_needed("listening", None)
                hung = pool.submit(writer.store)
                assert entered.wait(2)

                writer.record_if_needed("error", "gone")
                pool.submit(writer.store).result(timeout=1)
                assert commits == ["listening"], "the busy writer let a second write in"

                unrelated = pool.submit(lambda: "a trigger of another subscription")
                assert unrelated.result(timeout=1) == "a trigger of another subscription"
        finally:
            release.set()
            with suppress(Exception):
                hung.result(timeout=2)
            pool.shutdown()

    def test_a_status_the_running_write_is_already_committing_is_not_written_again(self):
        """A subscription under traffic reports ``listening`` after every message it
        triggers on, and those reports must cost the row nothing.

        The pair the running write is committing is the pair the row is about to hold,
        so a report of that same pair is already satisfied. Compared instead against the
        pair the row held *before* that write, every report landing inside a commit is
        noted afresh, the write loops and commits the identical pair again, and the next
        report lands inside that commit: the writer keeps a worker of the pool the DAG
        triggers run in for as long as the queue stays busy, and the row is updated once
        per message.
        """
        pool = BoundedExecutor("test-status-hot", 2)
        writer = _StatusWriter(7)
        entered = threading.Event()
        release = threading.Event()
        commits = []

        def write(session, sub_id, status, last_error=None):
            commits.append((status, last_error))
            entered.set()
            release.wait(5)

        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write):
                writer.record_if_needed("listening", None)
                running = pool.submit(writer.store)
                assert entered.wait(2)

                asked = [writer.record_if_needed("listening", None) for _ in range(20)]
                assert asked == [False] * 20, "a report of the pair being committed"

                release.set()
                running.result(timeout=5)
        finally:
            release.set()
            pool.shutdown()

        assert commits == [("listening", None)]
        assert not writer.has_pending, "the writer would keep its worker"
        assert writer.record_if_needed("listening", None) is False

    def test_a_different_status_landing_inside_the_running_write_still_lands(self):
        """The pair a running write is committing settles only callers asking for that
        pair. A subscription that fails while its ``listening`` is in flight reports the
        failure into the write that is running, and the row ends up naming the fault."""
        pool = BoundedExecutor("test-status-differs", 2)
        writer = _StatusWriter(7)
        entered = threading.Event()
        release = threading.Event()
        commits = []

        def write(session, sub_id, status, last_error=None):
            commits.append((status, last_error))
            if status == "listening":
                entered.set()
                release.wait(5)

        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write):
                writer.record_if_needed("listening", None)
                running = pool.submit(writer.store)
                assert entered.wait(2)

                assert writer.record_if_needed("error", "gone") is True
                release.set()
                running.result(timeout=5)
        finally:
            release.set()
            pool.shutdown()

        assert commits == [("listening", None), ("error", "gone")]
        assert writer.stored == ("error", "gone")

    def test_a_write_that_raises_leaves_no_pair_claiming_to_be_on_its_way(self):
        """A failed write settles nothing: the status it took on goes back to being the
        one to store next, and the caller that asks again is told to store.

        The pair in flight is read whenever a write is running, and the next write is
        running from the moment it claims the writer until it takes that pair out. A
        pair left over from a write that raised would answer for it in between, and
        would answer with a pair the database never took — the caller would skip the
        write, and the row would keep whatever it held before the outage. Hence the
        assertion on the attribute itself: the state it must not be in is a state no
        single call can be made to show.
        """
        writer = _StatusWriter(7)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=RuntimeError("metadata database is away")):
            writer.record_if_needed("listening", None)
            with pytest.raises(RuntimeError):
                writer.store()

        assert writer.has_pending
        assert writer.stored is None
        assert writer._storing_pair is None
        assert writer.record_if_needed("listening", None) is True

    def test_writes_that_arrive_in_order_all_land(self):
        writer = _StatusWriter(7)
        landed = []

        def write(session, sub_id, status, last_error=None):
            landed.append(status)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            for status in ("connecting", "listening", "error"):
                writer.record_if_needed(status, None)
                writer.store()
                assert not writer.has_pending

        assert landed == ["connecting", "listening", "error"]
        assert writer.stored == ("error", None)

    def test_a_status_noted_while_a_write_runs_is_taken_by_that_write(self):
        """The caller that finds the writer busy leaves without storing, so the status
        it noted reaches the row only because the running write picks it up before it
        gives up its worker."""
        pool = BoundedExecutor("test-status-handover", 2)
        writer = _StatusWriter(7)
        release = threading.Event()
        entered = threading.Event()
        landed = []

        def write(session, sub_id, status, last_error=None):
            landed.append(status)
            if status == "listening":
                entered.set()
                release.wait(5)

        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write):
                writer.record_if_needed("listening", None)
                running = pool.submit(writer.store)
                assert entered.wait(2)

                writer.record_if_needed("error", "gone")
                pool.submit(writer.store).result(timeout=1)

                release.set()
                running.result(timeout=2)
        finally:
            release.set()
            pool.shutdown()

        assert landed == ["listening", "error"]
        assert not writer.has_pending
        assert writer.stored == ("error", "gone")

    @pytest.mark.asyncio
    async def test_a_write_the_caller_gave_up_on_still_counts_as_stored(self):
        """The row and the marker cannot be allowed to disagree.

        A caller that times out while the database is stalled has not written nothing:
        the worker it walked away from commits that status once the database is back. A
        marker that stayed behind would then read ``error`` while the row reads
        ``listening``, and the guard that skips an unchanged status would suppress every
        write that could correct it — the subscription shows green for good while every
        trigger it makes fails.
        """
        pool = BoundedExecutor("test-status-desync", 4)
        state = _ConsumerState(sub_id=7, executor=pool)
        row = []
        release = threading.Event()

        def write(session, sub_id, status, last_error=None):
            if status == "listening":
                release.wait(5)     # the database stalls on exactly this write
            row.append(status)

        ctx, _ = _mock_session()
        try:
            with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write), \
                 patch("airflow_provider_rmq.watcher.consumer._DB_TIMEOUT", 0.2):
                await state.write("error", last_error="the trigger failed")
                await state.write("listening")      # times out for the caller, lands later
                release.set()
                deadline = time.monotonic() + 5
                while len(row) < 2 and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
                await state.write("error", last_error="the trigger failed again")
        finally:
            release.set()
            pool.shutdown()

        assert row == ["error", "listening", "error"], (
            f"the row is stuck on {row[-1]!r} for a subscription that triggers nothing"
        )

    @pytest.mark.asyncio
    async def test_a_removed_subscription_is_written_through_its_own_state(self, manager):
        """reconcile writes ``disconnected`` for a subscription whose task it has just
        cancelled — the same row that task was writing, so it takes the same order."""
        sub = _sub(id=7)
        entry = _register_active(manager, sub, real_task=True)
        writes = []

        async def capture(status, last_error=None, executor=None):
            writes.append((status, executor))

        with patch.object(entry.state, "write", side_effect=capture), \
             patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock):
            await manager.reconcile([])

        assert ("disconnected", manager._cycle_executor) in writes

    @pytest.mark.asyncio
    async def test_a_write_made_while_a_store_runs_is_not_taken_for_a_repeat(self):
        """"Does the row already say this?" cannot be answered beside a running store.

        A store takes the noted status out of the writer before it commits, so while it
        commits the writer holds the previous pair and nothing noted. A write of that
        previous pair arriving in that window looks like a repeat of what the row says
        and is skipped — and the store then puts the other status in the row. The
        subscription reports one thing and its row says another, with nothing left to
        correct it: a quiet queue makes no further write, so the row keeps the stale
        value until the subscription is next reattached or removed.
        """
        pool = BoundedExecutor("test-status-store-race", 4)
        state = _ConsumerState(7, pool)
        row = []
        entered = threading.Event()
        release = threading.Event()

        def write(session, sub_id, status, last_error=None):
            row.append((status, last_error))
            if status == "error":
                entered.set()
                release.wait(5)

        loop = asyncio.get_running_loop()
        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write):
                await state.write("listening")
                # The status a failed write left behind, handed to a cycle-pool worker
                # by the pass over the writers.
                _status_writer(7).record_if_needed("error", "boom")
                storing = pool.submit(_status_writer(7).store)
                assert await loop.run_in_executor(None, entered.wait, 5)

                await state.write("listening")

                release.set()
                await loop.run_in_executor(None, storing.result, 5)
        finally:
            release.set()
            pool.shutdown()

        assert row[-1] == ("listening", None), (
            f"the row is left saying {row[-1]!r} about a listening subscription"
        )
        assert _status_writer(7).stored == ("listening", None)
        assert not _status_writer(7).has_pending


class TestTheRowOfASubscriptionThatIsGone:
    """The last status of a removed subscription still has to reach its row.

    ``disconnected`` is written after reconcile has let go of the entry, so a write that
    does not land leaves nothing holding the subscription that could come back to it.
    The row then keeps saying ``listening`` about a consumer that no longer exists —
    the false green the watchdog exists to prevent — and says it for good.
    """

    @pytest.mark.asyncio
    async def test_the_next_cycle_writes_the_status_of_a_subscription_that_is_gone(
        self, manager
    ):
        """The subscription is out of the manager by the time the write fails, so the
        cycle is the only thing left that can finish its row."""
        _register_active(manager, _sub(id=7), real_task=True)
        landed = []
        db_up = False

        def write(session, sub_id, status, last_error=None):
            if not db_up:
                raise RuntimeError("metadata database is away")
            landed.append((sub_id, status))

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write), \
             patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock):
            await manager.reconcile([])

            assert landed == []
            assert 7 not in manager._active
            assert _status_writer(7).has_pending

            db_up = True
            await manager.reconcile([])

        assert landed == [(7, "disconnected")], (
            "the row of the removed subscription still reads what it read while the "
            "database was away"
        )

    @pytest.mark.asyncio
    async def test_two_failed_cycles_in_a_row_do_not_lose_the_status(self, manager):
        """An outage lasts as long as it lasts: the status is tried again every cycle
        until one of them gets it into the row."""
        _register_active(manager, _sub(id=7), real_task=True)
        attempts = []
        landed = []
        db_up = False

        def write(session, sub_id, status, last_error=None):
            attempts.append(status)
            if not db_up:
                raise RuntimeError("metadata database is away")
            landed.append(status)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write), \
             patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock):
            await manager.reconcile([])
            await manager.reconcile([])

            assert landed == []
            assert _status_writer(7).has_pending

            db_up = True
            await manager.reconcile([])

        assert landed == ["disconnected"]
        assert len(attempts) >= 3, attempts

    @pytest.mark.asyncio
    async def test_a_cycle_cancelled_on_the_final_write_does_not_lose_it(self, manager):
        """The cycle budget runs out on exactly the write a stalled database holds up,
        and the subscription it belongs to is already gone from the manager."""
        _register_active(manager, _sub(id=7), real_task=True)
        entered = threading.Event()
        release = threading.Event()
        landed = []
        db_up = False

        def write(session, sub_id, status, last_error=None):
            if not db_up:
                entered.set()
                release.wait(5)
                raise RuntimeError("metadata database is away")
            landed.append(status)

        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write), \
                 patch.object(manager, "_update_all_conn_counts", new_callable=AsyncMock):
                cycle = asyncio.create_task(manager.reconcile([]))
                deadline = time.monotonic() + 5
                while not entered.is_set() and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
                assert entered.is_set()

                cycle.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await cycle

                release.set()
                while not _status_writer(7).has_pending and time.monotonic() < deadline:
                    await asyncio.sleep(0.01)
                assert _status_writer(7).has_pending

                db_up = True
                await manager.reconcile([])
        finally:
            release.set()

        assert landed == ["disconnected"]

    @pytest.mark.asyncio
    async def test_the_pass_survives_a_row_that_is_no_longer_there(self, manager):
        """A subscription deleted from the table takes its status row with it.

        The write is an ``UPDATE ... WHERE id = ...``, so a row that is gone matches
        nothing (``tests/watcher/test_models.py`` puts that to a real session) and costs
        the cycle one attempt that leaves nothing pending rather than an error.
        """
        _status_writer(7).record_if_needed("disconnected", None)
        attempts = []

        def write(session, sub_id, status, last_error=None):
            attempts.append(sub_id)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await manager._store_unwritten_statuses()

        assert attempts == [7]
        assert not _status_writer(7).has_pending

    @pytest.mark.asyncio
    async def test_a_write_that_fails_does_not_stop_the_writers_behind_it(self, manager):
        """One row the database refuses costs the pass that row, not the rest of them."""
        _status_writer(7).record_if_needed("disconnected", None)
        _status_writer(8).record_if_needed("disconnected", None)
        landed = []

        def write(session, sub_id, status, last_error=None):
            if sub_id == 7:
                raise RuntimeError("metadata database is away")
            landed.append(sub_id)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await manager._store_unwritten_statuses()

        assert landed == [8]
        assert _status_writer(7).has_pending

    @pytest.mark.asyncio
    async def test_the_whole_pass_shares_one_budget(self, manager):
        """The registry holds a writer for every subscription the process has ever
        written a status for, so a bound per writer is a cost that grows with the age of
        the process: a database answering nothing would spend the cycle's whole budget
        here and park a cycle-pool worker for each row it was asked about."""
        for sub_id in (1, 2, 3, 4, 5, 6):
            _status_writer(sub_id).record_if_needed("disconnected", None)
        attempts = []
        release = threading.Event()

        def write(session, sub_id, status, last_error=None):
            attempts.append(sub_id)
            release.wait(5)

        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer._DB_TIMEOUT", 0.05), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write):
                await asyncio.wait_for(
                    manager._store_unwritten_statuses(), timeout=5
                )
        finally:
            release.set()

        assert attempts == [1], (
            "the pass asked the stalled database about more than one row"
        )

    @pytest.mark.asyncio
    async def test_the_registry_answers_other_callers_while_the_pass_waits(self, manager):
        """The snapshot is taken under the registry lock and the lock is let go before
        the first await. Holding it across the wait would shut every consumer task out of
        :func:`_status_writer` for as long as the database stayed away, and reading the
        live registry instead of a snapshot would end the pass on the first writer another
        thread registers under it."""
        _status_writer(7).record_if_needed("disconnected", None)
        entered = threading.Event()
        release = threading.Event()

        def write(session, sub_id, status, last_error=None):
            entered.set()
            release.wait(5)

        loop = asyncio.get_running_loop()
        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write):
                pass_task = asyncio.create_task(manager._store_unwritten_statuses())
                assert await loop.run_in_executor(None, entered.wait, 5)

                registered = await asyncio.wait_for(
                    loop.run_in_executor(None, _status_writer, 8), timeout=2
                )
                assert registered is _status_writers[8]

                release.set()
                await asyncio.wait_for(pass_task, timeout=5)
        finally:
            release.set()

    @pytest.mark.asyncio
    async def test_a_writer_with_nothing_left_does_not_touch_the_database(self, manager):
        """The pass is a repair, not a heartbeat: a row that already agrees with its
        subscription is not rewritten every cycle."""
        _register_active(manager, _sub(id=7))
        _status_writer(7)

        with patch.object(
            manager._cycle_executor, "run", new_callable=AsyncMock
        ) as offloaded:
            await manager._store_unwritten_statuses()

        offloaded.assert_not_called()

    @pytest.mark.asyncio
    async def test_the_pass_does_not_start_a_second_write_beside_a_running_one(
        self, manager
    ):
        """Two writes of one row at once are what the single writer exists to prevent,
        and the pass goes through that writer like every other caller: it finds the
        write running, leaves the status to it and returns its worker straight back."""
        writer = _status_writer(7)
        started = []
        entered = threading.Event()
        release = threading.Event()

        def write(session, sub_id, status, last_error=None):
            started.append(status)
            if status == "listening":
                entered.set()
                release.wait(5)

        pool = BoundedExecutor("test-flush-overlap", 4)
        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write):
                writer.record_if_needed("listening", None)
                running = pool.submit(writer.store)
                assert entered.wait(2)

                writer.record_if_needed("disconnected", None)
                await asyncio.wait_for(manager._store_unwritten_statuses(), timeout=2)
                assert started == ["listening"]

                release.set()
                running.result(timeout=5)
        finally:
            release.set()
            pool.shutdown()

        assert started == ["listening", "disconnected"]


class TestTheRowsAStopLeavesBehind:
    """A stopped manager cancels every consumer, and a cancelled task writes nothing.

    Whatever the stop was — the scheduler going down, or a cycle that outran its budget
    and costs the watcher its event loop — the subscriptions it was consuming are not
    being consumed by anything afterwards, and a row still saying ``listening`` is the
    false green the watchdog exists to prevent.
    """

    @pytest.mark.asyncio
    async def test_stopping_writes_the_final_status_of_every_subscription(self, manager):
        _register_active(manager, _sub(id=7), real_task=True)
        _register_active(manager, _sub(id=8), real_task=True)
        landed = []

        def write(session, sub_id, status, last_error=None):
            landed.append((sub_id, status))

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await manager.stop()

        assert sorted(landed) == [(7, "disconnected"), (8, "disconnected")]

    @pytest.mark.asyncio
    async def test_the_rows_are_written_before_the_connections_are_closed(self, manager):
        """The consumers are cancelled before either step, so the rows are already
        final and nothing about them waits on a socket.

        Closing comes second because the incident these rows exist for is a broker that
        has stopped answering, and on that broker every close runs to its full timeout.
        Writing behind them is writing on whatever the budget over the whole stop has
        left, in a daemon thread the scheduler has already stopped waiting for.
        """
        order = []
        _register_active(manager, _sub(id=7), real_task=True)

        connection = MagicMock()
        connection.is_closed = False

        async def close():
            order.append("connection closed")

        connection.close = close
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = connection

        def write(session, sub_id, status, last_error=None):
            order.append((sub_id, status))

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await manager.stop()

        assert order == [(7, "disconnected"), "connection closed"]

    @pytest.mark.asyncio
    async def test_a_close_that_never_answers_does_not_cost_the_rows_their_write(self):
        """The one incident the stop-time pass exists for: a broker whose socket is
        black-holed. Every close then burns its full timeout, the caller's budget over
        the stop runs out and cancels what is left of the call, and the process goes
        down with no next manager to store what a graceful stop left noted."""
        mgr = _make_manager()
        _register_active(mgr, _sub(id=7), real_task=True)
        landed = []

        connection = MagicMock()
        connection.is_closed = False

        async def never_answers():
            await asyncio.Future()

        connection.close = never_answers
        mgr._conn("rmq_default").connections[_ROLE_CONSUME] = connection

        def write(session, sub_id, status, last_error=None):
            landed.append((sub_id, status))

        # The bound the listener puts on the whole stop, scaled down: it expires while
        # the close is still waiting for an answer that never comes.
        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write), \
             patch("airflow_provider_rmq.watcher.consumer._CLOSE_TIMEOUT", 5.0), \
             pytest.raises(asyncio.TimeoutError):
            await call_with_timeout(mgr.stop(), timeout=0.3)

        assert landed == [(7, "disconnected")]

    @pytest.mark.asyncio
    async def test_the_subscriptions_the_stop_let_go_of_are_dunned_first(self):
        """A budget that runs out mid-pass decides nothing about which rows it reached,
        so the order does. The rows this manager was consuming up to the moment it
        stopped are the ones a reader would otherwise find still saying ``listening``;
        a writer left pending by an earlier outage describes a subscription this
        manager no longer holds."""
        mgr = _make_manager()
        _status_writer(1).record_if_needed("error", "an outage some cycles ago")
        _status_writer(2).record_if_needed("error", "an outage some cycles ago")
        _register_active(mgr, _sub(id=7), real_task=True)
        order = []

        def write(session, sub_id, status, last_error=None):
            order.append(sub_id)

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await mgr.stop()

        assert order[0] == 7, "the row of the subscription this stop let go of"
        assert sorted(order) == [1, 2, 7]

    @pytest.mark.asyncio
    async def test_a_status_the_stop_could_not_store_is_taken_by_the_next_manager(self):
        """The status is noted on the writer before anything else in the teardown, and
        the writer belongs to the process: the manager that replaces this one stores it
        on its first cycle."""
        mgr = _make_manager()
        _register_active(mgr, _sub(id=7), real_task=True)
        landed = []
        db_up = False

        def write(session, sub_id, status, last_error=None):
            if not db_up:
                raise RuntimeError("metadata database is away")
            landed.append((sub_id, status))

        with _patch_watcher_session(), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                   side_effect=write):
            await mgr.stop()
            assert landed == []
            assert _status_writer(7).has_pending, "the final status is gone for good"

            db_up = True
            await _make_manager()._store_unwritten_statuses()

        assert landed == [(7, "disconnected")]

    @pytest.mark.asyncio
    async def test_a_database_that_answers_nothing_costs_the_stop_its_bound(self):
        """The teardown is what a fresh event loop — or a scheduler shutdown — waits on,
        and the pass runs in the cycle pool, whose workers the next cycle reads the
        database through. One bound covers the pass as a whole."""
        pool = BoundedExecutor("test-stop-status-bound", 2)
        mgr = _make_manager(cycle_executor=pool)
        _register_active(mgr, _sub(id=7), real_task=True)
        _register_active(mgr, _sub(id=8), real_task=True)
        release = threading.Event()

        def write(session, sub_id, status, last_error=None):
            release.wait(5)

        try:
            with _patch_watcher_session(), \
                 patch("airflow_provider_rmq.watcher.consumer._STOP_STATUS_TIMEOUT", 0.05), \
                 patch("airflow_provider_rmq.watcher.consumer.set_consumer_status",
                       side_effect=write):
                await asyncio.wait_for(mgr.stop(), timeout=3)
        finally:
            release.set()
            pool.shutdown()

        assert pool.in_flight <= 1, "the stalled database was asked about a second row"


class TestConsumerRegistration:
    """The broker confirms the consumer before the manager says it has one.

    ``basic.consume`` is an RPC like any other and a zombie connection never answers
    it. Reporting ``listening`` and recording the tag before the answer arrives hands
    the liveness check a tag the broker never registered — and the passive-declare
    fallback, which only asks whether the connection answers at all, then vouches for
    it. That is a green connection with nothing consuming, which is the incident.
    """

    def _hanging_registration(self):
        """Queue whose iterator never finishes registering its consumer."""

        class _NeverRegisters:
            async def consume(self):
                await asyncio.Future()

            async def close(self):
                pass

        queue = MagicMock()
        queue.iterator.return_value = _NeverRegisters()
        return queue

    @pytest.mark.asyncio
    async def test_a_registration_that_hangs_is_not_reported_as_listening(self, manager):
        sub = _sub()
        channel = _make_live_channel(queue=self._hanging_registration())
        connection = _make_live_connection(channel=channel)
        # Registered the way reconcile does it, so the task reports into the very state
        # the liveness check reads.
        state = _register_active(manager, sub, status=None).state
        state.consumer_tag = None

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_rpc_timeout", return_value=0.05), \
             patch("airflow_provider_rmq.watcher.consumer._StatusWriter.store"):
            await _run_then_cancel(manager._consume_subscription(sub), timeout=0.5)

        assert state.consumer_tag is None
        assert state.status != "listening"
        channel.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_a_registration_that_hangs_is_reported_as_an_error(self, manager):
        """The row names the fault instead of showing a task that looks like it is
        starting up: ``connecting`` with no ``last_error`` is what a subscription whose
        ``basic.consume`` is never answered would otherwise show for as long as the
        fault lasts."""
        sub = _sub()
        channel = _make_live_channel(queue=self._hanging_registration())
        connection = _make_live_connection(channel=channel)
        state = _register_active(manager, sub, status=None).state
        writes: list = []

        async def capture(status, last_error=None, executor=None):
            writes.append((status, last_error))

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch.object(manager, "_rpc_timeout", return_value=0.05), \
             patch.object(state, "write", side_effect=capture):
            await _run_then_cancel(manager._consume_subscription(sub), timeout=0.5)

        assert any(status == _SUB_ERROR and last_error for status, last_error in writes), (
            f"the subscription reported {writes} and never said what went wrong"
        )

    @pytest.mark.asyncio
    async def test_the_registration_is_performed_before_the_iterator_is_entered(self, manager):
        """The iterator registers on entry when nobody registered before it, so the
        manager does it itself — that is the only way to put a bound on it."""
        queue = _make_push_queue([])
        connection = _make_live_connection(channel=_make_live_channel(queue=queue))

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            await _run_then_cancel(manager._consume_subscription(_sub()), timeout=0.3)

        assert queue.iterator.return_value.consumed is True


class TestConsumerChannelIsClosed:
    """A consumer that leaves its iterator closes the channel it opened.

    Cancelling the consumer is all aio_pika does on the way out; the channel stays
    open. A loop that reattaches after every failed publish or trigger would collect
    one more of them each time, until the broker's channel limit ends the connection.
    """

    @pytest.mark.asyncio
    async def test_the_channel_is_closed_when_the_consumer_is_cancelled(self, manager):
        channel = _make_live_channel(queue=_make_push_queue([]))
        connection = _make_live_connection(channel=channel)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            await _run_then_cancel(manager._consume_subscription(_sub()), timeout=0.3)

        channel.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_the_channel_is_closed_when_the_broker_ends_the_consumer(self, manager):
        """The iterator finishing without an exception is the broker cancelling us; the
        loop subscribes again, and the channel of the attempt just ended goes with it."""
        channel = _make_live_channel(queue=_ending_queue())
        connection = _make_live_connection(channel=channel)

        with patch.object(manager, "_get_or_create_connection", return_value=connection), \
             patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"), \
             _record_consumer_sleeps(lambda delay: None):
            await _run_then_cancel(manager._consume_subscription(_sub()), timeout=0.3)

        channel.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_the_fire_channel_is_closed_too(self, manager):
        channel = _make_live_channel(queue=_make_push_queue([]))
        connection = _make_live_connection(channel=channel)

        with patch("airflow_provider_rmq.watcher.consumer._ConsumerState.write"):
            await _run_then_cancel(
                manager._consume_fire_queue(connection, "rmq_default"), timeout=0.3
            )

        channel.close.assert_awaited()

    @pytest.mark.asyncio
    async def test_leaving_the_consumer_is_bounded(self, manager):
        """``basic.cancel`` is an RPC too, and aio_pika awaits it without a bound of its
        own — on a zombie connection that is where a cancelled task stays forever."""

        class _HangingClose:
            async def consume(self):
                pass

            async def close(self):
                await asyncio.Future()

            def __aiter__(self):
                return self

            async def __anext__(self):
                raise StopAsyncIteration

        queue = MagicMock()
        queue.iterator.return_value = _HangingClose()

        with patch("airflow_provider_rmq.watcher.consumer._CLOSE_TIMEOUT", 0.05):
            async with _attached(queue, "tag", 1.0):
                pass    # leaving the block must return, not wait on basic.cancel


class TestFireDeliveryFailure:
    """A fire event whose DAG will not start goes back on the queue.

    Leaving the loop hands back only what the iterator still holds in its buffer, not
    the delivery in hand: without a NACK the event sits unacknowledged on the abandoned
    channel until the broker's ``consumer_timeout`` expires.
    """

    @pytest.mark.asyncio
    async def test_a_failed_trigger_requeues_the_fire_event(self, manager):
        msg = _make_fake_message(b"", message_id="m1")
        msg.routing_key = "my_dag"
        state = _ConsumerState(sub_id=None, executor=manager._executor)
        delays: list = []

        with patch.object(
            manager, "_trigger_fire_dag", side_effect=RuntimeError("scheduler is down")
        ), _record_consumer_sleeps(delays.append):
            await manager._handle_fire_delivery(
                msg, state, _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX)
            )

        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()
        assert delays == [_TRIGGER_BACKOFF_START]
        assert state.status == _SUB_ERROR

    @pytest.mark.asyncio
    async def test_a_triggered_fire_event_is_acknowledged(self, manager):
        msg = _make_fake_message(b"", message_id="m1")
        msg.routing_key = "my_dag"
        state = _ConsumerState(sub_id=None, executor=manager._executor)

        with patch.object(
            manager, "_trigger_fire_dag", return_value=_OUTCOME_TRIGGERED
        ):
            await manager._handle_fire_delivery(
                msg, state, _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX)
            )

        msg.ack.assert_awaited_once()
        msg.nack.assert_not_awaited()
        assert state.status == _SUB_LISTENING


class TestAFireDagWithoutASerializedVersion:
    """A fire trigger answering ``DagNotFound`` says the DAG processor is warming up.

    The fire consumer starts with the scheduler, minutes before ``serialized_dag`` is
    filled, and for that stretch the connection, the channel and the registration are
    all in place: the event waits for the rest of Airflow. Its reported status is what
    the liveness check reads, so holding it at ``listening`` keeps a consumer that dies
    inside a long warmup something the check can still notice.
    """

    @staticmethod
    def _fire_message(dag_id: str = "my_dag"):
        msg = _make_fake_message(b"", message_id="m1")
        msg.routing_key = dag_id
        return msg

    @staticmethod
    def _warming_up(manager) -> None:
        """Let the trigger answer that the DAG has no serialized version yet.

        The answer is raised where the offloaded trigger runs, so it travels the whole
        way the live one does — the executor, the timeout around it and
        ``_trigger_fire_dag`` — instead of being handed to the handler ready-made.
        """
        async def handler(fn, *args):
            raise _DagNotReady("Dag id my_dag not found")

        manager._executor = _FakeExecutor(handler)

    @pytest.mark.asyncio
    async def test_the_event_goes_back_and_the_status_is_held_at_listening(self, manager):
        msg = self._fire_message()
        state = _ConsumerState(sub_id=None, executor=manager._executor)
        state.write = AsyncMock()
        delays: list = []
        self._warming_up(manager)

        with patch(f"{_CONSUMER_MODULE}.log") as log, \
             _record_consumer_sleeps(delays.append):
            await manager._handle_fire_delivery(
                msg, state, _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX)
            )

        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()
        state.write.assert_awaited_once_with(_SUB_LISTENING, last_error=None)
        assert delays == [_TRIGGER_BACKOFF_START]
        [(level, text)] = _warmup_lines(log)
        assert level == logging.INFO
        assert "not serialized yet" in text

    @pytest.mark.asyncio
    async def test_the_wait_has_no_bound_and_every_tenth_event_is_a_warning(self, manager):
        """Nothing here ever ends the wait, and the log is the whole of what says so.

        The immediate path gives up after a bound and colours its row; a fire consumer
        has no row of its own and serves every cooldown DAG at once, so a DAG that never
        becomes serializable circles here for as long as the process runs. Every tenth
        event is raised to WARNING to keep that visible in a filtered log.
        """
        state = _ConsumerState(sub_id=None, executor=manager._executor)
        state.write = AsyncMock()
        backoff = _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX)
        count = _NOT_READY_LIMIT + 5
        self._warming_up(manager)

        with patch(f"{_CONSUMER_MODULE}.log") as log, \
             _record_consumer_sleeps(lambda delay: None):
            for _ in range(count):
                msg = self._fire_message()
                await manager._handle_fire_delivery(msg, state, backoff)
                msg.nack.assert_awaited_once_with(requeue=True)
                msg.ack.assert_not_awaited()

        assert {c.args[0] for c in state.write.await_args_list} == {_SUB_LISTENING}
        lines = _warmup_lines(log)
        assert len(lines) == count
        assert [
            n for n, (level, _) in enumerate(lines, start=1)
            if level == logging.WARNING
        ] == list(range(10, count + 1, 10))
        assert "my_dag" in lines[9][1]
        assert "10 events in a row" in lines[9][1]

    @pytest.mark.asyncio
    async def test_the_count_belongs_to_the_dag_and_not_to_the_consumer(self, manager):
        """One fire consumer serves every cooldown DAG.

        A single count kept on it would mix the warmups of unrelated DAGs, and the log
        line would say a DAG has been waiting through events that were never about it.
        """
        state = _ConsumerState(sub_id=None, executor=manager._executor)
        state.write = AsyncMock()
        backoff = _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX)
        self._warming_up(manager)

        with patch(f"{_CONSUMER_MODULE}.log") as log, \
             _record_consumer_sleeps(lambda delay: None):
            for _ in range(9):
                await manager._handle_fire_delivery(
                    self._fire_message(), state, backoff
                )
            await manager._handle_fire_delivery(
                self._fire_message("other_dag"), state, backoff
            )

        level, text = _warmup_lines(log)[-1]
        assert level == logging.INFO
        assert "other_dag" in text
        assert "1 events in a row" in text

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "error, candidate",
        [
            (_DagNotReady("Dag id my_dag not found"), True),
            (RuntimeError("airflow metadata db is down"), False),
        ],
        ids=["warming-up", "trigger-failure"],
    )
    async def test_the_liveness_check_keeps_asking_about_a_warming_up_consumer(
        self, manager, error, candidate
    ):
        """A status of ``listening`` is what keeps the fire consumer in the check.

        The check asks the broker for the tag of every candidate; a fire consumer it
        counts out is asked about no more, and one that stopped consuming while the
        DAG processor was filling ``serialized_dag`` would go unnoticed.
        """
        fire = _register_fire(manager)
        delays: list = []

        with patch.object(manager, "_trigger_fire_dag", side_effect=error), \
             _record_consumer_sleeps(delays.append):
            await manager._handle_fire_delivery(
                self._fire_message(),
                fire.state,
                _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX),
            )

        assert (manager._fire_candidate() is fire) is candidate
        assert (fire.state.status == _SUB_ERROR) is not candidate

    @pytest.mark.asyncio
    async def test_a_warmup_takes_the_consumer_back_into_the_check(self, manager):
        """An ``error`` left by an unrelated failure does not outlast the next event.

        One trigger that timed out against a loaded metadata database is enough to have
        the consumer reporting ``error``, and a warmup that only declined to touch the
        status would leave it there for every event of the whole warmup — a fire
        consumer whose connection died in that window would be asked about by nobody.
        """
        fire = _register_fire(manager, status=_SUB_ERROR)
        assert manager._fire_candidate() is None
        self._warming_up(manager)

        with _record_consumer_sleeps(lambda delay: None):
            await manager._handle_fire_delivery(
                self._fire_message(),
                fire.state,
                _Backoff(_TRIGGER_BACKOFF_START, _TRIGGER_BACKOFF_MAX),
            )

        assert fire.state.status == _SUB_LISTENING
        assert manager._fire_candidate() is fire


class TestAbandonedTasks:
    """A cancelled task that never finished is kept, not forgotten.

    Recovery starts a replacement without it, so nothing else refers to it — asyncio
    itself holds only a weak reference — and its channel and consumer go on living
    unseen. Keeping it makes the number of them something the operator can read.
    """

    @pytest.mark.asyncio
    async def test_a_task_that_outlived_its_cancellation_is_kept(self, manager):
        real_sleep = asyncio.sleep

        async def stubborn():
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                await real_sleep(0.3)

        task = asyncio.create_task(stubborn())
        await real_sleep(0)
        task.cancel()

        with patch("airflow_provider_rmq.watcher.consumer._CANCEL_TIMEOUT", 0.05):
            pending = await _wait_cancelled([task])

        assert pending == {task}
        manager._abandon(pending)
        assert task in manager._abandoned
        await task
        await real_sleep(0)
        assert manager._abandoned == set(), "a task that finished lets go of itself"

    @pytest.mark.asyncio
    async def test_each_task_is_counted_once(self, manager):
        """``tasks_abandoned`` counts tasks, not the cycles that go on holding them.

        The number is read as "consumers stuck on a connection that answers nothing";
        counting the same task again every cycle, or counting a cycle that abandoned
        nothing at all, would make a single stuck task look like a spreading outage.
        """
        gate = asyncio.Event()
        first = asyncio.create_task(gate.wait())
        second = asyncio.create_task(gate.wait())
        await asyncio.sleep(0)

        try:
            with patch("airflow_provider_rmq.watcher.consumer.incr") as incr:
                manager._abandon({first})
                manager._abandon({first})       # the same task, still not finished
                manager._abandon(set())         # a cycle that abandoned nothing
                assert incr.call_count == 1

                manager._abandon({second})
                assert incr.call_count == 2
        finally:
            gate.set()
            await asyncio.gather(first, second)
