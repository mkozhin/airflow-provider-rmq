from __future__ import annotations

import asyncio
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

from airflow_provider_rmq.utils.amqp import DEFAULT_RPC_TIMEOUT, AmqpTimeouts
from airflow_provider_rmq.utils.executor import BoundedExecutor

from airflow_provider_rmq.watcher.consumer import (
    RMQConsumerManager,
    _ActiveSub,
    _ConnLiveness,
    _ConsumerState,
    _CYCLES_BEFORE_REDROP,
    _StatusOrder,
    _FireSub,
    _RECONNECT_DELAY,
    _ROLE_CONSUME,
    _ROLE_PUBLISH,
    _attach_nonce,
    _attached,
    _consumer_tag,
    _build_run_id,
    _safe_run_id,
    _sync_trigger,
    _wait_cancelled,
    _write_conn_error,
    _write_consumer_status,
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

    def __init_iter(self):
        self._pos = 0

    async def __anext__(self):
        if not hasattr(self, "_pos"):
            self._pos = 0
        if self._pos < len(self._messages):
            msg = self._messages[self._pos]
            self._pos += 1
            return msg
        if self._ends:
            raise StopAsyncIteration
        await asyncio.Future()  # block until cancelled → raises CancelledError


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


def _offloading_executor(outcome: str = _OUTCOME_TRIGGERED) -> _FakeExecutor:
    """Executor stand-in for whole-scenario tests.

    A trigger reports ``outcome``, a status write goes nowhere, and everything else —
    reading the Airflow connection, for one — runs as the manager offloaded it.
    """

    async def handler(fn, *args):
        if fn is _sync_trigger:
            return outcome
        if fn is _write_consumer_status:
            return None
        return fn(*args)

    return _FakeExecutor(handler)



_CONSUMER_MODULE = "airflow_provider_rmq.watcher.consumer"


@contextlib.contextmanager
def _record_consumer_sleeps(on_delay):
    """Collect the pauses ``consumer.py`` takes, ignoring every other module's sleep.

    ``consumer.asyncio`` is the asyncio module itself, so patching its ``sleep``
    patches it process-wide; the caller's frame tells whose pause this is.
    """
    real_sleep = asyncio.sleep

    async def fake_sleep(delay, *args, **kwargs):
        caller = inspect.currentframe().f_back
        if caller is not None and caller.f_globals.get("__name__") == _CONSUMER_MODULE:
            on_delay(delay)
        return await real_sleep(0)

    with patch(f"{_CONSUMER_MODULE}.asyncio.sleep", new=fake_sleep):
        yield


def _pooled_connections(manager) -> list:
    """Every connection the manager holds, whatever conn_id or role it is pooled under."""
    return [
        conn for state in manager._conns.values() for conn in state.connections.values()
    ]


def _test_pool(name: str = "test-pool") -> BoundedExecutor:
    """Real bounded pool for a test. The manager takes its pools from its caller."""
    return BoundedExecutor(name, 4)


def _make_manager(executor=None, cycle_executor=None) -> RMQConsumerManager:
    """Build a manager the way the listener does, with a pool for each role."""
    return RMQConsumerManager(
        executor=executor if executor is not None else _test_pool("test-consumer"),
        cycle_executor=(
            cycle_executor if cycle_executor is not None else _test_pool("test-cycle")
        ),
    )


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
    def _make_state(self, mock_ws, mock_set):
        return _ConsumerState(sub_id=42, executor=_test_pool())

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
            state._last_status = "error"  # simulate being in error state
            await state.write("connecting")
            await state.write("listening", last_error=None)

        assert ("listening", None) in writes

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
        # The stored marker did not move, so the next call tries the write again.
        assert state._stored_status is None


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

        with patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, return_value=connection) as mock_connect, \
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
        """cooldown>0: published Message must have expiration = str(cooldown * 1000)."""
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

        # aio_pika.Message stores expiration as string
        assert published_msg["msg"].expiration == "300000"

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
    async def test_reconcile_starts_fire_task_when_cooldown_sub_added(self):
        """reconcile starts _fire_task when first cooldown subscription appears."""
        manager = _make_manager()

        async def blocking_consume(sub):
            await asyncio.Future()

        async def blocking_fire(conn, conn_id=None):
            await asyncio.Future()

        connection = AsyncMock()

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

        connection = AsyncMock()

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


def _fast_timeouts(manager, conn_id: str = "rmq_default", rpc: float = 0.05) -> None:
    manager._conn(conn_id).timeouts = AmqpTimeouts(connect=0.05, rpc=rpc)


class TestConnectionPool:
    @pytest.mark.asyncio
    async def test_closed_connection_is_replaced(self, manager):
        closed = AsyncMock()
        closed.is_closed = True
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = closed
        fresh = _make_live_connection()

        with patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, return_value=fresh) as mock_connect, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=_make_conn_info()):
            result = await manager._get_or_create_connection("rmq_default")

        assert result is fresh
        assert manager._conn("rmq_default").connections[_ROLE_CONSUME] is fresh
        assert closed not in _pooled_connections(manager)
        assert mock_connect.await_count == 1

    @pytest.mark.asyncio
    async def test_live_connection_is_reused(self, manager):
        live = _make_live_connection()
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = live

        with patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock) as mock_connect:
            first = await manager._get_or_create_connection("rmq_default")
            second = await manager._get_or_create_connection("rmq_default")

        assert first is live and second is live
        mock_connect.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_connect_robust_gets_connect_timeout_from_extra(self, manager):
        conn_info = _make_conn_info()
        conn_info.extra_dejson = {"connect_timeout": 3}

        with patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, return_value=_make_live_connection()) as mock_connect, \
             patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                   return_value=conn_info):
            await manager._get_or_create_connection("rmq_default")

        assert mock_connect.await_args.kwargs["timeout"] == 3

    @pytest.mark.asyncio
    async def test_consume_and_publish_roles_are_separate_connections(self, manager):
        consume_conn = _make_live_connection()
        publish_conn = _make_live_connection()
        created = [consume_conn, publish_conn]

        async def fake_connect(**kwargs):
            return created.pop(0)

        with patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   side_effect=fake_connect), \
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

        with patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, return_value=connection), \
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
    state._stored_status = status
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
    manager._fire_state = _FireSub(conn_id=conn_id, state=_state_with(status, None))
    return manager._fire_state


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


def _mgmt_conn_info(url: str = "https://mb.example"):
    conn_info = _make_conn_info()
    conn_info.extra_dejson = {"management_url": url}
    return conn_info


def _patch_mgmt_connection(url: str = "https://mb.example"):
    return patch(
        "airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
        return_value=_mgmt_conn_info(url),
    )


class TestSubscriptionLiveness:
    @pytest.mark.asyncio
    async def test_missing_tag_twice_condemns_the_subscription(self, manager):
        sub = _sub(id=7, queue_name="orders")
        _register_active(manager, sub)
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            first = await manager._check_subscription_liveness([sub])
            second = await manager._check_subscription_liveness([sub])

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
            first = await manager._check_subscription_liveness([sub])

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
            result = await manager._check_subscription_liveness([sub])

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
            await manager._check_subscription_liveness([sub])
            result = await manager._check_subscription_liveness([sub])

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
            result = await manager._check_subscription_liveness([sub])

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
            await manager._check_subscription_liveness([sub])
            result = await manager._check_subscription_liveness([sub])

        assert result == ({7}, {"rmq_default"})
        assert requested and all("/api/nodes" not in url for url in requested)

    @pytest.mark.asyncio
    async def test_management_api_failure_is_no_data(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        manager._http_client = _mgmt_client({"error": "unavailable"}, status_code=503)

        with _patch_mgmt_connection(), \
             patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            first = await manager._check_subscription_liveness([sub])

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
        probe = AsyncMock(return_value=({entry.state.consumer_tag}, None, None))

        with _patch_mgmt_connection(), \
             patch.object(manager, "_probe_by_passive_declare", probe):
            await manager._check_subscription_liveness([sub])
            assert probe.await_count == 0, "a single failure is still 'no data'"
            await manager._check_subscription_liveness([sub])

        assert probe.await_count == 1
        assert manager._conn("rmq_default").liveness.status == "connected"
        assert entry.negative_checks == 0

    @pytest.mark.asyncio
    async def test_one_management_request_serves_every_conn_id_of_a_vhost(self, manager):
        """Several conn_ids often point at one broker, and GET /api/consumers/{vhost}
        answers for the whole vhost — asking once per conn_id multiplies the same call."""
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

        with _patch_mgmt_connection():
            result = await manager._check_subscription_liveness([a, b])

        assert result == (set(), set())
        assert len(requested) == 1, requested
        assert manager._conn("conn_a").liveness.status == "connected"
        assert manager._conn("conn_b").liveness.status == "connected"

    @pytest.mark.asyncio
    async def test_the_consumer_cache_lives_for_one_cycle_only(self, manager):
        """The cached answer covers a whole vhost and saves one request per conn_id
        inside a cycle. Kept across cycles it would hand every later check the consumer
        list of the first one, and a consumer that has since disappeared would go on
        vouching for itself forever."""
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        requested: list[str] = []
        manager._http_client = _mgmt_client(
            [_consumer_entry(entry.state.consumer_tag, "orders")], requested=requested
        )

        with _patch_mgmt_connection():
            await manager._check_subscription_liveness([sub])
            await manager._check_subscription_liveness([sub])

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
            result = await manager._check_subscription_liveness([sub])

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
            result = await manager._check_subscription_liveness([sub])

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
            result = await manager._check_subscription_liveness([sub])

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
            first = await manager._check_subscription_liveness([sub])
            assert first == (set(), set())
            assert manager._conn("rmq_default").liveness.status is None
            second = await manager._check_subscription_liveness([sub])

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
            await manager._check_subscription_liveness([sub])
            second = await manager._check_subscription_liveness([sub])
            third = await manager._check_subscription_liveness([sub])

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
            await manager._check_subscription_liveness([sub])
            assert manager._conn("rmq_default").stuck_cycles == 1

            # The task attaches, and the broker confirms it.
            entry.state._status = "listening"
            manager._http_client = _mgmt_client(
                [_consumer_entry(entry.state.consumer_tag, "orders")]
            )
            await manager._check_subscription_liveness([sub])
            assert manager._conn("rmq_default").stuck_cycles == 0

            # It drops back out of listening: the count starts from one again, so this
            # cycle alone cannot condemn the connection.
            entry.state._status = "connecting"
            result = await manager._check_subscription_liveness([sub])

        assert result == (set(), set())
        assert manager._conn("rmq_default").stuck_cycles == 1

    @pytest.mark.asyncio
    async def test_a_conn_id_with_no_running_task_is_not_green(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.task.done.return_value = True
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            result = await manager._check_subscription_liveness([sub])

        assert result == (set(), set())
        assert manager._conn("rmq_default").liveness.status == "error"

    @pytest.mark.asyncio
    async def test_finished_task_is_not_a_candidate(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.task.done.return_value = True
        manager._http_client = _mgmt_client([])

        with _patch_mgmt_connection():
            await manager._check_subscription_liveness([sub])
            result = await manager._check_subscription_liveness([sub])

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
            await manager._check_subscription_liveness([sub])
            condemned = await manager._check_subscription_liveness([sub])
            await manager._drop_connection("rmq_default")  # what reconcile does next
            held_back = await manager._check_subscription_liveness([sub])

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
            await manager._check_subscription_liveness([sub])
            await manager._check_subscription_liveness([sub])
            await manager._drop_connection("rmq_default")
            for _ in range(_CYCLES_BEFORE_REDROP - 1):
                assert await manager._check_subscription_liveness([sub]) == (set(), set())
            again = await manager._check_subscription_liveness([sub])

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
            await manager._check_subscription_liveness([sub])
            await manager._drop_connection("rmq_default", role=_ROLE_PUBLISH)
            result = await manager._check_subscription_liveness([sub])

        assert result == ({7}, {"rmq_default"})


class TestFireTaskLiveness:
    @pytest.mark.asyncio
    async def test_fire_task_pausing_after_an_error_is_not_a_candidate(self, manager):
        sub = _sub(id=7, queue_name="orders", cooldown=300)
        _register_active(manager, sub)
        fire = _register_fire(manager, status="error")
        manager._http_client = _mgmt_client([_consumer_entry(_consumer_tag(7), "orders")])

        with _patch_mgmt_connection():
            await manager._check_subscription_liveness([sub])
            result = await manager._check_subscription_liveness([sub])

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
            await manager._check_subscription_liveness([sub])
            result = await manager._check_subscription_liveness([sub])

        assert result == (set(), set())
        assert manager._fire_needs_restart is True
        assert manager._active[7].negative_checks == 0
        # The broker holds no fire consumer on this conn_id, so the row must not claim
        # otherwise while the restart is arranged.
        assert manager._conn("rmq_default").liveness.status == "error"
        assert "fire consumer" in manager._conn("rmq_default").liveness.reason

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
            result = await manager._check_subscription_liveness([sub])

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
            first = await manager._check_subscription_liveness([sub])
            second = await manager._check_subscription_liveness([sub])

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

        await manager._check_subscription_liveness([sub])
        result = await manager._check_subscription_liveness([sub])

        assert result == ({7}, {"rmq_default"})

    @pytest.mark.asyncio
    async def test_successful_passive_declare_keeps_the_subscription(self, manager):
        sub = _sub(id=7, queue_name="orders")
        entry = _register_active(manager, sub)
        entry.negative_checks = 1
        manager._http_client = None
        _fast_timeouts(manager)
        channel = _make_live_channel(queue=MagicMock())
        manager._conn("rmq_default").connections[_ROLE_CONSUME] = _make_live_connection(channel=channel)

        result = await manager._check_subscription_liveness([sub])

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
            result = await manager._check_subscription_liveness([sub])

            manager._active[7].task.cancel()
            await asyncio.gather(manager._active[7].task, return_exceptions=True)

        assert result == (set(), set())
        assert channel.declare_queue.await_count == 1


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
             patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, side_effect=[zombie, fresh]) as connect:
            await manager.reconcile([sub])
            await _wait_for_status(manager._active[7], "listening")
            first_task = manager._active[7].task

            await manager.reconcile([sub])  # first negative check
            assert manager._active[7].task is first_task

            await manager.reconcile([sub])  # second negative check → rebuild
            await _wait_for_status(manager._active[7], "listening")

            assert connect.await_count == 2
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
             patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, return_value=connection) as connect:
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
            assert connect.await_count == 1
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
            await manager._check_subscription_liveness([dead, alive])
            to_restart, to_recreate = await manager._check_subscription_liveness(
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
        with patch.object(manager, "_check_subscription_liveness", new_callable=AsyncMock,
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
             patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, side_effect=ConnectionError("refused")), \
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
             patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, side_effect=ConnectionError("refused")), \
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
            cycle_calls.append((fn.__name__, args))

        async def consumer_handler(fn, *args):
            consumer_calls.append((fn.__name__, args))

        manager._cycle_executor = _FakeExecutor(cycle_handler)
        manager._executor = _FakeExecutor(consumer_handler)

        await manager.reconcile([])   # the subscription is gone — mark it disconnected
        assert any(
            name == "_write_consumer_status" and args[:3] == (7, "disconnected", None)
            for name, args in cycle_calls
        ), cycle_calls

        # A cycle with subscriptions on it writes the status row of every conn_id, and
        # that write belongs in the cycle pool as well.
        cycle_calls.clear()
        still_there = _sub(id=8)
        _register_active(manager, still_there, real_task=True)
        with patch.object(manager, "_recover_dead_consumers", new_callable=AsyncMock):
            await manager.reconcile([still_there])

        assert any(name == "_write_conn_status_rows" for name, _ in cycle_calls), cycle_calls
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
                 patch(
                     "airflow_provider_rmq.watcher.consumer._write_consumer_status",
                     lambda sub_id, status, last_error, order, generation: (
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

        with patch.object(
            manager, "_get_or_create_connection", return_value=connection
        ), patch(
            "airflow_provider_rmq.watcher.consumer._ConsumerState.write"
        ), patch(
            "airflow_provider_rmq.watcher.consumer._RECONNECT_DELAY", 0.01
        ), patch(
            "airflow_provider_rmq.watcher.consumer.incr"
        ) as incr:
            await _run_then_cancel(
                manager._consume_subscription(_sub()), timeout=0.1
            )

        attaches = [
            c for c in incr.call_args_list if c == call("rmq_watcher.consumer_attached")
        ]
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
             patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, side_effect=[zombie, fresh]) as connect:
            await manager.reconcile([sub])
            await _wait_for_status(manager._active[7], "listening")
            silent_task = manager._active[7].task

            await manager.reconcile([sub])  # one negative check changes nothing
            assert manager._active[7].task is silent_task
            assert not acked.is_set()

            await manager.reconcile([sub])  # second negative check → rebuild
            await asyncio.wait_for(acked.wait(), timeout=2.0)

            assert connect.await_count == 2
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

        await manager._check_subscription_liveness([sub])
        await manager._check_subscription_liveness([sub])
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

        with patch("airflow_provider_rmq.watcher.consumer.aio_pika.connect_robust",
                   new_callable=AsyncMock, return_value=fresh_publish), \
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
            await manager._check_subscription_liveness([sub])
            result = await manager._check_subscription_liveness([sub])

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

    def test_a_write_that_a_newer_one_overtook_is_dropped(self):
        """The lock alone only orders writes that reach it in order. The number each
        write takes before it is submitted is what settles the other case: the newer
        write got there first, so the older one has nothing left to say."""
        order = _StatusOrder()
        first, second = order.issue(), order.issue()
        landed = []

        assert order.run(second, lambda: landed.append("error")) is True
        assert order.run(first, lambda: landed.append("listening")) is False
        assert landed == ["error"]

    def test_writes_that_arrive_in_order_all_land(self):
        order = _StatusOrder()
        landed = []

        for status in ("connecting", "listening", "error"):
            assert order.run(order.issue(), lambda s=status: landed.append(s)) is True

        assert landed == ["connecting", "listening", "error"]

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
             patch("airflow_provider_rmq.watcher.consumer._write_consumer_status"):
            await _run_then_cancel(manager._consume_subscription(sub), timeout=0.5)

        assert state.consumer_tag is None
        assert state.status != "listening"
        channel.close.assert_awaited()

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

        with patch.object(
            manager, "_trigger_fire_dag", side_effect=RuntimeError("scheduler is down")
        ), pytest.raises(RuntimeError):
            await manager._handle_fire_delivery(msg)

        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_triggered_fire_event_is_acknowledged(self, manager):
        msg = _make_fake_message(b"", message_id="m1")
        msg.routing_key = "my_dag"

        with patch.object(
            manager, "_trigger_fire_dag", return_value=_OUTCOME_TRIGGERED
        ):
            await manager._handle_fire_delivery(msg)

        msg.ack.assert_awaited_once()
        msg.nack.assert_not_awaited()


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
        manager._abandon(set())
        assert manager._abandoned == set(), "a task that finished is let go"
