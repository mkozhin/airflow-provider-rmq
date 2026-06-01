from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, call, patch

import aio_pika.exceptions
import pytest

from airflow_provider_rmq.watcher.consumer import (
    RMQConsumerManager,
    _ConsumerState,
    _RECONNECT_DELAY,
    _sync_trigger,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_fake_message(body: bytes = b"hello", headers: dict | None = None):
    msg = MagicMock()
    msg.body = body
    msg.headers = headers or {}
    msg.routing_key = "rk"
    msg.exchange = ""
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    return msg


class _QueueIterCtx:
    """Async context manager that yields `messages` then blocks until cancelled."""

    def __init__(self, messages: list):
        self._messages = messages

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

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
        await asyncio.Future()  # block until cancelled → raises CancelledError


def _make_push_queue(messages: list = ()):
    queue = MagicMock()
    queue.iterator.return_value = _QueueIterCtx(list(messages))
    return queue


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
) -> dict:
    return {
        "id": id,
        "dag_id": dag_id,
        "queue_name": queue_name,
        "conn_id": conn_id,
        "filter_data": filter_data or {},
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


class TestSyncTrigger:
    def test_trigger_dag_uses_watcher_session(self):
        fake_dag = MagicMock()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=fake_dag)
        with ws_patch as mock_ws, td_patch:
            _sync_trigger("my_dag", {}, "run_id_1")
        mock_ws.assert_called()

    def test_trigger_dag_skips_inactive_dag(self):
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=None)
        with ws_patch, td_patch as mock_td:
            _sync_trigger("missing_dag", {}, "run_id")
        mock_td.assert_not_called()

    def test_trigger_dag_skips_paused_dag(self):
        # filter_by includes is_paused=False; paused DAGs return None from .first()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=None)
        with ws_patch, td_patch as mock_td:
            _sync_trigger("paused_dag", {}, "run_id")
        mock_td.assert_not_called()

    def test_trigger_dag_handles_integrity_error(self):
        from sqlalchemy.exc import IntegrityError

        fake_dag = MagicMock()
        ws_patch, td_patch = _patch_sync_trigger_deps(dag_model=fake_dag)
        with ws_patch, td_patch as mock_td:
            mock_td.side_effect = IntegrityError("dup", {}, None)
            # Must not raise
            _sync_trigger("dag", {}, "run_id")


# ---------------------------------------------------------------------------
# Tests for _ConsumerState (in-memory status guard)
# ---------------------------------------------------------------------------

class TestConsumerState:
    def _make_state(self, mock_ws, mock_set):
        return _ConsumerState(sub_id=42)

    def test_state_guard_skips_duplicate_status_write(self):
        ctx, _ = _mock_session()
        with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status") as mock_set:
            state = _ConsumerState(sub_id=1)
            state.write("listening")
            state.write("listening")  # duplicate — should be skipped
            assert mock_set.call_count == 1

    def test_state_guard_writes_on_status_change(self):
        ctx, _ = _mock_session()
        with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status") as mock_set:
            state = _ConsumerState(sub_id=1)
            state.write("connecting")
            state.write("listening")
            state.write("error")
            assert mock_set.call_count == 3

    def test_last_error_cleared_on_successful_connect(self):
        writes = []
        ctx, _ = _mock_session()

        def capture(session, sub_id, status, last_error=None):
            writes.append((status, last_error))

        with patch("airflow_provider_rmq.watcher.consumer.WatcherSession", return_value=ctx), \
             patch("airflow_provider_rmq.watcher.consumer.set_consumer_status", side_effect=capture):
            state = _ConsumerState(sub_id=1)
            state._last_status = "error"  # simulate being in error state
            state.write("connecting")
            state.write("listening", last_error=None)

        assert ("listening", None) in writes


# ---------------------------------------------------------------------------
# Tests for RMQConsumerManager
# ---------------------------------------------------------------------------

@pytest.fixture
def manager():
    return RMQConsumerManager()


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
             patch.object(manager, "_update_all_conn_counts"):
            await manager.reconcile([_sub(id=1)])
            assert 1 in manager._tasks
            assert not manager._tasks[1].done()
            manager._tasks[1].cancel()
            await asyncio.gather(*manager._tasks.values(), return_exceptions=True)

    @pytest.mark.asyncio
    async def test_reconcile_cancels_removed_consumer(self, manager):
        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"):
            await manager.reconcile([_sub(id=1)])
            task = manager._tasks[1]
            assert not task.done()

            await manager.reconcile([])  # remove sub 1
            assert 1 not in manager._tasks
            assert task.done()

    @pytest.mark.asyncio
    async def test_stop_cancels_all_tasks(self, manager):
        async def blocking_consume(sub):
            await asyncio.Future()

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_update_all_conn_counts"):
            await manager.reconcile([_sub(id=1), _sub(id=2)])
            tasks = list(manager._tasks.values())
            assert all(not t.done() for t in tasks)

        await manager.stop()
        assert all(t.done() for t in tasks)


class TestConsumeSubscription:
    def _make_connection(self, queue):
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue)
        connection = AsyncMock()
        connection.channel = AsyncMock(return_value=channel)
        return connection

    @pytest.mark.asyncio
    async def test_matching_message_triggers_dag(self, manager):
        msg = _make_fake_message(b"order payload")
        queue = _make_push_queue([msg])
        connection = self._make_connection(queue)

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
        connection = self._make_connection(queue)

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
        connection = self._make_connection(queue)

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
        connection = self._make_connection(queue)

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

        def capture_write(self_arg, status, last_error=None):
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
