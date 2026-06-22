from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, call, patch

import aio_pika
import aio_pika.exceptions
import httpx
import pytest

from airflow_provider_rmq.watcher.consumer import (
    RMQConsumerManager,
    _ActiveSub,
    _ConsumerState,
    _RECONNECT_DELAY,
    _build_run_id,
    _sync_trigger,
    _ensure_fire_infrastructure,
    _ensure_pending_queue,
    _ensure_exchange_infrastructure,
    _ensure_sub_queue,
    _sync_bindings,
    _FIRE_EXCHANGE,
    _FIRE_QUEUE,
    _PENDING_QUEUE_PREFIX,
    _SUB_QUEUE_PREFIX,
    _EXCHANGE_TTL_MS,
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


# ---------------------------------------------------------------------------
# Tests for C5 (binary body) and C3 (status reset on reconcile removal)
# ---------------------------------------------------------------------------

class TestTriggerDagBinaryBody:
    @pytest.mark.asyncio
    async def test_binary_body_does_not_raise(self):
        """C5: невалидный UTF-8 в теле сообщения не должен бросать исключение."""
        manager = RMQConsumerManager()
        msg = _make_fake_message(body=b"\xff\xfe invalid utf-8")

        captured_conf = {}

        async def mock_executor(loop_or_none, func, *args):
            captured_conf.update(args[1])  # conf is 2nd positional arg to _sync_trigger

        with patch("airflow_provider_rmq.watcher.consumer.WatcherSession",
                   return_value=_mock_session()[0]), \
             patch("asyncio.AbstractEventLoop.run_in_executor",
                   side_effect=mock_executor):
            # Патчим loop.run_in_executor через patch объекта
            loop = asyncio.get_running_loop()
            original = loop.run_in_executor
            conf_result = {}

            async def capture_executor(executor, func, *args):
                conf_result.update(args[1])  # conf dict
                return None

            loop.run_in_executor = capture_executor
            try:
                await manager._trigger_dag("dag", "q", 1, msg)
            finally:
                loop.run_in_executor = original

        assert isinstance(conf_result.get("body"), str)

    @pytest.mark.asyncio
    async def test_binary_body_replaced_chars(self):
        """C5: невалидные байты заменяются replacement char, результат — строка."""
        manager = RMQConsumerManager()
        msg = _make_fake_message(body=b"\xff\xfe")

        loop = asyncio.get_running_loop()
        conf_result = {}

        async def capture_executor(executor, func, *args):
            conf_result.update(args[1])
            return None

        loop.run_in_executor = capture_executor
        try:
            await manager._trigger_dag("dag", "q", 1, msg)
        finally:
            del loop.run_in_executor

        assert isinstance(conf_result["body"], str)
        assert "�" in conf_result["body"]


class TestReconcileStatusReset:
    @pytest.mark.asyncio
    async def test_reconcile_sets_disconnected_on_removal(self):
        """C3: при удалении подписки из reconcile статус должен сбрасываться в disconnected."""
        manager = RMQConsumerManager()

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

        await _ensure_fire_infrastructure(channel)

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

        await _ensure_fire_infrastructure(channel)

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

        await _ensure_fire_infrastructure(channel)
        await _ensure_fire_infrastructure(channel)

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
        await _ensure_pending_queue(channel, dag_id)

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

        await _ensure_pending_queue(channel, "my_dag")

        # Confirm that iterator/consume was never called on the queue
        queue_mock.iterator.assert_not_called()
        queue_mock.consume.assert_not_called()

    @pytest.mark.asyncio
    async def test_queue_name_contains_dag_id(self):
        """Pending queue name must include the dag_id."""
        channel = AsyncMock()
        queue_mock = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue_mock)

        await _ensure_pending_queue(channel, "special_dag_123")

        call_args = channel.declare_queue.call_args
        assert "special_dag_123" in call_args[0][0]


class TestSubsChanged:
    def test_no_change_returns_false(self):
        manager = RMQConsumerManager()
        sub = _sub(id=1, cooldown=300, filter_data={"k": "v"}, conn_id="c1")
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=sub.copy())
        assert manager._subs_changed(1, sub) is False

    def test_cooldown_change_returns_true(self):
        manager = RMQConsumerManager()
        old_sub = _sub(id=1, cooldown=300)
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy())
        new_sub = _sub(id=1, cooldown=600)
        assert manager._subs_changed(1, new_sub) is True

    def test_filter_data_change_returns_true(self):
        manager = RMQConsumerManager()
        old_sub = _sub(id=1, filter_data={"type": "order"})
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy())
        new_sub = _sub(id=1, filter_data={"type": "payment"})
        assert manager._subs_changed(1, new_sub) is True

    def test_conn_id_change_returns_true(self):
        manager = RMQConsumerManager()
        old_sub = _sub(id=1, conn_id="conn_a")
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy())
        new_sub = _sub(id=1, conn_id="conn_b")
        assert manager._subs_changed(1, new_sub) is True

    def test_missing_sub_id_returns_true(self):
        manager = RMQConsumerManager()
        assert manager._subs_changed(999, _sub(id=999)) is True

    def test_exchange_and_routing_keys_change_does_not_restart_task(self):
        """Changing exchange=/routing_keys= with queue_name/dag_id/cooldown/filter_data/conn_id
        unchanged must NOT be treated as a change — only bind-diff in
        _provision_exchange_subs reacts to it, the consumer task/queue stay the same."""
        manager = RMQConsumerManager()
        old_sub = _exchange_sub(id=1, exchange="jetstat.airflow", routing_keys=["a.succeeded"])
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy())

        new_sub = _exchange_sub(id=1, exchange="jetstat.airflow", routing_keys=["b.failed", "c.*"])
        assert manager._subs_changed(1, new_sub) is False

    def test_exchange_name_change_does_not_restart_task(self):
        """Changing exchange= itself (queue_name/dag_id/cooldown/filter_data/conn_id unchanged)
        also does not restart the task — same rationale as routing_keys change."""
        manager = RMQConsumerManager()
        old_sub = _exchange_sub(id=1, exchange="jetstat.airflow")
        manager._active[1] = _ActiveSub(task=MagicMock(), sub=old_sub.copy())

        new_sub = _exchange_sub(id=1, exchange="some.other.exchange")
        assert manager._subs_changed(1, new_sub) is False


class TestHotReload:
    @pytest.mark.asyncio
    async def test_cooldown_change_restarts_task(self):
        """reconcile: sub with changed cooldown causes task to be cancelled and restarted."""
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()

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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()

        async def bad_get_conn(conn_id):
            raise ConnectionError("broker unavailable")

        with patch.object(manager, "_get_or_create_connection", side_effect=bad_get_conn):
            # Must not raise — errors are logged and swallowed
            await manager._provision_cooldown({"my_dag"}, "rmq_default")

    @pytest.mark.asyncio
    async def test_provision_cooldown_creates_fire_infra_and_pending(self):
        """_provision_cooldown calls _ensure_fire_infrastructure and _ensure_pending_queue."""
        manager = RMQConsumerManager()

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
        manager = RMQConsumerManager()
        manager._cooldown_tracker.mark_provisioned({"orphaned_dag"})

        with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            manager._check_orphaned_pending_queues({"active_dag"})
            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            assert any("orphaned_dag" in m for m in warning_messages)

    def test_no_duplicate_orphan_warning(self):
        """_check_orphaned_pending_queues only warns once per new orphan dag_id."""
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()

        async def blocking_consume(sub):
            await asyncio.Future()

        # First reconcile: add one cooldown subscription to populate _cooldown_dag_ids
        connection = AsyncMock()
        setup_channel = AsyncMock()
        setup_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
        setup_channel.declare_queue = AsyncMock(return_value=AsyncMock())
        connection.channel = AsyncMock(return_value=setup_channel)
        manager._connections["rmq_default"] = connection

        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue", side_effect=lambda c: asyncio.Future()), \
             patch.object(manager, "_update_all_conn_counts"), \
             patch.object(manager, "_get_or_create_connection", return_value=connection):
            await manager.reconcile([_sub(id=1, cooldown=300)])
            await asyncio.sleep(0)
            # _cooldown_tracker should now have "test_dag" provisioned (default dag_id in _sub())
            assert "test_dag" in manager._cooldown_tracker._provisioned

        # Cancel running tasks for clean state
        await manager.stop()
        manager._connections["rmq_default"] = connection

        # Second reconcile: remove ALL cooldown subscriptions (empty list)
        with patch.object(manager, "_consume_subscription", side_effect=blocking_consume), \
             patch.object(manager, "_consume_fire_queue", side_effect=lambda c: asyncio.Future()), \
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
        """reconcile() fires orphan WARNING for a removed dag even when RMQ is down.

        Regression test: previously _check_orphaned_pending_queues was called inside
        _provision_cooldown after a potential early return. If RMQ was unavailable,
        _provision_cooldown returned early and orphan detection was silently skipped.
        """
        manager = RMQConsumerManager()

        async def blocking_consume(sub):
            await asyncio.Future()

        # Simulate that dag_a and dag_b were previously provisioned
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
        """Return (channel_mock, connection_mock) where channel has default_exchange."""
        queue = _make_push_queue(messages)
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue)
        channel.default_exchange = AsyncMock()
        channel.default_exchange.publish = AsyncMock()
        connection = AsyncMock()
        connection.channel = AsyncMock(return_value=channel)
        return channel, connection

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
        manager = RMQConsumerManager()
        msg = _make_fake_message(b"hello")

        captured_conf = {}
        loop = asyncio.get_running_loop()
        original_run = loop.run_in_executor

        async def capture_executor(executor, func, *args):
            # args = (dag_id, conf, run_id)
            if len(args) >= 2 and isinstance(args[1], dict):
                captured_conf.update(args[1])
            return None

        loop.run_in_executor = capture_executor
        try:
            await manager._trigger_dag("my_dag", "my_queue", 1, msg)
        finally:
            loop.run_in_executor = original_run

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

    def _make_connection_with_queue(self, messages: list):
        """Return connection mock whose channel yields the given messages from rmq_watcher.fire."""
        queue = _make_push_queue(messages)
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue)
        connection = AsyncMock()
        connection.channel = AsyncMock(return_value=channel)
        return connection

    @pytest.mark.asyncio
    async def test_fire_consumer_triggers_dag_with_routing_key(self):
        """_consume_fire_queue triggers _sync_trigger with dag_id from routing_key."""
        manager = RMQConsumerManager()
        msg = _make_fire_message(routing_key="orders_dag", message_id="abc-123")
        connection = self._make_connection_with_queue([msg])

        triggered_calls = []
        triggered = asyncio.Event()

        async def mock_executor(executor, func, *args):
            # args = (dag_id, conf, run_id) for _sync_trigger
            triggered_calls.append(args)
            triggered.set()
            return None

        loop = asyncio.get_running_loop()
        original_run = loop.run_in_executor
        loop.run_in_executor = mock_executor
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection))
            await asyncio.wait_for(triggered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            loop.run_in_executor = original_run

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
        manager = RMQConsumerManager()
        msg = _make_fire_message(routing_key="my_dag", message_id="fixed-uuid-42")
        connection = self._make_connection_with_queue([msg])

        captured_run_id = {}
        triggered = asyncio.Event()

        async def mock_executor(executor, func, *args):
            captured_run_id["run_id"] = args[2]
            triggered.set()
            return None

        loop = asyncio.get_running_loop()
        original_run = loop.run_in_executor
        loop.run_in_executor = mock_executor
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection))
            await asyncio.wait_for(triggered.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            loop.run_in_executor = original_run

        assert captured_run_id["run_id"] == "rmq_cooldown__my_dag__fixed-uuid-42"

    @pytest.mark.asyncio
    async def test_fire_consumer_acks_after_trigger(self):
        """_consume_fire_queue ACKs the message after successful _sync_trigger."""
        manager = RMQConsumerManager()
        msg = _make_fire_message(routing_key="my_dag")
        connection = self._make_connection_with_queue([msg])

        acked = asyncio.Event()
        original_ack = msg.ack

        async def capture_ack(*args, **kwargs):
            acked.set()

        msg.ack = capture_ack

        async def mock_executor(executor, func, *args):
            return None

        loop = asyncio.get_running_loop()
        original_run = loop.run_in_executor
        loop.run_in_executor = mock_executor
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection))
            await asyncio.wait_for(acked.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            loop.run_in_executor = original_run

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

        manager = RMQConsumerManager()
        msg = _make_fire_message(routing_key="my_dag", message_id="dup-uuid")
        connection = self._make_connection_with_queue([msg])

        acked = asyncio.Event()

        async def capture_ack(*args, **kwargs):
            acked.set()

        msg.ack = capture_ack

        # _sync_trigger is called inside run_in_executor; simulate it raising IntegrityError
        def sync_trigger_raises_integrity(dag_id, conf, run_id):
            raise SaIntegrityError("dup run_id", {}, None)

        async def mock_executor(executor, func, *args):
            # func is _sync_trigger; call it synchronously to trigger the IntegrityError path
            try:
                func(*args)
            except SaIntegrityError:
                pass  # _sync_trigger already handles IntegrityError internally

        loop = asyncio.get_running_loop()
        original_run = loop.run_in_executor
        loop.run_in_executor = mock_executor
        try:
            with patch("airflow_provider_rmq.watcher.consumer._sync_trigger",
                       side_effect=sync_trigger_raises_integrity):
                task = asyncio.create_task(manager._consume_fire_queue(connection))
                await asyncio.wait_for(acked.wait(), timeout=2.0)
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        finally:
            loop.run_in_executor = original_run

        assert acked.is_set()

    @pytest.mark.asyncio
    async def test_fire_consumer_generic_exception_retries(self):
        """Generic Exception in fire consumer → logs warning and retries (no exit)."""
        manager = RMQConsumerManager()
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
            task = asyncio.create_task(manager._consume_fire_queue(connection))
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
        manager = RMQConsumerManager()
        msg = _make_fire_message(routing_key="", message_id="no-rk")
        connection = self._make_connection_with_queue([msg])

        trigger_called = asyncio.Event()
        acked = asyncio.Event()

        original_ack = msg.ack

        async def capture_ack(*args, **kwargs):
            acked.set()

        msg.ack = capture_ack

        async def fail_if_called(executor, func, *args):
            trigger_called.set()
            return None

        loop = asyncio.get_running_loop()
        original_run = loop.run_in_executor
        loop.run_in_executor = fail_if_called
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection))
            await asyncio.wait_for(acked.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            loop.run_in_executor = original_run

        assert acked.is_set()
        assert not trigger_called.is_set()

    @pytest.mark.asyncio
    async def test_fire_consumer_skips_message_with_missing_message_id(self):
        """Message with no message_id is ACKed and skipped (no trigger_dag) — idempotency guard."""
        manager = RMQConsumerManager()
        msg = _make_fire_message(routing_key="my_dag", message_id=None)
        connection = self._make_connection_with_queue([msg])

        trigger_called = asyncio.Event()
        acked = asyncio.Event()

        async def capture_ack(*args, **kwargs):
            acked.set()

        msg.ack = capture_ack

        async def fail_if_called(executor, func, *args):
            trigger_called.set()
            return None

        loop = asyncio.get_running_loop()
        original_run = loop.run_in_executor
        loop.run_in_executor = fail_if_called
        try:
            task = asyncio.create_task(manager._consume_fire_queue(connection))
            await asyncio.wait_for(acked.wait(), timeout=2.0)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        finally:
            loop.run_in_executor = original_run

        assert acked.is_set()
        assert not trigger_called.is_set()

    @pytest.mark.asyncio
    async def test_fire_consumer_channel_not_found_exits(self):
        """ChannelNotFoundEntity → fire consumer exits (fatal, no retry)."""
        manager = RMQConsumerManager()
        connection = AsyncMock()
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(
            side_effect=aio_pika.exceptions.ChannelNotFoundEntity("no such queue")
        )
        connection.channel = AsyncMock(return_value=channel)

        task = asyncio.create_task(manager._consume_fire_queue(connection))
        # Task should exit on its own — fatal error, no retry
        await asyncio.wait_for(task, timeout=2.0)

        assert task.done()
        assert not task.cancelled()

    @pytest.mark.asyncio
    async def test_fire_consumer_channel_closed_retries(self):
        """ChannelClosed → fire consumer retries after delay."""
        manager = RMQConsumerManager()
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
            task = asyncio.create_task(manager._consume_fire_queue(connection))
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
        manager = RMQConsumerManager()

        async def blocking_consume(sub):
            await asyncio.Future()

        async def blocking_fire(conn):
            await asyncio.Future()

        connection = AsyncMock()

        # Pre-populate _connections so reconcile can find it after _provision_cooldown
        manager._connections["rmq_default"] = connection

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
        manager = RMQConsumerManager()

        async def blocking_consume(sub):
            await asyncio.Future()

        async def blocking_fire(conn):
            await asyncio.Future()

        connection = AsyncMock()

        # Pre-populate _connections so reconcile can find it after _provision_cooldown
        manager._connections["rmq_default"] = connection

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
        manager = RMQConsumerManager()

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
            assert any("rmq_default" in m and "not available" in m for m in warning_messages), (
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

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow")

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

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow")

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

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow")

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

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow")

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

        await _ensure_exchange_infrastructure(channel, "jetstat.airflow")
        await _ensure_exchange_infrastructure(channel, "jetstat.airflow")

        assert channel.declare_exchange.call_count == 4
        assert channel.declare_queue.call_count == 4


class TestEnsureSubQueue:
    @pytest.mark.asyncio
    async def test_declares_queue_with_ttl_argument(self):
        """_ensure_sub_queue declares rmq_watcher.sub.{dag_id} with x-message-ttl."""
        channel = AsyncMock()
        queue_mock = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=queue_mock)

        result = await _ensure_sub_queue(channel, "my_dag")

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

        await _ensure_sub_queue(channel, "special_dag_123")

        call_args = channel.declare_queue.call_args
        assert "special_dag_123" in call_args[0][0]
        assert call_args[0][0] == f"{_SUB_QUEUE_PREFIX}special_dag_123"

    @pytest.mark.asyncio
    async def test_idempotent_no_exception_on_second_call(self):
        channel = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=AsyncMock())

        await _ensure_sub_queue(channel, "my_dag")
        await _ensure_sub_queue(channel, "my_dag")

        assert channel.declare_queue.call_count == 2


class TestPreconditionFailedRecognition:
    @pytest.mark.asyncio
    async def test_precondition_failed_logged_distinctly_and_does_not_break_other_groups(self):
        """A ChannelPreconditionFailed from declare_exchange (conflicting exchange properties)
        is logged as a distinct conflict, and other groups still get provisioned."""
        manager = RMQConsumerManager()
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

        async def fake_get_conn(conn_id):
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
        manager = RMQConsumerManager()
        manager._exchange_tracker.mark_provisioned({"orphaned_dag"})

        with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            manager._check_orphaned_exchange_bindings({"active_dag"})
            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            assert any("orphaned_dag" in m for m in warning_messages)

    def test_no_duplicate_warning_on_repeated_cycles(self):
        manager = RMQConsumerManager()
        manager._exchange_tracker.mark_provisioned({"orphaned_dag"})
        manager._check_orphaned_exchange_bindings({"active_dag"})  # first warning

        with patch("airflow_provider_rmq.watcher.consumer.log") as mock_log:
            manager._check_orphaned_exchange_bindings({"active_dag"})
            warning_messages = [str(c) for c in mock_log.warning.call_args_list]
            assert not any("orphaned_dag" in m for m in warning_messages)

    def test_info_logged_when_subscription_restored(self):
        manager = RMQConsumerManager()
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

        await _sync_bindings(queue, "jetstat.airflow", {"a.succeeded", "b.failed"}, {"a.succeeded"})

        queue.bind.assert_called_once_with("jetstat.airflow", routing_key="b.failed")
        queue.unbind.assert_not_called()

    @pytest.mark.asyncio
    async def test_unbinds_only_stale_keys(self):
        queue = AsyncMock()
        queue.name = "rmq_watcher.sub.my_dag"

        await _sync_bindings(queue, "jetstat.airflow", {"a.succeeded"}, {"a.succeeded", "old.key"})

        queue.unbind.assert_called_once_with("jetstat.airflow", routing_key="old.key")
        queue.bind.assert_not_called()

    @pytest.mark.asyncio
    async def test_noop_when_desired_equals_current(self):
        queue = AsyncMock()
        queue.name = "rmq_watcher.sub.my_dag"

        await _sync_bindings(queue, "jetstat.airflow", {"a.succeeded"}, {"a.succeeded"})

        queue.bind.assert_not_called()
        queue.unbind.assert_not_called()

    @pytest.mark.asyncio
    async def test_binds_and_unbinds_together(self):
        queue = AsyncMock()
        queue.name = "rmq_watcher.sub.my_dag"

        await _sync_bindings(
            queue, "jetstat.airflow",
            desired={"new.key"}, current={"old.key"},
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
        manager._http_client = AsyncMock()

        conn_info = self._conn_info()

        connection_a, setup_channel_a, queue_a = self._make_setup()
        connection_b, setup_channel_b, queue_b = self._make_setup()

        async def fake_get_conn(conn_id):
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

        error_messages = [str(c) for c in mock_log.error.call_args_list]
        assert any("dag_a" in m for m in error_messages)
        # dag_a's bind-diff failed, but the queue is still declared (group is "provisioned")
        assert "dag_a" in manager._exchange_tracker._provisioned
        # dag_b's group is entirely unaffected by dag_a's Management API error
        assert "dag_b" in manager._exchange_tracker._provisioned
        queue_b.bind.assert_not_called()  # desired == current == empty set

    @pytest.mark.asyncio
    async def test_get_connection_called_via_run_in_executor(self):
        """BaseHook.get_connection must be called through run_in_executor, not directly
        in the coroutine — verified the same way as _get_or_create_connection tests."""
        manager = RMQConsumerManager()
        manager._http_client = AsyncMock()
        connection, setup_channel, queue_mock = self._make_setup()
        conn_info = self._conn_info()

        executor_calls = []

        async def capture_executor(executor, func, *args):
            executor_calls.append((func, args))
            return func(*args)

        loop = asyncio.get_running_loop()
        original_run = loop.run_in_executor
        loop.run_in_executor = capture_executor
        try:
            with patch.object(manager, "_get_or_create_connection", return_value=connection), \
                 patch("airflow_provider_rmq.watcher.consumer.BaseHook.get_connection",
                       return_value=conn_info) as mock_get_connection, \
                 patch("airflow_provider_rmq.watcher.consumer.get_current_bindings",
                       new=AsyncMock(return_value=set())):
                await manager._provision_exchange_subs([
                    _exchange_sub(id=1, dag_id="test_dag", exchange="jetstat.airflow"),
                ])
        finally:
            loop.run_in_executor = original_run

        assert any(func is mock_get_connection for func, _ in executor_calls), (
            "BaseHook.get_connection must be invoked via loop.run_in_executor"
        )

    @pytest.mark.asyncio
    async def test_empty_exchange_subs_is_noop(self):
        manager = RMQConsumerManager()
        manager._http_client = AsyncMock()

        with patch.object(manager, "_get_or_create_connection") as mock_get_conn:
            await manager._provision_exchange_subs([])

        mock_get_conn.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_http_client_logs_error_and_does_not_raise(self):
        """If start() was never called, self._http_client is None — must not crash,
        just log ERROR and skip provisioning for this cycle."""
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
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
        manager = RMQConsumerManager()
        assert manager._http_client is None
        await manager.start()
        try:
            assert manager._http_client is not None
            assert isinstance(manager._http_client, httpx.AsyncClient)
        finally:
            await manager.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_http_client(self):
        manager = RMQConsumerManager()
        await manager.start()
        client = manager._http_client
        with patch.object(client, "aclose", new=AsyncMock()) as mock_aclose:
            await manager.stop()
        mock_aclose.assert_awaited_once()
        assert manager._http_client is None
