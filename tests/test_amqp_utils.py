from __future__ import annotations

import asyncio
import logging
import ssl
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow_provider_rmq.utils.amqp import (
    AMQP_PORT,
    AMQPS_PORT,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_HEARTBEAT,
    DEFAULT_RPC_TIMEOUT,
    call_with_timeout,
    match as _match,
    nack_and_sleep as _nack_and_sleep,
    build_amqp_connection,
    get_amqp_timeouts,
    match_and_ack,
)
from airflow_provider_rmq.utils.filters import MessageFilter
from tests.conftest import FakeAirflowConnection


# ---------------------------------------------------------------------------
# build_amqp_connection
# ---------------------------------------------------------------------------

class TestBuildAmqpConnection:
    def test_plain_url(self):
        conn = FakeAirflowConnection(host="rmq.local", port=None, login="user", password="pass", schema="/")
        url, ssl_ctx = build_amqp_connection(conn)
        assert url == f"amqp://user:pass@rmq.local:5672/%2F?heartbeat={DEFAULT_HEARTBEAT}"
        assert ssl_ctx is None

    def test_default_port_no_ssl(self):
        conn = FakeAirflowConnection(port=None)
        url, _ = build_amqp_connection(conn)
        assert f":{AMQP_PORT}/" in url

    def test_ssl_url_uses_amqps_scheme(self):
        conn = FakeAirflowConnection(port=None, extra='{"ssl_enabled": true}')
        with patch("airflow_provider_rmq.utils.amqp.build_ssl_context") as mock_ssl:
            mock_ssl.return_value = MagicMock(spec=ssl.SSLContext)
            url, ssl_ctx = build_amqp_connection(conn)
        assert url.startswith("amqps://")
        assert ssl_ctx is not None

    def test_ssl_url_uses_amqps_port(self):
        conn = FakeAirflowConnection(port=None, extra='{"ssl_enabled": true}')
        with patch("airflow_provider_rmq.utils.amqp.build_ssl_context") as mock_ssl:
            mock_ssl.return_value = MagicMock(spec=ssl.SSLContext)
            url, _ = build_amqp_connection(conn)
        assert f":{AMQPS_PORT}/" in url

    def test_custom_port(self):
        conn = FakeAirflowConnection(port=5700)
        url, _ = build_amqp_connection(conn)
        assert ":5700/" in url

    def test_login_url_encoding(self):
        conn = FakeAirflowConnection(login="user@domain", password="p@ss")
        url, _ = build_amqp_connection(conn)
        assert "user%40domain" in url
        assert "p%40ss" in url

    def test_vhost_url_encoding(self):
        conn = FakeAirflowConnection(schema="/app/v2")
        url, _ = build_amqp_connection(conn)
        assert "%2Fapp%2Fv2" in url

    def test_vhost_override(self):
        conn = FakeAirflowConnection(schema="/default")
        url, _ = build_amqp_connection(conn, vhost_override="/override")
        assert "%2Foverride" in url
        assert "%2Fdefault" not in url

    def test_default_vhost_when_schema_empty(self):
        conn = FakeAirflowConnection(schema="")
        url, _ = build_amqp_connection(conn)
        assert url.endswith(f"/%2F?heartbeat={DEFAULT_HEARTBEAT}")

    def test_returns_ssl_context_when_ssl_configured(self):
        conn = FakeAirflowConnection(extra='{"ssl_enabled": true}')
        with patch("airflow_provider_rmq.utils.amqp.build_ssl_context") as mock_ssl:
            fake_ctx = MagicMock(spec=ssl.SSLContext)
            mock_ssl.return_value = fake_ctx
            _, ssl_ctx = build_amqp_connection(conn)
        assert ssl_ctx is fake_ctx

    def test_returns_none_ssl_context_without_ssl(self):
        conn = FakeAirflowConnection()
        _, ssl_ctx = build_amqp_connection(conn)
        assert ssl_ctx is None


# ---------------------------------------------------------------------------
# heartbeat in the URL
# ---------------------------------------------------------------------------

class TestHeartbeatInUrl:
    def test_default_heartbeat_present(self):
        conn = FakeAirflowConnection()
        url, _ = build_amqp_connection(conn)
        assert url.endswith(f"?heartbeat={DEFAULT_HEARTBEAT}")

    def test_heartbeat_from_extra(self):
        conn = FakeAirflowConnection(extra='{"heartbeat": 90}')
        url, _ = build_amqp_connection(conn)
        assert url.endswith("?heartbeat=90")

    def test_heartbeat_from_extra_as_string(self):
        conn = FakeAirflowConnection(extra='{"heartbeat": "45"}')
        url, _ = build_amqp_connection(conn)
        assert url.endswith("?heartbeat=45")

    def test_scheme_credentials_port_and_vhost_unchanged(self):
        conn = FakeAirflowConnection(
            host="rmq.local", port=5700, login="user@domain", password="p@ss", schema="/app/v2",
            extra='{"heartbeat": 90}',
        )
        url, _ = build_amqp_connection(conn)
        assert url == "amqp://user%40domain:p%40ss@rmq.local:5700/%2Fapp%2Fv2?heartbeat=90"

    def test_credentials_and_vhost_escaping_kept_with_query(self):
        conn = FakeAirflowConnection(login="a b", password="p/w?x", schema="/v host")
        url, _ = build_amqp_connection(conn)
        assert "a%20b:p%2Fw%3Fx@" in url
        assert "/%2Fv%20host?heartbeat=" in url
        # the only "?" is the one starting the query string
        assert url.count("?") == 1

    def test_heartbeat_zero_kept_in_url(self):
        conn = FakeAirflowConnection(extra='{"heartbeat": 0}')
        url, _ = build_amqp_connection(conn)
        assert url.endswith("?heartbeat=0")

    def test_heartbeat_zero_logs_warning(self, caplog):
        conn = FakeAirflowConnection(extra='{"heartbeat": 0}')
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            build_amqp_connection(conn)
        assert any("heartbeat" in r.getMessage() for r in caplog.records)

    def test_garbage_heartbeat_falls_back_to_default(self, caplog):
        conn = FakeAirflowConnection(extra='{"heartbeat": "soon"}')
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            url, _ = build_amqp_connection(conn)
        assert url.endswith(f"?heartbeat={DEFAULT_HEARTBEAT}")
        assert caplog.records

    def test_negative_heartbeat_falls_back_to_default(self, caplog):
        conn = FakeAirflowConnection(extra='{"heartbeat": -5}')
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            url, _ = build_amqp_connection(conn)
        assert url.endswith(f"?heartbeat={DEFAULT_HEARTBEAT}")
        assert caplog.records

    def test_null_heartbeat_falls_back_to_default(self, caplog):
        conn = FakeAirflowConnection(extra='{"heartbeat": null}')
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            url, _ = build_amqp_connection(conn)
        assert url.endswith(f"?heartbeat={DEFAULT_HEARTBEAT}")
        assert caplog.records

    def test_ssl_url_keeps_query(self):
        conn = FakeAirflowConnection(port=None, extra='{"ssl_enabled": true, "heartbeat": 15}')
        with patch("airflow_provider_rmq.utils.amqp.build_ssl_context") as mock_ssl:
            mock_ssl.return_value = MagicMock(spec=ssl.SSLContext)
            url, _ = build_amqp_connection(conn)
        assert url.startswith("amqps://")
        assert f":{AMQPS_PORT}/%2F?heartbeat=15" in url


# ---------------------------------------------------------------------------
# get_amqp_timeouts
# ---------------------------------------------------------------------------

class TestGetAmqpTimeouts:
    def test_defaults_without_extra(self):
        timeouts = get_amqp_timeouts(FakeAirflowConnection())
        assert timeouts.connect == DEFAULT_CONNECT_TIMEOUT
        assert timeouts.rpc == DEFAULT_RPC_TIMEOUT

    def test_override_from_extra(self):
        conn = FakeAirflowConnection(extra='{"connect_timeout": 5, "rpc_timeout": 7.5}')
        timeouts = get_amqp_timeouts(conn)
        assert timeouts.connect == 5
        assert timeouts.rpc == 7.5

    def test_numeric_string_override(self):
        conn = FakeAirflowConnection(extra='{"connect_timeout": "5"}')
        assert get_amqp_timeouts(conn).connect == 5

    def test_non_numeric_string_falls_back(self, caplog):
        conn = FakeAirflowConnection(extra='{"connect_timeout": "fast"}')
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            timeouts = get_amqp_timeouts(conn)
        assert timeouts.connect == DEFAULT_CONNECT_TIMEOUT
        assert caplog.records

    def test_negative_value_falls_back(self, caplog):
        conn = FakeAirflowConnection(extra='{"rpc_timeout": -1}')
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            timeouts = get_amqp_timeouts(conn)
        assert timeouts.rpc == DEFAULT_RPC_TIMEOUT
        assert caplog.records

    def test_zero_value_falls_back(self, caplog):
        conn = FakeAirflowConnection(extra='{"rpc_timeout": 0}')
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            timeouts = get_amqp_timeouts(conn)
        assert timeouts.rpc == DEFAULT_RPC_TIMEOUT
        assert caplog.records

    def test_null_value_falls_back(self, caplog):
        conn = FakeAirflowConnection(extra='{"connect_timeout": null}')
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            timeouts = get_amqp_timeouts(conn)
        assert timeouts.connect == DEFAULT_CONNECT_TIMEOUT
        assert caplog.records

    def test_missing_key_does_not_warn(self, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            get_amqp_timeouts(FakeAirflowConnection())
        assert not caplog.records

    def test_one_bad_value_does_not_affect_the_other(self):
        conn = FakeAirflowConnection(extra='{"connect_timeout": "nope", "rpc_timeout": 3}')
        timeouts = get_amqp_timeouts(conn)
        assert timeouts.connect == DEFAULT_CONNECT_TIMEOUT
        assert timeouts.rpc == 3


# ---------------------------------------------------------------------------
# match_and_ack
# ---------------------------------------------------------------------------

def _make_aio_message(body: bytes = b"hello", headers: dict | None = None):
    msg = MagicMock()
    msg.body = body
    msg.headers = headers or {}
    msg.ack = AsyncMock()
    msg.nack = AsyncMock()
    return msg


class TestMatchAndAck:
    @pytest.mark.asyncio
    async def test_matching_message_acked(self):
        msg = _make_aio_message(headers={"type": "order"})
        f = MessageFilter(filter_headers={"type": "order"})
        result = await match_and_ack(msg, f)
        assert result is True
        msg.ack.assert_awaited_once()
        msg.nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_matching_message_nacked(self):
        msg = _make_aio_message(headers={"type": "payment"})
        f = MessageFilter(filter_headers={"type": "order"})
        result = await match_and_ack(msg, f)
        assert result is False
        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_filter_always_matches(self):
        msg = _make_aio_message()
        f = MessageFilter()
        result = await match_and_ack(msg, f)
        assert result is True
        msg.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_nack_includes_sleep(self):
        msg = _make_aio_message(headers={"type": "other"})
        f = MessageFilter(filter_headers={"type": "order"})
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await match_and_ack(msg, f)
        mock_sleep.assert_awaited_once_with(0.1)

    @pytest.mark.asyncio
    async def test_match_no_sleep(self):
        msg = _make_aio_message(headers={"type": "order"})
        f = MessageFilter(filter_headers={"type": "order"})
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await match_and_ack(msg, f)
        mock_sleep.assert_not_awaited()


# ---------------------------------------------------------------------------
# _match (pure predicate, no side effects)
# ---------------------------------------------------------------------------

class TestMatch:
    def test_match_returns_true_when_no_filters(self):
        msg = _make_aio_message(body=b"any")
        result = _match(msg, MessageFilter())
        assert result is True

    def test_match_returns_true_when_headers_match(self):
        msg = _make_aio_message(headers={"type": "order"})
        result = _match(msg, MessageFilter(filter_headers={"type": "order"}))
        assert result is True

    def test_match_returns_false_when_headers_differ(self):
        msg = _make_aio_message(headers={"type": "payment"})
        result = _match(msg, MessageFilter(filter_headers={"type": "order"}))
        assert result is False

    def test_match_does_not_call_ack(self):
        msg = _make_aio_message(headers={"type": "order"})
        _match(msg, MessageFilter(filter_headers={"type": "order"}))
        msg.ack.assert_not_called()
        msg.nack.assert_not_called()

    def test_match_does_not_call_nack_on_miss(self):
        msg = _make_aio_message(headers={"type": "other"})
        _match(msg, MessageFilter(filter_headers={"type": "order"}))
        msg.nack.assert_not_called()
        msg.ack.assert_not_called()

    def test_match_binary_body_does_not_raise(self):
        """Non-UTF-8 binary body must not raise UnicodeDecodeError (errors='replace')."""
        binary_body = b"\xff\xfe\x00invalid utf-8 \x80\x81\x82"
        msg = _make_aio_message(body=binary_body)
        # Should not raise — filter has no body filter so result is True regardless
        result = _match(msg, MessageFilter())
        assert result is True

    def test_match_binary_body_with_callable_filter_does_not_raise(self):
        """Binary body with a callable filter — replacement chars used, no exception."""
        binary_body = b"\xff\xfe"
        msg = _make_aio_message(body=binary_body)
        # Callable checks body text; body contains replacement chars, not "hello"
        result = _match(msg, MessageFilter(filter_callable=lambda props, body: "hello" in body))
        assert result is False


# ---------------------------------------------------------------------------
# _nack_and_sleep
# ---------------------------------------------------------------------------

class TestNackAndSleep:
    @pytest.mark.asyncio
    async def test_nack_called_with_requeue_true(self):
        msg = _make_aio_message()
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock):
            await _nack_and_sleep(msg)
        msg.nack.assert_awaited_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_sleep_called_with_01(self):
        msg = _make_aio_message()
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await _nack_and_sleep(msg)
        mock_sleep.assert_awaited_once_with(0.1)

    @pytest.mark.asyncio
    async def test_ack_not_called(self):
        msg = _make_aio_message()
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock):
            await _nack_and_sleep(msg)
        msg.ack.assert_not_called()


class TestCallWithTimeout:
    @pytest.mark.asyncio
    async def test_returns_the_result_of_a_prompt_call(self):
        async def quick():
            return "done"

        assert await call_with_timeout(quick(), 1.0) == "done"

    @pytest.mark.asyncio
    async def test_raises_timeout_and_cancels_the_call(self):
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def hangs():
            started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        with pytest.raises(asyncio.TimeoutError):
            await call_with_timeout(hangs(), 0.05)

        assert started.is_set()
        assert cancelled.is_set()

    @pytest.mark.asyncio
    async def test_caller_cancellation_is_not_swallowed(self):
        """The cancel must reach the caller even when the inner call has just finished.

        ``asyncio.wait_for`` below Python 3.11 returns the inner result in that race and
        the consumer task would keep running after stop() asked it to end.
        """
        finished = asyncio.Event()
        resumed = False

        async def inner():
            finished.set()
            return "value"

        async def caller():
            nonlocal resumed
            await call_with_timeout(inner(), 10.0)
            resumed = True

        task = asyncio.create_task(caller())
        await finished.wait()
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert resumed is False

    @pytest.mark.asyncio
    async def test_an_error_from_the_inner_call_reaches_the_caller_unchanged(self):
        """The helper bounds the wait; it must not turn a real failure into a timeout."""
        async def boom():
            raise ValueError("passive declare refused")

        with pytest.raises(ValueError, match="passive declare refused"):
            await call_with_timeout(boom(), 10.0)

    @pytest.mark.asyncio
    async def test_the_timer_is_dropped_once_the_call_returns(self):
        """The fast path leaves no timer behind: one per AMQP call, and every call of
        a busy watcher would otherwise keep a callback scheduled for its full timeout."""
        loop = asyncio.get_running_loop()
        handles = []
        real_call_later = loop.call_later

        def recording_call_later(delay, callback, *args):
            handle = real_call_later(delay, callback, *args)
            handles.append(handle)
            return handle

        async def quick():
            return "value"

        with patch.object(loop, "call_later", recording_call_later):
            assert await call_with_timeout(quick(), 3600.0) == "value"

        assert handles, "the helper schedules a timer for the call it bounds"
        assert all(handle.cancelled() for handle in handles), handles

    @pytest.mark.asyncio
    async def test_a_cancel_in_the_same_tick_as_the_timeout_is_still_a_cancel(self):
        """The timer fires and the caller is cancelled in the same tick.

        The handler reads that as a cancel, not as its own timeout: read the other way
        round, a consumer task would ignore stop() and keep consuming."""
        survived = False

        async def slow():
            await asyncio.sleep(100)

        async def caller():
            nonlocal survived
            try:
                await call_with_timeout(slow(), 0.05)
            except asyncio.TimeoutError:
                pass
            survived = True
            await asyncio.sleep(0.2)

        task = asyncio.create_task(caller())
        await asyncio.sleep(0.05)   # the timer fires in this tick
        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task

        assert survived is False
