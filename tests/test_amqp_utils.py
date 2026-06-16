from __future__ import annotations

import ssl
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow_provider_rmq.utils.amqp import (
    AMQP_PORT,
    AMQPS_PORT,
    match as _match,
    nack_and_sleep as _nack_and_sleep,
    build_amqp_connection,
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
        assert url == "amqp://user:pass@rmq.local:5672/%2F"
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
        assert url.endswith("/%2F")

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
