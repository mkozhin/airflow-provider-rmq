from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow_provider_rmq.utils.amqp import (
    AMQP_PORT,
    AMQPS_PORT,
    DEFAULT_CONNECT_TIMEOUT,
    DEFAULT_HEARTBEAT,
    DEFAULT_RPC_TIMEOUT,
    FilterMissState,
    _read_heartbeat,
    call_with_timeout,
    handle_miss,
    match as _match,
    nack_and_sleep as _nack_and_sleep,
    build_amqp_connection,
    get_amqp_timeouts,
    match_and_ack,
)
from airflow_provider_rmq.utils.filters import FilterVerdict, MessageFilter
from tests.conftest import (
    BATCH_DROPPED_TEXT,
    DROPPED_TEXT,
    EMPTY_BATCH_TEXT,
    ERROR_TEXT,
    STREAK_TEXT,
    FakeAirflowConnection,
    log_lines,
)


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

    def test_a_heartbeat_shorter_than_a_second_falls_back_to_default(self, caplog):
        """An interval too short to express as whole seconds is a mistake, and reading it
        as the zero opt-out would turn off the detection the caller asked to tighten."""
        conn = FakeAirflowConnection(extra='{"heartbeat": 0.5}')
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            url, _ = build_amqp_connection(conn)
        assert url.endswith(f"?heartbeat={DEFAULT_HEARTBEAT}")
        assert any("shorter than a second" in r.getMessage() for r in caplog.records)

    def test_a_heartbeat_too_large_for_a_float_falls_back_to_default(self, caplog):
        """A JSON integer no float can hold raises OverflowError out of the cast; the
        value is written as a bare literal because a quoted one reads as ``inf``."""
        conn = FakeAirflowConnection(extra='{"heartbeat": %s}' % ("9" * 400))
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            url, _ = build_amqp_connection(conn)
        assert url.endswith(f"?heartbeat={DEFAULT_HEARTBEAT}")
        assert any("is not a number" in r.getMessage() for r in caplog.records)

    def test_ssl_url_keeps_query(self):
        conn = FakeAirflowConnection(port=None, extra='{"ssl_enabled": true, "heartbeat": 15}')
        with patch("airflow_provider_rmq.utils.amqp.build_ssl_context") as mock_ssl:
            mock_ssl.return_value = MagicMock(spec=ssl.SSLContext)
            url, _ = build_amqp_connection(conn)
        assert url.startswith("amqps://")
        assert f":{AMQPS_PORT}/%2F?heartbeat=15" in url


# ---------------------------------------------------------------------------
# _read_heartbeat with a caller-supplied default
# ---------------------------------------------------------------------------

class TestReadHeartbeatDefault:
    def test_missing_key_uses_the_given_default(self, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            assert _read_heartbeat({}, default=600) == 600
        assert not caplog.records

    def test_non_number_uses_the_given_default(self, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            assert _read_heartbeat({"heartbeat": "abc"}, default=600) == 600
        assert any("600" in r.getMessage() for r in caplog.records)

    def test_interval_shorter_than_a_second_uses_the_given_default(self, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            assert _read_heartbeat({"heartbeat": 0.5}, default=600) == 600
        assert any(
            "shorter than a second" in r.getMessage() and "600" in r.getMessage()
            for r in caplog.records
        )

    def test_valid_value_ignores_the_default(self):
        assert _read_heartbeat({"heartbeat": "45"}, default=600) == 45

    def test_amqp_limit_is_accepted(self):
        assert _read_heartbeat({"heartbeat": 65535}, default=600) == 65535

    @pytest.mark.parametrize("raw", [65536, "100000"])
    def test_interval_above_amqp_limit_uses_the_given_default(self, raw, caplog):
        with caplog.at_level(logging.WARNING, logger="airflow_provider_rmq.utils.amqp"):
            assert _read_heartbeat({"heartbeat": raw}, default=600) == 600
        assert any(
            "above the AMQP limit of 65535" in r.getMessage() and "600" in r.getMessage()
            for r in caplog.records
        )

    def test_interval_above_amqp_limit_uses_async_default(self):
        assert _read_heartbeat({"heartbeat": 70000}) == DEFAULT_HEARTBEAT

    def test_default_is_the_async_interval_when_not_given(self):
        assert _read_heartbeat({}) == DEFAULT_HEARTBEAT


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

    @pytest.mark.parametrize("raw", ['"Infinity"', '"NaN"', "1e400"])
    def test_a_value_that_is_not_finite_falls_back(self, caplog, raw):
        """An infinite timeout is a call with no bound at all, which is what every bound
        here exists to prevent, and NaN compares False against every minimum."""
        conn = FakeAirflowConnection(extra=f'{{"connect_timeout": {raw}}}')
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


def _make_miss_state(ack_unmatched: bool = False, logger_name: str = "test.miss.state"):
    return FilterMissState("q", logging.getLogger(logger_name), ack_unmatched=ack_unmatched)


class TestMatchAndAck:
    @pytest.mark.asyncio
    async def test_matching_message_acked(self):
        msg = _make_aio_message(headers={"type": "order"})
        f = MessageFilter(filter_headers={"type": "order"})
        result = await match_and_ack(msg, f, _make_miss_state())
        assert result is True
        msg.ack.assert_awaited_once()
        msg.nack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_non_matching_message_nacked(self):
        msg = _make_aio_message(headers={"type": "payment"})
        f = MessageFilter(filter_headers={"type": "order"})
        result = await match_and_ack(msg, f, _make_miss_state())
        assert result is False
        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_filter_always_matches(self):
        msg = _make_aio_message()
        f = MessageFilter()
        result = await match_and_ack(msg, f, _make_miss_state())
        assert result is True
        msg.ack.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_message_nacked(self):
        """A body the filter could not read is returned to the queue, never acknowledged."""
        msg = _make_aio_message(body=b"not json at all")
        f = MessageFilter(filter_headers={"body.status": "ok"})
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock):
            result = await match_and_ack(msg, f, _make_miss_state())
        assert result is False
        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_error_message_nacked_even_when_ack_unmatched_is_on(self):
        """A failure to evaluate never reaches the acknowledging branch."""
        msg = _make_aio_message(body=b"not json at all")
        f = MessageFilter(filter_headers={"body.status": "ok"})
        state = _make_miss_state(ack_unmatched=True)
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock):
            result = await match_and_ack(msg, f, state)
        assert result is False
        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()
        assert state.dropped == 0
        assert state.error_streak == 1

    @pytest.mark.asyncio
    async def test_miss_acked_when_ack_unmatched_is_on(self):
        msg = _make_aio_message(headers={"type": "payment"})
        f = MessageFilter(filter_headers={"type": "order"})
        state = _make_miss_state(ack_unmatched=True)
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            result = await match_and_ack(msg, f, state)
        assert result is False
        msg.ack.assert_awaited_once()
        msg.nack.assert_not_awaited()
        mock_sleep.assert_not_awaited()
        assert state.dropped == 1

    @pytest.mark.asyncio
    async def test_counters_follow_the_verdict(self):
        f = MessageFilter(filter_headers={"type": "order"})
        state = _make_miss_state()
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock):
            await match_and_ack(_make_aio_message(headers={"type": "other"}), f, state)
        assert state.streak == 1
        await match_and_ack(_make_aio_message(headers={"type": "order"}), f, state)
        assert state.streak == 0

    @pytest.mark.asyncio
    async def test_nack_includes_sleep(self):
        msg = _make_aio_message(headers={"type": "other"})
        f = MessageFilter(filter_headers={"type": "order"})
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await match_and_ack(msg, f, _make_miss_state())
        mock_sleep.assert_awaited_once_with(0.1)

    @pytest.mark.asyncio
    async def test_match_no_sleep(self):
        msg = _make_aio_message(headers={"type": "order"})
        f = MessageFilter(filter_headers={"type": "order"})
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await match_and_ack(msg, f, _make_miss_state())
        mock_sleep.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_miss_state_is_required(self):
        """No default value: a caller that forgot the state must not keep the old path."""
        with pytest.raises(TypeError):
            await match_and_ack(_make_aio_message(), MessageFilter())


class TestHandleMiss:
    @pytest.mark.asyncio
    async def test_requeues_and_sleeps_by_default(self):
        msg = _make_aio_message()
        state = _make_miss_state()
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await handle_miss(msg, state)
        msg.nack.assert_awaited_once_with(requeue=True)
        msg.ack.assert_not_awaited()
        mock_sleep.assert_awaited_once_with(0.1)
        assert state.streak == 1
        assert state.dropped == 0

    @pytest.mark.asyncio
    async def test_acknowledges_when_asked(self):
        msg = _make_aio_message()
        state = _make_miss_state(ack_unmatched=True)
        with patch("airflow_provider_rmq.utils.amqp.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await handle_miss(msg, state)
        msg.ack.assert_awaited_once()
        msg.nack.assert_not_awaited()
        mock_sleep.assert_not_awaited()
        assert state.dropped == 1

    @pytest.mark.asyncio
    async def test_miss_state_is_required(self):
        with pytest.raises(TypeError):
            await handle_miss(_make_aio_message())


# ---------------------------------------------------------------------------
# FilterMissState
# ---------------------------------------------------------------------------



class TestFilterMissState:
    def test_streak_grows_and_a_match_resets_it(self):
        state = _make_miss_state()
        state.on_miss()
        state.on_miss()
        assert state.streak == 2
        state.on_match()
        assert state.streak == 0

    def test_streak_warns_on_a_hundred_and_a_thousand_only(self, caplog):
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            for _ in range(1000):
                state.on_miss()
        assert [r.getMessage().split()[0] for r in log_lines(caplog, STREAK_TEXT)] == ["100", "1000"]

    def test_streak_is_silent_below_a_hundred(self, caplog):
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            for _ in range(99):
                state.on_miss()
        assert log_lines(caplog, STREAK_TEXT) == []

    def test_the_requeueing_streak_says_the_messages_came_back(self, caplog):
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            for _ in range(100):
                state.on_miss()
        texts = [r.getMessage() for r in log_lines(caplog, STREAK_TEXT)]
        assert len(texts) == 1
        assert "returned to the queue" in texts[0]
        assert "acknowledged and dropped" not in texts[0]

    def test_the_acknowledging_streak_says_the_messages_are_gone(self, caplog):
        state = _make_miss_state(ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            for _ in range(100):
                state.on_miss()
        texts = [r.getMessage() for r in log_lines(caplog, STREAK_TEXT)]
        assert len(texts) == 1
        assert "acknowledged and dropped" in texts[0]
        assert "returned to the queue" not in texts[0]

    def test_a_run_equal_to_the_total_does_not_say_it_twice(self, caplog):
        """Both counters reach a hundred on the same miss; the run's line already
        carries the total, so the second line would only repeat the number."""
        state = _make_miss_state(ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            for _ in range(100):
                state.on_miss()
        assert state.dropped == 100
        assert log_lines(caplog, DROPPED_TEXT) == []

    def test_a_total_out_of_step_with_the_run_gets_its_own_line(self, caplog):
        """A match resets the run and leaves the total behind, so the two numbers
        part ways and the total needs saying."""
        state = _make_miss_state(ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            for _ in range(50):
                state.on_miss()
            state.on_match()
            for _ in range(50):
                state.on_miss()
        assert state.dropped == 100
        assert state.streak == 50
        assert len(log_lines(caplog, DROPPED_TEXT)) == 1
        assert log_lines(caplog, STREAK_TEXT) == []

    def test_error_warns_on_the_first_failure_then_throttles(self, caplog):
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            for _ in range(100):
                state.on_error("ValueError: boom")
        assert [r.getMessage() for r in log_lines(caplog, ERROR_TEXT)] != []
        numbers = [int(r.getMessage().split("(")[1].split()[0]) for r in log_lines(caplog, ERROR_TEXT)]
        assert numbers == [1, 100]

    def test_error_warning_carries_the_reason(self, caplog):
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            state.on_error("JSONDecodeError: Expecting value: line 1 column 1 (char 0)")
        assert "JSONDecodeError: Expecting value" in log_lines(caplog, ERROR_TEXT)[0].getMessage()

    def test_miss_does_not_reset_the_error_streak(self, caplog):
        """Alternating MISS and ERROR grows both counters and warns once on the failures."""
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            for _ in range(20):
                state.on_miss()
                state.on_error("ValueError: boom")
        assert state.streak == 20
        assert state.error_streak == 20
        assert len(log_lines(caplog, ERROR_TEXT)) == 1

    def test_match_does_not_reset_the_error_streak(self, caplog):
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            for _ in range(20):
                state.on_match()
                state.on_error("ValueError: boom")
        assert state.error_streak == 20
        assert len(log_lines(caplog, ERROR_TEXT)) == 1

    def test_error_streak_is_cleared_by_a_hundred_clean_outcomes(self, caplog):
        state = _make_miss_state()
        state.on_error("ValueError: boom")
        for _ in range(99):
            state.on_match()
        assert state.error_streak == 1
        state.on_match()
        assert state.error_streak == 0
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            state.on_error("ValueError: boom")
        assert len(log_lines(caplog, ERROR_TEXT)) == 1

    def test_dropped_counts_what_acknowledgement_deleted(self, caplog):
        """Ninety-nine misses and one match never reach the streak threshold."""
        state = _make_miss_state(ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            for _ in range(2):
                for _ in range(99):
                    state.on_miss()
                state.on_match()
        assert log_lines(caplog, STREAK_TEXT) == []
        assert state.dropped == 198
        assert len(log_lines(caplog, DROPPED_TEXT)) == 1

    def test_dropped_stays_at_zero_without_acknowledgement(self, caplog):
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            for _ in range(200):
                state.on_miss()
        assert state.dropped == 0
        assert log_lines(caplog, DROPPED_TEXT) == []

    def test_batch_done_warns_when_nothing_matched(self, caplog):
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            state.on_batch_done(matched=0, evaluated=5, dropped_in_batch=0)
        assert len(log_lines(caplog, EMPTY_BATCH_TEXT)) == 1

    def test_a_batch_that_matched_nothing_and_dropped_all_is_one_line(self, caplog):
        """The two conditions of the batch line describe one batch, so they share a line."""
        state = _make_miss_state(ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            state.on_batch_done(matched=0, evaluated=5, dropped_in_batch=5)
        assert len(caplog.records) == 1
        message = caplog.records[0].getMessage()
        assert "None of the 5 messages" in message
        assert "5 of them were acknowledged and dropped" in message

    def test_batch_done_is_silent_on_a_match_and_on_an_empty_batch(self, caplog):
        state = _make_miss_state()
        with caplog.at_level(logging.WARNING):
            state.on_batch_done(matched=1, evaluated=5, dropped_in_batch=0)
            state.on_batch_done(matched=0, evaluated=0, dropped_in_batch=0)
        assert caplog.records == []

    def test_batch_done_reports_dropped_even_when_something_matched(self, caplog):
        state = _make_miss_state(ack_unmatched=True)
        with caplog.at_level(logging.WARNING):
            state.on_batch_done(matched=1, evaluated=100, dropped_in_batch=99)
        assert len(log_lines(caplog, BATCH_DROPPED_TEXT)) == 1

    def test_warning_goes_to_the_logger_it_was_given(self, caplog):
        state = _make_miss_state(logger_name="airflow.task")
        with caplog.at_level(logging.WARNING):
            for _ in range(100):
                state.on_miss()
        assert [r.name for r in log_lines(caplog, STREAK_TEXT)] == ["airflow.task"]


# ---------------------------------------------------------------------------
# _match (pure predicate, no side effects)
# ---------------------------------------------------------------------------

class TestMatch:
    def test_match_returns_match_when_no_filters(self):
        msg = _make_aio_message(body=b"any")
        assert _match(msg, MessageFilter()) == (FilterVerdict.MATCH, None)

    def test_match_returns_match_when_headers_match(self):
        msg = _make_aio_message(headers={"type": "order"})
        assert _match(msg, MessageFilter(filter_headers={"type": "order"})) == (FilterVerdict.MATCH, None)

    def test_match_returns_miss_when_headers_differ(self):
        msg = _make_aio_message(headers={"type": "payment"})
        assert _match(msg, MessageFilter(filter_headers={"type": "order"})) == (FilterVerdict.MISS, None)

    def test_match_returns_error_when_body_is_not_json(self):
        """A body that does not read as JSON is a failure to evaluate, not a miss."""
        msg = _make_aio_message(body=b"not json at all")
        verdict, reason = _match(msg, MessageFilter(filter_headers={"body.status": "ok"}))
        assert verdict is FilterVerdict.ERROR
        assert "JSONDecodeError" in reason

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
        # Should not raise — filter has no body filter so the verdict is MATCH regardless
        assert _match(msg, MessageFilter()) == (FilterVerdict.MATCH, None)

    def test_match_binary_body_with_callable_filter_does_not_raise(self):
        """Binary body with a callable filter — replacement chars used, no exception."""
        binary_body = b"\xff\xfe"
        msg = _make_aio_message(body=binary_body)
        # Callable checks body text; body contains replacement chars, not "hello"
        result = _match(msg, MessageFilter(filter_callable=lambda props, body: "hello" in body))
        assert result == (FilterVerdict.MISS, None)


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
    async def test_a_call_slow_to_cancel_does_not_stretch_the_timeout(self):
        """The bound holds even when the call takes its time answering the cancel.

        An ``aiormq`` RPC answers a cancel by closing its channel, and the close writes
        a frame to the very socket the call is stuck on. Waiting for that would hand the
        bound back to the broker that stopped answering — the one case the timeout is
        there for.
        """
        cleaning_up = asyncio.Event()
        release = asyncio.Event()

        async def stubborn():
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                cleaning_up.set()
                await release.wait()
                raise

        call = asyncio.create_task(stubborn())
        bounded = asyncio.create_task(call_with_timeout(call, 0.05))
        started = time.monotonic()
        done, _ = await asyncio.wait({bounded}, timeout=1.0)
        try:
            assert bounded in done, "the caller is still waiting out the cancellation"
            with pytest.raises(asyncio.TimeoutError):
                bounded.result()

            assert time.monotonic() - started < 0.5
            assert cleaning_up.is_set(), "the call is cancelled, just not waited for"
            assert not call.done(), "the call is still cleaning up in the background"
        finally:
            bounded.cancel()
            release.set()
            with contextlib.suppress(asyncio.CancelledError):
                await call

    @pytest.mark.asyncio
    async def test_a_cancelled_caller_does_not_wait_for_a_slow_cancel_either(self):
        """stop() must reach the caller at once, whatever the call it is inside does."""
        release = asyncio.Event()

        async def stubborn():
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                await release.wait()
                raise

        call = asyncio.create_task(stubborn())
        caller = asyncio.create_task(call_with_timeout(call, 3600.0))
        await asyncio.sleep(0)
        caller.cancel()

        try:
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(caller, 0.5)
            assert not call.done()
        finally:
            release.set()
            with contextlib.suppress(asyncio.CancelledError):
                await call

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
