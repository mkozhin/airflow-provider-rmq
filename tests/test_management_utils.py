from __future__ import annotations

import httpx
import pytest

from airflow_provider_rmq.utils.management import (
    get_current_bindings,
    get_management_url,
    get_queue_consumers,
)
from tests.conftest import FakeAirflowConnection


# ---------------------------------------------------------------------------
# get_management_url
# ---------------------------------------------------------------------------

class TestGetManagementUrl:
    def test_returns_none_when_not_set(self):
        conn = FakeAirflowConnection(extra="{}")
        assert get_management_url(conn) is None

    def test_returns_url_when_set(self):
        conn = FakeAirflowConnection(extra='{"management_url": "https://mb.realcombi.mgcom.ru"}')
        assert get_management_url(conn) == "https://mb.realcombi.mgcom.ru"

    def test_strips_trailing_slash(self):
        conn = FakeAirflowConnection(extra='{"management_url": "https://mb.realcombi.mgcom.ru/"}')
        assert get_management_url(conn) == "https://mb.realcombi.mgcom.ru"

    def test_strips_multiple_trailing_slashes(self):
        conn = FakeAirflowConnection(extra='{"management_url": "https://mb.realcombi.mgcom.ru//"}')
        assert get_management_url(conn) == "https://mb.realcombi.mgcom.ru"

    def test_returns_none_when_empty_string(self):
        conn = FakeAirflowConnection(extra='{"management_url": ""}')
        assert get_management_url(conn) is None

    def test_returns_none_when_non_string_int(self):
        conn = FakeAirflowConnection(extra='{"management_url": 123}')
        assert get_management_url(conn) is None

    def test_returns_none_when_non_string_bool(self):
        conn = FakeAirflowConnection(extra='{"management_url": true}')
        assert get_management_url(conn) is None


# ---------------------------------------------------------------------------
# get_current_bindings
# ---------------------------------------------------------------------------

def _client_with_response(handler) -> httpx.AsyncClient:
    transport = httpx.MockTransport(handler)
    return httpx.AsyncClient(transport=transport)


class TestGetCurrentBindings:
    @pytest.mark.asyncio
    async def test_filters_by_source_exchange(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=[
                    {"source": "jetstat.airflow", "destination": "rmq_watcher.sub.my_dag", "routing_key": "id1.succeeded"},
                    {"source": "jetstat.airflow", "destination": "rmq_watcher.sub.my_dag", "routing_key": "id2.failed"},
                ],
            )

        async with _client_with_response(handler) as client:
            result = await get_current_bindings(
                client,
                "https://mb.realcombi.mgcom.ru",
                "/",
                "rmq_watcher.sub.my_dag",
                "jetstat.airflow",
                ("guest", "guest"),
            )
        assert result == {"id1.succeeded", "id2.failed"}

    @pytest.mark.asyncio
    async def test_excludes_default_exchange_bindings(self):
        """A binding with source="" (default exchange) must not be included."""
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=[
                    {"source": "", "destination": "rmq_watcher.sub.my_dag", "routing_key": "rmq_watcher.sub.my_dag"},
                    {"source": "jetstat.airflow", "destination": "rmq_watcher.sub.my_dag", "routing_key": "id1.succeeded"},
                ],
            )

        async with _client_with_response(handler) as client:
            result = await get_current_bindings(
                client,
                "https://mb.realcombi.mgcom.ru",
                "/",
                "rmq_watcher.sub.my_dag",
                "jetstat.airflow",
                ("guest", "guest"),
            )
        assert result == {"id1.succeeded"}

    @pytest.mark.asyncio
    async def test_excludes_bindings_from_other_exchanges(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=[
                    {"source": "some.other.exchange", "destination": "rmq_watcher.sub.my_dag", "routing_key": "region.eu.alert"},
                ],
            )

        async with _client_with_response(handler) as client:
            result = await get_current_bindings(
                client,
                "https://mb.realcombi.mgcom.ru",
                "/",
                "rmq_watcher.sub.my_dag",
                "jetstat.airflow",
                ("guest", "guest"),
            )
        assert result == set()

    @pytest.mark.asyncio
    async def test_empty_bindings_list(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        async with _client_with_response(handler) as client:
            result = await get_current_bindings(
                client,
                "https://mb.realcombi.mgcom.ru",
                "/",
                "rmq_watcher.sub.my_dag",
                "jetstat.airflow",
                ("guest", "guest"),
            )
        assert result == set()

    @pytest.mark.asyncio
    async def test_builds_correct_url_with_quoting(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, json=[])

        async with _client_with_response(handler) as client:
            await get_current_bindings(
                client,
                "https://mb.realcombi.mgcom.ru",
                "/",
                "rmq_watcher.sub.my_dag",
                "jetstat.airflow",
                ("guest", "guest"),
            )
        assert captured["url"] == (
            "https://mb.realcombi.mgcom.ru/api/queues/%2F/rmq_watcher.sub.my_dag/bindings"
        )

    @pytest.mark.asyncio
    async def test_http_error_status_raises(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404, json={"error": "not_found"})

        async with _client_with_response(handler) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await get_current_bindings(
                    client,
                    "https://mb.realcombi.mgcom.ru",
                    "/",
                    "rmq_watcher.sub.my_dag",
                    "jetstat.airflow",
                    ("guest", "guest"),
                )

    @pytest.mark.asyncio
    async def test_server_error_status_raises(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(500, json={"error": "internal"})

        async with _client_with_response(handler) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await get_current_bindings(
                    client,
                    "https://mb.realcombi.mgcom.ru",
                    "/",
                    "rmq_watcher.sub.my_dag",
                    "jetstat.airflow",
                    ("guest", "guest"),
                )

    @pytest.mark.asyncio
    async def test_passes_basic_auth(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["auth_header"] = request.headers.get("authorization")
            return httpx.Response(200, json=[])

        async with _client_with_response(handler) as client:
            await get_current_bindings(
                client,
                "https://mb.realcombi.mgcom.ru",
                "/",
                "rmq_watcher.sub.my_dag",
                "jetstat.airflow",
                ("admin", "s3cr3t"),
            )
        assert captured["auth_header"] is not None
        assert captured["auth_header"].startswith("Basic ")


# ---------------------------------------------------------------------------
# get_queue_consumers
# ---------------------------------------------------------------------------

def _consumer_entry(tag: str, queue: str) -> dict:
    return {
        "consumer_tag": tag,
        "queue": {"name": queue, "vhost": "/"},
        "channel_details": {"name": "127.0.0.1:5672 -> 127.0.0.1:1 (1)"},
    }


class TestGetQueueConsumers:
    @pytest.mark.asyncio
    async def test_groups_tags_by_queue(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=[
                    _consumer_entry("rmq_watcher.host.1.7", "orders"),
                    _consumer_entry("some.other.client", "orders"),
                    _consumer_entry("rmq_watcher.host.1.fire", "rmq_watcher.fire"),
                ],
            )

        async with _client_with_response(handler) as client:
            result = await get_queue_consumers(
                client, "https://mb.realcombi.mgcom.ru", "/", ("guest", "guest"),
            )
        assert result == {
            "orders": {"rmq_watcher.host.1.7", "some.other.client"},
            "rmq_watcher.fire": {"rmq_watcher.host.1.fire"},
        }

    @pytest.mark.asyncio
    async def test_empty_list_yields_empty_mapping(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[])

        async with _client_with_response(handler) as client:
            result = await get_queue_consumers(
                client, "https://mb.realcombi.mgcom.ru", "/", ("guest", "guest"),
            )
        assert result == {}

    @pytest.mark.asyncio
    async def test_entries_without_tag_or_queue_are_skipped(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(
                200,
                json=[
                    {"queue": {"name": "orders"}},
                    {"consumer_tag": "tagless-queue"},
                    _consumer_entry("rmq_watcher.host.1.7", "orders"),
                ],
            )

        async with _client_with_response(handler) as client:
            result = await get_queue_consumers(
                client, "https://mb.realcombi.mgcom.ru", "/", ("guest", "guest"),
            )
        assert result == {"orders": {"rmq_watcher.host.1.7"}}

    @pytest.mark.asyncio
    async def test_requests_the_consumers_endpoint_with_quoted_vhost(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["auth_header"] = request.headers.get("authorization")
            return httpx.Response(200, json=[])

        async with _client_with_response(handler) as client:
            await get_queue_consumers(
                client, "https://mb.realcombi.mgcom.ru", "/", ("admin", "s3cr3t"),
            )
        assert captured["url"] == "https://mb.realcombi.mgcom.ru/api/consumers/%2F"
        assert captured["auth_header"].startswith("Basic ")

    @pytest.mark.asyncio
    async def test_http_error_status_raises(self):
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, json={"error": "unavailable"})

        async with _client_with_response(handler) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await get_queue_consumers(
                    client, "https://mb.realcombi.mgcom.ru", "/", ("guest", "guest"),
                )
