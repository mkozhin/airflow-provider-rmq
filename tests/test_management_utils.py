from __future__ import annotations

import httpx
import pytest

from airflow_provider_rmq.utils.management import get_current_bindings, get_management_url
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
