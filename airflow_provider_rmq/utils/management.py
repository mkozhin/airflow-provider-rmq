from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx


def get_management_url(conn_info: Any) -> str | None:
    """Read the ``management_url`` extra from an Airflow Connection.

    :param conn_info: Airflow Connection object (``airflow.models.Connection``).
    :returns: The configured Management API base URL with any trailing slash stripped,
        or ``None`` if the extra is not set.
    """
    management_url = conn_info.extra_dejson.get("management_url")
    if not management_url:
        return None
    return management_url.rstrip("/")


async def get_current_bindings(
    client: httpx.AsyncClient,
    management_url: str,
    vhost: str,
    queue: str,
    exchange: str,
    auth: tuple[str, str],
) -> set[str]:
    """Fetch the routing keys currently bound from ``exchange`` to ``queue``.

    :param client: Shared ``httpx.AsyncClient`` used to issue the request.
    :param management_url: Management API base URL (no trailing slash), e.g.
        ``https://mb.realcombi.mgcom.ru``.
    :param vhost: RabbitMQ vhost (as resolved from ``conn_info.schema or "/"``).
    :param queue: Queue name whose bindings are being inspected.
    :param exchange: Source exchange to filter bindings by — bindings whose
        ``source`` does not match this exchange (e.g. the default exchange, ``source=""``)
        are excluded.
    :param auth: ``(login, password)`` tuple for HTTP basic auth.
    :returns: Set of ``routing_key`` values for bindings whose ``source == exchange``.
    :raises httpx.HTTPStatusError: If the Management API responds with an error status.
    :raises httpx.TimeoutException: If the request times out.
    """
    url = f"{management_url}/api/queues/{quote(vhost, safe='')}/{quote(queue, safe='')}/bindings"
    response = await client.get(url, auth=auth)
    response.raise_for_status()
    bindings = response.json()
    return {b["routing_key"] for b in bindings if b.get("source") == exchange}
