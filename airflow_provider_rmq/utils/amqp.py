from __future__ import annotations

import asyncio
import ssl
from typing import Any
from urllib.parse import quote

from airflow_provider_rmq.utils.filters import MessageFilter
from airflow_provider_rmq.utils.ssl import build_ssl_context

AMQP_PORT = 5672
AMQPS_PORT = 5671


class _PropsShim:
    """Shim to bridge aio_pika message headers to MessageFilter's HasHeaders protocol."""

    __slots__ = ("headers",)

    def __init__(self, headers: dict[str, Any]):
        self.headers = headers


def build_amqp_connection(
    conn_info: Any,
    vhost_override: str | None = None,
) -> tuple[str, ssl.SSLContext | None]:
    """Build aio_pika-compatible AMQP URL and SSL context from Airflow connection.

    :param conn_info: Airflow Connection object (``airflow.models.Connection``).
    :param vhost_override: Optional vhost to use instead of ``conn_info.schema``.
    :returns: ``(url, ssl_context)`` — pass ``ssl_context`` to ``connect_robust()`` if not None.
    """
    extras = conn_info.extra_dejson
    ssl_context = build_ssl_context(extras)
    vhost = vhost_override or conn_info.schema or "/"
    port = conn_info.port if conn_info.port else (AMQPS_PORT if ssl_context else AMQP_PORT)
    url = (
        f"{'amqps' if ssl_context else 'amqp'}://"
        f"{quote(conn_info.login or 'guest', safe='')}:{quote(conn_info.password or 'guest', safe='')}"
        f"@{conn_info.host or 'localhost'}:{port}/{quote(vhost, safe='')}"
    )
    return url, ssl_context


def match(message: Any, msg_filter: MessageFilter) -> bool:
    """Evaluate message body and headers against filter without any ACK/NACK side-effects.

    :param message: An ``aio_pika`` message (must have ``.body`` and ``.headers``).
    :param msg_filter: Pre-built :class:`~airflow_provider_rmq.utils.filters.MessageFilter`.
    :returns: ``True`` if the message matches (or there are no filters), ``False`` otherwise.

    Non-UTF-8 bytes in the body are replaced with the Unicode replacement character
    (``errors="replace"``) so binary payloads never raise :exc:`UnicodeDecodeError`.
    """
    body_str = message.body.decode("utf-8", errors="replace")
    props = _PropsShim(dict(message.headers or {}))
    return not msg_filter.has_filters or msg_filter.matches(props, body_str)



async def nack_and_sleep(message: Any) -> None:
    """NACK a message with requeue=True and sleep 0.1 s to prevent a hot redelivery loop.

    :param message: An ``aio_pika`` message (must have ``.nack()`` async method).
    """
    await message.nack(requeue=True)
    await asyncio.sleep(0.1)



async def match_and_ack(message: Any, msg_filter: MessageFilter) -> bool:
    """Evaluate message against filter, ACK on match, NACK+requeue on miss.

    :param message: An ``aio_pika`` message (must have ``.body``, ``.headers``,
        ``.ack()``, ``.nack()`` async methods).
    :param msg_filter: Pre-built :class:`~airflow_provider_rmq.utils.filters.MessageFilter`.
    :returns: ``True`` if the message matched and was ACKed, ``False`` if NACKed.

    After a NACK, sleeps 0.1 s to prevent a hot redelivery loop on classic queues.

    Note:
        On quorum queues (RabbitMQ 4.x default), the broker enforces a delivery-limit
        of 20 by default — non-matching messages are dead-lettered after 20 redeliveries.
        Use dedicated queues per DAG to avoid unintended message loss.
    """
    if match(message, msg_filter):
        await message.ack()
        return True
    await nack_and_sleep(message)
    return False
