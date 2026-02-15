from __future__ import annotations

import asyncio
import logging
import ssl as ssl_module
from typing import Any, AsyncIterator
from urllib.parse import quote

import aio_pika
from airflow.hooks.base import BaseHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

from apache_airflow_provider_rmq.utils.filters import MessageFilter

log = logging.getLogger("airflow.task")


class _PropsShim:
    """Shim to bridge aio_pika message headers to MessageFilter's HasHeaders protocol."""

    __slots__ = ("headers",)

    def __init__(self, headers: dict[str, Any]):
        self.headers = headers


class RMQTrigger(BaseTrigger):
    """Async trigger that waits for a matching message in a RabbitMQ queue.

    Uses aio_pika for non-blocking AMQP access. Polls the queue periodically
    and evaluates dict-based filters on each message.

    Note: callable filters are NOT supported here (cannot be serialized).
    """

    def __init__(
        self,
        rmq_conn_id: str,
        queue_name: str,
        filter_data: dict[str, Any] | None = None,
        poll_interval: float = 5.0,
    ):
        super().__init__()
        self.rmq_conn_id = rmq_conn_id
        self.queue_name = queue_name
        self.filter_data = filter_data or {}
        self.poll_interval = poll_interval

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            "apache_airflow_provider_rmq.triggers.rmq.RMQTrigger",
            {
                "rmq_conn_id": self.rmq_conn_id,
                "queue_name": self.queue_name,
                "filter_data": self.filter_data,
                "poll_interval": self.poll_interval,
            },
        )

    def _build_ssl_context(self, extras: dict[str, Any]) -> ssl_module.SSLContext | None:
        """Build SSLContext from Airflow connection extras, matching hook's SSL logic."""
        if not extras.get("ssl_enabled", False):
            return None

        ssl_context = ssl_module.create_default_context()
        ssl_opts_raw = extras.get("ssl_options", {})
        if isinstance(ssl_opts_raw, str):
            import json
            ssl_opts_raw = json.loads(ssl_opts_raw)

        if ssl_opts_raw.get("ca_certs"):
            ssl_context.load_verify_locations(ssl_opts_raw["ca_certs"])
        if ssl_opts_raw.get("certfile") and ssl_opts_raw.get("keyfile"):
            ssl_context.load_cert_chain(
                certfile=ssl_opts_raw["certfile"],
                keyfile=ssl_opts_raw["keyfile"],
            )
        if ssl_opts_raw.get("cert_reqs") == "CERT_NONE":
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl_module.CERT_NONE

        return ssl_context

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Main trigger loop. Polls RabbitMQ asynchronously."""
        msg_filter = MessageFilter.deserialize(self.filter_data)

        try:
            conn_info = await asyncio.get_running_loop().run_in_executor(
                None, BaseHook.get_connection, self.rmq_conn_id
            )
            extras = conn_info.extra_dejson
            ssl_enabled = extras.get("ssl_enabled", False)
            ssl_context = self._build_ssl_context(extras)
            vhost = conn_info.schema or "/"
            port = conn_info.port if conn_info.port else (5671 if ssl_enabled else 5672)

            # URL-encode all components
            vhost_encoded = quote(vhost, safe="")
            login_encoded = quote(conn_info.login or "guest", safe="")
            password_encoded = quote(conn_info.password or "guest", safe="")

            url = (
                f"{'amqps' if ssl_enabled else 'amqp'}://"
                f"{login_encoded}:{password_encoded}"
                f"@{conn_info.host or 'localhost'}:{port}/{vhost_encoded}"
            )

            connect_kwargs: dict[str, Any] = {"url": url}
            if ssl_context is not None:
                connect_kwargs["ssl_context"] = ssl_context

            connection = await aio_pika.connect_robust(**connect_kwargs)

            async with connection:
                channel = await connection.channel()
                queue = await channel.declare_queue(self.queue_name, passive=True)

                while True:
                    message = await queue.get(fail=False)

                    if message is None:
                        await asyncio.sleep(self.poll_interval)
                        continue

                    body_str = message.body.decode("utf-8")

                    if msg_filter.has_filters:
                        props_shim = _PropsShim(dict(message.headers or {}))

                        if msg_filter.matches(props_shim, body_str):
                            await message.ack()
                            yield TriggerEvent({
                                "status": "success",
                                "message": {
                                    "body": body_str,
                                    "headers": dict(message.headers or {}),
                                    "routing_key": message.routing_key or "",
                                    "exchange": message.exchange or "",
                                },
                            })
                            return
                        else:
                            await message.nack(requeue=True)
                            # Sleep then continue polling — the broker may
                            # re-deliver the same message, but also may deliver
                            # a different one. This avoids a tight CPU loop.
                            await asyncio.sleep(self.poll_interval)
                    else:
                        await message.ack()
                        yield TriggerEvent({
                            "status": "success",
                            "message": {
                                "body": body_str,
                                "headers": dict(message.headers or {}),
                                "routing_key": message.routing_key or "",
                                "exchange": message.exchange or "",
                            },
                        })
                        return

        except Exception as e:
            log.exception("RMQTrigger encountered an error")
            yield TriggerEvent({"status": "error", "error": str(e)})