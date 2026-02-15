from __future__ import annotations

import json
import logging
import ssl
from typing import Any

import pika
import pika.exceptions
import pika.frame
import pika.spec
from airflow.hooks.base import BaseHook
from pika.adapters.blocking_connection import BlockingChannel
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

log = logging.getLogger("airflow.task")


class RMQHook(BaseHook):
    """Hook for interacting with RabbitMQ via pika BlockingConnection.

    Features:
    - Lazy connection (connects on first use via get_channel())
    - SSL/TLS support (configured via Airflow connection extras)
    - Retry/reconnect logic (tenacity with configurable retry_count/retry_delay)
    - Dead Letter Queue helper (build_dlq_arguments)
    - Context manager support
    - QoS configuration
    """

    conn_name_attr = "rmq_conn_id"
    default_conn_name = "rmq_default"
    conn_type = "amqp"
    hook_name = "RabbitMQ"

    # --- UI field customization ---

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        return {
            "hidden_fields": [],
            "relabeling": {"schema": "Virtual Host"},
            "placeholders": {
                "login": "guest",
                "password": "guest",
                "port": "5672",
                "host": "localhost",
                "schema": "/",
                "extra": '{"ssl_enabled": false}',
            },
        }

    def __init__(
        self,
        rmq_conn_id: str = default_conn_name,
        vhost: str | None = None,
        qos: dict | None = None,
        retry_count: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        super().__init__()
        self.rmq_conn_id = rmq_conn_id
        self._vhost_override = vhost
        self._qos = qos
        self._retry_count = retry_count
        self._retry_delay = retry_delay
        self._connection: pika.BlockingConnection | None = None
        self._channel: BlockingChannel | None = None

    # --- Connection management ---

    def _get_connection_params(self) -> pika.ConnectionParameters:
        """Build pika ConnectionParameters from Airflow connection."""
        conn = self.get_connection(self.rmq_conn_id)
        extras = conn.extra_dejson

        vhost = self._vhost_override or conn.schema or "/"
        credentials = pika.PlainCredentials(conn.login or "guest", conn.password or "guest")

        ssl_options = None
        if extras.get("ssl_enabled", False):
            ssl_context = ssl.create_default_context()
            ssl_opts_raw = extras.get("ssl_options", {})
            if isinstance(ssl_opts_raw, str):
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
                ssl_context.verify_mode = ssl.CERT_NONE
            ssl_options = pika.SSLOptions(ssl_context, conn.host)

        port = conn.port if conn.port else (5671 if ssl_options else 5672)

        return pika.ConnectionParameters(
            host=conn.host or "localhost",
            port=port,
            virtual_host=vhost,
            credentials=credentials,
            ssl_options=ssl_options,
            heartbeat=extras.get("heartbeat", 600),
            blocked_connection_timeout=extras.get("blocked_connection_timeout", 300),
        )

    def _establish_connection(self) -> pika.BlockingConnection:
        """Establish connection with retry logic."""
        @retry(
            retry=retry_if_exception_type((
                pika.exceptions.AMQPConnectionError,
                pika.exceptions.ConnectionClosedByBroker,
                ConnectionError,
            )),
            stop=stop_after_attempt(self._retry_count),
            wait=wait_exponential(multiplier=self._retry_delay, min=self._retry_delay, max=30),
            reraise=True,
        )
        def _connect():
            params = self._get_connection_params()
            log.info("Connecting to RabbitMQ at %s:%s/%s", params.host, params.port, params.virtual_host)
            return pika.BlockingConnection(params)

        return _connect()

    def get_channel(self) -> BlockingChannel:
        """Return an open channel, reconnecting if necessary."""
        if self._connection is None or self._connection.is_closed:
            self._connection = self._establish_connection()
            self._channel = None
        if self._channel is None or self._channel.is_closed:
            self._channel = self._connection.channel()
            if self._qos:
                self._channel.basic_qos(
                    prefetch_size=self._qos.get("prefetch_size", 0),
                    prefetch_count=self._qos.get("prefetch_count", 0),
                    global_qos=self._qos.get("global_qos", False),
                )
        return self._channel

    # --- Context manager ---

    def __enter__(self) -> RMQHook:
        self.get_channel()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()

    def close(self) -> None:
        """Gracefully close channel and connection."""
        if self._channel and self._channel.is_open:
            try:
                self._channel.cancel()
            except Exception:
                log.debug("Error cancelling consumer on channel", exc_info=True)
            try:
                self._channel.close()
            except Exception:
                log.warning("Error closing RabbitMQ channel", exc_info=True)
        if self._connection and self._connection.is_open:
            try:
                self._connection.close()
            except Exception:
                log.warning("Error closing RabbitMQ connection", exc_info=True)
        self._channel = None
        self._connection = None

    # --- DLQ Helper ---

    @staticmethod
    def build_dlq_arguments(
        dlx_exchange: str = "",
        dlx_routing_key: str | None = None,
        message_ttl: int | None = None,
    ) -> dict[str, Any]:
        """Build arguments dict for queue_declare with Dead Letter Queue support.

        Args:
            dlx_exchange: Dead letter exchange name.
            dlx_routing_key: Optional dead letter routing key.
            message_ttl: Optional per-message TTL in milliseconds.

        Returns:
            Dict of x-* arguments for queue_declare.
        """
        args: dict[str, Any] = {"x-dead-letter-exchange": dlx_exchange}
        if dlx_routing_key is not None:
            args["x-dead-letter-routing-key"] = dlx_routing_key
        if message_ttl is not None:
            args["x-message-ttl"] = message_ttl
        return args

    # --- Queue operations ---

    def queue_declare(
        self,
        queue_name: str,
        passive: bool = False,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: dict | None = None,
    ) -> pika.frame.Method:
        channel = self.get_channel()
        return channel.queue_declare(
            queue=queue_name,
            passive=passive,
            durable=durable,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments,
        )

    def queue_delete(
        self,
        queue_name: str,
        if_unused: bool = False,
        if_empty: bool = False,
    ) -> pika.frame.Method:
        channel = self.get_channel()
        return channel.queue_delete(queue=queue_name, if_unused=if_unused, if_empty=if_empty)

    def queue_bind(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: dict | None = None,
    ) -> pika.frame.Method:
        channel = self.get_channel()
        return channel.queue_bind(
            queue=queue, exchange=exchange, routing_key=routing_key, arguments=arguments,
        )

    def queue_unbind(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: dict | None = None,
    ) -> pika.frame.Method:
        channel = self.get_channel()
        return channel.queue_unbind(
            queue=queue, exchange=exchange, routing_key=routing_key, arguments=arguments,
        )

    def queue_purge(self, queue_name: str) -> pika.frame.Method:
        channel = self.get_channel()
        return channel.queue_purge(queue=queue_name)

    def queue_info(self, queue_name: str) -> dict[str, Any]:
        """Get queue info via passive declare."""
        channel = self.get_channel()
        try:
            result = channel.queue_declare(queue=queue_name, passive=True)
            return {
                "queue": result.method.queue,
                "message_count": result.method.message_count,
                "consumer_count": result.method.consumer_count,
                "exists": True,
            }
        except pika.exceptions.ChannelClosedByBroker:
            self._channel = None
            return {"queue": queue_name, "message_count": 0, "exists": False}

    # --- Exchange operations ---

    def exchange_declare(
        self,
        exchange: str,
        exchange_type: str = "direct",
        passive: bool = False,
        durable: bool = False,
        auto_delete: bool = False,
        internal: bool = False,
        arguments: dict | None = None,
    ) -> pika.frame.Method:
        channel = self.get_channel()
        return channel.exchange_declare(
            exchange=exchange,
            exchange_type=exchange_type,
            passive=passive,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal,
            arguments=arguments,
        )

    def exchange_delete(self, exchange: str, if_unused: bool = False) -> pika.frame.Method:
        channel = self.get_channel()
        return channel.exchange_delete(exchange=exchange, if_unused=if_unused)

    def exchange_bind(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict | None = None,
    ) -> pika.frame.Method:
        channel = self.get_channel()
        return channel.exchange_bind(
            destination=destination, source=source, routing_key=routing_key, arguments=arguments,
        )

    def exchange_unbind(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict | None = None,
    ) -> pika.frame.Method:
        channel = self.get_channel()
        return channel.exchange_unbind(
            destination=destination, source=source, routing_key=routing_key, arguments=arguments,
        )

    # --- Publish ---

    def basic_publish(
        self,
        exchange: str,
        routing_key: str,
        body: str | bytes,
        properties: pika.BasicProperties | None = None,
    ) -> None:
        channel = self.get_channel()
        if isinstance(body, str):
            body = body.encode("utf-8")
        channel.basic_publish(
            exchange=exchange, routing_key=routing_key, body=body, properties=properties,
        )

    # --- Consume ---

    def consume_messages(
        self,
        queue_name: str,
        max_messages: int = 1,
        auto_ack: bool = False,
        inactivity_timeout: float | None = 1.0,
    ) -> list[dict[str, Any]]:
        """Consume up to max_messages from a queue.

        Returns list of dicts with keys: method, properties, body (decoded str).
        Messages are NOT auto-acked by default; caller must ack/nack.
        """
        channel = self.get_channel()
        messages: list[dict[str, Any]] = []
        for method, properties, body in channel.consume(
            queue=queue_name,
            auto_ack=auto_ack,
            inactivity_timeout=inactivity_timeout,
        ):
            if method is None:
                break
            messages.append({
                "method": method,
                "properties": properties,
                "body": body.decode("utf-8") if isinstance(body, bytes) else body,
            })
            if len(messages) >= max_messages:
                break
        channel.cancel()
        return messages

    def ack(self, delivery_tag: int) -> None:
        self.get_channel().basic_ack(delivery_tag=delivery_tag)

    def nack(self, delivery_tag: int, requeue: bool = True) -> None:
        self.get_channel().basic_nack(delivery_tag=delivery_tag, requeue=requeue)