from __future__ import annotations

import logging
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

# Suppress verbose pika internal logs (connection, channel, transport details).
# Only warnings and errors from pika will be shown.
logging.getLogger("pika").setLevel(logging.WARNING)


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

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Return custom connection form widgets for SSL configuration.

        :return: Mapping of field names to WTForms field instances.
        :rtype: dict[str, Any]
        """
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import BooleanField, StringField

        return {
            "ssl_enabled": BooleanField(lazy_gettext("SSL Enabled"), default=False),
            "ca_certs": StringField(lazy_gettext("CA Certs Path"), widget=BS3TextFieldWidget()),
            "certfile": StringField(lazy_gettext("Client Cert Path"), widget=BS3TextFieldWidget()),
            "keyfile": StringField(lazy_gettext("Client Key Path"), widget=BS3TextFieldWidget()),
        }

    def test_connection(self) -> tuple[bool, str]:
        """Test the RabbitMQ connection.

        :return: Tuple of ``(status, message)`` where *status* is ``True`` on success.
        :rtype: tuple[bool, str]
        """
        try:
            conn = self._establish_connection()
            conn.close()
            return True, "Connection successfully tested"
        except Exception as e:
            return False, str(e)

    def __init__(
        self,
        rmq_conn_id: str = default_conn_name,
        vhost: str | None = None,
        qos: dict | None = None,
        retry_count: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        """Create a new RMQHook instance.

        :param rmq_conn_id: Airflow connection ID for RabbitMQ.
        :type rmq_conn_id: str
        :param vhost: Override the virtual host from the connection.
        :type vhost: str | None
        :param qos: QoS settings dict with keys ``prefetch_size``, ``prefetch_count``, ``global_qos``.
        :type qos: dict | None
        :param retry_count: Number of connection retry attempts.
        :type retry_count: int
        :param retry_delay: Base delay in seconds between retries (exponential backoff).
        :type retry_delay: float
        """
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

        from apache_airflow_provider_rmq.utils.ssl import build_ssl_context

        ssl_context = build_ssl_context(extras)
        ssl_options = pika.SSLOptions(ssl_context, conn.host) if ssl_context else None

        port = conn.port if conn.port else (5671 if ssl_context else 5672)

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

    def __del__(self) -> None:
        """Auto-close connection when hook is garbage-collected."""
        if getattr(self, "_connection", None) and self._connection.is_open:
            self.close()

    def close(self) -> None:
        """Gracefully close channel and connection."""
        if self._channel and self._channel.is_open:
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

        :param dlx_exchange: Dead letter exchange name.
        :type dlx_exchange: str
        :param dlx_routing_key: Optional dead letter routing key.
        :type dlx_routing_key: str | None
        :param message_ttl: Optional per-message TTL in milliseconds.
        :type message_ttl: int | None
        :return: Dict of ``x-*`` arguments for ``queue_declare``.
        :rtype: dict[str, Any]
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
        """Declare a queue on the broker.

        :param queue_name: Name of the queue to declare.
        :type queue_name: str
        :param passive: Only check if the queue exists; do not create.
        :type passive: bool
        :param durable: Queue survives broker restart.
        :type durable: bool
        :param exclusive: Queue is used by only one connection and deleted when that connection closes.
        :type exclusive: bool
        :param auto_delete: Queue is deleted when last consumer unsubscribes.
        :type auto_delete: bool
        :param arguments: Optional ``x-*`` arguments for the queue.
        :type arguments: dict | None
        :return: Broker response frame.
        :rtype: pika.frame.Method
        """
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
        """Delete a queue from the broker.

        :param queue_name: Name of the queue to delete.
        :type queue_name: str
        :param if_unused: Only delete if the queue has no consumers.
        :type if_unused: bool
        :param if_empty: Only delete if the queue is empty.
        :type if_empty: bool
        :return: Broker response frame.
        :rtype: pika.frame.Method
        """
        channel = self.get_channel()
        return channel.queue_delete(queue=queue_name, if_unused=if_unused, if_empty=if_empty)

    def queue_bind(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: dict | None = None,
    ) -> pika.frame.Method:
        """Bind a queue to an exchange.

        :param queue: Queue name.
        :type queue: str
        :param exchange: Exchange name.
        :type exchange: str
        :param routing_key: Routing key for the binding.
        :type routing_key: str
        :param arguments: Optional binding arguments.
        :type arguments: dict | None
        :return: Broker response frame.
        :rtype: pika.frame.Method
        """
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
        """Unbind a queue from an exchange.

        :param queue: Queue name.
        :type queue: str
        :param exchange: Exchange name.
        :type exchange: str
        :param routing_key: Routing key for the binding.
        :type routing_key: str
        :param arguments: Optional binding arguments.
        :type arguments: dict | None
        :return: Broker response frame.
        :rtype: pika.frame.Method
        """
        channel = self.get_channel()
        return channel.queue_unbind(
            queue=queue, exchange=exchange, routing_key=routing_key, arguments=arguments,
        )

    def queue_purge(self, queue_name: str) -> pika.frame.Method:
        """Remove all messages from a queue.

        :param queue_name: Name of the queue to purge.
        :type queue_name: str
        :return: Broker response frame.
        :rtype: pika.frame.Method
        """
        channel = self.get_channel()
        return channel.queue_purge(queue=queue_name)

    def queue_info(self, queue_name: str) -> dict[str, Any]:
        """Get queue info via passive declare.

        :param queue_name: Name of the queue.
        :type queue_name: str
        :return: Dict with ``queue``, ``message_count``, ``consumer_count``, ``exists`` keys.
        :rtype: dict[str, Any]
        """
        channel = self.get_channel()
        try:
            result = channel.queue_declare(queue=queue_name, passive=True)
            return {
                "queue": result.method.queue,
                "message_count": result.method.message_count,
                "consumer_count": result.method.consumer_count,
                "exists": True,
            }
        except pika.exceptions.ChannelClosedByBroker as e:
            log.warning("Channel closed by broker for queue '%s': %s", queue_name, e)
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
        """Declare an exchange on the broker.

        :param exchange: Exchange name.
        :type exchange: str
        :param exchange_type: Exchange type (``direct``, ``fanout``, ``topic``, ``headers``).
        :type exchange_type: str
        :param passive: Only check if the exchange exists; do not create.
        :type passive: bool
        :param durable: Exchange survives broker restart.
        :type durable: bool
        :param auto_delete: Exchange is deleted when last queue is unbound.
        :type auto_delete: bool
        :param internal: Exchange cannot be published to directly by a client.
        :type internal: bool
        :param arguments: Optional ``x-*`` arguments.
        :type arguments: dict | None
        :return: Broker response frame.
        :rtype: pika.frame.Method
        """
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
        """Delete an exchange from the broker.

        :param exchange: Exchange name.
        :type exchange: str
        :param if_unused: Only delete if the exchange has no bindings.
        :type if_unused: bool
        :return: Broker response frame.
        :rtype: pika.frame.Method
        """
        channel = self.get_channel()
        return channel.exchange_delete(exchange=exchange, if_unused=if_unused)

    def exchange_bind(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict | None = None,
    ) -> pika.frame.Method:
        """Bind a destination exchange to a source exchange.

        :param destination: Destination exchange name.
        :type destination: str
        :param source: Source exchange name.
        :type source: str
        :param routing_key: Routing key for the binding.
        :type routing_key: str
        :param arguments: Optional binding arguments.
        :type arguments: dict | None
        :return: Broker response frame.
        :rtype: pika.frame.Method
        """
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
        """Unbind a destination exchange from a source exchange.

        :param destination: Destination exchange name.
        :type destination: str
        :param source: Source exchange name.
        :type source: str
        :param routing_key: Routing key for the binding.
        :type routing_key: str
        :param arguments: Optional binding arguments.
        :type arguments: dict | None
        :return: Broker response frame.
        :rtype: pika.frame.Method
        """
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
        """Publish a message to an exchange.

        :param exchange: Exchange name (empty string for default exchange).
        :type exchange: str
        :param routing_key: Routing key for the message.
        :type routing_key: str
        :param body: Message body; strings are encoded to UTF-8.
        :type body: str | bytes
        :param properties: Optional AMQP message properties.
        :type properties: pika.BasicProperties | None
        """
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
        """Consume up to *max_messages* from a queue.

        Messages are **not** auto-acked by default; the caller must ack/nack.

        :param queue_name: Name of the queue to consume from.
        :type queue_name: str
        :param max_messages: Maximum number of messages to consume.
        :type max_messages: int
        :param auto_ack: Automatically acknowledge messages on delivery.
        :type auto_ack: bool
        :param inactivity_timeout: Seconds of inactivity before stopping consumption.
        :type inactivity_timeout: float | None
        :return: List of dicts with ``method``, ``properties``, ``body`` (decoded str) keys.
        :rtype: list[dict[str, Any]]
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
        try:
            channel.cancel()
        except Exception:
            log.debug("Error cancelling consumer on channel", exc_info=True)
        return messages

    def ack(self, delivery_tag: int) -> None:
        """Acknowledge a message.

        :param delivery_tag: Delivery tag of the message to acknowledge.
        :type delivery_tag: int
        """
        self.get_channel().basic_ack(delivery_tag=delivery_tag)

    def nack(self, delivery_tag: int, requeue: bool = True) -> None:
        """Negatively acknowledge a message.

        :param delivery_tag: Delivery tag of the message to reject.
        :type delivery_tag: int
        :param requeue: Re-queue the message if ``True``.
        :type requeue: bool
        """
        self.get_channel().basic_nack(delivery_tag=delivery_tag, requeue=requeue)