from __future__ import annotations

import json
import logging
from typing import Any, Sequence

import pika
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from pika.exceptions import ChannelClosedByBroker, NackError, UnroutableError

from airflow_provider_rmq.hooks.rmq import RMQHook

log = logging.getLogger("airflow.task")


class RMQPublishOperator(BaseOperator):
    """Publish one or more messages to RabbitMQ.

    Supports publishing to a named exchange or directly to a queue
    (via default exchange with routing_key=queue_name).

    Messages can be strings, dicts (auto-serialized to JSON), or lists thereof.

    With ``confirm=True`` every message waits for the broker acknowledgement, and the
    task fails when the broker returns, rejects or cannot accept a message. Messages
    published before the failing one stay in the broker, so a retry publishes them again
    (at-least-once).
    """

    template_fields: Sequence[str] = ("exchange", "routing_key", "message")
    ui_color = "#ff6600"

    def __init__(
        self,
        *,
        rmq_conn_id: str = "rmq_default",
        exchange: str = "",
        routing_key: str = "",
        message: str | list[str] | dict | list[dict] | None = None,
        queue_name: str | None = None,
        content_type: str | None = None,
        delivery_mode: int | None = None,
        headers: dict | None = None,
        priority: int | None = None,
        expiration: str | None = None,
        correlation_id: str | None = None,
        reply_to: str | None = None,
        message_id: str | None = None,
        confirm: bool = True,
        mandatory: bool | None = None,
        **kwargs,
    ):
        """Create a new RMQPublishOperator.

        :param rmq_conn_id: Airflow connection ID for RabbitMQ.
        :type rmq_conn_id: str
        :param exchange: Exchange to publish to (empty string for default exchange).
        :type exchange: str
        :param routing_key: Routing key for the message.
        :type routing_key: str
        :param message: Message payload — string, dict, or list thereof. Dicts are JSON-serialized.
        :type message: str | list[str] | dict | list[dict] | None
        :param queue_name: Shortcut — sets ``exchange=""`` and ``routing_key=queue_name``.
        :type queue_name: str | None
        :param content_type: AMQP content type header.
        :type content_type: str | None
        :param delivery_mode: ``1`` for non-persistent, ``2`` for persistent.
        :type delivery_mode: int | None
        :param headers: Custom AMQP headers.
        :type headers: dict | None
        :param priority: Message priority (0–9).
        :type priority: int | None
        :param expiration: Per-message TTL in milliseconds (as string).
        :type expiration: str | None
        :param correlation_id: Application correlation identifier.
        :type correlation_id: str | None
        :param reply_to: Reply-to queue name.
        :type reply_to: str | None
        :param message_id: Application message identifier.
        :type message_id: str | None
        :param confirm: Publish with publisher confirms: wait for the broker to acknowledge
            each message and fail the task on a returned, rejected or refused message.
        :type confirm: bool
        :param mandatory: Ask the broker to return a message that no queue matches.
            ``None`` resolves at execution, after templates are rendered: ``True`` for the
            default exchange (``exchange=""`` or ``queue_name=``) with ``confirm=True``,
            ``False`` otherwise. ``True`` requires ``confirm=True``.
        :type mandatory: bool | None
        :raises ValueError: If ``mandatory=True`` is combined with ``confirm=False``.
        """
        super().__init__(**kwargs)
        if mandatory and not confirm:
            raise ValueError(
                "mandatory=True requires confirm=True: without publisher confirms "
                "a returned message is not reported to the task"
            )
        self.rmq_conn_id = rmq_conn_id
        if queue_name:
            self.exchange = ""
            self.routing_key = queue_name
        else:
            self.exchange = exchange
            self.routing_key = routing_key
        self.message = message
        self.content_type = content_type
        self.delivery_mode = delivery_mode
        self.headers = headers
        self.priority = priority
        self.expiration = expiration
        self.correlation_id = correlation_id
        self.reply_to = reply_to
        self.message_id = message_id
        self.confirm = confirm
        self.mandatory = mandatory

    def execute(self, context: Any) -> None:
        properties = pika.BasicProperties(
            content_type=self.content_type,
            delivery_mode=self.delivery_mode,
            headers=self.headers,
            priority=self.priority,
            expiration=self.expiration,
            correlation_id=self.correlation_id,
            reply_to=self.reply_to,
            message_id=self.message_id,
        )

        messages = self._normalize_messages()
        if not messages:
            log.warning(
                "Message is empty, nothing to publish to exchange='%s' routing_key='%s'",
                self.exchange,
                self.routing_key,
            )
            return

        mandatory = self._resolve_mandatory()
        total = len(messages)

        with RMQHook(rmq_conn_id=self.rmq_conn_id, publisher_confirms=self.confirm) as hook:
            for number, msg in enumerate(messages, start=1):
                try:
                    hook.basic_publish(
                        exchange=self.exchange,
                        routing_key=self.routing_key,
                        body=msg,
                        properties=properties,
                        mandatory=mandatory,
                    )
                except (UnroutableError, NackError, ChannelClosedByBroker) as exc:
                    # UnroutableError and NackError need confirms; without them only
                    # ChannelClosedByBroker gets here, and it can surface on a later
                    # publish than the one that caused it, so the message number would
                    # mislead: it is raised as is.
                    if not self.confirm:
                        raise
                    raise AirflowException(self._failure_text(number, total, exc)) from exc
                log.info(
                    "Published message to exchange='%s' routing_key='%s'",
                    self.exchange,
                    self.routing_key,
                )

    def _resolve_mandatory(self) -> bool:
        """Return the ``mandatory`` flag for the rendered exchange.

        An explicit value wins. Otherwise publishing to the default exchange with
        confirms is mandatory: a message to a missing queue there is always lost.

        :return: Value passed as ``mandatory`` to every publish.
        :rtype: bool
        """
        if self.mandatory is not None:
            return self.mandatory
        return self.confirm and self.exchange == ""

    def _failure_text(
        self,
        number: int,
        total: int,
        exc: UnroutableError | NackError | ChannelClosedByBroker,
    ) -> str:
        """Describe a publish the broker did not accept.

        :param number: One-based number of the failed message.
        :type number: int
        :param total: Number of messages in the batch.
        :type total: int
        :param exc: Exception raised by pika for this message.
        :type exc: UnroutableError | NackError | ChannelClosedByBroker
        :return: Text for the task failure.
        :rtype: str
        """
        target = (
            f"message {number} of {total} to exchange='{self.exchange}' "
            f"routing_key='{self.routing_key}'"
        )
        if isinstance(exc, UnroutableError):
            method = exc.messages[0].method
            return (
                f"{target} was returned by the broker: "
                f"{method.reply_code} {method.reply_text}"
            )
        if isinstance(exc, NackError):
            return f"{target} was rejected by the broker (nack)"
        return (
            f"{target} was not published, broker closed the channel: "
            f"{exc.reply_code} {exc.reply_text}"
        )

    def _normalize_messages(self) -> list[str]:
        """Convert message input to a list of string payloads.

        :return: List of string-encoded messages ready for publishing.
        :rtype: list[str]
        """
        if self.message is None:
            return []
        if isinstance(self.message, list):
            return [
                json.dumps(m) if isinstance(m, dict) else str(m)
                for m in self.message
            ]
        if isinstance(self.message, dict):
            return [json.dumps(self.message)]
        return [str(self.message)]