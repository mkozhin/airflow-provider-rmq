from __future__ import annotations

import logging
from typing import Any, Literal, Sequence

from airflow.models import BaseOperator

from airflow_provider_rmq.hooks.rmq import RMQHook

log = logging.getLogger("airflow.task")

VALID_ACTIONS = (
    "declare_queue", "delete_queue", "purge_queue", "bind_queue", "unbind_queue",
    "declare_exchange", "delete_exchange", "bind_exchange", "unbind_exchange",
)

ActionType = Literal[
    "declare_queue", "delete_queue", "purge_queue", "bind_queue", "unbind_queue",
    "declare_exchange", "delete_exchange", "bind_exchange", "unbind_exchange",
]


class RMQQueueManagementOperator(BaseOperator):
    """Perform queue and exchange management operations on RabbitMQ.

    Supported actions:
    - Queue: declare_queue, delete_queue, purge_queue, bind_queue, unbind_queue
    - Exchange: declare_exchange, delete_exchange, bind_exchange, unbind_exchange
    """

    template_fields: Sequence[str] = ("queue_name", "exchange_name", "routing_key", "arguments")
    ui_color = "#ff6600"

    def __init__(
        self,
        *,
        action: ActionType,
        rmq_conn_id: str = "rmq_default",
        queue_name: str | None = None,
        durable: bool = True,
        exclusive: bool = False,
        auto_delete: bool = False,
        exchange_name: str | None = None,
        exchange_type: str = "direct",
        internal: bool = False,
        if_unused: bool = False,
        if_empty: bool = False,
        routing_key: str = "",
        arguments: dict | None = None,
        source_exchange: str | None = None,
        **kwargs,
    ):
        """Create a new RMQQueueManagementOperator.

        :param action: Management action to perform (e.g., ``declare_queue``, ``delete_exchange``).
        :type action: str
        :param rmq_conn_id: Airflow connection ID for RabbitMQ.
        :type rmq_conn_id: str
        :param queue_name: Queue name (required for queue actions).
        :type queue_name: str | None
        :param durable: The declared queue or exchange survives a broker restart. Applies to
            both ``declare_queue`` and ``declare_exchange``.
        :type durable: bool
        :param exclusive: Queue is exclusive to the connection. The operator closes its
            connection when the task finishes, so an exclusive queue lives only while the
            task runs.
        :type exclusive: bool
        :param auto_delete: Resource is deleted when no longer in use.
        :type auto_delete: bool
        :param exchange_name: Exchange name (required for exchange actions).
        :type exchange_name: str | None
        :param exchange_type: Exchange type (``direct``, ``fanout``, ``topic``, ``headers``).
        :type exchange_type: str
        :param internal: Exchange cannot be published to directly.
        :type internal: bool
        :param if_unused: Only delete if resource has no consumers/bindings.
        :type if_unused: bool
        :param if_empty: Only delete queue if it is empty.
        :type if_empty: bool
        :param routing_key: Routing key for bind/unbind actions.
        :type routing_key: str
        :param arguments: Optional ``x-*`` arguments.
        :type arguments: dict | None
        :param source_exchange: Source exchange for exchange bind/unbind.
        :type source_exchange: str | None
        :raises ValueError: If ``action`` is unknown, or if ``action`` is ``declare_queue``
            with both ``durable`` and ``exclusive`` set to ``False``: such a queue relies on
            the RabbitMQ deprecated feature ``transient_nonexcl_queues``, denied by default
            since RabbitMQ 4.3.
        """
        super().__init__(**kwargs)
        if action not in VALID_ACTIONS:
            raise ValueError(f"Invalid action '{action}'. Must be one of {VALID_ACTIONS}")
        if action == "declare_queue" and not durable and not exclusive:
            raise ValueError(
                f"Queue '{queue_name}': a non-durable, non-exclusive queue relies on the "
                "RabbitMQ deprecated feature 'transient_nonexcl_queues', denied by default "
                "since RabbitMQ 4.3. Declare it with durable=True. exclusive=True does not "
                "fit this operator: it closes its connection when the task finishes, and an "
                "exclusive queue is deleted together with that connection."
            )
        self.action = action
        self.rmq_conn_id = rmq_conn_id
        self.queue_name = queue_name
        self.durable = durable
        self.exclusive = exclusive
        self.auto_delete = auto_delete
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.internal = internal
        self.if_unused = if_unused
        self.if_empty = if_empty
        self.routing_key = routing_key
        self.arguments = arguments
        self.source_exchange = source_exchange

    def execute(self, context: Any) -> dict[str, Any] | None:
        with RMQHook(rmq_conn_id=self.rmq_conn_id) as hook:
            method_map = {
                "declare_queue": self._declare_queue,
                "delete_queue": self._delete_queue,
                "purge_queue": self._purge_queue,
                "bind_queue": self._bind_queue,
                "unbind_queue": self._unbind_queue,
                "declare_exchange": self._declare_exchange,
                "delete_exchange": self._delete_exchange,
                "bind_exchange": self._bind_exchange,
                "unbind_exchange": self._unbind_exchange,
            }
            result = method_map[self.action](hook)
            log.info("Action '%s' completed successfully.", self.action)
            return result

    def _declare_queue(self, hook: RMQHook) -> dict[str, Any]:
        result = hook.queue_declare(
            queue_name=self.queue_name,
            durable=self.durable,
            exclusive=self.exclusive,
            auto_delete=self.auto_delete,
            arguments=self.arguments,
        )
        return {
            "queue": result.method.queue,
            "message_count": result.method.message_count,
        }

    def _delete_queue(self, hook: RMQHook) -> None:
        hook.queue_delete(
            queue_name=self.queue_name,
            if_unused=self.if_unused,
            if_empty=self.if_empty,
        )

    def _purge_queue(self, hook: RMQHook) -> None:
        hook.queue_purge(queue_name=self.queue_name)

    def _bind_queue(self, hook: RMQHook) -> None:
        hook.queue_bind(
            queue=self.queue_name,
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            arguments=self.arguments,
        )

    def _unbind_queue(self, hook: RMQHook) -> None:
        hook.queue_unbind(
            queue=self.queue_name,
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            arguments=self.arguments,
        )

    def _declare_exchange(self, hook: RMQHook) -> None:
        hook.exchange_declare(
            exchange=self.exchange_name,
            exchange_type=self.exchange_type,
            durable=self.durable,
            auto_delete=self.auto_delete,
            internal=self.internal,
            arguments=self.arguments,
        )

    def _delete_exchange(self, hook: RMQHook) -> None:
        hook.exchange_delete(
            exchange=self.exchange_name,
            if_unused=self.if_unused,
        )

    def _bind_exchange(self, hook: RMQHook) -> None:
        hook.exchange_bind(
            destination=self.exchange_name,
            source=self.source_exchange,
            routing_key=self.routing_key,
            arguments=self.arguments,
        )

    def _unbind_exchange(self, hook: RMQHook) -> None:
        hook.exchange_unbind(
            destination=self.exchange_name,
            source=self.source_exchange,
            routing_key=self.routing_key,
            arguments=self.arguments,
        )