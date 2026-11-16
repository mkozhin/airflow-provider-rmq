from __future__ import annotations

import logging
import warnings
from datetime import timedelta
from typing import Any, Callable, Literal, Sequence

import pika.exceptions
from airflow.exceptions import AirflowProviderDeprecationWarning, AirflowSkipException
from airflow.sensors.base import BaseSensorOperator

from airflow_provider_rmq.hooks.rmq import RMQHook
from airflow_provider_rmq.utils.amqp import FilterMissState, handle_miss_sync
from airflow_provider_rmq.utils.filters import FilterVerdict, MessageFilter

log = logging.getLogger("airflow.task")


class RMQSensor(BaseSensorOperator):
    """Wait for a message in a RabbitMQ queue matching optional conditions.

    Supports the ``poke`` and ``reschedule`` modes of
    :class:`~airflow.sensors.base.BaseSensorOperator` and deferrable mode (via
    :class:`~airflow_provider_rmq.triggers.rmq.RMQTrigger`). The matching message is
    returned via XCom.

    In deferrable mode the sensor's own ``timeout`` bounds the whole wait in the
    triggerer. When it runs out the scheduler ends the task with
    ``TaskDeferralError("Trigger/execution timeout")`` — FAILED and retried as the DAG
    says, every retry waiting the full ``timeout`` again — or SKIPPED with
    ``soft_fail=True``; ``execute_complete`` is not called. A ``message_wait_timeout``
    longer than ``timeout`` therefore never comes into play: the task fails by
    ``timeout`` instead of being skipped.

    A message the filter read and rejected is disposed of according to
    ``ack_unmatched``: NACKed with requeue=True by default, so it stays in the queue for
    another consumer, or ACKed and gone for good when the parameter is on. A message the
    filter could not be evaluated on — a callable that raised, a body that does not read
    as JSON — is always returned to the queue, whatever ``ack_unmatched`` says. So is the
    rest of the batch behind the matching message: the filter never read it.

    .. note::
        ``filter_callable`` is **not supported** in deferrable mode (``deferrable=True``)
        because callables cannot be serialized to the triggerer process. Use dict-based
        ``filter_headers`` instead, or set ``deferrable=False``.

    .. note::
        With ``ack_unmatched`` off, a rejected message is NACKed with requeue=True
        (``basic.nack``). A quorum queue's ``delivery-limit`` counts only
        ``basic.reject`` and failed deliveries, so a rejected message goes back to the
        head of the queue and is evaluated again, indefinitely. If heavy filtering is
        expected, use a dedicated queue with ``ack_unmatched=True``: the message is then
        acknowledged instead and never redelivered.
    """

    template_fields: Sequence[str] = ("queue_name", "message_wait_timeout")
    ui_color = "#ff6600"

    def __init__(
        self,
        *,
        queue_name: str,
        rmq_conn_id: str = "rmq_default",
        filter_headers: dict[str, Any] | None = None,
        filter_callable: Callable[[Any, str], bool] | None = None,
        deferrable: bool = False,
        poke_batch_size: int = 100,
        trigger_mode: Literal["pull", "push"] | None = None,
        message_wait_timeout: float | None = None,
        ack_unmatched: bool = False,
        **kwargs,
    ):
        """Create a new RMQSensor.

        :param queue_name: Name of the RabbitMQ queue to monitor.
        :param rmq_conn_id: Airflow connection ID for RabbitMQ.
        :param filter_headers: Dict of AMQP headers that a message must match.
        :param filter_callable: Callable ``(properties, body) -> bool`` for custom filtering.
            Not supported with ``deferrable=True``.
        :param deferrable: Run in deferrable mode using :class:`RMQTrigger`.
        :param poke_batch_size: Max messages to fetch per poke cycle.
        :param trigger_mode: How the trigger reads the queue when ``deferrable=True`` —
            ``"pull"`` (``basic_get`` in a loop with a pause on an empty queue, the
            default) or ``"push"`` (``basic_consume``, the broker sends messages as they
            arrive). Ignored without ``deferrable=True``.
        :param message_wait_timeout: Maximum seconds to wait for a matching message with
            ``trigger_mode="push"``; on expiry the task is skipped. ``None`` means wait
            until the sensor's ``timeout``. Only valid with ``trigger_mode="push"``. The
            wait may run over by at most ``rpc_timeout`` plus
            :data:`~airflow_provider_rmq.utils.amqp.CLOSE_TIMEOUT` seconds: the trigger
            cancels its consumer, returns the messages the broker had already sent and
            it had not read to the queue, and closes the connection.
        :param ack_unmatched: What to do with a message the filter rejected. ``False``
            (default) returns it to the queue. ``True`` acknowledges it instead: the
            message leaves the broker for good — DLX does not fire on an
            acknowledgement. Use it only on a queue this sensor owns. Must be a
            ``bool``: the string ``"false"`` is truthy, and the parameter destroys
            messages.

        ``mode`` is the parameter of
        :class:`~airflow.sensors.base.BaseSensorOperator` — ``"poke"`` (default) or
        ``"reschedule"`` — and the base class checks its value. ``mode="pull"`` and
        ``mode="push"`` are accepted as deprecated spellings of ``trigger_mode`` with an
        :class:`~airflow.exceptions.AirflowProviderDeprecationWarning`; the base class
        then gets its default ``"poke"``. ``mode=None`` counts as not given.
        """
        if "mode" in kwargs and kwargs["mode"] is None:
            del kwargs["mode"]
        if kwargs.get("mode") in ("pull", "push"):
            if trigger_mode is not None:
                raise ValueError(
                    f"Both trigger_mode={trigger_mode!r} and mode={kwargs['mode']!r} are "
                    "given. Pass the trigger's delivery mode as trigger_mode only; mode "
                    "takes the sensor's 'poke' or 'reschedule'."
                )
            warnings.warn(
                f"RMQSensor(mode={kwargs['mode']!r}) is deprecated; use "
                f"trigger_mode={kwargs['mode']!r}. The mode parameter takes the sensor's "
                "'poke' or 'reschedule'.",
                AirflowProviderDeprecationWarning,
                stacklevel=2,
            )
            trigger_mode = kwargs.pop("mode")
        super().__init__(**kwargs)
        self.queue_name = queue_name
        self.rmq_conn_id = rmq_conn_id
        self.filter_headers = filter_headers
        self.filter_callable = filter_callable
        self.deferrable = deferrable
        self.poke_batch_size = poke_batch_size
        self.trigger_mode = "pull" if trigger_mode is None else trigger_mode
        self.message_wait_timeout = message_wait_timeout
        self.ack_unmatched = ack_unmatched
        self._return_value: dict | None = None

        # One state per sensor instance. In ``poke`` mode BaseSensorOperator.execute reuses
        # the instance across its poke calls, so a streak of misses spanning many pokes
        # reaches the threshold, which a counter created inside poke never could past
        # poke_batch_size. In ``reschedule`` mode every poke is a task run of its own on a
        # new instance and starts from zero; poke reports each of them as a batch.
        self._miss_state: FilterMissState | None = None

        if self.trigger_mode not in ("pull", "push"):
            raise ValueError(
                f"trigger_mode must be 'pull' or 'push'; received {self.trigger_mode!r}."
            )

        if not isinstance(ack_unmatched, bool):
            raise ValueError("'ack_unmatched' must be a bool.")  # noqa: TRY004

        if self.deferrable and self.filter_callable is not None:
            raise ValueError(
                "Callable-based filters are not supported in deferrable mode "
                "because they cannot be serialized to the triggerer process. "
                "Use dict-based filter_headers instead, or disable deferrable mode."
            )

        if self.message_wait_timeout is not None and self.trigger_mode != "push":
            raise ValueError(
                "message_wait_timeout is only supported with trigger_mode='push'. "
                "Either set trigger_mode='push' or remove message_wait_timeout."
            )

    def execute(self, context: Any) -> dict | None:
        if self.deferrable:
            self._defer()
            return None  # unreachable, defer raises TaskDeferred
        super().execute(context)
        return self._return_value

    def _defer(self) -> None:
        from airflow_provider_rmq.triggers.rmq import RMQTrigger

        msg_filter = MessageFilter(filter_headers=self.filter_headers)

        timeout = float(self.message_wait_timeout) if self.message_wait_timeout is not None else None
        self.defer(
            trigger=RMQTrigger(
                rmq_conn_id=self.rmq_conn_id,
                queue_name=self.queue_name,
                filter_data=msg_filter.serialize(),
                mode=self.trigger_mode,
                message_wait_timeout=timeout,
                ack_unmatched=self.ack_unmatched,
            ),
            method_name="execute_complete",
            timeout=timedelta(seconds=self.timeout),
        )

    def execute_complete(self, context: Any, event: dict[str, Any]) -> dict | None:
        if event.get("status") == "success":
            return event.get("message")
        if event.get("status") == "timeout":
            raise AirflowSkipException(
                f"RMQSensor timed out waiting for message in queue '{self.queue_name}'"
            )
        raise RuntimeError(f"RMQTrigger failed: {event.get('error', 'unknown error')}")

    def poke(self, context: Any) -> bool:
        msg_filter = MessageFilter(
            filter_headers=self.filter_headers,
            filter_callable=self.filter_callable,
        )

        if self._miss_state is None:
            self._miss_state = FilterMissState(
                self.queue_name, log, ack_unmatched=self.ack_unmatched
            )
        miss_state = self._miss_state
        dropped_before = miss_state.dropped
        evaluated = 0

        hook = RMQHook(rmq_conn_id=self.rmq_conn_id)
        try:
            try:
                messages = hook.consume_messages(
                    queue_name=self.queue_name,
                    max_messages=self.poke_batch_size,
                    auto_ack=False,
                )
            except pika.exceptions.ChannelClosedByBroker as e:
                log.warning("Queue '%s' is not available: %s", self.queue_name, e)
                return False

            if not messages:
                return False

            for i, msg in enumerate(messages):
                verdict, reason = msg_filter.evaluate(msg["properties"], msg["body"])
                if verdict is not FilterVerdict.ERROR:
                    evaluated += 1
                if verdict is FilterVerdict.MATCH:
                    miss_state.on_match()
                    hook.ack(msg["method"].delivery_tag)
                    self._return_value = {
                        "body": msg["body"],
                        "headers": dict(msg["properties"].headers or {}),
                        "routing_key": msg["method"].routing_key or "",
                        "exchange": msg["method"].exchange or "",
                    }
                    # The filter never read the rest of the batch, so it goes back to the
                    # queue whatever ack_unmatched says — acknowledging it would delete
                    # messages that may well match.
                    for remaining in messages[i + 1:]:
                        hook.nack(remaining["method"].delivery_tag, requeue=True)
                    self._report_poke(miss_state, 1, evaluated, dropped_before)
                    return True
                if verdict is FilterVerdict.ERROR:
                    miss_state.on_error(reason)
                    hook.nack(msg["method"].delivery_tag, requeue=True)
                else:
                    handle_miss_sync(hook, msg["method"].delivery_tag, miss_state)
            self._report_poke(miss_state, 0, evaluated, dropped_before)
            return False
        finally:
            hook.close()

    def _report_poke(
        self, miss_state: FilterMissState, matched: int, evaluated: int, dropped_before: int
    ) -> None:
        """Report one poke as a batch in ``reschedule`` mode.

        :param miss_state: Counters of the sensor.
        :param matched: Messages of the poke that passed the filter.
        :param evaluated: Messages the filter reached a verdict on — a message it could
            not be evaluated on says nothing about ``filter_data``.
        :param dropped_before: ``miss_state.dropped`` when the poke started.

        Every poke in ``reschedule`` mode runs on a new sensor instance, so ``streak``
        never outlives one poke and its threshold is out of reach past
        ``poke_batch_size``; a line per poke that matched nothing is what makes the
        misses visible there. Every such poke is a task run with a log of its own, so the
        line is one per run. In ``poke`` mode the instance and its counters span every
        poke, and the thresholds do the reporting.
        """
        if not self.reschedule:
            return
        miss_state.on_batch_done(
            matched=matched,
            evaluated=evaluated,
            dropped_in_batch=miss_state.dropped - dropped_before,
        )
