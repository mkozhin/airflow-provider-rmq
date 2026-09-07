from __future__ import annotations

import enum
import json
import logging
from typing import Any, Callable, Protocol

log = logging.getLogger("airflow.task")


class FilterVerdict(enum.Enum):
    """Outcome of evaluating one message against a filter.

    ``MISS`` means the filter read the message and rejected it; ``ERROR`` means the
    filter could not be evaluated at all — a raising ``filter_callable`` or a body
    that does not read as JSON. Only ``MISS`` may be acknowledged: an ``ERROR``
    message is returned to the queue whatever the subscription asked for.
    """

    MATCH = "match"
    MISS = "miss"
    ERROR = "error"


class HasHeaders(Protocol):
    """Protocol for objects that have a headers attribute (pika BasicProperties, aio_pika shim)."""

    headers: dict[str, Any] | None


class MessageFilter:
    """Evaluate whether a RabbitMQ message matches given filter conditions.

    Supports two modes:
    1. Dict-based filtering: {"header_key": "value", "body.nested.key": "value"}
       - Keys starting with "body." traverse the JSON-parsed message body.
       - All other keys check the message properties.headers dict.
    2. Callable-based filtering: fn(properties, body_str) -> bool

    Both can be combined (AND logic: both must pass).
    """

    def __init__(
        self,
        filter_headers: dict[str, Any] | None = None,
        filter_callable: Callable[[Any, str], bool] | None = None,
    ):
        self._filter_headers = filter_headers
        self._filter_callable = filter_callable

    @property
    def has_filters(self) -> bool:
        return self._filter_headers is not None or self._filter_callable is not None

    def matches(self, properties: HasHeaders, body: str) -> bool:
        """Return True if the message passes all configured filters."""
        return self.evaluate(properties, body)[0] is FilterVerdict.MATCH

    def evaluate(self, properties: HasHeaders, body: str) -> tuple[FilterVerdict, str | None]:
        """Evaluate the message and tell a rejection apart from a failure to evaluate.

        :returns: ``(verdict, reason)``. ``reason`` carries the type and text of the
            exception behind an ``ERROR`` and is ``None`` for ``MATCH`` and ``MISS``.

        The reason goes to DEBUG here; the throttled WARNING naming the queue is the
        caller's, so that one series is not logged twice.
        """
        if not self.has_filters:
            return FilterVerdict.MATCH, None

        if self._filter_headers is not None:
            verdict, reason = self._evaluate_dict(properties, body)
            if verdict is not FilterVerdict.MATCH:
                return verdict, reason

        if self._filter_callable is not None:
            try:
                if not self._filter_callable(properties, body):
                    return FilterVerdict.MISS, None
            except Exception as exc:
                reason = f"{type(exc).__name__}: {exc}"
                log.debug("Filter callable raised an exception: %s", reason, exc_info=True)
                return FilterVerdict.ERROR, reason

        return FilterVerdict.MATCH, None

    def _evaluate_dict(self, properties: HasHeaders, body: str) -> tuple[FilterVerdict, str | None]:
        """Evaluate dict-based filter in two phases: headers first, body once and only after.

        Header conditions are checked in full before the body is touched, so the verdict
        does not depend on the order of the keys and the body of a message already
        rejected by a header is never parsed.
        """
        headers = properties.headers or {}
        body_conditions: list[tuple[str, Any]] = []

        for key, expected_value in self._filter_headers.items():
            if key.startswith("body."):
                body_conditions.append((key, expected_value))
            elif headers.get(key) != expected_value:
                return FilterVerdict.MISS, None

        if not body_conditions:
            return FilterVerdict.MATCH, None

        try:
            parsed_body = json.loads(body)
        except (json.JSONDecodeError, TypeError) as exc:
            reason = f"{type(exc).__name__}: {exc}"
            log.debug("Message body could not be read as JSON: %s", reason)
            return FilterVerdict.ERROR, reason

        for key, expected_value in body_conditions:
            # Navigate nested keys: "body.data.status" -> parsed_body["data"]["status"]
            path = key[5:].split(".")
            current = parsed_body
            for part in path:
                if isinstance(current, dict) and part in current:
                    current = current[part]
                else:
                    return FilterVerdict.MISS, None
            if current != expected_value:
                return FilterVerdict.MISS, None
        return FilterVerdict.MATCH, None

    def serialize(self) -> dict[str, Any]:
        """Serialize for use in Trigger (must be JSON-serializable).

        Note: callable filters cannot be serialized; only dict filters are passed to triggers.
        """
        return {"filter_headers": self._filter_headers}

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> MessageFilter:
        """Reconstruct MessageFilter from serialized data (dict-based only)."""
        return cls(filter_headers=data.get("filter_headers"))