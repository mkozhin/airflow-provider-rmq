from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from unittest.mock import MagicMock, PropertyMock

import pika
import pika.spec
import pytest


@dataclass
class FakeAirflowConnection:
    """Minimal stand-in for airflow.models.Connection."""

    conn_id: str = "rmq_default"
    host: str = "localhost"
    port: int = 5672
    login: str = "guest"
    password: str = "guest"
    schema: str = "/"
    extra: str = "{}"

    @property
    def extra_dejson(self) -> dict[str, Any]:
        import json
        return json.loads(self.extra) if self.extra else {}


@pytest.fixture
def fake_connection():
    """Return a default FakeAirflowConnection. Override fields as needed."""
    return FakeAirflowConnection()


@pytest.fixture
def mock_channel():
    """Return a MagicMock pretending to be a pika BlockingChannel."""
    channel = MagicMock()
    type(channel).is_open = PropertyMock(return_value=True)
    type(channel).is_closed = PropertyMock(return_value=False)
    return channel


@pytest.fixture
def mock_connection(mock_channel):
    """Return a MagicMock pretending to be a pika BlockingConnection."""
    conn = MagicMock()
    type(conn).is_open = PropertyMock(return_value=True)
    type(conn).is_closed = PropertyMock(return_value=False)
    conn.channel.return_value = mock_channel
    return conn


def make_method_frame(
    delivery_tag: int = 1,
    routing_key: str = "test.key",
    exchange: str = "",
) -> pika.spec.Basic.Deliver:
    """Create a pika Basic.Deliver method frame."""
    method = MagicMock(spec=pika.spec.Basic.Deliver)
    method.delivery_tag = delivery_tag
    method.routing_key = routing_key
    method.exchange = exchange
    return method


def make_properties(headers: dict[str, Any] | None = None) -> pika.spec.BasicProperties:
    """Create pika BasicProperties with optional headers."""
    props = MagicMock(spec=pika.spec.BasicProperties)
    props.headers = headers
    return props


def make_message(
    body: str = "test body",
    delivery_tag: int = 1,
    routing_key: str = "test.key",
    exchange: str = "",
    headers: dict[str, Any] | None = None,
) -> tuple:
    """Create a (method_frame, properties, body_bytes) tuple like pika yields."""
    return (
        make_method_frame(delivery_tag=delivery_tag, routing_key=routing_key, exchange=exchange),
        make_properties(headers=headers),
        body.encode("utf-8"),
    )