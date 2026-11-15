"""Gate, safety rules and fixtures for the broker-backed integration suite.

The suite talks to a real RabbitMQ and is therefore kept out of ``testpaths``:
``pytest tests/`` never collects it. It runs only when ``RMQ_TEST_URL`` names a
broker, and only inside a dedicated vhost — a URL without a vhost, or with the
default ``/``, aborts the run instead of touching a shared namespace.

Everything a test creates carries the ``rmqtest.{pid}.{uuid8}.`` prefix of its
:class:`BrokerScratch` and is deleted in teardown.
"""

from __future__ import annotations

import logging
import os
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any
from urllib.parse import unquote, urlsplit

import aio_pika
import pika
import pytest
from pika.adapters.blocking_connection import BlockingChannel

log = logging.getLogger(__name__)

#: Environment variable holding the AMQP URL of the test broker.
URL_ENV = "RMQ_TEST_URL"

#: Dotenv-style file read for the variable above, at the repository root.
ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

#: This directory. Only the items collected from it are gated and configured here.
INTEGRATION_DIR = Path(__file__).resolve().parent

#: Airflow connection id published by the :func:`rmq_conn_id` fixture.
CONN_ID = "rmq_integration"

SKIP_REASON = (
    f"{URL_ENV} is not set: the integration suite needs a live broker. "
    f"Put the AMQP URL into {ENV_FILE} or into the environment."
)


def _read_env_file(path: Path) -> dict[str, str]:
    """Parse a dotenv-style file into a dict.

    Comments, blank lines and lines without ``=`` are ignored; surrounding
    quotes are stripped from values. Written out here because the suite must
    not add ``python-dotenv`` to the project's dependencies.
    """
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip("'\"")
    return values


def _load_env() -> None:
    """Copy the suite's variable from the dotenv file into the environment.

    A value already present in the environment wins, so a one-off run can point
    the suite at another broker without editing the file.
    """
    value = _read_env_file(ENV_FILE).get(URL_ENV)
    if value and not os.environ.get(URL_ENV):
        os.environ[URL_ENV] = value


def vhost_of(url: str) -> str:
    """Return the vhost of an AMQP URL — the path segment, percent-decoded."""
    return unquote(urlsplit(url).path.lstrip("/"))


def _require_dedicated_vhost(url: str) -> str:
    """Return the vhost of *url*, refusing the default one and the empty one.

    The broker this suite is pointed at sits next to production queues, and the
    suite declares and deletes objects. A dedicated vhost is what keeps those
    two apart, so its absence is a hard stop rather than a skip.
    """
    vhost = vhost_of(url)
    if not vhost or vhost == "/":
        raise pytest.UsageError(
            f"{URL_ENV} must name a dedicated vhost (the last path segment); "
            f"got {'no vhost' if not vhost else 'the default vhost /'}. "
            "The integration suite declares and deletes broker objects and "
            "refuses to do that outside a vhost of its own."
        )
    return vhost


def _is_mine(item: pytest.Item) -> bool:
    """Tell whether *item* was collected from this directory."""
    path = getattr(item, "path", None)
    return path is not None and INTEGRATION_DIR in Path(path).resolve().parents


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Read the dotenv file, skip this directory without a broker, refuse a shared vhost.

    A directory-level hook is handed the whole session's items, so everything here
    is decided on this directory's own items: ``pytest tests integration`` without a
    broker has to skip the integration tests and run the unit suite, not skip both.
    The dotenv file is read, and the vhost refused, only when there is something here
    to run.
    """
    mine = [item for item in items if _is_mine(item)]
    if not mine:
        return
    _load_env()
    url = os.environ.get(URL_ENV)
    if not url:
        skip = pytest.mark.skip(reason=SKIP_REASON)
        for item in mine:
            item.add_marker(skip)
        return
    _require_dedicated_vhost(url)


class BrokerScratch:
    """Namespace of one test run on the broker, deleted whole in teardown.

    Every name handed out starts with ``prefix``; a name that does not is
    rejected, so a typo cannot reach a queue the suite did not create.
    """

    def __init__(self, prefix: str, connection: pika.BlockingConnection) -> None:
        self.prefix = prefix
        self._connection = connection
        self._queues: list[str] = []

    @property
    def queues(self) -> tuple[str, ...]:
        """Queues this run declared and has not deleted yet."""
        return tuple(self._queues)

    # --- names ---

    def name(self, suffix: str) -> str:
        """Return *suffix* prefixed with this run's namespace."""
        return f"{self.prefix}{suffix}"

    def _guard(self, name: str) -> str:
        if not name.startswith(self.prefix):
            raise ValueError(f"{name!r} is outside this run's namespace {self.prefix!r}")
        return name

    def register_queue(self, name: str) -> str:
        """Record a queue declared elsewhere (aio_pika, an operator) for teardown."""
        self._guard(name)
        if name not in self._queues:
            self._queues.append(name)
        return name

    # --- topology ---

    def declare_queue(
        self,
        suffix: str,
        *,
        durable: bool = True,
        arguments: dict[str, Any] | None = None,
    ) -> str:
        """Declare a queue named after *suffix* and return its full name.

        The queue is durable by default: RabbitMQ 4.3+ refuses non-durable,
        non-exclusive queues unless the deprecated ``transient_nonexcl_queues``
        feature is permitted on the broker.
        """
        name = self.register_queue(self.name(suffix))
        channel = self._connection.channel()
        try:
            channel.queue_declare(queue=name, durable=durable, arguments=arguments)
        finally:
            if channel.is_open:
                channel.close()
        return name

    def publish(self, queue: str, body: bytes, headers: dict[str, Any] | None = None) -> None:
        """Publish one message straight to *queue* via the default exchange."""
        self._guard(queue)
        channel = self._connection.channel()
        try:
            channel.basic_publish(
                exchange="",
                routing_key=queue,
                body=body,
                properties=pika.BasicProperties(headers=headers) if headers else None,
            )
        finally:
            if channel.is_open:
                channel.close()

    def depth(self, queue: str) -> int:
        """Return the number of ready messages in *queue*.

        Measured with a passive ``queue_declare``: the ``messages`` field of the
        Management API lags behind by the statistics collection interval, so it
        cannot carry an assertion. The declare runs on a channel of its own —
        a passive declare of a missing queue closes the channel it runs on.
        """
        self._guard(queue)
        channel = self._connection.channel()
        try:
            return channel.queue_declare(queue=queue, passive=True).method.message_count
        finally:
            if channel.is_open:
                channel.close()

    # --- teardown ---

    def teardown(self) -> None:
        """Delete every queue this run declared, each on a channel of its own.

        One queue that refuses to go must not take the rest of the namespace with
        it: every queue is attempted, a deleted one leaves the register, and the
        names still standing are both kept in the register and named in the error
        raised at the end, so a prod-adjacent vhost never keeps a queue whose name
        is nowhere to be found.
        """
        failures: list[tuple[str, BaseException]] = []
        for queue in list(self._queues):
            try:
                channel = self._connection.channel()
                try:
                    channel.queue_delete(queue=queue)
                finally:
                    if channel.is_open:
                        channel.close()
            except Exception as exc:  # noqa: BLE001 - every queue is attempted
                failures.append((queue, exc))
                log.error("failed to delete test queue %s: %r", queue, exc)
            else:
                self._queues.remove(queue)
        if failures:
            left = ", ".join(name for name, _ in failures)
            raise RuntimeError(
                f"{len(failures)} test queue(s) left on the broker, delete them by "
                f"hand: {left}"
            )


# --- broker coordinates ---


@pytest.fixture(scope="session")
def rmq_url() -> str:
    """AMQP URL of the test broker, with a dedicated vhost."""
    url = os.environ.get(URL_ENV)
    if not url:
        pytest.skip(SKIP_REASON)
    _require_dedicated_vhost(url)
    return url


@pytest.fixture(scope="session")
def rmq_conn_id(rmq_url: str) -> Iterator[str]:
    """Publish the broker as an Airflow connection and return its id.

    The operator and the sensor reach the broker through ``RMQHook``, which
    resolves a ``conn_id``; the environment backend gives them one without a
    metadata database.
    """
    variable = f"AIRFLOW_CONN_{CONN_ID.upper()}"
    previous = os.environ.get(variable)
    os.environ[variable] = rmq_url
    try:
        yield CONN_ID
    finally:
        if previous is None:
            os.environ.pop(variable, None)
        else:
            os.environ[variable] = previous


# --- connections ---


@pytest.fixture
def pika_connection(rmq_url: str) -> Iterator[pika.BlockingConnection]:
    """Blocking pika connection — the path the operator and the sensor take."""
    connection = pika.BlockingConnection(pika.URLParameters(rmq_url))
    try:
        yield connection
    finally:
        if connection.is_open:
            connection.close()


@pytest.fixture
def pika_channel(pika_connection: pika.BlockingConnection) -> Iterator[BlockingChannel]:
    """Channel on :func:`pika_connection`."""
    channel = pika_connection.channel()
    try:
        yield channel
    finally:
        if channel.is_open:
            channel.close()


@pytest.fixture
async def aio_connection(rmq_url: str) -> AsyncIterator[aio_pika.abc.AbstractRobustConnection]:
    """Robust aio_pika connection — the path the watcher and the trigger take."""
    connection = await aio_pika.connect_robust(rmq_url)
    try:
        yield connection
    finally:
        await connection.close()


@pytest.fixture
async def aio_channel(
    aio_connection: aio_pika.abc.AbstractRobustConnection,
) -> AsyncIterator[aio_pika.abc.AbstractChannel]:
    """Channel on :func:`aio_connection`."""
    channel = await aio_connection.channel()
    try:
        yield channel
    finally:
        await channel.close()


# --- namespace ---


@pytest.fixture
def rmq_prefix() -> str:
    """Prefix unique to this test: ``rmqtest.{pid}.{uuid8}.``."""
    return f"rmqtest.{os.getpid()}.{uuid.uuid4().hex[:8]}."


@pytest.fixture
def scratch(pika_connection: pika.BlockingConnection, rmq_prefix: str) -> Iterator[BrokerScratch]:
    """Namespace for the queues of one test, deleted afterwards.

    Finalized before ``pika_connection``, so the teardown runs on an open connection.
    """
    namespace = BrokerScratch(prefix=rmq_prefix, connection=pika_connection)
    try:
        yield namespace
    finally:
        namespace.teardown()
