"""Teardown of :class:`BrokerScratch` — no queue is lost on the way out.

The vhost is prod-adjacent, so the register of declared queues is the only trace
of what the run has to clean up. These tests drive the teardown with a stand-in
connection instead of the broker: they are about what happens when a delete
fails, and a broker that fails a delete on demand is not something the suite can
ask for.

They live here rather than beside the class they cover, because they need no
broker and the unit suite is what every automated pass runs: the guard against
leaking queues into that vhost is worth checking on a machine that has no broker
to leak into.
"""

from __future__ import annotations

import pytest

from integration.conftest import BrokerScratch

PREFIX = "rmqtest.0.deadbeef."


class FakeChannel:
    """Channel that deletes queues into a list, failing the ones it is told to."""

    def __init__(self, deleted: list[str], failing: set[str]) -> None:
        self._deleted = deleted
        self._failing = failing
        self.is_open = True

    def queue_delete(self, queue: str) -> None:
        if queue in self._failing:
            self.is_open = False
            raise RuntimeError(f"broker refused to delete {queue}")
        self._deleted.append(queue)

    def close(self) -> None:
        self.is_open = False


class FakeConnection:
    """Connection handing out :class:`FakeChannel`, or refusing to open one."""

    def __init__(self, failing: set[str] | None = None, *, broken: bool = False) -> None:
        self.deleted: list[str] = []
        self._failing = failing or set()
        self._broken = broken

    def channel(self) -> FakeChannel:
        if self._broken:
            raise RuntimeError("connection is closed")
        return FakeChannel(self.deleted, self._failing)


def _scratch(connection: FakeConnection, *names: str) -> BrokerScratch:
    namespace = BrokerScratch(prefix=PREFIX, connection=connection)
    for name in names:
        namespace.register_queue(PREFIX + name)
    return namespace


def test_a_clean_teardown_deletes_everything_and_empties_the_register():
    connection = FakeConnection()
    namespace = _scratch(connection, "a", "b", "c")

    namespace.teardown()

    assert connection.deleted == [PREFIX + "a", PREFIX + "b", PREFIX + "c"]
    assert namespace.queues == ()


def test_one_failing_delete_does_not_stop_the_others():
    """A transient error on one queue must not leak the queues behind it."""
    connection = FakeConnection(failing={PREFIX + "b"})
    namespace = _scratch(connection, "a", "b", "c")

    with pytest.raises(RuntimeError) as excinfo:
        namespace.teardown()

    assert connection.deleted == [PREFIX + "a", PREFIX + "c"]
    assert PREFIX + "b" in str(excinfo.value)


def test_a_queue_that_survived_stays_in_the_register():
    """The name is the only way back to it, so teardown keeps it — and a second
    attempt on a healthy connection finishes the job."""
    namespace = _scratch(FakeConnection(failing={PREFIX + "b"}), "a", "b", "c")
    with pytest.raises(RuntimeError):
        namespace.teardown()
    assert namespace.queues == (PREFIX + "b",)

    healthy = FakeConnection()
    retry = BrokerScratch(prefix=PREFIX, connection=healthy)
    for name in namespace.queues:
        retry.register_queue(name)
    retry.teardown()

    assert healthy.deleted == [PREFIX + "b"]
    assert retry.queues == ()


def test_a_broken_connection_names_every_queue_left_behind():
    namespace = _scratch(FakeConnection(broken=True), "a", "b")

    with pytest.raises(RuntimeError) as excinfo:
        namespace.teardown()

    message = str(excinfo.value)
    assert PREFIX + "a" in message
    assert PREFIX + "b" in message
    assert namespace.queues == (PREFIX + "a", PREFIX + "b")
