from __future__ import annotations

import pytest

from airflow_provider_rmq.watcher.subscription_builder import (
    build_subscriptions,
    has_exchange_conflict,
)


class TestBuildSubscriptionsMutex:
    def test_exchange_and_queue_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            build_subscriptions(dag_id="d", queue="q", exchange="ex", routing_keys=["a.b"])

    def test_exchange_and_queues_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            build_subscriptions(dag_id="d", queues=["q"], exchange="ex", routing_keys=["a.b"])

    def test_queue_and_queues_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            build_subscriptions(dag_id="d", queue="q", queues=["q"])

    def test_none_given_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            build_subscriptions(dag_id="d")

    def test_all_three_given_raises(self):
        with pytest.raises(ValueError, match="exactly one"):
            build_subscriptions(dag_id="d", queue="q", queues=["q"], exchange="ex")


class TestBuildSubscriptionsCooldown:
    def test_negative_cooldown_raises_queue_mode(self):
        with pytest.raises(ValueError, match="cooldown"):
            build_subscriptions(dag_id="d", queue="q", cooldown=-1)

    def test_negative_cooldown_raises_exchange_mode(self):
        with pytest.raises(ValueError, match="cooldown"):
            build_subscriptions(
                dag_id="d", exchange="ex", routing_keys=["a.b"], cooldown=-1
            )

    def test_zero_cooldown_ok(self):
        subs = build_subscriptions(dag_id="d", queue="q", cooldown=0)
        assert subs[0]["cooldown"] == 0

    def test_non_int_cooldown_string_raises_value_error(self):
        with pytest.raises(ValueError, match="cooldown"):
            build_subscriptions(dag_id="d", queue="q", cooldown="abc")

    def test_non_int_cooldown_list_raises_value_error(self):
        with pytest.raises(ValueError, match="cooldown"):
            build_subscriptions(dag_id="d", queue="q", cooldown=[1, 2])

    def test_non_int_cooldown_float_raises_value_error(self):
        with pytest.raises(ValueError, match="cooldown"):
            build_subscriptions(dag_id="d", queue="q", cooldown=1.5)

    def test_bool_cooldown_raises_value_error(self):
        # bool is a subclass of int in Python — explicitly rejected so
        # cooldown=True/False can't silently slip through as 1/0.
        with pytest.raises(ValueError, match="cooldown"):
            build_subscriptions(dag_id="d", queue="q", cooldown=True)


class TestBuildSubscriptionsQueueItemTypes:
    def test_non_string_queue_raises(self):
        with pytest.raises(ValueError, match="queue"):
            build_subscriptions(dag_id="d", queue=123)

    def test_queues_with_non_string_items_raises(self):
        with pytest.raises(ValueError, match="queues"):
            build_subscriptions(dag_id="d", queues=[1, 2, 3])

    def test_queues_with_mixed_string_and_non_string_raises(self):
        with pytest.raises(ValueError, match="queues"):
            build_subscriptions(dag_id="d", queues=["a", 2])

    def test_queues_with_empty_string_item_raises(self):
        with pytest.raises(ValueError, match="queues"):
            build_subscriptions(dag_id="d", queues=["a", ""])

    def test_queues_as_plain_string_raises(self):
        # A plausible typo — forgetting the list brackets — must not be
        # silently accepted: iterating a str yields one-character strings,
        # which would expand into bogus per-character subscriptions instead
        # of raising.
        with pytest.raises(ValueError, match="queues"):
            build_subscriptions(dag_id="d", queues="abc")

    def test_queues_as_dict_raises(self):
        with pytest.raises(ValueError, match="queues"):
            build_subscriptions(dag_id="d", queues={"a": 1, "b": 2})

    def test_queues_empty_list_raises(self):
        # queues=[] passes the queue/queues/exchange mutex check (it is not
        # None) but must not silently produce zero subscriptions — that is
        # config loss with no warning anywhere in the pipeline.
        with pytest.raises(ValueError, match="queues"):
            build_subscriptions(dag_id="d", queues=[])


class TestBuildSubscriptionsExchangeValidation:
    def test_no_routing_keys_or_ids_raises(self):
        with pytest.raises(ValueError, match="routing_keys.*routing_key_ids|routing_key_ids.*routing_keys"):
            build_subscriptions(dag_id="d", exchange="ex")

    def test_routing_keys_empty_list_raises(self):
        with pytest.raises(ValueError, match="routing_keys"):
            build_subscriptions(dag_id="d", exchange="ex", routing_keys=[])

    def test_routing_key_ids_empty_list_raises(self):
        with pytest.raises(ValueError, match="routing_key_ids"):
            build_subscriptions(dag_id="d", exchange="ex", routing_key_ids=[])

    def test_routing_keys_with_empty_string_raises(self):
        with pytest.raises(ValueError, match="non-empty string"):
            build_subscriptions(dag_id="d", exchange="ex", routing_keys=["", "valid.key"])

    def test_empty_string_exchange_raises(self):
        # An empty exchange name is the AMQP default exchange — declaring it
        # would fail far downstream at the broker with a confusing error
        # instead of failing fast and clearly at decoration time.
        with pytest.raises(ValueError, match="exchange"):
            build_subscriptions(dag_id="d", exchange="", routing_keys=["a.b"])

    def test_exchange_reserved_prefix_raises(self):
        with pytest.raises(ValueError, match="rmq_watcher\\."):
            build_subscriptions(
                dag_id="d", exchange="rmq_watcher.sub.foo", routing_keys=["a.b"]
            )

    def test_dot_in_routing_key_ids_raises(self):
        with pytest.raises(ValueError, match="routing_key_ids"):
            build_subscriptions(dag_id="d", exchange="ex", routing_key_ids=["abc.def"])

    def test_dot_in_routing_key_status_raises(self):
        with pytest.raises(ValueError, match="routing_key_status"):
            build_subscriptions(
                dag_id="d",
                exchange="ex",
                routing_key_ids=["abc"],
                routing_key_status="foo.bar",
            )

    def test_dots_allowed_in_literal_routing_keys(self):
        subs = build_subscriptions(
            dag_id="d", exchange="ex", routing_keys=["region.eu.alert"]
        )
        assert subs[0]["routing_keys"] == ["region.eu.alert"]

    def test_routing_key_ids_with_empty_status_list_raises(self):
        # routing_key_status=[] collapses the id x status cross-product to
        # nothing — without this guard the function would silently return
        # routing_keys=[], which downstream unbinds every existing routing
        # key from the sub queue and binds none (full silent unsubscribe).
        with pytest.raises(ValueError, match="routing key"):
            build_subscriptions(
                dag_id="d",
                exchange="jetstat.airflow",
                routing_key_ids=["abc123"],
                routing_key_status=[],
            )

    def test_non_string_exchange_raises_value_error(self):
        # exchange=123 (e.g. a typo'd literal) must not reach
        # exchange.startswith(...) and raise an uncaught AttributeError —
        # that would propagate out of the AST parser's except ValueError
        # and crash the entire reconcile cycle for every DAG, forever
        # (the broken file's mtime never gets recorded as "seen").
        with pytest.raises(ValueError, match="exchange"):
            build_subscriptions(dag_id="d", exchange=123, routing_keys=["a.b"])

    def test_non_string_non_list_routing_key_status_raises_value_error(self):
        # routing_key_status=123 must not reach list(routing_key_status) and
        # raise an uncaught TypeError for the same reason as above.
        with pytest.raises(ValueError, match="routing_key_status"):
            build_subscriptions(
                dag_id="d",
                exchange="ex",
                routing_key_ids=["abc"],
                routing_key_status=123,
            )

    def test_routing_keys_as_plain_string_raises_value_error(self):
        # A plausible typo — forgetting the list brackets — must not be
        # silently accepted: iterating a str yields one-character strings,
        # which would expand into a massive over-match of single-character
        # wildcard routing keys (e.g. "l.*", "i.*", "t.*", ...).
        with pytest.raises(ValueError, match="routing_keys"):
            build_subscriptions(dag_id="d", exchange="ex", routing_keys="literal.string")

    def test_routing_key_ids_as_plain_string_raises_value_error(self):
        with pytest.raises(ValueError, match="routing_key_ids"):
            build_subscriptions(
                dag_id="d",
                exchange="jetstat.airflow",
                routing_key_ids="670f877702775c2de8325b1f",
            )


class TestBuildSubscriptionsExchangeRoutingKeys:
    def test_default_status_wildcard(self):
        subs = build_subscriptions(dag_id="d", exchange="ex", routing_key_ids=["abc"])
        assert subs[0]["routing_keys"] == ["abc.*"]

    def test_explicit_single_status(self):
        subs = build_subscriptions(
            dag_id="d",
            exchange="ex",
            routing_key_ids=["abc"],
            routing_key_status="succeeded",
        )
        assert subs[0]["routing_keys"] == ["abc.succeeded"]

    def test_explicit_status_list(self):
        subs = build_subscriptions(
            dag_id="d",
            exchange="ex",
            routing_key_ids=["abc"],
            routing_key_status=["succeeded", "failed"],
        )
        assert subs[0]["routing_keys"] == ["abc.succeeded", "abc.failed"]

    def test_multiple_ids_cross_multiple_statuses(self):
        subs = build_subscriptions(
            dag_id="d",
            exchange="ex",
            routing_key_ids=["a", "b"],
            routing_key_status=["x", "y"],
        )
        assert subs[0]["routing_keys"] == ["a.x", "a.y", "b.x", "b.y"]

    def test_routing_keys_and_routing_key_ids_union(self):
        subs = build_subscriptions(
            dag_id="d",
            exchange="ex",
            routing_keys=["literal.key"],
            routing_key_ids=["abc"],
            routing_key_status="done",
        )
        assert set(subs[0]["routing_keys"]) == {"literal.key", "abc.done"}

    def test_exchange_name_preserved(self):
        subs = build_subscriptions(dag_id="d", exchange="my.exchange", routing_keys=["a.b"])
        assert subs[0]["exchange"] == "my.exchange"


class TestBuildSubscriptionsQueueName:
    def test_exchange_queue_name_from_dag_id(self):
        subs = build_subscriptions(
            dag_id="my_dag", exchange="ex", routing_keys=["a.b"]
        )
        assert subs[0]["queue_name"] == "rmq_watcher.sub.my_dag"

    def test_single_queue_name(self):
        subs = build_subscriptions(dag_id="d", queue="orders")
        assert subs[0]["queue_name"] == "orders"

    def test_queues_list_names(self):
        subs = build_subscriptions(dag_id="d", queues=["a", "b"])
        names = [s["queue_name"] for s in subs]
        assert names == ["a", "b"]


class TestBuildSubscriptionsShape:
    def test_queue_mode_shape(self):
        subs = build_subscriptions(dag_id="d", queue="q", conn_id="rmq_x", filter_data={"filter_headers": {}})
        assert set(subs[0].keys()) == {"queue_name", "conn_id", "filter_data", "cooldown"}

    def test_exchange_mode_shape(self):
        subs = build_subscriptions(dag_id="d", exchange="ex", routing_keys=["a.b"])
        assert set(subs[0].keys()) == {
            "queue_name",
            "conn_id",
            "filter_data",
            "cooldown",
            "exchange",
            "routing_keys",
        }

    def test_filter_data_none_becomes_empty_dict(self):
        subs = build_subscriptions(dag_id="d", queue="q", filter_data=None)
        assert subs[0]["filter_data"] == {}

    def test_default_conn_id(self):
        subs = build_subscriptions(dag_id="d", queue="q")
        assert subs[0]["conn_id"] == "rmq_default"


class TestHasExchangeConflict:
    def test_true_when_both_have_exchange(self):
        existing = [{"queue_name": "q", "exchange": "ex1", "routing_keys": ["a"]}]
        new = [{"queue_name": "q", "exchange": "ex2", "routing_keys": ["b"]}]
        assert has_exchange_conflict(existing, new) is True

    def test_false_when_existing_has_no_exchange(self):
        existing = [{"queue_name": "q"}]
        new = [{"queue_name": "q", "exchange": "ex2", "routing_keys": ["b"]}]
        assert has_exchange_conflict(existing, new) is False

    def test_false_when_new_has_no_exchange(self):
        existing = [{"queue_name": "q", "exchange": "ex1", "routing_keys": ["a"]}]
        new = [{"queue_name": "q2"}]
        assert has_exchange_conflict(existing, new) is False

    def test_false_when_neither_has_exchange(self):
        existing = [{"queue_name": "q"}]
        new = [{"queue_name": "q2"}]
        assert has_exchange_conflict(existing, new) is False

    def test_false_on_empty_lists(self):
        assert has_exchange_conflict([], []) is False
        assert has_exchange_conflict([], [{"exchange": "ex"}]) is False
        assert has_exchange_conflict([{"exchange": "ex"}], []) is False
