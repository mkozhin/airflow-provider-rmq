from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from airflow_provider_rmq.watcher.decorators import rmq_trigger


def _make_dag(dag_id="test_dag", schedule=None):
    dag = MagicMock()
    dag.dag_id = dag_id
    dag.schedule_interval = schedule
    # Remove _rmq_subscriptions so hasattr returns False by default
    del dag._rmq_subscriptions
    return dag


class TestRmqTriggerDecorator:
    def test_adds_rmq_subscriptions_attribute(self):
        dag = _make_dag()
        rmq_trigger(queue="orders")(dag)
        assert hasattr(dag, "_rmq_subscriptions")
        assert len(dag._rmq_subscriptions) == 1

    def test_dag_returned_unchanged(self):
        dag = _make_dag(dag_id="my_dag", schedule="@daily")
        result = rmq_trigger(queue="q")(dag)
        assert result is dag
        assert result.dag_id == "my_dag"
        assert result.schedule_interval == "@daily"

    def test_stacking_multiple_queues(self):
        dag = _make_dag()
        rmq_trigger(queue="q1")(dag)
        rmq_trigger(queue="q2", conn_id="rmq_alt")(dag)
        assert len(dag._rmq_subscriptions) == 2
        queue_names = {s["queue_name"] for s in dag._rmq_subscriptions}
        assert queue_names == {"q1", "q2"}

    def test_default_conn_id(self):
        dag = _make_dag()
        rmq_trigger(queue="orders")(dag)
        assert dag._rmq_subscriptions[0]["conn_id"] == "rmq_default"

    def test_custom_conn_id(self):
        dag = _make_dag()
        rmq_trigger(queue="orders", conn_id="rmq_eu")(dag)
        assert dag._rmq_subscriptions[0]["conn_id"] == "rmq_eu"

    def test_filter_data_filter_headers_format(self):
        dag = _make_dag()
        fd = {"filter_headers": {"type": "new_order", "region": "eu"}}
        rmq_trigger(queue="orders", filter_data=fd)(dag)
        assert dag._rmq_subscriptions[0]["filter_data"] == fd

    def test_filter_data_none_stored_as_empty_dict(self):
        dag = _make_dag()
        rmq_trigger(queue="orders", filter_data=None)(dag)
        assert dag._rmq_subscriptions[0]["filter_data"] == {}

    def test_subscription_dict_shape(self):
        dag = _make_dag()
        rmq_trigger(queue="payments", conn_id="rmq_pay", filter_data={"filter_headers": {"k": "v"}})(dag)
        sub = dag._rmq_subscriptions[0]
        assert set(sub.keys()) == {"queue_name", "conn_id", "filter_data", "cooldown"}
        assert sub["queue_name"] == "payments"

    def test_stacking_preserves_order(self):
        dag = _make_dag()
        rmq_trigger(queue="first")(dag)
        rmq_trigger(queue="second")(dag)
        rmq_trigger(queue="third")(dag)
        names = [s["queue_name"] for s in dag._rmq_subscriptions]
        assert names == ["first", "second", "third"]

    # --- cooldown ---

    def test_default_cooldown_is_zero(self):
        dag = _make_dag()
        rmq_trigger(queue="orders")(dag)
        assert dag._rmq_subscriptions[0]["cooldown"] == 0

    def test_explicit_cooldown(self):
        dag = _make_dag()
        rmq_trigger(queue="orders", cooldown=300)(dag)
        assert dag._rmq_subscriptions[0]["cooldown"] == 300

    # --- queues (list) ---

    def test_queues_list_creates_multiple_subscriptions(self):
        dag = _make_dag()
        rmq_trigger(queues=["orders", "payments"])(dag)
        assert len(dag._rmq_subscriptions) == 2
        names = [s["queue_name"] for s in dag._rmq_subscriptions]
        assert names == ["orders", "payments"]

    def test_queues_list_with_cooldown(self):
        dag = _make_dag()
        rmq_trigger(queues=["a", "b", "c"], cooldown=60)(dag)
        assert len(dag._rmq_subscriptions) == 3
        for sub in dag._rmq_subscriptions:
            assert sub["cooldown"] == 60
            assert sub["conn_id"] == "rmq_default"
            assert sub["filter_data"] == {}

    def test_queues_list_inherits_conn_id_and_filter(self):
        dag = _make_dag()
        fd = {"filter_headers": {"type": "order"}}
        rmq_trigger(queues=["q1", "q2"], conn_id="rmq_eu", filter_data=fd)(dag)
        for sub in dag._rmq_subscriptions:
            assert sub["conn_id"] == "rmq_eu"
            assert sub["filter_data"] == fd

    def test_queues_list_no_group_key_in_decorator(self):
        """group_key must NOT be set by the decorator — listener sets it."""
        dag = _make_dag()
        rmq_trigger(queues=["a", "b"])(dag)
        for sub in dag._rmq_subscriptions:
            assert "group_key" not in sub

    def test_single_queue_no_group_key_in_decorator(self):
        """group_key must NOT be set by the decorator — listener sets it."""
        dag = _make_dag()
        rmq_trigger(queue="a")(dag)
        for sub in dag._rmq_subscriptions:
            assert "group_key" not in sub

    # --- validation (delegated to build_subscriptions; just verify it surfaces as-is) ---

    def test_queue_and_queues_mutually_exclusive(self):
        dag = _make_dag()
        with pytest.raises(ValueError, match="exactly one"):
            rmq_trigger(queue="q", queues=["q"])(dag)

    def test_neither_queue_nor_queues_raises(self):
        dag = _make_dag()
        with pytest.raises(ValueError, match="exactly one"):
            rmq_trigger()(dag)

    def test_negative_cooldown_raises(self):
        dag = _make_dag()
        with pytest.raises(ValueError, match="cooldown"):
            rmq_trigger(queue="q", cooldown=-1)(dag)

    def test_exchange_validation_error_surfaces(self):
        """A ValueError raised by build_subscriptions (e.g. missing routing keys)
        propagates unchanged through the decorator."""
        dag = _make_dag()
        with pytest.raises(ValueError, match="routing_keys"):
            rmq_trigger(exchange="jetstat.airflow")(dag)

    # --- exchange stacking conflict ---

    def test_stacking_exchange_on_same_dag_raises(self):
        dag = _make_dag()
        rmq_trigger(exchange="jetstat.airflow", routing_keys=["a.b"])(dag)
        with pytest.raises(ValueError, match="not supported"):
            rmq_trigger(exchange="jetstat.other", routing_keys=["c.d"])(dag)

    def test_stacking_exchange_does_not_partially_mutate(self):
        """A rejected second exchange decorator must not leave the first
        subscription's metadata corrupted."""
        dag = _make_dag()
        rmq_trigger(exchange="jetstat.airflow", routing_keys=["a.b"])(dag)
        with pytest.raises(ValueError):
            rmq_trigger(exchange="jetstat.other", routing_keys=["c.d"])(dag)
        assert len(dag._rmq_subscriptions) == 1
        assert dag._rmq_subscriptions[0]["exchange"] == "jetstat.airflow"

    def test_exchange_subscription_dict_shape(self):
        dag = _make_dag(dag_id="my_dag")
        rmq_trigger(exchange="jetstat.airflow", routing_key_ids=["abc"])(dag)
        sub = dag._rmq_subscriptions[0]
        assert sub["queue_name"] == "rmq_watcher.sub.my_dag"
        assert sub["exchange"] == "jetstat.airflow"
        assert sub["routing_keys"] == ["abc.*"]

    def test_queue_and_exchange_does_not_conflict(self):
        """A queue-mode subscription followed by an exchange-mode subscription
        is fine — conflict only triggers when both entries carry 'exchange'."""
        dag = _make_dag()
        rmq_trigger(queue="q1")(dag)
        rmq_trigger(exchange="jetstat.airflow", routing_keys=["a.b"])(dag)
        assert len(dag._rmq_subscriptions) == 2

    # --- backward compatibility ---

    def test_backward_compat_single_queue_no_cooldown(self):
        """@rmq_trigger(queue="...") without cooldown works as before."""
        dag = _make_dag()
        rmq_trigger(queue="orders", conn_id="rmq_default")(dag)
        sub = dag._rmq_subscriptions[0]
        assert sub["queue_name"] == "orders"
        assert sub["conn_id"] == "rmq_default"
        assert sub["filter_data"] == {}
        assert sub["cooldown"] == 0
        assert len(dag._rmq_subscriptions) == 1
