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
        assert set(sub.keys()) == {"queue_name", "conn_id", "filter_data"}
        assert sub["queue_name"] == "payments"

    def test_stacking_preserves_order(self):
        dag = _make_dag()
        rmq_trigger(queue="first")(dag)
        rmq_trigger(queue="second")(dag)
        rmq_trigger(queue="third")(dag)
        names = [s["queue_name"] for s in dag._rmq_subscriptions]
        assert names == ["first", "second", "third"]
