from __future__ import annotations

import sys
from unittest.mock import MagicMock

import pytest

from airflow_provider_rmq.watcher import decorators
from airflow_provider_rmq.watcher.dag_file_scan import RMQTriggerNotStaticError
from airflow_provider_rmq.watcher.decorators import rmq_trigger


@pytest.fixture(autouse=True)
def static_check(monkeypatch):
    """Replace the decorator's static-readability check and return the stand-in.

    The tests here apply ``rmq_trigger(...)`` to DAG objects directly, a form the
    watcher's scan never reads, so the real check would reject every one of
    them; the check itself runs in ``test_decorator_static_check.py``.
    """
    check = MagicMock(return_value=None)
    monkeypatch.setattr(decorators, "_verify_static_readability", check)
    return check


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
        assert set(sub.keys()) == {
            "queue_name",
            "conn_id",
            "filter_data",
            "cooldown",
            "ack_unmatched",
        }
        assert sub["queue_name"] == "payments"

    def test_stacking_preserves_order(self):
        dag = _make_dag()
        rmq_trigger(queue="first")(dag)
        rmq_trigger(queue="second")(dag)
        rmq_trigger(queue="third")(dag)
        names = [s["queue_name"] for s in dag._rmq_subscriptions]
        assert names == ["first", "second", "third"]

    # --- ack_unmatched ---

    def test_default_ack_unmatched_is_false(self):
        dag = _make_dag()
        rmq_trigger(queue="orders")(dag)
        assert dag._rmq_subscriptions[0]["ack_unmatched"] is False

    def test_explicit_ack_unmatched(self):
        dag = _make_dag()
        rmq_trigger(queue="orders", ack_unmatched=True)(dag)
        assert dag._rmq_subscriptions[0]["ack_unmatched"] is True

    def test_queues_list_all_share_ack_unmatched(self):
        dag = _make_dag()
        rmq_trigger(queues=["a", "b", "c"], ack_unmatched=True)(dag)
        assert len(dag._rmq_subscriptions) == 3
        assert all(sub["ack_unmatched"] is True for sub in dag._rmq_subscriptions)

    def test_exchange_mode_carries_ack_unmatched(self):
        dag = _make_dag()
        rmq_trigger(exchange="ex", routing_keys=["a.b"], ack_unmatched=True)(dag)
        assert dag._rmq_subscriptions[0]["ack_unmatched"] is True

    def test_non_bool_ack_unmatched_raises(self):
        dag = _make_dag()
        with pytest.raises(ValueError, match="ack_unmatched"):
            rmq_trigger(queue="q", ack_unmatched="yes")(dag)

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


class TestRmqTriggerTaskFlowFactory:
    """@rmq_trigger stacked above an uncalled @dag(...)-style factory.

    These tests cover the lazy-wrapping branch added to support the
    TaskFlow pattern used throughout this repository's examples/README:

        @rmq_trigger(queue="orders")
        @dag(dag_id="my_dag", schedule=None)
        def my_dag():
            ...

        my_dag()
    """

    def test_uncalled_factory_is_wrapped_not_attached_immediately(self):
        dag = _make_dag(dag_id="factory_dag")

        def factory():
            return dag

        wrapped = rmq_trigger(queue="orders")(factory)
        # Decoration alone must not produce a DAG or touch _rmq_subscriptions.
        assert callable(wrapped)
        assert not hasattr(dag, "_rmq_subscriptions")

        result = wrapped()
        assert result is dag
        assert hasattr(dag, "_rmq_subscriptions")
        assert len(dag._rmq_subscriptions) == 1
        assert dag._rmq_subscriptions[0]["queue_name"] == "orders"

    def test_stacking_multiple_queues_on_factory(self):
        dag = _make_dag(dag_id="factory_dag")

        def factory():
            return dag

        wrapped = rmq_trigger(queue="q1")(factory)
        wrapped = rmq_trigger(queue="q2", conn_id="rmq_alt")(wrapped)

        result = wrapped()
        assert result is dag
        assert len(dag._rmq_subscriptions) == 2
        queue_names = {s["queue_name"] for s in dag._rmq_subscriptions}
        assert queue_names == {"q1", "q2"}

    def test_wraps_preserves_factory_metadata(self):
        def factory():
            """Factory docstring."""
            return _make_dag(dag_id="factory_dag")

        wrapped = rmq_trigger(queue="orders")(factory)
        assert wrapped.__name__ == "factory"
        assert wrapped.__doc__ == "Factory docstring."
        assert wrapped.__wrapped__ is factory

    def test_stacking_exchange_conflict_on_factory_raises_at_call_time(self):
        dag = _make_dag(dag_id="factory_dag")

        def factory():
            return dag

        wrapped = rmq_trigger(exchange="jetstat.airflow", routing_keys=["a.b"])(factory)
        wrapped = rmq_trigger(exchange="jetstat.other", routing_keys=["c.d"])(wrapped)

        # No error at decoration time — only when the wrapper chain is invoked.
        with pytest.raises(ValueError, match="not supported"):
            wrapped()

    def test_parametrized_factory_called_twice_creates_independent_dags(self):
        def make_factory():
            def factory(dag_id):
                return _make_dag(dag_id=dag_id)

            return factory

        wrapped = rmq_trigger(queue="orders")(make_factory())

        dag_a = wrapped("dag_a")
        dag_b = wrapped("dag_b")

        assert dag_a is not dag_b
        assert dag_a.dag_id == "dag_a"
        assert dag_b.dag_id == "dag_b"
        assert len(dag_a._rmq_subscriptions) == 1
        assert len(dag_b._rmq_subscriptions) == 1
        # Independent lists, not shared/aliased state.
        dag_a._rmq_subscriptions.append({"sentinel": True})
        assert len(dag_b._rmq_subscriptions) == 1

    def test_wrong_decorator_order_raises_type_error_not_attribute_error(self):
        """@dag(...) below @rmq_trigger(...) means rmq_trigger decorates a
        plain function that does not return a DAG — must raise a clear
        TypeError, not an AttributeError deep inside DAG construction."""

        def plain_function():
            return "not a dag"

        wrapped = rmq_trigger(queue="orders")(plain_function)
        with pytest.raises(TypeError, match="did not produce a DAG"):
            wrapped()

    def test_invalid_input_none_raises_type_error(self):
        with pytest.raises(TypeError, match="DAG instance or a callable"):
            rmq_trigger(queue="orders")(None)

    def test_invalid_input_string_raises_type_error(self):
        with pytest.raises(TypeError, match="DAG instance or a callable"):
            rmq_trigger(queue="orders")("not a dag or callable")


class TestRmqTriggerStaticCheckCall:
    """What ``_attach`` hands to ``_verify_static_readability`` and when."""

    def test_check_receives_call_site_dag_id_subscriptions_and_dags_folder(
        self, static_check, monkeypatch
    ):
        from airflow import settings

        monkeypatch.setattr(settings, "DAGS_FOLDER", "/opt/airflow/dags")
        dag = _make_dag(dag_id="checked_dag")
        line = sys._getframe().f_lineno + 1
        rmq_trigger(queue="orders", conn_id="rmq_eu")(dag)

        static_check.assert_called_once()
        path, lineno, dag_id, subs, dags_folder = static_check.call_args.args
        assert path == __file__
        assert lineno == line
        assert dag_id == "checked_dag"
        assert subs == dag._rmq_subscriptions
        assert dags_folder == "/opt/airflow/dags"

    def test_factory_check_runs_at_factory_call_with_decoration_call_site(self, static_check):
        dag = _make_dag(dag_id="factory_dag")

        def factory():
            return dag

        line = sys._getframe().f_lineno + 1
        wrapped = rmq_trigger(queue="orders")(factory)
        static_check.assert_not_called()

        wrapped()

        static_check.assert_called_once()
        path, lineno, dag_id, _subs, _folder = static_check.call_args.args
        assert (path, lineno, dag_id) == (__file__, line, "factory_dag")

    def test_multiline_call_site_line_lies_within_the_call(self, static_check):
        dag = _make_dag()
        first = sys._getframe().f_lineno + 1
        rmq_trigger(
            queue="orders",
            conn_id="rmq_eu",
        )(dag)
        last = first + 3

        lineno = static_check.call_args.args[1]
        assert first <= lineno <= last

    def test_builder_error_surfaces_before_the_check(self, static_check):
        dag = _make_dag()
        with pytest.raises(ValueError, match="'cooldown' must be >= 0"):
            rmq_trigger(queue="orders", cooldown=-1)(dag)
        static_check.assert_not_called()

    def test_check_error_propagates_and_leaves_dag_untouched(self, static_check):
        static_check.side_effect = RMQTriggerNotStaticError("not static")
        dag = _make_dag()
        with pytest.raises(RMQTriggerNotStaticError, match="not static"):
            rmq_trigger(queue="orders")(dag)
        assert not hasattr(dag, "_rmq_subscriptions")

    def test_each_stacked_call_is_checked_with_its_own_line(self, static_check):
        dag = _make_dag()
        first = sys._getframe().f_lineno + 1
        rmq_trigger(queue="q1")(dag)
        rmq_trigger(queue="q2")(dag)

        lines = [call.args[1] for call in static_check.call_args_list]
        assert lines == [first, first + 1]
