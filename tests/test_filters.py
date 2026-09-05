from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import patch

import pytest

from airflow_provider_rmq.utils.filters import FilterVerdict, MessageFilter


@dataclass
class FakeProperties:
    """Minimal stand-in for pika.spec.BasicProperties."""

    headers: dict[str, Any] | None = field(default_factory=dict)


# ---------------------------------------------------------------------------
# 2.2.1 — No filters (always match)
# ---------------------------------------------------------------------------
class TestNoFilters:
    def test_no_filters_matches_any_message(self):
        mf = MessageFilter()
        assert mf.matches(FakeProperties(), "any body")

    def test_has_filters_is_false(self):
        mf = MessageFilter()
        assert mf.has_filters is False

    def test_empty_body(self):
        mf = MessageFilter()
        assert mf.matches(FakeProperties(), "")


# ---------------------------------------------------------------------------
# 2.2.2 — Dict-based filtering
# ---------------------------------------------------------------------------
class TestDictFilter:
    def test_header_match(self):
        mf = MessageFilter(filter_headers={"x-type": "order"})
        props = FakeProperties(headers={"x-type": "order"})
        assert mf.matches(props, "body")

    def test_header_no_match(self):
        mf = MessageFilter(filter_headers={"x-type": "order"})
        props = FakeProperties(headers={"x-type": "invoice"})
        assert not mf.matches(props, "body")

    def test_header_missing_key(self):
        mf = MessageFilter(filter_headers={"x-type": "order"})
        props = FakeProperties(headers={})
        assert not mf.matches(props, "body")

    def test_header_none_headers(self):
        mf = MessageFilter(filter_headers={"x-type": "order"})
        props = FakeProperties(headers=None)
        assert not mf.matches(props, "body")

    def test_multiple_headers_all_match(self):
        mf = MessageFilter(filter_headers={"x-type": "order", "x-priority": "high"})
        props = FakeProperties(headers={"x-type": "order", "x-priority": "high"})
        assert mf.matches(props, "body")

    def test_multiple_headers_partial_match(self):
        mf = MessageFilter(filter_headers={"x-type": "order", "x-priority": "high"})
        props = FakeProperties(headers={"x-type": "order", "x-priority": "low"})
        assert not mf.matches(props, "body")

    def test_body_simple_key(self):
        body = json.dumps({"status": "active"})
        mf = MessageFilter(filter_headers={"body.status": "active"})
        assert mf.matches(FakeProperties(), body)

    def test_body_nested_key(self):
        body = json.dumps({"data": {"user": {"role": "admin"}}})
        mf = MessageFilter(filter_headers={"body.data.user.role": "admin"})
        assert mf.matches(FakeProperties(), body)

    def test_body_nested_key_no_match(self):
        body = json.dumps({"data": {"user": {"role": "viewer"}}})
        mf = MessageFilter(filter_headers={"body.data.user.role": "admin"})
        assert not mf.matches(FakeProperties(), body)

    def test_body_missing_path(self):
        body = json.dumps({"data": {"name": "test"}})
        mf = MessageFilter(filter_headers={"body.data.user.role": "admin"})
        assert not mf.matches(FakeProperties(), body)

    def test_body_non_json(self):
        mf = MessageFilter(filter_headers={"body.key": "value"})
        assert not mf.matches(FakeProperties(), "not json at all")

    def test_body_empty_string(self):
        mf = MessageFilter(filter_headers={"body.key": "value"})
        assert not mf.matches(FakeProperties(), "")

    def test_body_integer_value(self):
        body = json.dumps({"count": 42})
        mf = MessageFilter(filter_headers={"body.count": 42})
        assert mf.matches(FakeProperties(), body)

    def test_body_boolean_value(self):
        body = json.dumps({"active": True})
        mf = MessageFilter(filter_headers={"body.active": True})
        assert mf.matches(FakeProperties(), body)

    def test_mixed_header_and_body(self):
        body = json.dumps({"status": "ok"})
        mf = MessageFilter(filter_headers={"x-source": "api", "body.status": "ok"})
        props = FakeProperties(headers={"x-source": "api"})
        assert mf.matches(props, body)

    def test_mixed_header_match_body_no_match(self):
        body = json.dumps({"status": "error"})
        mf = MessageFilter(filter_headers={"x-source": "api", "body.status": "ok"})
        props = FakeProperties(headers={"x-source": "api"})
        assert not mf.matches(props, body)


# ---------------------------------------------------------------------------
# 2.2.3 — Callable-based filtering
# ---------------------------------------------------------------------------
class TestCallableFilter:
    def test_callable_pass(self):
        mf = MessageFilter(filter_callable=lambda props, body: body == "yes")
        assert mf.matches(FakeProperties(), "yes")

    def test_callable_fail(self):
        mf = MessageFilter(filter_callable=lambda props, body: body == "yes")
        assert not mf.matches(FakeProperties(), "no")

    def test_callable_uses_properties(self):
        def check(props, body):
            return (props.headers or {}).get("x-type") == "order"

        mf = MessageFilter(filter_callable=check)
        assert mf.matches(FakeProperties(headers={"x-type": "order"}), "body")
        assert not mf.matches(FakeProperties(headers={"x-type": "other"}), "body")

    def test_callable_exception_treated_as_no_match(self):
        def bad_filter(props, body):
            raise ValueError("boom")

        mf = MessageFilter(filter_callable=bad_filter)
        assert not mf.matches(FakeProperties(), "body")

    def test_has_filters_true_with_callable(self):
        mf = MessageFilter(filter_callable=lambda p, b: True)
        assert mf.has_filters is True


# ---------------------------------------------------------------------------
# 2.2.4 — Combined dict + callable (AND logic)
# ---------------------------------------------------------------------------
class TestCombinedFilters:
    def test_both_pass(self):
        mf = MessageFilter(
            filter_headers={"x-type": "order"},
            filter_callable=lambda props, body: "important" in body,
        )
        props = FakeProperties(headers={"x-type": "order"})
        assert mf.matches(props, "important message")

    def test_dict_pass_callable_fail(self):
        mf = MessageFilter(
            filter_headers={"x-type": "order"},
            filter_callable=lambda props, body: "important" in body,
        )
        props = FakeProperties(headers={"x-type": "order"})
        assert not mf.matches(props, "normal message")

    def test_dict_fail_callable_pass(self):
        mf = MessageFilter(
            filter_headers={"x-type": "invoice"},
            filter_callable=lambda props, body: "important" in body,
        )
        props = FakeProperties(headers={"x-type": "order"})
        assert not mf.matches(props, "important message")

    def test_both_fail(self):
        mf = MessageFilter(
            filter_headers={"x-type": "invoice"},
            filter_callable=lambda props, body: "important" in body,
        )
        props = FakeProperties(headers={"x-type": "order"})
        assert not mf.matches(props, "normal message")


# ---------------------------------------------------------------------------
# 2.2.5 — Serialize / Deserialize
# ---------------------------------------------------------------------------
class TestSerializeDeserialize:
    def test_serialize_with_headers(self):
        mf = MessageFilter(filter_headers={"x-type": "order", "body.status": "ok"})
        data = mf.serialize()
        assert data == {"filter_headers": {"x-type": "order", "body.status": "ok"}}

    def test_serialize_without_headers(self):
        mf = MessageFilter()
        data = mf.serialize()
        assert data == {"filter_headers": None}

    def test_serialize_ignores_callable(self):
        mf = MessageFilter(
            filter_headers={"x-type": "order"},
            filter_callable=lambda p, b: True,
        )
        data = mf.serialize()
        assert "filter_callable" not in data

    def test_deserialize_restores_filter(self):
        original = MessageFilter(filter_headers={"x-type": "order", "body.count": 5})
        data = original.serialize()
        restored = MessageFilter.deserialize(data)
        props = FakeProperties(headers={"x-type": "order"})
        body = json.dumps({"count": 5})
        assert restored.matches(props, body)

    def test_deserialize_no_callable(self):
        restored = MessageFilter.deserialize({"filter_headers": {"x-type": "a"}})
        assert restored._filter_callable is None

    def test_roundtrip_empty(self):
        original = MessageFilter()
        restored = MessageFilter.deserialize(original.serialize())
        assert not restored.has_filters
        assert restored.matches(FakeProperties(), "anything")

# ---------------------------------------------------------------------------
# FilterVerdict — a rejection told apart from a failure to evaluate
# ---------------------------------------------------------------------------
class TestEvaluateVerdicts:
    def test_match_has_no_reason(self):
        mf = MessageFilter(filter_headers={"x-type": "order"})
        props = FakeProperties(headers={"x-type": "order"})
        assert mf.evaluate(props, "body") == (FilterVerdict.MATCH, None)

    def test_miss_has_no_reason(self):
        mf = MessageFilter(filter_headers={"x-type": "order"})
        props = FakeProperties(headers={"x-type": "invoice"})
        assert mf.evaluate(props, "body") == (FilterVerdict.MISS, None)

    def test_no_filters_is_match(self):
        assert MessageFilter().evaluate(FakeProperties(), "any body") == (FilterVerdict.MATCH, None)

    def test_raising_callable_is_error_not_miss(self):
        def bad_filter(props, body):
            raise ValueError("boom")

        verdict, _reason = MessageFilter(filter_callable=bad_filter).evaluate(FakeProperties(), "body")
        assert verdict is FilterVerdict.ERROR

    def test_error_reason_carries_exception_type_and_text(self):
        def bad_filter(props, body):
            raise RuntimeError("external service is down")

        _verdict, reason = MessageFilter(filter_callable=bad_filter).evaluate(FakeProperties(), "body")
        assert "RuntimeError" in reason
        assert "external service is down" in reason

    def test_unreadable_body_is_error(self):
        mf = MessageFilter(filter_headers={"body.status": "ok"})
        verdict, reason = mf.evaluate(FakeProperties(), "not json at all")
        assert verdict is FilterVerdict.ERROR
        assert "JSONDecodeError" in reason

    def test_body_of_wrong_type_is_error(self):
        mf = MessageFilter(filter_headers={"body.status": "ok"})
        verdict, reason = mf.evaluate(FakeProperties(), None)
        assert verdict is FilterVerdict.ERROR
        assert "TypeError" in reason

    def test_parsed_body_without_key_is_miss(self):
        mf = MessageFilter(filter_headers={"body.status": "ok"})
        assert mf.evaluate(FakeProperties(), json.dumps({"other": 1})) == (FilterVerdict.MISS, None)

    def test_body_parsed_into_a_list_is_miss(self):
        """The filter read the body and rejected it — a list is a miss, not a failure."""
        mf = MessageFilter(filter_headers={"body.status": "ok"})
        assert mf.evaluate(FakeProperties(), json.dumps(["ok"])) == (FilterVerdict.MISS, None)

    def test_integer_over_conversion_limit_still_raises(self):
        """The catch stays exactly where it is: a plain ValueError keeps flying out."""
        mf = MessageFilter(filter_headers={"body.status": "ok"})
        with pytest.raises(ValueError, match="Exceeds the limit"):
            mf.evaluate(FakeProperties(), "1" * 4400)


# ---------------------------------------------------------------------------
# Phases: headers in full first, body once and only after
# ---------------------------------------------------------------------------
class TestEvaluationPhases:
    @pytest.mark.parametrize("filter_headers", [
        {"type": "other", "body.x": 1},
        {"body.x": 1, "type": "other"},
    ])
    def test_header_miss_wins_over_unreadable_body_in_any_key_order(self, filter_headers):
        mf = MessageFilter(filter_headers=filter_headers)
        props = FakeProperties(headers={"type": "order"})
        with patch("airflow_provider_rmq.utils.filters.json.loads") as mock_loads:
            assert mf.evaluate(props, "not json at all") == (FilterVerdict.MISS, None)
        mock_loads.assert_not_called()

    def test_dict_miss_does_not_call_the_callable(self):
        calls = []

        def bad_filter(props, body):
            calls.append(body)
            raise ValueError("boom")

        mf = MessageFilter(filter_headers={"x-type": "order"}, filter_callable=bad_filter)
        props = FakeProperties(headers={"x-type": "invoice"})
        assert mf.evaluate(props, "body") == (FilterVerdict.MISS, None)
        assert calls == []

    def test_body_parsed_once_for_several_body_keys(self):
        body = json.dumps({"a": 1, "b": 2})
        mf = MessageFilter(filter_headers={"body.a": 1, "body.b": 2})
        with patch("airflow_provider_rmq.utils.filters.json.loads", wraps=json.loads) as mock_loads:
            assert mf.evaluate(FakeProperties(), body) == (FilterVerdict.MATCH, None)
        assert mock_loads.call_count == 1


# ---------------------------------------------------------------------------
# matches() keeps its boolean contract on all three outcomes
# ---------------------------------------------------------------------------
class TestMatchesOverEvaluate:
    def test_match_is_true(self):
        mf = MessageFilter(filter_headers={"x-type": "order"})
        assert mf.matches(FakeProperties(headers={"x-type": "order"}), "body") is True

    def test_miss_is_false(self):
        mf = MessageFilter(filter_headers={"x-type": "order"})
        assert mf.matches(FakeProperties(headers={"x-type": "invoice"}), "body") is False

    def test_error_is_false(self):
        def bad_filter(props, body):
            raise ValueError("boom")

        assert MessageFilter(filter_callable=bad_filter).matches(FakeProperties(), "body") is False

    def test_error_reason_goes_to_debug_not_warning(self, caplog):
        def bad_filter(props, body):
            raise ValueError("boom")

        with caplog.at_level(logging.DEBUG, logger="airflow.task"):
            MessageFilter(filter_callable=bad_filter).matches(FakeProperties(), "body")
        levels = {rec.levelname for rec in caplog.records if rec.name == "airflow.task"}
        assert levels == {"DEBUG"}
