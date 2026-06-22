"""Tests for subscription_form.py — pure parsing, no Flask request context."""
from __future__ import annotations

import pytest

from airflow_provider_rmq.watcher.subscription_form import parse_cooldown, parse_filter_data


class TestParseCooldown:
    def test_empty_string_returns_zero(self):
        assert parse_cooldown("") == 0

    def test_valid_positive_integer(self):
        assert parse_cooldown("300") == 300

    def test_zero_is_valid(self):
        assert parse_cooldown("0") == 0

    def test_negative_raises_value_error(self):
        with pytest.raises(ValueError):
            parse_cooldown("-1")

    def test_non_numeric_raises_value_error(self):
        with pytest.raises(ValueError):
            parse_cooldown("abc")


class TestParseFilterData:
    def test_empty_string_returns_empty_dict(self):
        assert parse_filter_data("") == {}

    def test_valid_json_object(self):
        assert parse_filter_data('{"status": "succeeded"}') == {"status": "succeeded"}

    def test_invalid_json_raises_value_error(self):
        with pytest.raises(ValueError):
            parse_filter_data("{not valid json")
