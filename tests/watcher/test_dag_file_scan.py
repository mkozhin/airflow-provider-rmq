from __future__ import annotations

import ast

import pytest

from airflow_provider_rmq.watcher.dag_file_scan import (
    _UNRESOLVED_DAG_ID,
    _collect_module_constants,
    _extract_dag_id_from_decorators,
)


def _constants(src: str) -> dict:
    """Parse a module source snippet and return its constant map."""
    return _collect_module_constants(ast.parse(src))[0]


def _dag_id_of_last_function(src: str):
    """Resolve ``dag_id`` of the last top-level function against the file's constants."""
    tree = ast.parse(src)
    constants = {name: value for name, (value, _) in _collect_module_constants(tree)[0].items()}
    func = tree.body[-1]
    assert isinstance(func, ast.FunctionDef)
    return _extract_dag_id_from_decorators(func.decorator_list, constants)


# ---------------------------------------------------------------------------
# Values of any literal type
# ---------------------------------------------------------------------------


class TestCollectAnyLiteralType:
    @pytest.mark.parametrize(
        ("literal", "expected"),
        [
            ("'rmq_prod'", "rmq_prod"),
            ("30", 30),
            ("-5", -5),
            ("1.5", 1.5),
            ("True", True),
            ("False", False),
            ("None", None),
            ("['q1', 'q2']", ["q1", "q2"]),
            ("('q1', 'q2')", ("q1", "q2")),
            ("{'body.type': 'paid', 'n': [1, 2]}", {"body.type": "paid", "n": [1, 2]}),
        ],
        ids=["str", "int", "negative_int", "float", "true", "false", "none",
             "list", "tuple", "dict"],
    )
    def test_literal_value_is_collected(self, literal, expected):
        result = _constants(f"X = {literal}\n")
        assert result == {"X": (expected, 1)}
        assert type(result["X"][0]) is type(expected)

    def test_annotated_non_string_assignment_is_collected(self):
        assert _constants("COOLDOWN: int = 60\n") == {"COOLDOWN": (60, 1)}

    def test_non_literal_value_is_skipped(self):
        assert _constants("X = [get(), 1]\n") == {}


class TestCollectBindingRulesForNonStrings:
    """The single-binding rules hold for every value type."""

    @pytest.mark.parametrize(
        "src",
        [
            "X = [1]\nX = [2]\n",
            "X = 1\nX += 1\n",
            "X = {'a': 1}\ndef f():\n    global X\n    X = {}\n",
            "X = [1]\nif (X := [2]):\n    pass\n",
            "X = 1\nys = [(X := i) for i in range(3)]\n",
        ],
        ids=["double_binding", "aug_assign", "global", "walrus", "walrus_in_comprehension"],
    )
    def test_rebinding_drops_the_name(self, src):
        assert "X" not in _constants(src)

    def test_wildcard_import_disables_every_constant(self):
        assert _constants("X = [1]\nN = 2\nfrom settings import *\n") == {}


# ---------------------------------------------------------------------------
# Mutable values: every mention must be an rmq_trigger argument
# ---------------------------------------------------------------------------


class TestMutableConstantMentions:
    @pytest.mark.parametrize(
        "use",
        [
            "X['k'] = 'v'",
            "del X['k']",
            "X['a']['b'] = 'v'",
            "X.update({'k': 'v'})",
            "alias = X",
            "alias = X\nalias['k'] = 'v'",
            "helper(X)",
            "print(X['a'])",
            "def f():\n    X['k'] = 'v'",
        ],
        ids=["setitem", "delitem", "nested_setitem", "method_call", "alias",
             "write_through_alias", "passed_to_function", "read_by_index",
             "write_in_function_body"],
    )
    def test_mention_outside_rmq_trigger_drops_dict_constant(self, use):
        src = f"X = {{'a': {{'b': 1}}}}\n{use}\n"
        assert "X" not in _constants(src)

    @pytest.mark.parametrize(
        "use",
        ["X.append('q3')", "X[0] = 'q'", "alias = X", "helper(X)", "for q in X:\n    pass"],
        ids=["append", "setitem", "alias", "passed_to_function", "iterated"],
    )
    def test_mention_outside_rmq_trigger_drops_list_constant(self, use):
        src = f"X = ['q1', 'q2']\n{use}\n"
        assert "X" not in _constants(src)

    def test_tuple_holding_a_list_is_treated_as_mutable(self):
        assert "X" not in _constants("X = (['q1'], 'q2')\nX[0].append('q3')\n")

    def test_tuple_of_scalars_is_not_restricted(self):
        assert _constants("X = ('q1', 'q2')\nhelper(X)\n") == {"X": (("q1", "q2"), 1)}

    def test_dict_used_only_as_filter_data_stays_usable(self):
        src = (
            "FILTER = {'body.type': 'paid'}\n"
            "@rmq_trigger(queue='q', filter_data=FILTER)\n"
            "@dag(dag_id='d')\n"
            "def f(): pass\n"
        )
        assert _constants(src) == {"FILTER": ({"body.type": "paid"}, 1)}

    def test_dict_used_only_as_unpacking_stays_usable(self):
        src = (
            "RMQ_OPTS = {'queue': 'q', 'cooldown': 30}\n"
            "@decorators.rmq_trigger(**RMQ_OPTS)\n"
            "@dag(dag_id='d')\n"
            "def f(): pass\n"
        )
        assert _constants(src) == {"RMQ_OPTS": ({"queue": "q", "cooldown": 30}, 1)}

    def test_list_used_positionally_and_inside_display_stays_usable(self):
        src = (
            "QUEUES = ['q1', 'q2']\n"
            "PAID = ['paid']\n"
            "@rmq_trigger(None, QUEUES, filter_data={'body.type': PAID})\n"
            "@dag(dag_id='d')\n"
            "def f(): pass\n"
            "@rmq_trigger(queues=QUEUES)\n"
            "@dag(dag_id='e')\n"
            "def g(): pass\n"
        )
        result = _constants(src)
        assert result["QUEUES"] == (["q1", "q2"], 1)
        assert result["PAID"] == (["paid"], 2)

    def test_expression_inside_rmq_trigger_argument_drops_the_name(self):
        src = (
            "FILTER = {'a': {'b': 1}}\n"
            "@rmq_trigger(queue='q', filter_data=FILTER['a'])\n"
            "@dag(dag_id='d')\n"
            "def f(): pass\n"
        )
        assert "FILTER" not in _constants(src)

    def test_call_inside_rmq_trigger_argument_drops_the_name(self):
        src = (
            "FILTER = {'a': 1}\n"
            "@rmq_trigger(queue='q', filter_data=normalize(FILTER))\n"
            "@dag(dag_id='d')\n"
            "def f(): pass\n"
        )
        assert "FILTER" not in _constants(src)

    def test_one_stray_mention_drops_the_name_despite_trigger_uses(self):
        src = (
            "FILTER = {'a': 1}\n"
            "@rmq_trigger(queue='q', filter_data=FILTER)\n"
            "@dag(dag_id='d')\n"
            "def f(): pass\n"
            "FILTER['a'] = 2\n"
        )
        assert "FILTER" not in _constants(src)

    def test_mutation_of_one_constant_keeps_the_others(self):
        src = "A = {'k': 1}\nB = {'k': 2}\nA['k'] = 3\n"
        assert _constants(src) == {"B": ({"k": 2}, 2)}

    def test_string_constant_with_method_call_stays_usable(self):
        src = "DAG_ID = 'my_dag'\nlower = DAG_ID.lower()\nhelper(DAG_ID)\n"
        assert _constants(src) == {"DAG_ID": ("my_dag", 1)}

    def test_number_constant_passed_around_stays_usable(self):
        assert _constants("COOLDOWN = 30\nhelper(COOLDOWN)\n") == {"COOLDOWN": (30, 1)}


# ---------------------------------------------------------------------------
# dag_id accepts a string constant only
# ---------------------------------------------------------------------------


class TestDagIdFromNonStringConstant:
    @pytest.mark.parametrize("literal", ["None", "0", "False", "123", "['d']"])
    def test_non_string_constant_is_unresolved(self, literal):
        src = f"DAG_ID = {literal}\n@dag(dag_id=DAG_ID)\ndef f(): pass\n"
        assert _dag_id_of_last_function(src) is _UNRESOLVED_DAG_ID

    def test_non_string_constant_positional_is_unresolved(self):
        src = "DAG_ID = None\n@dag(DAG_ID)\ndef f(): pass\n"
        assert _dag_id_of_last_function(src) is _UNRESOLVED_DAG_ID

    def test_empty_string_constant_falls_back_to_function_name(self):
        src = "DAG_ID = ''\n@dag(dag_id=DAG_ID)\ndef f(): pass\n"
        assert _dag_id_of_last_function(src) is None

    def test_string_constant_resolves(self):
        src = "DAG_ID = 'real'\n@dag(dag_id=DAG_ID)\ndef f(): pass\n"
        assert _dag_id_of_last_function(src) == "real"

    def test_none_literal_falls_back_to_function_name(self):
        src = "@dag(dag_id=None)\ndef f(): pass\n"
        assert _dag_id_of_last_function(src) is None
