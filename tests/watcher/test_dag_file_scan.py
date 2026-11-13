from __future__ import annotations

import ast

import pytest

from airflow_provider_rmq.watcher.dag_file_scan import (
    _UNRESOLVED_DAG_ID,
    _collect_module_constants,
    _extract_dag_id_from_decorators,
    _is_rmq_trigger_call,
    _read_rmq_trigger_call,
    _resolve_static,
    _Unresolved,
    _usable_constants,
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


# ---------------------------------------------------------------------------
# _resolve_static
# ---------------------------------------------------------------------------


def _expr(src: str) -> ast.expr:
    return ast.parse(src, mode="eval").body


class TestResolveStatic:
    @pytest.mark.parametrize(
        ("src", "expected"),
        [
            ("'q'", "q"),
            ("30", 30),
            ("-5", -5),
            ("+5", 5),
            ("-1.5", -1.5),
            ("1+2j", 1 + 2j),
            ("True", True),
            ("None", None),
            ("['a', ('b', 1)]", ["a", ("b", 1)]),
            ("{'a': [1, {'b': None}]}", {"a": [1, {"b": None}]}),
            ("{'a', 'b'}", {"a", "b"}),
            ("set()", set()),
        ],
    )
    def test_literal(self, src, expected):
        result = _resolve_static(_expr(src), {})
        assert result == expected
        assert type(result) is type(expected)

    def test_constant_names_inside_displays(self):
        constants = {"Q1": "q1", "Q2": "q2", "PAID": "paid", "KEY": "body.type", "N": 3}
        assert _resolve_static(_expr("[Q1, Q2]"), constants) == ["q1", "q2"]
        assert _resolve_static(_expr("{KEY: PAID, 'n': -N}"), constants) == {
            "body.type": "paid", "n": -3,
        }
        assert _resolve_static(_expr("(Q1, [Q2, {'x': N}])"), constants) == (
            "q1", ["q2", {"x": 3}],
        )

    @pytest.mark.parametrize(
        ("src", "fragment"),
        [
            ("get_conn()", "function call"),
            ("settings.CONN", "attribute access"),
            ("os.environ['X']", "subscript"),
            ("f'{ENV}.orders'", "f-string"),
            ("ENV + '.orders'", "operator expression"),
            ("{**X}", "unpacks another dict"),
            ("[*Q]", "starred"),
            ("-'a'", "other than a number"),
            ("-True", "other than a number"),
            ("[Q, get()]", "function call"),
            ("{'k': get()}", "function call"),
            ("{[1]: 'v'}", "unhashable"),
            ("UNKNOWN", "'UNKNOWN' is not a constant the scan can read"),
        ],
    )
    def test_unreadable_has_reason(self, src, fragment):
        result = _resolve_static(_expr(src), {"ENV": "prod", "X": {"a": 1}, "Q": "q"})
        assert isinstance(result, _Unresolved)
        assert fragment in result.reason

    def test_constant_value_is_copied(self):
        constants = {"FILTER": {"body": {"type": ["paid"]}}}
        result = _resolve_static(_expr("FILTER"), constants)
        assert result == constants["FILTER"]
        result["body"]["type"].append("free")
        result["extra"] = 1
        assert constants == {"FILTER": {"body": {"type": ["paid"]}}}

    def test_constant_inside_display_is_copied(self):
        constants = {"QUEUES": ["q1"]}
        result = _resolve_static(_expr("[QUEUES]"), constants)
        result[0].append("q2")
        assert constants == {"QUEUES": ["q1"]}


# ---------------------------------------------------------------------------
# _read_rmq_trigger_call
# ---------------------------------------------------------------------------


def _read_call(src: str, function: str = "f"):
    """Read the single ``rmq_trigger`` decorator of ``function`` as the scan would."""
    tree = ast.parse(src)
    module_constants, module_scope_ids = _collect_module_constants(tree)
    func = next(
        node for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function
    )
    calls = [dec for dec in func.decorator_list if _is_rmq_trigger_call(dec)]
    assert len(calls) == 1
    constants = _usable_constants(func, module_constants, module_scope_ids)
    return _read_rmq_trigger_call(calls[0], constants)


def _decorated(decorator: str, prelude: str = "") -> str:
    return f"{prelude}@{decorator}\n@dag(dag_id='d')\ndef f():\n    pass\n"


class TestReadRmqTriggerCall:
    def test_bare_constant_name(self):
        read = _read_call(_decorated(
            "rmq_trigger(queue='q', conn_id=RMQ_CONN)", "RMQ_CONN = 'rmq_prod'\n"
        ))
        assert read.kwargs == {"queue": "q", "conn_id": "rmq_prod"}
        assert read.unreadable == []

    def test_positional_queue_constant(self):
        read = _read_call(_decorated("rmq_trigger(QUEUE)", "QUEUE = 'orders'\n"))
        assert read.kwargs == {"queue": "orders"}
        assert read.unreadable == []

    def test_positional_conn_id_filter_data_cooldown(self):
        read = _read_call(_decorated(
            "rmq_trigger(QUEUE, None, RMQ_CONN, {'body.type': 'paid'}, COOLDOWN)",
            "QUEUE = 'orders'\nRMQ_CONN = 'rmq_prod'\nCOOLDOWN = 30\n",
        ))
        assert read.kwargs == {
            "queue": "orders",
            "queues": None,
            "conn_id": "rmq_prod",
            "filter_data": {"body.type": "paid"},
            "cooldown": 30,
        }
        assert read.unreadable == []

    def test_queues_list_of_constants(self):
        read = _read_call(_decorated(
            "rmq_trigger(queues=[Q1, Q2])", "Q1 = 'a'\nQ2 = 'b'\n"
        ))
        assert read.kwargs == {"queues": ["a", "b"]}

    def test_filter_data_with_constant_value(self):
        read = _read_call(_decorated(
            "rmq_trigger(queue='q', filter_data={'body.type': PAID})", "PAID = 'paid'\n"
        ))
        assert read.kwargs == {"queue": "q", "filter_data": {"body.type": "paid"}}

    def test_unpacked_dict_constant(self):
        read = _read_call(_decorated(
            "rmq_trigger(**RMQ_OPTS)",
            "RMQ_OPTS = {'queue': 'q', 'conn_id': 'rmq_prod', 'cooldown': 30}\n",
        ))
        assert read.kwargs == {"queue": "q", "conn_id": "rmq_prod", "cooldown": 30}
        assert read.unreadable == []

    def test_unpacked_dict_constant_mixed_with_explicit_arguments(self):
        read = _read_call(_decorated(
            "rmq_trigger(QUEUE, filter_data={'k': 'v'}, **RMQ_OPTS)",
            "QUEUE = 'orders'\nRMQ_OPTS = {'conn_id': 'rmq_prod', 'ack_unmatched': True}\n",
        ))
        assert read.kwargs == {
            "queue": "orders",
            "filter_data": {"k": "v"},
            "conn_id": "rmq_prod",
            "ack_unmatched": True,
        }
        assert read.unreadable == []

    def test_attribute_form(self):
        read = _read_call(_decorated(
            "decorators.rmq_trigger(exchange=EX, routing_key_ids=['job'], "
            "routing_key_status=STATUS)",
            "EX = 'events'\nSTATUS = 'succeeded'\n",
        ))
        assert read.kwargs == {
            "exchange": "events",
            "routing_key_ids": ["job"],
            "routing_key_status": "succeeded",
        }

    def test_read_values_do_not_share_objects_with_constants(self):
        src = _decorated(
            "rmq_trigger(queue='q', filter_data=FILTER)", "FILTER = {'a': ['b']}\n"
        )
        tree = ast.parse(src)
        module_constants, module_scope_ids = _collect_module_constants(tree)
        func = tree.body[-1]
        assert isinstance(func, ast.FunctionDef)
        constants = _usable_constants(func, module_constants, module_scope_ids)
        call = func.decorator_list[0]
        assert isinstance(call, ast.Call)
        read = _read_rmq_trigger_call(call, constants)
        read.kwargs["filter_data"]["a"].append("c")
        assert constants["FILTER"] == {"a": ["b"]}
        assert module_constants["FILTER"] == ({"a": ["b"]}, 1)


class TestReadRmqTriggerCallUnreadable:
    @staticmethod
    def _single(read) -> tuple[str, str]:
        assert len(read.unreadable) == 1, read.unreadable
        return read.unreadable[0]

    @pytest.mark.parametrize(
        ("decorator", "prelude", "param", "fragment"),
        [
            ("rmq_trigger(queue='q', conn_id=RMQ_CONN)",
             "from common.rmq import RMQ_CONN\n", "conn_id", "'RMQ_CONN' is not a constant"),
            ("rmq_trigger(queue='q', conn_id=get_conn())", "", "conn_id", "function call"),
            ("rmq_trigger(queue='q', conn_id=settings.RMQ_CONN)", "", "conn_id",
             "attribute access"),
            ("rmq_trigger(queue=f'{ENV}.orders')", "ENV = 'prod'\n", "queue", "f-string"),
            ("rmq_trigger(queue=ENV + '.orders')", "ENV = 'prod'\n", "queue",
             "operator expression"),
            ("rmq_trigger(queue='q', filter_data={**BASE})", "BASE = {'a': 1}\n",
             "filter_data", "unpacks another dict"),
            ("rmq_trigger(*ARGS)", "ARGS = ('q',)\n", "*args", "starred"),
            ("rmq_trigger(queue='q', **load())", "", "**", "dict constant"),
            ("rmq_trigger(queue='q', **OPTS)", "OPTS = ['a']\n", "**OPTS",
             "not a dict constant"),
            ("rmq_trigger(queues_=['q'])", "", "queues_", "no such parameter"),
            ("rmq_trigger(**OPTS)", "OPTS = {'queue': 'q', 'queues_': ['q']}\n",
             "**OPTS['queues_']", "no such parameter"),
            ("rmq_trigger('q', queue='r')", "", "queue", "more than once"),
            ("rmq_trigger(queue='q', **OPTS)", "OPTS = {'queue': 'r'}\n", "queue",
             "more than once"),
            ("rmq_trigger('q', None, 'c', None, 0, 'extra')", "", "positional argument 6",
             "at most 5"),
        ],
        ids=["imported_name", "call", "attribute", "f_string", "concatenation",
             "dict_unpacking_in_display", "star_args", "unpacked_call",
             "unpacked_non_dict_constant", "unknown_keyword", "unknown_key_in_unpacking",
             "positional_and_keyword", "keyword_and_unpacking", "sixth_positional"],
    )
    def test_reason(self, decorator, prelude, param, fragment):
        read = _read_call(_decorated(decorator, prelude))
        got_param, reason = self._single(read)
        assert got_param == param
        assert fragment in reason

    def test_readable_arguments_beside_an_unreadable_one_are_kept(self):
        read = _read_call(_decorated("rmq_trigger(queue='q', conn_id=get_conn(), cooldown=5)"))
        assert read.kwargs == {"queue": "q", "cooldown": 5}
        assert [param for param, _ in read.unreadable] == ["conn_id"]

    def test_constant_below_the_function(self):
        src = _decorated("rmq_trigger(queue='q', conn_id=RMQ_CONN)") + "RMQ_CONN = 'rmq_prod'\n"
        param, reason = self._single(_read_call(src))
        assert param == "conn_id"
        assert "'RMQ_CONN' is not a constant" in reason

    def test_constant_used_in_a_nested_function(self):
        src = (
            "RMQ_CONN = 'rmq_prod'\n"
            "def factory():\n"
            "    @rmq_trigger(queue='q', conn_id=RMQ_CONN)\n"
            "    @dag(dag_id='d')\n"
            "    def f():\n"
            "        pass\n"
            "    return f\n"
        )
        param, reason = self._single(_read_call(src))
        assert param == "conn_id"
        assert "'RMQ_CONN' is not a constant" in reason

    def test_literals_in_a_nested_function_are_read(self):
        src = (
            "def factory():\n"
            "    @rmq_trigger(queue='q', conn_id='rmq_prod')\n"
            "    @dag(dag_id='d')\n"
            "    def f():\n"
            "        pass\n"
            "    return f\n"
        )
        read = _read_call(src)
        assert read.kwargs == {"queue": "q", "conn_id": "rmq_prod"}
        assert read.unreadable == []

    def test_mutated_constant(self):
        prelude = "FILTER = {'body.type': 'paid'}\nFILTER['body.type'] = 'free'\n"
        param, reason = self._single(_read_call(_decorated(
            "rmq_trigger(queue='q', filter_data=FILTER)", prelude
        )))
        assert param == "filter_data"
        assert "'FILTER' is not a constant" in reason

    def test_mutated_constant_in_unpacking(self):
        prelude = "OPTS = {'queue': 'q'}\nOPTS['queue'] = 'r'\n"
        param, reason = self._single(_read_call(_decorated("rmq_trigger(**OPTS)", prelude)))
        assert param == "**OPTS"
        assert "'OPTS' is not a constant" in reason


# ---------------------------------------------------------------------------
# _usable_constants
# ---------------------------------------------------------------------------


class TestUsableConstants:
    SRC = (
        "A = 'a'\n"
        "def f():\n"
        "    def inner():\n"
        "        pass\n"
        "B = 'b'\n"
        "def g():\n"
        "    pass\n"
    )

    def _functions(self):
        tree = ast.parse(self.SRC)
        module_constants, module_scope_ids = _collect_module_constants(tree)
        funcs = {
            node.name: node for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)
        }
        return funcs, module_constants, module_scope_ids

    def test_only_constants_above_the_function(self):
        funcs, module_constants, ids = self._functions()
        assert _usable_constants(funcs["f"], module_constants, ids) == {"A": "a"}
        assert _usable_constants(funcs["g"], module_constants, ids) == {"A": "a", "B": "b"}

    def test_nested_function_gets_none(self):
        funcs, module_constants, ids = self._functions()
        assert _usable_constants(funcs["inner"], module_constants, ids) == {}
