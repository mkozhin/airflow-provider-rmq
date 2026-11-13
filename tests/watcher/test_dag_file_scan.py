from __future__ import annotations

import ast

import pytest

from airflow_provider_rmq.watcher.dag_file_scan import (
    _collect_module_constants,
    _is_rmq_trigger_call,
    _iter_trigger_functions,
    _read_and_parse,
    _read_file_triggers,
    _read_rmq_trigger_call,
    _resolve_static,
    _static_dag_id,
    _Unreadable,
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
    return _static_dag_id(func, constants)


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
        assert _dag_id_of_last_function(src) is None

    def test_non_string_constant_positional_is_unresolved(self):
        src = "DAG_ID = None\n@dag(DAG_ID)\ndef f(): pass\n"
        assert _dag_id_of_last_function(src) is None

    def test_empty_string_constant_falls_back_to_function_name(self):
        src = "DAG_ID = ''\n@dag(dag_id=DAG_ID)\ndef f(): pass\n"
        assert _dag_id_of_last_function(src) == "f"

    def test_string_constant_resolves(self):
        src = "DAG_ID = 'real'\n@dag(dag_id=DAG_ID)\ndef f(): pass\n"
        assert _dag_id_of_last_function(src) == "real"

    def test_none_literal_falls_back_to_function_name(self):
        src = "@dag(dag_id=None)\ndef f(): pass\n"
        assert _dag_id_of_last_function(src) == "f"


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
            ("True", True),
            ("None", None),
            ("['a', ('b', 1)]", ["a", ("b", 1)]),
            ("{'a': [1, {'b': None}]}", {"a": [1, {"b": None}]}),
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
            ("{'a', 'b'}", "a set display"),
            ("set()", "function call"),
            ("1+2j", "operator expression"),
            ("2j", "complex literal"),
            ("b'q'", "bytes literal"),
            ("...", "ellipsis literal"),
        ],
    )
    def test_unreadable_has_reason(self, src, fragment):
        with pytest.raises(_Unreadable) as exc_info:
            _resolve_static(_expr(src), {"ENV": "prod", "X": {"a": 1}, "Q": "q"})
        assert fragment in str(exc_info.value)


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


# ---------------------------------------------------------------------------
# Reading a whole file
# ---------------------------------------------------------------------------


#: The imports that bind both forms of the decorator the scan reads.
_TRIGGER_IMPORTS = (
    "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
    "from airflow_provider_rmq.watcher import decorators\n"
)


#: The import that binds Airflow's ``dag``; ``_file_read`` puts it first in every file.
_DAG_IMPORT = "from airflow.decorators import dag\n"


def _file_read(src: str, imports: str = _TRIGGER_IMPORTS):
    return _read_file_triggers(ast.parse(_DAG_IMPORT + imports + src))


class TestReadAndParse:
    def test_utf8_bom_is_accepted(self, tmp_path):
        path = tmp_path / "bom.py"
        path.write_bytes(b"\xef\xbb\xbfX = 'x'\n")
        tree = _read_and_parse(str(path))
        assert isinstance(tree.body[0], ast.Assign)

    def test_pep263_declaration_is_honoured(self, tmp_path):
        path = tmp_path / "latin1.py"
        path.write_bytes(b"# -*- coding: latin-1 -*-\nX = '\xe9'\n")
        tree = _read_and_parse(str(path))
        assert _collect_module_constants(tree)[0]["X"][0] == "é"

    def test_undecodable_source_raises_syntax_error(self, tmp_path):
        path = tmp_path / "bad.py"
        path.write_bytes(b"X = '\xff'\n")
        with pytest.raises(SyntaxError):
            _read_and_parse(str(path))

    def test_missing_file_raises_os_error(self, tmp_path):
        with pytest.raises(OSError):
            _read_and_parse(str(tmp_path / "absent.py"))

    def test_filename_is_carried_into_the_tree_errors(self, tmp_path):
        path = tmp_path / "broken.py"
        path.write_text("def (:\n")
        with pytest.raises(SyntaxError) as exc_info:
            _read_and_parse(str(path))
        assert exc_info.value.filename == str(path)


class TestIterTriggerFunctions:
    def test_only_functions_with_an_rmq_trigger_call(self):
        tree = ast.parse(
            "A = 'a'\n"
            "@rmq_trigger(queue='q')\n@dag()\ndef with_trigger(): pass\n"
            "@dag()\ndef without_trigger(): pass\n"
            "def factory():\n"
            "    @rmq_trigger(queue='n')\n    @dag()\n    def nested(): pass\n"
        )
        found = {func.name: constants for func, constants in _iter_trigger_functions(tree)}
        assert found == {"with_trigger": {"A": "a"}, "nested": {}}


class TestStaticDagId:
    def _func(self, src: str):
        tree = ast.parse(src)
        func = tree.body[-1]
        assert isinstance(func, ast.FunctionDef)
        return func

    def test_literal(self):
        assert _static_dag_id(self._func("@dag(dag_id='x')\ndef f(): pass\n"), {}) == "x"

    def test_constant(self):
        func = self._func("@dag(dag_id=D)\ndef f(): pass\n")
        assert _static_dag_id(func, {"D": "from_const"}) == "from_const"

    def test_no_dag_id_falls_back_to_function_name(self):
        assert _static_dag_id(self._func("@dag()\ndef f(): pass\n"), {}) == "f"

    def test_unresolvable_is_none(self):
        assert _static_dag_id(self._func("@dag(dag_id=f'{X}')\ndef f(): pass\n"), {}) is None

    def test_no_dag_decorator_is_none(self):
        assert _static_dag_id(self._func("@rmq_trigger(queue='q')\ndef f(): pass\n"), {}) is None


class TestReadFileTriggers:
    def test_readable_file_gives_rows_with_dag_id_and_group_key(self):
        read = _file_read(
            "RMQ_CONN = 'rmq_prod'\n"
            "COOLDOWN = 30\n"
            "@rmq_trigger(queue='q', conn_id=RMQ_CONN, cooldown=COOLDOWN)\n"
            "@dag(dag_id='d')\n"
            "def d(): pass\n"
        )
        assert read.problems == []
        assert len(read.subscriptions) == 1
        sub = read.subscriptions[0]
        assert (sub["dag_id"], sub["conn_id"], sub["cooldown"], sub["group_key"]) == (
            "d", "rmq_prod", 30, "d",
        )

    def test_one_problem_empties_the_whole_file(self):
        read = _file_read(
            "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n"
            "@rmq_trigger(queue=get_queue())\n@dag(dag_id='b')\ndef b(): pass\n"
        )
        assert read.subscriptions == []
        assert len(read.problems) == 1
        assert "dag_id='b'" in read.problems[0]

    def test_builder_refusal_carries_the_builder_text(self):
        read = _file_read(
            "@rmq_trigger(queue='a')\n"
            "@rmq_trigger(queue='b', cooldown=-1)\n"
            "@dag(dag_id='d')\n"
            "def d(): pass\n"
        )
        assert read.subscriptions == []
        assert "'cooldown' must be >= 0" in read.problems[0]

    def test_unresolvable_dag_id_is_a_problem(self):
        read = _file_read(
            "@rmq_trigger(queue='a')\n@dag(dag_id=make_id())\ndef a(): pass\n"
        )
        assert read.subscriptions == []
        assert "cannot statically resolve dag_id" in read.problems[0]
        assert "'a'" in read.problems[0]

    def test_unresolvable_dag_id_without_rmq_trigger_is_no_problem(self):
        read = _file_read(
            "@dag(dag_id=make_id())\ndef other(): pass\n"
            "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n"
        )
        assert read.problems == []
        assert [s["dag_id"] for s in read.subscriptions] == ["a"]

    def test_rmq_trigger_below_dag_is_a_problem(self):
        read = _file_read("@dag(dag_id='a')\n@rmq_trigger(queue='a')\ndef a(): pass\n")
        assert read.subscriptions == []
        assert any("placed below @dag(...)" in p for p in read.problems)

    @pytest.mark.parametrize(
        "decorators_",
        [
            pytest.param("@rmq_trigger(queue='a')\n@dag()\n@no_wraps\n", id="below-dag"),
            pytest.param("@rmq_trigger(queue='a')\n@rename\n@dag()\n",
                         id="between-rmq-trigger-and-dag"),
            pytest.param("@rmq_trigger(queue='a')\n@helpers.wrap(1)\n"
                         "@rmq_trigger(queue='b')\n@dag()\n",
                         id="between-two-rmq-triggers"),
        ],
    )
    def test_other_decorator_below_rmq_trigger_refuses_the_file(self, decorators_):
        read = _file_read(
            "@rmq_trigger(queue='n')\n@dag(dag_id='neighbour')\ndef neighbour(): pass\n"
            + decorators_ + "def my_dag(): pass\n"
        )
        assert read.subscriptions == []
        assert any(
            "'my_dag'" in p and "sits below @rmq_trigger(...)" in p for p in read.problems
        ), read.problems

    def test_other_decorator_above_rmq_trigger_keeps_the_file_readable(self):
        read = _file_read(
            "@log_calls\n@helpers.wrap(1)\n@rmq_trigger(queue='a')\n@dag()\ndef my_dag(): pass\n"
        )
        assert read.problems == []
        assert [(s["dag_id"], s["queue_name"]) for s in read.subscriptions] == [
            ("my_dag", "a"),
        ]

    @pytest.mark.parametrize(
        ("src", "expected"),
        [
            ("rmq_trigger(queue='b')(DAG('B'))\n", "the name rmq_trigger"),
            ("from m import rmq_trigger as rt\n", "imported as 'rt'"),
            ("import m.rmq_trigger as rt\n", "imported as 'rt'"),
            ("rt = decorators.rmq_trigger\n", "the attribute .rmq_trigger"),
            ("RMQ = partial(rmq_trigger, conn_id='c')\n", "the name rmq_trigger"),
            ("helper(rmq_trigger)\n", "the name rmq_trigger"),
            ("@rmq_trigger\n@dag(dag_id='b')\ndef b(): pass\n", "the name rmq_trigger"),
            ("@rmq_trigger(queue='b')\nclass B: pass\n", "the name rmq_trigger"),
            ("@rmq_trigger(queue='b')(x)\n@dag(dag_id='b')\ndef b(): pass\n",
             "the name rmq_trigger"),
            ("def rmq_trigger(**kw): pass\n", "defines its own rmq_trigger"),
        ],
    )
    def test_mentions_the_scan_cannot_follow(self, src, expected):
        read = _file_read(
            "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n" + src
        )
        assert read.subscriptions == []
        assert any(expected in p for p in read.problems), read.problems

    @pytest.mark.parametrize(
        "imports",
        [
            pytest.param(_TRIGGER_IMPORTS, id="from-decorators-and-from-watcher"),
            pytest.param(
                "from airflow_provider_rmq.watcher.decorators import rmq_trigger as rmq_trigger\n"
                "import airflow_provider_rmq.watcher.decorators as decorators\n",
                id="import-as-own-name-and-module-alias",
            ),
            pytest.param(
                "from airflow_provider_rmq.watcher import rmq_trigger, decorators\n",
                id="package-re-export",
            ),
            pytest.param(
                _TRIGGER_IMPORTS + "NAME = 'rmq_trigger'\n",
                id="string-holding-the-name",
            ),
        ],
    )
    def test_mentions_the_scan_follows(self, imports):
        read = _file_read(
            "@rmq_trigger(queue='a')\n@decorators.rmq_trigger(queue='b')\n"
            "@dag(dag_id='a')\ndef a(): pass\n",
            imports,
        )
        assert read.problems == []
        assert [s["queue_name"] for s in read.subscriptions] == ["a", "b"]

    @pytest.mark.parametrize(
        ("imports", "decorator"),
        [
            pytest.param(
                "import airflow_provider_rmq.watcher.decorators\n",
                "airflow_provider_rmq.watcher.decorators.rmq_trigger",
                id="dotted-module-path",
            ),
            pytest.param(
                "from airflow_provider_rmq import watcher\n",
                "watcher.rmq_trigger",
                id="package-attribute",
            ),
            pytest.param(
                "import airflow_provider_rmq.watcher.decorators\nimport airflow_provider_rmq\n",
                "airflow_provider_rmq.watcher.decorators.rmq_trigger",
                id="top-package-imported-twice",
            ),
        ],
    )
    def test_attribute_forms_bound_to_the_provider_are_read(self, imports, decorator):
        read = _file_read(
            f"@{decorator}(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n", imports
        )
        assert read.problems == []
        assert [s["queue_name"] for s in read.subscriptions] == ["a"]

    @pytest.mark.parametrize(
        ("imports", "decorator", "expected"),
        [
            pytest.param(
                "from common_rmq import rmq_trigger\n", "rmq_trigger",
                "rmq_trigger is common_rmq.rmq_trigger", id="same-name-of-another-module",
            ),
            pytest.param(
                "from .common import rmq_trigger\n", "rmq_trigger",
                "rmq_trigger is not bound by one absolute import alone", id="relative-import",
            ),
            pytest.param(
                "from common import *\n", "rmq_trigger",
                "'from ... import *'", id="wildcard-import",
            ),
            pytest.param(
                _TRIGGER_IMPORTS + "from common import *\n", "rmq_trigger",
                "'from ... import *'", id="wildcard-beside-the-provider-import",
            ),
            pytest.param("", "rmq_trigger", "rmq_trigger is not bound by one absolute import alone",
                         id="no-import"),
            pytest.param(
                "import common_rmq as decorators\n", "decorators.rmq_trigger",
                "decorators.rmq_trigger is common_rmq.rmq_trigger", id="other-module-alias",
            ),
            pytest.param(
                "from airflow_provider_rmq.watcher import decorators\n"
                "decorators = None\n",
                "decorators.rmq_trigger",
                "decorators.rmq_trigger is not bound by one absolute import alone", id="alias-rebound",
            ),
            pytest.param(
                "from airflow_provider_rmq.watcher.decorators import rmq_trigger\n"
                "def swap():\n    global rmq_trigger\n    rmq_trigger = None\n",
                "rmq_trigger",
                "rmq_trigger is not bound by one absolute import alone", id="global-rebinding",
            ),
            pytest.param(
                "from airflow_provider_rmq.watcher import decorators\n",
                "get_module().rmq_trigger",
                "is not bound by one absolute import alone", id="computed-base",
            ),
        ],
    )
    def test_rmq_trigger_not_bound_to_the_provider_refuses_the_file(
        self, imports, decorator, expected
    ):
        read = _file_read(
            "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n"
            .replace("rmq_trigger", decorator, 1),
            imports,
        )
        assert read.subscriptions == []
        assert any(expected in p for p in read.problems), read.problems
        assert any("airflow_provider_rmq.watcher.decorators" in p for p in read.problems)

    @pytest.mark.parametrize(
        ("imports", "decorator"),
        [
            pytest.param("from airflow.decorators import dag\n", "dag", id="from-decorators"),
            pytest.param("from airflow.models.dag import dag\n", "dag", id="from-models-dag"),
            pytest.param("from airflow import decorators as af\n", "af.dag",
                         id="decorators-module"),
            pytest.param("import airflow.decorators\n", "airflow.decorators.dag",
                         id="dotted-decorators-path"),
            pytest.param("import airflow.models.dag as dag_module\n", "dag_module.dag",
                         id="models-dag-module-alias"),
        ],
    )
    def test_dag_bound_to_airflow_is_read(self, imports, decorator):
        read = _read_file_triggers(ast.parse(
            _TRIGGER_IMPORTS + imports
            + f"@rmq_trigger(queue='a')\n@{decorator}(dag_id='a')\ndef a(): pass\n"
        ))
        assert read.problems == []
        assert [(s["dag_id"], s["queue_name"]) for s in read.subscriptions] == [("a", "a")]

    @pytest.mark.parametrize(
        ("imports", "decorator", "expected"),
        [
            pytest.param("from company import dag\n", "dag",
                         "dag is company.dag", id="same-name-of-another-module"),
            pytest.param("from .common import dag\n", "dag",
                         "dag is not bound by one absolute import alone", id="relative-import"),
            pytest.param("", "dag", "dag is not bound by one absolute import alone",
                         id="no-import"),
            pytest.param("from airflow.models import dag\n", "dag",
                         "dag is airflow.models.dag,", id="the-module-not-the-decorator"),
            pytest.param(
                "from airflow.decorators import dag\n"
                "def dag(dag_id, **kw): pass\n",
                "dag", "dag is not bound by one absolute import alone",
                id="local-definition",
            ),
            pytest.param("import company as af\n", "af.dag",
                         "af.dag is company.dag", id="other-module-alias"),
            pytest.param(
                "from airflow.decorators import dag\nfrom common import *\n", "dag",
                "'from ... import *'", id="wildcard-import",
            ),
        ],
    )
    def test_dag_not_bound_to_airflow_refuses_the_file(self, imports, decorator, expected):
        read = _read_file_triggers(ast.parse(
            _TRIGGER_IMPORTS + imports
            + "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n"
            + "@rmq_trigger(queue='b')\n"
            + f"@{decorator}(dag_id='b')\ndef b(): pass\n"
        ))
        assert read.subscriptions == []
        assert any(expected in p for p in read.problems), read.problems
        assert any("airflow.decorators" in p for p in read.problems)

    @pytest.mark.parametrize(
        ("before", "after"),
        [
            pytest.param(
                "def on_failure(context):\n    dag = context['dag']\n    return dag\n",
                "", id="local-in-a-callback",
            ),
            pytest.param("def helper(dag):\n    return dag\n", "", id="parameter"),
            pytest.param("def other():\n    dag = 1\n", "", id="local-in-another-function"),
            pytest.param("pick = lambda dag: dag\n", "", id="lambda-parameter"),
            pytest.param("IDS = [dag for dag in ('x', 'y')]\n", "", id="comprehension-variable"),
            pytest.param("class Holder:\n    dag = None\n", "", id="class-attribute"),
            pytest.param("", "dag = a()\n", id="factory-result-below"),
            pytest.param(
                "", "from airflow import DAG\nwith DAG('b') as dag:\n    pass\n",
                id="with-dag-as-dag-below",
            ),
            pytest.param(
                "def helper(decorators, rmq_trigger):\n    return decorators\n", "",
                id="provider-names-as-parameters",
            ),
        ],
    )
    def test_bindings_the_decorator_cannot_see_keep_the_file_readable(self, before, after):
        read = _file_read(
            before
            + "@rmq_trigger(queue='a')\n@decorators.rmq_trigger(queue='b')\n"
            + "@dag(dag_id='a')\ndef a():\n"
            + "    @task\n    def t(dag=None):\n        return dag\n    t()\n"
            + after
        )
        assert read.problems == []
        assert [(s["dag_id"], s["queue_name"]) for s in read.subscriptions] == [
            ("a", "a"), ("a", "b"),
        ]

    def test_nested_dag_function_ignores_bindings_of_other_scopes(self):
        read = _file_read(
            "def helper(dag):\n    return dag\n"
            "def make():\n"
            "    @rmq_trigger(queue='n')\n    @dag(dag_id='nested')\n    def nested():\n"
            "        dag = 1\n"
            "    def callback(dag): pass\n"
            "    return nested()\n"
            "dag_object = make()\n"
        )
        assert read.problems == []
        assert [s["dag_id"] for s in read.subscriptions] == ["nested"]

    @pytest.mark.parametrize(
        "after",
        [
            pytest.param("dag = make()\n", id="factory-result-assigned-to-dag"),
            pytest.param("dag: object = make()\n", id="annotated-factory-result"),
            pytest.param("first, dag = 1, make()\n", id="tuple-assignment-calling-the-factory"),
            pytest.param(
                "make()\nfrom airflow import DAG\nwith DAG('legacy') as dag:\n    pass\n",
                id="with-dag-as-dag-below-the-factory-call",
            ),
            pytest.param(
                "if ENABLED:\n    made = make()\nelse:\n    dag = None\n",
                id="rebinding-below-the-call-under-if",
            ),
            pytest.param("", id="factory-never-mentioned-and-no-binding-below"),
        ],
    )
    def test_module_bindings_below_a_factory_call_keep_the_file_readable(self, after):
        read = _file_read(
            "def make():\n"
            "    @rmq_trigger(queue='n')\n    @decorators.rmq_trigger(queue='m')\n"
            "    @dag(dag_id='factory_dag')\n    def nested(): pass\n"
            "    return nested()\n"
            + after
        )
        assert read.problems == []
        assert [(s["dag_id"], s["queue_name"]) for s in read.subscriptions] == [
            ("factory_dag", "n"), ("factory_dag", "m"),
        ]

    @pytest.mark.parametrize(
        "src",
        [
            pytest.param("dag = None\n" + "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n",
                         id="module-rebinding-above"),
            pytest.param(
                "from airflow import DAG\nwith DAG('b') as dag:\n    pass\n"
                "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n",
                id="with-dag-as-dag-above",
            ),
            pytest.param(
                "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n"
                "def swap():\n    global dag\n    dag = None\n",
                id="global-in-a-function-below",
            ),
            pytest.param(
                "IDS = [(dag := i) for i in range(2)]\n"
                "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n",
                id="walrus-in-a-comprehension-above",
            ),
            pytest.param(
                "for _ in range(2):\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    dag = None\n",
                id="rebinding-below-in-a-loop",
            ),
            pytest.param(
                "def make(dag):\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n",
                id="parameter-of-the-enclosing-factory",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    dag = a()\n    return dag\n",
                id="local-of-the-enclosing-factory",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "dag = None\nmake()\n",
                id="module-rebinding-below-a-factory",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "dag = None; make()\n",
                id="factory-call-after-a-rebinding-on-the-same-line",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "from airflow import DAG\nwith DAG('legacy') as dag:\n    made = make()\n",
                id="factory-call-inside-with-dag-as-dag",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "def build():\n    return make()\n"
                "dag = None\n",
                id="factory-called-from-a-function",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "for _ in range(2):\n    made = make()\n"
                "dag = None\n",
                id="factory-called-in-a-loop",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "made = make() if (dag := None) is None else None\n",
                id="walrus-below-a-factory",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "made = make()\nalias = make\ndag = None\n",
                id="factory-assigned-to-another-name",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "FACTORIES = []\nFACTORIES.append(make)\n"
                "from airflow import DAG\nwith DAG('legacy') as dag:\n    pass\n"
                "for factory in FACTORIES:\n    factory()\n",
                id="factory-appended-to-a-list",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "register(make)\ndag = None\n",
                id="factory-passed-to-a-call",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n"
                "from airflow import DAG\nwith DAG('legacy') as dag:\n    pass\n",
                id="factory-never-mentioned",
            ),
            pytest.param(
                "class Holder:\n    dag = None\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n",
                id="class-attribute-above-a-method",
            ),
        ],
    )
    def test_bindings_the_decorator_sees_refuse_the_file(self, src):
        read = _file_read(src)
        assert read.subscriptions == []
        assert any("dag is not bound by one absolute import alone" in p for p in read.problems), (
            read.problems
        )

    def test_import_below_the_decorated_function_is_not_seen(self):
        read = _read_file_triggers(ast.parse(
            _TRIGGER_IMPORTS
            + "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n"
            + "from airflow.decorators import dag\n"
        ))
        assert read.subscriptions == []
        assert any("dag is not bound by one absolute import alone" in p for p in read.problems)

    def test_foreign_dag_without_rmq_trigger_is_no_problem(self):
        read = _read_file_triggers(ast.parse(
            _TRIGGER_IMPORTS
            + "from airflow.decorators import dag\n"
            + "import company\n"
            + "@company.dag(dag_id='other')\ndef other(): pass\n"
            + "@rmq_trigger(queue='a')\n@dag(dag_id='a')\ndef a(): pass\n"
        ))
        assert read.problems == []
        assert [s["dag_id"] for s in read.subscriptions] == ["a"]

    def test_calls_record_their_lines_dag_id_and_built_subscriptions(self):
        read = _file_read(
            "@rmq_trigger(queue='a')\n"
            "@rmq_trigger(\n    queue='b',\n    cooldown=5,\n)\n"
            "@dag(dag_id='d')\ndef d(): pass\n",
            imports="from airflow_provider_rmq.watcher.decorators import rmq_trigger\n",
        )
        assert read.problems == []
        assert [(c.first_line, c.last_line, c.dag_id) for c in read.calls] == [
            (3, 3, "d"), (4, 7, "d"),
        ]
        assert read.calls[1].subscriptions == [{
            "queue_name": "b", "conn_id": "rmq_default", "filter_data": {},
            "cooldown": 5, "ack_unmatched": False,
        }]

    def test_nested_factory_and_module_if_with_literals(self):
        read = _file_read(
            "def make():\n"
            "    @rmq_trigger(queue='n')\n    @dag(dag_id='nested')\n    def nested(): pass\n"
            "    return nested()\n"
            "if ENABLED:\n"
            "    @rmq_trigger(queue='i')\n    @dag(dag_id='under_if')\n    def under_if(): pass\n"
        )
        assert read.problems == []
        assert sorted((s["dag_id"], s["queue_name"]) for s in read.subscriptions) == [
            ("nested", "n"), ("under_if", "i"),
        ]

    def test_file_without_rmq_trigger_is_empty_and_clean(self):
        read = _file_read("@dag(dag_id='a')\ndef a(): pass\n")
        assert read == ([], [], [])
