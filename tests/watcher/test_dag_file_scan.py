from __future__ import annotations

import ast
import inspect
import subprocess
import sys

import pytest

from airflow_provider_rmq.watcher.dag_file_scan import (
    _RMQ_TRIGGER_KWARGS,
    _RMQ_TRIGGER_POSITIONAL,
    _collect_module_constants,
    _is_rmq_trigger_call,
    _iter_trigger_functions,
    _read_and_parse,
    _read_file_triggers,
    _read_function_triggers,
    _read_rmq_trigger_call,
    _resolve_static,
    _static_dag_id,
    _Unreadable,
    _usable_constants,
)
from airflow_provider_rmq.watcher.decorators import rmq_trigger


def _constants(src: str) -> dict:
    """Parse a module source snippet and return its constant map."""
    return _collect_module_constants(ast.parse(src))[0]


def _function(src: str):
    """Parse a one-function snippet and return its function node."""
    return ast.parse(src).body[0]


def _dag_id_of_last_function(src: str):
    """Resolve ``dag_id`` of the last top-level function against the file's constants."""
    tree = ast.parse(src)
    constants = {name: value for name, (value, _) in _collect_module_constants(tree)[0].items()}
    func = tree.body[-1]
    assert isinstance(func, ast.FunctionDef)
    return _static_dag_id(func, constants)


# ---------------------------------------------------------------------------
# dag_file_scan import footprint
# ---------------------------------------------------------------------------


class TestDagFileScanImportFootprint:
    def test_importing_the_scan_module_leaves_the_watcher_runtime_unloaded(self):
        """The decorator imports ``dag_file_scan`` inside the DAG processor, so the
        module must not drag in Airflow, the DAG folder listing, the listener, the
        consumer, the runner, the ORM models, ``aio_pika`` or ``httpx``. A fresh
        interpreter is the only place where ``sys.modules`` says what one import
        pulled in."""
        probe = (
            "import sys\n"
            "import airflow_provider_rmq.watcher.dag_file_scan\n"
            "loaded = [\n"
            "    name for name in (\n"
            "        'airflow',\n"
            "        'airflow_provider_rmq.watcher.dag_folder_listing',\n"
            "        'airflow_provider_rmq.watcher.listener',\n"
            "        'airflow_provider_rmq.watcher.consumer',\n"
            "        'airflow_provider_rmq.watcher.models',\n"
            "        'airflow_provider_rmq.watcher.runner',\n"
            "        'aio_pika',\n"
            "        'httpx',\n"
            "    ) if name in sys.modules\n"
            "]\n"
            "print(','.join(loaded))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", probe],
            capture_output=True, text=True, timeout=60, check=False,
        )

        assert result.returncode == 0, result.stderr
        assert result.stdout.strip() == ""


# ---------------------------------------------------------------------------
# _collect_module_constants
# ---------------------------------------------------------------------------


class TestCollectModuleConstantsBaseCases:
    def test_single_string_constant_resolves_with_correct_lineno(self):
        result = _constants("DAG_ID = 'my_dag'\n")
        assert result == {"DAG_ID": ("my_dag", 1)}

    def test_single_string_constant_lineno_matches_assignment_line(self):
        result = _constants("\n\nDAG_ID = 'my_dag'\n")
        assert result["DAG_ID"] == ("my_dag", 3)

    def test_annotated_assignment_resolves(self):
        result = _constants("DAG_ID: str = 'my_dag'\n")
        assert result == {"DAG_ID": ("my_dag", 1)}

    def test_non_string_constant_collected(self):
        result = _constants("DAG_ID = 123\n")
        assert result == {"DAG_ID": (123, 1)}

    def test_non_literal_rhs_ignored(self):
        result = _constants("DAG_ID = some_func()\n")
        assert result == {}


class TestCollectModuleConstantsReassignment:
    """All of these forms poison the constant → empty map."""

    def test_re_literal_assignment(self):
        result = _constants("DAG_ID = 'first'\nDAG_ID = 'second'\n")
        assert result == {}

    def test_aug_assign(self):
        result = _constants("DAG_ID = 'first'\nDAG_ID += '_daily'\n")
        assert result == {}

    def test_tuple_unpacking(self):
        result = _constants("DAG_ID, Q = 'real', 'q'\n")
        assert result == {}

    def test_chained_assignment(self):
        result = _constants("DAG_ID = OTHER = 'real'\n")
        assert result == {}

    def test_re_import(self):
        result = _constants("DAG_ID = 'real'\nfrom settings import DAG_ID\n")
        assert result == {}

    def test_import_as(self):
        result = _constants("DAG_ID = 'real'\nimport x as DAG_ID\n")
        assert result == {}

    def test_def_with_same_name(self):
        result = _constants("DAG_ID = 'real'\ndef DAG_ID(): pass\n")
        assert result == {}

    def test_class_with_same_name(self):
        result = _constants("DAG_ID = 'real'\nclass DAG_ID: pass\n")
        assert result == {}

    def test_del(self):
        result = _constants("DAG_ID = 'real'\ndel DAG_ID\n")
        assert result == {}

    def test_top_level_for_target(self):
        result = _constants("DAG_ID = 'real'\nfor DAG_ID in range(3): pass\n")
        assert result == {}

    def test_with_as(self):
        result = _constants("DAG_ID = 'real'\nwith open('x') as DAG_ID: pass\n")
        assert result == {}

    def test_conditional_reassignment_inside_if(self):
        result = _constants("DAG_ID = 'old'\nif True:\n    DAG_ID = 'new'\n")
        assert result == {}


class TestCollectModuleConstantsExoticBindingForms:
    """Module-scope binding forms that earlier, less complete enumerations
    of binding forms missed — all must poison the constant."""

    @pytest.mark.parametrize(
        "src",
        [
            "DAG_ID = 'real'\ntry:\n    pass\nexcept Exception as DAG_ID:\n    pass\n",
            "DAG_ID = 'real'\ndef f():\n    global DAG_ID\n    DAG_ID = 'local'\n",
            "DAG_ID = 'real'\nif (DAG_ID := 'walrus'):\n    pass\n",
            "DAG_ID = 'real'\nmatch 1:\n    case DAG_ID:\n        pass\n",
            "DAG_ID = 'real'\nmatch [1, 2]:\n    case [*DAG_ID]:\n        pass\n",
            "DAG_ID = 'real'\nmatch {'k': 1}:\n    case {'k': _, **DAG_ID}:\n        pass\n",
            "DAG_ID = 'real'\nif True:\n    for DAG_ID in range(3):\n        pass\n",
        ],
        ids=[
            "except_as",
            "global_plus_function_assign",
            "walrus_in_if",
            "match_as",
            "match_star",
            "match_mapping_rest",
            "nested_for_inside_if",
        ],
    )
    def test_exotic_binding_forms_poison_constant(self, src):
        assert _constants(src) == {}


class TestCollectModuleConstantsWildcardImport:
    def test_wildcard_import_disables_resolution_entirely(self):
        result = _constants("DAG_ID = 'local'\nfrom settings import *\n")
        assert result == {}

    def test_wildcard_import_disables_unrelated_constant_too(self):
        # With only one constant name in the file, "DAG_ID absent" is
        # ambiguous between "wildcard poisons file-wide" and "wildcard only
        # poisons names that plausibly collide with its exports". A second,
        # entirely unrelated constant name proves it's the former: the
        # wildcard import disables resolution for the WHOLE file
        # unconditionally, not just for names that happen to collide.
        result = _constants(
            "DAG_ID = 'local'\nUNRELATED_NAME = 'other'\nfrom settings import *\n"
        )
        assert result == {}


class TestCollectModuleConstantsWalrusInNestedScope:
    """Cases that only _outer_parts (traversing the enclosing-scope parts of
    a scope-boundary node) catches — a naive scope-pruned walk that skipped
    these entirely would miss the walrus poisoning."""

    def test_walrus_inside_comprehension(self):
        result = _constants("DAG_ID = 'real'\nxs = [(DAG_ID := i) for i in range(3)]\n")
        assert result == {}

    def test_walrus_in_parameter_default(self):
        result = _constants(
            "DAG_ID = 'real'\ndef g(x=(DAG_ID := 'other')): pass\n"
        )
        assert result == {}

    def test_walrus_in_decorator_expression(self):
        result = _constants(
            "DAG_ID = 'real'\n@deco(DAG_ID := 'n')\ndef f(): pass\n"
        )
        assert result == {}


class TestCollectModuleConstantsForeignScopeDoesNotPoison:
    """Binding a same-named identifier in a FOREIGN (nested) scope must NOT
    poison the module-level constant — this is the payoff of the
    scope-aware walk over a scope-blind ast.walk."""

    def test_same_named_function_parameter(self):
        result = _constants("DAG_NAME = 'real'\ndef process(DAG_NAME): pass\n")
        assert result == {"DAG_NAME": ("real", 1)}

    def test_class_body_attribute(self):
        result = _constants("DAG_NAME = 'real'\nclass C:\n    DAG_NAME = 'b'\n")
        assert result == {"DAG_NAME": ("real", 1)}

    def test_local_variable_inside_function(self):
        result = _constants("DAG_NAME = 'real'\ndef f():\n    DAG_NAME = 'loc'\n")
        assert result == {"DAG_NAME": ("real", 1)}

    def test_comprehension_target(self):
        result = _constants("DAG_NAME = 'real'\nys = [DAG_NAME for DAG_NAME in range(3)]\n")
        assert result == {"DAG_NAME": ("real", 1)}

    def test_lambda_parameter(self):
        result = _constants("DAG_NAME = 'real'\nf = lambda DAG_NAME: DAG_NAME\n")
        assert result == {"DAG_NAME": ("real", 1)}


class TestCollectModuleConstantsDonstroyRegression:
    def test_module_constant_used_as_parameter_default_value_resolves(self):
        # Matches the real donstroy_pipeline_spark_all.py shape: the module
        # constant is referenced (Name/Load, not a binding) as a parameter
        # default value elsewhere in the file — it must still resolve.
        src = (
            "DAG_NAME = 'donstroy_pipeline_spark_all'\n"
            "def create_cluster(x, dag_name: str = DAG_NAME): pass\n"
        )
        result = _constants(src)
        assert result == {"DAG_NAME": ("donstroy_pipeline_spark_all", 1)}


class TestCollectModuleConstantsPep695:
    # PEP 695 `type X = ...` binds the alias name via an ordinary ast.Name
    # in Store context, already covered by the first poison branch — no
    # separate handling needed. Raw 3.12 syntax cannot appear directly in
    # this file's body since CI also runs 3.10/3.11 (SyntaxError at parse
    # time), so the snippet is parsed from a string instead.
    @pytest.mark.skipif(sys.version_info < (3, 12), reason="PEP 695 `type` statement requires 3.12+")
    def test_pep695_type_alias_with_same_name_poisons_constant(self):
        src = "DAG_ID = 'real'\ntype DAG_ID = int\n"
        result = _constants(src)
        assert result == {}


# ---------------------------------------------------------------------------
# _static_dag_id
# ---------------------------------------------------------------------------

class TestStaticDagIdBaseCases:
    def test_string_literal_dag_id(self):
        func = _function("@dag(dag_id='my_dag')\ndef f(): pass")
        assert _static_dag_id(func, {}) == "my_dag"

    def test_attribute_access_dag(self):
        func = _function("@decorators.dag(dag_id='my_dag')\ndef f(): pass")
        assert _static_dag_id(func, {}) == "my_dag"

    def test_no_dag_id_kwarg_falls_back_to_the_function_name(self):
        func = _function("@dag(schedule_interval=None)\ndef f(): pass")
        assert _static_dag_id(func, {}) == "f"

    def test_no_dag_decorator_is_unresolved(self):
        func = _function("@some_other_decorator\ndef f(): pass")
        assert _static_dag_id(func, {}) is None

    def test_non_literal_dag_id_without_constant_is_unresolved(self):
        func = _function("@dag(dag_id=VARIABLE)\ndef f(): pass")
        assert _static_dag_id(func, {}) is None

    def test_non_string_literal_dag_id_is_unresolved(self):
        func = _function("@dag(dag_id=123)\ndef f(): pass")
        assert _static_dag_id(func, {}) is None

    def test_async_function_with_explicit_dag_id(self):
        src = "@dag(dag_id='async_dag')\nasync def f(): pass"
        assert _static_dag_id(_function(src), {}) == "async_dag"

    def test_empty_decorator_list_is_unresolved(self):
        assert _static_dag_id(_function("def f(): pass"), {}) is None


class TestStaticDagIdConstants:
    def test_variable_dag_id_resolved_via_constants(self):
        func = _function("@dag(dag_id=DAG_ID)\ndef f(): pass")
        assert _static_dag_id(func, {"DAG_ID": "real_id"}) == "real_id"

    def test_variable_dag_id_not_in_constants_is_unresolved(self):
        func = _function("@dag(dag_id=DAG_ID)\ndef f(): pass")
        assert _static_dag_id(func, {"OTHER": "x"}) is None

    def test_empty_string_constant_falls_back_to_the_function_name(self):
        # Regression: DAG_ID = "" (a str, so it IS collected into `constants`
        # by _collect_module_constants) referenced via dag_id=DAG_ID must
        # fall back to the function name, not be returned verbatim as
        # "" — matching the falsy rule already applied to the literal branch.
        func = _function("@dag(dag_id=DAG_ID)\ndef f(): pass")
        assert _static_dag_id(func, {"DAG_ID": ""}) == "f"

    @pytest.mark.parametrize(
        "literal_src",
        ["''", "None", "False", "0", "0.0", "[]", "{}", "()"],
    )
    def test_falsy_literal_dag_id_falls_back_to_the_function_name(self, literal_src):
        func = _function(f"@dag(dag_id={literal_src})\ndef f(): pass")
        assert _static_dag_id(func, {}) == "f"


class TestStaticDagIdPositional:
    def test_positional_string_literal_dag_id(self):
        func = _function("@dag('my_dag')\ndef f(): pass")
        assert _static_dag_id(func, {}) == "my_dag"

    def test_positional_variable_dag_id_resolved_via_constants(self):
        func = _function("@dag(DAG_NAME)\ndef f(): pass")
        assert _static_dag_id(func, {"DAG_NAME": "resolved"}) == "resolved"

    def test_positional_unresolvable_dag_id_is_unresolved(self):
        func = _function("@dag(SOME_VAR)\ndef f(): pass")
        assert _static_dag_id(func, {}) is None

    def test_positional_empty_string_dag_id_falls_back_to_the_function_name(self):
        func = _function("@dag('')\ndef f(): pass")
        assert _static_dag_id(func, {}) == "f"

    def test_positional_truthy_non_string_literal_is_unresolved(self):
        func = _function("@dag(123)\ndef f(): pass")
        assert _static_dag_id(func, {}) is None

    def test_positional_and_keyword_dag_id_conflict_is_unresolved(self):
        # @dag("a", dag_id="b") is invalid Python at call time (TypeError:
        # multiple values for argument 'dag_id') — must not resolve to
        # either value.
        func = _function("@dag('a', dag_id='b')\ndef f(): pass")
        assert _static_dag_id(func, {}) is None


class TestStaticDagIdUnpacking:
    def test_dict_unpacking_is_unresolved(self):
        func = _function('@dag(**{"dag_id": "real"})\ndef f(): pass')
        assert _static_dag_id(func, {}) is None

    def test_name_unpacking_is_unresolved(self):
        func = _function("@dag(**DAG_KWARGS)\ndef f(): pass")
        assert _static_dag_id(func, {}) is None

    def test_star_args_is_unresolved(self):
        func = _function("@dag(*ARGS)\ndef f(): pass")
        assert _static_dag_id(func, {}) is None


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


class TestRmqTriggerParameterNames:
    """The parameter names the scan maps arguments onto are ``rmq_trigger``'s own.

    A parameter of ``rmq_trigger`` missing here would make every file passing it
    a file the scan refuses ("rmq_trigger has no such parameter").
    """

    def test_positional_names_are_the_positional_or_keyword_parameters_in_order(self):
        parameters = inspect.signature(rmq_trigger).parameters.values()
        assert _RMQ_TRIGGER_POSITIONAL == tuple(
            p.name for p in parameters if p.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        )

    def test_keyword_names_are_every_parameter(self):
        parameters = inspect.signature(rmq_trigger).parameters
        assert all(
            p.kind in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
            for p in parameters.values()
        )
        assert len(set(_RMQ_TRIGGER_KWARGS)) == len(_RMQ_TRIGGER_KWARGS)
        assert set(_RMQ_TRIGGER_KWARGS) == set(parameters)


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

    @pytest.mark.parametrize("name", ["dag", "rmq_trigger"])
    @pytest.mark.parametrize(
        ("factory", "after"),
        [
            pytest.param(
                "def make():\n"
                "    def middle():\n"
                "        @rmq_trigger(queue='a')\n        @dag(dag_id='a')\n        def a(): pass\n"
                "        return a()\n"
                "    return middle\n",
                "m = make()\n{name} = None\nm()\n",
                id="two-level-factory",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    yield a()\n",
                "made = make()\n{name} = None\nlist(made)\n",
                id="generator-factory",
            ),
            pytest.param(
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    yield from [a()]\n",
                "made = make()\n{name} = None\nlist(made)\n",
                id="yield-from-factory",
            ),
            pytest.param(
                "async def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n",
                "made = make()\n{name} = None\nimport asyncio\nasyncio.run(made)\n",
                id="async-factory",
            ),
            pytest.param(
                "@register\n"
                "def make():\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n",
                "make()\n{name} = None\nfor factory in FACTORIES:\n    factory()\n",
                id="decorated-factory",
            ),
            pytest.param(
                "def make():\n"
                "    class Holder:\n"
                "        @rmq_trigger(queue='a')\n        @dag(dag_id='a')\n        def a(): pass\n"
                "    return Holder.a()\n",
                "make()\n{name} = None\n",
                id="class-between-the-factory-and-the-function",
            ),
        ],
    )
    def test_a_factory_whose_call_may_not_run_the_def_counts_bindings_below(
        self, factory, after, name
    ):
        read = _file_read(factory + after.format(name=name))
        assert read.subscriptions == []
        assert any(f"{name} is not bound by one absolute import alone" in p for p in read.problems), (
            read.problems
        )

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param(
                "    def numbers():\n        yield 1\n"
                "    @rmq_trigger(queue='a')\n    @dag(dag_id='a')\n    def a(): pass\n"
                "    return a()\n",
                id="generator-nested-in-the-factory",
            ),
            pytest.param(
                "    for _ in range(1):\n"
                "        @rmq_trigger(queue='a')\n        @dag(dag_id='a')\n        def a(): pass\n"
                "    return a()\n",
                id="def-in-a-loop-of-the-factory",
            ),
        ],
    )
    def test_a_plain_factory_running_the_def_in_its_call_keeps_bindings_below_out(self, body):
        read = _file_read("def make():\n" + body + "made = make()\ndag = None\n")
        assert read.problems == []
        assert [(s["dag_id"], s["queue_name"]) for s in read.subscriptions] == [("a", "a")]

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


# ---------------------------------------------------------------------------
# _is_rmq_trigger_call
# ---------------------------------------------------------------------------


class TestIsRmqTriggerCall:
    def test_bare_call_returns_true(self):
        assert _is_rmq_trigger_call(_expr("rmq_trigger(queue='orders')")) is True

    def test_attribute_access_call_returns_true(self):
        assert _is_rmq_trigger_call(_expr("decorators.rmq_trigger(queue='orders')")) is True

    def test_unrelated_call_returns_false(self):
        assert _is_rmq_trigger_call(_expr("some_other_call(queue='orders')")) is False

    def test_non_call_node_returns_false(self):
        # A bare `@some_name` decorator is an ast.Name, not an ast.Call.
        assert _is_rmq_trigger_call(_expr("some_name")) is False


# ---------------------------------------------------------------------------
# _read_function_triggers — one decorator call, queue and cooldown parameters
# ---------------------------------------------------------------------------

def _read_decorator(src: str, dag_id: str = "test_dag"):
    """Read one decorator call string placed above ``@dag(dag_id=...)`` on a function."""
    tree = ast.parse(f"@{src}\n@dag(dag_id={dag_id!r})\ndef f(): pass\n")
    func = tree.body[0]
    assert isinstance(func, ast.FunctionDef)
    return _read_function_triggers(func, {}, dag_id)


def _decorator_subscriptions(src: str, dag_id: str = "test_dag") -> list[dict]:
    """Subscription dicts of one decorator call string, empty when it is refused."""
    return _read_decorator(src, dag_id).subscriptions


def _decorator_problems(src: str, dag_id: str = "test_dag") -> str:
    """All problems of one decorator call string, as one text."""
    return "\n".join(_read_decorator(src, dag_id).problems)


class TestReadFunctionTriggers:
    def test_single_queue_no_cooldown_returns_one_dict(self):
        result = _decorator_subscriptions("rmq_trigger(queue='orders')")
        assert len(result) == 1
        assert result[0]["queue_name"] == "orders"
        assert result[0]["cooldown"] == 0
        assert result[0]["conn_id"] == "rmq_default"

    def test_queues_list_returns_n_dicts(self):
        result = _decorator_subscriptions("rmq_trigger(queues=['orders', 'payments'])")
        assert len(result) == 2
        queue_names = [d["queue_name"] for d in result]
        assert "orders" in queue_names
        assert "payments" in queue_names

    def test_queues_list_all_share_same_cooldown(self):
        result = _decorator_subscriptions("rmq_trigger(queues=['a', 'b', 'c'], cooldown=300)")
        assert len(result) == 3
        assert all(d["cooldown"] == 300 for d in result)

    def test_cooldown_parsed_correctly(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q', cooldown=60)")
        assert len(result) == 1
        assert result[0]["cooldown"] == 60

    def test_cooldown_zero_is_default(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q')")
        assert result[0]["cooldown"] == 0

    def test_dag_id_and_group_key_are_set_on_every_row(self):
        result = _decorator_subscriptions("rmq_trigger(queues=['a', 'b'], cooldown=60)", "my_dag")
        assert [(d["dag_id"], d["group_key"]) for d in result] == [
            ("my_dag", "my_dag"),
            ("my_dag", "my_dag"),
        ]

    def test_non_rmq_trigger_returns_empty_list(self):
        read = _read_decorator("some_other_decorator(queue='q')")
        assert read.subscriptions == []
        assert read.problems == []

    def test_no_queue_or_queues_is_refused_by_the_builder(self):
        read = _read_decorator("rmq_trigger(conn_id='rmq')")
        assert read.subscriptions == []
        assert "the arguments are refused" in "\n".join(read.problems)

    def test_non_literal_queues_is_a_problem(self):
        read = _read_decorator("rmq_trigger(queues=SOME_VAR)")
        assert read.subscriptions == []
        assert "queues cannot be read" in "\n".join(read.problems)

    def test_conn_id_propagated_to_all_entries(self):
        result = _decorator_subscriptions("rmq_trigger(queues=['a', 'b'], conn_id='my_conn')")
        assert all(d["conn_id"] == "my_conn" for d in result)

    def test_filter_data_propagated_to_all_entries(self):
        result = _decorator_subscriptions(
            "rmq_trigger(queues=['a', 'b'], filter_data={'filter_headers': {'k': 'v'}})"
        )
        assert all(d["filter_data"] == {"filter_headers": {"k": "v"}} for d in result)

    def test_positional_queue_name_parsed(self):
        result = _decorator_subscriptions("rmq_trigger('my_queue')")
        assert len(result) == 1
        assert result[0]["queue_name"] == "my_queue"

    def test_positional_conn_id_filter_data_and_cooldown_parsed(self):
        result = _decorator_subscriptions(
            "rmq_trigger('q', None, 'rmq_prod', {'body.type': 'paid'}, 30)"
        )
        assert len(result) == 1
        assert result[0]["conn_id"] == "rmq_prod"
        assert result[0]["filter_data"] == {"body.type": "paid"}
        assert result[0]["cooldown"] == 30

    def test_attribute_access_rmq_trigger(self):
        result = _decorator_subscriptions("decorators.rmq_trigger(queue='q')")
        assert len(result) == 1
        assert result[0]["queue_name"] == "q"

    def test_ack_unmatched_literal_true_parsed(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q', ack_unmatched=True)")
        assert result[0]["ack_unmatched"] is True

    def test_ack_unmatched_literal_false_parsed(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q', ack_unmatched=False)")
        assert result[0]["ack_unmatched"] is False

    def test_ack_unmatched_absent_is_false(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q')")
        assert result[0]["ack_unmatched"] is False

    def test_non_literal_ack_unmatched_is_refused_not_defaulted(self):
        read = _read_decorator("rmq_trigger(queue='q', ack_unmatched=ACK_UNMATCHED)")
        assert read.subscriptions == []
        assert "ack_unmatched cannot be read" in "\n".join(read.problems)

    def test_problem_names_dag_line_parameter_and_reason(self):
        text = _decorator_problems("rmq_trigger(queue='orders', conn_id=get_conn())", dag_id="my_dag")
        assert "dag_id='my_dag'" in text
        assert "line 1" in text
        assert "conn_id cannot be read" in text
        assert "a function call" in text

    def test_non_literal_queue_selector_is_a_problem(self):
        read = _read_decorator("rmq_trigger(queue=QUEUE_CONST)", dag_id="my_dag")
        assert read.subscriptions == []
        assert "queue cannot be read" in "\n".join(read.problems)

    def test_an_unreadable_argument_skips_the_build(self):
        """The arguments that were read do not say what the decorator receives, so the
        builder is not asked about them: the problem is the unreadable argument."""
        text = _decorator_problems("rmq_trigger(exchange='ex', routing_key_ids=REPORT_IDS)")
        assert "routing_key_ids cannot be read" in text
        assert "the arguments are refused" not in text

    def test_unreadable_cooldown_beside_a_buildable_exchange_call_is_refused(self):
        read = _read_decorator(
            "rmq_trigger(exchange='ex', routing_keys=['a.b'], cooldown=COOLDOWN)"
        )
        assert read.subscriptions == []
        assert "cooldown cannot be read" in "\n".join(read.problems)

    def test_dict_unpacking_of_an_unknown_name_is_a_problem(self):
        read = _read_decorator("rmq_trigger(**opts)", dag_id="my_dag")
        assert read.subscriptions == []
        assert "**opts cannot be read" in "\n".join(read.problems)

    def test_dict_unpacking_beside_a_literal_queue_is_refused(self):
        read = _read_decorator("rmq_trigger(queue='orders', **opts)")
        assert read.subscriptions == []
        assert "**opts cannot be read" in "\n".join(read.problems)

    def test_a_non_literal_positional_queue_is_a_problem(self):
        read = _read_decorator("rmq_trigger(QUEUE_CONST)", dag_id="my_dag")
        assert read.subscriptions == []
        assert "queue cannot be read" in "\n".join(read.problems)

    def test_a_queues_argument_that_is_not_a_list_does_not_break_the_report(self):
        read = _read_decorator("rmq_trigger(queues=5, cooldown=COOLDOWN)")
        assert read.subscriptions == []
        assert "cooldown cannot be read" in "\n".join(read.problems)

    def test_literal_kwargs_produce_no_problem(self):
        read = _read_decorator("rmq_trigger(queue='q', cooldown=60, ack_unmatched=True)")
        assert read.problems == []

    def test_non_bool_ack_unmatched_skipped_returns_empty(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q', ack_unmatched='yes')")
        assert result == []

    def test_non_bool_ack_unmatched_problem_carries_the_builder_text(self):
        assert "ack_unmatched" in _decorator_problems("rmq_trigger(queue='q', ack_unmatched='yes')")

    def test_negative_cooldown_skipped_returns_empty(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q', cooldown=-1)")
        assert result == []

    def test_negative_cooldown_problem_names_the_dag_and_the_builder_text(self):
        text = _decorator_problems("rmq_trigger(queue='q', cooldown=-1)", dag_id="neg_dag")
        assert "neg_dag" in text
        assert "'cooldown' must be >= 0" in text

    def test_string_cooldown_skipped_returns_empty(self):
        # A typo'd literal like cooldown="abc" is a valid string literal but
        # semantically invalid — refused by the builder, not raised out of here.
        result = _decorator_subscriptions("rmq_trigger(queue='q', cooldown='abc')")
        assert result == []

    def test_string_cooldown_problem_names_the_dag(self):
        text = _decorator_problems("rmq_trigger(queue='q', cooldown='abc')", dag_id="str_cooldown_dag")
        assert "str_cooldown_dag" in text

    def test_list_cooldown_skipped_returns_empty(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q', cooldown=[1, 2])")
        assert result == []

    def test_float_cooldown_skipped_returns_empty(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q', cooldown=1.5)")
        assert result == []

    def test_bool_cooldown_skipped_returns_empty(self):
        result = _decorator_subscriptions("rmq_trigger(queue='q', cooldown=True)")
        assert result == []

    def test_queues_with_non_string_items_skipped_returns_empty(self):
        result = _decorator_subscriptions("rmq_trigger(queues=[1, 2, 3])")
        assert result == []

    def test_queues_with_mixed_string_and_int_items_skipped_returns_empty(self):
        result = _decorator_subscriptions("rmq_trigger(queues=['a', 2])")
        assert result == []

    def test_queues_with_non_string_items_problem_names_the_dag(self):
        assert "bad_queues_dag" in _decorator_problems(
            "rmq_trigger(queues=[1, 2, 3])", dag_id="bad_queues_dag"
        )

    def test_stacked_calls_each_contribute_rows(self):
        tree = ast.parse(
            "@rmq_trigger(queue='a')\n"
            "@rmq_trigger(queue='b', cooldown=30)\n"
            "@dag(dag_id='d')\n"
            "def f(): pass\n"
        )
        read = _read_function_triggers(tree.body[0], {}, "d")
        assert read.problems == []
        assert [s["queue_name"] for s in read.subscriptions] == ["a", "b"]

    def test_stacked_readable_and_unreadable_calls_report_the_unreadable_one(self):
        tree = ast.parse(
            "@rmq_trigger(queue='a')\n"
            "@rmq_trigger(queue='b', conn_id=get_conn())\n"
            "@dag(dag_id='d')\n"
            "def f(): pass\n"
        )
        read = _read_function_triggers(tree.body[0], {}, "d")
        assert len(read.problems) == 1
        assert "line 2" in read.problems[0]


# ---------------------------------------------------------------------------
# _read_function_triggers — exchange-mode
# ---------------------------------------------------------------------------


class TestReadFunctionTriggersExchange:
    def test_exchange_with_routing_key_ids_literal_list(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'])",
            dag_id="my_dag",
        )
        assert len(result) == 1
        sub = result[0]
        assert sub["exchange"] == "jetstat.airflow"
        assert sub["queue_name"] == "rmq_watcher.sub.my_dag"
        assert sub["routing_keys"] == ["abc123.*"]

    def test_exchange_default_routing_key_status_is_wildcard(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'])"
        )
        assert result[0]["routing_keys"] == ["abc123.*"]

    def test_exchange_explicit_string_routing_key_status(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'], "
            "routing_key_status='succeeded')"
        )
        assert result[0]["routing_keys"] == ["abc123.succeeded"]

    def test_exchange_explicit_list_routing_key_status(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'], "
            "routing_key_status=['succeeded', 'failed'])"
        )
        assert set(result[0]["routing_keys"]) == {"abc123.succeeded", "abc123.failed"}

    def test_exchange_with_literal_routing_keys_directly(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='some.other.exchange', routing_keys=['region.eu.alert'])"
        )
        assert len(result) == 1
        assert result[0]["exchange"] == "some.other.exchange"
        assert result[0]["routing_keys"] == ["region.eu.alert"]

    def test_exchange_routing_keys_and_routing_key_ids_union(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', routing_keys=['literal.key'], "
            "routing_key_ids=['abc123'])"
        )
        assert set(result[0]["routing_keys"]) == {"literal.key", "abc123.*"}

    def test_non_literal_routing_key_ids_skipped(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=SOME_VAR)"
        )
        assert result == []

    def test_non_literal_routing_keys_skipped(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', routing_keys=SOME_VAR)"
        )
        assert result == []

    def test_non_literal_routing_key_status_is_refused_not_defaulted(self):
        # A wildcard default would bind more routing keys than the decorator asks
        # for, so the call is refused rather than built from the rest.
        read = _read_decorator(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'], "
            "routing_key_status=SOME_VAR)"
        )
        assert read.subscriptions == []
        assert "routing_key_status cannot be read" in "\n".join(read.problems)

    def test_exchange_and_queue_mutex_violation_skipped(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', queue='q', routing_keys=['a.b'])"
        )
        assert result == []

    def test_exchange_without_routing_keys_skipped(self):
        result = _decorator_subscriptions("rmq_trigger(exchange='jetstat.airflow')")
        assert result == []

    def test_exchange_dot_in_routing_key_id_skipped(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc.123'])"
        )
        assert result == []

    def test_exchange_reserved_prefix_skipped(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='rmq_watcher.something', routing_keys=['a.b'])"
        )
        assert result == []

    def test_exchange_empty_routing_key_status_list_skipped(self):
        # routing_key_status=[] collapses the routing_key_ids cross-product to
        # an empty set — build_subscriptions raises ValueError, which becomes a
        # problem, not an exception out of the reader.
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', routing_key_ids=['abc123'], "
            "routing_key_status=[])"
        )
        assert result == []

    def test_non_string_exchange_skipped_returns_empty(self):
        # exchange=123 is a valid literal but semantically invalid — refused by
        # build_subscriptions, not raised as an AttributeError from
        # exchange.startswith(...).
        result = _decorator_subscriptions("rmq_trigger(exchange=123, routing_keys=['a.b'])")
        assert result == []

    def test_non_string_exchange_problem_names_the_dag(self):
        assert "bad_exchange_dag" in _decorator_problems(
            "rmq_trigger(exchange=123, routing_keys=['a.b'])", dag_id="bad_exchange_dag"
        )

    def test_non_str_non_list_routing_key_status_skipped_returns_empty(self):
        # routing_key_status=123 must not reach list(routing_key_status) and
        # raise an uncaught TypeError.
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='ex', routing_key_ids=['abc'], routing_key_status=123)"
        )
        assert result == []

    def test_routing_keys_as_plain_string_skipped_returns_empty(self):
        # Forgetting list brackets (routing_keys="literal.string" instead of
        # routing_keys=["literal.string"]) must not silently expand into
        # one-character wildcard routing keys — build_subscriptions rejects
        # non-list routing_keys with ValueError.
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='ex', routing_keys='literal.string')"
        )
        assert result == []

    def test_routing_key_ids_as_plain_string_skipped_returns_empty(self):
        result = _decorator_subscriptions(
            "rmq_trigger(exchange='jetstat.airflow', "
            "routing_key_ids='670f877702775c2de8325b1f')"
        )
        assert result == []

    def test_second_exchange_call_on_one_dag_is_a_problem(self):
        tree = ast.parse(
            "@rmq_trigger(exchange='exchange.one', routing_keys=['a.b'])\n"
            "@rmq_trigger(exchange='exchange.two', routing_keys=['c.d'])\n"
            "@dag(dag_id='d')\n"
            "def f(): pass\n"
        )
        read = _read_function_triggers(tree.body[0], {}, "d")
        assert len(read.problems) == 1
        assert "second exchange=" in read.problems[0]
        assert "rmq_watcher.sub.d" in read.problems[0]
