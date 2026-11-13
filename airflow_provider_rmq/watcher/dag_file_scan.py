"""Static reading of ``@rmq_trigger`` subscriptions from a DAG file's source.

The watcher finds subscriptions by parsing DAG files with :mod:`ast` and never
executes them. Everything that reads a DAG file — reading and parsing it,
module-scope traversal, module-level constant collection, ``dag_id``
resolution, the reading of ``rmq_trigger(...)`` calls and the file-level
verdict of ``_read_file_triggers`` — lives here. Nothing here logs: a file the
scan cannot read fully comes back as a list of problems, and the caller
reports them.

The decorator runs the same reading on its own call site at import time
(``_verify_static_readability``) and raises ``RMQTriggerNotStaticError`` when
the scan would not see the subscriptions the decorator builds.

The module depends only on the standard library and ``subscription_builder``, so
it can be imported inside the DAG processor without pulling in the watcher's
runtime modules (``listener``, ``consumer``, ``models``, ``runner``) and their
third-party dependencies.
"""

from __future__ import annotations

import ast
import copy
import enum
import os
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any, NamedTuple, TypeGuard

from airflow_provider_rmq.watcher.subscription_builder import (
    build_subscriptions,
    has_exchange_conflict,
)

# ---------------------------------------------------------------------------
# Module-level constant collection (for dag_id=NAME and rmq_trigger arguments)
# ---------------------------------------------------------------------------

# Nodes whose body is a separate scope: the node's own name binds in the
# enclosing scope, but its contents do not affect module scope.
_SCOPE_BOUNDARIES = (
    ast.FunctionDef,
    ast.AsyncFunctionDef,
    ast.ClassDef,
    ast.Lambda,
    ast.ListComp,
    ast.SetComp,
    ast.DictComp,
    ast.GeneratorExp,
)


def _outer_parts(node: ast.AST) -> Iterator[ast.AST]:
    """Sub-expressions of a scope-boundary node that still evaluate in the
    ENCLOSING scope — decorators, parameter defaults, class bases/keywords,
    return annotations, and a comprehension's outermost iterable. Everything
    else inside the boundary belongs to the new scope.
    """
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        yield from node.decorator_list
        yield from node.args.defaults
        yield from (d for d in node.args.kw_defaults if d is not None)
        if node.returns is not None:
            yield node.returns
    elif isinstance(node, ast.ClassDef):
        yield from node.decorator_list
        yield from node.bases
        yield from (kw.value for kw in node.keywords)
    elif isinstance(node, ast.Lambda):
        yield from node.args.defaults
        yield from (d for d in node.args.kw_defaults if d is not None)
    elif isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
        if node.generators:
            yield node.generators[0].iter   # evaluated in the enclosing scope


def _module_scope_nodes(tree: ast.Module) -> Iterator[ast.AST]:
    """Yield every node that executes in the module's own scope.

    Like ``ast.walk``, but prunes at nested-scope boundaries: a
    ``FunctionDef``/``ClassDef``/``Lambda``/comprehension node is yielded
    itself (so the name it binds in the enclosing scope is counted) and its
    ``_outer_parts`` are still traversed, but its body is not — bindings in
    there belong to a different scope and must not poison a module-level
    constant.
    """
    stack: list[ast.AST] = list(tree.body)
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, _SCOPE_BOUNDARIES):
            stack.extend(_outer_parts(node))
            continue
        stack.extend(ast.iter_child_nodes(node))


def _contains_mutable(value: Any) -> bool:
    """Return True if ``value`` is, or holds at any depth, a ``list``, ``dict`` or ``set``.

    These are the mutable containers ``ast.literal_eval`` can produce; a tuple is
    looked into because a tuple of lists is mutable through its elements.
    """
    if isinstance(value, (list, dict, set)):
        return True
    if isinstance(value, tuple):
        return any(_contains_mutable(item) for item in value)
    return False


def _trigger_argument_name_ids(tree: ast.Module) -> set[int]:
    """Return ``id()`` of every ``Name`` that is an ``rmq_trigger(...)`` argument value.

    A name counts when it is a positional argument, a keyword value or a ``**NAME``
    unpacking of an ``rmq_trigger`` call — directly, or nested only inside list,
    tuple and dict displays there (``queues=[Q1, Q2]``, ``filter_data={"k": V}``).
    These are exactly the positions where the decorator receives the value as is,
    without any code of the DAG file touching it.
    """
    ids: set[int] = set()
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _is_rmq_trigger_call(node)):
            continue
        stack: list[ast.expr | None] = [*node.args, *(kw.value for kw in node.keywords)]
        while stack:
            item = stack.pop()
            if isinstance(item, ast.Name):
                ids.add(id(item))
            elif isinstance(item, (ast.List, ast.Tuple)):
                stack.extend(item.elts)
            elif isinstance(item, ast.Dict):
                stack.extend(item.keys)
                stack.extend(item.values)
    return ids


def _collect_module_constants(
    tree: ast.Module,
) -> tuple[dict[str, tuple[Any, int]], set[int]]:
    """Collect simple top-level ``NAME = <literal>`` assignments (plus annotated
    ``NAME: T = <literal>``), each paired with its line number so callers can
    enforce "only usable before this point in the file" — see
    ``_usable_constants``.

    The value is anything ``ast.literal_eval`` produces: strings, numbers,
    booleans, ``None``, and lists, tuples, dicts and sets built from them.
    Callers decide which types they accept — ``dag_id=NAME`` takes a string
    constant only.

    A value that is or holds a ``list``, ``dict`` or ``set`` could be changed
    after its assignment (``X["k"] = v``, ``X.append(...)``, ``helper(X)``,
    a write through ``alias = X``), and no static reading can follow that. Such
    a name is kept only if every mention of it anywhere in the file, other
    than the assignment itself, is an ``rmq_trigger(...)`` argument value —
    directly, inside list/tuple/dict displays there, or as ``**NAME`` (see
    ``_trigger_argument_name_ids``). Any other mention, a read included,
    drops the name. Strings and numbers cannot change, so for them the rule
    does not apply and ``DAG_ID.lower()`` elsewhere in the file keeps
    ``DAG_ID`` usable.

    Returns ``(constants, module_scope_function_ids)``. The second element
    is ``id()`` of every ``FunctionDef``/``AsyncFunctionDef`` whose
    decorators execute in module scope, collected from the SAME
    ``_module_scope_nodes(tree)`` pass used to build ``constants`` —
    ``_usable_constants`` needs this set to decide whether a nested
    function may use ``constants`` at all, and reusing this pass avoids
    walking the tree a second time just for that.

    A name is kept only if it is bound **exactly once** in the module's own
    scope, and that one binding is the top-level literal assignment itself.
    The binding forms counted below are the complete set of ``ast`` fields
    that can bind a name (derived by introspecting every ``ast.AST``
    subclass's ``_fields`` for name-bearing entries, not by guessing):
    ``Name`` in ``Store``/``Del`` (``=``, ``+=``, tuple-unpack, chained
    assignment, ``for`` targets, ``with ... as``, walrus, ``del``, and the
    PEP 695 ``type X = ...`` alias name, which is itself a ``Name`` in
    ``Store``), ``alias`` (imports), ``FunctionDef``/``AsyncFunctionDef``/
    ``ClassDef`` names, ``ExceptHandler.name``, ``MatchAs``/``MatchStar``
    names, ``MatchMapping.rest``. ``arg`` (parameters) is deliberately NOT
    counted — parameters live in the function's own scope, which
    ``_module_scope_nodes`` already excludes.

    Three cases the scope-pruned walk would otherwise miss are handled
    explicitly: ``global NAME`` anywhere in the file (a function may rebind a
    module-level name) and walrus ``:=`` anywhere (PEP 572 binds it in the
    *enclosing* scope, so one inside a comprehension still hits module level)
    both always poison; ``from module import *`` disables constant
    resolution for the whole file — the imported names are
    statically unknown, so any earlier literal could have been silently
    replaced.

    Refusing to resolve an ambiguous name is safe (the caller reports it as
    unreadable); guessing wrong is not — same principle as
    ``_UNRESOLVED_DAG_ID`` below.
    """
    binding_counts: dict[str, int] = {}
    has_wildcard_import = False
    module_scope_function_ids: set[int] = set()

    def _bump(name: str) -> None:
        binding_counts[name] = binding_counts.get(name, 0) + 1

    for node in _module_scope_nodes(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            _bump(node.id)
        elif isinstance(node, ast.alias):
            if node.name == "*" and node.asname is None:
                has_wildcard_import = True
            else:
                _bump(node.asname or node.name.split(".")[0])
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            _bump(node.name)
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                module_scope_function_ids.add(id(node))
        elif isinstance(node, ast.ExceptHandler) and node.name:
            _bump(node.name)
        elif isinstance(node, (ast.MatchAs, ast.MatchStar)) and node.name:
            _bump(node.name)
        elif isinstance(node, ast.MatchMapping) and node.rest:
            _bump(node.rest)

    # Two forms that a scope-pruned walk cannot see reliably, both scanned
    # over the WHOLE tree (over-counting here is safe — it only turns a
    # constant unresolvable):
    #   * `global NAME` — a function may rebind a module-level name;
    #   * walrus `:=` — per PEP 572 it binds in the ENCLOSING scope, so a
    #     walrus inside a comprehension binds at module level even though
    #     the comprehension body itself is a separate scope.
    for node in ast.walk(tree):
        if isinstance(node, ast.Global):
            for name in node.names:
                _bump(name)
        elif isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name):
            _bump(node.target.id)

    if has_wildcard_import:
        return {}, module_scope_function_ids

    constants: dict[str, tuple[Any, int]] = {}
    assignment_targets: dict[str, ast.Name] = {}
    for node in tree.body:  # value extraction: top-level ONLY, unconditional
        if isinstance(node, ast.Assign):
            if len(node.targets) != 1:
                continue
            target, value_node = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign):
            if node.value is None:
                continue
            target, value_node = node.target, node.value
        else:
            continue
        if not isinstance(target, ast.Name):
            continue
        name = target.id
        if binding_counts.get(name, 0) != 1:
            continue
        try:
            value = ast.literal_eval(value_node)
        except (ValueError, TypeError):
            continue
        constants[name] = (value, node.lineno)
        assignment_targets[name] = target

    mutable = {name for name, (value, _) in constants.items() if _contains_mutable(value)}
    if mutable:
        allowed = _trigger_argument_name_ids(tree)
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Name)
                and node.id in mutable
                and node is not assignment_targets[node.id]
                and id(node) not in allowed
            ):
                del constants[node.id]
                mutable.discard(node.id)
    return constants, module_scope_function_ids


# Sentinel distinguishing "dag_id= is set but cannot be statically resolved"
# from "dag_id= was never set at all" (see _extract_dag_id_from_decorators).
# An enum member (rather than a bare object()) so mypy narrows
# `str | _DagIdSentinel | None` correctly after `is` comparisons.
class _DagIdSentinel(enum.Enum):
    UNRESOLVED = enum.auto()


_UNRESOLVED_DAG_ID = _DagIdSentinel.UNRESOLVED


def _extract_dag_id_from_decorators(
    decorators: list[ast.expr], constants: dict[str, Any] | None = None
) -> str | _DagIdSentinel | None:
    """Resolve the ``dag_id`` a recognized ``@dag(...)`` call among ``decorators`` would produce.

    Three-way return contract:

    - ``str`` — the resolved dag_id, either a string literal (``dag_id='x'``)
      or a module-level string constant referenced by name and found in
      ``constants`` (``dag_id=NAME`` with ``constants={"NAME": "x"}``).
    - ``None`` — a recognized ``@dag(...)`` call was found, and EITHER no
      ``dag_id=`` keyword was set at all (Airflow's own default behaviour is
      to fall back to the decorated function's name in this case, so it is
      safe for the caller to do the same), OR ``dag_id=`` is a falsy literal
      (``""``, ``None``, ``False``, ``0``, and generally anything falsy
      ``ast.literal_eval`` can produce — this mirrors Airflow's own
      ``dag_id or f.__name__`` in ``airflow/models/dag.py``), OR ``dag_id=``
      names an empty-string constant. Falling back to the function name is
      only ever safe for this ``None`` case.
    - ``_UNRESOLVED_DAG_ID`` — ``dag_id=`` is set to something that cannot
      be statically proven (a non-literal expression whose name is not in
      ``constants``, a name whose constant is not a string such as
      ``DAG_ID = None``/``0``/``False``, or a truthy non-string literal such
      as ``dag_id=123``), OR no recognized ``@dag(...)`` call exists among
      ``decorators`` at all
      (e.g. an aliased import — ``from airflow.decorators import dag as
      airflow_dag`` — is not matched by the ``is_dag`` predicate below).
      Guessing the function name here would silently register a
      subscription under a ``dag_id`` that does not correspond to any real
      Airflow DAG — the caller must refuse the file's subscriptions instead
      (see ``_static_dag_id`` and ``_read_file_triggers``).

    **Deliberate literal/constant asymmetry**: a falsy literal of any type
    returns ``None`` and falls back to the function name, as in Airflow. A
    constant is accepted only as a string: the "not a string" check runs
    before the "empty" check, so ``DAG_ID = None`` (or ``0``, ``False``)
    referenced as ``dag_id=DAG_ID`` stays ``_UNRESOLVED_DAG_ID``, while
    ``DAG_ID = ""`` falls back to the function name. A name that could not
    be resolved at all stays ``_UNRESOLVED_DAG_ID``. See the two
    ``_resolve()`` branches below.

    No logging happens inside this function. Only functions carrying
    ``@rmq_trigger`` are ever resolved (``_iter_trigger_functions``), and for
    them ``_read_file_triggers`` turns ``_UNRESOLVED_DAG_ID`` into a problem
    that refuses the whole file; an unresolvable ``dag_id=`` on a DAG with no
    ``@rmq_trigger`` at all is not this plugin's concern and stays silent.

    ``constants`` is a flat ``dict[str, Any]`` of name → value; values of
    any literal type may be present, and only strings are accepted for
    ``dag_id``. The caller (``_iter_trigger_functions``) is responsible for
    turning ``_collect_module_constants``'s
    ``dict[str, tuple[Any, int]]`` output into this shape — filtering out
    constants assigned after the decorated function's own line (use-before-
    definition) and passing an empty dict for nested functions, whose
    module-level constants may be shadowed by a local binding of the same
    name.

    ``dag_id`` in Airflow's own ``@dag(...)`` signature is the first
    parameter and is ``POSITIONAL_OR_KEYWORD``, so a positional call such as
    ``@dag("my_id", schedule=None)`` is just as legal as the keyword form
    and is resolved the same way: if ``dec.args`` is non-empty, its first
    element is the ``dag_id`` value node; otherwise ``dag_id=`` is looked up
    among ``dec.keywords`` as before. Whichever source it comes from, the
    exact same resolution ladder (string literal → constant lookup →
    ``_UNRESOLVED_DAG_ID``) and the same falsy rule apply.

    ``ast.Starred`` among ``dec.args`` (i.e. ``@dag(*ARGS, ...)``) is checked
    **before** looking at ``dec.args[0]`` — the value at that position is
    statically undecidable, so it must return ``_UNRESOLVED_DAG_ID``
    immediately rather than being treated as if it were the ``dag_id`` value
    node.

    If ``dec.args`` is non-empty (a positional ``dag_id`` value is present)
    **and** an explicit ``dag_id=`` keyword is also present in the same call
    (``@dag("a", dag_id="b")``), the call is invalid Python: ``dag_id`` is
    ``POSITIONAL_OR_KEYWORD``, so the decorator call would raise ``TypeError:
    dag() got multiple values for argument 'dag_id'`` at import time and the
    DAG would never actually be created. Picking either value would silently
    register a subscription for a ``dag_id`` that can never exist, so this
    returns ``_UNRESOLVED_DAG_ID`` instead of guessing.

    ``**``-unpacking in ``dec.keywords`` (``@dag(**{"dag_id": "x"})`` or
    ``@dag(**DAG_KWARGS)``) produces a ``keyword`` entry with ``arg is
    None`` — a search by ``kw.arg == "dag_id"`` never matches it. If no
    explicit ``dag_id=`` keyword is found and such an entry is present, the
    absence of ``dag_id=`` cannot be proven (the unpacked dict could contain
    it), so ``_UNRESOLVED_DAG_ID`` is returned instead of ``None``. Parsing
    the unpacked contents (even a literal ``**{"dag_id": ...}``) is out of
    scope — only the presence of unpacking is detected.
    """
    constants = constants or {}

    def _resolve(value_node: ast.expr) -> str | _DagIdSentinel | None:
        if isinstance(value_node, ast.Name):
            if value_node.id in constants:
                # Type check comes FIRST: a constant holds any literal type,
                # and only a string names a DAG. An empty-string constant
                # then falls back to the function name, like an empty
                # literal below.
                resolved = constants[value_node.id]
                if not isinstance(resolved, str):
                    return _UNRESOLVED_DAG_ID
                return resolved if resolved else None
            return _UNRESOLVED_DAG_ID
        try:
            value = ast.literal_eval(value_node)
        except (ValueError, TypeError):
            return _UNRESOLVED_DAG_ID
        # Falsy check comes FIRST — an empty string is falsy too, and per
        # the falsy asymmetry documented above, ANY falsy literal
        # (including "") must fall back to the function name, not be
        # returned verbatim.
        if not value:
            return None
        if isinstance(value, str):
            return value
        return _UNRESOLVED_DAG_ID

    for dec in decorators:
        if not _is_dag_call(dec):
            continue
        # Priority check: a Starred positional arg (`@dag(*ARGS, ...)`) makes
        # the value at dec.args[0] undecidable — must be ruled out BEFORE
        # treating dec.args[0] as the dag_id value node.
        if any(isinstance(arg, ast.Starred) for arg in dec.args):
            return _UNRESOLVED_DAG_ID
        if dec.args:
            # A positional dag_id value AND an explicit dag_id= keyword in
            # the same call is invalid Python (TypeError: multiple values
            # for argument 'dag_id') — the DAG would never actually get
            # created, so neither value can be trusted; see docstring.
            if any(kw.arg == "dag_id" for kw in dec.keywords):
                return _UNRESOLVED_DAG_ID
            return _resolve(dec.args[0])
        for kw in dec.keywords:
            if kw.arg != "dag_id":
                continue
            return _resolve(kw.value)
        # No explicit dag_id= keyword found. If the call also unpacks an
        # unknown dict (`@dag(**kwargs)`), the absence of dag_id= cannot be
        # proven — the dict could contain it.
        if any(kw.arg is None for kw in dec.keywords):
            return _UNRESOLVED_DAG_ID
        # Recognized @dag(...) call, but no dag_id= keyword at all.
        return None
    # No recognized @dag(...) call among decorators.
    return _UNRESOLVED_DAG_ID


_RMQ_TRIGGER_KWARGS = (
    "queue",
    "queues",
    "exchange",
    "routing_keys",
    "routing_key_ids",
    "routing_key_status",
    "conn_id",
    "filter_data",
    "cooldown",
    "ack_unmatched",
)


def _is_call_named(node: ast.AST, name: str) -> TypeGuard[ast.Call]:
    """Return True if ``node`` calls ``name(...)`` or ``<anything>.name(...)``."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return (
        (isinstance(func, ast.Name) and func.id == name)
        or (isinstance(func, ast.Attribute) and func.attr == name)
    )


def _is_rmq_trigger_call(node: ast.AST) -> TypeGuard[ast.Call]:
    """Return True if node is a call to rmq_trigger(...).

    Handles both bare name ``rmq_trigger(...)`` and attribute access
    ``decorators.rmq_trigger(...)`` — the two forms the scan reads as the
    ``@rmq_trigger`` decorator.
    """
    return _is_call_named(node, "rmq_trigger")


def _is_dag_call(node: ast.AST) -> TypeGuard[ast.Call]:
    """Return True if node is a call to Airflow's ``dag(...)`` decorator.

    Matches ``dag(...)`` and ``<module>.dag(...)``; a ``dag`` imported under
    another name is not recognized, so its ``dag_id`` stays unresolved.
    """
    return _is_call_named(node, "dag")


def _usable_constants(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    module_constants: dict[str, tuple[Any, int]],
    module_scope_function_ids: set[int],
) -> dict[str, Any]:
    """Return the module constants the decorators of ``node`` may refer to.

    One filter for both ``dag_id=NAME`` and the ``rmq_trigger`` arguments:

    - a function whose decorators do not run in module scope (nested in a
      factory, a class body, ...) gets no constants — a local binding of the
      same name could shadow the module-level one;
    - a module-level function gets the constants assigned above its own line.
      A constant assigned later would raise ``NameError`` when the decorator
      runs at import time, so resolving it statically would be a false
      positive.

    ``module_constants`` and ``module_scope_function_ids`` are the two halves
    of ``_collect_module_constants``'s result. The values are returned as
    stored; ``_resolve_static`` copies them when it hands them out.
    """
    if id(node) not in module_scope_function_ids:
        return {}
    return {
        name: value
        for name, (value, lineno) in module_constants.items()
        if lineno < node.lineno
    }


# ---------------------------------------------------------------------------
# Static values of rmq_trigger(...) arguments
# ---------------------------------------------------------------------------

#: ``rmq_trigger``'s positional-or-keyword parameters, in signature order.
_RMQ_TRIGGER_POSITIONAL = ("queue", "queues", "conn_id", "filter_data", "cooldown")


@dataclass(frozen=True)
class _Unresolved:
    """An argument expression the scan cannot evaluate, and why."""

    reason: str


#: What an expression the scan does not evaluate is, by node type, for the reason text.
_UNREADABLE_EXPRESSIONS: dict[type[ast.AST], str] = {
    ast.Call: "a function call",
    ast.Attribute: "an attribute access",
    ast.Subscript: "a subscript",
    ast.JoinedStr: "an f-string",
    ast.BinOp: "an operator expression (concatenation, arithmetic, ...)",
    ast.BoolOp: "a boolean expression",
    ast.IfExp: "a conditional expression",
    ast.Compare: "a comparison",
    ast.Starred: "a starred (*) unpacking",
    ast.Lambda: "a lambda",
    ast.ListComp: "a comprehension",
    ast.SetComp: "a comprehension",
    ast.DictComp: "a comprehension",
    ast.GeneratorExp: "a comprehension",
    ast.NamedExpr: "an assignment expression (:=)",
}


def _unknown_name_reason(name: str) -> str:
    return (
        f"{name!r} is not a constant the scan can read — it must be assigned a "
        "literal exactly once at the top level of this file, above a module-level "
        "function, and a list or dict constant may appear only in rmq_trigger "
        "arguments"
    )


def _resolve_static(node: ast.expr, constants: dict[str, Any]) -> Any:
    """Evaluate an ``rmq_trigger`` argument expression without executing the file.

    Returns the value, or an ``_Unresolved`` carrying the reason. Readable are:
    literals; names found in ``constants`` (the ``_usable_constants`` of the
    decorated function); list, tuple, set and dict displays whose elements, keys
    included, are readable themselves (``queues=[Q1, Q2]``,
    ``filter_data={"body.type": PAID}``); ``-``/``+`` applied to a number. Anything
    else ``ast.literal_eval`` accepts (``1+2j``, ``set()``) is read through it.
    Everything else is unreadable: it is computed when the file is imported, and
    the scan never imports it.

    The result never shares objects with ``constants``: a constant's value is
    deep-copied, and displays are built afresh.
    """
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id in constants:
            return copy.deepcopy(constants[node.id])
        return _Unresolved(_unknown_name_reason(node.id))
    if isinstance(node, (ast.List, ast.Tuple, ast.Set)):
        items = []
        for elt in node.elts:
            item = _resolve_static(elt, constants)
            if isinstance(item, _Unresolved):
                return item
            items.append(item)
        if isinstance(node, ast.List):
            return items
        if isinstance(node, ast.Tuple):
            return tuple(items)
        try:
            return set(items)
        except TypeError:
            return _Unresolved("a set display holds an unhashable element")
    if isinstance(node, ast.Dict):
        result: dict[Any, Any] = {}
        for key_node, value_node in zip(node.keys, node.values):
            if key_node is None:
                return _Unresolved("a dict display unpacks another dict (**)")
            key = _resolve_static(key_node, constants)
            if isinstance(key, _Unresolved):
                return key
            value = _resolve_static(value_node, constants)
            if isinstance(value, _Unresolved):
                return value
            try:
                result[key] = value
            except TypeError:
                return _Unresolved("a dict display has an unhashable key")
        return result
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        operand = _resolve_static(node.operand, constants)
        if isinstance(operand, _Unresolved):
            return operand
        # Exact type, as in ast.literal_eval: a sign on a bool is not a number.
        if type(operand) not in (int, float, complex):
            return _Unresolved("a sign applied to something other than a number")
        return -operand if isinstance(node.op, ast.USub) else +operand
    try:
        return ast.literal_eval(node)
    except (ValueError, TypeError, SyntaxError, MemoryError, RecursionError):
        pass
    what = _UNREADABLE_EXPRESSIONS.get(type(node), f"a {type(node).__name__} expression")
    return _Unresolved(f"{what} is computed when the file is imported")


class _CallRead(NamedTuple):
    """What the scan read from one ``rmq_trigger(...)`` call.

    ``kwargs`` maps parameter names to the values read, ready for
    ``build_subscriptions``. ``unreadable`` lists ``(parameter, reason)`` for every
    argument that could not be read; the parameter is a placeholder such as
    ``"*args"`` or ``"**OPTS"`` when the argument names none. A call with any
    unreadable argument does not say what the decorator receives, so its
    ``kwargs`` must not be used to build subscriptions.
    """

    kwargs: dict[str, Any]
    unreadable: list[tuple[str, str]]


def _read_rmq_trigger_call(call: ast.Call, constants: dict[str, Any]) -> _CallRead:
    """Read the arguments of one ``rmq_trigger(...)`` call statically.

    ``constants`` are the ``_usable_constants`` of the decorated function. Read are:

    - positional arguments, mapped onto ``queue, queues, conn_id, filter_data,
      cooldown`` (the parameters ``rmq_trigger`` accepts positionally);
    - keyword arguments of ``rmq_trigger``'s own parameters;
    - ``**NAME``, where ``NAME`` is a dict constant whose keys are parameter names.

    Each value goes through ``_resolve_static``. Unreadable, with the reason, is: a
    value ``_resolve_static`` cannot evaluate, a sixth positional argument, ``*args``,
    ``**`` of anything but a dict constant, a key that is not a parameter (written
    out or inside the unpacked dict), and a parameter given more than once across
    positional, keyword and ``**`` arguments — Python rejects the last three when it
    calls the decorator, so the call never produces what the scan would read.
    """
    kwargs: dict[str, Any] = {}
    unreadable: list[tuple[str, str]] = []

    def _take(param: str, value: Any) -> None:
        if param in kwargs:
            unreadable.append((param, "the parameter is given more than once"))
        elif isinstance(value, _Unresolved):
            unreadable.append((param, value.reason))
        else:
            kwargs[param] = value

    for index, arg in enumerate(call.args):
        if isinstance(arg, ast.Starred):
            # The positions the unpacked values land on are unknown, so nothing
            # after it can be mapped onto a parameter either.
            unreadable.append(
                ("*args", "a starred (*) unpacking hides which parameters it fills")
            )
            break
        if index >= len(_RMQ_TRIGGER_POSITIONAL):
            limit = len(_RMQ_TRIGGER_POSITIONAL)
            unreadable.append((
                f"positional argument {index + 1}",
                f"rmq_trigger takes at most {limit} positional arguments",
            ))
            continue
        _take(_RMQ_TRIGGER_POSITIONAL[index], _resolve_static(arg, constants))

    for kw in call.keywords:
        if kw.arg is not None:
            if kw.arg not in _RMQ_TRIGGER_KWARGS:
                unreadable.append((kw.arg, "rmq_trigger has no such parameter"))
                continue
            _take(kw.arg, _resolve_static(kw.value, constants))
            continue
        # **expr: only a dict constant of this file says which arguments it carries.
        if not isinstance(kw.value, ast.Name):
            unreadable.append(
                ("**", "only a dict constant of this file can be unpacked (**NAME)")
            )
            continue
        label = f"**{kw.value.id}"
        unpacked = _resolve_static(kw.value, constants)
        if isinstance(unpacked, _Unresolved):
            unreadable.append((label, unpacked.reason))
            continue
        if not isinstance(unpacked, dict):
            unreadable.append((label, f"{kw.value.id!r} is not a dict constant"))
            continue
        for key, value in unpacked.items():
            if not isinstance(key, str) or key not in _RMQ_TRIGGER_KWARGS:
                unreadable.append((
                    f"{label}[{key!r}]", "rmq_trigger has no such parameter"
                ))
                continue
            _take(key, value)

    return _CallRead(kwargs, unreadable)


# ---------------------------------------------------------------------------
# Reading a whole DAG file
# ---------------------------------------------------------------------------

_FunctionNode = ast.FunctionDef | ast.AsyncFunctionDef


def _read_and_parse(path: str) -> ast.Module:
    """Read ``path`` as bytes and parse it the way an import compiles it.

    ``ast.parse`` is handed the raw bytes, so it decodes the source exactly as the
    import system does: a UTF-8 BOM and a PEP 263 ``coding:`` declaration are
    honoured, and content an import would refuse to decode is refused here too.

    :raises OSError: The file gave no access.
    :raises Exception: Whatever ``ast.parse`` raises for content it refuses —
        ``SyntaxError`` (undecodable source included), ``ValueError`` for null
        bytes, ``RecursionError`` for pathological nesting.
    """
    with open(path, "rb") as f:
        raw = f.read()
    return ast.parse(raw, filename=path)


def _iter_trigger_functions(
    tree: ast.Module,
) -> Iterator[tuple[_FunctionNode, dict[str, Any]]]:
    """Yield every function carrying an ``rmq_trigger(...)`` decorator, with its constants.

    Every ``FunctionDef``/``AsyncFunctionDef`` of the file is visited, nested ones
    included (a DAG declared inside a factory), in ``ast.walk`` order. The
    constants paired with a function are its ``_usable_constants``: those
    assigned above it for a module-level function, none for a nested one.
    """
    module_constants, module_scope_function_ids = _collect_module_constants(tree)
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not any(_is_rmq_trigger_call(d) for d in node.decorator_list):
            continue
        yield node, _usable_constants(node, module_constants, module_scope_function_ids)


def _static_dag_id(func: _FunctionNode, constants: dict[str, Any]) -> str | None:
    """Return the ``dag_id`` the ``@dag(...)`` decorator of ``func`` produces, or ``None``.

    A ``@dag(...)`` without ``dag_id=`` (or with a falsy literal) names the DAG
    after the function, as Airflow does. ``None`` means the ``dag_id`` cannot be
    proven statically (see ``_extract_dag_id_from_decorators``).
    """
    dag_id = _extract_dag_id_from_decorators(func.decorator_list, constants)
    if dag_id is _UNRESOLVED_DAG_ID:
        return None
    if dag_id is None:
        return func.name
    return dag_id


class _FunctionRead(NamedTuple):
    """The subscriptions of one decorated function, and what kept the scan from them.

    Each problem is one line naming the DAG, the ``rmq_trigger`` call's line and
    the reason. A function with problems does not say what its decorators
    receive, so its ``subscriptions`` must not be registered.
    """

    subscriptions: list[dict]
    problems: list[str]


def _read_function_triggers(
    func: _FunctionNode, constants: dict[str, Any], dag_id: str
) -> _FunctionRead:
    """Read every ``rmq_trigger(...)`` decorator of ``func`` and build its subscriptions.

    Each call is read by ``_read_rmq_trigger_call`` and built by
    ``build_subscriptions``, the same builder the decorator runs at import time.
    A problem is recorded, and the call contributes no subscription, when:

    - an argument cannot be read statically (each one with its reason);
    - ``build_subscriptions`` refuses the arguments — the problem carries its text;
    - the call is a second ``exchange=`` subscription on the same DAG
      (``has_exchange_conflict``), which the decorator refuses as well.

    Every subscription returned carries ``dag_id`` and ``group_key`` (the DAG id
    for a cooldown subscription, ``None`` otherwise), ready to be stored.
    """
    subscriptions: list[dict] = []
    problems: list[str] = []
    for decorator in func.decorator_list:
        if not _is_rmq_trigger_call(decorator):
            continue
        where = f"dag_id={dag_id!r}, rmq_trigger at line {decorator.lineno}"
        read = _read_rmq_trigger_call(decorator, constants)
        if read.unreadable:
            problems.extend(
                f"{where}: {param} cannot be read — {reason}"
                for param, reason in read.unreadable
            )
            continue
        try:
            built = build_subscriptions(dag_id=dag_id, **read.kwargs)
        except (ValueError, TypeError) as exc:
            problems.append(f"{where}: the arguments are refused — {exc}")
            continue
        if has_exchange_conflict(subscriptions, built):
            problems.append(
                f"{where}: a second exchange= subscription on the same DAG — every "
                f"one would use the 'rmq_watcher.sub.{dag_id}' queue; use one call "
                "with the union of routing keys"
            )
            continue
        for sub in built:
            sub["dag_id"] = dag_id
            sub["group_key"] = dag_id if sub.get("cooldown", 0) > 0 else None
        subscriptions.extend(built)
    return _FunctionRead(subscriptions, problems)


def _stray_trigger_mentions(tree: ast.Module) -> list[str]:
    """Describe every mention of ``rmq_trigger`` the scan cannot follow.

    The scan reads ``rmq_trigger`` only as the ``func`` of a call that is itself
    an element of a function's ``decorator_list`` — ``@rmq_trigger(...)`` or
    ``@<module>.rmq_trigger(...)``. Any other mention can reach the decorator by
    a path the scan does not see: a direct ``rmq_trigger(...)(dag)``, an import
    under another name, ``rt = decorators.rmq_trigger``,
    ``partial(rmq_trigger, ...)``, a bare ``@rmq_trigger`` without a call, a
    decorator on a class, a local definition of that name. ``from ... import
    rmq_trigger`` under its own name binds nothing new and is fine.
    """
    followed: set[int] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            followed.update(
                id(dec.func) for dec in node.decorator_list if _is_rmq_trigger_call(dec)
            )

    problems: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "rmq_trigger":
            what = "the name rmq_trigger"
        elif isinstance(node, ast.Attribute) and node.attr == "rmq_trigger":
            what = "the attribute .rmq_trigger"
        elif (
            isinstance(node, ast.alias)
            and node.name.rsplit(".", 1)[-1] == "rmq_trigger"
            and node.asname not in (None, "rmq_trigger")
        ):
            problems.append(
                f"line {node.lineno}: rmq_trigger is imported as {node.asname!r} — "
                "import it under its own name"
            )
            continue
        elif (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef))
            and node.name == "rmq_trigger"
        ):
            problems.append(
                f"line {node.lineno}: the file defines its own rmq_trigger"
            )
            continue
        else:
            continue
        if id(node) in followed:
            continue
        problems.append(
            f"line {node.lineno}: {what} is used other than as a decorator call "
            "@rmq_trigger(...) on a function"
        )
    return problems


def _decorator_order_problems(func: _FunctionNode) -> list[str]:
    """Report ``rmq_trigger(...)`` decorators placed below ``@dag(...)`` on ``func``.

    Decorators apply bottom-up, so such a call wraps the plain function rather
    than the DAG factory, and the decorator raises ``TypeError`` when the
    factory is called.
    """
    dag_index = next(
        (i for i, dec in enumerate(func.decorator_list) if _is_dag_call(dec)), None
    )
    if dag_index is None:
        return []
    return [
        f"function {func.name!r}, rmq_trigger at line {dec.lineno}: placed below "
        "@dag(...) — @rmq_trigger must be the outermost decorator, above @dag(...)"
        for dec in func.decorator_list[dag_index + 1:]
        if _is_rmq_trigger_call(dec)
    ]


class _FileRead(NamedTuple):
    """The subscriptions of one DAG file, or the problems that refuse all of them.

    ``subscriptions`` is empty whenever ``problems`` is not.
    """

    subscriptions: list[dict]
    problems: list[str]


def _read_file_triggers(tree: ast.Module) -> _FileRead:
    """Read every ``@rmq_trigger`` of a parsed DAG file, or refuse the whole file.

    The decorator raises at import time on a call it cannot match with this
    reading, and the import error takes every DAG of the file out of Airflow,
    not only the one the call sits on. A subscription the scan kept for a
    neighbouring DAG would then consume for a DAG Airflow no longer runs, so any
    one problem refuses every subscription of the file:

    - a problem of ``_read_function_triggers`` on any function — an unreadable
      argument, a refused build, a second ``exchange=`` subscription;
    - a function carrying ``@rmq_trigger`` whose ``dag_id`` cannot be proven
      (``_static_dag_id``);
    - ``@rmq_trigger`` placed below ``@dag(...)`` (``_decorator_order_problems``);
    - a mention of ``rmq_trigger`` the scan cannot follow
      (``_stray_trigger_mentions``).
    """
    problems = _stray_trigger_mentions(tree)
    subscriptions: list[dict] = []
    for func, constants in _iter_trigger_functions(tree):
        problems.extend(_decorator_order_problems(func))
        dag_id = _static_dag_id(func, constants)
        if dag_id is None:
            problems.append(
                f"function {func.name!r} at line {func.lineno}: cannot statically "
                "resolve dag_id — dag_id= of @dag(...) must be a string literal, or "
                "a string constant assigned once at the top level of this file above "
                "a module-level function, and dag must be imported under its own name"
            )
            continue
        read = _read_function_triggers(func, constants, dag_id)
        problems.extend(read.problems)
        subscriptions.extend(read.subscriptions)
    if problems:
        return _FileRead([], problems)
    return _FileRead(subscriptions, [])


# ---------------------------------------------------------------------------
# Checking a decorator call against the scan
# ---------------------------------------------------------------------------

# Keys the watcher adds to every stored row itself; the decorator's subscriptions
# do not carry them.
_ROW_KEYS = frozenset({"dag_id", "group_key"})

_MISSING = object()


class RMQTriggerNotStaticError(ValueError):
    """The watcher's scan would not see the subscriptions an ``@rmq_trigger`` call builds.

    Raised by the decorator while the DAG file is imported, so the DAG processor
    records an import error for the file instead of running DAGs the watcher
    would serve with other arguments, or not at all.
    """


def _watcher_scans(path: str, dags_folder: str) -> bool:
    """Tell whether the watcher's ``glob(DAGS_FOLDER/**/*.py)`` lists ``path``.

    The file must end in ``.py``, lie inside ``dags_folder``, and no component of
    its path below the folder — the file name included — may start with a dot:
    ``glob`` skips hidden directories and files such as ``.x.py``. Both paths go
    through ``os.path.abspath`` only, without resolving symlinks, because
    ``glob`` returns the paths below the folder as they are spelled there.
    """
    if not path.endswith(".py"):
        return False
    prefix = os.path.join(os.path.abspath(dags_folder), "")
    full = os.path.abspath(path)
    if not full.startswith(prefix):
        return False
    below = full[len(prefix):]
    return not any(part.startswith(".") for part in below.split(os.sep))


def _same_value(a: Any, b: Any) -> bool:
    """Compare two argument values the way the stored row would tell them apart.

    Scalars are equal only with the same exact type, so ``1``, ``1.0`` and
    ``True`` differ. Lists and tuples compare element by element, sets by
    members. Dicts compare by key sets — a key matches only a key of the same
    type — and the values under matching keys; keys are never sorted, so a dict
    with keys of mixed types compares without error.
    """
    if type(a) is not type(b):
        return False
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(_same_value(x, y) for x, y in zip(a, b))
    if isinstance(a, (set, frozenset)):
        return len(a) == len(b) and all(any(_same_value(x, y) for y in b) for x in a)
    if isinstance(a, dict):
        if len(a) != len(b):
            return False
        for key_a, value_a in a.items():
            key_b = next((k for k in b if _same_value(key_a, k)), _MISSING)
            if key_b is _MISSING or not _same_value(value_a, b[key_b]):
                return False
        return True
    return bool(a == b)


def _subscription_differences(static: list[dict], runtime: list[dict]) -> list[str]:
    """List the fields in which the scan's subscriptions differ from the decorator's."""
    if len(static) != len(runtime):
        return [(
            f"the watcher builds {len(static)} subscription(s), the decorator "
            f"builds {len(runtime)}"
        )]
    differences: list[str] = []
    for index, (static_sub, runtime_sub) in enumerate(zip(static, runtime), start=1):
        keys = [k for k in static_sub if k not in _ROW_KEYS]
        keys += [k for k in runtime_sub if k not in _ROW_KEYS and k not in static_sub]
        for key in keys:
            read = static_sub.get(key, _MISSING)
            given = runtime_sub.get(key, _MISSING)
            if read is not _MISSING and given is not _MISSING and _same_value(read, given):
                continue
            read_text = "nothing" if read is _MISSING else repr(read)
            given_text = "nothing" if given is _MISSING else repr(given)
            differences.append(
                f"subscription {index}, {key}: the watcher reads {read_text}, "
                f"the decorator receives {given_text}"
            )
    return differences


def _find_trigger_call(
    tree: ast.Module, lineno: int
) -> tuple[_FunctionNode, dict[str, Any], ast.Call] | None:
    """Find the ``@rmq_trigger(...)`` decorator whose lines span ``lineno``.

    The function, its ``_usable_constants`` and the call are returned; ``None``
    when no ``rmq_trigger(...)`` call in any ``decorator_list`` covers the line.
    """
    for func, constants in _iter_trigger_functions(tree):
        for decorator in func.decorator_list:
            if not _is_rmq_trigger_call(decorator):
                continue
            end = decorator.end_lineno or decorator.lineno
            if decorator.lineno <= lineno <= end:
                return func, constants, decorator
    return None


def _verify_static_readability(
    path: str,
    lineno: int,
    dag_id: str,
    runtime_subs: list[dict],
    dags_folder: str,
) -> None:
    """Check that the watcher's scan reads this ``rmq_trigger(...)`` call as the decorator does.

    ``path`` and ``lineno`` are the call site of ``rmq_trigger(...)``, ``dag_id``
    the DAG it decorates and ``runtime_subs`` the subscriptions
    ``build_subscriptions`` made from the arguments the decorator received.
    In order:

    1. the watcher must scan the file (``_watcher_scans`` against
       ``dags_folder``);
    2. the file must read and parse (``_read_and_parse``), and
       ``_read_file_triggers`` must find no problem anywhere in it — any problem
       would make the watcher refuse every subscription of the file;
    3. an ``rmq_trigger(...)`` call in a ``decorator_list`` must span ``lineno``;
    4. the ``dag_id`` the scan resolves for that function must be ``dag_id``;
    5. the subscriptions the scan builds from that call must match
       ``runtime_subs`` field by field (``_same_value``), leaving out the
       ``dag_id``/``group_key`` keys the watcher adds to its rows.

    :raises RMQTriggerNotStaticError: The first check that fails, with its reason.
    """
    where = f"@rmq_trigger(...) at {path}:{lineno} on DAG {dag_id!r}"

    if not _watcher_scans(path, dags_folder):
        raise RMQTriggerNotStaticError(
            f"{where} is in a file the RMQ watcher does not scan, so the watcher "
            "never registers its subscriptions. The watcher reads only *.py files "
            f"inside DAGS_FOLDER ({dags_folder}) with no directory or file name "
            "starting with '.' below it; declare @rmq_trigger(...) above @dag(...) "
            "in such a DAG file."
        )

    try:
        tree = _read_and_parse(path)
    except Exception as exc:
        raise RMQTriggerNotStaticError(
            f"{where}: the RMQ watcher cannot read this file "
            f"({type(exc).__name__}: {exc}), so it never registers its "
            "subscriptions."
        ) from exc

    file_read = _read_file_triggers(tree)
    if file_read.problems:
        listed = "\n".join(f"- {problem}" for problem in file_read.problems)
        raise RMQTriggerNotStaticError(
            f"{where}: the RMQ watcher cannot read every @rmq_trigger of this file "
            "statically and refuses all of its subscriptions:\n"
            f"{listed}\n"
            "Arguments and dag_id must be literals or constants assigned once at "
            "the top level of this file above the decorated function."
        )

    found = _find_trigger_call(tree, lineno)
    if found is None:
        raise RMQTriggerNotStaticError(
            f"{where}: rmq_trigger is called through a name the RMQ watcher does "
            "not recognize. The watcher sees only the form @rmq_trigger(...) "
            "above @dag(...) on a function of the DAG file; import rmq_trigger "
            "from airflow_provider_rmq.watcher.decorators under its own name."
        )
    func, constants, call = found

    static_dag_id = _static_dag_id(func, constants)
    if static_dag_id != dag_id:
        raise RMQTriggerNotStaticError(
            f"{where}: the RMQ watcher reads dag_id={static_dag_id!r} for this "
            f"call, but it decorates the DAG {dag_id!r}."
        )

    static_subs = build_subscriptions(
        dag_id=dag_id, **_read_rmq_trigger_call(call, constants).kwargs
    )
    differences = _subscription_differences(static_subs, runtime_subs)
    if differences:
        listed = "\n".join(f"- {difference}" for difference in differences)
        raise RMQTriggerNotStaticError(
            f"{where}: the RMQ watcher reads other arguments from the file than "
            f"the decorator receives:\n{listed}\n"
            "A constant the call uses must keep its value until the DAG is built."
        )
