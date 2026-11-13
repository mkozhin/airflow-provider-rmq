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
the scan would not see the subscriptions the decorator builds. That check reads
each file once per version of it on disk (``_analyse_file``), however many
``@rmq_trigger`` calls of the file it is asked about.

The module depends only on the standard library and ``subscription_builder``, so
it can be imported inside the DAG processor without pulling in the watcher's
runtime modules (``listener``, ``consumer``, ``models``, ``runner``) and their
third-party dependencies.
"""

from __future__ import annotations

import ast
import os
from collections.abc import Iterable, Iterator
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


def _scope_nodes(body: Iterable[ast.AST]) -> Iterator[ast.AST]:
    """Yield every node of ``body`` that executes in the scope ``body`` belongs to.

    Like ``ast.walk``, but prunes at nested-scope boundaries: a
    ``FunctionDef``/``ClassDef``/``Lambda``/comprehension node is yielded
    itself (so the name it binds in the enclosing scope is counted) and its
    ``_outer_parts`` are still traversed, but its body is not.
    """
    stack: list[ast.AST] = list(body)
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, _SCOPE_BOUNDARIES):
            stack.extend(_outer_parts(node))
            continue
        stack.extend(ast.iter_child_nodes(node))


def _module_scope_nodes(tree: ast.Module) -> Iterator[ast.AST]:
    """Yield every node that executes in the module's own scope (``_scope_nodes``).

    Bindings inside a function, class, lambda or comprehension body belong to a
    different scope and must not poison a module-level constant.
    """
    return _scope_nodes(tree.body)


def _contains_mutable(value: Any) -> bool:
    """Return True if ``value`` is, or holds at any depth, a ``list`` or ``dict``.

    These are the mutable containers ``_resolve_static`` can produce; a tuple is
    looked into because a tuple of lists is mutable through its elements.
    """
    if isinstance(value, (list, dict)):
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

    The value is anything ``_resolve_static`` reads without constants: strings,
    numbers, booleans, ``None``, and lists, tuples and dicts built from them —
    the values a subscription stored as JSON can hold. Callers decide which
    types they accept — ``dag_id=NAME`` takes a string constant only.

    A value that is or holds a ``list`` or ``dict`` could be changed
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
    unreadable); guessing wrong is not.
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
            value = _resolve_static(value_node, {})
        except _Unreadable:
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
    ``@rmq_trigger`` decorator. The match is by name; whether the name is bound
    to the provider's decorator is ``_trigger_origin_problems``'s question.
    """
    return _is_call_named(node, "rmq_trigger")


def _is_dag_call(node: ast.AST) -> TypeGuard[ast.Call]:
    """Return True if node is a call to Airflow's ``dag(...)`` decorator.

    Matches ``dag(...)`` and ``<module>.dag(...)``; a ``dag`` imported under
    another name is not recognized, so its ``dag_id`` stays unresolved. The
    match is by name; whether the name is bound to Airflow's decorator is
    ``_dag_origin_problems``'s question.
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
    of ``_collect_module_constants``'s result.
    """
    if id(node) not in module_scope_function_ids:
        return {}
    return {
        name: value
        for name, (value, lineno) in module_constants.items()
        if lineno < node.lineno
    }


# ---------------------------------------------------------------------------
# Where the names of a decorator expression come from
# ---------------------------------------------------------------------------

_ScopeOwner = ast.Module | ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef

_COMPREHENSIONS = (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)

#: Statements whose body can run more than once.
_LOOPS = (ast.For, ast.AsyncFor, ast.While)


def _walrus_targets(comprehension: ast.AST) -> Iterator[ast.Name]:
    """Yield the ``:=`` targets of a comprehension, which bind in its enclosing scope."""
    stack: list[ast.AST] = [comprehension]
    while stack:
        node = stack.pop()
        if isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name):
            yield node.target
        if node is not comprehension and isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
        ):
            continue
        stack.extend(ast.iter_child_nodes(node))


def _bindings_of(node: ast.AST) -> Iterator[tuple[str, str | None]]:
    """Yield ``(name, path)`` for every name ``node`` binds in its own scope.

    ``path`` is the dotted path an absolute import binds the name to —
    ``import a.b.c`` binds ``a`` to ``"a"``, ``import a.b.c as X`` binds ``X`` to
    ``"a.b.c"``, ``from a.b import c as X`` binds ``X`` to ``"a.b.c"`` — and
    ``None`` for any other binding: an assignment target, ``del``, a ``def`` or
    ``class``, a parameter, an ``except ... as`` or ``match`` capture, a relative
    import.
    """
    if isinstance(node, ast.Import):
        for alias in node.names:
            if alias.asname:
                yield alias.asname, alias.name
            else:
                top = alias.name.split(".")[0]
                yield top, top
    elif isinstance(node, ast.ImportFrom):
        absolute = node.level == 0 and node.module
        for alias in node.names:
            if alias.name != "*":
                yield (
                    alias.asname or alias.name,
                    f"{node.module}.{alias.name}" if absolute else None,
                )
    elif isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
        yield node.id, None
    elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
        yield node.name, None
    elif isinstance(node, ast.arg):
        yield node.arg, None
    elif isinstance(node, (ast.ExceptHandler, ast.MatchAs, ast.MatchStar)) and node.name:
        yield node.name, None
    elif isinstance(node, ast.MatchMapping) and node.rest:
        yield node.rest, None


class _Scope(NamedTuple):
    """The names one scope binds itself, and the names it hands to another scope.

    ``bindings`` maps each name to ``(path, node)`` for every binding of it in the
    scope (see ``_bindings_of``), ``node`` being the node that binds it;
    ``global_names`` and ``nonlocal_names`` are the names its ``global`` and
    ``nonlocal`` statements declare.
    """

    bindings: dict[str, list[tuple[str | None, ast.AST]]]
    global_names: set[str]
    nonlocal_names: set[str]


def _read_scope(owner: _ScopeOwner) -> _Scope:
    """Collect the bindings of the scope ``owner`` opens: its parameters and body."""
    nodes: list[ast.AST]
    if isinstance(owner, (ast.FunctionDef, ast.AsyncFunctionDef)):
        args = owner.args
        nodes = [*args.posonlyargs, *args.args, *args.kwonlyargs]
        nodes.extend(arg for arg in (args.vararg, args.kwarg) if arg is not None)
        nodes.extend(owner.body)
    else:
        nodes = list(owner.body)
    bindings: dict[str, list[tuple[str | None, ast.AST]]] = {}
    global_names: set[str] = set()
    nonlocal_names: set[str] = set()
    for node in _scope_nodes(nodes):
        if isinstance(node, ast.Global):
            global_names.update(node.names)
        elif isinstance(node, ast.Nonlocal):
            nonlocal_names.update(node.names)
        elif isinstance(node, _COMPREHENSIONS):
            for target in _walrus_targets(node):
                bindings.setdefault(target.id, []).append((None, target))
        for name, path in _bindings_of(node):
            bindings.setdefault(name, []).append((path, node))
    return _Scope(bindings, global_names, nonlocal_names)


class _NameOrigins:
    """Tell which import a name in a function's decorators is bound to.

    A decorator expression is evaluated in the scope enclosing the decorated
    function, so only the bindings of that scope and of the scopes Python looks
    a name up in after it decide what the name is:

    - a function scope that binds the name itself — a parameter, an assignment,
      an import, a ``with ... as`` — decides alone, whichever line binds it, since
      the name is local to it throughout; a ``nonlocal`` declaration of the name
      in a function nested in it counts as one more binding;
    - a class body decides when the decorated function sits directly in it;
      a class enclosing it further out is skipped, as Python skips it;
    - a ``global`` declaration of the name in an enclosing function sends the
      lookup to the module, and a ``nonlocal`` one on to the next function out;
    - the module scope decides last. Every ``global`` declaration of the name in
      any function of the file counts as a binding, since the function may run
      before the decorator. The module bindings that run only after the
      decorators are left out (``_runs_after_decorators``): for a function at
      module level, outside any loop, those at or below its ``def`` line; for a
      function nested in a factory — a ``def`` at module level, outside any
      loop — those at or below the factory's ``def`` line when the file calls the
      factory by its name, only by its name, and every such call runs once
      before them (``dag = create_dag()`` below the factory, a
      ``with DAG(...) as dag:`` below its last call).

    Bindings local to any other scope — a parameter or local variable of an
    unrelated function or callback, of a function nested in the decorated one,
    of a lambda or a comprehension — cannot affect the decorator and are not
    looked at.

    The name is bound to a path only when every binding that decides it is an
    absolute import of that one path; any other binding, two imports of
    different paths, or no binding at all give ``None``.
    """

    def __init__(self, tree: ast.Module) -> None:
        self._tree = tree
        self._parents: dict[int, ast.AST] = {}
        self._global_names: set[str] = set()
        self._mentions: dict[str, list[ast.Name]] = {}
        self._nonlocal_names: dict[int, set[str]] = {}
        for node in ast.walk(tree):
            for child in ast.iter_child_nodes(node):
                self._parents[id(child)] = node
            if isinstance(node, ast.Global):
                self._global_names.update(node.names)
            elif isinstance(node, ast.Name):
                self._mentions.setdefault(node.id, []).append(node)
        self._scopes: dict[int, _Scope] = {}

    def _scope(self, owner: _ScopeOwner) -> _Scope:
        scope = self._scopes.get(id(owner))
        if scope is None:
            scope = self._scopes[id(owner)] = _read_scope(owner)
        return scope

    def _nonlocal_below(self, owner: _ScopeOwner) -> set[str]:
        """Names declared ``nonlocal`` anywhere inside ``owner``."""
        names = self._nonlocal_names.get(id(owner))
        if names is None:
            names = self._nonlocal_names[id(owner)] = {
                name
                for node in ast.walk(owner)
                if isinstance(node, ast.Nonlocal)
                for name in node.names
            }
        return names

    def _owner(self, node: ast.AST) -> tuple[_ScopeOwner, bool]:
        """Return the scope enclosing ``node`` and whether a loop lies in between."""
        in_loop = False
        parent = self._parents[id(node)]
        while not isinstance(
            parent, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ):
            in_loop = in_loop or isinstance(parent, _LOOPS)
            parent = self._parents[id(parent)]
        return parent, in_loop

    def _within(self, node: ast.AST, ancestor: ast.AST) -> bool:
        """Tell whether ``node`` is ``ancestor`` or lies inside it."""
        while node is not ancestor:
            parent = self._parents.get(id(node))
            if parent is None:
                return False
            node = parent
        return True

    def _is_called(self, mention: ast.Name) -> bool:
        """Tell whether ``mention`` is the callee of a call, as in ``create_dag()``."""
        parent = self._parents[id(mention)]
        return isinstance(parent, ast.Call) and parent.func is mention

    def _runs_once_before(self, mention: ast.Name, binding: ast.AST) -> bool:
        """Tell whether the module-level code at ``mention`` runs once, before ``binding``.

        ``mention`` must run in the module's own flow, outside any loop, function,
        class body, lambda or comprehension, which could run it again or later.
        Module statements run top to bottom, so it runs before a binding further
        down the file; and the value of an assignment runs before its targets
        are bound, so ``dag = create_dag()`` calls the factory before it binds
        ``dag``.
        """
        statement: ast.AST | None = None
        parent = self._parents[id(mention)]
        while not isinstance(parent, ast.Module):
            if isinstance(parent, (*_SCOPE_BOUNDARIES, *_LOOPS)):
                return False
            if statement is None and isinstance(parent, ast.stmt):
                statement = parent
            parent = self._parents[id(parent)]
        if (
            isinstance(statement, (ast.Assign, ast.AnnAssign, ast.AugAssign))
            and statement.value is not None
            and self._within(mention, statement.value)
            and self._within(binding, statement)
        ):
            return True
        return (mention.lineno, mention.col_offset) < (
            getattr(binding, "lineno", 0), getattr(binding, "col_offset", 0)
        )

    def _runs_after_decorators(
        self, binding: ast.AST, top: _FunctionNode, func: _FunctionNode
    ) -> bool:
        """Tell whether a module-level ``binding`` runs only after the decorators of ``func``.

        ``top`` is the function at module level, outside any loop, that ``func``
        is or is nested in. The decorators of ``func`` run when ``top`` is
        defined (``top`` is ``func``) or each time it is called, so a binding
        above the ``def`` of ``top`` runs before them. A binding at or below it
        runs after them when ``top`` is ``func``, and when ``top`` is a factory
        the file mentions at least once, every mention being a call
        (``_is_called``) that runs once before the binding
        (``_runs_once_before``). Any other mention — ``make = create_dag``,
        ``FACTORIES.append(create_dag)``, ``register(create_dag)`` — hands the
        factory to code that may call it after the binding, and a factory its
        file never mentions is there for another file to import and call after
        the whole module has run; then the binding counts. A ``:=`` binding is
        never left out: an expression may evaluate it before the call it is
        written after.
        """
        if getattr(binding, "lineno", 0) < top.lineno:
            return False
        if top is func:
            return True
        if isinstance(self._parents.get(id(binding)), ast.NamedExpr):
            return False
        mentions = self._mentions.get(top.name, [])
        return bool(mentions) and all(
            self._is_called(mention) and self._runs_once_before(mention, binding)
            for mention in mentions
        )

    @staticmethod
    def _single_path(paths: list[str | None]) -> str | None:
        distinct = set(paths)
        if len(distinct) != 1:
            return None
        return distinct.pop()

    def _module_path(
        self, name: str, top: _FunctionNode | None, func: _FunctionNode
    ) -> str | None:
        """Resolve ``name`` in the module scope for the decorators of ``func``.

        ``top`` is the function at module level, outside any loop, that ``func``
        is or is nested in, or ``None`` when there is none or the lookup arrives
        from a ``global`` declaration; then every module binding counts.
        """
        paths = [
            path
            for path, node in self._scope(self._tree).bindings.get(name, [])
            if top is None or not self._runs_after_decorators(node, top, func)
        ]
        if name in self._global_names:
            paths.append(None)
        return self._single_path(paths)

    def path(self, name: str, func: _FunctionNode) -> str | None:
        """Return the dotted path ``name`` in the decorators of ``func`` is bound to."""
        owner, in_loop = self._owner(func)
        top: _ScopeOwner | _FunctionNode = func
        while not isinstance(owner, ast.Module):
            if top is func or not isinstance(owner, ast.ClassDef):
                scope = self._scope(owner)
                if name in scope.global_names:
                    return self._module_path(name, None, func)
                if name not in scope.nonlocal_names and name in scope.bindings:
                    paths = [path for path, _ in scope.bindings[name]]
                    if name in self._nonlocal_below(owner):
                        paths.append(None)
                    return self._single_path(paths)
            top = owner
            owner, in_loop = self._owner(owner)
        if in_loop or not isinstance(top, (ast.FunctionDef, ast.AsyncFunctionDef)):
            return self._module_path(name, None, func)
        return self._module_path(name, top, func)

    def dotted_path(self, expr: ast.expr, func: _FunctionNode) -> str | None:
        """Return the dotted path ``expr`` in the decorators of ``func`` names, or ``None``."""
        if isinstance(expr, ast.Name):
            return self.path(expr.id, func)
        if isinstance(expr, ast.Attribute):
            base = self.dotted_path(expr.value, func)
            return f"{base}.{expr.attr}" if base is not None else None
        return None


def _has_wildcard_import(tree: ast.Module) -> bool:
    """Tell whether the file has a ``from ... import *``, which can bind any name."""
    return any(
        isinstance(node, ast.ImportFrom) and any(a.name == "*" for a in node.names)
        for node in ast.walk(tree)
    )


#: The dotted paths under which the provider's decorator is importable.
_PROVIDER_TRIGGER_PATHS = frozenset({
    "airflow_provider_rmq.watcher.decorators.rmq_trigger",
    "airflow_provider_rmq.watcher.rmq_trigger",
})


def _trigger_origin_problems(tree: ast.Module, origins: _NameOrigins) -> list[str]:
    """Report every ``@rmq_trigger(...)`` decorator call not bound to the provider's decorator.

    The scan builds a subscription from what ``rmq_trigger`` of
    ``airflow_provider_rmq.watcher.decorators`` would build. A same-named wrapper
    of another module — ``functools.partial(rmq_trigger, conn_id=...)``, a
    ``def rmq_trigger(**kw)`` — passes the decorator other arguments than the
    file shows, so a call is read only when its ``rmq_trigger`` is:

    - the bare name, bound by ``from airflow_provider_rmq.watcher.decorators
      import rmq_trigger`` (or ``from airflow_provider_rmq.watcher import
      rmq_trigger``);
    - ``<module>.rmq_trigger``, where ``<module>`` is bound to
      ``airflow_provider_rmq.watcher.decorators`` or
      ``airflow_provider_rmq.watcher`` by an import
      (``from airflow_provider_rmq.watcher import decorators``,
      ``import airflow_provider_rmq.watcher.decorators as d``).

    The names are resolved in the scope the decorator runs in (``origins``):
    an unrelated function's own ``rmq_trigger`` or ``decorators`` does not
    count, a rebinding the decorator would see does. A file with ``from ...
    import *`` cannot tell where any name comes from, so each of its calls is
    reported.
    """
    has_wildcard = _has_wildcard_import(tree)
    problems: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for dec in node.decorator_list:
            if not _is_rmq_trigger_call(dec):
                continue
            written = ast.unparse(dec.func)
            if has_wildcard:
                problems.append(
                    f"line {dec.lineno}: the file has 'from ... import *', so the "
                    f"scan cannot tell where {written} comes from — import "
                    "rmq_trigger by name from airflow_provider_rmq.watcher.decorators "
                    "and drop the wildcard import"
                )
                continue
            path = origins.dotted_path(dec.func, node)
            if path in _PROVIDER_TRIGGER_PATHS:
                continue
            what = (
                f"{written} is {path}"
                if path is not None
                else f"{written} is not bound by one absolute import alone in the scope the decorator runs in"
            )
            problems.append(
                f"line {dec.lineno}: {what}, not rmq_trigger of "
                "airflow_provider_rmq.watcher.decorators — import it with "
                "'from airflow_provider_rmq.watcher.decorators import rmq_trigger'"
            )
    return problems


#: The dotted paths under which Airflow's ``@dag`` decorator is importable in
#: Airflow 2.x: ``airflow.decorators`` re-exports the function defined in
#: ``airflow.models.dag``.
_AIRFLOW_DAG_PATHS = frozenset({
    "airflow.decorators.dag",
    "airflow.models.dag.dag",
})


def _dag_origin_problems(tree: ast.Module, origins: _NameOrigins) -> list[str]:
    """Report every ``@dag(...)`` call on an ``@rmq_trigger`` function not bound to Airflow's decorator.

    The scan takes the ``dag_id`` from the ``@dag(...)`` call as Airflow's
    decorator reads it. A same-named wrapper of another module — a ``def
    dag(dag_id, **kw)`` that prefixes the id, a ``partial`` with other defaults
    — gives the DAG another ``dag_id`` than the file shows, so on a function
    carrying ``@rmq_trigger(...)`` a ``dag(...)`` decorator call is read only when
    its ``dag`` is:

    - the bare name, bound by ``from airflow.decorators import dag`` (or
      ``from airflow.models.dag import dag``);
    - ``<module>.dag``, where ``<module>`` is bound to ``airflow.decorators`` or
      ``airflow.models.dag`` by an import (``from airflow import decorators``,
      ``import airflow.decorators``).

    The names are resolved in the scope the decorator runs in (``origins``): a
    ``dag`` parameter of a task, ``dag = context["dag"]`` in a callback or a
    ``with DAG(...) as dag:`` below the decorated function does not count, a
    module-level rebinding above it, ``global dag`` in a function or a ``dag``
    of the factory enclosing a nested DAG function does. A file with ``from ...
    import *`` cannot tell where any name comes from, so each such call is
    reported.
    """
    has_wildcard = _has_wildcard_import(tree)
    problems: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if not any(_is_rmq_trigger_call(dec) for dec in node.decorator_list):
            continue
        for dec in node.decorator_list:
            if not _is_dag_call(dec):
                continue
            written = ast.unparse(dec.func)
            if has_wildcard:
                problems.append(
                    f"line {dec.lineno}: the file has 'from ... import *', so the "
                    f"scan cannot tell where {written} comes from — import dag by "
                    "name from airflow.decorators and drop the wildcard import"
                )
                continue
            path = origins.dotted_path(dec.func, node)
            if path in _AIRFLOW_DAG_PATHS:
                continue
            what = (
                f"{written} is {path}"
                if path is not None
                else f"{written} is not bound by one absolute import alone in the scope the decorator runs in"
            )
            problems.append(
                f"line {dec.lineno}: {what}, not Airflow's dag decorator — import "
                "it with 'from airflow.decorators import dag'"
            )
    return problems


# ---------------------------------------------------------------------------
# Static values of rmq_trigger(...) arguments
# ---------------------------------------------------------------------------

#: ``rmq_trigger``'s positional-or-keyword parameters, in signature order.
_RMQ_TRIGGER_POSITIONAL = ("queue", "queues", "conn_id", "filter_data", "cooldown")


class _Unreadable(Exception):
    """An argument expression the scan cannot evaluate; the message is the reason."""


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

#: The scalar types a subscription stored as JSON can hold.
_JSON_SCALARS = (str, int, float, bool, type(None))


def _unknown_name_reason(name: str) -> str:
    return (
        f"{name!r} is not a constant the scan can read — it must be assigned a "
        "literal exactly once at the top level of this file, above a module-level "
        "function, and a list or dict constant may appear only in rmq_trigger "
        "arguments"
    )


def _resolve_static(node: ast.expr, constants: dict[str, Any]) -> Any:
    """Evaluate an ``rmq_trigger`` argument expression without executing the file.

    Readable are: literals of the JSON scalar types (strings, numbers, booleans,
    ``None``); names found in ``constants`` (the ``_usable_constants`` of the
    decorated function); list, tuple and dict displays whose elements, keys
    included, are readable themselves (``queues=[Q1, Q2]``,
    ``filter_data={"body.type": PAID}``); ``-``/``+`` applied to a number.
    Everything else is unreadable: it is computed when the file is imported, and
    the scan never imports it, or — a set display, a bytes or complex literal —
    it is a value a subscription stored as JSON cannot hold.

    A constant's value is returned as stored in ``constants``.

    :raises _Unreadable: The expression cannot be read; the message says why.
    """
    if isinstance(node, ast.Constant):
        if isinstance(node.value, _JSON_SCALARS):
            return node.value
        raise _Unreadable(
            f"a {type(node.value).__name__} literal is not a value a subscription "
            "can store"
        )
    if isinstance(node, ast.Name):
        if node.id in constants:
            return constants[node.id]
        raise _Unreadable(_unknown_name_reason(node.id))
    if isinstance(node, ast.List):
        return [_resolve_static(elt, constants) for elt in node.elts]
    if isinstance(node, ast.Tuple):
        return tuple(_resolve_static(elt, constants) for elt in node.elts)
    if isinstance(node, ast.Set):
        raise _Unreadable(
            "a set display is not a value a subscription can store — use a list"
        )
    if isinstance(node, ast.Dict):
        result: dict[Any, Any] = {}
        for key_node, value_node in zip(node.keys, node.values):
            if key_node is None:
                raise _Unreadable("a dict display unpacks another dict (**)")
            key = _resolve_static(key_node, constants)
            value = _resolve_static(value_node, constants)
            try:
                result[key] = value
            except TypeError:
                raise _Unreadable("a dict display has an unhashable key") from None
        return result
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, (ast.USub, ast.UAdd)):
        operand = _resolve_static(node.operand, constants)
        # Exact type, as in ast.literal_eval: a sign on a bool is not a number.
        if type(operand) not in (int, float):
            raise _Unreadable("a sign applied to something other than a number")
        return -operand if isinstance(node.op, ast.USub) else +operand
    what = _UNREADABLE_EXPRESSIONS.get(type(node), f"a {type(node).__name__} expression")
    raise _Unreadable(f"{what} is computed when the file is imported")


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
        else:
            kwargs[param] = value

    def _read(label: str, node: ast.expr) -> tuple[bool, Any]:
        try:
            return True, _resolve_static(node, constants)
        except _Unreadable as exc:
            unreadable.append((label, str(exc)))
            return False, None

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
        param = _RMQ_TRIGGER_POSITIONAL[index]
        ok, value = _read(param, arg)
        if ok:
            _take(param, value)

    for kw in call.keywords:
        if kw.arg is not None:
            if kw.arg not in _RMQ_TRIGGER_KWARGS:
                unreadable.append((kw.arg, "rmq_trigger has no such parameter"))
                continue
            ok, value = _read(kw.arg, kw.value)
            if ok:
                _take(kw.arg, value)
            continue
        # **expr: only a dict constant of this file says which arguments it carries.
        if not isinstance(kw.value, ast.Name):
            unreadable.append(
                ("**", "only a dict constant of this file can be unpacked (**NAME)")
            )
            continue
        label = f"**{kw.value.id}"
        ok, unpacked = _read(label, kw.value)
        if not ok:
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

    ``None`` means the ``dag_id`` cannot be proven statically; the caller refuses
    the file rather than guess, because a guess would register a subscription
    under a ``dag_id`` no Airflow DAG has. ``constants`` are the
    ``_usable_constants`` of ``func``. The first recognized ``@dag(...)`` call
    (``_is_dag_call``) among the decorators decides:

    - no ``dag_id`` given: the function's name, as Airflow names the DAG;
    - ``dag_id`` given as the first positional argument or as ``dag_id=``: a
      string literal is the ``dag_id``, and a falsy literal of any type (``""``,
      ``None``, ``0``, ``[]``, ...) gives the function's name, mirroring
      Airflow's ``dag_id or f.__name__``; a name is read from ``constants`` and
      only a string constant counts — an empty one gives the function's name,
      any other type (``DAG_ID = None``) is unresolved. Anything else — a truthy
      non-string literal, a name that is not a constant, a computed expression —
      is unresolved.

    Also unresolved: no recognized ``@dag(...)`` call at all (``dag`` imported
    under another name); ``*ARGS`` among the positional arguments, which hides
    what lands on ``dag_id``; a positional ``dag_id`` together with ``dag_id=``,
    which Python rejects when it calls ``dag``; and, when ``dag_id=`` is not
    written out, a ``**`` unpacking that could carry it.
    """
    for dec in func.decorator_list:
        if not _is_dag_call(dec):
            continue
        if any(isinstance(arg, ast.Starred) for arg in dec.args):
            return None
        value_node: ast.expr | None
        if dec.args:
            if any(kw.arg == "dag_id" for kw in dec.keywords):
                return None
            value_node = dec.args[0]
        else:
            value_node = next(
                (kw.value for kw in dec.keywords if kw.arg == "dag_id"), None
            )
            if value_node is None:
                if any(kw.arg is None for kw in dec.keywords):
                    return None
                return func.name
        if isinstance(value_node, ast.Name):
            if value_node.id not in constants:
                return None
            value = constants[value_node.id]
            if not isinstance(value, str):
                return None
            return value or func.name
        try:
            value = _resolve_static(value_node, {})
        except _Unreadable:
            return None
        if not value:
            return func.name
        return value if isinstance(value, str) else None
    return None


class _TriggerCall(NamedTuple):
    """One ``rmq_trigger(...)`` call the scan read: its lines, DAG and subscriptions.

    ``subscriptions`` are what ``build_subscriptions`` made of the arguments read,
    without the ``dag_id``/``group_key`` keys of a stored row — the shape the
    decorator builds from the arguments it receives.
    """

    first_line: int
    last_line: int
    dag_id: str
    subscriptions: list[dict]


class _TriggerRead(NamedTuple):
    """The subscriptions of a function or a file, and what kept the scan from them.

    ``subscriptions`` are rows ready to be stored, each carrying ``dag_id`` and
    ``group_key``; ``calls`` lists every call read, in file order. Each problem is
    one line naming where it is and why. A read with problems does not say what
    the decorators receive, so its subscriptions must not be registered: for a
    file, ``subscriptions`` is empty whenever ``problems`` is not.
    """

    subscriptions: list[dict]
    problems: list[str]
    calls: list[_TriggerCall]


def _read_function_triggers(
    func: _FunctionNode, constants: dict[str, Any], dag_id: str
) -> _TriggerRead:
    """Read every ``rmq_trigger(...)`` decorator of ``func`` and build its subscriptions.

    Each call is read by ``_read_rmq_trigger_call`` and built by
    ``build_subscriptions``, the same builder the decorator runs at import time.
    A problem is recorded, and the call contributes no subscription, when:

    - an argument cannot be read statically (each one with its reason);
    - ``build_subscriptions`` refuses the arguments — the problem carries its text;
    - the call is a second ``exchange=`` subscription on the same DAG
      (``has_exchange_conflict``), which the decorator refuses as well.

    Every subscription row returned carries ``dag_id`` and ``group_key`` (the DAG
    id for a cooldown subscription, ``None`` otherwise), ready to be stored.
    """
    subscriptions: list[dict] = []
    problems: list[str] = []
    calls: list[_TriggerCall] = []
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
        calls.append(_TriggerCall(
            decorator.lineno,
            decorator.end_lineno or decorator.lineno,
            dag_id,
            built,
        ))
        subscriptions.extend(
            {
                **sub,
                "dag_id": dag_id,
                "group_key": dag_id if sub.get("cooldown", 0) > 0 else None,
            }
            for sub in built
        )
    return _TriggerRead(subscriptions, problems, calls)


def _stray_trigger_mentions(tree: ast.Module) -> list[str]:
    """Describe every mention of ``rmq_trigger`` the scan cannot follow.

    The scan reads ``rmq_trigger`` only as the ``func`` of a call that is itself
    an element of a function's ``decorator_list`` — ``@rmq_trigger(...)`` or
    ``@<module>.rmq_trigger(...)``. Any other mention can reach the decorator by
    a path the scan does not see: a direct ``rmq_trigger(...)(dag)``, an import
    under another name, ``rt = decorators.rmq_trigger``,
    ``partial(rmq_trigger, ...)``, a bare ``@rmq_trigger`` without a call, a
    decorator on a class, a local definition of that name. An import of
    ``rmq_trigger`` under its own name is not a mention; where it imports the
    decorator from is ``_trigger_origin_problems``'s question.
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


def _other_decorator_problems(func: _FunctionNode) -> list[str]:
    """Report decorators of ``func`` below its first ``rmq_trigger(...)`` that the scan does not read.

    ``rmq_trigger`` takes the ``dag_id`` and attaches its subscriptions to what
    the decorators below it produce, and the scan reads only ``rmq_trigger(...)``
    and ``dag(...)`` calls there. Any other decorator in that place can change
    the DAG out of the scan's sight: one below ``@dag(...)`` without
    ``functools.wraps`` renames the function, which names a DAG declared without
    ``dag_id``; one between ``@rmq_trigger`` and ``@dag`` can rename the DAG or
    return something else. Decorators above the first ``rmq_trigger(...)`` wrap
    what it returns and are not reported.
    """
    first = next(
        (i for i, dec in enumerate(func.decorator_list) if _is_rmq_trigger_call(dec)), None
    )
    if first is None:
        return []
    return [
        f"function {func.name!r}, line {dec.lineno}: @"
        f"{ast.unparse(dec.func if isinstance(dec, ast.Call) else dec)} sits below "
        "@rmq_trigger(...) and can change the DAG it decorates — only "
        "@rmq_trigger(...) and @dag(...) may sit below the first @rmq_trigger(...); "
        "place other decorators above it"
        for dec in func.decorator_list[first + 1:]
        if not (_is_rmq_trigger_call(dec) or _is_dag_call(dec))
    ]


def _read_file_triggers(tree: ast.Module) -> _TriggerRead:
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
    - a decorator other than ``rmq_trigger(...)`` and ``dag(...)`` below the
      first ``@rmq_trigger`` of a function (``_other_decorator_problems``);
    - a mention of ``rmq_trigger`` the scan cannot follow
      (``_stray_trigger_mentions``);
    - a decorator call whose ``rmq_trigger`` is not bound to the provider's
      decorator (``_trigger_origin_problems``);
    - a ``@dag(...)`` call on an ``@rmq_trigger`` function whose ``dag`` is not
      bound to Airflow's decorator (``_dag_origin_problems``).

    ``calls`` lists every call read, problems or not, so the decorator's check
    can look its own call up.
    """
    origins = _NameOrigins(tree)
    problems = (
        _stray_trigger_mentions(tree)
        + _trigger_origin_problems(tree, origins)
        + _dag_origin_problems(tree, origins)
    )
    subscriptions: list[dict] = []
    calls: list[_TriggerCall] = []
    for func, constants in _iter_trigger_functions(tree):
        problems.extend(_decorator_order_problems(func))
        problems.extend(_other_decorator_problems(func))
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
        calls.extend(read.calls)
    if problems:
        return _TriggerRead([], problems, calls)
    return _TriggerRead(subscriptions, [], calls)


# ---------------------------------------------------------------------------
# Checking a decorator call against the scan
# ---------------------------------------------------------------------------

_MISSING = object()


class RMQTriggerNotStaticError(ValueError):
    """The watcher's scan would not see the subscriptions an ``@rmq_trigger`` call builds.

    Raised by the decorator while the DAG file is imported, so the DAG processor
    records an import error for the file instead of running DAGs the watcher
    would serve with other arguments, or not at all.
    """


def _is_scanned_below(path: str, folder: str) -> bool:
    """Tell whether ``path`` lies inside ``folder`` with no dot-leading component below it."""
    prefix = os.path.join(folder, "")
    if not path.startswith(prefix):
        return False
    below = path[len(prefix):]
    return not any(part.startswith(".") for part in below.split(os.sep))


def _watcher_scans(path: str, dags_folder: str) -> bool:
    """Tell whether the watcher's ``glob(DAGS_FOLDER/**/*.py)`` reads the file at ``path``.

    The file must end in ``.py``, lie inside ``dags_folder``, and no component of
    its path below the folder — the file name included — may start with a dot:
    ``glob`` skips hidden directories and files such as ``.x.py``.

    Both paths are compared through ``os.path.abspath`` first, which keeps the
    spelling ``glob`` lists the files under (it follows symlinked directories
    below the folder without resolving them). A module imported through another
    spelling of the same folder — a ``sys.path`` entry naming the folder's real
    path, or another symlink to it — is then compared through
    ``os.path.realpath`` of both, so the file the watcher reads is recognized
    whichever way the import reached it.
    """
    if not path.endswith(".py"):
        return False
    if _is_scanned_below(os.path.abspath(path), os.path.abspath(dags_folder)):
        return True
    return _is_scanned_below(os.path.realpath(path), os.path.realpath(dags_folder))


def _same_value(a: Any, b: Any) -> bool:
    """Compare two argument values the way the stored row would tell them apart.

    Scalars are equal only with the same exact type, so ``1``, ``1.0`` and
    ``True`` differ. Lists and tuples compare element by element. Dicts compare
    by key sets — a key matches only a key of the same type — and the values
    under matching keys; keys are never sorted, so a dict with keys of mixed
    types compares without error.
    """
    if type(a) is not type(b):
        return False
    if isinstance(a, (list, tuple)):
        return len(a) == len(b) and all(_same_value(x, y) for x, y in zip(a, b))
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
        keys = list(static_sub) + [k for k in runtime_sub if k not in static_sub]
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


#: The file's identity on disk: inode, modification time in ns, size.
_FileVersion = tuple[int, int, int]

#: The last analysis of each file the check read, by absolute path: the version
#: read and either the file's ``_read_file_triggers`` result or the exception
#: reading or parsing it raised. One entry per file, replaced when it changes.
_analyses: dict[str, tuple[_FileVersion, _TriggerRead | Exception]] = {}


def _analyse_file(path: str) -> _TriggerRead | Exception:
    """Return the scan's reading of the file at ``path``, read once per version of it.

    A DAG file with many ``@rmq_trigger`` calls asks the check about each of them
    while it is imported; every one after the first is answered from the
    analysis of the same version — the file's inode, modification time and size
    on disk — without reading or parsing the file again. A failure to read or
    parse is kept the same way and returned, not raised.

    The file is read from disk when the check runs, apart from the source the
    import compiled. A file replaced between the two — a git-sync swap during
    the import — is checked as it is now; the next parse of the file imports and
    checks the same version.

    :raises OSError: The file gives no ``stat``; nothing is kept.
    """
    full = os.path.abspath(path)
    stat = os.stat(full)
    version = (stat.st_ino, stat.st_mtime_ns, stat.st_size)
    cached = _analyses.get(full)
    if cached is not None and cached[0] == version:
        return cached[1]
    analysis: _TriggerRead | Exception
    try:
        analysis = _read_file_triggers(_read_and_parse(full))
    except Exception as exc:  # noqa: BLE001 - every failure is the file's verdict
        analysis = exc
    _analyses[full] = (version, analysis)
    return analysis


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
    2. the file must read and parse, and ``_read_file_triggers`` must find no
       problem anywhere in it — any problem would make the watcher refuse every
       subscription of the file (``_analyse_file``, once per version of the
       file);
    3. a call the scan read must span ``lineno``;
    4. the ``dag_id`` the scan resolved for that call must be ``dag_id``;
    5. the subscriptions the scan built from that call must match
       ``runtime_subs`` field by field (``_same_value``).

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
        analysis = _analyse_file(path)
    except OSError as exc:
        analysis = exc
    if isinstance(analysis, Exception):
        raise RMQTriggerNotStaticError(
            f"{where}: the RMQ watcher cannot read this file "
            f"({type(analysis).__name__}: {analysis}), so it never registers its "
            "subscriptions."
        ) from analysis

    if analysis.problems:
        listed = "\n".join(f"- {problem}" for problem in analysis.problems)
        raise RMQTriggerNotStaticError(
            f"{where}: the RMQ watcher cannot read every @rmq_trigger of this file "
            "statically and refuses all of its subscriptions:\n"
            f"{listed}\n"
            "Arguments and dag_id must be literals or constants assigned once at "
            "the top level of this file above the decorated function, "
            "rmq_trigger must be imported from airflow_provider_rmq.watcher.decorators "
            "and dag from airflow.decorators."
        )

    found = next(
        (c for c in analysis.calls if c.first_line <= lineno <= c.last_line), None
    )
    if found is None:
        raise RMQTriggerNotStaticError(
            f"{where}: rmq_trigger is called through a name the RMQ watcher does "
            "not recognize. The watcher sees only the form @rmq_trigger(...) "
            "above @dag(...) on a function of the DAG file; import rmq_trigger "
            "from airflow_provider_rmq.watcher.decorators under its own name."
        )

    if found.dag_id != dag_id:
        raise RMQTriggerNotStaticError(
            f"{where}: the RMQ watcher reads dag_id={found.dag_id!r} for this "
            f"call, but it decorates the DAG {dag_id!r}."
        )

    differences = _subscription_differences(found.subscriptions, runtime_subs)
    if differences:
        listed = "\n".join(f"- {difference}" for difference in differences)
        raise RMQTriggerNotStaticError(
            f"{where}: the RMQ watcher reads other arguments from the file than "
            f"the decorator receives:\n{listed}\n"
            "A constant the call uses must keep its value until the DAG is built."
        )
