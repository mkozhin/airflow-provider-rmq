"""Static reading of ``@rmq_trigger`` subscriptions from a DAG file's source.

The watcher finds subscriptions by parsing DAG files with :mod:`ast` and never
executes them. Everything that reads a parsed file — module-scope traversal,
module-level constant collection, ``dag_id`` resolution and the reading of
``rmq_trigger(...)`` calls — lives here.

The module depends only on the standard library and ``subscription_builder``, so
it can be imported inside the DAG processor without pulling in the watcher's
runtime modules (``listener``, ``consumer``, ``models``, ``runner``) and their
third-party dependencies.
"""

from __future__ import annotations

import ast
import enum
import logging
from collections.abc import Iterator

from airflow_provider_rmq.watcher.subscription_builder import build_subscriptions

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level constant collection (for resolving dag_id=NAME references)
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


def _collect_module_constants(
    tree: ast.Module,
) -> tuple[dict[str, tuple[str, int]], set[int]]:
    """Collect simple top-level ``NAME = "literal string"`` assignments
    (plus annotated ``NAME: str = "literal"``), each paired with its line
    number so callers can enforce "only usable before this point in the
    file" — see ``_resolve_function_dag_id``.

    Returns ``(constants, module_scope_function_ids)``. The second element
    is ``id()`` of every ``FunctionDef``/``AsyncFunctionDef`` whose
    decorators execute in module scope, collected from the SAME
    ``_module_scope_nodes(tree)`` pass used to build ``constants`` —
    ``_resolve_function_dag_id`` needs this set to decide whether a nested
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

    Refusing to resolve an ambiguous name is safe (the caller skips
    registration and logs a WARNING); guessing wrong is not — same principle
    as ``_UNRESOLVED_DAG_ID`` below.
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

    constants: dict[str, tuple[str, int]] = {}
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
        if isinstance(value, str):
            constants[name] = (value, node.lineno)
    return constants, module_scope_function_ids


# Sentinel distinguishing "dag_id= is set but cannot be statically resolved"
# from "dag_id= was never set at all" (see _extract_dag_id_from_decorators).
# An enum member (rather than a bare object()) so mypy narrows
# `str | _DagIdSentinel | None` correctly after `is` comparisons.
class _DagIdSentinel(enum.Enum):
    UNRESOLVED = enum.auto()


_UNRESOLVED_DAG_ID = _DagIdSentinel.UNRESOLVED


def _extract_dag_id_from_decorators(
    decorators: list[ast.expr], constants: dict[str, str] | None = None
) -> str | _DagIdSentinel | None:
    """Resolve the ``dag_id`` a recognized ``@dag(...)`` call among ``decorators`` would produce.

    Three-way return contract:

    - ``str`` — the resolved dag_id, either a string literal (``dag_id='x'``)
      or a module-level string constant referenced by name and found in
      ``constants`` (``dag_id=NAME`` with ``constants={"NAME": "x"}``).
    - ``None`` — a recognized ``@dag(...)`` call was found, and EITHER no
      ``dag_id=`` keyword was set at all (Airflow's own default behaviour is
      to fall back to the decorated function's name in this case, so it is
      safe for the caller to do the same), OR ``dag_id=`` resolved to a
      falsy value (``""``, ``None``, ``False``, ``0``, and generally
      anything falsy ``ast.literal_eval`` can produce — this mirrors
      Airflow's own ``dag_id or f.__name__`` in ``airflow/models/dag.py``).
      Falling back to the function name is only ever safe for this ``None``
      case.
    - ``_UNRESOLVED_DAG_ID`` — ``dag_id=`` is set to something that cannot
      be statically proven (a non-literal expression whose name is not in
      ``constants``, or a truthy non-string literal such as ``dag_id=123``),
      OR no recognized ``@dag(...)`` call exists among ``decorators`` at all
      (e.g. an aliased import — ``from airflow.decorators import dag as
      airflow_dag`` — is not matched by the ``is_dag`` predicate below).
      Guessing the function name here would silently register a
      subscription under a ``dag_id`` that does not correspond to any real
      Airflow DAG — the caller must skip registration and log a warning
      instead (see ``_resolve_function_dag_id``).

    **Deliberate falsy asymmetry**: any falsy value that was actually
    resolved (a literal or a module constant — both always ``str`` here)
    returns ``None`` and falls back to the function name; a name that
    could not be resolved at all stays ``_UNRESOLVED_DAG_ID``. See the two
    ``_resolve()`` branches below for exactly why this makes
    ``DAG_ID = None`` behave differently from ``DAG_ID = ""``.

    No logging happens inside this function — the caller decides whether an
    ``_UNRESOLVED_DAG_ID`` deserves a WARNING, because only it knows the
    file path and whether ``@rmq_trigger`` is actually present (an
    unresolvable ``dag_id=`` on a DAG with no ``@rmq_trigger`` at all is not
    this plugin's concern and must stay silent).

    ``constants`` is a flat ``dict[str, str]`` of name → value. The caller
    (``_resolve_function_dag_id``) is responsible for turning
    ``_collect_module_constants``'s
    ``dict[str, tuple[str, int]]`` output into this shape — filtering out
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
                # Same falsy rule as the literal branch below — a module
                # constant IS a str (per _collect_module_constants), so an
                # empty-string constant must fall back to the function name
                # too, not be returned verbatim.
                resolved = constants[value_node.id]
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
        if not isinstance(dec, ast.Call):
            continue
        func = dec.func
        # Structurally identical to _is_rmq_trigger_call, but kept inline: this
        # check has exactly one call site (here), unlike _is_rmq_trigger_call
        # which is shared between _parse_rmq_trigger_decorator and
        # _resolve_function_dag_id — extracting a helper for a single
        # use site would add indirection without enabling reuse.
        is_dag = (
            (isinstance(func, ast.Name) and func.id == "dag")
            or (isinstance(func, ast.Attribute) and func.attr == "dag")
        )
        if not is_dag:
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


def _is_rmq_trigger_call(node: ast.expr) -> bool:
    """Return True if node is a call to rmq_trigger(...).

    Handles both bare name ``rmq_trigger(...)`` and attribute access
    ``decorators.rmq_trigger(...)``. Shared by ``_parse_rmq_trigger_decorator`` and
    ``_resolve_function_dag_id``, which uses it to decide whether an unresolvable
    dag_id= deserves a warning.
    """
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return (
        (isinstance(func, ast.Name) and func.id == "rmq_trigger")
        or (isinstance(func, ast.Attribute) and func.attr == "rmq_trigger")
    )


def _scanned_queue_label(kwargs: dict) -> str:
    """Name the queue an ``@rmq_trigger`` call is about, for a diagnostic line.

    The label is built from whatever the scan managed to read: ``queue=``,
    ``queues=`` or ``exchange=``. A call whose own queue argument is the
    unreadable one gets ``"<unresolved>"`` — the warning still names the DAG and
    the parameter, which is what points at the decorator.
    """
    if "queue" in kwargs:
        return str(kwargs["queue"])
    if isinstance(kwargs.get("queues"), (list, tuple)):
        return ",".join(str(q) for q in kwargs["queues"])
    if "exchange" in kwargs:
        return f"exchange:{kwargs['exchange']}"
    return "<unresolved>"


#: What the build made of an ``@rmq_trigger`` call whose arguments the scan could not
#: read fully, and the sentence each outcome puts into the warning. What the operator
#: has to do differs per outcome: a registered subscription runs with a default the DAG
#: does not ask for, while the other two do not run at all — one because the call named
#: a queue the builder refused, the other because no queue selector survived the scan.
_UNREADABLE_KWARG_OUTCOMES = {
    "registered": (
        "the subscription is registered with that parameter's default instead "
        "of what the decorator says"
    ),
    # A ``**`` unpacking names no parameter, so the registered sentence cannot point
    # at one: it says what happens to every argument the dict carries instead.
    "registered_unpacking": (
        "the subscription is registered with the default for every argument the "
        "dict carries, and only the arguments written out beside it take effect"
    ),
    "refused": (
        "the arguments the scan did read do not build a subscription and no "
        "subscription is registered for it — see the line below for what the "
        "build refused"
    ),
    "no_selector": (
        "the call is left without a queue selector and no subscription is "
        "registered for it"
    ),
}


def _warn_unreadable_kwarg(
    dag_id: str, kwargs: dict, name: str | None, *, outcome: str
) -> None:
    """Report an ``@rmq_trigger`` argument the AST scan could not read.

    :param dag_id: DAG the decorator sits on.
    :param kwargs: Arguments the scan did read, for naming the queue.
    :param name: Parameter that could not be read, or ``None`` for a ``**`` unpacking
        that hides every argument of the call.
    :param outcome: Key of :data:`_UNREADABLE_KWARG_OUTCOMES` — what the build made of
        the call. The caller establishes it by attempting the build first, so a call
        that names a queue but is refused by ``build_subscriptions`` is reported as the
        unregistered one it is, and the refusal itself is logged on its own line there.

    The scan never executes the DAG file, so a value the decorator computes — a
    module constant, an import, an f-string — is invisible to it.

    A ``**`` unpacking hides only its own dict: literals written out beside it are
    read normally, so the line says what the scan could not reach rather than
    claiming the call was unreadable whole.
    """
    if name is not None:
        what = f"{name!r} is not a literal"
        sentence = _UNREADABLE_KWARG_OUTCOMES[outcome]
    else:
        what = (
            "the decorator unpacks a dict (**kwargs), so the scan cannot read the "
            "arguments it carries"
            if kwargs
            else "the decorator unpacks a dict (**kwargs), so the scan reads no "
            "parameter from this call at all"
        )
        sentence = _UNREADABLE_KWARG_OUTCOMES[
            "registered_unpacking" if outcome == "registered" else outcome
        ]
    log.warning(
        "rmq_trigger: dag_id=%s queue=%s — %s, and the DAG file scan does not "
        "execute the file, so %s. Write the arguments out as literals in the "
        "decorator, or set the values on the Subscriptions page",
        dag_id, _scanned_queue_label(kwargs), what, sentence,
    )


def _parse_rmq_trigger_decorator(node: ast.expr, dag_id: str) -> list[dict]:
    """Return list of subscription dicts if node is an rmq_trigger(...) call, else [].

    Handles both bare name ``rmq_trigger(...)`` and attribute access
    ``decorators.rmq_trigger(...)``.  Only literal argument values are extracted:
    a non-literal expression — the positional queue name included — and a ``**``
    unpacking that hides every argument are reported by ``_warn_unreadable_kwarg``
    and leave the parameter at its default.  A call that registers nothing —
    because ``queue``/``queues``/``exchange`` was the unreadable one, or because
    the arguments that were read are refused by ``build_subscriptions`` — has the
    warning say so, and the subscription is created via the UI instead.

    Validation/construction of the subscription dict is delegated to
    ``build_subscriptions()`` (``subscription_builder.py``) — this function is
    only responsible for the AST-specific part: extracting literal kwarg values
    via ``ast.literal_eval``. Any ``ValueError`` raised by ``build_subscriptions``
    (mutex violation, empty lists, dots in ids/status, negative cooldown, ...)
    is logged as a WARNING and the subscription is skipped — the same graceful
    degradation pattern already used for non-literal values.

    Returns a list:
    - empty list if node is not an rmq_trigger call, required args are missing,
      or validation failed
    - one dict for ``queue=``/``exchange=`` (single subscription)
    - N dicts for ``queues=[...]`` (one per queue in the list)

    ``group_key`` is NOT set here — the caller,
    ``RMQWatcherListener._extract_subscriptions_from_file`` in ``listener.py``,
    sets it together with ``dag_id`` on every subscription it keeps.
    """
    if not isinstance(node, ast.Call) or not _is_rmq_trigger_call(node):
        return []

    kwargs: dict = {}
    unreadable: list[str | None] = []
    # positional: rmq_trigger("queue_name")
    if node.args:
        val = node.args[0]
        if isinstance(val, ast.Constant) and isinstance(val.value, str):
            kwargs["queue"] = val.value
        else:
            unreadable.append("queue")
    # keyword arguments
    for kw in node.keywords:
        if kw.arg is None:
            # `@rmq_trigger(**opts)` — the dict is built at import time, so the
            # scan cannot read the arguments it carries. Literals written out
            # beside the unpacking are still read by the loop below.
            unreadable.append(None)
            continue
        if kw.arg not in _RMQ_TRIGGER_KWARGS:
            continue
        try:
            value = ast.literal_eval(kw.value)
        except (ValueError, TypeError):
            unreadable.append(kw.arg)
            continue
        kwargs[kw.arg] = value

    # The outcome of an unreadable argument is what the build made of the rest, so
    # the build goes first and the warnings report the rows it actually produced.
    subscriptions: list[dict] = []
    refusal: str | None = None
    if "queue" in kwargs or "queues" in kwargs or "exchange" in kwargs:
        try:
            subscriptions = build_subscriptions(dag_id=dag_id, **kwargs)
        except ValueError as exc:
            refusal = str(exc)

    if subscriptions:
        outcome = "registered"
    elif refusal is not None:
        outcome = "refused"
    else:
        outcome = "no_selector"
    for name in unreadable:
        _warn_unreadable_kwarg(dag_id, kwargs, name, outcome=outcome)

    if refusal is not None:
        log.warning(
            "rmq_trigger: skipping invalid subscription for dag_id=%s: %s", dag_id, refusal
        )
    return subscriptions


def _resolve_function_dag_id(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    module_constants: dict[str, tuple[str, int]],
    module_scope_function_ids: set[int],
    path: str,
) -> str | None:
    """Resolve the dag_id to use for one decorated function, or None to skip it.

    Called by ``RMQWatcherListener._extract_subscriptions_from_file`` in
    ``listener.py`` for every function of the file, and combines three steps:
    (1) scope-eligibility filtering — module-level constants are only
    handed to functions whose decorators actually execute in module scope
    (nested functions get an empty map, since a local binding of the same name
    could shadow the module-level one), with a
    use-before-definition line filter for the eligible case; (2) dag_id
    resolution via ``_extract_dag_id_from_decorators``'s three-way contract;
    (3) the warn-if-``@rmq_trigger``-is-present policy for an unresolvable
    dag_id=.

    Returns ``None`` when this function's subscriptions must not be
    registered (dag_id unresolvable); otherwise returns the dag_id string to
    use, already carrying the "no dag_id= at all -> function name" fallback.
    """
    if id(node) not in module_scope_function_ids:
        usable_constants: dict[str, str] = {}  # nested: refuse to resolve
    else:
        # Position filter: a constant assigned AFTER this function's own
        # line must not be used to resolve its dag_id= — real Python
        # would NameError at import time in that case (use-before-
        # definition), so statically resolving it would be a false
        # positive.
        usable_constants = {
            name: value
            for name, (value, lineno) in module_constants.items()
            if lineno < node.lineno
        }
    dag_id = _extract_dag_id_from_decorators(node.decorator_list, usable_constants)
    if dag_id is _UNRESOLVED_DAG_ID:
        if any(_is_rmq_trigger_call(d) for d in node.decorator_list):
            log.warning(
                "rmq_trigger: cannot statically resolve dag_id for function %r "
                "in %s — dag_id= is not a string literal or a simple "
                "module-level string constant defined earlier in the file. "
                "Subscription NOT registered from dag_file; for queue=/queues= "
                "subscriptions, create it manually via the RMQ Watcher UI "
                "(source='ui') instead — the UI form does not support "
                "exchange=/routing-key subscriptions, so for those dag_id= "
                "must be made statically resolvable.",
                node.name, path,
            )
        return None
    if dag_id is None:
        return node.name
    return dag_id
