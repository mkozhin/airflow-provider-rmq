from __future__ import annotations

import ast
import asyncio
import enum
import glob
import logging
import os
import threading
import time
import traceback
from collections.abc import Iterator
from concurrent.futures import Future
from typing import Any

from airflow.listeners import hookimpl

from airflow_provider_rmq.utils.amqp import call_with_timeout
from airflow_provider_rmq.utils.executor import BoundedExecutor
from airflow_provider_rmq.utils.metrics import incr as _incr
from airflow_provider_rmq.watcher.consumer import RMQConsumerManager
from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    WatcherSession,
    ensure_table_exists,
    get_enabled_subscriptions,
    is_schema_ready,
    upsert_subscription,
)
from airflow_provider_rmq.watcher.subscription_builder import (
    build_subscriptions,
    has_exchange_conflict,
)
from airflow_provider_rmq.watcher.tunables import (
    CYCLE_TIMEOUT_VAR,
    DEFAULT_RECONCILE_INTERVAL,
    RECONCILE_INTERVAL_VAR,
)

log = logging.getLogger(__name__)

#: A cycle may take this many reconcile intervals, but never less than
#: ``_MIN_CYCLE_TIMEOUT`` seconds. The budget is generous on purpose: hitting it
#: cancels every consumer task and pauses consumption on every conn_id for the
#: 30s loop-restart delay, while the per-call AMQP timeouts catch a stuck network
#: operation far earlier and only for the subscription that owns it.
_CYCLE_TIMEOUT_FACTOR = 3
_MIN_CYCLE_TIMEOUT = 300

#: Seconds allowed for reading the tunables out of the Airflow Variables table.
_VARIABLE_TIMEOUT = 15.0

#: Seconds allowed for one schema-migration attempt.
_MIGRATION_TIMEOUT = 30.0

#: Seconds allowed for one blocking step of the cycle — the DAG-file scan and the two
#: subscription queries. All three go to the cycle pool, and a call that never returns
#: holds its worker until the operating system gives up on the socket, so without a
#: bound of their own they would spend the whole cycle budget and cost the loop, every
#: consumer task and every connection with it. Three steps at this bound still fit
#: inside the smallest cycle budget with room for the reconcile that follows them.
_STEP_TIMEOUT = 60.0

#: Cycles to skip after a failed migration attempt: one, then doubling up to the
#: cap (an hour of cycles at the default interval).
_MIGRATION_BACKOFF_START = 1
_MIGRATION_BACKOFF_CAP = 60

#: Seconds allowed for the manager to stop before the loop is torn down anyway.
_STOP_TIMEOUT = 30.0

#: Seconds ``before_stopping`` waits for the watcher thread to leave. The woken loop
#: only has to finish the current cycle step, and the scheduler's own shutdown must
#: not stall behind a watcher that refuses to.
_JOIN_TIMEOUT = 5.0

#: Workers of the cycle pool: the reconcile loop's own blocking calls (schema
#: migration, Variable reads, DAG-file scan, subscription queries).
_CYCLE_POOL_WORKERS = 4

#: Workers of the consumer pool, handed to the manager. Kept apart from the cycle
#: pool so that a database that stopped answering degrades consumption without
#: also starving the cycle that writes statuses and rebuilds connections.
_CONSUMER_POOL_WORKERS = 32


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
    file" — see the wiring below.

    Returns ``(constants, module_scope_function_ids)``. The second element
    is ``id()`` of every ``FunctionDef``/``AsyncFunctionDef`` whose
    decorators execute in module scope, collected from the SAME
    ``_module_scope_nodes(tree)`` pass used to build ``constants`` — the
    caller (``_extract_subscriptions_from_file``) needs this set to decide
    whether a nested function may use ``constants`` at all, and reusing this
    pass avoids walking the tree a second time just for that.

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
      instead (see ``_extract_subscriptions_from_file``).

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
    is responsible for turning ``_collect_module_constants``'s
    ``dict[str, tuple[str, int]]`` output into this shape — filtering out
    constants assigned after the decorated function's own line (use-before-
    definition) and passing an empty dict for nested functions, whose
    module-level constants may be shadowed by a local binding of the same
    name (see ``_extract_subscriptions_from_file``).

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
        # _extract_subscriptions_from_file — extracting a helper for a single
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
)


def _is_rmq_trigger_call(node: ast.expr) -> bool:
    """Return True if node is a call to rmq_trigger(...).

    Handles both bare name ``rmq_trigger(...)`` and attribute access
    ``decorators.rmq_trigger(...)``. Extracted from ``_parse_rmq_trigger_decorator``
    so callers elsewhere (e.g. deciding whether an unresolvable dag_id= deserves a
    warning) can reuse the same check without re-implementing it.
    """
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return (
        (isinstance(func, ast.Name) and func.id == "rmq_trigger")
        or (isinstance(func, ast.Attribute) and func.attr == "rmq_trigger")
    )


def _parse_rmq_trigger_decorator(node: ast.expr, dag_id: str) -> list[dict]:
    """Return list of subscription dicts if node is an rmq_trigger(...) call, else [].

    Handles both bare name ``rmq_trigger(...)`` and attribute access
    ``decorators.rmq_trigger(...)``.  Only literal argument values are extracted;
    non-literal expressions are skipped (subscription won't be registered from
    AST scan — user should create it via the UI instead).

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

    ``group_key`` is NOT set here — it is set in _extract_subscriptions_from_file
    where dag_id is known.
    """
    if not isinstance(node, ast.Call) or not _is_rmq_trigger_call(node):
        return []

    kwargs: dict = {}
    # positional: rmq_trigger("queue_name")
    if node.args:
        val = node.args[0]
        if isinstance(val, ast.Constant) and isinstance(val.value, str):
            kwargs["queue"] = val.value
    # keyword arguments
    for kw in node.keywords:
        if kw.arg not in _RMQ_TRIGGER_KWARGS:
            continue
        try:
            value = ast.literal_eval(kw.value)
        except (ValueError, TypeError):
            continue
        kwargs[kw.arg] = value

    if "queue" not in kwargs and "queues" not in kwargs and "exchange" not in kwargs:
        return []

    try:
        return build_subscriptions(dag_id=dag_id, **kwargs)
    except ValueError as exc:
        log.warning("rmq_trigger: skipping invalid subscription for dag_id=%s: %s", dag_id, exc)
        return []


def _resolve_function_dag_id(
    node: ast.FunctionDef | ast.AsyncFunctionDef,
    module_constants: dict[str, tuple[str, int]],
    module_scope_function_ids: set[int],
    path: str,
) -> str | None:
    """Resolve the dag_id to use for one decorated function, or None to skip it.

    Combines three steps that ``_extract_subscriptions_from_file`` previously
    inlined: (1) scope-eligibility filtering — module-level constants are only
    handed to functions whose decorators actually execute in module scope
    (nested functions get an empty map, since a local binding of the same name
    could shadow the module-level one — see that method's docstring), with a
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


def _read_settings() -> tuple[int | None, float | None]:
    """Read the watcher tunables from Airflow Variables. Blocking: hits the database.

    Returns ``(reconcile_interval, cycle_timeout)``; either is ``None`` when the
    Variable is unset or holds something that is not a positive number, which the
    caller reads as "keep the built-in default".
    """
    from airflow.models import Variable

    def _positive(name: str, cast: Any) -> Any:
        raw = Variable.get(name, default_var=None)
        if raw is None:
            return None
        try:
            value = cast(raw)
        except (TypeError, ValueError):
            log.warning("RMQ Watcher: Variable %s=%r is not a number — ignoring", name, raw)
            return None
        if value <= 0:
            log.warning("RMQ Watcher: Variable %s=%r must be positive — ignoring", name, raw)
            return None
        return value

    return _positive(RECONCILE_INTERVAL_VAR, int), _positive(CYCLE_TIMEOUT_VAR, float)


def _read_active_subs(exchange_meta: dict) -> list[dict]:
    """Read the enabled subscriptions, merging the in-memory exchange metadata onto them.

    A blocking SQLAlchemy query, so it runs in the cycle pool rather than on the loop
    thread; the exchange/routing_keys pairs are keyed the same way as the unique
    constraint on ``RMQSubscription`` and are looked up here, where the rows are.
    """
    with WatcherSession() as session:
        active_subs = []
        for sub in get_enabled_subscriptions(session):
            entry = {
                "id": sub.id,
                "dag_id": sub.dag_id,
                "queue_name": sub.queue_name,
                "conn_id": sub.conn_id,
                "filter_data": sub.filter_data or {},
                "cooldown": sub.cooldown or 0,
            }
            meta = exchange_meta.get((sub.dag_id, sub.queue_name, sub.conn_id))
            if meta is not None:
                entry.update(meta)
            active_subs.append(entry)
    return active_subs


class _StepInFlight(Exception):
    """The previous attempt at a cycle step is still occupying a worker of the pool.

    Raised instead of handing the pool a second copy of a call that has not come back:
    the cycle pool has four workers, and one stuck attempt per cycle saturates it in a
    handful of cycles — after which even the pure-filesystem scan cannot start and the
    liveness check stops running altogether.
    """


class RMQWatcherListener:
    """Airflow Listener that runs a background RabbitMQ consumer loop inside the Scheduler process.

    Lifecycle:
    - ``on_starting`` fires when the Scheduler process starts; we spawn a daemon thread
      with its own asyncio event loop.
    - The loop reconciles subscriptions from DAG files (mtime-based scan) and the DB
      every ``reconcile_interval`` seconds, then delegates to ``RMQConsumerManager``.
    - ``before_stopping`` sets the stop event, wakes the loop out of its wait and
      joins the thread briefly, so shutdown does not wait out a reconcile interval.
    """

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event: threading.Event | None = None
        self._manager: RMQConsumerManager | None = None
        # mtime-based incremental scan state (lives in the daemon thread only)
        self._last_mtimes: dict[str, float] = {}   # filepath → mtime
        self._cached_subs: dict[str, list[dict]] = {}  # filepath → list[sub dict]
        # Both pools are built here, in whatever thread constructs the listener, and
        # outlive every event loop the watcher thread creates.
        self._cycle_pool = BoundedExecutor("rmq-watcher-cycle", _CYCLE_POOL_WORKERS)
        self._consumer_pool = BoundedExecutor("rmq-watcher-consumer", _CONSUMER_POOL_WORKERS)
        # Tunables, cached between refreshes so that a database outage cannot stall
        # the loop at a point the cycle watchdog does not cover.
        self._reconcile_interval = DEFAULT_RECONCILE_INTERVAL
        self._cycle_timeout_override: float | None = None
        self._settings_attempt: Future | None = None
        # Blocking cycle steps that are still in a worker, keyed by step name, so the
        # next cycle asks whether the previous attempt returned instead of adding one.
        self._step_attempts: dict[str, Future] = {}
        # Schema migration retry state
        self._migration_attempt: Future | None = None
        self._migration_backoff = 0          # cycles to skip after the last failure
        self._migration_skip_cycles = 0
        #: Step the current cycle is on, reported when the cycle runs out of budget.
        self._phase = "idle"
        # Loop and wake-up event of the currently running cycle, published as one
        # tuple by every ``_main`` so that ``before_stopping`` can never pair the live
        # loop with the previous loop's event.
        self._waker: tuple[asyncio.AbstractEventLoop, asyncio.Event] | None = None

    # ------------------------------------------------------------------
    # Listener API
    # ------------------------------------------------------------------

    @hookimpl
    def on_starting(self, component: Any) -> None:
        name = type(component).__name__
        job_type = getattr(component, 'job_type', '') or ''
        # In Airflow 2.9+, on_starting fires inside Job.__init__() before super().__init__()
        # sets job_type, so job_type is always None here. The class is renamed from
        # SchedulerJobRunner to Job (ORM model). Detect scheduler via call-stack:
        # scheduler_command.py is present for the scheduler, triggerer_command.py for the triggerer.
        stack_files = [frame.filename for frame in traceback.extract_stack()]
        is_scheduler_stack = any('scheduler_command' in f for f in stack_files)
        is_scheduler = "Scheduler" in name or "Scheduler" in job_type or is_scheduler_stack
        log.info(
            "RMQWatcherListener.on_starting: component=%s (job_type=%s, is_scheduler=%s)%s",
            name, job_type, is_scheduler,
            "" if is_scheduler
            else " — watcher not started: only the scheduler process runs it",
        )
        if is_scheduler:
            self._start()

    @hookimpl
    def before_stopping(self, component: Any) -> None:
        if self._stop_event is not None:
            self._stop_event.set()
        self._wake_loop()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=_JOIN_TIMEOUT)
            if thread.is_alive():
                log.warning(
                    "RMQ Watcher thread still running %.0fs after the stop signal — "
                    "leaving it to the process exit", _JOIN_TIMEOUT,
                )

    def _wake_loop(self) -> None:
        """Nudge the running cycle so it sees the stop event without waiting out the
        reconcile interval.

        The threading event is the authoritative signal; this only shortens the wait.
        An :class:`asyncio.Event` may be touched from its own loop alone, and that loop
        may already be closing — hence the guard and the swallowed ``RuntimeError``.
        """
        waker = self._waker
        if waker is None:
            return
        loop, wakeup = waker
        if loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(wakeup.set)
        except RuntimeError:
            pass

    # ------------------------------------------------------------------
    # Thread / event-loop bootstrap
    # ------------------------------------------------------------------

    def _start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            if self._stop_event is None or not self._stop_event.is_set():
                log.warning("RMQ Watcher thread already running — ignoring duplicate on_starting")
                return
            # Previous lifecycle is shutting down — wait briefly then start fresh
            log.info("RMQ Watcher: waiting for previous thread to stop...")
            self._thread.join(timeout=10)
        self._stop_event = threading.Event()
        self._thread = threading.Thread(
            target=self._run_loop,
            name="rmq-watcher",
            daemon=True,
        )
        self._thread.start()
        log.info(
            "RMQ Watcher thread started with the default reconcile interval of %ss and "
            "a cycle budget of %.0fs; the first cycle reads %s and %s and logs any "
            "override it finds",
            self._reconcile_interval, self._cycle_timeout(),
            RECONCILE_INTERVAL_VAR, CYCLE_TIMEOUT_VAR,
        )

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._main())
            except Exception:
                log.exception("RMQ Watcher loop crashed — restarting in 30s")
            finally:
                # Both thread pools belong to the listener, not to the loop, so the
                # next loop reuses them. loop.shutdown_default_executor() is
                # deliberately not called: on Python 3.10/3.11 it ends in a
                # thread.join() with no timeout that wait_for cannot interrupt, so a
                # single call stuck on the database would freeze this thread for good.
                # The pools' own workers are joined by the interpreter's exit hook
                # instead, where a stuck call delays process exit rather than the
                # watcher thread — see BoundedExecutor.
                loop.close()
            if not self._stop_event.is_set():
                self._stop_event.wait(timeout=30)
        log.info("RMQ Watcher loop stopped")

    async def _main(self) -> None:
        wakeup = asyncio.Event()
        self._waker = (asyncio.get_running_loop(), wakeup)
        self._manager = RMQConsumerManager(
            executor=self._consumer_pool, cycle_executor=self._cycle_pool
        )
        await self._manager.start()
        try:
            while not self._stop_event.is_set():
                await self._refresh_settings()
                budget = self._cycle_timeout()
                started = time.monotonic()
                try:
                    await call_with_timeout(self._run_cycle(), budget)
                except asyncio.TimeoutError:
                    log.error(
                        "RMQ Watcher cycle exceeded its %.0fs budget in phase %r after "
                        "%.0fs — recreating the event loop",
                        budget, self._phase, time.monotonic() - started,
                    )
                    _incr("rmq_watcher.cycle_timeout")
                    raise
                await self._wait_for_next_cycle()
        finally:
            await self._stop_manager()

    async def _wait_for_next_cycle(self) -> None:
        """Wait one reconcile interval, returning at once once the watcher is stopping.

        The wake-up event is cleared afterwards, so it signals "somebody nudged the
        loop just now" rather than latching on the first nudge for the rest of the
        process; whether the watcher keeps going is decided by the stop event alone.
        """
        if self._stop_event.is_set():
            return
        waker = self._waker
        if waker is None:
            return
        wakeup = waker[1]
        try:
            await call_with_timeout(wakeup.wait(), self._reconcile_interval)
        except asyncio.TimeoutError:
            pass
        finally:
            wakeup.clear()

    async def _run_cycle(self) -> None:
        """Run one reconciliation pass: migrate, scan, sync, read subscriptions, reconcile.

        Errors are logged here rather than around the call site, so that the only
        thing the caller's timeout can observe is the timeout itself.
        ``asyncio.TimeoutError`` is a subclass of ``Exception`` (an alias of the
        builtin ``TimeoutError`` from Python 3.11 on), so an ``except Exception``
        wrapped around this call instead of placed inside it would swallow the cycle
        watchdog whole and leave the layer a no-op.
        """
        try:
            self._phase = "migrate"
            await self._ensure_schema()

            self._phase = "scan"
            scanned = await self._cycle_step("scan", self._scan_subscriptions)

            self._phase = "sync"
            await self._cycle_step("sync", self._sync_to_db, scanned)

            # Exchange/routing_keys metadata is never persisted to the DB (see
            # plan Technical Details → "Почему миграция БД не нужна") — it only
            # lives in-memory in the AST scan cache, re-derived every cycle. Build
            # a lookup keyed the same way as the unique constraint on
            # RMQSubscription so it can be merged back onto DB rows below.
            exchange_meta = {
                (s["dag_id"], s["queue_name"], s.get("conn_id", "rmq_default")): {
                    "exchange": s["exchange"],
                    "routing_keys": s["routing_keys"],
                }
                for s in scanned
                if "exchange" in s
            }

            self._phase = "read subs"
            active_subs = await self._cycle_step("read subs", _read_active_subs, exchange_meta)

            self._phase = "reconcile"
            await self._manager.reconcile(active_subs)
        except _StepInFlight as exc:
            log.warning("RMQ Watcher: skipping this reconciliation cycle — %s", exc)
        except Exception:
            log.exception("Error in RMQ Watcher reconciliation cycle")

    async def _cycle_step(self, name: str, fn: Any, *args: Any) -> Any:
        """Run one blocking step of the cycle in the cycle pool, bounded and unrepeated.

        :param name: Step name, used for the in-flight bookkeeping and the log line.
        :param fn: Blocking callable to run in the cycle pool.
        :returns: Whatever ``fn`` returned.
        :raises _StepInFlight: The previous attempt at this step has not returned.
        :raises asyncio.TimeoutError: The attempt outlived :data:`_STEP_TIMEOUT`.

        Either failure ends the cycle: the steps feed each other, and a reconcile run on
        a subscription list that could not be read would cancel every consumer of a
        subscription the query simply did not return.

        The bound buys back the coroutine, not the worker — a running thread cannot be
        interrupted — hence the second half: one attempt of a step in flight at a time.
        Resubmitting every cycle would fill the four-worker pool with stuck copies of the
        same call, and the steps that need no database would stop running with it.
        """
        attempt = self._step_attempts.get(name)
        if attempt is not None and not attempt.done():
            _incr("rmq_watcher.cycle_step_in_flight")
            raise _StepInFlight(
                f"the {name!r} step of an earlier cycle is still running in the "
                f"{self._cycle_pool.name!r} pool "
                f"({self._cycle_pool.in_flight}/{self._cycle_pool.max_workers} "
                f"workers busy)"
            )
        attempt = self._cycle_pool.submit(fn, *args)
        self._step_attempts[name] = attempt
        try:
            return await call_with_timeout(asyncio.wrap_future(attempt), _STEP_TIMEOUT)
        except asyncio.TimeoutError:
            _incr("rmq_watcher.cycle_step_timeout")
            log.warning(
                "RMQ Watcher: the %r step did not finish within %ss — ending this cycle; "
                "its worker stays busy until the call itself returns",
                name, _STEP_TIMEOUT,
            )
            raise

    async def _stop_manager(self) -> None:
        """Stop the manager, giving up after ``_STOP_TIMEOUT`` seconds.

        Reached while the loop is already being torn down, so a manager that cannot
        finish must not keep the thread from starting a fresh loop.
        """
        try:
            await call_with_timeout(self._manager.stop(), _STOP_TIMEOUT)
        except Exception:
            log.warning(
                "RMQ Watcher: manager.stop() did not finish within %ss — continuing "
                "with loop teardown", _STOP_TIMEOUT, exc_info=True,
            )

    # ------------------------------------------------------------------
    # Tunables and schema migration — blocking calls kept off the loop thread
    # ------------------------------------------------------------------

    def _cycle_timeout(self) -> float:
        """Seconds one cycle may take before the event loop is recreated."""
        if self._cycle_timeout_override is not None:
            return self._cycle_timeout_override
        return float(
            max(self._reconcile_interval * _CYCLE_TIMEOUT_FACTOR, _MIN_CYCLE_TIMEOUT)
        )

    async def _refresh_settings(self) -> None:
        """Re-read the tunables, keeping the last known values on any failure.

        ``Variable.get`` talks to the database and the result decides the cycle
        budget, so it has to be read *before* the budget starts counting — outside
        everything the cycle watchdog covers. It therefore runs in the cycle pool
        under a short timeout of its own. A read still stuck in a worker blocks the
        next one from starting, so an unresponsive database costs one worker rather
        than one per cycle, and a changed Variable takes effect on the next cycle.
        """
        if self._settings_attempt is not None and not self._settings_attempt.done():
            return
        attempt = self._cycle_pool.submit(_read_settings)
        self._settings_attempt = attempt
        try:
            interval, cycle_timeout = await call_with_timeout(
                asyncio.wrap_future(attempt), _VARIABLE_TIMEOUT
            )
        except Exception:
            log.warning(
                "RMQ Watcher: cannot read tunables from Airflow Variables — keeping "
                "interval=%ss, cycle timeout=%ss",
                self._reconcile_interval, self._cycle_timeout(), exc_info=True,
            )
            return
        previous = (self._reconcile_interval, self._cycle_timeout())
        self._reconcile_interval = (
            interval if interval is not None else DEFAULT_RECONCILE_INTERVAL
        )
        self._cycle_timeout_override = cycle_timeout
        if previous != (self._reconcile_interval, self._cycle_timeout()):
            log.info(
                "RMQ Watcher tunables in effect: reconcile interval %ss, cycle budget "
                "%.0fs", self._reconcile_interval, self._cycle_timeout(),
            )

    async def _ensure_schema(self) -> None:
        """Retry table creation and column migration until the schema is ready.

        The plugin runs the migration once at load; a database unreachable at that
        moment would leave the ORM model describing columns the live table lacks,
        which breaks every status upsert and the Subscriptions page with it. So each
        cycle tries again until the schema reports itself ready.

        The call blocks (``create_all``, ``inspect``, ``ALTER TABLE``) and goes to the
        cycle pool: run inline it would block the loop thread, and a blocked loop
        services no timers — the very watchdog wrapped around this cycle would never
        fire, and AMQP heartbeats would stop leaving the process with it.

        ``ALTER TABLE ... ADD COLUMN`` needs an ACCESS EXCLUSIVE lock and can queue
        behind whoever holds one, hence both the private timeout and the single
        attempt in flight: a timeout does not free the worker, so a retry every cycle
        would fill the pool with stuck attempts and hold that many connections of
        Airflow's shared engine.
        """
        if is_schema_ready():
            return
        if self._migration_attempt is not None and not self._migration_attempt.done():
            return
        if self._migration_skip_cycles > 0:
            self._migration_skip_cycles -= 1
            return

        attempt = self._cycle_pool.submit(ensure_table_exists)
        self._migration_attempt = attempt
        try:
            await call_with_timeout(asyncio.wrap_future(attempt), _MIGRATION_TIMEOUT)
        except Exception:
            log.warning(
                "RMQ Watcher: schema migration attempt failed or exceeded %ss",
                _MIGRATION_TIMEOUT, exc_info=True,
            )
            self._defer_migration()
            return

        if is_schema_ready():
            self._migration_backoff = 0
            self._migration_skip_cycles = 0
            log.info("RMQ Watcher: watcher schema is ready")
        else:
            self._defer_migration()

    def _defer_migration(self) -> None:
        """Hold off the next migration attempt, doubling the wait up to the cap."""
        self._migration_backoff = min(
            max(self._migration_backoff * 2, _MIGRATION_BACKOFF_START),
            _MIGRATION_BACKOFF_CAP,
        )
        self._migration_skip_cycles = self._migration_backoff
        log.warning(
            "RMQ Watcher: schema is not ready — retrying the migration in %s cycle(s)",
            self._migration_backoff,
        )

    # ------------------------------------------------------------------
    # DAG-file scanning (mtime-based incremental)
    # ------------------------------------------------------------------

    def _get_dags_folder(self) -> str:
        try:
            from airflow.configuration import conf as airflow_conf
            return airflow_conf.get("core", "dags_folder")
        except Exception:
            return "/opt/airflow/dags"

    def _scan_subscriptions(self) -> list[dict]:
        """Incrementally scan DAG files for @rmq_trigger subscriptions using mtime.

        On a stable deployment (no file changes) this costs only N mtime syscalls.
        Changed or new files are re-parsed; deleted files are evicted from the cache.
        Memory is bounded to the number of current DAG files on disk.
        """
        dags_folder = self._get_dags_folder()
        current_files = set(glob.glob(f"{dags_folder}/**/*.py", recursive=True))

        # Evict deleted files
        for path in list(self._last_mtimes):
            if path not in current_files:
                self._last_mtimes.pop(path)
                self._cached_subs.pop(path, None)

        # Re-parse changed or new files
        for path in current_files:
            try:
                mtime = os.path.getmtime(path)
            except OSError:
                continue
            if mtime == self._last_mtimes.get(path):
                continue
            subs = self._extract_subscriptions_from_file(path)
            self._cached_subs[path] = subs
            self._last_mtimes[path] = mtime

        # Flatten all cached subscriptions
        result: list[dict] = []
        for subs in self._cached_subs.values():
            result.extend(subs)
        return result

    def _extract_subscriptions_from_file(self, path: str) -> list[dict]:
        """Extract @rmq_trigger subscriptions from a DAG file via AST parsing.

        AST parsing never executes the file and never acquires the Python import
        lock, so it is safe to call from a background thread inside the Scheduler
        process.  DagBag would acquire the import lock and could deadlock with the
        Scheduler's own import activity, causing heartbeat failures and tasks being
        marked as killed externally.

        dag_id is resolved from the explicit dag_id= argument of @dag(...) via
        ``_extract_dag_id_from_decorators``'s three-way contract: a string
        literal or a simple module-level string constant assigned earlier in
        the file (``DAG_NAME = 'x'`` then ``dag_id=DAG_NAME``) resolves
        directly; a recognized ``@dag(...)`` call with no dag_id= at all (or
        a falsy resolved value) falls back to the decorated function's name,
        matching Airflow's own default; anything else that cannot be
        statically proven (a non-literal expression not found among the
        file's module constants, a truthy non-string literal, or no
        recognized ``@dag(...)`` call among the decorators at all — e.g. an
        aliased import) is treated as unresolved: the subscription is
        **not** registered, and a WARNING is logged if — and only if —
        ``@rmq_trigger`` is actually present on the same function (an
        unresolvable dag_id= with no @rmq_trigger is not this plugin's
        concern and stays silent). Module constants are never handed to a
        nested function's decorators (a DAG declared inside a factory
        function): a local binding of the same name could shadow the
        module-level one, so guessing would be unsafe there too.

        Multiple ``@rmq_trigger(exchange=...)`` decorators on the same DAG are
        not supported (see ``subscription_builder.has_exchange_conflict`` /
        Technical Details → "Стекинг exchange= на одном DAG" in the plan). The
        decorator raises ``ValueError`` for this case (it can abort the
        decorator call); the AST parser cannot abort a DAG import, so it logs a
        WARNING and skips the duplicate, keeping the first one parsed.

        Defense in depth: the whole function body is wrapped in a broad
        ``except Exception`` (in addition to the read/parse-specific except
        below). ``_parse_rmq_trigger_decorator`` already turns validation
        failures from ``build_subscriptions`` into a graceful per-decorator
        WARNING+skip via its own ``except ValueError`` — but if a future bug
        ever lets an unexpected exception type leak past that (e.g. a
        non-str/non-list literal reaching a helper that isn't guarded yet),
        it must still only cost this one DAG file's subscriptions, not crash
        the entire reconcile cycle for every other DAG. ``_scan_subscriptions``
        relies on this function never raising so it can still record the
        file's mtime — without that, a permanently malformed file would be
        re-parsed (and re-crash) on every single cycle forever.
        """
        try:
            with open(path, encoding="utf-8") as f:
                source = f.read()
            tree = ast.parse(source, filename=path)
        except (SyntaxError, OSError, UnicodeDecodeError) as exc:
            log.warning("Failed to read/parse DAG file %s: %s", path, exc)
            return []

        try:
            # module_constants: {name: (value, lineno)}; module_scope_function_ids:
            # id() of functions whose decorators are evaluated in module scope, so a
            # nested function (or a method) — which could be shadowed by a
            # local/class-scope binding of the same name, making the module value
            # simply wrong — never gets handed the module-level constant map. See
            # this method's docstring above and _resolve_function_dag_id below.
            module_constants, module_scope_function_ids = _collect_module_constants(tree)

            result: list[dict] = []
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
                dag_id = _resolve_function_dag_id(
                    node, module_constants, module_scope_function_ids, path
                )
                if dag_id is None:
                    continue
                dag_subs: list[dict] = []
                for decorator in node.decorator_list:
                    for sub in _parse_rmq_trigger_decorator(decorator, dag_id):
                        if has_exchange_conflict(dag_subs, [sub]):
                            log.warning(
                                "rmq_trigger: dag_id=%s already has an exchange= subscription — "
                                "skipping duplicate (stacking multiple exchange= decorators on one "
                                "DAG is not supported)",
                                dag_id,
                            )
                            continue
                        sub["dag_id"] = dag_id
                        sub["group_key"] = dag_id if sub.get("cooldown", 0) > 0 else None
                        dag_subs.append(sub)
                result.extend(dag_subs)
            return result
        except Exception:
            log.exception(
                "Unexpected error extracting @rmq_trigger subscriptions from DAG file %s — "
                "skipping this file's subscriptions for this cycle",
                path,
            )
            return []

    # ------------------------------------------------------------------
    # DB synchronisation
    # ------------------------------------------------------------------

    def _sync_to_db(self, scanned: list[dict]) -> None:
        """Reconcile dag_file subscriptions in DB with the current scan result.

        - Upserts all subscriptions found in code (source='dag_file').
        - Deletes dag_file subscriptions that no longer exist in code.
        - Never touches ui-sourced subscriptions.
        """
        scanned_keys = {
            (s["dag_id"], s["queue_name"], s.get("conn_id", "rmq_default"))
            for s in scanned
        }

        with WatcherSession() as session:
            existing = (
                session.query(RMQSubscription)
                .filter_by(source="dag_file")
                .all()
            )

            for sub in existing:
                key = (sub.dag_id, sub.queue_name, sub.conn_id)
                if key not in scanned_keys:
                    session.query(RMQSubscription).filter_by(
                        dag_id=sub.dag_id,
                        queue_name=sub.queue_name,
                        conn_id=sub.conn_id,
                        source="dag_file",
                    ).delete()

            for s in scanned:
                upsert_subscription(
                    session,
                    dag_id=s["dag_id"],
                    queue_name=s["queue_name"],
                    conn_id=s.get("conn_id", "rmq_default"),
                    filter_data=s.get("filter_data", {}),
                    source="dag_file",
                    cooldown=s.get("cooldown", 0) or None,
                    group_key=s.get("group_key"),
                )

            session.commit()
