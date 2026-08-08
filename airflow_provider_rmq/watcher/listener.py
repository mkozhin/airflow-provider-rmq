from __future__ import annotations

import ast
import asyncio
import enum
import glob
import logging
import os
import threading
import traceback
from collections.abc import Iterator
from typing import Any

from airflow.listeners import hookimpl

from airflow_provider_rmq.watcher.consumer import RMQConsumerManager
from airflow_provider_rmq.watcher.models import (
    RMQSubscription,
    WatcherSession,
    get_enabled_subscriptions,
    upsert_subscription,
)
from airflow_provider_rmq.watcher.subscription_builder import (
    build_subscriptions,
    has_exchange_conflict,
)

log = logging.getLogger(__name__)

_DEFAULT_RECONCILE_INTERVAL = 60


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


def _collect_module_constants(tree: ast.Module) -> dict[str, tuple[str, int]]:
    """Collect simple top-level ``NAME = "literal string"`` assignments
    (plus annotated ``NAME: str = "literal"``), each paired with its line
    number so callers can enforce "only usable before this point in the
    file" — see the wiring below.

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
        return {}

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
    return constants


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

    **Deliberate falsy asymmetry**: a literal ``@dag(dag_id=None)`` returns
    ``None`` (falls back to the function name, matching Airflow's own
    default), but ``DAG_ID = None`` followed by ``dag_id=DAG_ID`` returns
    ``_UNRESOLVED_DAG_ID`` instead — only ``str`` values are ever collected
    into ``constants`` by ``_collect_module_constants``, so a module-level
    ``None``/``False``/``0``/``""`` constant is never present in
    ``constants`` and its reference is treated the same as any other
    unresolvable name. This is intentional, not a bug: the direction of the
    asymmetry is the safe one (it skips + warns rather than silently
    resolving to a guess).

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

    Note: only ``dec.keywords`` is examined here — a positional
    ``@dag("my_id")`` and ``**kwargs``/``*args`` unpacking are handled by
    later extensions to this function, not by this contract.
    """
    constants = constants or {}
    for dec in decorators:
        if not isinstance(dec, ast.Call):
            continue
        func = dec.func
        is_dag = (
            (isinstance(func, ast.Name) and func.id == "dag")
            or (isinstance(func, ast.Attribute) and func.attr == "dag")
        )
        if not is_dag:
            continue
        for kw in dec.keywords:
            if kw.arg != "dag_id":
                continue
            value_node = kw.value
            if isinstance(value_node, ast.Name):
                if value_node.id in constants:
                    return constants[value_node.id]
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


class RMQWatcherListener:
    """Airflow Listener that runs a background RabbitMQ consumer loop inside the Scheduler process.

    Lifecycle:
    - ``on_starting`` fires when the Scheduler process starts; we spawn a daemon thread
      with its own asyncio event loop.
    - The loop reconciles subscriptions from DAG files (mtime-based scan) and the DB
      every ``reconcile_interval`` seconds, then delegates to ``RMQConsumerManager``.
    - ``before_stopping`` sets a stop event; the loop exits after the current iteration.
    """

    def __init__(self) -> None:
        self._thread: threading.Thread | None = None
        self._stop_event: threading.Event | None = None
        self._manager: RMQConsumerManager | None = None
        # mtime-based incremental scan state (lives in the daemon thread only)
        self._last_mtimes: dict[str, float] = {}   # filepath → mtime
        self._cached_subs: dict[str, list[dict]] = {}  # filepath → list[sub dict]

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
            "RMQWatcherListener.on_starting: component=%s (job_type=%s, is_scheduler=%s)",
            name, job_type, is_scheduler,
        )
        if is_scheduler:
            self._start()

    @hookimpl
    def before_stopping(self, component: Any) -> None:
        if self._stop_event is not None:
            self._stop_event.set()

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

    def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(self._main())
            except Exception:
                log.exception("RMQ Watcher loop crashed — restarting in 30s")
            finally:
                loop.close()
            if not self._stop_event.is_set():
                self._stop_event.wait(timeout=30)
        log.info("RMQ Watcher loop stopped")

    async def _main(self) -> None:
        self._manager = RMQConsumerManager()
        await self._manager.start()
        try:
            while not self._stop_event.is_set():
                try:
                    scanned = self._scan_subscriptions()
                    self._sync_to_db(scanned)

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
                            meta = exchange_meta.get(
                                (sub.dag_id, sub.queue_name, sub.conn_id)
                            )
                            if meta is not None:
                                entry.update(meta)
                            active_subs.append(entry)

                    await self._manager.reconcile(active_subs)
                except Exception:
                    log.exception("Error in RMQ Watcher reconciliation cycle")

                await asyncio.sleep(self._get_reconcile_interval())
        finally:
            await self._manager.stop()

    def _get_reconcile_interval(self) -> int:
        try:
            from airflow.models import Variable
            val = Variable.get("rmq_watcher_reconcile_interval", default_var=None)
            if val is not None:
                return int(val)
        except Exception:
            pass
        return _DEFAULT_RECONCILE_INTERVAL

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
            module_constants = _collect_module_constants(tree)  # {name: (value, lineno)}
            # Only functions whose decorators are evaluated in module scope may use
            # the module-level constant map. A nested function (or a method) can be
            # shadowed by a local/class-scope binding of the same name, in which case
            # the module value is simply wrong — see this function's docstring above.
            module_scope_function_ids = {
                id(node)
                for node in _module_scope_nodes(tree)
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            }

            result: list[dict] = []
            for node in ast.walk(tree):
                if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue
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
                    continue
                if dag_id is None:
                    dag_id = node.name
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
