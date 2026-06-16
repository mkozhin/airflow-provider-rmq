# Changelog

## v2.1.0

- **Added:** `queues=[...]` parameter in `@rmq_trigger` — subscribe a single DAG to multiple RabbitMQ queues; a message from any of them triggers the DAG
- **Added:** `cooldown=N` parameter in `@rmq_trigger` — after the first matching message the DAG starts exactly once after N seconds; all messages in the cooldown window are ACKed silently; `cooldown=0` (default) keeps the existing immediate-trigger behaviour
- **Added:** DLX-based cooldown mechanism — no writes to Airflow DB; uses `rmq_watcher.pending.{dag_id}` (topic DLX queue, x-max-length=1) as a per-message TTL timer and `rmq_watcher.fire` (topic exchange + queue) as the trigger; RMQ infrastructure is created automatically by the plugin on startup
- **Added:** Fire consumer — subscribes to `rmq_watcher.fire`, extracts `dag_id` from `routing_key`, and calls `trigger_dag()` with an idempotent run_id (`rmq_cooldown__{dag_id}__{message_id}`) to prevent duplicates on redelivery
- **Added:** Hot-reload — changes to `cooldown` or `filter_data` in a DAG file are picked up on the next reconcile cycle without restarting the Scheduler
- **Added:** Orphaned pending-queue warnings — when a cooldown subscription is removed the plugin logs a WARNING with the queue name and the `rabbitmqadmin` delete command; subsequent reconcile cycles are silent
- **Added:** UI grouping — subscriptions sharing the same `group_key` (= dag_id when cooldown > 0) are shown as a single group row; `Cooldown` column added; Enable/Disable/Delete apply to all queues in the group
- **Added:** Multi-queue form — subscription form supports a dynamic list of queues (+ Add / ✕ remove); creates one DB row per queue within the same group
- **Changed:** `_ActiveSub` dataclass replaces parallel `_tasks` / `_sub_conn_ids` dicts in `RMQConsumerManager` — snapshot of the full sub dict drives change detection and hot-reload
- **Limitation:** when cooldown is active `conf["body"]` and `conf["headers"]` in the triggered DAG run are empty — original message data is not preserved through the DLX chain

## v2.0.9

- **Fixed:** AST parser used the Python function name as `dag_id` even when `@dag(dag_id='...')` specified an explicit value — caused subscriptions to point at the wrong DAG. Now reads `dag_id` from the `@dag` decorator when it is a string literal; falls back to the function name for non-literal values (e.g. `dag_id=VARIABLE`)
- **Changed:** Airflow UI menu category renamed from `RabbitMQ Watcher` to `RabbitMQ`
- **Changed:** minimum Airflow version corrected to `>=2.9.0` in README (pyproject.toml was already correct)

## v2.0.8

- **Fixed:** `RMQWatcherListener.on_starting` still never started the consumer thread on Airflow 2.9+. Root cause: `on_starting` is called inside `Job.__init__()` **before** `super().__init__()` sets the SQLAlchemy column values, so `job_type` is always `None` at that point — the v2.0.7 `job_type` check was ineffective. Fixed by inspecting the Python call stack: `scheduler_command.py` is present in the stack when the scheduler starts, and absent for the triggerer and other components.

## v2.0.7

- **Fixed:** `RMQWatcherListener.on_starting` never started the consumer thread on Airflow 2.9+ — in that version the component class is named `Job` (ORM model) rather than `SchedulerJobRunner`, so the guard `"Scheduler" in name` was always `False`. Now also checks `job_type` attribute: starts when `"Scheduler" in job_type` (e.g. `job_type="SchedulerJob"`)

## v2.0.6

- **Fixed:** `on_starting` called twice (e.g. Scheduler restart) spawned a second parallel consumer loop — `_start()` now checks `_thread.is_alive()` and ignores duplicate calls; if the previous lifecycle is shutting down, waits up to 10 s before starting fresh (L2)
- **Fixed:** Unhandled exception outside the inner `try` in `_run_loop` (e.g. asyncio loop crash, `_manager.start()` failure) silently killed the background thread — `_run_loop` now restarts `_main()` after any exception with a 30 s back-off (L3)
- **Fixed:** `upsert_subscription` unconditionally overwrote `enabled` from the `dag_file` reconcile default (`True`), reverting UI toggle changes — `enabled` is now only updated when `source="ui"`; dag_file reconciles preserve the current DB value (M2)
- **Fixed:** Binary or non-UTF-8 message bodies raised `UnicodeDecodeError` in `_trigger_dag`, causing infinite redelivery — body is now decoded with `errors="replace"` (C5)
- **Fixed:** `is_dag_file` not passed to `render_template` in `create()` error paths — Jinja2 received an `Undefined` variable; all three early-return render calls now explicitly pass `is_dag_file=False` (V1)
- **Fixed:** `ensure_table_exists()` in `plugin.on_load` had no error handling — a DB unavailability at plugin load time crashed the Scheduler startup; wrapped in `try/except` with `log.exception` (P1)
- **Fixed:** `consumer_status` stayed `"listening"` after a subscription was removed or disabled via reconcile — status is now set to `"disconnected"` after task cancellation (C3)
- **Changed:** `datetime.utcnow()` replaced with `datetime.now(timezone.utc)` in `_build_run_id` — eliminates `DeprecationWarning` on Python 3.12+ (C4)

## v2.0.5

- **Fixed:** CSRF token not submitted on form POST — `{{ csrf_token() }}` replaced with `<input type="hidden">` in `subscription_form.html` and `subscriptions.html`

## v2.0.4

- **Changed:** `_extract_subscriptions_from_file` now uses AST parsing instead of `DagBag` — eliminates risk of Python import-lock deadlock when scanning DAG files from a background thread inside the Scheduler process

## v2.0.3

- **Fixed:** `TemplateNotFound: rmq_watcher/subscriptions.html` — registered a Flask Blueprint with `template_folder` via `flask_blueprints` so Airflow's Jinja2 loader finds the package templates

## v2.0.2

- **Fixed:** `RMQWatcherView` raising `KeyError: 'can_subscriptions'` on Airflow 2.9+ — added `method_permission_name`, `class_permission_name`, and `base_permissions` to map view methods to standard FAB actions (`can_read`, `can_edit`, `can_create`, `can_delete`); RBAC enforced via `@has_access`
- **Changed:** minimum Airflow version bumped from 2.7.0 to 2.9.0

## v2.0.1

- **Fixed:** `RMQWatcherPlugin` not appearing in Airflow UI — registered via `airflow.plugins` entry point so the plugin is loaded by `load_entrypoint_plugins()` regardless of `lazy_discover_providers` (defaults to `True` since Airflow 2.8)

## v2.0.0

**RMQ Watcher Plugin** — reactive DAG triggering from RabbitMQ (fully backwards compatible with 1.x)

- **Added:** `RMQWatcherPlugin` — Airflow plugin that starts a background RabbitMQ consumer inside the Scheduler process. Incoming messages automatically trigger associated DAGs without polling, sensor tasks, or worker slots
- **Added:** `@rmq_trigger(queue, conn_id, filter_data)` decorator — attach RabbitMQ queue subscriptions directly to DAG definitions (Infrastructure as Code). Supports stacking multiple queues on one DAG
- **Added:** Scheduler Listener (`RMQWatcherListener`) — uses Airflow Listener API (`on_starting`/`before_stopping`) to run an asyncio consumer loop in a background daemon thread; one `connect_robust` connection per `conn_id` shared across all subscriptions to that cluster
- **Added:** Reconciliation loop — every 60 s (configurable via Airflow Variable `rmq_watcher_reconcile_interval`) re-scans DAG files with mtime-based incremental parsing and syncs subscriptions to DB; only changed files are re-parsed
- **Added:** `RMQWatcherView` — Flask-AppBuilder page at `/rmq-watcher/subscriptions` to create, edit, toggle and delete subscriptions; connection and consumer status with colored badges
- **Added:** DB tables `rmq_watcher_subscriptions` and `rmq_watcher_conn_status`, created automatically via `plugin.on_load()` (`checkfirst=True` — safe to call from multiple containers)
- **Added:** Example DAG `docs/example_dags/rmq_watcher_triggered_dag.py`
- **Changed:** Version bump to 2.0.0 — new reactive infrastructure (Scheduler Listener, background asyncio loop, DB tables, Airflow UI) represents a qualitatively new capability; all 1.x components (`RMQHook`, `RMQSensor`, `RMQTrigger`, operators) are unchanged and fully backwards compatible

## v1.2.1

- **Fixed:** `arguments` added to `template_fields` in `RMQQueueManagementOperator` — `x-*` arguments (e.g., `alternate-exchange`, DLQ settings) now support Jinja templates and XCom

## v1.2.0

- **Added:** `message_wait_timeout` added to `template_fields` in `RMQSensor` — the parameter now supports Jinja templates and XCom, enabling dynamic timeouts computed at runtime (e.g., remaining seconds until end of business hours)
- **Changed:** `RMQSensor` timeout behaviour — when `message_wait_timeout` expires, `AirflowSkipException` is now raised instead of `RuntimeError`. The task is marked **SKIPPED** rather than FAILED, and `on_failure_callback` is not triggered. This is a **breaking change** for code that caught `RuntimeError` on timeout

## v1.1.0

- **Added:** `mode="push"` in `RMQSensor` and `RMQTrigger` — deferrable sensor now supports push delivery via AMQP `basic_consume`. The broker delivers messages instantly as they arrive, eliminating polling delay. Default remains `mode="pull"` (backwards compatible)
- **Added:** `message_wait_timeout` parameter in `RMQSensor` and `RMQTrigger` — optional client-side timeout (seconds) for push mode. Sensor raises `RuntimeError` on expiry. Only valid with `mode="push"`
- **Added:** Example DAG `docs/example_dags/rmq_sensor_push.py` demonstrating push mode with filtering and timeout
- **Changed:** Example DAGs moved from `airflow_provider_rmq/example_dags/` to `docs/example_dags/` (no longer shipped inside the pip package)
- **Changed:** Version is now derived from git tags via `setuptools-scm` — no hardcoded version in source
- **Changed:** CI workflow now runs tests on Python 3.10/3.11/3.12 before publishing; triggers on `v*` tag push

## v1.0.1

- **Fixed:** SSL context and URL encoding improvements in `RMQTrigger`
- **Fixed:** Connection stability improvements in `RMQHook` (auto-reconnect, GC cleanup)

## v1.0.0

- **Added:** `RMQHook` — synchronous AMQP hook with retry logic, SSL/TLS support, queue/exchange management
- **Added:** `RMQPublishOperator` — publish messages to RabbitMQ exchanges or queues
- **Added:** `RMQConsumeOperator` — consume messages with dict and callable filter support
- **Added:** `RMQQueueManagementOperator` — declare, delete, purge, bind queues and exchanges
- **Added:** `RMQSensor` — wait for a matching message in poke and deferrable modes
- **Added:** `RMQTrigger` — async trigger using aio-pika for deferrable sensor support
- **Added:** `MessageFilter` — dict-based and callable-based message filtering
- **Added:** SSL/TLS configuration via Airflow connection extras
