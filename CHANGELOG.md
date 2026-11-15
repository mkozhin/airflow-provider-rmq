# Changelog

## v2.7.0

### Changed
- `@rmq_trigger` arguments must be statically readable: literals or constants defined at the top of the same file (positional arguments and `**CONST` included). Otherwise the DAG file fails to import with `RMQTriggerNotStaticError` and the watcher skips the whole file.
- `rmq_trigger` must be imported from the provider and `dag` from Airflow (`airflow.decorators` or `airflow.models.dag`). Wrappers, aliases, `import *` and decorators placed below `@rmq_trigger` are rejected.
- The watcher lists DAG files the way Airflow does: `.airflowignore` is respected, hidden files and directories are scanned, zip archives are not read.
- `@rmq_trigger` in a file outside `DAGS_FOLDER` or excluded by `.airflowignore` fails the import. Tests and CI that build a `DagBag` must set `DAGS_FOLDER` to the same folder.
- `RMQPublishOperator` waits for the broker to confirm each message (`confirm=True`), so large batches publish slower; `confirm=False` publishes without waiting.
- `RMQPublishOperator` publishes to the default exchange (`queue_name=…` or `exchange=""`) with `mandatory=True` by default: a message no queue takes fails the task.
- `RMQPublishOperator` fails the task when the broker rejects a message (nack) or closes the channel (404 on a missing exchange, 403).

### Added
- `RMQHook(publisher_confirms=…)` and `RMQHook.basic_publish(mandatory=…)`.
- `RMQPublishOperator` logs a WARNING when there is nothing to publish (`message=None` or an empty list).

### Fixed
- Unreadable `@rmq_trigger` arguments are no longer silently replaced by defaults.
- An unreadable subdirectory of `DAGS_FOLDER` no longer drops its subscriptions silently: it is logged and its files are held like unreadable files.
- `RMQHook` reads `heartbeat` and `blocked_connection_timeout` in Extra as numbers or numeric strings; invalid values fall back to the defaults with a WARNING.
- `RMQHook` logs a WARNING on `"heartbeat": 0` and `"blocked_connection_timeout": 0` and uses the default for a `heartbeat` under one second; a `heartbeat` above 65535 falls back to the default with a WARNING on both paths.
- `RMQHook.queue_info` returns `consumer_count` for a missing queue as well.
- `RMQHook.__del__` does not raise at interpreter shutdown.

### Upgrade notes
- RabbitMQ 4.3 or newer is required: on earlier versions a requeueing nack spends a quorum queue's `delivery-limit`, so redelivered messages can be dead-lettered or dropped.
- Before rollout, check the production DAG folder for: files that will now fail to import, `.py` files in hidden directories, heavy directories (`.git`, `.venv`, `node_modules`) that belong in `.airflowignore`, and `@rmq_trigger` factories under `.airflowignore`.
- Publishing to the default exchange (`queue_name=…` or `exchange=""`) fails when the queue does not exist: create target queues before publishing, or set `mandatory=False`.
- Set `execution_timeout` on publish tasks: the wait for a broker confirmation has no deadline of its own.

## v2.6.0

### Changed
- The watcher runs in a child process of the Scheduler instead of a thread, so the Scheduler forks safely ([ADR 0008](docs/adr/0008-watcher-child-process.md)). Installation and configuration are unchanged.
- Stopping the Scheduler stops the watcher process within a bounded time (SIGTERM, 5 s, then SIGKILL); unacknowledged deliveries return to their queues.
- Watcher start/stop log lines are reworded (`RMQ Watcher loop started …`, `RMQ Watcher process <pid> is already running …`); update log searches and alerts keyed on the old text.
- Modules in the Scheduler's working directory (e.g. a `logging.py` in `AIRFLOW_HOME`) are not imported by the watcher process; a provider installed in the plugins folder keeps working.

### Added
- The watcher process exits on its own when the Scheduler dies without a shutdown, and ignores SIGHUP, SIGQUIT, SIGUSR1 and SIGUSR2 (e.g. an executor dump via `docker kill -s USR2`).
- The watcher process always exits within 60 s once it starts stopping, even when stuck; the Scheduler logs such an exit with code `-14`.
- StatsD counter `rmq_watcher.cycle_completed`, one per finished reconcile cycle. A dead watcher process is not restarted until the next Scheduler restart, so alert on this counter standing still (one alert per Scheduler).
- The watcher process loads provider configuration and Airflow plugins before its loop starts, as the Scheduler does.

### Upgrade notes
- Each Scheduler opens up to `sql_alchemy_pool_size + sql_alchemy_max_overflow` extra metadata-database connections for the watcher process; size the database or pooler for it.
- The watcher process is a second Airflow interpreter in the Scheduler container; leave room for it in the container memory limit.
- There is no setting to return to the in-thread watcher; downgrade the package to go back.

## v2.5.0

### Changed
- `MessageFilter` tells a filter miss from an evaluation error (exception in `filter_callable`, unparsable JSON body); errors are always requeued and logged as a throttled WARNING.
- Header conditions are checked before the body, so the result no longer depends on key order in `filter_headers`.
- `MessageFilter.matches()` called from user code logs `filter_callable` exceptions at DEBUG instead of WARNING.
- A `dag_file` subscription missing from a scan is deleted only after the fourth consecutive miss (from the third if its DAG is no longer active); every step is logged.
- The watcher skips syncing subscriptions when the DAG folder is missing, unreadable or mostly unreadable (e.g. a half-mounted volume), logging an ERROR; such a listing is accepted after three cycles.
- A sync step that outlived its timeout commits nothing once a newer scan is accepted; counted as `rmq_watcher.sync_superseded`.
- The DAG folder is taken from `airflow.settings.DAGS_FOLDER`, the same path the Scheduler parses.
- A DAG file that fails to parse or cannot be read keeps its previous subscriptions for one cycle.

### Added
- `ack_unmatched` on `@rmq_trigger`, `RMQConsumeOperator`, `RMQSensor`, `RMQTrigger` and the subscription form: acknowledge messages the filter rejects instead of requeueing them forever. Default `False`.
- Column `ack_unmatched` in `rmq_watcher_subscriptions` (migrated automatically); a change applies on the next reconcile cycle.
- WARNINGs on long runs of filter misses and of messages dropped by `ack_unmatched`, a per-batch line in the operator, and StatsD `rmq_watcher.filter_missed` and `rmq_watcher.filter_errored`.
- A Scheduler WARNING names each `@rmq_trigger` argument the DAG file scan cannot read.
- The Subscriptions page flags a queue consumed by several enabled subscriptions of one `conn_id` (`messages deleted` when one of them uses `ack_unmatched`) and warns on save.
- A WARNING when a `dag_file` subscription deleted by the watcher comes back, since it returns enabled.
- Column `absent_cycles` in `rmq_watcher_subscriptions` (migrated automatically); until then the page shows a notice and hides Create.

### Upgrade notes
- `ack_unmatched=True` deletes rejected messages without dead-lettering; use it only on queues the subscription or task owns.
- `prefetch` stays unset, so large backlogs are read into Scheduler memory and may hit RabbitMQ `consumer_timeout`; raise it if large backlogs are expected.

## v2.4.0

### Changed
- Immediate mode (`cooldown=0`) is at-least-once: a delivery is acknowledged after the DAG run is created, a failed trigger is requeued with backoff (1–60 s). Redeliveries are deduplicated by run id `rmq__{queue}__{message_id}`.
- Connection `status` in `rmq_watcher_conn_status` follows the broker's liveness verdict instead of the task count; never-checked connections show `unknown`, and rows are refreshed every cycle.
- The cooldown fire consumer stays on one `conn_id`; cooldown subscriptions spread over several `conn_id`s log an ERROR.
- A cooldown placeholder rejected by the broker (window already open) is acknowledged and logged at INFO instead of requeued.
- Scheduler shutdown wakes the watcher loop at once, and every stop step is time-bounded.
- One unreachable broker no longer delays connections to other `conn_id`s.
- A subscription that cannot attach shows `error` with a reason instead of staying `connecting`; `last_error` is never empty.
- Deliveries for a DAG not yet serialized (Scheduler warmup) are requeued with backoff while the subscription stays `listening`; 25 in a row turn it to `error`.

### Added
- AMQP heartbeat on watcher connections, 30 s by default, set with Connection extra `heartbeat` (`0` disables it, with a WARNING).
- Connection extras `connect_timeout` (15 s) and `rpc_timeout` (30 s) bound the watcher's AMQP calls; invalid values fall back to defaults with a WARNING.
- Liveness watchdog: each cycle checks with the broker that every consumer is still registered, via the Management API or a passive `queue_declare` ([ADR 0007](docs/adr/0007-connection-liveness-two-tier-check.md)).
- Two failed liveness checks restart the affected consumers and their connection, at most once per 5 cycles per `conn_id`; a restart held back by that limit shows status `degraded`.
- A `conn_id` whose consumers never reach `listening` is probed directly and recreated if the broker does not answer.
- Reconcile cycle timeout `max(reconcile_interval * 3, 300)` s, set with Variable `rmq_watcher_cycle_timeout`; each blocking step is limited to 60 s and a timeout logs the phase.
- Cooldown publishing uses its own connection per `conn_id`, so a RabbitMQ resource alarm does not block consumers; failing publishes back off 1–60 s.
- Columns `broker_consumer_count` and `last_reconcile_at` in `rmq_watcher_conn_status` (migrated automatically, retried until it succeeds).
- Subscriptions page shows the broker's consumer count next to the watcher's, the age of the last reconcile per `conn_id` with a staleness flag, and a notice while the schema is not migrated.
- Blocking calls run in bounded watcher thread pools; saturation is logged instead of queued.
- StatsD metrics `rmq_watcher.consumer_attached`, `rmq_watcher.consumer_restarted`, `rmq_watcher.dag_triggered`, `rmq_watcher.cycle_timed_out`, `rmq_watcher.cycle_step_timed_out`, `rmq_watcher.cycle_step_skipped` and `rmq_watcher.tasks_abandoned`.
- INFO log lines on watcher start (with interval and cycle budget), on Variable overrides and on every (re)connection; a reason when `on_starting` does not start the watcher.
- The Op role gets access to the Subscriptions page at webserver start; `[rmq_watcher] grant_op_access = false` removes it (any unrecognised value also removes it).

### Fixed
- Cooldown never fired: the pending placeholder failed to publish. Affects every earlier release with cooldown.
- A cooldown placeholder that reached no queue silently dropped the event; the delivery is now requeued and the queue redeclared.
- `listening` was reported before the broker confirmed the consumer; registration is now bounded by `rpc_timeout`.
- A fire-queue delivery whose trigger failed stayed unacknowledged; it is now requeued with backoff.
- Each failed publish or trigger leaked a channel.
- A connect made during a RabbitMQ resource alarm never returned and stalled all connections.
- A consumer cancelled while connecting left an orphan connection to the broker.
- A connection whose reconnect never finished was reused forever, leaving consumers failing until a container restart.
- The cooldown fire consumer could get stuck on a dead connection while the `conn_id` showed `connected`.
- A cancelled consumer could keep consuming, triggering DAGs of removed or edited subscriptions.
- Subscription and connection status could show a stale `listening`/`connected`, or a stale `last_error`, after a later failure.
- A redelivered message was reported as a trigger error instead of a duplicate (`DagRunAlreadyExists` on Airflow 2.9+).
- Two messages arriving within the same second produced a single DAG run.
- `RMQTrigger` in push mode lost non-UTF-8 messages.
- A consumer restarted without delay after its queue iterator ended, spinning the loop.

### Upgrade notes
- Immediate-mode deduplication trusts the producer's `message_id`: messages sharing one collapse into one DAG run, and without one a redelivery runs the DAG again.
- `prefetch` is not set, so large backlogs are read into Scheduler memory and may hit RabbitMQ `consumer_timeout` (30 min default); raise it if large backlogs are expected.

## v2.3.0

### Changed
- An unresolvable `dag_id` (imported constant, f-string, function call, etc.) skips the subscription with a WARNING instead of falling back to the function name.

### Added
- Subscriptions page shows a `⚠ dag not found` badge for subscriptions whose `dag_id` matches no active DAG.
- `dag_id` in `@dag(...)` resolves module-level string constants, including positional `@dag(DAG_NAME)`.

### Upgrade notes
- A subscription that worked only because the function name matched the real `dag_id` disappears; use a literal or a constant defined in the same file, or create it in the UI.

## v2.2.0

### Added
- `exchange=`, `routing_keys=`, `routing_key_ids=` and `routing_key_status=` in `@rmq_trigger`: subscribe a DAG to a topic exchange; the provider declares the exchange, its queues and keeps bindings in sync.
- Connection extra `management_url` for the RabbitMQ Management HTTP API, used to read current bindings with the connection's login and password.
- Dependency `httpx`.

### Fixed
- `@rmq_trigger` above a TaskFlow `@dag(...)` failed the DAG import with `AttributeError`; a wrong decorator order now raises a clear `TypeError`.

## v2.1.0

### Added
- `queues=[...]` in `@rmq_trigger`: one DAG subscribes to several queues.
- `cooldown=N` in `@rmq_trigger`: the DAG runs once, N seconds after the first matching message; messages in the window are acknowledged. The run gets empty `conf["body"]` and `conf["headers"]`.
- Cooldown runs on RabbitMQ dead-lettering (`rmq_watcher.pending.{dag_id}`, `rmq_watcher.fire`), created automatically, with no writes to the Airflow DB; run id `rmq_cooldown__{dag_id}__{message_id}` prevents duplicates.
- Changes to `cooldown` or `filter_data` in a DAG file apply on the next reconcile cycle without a Scheduler restart.
- A WARNING with the `rabbitmqadmin` delete command for a pending queue left by a removed cooldown subscription.
- Subscriptions page groups queues of one cooldown DAG into a single row with a `Cooldown` column; enable, disable and delete act on the whole group.
- Subscription form accepts several queues.

### Fixed
- The fire consumer no longer invents a run id for a message without `message_id` (which broke deduplication); it logs a warning and skips it.
- A WARNING when the fire consumer cannot start because its connection is not ready.

## v2.0.9

### Changed
- Airflow UI menu category is renamed from `RabbitMQ Watcher` to `RabbitMQ`.
- README states the minimum Airflow version `>=2.9.0`.

### Fixed
- The watcher used the function name as `dag_id` even when `@dag(dag_id='...')` set one.

## v2.0.8

### Fixed
- The watcher still did not start on Airflow 2.9+; Scheduler detection is reworked.

## v2.0.7

### Fixed
- The watcher did not start on Airflow 2.9+.

## v2.0.6

### Changed
- No `DeprecationWarning` from run id generation on Python 3.12+.

### Fixed
- A repeated `on_starting` no longer starts a second consumer loop.
- An unexpected error no longer kills the watcher silently; it restarts after 30 s.
- DAG-file sync no longer re-enables subscriptions disabled in the UI.
- Non-UTF-8 message bodies no longer cause endless redelivery.
- Subscription form error pages render correctly.
- An unavailable database at plugin load no longer crashes Scheduler startup.
- A removed or disabled subscription shows `disconnected` instead of `listening`.

## v2.0.5

### Fixed
- Subscription forms did not submit the CSRF token.

## v2.0.4

### Changed
- DAG files are read by static parsing instead of `DagBag`, avoiding an import-lock deadlock in the Scheduler.

## v2.0.3

### Fixed
- `TemplateNotFound: rmq_watcher/subscriptions.html`.

## v2.0.2

### Changed
- Minimum Airflow version is 2.9.0 (was 2.7.0).

### Fixed
- `KeyError: 'can_subscriptions'` on Airflow 2.9+; the Subscriptions page uses the standard `can_read`, `can_edit`, `can_create`, `can_delete` permissions.

## v2.0.1

### Fixed
- `RMQWatcherPlugin` did not appear in the Airflow UI; it is registered via the `airflow.plugins` entry point.

## v2.0.0

### Added
- `RMQWatcherPlugin`: a RabbitMQ consumer inside the Scheduler triggers DAGs on incoming messages, with no sensors or worker slots; one connection per `conn_id`.
- `@rmq_trigger(queue, conn_id, filter_data)` decorator declares queue subscriptions in the DAG file; it can be stacked for several queues.
- Subscriptions are re-synced from DAG files every 60 s (Variable `rmq_watcher_reconcile_interval`); only changed files are re-parsed.
- Subscriptions page at `/rmq-watcher/subscriptions` to create, edit, toggle and delete subscriptions and see connection and consumer status.
- Tables `rmq_watcher_subscriptions` and `rmq_watcher_conn_status`, created automatically.
- Example DAG [`rmq_watcher_triggered_dag.py`](docs/example_dags/rmq_watcher_triggered_dag.py).

### Upgrade notes
- Backwards compatible with 1.x: `RMQHook`, `RMQSensor`, `RMQTrigger` and the operators are unchanged.

## v1.2.1

### Fixed
- `arguments` of `RMQQueueManagementOperator` supports Jinja templates and XCom.

## v1.2.0

### Changed
- `RMQSensor` raises `AirflowSkipException` when `message_wait_timeout` expires: the task is SKIPPED instead of FAILED.

### Added
- `message_wait_timeout` of `RMQSensor` supports Jinja templates and XCom.

### Upgrade notes
- Code that caught `RuntimeError` on the sensor timeout must handle the skip instead; `on_failure_callback` no longer fires on timeout.

## v1.1.0

### Changed
- Example DAGs moved to `docs/example_dags/` and are no longer shipped in the package.
- The package version comes from git tags (`setuptools-scm`).
- CI runs tests on Python 3.10–3.12 before publishing on a `v*` tag.

### Added
- `mode="push"` in `RMQSensor` and `RMQTrigger`: messages are delivered as they arrive instead of polled. Default stays `mode="pull"`.
- `message_wait_timeout` in `RMQSensor` and `RMQTrigger` for push mode; the sensor raises `RuntimeError` when it expires.
- Example DAG [`rmq_sensor_push.py`](docs/example_dags/rmq_sensor_push.py).

## v1.0.1

### Fixed
- SSL context and URL encoding in `RMQTrigger`.
- Connection stability in `RMQHook` (auto-reconnect, cleanup).

## v1.0.0

### Added
- `RMQHook`: synchronous AMQP hook with retries, SSL/TLS and queue/exchange management.
- `RMQPublishOperator`: publish messages to exchanges or queues.
- `RMQConsumeOperator`: consume messages with dict and callable filters.
- `RMQQueueManagementOperator`: declare, delete, purge and bind queues and exchanges.
- `RMQSensor`: wait for a matching message in poke and deferrable modes.
- `RMQTrigger`: async trigger on aio-pika for the deferrable sensor.
- `MessageFilter`: dict-based and callable-based message filtering.
- SSL/TLS configuration via Airflow connection extras.
