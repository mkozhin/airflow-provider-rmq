<h1 align="center">
  Apache Airflow Provider for RabbitMQ
</h1>
<h3 align="center">
  Trigger Airflow DAGs reactively from RabbitMQ queues — plus hooks, operators, sensors, and deferrable triggers.
</h3>

<p align="center">
  <a href="#installation">Installation</a> &bull;
  <a href="#connection-setup">Connection</a> &bull;
  <a href="#components">Components</a> &bull;
  <a href="#example-dags">Examples</a> &bull;
  <a href="#contributing">Contributing</a>
</p>

---

*Powered by [Claude Code](https://claude.ai/code)*

---

## Overview

`airflow-provider-rmq` is a community provider package for Apache Airflow × RabbitMQ. It supports:

- **Reactive DAG triggering** — [RMQ Watcher Plugin](#rmq-watcher-plugin) starts DAGs automatically when messages arrive in a queue, with no polling and no worker slots consumed
- Publishing messages to exchanges and queues
- Consuming messages with header-based and callable-based filtering
- Waiting for specific messages with sensors (classic poke and deferrable mode)
- Deferrable sensor in **pull mode** (periodic polling) and **push mode** (broker-delivered via `basic_consume`) — choose based on latency requirements
- Full queue and exchange management (declare, delete, purge, bind, unbind)
- SSL/TLS connections
- Dead Letter Queue (DLQ) setup helpers
- QoS configuration (prefetch)

### Requirements

| Dependency | Version |
|---|---|
| Apache Airflow | `>=2.9.0, <3.0.0` |
| pika | `>=1.3.0, <2.0.0` |
| aio-pika | `>=9.0.0, <10.0.0` |
| tenacity | `>=8.0.0` |
| httpx | `>=0.27` |
| Python | `>=3.10` |

---

## Installation

### Install from PyPI

```bash
pip install airflow-provider-rmq
```

### Building from source

```bash
git clone https://github.com/mkozhin/airflow-provider-rmq.git
cd airflow-provider-rmq
pip install build
python -m build
pip install dist/airflow_provider_rmq-*.whl
```

---

## Connection Setup

Create a new connection in the Airflow UI (**Admin > Connections**) with:

| Field | Value | Description |
|---|---|---|
| Connection Id | `rmq_default` | Any unique ID |
| Connection Type | `AMQP` | Registered by the provider |
| Host | `localhost` | RabbitMQ server hostname |
| Port | `5672` | `5671` for SSL |
| Login | `guest` | RabbitMQ username |
| Password | `guest` | RabbitMQ password |
| Schema | `/` | Virtual host |

### SSL/TLS Configuration

Add SSL settings in the **Extra** field as JSON:

```json
{
  "ssl_enabled": true,
  "ca_certs": "/path/to/ca.pem",
  "certfile": "/path/to/client-cert.pem",
  "keyfile": "/path/to/client-key.pem",
  "cert_reqs": "CERT_REQUIRED"
}
```

The hook also provides custom form widgets for SSL fields (`ssl_enabled`, `ca_certs`, `certfile`, `keyfile`) visible in the Airflow connection form.

Set `"cert_reqs": "CERT_NONE"` to disable certificate verification (not recommended for production).

### Connection Timing

Three optional **Extra** keys control AMQP timing. All of them are plain JSON values next to the SSL settings:

```json
{
  "heartbeat": 30,
  "connect_timeout": 15,
  "rpc_timeout": 30
}
```

| Key | Default | Used by | Meaning |
|---|---|---|---|
| `heartbeat` | `30` on the async path, `600` in `RMQHook` | every AMQP connection | Seconds between AMQP heartbeat frames. `aiormq` allows three missed intervals and checks every half-interval, so a broken connection becomes an exception in roughly `3 × heartbeat` seconds — about 90 s at the default — and `connect_robust` reconnects |
| `connect_timeout` | `15` | RMQ Watcher | Seconds allowed for establishing an async connection |
| `rpc_timeout` | `30` | RMQ Watcher | Seconds allowed for a single async AMQP call — `channel()`, declare, bind, publish |

`heartbeat` is the same key the synchronous hook reads; the two paths differ only in default, because `RMQHook` lives in short sessions inside a task while the watcher holds a connection for days. Setting `"heartbeat": 0` is accepted as a deliberate opt-out and logged as a WARNING: with heartbeats off, a link that dies silently (a broker restart behind a proxy that keeps the client half open) stays undetected.

`connect_timeout` and `rpc_timeout` have no counterpart in `RMQHook` — pika is driven by its own retry logic there. A value that is not a positive number falls back to the default with a WARNING.

---

## Components

### RMQHook

**Import:** `from airflow_provider_rmq.hooks.rmq import RMQHook`

Core hook for all RabbitMQ interactions. Uses pika `BlockingConnection` with automatic retry logic (tenacity). The connection is closed automatically when the hook object is garbage-collected, so you do not need to call `close()` manually. Context manager (`with`) is also supported.

#### Constructor Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `vhost` | `str \| None` | `None` | No | Override virtual host from connection |
| `qos` | `dict \| None` | `None` | No | QoS settings: `prefetch_size`, `prefetch_count`, `global_qos` |
| `retry_count` | `int` | `3` | No | Number of connection retry attempts |
| `retry_delay` | `float` | `1.0` | No | Base delay (seconds) between retries (exponential backoff) |

#### Key Methods

| Method | Description |
|---|---|
| `get_channel()` | Returns a pika `BlockingChannel` (creates connection lazily) |
| `queue_declare(queue_name, passive, durable, exclusive, auto_delete, arguments)` | Declare a queue |
| `queue_delete(queue_name, if_unused, if_empty)` | Delete a queue |
| `queue_bind(queue, exchange, routing_key, arguments)` | Bind a queue to an exchange |
| `queue_unbind(queue, exchange, routing_key, arguments)` | Unbind a queue from an exchange |
| `queue_purge(queue_name)` | Remove all messages from a queue |
| `queue_info(queue_name)` | Get queue info (message_count, consumer_count, exists) via passive declare |
| `exchange_declare(exchange, exchange_type, passive, durable, auto_delete, internal, arguments)` | Declare an exchange |
| `exchange_delete(exchange, if_unused)` | Delete an exchange |
| `exchange_bind(destination, source, routing_key, arguments)` | Bind exchange to exchange |
| `exchange_unbind(destination, source, routing_key, arguments)` | Unbind exchange from exchange |
| `basic_publish(exchange, routing_key, body, properties)` | Publish a message |
| `consume_messages(queue_name, max_messages, auto_ack, inactivity_timeout)` | Consume messages from a queue |
| `ack(delivery_tag)` | Acknowledge a message |
| `nack(delivery_tag, requeue)` | Negatively acknowledge a message |
| `build_dlq_arguments(dlx_exchange, dlx_routing_key, message_ttl)` | Static method: build `x-*` args for DLQ support |
| `test_connection()` | Test the connection (used by Airflow UI) |
| `close()` | Close channel and connection |

#### Usage Example

```python
from airflow_provider_rmq.hooks.rmq import RMQHook

hook = RMQHook(rmq_conn_id="rmq_default")
info = hook.queue_info("my_queue")
print(f"Messages in queue: {info['message_count']}")

hook.basic_publish(
    exchange="",
    routing_key="my_queue",
    body='{"key": "value"}',
)
# Connection is closed automatically when hook goes out of scope
```

---

### RMQPublishOperator

**Import:** `from airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator`

Publishes one or more messages to RabbitMQ. Supports strings, dicts (auto-serialized to JSON), and lists.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `exchange` | `str` | `""` | No | Exchange to publish to (empty = default exchange) |
| `routing_key` | `str` | `""` | No | Routing key for the message |
| `message` | `str \| list[str] \| dict \| list[dict] \| None` | `None` | No | Message payload. Dicts are JSON-serialized |
| `queue_name` | `str \| None` | `None` | No | Shortcut: sets `exchange=""` and `routing_key=queue_name` |
| `content_type` | `str \| None` | `None` | No | AMQP content type header (e.g., `"application/json"`) |
| `delivery_mode` | `int \| None` | `None` | No | `1` = non-persistent, `2` = persistent |
| `headers` | `dict \| None` | `None` | No | Custom AMQP headers |
| `priority` | `int \| None` | `None` | No | Message priority (0-9) |
| `expiration` | `str \| None` | `None` | No | Per-message TTL in milliseconds (as string, e.g., `"60000"`) |
| `correlation_id` | `str \| None` | `None` | No | Application correlation identifier |
| `reply_to` | `str \| None` | `None` | No | Reply-to queue name |
| `message_id` | `str \| None` | `None` | No | Application message identifier |

**Template fields:** `exchange`, `routing_key`, `message`

#### Usage Example

```python
# Publish a single dict to a queue
RMQPublishOperator(
    task_id="publish",
    queue_name="my_queue",
    message={"event": "order_created", "id": 42},
    delivery_mode=2,
    headers={"x-source": "airflow"},
)

# Publish a batch of messages to an exchange
RMQPublishOperator(
    task_id="publish_batch",
    exchange="events",
    routing_key="orders.new",
    message=[
        {"id": 1, "item": "widget"},
        {"id": 2, "item": "gadget"},
    ],
)
```

---

### RMQConsumeOperator

**Import:** `from airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator`

Consumes messages from a RabbitMQ queue. Matching messages are ACKed and returned via XCom. Non-matching messages are NACKed with `requeue=True`.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `queue_name` | `str` | — | **Yes** | Name of the queue to consume from |
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `max_messages` | `int` | `100` | No | Maximum number of messages to consume per execution |
| `filter_headers` | `dict[str, Any] \| None` | `None` | No | Dict of AMQP headers that a message must match. Supports `body.*` keys for JSON body filtering (e.g., `{"body.data.status": "active"}`) |
| `filter_callable` | `Callable[[Any, str], bool] \| None` | `None` | No | Custom filter function `(properties, body_str) -> bool` |
| `qos` | `dict \| None` | `None` | No | QoS settings: `{"prefetch_count": 10}` |

**Template fields:** `queue_name`

**Returns:** `list[dict]` — list of matched messages, each with keys: `body`, `headers`, `routing_key`, `exchange`

#### Usage Example

```python
# Consume with header filter
RMQConsumeOperator(
    task_id="consume_orders",
    queue_name="orders",
    filter_headers={"x-type": "order"},
    max_messages=50,
    qos={"prefetch_count": 10},
)

# Consume with body-path filter
RMQConsumeOperator(
    task_id="consume_active",
    queue_name="events",
    filter_headers={"body.status": "active"},
)

# Consume with custom callable filter
def large_orders(properties, body: str) -> bool:
    import json
    data = json.loads(body)
    return data.get("amount", 0) > 1000

RMQConsumeOperator(
    task_id="consume_large",
    queue_name="orders",
    filter_callable=large_orders,
)
```

#### Processing Messages with TaskFlow API

`RMQConsumeOperator` returns `list[dict]` via XCom. Use `consume.output` in a `@task` function to access and process each message:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator

@dag(...)
def my_pipeline():
    consume = RMQConsumeOperator(
        task_id="consume",
        queue_name="orders",
        max_messages=50,
    )

    @task
    def process_messages(messages: list[dict]) -> list[dict]:
        results = []
        for msg in messages:
            body = msg["body"]          # message body (str)
            headers = msg["headers"]    # AMQP headers (dict)
            rk = msg["routing_key"]     # routing key
            exchange = msg["exchange"]  # source exchange
            log.info("Message: body=%s, headers=%s", body, headers)

            data = json.loads(body)
            results.append(data)
        return results

    processed = process_messages(consume.output)
    processed >> next_task  # pass results downstream
```

---

### RMQQueueManagementOperator

**Import:** `from airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator`

Performs queue and exchange management operations on RabbitMQ.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `action` | `str` | — | **Yes** | Action to perform (see table below) |
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `queue_name` | `str \| None` | `None` | Conditional | Queue name (required for queue actions) |
| `durable` | `bool` | `False` | No | Resource survives broker restart |
| `exclusive` | `bool` | `False` | No | Queue is exclusive to this connection |
| `auto_delete` | `bool` | `False` | No | Resource is deleted when no longer in use |
| `exchange_name` | `str \| None` | `None` | Conditional | Exchange name (required for exchange actions) |
| `exchange_type` | `str` | `"direct"` | No | Exchange type: `direct`, `fanout`, `topic`, `headers` |
| `internal` | `bool` | `False` | No | Exchange cannot be published to directly |
| `if_unused` | `bool` | `False` | No | Only delete if resource has no consumers/bindings |
| `if_empty` | `bool` | `False` | No | Only delete queue if it is empty |
| `routing_key` | `str` | `""` | No | Routing key for bind/unbind actions |
| `arguments` | `dict \| None` | `None` | No | Optional `x-*` arguments (e.g., DLQ settings) |
| `source_exchange` | `str \| None` | `None` | Conditional | Source exchange for exchange bind/unbind |

**Template fields:** `queue_name`, `exchange_name`, `routing_key`, `arguments`

#### Supported Actions

| Action | Required Parameters | Description |
|---|---|---|
| `declare_queue` | `queue_name` | Create a queue |
| `delete_queue` | `queue_name` | Delete a queue |
| `purge_queue` | `queue_name` | Remove all messages from a queue |
| `bind_queue` | `queue_name`, `exchange_name` | Bind a queue to an exchange |
| `unbind_queue` | `queue_name`, `exchange_name` | Unbind a queue from an exchange |
| `declare_exchange` | `exchange_name` | Create an exchange |
| `delete_exchange` | `exchange_name` | Delete an exchange |
| `bind_exchange` | `exchange_name`, `source_exchange` | Bind exchange to exchange |
| `unbind_exchange` | `exchange_name`, `source_exchange` | Unbind exchange from exchange |

#### Usage Example

```python
# Create a durable queue
RMQQueueManagementOperator(
    task_id="create_queue",
    action="declare_queue",
    queue_name="my_queue",
    durable=True,
)

# Create a topic exchange and bind a queue
RMQQueueManagementOperator(
    task_id="create_exchange",
    action="declare_exchange",
    exchange_name="events",
    exchange_type="topic",
    durable=True,
)

RMQQueueManagementOperator(
    task_id="bind",
    action="bind_queue",
    queue_name="my_queue",
    exchange_name="events",
    routing_key="orders.*",
)
```

---

### RMQSensor

**Import:** `from airflow_provider_rmq.sensors.rmq import RMQSensor`

Waits for a message in a RabbitMQ queue that matches optional filter conditions. Supports classic poke mode and deferrable mode.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `queue_name` | `str` | — | **Yes** | Name of the queue to monitor |
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `filter_headers` | `dict[str, Any] \| None` | `None` | No | Dict-based header/body filter |
| `filter_callable` | `Callable \| None` | `None` | No | Custom filter function. **Not supported with `deferrable=True`** |
| `deferrable` | `bool` | `False` | No | Use deferrable mode (frees worker slot while waiting) |
| `poke_batch_size` | `int` | `100` | No | Max messages to fetch per poke cycle |
| `poke_interval` | `float` | `60` | No | Seconds between poke attempts (inherited from BaseSensorOperator) |
| `timeout` | `float` | `604800` | No | Max seconds to wait before failing (inherited from BaseSensorOperator) |
| `mode` | `Literal["pull", "push"]` | `"pull"` | No | Trigger delivery mode when `deferrable=True`: `"pull"` = periodic polling, `"push"` = broker-pushed via `basic_consume` |
| `message_wait_timeout` | `float \| None` | `None` | No | Max seconds to wait for a matching message in push mode. `None` = no limit. Only valid with `mode="push"`. Supports Jinja templates and XCom |

**Template fields:** `queue_name`, `message_wait_timeout`

**Returns:** `dict | None` — matched message with keys: `body`, `headers`, `routing_key`, `exchange`

#### Deferrable Mode

When `deferrable=True`, the sensor defers execution to the Airflow triggerer process using `RMQTrigger`. This frees the worker slot while waiting for a message, which is more resource-efficient for long waits.

**Limitation:** `filter_callable` cannot be used with `deferrable=True` because Python callables cannot be serialized to the triggerer process. Use `filter_headers` instead.

#### Pull vs Push Mode

The `mode` parameter (only relevant with `deferrable=True`) controls how the trigger receives messages:

| | `mode="pull"` (default) | `mode="push"` |
|---|---|---|
| Mechanism | Periodic `queue.get()` + sleep | `basic_consume` subscription |
| Latency | Up to `poll_interval` delay | Instant — broker delivers immediately |
| Idle cost | Polling even when queue is empty | No activity until message arrives |
| When to use | Simplicity, predictable behavior | Low-latency requirements, idle queues |

> **Timeout behaviour:** when `message_wait_timeout` expires, the sensor raises `AirflowSkipException` — the task is marked **SKIPPED** (not FAILED) and downstream tasks are skipped. No `on_failure_callback` is triggered. This makes it safe to use `message_wait_timeout` for planned stops (e.g., end of business hours) without generating false alerts.

> **RabbitMQ 4.0+ quorum queue note:** non-matching messages are NACKed with `requeue=True`. Quorum queues enforce a default redelivery limit of 20 — after 20 redeliveries the message is dead-lettered or dropped. Applies to both pull and push modes.

#### Usage Example

```python
# Classic poke mode with callable filter
RMQSensor(
    task_id="wait_for_order",
    queue_name="orders",
    filter_callable=lambda props, body: "urgent" in body,
    poke_interval=10,
    timeout=300,
    mode="reschedule",
)

# Deferrable pull mode (default)
RMQSensor(
    task_id="wait_for_event",
    queue_name="events",
    filter_headers={"x-type": "payment"},
    deferrable=True,
    timeout=600,
)

# Deferrable push mode — broker delivers instantly, give up after 60 s
RMQSensor(
    task_id="wait_for_event_push",
    queue_name="events",
    filter_headers={"x-type": "payment"},
    deferrable=True,
    mode="push",
    message_wait_timeout=60,
    timeout=120,
)

# Dynamic timeout via XCom — e.g. compute remaining seconds until end of business hours
RMQSensor(
    task_id="wait_for_message",
    queue_name="events",
    deferrable=True,
    mode="push",
    message_wait_timeout="{{ ti.xcom_pull(task_ids='compute_timeout') }}",
)
```

#### Processing Sensor Result with TaskFlow API

`RMQSensor` returns `dict | None` via XCom. Use `sensor.output` in a `@task` function to access the matched message:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.sensors.rmq import RMQSensor

@dag(...)
def my_pipeline():
    wait = RMQSensor(
        task_id="wait_for_event",
        queue_name="events",
        filter_headers={"x-type": "payment"},
        deferrable=True,
    )

    @task
    def handle_event(message: dict):
        log.info("Received: %s", message)
        return message

    handle_event(wait.output)
```

---

### RMQTrigger

**Import:** `from airflow_provider_rmq.triggers.rmq import RMQTrigger`

Async trigger for deferrable sensor mode. Uses `aio_pika` for non-blocking AMQP access. Typically not used directly — `RMQSensor` with `deferrable=True` creates it automatically.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | — | **Yes** | Airflow connection ID |
| `queue_name` | `str` | — | **Yes** | Queue to monitor |
| `filter_data` | `dict \| None` | `None` | No | Serialized filter from `MessageFilter.serialize()` |
| `poll_interval` | `float` | `5.0` | No | Seconds between polls when queue is empty (pull mode only) |
| `mode` | `Literal["pull", "push"]` | `"pull"` | No | Delivery mode: `"pull"` = polling, `"push"` = `basic_consume` |
| `message_wait_timeout` | `float \| None` | `None` | No | Max seconds to wait in push mode. Actual wait may slightly exceed this due to `basic_cancel` cleanup |

---

### MessageFilter (Utility)

**Import:** `from airflow_provider_rmq.utils.filters import MessageFilter`

Evaluates whether a RabbitMQ message matches given filter conditions. Used internally by operators and sensors.

#### Filter Modes

1. **Header filtering** (`filter_headers`): dict of key-value pairs that message headers must match.
   - Regular keys check `properties.headers` dict
   - Keys starting with `body.` traverse the JSON-parsed message body (e.g., `{"body.data.status": "active"}`)

2. **Callable filtering** (`filter_callable`): `fn(properties, body_str) -> bool`

Both can be combined (AND logic: both must pass).

---

## RMQ Watcher Plugin

The **RMQ Watcher Plugin** inverts the usual sensor pattern: instead of a DAG waiting for a message, a RabbitMQ message *causes* the DAG to start automatically — without polling, without `deferred` task slots, without worker resources.

### How it works

The Scheduler process runs a background asyncio loop (via Airflow Listener API) that subscribes to queues via AMQP `basic_consume`. When a matching message arrives, `trigger_dag()` is called directly inside the process. One `RobustConnection` per `conn_id` is shared across all subscriptions to that cluster, with cooldown publishing on a second connection of its own (see [Connection Resilience](#connection-resilience)).

Every 60 seconds (configurable via Airflow Variable `rmq_watcher_reconcile_interval`) a reconciliation loop re-scans DAG files for `@rmq_trigger` decorators (mtime-based — only changed files are re-parsed) and syncs subscriptions to the database.

`dag_id` in `@dag(...)` (positional or keyword) must be a string literal, or a simple module-level string constant defined earlier in the file than the decorated function, for the AST scan of `dag_file` subscriptions to find it. If it isn't, the subscription simply does not appear on the Subscriptions page at all. This is the one case the `⚠ dag not found` badge (see [Subscription Management](#subscription-management)) cannot catch: there's no row to highlight, since the subscription never registers in the first place. The only signal is a WARNING in the Scheduler log, and only after a restart or when the file's mtime changes (not on every reconcile cycle — only when the file is actually re-scanned).

### Quick Start

**Step 1 — annotate your DAG:**

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.watcher.decorators import rmq_trigger

@rmq_trigger(queue="orders", conn_id="rmq_default")
@dag(schedule=None)
def orders_dag():
    @task
    def process(**context):
        conf = context["dag_run"].conf
        print(f"Body: {conf['body']}, Headers: {conf['headers']}")
    process()

orders_dag()
```

`@rmq_trigger` must be the outermost decorator, directly above `@dag(...)` (as shown) — putting it below raises a `TypeError` at import time instead of silently doing nothing.

**Step 2** — restart the Scheduler. The plugin activates automatically; no extra configuration is needed.

**Step 3** — publish a message to `orders` — the DAG starts within seconds.

### Multi-Queue and Cooldown

Subscribe one DAG to several queues and throttle repeated triggers with `cooldown`:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.watcher.decorators import rmq_trigger

@rmq_trigger(
    queues=["orders", "payments"],  # message from any queue starts the DAG
    cooldown=300,                    # 300 s cooldown — DAG runs once per window
    conn_id="rmq_default",
)
@dag(dag_id="my_dag", schedule=None)
def my_dag():
    @task
    def process(**context):
        conf = context["dag_run"].conf
        # conf["source"] == "cooldown" when triggered via cooldown mechanism
        # conf["body"] and conf["headers"] are empty — original data is not
        # preserved through the DLX chain
        print(conf["source"])
    process()

my_dag()
```

**How cooldown works:**

- When the first matching message arrives, the plugin publishes a TTL marker to `rmq_watcher.pending.{dag_id}` (a no-consumer queue with `x-max-length=1` and DLX to `rmq_watcher.fire`).
- After N seconds the marker expires and is routed to `rmq_watcher.fire`; the fire consumer calls `trigger_dag()` with an idempotent run_id.
- Additional messages arriving during the cooldown window are ACKed silently — the pending queue rejects the duplicate publish (`x-overflow=reject-publish`).
- All RMQ infrastructure (`rmq_watcher.fire` exchange, queue, and per-DAG `rmq_watcher.pending.*` queues) is created automatically by the plugin on startup.

**Limitations:**
- All cooldown subscriptions for a DAG share one pending queue and one timer.
- All cooldown DAGs must use the same `conn_id` / vhost.
- `conf["body"]` and `conf["headers"]` in the DAG run conf are empty when triggered via cooldown — original message data is lost in the DLX chain.
- Changing `cooldown` in a DAG file takes effect on the next reconcile cycle (default 60 s); already-running timers in RMQ are not affected.

**RabbitMQ Permissions (cooldown only):**

When `cooldown > 0` is used, the Airflow RMQ user needs configure/write/read permissions on the `rmq_watcher.*` resource pattern in addition to permissions on your application queues:

```
rabbitmqctl set_permissions -p <vhost> <user> "^(rmq_watcher\\..*|your-queue.*)$" "^(rmq_watcher\\..*|your-queue.*)$" "^(rmq_watcher\\..*|your-queue.*)$"
```

This covers the `rmq_watcher.fire` exchange, `rmq_watcher.fire` queue and `rmq_watcher.pending.<dag_id>` queues that the cooldown mechanism creates automatically.

### Exchange-mode triggers

Instead of subscribing to a pre-existing, manually-bound queue (`queue=`/`queues=`), a DAG can subscribe directly to a topic exchange. When `exchange=` is given, the provider owns the RMQ infrastructure end-to-end — no manual queue creation, no external YAML route table:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.watcher.decorators import rmq_trigger

# Jetstat-shaped routing keys: id × status cross-product
@rmq_trigger(
    exchange="jetstat.airflow",
    routing_key_ids=["670f877702775c2de8325b1f"],
    routing_key_status="succeeded",   # defaults to "*" = any status
)
@dag(dag_id="jetstat_succeeded", schedule=None)
def jetstat_succeeded_dag():
    @task
    def process(**context):
        conf = context["dag_run"].conf
        print(conf["routing_key"])  # "670f877702775c2de8325b1f.succeeded"
    process()

jetstat_succeeded_dag()

# Literal routing keys of any shape (not tied to the id/status form)
@rmq_trigger(exchange="some.other.exchange", routing_keys=["region.eu.alert"])
@dag(dag_id="region_alerts", schedule=None)
def region_alerts_dag():
    ...
```

Both forms can be combined on the same call — the final routing key set is the union of `routing_keys` and the `routing_key_ids` × `routing_key_status` cross-product.

**What the provider provisions automatically** on every reconcile cycle:

- The exchange itself (topic, durable, with an `alternate-exchange` for unroutable messages)
- A dedicated queue `rmq_watcher.sub.{dag_id}` — **one shared queue per DAG**, consumed exactly like any `queue=` subscription
- Bindings between that queue and the exchange, kept in sync with the routing keys currently declared in the decorator (diffed against RabbitMQ's actual binding state via the Management HTTP API — not against anything stored in the Airflow DB)
- Safety nets: unroutable messages land in `{exchange}.unrouted` (TTL 8h); every routed message is mirrored into `{exchange}.log` (catch-all `#` binding, TTL 8h) for downstream logging/auditing

**Connection extra — `management_url`:** the broker's Management HTTP API endpoint. Exchange-mode needs it to read current bindings (AMQP 0-9-1 has no "show my bindings" operation), and the liveness watchdog uses it for subscriptions of every type. Add it to the same Airflow Connection used for AMQP:

```json
{
  "management_url": "https://rabbitmq.example.com"
}
```

What the Management API replies is shaped by the rights of the account that asked: a user tagged `management` is shown only the channels of its own connections, while `monitoring` and `administrator` see the whole vhost. With the minimum-privilege account described above, the watcher therefore sees its own consumers and no one else's.

The same `login`/`password` from the connection are reused for the Management API call. If `management_url` is not set, bind-diff is skipped on every cycle (logged as ERROR) — the queue is still declared and consumed normally, but bindings never get created/updated.

`management_url` also selects the liveness probe for **every** subscription, `queue=` and `exchange=` alike. With it, the check reads `GET /api/consumers/{vhost}`, can name individual consumer tags and reports the Broker consumers number. Without it, the check falls back to a passive queue declare: that vouches for — or condemns — all consumers of the `conn_id` at once and returns no count, so the Broker consumers column stays `—` even when the verdict is positive. After two consecutive Management API failures on one `conn_id` (wrong URL, credentials without the `management` tag, 404, timeout) the check switches to that same passive declare rather than staying blind. The same two-cycle bound covers a `conn_id` whose Airflow connection cannot be read at all — renamed, deleted while subscriptions still name it, or a secrets backend that keeps refusing: the check then asks the pooled connection itself, which needs no metadata row.

**No stacking — one DAG, one exchange.** Multiple `@rmq_trigger(exchange=...)` decorators on the same DAG raise `ValueError` — they would all resolve to the same `rmq_watcher.sub.{dag_id}` queue, and the last one parsed would silently win. The error fires as soon as the DAG object actually exists: immediately at decoration time for a direct `DAG` instance, or when the `@dag(...)` factory is called (e.g. `jetstat_succeeded_dag()` at the end of the file, as in the example above) for TaskFlow style — either way, always during import, before the DAG is registered. Use a single decorator call with the union of routing keys, or subscribe to multiple exchanges across multiple DAGs. To consume from several exchanges on the same DAG, fall back to `queue=`/`queues=` with manually created and bound queues.

**RabbitMQ permissions (exchange-mode only):** in addition to the `rmq_watcher\..*` pattern already required for cooldown, the Airflow RMQ user needs:

```
# configure: declare the exchange / alternate-exchange / its queues
rabbitmqctl set_permissions -p <vhost> <user> "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted|\\.log)?|...)$" \
  "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted|\\.log)?|...)$" \
  "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted)?|...)$"
```

`configure`/`write` are needed on `{exchange}(.unrouted|.log)?`; `read` is additionally needed on `{exchange}(.unrouted)?` because binding a queue *from* an exchange requires read access on the source exchange, not just configure on the destination queue. Replace `jetstat.airflow` with whatever name is actually passed to `exchange=`.

**Migrating from `queue=` to `exchange=`:** switching an existing subscription does not clean up after itself — the old, manually-created queue is **not** deleted automatically and is left without a consumer once the DAG file is redeployed with `exchange=`. Remove it manually once the migration is confirmed working.

**Renaming the DAG:** changing `dag_id` provisions a new `rmq_watcher.sub.{new_dag_id}` queue/bindings on the next reconcile cycle. The old `rmq_watcher.sub.{old_dag_id}` becomes orphaned (its subscription metadata no longer exists in any parsed DAG file — see ADR-0005) and is **not** removed automatically. Delete it manually.

**Monitoring:**

- RabbitMQ Management UI — `rmq_watcher.sub.{dag_id}` should show `consumer count > 0` when the DAG's subscription is active
- Airflow logs — WARNING for orphaned `rmq_watcher.sub.*` queues/bindings (with a `rabbitmqadmin delete queue ...` hint); ERROR for a skipped bind-diff (Management API unreachable) or for an exchange property conflict (`PRECONDITION_FAILED` — the exchange name is already used by something else with different properties)

**Rollback:** remove `exchange=`/`routing_keys=`/`routing_key_ids=`/`routing_key_status=` from the decorator and redeploy. `rmq_watcher.sub.{dag_id}` becomes orphaned — a WARNING appears in the logs, the TTL (8h) caps unbounded growth, and manual cleanup follows the hint in the WARNING text. The exchange itself and its `.unrouted`/`.log` queues are **not** touched by rollback (other DAGs may still be using them).

### Payload passed to the DAG

```python
conf = context["dag_run"].conf
# Immediate trigger (cooldown=0 or no cooldown):
# {
#     "source":          "immediate",
#     "body":            "<UTF-8 decoded message body>",
#     "headers":         {"key": "value", ...},
#     "routing_key":     "orders.created",
#     "queue":           "orders",
#     "subscription_id": 42,
# }
# Exchange-mode triggers (exchange=) reuse this exact "immediate" shape — "queue" is
# always rmq_watcher.sub.{dag_id} (not the exchange name), and "routing_key" is the
# actual matched routing key (e.g. "<id>.<status>" for the routing_key_ids form).
#
# Cooldown trigger (fired after TTL expires in rmq_watcher.fire):
# {
#     "source":          "cooldown",
#     "dag_id":          "<dag_id>",
#     "body":            "",        # empty — original message body not preserved
#     "headers":         {},        # empty — original headers not preserved
#     "routing_key":     "<dag_id>",
#     "queue":           "rmq_watcher.fire",
#     "subscription_id": None,
# }
```

### Subscription Management

| Method | Description |
|---|---|
| `@rmq_trigger` decorator | Infrastructure as Code — subscription lives in the DAG file, managed by git |
| Airflow UI at `/rmq-watcher/subscriptions` | Create, edit, toggle, delete (UI-created subscriptions only) |
| Direct DB insert | For automation via Terraform / scripts (`source='ui'`) |

`dag_file` subscriptions are **read-only** in the UI — reconciliation overwrites DB from code every 60 s. Only the `enabled` toggle can be changed via UI for code-managed subscriptions.

Exchange-mode subscriptions show up in the UI like any other `dag_file` subscription — by their queue name (`rmq_watcher.sub.{dag_id}`) only. The `exchange`/`routing_keys` metadata is not displayed there; the DAG source file is the single source of truth for that.

Any subscription whose `dag_id` doesn't correspond to an active Airflow DAG is marked on the page with a `⚠ dag not found` badge. This is a **one-sided** signal: its presence means a real problem — a matching message will be ACKed without triggering anything — but its **absence guarantees nothing**. Three reasons the badge can miss a broken subscription: `is_paused` is not checked, so a paused DAG never gets the badge even though a matching message is still ACKed without triggering a run, exactly like a missing DAG (see [ADR 0006](docs/adr/0006-badge-dag-lookup-not-unified-with-sync-trigger.md)); a just-added DAG may be badged for a short while until the Scheduler parses it; and a just-deleted or renamed DAG conversely stays un-badged for a while until `DagModel.is_active` catches up. The last two are expected lag, not bugs.

### Connection Resilience

The watcher holds long-lived AMQP connections inside the Scheduler, so it treats a broker restart, a silently dropped TCP link, a resource alarm or an unavailable database as normal weather: once the external problem is gone, subscriptions come back on their own. The design is written up in [ADR 0007](docs/adr/0007-connection-liveness-two-tier-check.md); the short version is four layers.

**1 — Heartbeats and per-call timeouts.** Every async connection carries a `heartbeat` (30 s by default), and every AMQP call — connect, `channel()`, declare, bind, publish — runs under `connect_timeout`/`rpc_timeout` (see [Connection Timing](#connection-timing)). A link that dies turns into an exception instead of a call that waits forever. The connect carries the watcher's own bound rather than the library's: `connect_robust(timeout=…)` covers the TCP connect and the AMQP handshake and stops there, while the wait that follows it — for the broker to declare the connection unblocked — is outside it, and under a resource alarm that wait lasts as long as the alarm does. A connect that runs out of time is not cancelled but left in flight, and its connection stays in the pool: cancelling it would poison the connection object for good, and dropping it would leave an open connection to the broker that nothing can close. Each `conn_id` builds its connections under a lock of its own, and that lock covers starting a connect rather than waiting for one: however many callers want the connection, the broker is asked for a single attempt, and they wait for it side by side rather than one behind another. A broker that stopped answering therefore costs each caller one `connect_timeout`, and holds up the `conn_id`s that use it and no others. A pooled connection is handed out only while it is usable — not merely unclosed: a reconnect that never finished leaves an object with no transport under it, which reports itself open and fails every call made through it, so it is replaced rather than handed to the next caller.

**2 — Liveness watchdog.** Every reconcile cycle the watcher asks the broker whether its consumers are still registered. Each subscription attaches under its own consumer tag `rmq_watcher.{hostname}.{pid}.{sub_id}.{nonce}` (the cooldown fire consumer uses `fire` in place of `{sub_id}`), and the check looks for that exact tag — a foreign consumer on the same queue, a second DAG subscribed to it, or another scheduler replica in HA cannot vouch for ours, and the `{nonce}`, fresh for every attach, keeps a ghost consumer left on the broker by a `close()` that never returned from vouching for the task that replaced it. The tag is claimed — and the subscription reported as `listening` — only once the broker has confirmed the registration, which itself runs under `rpc_timeout`: a `basic.consume` that never comes back would otherwise leave the check looking for a tag the broker never held. With `management_url` configured the tag is looked up in `GET /api/consumers/{vhost}`; without it, the watcher falls back to a passive `queue_declare` probe on a separate channel, whose verdict covers every consumer of that `conn_id` at once. Either way the tag is also checked against the channel the client holds it on: RabbitMQ cancels a consumer of ours — a deleted queue, a quorum-queue or stream leader change, the node hosting a classic queue restarting under a client connected to another node — with a `basic.cancel` that closes neither the connection nor the channel, so every probe of that connection keeps succeeding while nothing consumes; the tag missing from our own channel is what condemns it. Two negative checks in a row (never less than two reconcile intervals) cancel exactly those subscriptions whose own tag the broker did not confirm and drop the connection they share — restarting a task alone would hand it the same zombie connection out of the pool. The status row does not wait for the second check: a `conn_id` whose consumers the broker denies reads `error` with the reason and the check number straight away. The confirmed subscriptions of that `conn_id` are neither cancelled nor restarted: the shared connection is dropped underneath them, they reconnect through their own retry loop and surface the drop as a transient consumer error (a WARNING names them). The fire consumer goes down with the connection it runs on, because it holds the connection object it was handed at startup for its whole life; the reconcile cycle starts it again — on the connection the pool holds now — when that object was replaced under it, and moves it when the cooldown `conn_id` moves. A `conn_id` whose tasks are alive but never reach `listening` for two cycles offers the check no candidate at all, so the connection itself is asked whether it still answers an RPC: silence recreates it, an answer leaves it in place and the row says the fault is downstream of the connection — a subscription stuck in `error` because its trigger keeps failing is attached to a healthy broker connection. A Management API failure is "no data", not a verdict: it logs a WARNING, leaves the counters alone and keeps the stored status — for two cycles, after which the passive declare takes over; an unreadable Airflow connection is treated the same way. A hung AMQP probe, on the other hand, counts as dead. The same `conn_id` is never recreated more often than once every 5 cycles; a verdict held back by that limit is logged and stored as `degraded`.

**3 — Cycle timeout.** The whole reconcile iteration runs under a budget of `max(reconcile_interval × 3, 300)` seconds. Exceeding it stops the manager and recreates the event loop after 30 s, with an ERROR naming the phase (`migrate` / `scan` / `sync` / `read subs` / `reconcile`) and how long it took. The budget is deliberately generous — hitting it pauses consumption on every `conn_id` — while the per-call timeouts of layer 1 catch a stuck network operation much earlier and only for the subscription that owns it. Each blocking step of the cycle (the DAG-file scan, the sync to the database, the subscription read) has a 60-second bound of its own and only ever one attempt in flight, so an unresponsive database ends a single cycle rather than costing the loop, and cannot fill the cycle's four-worker pool with stuck copies of the same call.

**4 — Publishing on its own connection.** Cooldown publishing uses a separate, lazily opened connection per `conn_id`. Under a memory or disk alarm RabbitMQ blocks publishing connections by stopping to read from the socket, which would otherwise take the acknowledgements of every consumer on the same connection down with it — and a connection the broker has flagged as blocked stops writing on the client side too (`aiormq` holds its own writer until `connection.unblocked` arrives), so the split is what keeps `basic.ack` moving: the consuming connection publishes nothing and is never flagged. What the split does not cover is a connection opened *during* an alarm, which does not become usable until the alarm clears — hence the client-side bound on connect in layer 1. A placeholder the broker returns as unroutable — the pending queue is missing, so nothing would ever fire the DAG after the window — puts the delivery back on the queue instead of acknowledging it. Two publish timeouts in a row recreate the publishing connection alone and leave consumption untouched, and a publish that keeps failing pauses that subscription for a growing interval (1 s, doubling up to 60 s) before reattaching, which stretches how long an alarm can last before it costs the message: every failed attempt requeues the delivery and spends one of the quorum queue's 20 redeliveries, and the pauses spread those 20 over roughly a quarter of an hour instead of seconds. An alarm outlasting that still ends with the broker dead-lettering or dropping the delivery. The cooldown fire consumer runs on the lowest `conn_id` among the cooldown subscriptions, which keeps it from being moved by the order rows happen to come back in; cooldown subscriptions spread over several `conn_id`s are logged as an ERROR, because the pending queues live on one broker only.

**Airflow Variables:**

| Variable | Default | Meaning |
|---|---|---|
| `rmq_watcher_reconcile_interval` | `60` | Seconds between reconcile cycles |
| `rmq_watcher_cycle_timeout` | `max(interval × 3, 300)` | Seconds one cycle may take before the event loop is recreated |

Both are re-read at the start of every cycle in a thread pool under a short timeout of their own, so a changed Variable takes effect on the next cycle; a read still stuck in a worker blocks the next one from starting, and an unreachable database leaves the last known values in place instead of stalling the loop.

**What the Subscriptions page shows.** The Connections block at `/rmq-watcher/subscriptions` lists one row per `conn_id`:

| Column | Meaning |
|---|---|
| Status | `connected` when the broker confirmed every one of our consumer tags on this `conn_id`; `error` on a negative verdict — a consumer the broker did not confirm, no running task of this `conn_id` at all, a failed connection attempt, or tasks that are alive while not one of them reaches `listening` and the connection still answers an RPC, which puts the fault downstream of the connection (the reason is in Last Error); `degraded` when a negative verdict was held back by the recreation rate limit and the connection was left in place instead of being recreated again so soon; `unknown` for a `conn_id` no liveness check has ever reached a verdict on. The status follows the verdict alone — a `conn_id` with no verdict keeps whatever is stored, and the number of tasks the watcher started never makes a row green |
| Consumers | How many live consumer tasks the watcher holds for this `conn_id`, the cooldown fire task included when it runs there |
| Broker consumers | How many consumers the broker reports on the queues of the subscriptions that were actually probed — those whose own status is `listening` — plus `rmq_watcher.fire` when the fire consumer runs on this `conn_id`. `—` means there is no number at all: the passive-declare fallback never returns one, so a connection without `management_url` keeps the em dash even when the verdict is positive. A number that differs from the previous column is highlighted — lower means a subscription that has not attached yet (still connecting, or backing off after an error) and is therefore not probed, higher means another scheduler replica using the same credentials or — on a `monitoring`/`administrator` account, which is what makes foreign channels visible at all — other clients on the same queues |
| Last reconcile | Age of the last cycle for this `conn_id`, flagged when it is older than two reconcile intervals — the loop is stuck, restarting, or not running at all |

A row is written for **every** `conn_id` that appears in the subscription list, including one whose tasks all died. The counts and `last_reconcile_at` move on every cycle, which is what proves the loop is alive; `status` keeps its last stored value whenever the check produced no data.

The Status column of the Subscriptions table further down the page is a different one: it reports a single subscription's own task — `connecting`, `listening`, `error` or `disconnected` — while the Connections block above reports the `conn_id` those tasks share (a grouped row shows how many of the group's subscriptions are `listening`).

**Schema migration.** The status table carries `broker_consumer_count` and `last_reconcile_at` alongside its other columns, and an idempotent migration adds whatever a given database lacks: it inspects the live table and issues `ALTER TABLE … ADD COLUMN` only for what is missing. The plugin runs it once at load, and the watcher loop retries it at the start of each cycle (one attempt in flight, with a growing backoff between failures) until it succeeds, so a database that was unreachable at load time does not leave the process running against an outdated table. While the table is behind, the Connections block shows a short notice instead of failing to render the page.

### Delivery Guarantee (immediate mode)

For subscriptions with `cooldown=0` the watcher acknowledges a delivery only **after** `trigger_dag` has created the DAG run:

- A failed trigger is NACKed with requeue, logged as a WARNING and retried with a growing backoff (1 s, doubling up to 60 s); the subscription status becomes `error` with the reason, so a subscription that stopped starting DAG runs is visible on the page.
- Deduplication rests on the producer. When the message carries an AMQP `message_id`, the run id is deterministic (`rmq__{queue}__{message_id}`, sanitized and truncated with a digest when it does not fit), so a redelivery lands on the run it already produced and is acknowledged as a duplicate. Two *different* messages sharing one `message_id` therefore collapse into a single DAG run. Without `message_id` the run id carries a timestamp instead and a redelivery starts the DAG again.
- **Boundary of the guarantee:** a DAG that is paused, inactive or missing ends in a terminal ACK — the event is dropped on purpose, with a WARNING in the log. NACKing it would turn a paused DAG into an accumulator of redeliveries.
- Messages that do not match the filter are NACKed with requeue (see [ADR 0002](docs/adr/0002-four-consume-loops-not-unified.md)) and go back to the head of the queue.

**`prefetch` is deliberately not set.** Because filter misses are requeued and return to the head of the queue, any finite prefetch window would eventually fill up with them and consumption would stop for good — while the status still read `connected`. The cost is accepted explicitly: the number of unacknowledged deliveries is unbounded, so a queue with a large backlog is read into scheduler memory, and the oldest delivery can hit the broker's `consumer_timeout` (30 minutes by default), after which RabbitMQ closes the channel with `PRECONDITION_FAILED` and returns the whole window to the queue. If large backlogs are expected on a watched queue, raise `consumer_timeout` in the RabbitMQ configuration.

### Best Practices

- Use a **dedicated queue** per DAG trigger (e.g. `orders.airflow-trigger` separate from `orders`). Avoids NACK hot-loops on quorum queues and interference with other consumers.
- To pause message consumption without stopping the DAG: **toggle the subscription off** in the UI rather than pausing the DAG. Pausing the DAG acks messages silently.
- In **multi-scheduler HA** deployments each active scheduler runs its own consumer, which may cause duplicate runs. The protection is a deterministic `run_id`: subscriptions with `cooldown > 0` always have one (`rmq_cooldown__{dag_id}__{message_id}`), and immediate ones have it whenever the producer sets an AMQP `message_id` (`rmq__{queue}__{message_id}`) — in both cases the second replica's trigger lands on the DAG run the first one already created. Without a `message_id` an immediate run id carries a timestamp instead and both replicas start the DAG; set `max_active_runs=1` as a lightweight mitigation there.

---

## Example DAGs

The package includes several example DAGs in `docs/example_dags/`. All examples use the **TaskFlow API** (`@dag` / `@task` decorators) and demonstrate how to **process consumed messages** in downstream tasks via XCom.

| DAG | Description |
|---|---|
| `rmq_example_basic` | Publish, wait, consume, process messages, cleanup |
| `rmq_publish_advanced` | Advanced publishing with all AMQP properties, batch messages, topic exchange |
| `rmq_consume_with_filters` | Header filters, body-path filters, callable filters, QoS — with per-step message processing |
| `rmq_sensor_deferrable` | Deferrable sensor in pull mode with header filtering and message processing |
| `rmq_sensor_push` | Deferrable sensor in **push mode** — broker delivers messages instantly via `basic_consume` |
| `rmq_watcher_demo` | **RMQ Watcher Plugin** — DAG triggered reactively by RabbitMQ messages via `@rmq_trigger`; also runs on daily schedule |
| `rmq_pipeline_start` / `rmq_pipeline_finish` | Pipeline lock pattern — prevent concurrent executions |
| `rmq_dlq_setup` | Dead Letter Queue infrastructure setup with DLX, TTL, exchange-to-exchange bindings |
| `rmq_watcher_orders_payments_cooldown` | Cooldown trigger shared across multiple queues — debounce bursts into one DAG run |
| `rmq_watcher_jetstat_report_succeeded` | Exchange-mode trigger — subscribe directly to a topic exchange (Jetstat id × status routing keys) |

---

## Repository Structure

```
airflow-provider-rmq/
├── airflow_provider_rmq/
│   ├── __init__.py                  # Provider metadata & get_provider_info()
│   ├── hooks/
│   │   └── rmq.py                   # RMQHook
│   ├── operators/
│   │   ├── rmq_publish.py           # RMQPublishOperator
│   │   ├── rmq_consume.py           # RMQConsumeOperator
│   │   └── rmq_management.py        # RMQQueueManagementOperator
│   ├── sensors/
│   │   └── rmq.py                   # RMQSensor
│   ├── triggers/
│   │   └── rmq.py                   # RMQTrigger
│   ├── utils/
│   │   ├── amqp.py                  # build_amqp_connection(), get_amqp_timeouts(), call_with_timeout()
│   │   ├── executor.py              # BoundedExecutor (thread pool for blocking calls)
│   │   ├── filters.py               # MessageFilter
│   │   ├── management.py            # Management HTTP API client (bindings, queue consumers)
│   │   ├── metrics.py               # Stats counters, no-op without statsd
│   │   └── ssl.py                   # build_ssl_context()
│   └── watcher/
│       ├── decorators.py            # @rmq_trigger
│       ├── subscription_builder.py  # build_subscriptions(), has_exchange_conflict()
│       ├── subscription_form.py     # parse_cooldown(), parse_filter_data() (UI form parsing)
│       ├── models.py                # RMQSubscription, RMQConnStatus, WatcherSession
│       ├── tunables.py              # Airflow Variables tuning the watcher, with their defaults
│       ├── consumer.py              # RMQConsumerManager
│       ├── orphan_tracker.py        # OrphanTracker (cooldown + exchange-mode orphan detection)
│       ├── listener.py              # RMQWatcherListener (Scheduler Listener)
│       ├── views.py                 # RMQWatcherView (Flask-AppBuilder UI)
│       └── plugin.py                # RMQWatcherPlugin (AirflowPlugin)
├── docs/
│   ├── adr/                         # Architecture decision records
│   └── example_dags/                # Example DAGs
├── tests/                           # Unit tests
├── CHANGELOG.md
├── pyproject.toml
└── readme.md
```

---

## Running Tests

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run all tests
pytest tests/

# Run specific test module
pytest tests/test_trigger.py -v
```

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
