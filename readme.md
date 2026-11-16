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
- Publishing messages to exchanges and queues with publisher confirms
- Consuming messages with header-based and callable-based filtering
- Waiting for specific messages with sensors (`poke` and `reschedule` modes, and deferrable mode)
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
| aiormq | `>=6.8.0, <7.0.0` |
| tenacity | `>=8.0.0` |
| httpx | `>=0.27` |
| Python | `>=3.10` |
| RabbitMQ | `>=4.3` |

On RabbitMQ 4.3+ a requeueing `basic.nack` does not count against a quorum queue's `delivery-limit`; on earlier versions repeated requeues can dead-letter or drop messages, so they are not supported.

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

Four optional **Extra** keys control AMQP timing. All of them are plain JSON values next to the SSL settings:

```json
{
  "heartbeat": 30,
  "connect_timeout": 15,
  "rpc_timeout": 30,
  "blocked_connection_timeout": 300
}
```

| Key | Default | Used by | Meaning |
|---|---|---|---|
| `heartbeat` | `30` on the async path, `600` in `RMQHook` | every AMQP connection | Seconds between AMQP heartbeat frames. On the async path `aiormq` allows three missed intervals and checks every half-interval, so a broken connection becomes an exception in roughly `3 × heartbeat` seconds — about 90 s at the default. The watcher's robust connection then reconnects, and `RMQTrigger` closes the attempt and starts a new one |
| `connect_timeout` | `15` | RMQ Watcher, `RMQTrigger` | Seconds allowed for establishing an async connection: the TCP connect and the AMQP handshake |
| `rpc_timeout` | `30` | RMQ Watcher, `RMQTrigger` | Seconds allowed for a single async AMQP call — `channel()`, declare, bind, publish; in `RMQTrigger` also `basic_get`, `basic_consume`, ACK/NACK and `basic_cancel` |
| `blocked_connection_timeout` | `300` | `RMQHook` | Seconds a connection blocked by the broker (resource alarm) may stay blocked before the hook drops it with an exception. `0` drops it as soon as the broker blocks it and is logged as a WARNING; `null` waits without a deadline |

`heartbeat` is the same key on both paths. The paths differ in two things:

- **Default.** `RMQHook` uses `600`: pika's `BlockingConnection` services heartbeats only inside pika calls, so a short interval drops the link of a task that works for more than about a minute between calls. The async path uses `30`, because the watcher holds a connection for days and has to notice a dead link quickly.
- **`null`.** `RMQHook` passes `null` in `heartbeat` or `blocked_connection_timeout` to pika as is: the heartbeat follows the broker's proposal, and a blocked connection waits without a deadline. The async path reads `null` in `heartbeat` as an unusable value and uses the default with a WARNING.

Setting `"heartbeat": 0` is accepted as a deliberate opt-out and logged as a WARNING: with heartbeats off, a link that dies silently (a broker restart behind a proxy that keeps the client half open) stays undetected.

`connect_timeout` and `rpc_timeout` have no counterpart in `RMQHook` — pika is driven by its own retry logic there. Every key takes a number or a numeric string (`"30"`). A value that is not a finite number, or is below the key's minimum, falls back to the default with a WARNING. The minimum is `0` for `heartbeat` and `blocked_connection_timeout` and anything above zero for `connect_timeout` and `rpc_timeout`; a `heartbeat` between zero and one second or above `65535` (the largest value AMQP can carry) also falls back to the default.

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
| `qos` | `dict \| None` | `None` | No | QoS settings: `{"prefetch_count": 10}` — the number of unacknowledged messages delivered to a consumer, the only key sent to the broker. A true `global_qos` is not sent and the hook logs a WARNING: per-channel prefetch is the deprecated RabbitMQ feature `global_qos`, denied by default since RabbitMQ 4.3. `prefetch_size` other than `0` raises `ValueError`: RabbitMQ does not implement prefetch size limits ([AMQP 0-9-1 conformance](https://www.rabbitmq.com/docs/specification)) |
| `retry_count` | `int` | `3` | No | Number of connection retry attempts |
| `retry_delay` | `float` | `1.0` | No | Base delay (seconds) between retries (exponential backoff) |
| `publisher_confirms` | `bool` | `False` | No | Put every channel the hook opens, a reopened one included, into publisher confirms mode: `basic_publish` waits for the broker to acknowledge each message |

#### Key Methods

| Method | Description |
|---|---|
| `get_channel()` | Returns a pika `BlockingChannel` (creates connection lazily; opens a new channel when the current one is closed) |
| `queue_declare(queue_name, passive, durable, exclusive, auto_delete, arguments)` | Declare a queue. `durable` defaults to `True`. An active declare (`passive=False`) with `durable=False` requires `exclusive=True`: a non-durable, non-exclusive queue relies on the deprecated RabbitMQ feature `transient_nonexcl_queues`, denied by default since RabbitMQ 4.3, and raises `ValueError` before the broker is contacted. `auto_delete` does not affect the rule. Declaring an existing queue with a different `durable` fails with 406 `PRECONDITION_FAILED`; pass the flag the queue was created with |
| `queue_delete(queue_name, if_unused, if_empty)` | Delete a queue |
| `queue_bind(queue, exchange, routing_key, arguments)` | Bind a queue to an exchange |
| `queue_unbind(queue, exchange, routing_key, arguments)` | Unbind a queue from an exchange |
| `queue_purge(queue_name)` | Remove all messages from a queue |
| `queue_info(queue_name)` | Get queue info via passive declare: a dict with `queue`, `message_count`, `consumer_count` and `exists`. A missing queue gives the same keys with both counts `0` and `exists=False` |
| `exchange_declare(exchange, exchange_type, passive, durable, auto_delete, internal, arguments)` | Declare an exchange. `durable` defaults to `True`. Declaring an existing exchange with a different `durable` fails with 406 `PRECONDITION_FAILED`; pass the flag the exchange was created with |
| `exchange_delete(exchange, if_unused)` | Delete an exchange |
| `exchange_bind(destination, source, routing_key, arguments)` | Bind exchange to exchange |
| `exchange_unbind(destination, source, routing_key, arguments)` | Unbind exchange from exchange |
| `basic_publish(exchange, routing_key, body, properties, mandatory)` | Publish a message. `mandatory=True` (default `False`) asks the broker to return a message that no queue matches. With `publisher_confirms=True` the call waits for the broker's acknowledgement and raises `pika.exceptions.UnroutableError` (message returned), `NackError` (message rejected) or `ChannelClosedByBroker` (e.g. 404 on a missing exchange, 403 without permission; the next `get_channel()` opens a new channel). Without publisher confirms the call returns once the message is written to the socket, a returned message is not reported, and a channel the broker closes surfaces on a later call |
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

Publishes one or more messages to RabbitMQ and, by default, waits for the broker to confirm each one (see [Publisher Confirms](#publisher-confirms)). Supports strings, dicts (auto-serialized to JSON), and lists.

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
| `confirm` | `bool` | `True` | No | Publisher confirms: wait for the broker to acknowledge each message and fail the task when the broker returns, rejects or does not accept one |
| `mandatory` | `bool \| None` | `None` | No | Ask the broker to return a message that no queue matches. `None` resolves after templates are rendered: `True` for the default exchange (`exchange=""` or `queue_name=`) with `confirm=True`, `False` otherwise. `True` with `confirm=False` raises `ValueError` when the operator is constructed (for a regular task, at DAG import) |

**Template fields:** `exchange`, `routing_key`, `message`

`message=None` or an empty list logs a WARNING `Message is empty, nothing to publish to exchange='…' routing_key='…'`; the task succeeds without connecting to the broker.

#### Publisher Confirms

With `confirm=True` each message waits for the broker's acknowledgement before the next one is sent. When the broker does not accept a message, the task fails with an `AirflowException` that names the message by its position in the batch, the exchange and the routing key; the pika exception is kept as `__cause__`.

| Broker response | Failure text |
|---|---|
| Returned: no queue matches the routing key and `mandatory` is on (e.g. `queue_name=` names a missing queue) | `message 3 of 5 to exchange='' routing_key='orders' was returned by the broker: 312 NO_ROUTE` |
| Rejected (nack), e.g. a full queue with `x-max-length` and `x-overflow=reject-publish` | `message 3 of 5 to exchange='' routing_key='orders' was rejected by the broker (nack)` |
| Channel closed: 404 on a missing exchange, 403 without permission | `message 1 of 1 to exchange='events' routing_key='orders.new' was not published, broker closed the channel: 404 NOT_FOUND - no exchange 'events' in vhost '/'` |

- **Default `mandatory`.** On the default exchange a message to a missing queue is always lost, so `mandatory` is on there. On a named exchange no queue bound for the key can be a normal state, so `mandatory` is off; set `mandatory=True` to fail on unrouted messages there too.
- **What an acknowledgement means.** The broker has taken the message. An unrouted message with `mandatory=False` is acknowledged too, and dropped. A persistent message (`delivery_mode=2`) is on disk at acknowledgement only in durable queues; a transient queue does not write it to disk.
- **Returned messages in the log.** pika's WARNING about a returned message, which carries the message's headers, `correlation_id`, `reply_to`, `message_id` and the start of its body, is kept out of the log, since headers and bodies may hold credentials or personal data. The task failure names the message, the exchange, the routing key and the reply code.
- **Retries publish again.** Messages before the failing one are already in the broker, and a task retry publishes the whole batch again: publishing is at-least-once, so consumers should tolerate duplicates, for example by a business key carried in each message body. `message_id` does not tell messages apart: it is set once on the operator and is not a template field, so every message of the task, in every run, carries the same value. The publish call itself is not retried; only the connection is.
- **Cost and waiting.** Each message costs one round trip to the broker — about a millisecond on a local network, more for persistent messages and quorum queues — so a large batch publishes noticeably slower. The wait for an acknowledgement has no deadline of its own: a quorum queue that has lost its majority holds the acknowledgement on a live, unblocked connection, and the task waits until its `execution_timeout`. Set `execution_timeout` on publish tasks.

With `confirm=False` the task does not wait for the broker: a returned or rejected message goes unnoticed, a publish to a missing exchange can leave the task green, and a channel the broker closes surfaces on a later message as the pika exception itself.

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

Consumes messages from a RabbitMQ queue. Matching messages are ACKed and returned via XCom. A message the filter read and rejected is NACKed with `requeue=True` by default, or ACKed and gone for good when `ack_unmatched=True`; a message the filter could not be evaluated on is returned to the queue whatever the parameter says. See [Filter misses and `ack_unmatched`](#filter-misses-and-ack_unmatched).

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `queue_name` | `str` | — | **Yes** | Name of the queue to consume from |
| `rmq_conn_id` | `str` | `"rmq_default"` | No | Airflow connection ID |
| `max_messages` | `int` | `100` | No | Maximum number of messages to consume per execution |
| `filter_headers` | `dict[str, Any] \| None` | `None` | No | Dict of AMQP headers that a message must match. Supports `body.*` keys for JSON body filtering (e.g., `{"body.data.status": "active"}`) |
| `filter_callable` | `Callable[[Any, str], bool] \| None` | `None` | No | Custom filter function `(properties, body_str) -> bool` |
| `qos` | `dict \| None` | `None` | No | QoS settings: `{"prefetch_count": 10}` — the only key sent to the broker. `prefetch_size` other than `0` raises `ValueError` when the operator is created; a true `global_qos` is not sent to the broker, and the hook logs a WARNING in the task. See [`RMQHook`](#rmqhook) |
| `ack_unmatched` | `bool` | `False` | No | ACK a message the filter rejected instead of returning it to the queue. The message leaves the broker for good — DLX does not fire on an acknowledgement. Must be a `bool` |

**Template fields:** `queue_name`

**Returns:** `list[dict]` — list of matched messages, each with keys: `body`, `headers`, `routing_key`, `exchange`

> **`ack_unmatched=True` deletes other consumers' messages.** On a queue a watcher subscription or any other consumer also reads, every message this task's filter rejects is acknowledged and gone: the neighbour keeps saying `listening`, reports no error, and its DAG simply never runs. The Subscriptions page badges a queue two subscriptions share, but it knows nothing about operators and sensors — on their side this paragraph is the only guard. Turn the parameter on only for a queue this task owns.

**One batch per task run.** `execute()` makes exactly one `consume_messages` call, so the task sees at most `max_messages` messages from the head of the queue and nothing behind them — with `ack_unmatched=True` too. What the parameter changes is that the rejected ones leave the queue for good: the next run reads the next batch, and a message sitting behind 150 foreign ones is found after two or three runs instead of never. The miss counters live for one run and therefore for one batch, so the threshold of a hundred consecutive misses is out of reach whenever `max_messages` is smaller; the operator gets a batch-level WARNING instead — that nothing in the batch matched, how many of it were dropped, or both in the same line. Only what the filter reached a verdict on is counted there: a body it could not read says nothing about `filter_data`.

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
| `durable` | `bool` | `True` | No | The declared queue or exchange survives a broker restart. Applies to `declare_queue` and `declare_exchange`. Declaring an existing queue or exchange with a different `durable` fails with 406 `PRECONDITION_FAILED`; pass the flag the resource was created with |
| `exclusive` | `bool` | `False` | No | Queue is exclusive to this connection. The operator closes its connection when the task finishes, so an exclusive queue lives only while the task runs |
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

**Queues are durable.** `action="declare_queue"` with `durable=False` and `exclusive=False` raises `ValueError` when the operator is created: such a queue relies on the deprecated RabbitMQ feature `transient_nonexcl_queues`, denied by default since RabbitMQ 4.3. For a regular task the error surfaces at DAG import, for a mapped task (`.partial(...).expand(...)`) when the task runs. `queue_name` in the message may be the unrendered template. Declare the queue with `durable=True`, the default.

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

Waits for a message in a RabbitMQ queue that matches optional filter conditions. Supports the `poke` and `reschedule` modes of `BaseSensorOperator` and deferrable mode.

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
| `timeout` | `float` | `604800` | No | Max seconds to wait before failing (inherited from BaseSensorOperator). With `deferrable=True` it bounds the whole wait in the triggerer (see [Deferrable Mode](#deferrable-mode)) |
| `mode` | `str` | `"poke"` | No | `"poke"` or `"reschedule"` (inherited from BaseSensorOperator, which checks the value). `"pull"` and `"push"` are accepted as deprecated spellings of `trigger_mode` with an `AirflowProviderDeprecationWarning`, also as `"mode"` in the DAG's or the sensor's `default_args`; an explicit `trigger_mode` wins over the one from `default_args` |
| `trigger_mode` | `Literal["pull", "push"] \| None` | `None` (= `"pull"`) | No | How the trigger reads the queue when `deferrable=True`: `"pull"` = `basic_get` in a loop, `"push"` = broker-pushed via `basic_consume`. Ignored without `deferrable=True`. Passing it together with `mode="pull"`/`"push"` raises `ValueError` |
| `message_wait_timeout` | `float \| None` | `None` | No | Max seconds to wait for a matching message in push mode. `None` = wait until the sensor's `timeout` (see [Deferrable Mode](#deferrable-mode)). Only valid with `trigger_mode="push"`. Supports Jinja templates and XCom |
| `ack_unmatched` | `bool` | `False` | No | ACK a message the filter rejected instead of returning it to the queue. The message leaves the broker for good — DLX does not fire on an acknowledgement. Must be a `bool` |

**Template fields:** `queue_name`, `message_wait_timeout`

**Returns:** `dict | None` — matched message with keys: `body`, `headers`, `routing_key`, `exchange`

#### Deferrable Mode

When `deferrable=True`, the sensor defers execution to the Airflow triggerer process using `RMQTrigger`. This frees the worker slot while waiting for a message, which is more resource-efficient for long waits.

**`timeout` bounds the whole wait.** The sensor hands its own `timeout` to the deferral, so the wait in the triggerer — every reconnect of the trigger included — ends after `timeout` seconds. The scheduler then ends the task with `TaskDeferralError("Trigger/execution timeout")`: the task is **FAILED** and retried as the DAG says, every retry waiting the full `timeout` again, or **SKIPPED** with `soft_fail=True`. `execute_complete` is not called. A `message_wait_timeout` longer than `timeout` never comes into play: the task fails by `timeout` instead of being skipped. A `timeout` that reaches past the year 9999 leaves the deferred wait without a limit; `float("inf")` is rejected by `BaseSensorOperator` itself.

**Limitation:** `filter_callable` cannot be used with `deferrable=True` because Python callables cannot be serialized to the triggerer process. Use `filter_headers` instead.

#### Pull vs Push Mode

The `trigger_mode` parameter (only relevant with `deferrable=True`) controls how the trigger receives messages:

| | `trigger_mode="pull"` (default) | `trigger_mode="push"` |
|---|---|---|
| Mechanism | `basic_get` in a loop, `poll_interval` pause on an empty queue | `basic_consume` subscription |
| Latency | Up to `poll_interval` delay | Instant — broker delivers immediately |
| Idle cost | Polling even when queue is empty | No activity until message arrives |
| When to use | Simplicity, predictable behavior | Low-latency requirements, idle queues |

> **Timeout behaviour:** when `message_wait_timeout` expires, the sensor raises `AirflowSkipException` — the task is marked **SKIPPED** (not FAILED) and downstream tasks are skipped. No `on_failure_callback` is triggered. This makes it safe to use `message_wait_timeout` for planned stops (e.g., end of business hours) without generating false alerts. The deadline counts from the start of the wait across every reconnect, and the event comes at most `rpc_timeout` + 5 s (`CLOSE_TIMEOUT`) after it: the trigger cancels its consumer, returns the messages the broker had already sent and it had not read to the queue, and closes the connection.

> **Quorum queue note:** with the default `ack_unmatched=False` a rejected message is NACKed with `requeue=True` (`basic.nack`). A quorum queue's `delivery-limit` counts only `basic.reject` and failed deliveries, so a message the filter keeps rejecting goes back to the head of the queue and is evaluated again, indefinitely. Applies to both pull and push modes. With `ack_unmatched=True` on a dedicated queue the message is acknowledged instead and never redelivered.

> **The rest of the batch always goes back to the queue.** A poke reads up to `poke_batch_size` messages and stops at the first match. The messages behind it were never shown to the filter, so they are NACKed with `requeue=True` whatever `ack_unmatched` says, and the next poke sees them again.

> **`ack_unmatched=True` deletes other consumers' messages.** On a queue a watcher subscription or any other consumer also reads, every message this sensor's filter rejects is acknowledged and gone: the neighbour keeps saying `listening`, reports no error, and its DAG simply never runs. The Subscriptions page badges a queue two subscriptions share, but it knows nothing about operators and sensors — on their side this paragraph is the only guard. Turn the parameter on only for a queue this task owns.

**One batch per poke, and where a miss is visible.** Each poke reads one batch from the head of the queue and sees nothing behind it; with `ack_unmatched=True` the rejected messages leave the queue, so a message behind a large foreign backlog is reached after several pokes instead of never. In `poke` mode the miss counters live on the sensor instance, which `BaseSensorOperator.execute` reuses across pokes, so a hundred consecutive misses produce a WARNING however small `poke_batch_size` is. In `reschedule` mode `AirflowRescheduleException` ends the task instance after every poke and the next poke runs on a fresh object, so the counters start from zero each time. There every poke that read messages and matched none of them, or acknowledged and dropped some, writes one WARNING about its batch into the log of its own task run, as `RMQConsumeOperator` does for its run. A sensor is in `reschedule` mode when the DAG sets `mode="reschedule"`; `BaseSensorOperator.prepare_for_execution()` also switches it there by itself under `DebugExecutor`. `trigger_mode` stays as given in both cases.

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
    trigger_mode="push",
    message_wait_timeout=60,
    timeout=120,
)

# Dynamic timeout via XCom — e.g. compute remaining seconds until end of business hours
RMQSensor(
    task_id="wait_for_message",
    queue_name="events",
    deferrable=True,
    trigger_mode="push",
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

Async trigger for deferrable sensor mode. Talks to the broker over `aiormq` directly, on a connection and a channel of its own for every attempt. Typically not used directly — `RMQSensor` with `deferrable=True` creates it automatically.

#### Parameters

| Parameter | Type | Default | Required | Description |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | — | **Yes** | Airflow connection ID |
| `queue_name` | `str` | — | **Yes** | Queue to monitor |
| `filter_data` | `dict \| None` | `None` | No | Serialized filter from `MessageFilter.serialize()` |
| `poll_interval` | `float` | `5.0` | No | Seconds between polls when queue is empty (pull mode only) |
| `mode` | `Literal["pull", "push"]` | `"pull"` | No | Delivery mode: `"pull"` = `basic_get` in a loop, `"push"` = `basic_consume`. `RMQSensor` passes its `trigger_mode` here |
| `message_wait_timeout` | `float \| None` | `None` | No | Max seconds to wait in push mode, counted from the start of `run()` across every attempt and every pause between them. The timeout event comes at most `rpc_timeout` + `CLOSE_TIMEOUT` (5 s) after it: the trigger finishes the ACK or NACK of the delivery in hand, cancels its consumer, NACKs the unread deliveries back to the queue and closes the connection. Pull mode ignores it |
| `ack_unmatched` | `bool` | `False` | No | ACK a message the filter rejected instead of returning it to the queue. The message leaves the broker for good — DLX does not fire on an acknowledgement. Must be a `bool` |

#### Bounds, retries and errors

**Every broker call is bounded.** Each attempt opens a connection of its own — listed as `rmq-trigger:<queue>` in the Management UI — and a channel on it, and closes the connection whatever ends the attempt, a cancellation of the task included. The handshake runs under `connect_timeout`; opening the channel, the passive declare of the queue, `basic_get`, `basic_consume`, every ACK/NACK and `basic_cancel` run under `rpc_timeout` (see [Connection Timing](#connection-timing)). In push mode a closed channel ends the wait for a delivery at once, and every `rpc_timeout` seconds without a delivery the trigger checks that the broker has not cancelled its consumer. A resource alarm does not hold up the connect: the trigger only reads, and the broker blocks publishing connections.

**What is retried.** A step that ran out of time, a lost connection or channel (heartbeat, a broker restart, a reset socket) and a consumer the broker cancelled (the queue deleted, a quorum leader moving, a node restarting) end the attempt with a WARNING in the task log, for example:

```
RMQTrigger: connect did not complete within 15s on conn_id='rabbitmq' (attempt 3); retrying queue 'q' in 60s
```

The pause before the next attempt is 15 s, then 30 s, then 60 s for every one after that, and starts over at 15 s once an attempt has reached the wait for a message. The task stays deferred meanwhile and carries on by itself once the broker is back; the sensor's `timeout` bounds the whole wait (see [Deferrable Mode](#deferrable-mode)).

**What is an error.** The trigger fires an error event — the sensor then fails with `RuntimeError` — on any attempt for a wrong or changed password (403 during the handshake), no access to the vhost (530 on `Connection.Open`), a 403, 404 or 405 RESOURCE_LOCKED on the queue (an exclusive queue of another connection, which goes away with it), a connection the broker closed with 540 NOT_IMPLEMENTED (for instance `basic_get` on a stream queue), 501 FRAME_ERROR or 503 COMMAND_INVALID, a 406 PRECONDITION_FAILED in answer to `basic_consume` (push mode on a stream queue, which takes no consumer without a prefetch limit), and any exception other than the ones listed here. A 404 whose reply says the home node of a durable classic queue is down or inaccessible is a node restart, not a missing queue, and is retried. A refused connection and a handshake cut short (EOF, a `Connection.Close` in the middle of it, a vhost that is down) are an error on the first attempt — a wrong host or port, a broker that is not there — and retried on every later one.

**The matching message is acknowledged before the success event.** When that ACK cannot be confirmed — it timed out, or the connection went away under it — the event fires all the same, with the WARNING `ACK not confirmed … the message may be delivered again`: the frame may or may not have reached the broker, so a duplicate is possible and a lost message is not. When nothing can have been written — the connection or the channel had closed before the ACK, or the ACK failed on a closed channel before writing — the attempt is retried instead: the broker puts the message back. In push mode a closed connection or channel ends the attempt before the next buffered delivery is read: the broker has put the buffered deliveries back. A delivery taken before `message_wait_timeout` runs out gets its ACK or NACK under the full `rpc_timeout`. In push mode a match, an expired `message_wait_timeout` and a cancellation of the trigger (the sensor's `timeout`, the task cleared, the triggerer stopping) cancel the consumer and NACK the deliveries the broker had already sent back to the queue, in one `basic.nack` with `multiple=True`, before the connection closes: a quorum queue counts a delivery dropped with its connection against `delivery-limit`, and a NACK it does not. The push consumer takes no prefetch limit, so the broker sends it every ready message of the queue, and they wait in the triggerer's memory until the trigger reads them or gives them back. The success event is fired before the consumer is left and the connection closes: a trigger cancelled during that cleanup — the triggerer stopping, the task cleared, the deferral timing out — has already handed the event over. The one exception is a cancellation that lands while the ACK of the match is on its way: `aiormq` has queued the frame, so the message is acknowledged and no event carries it. When the task itself is ending (its `timeout`, a clear) nothing would take the event anyway; a triggerer stopping in exactly that window loses the message.

**A broker that never starts the handshake.** When the broker accepts the TCP connection and never sends the first handshake frame, `aiormq` leaves that socket open after the connect step is given up on, until the other side closes it. Once the pause between attempts has reached 60 s, that is at most one socket a minute for as long as the broker stays that way.

---

### MessageFilter (Utility)

**Import:** `from airflow_provider_rmq.utils.filters import MessageFilter`

Evaluates whether a RabbitMQ message matches given filter conditions. Used internally by operators and sensors.

#### Filter Modes

1. **Header filtering** (`filter_headers`): dict of key-value pairs that message headers must match.
   - Regular keys check `properties.headers` dict
   - Keys starting with `body.` traverse the JSON-parsed message body (e.g., `{"body.data.status": "active"}`)

2. **Callable filtering** (`filter_callable`): `fn(properties, body_str) -> bool`

Both can be combined (AND logic: both must pass), and they are evaluated in that order: the dict conditions first, and a callable only where the dict matched or is absent.

`matches(properties, body) -> bool` answers the yes/no question. Code that needs to tell a rejection apart from a failure to evaluate calls `evaluate(properties, body) -> tuple[FilterVerdict, str | None]` instead: the verdict is one of `FilterVerdict.MATCH`, `MISS` and `ERROR` (`from airflow_provider_rmq.utils.filters import FilterVerdict`), and the second element carries the type and text of the exception behind an `ERROR` and is `None` otherwise. The three verdicts are what the table below is about.

> **Behaviour change for code that calls `MessageFilter.matches()` directly.** An exception out of `filter_callable` is still swallowed and still reads as a non-match, but its type, text and traceback now go to the **DEBUG** level instead of WARNING. Inside the provider the visible line is put by the miss counters of the consuming loop, which name the queue and throttle the series — see below. External code that relied on the WARNING should log the failure itself inside `filter_callable`; the records go to the `airflow.task` logger, so lowering that one to DEBUG brings them back at the price of every other task log record.

---

### Filter misses and `ack_unmatched`

Every consuming loop in the provider — `RMQConsumeOperator`, `RMQSensor`, `RMQTrigger` and the watcher — reaches one of three verdicts on a message:

| Verdict | What it means | What happens to the message |
|---|---|---|
| **match** | the filter accepted it | ACKed, and the message is handed on — returned via XCom or used to start the DAG |
| **miss** | the filter read the message and rejected it | with `ack_unmatched=False` (default) NACKed with `requeue=True`; with `ack_unmatched=True` ACKed and gone |
| **error** | the filter could not be evaluated: `filter_callable` raised, or a `body.*` key was asked for on a body that does not read as JSON | NACKed with `requeue=True` always, whatever `ack_unmatched` says — so a body that never parses keeps redelivering until its TTL |

`ack_unmatched` is a `bool` defaulting to `False`, which is exactly the behaviour of every earlier release. It is available on `@rmq_trigger(...)`, on `RMQConsumeOperator`, on `RMQSensor`, on `RMQTrigger`, and as a checkbox on the Subscriptions page. Every one of those constructors rejects a non-`bool`: the string `"false"` is truthy, and the parameter destroys messages.

```python
@rmq_trigger(
    queue="orders.airflow-trigger",       # a queue this subscription owns
    filter_data={"filter_headers": {"event": "trigger"}},
    ack_unmatched=True,                   # anything else is acknowledged and dropped
)
@dag(schedule=None)
def orders_dag():
    ...
```

**Why it exists.** With the default, a message no filter accepts never leaves the queue: the broker returns it to the head, the consumer sees it again, rejects it again, and the loop turns roughly ten times a second until the message's TTL expires — on the order of 300 000 redeliveries over an eight-hour TTL, measured on a live installation. `ack_unmatched=True` does not slow the loop down, it removes it: every message the filter reaches a verdict on is delivered once. The one message it does not cover is the one the filter could not read at all — see the paragraph below.

**An acknowledged message is not dead-lettered.** Dead-lettering follows a reject, a TTL expiry or an overflow, and an acknowledgement is none of those. A queue with `x-dead-letter-exchange` set keeps no copy of what `ack_unmatched` dropped — the message is gone from the broker entirely.

**A failure to evaluate the filter is not a miss.** An exception out of `filter_callable`, and a body that `json.loads` refuses when the filter asks for a `body.*` key, both mean the message was never read — so it goes back to the queue and never reaches the acknowledging branch. A minute of an unreachable external service therefore costs redeliveries, not a batch of messages. Both failures are reachable in the operator and the sensor; in the watcher and the trigger a callable does not survive serialisation ([ADR 0001](docs/adr/0001-message-filter-two-instantiation-paths.md)), so an unreadable body is the only one there.

**Where the guarantee has a hole: a body that never parses.** An outage ends and the message goes through on the next turn, but a truncated or non-JSON body is a permanent property of that message: it fails the same way every time. Returning it to the queue is the deliberate choice — an acknowledgement here would delete a message the filter never read, which is exactly what `ack_unmatched` promises not to do — and the cost is that on a classic queue and on a quorum queue alike (on RabbitMQ 4.3+ a requeueing nack is not limited) this one message keeps the ~10-turns-a-second redelivery loop going until its TTL, `ack_unmatched=True` or not. The WARNING series described below is what makes it visible; the way out is a TTL on the queue, a dead-letter exchange fed by that TTL, or a filter that does not ask for a `body.*` key on a queue carrying bodies that are not JSON. A bounded escape that eventually acknowledges such a message is recorded in `docs/backlog/error-verdict-redelivers-until-ttl.md` and is deliberately not part of this behaviour.

**One queue, one subscription.** Two enabled subscriptions on the same `queue_name` and `conn_id` are two competing consumers, and the broker splits deliveries between them round-robin. A message that suits both filters — every message, when `filter_data` is empty — is acknowledged by whichever consumer received it, and the other subscription's DAG does not run. That is not new, and the unique key of a subscription row still allows it, so this is a recommendation rather than a prohibition: an installation whose filters are strictly disjoint works as it always did. With `ack_unmatched=True` the stakes are higher — a message only the neighbour's filter accepts is deleted the moment it lands on the acknowledging consumer, the neighbour stays `listening`, and nothing anywhere reports an error. The Subscriptions page badges any shared queue for that reason, with a harder text where one of the two acknowledges. Fan-out is built from separate queues and bindings, not from two subscriptions on one queue.

**What a miss puts in the log.** Nothing per message: a filter miss is normal traffic, and a line on each one would be a line per foreign message. The counters of one consuming loop produce instead:

- a WARNING on every power of ten of consecutive misses from a hundred up (100, 1 000, 10 000 …), naming the queue, saying whether the messages were returned or dropped, and suggesting a look at the filter. A match ends the run and clears it.
- a WARNING on every power of ten of *dropped* messages from a hundred up, which nothing ever clears. A stream of ninety-nine misses and one match never reaches the first threshold while deleting ninety-nine messages out of every hundred, and this counter is what makes that visible. Where the two counters hit the same number on the same message, the line above already carries it and this one is not repeated.
- a WARNING on the **first** failure to evaluate the filter and then on every power of ten from a hundred up, carrying the type and text of the exception behind it. A match does not clear that series — only a hundred outcomes in a row without a failure does. An error message returns to the queue whatever `ack_unmatched` says, so it comes back every 0.1 s, and a line per turn would put the redelivery loop itself into the log.
- in the watcher only, the metrics `rmq_watcher.filter_missed` and `rmq_watcher.filter_errored`, one per verdict. The operator, the sensor and the trigger have no metric wiring, and the `rmq_watcher` prefix would be untrue in them.

How far a counter reaches depends on how long its owner lives: a consumer task in the watcher (across reconnects), one `run()` in the trigger, one task run in the operator, the sensor instance across its pokes. The two places where the threshold of a hundred is out of reach are named in the `RMQConsumeOperator` and `RMQSensor` sections above, together with what stands in for it.

**What this does not change.** An operator or a sensor still reads one batch from the head of the queue per run, and still cannot look past it: reaching behind the head would mean holding foreign messages unacknowledged, which runs into the task's own `qos.prefetch_count` and hurts every other consumer of that queue. The answer for that case is `ack_unmatched=True` on a queue that belongs to the subscription or the task, not a larger batch.

---

## RMQ Watcher Plugin

The **RMQ Watcher Plugin** inverts the usual sensor pattern: instead of a DAG waiting for a message, a RabbitMQ message *causes* the DAG to start automatically — without polling, without `deferred` task slots, without worker resources.

### How it works

The watcher runs in a child process of the Scheduler, `python -c <bootstrap> airflow_provider_rmq.watcher.runner`, which the plugin's listener (Airflow Listener API) starts when the Scheduler starts and stops when the Scheduler stops — see [The watcher process](#the-watcher-process). That process runs an asyncio loop that subscribes to queues via AMQP `basic_consume`. When a matching message arrives, `trigger_dag()` is called directly inside it. One `RobustConnection` per `conn_id` is shared across all subscriptions to that cluster, with cooldown publishing on a second connection of its own (see [Connection Resilience](#connection-resilience)).

Every 60 seconds (configurable via Airflow Variable `rmq_watcher_reconcile_interval`) a reconciliation loop re-scans DAG files for `@rmq_trigger` decorators (mtime-based — only changed files are re-parsed) and syncs subscriptions to the database. The folder it walks is `airflow.settings.DAGS_FOLDER`, the same path the Scheduler parses DAGs from.

The files it reads there are the `*.py` files Airflow loads from that folder. The walk follows symlinked directories, reads the `.airflowignore` of every directory with the syntax of `[core] dag_ignore_file_syntax` (`regexp` or `glob`) and applies its rules to that directory and everything below it with Airflow's own rule classes, and lists hidden directories and files such as `.x.py` like any other, as Airflow loads them. Two parts of Airflow's listing are not repeated. A `.zip` DAG archive is not read: the watcher parses Python source only, and `@rmq_trigger` inside an archive fails the import (see below). Airflow's `safe_mode` heuristic is not applied: every `.py` file of the listing is parsed, so a heavy directory inside `DAGS_FOLDER` — `.git`, `.venv`, `node_modules` — belongs in `.airflowignore`. A file the scan accepts imports `dag` from Airflow, so the default heuristic always passes it; a custom `[core] might_contain_dag_callable` can skip a file the watcher reads, and the watcher then registers subscriptions for a DAG Airflow does not load. The reasoning is in [ADR 0010](docs/adr/0010-watcher-lists-dag-folder-like-airflow.md).

A subscription key going missing from a scan is not an event in itself: the row stays exactly where it was — same `id`, same consumer task, same `enabled` — while the cycle counts the misses and logs a WARNING with the attempts left. Three trusted scans without the key are survivable; the fourth deletes the row. An enabled subscription whose `dag_id` Airflow no longer counts as active goes sooner — on the third miss: a matching message on such a subscription is ACKed without triggering anything, which makes its consumer more expensive than the row. A disabled subscription always gets the full grace period, whatever the state of its DAG: it has no consumer, its messages pile up in the queue, and deleting it early would cost it the `enabled` flag — a returning key recreates the row enabled. Under concurrent schedulers an enabled row of an inactive DAG can go one counted miss earlier than that: a race between two schedulers, rather than a real miss, can leave a `1` in the counter. It costs a new `id`, a restarted consumer task and a warmup streak started over — the scan brings the row back enabled, exactly as it was. A disabled row is out of reach of this: both the condition of the early deletion and the predicate of its `DELETE` require `enabled`.

A scan that is not a statement about the code is not believed at all: the sync step is skipped whole, and no row counts a miss. That covers a DAG folder setting that is unreadable or empty, a folder that does not exist or cannot be listed, and a listing of which less than half of what the folder is known to have held comes back readable — an empty listing, and a listing that named files and read none of them, included. Which way a file refused takes no part in it: an aborted checkout leaves files truncated (they read, they do not parse), a storage backend losing access leaves them unstattable, and both say the same thing — what is on disk right now is not the code. One broken file among many is about the code and stays trusted. A listing that is not is distrusted for three cycles in a row and believed on the fourth — a folder that emptied for real, and a filesystem that is not coming back, both have to become the truth eventually. Believing the shrink is what brings the known count down to it, so a folder that legitimately lost most of its files is measured against its new size from then on, the next collapse meets the three cycles in full, and an installation whose DAG folder is empty for good is believed and left alone rather than reported every cycle. A folder whose files stopped reading altogether ends the same way: once that listing has been given in to, the same listing after it is the steady state already paid for, and the distrust starts over only when a file reads again. What the count is otherwise is a high-water mark: a folder unmounting a slice per cycle is compared with what it held, not with its own previous step. A folder-level outage in between produces no listing and leaves that budget exactly where it was — refilling it would let a folder alternating between unlistable and degraded hold the distrust for as long as it kept alternating, and the sync step would never run again. A process that has believed no listing yet has nothing to compare one against and takes what it is given, short of a listing with nothing readable in it: a scheduler starting into a half-mounted DAG volume has only the three-scan grace period between that start and the deletions. The reason goes to the log as an ERROR; the rest of the cycle — reading subscriptions, reconcile, the liveness check, the status rows — runs as usual, off the database, with one caveat: exchange metadata lives only in the scan, so a distrusted cycle reconciles from the last trusted scan, and on a scheduler that has not completed one yet there is no metadata at all — no sub-queue or binding provisioning, and no line in the log either: the orphan WARNING is written only for keys the process provisioned itself, so a consumer starts on `rmq_watcher.sub.{dag_id}` against a queue nobody declared and nothing reports it until the first trusted scan. A file whose content failed to parse, or gave no access at all, holds its previous parse for one cycle and then joins the ordinary miss count. A key that comes back after its row was deleted is reported with a WARNING naming it: what the scan recreates is a new row with the default `enabled` on it, so a subscription switched off before the deletion starts consuming again. The deletions a scheduler matches that line against are the ones it made itself and still holds in memory, so a restart in between — or another scheduler in HA recreating what this one deleted — leaves the recreation to the INFO lines of the miss counter alone.

A directory inside the folder that the walk cannot read — no permission, a failing or stale mount, a symlink whose target directory is out of reach — hides its files, not their absence. A symlink whose target cannot be reached at all is taken for such a directory even when it points to a file, as nothing tells the two apart while the target is out of reach. The directory gets one WARNING in the Scheduler log when it becomes unreadable (`RMQ Watcher: DAG folder directory <path> cannot be read …`), DEBUG lines while it stays so, and an INFO `… is readable again` once a walk reads it. Every DAG file the watcher knows under it counts as a file of the listing that did not read, exactly like an unreadable file: it holds its previous parse for one cycle with its own WARNING (`… holding its previous parse …`), counts against the half of the listing that has to read, and then its keys join the miss count. A subtree large enough to take the readable share below half makes the listing distrusted, and the ERROR names the unreadable directories — the first five, and a count of the rest. A directory unreadable since the watcher started holds nothing back, as the watcher knows no files under it, and gets the WARNING alone. An I/O error on a single `.py` file stays an error of that file. What Airflow's own listing fails on as a whole — a symlink loop, an unsupported `dag_ignore_file_syntax`, an invalid glob pattern, an `.airflowignore` that does not decode, a file name the regexp rules cannot encode — and an unreadable `.airflowignore` of the folder's root produce no listing at all: as with an unlistable folder, the cycle is distrusted and the budget of distrusted listings stays where it was. A new `.airflowignore` that excludes more than half of the folder is a shrink like any other: distrusted for three cycles and believed on the fourth, after which the rows of the excluded files go the counted-miss way.

The scan reads the DAG file's source and never executes it, so it sees of `@rmq_trigger(...)` only what is written in the file itself:

- `dag_id` in `@dag(...)` (positional or keyword) — a string literal, or a string constant of the file; a `@dag(...)` without `dag_id` names the DAG after the function, as Airflow does. `dag` must be called under its own name — `@dag(...)` or `@<module>.dag(...)`: with `from airflow.decorators import dag as airflow_dag` the scan finds no `@dag(...)`, cannot resolve the `dag_id` and refuses the file. It must also be Airflow's decorator: the bare `dag` imported from `airflow.decorators` (or `airflow.models.dag`), or `<module>.dag` with `<module>` bound by an import to `airflow.decorators` or `airflow.models.dag` (`from airflow import decorators`, `import airflow.decorators`). On a function carrying `@rmq_trigger`, a same-named `dag` of another module (`from company import dag` — a wrapper that prefixes the `dag_id`, or one that passes it through unchanged), a relative import and a file with `from ... import *` are refused: such a decorator can give the DAG another `dag_id` than the file shows. The import must be what the name means where the decorator runs: a module-level rebinding of `dag` above the decorated function, `global dag` in any function of the file, a `dag` of the factory a nested DAG function sits in, or a `dag` rebound below in a module-level loop refuses the file too, while a binding the decorator cannot see — a `dag` parameter (`@task def report(dag=None)`), `dag = context["dag"]` in a callback, `dag = my_dag()` or `with DAG(...) as dag:` below the decorated function — does not matter. For a DAG function defined directly in a factory — a plain `def` at module level, outside any loop, with no decorator, not `async def` and with no `yield` in its body — a module-level binding below the factory's `def` does not matter when the file mentions the factory only to call it, and every such call runs once before the binding: `dag = create_dag()` below the factory, or a `with DAG(...) as dag:` below the factory's last call. The binding refuses the file when the factory is called after it or inside a function, a class body, a lambda, a comprehension or a loop, where it may run later; when the file hands the factory on without calling it (`make = create_dag`, `FACTORIES.append(create_dag)`, `register(create_dag)`); and when the file never mentions the factory, since another DAG file may import and call it after this module has run. Any module-level binding below the factory refuses the file as well when the DAG function sits in a function or class nested in the factory (`def outer(): def create_dag(): ...; return create_dag`), or when the factory is `async def`, a generator (`yield dag_obj`) or carries a decorator (`@register`): a call of such a factory may run the DAG function's `def` after the binding. The same holds for `rmq_trigger` and for the module names of `<module>.rmq_trigger` and `<module>.dag`;
- every argument of `@rmq_trigger(...)`, by keyword or by position (`queue`, `queues`, `conn_id`, `filter_data`, `cooldown`, in that order) — a literal, a constant of the file, a list, tuple or dict display built from them (`queues=[Q1, Q2]`, `filter_data={"body.type": PAID}`), a sign on a number, and `**OPTS` where `OPTS` is a dict constant of the file whose keys are `rmq_trigger` parameters.

A constant of the file is a name assigned a literal exactly once at the top level of the DAG file, above the decorated function. The function must be at module level itself — a DAG declared inside a factory function reads literals only — and a file with `from ... import *` has no constants. A constant holding a list or a dict may appear nowhere in the file but in `rmq_trigger(...)` arguments: any other mention — `alias = FILTER`, `FILTER["a"] = 1`, `helper(QUEUES)`, a read by index — could change it where the scan cannot follow, so the scan does not read it. Not read either: an imported name (`from common.rmq import RMQ_CONN`), a call, an attribute, `os.environ[...]`, an f-string, concatenation, `*args`, `**` of anything but a dict constant, an unknown keyword, a parameter given twice, and values a subscription stored as JSON cannot hold — a set, a bytes or a complex literal. The scan recognizes `rmq_trigger` only as a decorator call on a function — `@rmq_trigger(...)` or `@<module>.rmq_trigger(...)` — placed above `@dag(...)` and imported under its own name from the provider: `from airflow_provider_rmq.watcher.decorators import rmq_trigger` (or `from airflow_provider_rmq.watcher import rmq_trigger`), and for the attribute form a `<module>` bound by an import to `airflow_provider_rmq.watcher.decorators` or `airflow_provider_rmq.watcher` (`from airflow_provider_rmq.watcher import decorators`, `import airflow_provider_rmq.watcher.decorators as d`). A same-named `rmq_trigger` of another module (`from common_rmq import rmq_trigger` holding `partial(rmq_trigger, conn_id=...)` or a wrapper function), a relative import (`from .common import rmq_trigger`) and a file with `from ... import *` are refused: such a decorator can receive other arguments than the file shows. `import ... as rt`, `rt = decorators.rmq_trigger` and `partial(rmq_trigger, ...)` in the DAG file reach the decorator by paths the scan cannot follow, and `rmq_trigger(...)(dag)` on a ready `DAG` raises `TypeError` at once. Below the first `@rmq_trigger(...)` of a function only `@rmq_trigger(...)` and `@dag(...)` may sit: any other decorator there can change the DAG out of the scan's sight — one below `@dag(...)` without `functools.wraps` renames the function a DAG without `dag_id` is named after, one between `@rmq_trigger` and `@dag` can rename the DAG or return something else — and refuses the file. Decorators above the first `@rmq_trigger(...)` wrap what it returns and do not matter.

A file the scan cannot read in full is refused whole. When any `@rmq_trigger` of it has an argument the scan does not read, a `dag_id` it cannot resolve, arguments `build_subscriptions` refuses or a second `exchange=` subscription on the same DAG, when `@rmq_trigger` sits below `@dag(...)` or another decorator sits below `@rmq_trigger`, when its `rmq_trigger` is not the provider's or its `dag` is not Airflow's, or when the file mentions `rmq_trigger` other than as that decorator call, not one subscription of the file is registered, and the Scheduler log carries an ERROR naming the file and every problem with its DAG, line, parameter and reason. The ERROR is written when the watcher starts and each time the file changes, not on every cycle. Rows the file registered earlier go the counted-miss way described above, like those of a removed decorator. The import of the file fails on the same grounds: while the DAG is built, `@rmq_trigger` runs the scan's own code on its file and raises `RMQTriggerNotStaticError` (a `ValueError`, importable from `airflow_provider_rmq.watcher`) when the watcher would refuse the file, would not recognize the call, would resolve another `dag_id`, or would read other values than the decorator received — a constant changed between the decorator line and the factory call, for one. Airflow records an import error for the file and shows it in the Broken DAG banner with the list of problems, and every DAG of the file is gone, those without `@rmq_trigger` included: a subscription that runs against what the file declares is worse than a file that visibly fails to load. The check covers the whole file, but it runs only when some `@rmq_trigger` of the file executes at import. In a file where every call sits in a factory the file never calls, or under an `if` that is false in this environment, the watcher's ERROR is the only signal. `rmq_trigger` re-exported by another module under another name (`from common import trig`) is not `rmq_trigger` to the scan at all: `@trig(...)` fails the import, while the watcher goes on registering the file's other subscriptions.

`@rmq_trigger` must also sit in a file the watcher scans: a `*.py` file inside `DAGS_FOLDER` that no `.airflowignore` excludes — the listing described above, hidden directories and files included. A call anywhere else fails the import with "is in a file the RMQ watcher does not scan" — a factory carrying `@rmq_trigger` declared in `plugins/`, in a package on `PYTHONPATH`, or in a directory of the folder that `.airflowignore` excludes (a shared `common/` package listed there), and called from a DAG file: Airflow would load that DAG, and the watcher would never see its subscription. The check applies to the file of the `rmq_trigger(...)` call, so importing a file under `.airflowignore` inside `DAGS_FOLDER` directly — a test importing a copy of a DAG file kept in an ignored `tests/` — fails the same way; a `DagBag` does not import such files at all. An `.airflowignore` on the way to the file that cannot be read or applied fails the import with "the RMQ watcher cannot tell whether it scans this file", naming the cause, and a file inside a `.zip` archive fails it with "the RMQ watcher cannot read this file". The check compares the call site with `airflow.settings.DAGS_FOLDER` of the process importing the file, so a CI job or a test that loads DAGs with `DagBag(dag_folder=<path>)` must point `DAGS_FOLDER` at the same folder — `AIRFLOW__CORE__DAGS_FOLDER=<path>` in its environment, or `airflow.settings.DAGS_FOLDER` set to it before the `DagBag` is built. Otherwise every DAG carrying `@rmq_trigger` fails with that import error. A module imported through another spelling of the folder — a `sys.path` entry naming its real path, or another symlink to it — counts as inside it. The check reads each file once per version on disk (inode, modification time, size), however many `@rmq_trigger` calls the file has, and it reads the file as it is on disk while the DAG is built: a file replaced between the import and the check, such as a git-sync update landing mid-import, can fail that one parse, and the next parse imports the new version cleanly.

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
- A cooldown DAG Airflow never serializes — removed from the folder, broken by an import error, present on another instance only — leaves its fire event circling `rmq_watcher.fire` for as long as the process runs, while every subscription row of that DAG keeps saying `listening`. The fire consumer holds no row of its own, so the scheduler log is the only place it shows: an INFO line per event and a WARNING on every tenth.

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

The account needs the `management` tag on top of its AMQP permissions — the Management API answers a tagless user with 401/403 whatever its `set_permissions` say:

```
rabbitmqctl set_user_tags <user> management
```

What the Management API replies is shaped by the rights of the account that asked: a user tagged `management` is shown only the channels of its own connections, while `monitoring` and `administrator` see the whole vhost. On the minimum-privilege account above, the watcher therefore sees its own consumers and no one else's.

The same `login`/`password` from the connection are reused for the Management API call. If `management_url` is not set, bind-diff is skipped on every cycle (logged as ERROR) — the queue is still declared and consumed normally, but bindings never get created/updated.

`management_url` also selects the liveness probe for **every** subscription, `queue=` and `exchange=` alike. With it, the check reads `GET /api/consumers/{vhost}`, can name individual consumer tags and reports the Broker consumers number. Without it, the check falls back to a passive queue declare: that vouches for — or condemns — all consumers of the `conn_id` at once and returns no count, so the Broker consumers column stays `—` even when the verdict is positive. After two consecutive Management API failures on one `conn_id` (wrong URL, credentials without the `management` tag, 404, timeout) the check switches to that same passive declare rather than staying blind. The same two-cycle bound covers a `conn_id` whose Airflow connection cannot be read at all — renamed, deleted while subscriptions still name it, or a secrets backend that keeps refusing: the check then asks the pooled connection itself, which needs no metadata row.

**No stacking — one DAG, one exchange.** Multiple `@rmq_trigger(exchange=...)` decorators on the same DAG raise `ValueError` — they would all resolve to the same `rmq_watcher.sub.{dag_id}` queue, and the last one parsed would silently win. The error fires when the `@dag(...)` factory is called (e.g. `jetstat_succeeded_dag()` at the end of the file, as in the example above) — during import, before the DAG is registered — and the watcher refuses every subscription of such a file. Use a single decorator call with the union of routing keys, or subscribe to multiple exchanges across multiple DAGs. To consume from several exchanges on the same DAG, fall back to `queue=`/`queues=` with manually created and bound queues.

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
- Airflow logs — WARNING for orphaned `rmq_watcher.sub.*` queues/bindings (with a `rabbitmqadmin delete queue ...` hint); ERROR for a skipped bind-diff (Management API unreachable) or for an exchange property conflict (`PRECONDITION_FAILED` — the exchange name is already used by something else with different properties). The orphan WARNING runs ahead of the deletion: `exchange`/`routing_keys` come from the current scan only, so a subscription whose key is missing from a trusted scan reads as orphaned from its first miss on, up to four cycles before its row goes — and for those cycles its bindings are not reconciled either, while the row and its consumer carry on

**Rollback:** remove `exchange=`/`routing_keys=`/`routing_key_ids=`/`routing_key_status=` from the decorator and redeploy. `rmq_watcher.sub.{dag_id}` becomes orphaned — a WARNING appears in the logs from the first trusted scan without the key, several cycles before the subscription row itself is deleted; the TTL (8h) caps unbounded growth, and manual cleanup follows the hint in the WARNING text. The exchange itself and its `.unrouted`/`.log` queues are **not** touched by rollback (other DAGs may still be using them).

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

Toggling a `dag_file` subscription off survives a failure to read the DAG files. A cycle whose scan cannot be trusted — the DAG folder gone for a second under git-sync, stripped of read permission, or listing nothing — skips the sync altogether; a trusted scan missing the key gives the row a three-cycle grace period, and a disabled row gets that grace in full even when Airflow has already stopped counting its DAG as active. Throughout, it is the same row, and with it the flag the operator cleared. The grace is not indefinite: a key that misses a fourth trusted scan in a row deletes the row — and a decorator that comes back later recreates it enabled.

Exchange-mode subscriptions show up in the UI like any other `dag_file` subscription — by their queue name (`rmq_watcher.sub.{dag_id}`) only. The `exchange`/`routing_keys` metadata is not displayed there; the DAG source file is the single source of truth for that.

Any subscription whose `dag_id` doesn't correspond to an active Airflow DAG is marked on the page with a `⚠ dag not found` badge. This is a **one-sided** signal: its presence means a real problem — a matching message will be ACKed without triggering anything — but its **absence guarantees nothing**. Three reasons the badge can miss a broken subscription: `is_paused` is not checked, so a paused DAG never gets the badge even though a matching message is still ACKed without triggering a run, exactly like a missing DAG (see [ADR 0006](docs/adr/0006-badge-dag-lookup-not-unified-with-sync-trigger.md)); a just-added DAG may be badged for a short while until the Scheduler parses it; and a just-deleted or renamed DAG conversely stays un-badged for a while until `DagModel.is_active` catches up. The last two are expected lag, not bugs.

A queue more than one **enabled** subscription of the same `conn_id` listens to is marked with a `⚠ shared queue` badge; a grouped row names which of its queues are shared and reads `⚠ shared queue: q1, q2`. A disabled row runs no consumer and is never badged. The badge is on both rows of the pair, because either of them is equally affected. Where one of those subscriptions has `ack_unmatched` on, the badge gains ` — messages deleted` and its tooltip says what that costs: a message only the neighbour's filter accepts is deleted the moment it lands on the acknowledging consumer, and the neighbour goes on saying `listening`. The `ack_unmatched` column of the table shows `on` for a subscription that acknowledges its misses.

The same setting is a checkbox on the subscription form, available in all three of its flows — create, edit and edit-group — and it survives an edit-group save, which rewrites the rows of a group from scratch. On a group the box stands for every row at once: it is drawn on only when every queue of the group acknowledges, and a save writes that one value to all of them. A group whose rows disagree is therefore drawn off, with a notice under the checkbox saying so — the table column reads such a group as acknowledging, because one of its queues does, and the form deliberately answers the other way: a save that only touched the cooldown must not switch permanent deletion on for a queue that runs without it. A submit rejected for an unrelated reason — a negative cooldown, `filter_data` that is not JSON — redraws the checkbox from what was submitted rather than from the stored row, so a box just cleared comes back cleared.

Saving a subscription that shares its queue with another enabled one raises a `flash` warning naming the queue; so does toggling one on. The warning comes after the save rather than before it: the form renders `dag_id`, the queue list, `conn_id`, the cooldown and `filter_data` from the stored row, so a confirmation round trip would redraw them from the database and lose the very input being confirmed.

### Subscriptions Page Access

The page and its menu entry belong to two roles: Admin and Op. Viewer and User see neither the entry nor the `RabbitMQ` category it hangs under, and a direct visit to `/rmq-watcher/subscriptions` is refused.

Admin gets the page from Airflow itself, which hands that role every non-DAG permission on every role synchronisation. Op gets it from the provider: at each webserver start the plugin grants the role `can_read`, `can_create`, `can_edit` and `can_delete` on the resource `RMQ Subscriptions`, plus `menu_access` on `Subscriptions` and on `RabbitMQ`. The grant runs on every start, so upgrading the provider opens the page to Op by itself, with nothing to do by hand.

Two starts are needed on a virgin metadata database: Airflow creates the `Op` role after the plugin's callback has run, so the first start finds no role, logs a warning and leaves the grant to the start after it. The grant also needs the FAB auth manager, which is the default one — under another `auth_manager` there is no role to grant to and the callback logs a warning instead.

> **Warning.** Those permissions include creating and deleting subscriptions, so access to the page is the right to make an arbitrary DAG run on a message from a queue. An installation where Op should not hold that right puts `grant_op_access = false` in the `[rmq_watcher]` section of `airflow.cfg` — or sets `AIRFLOW__RMQ_WATCHER__GRANT_OP_ACCESS=false` in the webserver's environment — and restarts the webserver.

The webserver is the only process that acts on the switch. A plugin blueprint is registered by every application that builds an appbuilder — the FAB commands behind `airflow users`, `airflow roles` and `airflow sync-perm` build one too — and the permissions of a page belong to the application that serves the page, so the grant asks for Airflow's own web UI and stands aside where there is none. A command run in a shell that does not carry the switch therefore leaves the role alone instead of answering to a value the operator never set there.

The switch governs the access, not merely the grant: a false value **removes** the same six permissions from the Op role at the next webserver start. `false`, `off`, `no`, `n` and `0` all say it, and so does a value in no spelling the switch knows: an unreadable answer closes the page rather than holding it open. Granting or revoking them by hand while the switch is true or unset achieves nothing — the next start puts the provider's own answer back.

It is an Airflow configuration option and not an Airflow Variable because Airflow gives the `Op` role create, edit and delete on Variables. A switch kept there is one the very role it restricts can set back to `true` for itself, taking the page back at the next webserver restart; `airflow.cfg` and the process environment are outside the reach of every role the web UI has, which is what a switch that withdraws a privilege has to be.

`menu_access` is granted on the **displayed** names of the entry and of its category, and the FAB menu is flat. Another installed plugin that names its own entry `Subscriptions` or its own category `RabbitMQ` therefore shares those two permissions, and a revocation closes its menu to Op along with this one.

Any other role opens the page through Airflow's CLI:

```bash
airflow roles add-perms MyRole -a can_read -r "RMQ Subscriptions"
airflow roles add-perms MyRole -a menu_access -r "Subscriptions"
airflow roles add-perms MyRole -a menu_access -r "RabbitMQ"
```

Without `menu_access` on the category the entry stays out of the menu even with the other two granted. Without `can_create`, `can_edit` and `can_delete` the page still draws all eight of its controls — the template checks no permission — and each of them answers with a refusal.

### The watcher process

The Scheduler process itself holds no thread, event loop or thread pool of the plugin. Airflow forks the Scheduler — the `CeleryExecutor` pool that sends tasks to Celery, the DAG processor manager — and a fork is only safe from a process with a single thread, so the watcher loop runs in a process of its own; the reasoning is in [ADR 0008](docs/adr/0008-watcher-child-process.md). The listener starts it in `on_starting`, from the Scheduler's main thread, with `subprocess.Popen` and the Scheduler's environment, so the process reads the same Airflow configuration, metadata database, secrets backend and StatsD settings. Installation is the plain `pip install` — no extra service, no extra command: one Scheduler, one watcher process under it, visible in `docker top` of the Scheduler container as `python -c <bootstrap> airflow_provider_rmq.watcher.runner` under `airflow scheduler`. The bootstrap is a one-line program that runs the runner module the way `python -m` would. Before it imports anything it takes off the module search path the Scheduler's working directory — usually `AIRFLOW_HOME` — which `python -c` puts in front of it and the Scheduler's own path does not hold, so a `runpy.py`, a `logging.py` or a package named like this one lying there is not imported in the watcher process; the working directory itself is the Scheduler's, so relative paths in the environment resolve as they do there. For a package installed in the plugins folder rather than with pip the bootstrap appends the plugins folder to the end of the module search path, where the Scheduler has it too, so a module there named like one of the standard library or an installed package — `plugins/secrets/` for a custom secrets backend — shadows it in neither process. Before its loop starts, the process loads the providers' configuration and the Airflow plugins, as the Scheduler has them loaded before its jobs run. The loading runs in the process's main thread, where the Scheduler loads its plugins too, so a plugin that sets a signal handler or the signal wakeup fd on import loads in both processes alike. Meanwhile a thread of its own already listens for a stop and watches the Scheduler, so a plugin that hangs on import does not keep the process from stopping; the loop thread starts only once the loading is done. The process runs on Linux and on macOS alike; the parent-death signal described under **Stopping** is Linux's, and elsewhere the parent checks alone notice a Scheduler that died — the thread that watches the plugin loading, then the process's main thread every second and the loop before every cycle. Its stdout and stderr are the Scheduler's, so every watcher line lands in the Scheduler container's log, and so do the lines of the process itself:

| Line | Meaning |
|---|---|
| `RMQ Watcher process started with PID … (scheduler PID …)` | The Scheduler started the process |
| `RMQ Watcher process … started for the scheduler process …` | The process has imported Airflow and starts the loop |
| `RMQ Watcher process received SIGTERM — stopping the watcher` | A stop was asked for; the loop stops waiting for the step it is on, starts no further step and stops the consumers |
| `RMQ Watcher: stop requested — ending this cycle …` | The stop came during a cycle; the rest of the cycle is skipped, the reconcile included, and the loop goes on to stop the consumers |
| `RMQ Watcher: the scheduler process … has exited (parent is now …) — stopping the watcher` | ERROR — the Scheduler died without stopping the process, and the loop noticed it before a cycle |
| `RMQ Watcher process: the scheduler process … has exited (parent is now …) — stopping the watcher` | ERROR — the Scheduler died without stopping the process, and the process's main thread noticed it between two waits for a stop signal |
| `RMQ Watcher process … exited with code …` | The Scheduler stopped the process and collected its exit code |
| `RMQ Watcher process … had already exited with code …` | The process was gone by the time the Scheduler stopped it — the usual line under `docker stop` with `dumb-init`, which sends it SIGTERM together with the Scheduler |
| `RMQ Watcher process … still running 5s after SIGTERM — killing it` | WARNING — the process did not leave within 5 s; the listener sends SIGKILL |
| `RMQ Watcher process … not reaped 2s after SIGKILL — leaving it behind, the scheduler goes on stopping` | ERROR — the process is stuck in the kernel, on a hung NFS mount for one; the Scheduler stops without it |
| `RMQ Watcher process …: the loop is still running 45s after the stop — exiting without it` | stderr, written past Airflow's logging, whose lock the loop thread may hold — the loop did not return within 45 s of the stop; the process leaves without it |
| `RMQ Watcher process …: logging did not shut down within 2s — exiting without it` | stderr — a thread the process leaves behind holds the lock of a log handler; the process exits without waiting for it |
| `RMQ Watcher loop failed — exiting` | ERROR with a traceback — the loop raised out of its thread; exit code 1 |
| `RMQ Watcher process failed — exiting` | ERROR with a traceback — setting up the process, or waiting for the stop signal, raised; exit code 1 |
| `RMQ Watcher process …: AIRFLOW_PROVIDER_RMQ_WATCHER_PARENT_PID does not hold the scheduler's PID — not starting the watcher` | stderr, before Airflow's logging is set up — the process was started without the Scheduler's PID; exit code 2 |
| `RMQ Watcher process …: the scheduler process … exited before the watcher started` | stderr — the Scheduler died while the process was starting; exit code 0 |
| `RMQ Watcher process …: received SIGTERM while Airflow was being prepared — exiting` | stderr — a stop came while the process was still importing Airflow and loading the plugins; it exits at once, the loop never started; exit code 0 |
| `RMQ Watcher process …: the scheduler process … exited while Airflow was being prepared — exiting` | stderr — the Scheduler died while the process was still importing Airflow and loading the plugins; exit code 0 |
| `RMQ Watcher process …: prctl(PR_SET_PDEATHSIG) failed: …` or `… cannot request the parent-death signal …` | stderr — the kernel refused the parent-death signal; the parent checks of the process's main thread, every second, and of the loop, before every cycle, still end the process once the Scheduler is gone |
| `RMQWatcherListener in process …, a fork of the scheduler process … — the watcher process belongs to the scheduler, nothing to start or stop here` | In a LocalExecutor task log — the task supervisor, forked from the Scheduler, inherits the listener and leaves the watcher process to the Scheduler |

Exit codes of the process: 0 — stopped by a signal or by the Scheduler's death; 1 — setting up the process or running the loop failed; 2 — started without the Scheduler's PID. `-9` in the Scheduler's `exited with code` line means the listener killed the process with SIGKILL; a negative code in the `had already exited` line names the signal that ended it on its own, `-9` for the OOM killer and `-14` (SIGALRM) for the process's own exit deadline: its way out blocked — on a stderr nobody reads, a log handler, the database — for 60 s after it decided to leave, and the kernel ended it.

**Stopping.** When the Scheduler stops, the listener (`before_stopping`) sends the process SIGTERM and waits 5 s for it to stop the consumers and exit, then sends SIGKILL and waits 2 s more. A process still not reaped after that is logged as an ERROR with its PID and left behind: a Scheduler shutdown never waits for the watcher longer than 7 s. A SIGKILL cuts the consumers' own shutdown short, and the broker returns their unacknowledged deliveries to the queues. Under `docker stop` with `dumb-init` and `DUMB_INIT_SETSID=1` the watcher process gets its SIGTERM from the init directly, together with the Scheduler, and the listener's own request after it is harmless. On its own SIGTERM the process waits up to 45 s for the loop to stop the consumers — whose own budget is 30 s — and then exits without it. The loop does not wait for a blocking step of the cycle that is still running — the call is left in its worker to the process exit — and starts no further step, the reconcile included, so the consumers get their whole budget. A reconcile the stop finds already running is not interrupted: it runs to its end, under the cycle budget, and only then are the consumers stopped; one that takes longer than the 45 s leaves the process exiting without that stop, and the subscription rows are not marked `disconnected`. Whatever its way out blocks on after that — a stderr pipe nobody reads, a log handler's lock, a database that does not answer the close — the process is gone 60 s after it decided to leave: before it writes or logs a single line on the way out, it has the kernel arm an alarm, SIGALRM, whose default action ends the process whatever its threads are stuck in. A plugin that sets a SIGALRM handler of its own on import is the one exception: its handler stays in force until the plugins have loaded, so a stop that comes during the loading goes without the deadline, with one stderr line between it and the exit. Once the loading has returned the process restores the default action and cancels any SIGALRM timer a plugin left armed, and from then on 60 s is the most the watcher can outlive a Scheduler that has stopped waiting for it, or one that died. A Scheduler that dies without a shutdown — `kill -9`, the OOM killer — takes the watcher with it all the same: on Linux the kernel sends the process SIGTERM once the Scheduler's main thread exits, and where the kernel refused that signal or, on macOS, has no such signal to send, the process compares its parent PID with the Scheduler's every second and the loop before every cycle, which ends it with one of the ERROR lines above. A stop or the Scheduler's death that comes while the process is still importing Airflow and loading the plugins ends it at once, however long that loading takes: a thread of its own listens for the signal and checks the parent from the start, and there is no loop yet to stop.

**Other signals.** The watcher process ignores SIGHUP, SIGQUIT, SIGUSR1 and SIGUSR2. It is in the Scheduler's process group, and `dumb-init` with `DUMB_INIT_SETSID=1` forwards every signal it receives to the whole group, so a `docker kill -s USR2` that asks the Scheduler for its executor dump, or a `-s QUIT` for its thread stacks, reaches the watcher too — and leaves it running. SIGTERM and SIGINT stop it as described above. A program the watcher process starts — a `_cmd` option of Airflow's configuration — begins with these four signals ignored; one that sets a handler of its own receives them as usual. The watcher's own thread stacks are what `py-spy dump --pid <watcher PID>` shows.

**A watcher process that dies is not restarted.** A crash inside the loop is recovered in place — the event loop is recreated 30 s later. A process that dies as a whole — OOM, a segfault, `kill -9` — stays dead until the next Scheduler restart: restarting it would take a thread or a `SIGCHLD` handler in the Scheduler process, which is exactly what the plugin keeps out of it. Until that restart it shows up in `docker top` as `<defunct>` under `airflow scheduler`, and the Subscriptions page flags every `Last reconcile` once it is older than one interval plus one cycle budget.

**Liveness metric.** The StatsD counter `rmq_watcher.cycle_completed` (sent under Airflow's `[metrics] statsd_prefix`, `airflow.rmq_watcher.cycle_completed` by default) grows by one on every reconcile cycle that ran to its end — including a cycle in which single steps failed and were logged. A cycle that ran out of its budget does not count here; it counts `rmq_watcher.cycle_timed_out`. The counter says that the process is alive and its loop is turning, not that the watcher is healthy: a subscription stuck in `error` or a connection the liveness check condemned leaves it growing all the same, and the Subscriptions page is where those show. An alert on it standing still is left to each installation; a window longer than one reconcile interval plus one cycle budget — `60 + 300` seconds with both Variables unset — keeps a slow but working cycle from firing it. With several Schedulers the alert has to be one per Scheduler, since each runs a watcher process of its own and one of them can die while the others go on. The counter carries no dimension naming its Scheduler, so that dimension has to come from the metrics collector — a StatsD agent or exporter per host that labels what it receives with the host or instance; a single collector shared by all the Schedulers merges their increments into one series, and a dead watcher does not show in it while any other is alive.

**Metadata-database connections.** The watcher process has its own SQLAlchemy engine, built by Airflow from the same `[database]` settings, so each Scheduler holds up to `sql_alchemy_pool_size + sql_alchemy_max_overflow` (5 + 10 by default) connections to the metadata database on top of the Scheduler's own. Size `max_connections` of the database, or the pooler in front of it, for that.

**Memory.** The watcher process is a second Airflow interpreter in the Scheduler container: it imports Airflow, the providers and the plugins, as the Scheduler does, and holds the consumers' unacknowledged deliveries on top of that. Leave room for it in the container's memory limit — on a tight limit the OOM killer takes one of the two processes, and a watcher process killed that way is not restarted.

### Connection Resilience

The watcher process holds long-lived AMQP connections for as long as the Scheduler runs, so it treats a broker restart, a silently dropped TCP link, a resource alarm or an unavailable database as normal weather: once the external problem is gone, subscriptions come back on their own. The design is written up in [ADR 0007](docs/adr/0007-connection-liveness-two-tier-check.md); the short version is four layers.

**1 — Heartbeats and per-call timeouts.** Every async connection carries a `heartbeat` (30 s by default), and every AMQP call — connect, `channel()`, declare, bind, publish — runs under `connect_timeout`/`rpc_timeout` (see [Connection Timing](#connection-timing)). A link that dies turns into an exception instead of a call that waits forever. The connect carries the watcher's own bound rather than the library's: `connect_robust(timeout=…)` covers the TCP connect and the AMQP handshake and stops there, while the wait that follows it — for the broker to declare the connection unblocked — is outside it, and under a resource alarm that wait lasts as long as the alarm does. A connect that runs out of time is not cancelled but left in flight, and its connection stays in the pool: cancelling it would poison the connection object for good, and dropping it would leave an open connection to the broker that nothing can close. Each `conn_id` builds its connections under a lock of its own, and that lock covers starting a connect rather than waiting for one: however many callers want the connection, the broker is asked for a single attempt, and they wait for it side by side rather than one behind another. A broker that stopped answering therefore costs each caller one `connect_timeout`, and holds up the `conn_id`s that use it and no others. A pooled connection is handed out only while it is usable — not merely unclosed: a reconnect that never finished leaves an object with no transport under it, which reports itself open and fails every call made through it, so it is replaced rather than handed to the next caller.

**2 — Liveness watchdog.** Every reconcile cycle the watcher asks the broker whether its consumers are still registered. Each subscription attaches under its own consumer tag `rmq_watcher.{hostname}.{pid}.{sub_id}.{nonce}` (the cooldown fire consumer uses `fire` in place of `{sub_id}`), and the check looks for that exact tag — a foreign consumer on the same queue, a second DAG subscribed to it, or another scheduler replica in HA cannot vouch for ours, and the `{nonce}`, fresh for every attach, keeps a ghost consumer left on the broker by a `close()` that never returned from vouching for the task that replaced it. The tag is claimed — and the subscription reported as `listening` — only once the broker has confirmed the registration, which itself runs under `rpc_timeout`: a `basic.consume` that never comes back would otherwise leave the check looking for a tag the broker never held. With `management_url` configured the tag is looked up in `GET /api/consumers/{vhost}`; without it, the watcher falls back to a passive `queue_declare` probe on a separate channel, whose verdict covers every consumer of that `conn_id` at once. Either way the tag is also checked against the channel the client holds it on: RabbitMQ cancels a consumer of ours — a deleted queue, a quorum-queue or stream leader change, the node hosting a classic queue restarting under a client connected to another node — with a `basic.cancel` that closes neither the connection nor the channel, so every probe of that connection keeps succeeding while nothing consumes; the tag missing from our own channel is what condemns it. Two negative checks in a row (never less than two reconcile intervals) cancel exactly those subscriptions whose own tag the broker did not confirm and drop the connection they share — restarting a task alone would hand it the same zombie connection out of the pool. The status row does not wait for the second check: a `conn_id` whose consumers the broker denies reads `error` with the reason and the check number straight away. The confirmed subscriptions of that `conn_id` are neither cancelled nor restarted: the shared connection is dropped underneath them, they reconnect through their own retry loop and surface the drop as a transient consumer error (a WARNING names them). The fire consumer goes down with the connection it runs on, because it holds the connection object it was handed at startup for its whole life; the reconcile cycle starts it again — on the connection the pool holds now — when that object was replaced under it, and moves it when the cooldown `conn_id` moves. A `conn_id` whose only attached consumer is the fire consumer is recreated on that consumer's verdict alone: nothing else on it can be asked about — its subscriptions are in their own retry loop — and a restart would otherwise be handed the same pooled object every cycle. A `conn_id` whose tasks are alive but never reach `listening` for two cycles offers the check no candidate at all, so the connection itself is asked whether it still answers an RPC: silence recreates it, an answer leaves it in place and the row says the fault is downstream of the connection — a subscription stuck in `error` because its trigger keeps failing is attached to a healthy broker connection. A Management API failure is "no data", not a verdict: it logs a WARNING, leaves the counters alone and keeps the stored status — until the second failure in a row, on which the passive declare takes over; an unreadable Airflow connection is treated the same way. A hung AMQP probe, on the other hand, counts as dead. The same `conn_id` is never recreated more often than once every 5 cycles; a verdict held back by that limit is logged and stored as `degraded`.

**3 — Cycle timeout.** The whole reconcile iteration runs under a budget of `max(reconcile_interval × 3, 300)` seconds. Exceeding it stops the manager and recreates the event loop after 30 s, with an ERROR naming the phase (`migrate` / `scan` / `sync` / `read subs` / `reconcile`) and how long it took. The budget is deliberately generous — hitting it pauses consumption on every `conn_id` — while the per-call timeouts of layer 1 catch a stuck network operation much earlier and only for the subscription that owns it. Each blocking step of the cycle (the DAG-file scan, the sync to the database, the subscription read) has a 60-second bound of its own and only ever one attempt in flight, so an unresponsive database ends a single cycle rather than costing the loop, and cannot fill the cycle's four-worker pool with stuck copies of the same call.

**4 — Publishing on its own connection.** Cooldown publishing uses a separate, lazily opened connection per `conn_id`. Under a memory or disk alarm RabbitMQ blocks publishing connections by stopping to read from the socket, which would otherwise take the acknowledgements of every consumer on the same connection down with it — and a connection the broker has flagged as blocked stops writing on the client side too (`aiormq` holds its own writer until `connection.unblocked` arrives), so the split is what keeps `basic.ack` moving: the consuming connection publishes nothing and is never flagged. What the split does not cover is a connection opened *during* an alarm, which does not become usable until the alarm clears — hence the client-side bound on connect in layer 1. A placeholder the broker returns as unroutable — the pending queue is missing, so nothing would ever fire the DAG after the window — puts the delivery back on the queue instead of acknowledging it. Two publish timeouts in a row recreate the publishing connection alone and leave consumption untouched, and a publish that keeps failing pauses that subscription for a growing interval (1 s, doubling up to 60 s) before reattaching. Every failed attempt requeues the delivery, and a requeueing nack does not count against a quorum queue's `delivery-limit`, so the delivery waits out an alarm of any length; the pauses keep the requeue from spinning at full speed meanwhile. The cooldown fire consumer runs on the lowest `conn_id` among the cooldown subscriptions, which keeps it from being moved by the order rows happen to come back in; cooldown subscriptions spread over several `conn_id`s are logged as an ERROR, because the pending queues live on one broker only.

**Airflow Variables:**

| Variable | Default | Meaning |
|---|---|---|
| `rmq_watcher_reconcile_interval` | `60` | Seconds between reconcile cycles |
| `rmq_watcher_cycle_timeout` | `max(interval × 3, 300)` | Seconds one cycle may take before the event loop is recreated |

Both are re-read at the start of every cycle in a thread pool under a short timeout of their own, so a changed Variable takes effect on the next cycle; a read still stuck in a worker blocks the next one from starting, and the loop gives up on it at its own timeout and keeps the values it already has instead of stalling; a database that refuses the read outright is not told apart from an unset Variable — it reads as no value at all, and the built-in defaults take effect until a later cycle reads a real one.

**Airflow configuration:**

| Option | Default | Meaning |
|---|---|---|
| `[rmq_watcher] grant_op_access` | `true` | Whether the Op role holds the permissions of the [Subscriptions page](#subscriptions-page-access) |

It is read once per webserver start, when the page's permissions are set, and it is read from `airflow.cfg` and the webserver's environment and from nowhere else: the `_cmd` and `_secret` indirections that would reach a shell command or a secrets backend are honoured only for the options Airflow lists as sensitive, and this one is not among them. The read therefore touches neither the network nor the database, and nothing in it can hang while the webserver is still building its application. It happens before anything is touched, and the answer is read towards withholding. `1`, `t`, `true`, `y`, `yes` and `on` hold the page open; `0`, `f`, `false`, `n`, `no` and `off` close it — case, surrounding whitespace and a trailing `#` comment do not matter. A value in none of those spellings closes the page as well, with the value named in a warning in the log: the switch has one job, and it is taking a privilege away, so what nobody can read is answered in the direction that withholds the privilege rather than the one that hands it out. Only an option nobody set at all is read as the default `true`. The provider declares the option in its configuration metadata, so Airflow's Configuration page, `airflow config list` and `airflow config get-value rmq_watcher grant_op_access` show it with its default on an installation where nobody has set it.

**What the Subscriptions page shows.** The Connections block at `/rmq-watcher/subscriptions` lists one row per `conn_id`:

| Column | Meaning |
|---|---|
| Status | `connected` when the broker confirmed every one of our consumer tags on this `conn_id`; `error` on a negative verdict — a consumer the broker did not confirm, no running task of this `conn_id` at all, a failed connection attempt, or tasks that are alive while not one of them reaches `listening` and the connection still answers an RPC, which puts the fault downstream of the connection (the reason is in Last Error); `degraded` when a negative verdict was held back by the recreation rate limit and the connection was left in place instead of being recreated again so soon; `unknown` for a `conn_id` no liveness check has ever reached a verdict on. The status follows the verdict alone — a `conn_id` with no verdict keeps whatever is stored, and the number of tasks the watcher started never makes a row green |
| Consumers | How many live consumer tasks the watcher holds for this `conn_id`, the cooldown fire task included when it runs there |
| Broker consumers | How many consumers the broker reports on the queues of the subscriptions that were actually probed — those whose own status is `listening` — plus `rmq_watcher.fire` when the fire consumer runs on this `conn_id`. `—` means there is no number at all: the passive-declare fallback never returns one, so a connection without `management_url` keeps the em dash even when the verdict is positive. A number that differs from the previous column is highlighted — lower means a subscription that has not attached yet (still connecting, or backing off after an error) and is therefore not probed, higher means another scheduler replica using the same credentials or — on a `monitoring`/`administrator` account, which is what makes foreign channels visible at all — other clients on the same queues |
| Last reconcile | Age of the last cycle for this `conn_id`, flagged when it is older than one reconcile interval plus one whole cycle budget (`60 + 300` seconds with both Variables unset) — the stamp is written at the end of a cycle and the loop then waits an interval, so that is the oldest age a watcher doing exactly what it is configured to do can reach. A flagged row means the loop is stuck, restarting or not running at all — or that no enabled subscription names this `conn_id` any more |

A row is written for **every** `conn_id` that appears in the subscription list, including one whose tasks all died. The counts and `last_reconcile_at` move on every cycle, which is what proves the loop is alive; `status` keeps its last stored value whenever the check produced no data.

Rows are never deleted. A `conn_id` no enabled subscription mentions any more stops being stamped and keeps the age it had at that moment, so its row grows permanently stale — including after the documented way to pause consumption, turning the last subscription of that `conn_id` off in the UI. Turning one back on resumes the stamps on the next cycle.

The Status column of the Subscriptions table further down the page is a different one: it reports a single subscription's own task — `connecting`, `listening`, `error` or `disconnected` — while the Connections block above reports the `conn_id` those tasks share (a grouped row shows how many of the group's subscriptions are `listening`).

**Schema migration.** The status table carries `broker_consumer_count` and `last_reconcile_at` alongside its other columns, and the subscription table carries `absent_cycles`, the miss counter of the reconcile cycle; one idempotent migration adds whatever a given database lacks: it inspects the live tables and issues `ALTER TABLE … ADD COLUMN` only for what is missing. The plugin runs it once at load, and the watcher loop retries it at the start of each cycle (one attempt in flight, with a growing backoff between failures) until it succeeds, so a database that was unreachable at load time does not leave the process running against an outdated table. While either table is behind, the block that reads it — Connections or Subscriptions — shows a short notice instead of failing to render the page; while the Subscriptions notice stands, the button that creates a subscription is not offered, because the form behind it writes through the very mapping that could not be read.

### Delivery Guarantee (immediate mode)

For subscriptions with `cooldown=0` the watcher acknowledges a delivery only **after** `trigger_dag` has created the DAG run:

- A failed trigger is NACKed with requeue, logged as a WARNING and retried with a growing backoff (1 s, doubling up to 60 s); the subscription status becomes `error` with the reason, so a subscription that stopped starting DAG runs is visible on the page. One answer out of `trigger_dag` is exempt, and it is the next bullet.
- **One trigger failure the status keeps quiet about:** a DAG Airflow holds no serialized version of yet — `DagNotFound` out of `trigger_dag`, which is the state a freshly started scheduler is in until its DAG processor has parsed the files. The connection, the channel and the consumer registration are all intact, and what the delivery waits for is the rest of Airflow, so the row is held at `listening` — which is what the consumer is doing, and what keeps the subscription a candidate of the liveness check. An `error` an earlier, unrelated failure left there is cleared by the first not-yet-serialized delivery rather than carried through the whole warmup, so a `conn_id` whose subscriptions are all warming up keeps its own row green as well. The delivery itself is requeued and paused for the same growing interval. The quiet is bounded: 25 such deliveries in a row are long enough to be a failure of their own, and the row then reads `error` naming the missing serialized version, with Airflow's own `Dag id ... not found` kept behind it. The streak counts one uninterrupted warmup — a reconnect starts it over, and so does any delivery that goes through or fails for another reason — and every tenth not-yet-serialized delivery is logged as a WARNING. A requeueing nack does not count against a quorum queue's `delivery-limit`, so the delivery keeps coming back until the DAG is serialized and the run starts, or until the streak reaches 25 and the row turns to `error`. The cooldown fire consumer treats the same answer the same way and holds its reported status at `listening`: it has no row of its own, but that status is what keeps it a candidate of the liveness check, so a fire consumer that stops consuming during a warmup is noticed all the same. One such consumer serves every cooldown DAG, so its events are counted per `dag_id` to pick the log level and for nothing else — every tenth is a WARNING. A cooldown DAG that never gets serialized therefore shows up in the scheduler log alone.
- Deduplication rests on the producer. When the message carries an AMQP `message_id`, the run id is deterministic (`rmq__{queue}__{message_id}`, sanitized and truncated with a digest when it does not fit), so a redelivery lands on the run it already produced and is acknowledged as a duplicate. Two *different* messages sharing one `message_id` therefore collapse into a single DAG run. Without `message_id` the run id carries a timestamp instead and a redelivery starts the DAG again.
- **Boundary of the guarantee:** a DAG that is paused, inactive or missing ends in a terminal ACK — the event is dropped on purpose, with a WARNING in the log. NACKing it would turn a paused DAG into an accumulator of redeliveries.
- A message the subscription's filter rejects is NACKed with requeue and goes back to the head of the queue, unless the subscription asked for `ack_unmatched` — then it is acknowledged and gone (see [ADR 0002](docs/adr/0002-four-consume-loops-not-unified.md) and [Filter misses and `ack_unmatched`](#filter-misses-and-ack_unmatched)). A message the filter could not be evaluated on goes back to the queue either way.

**`prefetch` is deliberately not set.** With the default `ack_unmatched=False` filter misses are requeued and return to the head of the queue, so any finite prefetch window would eventually fill up with them and consumption would stop for good — while the status still read `connected`. The window is not set at all rather than set for the subscriptions that acknowledge their misses, because `ack_unmatched=True` does not close the scenario: a message the filter could not be evaluated on goes back to the queue whatever the subscription asked for, and one of those is enough to fill a finite window in time. The cost is accepted explicitly: the number of unacknowledged deliveries is the broker's default consumer prefetch, which is unlimited unless RabbitMQ sets `default_consumer_prefetch` ([consumer prefetch](https://www.rabbitmq.com/docs/consumer-prefetch#configurable-default-prefetch)), so a queue with a large backlog is read into the memory of the watcher process, and the oldest delivery can hit the broker's `consumer_timeout` (30 minutes by default), after which RabbitMQ closes the channel with `PRECONDITION_FAILED` and returns the whole window to the queue. If large backlogs are expected on a watched queue, raise `consumer_timeout` in the RabbitMQ configuration. A finite `default_consumer_prefetch` gives the watcher exactly the finite window described above, so brokers serving the watcher keep it unset.

### Best Practices

- Use a **dedicated queue** per DAG trigger (e.g. `orders.airflow-trigger` separate from `orders`). Avoids NACK hot-loops on quorum queues and interference with other consumers.
- **One queue, one subscription.** A queue that belongs to a single subscription is the configuration the provider is built for, and the one where `ack_unmatched=True` is safe. Two enabled subscriptions on the same `queue_name` and `conn_id` compete for deliveries and are badged on the Subscriptions page — see [Filter misses and `ack_unmatched`](#filter-misses-and-ack_unmatched).
- To pause message consumption without stopping the DAG: **toggle the subscription off** in the UI rather than pausing the DAG. Pausing the DAG acks messages silently.
- In **multi-scheduler HA** deployments each active scheduler runs its own consumer, which may cause duplicate runs. Each of them also runs its own reconcile cycle, and the miss counter of a `dag_file` subscription is shared: N schedulers spend the three-scan grace N times faster, so with three or more of them a subscription can be deleted within a single reconcile interval of going missing. What one scheduler cannot do is undo another's work: every write the counter makes — the miss it records, and the deletion the count leads to — is matched on the value the row was read as, so a scheduler holding a value another has already moved on writes nothing at all. The protection is a deterministic `run_id`: subscriptions with `cooldown > 0` always have one (`rmq_cooldown__{dag_id}__{message_id}`), and immediate ones have it whenever the producer sets an AMQP `message_id` (`rmq__{queue}__{message_id}`) — in both cases the second replica's trigger lands on the DAG run the first one already created. Without a `message_id` an immediate run id carries a timestamp instead and both replicas start the DAG; set `max_active_runs=1` as a lightweight mitigation there.

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
│   │   ├── amqp.py                  # build_amqp_connection(), get_amqp_timeouts(), call_with_timeout(), FilterMissState, handle_miss(), match_and_ack()
│   │   ├── executor.py              # BoundedExecutor (thread pool for blocking calls)
│   │   ├── filters.py               # MessageFilter, FilterVerdict
│   │   ├── management.py            # Management HTTP API client (bindings, queue consumers)
│   │   ├── metrics.py               # Stats counters, no-op without statsd
│   │   └── ssl.py                   # build_ssl_context()
│   └── watcher/
│       ├── decorators.py            # @rmq_trigger
│       ├── subscription_builder.py  # build_subscriptions(), has_exchange_conflict()
│       ├── dag_file_scan.py         # static reading of @rmq_trigger from DAG files; RMQTriggerNotStaticError
│       ├── subscription_form.py     # parse_cooldown(), parse_filter_data() (UI form parsing)
│       ├── models.py                # RMQSubscription, RMQConnStatus, WatcherSession
│       ├── tunables.py              # settings tuning the watcher, with their defaults
│       ├── consumer.py              # RMQConsumerManager
│       ├── orphan_tracker.py        # OrphanTracker (cooldown + exchange-mode orphan detection)
│       ├── listener.py              # RMQWatcherListener (starts and stops the watcher process; the watcher loop)
│       ├── runner.py                # Watcher process entry point (python -c <bootstrap> airflow_provider_rmq.watcher.runner)
│       ├── views.py                 # RMQWatcherView (Flask-AppBuilder UI)
│       └── plugin.py                # RMQWatcherPlugin (AirflowPlugin)
├── docs/
│   ├── adr/                         # Architecture decision records
│   └── example_dags/                # Example DAGs
├── tests/                           # Unit tests
├── integration/                     # Live-broker tests (opt-in, outside testpaths)
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

```bash
# Integration tests against a live broker (opt-in)
pytest integration/ -q
```

The `integration/` directory sits outside `testpaths`, so no ordinary run — `pytest`, `pytest tests/`, or CI — ever touches a broker. It runs only when named explicitly, skips entirely without `RMQ_TEST_URL`, and refuses to run at all unless that URL names a dedicated vhost — never the default `/`. Everything it creates carries a per-run `rmqtest.{pid}.{uuid8}.` prefix and is deleted in teardown. See [integration/README.md](integration/README.md) for the variables, the fixtures and the safety rules.

---

## License

Apache License 2.0. See [LICENSE](LICENSE) for details.
