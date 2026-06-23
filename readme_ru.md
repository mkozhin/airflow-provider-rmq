<h1 align="center">
  Apache Airflow Provider for RabbitMQ
</h1>
<h3 align="center">
  Реактивный запуск DAG из очередей RabbitMQ — плюс хуки, операторы, сенсоры и отложенные триггеры.
</h3>

<p align="center">
  <a href="#установка">Установка</a> &bull;
  <a href="#настройка-подключения">Подключение</a> &bull;
  <a href="#компоненты">Компоненты</a> &bull;
  <a href="#примеры-dag">Примеры</a> &bull;
  <a href="#участие-в-разработке">Участие</a>
</p>

---

*Powered by [Claude Code](https://claude.ai/code)*

---

## Обзор

`airflow-provider-rmq` — провайдер для Apache Airflow × RabbitMQ. Возможности:

- **Реактивный запуск DAG** — [плагин RMQ Watcher](#плагин-rmq-watcher) автоматически запускает DAG при поступлении сообщения в очередь, без поллинга и без занятия слотов воркеров
- Публикация сообщений в обменники (exchanges) и очереди
- Потребление сообщений с фильтрацией по заголовкам и пользовательским функциям
- Ожидание конкретных сообщений с помощью сенсоров (классический poke-режим и отложенный/deferrable режим)
- Отложенный сенсор в режимах **pull** (периодический опрос) и **push** (доставка брокером через `basic_consume`) — выбор зависит от требований к задержке
- Полное управление очередями и обменниками (создание, удаление, очистка, привязка, отвязка)
- SSL/TLS подключения
- Помощник для настройки Dead Letter Queue (DLQ)
- Настройка QoS (prefetch)

### Требования

| Зависимость | Версия |
|---|---|
| Apache Airflow | `>=2.9.0, <3.0.0` |
| pika | `>=1.3.0, <2.0.0` |
| aio-pika | `>=9.0.0, <10.0.0` |
| tenacity | `>=8.0.0` |
| Python | `>=3.10` |

---

## Установка

### Установка из PyPI

```bash
pip install airflow-provider-rmq
```

### Сборка из исходников

```bash
git clone https://github.com/mkozhin/airflow-provider-rmq.git
cd airflow-provider-rmq
pip install build
python -m build
pip install dist/airflow_provider_rmq-*.whl
```

---

## Настройка подключения

Создайте новое подключение в интерфейсе Airflow (**Admin > Connections**):

| Поле | Значение | Описание |
|---|---|---|
| Connection Id | `rmq_default` | Любой уникальный ID |
| Connection Type | `AMQP` | Зарегистрирован провайдером |
| Host | `localhost` | Хост сервера RabbitMQ |
| Port | `5672` | `5671` для SSL |
| Login | `guest` | Имя пользователя RabbitMQ |
| Password | `guest` | Пароль RabbitMQ |
| Schema | `/` | Виртуальный хост (vhost) |

### Настройка SSL/TLS

Добавьте SSL-настройки в поле **Extra** в формате JSON:

```json
{
  "ssl_enabled": true,
  "ca_certs": "/path/to/ca.pem",
  "certfile": "/path/to/client-cert.pem",
  "keyfile": "/path/to/client-key.pem",
  "cert_reqs": "CERT_REQUIRED"
}
```

Хук также предоставляет пользовательские виджеты для SSL-полей (`ssl_enabled`, `ca_certs`, `certfile`, `keyfile`), видимые в форме подключения Airflow.

Установите `"cert_reqs": "CERT_NONE"` для отключения проверки сертификатов (не рекомендуется для продакшена).

---

## Компоненты

### RMQHook

**Импорт:** `from airflow_provider_rmq.hooks.rmq import RMQHook`

Основной хук для всех взаимодействий с RabbitMQ. Использует pika `BlockingConnection` с автоматической логикой повторных попыток (tenacity). Соединение закрывается автоматически, когда объект хука уничтожается сборщиком мусора, поэтому вызывать `close()` вручную не нужно. Контекстный менеджер (`with`) также поддерживается.

#### Параметры конструктора

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `vhost` | `str \| None` | `None` | Нет | Переопределить виртуальный хост из подключения |
| `qos` | `dict \| None` | `None` | Нет | Настройки QoS: `prefetch_size`, `prefetch_count`, `global_qos` |
| `retry_count` | `int` | `3` | Нет | Количество попыток переподключения |
| `retry_delay` | `float` | `1.0` | Нет | Базовая задержка (секунды) между попытками (экспоненциальный рост) |

#### Основные методы

| Метод | Описание |
|---|---|
| `get_channel()` | Возвращает pika `BlockingChannel` (создаёт подключение лениво) |
| `queue_declare(queue_name, passive, durable, exclusive, auto_delete, arguments)` | Объявить очередь |
| `queue_delete(queue_name, if_unused, if_empty)` | Удалить очередь |
| `queue_bind(queue, exchange, routing_key, arguments)` | Привязать очередь к обменнику |
| `queue_unbind(queue, exchange, routing_key, arguments)` | Отвязать очередь от обменника |
| `queue_purge(queue_name)` | Удалить все сообщения из очереди |
| `queue_info(queue_name)` | Получить информацию об очереди (message_count, consumer_count, exists) |
| `exchange_declare(exchange, exchange_type, passive, durable, auto_delete, internal, arguments)` | Объявить обменник |
| `exchange_delete(exchange, if_unused)` | Удалить обменник |
| `exchange_bind(destination, source, routing_key, arguments)` | Привязать обменник к обменнику |
| `exchange_unbind(destination, source, routing_key, arguments)` | Отвязать обменник от обменника |
| `basic_publish(exchange, routing_key, body, properties)` | Опубликовать сообщение |
| `consume_messages(queue_name, max_messages, auto_ack, inactivity_timeout)` | Потребить сообщения из очереди |
| `ack(delivery_tag)` | Подтвердить сообщение |
| `nack(delivery_tag, requeue)` | Отклонить сообщение |
| `build_dlq_arguments(dlx_exchange, dlx_routing_key, message_ttl)` | Статический метод: собрать `x-*` аргументы для поддержки DLQ |
| `test_connection()` | Проверить подключение (используется UI Airflow) |
| `close()` | Закрыть канал и подключение |

#### Пример использования

```python
from airflow_provider_rmq.hooks.rmq import RMQHook

hook = RMQHook(rmq_conn_id="rmq_default")
info = hook.queue_info("my_queue")
print(f"Сообщений в очереди: {info['message_count']}")

hook.basic_publish(
    exchange="",
    routing_key="my_queue",
    body='{"key": "value"}',
)
# Соединение закроется автоматически при выходе hook из области видимости
```

---

### RMQPublishOperator

**Импорт:** `from airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator`

Публикует одно или несколько сообщений в RabbitMQ. Поддерживает строки, словари (автоматическая сериализация в JSON) и списки.

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `exchange` | `str` | `""` | Нет | Обменник для публикации (пустая строка = обменник по умолчанию) |
| `routing_key` | `str` | `""` | Нет | Ключ маршрутизации сообщения |
| `message` | `str \| list[str] \| dict \| list[dict] \| None` | `None` | Нет | Тело сообщения. Словари сериализуются в JSON |
| `queue_name` | `str \| None` | `None` | Нет | Ярлык: устанавливает `exchange=""` и `routing_key=queue_name` |
| `content_type` | `str \| None` | `None` | Нет | AMQP заголовок content type (например, `"application/json"`) |
| `delivery_mode` | `int \| None` | `None` | Нет | `1` = непостоянное, `2` = постоянное (persistent) |
| `headers` | `dict \| None` | `None` | Нет | Пользовательские AMQP заголовки |
| `priority` | `int \| None` | `None` | Нет | Приоритет сообщения (0-9) |
| `expiration` | `str \| None` | `None` | Нет | TTL сообщения в миллисекундах (строка, например `"60000"`) |
| `correlation_id` | `str \| None` | `None` | Нет | Идентификатор корреляции |
| `reply_to` | `str \| None` | `None` | Нет | Имя очереди для ответа |
| `message_id` | `str \| None` | `None` | Нет | Идентификатор сообщения |

**Шаблонные поля:** `exchange`, `routing_key`, `message`

#### Пример использования

```python
# Публикация словаря в очередь
RMQPublishOperator(
    task_id="publish",
    queue_name="my_queue",
    message={"event": "order_created", "id": 42},
    delivery_mode=2,
    headers={"x-source": "airflow"},
)

# Публикация пакета сообщений в обменник
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

**Импорт:** `from airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator`

Потребляет сообщения из очереди RabbitMQ. Подходящие сообщения подтверждаются (ACK) и возвращаются через XCom. Неподходящие отклоняются (NACK) с `requeue=True`.

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `queue_name` | `str` | — | **Да** | Имя очереди для потребления |
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `max_messages` | `int` | `100` | Нет | Максимальное количество сообщений за выполнение |
| `filter_headers` | `dict[str, Any] \| None` | `None` | Нет | Словарь AMQP заголовков для фильтрации. Поддерживает ключи `body.*` для фильтрации по JSON-телу (например, `{"body.data.status": "active"}`) |
| `filter_callable` | `Callable[[Any, str], bool] \| None` | `None` | Нет | Пользовательская функция фильтрации `(properties, body_str) -> bool` |
| `qos` | `dict \| None` | `None` | Нет | Настройки QoS: `{"prefetch_count": 10}` |

**Шаблонные поля:** `queue_name`

**Возвращает:** `list[dict]` — список подошедших сообщений, каждое с ключами: `body`, `headers`, `routing_key`, `exchange`

#### Пример использования

```python
# Потребление с фильтром по заголовкам
RMQConsumeOperator(
    task_id="consume_orders",
    queue_name="orders",
    filter_headers={"x-type": "order"},
    max_messages=50,
    qos={"prefetch_count": 10},
)

# Потребление с фильтром по телу сообщения
RMQConsumeOperator(
    task_id="consume_active",
    queue_name="events",
    filter_headers={"body.status": "active"},
)

# Потребление с пользовательской функцией фильтрации
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

#### Обработка сообщений через TaskFlow API

`RMQConsumeOperator` возвращает `list[dict]` через XCom. Используйте `consume.output` в `@task`-функции для доступа к каждому сообщению:

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
            body = msg["body"]          # тело сообщения (str)
            headers = msg["headers"]    # AMQP заголовки (dict)
            rk = msg["routing_key"]     # ключ маршрутизации
            exchange = msg["exchange"]  # обменник-источник
            log.info("Сообщение: body=%s, headers=%s", body, headers)

            data = json.loads(body)
            results.append(data)
        return results

    processed = process_messages(consume.output)
    processed >> next_task  # передать результаты дальше
```

---

### RMQQueueManagementOperator

**Импорт:** `from airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator`

Выполняет операции управления очередями и обменниками в RabbitMQ.

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `action` | `str` | — | **Да** | Действие (см. таблицу ниже) |
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `queue_name` | `str \| None` | `None` | Условно | Имя очереди (обязательно для операций с очередями) |
| `durable` | `bool` | `False` | Нет | Ресурс переживает перезапуск брокера |
| `exclusive` | `bool` | `False` | Нет | Очередь эксклюзивна для этого подключения |
| `auto_delete` | `bool` | `False` | Нет | Ресурс удаляется, когда больше не используется |
| `exchange_name` | `str \| None` | `None` | Условно | Имя обменника (обязательно для операций с обменниками) |
| `exchange_type` | `str` | `"direct"` | Нет | Тип обменника: `direct`, `fanout`, `topic`, `headers` |
| `internal` | `bool` | `False` | Нет | Обменник не может принимать сообщения напрямую |
| `if_unused` | `bool` | `False` | Нет | Удалять только если нет потребителей/привязок |
| `if_empty` | `bool` | `False` | Нет | Удалять очередь только если она пуста |
| `routing_key` | `str` | `""` | Нет | Ключ маршрутизации для bind/unbind |
| `arguments` | `dict \| None` | `None` | Нет | Дополнительные `x-*` аргументы (например, настройки DLQ) |
| `source_exchange` | `str \| None` | `None` | Условно | Обменник-источник для exchange bind/unbind |

**Шаблонные поля:** `queue_name`, `exchange_name`, `routing_key`, `arguments`

#### Поддерживаемые действия

| Действие | Обязательные параметры | Описание |
|---|---|---|
| `declare_queue` | `queue_name` | Создать очередь |
| `delete_queue` | `queue_name` | Удалить очередь |
| `purge_queue` | `queue_name` | Удалить все сообщения из очереди |
| `bind_queue` | `queue_name`, `exchange_name` | Привязать очередь к обменнику |
| `unbind_queue` | `queue_name`, `exchange_name` | Отвязать очередь от обменника |
| `declare_exchange` | `exchange_name` | Создать обменник |
| `delete_exchange` | `exchange_name` | Удалить обменник |
| `bind_exchange` | `exchange_name`, `source_exchange` | Привязать обменник к обменнику |
| `unbind_exchange` | `exchange_name`, `source_exchange` | Отвязать обменник от обменника |

#### Пример использования

```python
# Создать durable очередь
RMQQueueManagementOperator(
    task_id="create_queue",
    action="declare_queue",
    queue_name="my_queue",
    durable=True,
)

# Создать topic обменник и привязать очередь
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

**Импорт:** `from airflow_provider_rmq.sensors.rmq import RMQSensor`

Ожидает сообщение в очереди RabbitMQ, соответствующее условиям фильтрации. Поддерживает классический poke-режим и отложенный (deferrable) режим.

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `queue_name` | `str` | — | **Да** | Имя очереди для мониторинга |
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `filter_headers` | `dict[str, Any] \| None` | `None` | Нет | Фильтр по заголовкам/телу (словарь) |
| `filter_callable` | `Callable \| None` | `None` | Нет | Пользовательская функция фильтрации. **Не поддерживается с `deferrable=True`** |
| `deferrable` | `bool` | `False` | Нет | Использовать отложенный режим (освобождает слот воркера) |
| `poke_batch_size` | `int` | `100` | Нет | Максимум сообщений за один цикл проверки |
| `poke_interval` | `float` | `60` | Нет | Секунды между проверками (наследуется от BaseSensorOperator) |
| `timeout` | `float` | `604800` | Нет | Максимум секунд ожидания до ошибки (наследуется от BaseSensorOperator) |
| `mode` | `Literal["pull", "push"]` | `"pull"` | Нет | Режим доставки при `deferrable=True`: `"pull"` = периодический опрос, `"push"` = доставка брокером через `basic_consume` |
| `message_wait_timeout` | `float \| None` | `None` | Нет | Максимум секунд ожидания в push-режиме. `None` = без ограничений. Только с `mode="push"`. Поддерживает Jinja-шаблоны и XCom |

**Шаблонные поля:** `queue_name`, `message_wait_timeout`

**Возвращает:** `dict | None` — подошедшее сообщение с ключами: `body`, `headers`, `routing_key`, `exchange`

#### Отложенный (deferrable) режим

При `deferrable=True` сенсор передаёт выполнение в процесс triggerer через `RMQTrigger`. Это освобождает слот воркера на время ожидания, что более эффективно при длительном ожидании.

**Ограничение:** `filter_callable` нельзя использовать с `deferrable=True`, так как Python-функции не могут быть сериализованы для передачи в triggerer. Используйте `filter_headers` вместо этого.

#### Pull vs Push режим

Параметр `mode` (актуален только при `deferrable=True`) определяет способ получения сообщений триггером:

| | `mode="pull"` (по умолчанию) | `mode="push"` |
|---|---|---|
| Механизм | Периодический `queue.get()` + sleep | Подписка через `basic_consume` |
| Задержка | До `poll_interval` секунд | Мгновенно — брокер доставляет сразу |
| Idle-нагрузка | Опрос даже при пустой очереди | Нет активности до прихода сообщения |
| Когда использовать | Простота, предсказуемость | Минимальная задержка, тихие очереди |

> **Поведение при таймауте:** когда `message_wait_timeout` истекает, сенсор бросает `AirflowSkipException` — таск помечается как **SKIPPED** (не FAILED), downstream-таски пропускаются. `on_failure_callback` не вызывается. Это делает `message_wait_timeout` безопасным для плановых остановок (например, конец рабочего дня) без ложных алертов.

> **RabbitMQ 4.0+ quorum queues:** несовпадающие сообщения отклоняются с `requeue=True`. Quorum-очереди ограничивают повторные доставки до 20 по умолчанию — после этого сообщение dead-letter'ится или удаляется. Актуально для обоих режимов.

#### Пример использования

```python
# Классический poke-режим с пользовательским фильтром
RMQSensor(
    task_id="wait_for_order",
    queue_name="orders",
    filter_callable=lambda props, body: "urgent" in body,
    poke_interval=10,
    timeout=300,
    mode="reschedule",
)

# Отложенный pull-режим (по умолчанию)
RMQSensor(
    task_id="wait_for_event",
    queue_name="events",
    filter_headers={"x-type": "payment"},
    deferrable=True,
    timeout=600,
)

# Отложенный push-режим — брокер доставляет мгновенно, таймаут 60 сек
RMQSensor(
    task_id="wait_for_event_push",
    queue_name="events",
    filter_headers={"x-type": "payment"},
    deferrable=True,
    mode="push",
    message_wait_timeout=60,
    timeout=120,
)

# Динамический таймаут через XCom — например, сколько секунд осталось до конца рабочего дня
RMQSensor(
    task_id="wait_for_message",
    queue_name="events",
    deferrable=True,
    mode="push",
    message_wait_timeout="{{ ti.xcom_pull(task_ids='compute_timeout') }}",
)
```

#### Обработка результата сенсора через TaskFlow API

`RMQSensor` возвращает `dict | None` через XCom. Используйте `sensor.output` в `@task`-функции для доступа к найденному сообщению:

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
        log.info("Получено: %s", message)
        return message

    handle_event(wait.output)
```

---

### RMQTrigger

**Импорт:** `from airflow_provider_rmq.triggers.rmq import RMQTrigger`

Асинхронный триггер для отложенного режима сенсора. Использует `aio_pika` для неблокирующего AMQP-доступа. Обычно не используется напрямую — `RMQSensor` с `deferrable=True` создаёт его автоматически.

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | — | **Да** | ID подключения Airflow |
| `queue_name` | `str` | — | **Да** | Очередь для мониторинга |
| `filter_data` | `dict \| None` | `None` | Нет | Сериализованный фильтр из `MessageFilter.serialize()` |
| `poll_interval` | `float` | `5.0` | Нет | Секунды между опросами при пустой очереди (только pull-режим) |
| `mode` | `Literal["pull", "push"]` | `"pull"` | Нет | Режим доставки: `"pull"` = опрос, `"push"` = `basic_consume` |
| `message_wait_timeout` | `float \| None` | `None` | Нет | Максимум секунд ожидания в push-режиме. Фактическое время может быть чуть больше из-за `basic_cancel` |

---

### MessageFilter (утилита)

**Импорт:** `from airflow_provider_rmq.utils.filters import MessageFilter`

Проверяет, соответствует ли сообщение RabbitMQ заданным условиям фильтрации. Используется внутри операторами и сенсорами.

#### Режимы фильтрации

1. **Фильтрация по заголовкам** (`filter_headers`): словарь пар ключ-значение, которым должны соответствовать заголовки сообщения.
   - Обычные ключи проверяют словарь `properties.headers`
   - Ключи, начинающиеся с `body.`, обходят JSON-тело сообщения (например, `{"body.data.status": "active"}`)

2. **Фильтрация функцией** (`filter_callable`): `fn(properties, body_str) -> bool`

Оба режима можно комбинировать (логика И: оба условия должны выполниться).

---

## Плагин RMQ Watcher

**Плагин RMQ Watcher** инвертирует обычный паттерн с сенсором: вместо того чтобы DAG ждал сообщения, сообщение в RabbitMQ само *вызывает* запуск DAG — без поллинга, без `deferred`-тасков, без потребления ресурсов воркеров.

### Как это работает

Внутри процесса Scheduler запускается фоновый asyncio-цикл (через Airflow Listener API), который подписывается на очереди через AMQP `basic_consume`. При поступлении подходящего сообщения `trigger_dag()` вызывается напрямую внутри процесса. Одно соединение `connect_robust` на `conn_id` разделяется между всеми подписками к одному кластеру.

Каждые 60 секунд (настраивается через Airflow Variable `rmq_watcher_reconcile_interval`) цикл reconciliation пересканирует DAG-файлы на наличие декораторов `@rmq_trigger` (инкрементально по mtime — перепарсиваются только изменившиеся файлы) и синхронизирует подписки в БД.

### Быстрый старт

**Шаг 1 — добавьте декоратор к DAG:**

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.watcher.decorators import rmq_trigger

@rmq_trigger(queue="orders", conn_id="rmq_default")
@dag(schedule=None)
def orders_dag():
    @task
    def process(**context):
        conf = context["dag_run"].conf
        print(f"Тело: {conf['body']}, Заголовки: {conf['headers']}")
    process()

orders_dag()
```

**Шаг 2** — перезапустите Scheduler. Плагин активируется автоматически, дополнительная конфигурация не требуется.

**Шаг 3** — опубликуйте сообщение в очередь `orders` — DAG запустится в течение нескольких секунд.

### Несколько очередей и cooldown

Подпишите один DAG на несколько очередей и ограничьте частоту запусков параметром `cooldown`:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.watcher.decorators import rmq_trigger

@rmq_trigger(
    queues=["orders", "payments"],  # сообщение из любой очереди запускает DAG
    cooldown=300,                    # cooldown 300 с — DAG запускается один раз за окно
    conn_id="rmq_default",
)
@dag(dag_id="my_dag", schedule=None)
def my_dag():
    @task
    def process(**context):
        conf = context["dag_run"].conf
        # conf["source"] == "cooldown" при запуске через механизм cooldown
        # conf["body"] и conf["headers"] пусты — исходные данные теряются в DLX-цепочке
        print(conf["source"])
    process()

my_dag()
```

**Как работает cooldown:**

- При поступлении первого подходящего сообщения плагин публикует TTL-маркер в `rmq_watcher.pending.{dag_id}` (очередь без потребителей, `x-max-length=1`, DLX в `rmq_watcher.fire`).
- Через N секунд маркер истекает и маршрутизируется в `rmq_watcher.fire`; fire consumer вызывает `trigger_dag()` с идемпотентным run_id.
- Дополнительные сообщения в течение окна cooldown подтверждаются (ACK) молча — pending-очередь отклоняет повторный publish (`x-overflow=reject-publish`).
- Вся RMQ-инфраструктура (exchange и очередь `rmq_watcher.fire`, а также per-DAG очереди `rmq_watcher.pending.*`) создаётся плагином автоматически при запуске.

**Ограничения:**
- Все cooldown-подписки одного DAG делят одну pending-очередь и один таймер.
- Все cooldown-DAG должны использовать один `conn_id` / vhost.
- `conf["body"]` и `conf["headers"]` пусты при запуске через cooldown — исходные данные сообщения теряются в DLX-цепочке.
- Изменение `cooldown` в DAG-файле вступает в силу на следующем reconcile-цикле (по умолчанию 60 с); уже запущенные таймеры в RMQ не затрагиваются.

**Права доступа RabbitMQ (только при cooldown):**

При `cooldown > 0` пользователю Airflow необходимы права configure/write/read на шаблон ресурсов `rmq_watcher.*` помимо прав на прикладные очереди:

```
rabbitmqctl set_permissions -p <vhost> <user> "^(rmq_watcher\\..*|your-queue.*)$" "^(rmq_watcher\\..*|your-queue.*)$" "^(rmq_watcher\\..*|your-queue.*)$"
```

Это охватывает exchange `rmq_watcher.fire`, очередь `rmq_watcher.fire` и очереди `rmq_watcher.pending.<dag_id>`, которые создаёт механизм cooldown.

### Триггеры в режиме exchange

Вместо подписки на заранее созданную и привязанную вручную очередь (`queue=`/`queues=`), DAG может подписаться напрямую на topic-обменник. Когда указан `exchange=`, провайдер полностью берёт на себя RMQ-инфраструктуру — ручное создание очереди и внешняя YAML-таблица маршрутов не нужны:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.watcher.decorators import rmq_trigger

# Routing key в форме Jetstat: декартово произведение id × status
@rmq_trigger(
    exchange="jetstat.airflow",
    routing_key_ids=["670f877702775c2de8325b1f"],
    routing_key_status="succeeded",   # по умолчанию "*" = любой статус
)
@dag(dag_id="jetstat_succeeded", schedule=None)
def jetstat_succeeded_dag():
    @task
    def process(**context):
        conf = context["dag_run"].conf
        print(conf["routing_key"])  # "670f877702775c2de8325b1f.succeeded"
    process()

jetstat_succeeded_dag()

# Произвольные routing key любой формы (не привязаны к форме id/status)
@rmq_trigger(exchange="some.other.exchange", routing_keys=["region.eu.alert"])
@dag(dag_id="region_alerts", schedule=None)
def region_alerts_dag():
    ...
```

Обе формы можно комбинировать в одном вызове — итоговый набор routing key является объединением `routing_keys` и декартова произведения `routing_key_ids` × `routing_key_status`.

**Что провайдер создаёт автоматически** на каждом reconcile-цикле:

- Сам обменник (topic, durable, с `alternate-exchange` для немаршрутизируемых сообщений)
- Выделенную очередь `rmq_watcher.sub.{dag_id}` — **одна общая очередь на DAG**, потребляется точно так же, как любая подписка через `queue=`
- Привязки (bindings) между этой очередью и обменником, синхронизированные с routing key, объявленными в декораторе на текущий момент (сравниваются с реальным состоянием привязок в RabbitMQ через Management HTTP API — а не с тем, что хранится в БД Airflow)
- Страховочные механизмы: немаршрутизируемые сообщения попадают в `{exchange}.unrouted` (TTL 8ч); каждое промаршрутизированное сообщение зеркалируется в `{exchange}.log` (catch-all привязка `#`, TTL 8ч) для последующего логирования/аудита

**Extra-параметр подключения — `management_url`:** режим exchange требует эндпоинт Management HTTP API для чтения текущих привязок (в AMQP 0-9-1 нет операции «показать мои привязки»). Добавьте его в тот же Airflow Connection, который используется для AMQP:

```json
{
  "management_url": "https://rabbitmq.example.com"
}
```

Те же `login`/`password` из подключения повторно используются для вызова Management API. Если `management_url` не задан, bind-diff пропускается на каждом цикле (логируется как ERROR) — очередь всё равно объявляется и потребляется как обычно, но привязки никогда не создаются/обновляются.

**Без стекинга — один DAG, один обменник.** Несколько декораторов `@rmq_trigger(exchange=...)` на одном DAG вызывают `ValueError` на этапе декорирования — все они привели бы к одной и той же очереди `rmq_watcher.sub.{dag_id}`, и последний обработанный декоратор молча победил бы. Используйте один вызов декоратора с объединением routing key, либо подписывайтесь на разные обменники из разных DAG. Чтобы потреблять из нескольких обменников в одном DAG, используйте `queue=`/`queues=` с очередями, созданными и привязанными вручную.

**Права RabbitMQ (только для режима exchange):** в дополнение к шаблону `rmq_watcher\..*`, уже требуемому для cooldown, пользователю Airflow в RMQ нужны:

```
# configure: объявление обменника / alternate-exchange / его очередей
rabbitmqctl set_permissions -p <vhost> <user> "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted|\\.log)?|...)$" \
  "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted|\\.log)?|...)$" \
  "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted)?|...)$"
```

`configure`/`write` нужны на `{exchange}(.unrouted|.log)?`; `read` дополнительно нужен на `{exchange}(.unrouted)?`, потому что привязка очереди *от* обменника требует права read на обменник-источник, а не только configure на целевую очередь. Замените `jetstat.airflow` на имя, которое реально передаётся в `exchange=`.

**Миграция с `queue=` на `exchange=`:** переключение существующей подписки не убирает за собой старую инфраструктуру — старая, созданная вручную очередь **не** удаляется автоматически и остаётся без консьюмера после повторного деплоя DAG-файла с `exchange=`. Удалите её вручную после того, как убедитесь, что миграция работает корректно.

**Переименование DAG:** изменение `dag_id` приводит к созданию новой очереди/привязок `rmq_watcher.sub.{new_dag_id}` на следующем reconcile-цикле. Старая `rmq_watcher.sub.{old_dag_id}` становится сиротой (orphan) — её метаданных подписки больше нет ни в одном распарсенном DAG-файле (см. ADR-0005) — и **не** удаляется автоматически. Удалите её вручную.

**Мониторинг:**

- RabbitMQ Management UI — `rmq_watcher.sub.{dag_id}` должна показывать `consumer count > 0`, когда подписка DAG активна
- Логи Airflow — WARNING для осиротевших очередей/привязок `rmq_watcher.sub.*` (с подсказкой `rabbitmqadmin delete queue ...`); ERROR для пропущенного bind-diff (Management API недоступен) или конфликта свойств обменника (`PRECONDITION_FAILED` — имя обменника уже занято чем-то другим с другими свойствами)

**Откат (rollback):** удалите `exchange=`/`routing_keys=`/`routing_key_ids=`/`routing_key_status=` из декоратора и передеплойте. `rmq_watcher.sub.{dag_id}` становится сиротой — в логах появляется WARNING, TTL (8ч) ограничивает неконтролируемый рост, а ручная очистка выполняется по подсказке из текста WARNING. Сам обменник и его очереди `.unrouted`/`.log` откатом **не** затрагиваются (ими могут всё ещё пользоваться другие DAG).

### Payload, передаваемый в DAG

```python
conf = context["dag_run"].conf
# Немедленный запуск (cooldown=0 или без cooldown):
# {
#     "source":          "immediate",
#     "body":            "<тело сообщения в UTF-8>",
#     "headers":         {"key": "value", ...},
#     "routing_key":     "orders.created",
#     "queue":           "orders",
#     "subscription_id": 42,
# }
# Триггеры в режиме exchange (exchange=) используют точно такую же форму "immediate" —
# "queue" — это всегда rmq_watcher.sub.{dag_id} (а не имя обменника), а "routing_key" —
# это фактический подошедший routing key (например, "<id>.<status>" для формы routing_key_ids).
#
# Cooldown-запуск (после истечения TTL в rmq_watcher.fire):
# {
#     "source":          "cooldown",
#     "body":            "",        # пусто — тело исходного сообщения не сохраняется
#     "headers":         {},        # пусто — заголовки исходного сообщения не сохраняются
#     "routing_key":     "<dag_id>",
#     "queue":           "rmq_watcher.fire",
#     "subscription_id": None,
# }
```

### Управление подписками

| Способ | Описание |
|---|---|
| Декоратор `@rmq_trigger` | Infrastructure as Code — подписка живёт в DAG-файле, управляется через git |
| Airflow UI по адресу `/rmq-watcher/subscriptions` | Создание, редактирование, включение/отключение, удаление (только для UI-подписок) |
| Прямая запись в БД | Для автоматизации через Terraform / скрипты (`source='ui'`) |

Подписки типа `dag_file` — **только для чтения** в UI: reconciliation перезаписывает БД из кода каждые 60 с. Через UI можно изменить только переключатель `enabled`.

Подписки в режиме exchange отображаются в UI как любая другая подписка `dag_file` — только по имени их очереди (`rmq_watcher.sub.{dag_id}`). Метаданные `exchange`/`routing_keys` там не показываются; единственным источником истины для них остаётся файл DAG.

### Лучшие практики

- Используйте **выделенную очередь** для каждого триггера DAG (например, `orders.airflow-trigger` отдельно от `orders`). Это исключает NACK-циклы на quorum queues и интерференцию с другими консьюмерами.
- Чтобы приостановить потребление сообщений без остановки DAG: **отключите подписку** в UI, а не ставьте DAG на паузу. Пауза DAG подтверждает (ack) сообщения без их обработки.
- В **HA-режиме с несколькими scheduler'ами** каждый активный scheduler запускает свой консьюмер, что может привести к дублирующимся запускам. Установите `max_active_runs=1` как минимальную меру защиты. Исключение: подписки с `cooldown > 0` идемпотентны по своей природе — детерминированный `run_id` (`rmq_cooldown__{dag_id}__{message_id}`) предотвращает дублирующиеся запуски DAG даже при нескольких scheduler'ах. Подпискам с `cooldown=0` по-прежнему нужен `max_active_runs=1`.

---

## Примеры DAG

Пакет включает несколько примеров DAG в `docs/example_dags/`. Все примеры используют **TaskFlow API** (декораторы `@dag` / `@task`) и демонстрируют **обработку полученных сообщений** в downstream-тасках через XCom.

| DAG | Описание |
|---|---|
| `rmq_example_basic` | Публикация, ожидание, потребление, обработка сообщений, очистка |
| `rmq_publish_advanced` | Продвинутая публикация со всеми AMQP-свойствами, пакетная отправка, topic exchange |
| `rmq_consume_with_filters` | Фильтры по заголовкам, по телу, пользовательские функции, QoS — с пошаговой обработкой сообщений |
| `rmq_sensor_deferrable` | Отложенный сенсор в pull-режиме с фильтрацией по заголовкам и обработкой сообщений |
| `rmq_sensor_push` | Отложенный сенсор в **push-режиме** — брокер доставляет сообщения мгновенно через `basic_consume` |
| `rmq_watcher_demo` | **Плагин RMQ Watcher** — DAG, запускаемый реактивно сообщениями из RabbitMQ через `@rmq_trigger`; также работает по расписанию |
| `rmq_pipeline_start` / `rmq_pipeline_finish` | Паттерн блокировки пайплайна — предотвращение параллельных запусков |
| `rmq_dlq_setup` | Настройка инфраструктуры Dead Letter Queue с DLX, TTL, exchange-to-exchange привязками |

---

## Структура репозитория

```
airflow-provider-rmq/
├── airflow_provider_rmq/
│   ├── __init__.py                  # Метаданные провайдера и get_provider_info()
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
│   │   ├── amqp.py                  # build_amqp_connection(), match_and_ack()
│   │   ├── filters.py               # MessageFilter
│   │   └── ssl.py                   # build_ssl_context()
│   └── watcher/
│       ├── decorators.py            # @rmq_trigger
│       ├── models.py                # RMQSubscription, RMQConnStatus, WatcherSession
│       ├── consumer.py              # RMQConsumerManager
│       ├── listener.py              # RMQWatcherListener (Listener API)
│       ├── views.py                 # RMQWatcherView (Flask-AppBuilder UI)
│       └── plugin.py                # RMQWatcherPlugin (AirflowPlugin)
├── docs/
│   └── example_dags/                # Примеры DAG
├── tests/                           # Модульные тесты
├── CHANGELOG.md
├── pyproject.toml
└── readme.md
```

---

## Запуск тестов

```bash
# Установка dev-зависимостей
pip install -e ".[dev]"

# Запуск всех тестов
pytest tests/

# Запуск конкретного модуля тестов
pytest tests/test_trigger.py -v
```

---

## Лицензия

Apache License 2.0. См. [LICENSE](LICENSE) для подробностей.
