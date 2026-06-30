# RMQ Watcher Plugin — push-триггер DAG'ов из RabbitMQ

## Overview

### Идея и назначение

Этот плагин решает задачу **реактивного запуска DAG'ов**: когда сообщение появляется в очереди
RabbitMQ, Airflow автоматически запускает связанный DAG — без какого-либо периодического опроса,
без постоянно висящих DAG-задач и без занятия слотов воркеров.

**Проблема, которую решает:**

Стандартный `RMQSensor` требует, чтобы DAG уже выполнялся и задача висела в состоянии
`deferred` — то есть DAG-ран должен быть инициирован заранее. Это неудобно, если события
в очереди редкие или непредсказуемые: нужно либо запускать DAG по расписанию с запасом,
либо вручную. Плагин инвертирует эту логику: сообщение в очереди само является причиной
запуска DAG, а не его следствием.

### Как это работает

Плагин добавляет в Airflow три связанных механизма:

**1. Scheduler Listener — постоянный слушатель RabbitMQ**

При старте процесса scheduler'а (через Airflow Listener API, доступный с Airflow 2.4+)
запускается фоновый daemon-поток с собственным asyncio event loop. Этот поток:
- устанавливает соединения с RabbitMQ — **одно соединение на каждый `conn_id`**,
  а не одно на подписку; для 10 DAG'ов с одним и тем же `conn_id` будет 1 соединение
  и 10 каналов (channels) внутри него
- использует `queue.iterator()` (AMQP `basic_consume`) — брокер сам доставляет сообщения,
  без поллинга
- при получении сообщения, прошедшего фильтр, вызывает `trigger_dag()` напрямую
  (без HTTP, внутри процесса)
- тело сообщения передаётся в DAG через `conf`

Поток не виден в UI как DAG, не занимает task slots, не зависит от воркеров.

**2. Reconciliation loop — синхронизация подписок**

Каждые 60 секунд (настраиваемо) reconciliation loop:
- создаёт `DagBag` из DAG-файлов (не из DB) — чтобы прочитать атрибут `_rmq_subscriptions`,
  добавляемый декоратором
- сравнивает найденные подписки с таблицей `rmq_watcher_subscriptions` в Airflow DB
- делает upsert для подписок из кода (source='dag_file')
- удаляет `dag_file`-подписки которых **больше нет в коде** — это касается как
  удалённых DAG-файлов, так и DAG'ов у которых убрали `@rmq_trigger` с конкретной очереди
  (DAG может остаться, а подписка исчезнуть — они независимы)
- перезагружает RMQ-консьюмеры при изменениях

**3. Три способа задать подписку:**

*Способ A — декоратор в DAG-файле (Infrastructure as Code):*
```python
# orders_dag.py
from airflow_provider_rmq.watcher.decorators import rmq_trigger

@rmq_trigger(queue="orders", conn_id="rmq_default", filter_data={"type": "new_order"})
@dag(schedule="0 9 * * *")
def orders_dag(): ...
```
Декоратор добавляет атрибут `_rmq_subscriptions` на DAG-объект. Подписка живёт в git
рядом с кодом DAG, меняется и удаляется вместе с ним.

*Способ B — через UI:*
Страница `/rmq-watcher/subscriptions` в Airflow webserver — список всех подписок,
создание, редактирование, удаление через форму.

**Правило конфликта кода и UI:** `dag_file`-подписки — read-only в UI. Код является
источником истины. Если подписка задана в DAG-файле, в UI можно только переключить
`enabled`. Редактирование других полей и удаление заблокированы (показывается flash-ошибка).
При каждом reconciliation код перезаписывает DB — любая ручная правка `dag_file`-подписки
через прямой SQL будет отменена в течение 60 секунд.

*Способ C — прямая запись в таблицу:*
Для автоматизации через скрипты/Terraform/etc.

### Ключевые архитектурные решения

| Решение | Почему |
|---------|--------|
| Listener API (не отдельный процесс) | Живёт внутри Airflow, единая установка пакета |
| Фоновый поток в scheduler (не в webserver) | Scheduler имеет built-in leader election в HA-режиме |
| asyncio + aio_pika (не pika) | queue.iterator() — push без поллинга; совместимо с event loop |
| DagBag с чтением файлов в reconciliation | Атрибуты DAG-объектов не сериализуются в Airflow DB |
| trigger_dag() без HTTP | Прямой вызов внутри процесса быстрее и надёжнее |
| source='dag_file'/'ui' в таблице | Позволяет отличать IaC-подписки от ручных; управлять ими по-разному |

### Решённые технические риски

**trigger_dag() из фонового потока — thread-safety:**
`airflow.api.common.trigger_dag.trigger_dag()` обращается к Airflow DB.
Вызывать из фонового asyncio-потока через `run_in_executor` можно, но только если каждый
вызов использует **свою краткоживущую сессию** (`with create_session() as session`),
а не переиспользует Airflow scoped session основного потока.
Решение: `_trigger_dag()` создаёт сессию явно, не полагается на thread-local session.

**ensure_table_exists() — где запускать:**
`plugin.on_load()` выполняется во всех процессах (webserver, scheduler, triggerer, worker).
Решение: `ensure_table_exists()` вызывается **только в `on_load()`** плагина —
это гарантирует, что таблицы существуют до первого HTTP-запроса к webserver.
`create_all(checkfirst=True)` идемпотентен, вызов из нескольких контейнеров безопасен.
В `_start()` таблицы НЕ создаются (это уже сделано в `on_load()`).
Предпосылка: docker-compose с `depends_on: postgres` — DB готова до старта любого контейнера.

**NACK-requeue горячий цикл:**
При нефильтрующей подписке broker немедленно ревоставляет nacked сообщения → CPU/сетевой
hot loop. На quorum queues RMQ 4.x limit=20 доставок → dead-letter.
Решение: после `nack(requeue=True)` добавить `await asyncio.sleep(0.1)` (аналог триггера).

Поведение нематчащих сообщений: `nack(requeue=True)` — сообщение возвращается в очередь
и доступно другим консьюмерам. Это безопасный дефолт для случаев, когда Watcher делит
очередь с другими сервисами.

Рекомендация в документации (best practice): использовать **выделенную очередь** для каждого
DAG-триггера (например, `orders.airflow-trigger` отдельно от `orders`). Тогда Watcher —
единственный консьюмер, нет интерференции, нет риска цикличной доставки на classic queues.
В docstring `@rmq_trigger` и в README указать это явно.

**passive=True + несуществующая очередь:**
Опечатка в имени очереди даёт вечный silent reconnect loop.
Решение: отличать fatal ошибки (`ChannelNotFoundEntity`) от transient; при fatal —
логировать как ERROR, записывать `last_error` в DB, **не** делать retry до следующего
reconciliation. Добавить колонку `last_error` в таблицу.

**Имя компонента в on_starting:**
В Airflow 2.7+ компонент — `SchedulerJobRunner`. Проверка по подстроке `"Scheduler"` в имени
работает для 2.7–2.10. Добавить явный тест с реальным именем класса и логировать
имя компонента при старте для диагностики.

**run_id — убрать противоречие:**
Timestamp run_id (`rmq__{queue}__{utcnow}`) не даёт идемпотентности.
Решение: использовать timestamp (прагматично), но убрать из Overview упоминание
"unique constraint защищает от дублей в HA" — это неверно. HA-защита остаётся future work.

### Будущие режимы триггера (не реализуются сейчас)

Текущая реализация поддерживает только режим `'any'` (OR): любое сообщение в любой из
подписанных очередей запускает DAG независимо. Это поведение двух `@rmq_trigger` на одном DAG.

DB-схема уже содержит колонки `trigger_mode`, `group_key`, `cooldown` для поддержки:
- `'all'` (AND): DAG запускается только когда **все** очереди группы получили сообщение
- `'debounce'`: первое сообщение запускает таймер `cooldown` секунд; все последующие сообщения
  за этот период ackуются без запуска; по истечении таймера — один запуск DAG со списком
  всех накопленных сообщений в `conf["messages"]`; новое сообщение после запуска — новый цикл

`group_key` связывает подписки в одну группу. Consumer manager в текущей реализации
игнорирует эти колонки (всегда работает как `'any'`). Реализация остальных режимов —
отдельная задача следующего релиза.

Декоратор для будущих режимов (API-набросок, не реализуется сейчас):
```python
@rmq_trigger_group(queues=["q1", "q2"], mode="debounce", cooldown=120, conn_id="rmq_default")
@dag()
def my_dag(): ...
```

### HA-ограничения (известные, для документации)

В multi-scheduler HA-режиме фоновый поток запустится на каждом активном scheduler'е,
что может привести к дублирующимся DAG-ранам. Решение для MVP: документировать ограничение.
В будущем: DB-блокировка через `airflow.models.TaskInstance.ti_selector_condition` или
отдельная lock-таблица.

Timestamp-based run_id (`rmq__{queue}__{utcnow}`) не даёт идемпотентности в HA.
Для настоящей защиты нужна DB-блокировка — future work.

### Тот же репозиторий или новый?

Решение: **тот же репозиторий** (`apache-airflow-provider-rmq`), новый подпакет `watcher/`.

Обоснование:
- `MessageFilter`, паттерн aio_pika-подключения, SSL-утилиты — уже есть, дублировать не нужно
- Пользователи устанавливают один пакет для всего RMQ-функционала
- Меньше накладных расходов на поддержку (CI, версионирование, PyPI)
- Watcher логически связан с провайдером — это инфраструктурная надстройка над тем же RMQ

Когда имело бы смысл выносить в отдельный пакет: если watcher потребовал бы зависимостей,
несовместимых с провайдером (сейчас таких нет).

### Безопасная работа с Airflow DB

Мы добавляем свои таблицы в ту же БД что использует Airflow. Это нормально, но требует
**явной изоляции** — чтобы наш код не мог случайно повредить Airflow's session state.

**Правило 1: собственный `WatcherBase`, не Airflow's `Base`**

`airflow.models.base.Base` — это общий SQLAlchemy declarative base для ВСЕХ таблиц Airflow.
Вызов `Base.metadata.create_all()` итерирует метаданные сотен таблиц Airflow (dag, dag_run,
task_instance и т.д.). Это нежелательный side effect даже с `checkfirst=True`.

Решение: объявить **отдельный** `WatcherBase` только для наших 2 таблиц:
```python
from sqlalchemy.orm import declarative_base
WatcherBase = declarative_base()
# WatcherBase.metadata.create_all() — трогает только rmq_watcher_* таблицы
```

**Правило 2: собственный `WatcherSession`, не Airflow's `create_session()`**

Airflow's `Session` — это `scoped_session` привязанный к thread-local state. Использование
его из фонового потока безопасно, но риск ошибки (незакрытая сессия → грязный state) высок.

Решение: свой `sessionmaker` на том же движке, полностью независимый:
```python
from airflow.settings import engine
from sqlalchemy.orm import sessionmaker
WatcherSession = sessionmaker(bind=engine)

# везде в коде:
with WatcherSession() as session:
    do_something(session)
    session.commit()
# сессия закрывается при выходе из with — гарантировано
```

Webserver читает через тот же `WatcherSession` — ему тоже не нужен Airflow's scoped session.

**Правило 3: in-memory state guard — писать только при изменении статуса**

При нестабильном RMQ consumer task может входить в reconnect loop (connecting → listening →
connecting → ...). Без guard это N writes/секунду. С guard — пишем только при реальном
изменении значения:
```python
if self._last_written_status != new_status:
    _write_status_to_db(new_status)
    self._last_written_status = new_status
```
Стабильный RMQ: 2-3 writes при старте. Flapping RMQ: 2-4 writes/мин вместо 20+.

**Правило 4: проверять DAG перед trigger_dag() + ловить IntegrityError**
```python
def _sync_trigger(dag_id, conf, run_id):
    with WatcherSession() as session:
        dag_model = session.query(DagModel).filter_by(
            dag_id=dag_id, is_active=True, is_paused=False
        ).first()
        if not dag_model:
            log.warning("DAG %s not found, inactive or paused — message acked, skipping trigger", dag_id)
            return
    try:
        trigger_dag(dag_id=dag_id, run_id=run_id, conf=conf)
    except IntegrityError:
        log.warning("DAG run %s already exists (duplicate), skipping", run_id)
```
Поведение при паузе DAG: сообщение **ackуется** (удаляется из очереди), DAG не запускается.
Consumer продолжает работать и потреблять сообщения. Документировать как known behavior:
"пауза DAG в Airflow UI не останавливает потребление очереди — для этого используйте
toggle enabled на подписке в RMQ Watcher UI".

**SQLite (dev/test окружения):**
SQLite имеет table-level lock. Частые writes нашего потока могут временно блокировать
scheduler heartbeat. Для production (PostgreSQL/MySQL) — row-level locks, проблем нет.
Задокументировать как known limitation для SQLite-окружений.

### Что плагин НЕ меняет

- `RMQTrigger`, `RMQSensor`, `RMQHook` — без изменений, полная обратная совместимость
- Существующие DAG'и без `@rmq_trigger` — не затронуты
- Логика фильтрации — переиспользуется из `airflow_provider_rmq.utils.filters.MessageFilter`

---

## Context (from discovery)

- **Пакет:** `airflow-provider-rmq`, Python ≥ 3.10, Airflow ≥ 2.7.0 (Listener API доступен)
- **Версия релиза:** `2.0.0` (major bump — новая реактивная инфраструктура, обратно совместима с 1.x)
- **Переиспользуемый код:**
  - `airflow_provider_rmq/utils/filters.py` — `MessageFilter` (dict/callable фильтрация)
  - `airflow_provider_rmq/triggers/rmq.py` — паттерн aio_pika подключения и `_handle_message`
  - `airflow_provider_rmq/hooks/rmq.py` — `RMQHook` (pika, синхронный — не используется в watcher)
- **Тестирование:** pytest + pytest-asyncio, паттерны мокирования aio_pika в `tests/test_trigger.py`
- **Существующие планы в completed/:** `20260406-push-mode-trigger.md`
- **Нет существующего `watcher/` модуля** — создаём с нуля

## Development Approach

- **Testing approach:** Regular (сначала код, потом тесты)
- Каждая задача полностью завершается до перехода к следующей
- Все тесты проходят до старта следующей задачи
- Каждая задача включает тесты как обязательный deliverable

## Testing Strategy

- **Unit tests:** pytest + pytest-asyncio; мокирование aio_pika (паттерн из `test_trigger.py`)
- **Для models:** SQLite in-memory engine с `WatcherBase.metadata.create_all()` — реальная DB, не mock
- **Для consumer:** mock `queue.iterator()`, mock `WatcherSession` для проверки DB-вызовов
- **Для listener:** mock DagBag, mock `WatcherSession`
- **Для views:** Flask test client; mock `WatcherSession` для изоляции от реальной DB
- **Для декоратора:** проверка атрибутов DAG-объекта без DB

## Progress Tracking

- `[x]` — задача выполнена
- `➕` — добавлено в процессе
- `⚠️` — блокер

## Solution Overview

```
airflow_provider_rmq/watcher/
  decorators.py   → @rmq_trigger — добавляет _rmq_subscriptions на DAG
  models.py       → SQLAlchemy модель rmq_watcher_subscriptions
  consumer.py     → asyncio менеджер RMQ-консьюмеров
  listener.py     → Scheduler Listener: on_starting → background thread,
                    reconciliation loop (DagBag → DB → consumers)
  views.py        → Flask-AppBuilder view: /rmq-watcher/subscriptions (UI)
  plugin.py       → AirflowPlugin: регистрирует listener + view
  __init__.py     → реэкспорт публичного API
```

---

## Technical Details

### Схема таблицы `rmq_watcher_subscriptions`

```sql
-- Иллюстративный DDL (реальная схема через SQLAlchemy create_all)
CREATE TABLE rmq_watcher_subscriptions (
    id              INTEGER PRIMARY KEY,
    dag_id          VARCHAR(250) NOT NULL,
    queue_name      VARCHAR(250) NOT NULL,
    conn_id         VARCHAR(250) NOT NULL DEFAULT 'rmq_default',
    filter_data     JSON,
    source          VARCHAR(10)  NOT NULL,   -- 'dag_file' | 'ui'
    enabled         BOOLEAN      NOT NULL DEFAULT TRUE,
    consumer_status VARCHAR(20)  NOT NULL DEFAULT 'connecting',
                                             -- 'listening' | 'connecting' | 'error' | 'disabled'
    last_error      TEXT,
    -- Расширяемость для будущих режимов триггера (AND, Debounce):
    trigger_mode    VARCHAR(20)  NOT NULL DEFAULT 'any',
                                             -- 'any' (OR, текущий) | 'all' | 'debounce' (future)
    group_key       VARCHAR(250),            -- NULL = standalone; одинаковый ключ связывает
                                             -- подписки одной группы (для режимов 'all'/'debounce')
    cooldown        INTEGER,                 -- секунды ожидания для режима 'debounce' (future)
    created_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dag_id, queue_name, conn_id)     -- conn_id включён: один DAG может слушать
                                             -- одну очередь через разные RMQ-кластеры
);

-- Статус соединений по conn_id (один ряд на conn_id)
CREATE TABLE rmq_watcher_conn_status (
    conn_id        VARCHAR(250) PRIMARY KEY,
    status         VARCHAR(20)  NOT NULL DEFAULT 'disconnected',
                                          -- 'connected' | 'connecting' | 'disconnected' | 'error'
    consumer_count INTEGER      NOT NULL DEFAULT 0,
    last_error     TEXT,
    updated_at     DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```
`updated_at` требует `onupdate=func.now()` в SQLAlchemy-моделях (не только DB default).

### Формат `_rmq_subscriptions` на DAG-объекте

```python
dag._rmq_subscriptions = [
    {
        "queue_name": "orders",
        "conn_id": "rmq_default",
        "filter_data": {"filter_headers": {"type": "new_order"}},
    }
]
```

### conf, передаваемый в DAG при запуске

```python
conf = {
    "body": "<decoded UTF-8 string>",
    "headers": {"type": "new_order", ...},
    "routing_key": "orders.created",
    "queue": "orders",
    "subscription_id": 42,
}
```

### run_id для идемпотентности

```
rmq__{queue_name}__{datetime_utc_iso}
```
Детерминированный run_id на основе delivery_tag сложен (не уникален across connections),
поэтому используем timestamp. Для настоящей идемпотентности в HA — future work.

### Consumer Manager — connection pooling и жизненный цикл

**Одно соединение на `conn_id`**, не одно на подписку:
```
_connections: dict[str, aio_pika.RobustConnection]  # conn_id → connection
_tasks: dict[int, asyncio.Task]                      # sub.id → consumer task
```

Каждый `_consume_subscription` получает channel из уже открытого соединения для своего
`conn_id` — если соединение ещё не создано, создаёт. `connect_robust` автоматически
переподключается при обрывах, все каналы на нём получают reconnect бесплатно.

```
RMQConsumerManager.reconcile(subscriptions)
  ├── сгруппировать подписки по conn_id
  ├── закрыть соединения для conn_id которые больше не используются
  ├── остановить консьюмеры (task) для удалённых/изменённых подписок
  └── запустить новые asyncio.Task для добавленных подписок

каждый _consume_subscription(sub):
  loop:
    try:
      connection = _get_or_create_connection(sub["conn_id"])
      channel = await connection.channel()
      queue = await channel.declare_queue(sub["queue_name"], passive=True)
      async with queue.iterator() as q_iter:
        async for message in q_iter:
          _handle_message(message, filter) → ack + trigger_dag | nack+requeue
    except asyncio.CancelledError:
      break  # чистый выход при reconcile/stop
    except Exception:
      log + await asyncio.sleep(reconnect_delay) + retry
```

### Resilience — поведение при ошибках соединения

| Сценарий | Тип ошибки | Поведение |
|----------|-----------|-----------|
| RMQ недоступен при старте Airflow | `AMQPConnectionError` | `connect_robust` ретраится с exp backoff — ждёт пока сервер не поднимется; consumer task висит в retry loop, не падает |
| RMQ упал пока слушаем (кратковременно) | connection drop | `connect_robust` автоматически переподключается, заново открывает каналы — прозрачно для consumer loop |
| RMQ упал надолго | — | Consumer task остаётся в retry loop; reconciliation продолжает работать (читает DB, синхронизирует подписки); когда RMQ поднимается — consumer подключается |
| Очередь удалена пока слушаем | `ChannelClosed` (channel-level) | Поймать как recoverable: sleep(backoff) + retry с новым channel; если очередь так и не появилась — после N попыток записать `last_error`, ждать reconciliation |
| Очередь не существует при объявлении | `ChannelNotFoundEntity` (403/404) | Fatal на этот цикл: записать в `last_error`, логировать ERROR, не retry до следующего reconciliation |
| Reconciliation видит `last_error` | — | **Попробовать перезапустить** consumer task (очередь могла быть пересоздана); сбросить `last_error` если успешно |

**Ключевое:** consumer tasks никогда не завершаются сами по себе из-за ошибки —
они либо ждут (retry loop), либо ждут reconciliation (fatal). Только `stop()` и отмена
через reconciliation прекращают их.

`connect_robust` принимает `reconnect_interval` — задать разумный дефолт (5s) и
`reconnect_interval_multiplier` для backoff.

**Что видно в UI при проблемах:**
- Блок "Connections" вверху страницы — статус по каждому `conn_id` + кол-во активных консьюмеров
- `consumer_status` + `last_error` в строке каждой подписки — цветной бейдж
- Enabled/disabled позволяет временно отключить проблемную подписку

**Переходы consumer_status:**
```
(создана) → connecting → listening
                      ↘ error (fatal: queue not found)
                      ↗ (reconciliation перезапускает)
disabled ←→ (toggle в UI или enabled=False в DB)
```

**Consumer manager пишет статусы при переходе** через `WatcherSession` + **in-memory guard**
(запись только при реальном изменении значения, не при каждом retry-цикле):
- При старте consumer task → `connecting`
- После успешного `declare_queue` → `listening`, `last_error=None`
- `ChannelNotFoundEntity` → `error`, `last_error=<msg>`
- `enabled=False` → `disabled`
- `conn_id` connection: `connected`/`disconnected`/`error` + `consumer_count`

### Listener — компонентный check

```python
def on_starting(self, component):
    name = type(component).__name__
    if "Scheduler" in name:
        self._start_background_thread()
```

---

## What Goes Where

- **Implementation Steps** — изменения в коде и тестах в этом репозитории
- **Post-Completion** — ручная проверка с реальным RabbitMQ, документация changelog

---

## Implementation Steps

### Task 0: Рефакторинг — извлечь `utils/amqp.py`

**Files:**
- Create: `airflow_provider_rmq/utils/amqp.py`
- Modify: `airflow_provider_rmq/triggers/rmq.py`
- Create: `tests/test_amqp_utils.py`

Предварительное условие для Watcher: `consumer.py` и `triggers/rmq.py` должны использовать
одну функцию для построения aio_pika URL, а не дублировать 19 строк.

- [x] Создать `airflow_provider_rmq/utils/amqp.py` с константами и функцией:
  ```python
  AMQP_PORT = 5672
  AMQPS_PORT = 5671
  ```
- [x] В `hooks/rmq.py` заменить магические числа 5671/5672 на импорт констант:
  `from airflow_provider_rmq.utils.amqp import AMQP_PORT, AMQPS_PORT`
- [x] Добавить в `utils/amqp.py` функцию:
  ```python
  def build_amqp_connection(
      conn_info,
      vhost_override: str | None = None,
  ) -> tuple[str, ssl.SSLContext | None]:
      """Build aio_pika-compatible AMQP URL and SSL context from Airflow connection.

      Returns (url, ssl_context). Pass ssl_context to connect_robust() if not None.
      """
      extras = conn_info.extra_dejson
      ssl_context = build_ssl_context(extras)
      vhost = vhost_override or conn_info.schema or "/"
      port = conn_info.port if conn_info.port else (5671 if ssl_context else 5672)
      url = (
          f"{'amqps' if ssl_context else 'amqp'}://"
          f"{quote(conn_info.login or 'guest', safe='')}:{quote(conn_info.password or 'guest', safe='')}"
          f"@{conn_info.host or 'localhost'}:{port}/{quote(vhost, safe='')}"
      )
      return url, ssl_context
  ```
- [x] В `utils/amqp.py` добавить приватный `_PropsShim` (перенести из `triggers/rmq.py`)
  и функцию:
  ```python
  async def match_and_ack(message, msg_filter: MessageFilter) -> bool:
      """ACK matching messages, NACK+requeue non-matching. Returns True on match.

      After NACK sleeps 0.1s to prevent hot redelivery loop on classic queues.
      On quorum queues (RabbitMQ 4.x) broker enforces delivery-limit=20 — see docs.
      """
      body_str = message.body.decode("utf-8")
      props = _PropsShim(dict(message.headers or {}))
      if not msg_filter.has_filters or msg_filter.matches(props, body_str):
          await message.ack()
          return True
      await message.nack(requeue=True)
      await asyncio.sleep(0.1)
      return False
  ```
- [x] Рефакторить `triggers/rmq.py`:
  - Заменить inline-блок построения URL (строки ~144–168) вызовом `build_amqp_connection(conn_info)`
  - Удалить `_PropsShim` (переехал в `utils/amqp.py`)
  - `_handle_message` делегирует в `match_and_ack`; payload формирует сам
  - Поведение не меняется, существующие тесты остаются зелёными
- [x] Написать тесты в `tests/test_amqp_utils.py`:
  - `test_plain_url` — стандартное подключение без SSL
  - `test_ssl_url_uses_amqps_scheme` — с SSL → схема `amqps`, порт 5671
  - `test_custom_port` — явно заданный порт из conn_info
  - `test_login_url_encoding` — `user@domain` → `user%40domain` в URL
  - `test_vhost_url_encoding` — vhost `/app/v2` корректно кодируется
  - `test_vhost_override` — `vhost_override` имеет приоритет над `conn_info.schema`
  - `test_default_vhost_when_schema_empty` — пустой schema → vhost `"/"`
  - `test_returns_ssl_context_when_ssl_configured` — возвращает не-None ssl_context при SSL
  - `test_returns_none_ssl_context_without_ssl` — без SSL → ssl_context is None
  - `test_match_and_ack_matching_message` — сообщение прошло фильтр → ack вызван, returns True
  - `test_match_and_ack_non_matching_message` — не прошло → nack(requeue=True) вызван, returns False
  - `test_match_and_ack_no_filter_always_matches` — без фильтра → ack, True
  - `test_match_and_ack_nack_includes_sleep` — после nack вызван `asyncio.sleep(0.1)`
- [x] Запустить `pytest tests/ -v` — весь suite проходит (включая существующие trigger-тесты)

---

### Task 1: Модель данных и таблица DB

**Files:**
- Create: `airflow_provider_rmq/watcher/__init__.py`
- Create: `airflow_provider_rmq/watcher/models.py`
- Create: `tests/watcher/__init__.py`
- Create: `tests/watcher/test_models.py`

- [x] Создать пакет `airflow_provider_rmq/watcher/__init__.py` (пустой)
- [x] В `models.py` объявить `WatcherBase = declarative_base()` — **отдельный** Base,
  НЕ `airflow.models.base.Base`; это изолирует наши таблицы от Airflow's metadata
- [x] Определить модель `RMQSubscription(WatcherBase)`:
  - поля: `id`, `dag_id`, `queue_name`, `conn_id` (default `"rmq_default"`),
    `filter_data` (JSON), `source` (`"dag_file"/"ui"`), `enabled` (bool, default True),
    `consumer_status` (default `"connecting"`), `last_error` (Text, nullable),
    `trigger_mode` (String, default `"any"`), `group_key` (String, nullable),
    `cooldown` (Integer, nullable), `created_at`, `updated_at` (с `onupdate=func.now()`)
  - unique constraint на `(dag_id, queue_name, conn_id)` — три поля, чтобы один DAG мог
    слушать одну очередь через разные RMQ-кластеры; текущая реализация читает только `'any'`
- [x] Определить модель `RMQConnStatus(WatcherBase)`:
  - поля: `conn_id` (PK), `status` (default `"disconnected"`), `consumer_count` (int),
    `last_error` (Text, nullable), `updated_at` (с `onupdate=func.now()`)
- [x] Определить `WatcherSession = sessionmaker(bind=engine)` где `engine` из `airflow.settings`;
  это наш единственный session factory — использовать везде вместо `create_session()`
- [x] Добавить функцию `ensure_table_exists()`:
  `WatcherBase.metadata.create_all(engine, checkfirst=True)` — только наши 2 таблицы,
  без касания Airflow's metadata; вызывается из `plugin.on_load()`
- [x] Добавить CRUD-хелперы:
  - `upsert_subscription(session, dag_id, queue_name, ...)` — upsert подписки
  - `delete_subscriptions_for_dag(session, dag_id)` — удаление при исчезновении DAG
  - `get_enabled_subscriptions(session) -> list`
  - `set_consumer_status(session, sub_id, status, last_error=None)` — обновить статус консьюмера
  - `upsert_conn_status(session, conn_id, status, consumer_count, last_error=None)` — статус соединения
  - `get_conn_statuses(session) -> list` — для UI
- [x] Создать `tests/watcher/__init__.py` (пустой)
- [x] В `tests/watcher/test_models.py` написать тесты с SQLite in-memory DB:
  - `test_create_subscription` — создание записи
  - `test_upsert_updates_existing` — повторный upsert обновляет поля
  - `test_unique_constraint_dag_queue_conn` — нельзя создать две подписки с одинаковыми dag_id+queue_name+conn_id
  - `test_same_queue_different_conn_id_allowed` — одна очередь с разными conn_id → две записи разрешены
  - `test_delete_subscriptions_for_dag` — удаляет все подписки DAG'а
  - `test_get_enabled_subscriptions_filters_disabled` — disabled не возвращаются
  - `test_set_consumer_status_updates_field` — статус и last_error обновляются
  - `test_upsert_conn_status_creates_and_updates` — upsert по conn_id
  - `test_get_conn_statuses_returns_all` — возвращает все записи
  - `test_watcher_base_isolated_from_airflow_base` — `WatcherBase.metadata.tables` содержит
    только `rmq_watcher_*` таблицы, не таблицы Airflow (`dag`, `dag_run` и т.д.)
- [x] Запустить `pytest tests/watcher/test_models.py -v` — все тесты проходят

---

### Task 2: Декоратор `@rmq_trigger`

**Files:**
- Create: `airflow_provider_rmq/watcher/decorators.py`
- Create: `tests/watcher/test_decorators.py`

- [x] В `decorators.py` реализовать `rmq_trigger(queue, conn_id="rmq_default", filter_data=None)`:
  - возвращает decorator, который принимает DAG-объект
  - добавляет/расширяет список `dag._rmq_subscriptions` (dict с `queue_name`, `conn_id`, `filter_data`)
  - `filter_data` принимается строго в формате `{"filter_headers": {...}}` —
    таком же, который возвращает `MessageFilter.serialize()`; нормализация плоских dict не делается
  - если `filter_data=None` — сохраняется как `{}` (без фильтра, любое сообщение запускает DAG)
  - возвращает DAG без изменений
  - поддерживает стекирование (несколько `@rmq_trigger` на один DAG → несколько подписок)
- [x] Написать тесты в `test_decorators.py`:
  - `test_adds_rmq_subscriptions_attribute` — декоратор добавляет `_rmq_subscriptions`
  - `test_dag_returned_unchanged` — dag_id, schedule и прочие атрибуты не меняются
  - `test_stacking_multiple_queues` — два декоратора → два элемента в списке
  - `test_default_conn_id` — conn_id по умолчанию `"rmq_default"`
  - `test_filter_data_filter_headers_format` — `{"filter_headers": {"type": "x"}}` сохраняется как есть
  - `test_filter_data_none` — без фильтра `filter_data=None` → `{}`
- [x] Запустить `pytest tests/watcher/test_decorators.py -v` — все тесты проходят

---

### Task 3: Consumer Manager (asyncio RMQ)

**Files:**
- Create: `airflow_provider_rmq/watcher/consumer.py`
- Create: `tests/watcher/test_consumer.py`

- [x] В `consumer.py` реализовать `RMQConsumerManager`:
  - `__init__`: хранит `_tasks: dict[int, asyncio.Task]` (sub.id → task),
    `_connections: dict[str, aio_pika.RobustConnection]` (conn_id → connection),
    `_stop_event: asyncio.Event`
  - `async start()`: инициализация (создать event)
  - `async stop()`: отменяет все tasks, закрывает все connections
  - `async reconcile(subscriptions: list[SubscriptionDict])`: сравнивает текущие tasks
    с новым списком; отменяет задачи для удалённых/изменённых подписок; запускает новые
    `asyncio.Task` для добавленных; закрывает соединения для conn_id которые больше не используются
  - `async _get_or_create_connection(conn_id) -> RobustConnection`: возвращает существующее
    или создаёт новое соединение через `aio_pika.connect_robust(url, ssl_context=...)`;
    URL и ssl_context строятся через `build_amqp_connection(conn_info)` из `utils/amqp.py`
    (не дублировать логику из trigger — Task 0 создаёт эту функцию);
    при успехе → `upsert_conn_status(conn_id, "connected", consumer_count=...)`
    при ошибке → `upsert_conn_status(conn_id, "error", last_error=...)`
  - `_update_all_conn_counts()`: после каждого reconcile пересчитывает `consumer_count`
    по активным tasks для каждого conn_id и пишет в DB
  - `async _consume_subscription(sub: dict)`: основной loop:
    - записать `consumer_status="connecting"` в DB при старте
    - `connection = await _get_or_create_connection(sub["conn_id"])`
    - `channel = await connection.channel()` → `declare_queue(passive=True)`
    - при успехе → `consumer_status="listening"`, `last_error=None`; обновить `conn_status` для conn_id
    - `async with queue.iterator() as q_iter: async for message in q_iter:`
    - использовать `MessageFilter.deserialize(sub["filter_data"])` для фильтрации
    - `matched = await match_and_ack(message, msg_filter)` из `utils/amqp.py`
      (ack/nack + sleep(0.1) инкапсулированы там — не дублировать логику)
    - если `matched` → `_trigger_dag(sub["dag_id"], message)`
    - при успешном `declare_queue` — сбросить `last_error = None` в DB (восстановились после ошибки)
    - `except asyncio.CancelledError`: выход без retry
    - `except ChannelNotFoundEntity` (404, очередь не существует): fatal на этот цикл —
      записать `consumer_status="error"`, `last_error=<msg>` в DB, логировать ERROR;
      **не** retry; выход из loop; ждать следующего reconciliation (перезапустит task)
    - `except ChannelClosed` (канал закрыт брокером, например очередь удалили в рантайме):
      log WARNING + `await asyncio.sleep(reconnect_delay)` + retry с новым channel (recoverable)
    - `except Exception` (transient — обрыв сети, timeout): log WARNING + `await asyncio.sleep(reconnect_delay)` + retry
  - `async _trigger_dag(dag_id, message)`: вызывает `_sync_trigger` через
    `loop.run_in_executor(None, _sync_trigger, dag_id, conf, run_id)`;
    `_sync_trigger(dag_id, conf, run_id)`:
    ```python
    with WatcherSession() as s:          # наша сессия, не Airflow's
        dag = s.query(DagModel).filter_by(dag_id=dag_id, is_active=True).first()
        if not dag:
            log.warning("DAG %s not found/inactive, skip", dag_id); return
    try:
        trigger_dag(dag_id=dag_id, run_id=run_id, conf=conf)
    except IntegrityError:
        log.warning("run_id %s already exists, skip", run_id)  # dup защита
    ```
  - каждый `_ConsumerTask` хранит `_last_status: str | None = None` — **in-memory state guard**:
    `_write_status(new_status)` пишет в DB только если `new_status != _last_status`
  - `_build_run_id(queue_name)` → `f"rmq__{queue_name}__{datetime.utcnow().strftime('%Y%m%dT%H%M%S%f')}"`
- [x] Написать тесты в `test_consumer.py` (переиспользовать паттерн mock из `tests/test_trigger.py`):
  - хелперы `_make_push_queue(messages)` и `_make_blocking_queue()`
  - `test_matching_message_triggers_dag` — сообщение без фильтра → `trigger_dag` вызван
  - `test_non_matching_message_nacked` — не совпало по фильтру → nack, trigger_dag НЕ вызван
  - `test_multiple_messages_only_matching_triggers` — 3 сообщения, 1 совпало → 1 вызов
  - `test_reconcile_starts_new_consumer` — reconcile с новой подпиской → task создан
  - `test_reconcile_cancels_removed_consumer` — reconcile без старой подписки → task отменён
  - `test_stop_cancels_all_tasks` — stop() отменяет все running tasks
  - `test_connection_error_retries` — transient ошибка подключения → retry через backoff
  - `test_missing_queue_fatal_no_retry` — `ChannelNotFoundEntity` (404) → `last_error` записан, retry НЕ происходит
  - `test_non_matching_nack_has_sleep` — после nack вызывается `asyncio.sleep(0.1)` (защита от hot loop)
  - `test_two_subs_same_conn_id_share_one_connection` — два консьюмера с одним conn_id → `connect_robust` вызван один раз
  - `test_trigger_dag_uses_watcher_session` — `_sync_trigger` использует `WatcherSession`, не `create_session()`
  - `test_trigger_dag_skips_inactive_dag` — DAG с `is_active=False` → trigger НЕ вызван
  - `test_trigger_dag_skips_paused_dag` — DAG с `is_paused=True` → trigger НЕ вызван, сообщение ackнуто
  - `test_trigger_dag_handles_integrity_error` — `IntegrityError` при дубле run_id → логируется, не падает
  - `test_state_guard_skips_duplicate_status_write` — повторный вызов `_write_status("listening")` → DB write вызван 1 раз, не 2
  - `test_state_guard_writes_on_status_change` — `connecting` → `listening` → `error` → 3 writes в DB
  - `test_channel_closed_recovers_with_retry` — `ChannelClosed` → sleep + retry (не fatal, не выход)
  - `test_last_error_cleared_on_successful_connect` — после успешного reconnect `last_error` сбрасывается в DB
  - `test_rmq_unavailable_at_start_retries` — `connect_robust` ретраится при `AMQPConnectionError` (не падает)
- [x] Запустить `pytest tests/watcher/test_consumer.py -v` — все тесты проходят

---

### Task 4: Scheduler Listener + Reconciliation Loop

**Files:**
- Create: `airflow_provider_rmq/watcher/listener.py`
- Create: `tests/watcher/test_listener.py`

- [x] В `listener.py` реализовать `RMQWatcherListener`:
  - `__init__`: `_thread`, `_loop`, `_manager`, `_stop_event`,
    `_last_mtimes: dict[str, float]` (filepath → mtime),
    `_cached_subs: dict[str, list[dict]]` (filepath → подписки из этого файла)
  - `on_starting(component)`: проверить `"Scheduler" in type(component).__name__`
    (в Airflow 2.7+ это `SchedulerJobRunner`; логировать реальное имя класса при старте для диагностики);
    если да — запустить `_start()`
  - `before_stopping(component)`: если запущен — вызвать `_stop()`
  - `_start()`: создать `threading.Event`, запустить daemon thread → `_run_loop()`
    (таблицы уже созданы в `plugin.on_load()` — здесь вызывать `ensure_table_exists()` не нужно)
  - `_run_loop()`: создать новый `asyncio.new_event_loop()`, запустить `_main()`
  - `async _main()`: создать `RMQConsumerManager`, запустить reconciliation loop:
    ```
    while not stop_event.is_set():
        subs = _scan_subscriptions()   # инкрементально, mtime-based
        _sync_to_db(subs)
        await manager.reconcile(active_subs_from_db())
        await asyncio.sleep(reconcile_interval)  # default 60s
    await manager.stop()
    ```
  - `_scan_subscriptions()`: **инкрементальное mtime-based сканирование**:
    1. `current_files = set(glob.glob(DAGS_FOLDER/**/*.py, recursive=True))`
    2. Удалить из `_last_mtimes` и `_cached_subs` файлы, которых нет в `current_files`
    3. Для каждого файла в `current_files`: если `mtime != _last_mtimes.get(path)` →
       создать `DagBag(dag_folder=path)`, обновить `_cached_subs[path]`, обновить `_last_mtimes[path]`
    4. Вернуть все значения из `_cached_subs` (flat list)
    - При первом запуске `_last_mtimes` пустой → все файлы парсятся (полный скан при старте)
    - На стабильном деплое (ничего не менялось): только N mtime-syscalls (~миллисекунды)
    - Изменился 1 файл: парсится только он
    - Размер кэша ≤ количество текущих DAG-файлов на диске (самоочищается при удалении)
  - `_sync_to_db(scanned)`: `with WatcherSession() as s:` — upsert всех scanned
    (source='dag_file'); удалить из DB все `dag_file`-записи с `(dag_id, queue_name)`,
    которых **нет в текущем scanned** (удалённый DAG-файл ИЛИ убран `@rmq_trigger`);
    `ui`-подписки не трогать
  - `reconcile_interval`: читается из Airflow Variable `rmq_watcher_reconcile_interval`
    или дефолт 60
- [x] Написать тесты в `test_listener.py`:
  - `test_on_starting_with_scheduler_starts_thread` — component с "Scheduler" в имени → thread запускается
  - `test_on_starting_with_webserver_ignores` — webserver component → thread НЕ запускается
  - `test_before_stopping_sets_stop_event` — вызов before_stopping → stop event установлен
  - `test_scan_subscriptions_first_run_parses_all_files` — при пустом кэше парсятся все файлы
  - `test_scan_subscriptions_unchanged_files_not_reparsed` — файл не изменился (mtime тот же) → DagBag НЕ создаётся повторно
  - `test_scan_subscriptions_changed_file_reparsed` — mtime изменился → файл перепарсивается, кэш обновляется
  - `test_scan_subscriptions_deleted_file_removed_from_cache` — файл удалён → убирается из `_cached_subs` и `_last_mtimes`
  - `test_scan_subscriptions_finds_decorated_dags` — DAG с `_rmq_subscriptions` → возвращается в результате
  - `test_scan_subscriptions_ignores_dags_without_attribute` — DAG без атрибута → пропускается
  - `test_sync_to_db_upserts_dag_file_subscriptions` — mock session → upsert вызван
  - `test_sync_to_db_deletes_removed_dag_subscriptions` — DAG исчез → delete вызван
  - `test_sync_to_db_preserves_ui_subscriptions` — ui-подписки НЕ удаляются при reconciliation dag_file
  - `test_scheduler_component_name_matches` — проверить что `"Scheduler" in "SchedulerJobRunner"` == True (регрессия)
- [x] Запустить `pytest tests/watcher/test_listener.py -v` — все тесты проходят

---

### Task 5: AppBuilder View (UI)

**Files:**
- Create: `airflow_provider_rmq/watcher/views.py`
- Create: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`
- Create: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscription_form.html`
- Create: `tests/watcher/test_views.py`

- [x] В `views.py` реализовать `RMQWatcherView(BaseView)`:
  - `@expose("/subscriptions")` → `subscriptions()`: главная страница с двумя секциями:
    1. Блок **Connections** — таблица `rmq_watcher_conn_status`:
       `conn_id | status (●/✕/○ + цвет) | consumers | last_error`
    2. Таблица подписок — `dag_id | queue | conn_id | source | status (бейдж) | last_error | actions`
       Бейдж `consumer_status`: зелёный=listening, жёлтый=connecting, красный=error, серый=disabled
  - `@expose("/subscriptions/create", methods=["GET", "POST"])` → `create()`:
    форма создания (dag_id, queue_name, conn_id, filter_data JSON, enabled);
    POST → валидация → запись в DB с source='ui' → redirect на список
  - `@expose("/subscriptions/<int:sub_id>/edit", methods=["GET", "POST"])` → `edit()`:
    форма редактирования; dag_file-подписки не редактируются (только enabled toggle)
  - `@expose("/subscriptions/<int:sub_id>/delete", methods=["POST"])` → `delete()`:
    удаление; dag_file-подписки не удаляются через UI (показать error flash)
  - `@expose("/subscriptions/<int:sub_id>/toggle", methods=["POST"])` → `toggle()`:
    переключение enabled/disabled
  - все read/write операции через `WatcherSession()` — не через Airflow's request-scoped session
  - **RBAC:** определить `base_permissions = ["can_read", "can_edit", "can_create", "can_delete"]`
    на классе; каждый view-метод декорировать `@has_access`:
    - `can_read` → роль `User` и выше (тот же уровень что "вручную запустить DAG из UI")
    - `can_edit`, `can_create`, `can_delete` → роль `Op` и выше (тот же уровень что "pause/unpause DAG")
    POST-формы включают CSRF-токен (Flask-WTF) — использовать `airflow.www.utils.get_csrf_token()`
    или аналог в шаблоне
- [x] Создать минималистичные HTML-шаблоны (Jinja2, extends Airflow base template):
  - `subscriptions.html` — блок Connections (статус по conn_id) + таблица подписок
    с цветными бейджами `consumer_status`; кнопки Create/Edit/Delete/Toggle
  - `subscription_form.html` — форма с полями
- [x] Написать тесты в `test_views.py`:
  - `test_subscriptions_list_returns_200` — GET /subscriptions → 200
  - `test_subscriptions_list_shows_conn_status` — страница содержит данные из `rmq_watcher_conn_status`
  - `test_subscriptions_list_shows_consumer_status_badge` — `consumer_status` присутствует в HTML
  - `test_create_subscription_post_valid` — POST с валидными данными → подписка в DB
  - `test_create_subscription_post_invalid` — POST без dag_id → ошибка валидации
  - `test_delete_ui_subscription` — DELETE ui-подписки → удалена из DB
  - `test_delete_dag_file_subscription_rejected` — DELETE dag_file-подписки → ошибка
  - `test_toggle_subscription` — POST toggle → enabled меняется
- [x] Запустить `pytest tests/watcher/test_views.py -v` — все тесты проходят

---

### Task 6: AirflowPlugin registration

**Files:**
- Modify: `airflow_provider_rmq/watcher/__init__.py`
- Create: `airflow_provider_rmq/watcher/plugin.py`
- Modify: `airflow_provider_rmq/__init__.py`
- Create: `tests/watcher/test_plugin.py`

- [x] В `plugin.py` реализовать `RMQWatcherPlugin(AirflowPlugin)`:
  ```python
  class RMQWatcherPlugin(AirflowPlugin):
      name = "rmq_watcher"
      listeners = [RMQWatcherListener()]
      appbuilder_views = [{
          "name": "Subscriptions",
          "category": "RabbitMQ Watcher",
          "view": RMQWatcherView(),
      }]
  ```
- [x] В `on_load()` плагина вызвать `ensure_table_exists()` — создать таблицу если не существует
- [x] Обновить `airflow_provider_rmq/watcher/__init__.py` — реэкспортировать `rmq_trigger`
  из `decorators` как публичный API
- [x] Обновить `airflow_provider_rmq/__init__.py` → `get_provider_info()`:
  добавить секцию `"plugins"` (её сейчас нет) с записью о `RMQWatcherPlugin`;
  формат: `"plugins": [{"name": "rmq_watcher", "plugin-class": "airflow_provider_rmq.watcher.plugin.RMQWatcherPlugin"}]`
- [x] Написать тесты в `test_plugin.py`:
  - `test_plugin_name` — `plugin.name == "rmq_watcher"`
  - `test_plugin_has_listener` — `len(plugin.listeners) == 1`
  - `test_plugin_has_appbuilder_view` — appbuilder_views не пустой
  - `test_rmq_trigger_importable_from_watcher` — `from airflow_provider_rmq.watcher import rmq_trigger`
- [x] Запустить `pytest tests/watcher/ -v` — все тесты проходят

---

### Task 7: Пример DAG

**Files:**
- Create: `docs/example_dags/rmq_watcher_triggered_dag.py`

- [x] Создать DAG `rmq_watcher_demo`, демонстрирующий:
  1. `@rmq_trigger(queue="watcher-demo", filter_data={"event": "trigger"})` на DAG-функции
  2. `@task` — логирует `context["dag_run"].conf` (тело сообщения из RMQ)
  3. Комментарии: как работает механизм, что передаётся в conf
  4. Примечание: DAG запускается и по расписанию (`schedule="@daily"`), и по RMQ-событию

---

### Task 8: Проверка критериев приёмки

- [x] `pytest tests/ -v` — весь существующий suite + новые тесты проходят
- [x] Проверить обратную совместимость: импорт `RMQTrigger`, `RMQSensor`, `RMQHook` без изменений
- [x] Проверить что DAG без `@rmq_trigger` работает как раньше
- [x] Проверить что `@rmq_trigger` на DAG с schedule — оба источника запуска работают независимо
- [x] Проверить что два `@rmq_trigger` на одном DAG создают две записи в DB

---

### Task 9: [Final] Документация

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `readme.md`
- Modify: `readme_ru.md`

- [ ] Обновить docstrings в `decorators.py`, `listener.py`, `consumer.py` — описать публичный API
- [ ] Обновить `CHANGELOG.md` — секция `2.0.0` с описанием RMQ Watcher Plugin; обоснование
  major bump: новая реактивная инфраструктура (Scheduler Listener, background thread, DB-таблицы,
  UI) — качественно иной уровень функционала, хотя и полностью обратно совместима с 1.x
- [ ] Обновить `readme.md` (EN) — добавить раздел "RMQ Watcher Plugin" с примером
- [ ] Обновить `readme_ru.md` (RU) — аналогично
- [ ] Переместить этот план в `docs/plans/completed/`

---

## Post-Completion

**Ручная проверка с реальным RabbitMQ:**
- Запустить локальный Airflow + RabbitMQ (docker-compose)
- Создать DAG с `@rmq_trigger`, убедиться что он появляется в UI
- Отправить сообщение в очередь → DAG должен запуститься
- Отправить сообщение, не проходящее фильтр → DAG НЕ запускается
- Удалить DAG-файл → подписка исчезает после следующего reconciliation (≤60с)
- Создать подписку через UI → DAG запускается
- Остановить RabbitMQ → в UI появляется `last_error` (или статус недоступен); Airflow не падает
- Поднять RabbitMQ обратно → consumer сам переподключается, DAG запускается по следующему сообщению
- Удалить очередь пока Airflow работает → `last_error` в UI; пересоздать очередь → после следующего reconciliation подписка восстанавливается
- Ввести неверное имя очереди → `last_error` в UI сразу после первого reconciliation

**SQLite (dev окружения):**
SQLite использует table-level lock. При нестабильном RMQ (частые reconnects) наш фоновый
поток может временно блокировать scheduler heartbeat. Для production (PostgreSQL/MySQL)
row-level locks — проблем нет. Задокументировать в README как known limitation.

**HA-ограничение (задокументировать, не реализовывать):**
- В multi-scheduler режиме возможны дублирующиеся запуски DAG
- Рекомендация для пользователей: использовать `max_active_runs=1` или уникальный `run_id`
- Future work: DB-блокировка для выбора активного watcher

**Совместимость Airflow:**
- Listener API (`on_starting`/`before_stopping`) доступен с Airflow 2.5+
  (пакет требует ≥2.7.0 — ок)
- `trigger_dag()` из `airflow.api.common.trigger_dag` стабилен с Airflow 2.x
- Класс scheduler-компонента: проверяем по имени класса (не по импорту) — меньше зависимость от версии
