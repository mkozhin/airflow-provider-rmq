# Push Mode для RMQTrigger и RMQSensor

## Overview

Добавить в `RMQTrigger` и `RMQSensor` поддержку **push-режима** (AMQP `basic_consume`), в
котором брокер сам доставляет сообщения через `queue.iterator()`, вместо
периодического опроса (`queue.get()` + sleep).

**Проблема, которую решает:**
- В pull-режиме сообщение обрабатывается с задержкой до `poll_interval` секунд после появления.
- Push-режим реагирует мгновенно (брокер пушит сразу).
- При тихой очереди pull генерирует лишние запросы к брокеру.

**Ключевые решения:**
- `mode: Literal["pull", "push"] = "pull"` — явный параметр, pull остаётся дефолтом
  (обратная совместимость).
- `message_wait_timeout: float | None = None` — необязательный клиентский таймаут для
  push-режима (сколько ждать сообщения); по умолчанию нет ограничения.
  **Намеренно НЕ называть `consumer_timeout`** — это имя занято серверным таймаутом
  RabbitMQ (ACK-timeout, по умолчанию 30 мин), который совершенно другое понятие.
- При истечении `message_wait_timeout` триггер возвращает `{"status": "timeout"}`,
  сенсор поднимает `RuntimeError`.

**Совместимость с RabbitMQ 4.x:**
- **Quorum queues + NACK requeue (RabbitMQ 4.0+):** дефолтный redelivery limit = 20.
  При фильтрации non-matching сообщения NACKаются с `requeue=True`. На quorum-очереди
  после 20 доставок сообщение dead-letter'ится. Это затрагивает оба режима (pull и push).
  Необходимо задокументировать в docstring и добавить примечание в example DAG.
- **frame_max (RabbitMQ 4.1+):** aio_pika использует дефолтный frame_max — без изменений.
- **Khepri (RabbitMQ 4.2+):** passive queue declare может упасть при partial cluster
  outage — уже текущее поведение, `connect_robust` переподключится.
- **Server-side consumer_timeout (30 мин default):** если сообщение получено но не ack/nack
  за 30 мин — брокер закроет канал. В нашем триггере ACK происходит сразу при получении
  совпадающего сообщения — проблем нет.

**Совместимость с Airflow 2.9.1 + Python 3.10:**
- **BaseTrigger API:** не менялся с 2.7.x — `run()`, `serialize()`, `TriggerEvent` полностью
  совместимы.
- **Kwargs encryption (Airflow 2.9.0+):** kwargs триггера шифруются в БД. Все наши параметры
  (`mode: str`, `message_wait_timeout: float|None`, `filter_data: dict`) JSON-сериализуемы — ок.
- **`Literal["pull", "push"]`:** доступен в `typing` с Python 3.8 — ок.
- **`asyncio.TimeoutError` (Python 3.10):** в 3.10 это НЕ встроенный `TimeoutError` —
  использовать только `except asyncio.TimeoutError`, не `except TimeoutError`. В плане так и есть.
- **`asyncio.wait_for` + `queue.iterator()` cleanup:** при срабатывании таймаута `wait_for`
  отправляет `CancelledError` внутрь `async with queue.iterator()`. aio_pika вызывает
  `basic_cancel` на брокере (сетевой вызов). Фактическое время ожидания может **превысить**
  `message_wait_timeout` на время cleanup'а. Задокументировать в docstring.

## Context (from discovery)

- **Триггер:** `airflow_provider_rmq/triggers/rmq.py` — async, aio_pika,
  pull-loop через `queue.get(fail=False)` + `asyncio.sleep(poll_interval)`
- **Сенсор:** `airflow_provider_rmq/sensors/rmq.py` — передаёт `filter_data`
  в триггер через `_defer()`
- **Тесты триггера:** `tests/test_trigger.py` — mock через `fake_queue.get = AsyncMock()`
- **Тесты сенсора:** `tests/test_sensor.py`
- **Пример DAG:** `airflow_provider_rmq/example_dags/rmq_sensor_deferrable.py`
- **aio_pika API:** `queue.iterator()` возвращает async context manager → async iterator
  (брокер пушит сообщения сам); таймаут через `asyncio.wait_for()` (Python ≥3.10)

## Development Approach

- **Testing approach:** Regular (сначала код, потом тесты)
- Каждая задача полностью завершается до перехода к следующей
- Все тесты проходят до старта следующей задачи

## Testing Strategy

- **Unit tests:** pytest + pytest-asyncio; мокируем `aio_pika` через `unittest.mock`
- Для push-режима mock `queue.iterator()` — async context manager, возвращающий
  async iterator из списка сообщений
- Покрыть: успех (matching), пропуск (non-matching → nack), таймаут, ошибка соединения

## Progress Tracking

- `[x]` — задача выполнена
- `➕` — добавлено в процессе
- `⚠️` — блокер

## What Goes Where

- **Implementation Steps** — изменения в коде и тестах в этом репозитории
- **Post-Completion** — ручная проверка, документация changelog

---

## Implementation Steps

### Task 1: Добавить push-режим в RMQTrigger

**Files:**
- Modify: `airflow_provider_rmq/triggers/rmq.py`

**Что изменить:**

- [x] Добавить `mode: Literal["pull", "push"] = "pull"` и
  `message_wait_timeout: float | None = None` в `__init__` и docstring
- [x] Обновить `serialize()` — добавить `"mode"` и `"message_wait_timeout"` в kwargs
- [x] В `run()` после объявления очереди разветвить логику:
  - pull — текущий цикл `queue.get()` + sleep (без изменений)
  - push — `_run_push(queue, msg_filter)` (новый приватный метод)
- [x] Реализовать `async def _run_push(queue, msg_filter)` (используем `asyncio.wait_for` —
  работает на Python ≥3.10; `asyncio.timeout()` только с 3.11):
  ```python
  async def _run_push(self, queue, msg_filter) -> dict | None:
      async def _consume():
          async with queue.iterator() as q_iter:
              async for message in q_iter:
                  result = await self._handle_message(message, msg_filter)
                  if result is not None:
                      return result
          return None

      try:
          if self.message_wait_timeout is not None:
              return await asyncio.wait_for(_consume(), timeout=self.message_wait_timeout)
          else:
              return await _consume()
      except asyncio.TimeoutError:
          return {"status": "timeout"}
  ```
  Вынести общую логику обработки сообщения в `_handle_message(message, msg_filter) -> dict | None`
  (используется и в pull, и в push для DRY).
- [x] `run()` вызывает `_run_push`, ловит результат:
  - `result["status"] == "success"` или `"timeout"` → `yield TriggerEvent(result)`
  - `result is None` (итератор завершился без совпадения) → yield error event:
    `TriggerEvent({"status": "error", "error": "Push consumer ended without matching message"})`
- [x] Вынести общую логику ACK/NACK в `_handle_message(message, msg_filter) -> dict | None`
  и использовать её в pull-режиме тоже (DRY-рефакторинг); сохранить `asyncio.sleep(0.1)`
  после nack в pull-цикле — он там нужен против tight loop
- [x] Запустить `pytest tests/test_trigger.py -v` — 13/13 passed (обновлён test_serialize_full)

---

### Task 2: Обновить RMQSensor

**Files:**
- Modify: `airflow_provider_rmq/sensors/rmq.py`

- [x] Добавить `mode: Literal["pull", "push"] = "pull"` в `__init__` и docstring
  (не добавлять в `template_fields` — это конфигурационный параметр, Jinja не нужен)
- [x] Добавить `message_wait_timeout: float | None = None` в `__init__`
- [x] В `_defer()` передать `mode` и `message_wait_timeout` в `RMQTrigger`
- [x] В `execute_complete()` добавить обработку `status == "timeout"`:
  ```python
  if event.get("status") == "timeout":
      raise RuntimeError(
          f"RMQSensor timed out waiting for message in queue '{self.queue_name}'"
      )
  ```
- [x] Добавить валидацию: `message_wait_timeout` применим только в push-режиме;
  если задан при `mode="pull"` — бросить `ValueError` с понятным сообщением
- [x] Запустить `pytest tests/test_sensor.py -v` — 20/20 passed

---

### Task 3: Тесты для push-режима RMQTrigger

**Files:**
- Modify: `tests/test_trigger.py`

**Вспомогательные моки для `queue.iterator()`:**

В aio_pika `queue.iterator()` → async context manager → `__aenter__` возвращает async iterator.
Нужен helper:
```python
def _make_push_queue(messages: list):
    """Fake queue с iterator(), выдающим messages."""
    class _AsyncIter:
        def __init__(self, items): self._items = iter(items)
        def __aiter__(self): return self
        async def __anext__(self):
            try: return next(self._items)
            except StopIteration: raise StopAsyncIteration

    class _IterCM:
        async def __aenter__(self): return _AsyncIter(messages)
        async def __aexit__(self, *a): return False

    q = MagicMock()
    q.get = AsyncMock(return_value=None)  # pull path не используется
    # side_effect=lambda: _IterCM() — каждый вызов iterator() даёт свежий CM
    q.iterator = MagicMock(side_effect=lambda: _IterCM())
    return q


def _make_blocking_push_queue():
    """Queue с iterator(), который блокируется вечно — для теста таймаута."""
    class _BlockingIter:
        def __aiter__(self): return self
        async def __anext__(self):
            await asyncio.sleep(999)  # будет прерван asyncio.wait_for

    class _IterCM:
        async def __aenter__(self): return _BlockingIter()
        async def __aexit__(self, *a): return False

    q = MagicMock()
    q.iterator = MagicMock(side_effect=lambda: _IterCM())
    return q
```

**Тесты:**

- [x] `TestPushModeNoFilter::test_first_message_taken` — push, нет фильтра, первое сообщение ACK
- [x] `TestPushModeWithFilter::test_non_matching_nacked_matching_acked` — non-match NACK,
  match ACK, один success event
- [x] `TestPushModeTimeout::test_message_wait_timeout_yields_timeout_event` — использовать
  `_make_blocking_push_queue()` (итератор висит на `asyncio.sleep(999)`),
  `message_wait_timeout=0.05`, должен вернуть `{"status": "timeout"}`
- [x] `TestPushModeSerialize::test_serialize_with_mode_and_message_wait_timeout` — проверить что
  `mode` и `message_wait_timeout` сериализуются и восстанавливаются через roundtrip
- [x] `TestPushModeSerialize::test_serialize_defaults_mode_pull` — дефолт `mode="pull"`,
  `message_wait_timeout=None`
- [x] `TestPushModeError::test_connection_error_yields_error_event` — аналог pull-теста
  для push
- [x] Запустить все тесты: `pytest tests/test_trigger.py -v` — 20/20 passed

---

### Task 4: Тесты для RMQSensor (новые параметры)

**Files:**
- Modify: `tests/test_sensor.py`

- [x] `test_defer_passes_mode_push_to_trigger` — убедиться что `mode="push"` пробрасывается
  в `RMQTrigger`
- [x] `test_defer_passes_message_wait_timeout_to_trigger` — `message_wait_timeout=30.0` пробрасывается
- [x] `test_execute_complete_timeout_raises_runtime_error` — event `{"status": "timeout"}`
  → `RuntimeError`
- [x] `test_message_wait_timeout_with_pull_mode_raises_value_error` — `mode="pull"` +
  `message_wait_timeout=10` → `ValueError`
- [x] Запустить: `pytest tests/test_sensor.py -v` — 24/24 passed

---

### Task 5: Пример DAG для push-режима

**Files:**
- Create: `docs/example_dags/rmq_sensor_push.py`

- [x] Создать DAG `rmq_sensor_push`, демонстрирующий:
  1. `RMQQueueManagementOperator` — объявить очередь
  2. `RMQPublishOperator` — опубликовать "шум" и целевое сообщение
  3. `RMQSensor(deferrable=True, mode="push", message_wait_timeout=60)` — ждать с push
  4. `@task` — обработать результат
  5. `RMQQueueManagementOperator` — cleanup
- [x] Добавить комментарии в DAG: когда использовать push vs pull

---

### Task 6: Проверка критериев приёмки

- [x] `pytest tests/ -v` — 180/180 passed
- [x] Проверить обратную совместимость: существующий код без `mode=` работает как раньше
- [x] Убедиться что `message_wait_timeout=None` (дефолт) не устанавливает никаких ограничений
  в push-режиме
- [x] Проверить что `serialize()` / `deserialize()` правильно работают (roundtrip)

### Task 7: [Final] Документация

- [ ] Обновить docstring `RMQTrigger` и `RMQSensor` — описать `mode` и `message_wait_timeout`
- [ ] В docstring `message_wait_timeout` указать: фактическое время ожидания может превысить
  заданное значение на время graceful-cleanup потребителя (`basic_cancel` к брокеру)
- [ ] В docstring добавить **предупреждение о quorum queues** (RabbitMQ 4.0+):
  ```
  Warning:
      When using quorum queues (default in RabbitMQ 4.x clusters), non-matching
      messages are NACKed with requeue=True. RabbitMQ 4.0+ enforces a default
      redelivery limit of 20 for quorum queues — after 20 redeliveries the message
      is dead-lettered or dropped. If heavy filtering is expected, consider using
      a dedicated queue or increasing the delivery-limit policy.
  ```
- [ ] В `docs/example_dags/rmq_sensor_push.py` добавить комментарий про quorum queue redelivery limit
- [ ] Обновить `CHANGELOG.md`
- [ ] Обновить `readme.md` (EN) и `readme_ru.md` (RU) — оба файла, описать push-режим и новые параметры
- [ ] Переместить план в `docs/plans/completed/`

---

## Technical Details

### Push mode — структура кода в триггере

```
run()
├── [общая часть] — соединение, канал, объявление очереди (без изменений)
├── if mode == "pull"  → цикл queue.get() + sleep, использует _handle_message()
└── if mode == "push"  → _run_push(queue, msg_filter)
                          ├── _consume() — async with queue.iterator() as q_iter:
                          │                 async for message in q_iter:
                          │                     _handle_message() -> dict | None
                          ├── [если message_wait_timeout] asyncio.wait_for(_consume(), N)
                          │   except asyncio.TimeoutError → {"status": "timeout"}
                          └── result is None → {"status": "error", "error": "..."}
```

### Вспомогательный метод `_handle_message`

Выносит общую логику ACK/NACK:
```python
async def _handle_message(self, message, msg_filter) -> dict | None:
    body_str = message.body.decode("utf-8")
    props_shim = _PropsShim(dict(message.headers or {}))
    if not msg_filter.has_filters or msg_filter.matches(props_shim, body_str):
        await message.ack()
        return {
            "status": "success",
            "message": {
                "body": body_str,
                "headers": dict(message.headers or {}),
                "routing_key": message.routing_key or "",
                "exchange": message.exchange or "",
            },
        }
    else:
        await message.nack(requeue=True)
        return None
```

В pull-режиме тоже использовать `_handle_message` для DRY (рефакторинг),
но сохранить `await asyncio.sleep(0.1)` после nack в pull-режиме — он там нужен.

### Параметры

| Параметр | Тип | Дефолт | Описание |
|---|---|---|---|
| `mode` | `Literal["pull", "push"]` | `"pull"` | Режим получения сообщений |
| `message_wait_timeout` | `float \| None` | `None` | Таймаут ожидания в push-режиме (сек). `None` = без ограничений |
| `poll_interval` | `float` | `5.0` | Используется только в pull-режиме |

### Обработка статусов в `execute_complete`

| `event["status"]` | Действие сенсора |
|---|---|
| `"success"` | Вернуть `event["message"]` |
| `"timeout"` | Поднять `RuntimeError` |
| `"error"` | Поднять `RuntimeError` (без изменений) |

---

## Post-Completion

**Ручная проверка:**
- Запустить `rmq_sensor_push` DAG в локальной среде с реальным RabbitMQ
- Убедиться что сообщение приходит без задержки poll_interval
- Проверить поведение при `message_wait_timeout`: DAG должен завершиться с ошибкой через N секунд

**Зависимости:**
- Проект требует Python `>=3.10`. `asyncio.timeout()` появился только в 3.11 — **не использовать**.
  В плане принято решение: `asyncio.wait_for(_consume(), timeout=N)` — работает на Python ≥3.10.

**Совместимость с RabbitMQ 4.x (проверено):**

| Изменение | Версия RMQ | Влияние на наш код |
|---|---|---|
| Quorum queue redelivery limit = 20 | RMQ 4.0+ | ⚠️ NACK+requeue при фильтрации — задокументировать |
| Classic queue mirroring removed | RMQ 4.0 | Нет — topology change, не API |
| Server-side consumer_timeout = 30 мин | RMQ 3.8.15+ | Нет — ACK происходит немедленно |
| `message_wait_timeout` vs `consumer_timeout` | — | Намеренно выбрано другое имя |
| frame_max минимум 8192 | RMQ 4.1 | Нет — aio_pika использует дефолт сервера |
| Khepri как metadata store | RMQ 4.2 | Нет — passive declare уже так работает |
| Max message size 16 MiB | RMQ 4.0 | Нет — не затрагивает логику триггера |
| BaseTrigger API стабилен | Airflow 2.7–2.9 | Нет — API не менялся |
| Kwargs шифрование | Airflow 2.9.0+ | Нет — все параметры JSON-сериализуемы |
| `asyncio.TimeoutError` ≠ `TimeoutError` | Python 3.10 | ✅ используем `asyncio.TimeoutError` |
| `wait_for` + iterator cleanup overhead | Python 3.10+ | ⚠️ задокументировать в docstring |
