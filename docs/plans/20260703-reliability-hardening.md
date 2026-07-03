# Повышение надёжности провайдера (итоги архитектурного ревью)

## Overview

Пакет улучшений надёжности по итогам архитектурного ревью 2026-07-03. Закрывает
шесть классов проблем:

1. **HA**: защита от двойного запуска watcher'а при нескольких репликах шедулера —
   лидер-лок через exclusive-очередь RabbitMQ (ноль записей в БД).
2. **Потеря сообщений**: at-least-once вместо at-most-once в immediate-режиме
   watcher'а; фикс строгого UTF-8 decode, теряющего бинарные сообщения; publisher
   confirms в `RMQPublishOperator`.
3. **Тихие сбои**: валидация cooldown conn_id (сейчас publish в чужой vhost молча
   теряется), детерминированный выбор fire-брокера.
4. **Стабильность event loop**: вынос файлового I/O, AST-парсинга и записей в БД
   из корутин в executor.
5. **Жизненный цикл**: graceful stop watcher-петли (сейчас соединения почти никогда
   не закрываются штатно), диагностика нераспознанного компонента в `on_starting`.
6. **Гигиена интерфейсов**: защита от «угона» подписок между UI и dag_file,
   ленивый `WatcherSession`, согласованная форма `queue_info`, statsd-метрики.

Ограничение (осознанное решение владельца): **минимум записей в БД Airflow** —
лидер-лок и метрики реализуются без обращений к базе.

## Context (from discovery)

- Файлы: `airflow_provider_rmq/hooks/rmq.py`, `operators/rmq_publish.py`,
  `triggers/rmq.py`, `watcher/listener.py`, `watcher/consumer.py`,
  `watcher/models.py`, `watcher/views.py`, `watcher/plugin.py`.
- Паттерны: graceful degradation в reconcile (ошибки одной подписки не валят
  остальные), `run_in_executor` для синхронных вызовов (`_sync_trigger`),
  `OrphanTracker` как чистая алгебра множеств, единая валидация в
  `subscription_builder`.
- Тесты: pytest, `asyncio_mode=auto`, моки aio_pika (`tests/watcher/test_consumer.py`,
  `test_listener.py`), стиль — Regular (код, затем тесты в той же задаче).
- Airflow 3 вне скоупа (провайдер живёт на ветке 2.x, pin `<3.0.0`).

## Development Approach

- **testing approach**: Regular (код, затем тесты в рамках той же задачи)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - tests are not optional - they are a required part of the checklist
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change: `pytest`
- maintain backward compatibility (кроме зафиксированного поведенческого изменения
  `confirm=True` в `RMQPublishOperator` — отражается в CHANGELOG)
- **никаких новых регулярных записей в БД Airflow** — лидер-лок через RMQ,
  метрики через statsd

## Testing Strategy

- **unit tests**: обязательны в каждой задаче, в существующем стиле
  (`tests/test_hook.py`, `tests/watcher/test_consumer.py` и т.д.)
- e2e-тестов в проекте нет; регрессия DagBag для example DAGs уже существует
  (`tests/watcher/test_example_dags_dagbag.py`) — прогонять в составе полного сьюта

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

Ключевые дизайн-решения (провалидированы с владельцем в брейнсторме):

- **Лидер-лок**: exclusive-очередь `rmq_watcher.leader` на арбитражном брокере.
  Арбитр = `sorted(set(conn_id отсканированных и enabled-подписок))[0]`, fallback
  `rmq_default`. Лок держится на **выделенном НЕ-robust** соединении (robust
  молча передекларирует очередь при реконнекте и позволяет двум репликам
  незаметно отобрать лидерство друг у друга). Follower пропускает весь цикл,
  включая `_sync_to_db`. Потеря соединения = потеря лидерства = `manager.stop()`
  и перезахват на следующем цикле. Недоступность арбитра = watcher нигде не
  активен (согласованная деградация: консьюмить с него всё равно нельзя).
- **At-least-once**: в immediate-режиме порядок становится match → `trigger_dag`
  → ACK; ошибка триггера → NACK+requeue. Дедуп при редоставке: `run_id =
  rmq__{queue}__{message_id}` при наличии AMQP `message_id` (IntegrityError →
  INFO + ACK), иначе timestamp-fallback (дубль возможен только в узком окне
  краша — лучше дубль, чем потеря).
- **Confirms**: хук — opt-in (`publisher_confirms=False`), оператор — opt-out
  (`confirm=True` по умолчанию); `mandatory=False` везде отдельным opt-in
  (публикация в exchange без биндингов — легитимный паттерн).
- **UI vs dag_file**: код — источник истины. UI отказывает при коллизии с
  dag_file-строкой; скан при коллизии с ui-строкой забирает её себе с WARNING;
  `upsert_subscription` допускает смену `source` только ui→dag_file.

## Technical Details

### Лидер-лок (`watcher/leader.py`, новый модуль)

```python
class RMQLeaderLock:
    """Лидер-лок через exclusive-очередь rmq_watcher.leader.

    Владеет выделенным НЕ-robust aio_pika-соединением. Интерфейс:
    - async try_acquire(conn_id) -> bool  # False при RESOURCE_LOCKED (405)
    - is_held() -> bool                   # соединение живо => лидерство наше
    - conn_id -> str | None               # на каком арбитре держим лок
    - async release()                     # закрыть соединение (отдать лок)
    """
```

- `try_acquire`: `BaseHook.get_connection` через executor →
  `build_amqp_connection` → `aio_pika.connect(...)` (не robust) →
  `channel.declare_queue("rmq_watcher.leader", exclusive=True)`.
  RESOURCE_LOCKED (reply code 405; в aio_pika это подкласс `ChannelClosed` —
  точный класс уточнить по факту, ожидаемо `ChannelLockedResource` из aiormq) →
  закрыть соединение, вернуть `False`. Прочие ошибки (брокер недоступен) —
  пробросить: цикл залогирует и попробует снова.
- Интеграция в `listener._main` (каждый цикл):
  1. `_scan_subscriptions()` — локальная операция, выполняют все реплики
     (через executor, см. Task 9).
  2. Вычислить арбитра из union(conn_id скана, conn_id enabled-подписок в БД —
     read-only запрос), fallback `"rmq_default"`.
  3. Не лидер → `try_acquire(arbiter)`; `False` → follower: лог DEBUG, спать.
     Ошибка соединения → WARNING, спать.
  4. Лидер, но лок умер (`not is_held()`) или арбитр сменился →
     `await manager.stop()`, `await lock.release()`, перезахват в этом же цикле.
  5. Лидер с живым локом → `_sync_to_db` + `reconcile` как сейчас.
- Метрики: `Stats.incr("rmq_watcher.leader_acquired" / "leader_lost")`.

### At-least-once (`consumer.py`)

- `_sync_trigger` → возвращает `"triggered" | "skipped" | "duplicate"`,
  исключения пробрасывает, кроме двух, означающих «дубль»:
  - **`airflow.exceptions.DagRunAlreadyExists`** — основной случай: в Airflow
    2.9+ `trigger_dag` вызывает `DagRun.find_duplicate(...)` и бросает это
    исключение **до** INSERT (проверено по `airflow/api/common/trigger_dag.py`);
    это НЕ подкласс `IntegrityError` — текущий `except IntegrityError` реальную
    редоставку не ловит (та же скрытая ошибка есть в fire-консьюмере cooldown —
    исправляется этим же изменением, т.к. `_sync_trigger` общий);
  - `sqlalchemy.exc.IntegrityError` — страховка от гонки, когда два процесса
    прошли `find_duplicate` до того, как один из них вставил строку.
- `_build_run_id(queue_name, message_id: str | None)` — при `message_id`
  детерминированный `rmq__{queue}__{message_id}`, иначе текущий формат.
- Immediate-ветка `_consume_subscription`:
  ```
  if not _match(...): await _nack_and_sleep(...); continue
  try:    outcome = await self._trigger_dag(...)   # executor
  except Exception: log WARNING; await _nack_and_sleep(message); continue
  await message.ack()                              # любой исход triggered/skipped/duplicate
  ```
- Fire-queue-ветка (cooldown): логика не меняется, но получает фикс
  `DagRunAlreadyExists` бесплатно через общий `_sync_trigger`.
- Ограничение дизайна (в README): дедуп доверяет уникальности `message_id`
  на стороне продюсера — два разных сообщения с одинаковым `message_id`
  молча схлопнутся в один запуск.
- **Граница гарантии** (осознанное решение, зафиксировать в README): at-least-once
  действует только после успешной eligibility-проверки DAG. Исход `"skipped"`
  (DAG paused/inactive/отсутствует) — терминальный ACK, сообщение выбрасывается;
  NACK здесь превратил бы паузу DAG'а в накопитель редоставок и упёрся бы в
  delivery-limit quorum-очередей (20).

### Cooldown conn_id (`consumer.py::reconcile`)

- `fire_conn_id = sorted({sub["conn_id"] for sub in cooldown_subs})[0]` —
  вычисляется в **начале** `reconcile`, до формирования desired active set.
- Подписки с `cooldown > 0` и `conn_id != fire_conn_id` (invalid) исключаются
  из desired active set **до** `new_ids` — существующий cancel-блок снимает и
  их уже запущенные consumer-таски (reconcile отменяет таски только по
  отсутствующим sub_id, поэтому фильтра в цикле старта недостаточно).
- Invalid-подписки: `consumer_status='error'`, `last_error` = «cooldown
  subscriptions must share one conn_id; infrastructure is provisioned on
  '<fire_conn_id>'», ERROR-лог раз в цикл (через существующий дедуп
  `_ConsumerState`).

### Publisher confirms

- `RMQHook.__init__(..., publisher_confirms: bool = False)`; в `get_channel()`
  после открытия канала — `channel.confirm_delivery()` при включённом флаге.
- `RMQHook.basic_publish(..., mandatory: bool = False)` — прокинуть в pika,
  исключения (`UnroutableError`, `NackError`) не глотать.
- `RMQPublishOperator(..., confirm: bool = True, mandatory: bool = False)`;
  в `execute` — хук с `publisher_confirms=self.confirm`; при
  `NackError`/`UnroutableError` → `AirflowException` с индексом сообщения
  в батче («message 3 of 7 was nacked/unroutable»).

### UI vs dag_file

- `models.upsert_subscription`: если существующая строка имеет
  `source='dag_file'`, а вызов пришёл с `source='ui'` — `ValueError`
  (защита на уровне модели); направление ui→dag_file разрешено (takeover
  сканом, WARNING логируется вызывающей стороной).
- `views.create` / `edit` / `edit_group`: предварительная проверка коллизии
  ключа `(dag_id, queue_name, conn_id)` с dag_file-строкой → flash-ошибка
  «this subscription is managed by @rmq_trigger in the DAG file», без записи.
- `listener._sync_to_db`: при upsert поверх ui-строки — WARNING
  «taking over UI subscription <key>: @rmq_trigger in code is the source of truth».

### Прочее

- `WatcherSession`: модульная функция с ленивой инициализацией фабрики
  (`_session_factory` создаётся при первом вызове); колл-сайты
  `with WatcherSession() as session:` не меняются.
- Метрики (`airflow.stats.Stats.incr`, no-op без statsd):
  `rmq_watcher.dag_triggered`, `rmq_watcher.consumer_reconnect`,
  `rmq_watcher.orphan_detected`, `rmq_watcher.leader_acquired`,
  `rmq_watcher.leader_lost`.

## What Goes Where

- **Implementation Steps**: изменения кода, тестов и документации этого репозитория.
- **Post-Completion**: ручная проверка на живом стенде (двухрепличный шедулер,
  RabbitMQ 4.x quorum queues), релиз на PyPI.

## Implementation Steps

### Task 1: Decode-фиксы (errors="replace")

**Files:**
- Modify: `airflow_provider_rmq/hooks/rmq.py`
- Modify: `airflow_provider_rmq/triggers/rmq.py`
- Modify: `tests/test_hook.py`
- Modify: `tests/test_trigger.py`

- [ ] `hooks/rmq.py::consume_messages`: `body.decode("utf-8")` → `decode("utf-8", errors="replace")`, сохранив существующий guard `if isinstance(body, bytes)`
- [ ] `triggers/rmq.py::_handle_message`: `message.body.decode("utf-8")` → `decode("utf-8", errors="replace")`
- [ ] тест: `consume_messages` возвращает тело с replacement-символом для бинарного payload, не бросает
- [ ] тест: `_handle_message` для бинарного payload — ACK и success-событие, не исключение после ACK
- [ ] run tests - must pass before task 2

### Task 2: Мелочи хука и publish-оператора

**Files:**
- Modify: `airflow_provider_rmq/hooks/rmq.py`
- Modify: `airflow_provider_rmq/operators/rmq_publish.py`
- Modify: `tests/test_hook.py`
- Modify: `tests/test_publish_operator.py`

- [ ] `queue_info`: ветка «очередь не существует» дополнена `"consumer_count": 0`
- [ ] `__del__`: тело целиком в `try/except Exception: pass` (защита от interpreter shutdown)
- [ ] `RMQPublishOperator.execute` при пустом `_normalize_messages()` → `log.warning("No messages to publish...")`
- [ ] тест: форма ответа `queue_info` для несуществующей очереди содержит все четыре ключа
- [ ] тест: `message=None` → warning в логе, publish не вызывается
- [ ] тест: `__del__` при исключении из `_connection.is_open`/`close()` не бросает
- [ ] run tests - must pass before task 3

### Task 3: Publisher confirms в RMQHook

**Files:**
- Modify: `airflow_provider_rmq/hooks/rmq.py`
- Modify: `tests/test_hook.py`

- [ ] параметр `publisher_confirms: bool = False` в `__init__`
- [ ] `get_channel()`: при включённом флаге — `channel.confirm_delivery()` после открытия канала (и после reconnect)
- [ ] `basic_publish(..., mandatory: bool = False)` — прокинуть в `channel.basic_publish`, исключения pika наружу
- [ ] тест: `confirm_delivery` вызывается при `publisher_confirms=True` и не вызывается по умолчанию
- [ ] тест: после reconnect (пересоздание канала в `get_channel`) `confirm_delivery` применяется к новому каналу — вызов размещается внутри `get_channel`, это даёт реконнект бесплатно
- [ ] тест: `mandatory` прокидывается; `UnroutableError`/`NackError` пробрасываются
- [ ] run tests - must pass before task 4

### Task 4: Publisher confirms в RMQPublishOperator (дефолт ON)

**Files:**
- Modify: `airflow_provider_rmq/operators/rmq_publish.py`
- Modify: `tests/test_publish_operator.py`
- Modify: `CHANGELOG.md`

- [ ] параметры `confirm: bool = True`, `mandatory: bool = False`
- [ ] `execute`: хук создаётся с `publisher_confirms=self.confirm`; publish с `mandatory=self.mandatory`
- [ ] `NackError`/`UnroutableError` → `AirflowException` с индексом сообщения в батче
- [ ] тест: дефолтное поведение включает confirms; `confirm=False` отключает
- [ ] тест: nack на 3-м сообщении из 7 → AirflowException с «message 3 of 7»
- [ ] тест: `UnroutableError` при `mandatory=True` → AirflowException с индексом сообщения
- [ ] CHANGELOG: запись о поведенческом изменении (confirm=True по умолчанию)
- [ ] run tests - must pass before task 5

### Task 5: Ленивый WatcherSession

**Files:**
- Modify: `airflow_provider_rmq/watcher/models.py`
- Modify: `tests/watcher/test_models.py`

- [ ] заменить module-level `WatcherSession = _make_session_factory()` на функцию `WatcherSession()` с ленивой инициализацией кэшированной фабрики
- [ ] сигнатура вызова `with WatcherSession() as session:` сохраняется во всех колл-сайтах без изменений
- [ ] тест: импорт `models` не трогает `airflow.settings` (например, через мок/подмену)
- [ ] тест: повторные вызовы используют одну фабрику
- [ ] run tests - must pass before task 6

### Task 6: Защита source в upsert + отказ UI при коллизии с dag_file

**Files:**
- Modify: `airflow_provider_rmq/watcher/models.py`
- Modify: `airflow_provider_rmq/watcher/views.py`
- Modify: `tests/watcher/test_models.py`
- Modify: `tests/watcher/test_views.py`

- [ ] `upsert_subscription`: попытка ui-upsert поверх dag_file-строки → `ValueError`; направление ui→dag_file разрешено
- [ ] `views.create`: проверка коллизии каждого queue_name с dag_file-строкой → flash-ошибка, без записи
- [ ] `views.edit` / `edit_group`: проверка **нового** ключа `(dag_id, queue_name, conn_id)` после изменения формы — эти пути НЕ проходят через `upsert_subscription` (`edit` мутирует ORM-объект напрямую, `edit_group` делает delete+re-insert), поэтому view-проверка здесь несущая, а не дублирующая
- [ ] тест: upsert ui поверх dag_file → ValueError; dag_file поверх ui → source меняется
- [ ] тест: create через UI при занятом dag_file-ключе → flash error, строка не изменена
- [ ] тест: edit меняет ключ на существующую dag_file-строку → flash error, без записи (не 500)
- [ ] тест: edit_group меняет ключ на существующую dag_file-строку → flash error **до** delete/flush — группа не удалена, commit не вызван (самый рискованный путь: delete + flush + re-insert)
- [ ] run tests - must pass before task 7

### Task 7: Takeover ui-строк сканом (_sync_to_db)

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] `_sync_to_db`: при upsert поверх строки с `source='ui'` — WARNING «taking over UI subscription» (однократное событие, не мигание)
- [ ] тест: scanned-подписка с ключом существующей ui-строки → source становится dag_file, WARNING залогирован
- [ ] тест: повторный цикл с тем же сканом → без повторного WARNING (source уже dag_file)
- [ ] run tests - must pass before task 8

### Task 8: At-least-once в immediate-режиме

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`

- [ ] `_sync_trigger`: возвращает `"triggered" | "skipped" | "duplicate"`; **`DagRunAlreadyExists`** (основной случай редоставки, см. Technical Details) и `IntegrityError` (гонка) → INFO + `"duplicate"`; прочие исключения пробрасываются
- [ ] `_build_run_id(queue_name, message_id)`: детерминированный `rmq__{queue}__{message_id}` при наличии message_id, иначе timestamp-формат
- [ ] immediate-ветка `_consume_subscription`: `_match` (без ack) → `_trigger_dag` → `message.ack()`; исключение из триггера → WARNING + `_nack_and_sleep`
- [ ] `_trigger_dag`: принимает message_id, возвращает исход
- [ ] тест: успешный триггер → ack после trigger_dag (порядок вызовов)
- [ ] тест: trigger_dag бросает → nack+requeue, ack не вызван
- [ ] тест: `DagRunAlreadyExists` (редоставка) → outcome `"duplicate"`, ack, nack не вызван
- [ ] тест: `IntegrityError` (гонка двух инсертов) → outcome `"duplicate"`, ack
- [ ] тест: run_id детерминирован при message_id, timestamp-fallback без него
- [ ] run tests - must pass before task 9

### Task 9: Валидация cooldown conn_id

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`

- [ ] `reconcile`: `fire_conn_id = sorted({conn_id cooldown-подписок})[0]`, вычисление **перенести в начало `reconcile`** — сейчас оно стоит после цикла старта консьюмеров (consumer.py:206–212), а фильтрация должна сработать до него
- [ ] invalid cooldown-подписки (`cooldown > 0` и `conn_id != fire_conn_id`) исключаются из **desired active set до** формирования `new_ids` — тогда существующий cancel-блок «удалённых» подписок снимает их **уже запущенные** consumer-таски (reconcile удаляет таски только по отсутствующим sub_id; простой фильтр в цикле старта оставил бы работающий консьюмер активным после смены конфигурации)
- [ ] invalid-подписки: `consumer_status='error'` + объясняющий `last_error`, ERROR-лог
- [ ] восстановление: приведение conn_id к общему → подписка стартует на следующем цикле
- [ ] тест: два cooldown conn_id → детерминированный выбор арбитра, вторая подписка в error
- [ ] тест: подписка была запущена, затем стала invalid (сменился состав conn_id) → её consumer-таск отменён, статус error
- [ ] тест: один conn_id → поведение не изменилось
- [ ] run tests - must pass before task 10

### Task 10: Разгрузка event loop (executor для I/O и БД)

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_listener.py`
- Modify: `tests/watcher/test_consumer.py`

- [ ] `listener._main`: `_scan_subscriptions` и `_sync_to_db` через `run_in_executor`
- [ ] `listener._main`: блок чтения `get_enabled_subscriptions` (сейчас синхронный SQLAlchemy-запрос прямо в корутине, listener.py:234–252) → тоже через executor — иначе крупнейший per-cycle запрос остаётся на loop'е
- [ ] `_ConsumerState.write` → `async def`, запись в БД через `run_in_executor`; обновить колл-сайты
- [ ] записи статусов в `reconcile` (`set_consumer_status` при remove, `_update_all_conn_counts`) → через executor
- [ ] `consumer._get_or_create_connection`: `upsert_conn_status` в error-ветке пишет в БД прямо из корутины → через executor
- [ ] `listener._get_reconcile_interval`: `Variable.get` — синхронный запрос к БД из корутины → через executor
- [ ] тест: `_main` вызывает scan/sync не в event loop (мок executor)
- [ ] тест: чтение `get_enabled_subscriptions` уходит через executor
- [ ] тест: `_ConsumerState.write` дедуплицирует статусы как раньше
- [ ] тест: remove-status при удалении подписки и `_update_all_conn_counts` идут через executor
- [ ] run tests - must pass before task 11

### Task 11: Graceful stop + диагностика on_starting

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] `_main`: `asyncio.Event`; ссылки на loop и event сохраняются в listener'е и **обновляются при каждом рестарте петли** (`_run_loop` пересоздаёт loop после краша — старые ссылки протухают)
- [ ] `before_stopping`: `threading.Event.set()` (авторитетный сигнал) + `loop.call_soon_threadsafe(async_event.set)` как оптимизация раннего пробуждения — под guard'ом (`loop.is_closed()` / `except RuntimeError`), чтобы закрытый после краша loop не ронял shutdown шедулера + `thread.join(timeout=5)`
- [ ] `_main`: `asyncio.wait_for(event.wait(), timeout=interval)` вместо `asyncio.sleep(interval)`
- [ ] `on_starting`: INFO-лог при нераспознанном компоненте («not recognized as scheduler, watcher not started»)
- [ ] тест: before_stopping будит петлю немедленно, manager.stop() вызывается
- [ ] тест: before_stopping при уже закрытом loop → не бросает, threading.Event выставлен
- [ ] тест: нераспознанный компонент → INFO-лог, поток не стартует
- [ ] run tests - must pass before task 12

### Task 12: RMQLeaderLock (новый модуль)

**Files:**
- Create: `airflow_provider_rmq/watcher/leader.py`
- Create: `tests/watcher/test_leader.py`

- [ ] класс `RMQLeaderLock`: `try_acquire(conn_id)`, `is_held()`, `conn_id`, `release()` (см. Technical Details)
- [ ] выделенное НЕ-robust соединение (`aio_pika.connect`, не `connect_robust`)
- [ ] RESOURCE_LOCKED (405; ожидаемый класс `aio_pika.exceptions.ChannelLockedResource`, реэкспорт из aiormq — проверить по факту) → закрыть соединение, вернуть False; прочие ошибки пробрасывать
- [ ] тест: успешный захват → is_held() True, conn_id выставлен
- [ ] тест: RESOURCE_LOCKED → False, соединение закрыто, исключение не утекло
- [ ] тест: остаточная НЕ-exclusive очередь `rmq_watcher.leader` → `ChannelPreconditionFailed` пробрасывается (цикл залогирует и повторит, лидерство не захватывается молча)
- [ ] тест: падение соединения → is_held() False; release() идемпотентен
- [ ] run tests - must pass before task 13

### Task 13: Интеграция лидер-лока в reconcile-петлю

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`
- Create: `docs/adr/0006-leader-lock-via-rmq-exclusive-queue.md`

- [ ] `_main`: вычисление арбитра (union conn_id скана и enabled-подписок БД, fallback `rmq_default`); запрос к БД — через executor (см. Task 10), иначе медленная БД задерживает продление лидерства
- [ ] follower-ветка: `try_acquire` → False → пропуск `_sync_to_db` и `reconcile`, DEBUG-лог; ошибка соединения с арбитром → WARNING, пропуск цикла
- [ ] лидер-ветка: проверка `is_held()` и смены арбитра → demote (`manager.stop()` + `release()`) и перезахват в том же цикле
- [ ] graceful stop закрывает лок-соединение (мгновенная передача лидерства при рестарте)
- [ ] тест: follower не вызывает _sync_to_db/reconcile
- [ ] тест: ошибка соединения с арбитром в follower-ветке → WARNING, цикл пропущен (без исключения наружу)
- [ ] тест: потеря лок-соединения → manager.stop() + перезахват
- [ ] тест: смена арбитра (изменился состав conn_id) → release + захват на новом
- [ ] тест: graceful stop вызывает `lock.release()` — лок-соединение закрыто при shutdown
- [ ] ADR-0006: лидер-лок через exclusive-очередь (альтернативы: heartbeat в БД — отвергнуто из-за политики минимума записей; env-флаг — отвергнуто из-за отсутствия failover); зафиксировать известную границу — split-brain-окно при раскатке, если реплики транзиентно вычисляют разных арбитров из разошедшихся dags_folder (сходится после синхронизации файлов)
- [ ] run tests - must pass before task 14

### Task 14: Statsd-метрики

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_consumer.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] `Stats.incr` (из `airflow.stats`): `rmq_watcher.dag_triggered`, `rmq_watcher.consumer_reconnect`, `rmq_watcher.orphan_detected`, `rmq_watcher.leader_acquired`, `rmq_watcher.leader_lost`
- [ ] вызовы обёрнуты так, что сбой Stats не влияет на поток управления
- [ ] тест: метрики инкрементируются в соответствующих точках (мок Stats)
- [ ] тест: `Stats.incr` бросает исключение → основной путь (триггер/reconcile) продолжает выполнение
- [ ] run tests - must pass before task 15

### Task 15: Verify acceptance criteria

- [ ] все пункты Overview реализованы, крайние случаи из Technical Details покрыты
- [ ] полный тестовый сьют: `pytest`
- [ ] DagBag-регрессия example DAGs проходит
- [ ] `ruff check .` без новых замечаний
- [ ] поведенческие изменения отражены в CHANGELOG (confirm=True, at-least-once, лидер-лок)

### Task 16: [Final] Update documentation

- [ ] README: раздел HA («один активный watcher», лидер-лок, «один vhost = один Airflow», поведение при недоступности арбитра); заметка про at-least-once — дедуп доверяет уникальности `message_id` продюсера; граница гарантии — paused/inactive DAG = терминальный ACK (сообщение выбрасывается осознанно)
- [ ] README: publisher confirms в описании `RMQPublishOperator` и `RMQHook`
- [ ] readme_ru.md: те же разделы
- [ ] CONTEXT.md: термин **Leader lock** в глоссарий
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

**Manual verification**:
- стенд с двумя репликами шедулера: убедиться, что консьюмит ровно одна; убить
  лидера — follower подхватывает в пределах reconcile-интервала
- RabbitMQ 4.x с quorum-очередями: проверить at-least-once (kill шедулера в окне
  trigger→ACK → редоставка → дедуп по run_id)
- брокер в alarm-состоянии: `RMQPublishOperator` падает по таймауту, а не «зелёный»

**External system updates**:
- релиз на PyPI (workflow по тегу); в release notes выделить поведенческое
  изменение `confirm=True`
- потребителям с несколькими шедулерами — обновиться в первую очередь
