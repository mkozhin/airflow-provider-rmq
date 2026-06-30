# Multi-Queue Cooldown Trigger

## Overview

Расширение `@rmq_trigger` декоратора для поддержки нескольких очередей и режима cooldown.

**Проблема:** текущий декоратор слушает ровно одну очередь и запускает DAG немедленно при каждом сообщении.

**Решение:**
- `queues=[...]` — одна подписка покрывает несколько очередей; сообщение из любой запускает DAG
- `cooldown=N` — после первого сообщения DAG запускается ровно один раз через N секунд; все сообщения в окне ackаются; следующее сообщение после срабатывания таймера запускает новый отсчёт; `cooldown=0` — немедленный запуск (поведение как сейчас)

**Механизм cooldown:** RabbitMQ DLX-паттерн — без записи в Airflow DB:
- `rmq_watcher.pending.{dag_id}` — очередь-таймер (x-max-length=1, DLX); TTL per-message через `expiration` property; **потребителей НЕТ** — сообщение живёт ровно cooldown секунд, потом умирает через DLX
- `rmq_watcher.fire` — topic-exchange + одноимённая queue; routing_key = dag_id; Airflow слушает только её
- При publish в pending: если очередь полна (таймер активен) — брокер отклоняет (reject-publish) без publisher confirms, молча; отдельная обработка не нужна

## Context

- `airflow_provider_rmq/watcher/decorators.py` — публичный API декоратора
- `airflow_provider_rmq/watcher/listener.py` — AST-парсер DAG-файлов, синхронизация с DB; `active_subs` проекция (lines 190-199), `_sync_to_db` (lines 299-338)
- `airflow_provider_rmq/watcher/consumer.py` — RMQConsumerManager, asyncio задачи; `_consume_subscription` (lines 200-247), `reconcile` (lines 107-149)
- `airflow_provider_rmq/watcher/models.py` — `RMQSubscription` (поля `group_key`, `cooldown` уже есть, nullable)
- `tests/watcher/` — тесты watcher-модуля
- `docs/example_dags/rmq_watcher_triggered_dag.py` — пример DAG

## Development Approach

- **Testing approach**: Regular (code first, then tests)
- Полная обратная совместимость: `@rmq_trigger(queue="...", ...)` работает без изменений, `cooldown=NULL` → трактуется как 0
- Каждый таск завершается рабочими тестами до перехода к следующему

## Testing Strategy

- Юнит-тесты: мокать aio_pika connection/channel/queue
- Тесты AST-парсера: строки кода как входные данные, проверять выходные subscription dicts включая cooldown/group_key
- Тест end-to-end цепочки: parser→_sync_to_db→upsert сохраняет cooldown и group_key в DB
- Тесты consumer: при cooldown>0 вызывается publish в pending, НЕ вызывается trigger_dag; при cooldown=None/0 — прямой trigger_dag
- Тест fire consumer: routing_key → правильный dag_id

## Solution Overview

**API (декоратор):**
```python
@rmq_trigger(
    queues=["orders", "payments"],  # новое: список очередей
    cooldown=300,                    # новое: секунды (0 = немедленно)
    conn_id="rmq_default",
)
@dag(dag_id="my_dag", ...)
def my_dag(): ...
```

**Модель (без изменений схемы):** поля `group_key` и `cooldown` в `RMQSubscription` уже есть и nullable. Для списка очередей создаётся несколько строк с одинаковым `group_key` и `cooldown`. `group_key` выставляется в listener (не в decorator), где dag_id известен.

**Ограничение: все cooldown-подписки одного dag_id делят один таймер.** Если у DAG несколько стеков `@rmq_trigger` — они разделяют pending-очередь и cooldown. Это ожидаемое поведение.

**Ограничение: все cooldown-DAG должны использовать один conn_id/vhost.** Очереди `rmq_watcher.*` создаются в vhost первого найденного cooldown-соединения. Смешение conn_id с cooldown не поддерживается (documented limitation).

**Multi-scheduler deployment:** каждый активный scheduler создаёт собственные pending-очереди (имя включает dag_id, не scheduler_id). При N scheduler'ах первое сообщение вызывает N publish в pending — но `x-max-length=1` + `x-overflow=reject-publish` отклоняет N-1 из них молча. Итог: один DAG-run через cooldown секунд. Дубликаты невозможны благодаря детерминированному run_id (message_id из pending-сообщения).

**RMQ-инфраструктура (создаётся плагином автоматически):**
```
Exchange: rmq_watcher.fire  (topic, durable)
Queue:    rmq_watcher.fire  (durable, binding key=#)

Per DAG с cooldown>0:
Queue:    rmq_watcher.pending.{dag_id}  (durable, БЕЗ consumer)
  x-dead-letter-exchange    = rmq_watcher.fire
  x-dead-letter-routing-key = {dag_id}
  x-max-length              = 1
  x-overflow                = reject-publish
  (TTL задаётся per-message через expiration property)
```

**Почему topic exchange:** direct exchange с binding key `#` матчит только routing_key буквально равный `#`; topic exchange с `#` матчит любой routing_key — что нужно для одного consumer на все dag_id.

**Publish в pending (без publisher confirms):**
```python
import uuid
msg_id = str(uuid.uuid4())
await channel.default_exchange.publish(
    Message(b"", expiration=str(cooldown * 1000), message_id=msg_id),
    routing_key=f"rmq_watcher.pending.{dag_id}",
)
await message.ack()  # ACK после publish, не до
# Если очередь полна (x-max-length=1): брокер молча отклоняет.
# Если ACK не дошёл и сообщение пришло повторно — pending полна, publish молча отклоняется, ACKаем.
# Publisher confirms НЕ включаются — нет нужды различать принято/отклонено.
```

**Fire consumer — idempotent run_id:**
Run_id строится из `f"rmq_cooldown__{dag_id}__{message.message_id}"` — message_id задан при publish в pending (uuid4), сохраняется через DLX, одинаков при любом redelivery. ACK только после успешного trigger.

## What Goes Where

**Implementation Steps** — всё что меняется в этом репозитории.

**Post-Completion** — ручная проверка поведения в реальном RMQ.

## Implementation Steps

### Task 1: Расширить декоратор @rmq_trigger

**Files:**
- Modify: `airflow_provider_rmq/watcher/decorators.py`

- [x] добавить параметр `queues: list[str] | None = None`
- [x] добавить параметр `cooldown: int = 0`
- [x] если задан `queues` — создавать несколько записей в `_rmq_subscriptions` (по одной на очередь); `group_key` не выставлять в декораторе (устанавливается в listener где известен dag_id)
- [x] если задан `queue` (одиночный) — поведение без изменений, `cooldown` передаётся как есть (0 по умолчанию); `group_key` не выставлять в декораторе ни для одиночного, ни для списка — выставляется в listener
- [x] валидация: нельзя задать одновременно `queue` и `queues`; `cooldown >= 0`
- [x] написать тесты: одиночный queue без cooldown, список queues с cooldown, обратная совместимость, валидационные ошибки
- [x] запустить тесты — должны пройти

### Task 2: Обновить AST-парсер и DB-синхронизацию в listener.py

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`

- [x] в `_parse_rmq_trigger_decorator`: парсить `queues=[...]` (список строковых литералов) и `cooldown=N`; **всегда возвращать `list[dict]`** — пустой список если не распознан, один элемент для `queue=`, N элементов для `queues=`; убрать `dict | None` из возвращаемого типа
- [x] каждый subscription-dict получает ключи `cooldown` (int, default 0) и `group_key`: если `cooldown > 0` — `group_key = dag_id`; если `cooldown == 0` — `group_key = None`; выставляется в `_extract_subscriptions_from_file` где dag_id известен (для одиночного queue и для каждого из queues=[...])
- [x] в `_extract_subscriptions_from_file`: caller всегда итерирует результат (`for sub in _parse_rmq_trigger_decorator(decorator):`), без `if sub is not None`; выставлять `group_key=dag_id` для каждого dict
- [x] в `_sync_to_db`: передавать `cooldown` и `group_key` в `upsert_subscription` (уже есть в сигнатуре, сейчас не передаются)
- [x] в `_main` (active_subs проекция): добавить `"cooldown": sub.cooldown or 0` (не добавлять `group_key` — в consumer pipeline он не используется, нужен только UI)
- [x] написать тесты: парсер с `queues=[...]`, парсер с `cooldown=N`; **end-to-end тест**: parser→_sync_to_db→DB проверить что cooldown и group_key сохранены; тест что `cooldown=NULL` в DB → `active_subs` получает 0 (не падает)
- [x] запустить тесты — должны пройти

### Task 3: RMQ инфраструктура — fire exchange/queue и pending queues

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`

- [x] добавить `_ensure_fire_infrastructure(connection)`: создать **topic** exchange `rmq_watcher.fire` (durable) и queue `rmq_watcher.fire` (durable) с binding key `#`; идемпотентно
- [x] добавить `_ensure_pending_queue(channel, dag_id)`: объявить `rmq_watcher.pending.{dag_id}` с аргументами `x-dead-letter-exchange=rmq_watcher.fire`, `x-dead-letter-routing-key={dag_id}`, `x-max-length=1`, `x-overflow=reject-publish`; **БЕЗ consumer** на эту очередь; `_ensure_fire_infrastructure` и `_ensure_pending_queue` вызываются на **отдельном короткоживущем канале** (открыть → setup → закрыть), не на каналах consumers
- [x] **не удалять** pending-очереди автоматически при удалении подписки — таймер может быть ещё активен, очередь не потребляет ресурсы (нет consumer, x-max-length=1)
- [x] добавить `_orphaned_pending_dag_ids: set[str]` в `__init__` — dag_id для которых ранее была создана pending-очередь, но подписка теперь отсутствует
- [x] `RMQConsumerManager.__init__`: добавить `_cooldown_dag_ids: set[str]` (dag_id для которых созданы pending-очереди) и `_orphaned_pending_dag_ids: set[str]` (dag_id без активной подписки)
- [x] в `_provision_cooldown()`: вычислять `orphaned = self._cooldown_dag_ids - cooldown_dag_ids` (аргумент метода); добавлять новые в `_orphaned_pending_dag_ids` → логировать WARNING только при изменении (новые dag_id появились) с именами очередей и командой удаления `rabbitmqadmin delete queue name=rmq_watcher.pending.{dag_id}`; убирать из `_orphaned_pending_dag_ids` если dag_id вернулся → логировать INFO "restored, removing from orphaned set"; в остальных вызовах — молчать
- [x] ограничение: `_orphaned_pending_dag_ids` живёт в памяти — после рестарта scheduler не знает о pending-очередях от предыдущих сессий; для обнаружения через рестарты нужен RabbitMQ Management API (вне scope этого PR)
- [x] **заменить два параллельных dict одной структурой**: вместо `_tasks: dict[int, Task]` + `_sub_conn_ids: dict[int, str]` — ввести `_active: dict[int, _ActiveSub]`, где `_ActiveSub = dataclass(task: asyncio.Task, sub: dict)`; `sub` содержит полный snapshot (включая `conn_id`) на момент старта; удалить `_tasks` и `_sub_conn_ids` из `__init__`, заменить все обращения на `self._active[sub_id].task` / `self._active[sub_id].sub`; в том числе обновить `_update_all_conn_counts`: `self._tasks.get(sub["id"])` → `entry.task if (entry := self._active.get(sub["id"])) else None`
- [x] в `reconcile()`: для каждого sub проверять изменились ли поля влияющие на поведение consumer (`cooldown`, `filter_data`, `conn_id`) — сравнивать `self._active[sub_id].sub` с новым `sub`; если да — отменить task и стартовать новый; `self._active[sub_id] = _ActiveSub(task=new_task, sub=sub.copy())`; при удалении — `self._active.pop(sub_id)`; это даёт hot-reload без рестарта scheduler для **любых** изменений подписки
- [x] добавить `_fire_task: asyncio.Task | None = None` в `__init__`; **dict не нужен** — fire task единственный инфраструктурный
- [x] декомпозировать `reconcile()` — выделить два приватных метода: `async def _provision_cooldown(self, cooldown_dag_ids: set[str]) -> None` (создать fire infra + pending очереди + orphan tracking; всё что касается RMQ-команд и orphaned логики) и `def _subs_changed(self, sub_id: int, new_sub: dict) -> bool` (сравнение snapshot с `self._active[sub_id].sub` по полям `cooldown`, `filter_data`, `conn_id`); `reconcile()` остаётся диспетчером: вычислить diff → lifecycle tasks → вызвать `_provision_cooldown` → управлять `_fire_task`
- [x] в `reconcile()`: определить conn_id для fire инфраструктуры (первый conn_id среди cooldown-подписок); вызвать `_provision_cooldown`; если `has_cooldown` и `_fire_task is None or done` — запустить задачу; если `not has_cooldown` — отменить и обнулить `_fire_task`
- [x] в `stop()`: отменить `_fire_task` явно рядом с задачами из `_active` — один `gather` на все сразу; не забыть обнулить `_fire_task = None` после отмены; **не удалять** pending-очереди при stop() — таймер продолжает тикать в RMQ, DAG запустится после restart если cooldown ещё не истёк
- [x] **обработка ошибок при создании инфраструктуры**: если `_provision_cooldown` падает (нет прав, недоступен RMQ) — поймать исключение, логировать ERROR с деталями и именами очередей, не запускать `_fire_task`; обычные consumers продолжают работу; повторная попытка на следующем reconcile-цикле (стандартный reconcile-loop сам повторит)
- [x] написать тесты: мок channel, проверить `declare_exchange` с type=topic, `declare_queue` pending с правильными x-аргументами, отсутствие consumer на pending
- [x] тест hot-reload: sub с cooldown=300 уже работает → reconcile с cooldown=600 → task перезапущен; sub с изменённым filter_data → task перезапущен; sub без изменений → task НЕ перезапускается
- [x] запустить тесты — должны пройти

### Task 4: Логика cooldown в consumer

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `airflow_provider_rmq/utils/amqp.py`

- [x] **вместо `match_message`**: разбить внутренности `match_and_ack` на два composable шага в `amqp.py`: `_match(message, msg_filter) -> bool` (чистая проверка, без ACK) и `_nack_and_sleep(message)` (NACK+requeue+sleep(0.1)); `match_and_ack` остаётся публичной обёрткой поверх обоих — публичный интерфейс amqp.py не меняется, триггер продолжает использовать `match_and_ack` без изменений
- [x] в `_consume_subscription`: при получении сообщения проверять `sub["cooldown"] > 0` (cooldown уже всегда int ≥ 0 из проекции)
- [x] если cooldown=0: существующая логика `match_and_ack` + `_trigger_dag` без изменений; добавить `"source": "immediate"` в conf (симметрично с cooldown-trigger, чтобы DAG-код не получал KeyError при обращении к `conf["source"]`)
- [x] если cooldown>0: использовать `_match` + `_nack_and_sleep` — если не совпадает: `await _nack_and_sleep(message)` + `continue`; если совпадает: генерировать `msg_id = str(uuid.uuid4())`, publish в pending через **default exchange** текущего channel: `await channel.default_exchange.publish(Message(b"", expiration=str(sub["cooldown"]*1000), message_id=msg_id), routing_key=f"rmq_watcher.pending.{dag_id}")` → `await message.ack()`; NACK-логика не дублируется — живёт только в `_nack_and_sleep`
- [x] написать тесты: cooldown=0 → вызов trigger_dag; cooldown=300 → вызов publish в pending, НЕ trigger_dag; два быстрых сообщения подряд — publish вызван один раз (второй отклонён брокером молча, нет исключений)
- [x] запустить тесты — должны пройти

### Task 5: Consumer для rmq_watcher.fire

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`

- [x] добавить `_consume_fire_queue(connection)` — asyncio задача с той же `while True / except` структурой что и `_consume_subscription`: `ChannelNotFoundEntity` → фатально (exit, reconcile перезапустит на следующем цикле), `ChannelClosed` → retry с задержкой, `CancelledError` → return, generic → retry с задержкой; подписаться на `rmq_watcher.fire`, при сообщении читать `message.routing_key` как dag_id
- [x] run_id строить детерминированно: `f"rmq_cooldown__{dag_id}__{message.message_id}"` — message_id задаётся при publish в pending (uuid4), сохраняется через DLX, одинаков при любом redelivery; гарантирует что повторная доставка не создаёт дубликат
- [x] ACK сообщение только после успешного `_sync_trigger` (или при дубликате IntegrityError — тоже ACK)
- [x] conf для triggered DAG: `{"source": "cooldown", "dag_id": dag_id, "body": "", "headers": {}, "routing_key": dag_id, "queue": "rmq_watcher.fire", "subscription_id": None}` — заглушки вместо оригинальных данных сообщения (они потеряны в DLX-цепочке); DAG-код не получит KeyError при обращении к стандартным ключам
- [x] в `reconcile()`: если есть хотя бы одна cooldown-подписка — запустить `_consume_fire_queue` задачу если не запущена; если cooldown-подписок нет — остановить
- [x] написать тесты: fire-consumer вызывает trigger_dag с dag_id из routing_key; повторная доставка (IntegrityError) — ACK без повторного trigger; задача запускается/останавливается при добавлении/удалении cooldown-подписок
- [x] запустить тесты — должны пройти

### Task 6: UI — поддержка групп и cooldown

**Files:**
- Modify: `airflow_provider_rmq/watcher/views.py`
- Modify: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`
- Modify: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscription_form.html`

**Группировка в списке:**
- [x] в `views.py`: перед передачей в шаблон группировать подписки по `(group_key, conn_id, source)` где `group_key` не NULL; подписки с `group_key=NULL` — отдельные строки как сейчас
- [x] каждая "группа" содержит: dag_id, список queue_name, conn_id, cooldown, source, enabled, список sub_id, статус "X/N listening" (N = всего очередей в группе, X = в статусе listening), first non-null last_error
- [x] добавить колонку `Cooldown` в таблицу (пусто если NULL/0, иначе `Ns`)
- [x] для группы: кнопки Enable/Disable и Delete применяются ко **всем** sub_id группы; Edit открывает форму группы
- [x] `group_key` для UI-подписок: выставляется в бэкенде автоматически = dag_id при cooldown > 0, NULL при cooldown = 0

**Форма создания/редактирования:**
- [x] добавить поле `cooldown` (number, min=0, default=0, help: "0 = немедленный запуск")
- [x] заменить одиночный `queue_name` на динамический список очередей (кнопка "+ Add queue", кнопка "✕" на каждой строке); минимум 1 очередь
- [x] при POST с несколькими очередями: создавать/обновлять по одной DB-строке на очередь с одинаковым cooldown и group_key; при cooldown > 0 — group_key = dag_id; при cooldown = 0 — group_key = NULL
- [x] при редактировании группы: форма показывает все текущие очереди; добавление новой очереди = INSERT новой строки; удаление очереди = DELETE той строки; изменение cooldown/filter_data применяется ко всем строкам группы
- [x] валидация: минимум 1 очередь; cooldown ≥ 0; dag_id и conn_id обязательны
- [x] для dag_file-групп: все поля кроме enabled — readonly (без изменений)
- [x] написать тесты views: POST create с несколькими очередями создаёт N строк; POST edit группы обновляет cooldown на всех строках; DELETE группы удаляет все строки

### Task 7: Проверка acceptance criteria

- [x] `@rmq_trigger(queue="single")` — существующие тесты проходят без изменений
- [x] `@rmq_trigger(queues=["a","b"], cooldown=5)` — AST парсер создаёт 2 subscription с cooldown=5 и group_key=dag_id
- [x] `cooldown=NULL` в существующих DB-строках — не вызывает TypeError, трактуется как 0
- [x] при сообщении в queue a: публикуется в pending, не вызывается trigger_dag; второе сообщение в window: нет исключений
- [x] orphaned-очереди: при удалении DAG лог WARNING при первом reconcile-цикле после удаления, молчание в последующих
- [x] hot-reload: изменение cooldown в DAG-файле подхватывается на следующем reconcile без рестарта scheduler
- [x] UI: список показывает группы с "X/N listening"; форма создаёт несколько строк при нескольких очередях
- [x] запустить полный тест-сьют: `python -m pytest tests/ -v` — 390 passed

### Task 8: Документация и пример DAG

**Files:**
- Modify: `docs/example_dags/rmq_watcher_triggered_dag.py`
- Modify: `airflow_provider_rmq/watcher/decorators.py` (docstring)

- [x] добавить пример с `queues=[...]` и `cooldown=N` в example DAG
- [x] обновить docstring: описать `queues`, `cooldown`, ограничение на один conn_id/vhost для cooldown, инвариант "нет consumer на pending", **ограничение: при cooldown DAG-run получает пустые `conf["body"]`/`conf["headers"]` — оригинальные данные сообщения в DLX-цепочке не сохраняются**, изменение cooldown вступает в силу на следующем reconcile-цикле (не мгновенно для уже опубликованных pending-сообщений)

### Task 9: Подготовка к релизу

**Files:**
- Modify: `CHANGELOG.md`
- Modify: `readme.md`
- Modify: `readme_ru.md`

- [x] определить версию релиза: `v2.1.0` (minor bump — новая функциональность, обратная совместимость сохранена)
- [x] добавить раздел `## v2.1.0` в `CHANGELOG.md`: multi-queue `queues=[...]`, `cooldown=N`, DLX-механизм в RMQ
- [x] обновить `readme.md` (EN): добавить примеры с `queues` и `cooldown`, описать поведение cooldown
- [x] обновить `readme_ru.md` (RU): те же правки на русском
- [x] убедиться что все тесты проходят: `python -m pytest tests/ -v`
- [x] сделать коммит всех изменений
- [x] поставить тег: `git tag v2.1.0`
- [x] переместить план в `docs/plans/completed/`

## Post-Completion

**Ручная проверка в реальном RMQ (vhost realcom):**
- убедиться что exchange `rmq_watcher.fire` (topic) и queue `rmq_watcher.fire` созданы
- убедиться что pending-очереди созданы с правильными x-аргументами (visible в Management UI)
- убедиться что на pending-очереди нет consumer
- отправить тестовое сообщение в отслеживаемую очередь, проверить что DAG запустился ровно через cooldown секунд
- отправить несколько сообщений подряд в разные очереди группы, убедиться что DAG запустился один раз
- перезапустить Airflow scheduler в середине cooldown, убедиться что DAG всё равно запустился (таймер продолжил тикать в RMQ)

**Права пользователя Airflow в RMQ:**
- пользователь Airflow должен иметь права configure/write/read на `rmq_watcher\..*`
- добавить `rmq_watcher\..*` в configure и write/read permissions если права ограничены паттерном
