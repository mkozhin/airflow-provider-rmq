# Exchange-mode triggers for `@rmq_trigger`

## Overview

Сейчас `@rmq_trigger(queue=...)` подписывает DAG на одну заранее существующую,
вручную забинженную очередь. Для потока Jetstat→Airflow логика подписки ("какой DAG
реагирует на какой id/status от Jetstat") сегодня живёт в отдельной YAML-таблице маршрутов
(`rabbitmq/benthos/config/webhook_routes.yaml`), которую читает Benthos `out`-stream и делает
прямой HTTP POST в Airflow REST API. Эта таблица — единственное место, которое нужно
трогать при изменении условия срабатывания DAG, и оно находится вне DAG-файла и вне провайдера.

Этот план добавляет в `@rmq_trigger` второй режим подписки:

```python
@rmq_trigger(
    exchange="jetstat.airflow",
    routing_key_ids=["670f877702775c2de8325b1f"],
    routing_key_status="succeeded",   # по умолчанию "*" = любой статус
)

# либо литеральные routing key любой формы (не привязанные к id/status):
@rmq_trigger(exchange="some.other.exchange", routing_keys=["region.eu.alert"])
```

`routing_key_ids`×`routing_key_status` — удобный cross-product синтаксис под форму ключей
Jetstat (`{id}.{status}`); `routing_keys` — литеральные ключи любой формы для остальных
случаев. Можно указывать оба — итоговый набор ключей — их объединение (см. ADR-0004).

Когда указан `exchange=`, за RMQ-инфраструктуру отвечает провайдер — не оператор, не YAML-файл:
- декларирует topic exchange (идемпотентно, активный declare)
- декларирует выделенную очередь на DAG `rmq_watcher.sub.{dag_id}` (с TTL 8ч — см. ADR-0005)
- биндит/анбиндит эту очередь ровно на те routing key, которые сейчас объявлены в декораторе
  (пересчитывается каждый reconcile-цикл и сверяется с реальным состоянием RabbitMQ через
  Management HTTP API — а не с чем-то, что хранится в БД Airflow)
- страховка: немаршрутизируемые сообщения попадают в `{exchange}.unrouted`
  (alternate-exchange, TTL 8ч), а каждое смаршрутизированное сообщение зеркалируется в
  `{exchange}.log` (catch-all биндинг по `#`, TTL 8ч) для системы логирования
- запрещает (валидирует, а не только документирует) несколько `@rmq_trigger(exchange=...)`
  на одном DAG — все они претендуют на одну и ту же очередь `rmq_watcher.sub.{dag_id}`

Изменение полностью аддитивное: `queue=`/`queues=` продолжают работать без изменений.
Сопутствующий план в репозитории `rabbitmq` (`jetstat-airflow-exchange-routing`) добавляет
сторону Benthos, которая публикует в новый exchange; реализация обоих планов идёт параллельно
и независимо друг от друга.

## Context (from discovery)

- `airflow_provider_rmq/watcher/decorators.py` — `rmq_trigger()`: проверяет взаимоисключение
  `queue`/`queues`, собирает subscription dicts, добавляет их в `dag._rmq_subscriptions` (этот
  атрибут — только для инспекции/документации, во время работы листенера он не читается).
- `airflow_provider_rmq/watcher/listener.py` — `_parse_rmq_trigger_decorator` парсит
  `@rmq_trigger(...)` прямо из AST исходника DAG-файла (файл никогда не исполняется — чтобы не
  ловить import lock шедулера). `_extract_subscriptions_from_file` резолвит `dag_id` и
  выставляет `group_key`. `_sync_to_db` апсертит `dag_file`-подписки; `_main()` на каждом
  reconcile-цикле перестраивает `active_subs` из БД и зовёт
  `RMQConsumerManager.reconcile(active_subs)`.
- `airflow_provider_rmq/watcher/consumer.py` — `RMQConsumerManager`: одна asyncio-задача на
  подписку (`_consume_subscription`, passive declare очереди). `_provision_cooldown` +
  `_ensure_fire_infrastructure`/`_ensure_pending_queue` — прямой прецедент идемпотентного
  самопровиженинга RMQ-инфраструктуры из `reconcile()`. `_trigger_dag`'s `conf` уже прокидывает
  `routing_key` — здесь менять ничего не нужно.
- `airflow_provider_rmq/watcher/models.py` — у `RMQSubscription` unique constraint —
  `(dag_id, queue_name, conn_id)`. **Новая колонка не нужна** — почему, см. Technical Details.
- `airflow_provider_rmq/utils/amqp.py` — `build_amqp_connection()` берёт vhost из
  `conn_info.schema or "/"` — то же самое извлечение используется для vhost в Management API.
- Текущие зависимости (`pyproject.toml`) — `apache-airflow`, `pika`, `aio-pika`, `tenacity`;
  HTTP-клиента среди них нет — нужен новый (`httpx`) для bind-diff вызовов к Management API.

## Development Approach

- **Testing approach**: Regular (код сначала, тесты сразу после в той же задаче).
- Каждую задачу доводим до конца, прогоняем тесты, и только потом переходим к следующей.
- Никакой DB-миграции: почему exchange/routing-key метаданные никогда не попадают в таблицу
  `rmq_watcher_subscriptions` — см. Technical Details.
- Поведение режима `queue=`/`queues=` для валидных вызовов не меняется. Реализация — да:
  `decorators.py` и `listener.py` теперь делегируют построение subscription dict общему
  `subscription_builder.py` (Task 1/2, найдено на сессии `/improve-codebase-architecture`
  2026-06-22) вместо дублирования одной и той же логики в двух файлах. Один наблюдаемый побочный
  эффект: AST-парсер теперь тоже отклоняет `cooldown < 0` (раньше это делал только декоратор при
  обычном импорте DAG) — см. Task 2.

## Testing Strategy

- Юнит-тесты на каждую новую/изменённую функцию (валидация декоратора, AST-парсинг, клиент
  Management API с мокнутым `httpx`, provisioning/bind-diff с мокнутым `aio_pika`-каналом —
  по образцу существующих `_QueueIterCtx`/`MagicMock`-паттернов в
  `tests/watcher/test_consumer.py` и `tests/watcher/test_listener.py`).
- Правила валидации/построения subscription dict (mutex, non-empty, dots, union, `cooldown<0`)
  тестируются один раз напрямую против `subscription_builder.py`
  (`tests/watcher/test_subscription_builder.py`), а не дублируются в тестах декоратора и
  AST-парсера. Аналогично — алгебра orphan-diff тестируется один раз против `orphan_tracker.py`
  (`tests/watcher/test_orphan_tracker.py`), используется и cooldown, и exchange-режимом.
- e2e/UI-тестов в этом репозитории нет (exchange-режим — только из DAG-файла, в
  `RMQWatcherView` не выводится).

## Solution Overview

- **Имя exchange, routing keys**: задаются декоратором двумя взаимодополняемыми способами —
  `routing_key_ids` × `routing_key_status` (по умолчанию `"*"` — AMQP topic wildcard сегмент,
  разворачивается в `{id}.{status}`) для Jetstat-формы ключей, и/или `routing_keys` —
  литеральные ключи любой формы для остальных случаев. Итоговый набор — объединение того, что
  передано (см. ADR-0004). Несколько `@rmq_trigger(exchange=...)` на одном DAG — ошибка
  валидации, а не молчаливая потеря метаданных (одна очередь `rmq_watcher.sub.{dag_id}` на DAG).
- **Имя очереди**: `rmq_watcher.sub.{dag_id}` — одна общая очередь на DAG, по аналогии с уже
  существующей `rmq_watcher.pending.{dag_id}` из cooldown, но с TTL 8ч на сообщениях (в отличие
  от pending queue, у sub queue есть живой consumer — TTL здесь не таймер-механизм, а только
  страховка от бесконечного роста orphaned-очереди, см. ADR-0005). Потребление идёт по тому же
  самому `_consume_subscription`, что и для `queue=`; новым является только provisioning.
- **Состояние биндингов живёт в RabbitMQ, а не в БД Airflow**: на каждом reconcile-цикле
  желаемый набор routing key (из свежепарсенного декоратора) сверяется с *текущими* живыми
  биндингами через `GET /api/queues/{vhost}/{queue}/bindings` (Management HTTP API), а разница
  применяется обычным AMQP `bind`/`unbind`. Это продолжает принцип, заложенный в cooldown:
  минимизировать записи в метаданную БД Airflow, использовать RabbitMQ как источник истины для
  RMQ-стороны состояния. Orphaned-биндинги (DAG удалён/переименован/временно не парсится) не
  снимаются автоматически — только WARNING + TTL-страховка, см. ADR-0005.
- **Изоляция отказов**: если Management API недоступен (например, проблема сети/Traefik до
  `management_url` в connection'е Airflow), bind-diff на этом цикле пропускается с ERROR-логом —
  очередь всё равно декларируется и потребляется нормально; биндинги досчитаются на следующем
  цикле. Тот же паттерн изоляции, что и в `_provision_cooldown`. Отдельно распознаётся конфликт
  при декларировании exchange с несовпадающими свойствами (`PRECONDITION_FAILED`, если имя
  `exchange=` уже занято чем-то внешним) — логируется с явным указанием причины, а не как общая
  transient-ошибка RMQ.

## Technical Details

### Почему миграция БД не нужна

`RMQSubscription` должна описывать только *что потреблять* (`queue_name`, `conn_id`,
`filter_data`, `cooldown`) — для exchange-режима это не меняется, потому что очередь на DAG
потребляется точно так же, как любая вручную забинженная очередь. Метаданные *биндинга*
(`exchange`, `routing_keys`) нужны только мимолётно, один раз на reconcile-цикл, чтобы
провижинить/сверить биндинги — а они уже лежат в памяти в AST-кэше листенера
(`self._cached_subs`) на каждом цикле, потому что листенер всё равно перепарсивает каждый
изменённый DAG-файл перед синком в БД. Поэтому вместо того чтобы писать `exchange`/
`routing_keys` в БД, листенер примешивает их обратно к списку `active_subs`, полученному из БД,
в памяти, по ключу `(dag_id, queue_name, conn_id)`, прямо перед вызовом `reconcile()`. Это не
трогает текущий смысл `filter_data` (блоб формата `MessageFilter.serialize()`) и полностью
избегает миграции.

Принятый trade-off: список подписок в `RMQWatcherView` для exchange-режима будет показывать имя
очереди (`rmq_watcher.sub.my_dag`) так же, как для любой другой подписки на очередь, но не
будет показывать exchange/routing keys — они видны в исходнике DAG, что и есть единственный
источник истины по замыслу исходной задачи.

### Валидация декоратора

Правила ниже реализуются один раз в `subscription_builder.build_subscriptions()` (Task 1) и
используются и `decorators.py`, и AST-парсером в `listener.py` (Task 2) — не дублируются в
обоих файлах по отдельности.

`queue`, `queues`, `exchange` взаимоисключающие (ровно один обязателен). Когда указан
`exchange`: нужен хотя бы один из `routing_keys` (непустой список строк, литеральные ключи) или
`routing_key_ids` (непустой список строк; `routing_key_status` — `str` или `list[str]`, по
умолчанию `"*"`, нормализуется в список). Итоговые routing key = `routing_keys` (если задан)
объединённые с `[f"{id_}.{status}" for id_ in routing_key_ids for status in statuses]` (если
задан `routing_key_ids`) — см. ADR-0004 про то, почему оба способа сосуществуют, а не один
заменяет другой.

Каждый id из `routing_key_ids` и каждый status из `routing_key_status` валидируются на
отсутствие `.` — точка внутри id/status молча превратила бы двух-сегментный routing key в
трёх-сегментный и изменила бы семантику wildcard `*` по умолчанию. `routing_keys` (литеральные
ключи) этой проверке не подлежит — там точки — часть осознанно написанного топик-паттерна.

`cooldown` и `filter_data` применяются к exchange-режиму ровно так же ортогонально, как и
сегодня — механизму DLX-cooldown важно только то, из какой очереди вычитано сообщение, а не то,
как эта очередь получила свои биндинги.

**Стекинг `exchange=` на одном DAG — ошибка валидации, не документированное ограничение.**
Несколько `@rmq_trigger(exchange=...)` на одном DAG не поддерживаются — все они резолвились бы
в одну и ту же очередь `rmq_watcher.sub.{dag_id}`, и при слиянии метаданных в памяти в листенере
выжили бы только `exchange`/`routing_keys` последнего распарсенного декоратора, молча роняя
часть routing key, которые разработчик явно написал в коде DAG. Это серьёзнее, чем cooldown-
прецедент "все cooldown-подписки делят одну pending-очередь" (там последствие — общий таймер,
не потеря конфигурации), поэтому `decorator(dag)` явно проверяет `dag._rmq_subscriptions` на
существующую запись с `exchange` для этого DAG (через `has_exchange_conflict()` из
`subscription_builder.py`, см. Task 1) и кидает `ValueError` при повторном вызове с
`exchange=`; AST-парсер в `listener.py` использует ту же функцию, но реагирует мягче — WARNING
и пропуск дубликата (см. Task 2), так как там нет возможности прервать импорт DAG. Ограничение
по-прежнему документируется в docstring — используйте один вызов декоратора с объединённым
списком routing key.

### RMQ-инфраструктура на exchange (провижинится провайдером, не Benthos)

- Exchange `{exchange}` — topic, durable, `arguments={"alternate-exchange": "{exchange}.unrouted"}`
- Exchange `{exchange}.unrouted` — fanout, durable (цель alternate-exchange)
- Queue `{exchange}.unrouted` — durable, `x-message-ttl=28800000` (8ч), забинжена на fanout
  exchange выше без routing key
- Queue `{exchange}.log` — durable, `x-message-ttl=28800000` (8ч), забинжена на `{exchange}` с
  routing key `#` (catch-all — зеркалирует каждое смаршрутизированное сообщение для системы
  логирования)
- Queue `rmq_watcher.sub.{dag_id}` — durable, **`x-message-ttl=28800000` (8ч)**, активно
  вычитывается живым консьюмером (как и любая другая подписка на `queue=`). TTL здесь — не
  таймер-механизм (как в cooldown), а исключительно страховка от бесконечного роста очереди,
  если подписка станет orphaned (см. ADR-0005); в штатной работе сообщения вычитываются
  консьюмером намного раньше TTL.

Все declare идемпотентны (`channel.declare_*`, без `passive=True`) — безопасно повторять каждый
reconcile-цикл, тот же паттерн, что в `_ensure_fire_infrastructure`.

**Конфликт с существующим exchange:** так как `exchange=` полностью задаётся разработчиком DAG
и не привязан к Jetstat, он теоретически может указать имя уже существующего exchange,
созданного вне провайдера с другими свойствами (другой тип, без `alternate-exchange`). Активный
`declare_exchange` в этом случае получит от RabbitMQ `PRECONDITION_FAILED` (канал закрывается).
Это распознаётся отдельно от прочих RMQ-ошибок и логируется на ERROR с явной причиной
("exchange %r уже существует с другими свойствами"), а не как общая transient-ошибка сети.

### Клиент Management API

Новый extra в Airflow Connection: `management_url`. **Текущее реальное значение —
`https://mb.realcombi.mgcom.ru` (без префикса `/rabbitmq`)**: хотя в `rabbitmq.conf` задан
`management.path_prefix = /rabbitmq`, монтирование этого файла в `rabbitmq/docker-compose.yml`
сейчас закомментировано (зафиксировано как критичная находка в `docs/audit_report.md`
репозитория `rabbitmq`), так что на живом сервере path_prefix не применяется и Management API
отдаётся с корня. Если когда-нибудь этот конфиг примонтируют — значение `management_url` нужно
будет обновить на вариант с суффиксом `/rabbitmq`; это чистая конфигурация в Airflow Connection,
код клиента не зависит от префикса. Тот же `login`/`password`, что и для AMQP-соединения
(подтверждено: пароль admin-пользователя RabbitMQ одинаково работает и для AMQP, и для
Management HTTP API). Vhost берётся так же, как уже делает `build_amqp_connection`:
`conn_info.schema or "/"`.

```
GET {management_url}/api/queues/{quote(vhost)}/{quote(queue_name)}/bindings
```
Ответ — JSON-список биндингов; фильтруем `source == exchange`, собираем значения
`routing_key` → текущий набор биндингов. Сверяем с желаемым набором из декоратора; разницу
биндим (`+`) и анбиндим (`-`) обычным AMQP (`queue.bind`/`queue.unbind`) — Management API
используется только для *чтения* в этом diff'е, потому что у самого AMQP 0-9-1 нет операции
"покажи мои текущие биндинги".

### Необходимое исправление порядка в `reconcile()`

Очереди exchange-режима создаёт провайдер (в отличие от режима `queue=`, где очередь заранее
создаёт оператор, а `_consume_subscription` всегда делает только `passive=True` declare).
Сейчас `reconcile()` запускает новые consumer-задачи *до* cooldown-provisioning'а. Для
exchange-режима именно для нового шага provisioning'а этот порядок нужно развернуть: он должен
выполняться **до** блока "отменить снятые / запустить новые подписки", иначе первая
consumer-задача для очереди может попытаться сделать passive declare против ещё не
существующей очереди и фатально завершиться (`ChannelNotFoundEntity` убивает задачу; она
перезапустится только на следующем цикле как no-op restart, удвоив задержку первого срабатывания
на величину `reconcile_interval`).

## What Goes Where

- **Implementation Steps**: общий модуль построения subscription dict (`subscription_builder.py`)
  + декоратор, AST-парсер, шаг слияния в листенере, клиент Management API, общий модуль
  orphan-tracking (`orphan_tracker.py`) + provisioning/bind-diff в consumer'е, попутный
  рефакторинг формы UI (`subscription_form.py`, не связан с exchange-режимом), тесты,
  документация.
- **Post-Completion**: расширение прав RMQ для пользователя Airflow (выполняется в
  сопутствующем плане репозитория `rabbitmq` — здесь только документируется, что нужно),
  end-to-end ручная проверка на реальном Jetstat-подписанном DAG.

## Implementation Steps

### Task 1: Decorator API — `exchange`/`routing_keys`/`routing_key_ids`/`routing_key_status`

**Архитектурная заметка:** валидация и построение subscription dict для всех трёх режимов
(`queue`/`queues`/`exchange`) выносятся в общий модуль `subscription_builder.py`, который
использует и `decorators.py` (эта задача), и AST-парсер в `listener.py` (Task 2) — иначе план
дублирует одни и те же правила (mutex, non-empty, dots, union, `cooldown < 0`) в двух файлах с
двумя разными extraction-путями (реальные Python-значения vs. `ast.literal_eval`). Найдено и
решено на сессии `/improve-codebase-architecture` 2026-06-22.

**Files:**
- Create: `airflow_provider_rmq/watcher/subscription_builder.py`
- Create: `tests/watcher/test_subscription_builder.py`
- Modify: `airflow_provider_rmq/watcher/decorators.py`
- Modify: `tests/watcher/test_decorators.py`

- [x] в `subscription_builder.py` добавить
  `build_subscriptions(*, dag_id: str, queue: str | None = None, queues: list[str] | None = None, exchange: str | None = None, routing_keys: list[str] | None = None, routing_key_ids: list[str] | None = None, routing_key_status: str | list[str] = "*", conn_id: str = "rmq_default", filter_data: dict | None = None, cooldown: int = 0) -> list[dict]`
  — проверяет взаимоисключение `queue`/`queues`/`exchange` (ровно один обязателен);
  `cooldown < 0` → `ValueError`; для exchange-режима — `exchange` не может начинаться с
  `rmq_watcher.` (зарезервировано под собственную инфраструктуру cooldown/fire —
  `rmq_watcher.fire`/`rmq_watcher.pending.*`/`rmq_watcher.sub.*`; иначе конфликт проявится
  позже как малопонятный `PRECONDITION_FAILED` при declare, а не как явная ошибка
  валидации); хотя бы один из `routing_keys`/`routing_key_ids` (оба пустые/не заданы →
  ошибка), непустые списки строк, каждый элемент `routing_keys` — непустая строка (пустая
  строка `""` → ошибка), `.` в `routing_key_ids`/`routing_key_status` → ошибка (литеральный
  `routing_keys` этой last-проверке не подлежит, но проверке на непустоту строки —
  подлежит), нормализует `routing_key_status` в список, считает
  `from_ids = [f"{id_}.{status}" for id_ in routing_key_ids for status in statuses]`, итоговые
  routing key = объединение `routing_keys` и `from_ids` (см. ADR-0004); сам строит
  `queue_name = f"rmq_watcher.sub.{dag_id}"` для exchange-режима. Возвращает список dict с
  полями `{"queue_name", "conn_id", "filter_data", "cooldown"}` (+ `{"exchange", "routing_keys"}`
  для exchange-режима) — одна запись на каждую `queue` из `queues`, либо одна запись для
  `queue=`/`exchange=`
- [x] в `subscription_builder.py` добавить
  `has_exchange_conflict(existing: list[dict], new: list[dict]) -> bool` — `True`, если и в
  `existing`, и в `new` есть хотя бы одна запись с ключом `"exchange"` (стекинг `exchange=` на
  одном DAG, см. Technical Details); чистая функция над списками dict, без `dag`/AST
- [x] переписать `rmq_trigger()` в `decorators.py`: верхнеуровневая функция только собирает
  raw-аргументы в closure; вся валидация и построение dict — внутри `decorator(dag)` через
  `build_subscriptions(dag_id=dag.dag_id, ...)`; перед `extend` вызвать
  `has_exchange_conflict(dag._rmq_subscriptions, new_subs)` → `ValueError` при `True`
- [x] оставить `queue` первым позиционным параметром `rmq_trigger()` (как сегодня) —
  `exchange`/`routing_keys`/`routing_key_ids`/`routing_key_status` добавляются только как
  keyword-параметры после уже существующих. Это значит: позиционный вызов
  `@rmq_trigger("some.name")` всегда трактуется как `queue="some.name"`, никогда как
  `exchange=`; exchange-режим требует явного `exchange=` — без позиционного шортката
- [x] расширить docstring `rmq_trigger()` примером exchange-режима (обе формы routing key),
  явным ограничением про запрет stacking и явной фразой про то, что позиционный аргумент
  не работает для exchange-режима
- [x] написать тесты на `build_subscriptions` (в `test_subscription_builder.py`): ошибки
  взаимоисключения (`exchange` + `queue`, `exchange` + `queues`, ни один не задан), ни
  `routing_keys` ни `routing_key_ids` не заданы — ошибка, `routing_keys` пустой список —
  ошибка, `routing_key_ids` пустой список — ошибка, `routing_keys=["", "valid.key"]` —
  ошибка (пустая строка в списке), `exchange` начинается с `rmq_watcher.` — ошибка, дефолтный
  `routing_key_status="*"` даёт ключи `{id}.*`, явный одиночный статус, явный список статусов,
  одновременно заданные `routing_keys` и `routing_key_ids` дают объединение, `queue_name`
  резолвится из `dag_id`, переданного аргументом, `.` в `routing_key_ids`/`routing_key_status`
  — ошибка, отрицательный `cooldown` — ошибка (включая exchange-режим)
- [x] написать тесты на `has_exchange_conflict` (true при двух exchange-записях, false когда
  одна или обе — queue-записи, false на пустых списках)
- [x] написать в `test_decorators.py` только тесты декораторской специфики: `rmq_trigger()`
  возвращает `dag` без изменений, `dag._rmq_subscriptions` пополняется корректно, повторный
  вызов с `exchange=` на одном DAG (через два декоратора) кидает `ValueError` (проброс из
  `has_exchange_conflict`), любая ошибка валидации из `build_subscriptions` всплывает как есть
- [x] прогнать тесты — должны проходить перед следующей задачей:
  `pytest tests/watcher/test_subscription_builder.py tests/watcher/test_decorators.py`

### Task 2: Поддержка exchange-режима в AST-парсере

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [x] расширить allow-list ключевых аргументов в `_parse_rmq_trigger_decorator` полями
  `exchange`, `routing_keys`, `routing_key_ids`, `routing_key_status` (помимо уже существующих
  `queue`/`queues`/`conn_id`/`filter_data`/`cooldown`) — функция остаётся AST-специфичной:
  `ast.literal_eval` каждого kwarg, нелитеральные значения пропускаются как сегодня
- [x] добавить `_parse_rmq_trigger_decorator` параметр `dag_id: str`; после извлечения kwargs
  вызвать `build_subscriptions(dag_id=dag_id, **kwargs)` из `subscription_builder.py` (Task 1)
  вместо реализации mutex/non-empty/dots/union/`cooldown < 0` внутри этого файла; `dag_id`
  резолвится в `_extract_subscriptions_from_file` раньше, чем сегодня — до цикла по
  `node.decorator_list`, а не после него. На `ValueError` из `build_subscriptions` —
  `log.warning(...)` и пропустить эту подписку (тот же паттерн graceful degradation, что и для
  нелитеральных значений сегодня). **Новое поведение**: это распространяется и на
  `cooldown < 0` в `queue=`/`queues=`-режиме, который сегодня листенер тихо принимает
- [x] **важно**: сегодняшняя extraction-логика несовместима с прямой передачей `**kwargs` в
  `build_subscriptions` в двух местах — нужно убрать оба переименования/специальные случаи,
  чтобы все извлечённые значения лежали в одном `kwargs` dict под литеральными именами
  параметров декоратора (`queue`, `queues`, `exchange`, `routing_keys`, `routing_key_ids`,
  `routing_key_status`, `conn_id`, `filter_data`, `cooldown`):
  - сейчас `queue=`/позиционный аргумент кладётся в `kwargs["queue_name"]`, а не
    `kwargs["queue"]` — переименовать обратно в `kwargs["queue"]`
  - сейчас `queues=` вообще не попадает в `kwargs` — сохраняется в отдельную переменную
    `queues_list`, потому что старая ветвящаяся логика ("если `queues_list` задан — вернуть
    список dict, иначе — один dict из `kwargs`") была реализована в этой функции напрямую.
    Эта ветвящаяся логика теперь живёт внутри `build_subscriptions` — здесь нужно просто
    положить значение `queues=` в `kwargs["queues"]`, как и остальные параметры
  - без этих двух правок `build_subscriptions(dag_id=dag_id, **kwargs)` либо упадёт с
    `TypeError` (неожидаемый kwarg `queue_name`), либо тихо проигнорирует `queues=`
  - дальше по пайплайну (`_extract_subscriptions_from_file`, `_sync_to_db`, `consumer.py`)
    везде используется именно `queue_name` — это поле появляется только в *возвращаемом* dict
    `build_subscriptions`, не во входных kwargs
- [x] в `_extract_subscriptions_from_file`, при сборке списка подписок одного DAG: накопить их
  в локальный список и вызвать `has_exchange_conflict` (Task 1) перед добавлением каждой новой
  exchange-подписки в этот список — при конфликте `log.warning(...)` и пропустить дубликат
  (см. Technical Details: stacking)
- [x] убрать отдельный пост-шаг "выставить `sub["queue_name"]` для exchange" — теперь
  `queue_name` приходит уже посчитанным из `build_subscriptions`
- [x] написать тесты для `_parse_rmq_trigger_decorator`: `exchange=` с литералом-списком
  `routing_key_ids=`, дефолтный и явный `routing_key_status` (строка и список), `exchange=` с
  литералом-списком `routing_keys=` напрямую, нелитеральный `routing_key_ids`/`routing_keys`/
  `routing_key_status` (например переменная) пропускается так же, как сегодня пропускаются
  прочие нелитеральные значения, отрицательный `cooldown` (включая `queue=`) пропускается с
  WARNING (новое поведение)
- [x] написать тесты для `_extract_subscriptions_from_file`: exchange-подписка получает
  правильный `queue_name`/`group_key`; два `@rmq_trigger(exchange=...)` на одной функции —
  второй пропускается с WARNING, первый остаётся в результате
- [x] прогнать тесты — должны проходить перед следующей задачей:
  `pytest tests/watcher/test_listener.py`

### Task 3: Слияние exchange-метаданных в `active_subs` (`_main` листенера)

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [x] в `_main()`, после `scanned = self._scan_subscriptions()`, собрать
  `exchange_meta = {(s["dag_id"], s["queue_name"], s.get("conn_id", "rmq_default")): {"exchange": s["exchange"], "routing_keys": s["routing_keys"]} for s in scanned if "exchange" in s}`
- [x] при сборке каждой записи `active_subs` из `get_enabled_subscriptions(session)` искать в
  `exchange_meta` по ключу `(sub.dag_id, sub.queue_name, sub.conn_id)` и примешивать
  `exchange`/`routing_keys` в dict, если найдено
- [x] написать тесты: строка БД, совпадающая со scanned exchange-подпиской, получает
  примешанные `exchange`/`routing_keys`; обычная строка `queue=` не затрагивается (поля
  `exchange` нет); UI-подписка никогда не получает exchange-метаданные (совпадения в
  `exchange_meta` нет, так как такая подписка не может прийти из декоратора)
- [x] прогнать тесты — должны проходить перед следующей задачей:
  `pytest tests/watcher/test_listener.py`

### Task 4: Клиент Management API

**Files:**
- Create: `airflow_provider_rmq/utils/management.py`
- Create: `tests/test_management_utils.py`
- Modify: `pyproject.toml` — добавить `httpx` в зависимости

- [x] добавить `httpx>=0.27` в основные зависимости `pyproject.toml`. Выбран `httpx`, а не
  `aiohttp`/`requests`: `get_current_bindings` вызывается из async-контекста
  `_provision_exchange_subs`, значит клиент должен быть async; `aiohttp` не входит в
  зависимости Airflow по умолчанию, а `requests` (sync) потребовал бы `run_in_executor` на
  каждый вызов — лишняя сложность без выгоды
- [x] `get_management_url(conn_info) -> str | None` — читает
  `conn_info.extra_dejson.get("management_url")`, `rstrip("/")`, `None` если не задан
- [x] `async get_current_bindings(client, management_url, vhost, queue, exchange, auth) -> set[str]`
  — `GET {management_url}/api/queues/{quote(vhost, safe="")}/{quote(queue, safe="")}/bindings`,
  вернуть множество значений `routing_key`, где `source == exchange`; ошибки HTTP/таймаута
  пропускать дальше (вызывающий код решает, как логировать/пропускать). `self._http_client`
  создаётся с явным `timeout=5.0` (секунд) — не дефолтный httpx-таймаут, чтобы зависший
  Management API не задерживал весь reconcile-цикл дольше ожидаемого
- [x] написать тесты с мокнутым `httpx.AsyncClient` (успех: фильтрация по `source`,
  несколько биндингов включая нерелевантные с `source=""` default exchange исключаются;
  HTTP-ошибка статуса вызывает исключение через `raise_for_status`)
- [x] написать тесты для `get_management_url` (задан, не задан, обрезка хвостового слэша)
- [x] прогнать тесты — должны проходить перед следующей задачей:
  `pytest tests/test_management_utils.py`

### Task 5: Provisioning exchange/очередей и bind-diff в `RMQConsumerManager`

**Архитектурная заметка:** orphan-detection для cooldown (`_check_orphaned_pending_queues`,
уже существует) и для exchange-режима (новое в этой задаче) — одна и та же форма: накопить
множество dag_id, которым инфраструктура когда-либо успешно провижинилась, и каждый цикл
сравнивать с активным множеством (warn once при выпадении, info once при восстановлении). Эта
задача выносит её в общий `OrphanTracker` и **рефакторит существующий cooldown-код**, а не
только добавляет копию для exchange — иначе план создаёт третью почти идентичную пару
state+методы на `RMQConsumerManager`. Найдено и решено на сессии
`/improve-codebase-architecture` 2026-06-22.

**Files:**
- Create: `airflow_provider_rmq/watcher/orphan_tracker.py`
- Create: `tests/watcher/test_orphan_tracker.py`
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`

- [x] в `orphan_tracker.py` добавить `class OrphanTracker` — чистая алгебра множеств, без
  зависимостей от aio_pika/httpx:
  - `mark_provisioned(dag_ids: set[str]) -> None` — добавляет в "когда-либо провижинено"
  - `diff(active_ids: set[str]) -> tuple[set[str], set[str]]` — возвращает
    `(newly_orphaned, restored)` относительно множества, накопленного `mark_provisioned`;
    сама не логирует — текст WARNING/INFO разный для pending queue и sub queue и остаётся у
    вызывающего кода
- [x] **рефактор существующего кода**: заменить `self._cooldown_dag_ids`/
  `self._orphaned_pending_dag_ids` на `self._cooldown_tracker = OrphanTracker()` в `__init__`;
  в `_provision_cooldown` заменить `self._cooldown_dag_ids.update(cooldown_dag_ids)` на
  `self._cooldown_tracker.mark_provisioned(cooldown_dag_ids)`; переписать
  `_check_orphaned_pending_queues` на вызов
  `self._cooldown_tracker.diff(active_cooldown_dag_ids)` и логирование `newly_orphaned`/
  `restored` тем же текстом, что и сегодня (поведение и существующие тесты не должны
  измениться — только реализация)
- [x] добавить на уровне модуля `_SUB_QUEUE_PREFIX = "rmq_watcher.sub."` и
  `_EXCHANGE_TTL_MS = 28800000` (тот же TTL 8ч, что и везде — используется для
  `.unrouted`/`.log` очередей **и** для `rmq_watcher.sub.{dag_id}`)
- [x] добавить `_ensure_exchange_infrastructure(channel, exchange)` — декларирует `{exchange}`
  (topic, `alternate-exchange={exchange}.unrouted`), `{exchange}.unrouted` (fanout) + его
  TTL-очередь, забинженную на него, и `{exchange}.log` (TTL-очередь, забинженная на
  `{exchange}` с `routing_key="#"`)
- [x] добавить `_ensure_sub_queue(channel, dag_id)` — декларирует `rmq_watcher.sub.{dag_id}`
  (durable, `x-message-ttl=_EXCHANGE_TTL_MS`), возвращает объект очереди. TTL здесь — страховка
  от бесконечного роста orphaned-очереди (см. ADR-0005), а не таймер-механизм: очередь активно
  потребляется живым консьюмером в штатной работе
- [x] добавить `_sync_bindings(queue, exchange, desired, current)` — биндит `desired - current`,
  анбиндит `current - desired`, логирует каждое изменение на INFO
- [x] добавить `RMQConsumerManager._provision_exchange_subs(exchange_subs)` — группирует
  подписки по `(conn_id, exchange)`; на каждую группу: вызвать
  `await self._get_or_create_connection(conn_id)` (тот же метод, что использует
  `_consume_subscription` и `_provision_cooldown` — не предполагать, что соединение уже
  существует, на момент вызова `_connections` может быть пустым словарём, так как этот шаг
  теперь выполняется первым в `reconcile()`), открыть setup-канал, вызвать
  `_ensure_exchange_infrastructure`, затем на каждую подписку получить `conn_info` через
  `await loop.run_in_executor(None, BaseHook.get_connection, conn_id)` (синхронный DB-вызов —
  обязательно через executor, как везде в этом файле, см. `_get_or_create_connection`;
  **не вызывать `BaseHook.get_connection` напрямую в корутине**), резолвнуть vhost +
  `management_url`, собрать `auth=(conn_info.login, conn_info.password)`;
  если `management_url` равен `None` — залогировать ERROR и пропустить bind-diff для этой
  подписки (очередь всё равно декларируется); иначе получить текущие биндинги через
  `get_current_bindings(self._http_client, management_url, vhost, queue_name, exchange, auth)`,
  посчитать желаемые из `sub["routing_keys"]`, вызвать `_sync_bindings`. Весь блок на группу
  (включая получение соединения и `_ensure_exchange_infrastructure`) оборачивается в
  try/except: отдельный except на `PRECONDITION_FAILED` (точный тип исключения уточнить по
  факту — `aio_pika`/`aiormq` поднимают канал-уровневую ошибку с кодом 406; проверить
  `aiormq.exceptions.ChannelPreconditionFailed` или аналогичный атрибут кода) с логом ERROR
  явно про конфликт свойств exchange, и общий except (как в `_provision_cooldown`) для всего
  остального (RMQ недоступен, не удалось установить соединение, Management API недоступен) —
  оба логируются на ERROR и не должны прерывать обработку остальных групп и не должны
  блокировать последующий запуск обычных `queue=`-консьюмеров в этом же вызове `reconcile()`.
  При успехе вызвать `self._exchange_tracker.mark_provisioned({sub["dag_id"] for sub in group})`
- [x] добавить `self._http_client: httpx.AsyncClient | None` в `__init__`; создавать его в
  `start()` (вместо no-op); закрывать в `stop()`
- [x] **не менять** `_subs_changed` — он не должен сравнивать `exchange`/`routing_keys`.
  Изменение routing key обновляет только биндинг (через bind-diff в
  `_provision_exchange_subs`); очередь (`queue_name`) и сам consumer-таск не меняются, значит
  перезапускать таск не нужно. Зафиксировать это явно тестом (см. тесты ниже) как осознанное
  поведение, не случайный недосмотр
- [x] в `reconcile()` посчитать `exchange_subs = [s for s in subscriptions if s.get("exchange")]`
  и вызвать `await self._provision_exchange_subs(exchange_subs)` **до** блока
  "отменить снятые / запустить новые подписки" (см. Technical Details: исправление порядка)
- [x] добавить `self._exchange_tracker = OrphanTracker()` в `__init__`; добавить
  `_check_orphaned_exchange_bindings(active_exchange_dag_ids)` — вызывает
  `self._exchange_tracker.diff(active_exchange_dag_ids)`, логирует `newly_orphaned` на WARNING
  с именем очереди `rmq_watcher.sub.{dag_id}` и подсказкой
  `rabbitmqadmin delete queue name=rmq_watcher.sub.{dag_id}`, `restored` — на INFO (то же
  построение сообщений, что и в рефакторенной `_check_orphaned_pending_queues`, но с другим
  текстом про биндинг/очередь). Биндинги физически не снимаются — метаданные подписки больше
  нет, поэтому "current - desired" для неё не считается ни в одном будущем цикле. Это
  намеренно — авто-анбинд рассматривался и был отклонён, потому что не отличает временный сбой
  AST-парсинга (DAG вернётся без потерь — TTL на очереди не даст ей расти бесконечно) от
  постоянного ухода DAG (переименование/удаление — тогда это ограниченная утечка до ручной
  уборки); см. ADR-0005
- [x] вызвать `self._check_orphaned_exchange_bindings(...)` безусловно в конце `reconcile()`,
  рядом с существующим вызовом для cooldown
- [x] написать тесты на `OrphanTracker` (в `test_orphan_tracker.py`): `mark_provisioned` +
  `diff` даёт `newly_orphaned` при выпадении dag_id, пустой результат на повторных циклах без
  изменений, `restored` при возврате dag_id в активное множество, независимость нескольких
  экземпляров друг от друга
- [x] написать тесты на `_ensure_exchange_infrastructure`/`_ensure_sub_queue` (правильные
  вызовы declare и аргументы — включая `x-message-ttl` на sub queue, мокнутый канал — по
  образцу существующих `test_declares_topic_exchange`/`test_declares_durable_queue_and_binds`/
  `test_idempotent_no_exception_on_second_call` в этом файле, написанных для
  `_ensure_fire_infrastructure`/`_ensure_pending_queue`)
- [x] написать тест на распознавание `PRECONDITION_FAILED` при конфликте свойств exchange:
  мокнутый `declare_exchange` кидает соответствующее исключение → лог ERROR с упоминанием
  конфликта свойств (не общий "transient error"), обработка остальных групп не прерывается
- [x] обновить существующие тесты `_check_orphaned_pending_queues` в `test_consumer.py` так,
  чтобы они проверяли поведение через рефакторенный код на `OrphanTracker` — ассерты на текст
  WARNING/INFO не меняются
- [x] написать тесты на `_check_orphaned_exchange_bindings` (warning при первом исчезновении
  dag_id, молчание на повторных циклах, info-лог при восстановлении подписки — зеркало тестов
  для cooldown)
- [x] написать тесты на `_sync_bindings` (биндит только недостающие ключи, анбиндит только
  устаревшие, no-op когда desired == current)
- [x] написать тест на `_subs_changed`: изменение `routing_keys`/`exchange` при неизменных
  `queue_name`/`dag_id`/`cooldown`/`filter_data`/`conn_id` не считается изменением (возвращает
  `False`, consumer-таск не перезапускается)
- [x] написать тесты на `_provision_exchange_subs`: happy path (declare + bind через мокнутый
  channel/queue, Management API мокнут на возврат текущего набора, `mark_provisioned` вызван с
  правильными dag_id), отсутствует `management_url` (bind-diff пропускается, очередь всё равно
  декларируется, исключение не всплывает), Management API кидает ошибку (логируется и
  пропускается, не влияет на другие группы), `BaseHook.get_connection` вызывается через
  `run_in_executor` (не блокирует event loop — проверить вызов мока executor'а, по аналогии с
  существующими тестами на `_get_or_create_connection`)
- [x] написать тест на порядок в `reconcile()`: exchange-provisioning ожидается (`await`)
  раньше, чем вызывается `asyncio.create_task` для новой exchange-подписки (проверить порядок
  вызовов на моке либо что мок declare-очереди был awaited раньше создания задачи)
- [x] прогнать полный набор — должен проходить перед следующей задачей:
  `pytest tests/watcher/test_orphan_tracker.py tests/watcher/test_consumer.py`

### Task 6: Refactor `RMQWatcherView` form parsing (опционально, не связано с exchange-режимом)

**Эта задача не нужна для работы exchange-mode triggers** — `views.py` этим планом не
затрагивается (см. "Принятый trade-off" в Technical Details → "Почему миграция БД не нужна":
exchange-режим не отображается в UI). Включена в план как попутный рефакторинг, найденный на
той же сессии
`/improve-codebase-architecture` 2026-06-22: `create()`, `edit()`, `edit_group()` в
`RMQWatcherView` дублируют разбор `cooldown` и `filter_data` из формы практически построчно.
Можно выполнить независимо от Task 1–5, в любом порядке, и **не обязательно до** Task N
(перенос плана в `docs/plans/completed/`) — exchange-mode triggers считается завершённым по
Task 1–5 + N-1 + N независимо от того, сделан Task 6 или нет; если на момент Task N он не
сделан, просто оставить его невыполненным чекбоксом в перемещённом файле плана либо вынести в
отдельный тикет.

**Files:**
- Create: `airflow_provider_rmq/watcher/subscription_form.py`
- Create: `tests/watcher/test_subscription_form.py`
- Modify: `airflow_provider_rmq/watcher/views.py`
- Modify: `tests/watcher/test_views.py`

- [x] в `subscription_form.py` добавить `parse_cooldown(raw: str) -> int` — `int(raw) if raw
  else 0`, кидает `ValueError` при `< 0` или не-числе; без зависимости от Flask
- [x] в `subscription_form.py` добавить `parse_filter_data(raw: str) -> dict` — `json.loads(raw)
  if raw else {}`, кидает `ValueError` при невалидном JSON; без зависимости от Flask
- [x] в `views.py`: `create()`, `edit()`, `edit_group()` заменяют свои инлайновые try/except
  блоки на вызов `parse_cooldown`/`parse_filter_data`, ловят `ValueError` и flash'ят свой
  текущий текст ошибки (текст самого исключения не показывается пользователю — не меняется);
  `group_key = dag_id if cooldown > 0 else None` остаётся инлайн в каждом хендлере (тривиально,
  разный источник `dag_id` между хендлерами — не тянет на отдельную функцию)
- [x] написать тесты на `parse_cooldown`/`parse_filter_data` в `test_subscription_form.py` —
  без Flask request context
- [x] прогнать существующие тесты `test_views.py` без изменений ассертов (flash-текст и
  поведение редиректов не меняются) — должны проходить:
  `pytest tests/watcher/test_subscription_form.py tests/watcher/test_views.py`

### Task N-1: Verify acceptance criteria

- [x] проверить: поведение режима `queue=`/`queues=` не изменилось, кроме одного принятого
  исключения — AST-парсер теперь тоже отклоняет `cooldown < 0` (см. Development Approach); все
  существующие тесты `queue=`/`queues=` проходят без изменения ассертов, кроме тех, что
  целенаправленно обновлены под это исключение в Task 2
- [x] проверить: путь exchange-декоратор → AST scan → строка БД → слитый `active_subs` →
  provisioned RMQ-инфраструктура → потребление → вызов `trigger_dag()` — пройти его вручную по
  изменённым функциям
- [x] проверить: в diff'е не появилось ни одной новой колонки в `RMQSubscription`/никакой
  Alembic-подобной миграции
- [x] проверить: повторный `@rmq_trigger(exchange=...)` на одном DAG кидает `ValueError` (не
  тихо перезаписывает метаданные)
- [x] проверить: `rmq_watcher.sub.{dag_id}` декларируется с `x-message-ttl=28800000`
- [x] проверить: `docs/adr/0004-routing-key-dual-api-shape.md` и
  `docs/adr/0005-exchange-orphan-no-auto-unbind.md` существуют и описывают принятые решения
- [x] проверить: `decorators.py` и `listener.py` оба вызывают `build_subscriptions`/
  `has_exchange_conflict` из `subscription_builder.py`, а не дублируют валидацию
- [x] проверить: `_check_orphaned_pending_queues` (cooldown) и `_check_orphaned_exchange_bindings`
  оба используют `OrphanTracker`, существующие тесты cooldown-орфанов проходят без изменения
  ассертов
- [x] прогнать полный набор тестов: `pytest`
- [x] прогнать линтеры: `ruff check .` и `mypy airflow_provider_rmq`

### Task N: Update documentation

- [x] обновить `readme.md` — добавить подраздел "Exchange-mode triggers" рядом с существующей
  документацией `@rmq_trigger` (около примера с cooldown), включая:
  - обе формы routing key (`routing_keys` и `routing_key_ids`×`routing_key_status`)
  - extra `management_url`
  - явный запрет stacking `exchange=` на одном DAG (один DAG — один exchange; для подписки на
    несколько exchange использовать несколько DAG либо `queue=`-режим с вручную созданными
    очередями) и заметку про права из Post-Completion ниже
  - **миграция с `queue=` на `exchange=`**: старая вручную созданная очередь не удаляется
    автоматически и остаётся без консьюмера — удалить вручную после перехода
  - **переименование DAG**: смена `dag_id` создаёт новую `rmq_watcher.sub.{new_dag_id}`;
    старая `rmq_watcher.sub.{old_dag_id}` становится orphaned (см. ADR-0005) — удалить вручную
  - **мониторинг**: в RabbitMQ Management UI — consumer count > 0 у `rmq_watcher.sub.{dag_id}`;
    в логах Airflow — WARNING про orphaned-очереди/биндинги, ERROR про пропущенный bind-diff
    (Management API недоступен) или про конфликт свойств exchange (`PRECONDITION_FAILED`)
  - **откат**: убрать `exchange=` из декоратора и задеплоить — `rmq_watcher.sub.{dag_id}`
    станет orphaned (WARNING в логах, TTL 8ч ограничивает рост, ручная уборка по подсказке из
    текста WARNING); сам `{exchange}`/`.unrouted`/`.log` не трогаются (могут использоваться
    другими DAG)
- [x] добавить запись в `CHANGELOG.md` под новой версией: **Added** параметры `exchange=`/
  `routing_keys=`/`routing_key_ids=`/`routing_key_status=` в `@rmq_trigger`; **Added**
  connection extra `management_url`; **Added** зависимость `httpx`
- [x] переместить этот план в `docs/plans/completed/` — deferred to orchestrator (handled by
  move-plan.sh after all phases complete)

## Post-Completion

**Расширение прав RMQ (выполняется в сопутствующем плане репозитория `rabbitmq`, не здесь):**
Пользователю Airflow в RMQ сейчас нужны `configure`/`write`/`read` на `rmq_watcher\..*`
(заведено фичей cooldown). Exchange-режим дополнительно требует:
- `configure` на `jetstat\.airflow(\.unrouted|\.log)?` (или эквивалентный паттерн под то имя,
  которое реально указано в `exchange=`) — чтобы декларировать exchange/AE-exchange/очереди
- `read` на `jetstat\.airflow(\.unrouted)?` — биндинг очереди *из* exchange требует read на
  exchange-источнике, а не только configure на очереди-назначении

Применяется через `rabbitmqctl set_permissions` на сервере RMQ — см.
`rabbitmq/docs/plans/20260619-jetstat-airflow-exchange-routing.md`.

**Ручная end-to-end проверка** (после реализации и деплоя обоих планов):
- добавить `@rmq_trigger(exchange="jetstat.airflow", routing_key_ids=["<реальный-id-из-jetstat>"])`
  в тестовый DAG, задеплоить, подождать следующий reconcile-цикл (или форсировать через
  Variable `rmq_watcher_reconcile_interval`), убедиться через RabbitMQ Management UI, что
  `rmq_watcher.sub.<dag_id>` существует и забинжена на `jetstat.airflow` с ожидаемыми ключами
- отправить настоящий (или реплеенный) Jetstat-вебхук с этим id/status на штатный эндпоинт
  `https://mb.realcombi.mgcom.ru/webhooks/jetstat` (Benthos зеркалирует каждый такой вебхук в
  `jetstat.airflow` — отдельный URL не нужен) и убедиться, что DAG run запустился с
  `conf["routing_key"] == "<id>.<status>"`
- удалить декоратор, задеплоить, убедиться что в логах появился WARNING про orphaned
  `rmq_watcher.sub.<dag_id>` (см. Task 5: `_check_orphaned_exchange_bindings`) — **биндинг и
  очередь НЕ снимаются автоматически** (см. ADR-0005): метаданные подписки исчезают из памяти
  листенера вместе с DAG-файлом, так что bind-diff больше не пересчитывается для этой очереди
  ни в одном будущем цикле. В отличие от orphaned pending queue (там нет ни одного сообщения,
  кроме самого таймера), эта очередь продолжает получать реальные сообщения из exchange — рост
  ограничен только TTL (`x-message-ttl=28800000`, 8ч), а не отсутствием трафика. Ручная очистка —
  по подсказке из текста WARNING (`rabbitmqadmin delete queue name=rmq_watcher.sub.<dag_id>`)
- убедиться, что немаршрутизируемый id/status попадает в `jetstat.airflow.unrouted`, а
  смаршрутизированное сообщение зеркалируется без изменений в `jetstat.airflow.log`
