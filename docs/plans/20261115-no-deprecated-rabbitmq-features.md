# Провайдер не опирается на устаревшие функции RabbitMQ 4.x

## Overview
- Провайдер требует RabbitMQ 4.3+, а в 4.3 брокер по умолчанию запрещает две устаревшие функции,
  на которые опираются умолчания и параметры провайдера:
  - `transient_nonexcl_queues` — очередь, одновременно non-durable и non-exclusive. Declare такой
    очереди брокер отклоняет с `541 INTERNAL_ERROR`. У `RMQHook.queue_declare` и
    `RMQQueueManagementOperator` умолчания именно такие: `durable=False`, `exclusive=False`.
  - `global_qos` — per-channel prefetch. Хук передаёт `global_qos` из `qos` в `basic_qos`.
- Цель:
  - провайдер не использует ничего, что RabbitMQ объявил устаревшим;
  - умолчания работают на 4.3+ без включения устаревших функций на брокере;
  - запрещённое сочетание и неподдерживаемые параметры отклоняются до обращения к брокеру с
    понятной причиной.
- Попутно: `prefetch_size != 0` RabbitMQ не реализует (соединение закрывается с
  `540 NOT_IMPLEMENTED`), провайдер отклоняет его заранее.
- Совместимость:
  - на проде (vhost `realcom`, проверено через Management API) все 16 exchange'ей и 25 очередей
    durable, exclusive-очередей нет — смена умолчаний на проде ничего не меняет;
  - примеры в `docs/example_dags/` везде явно передают `durable=True`, `qos` —
    только `{"prefetch_count": 10}`;
  - `qos={"global_qos": True}` продолжает работать: флаг брокеру не передаётся, в лог —
    WARNING в задаче; prefetch действует на консьюмера;
  - на чужом брокере повторный declare ранее созданного временного exchange'а или очереди с
    новым умолчанием даёт 406 `PRECONDITION_FAILED` — строка в Upgrade notes.
- Факты с продового брокера (RabbitMQ 4.4.0, `GET /api/deprecated-features`):
  `transient_nonexcl_queues`, `global_qos`, `queue_master_locator`, `amqp_address_v1`,
  `amqp_filter_set_bug` — `denied_by_default`; `management_metrics_collection` —
  `permitted_by_default`; `classic_queue_mirroring`, `ram_node_type` — `removed`.
  `GET /api/deprecated-features/used` — пусто (этот эндпоинт охватывает весь узел и не
  отслеживает использование `global_qos`).
- Вне объёма плана: проверка живости watcher'а через `GET /api/consumers/{vhost}` опирается на
  `management_metrics_collection`. Это фиксируется пунктом бэклога (задача 6), переделка —
  отдельным планом.

## Context (from discovery)
- `airflow_provider_rmq/hooks/rmq.py`: `RMQHook.__init__` (параметр `qos`, docstring `:138`),
  `get_channel` (`basic_qos` с `prefetch_size` и `global_qos`, `:239-244`), `queue_declare`
  (`:325-359`, `durable=False`, `exclusive=False`, `passive=False`), `exchange_declare`
  (`durable=False`, `:478`).
- `airflow_provider_rmq/operators/rmq_management.py`: `template_fields` (`:31`, без `durable`,
  `exclusive`), `RMQQueueManagementOperator.__init__` (`durable: bool = False` `:40`,
  `exclusive` `:41`; `durable` применяется к `declare_queue` `:120-126` и к `declare_exchange`
  `:158-166`), `ValueError` на неверный `action` в `__init__` (`:86-87`) — образец проверки при
  создании оператора. `execute` открывает `RMQHook` контекстным менеджером (`:103`) и закрывает
  соединение по завершении.
- `airflow_provider_rmq/operators/rmq_consume.py`: `template_fields = ("queue_name",)` (`:28`),
  параметр `qos` (`:39`, docstring `:55`), проверка `ack_unmatched` в `__init__` (`:72-73`),
  передача `qos` в `RMQHook(qos=...)` в `execute` (`:86`).
- Сенсор строит `RMQHook` без `qos`; триггер объявляет только пассивно; watcher объявляет только
  durable-очереди и `basic_qos` не вызывает — изменений не требуют.
- Тесты: `tests/test_hook.py` (существующий declare-тест на `:407` передаёт `durable=True` явно),
  `tests/test_management_operator.py` (`TestValidation`, `test_declare_queue` `:51-52` и
  `test_declare_exchange` `:114-115` передают `durable=True` явно), `tests/test_consume_operator.py`
  (`:202-212`, `qos`). Integration: `integration/conftest.py` (`scratch.name()`,
  `scratch.register_queue()` — имя вне префикса отвергается; очереди durable по умолчанию),
  `integration/README.md`.
- README: `readme.md:149` (`qos` у `RMQHook`), `:159` (`queue_declare`), `:165`
  (`exchange_declare`), `:270-285` (`RMQConsumeOperator`, `qos`), `:364-430`
  (`RMQQueueManagementOperator`, таблица параметров, `durable` `False` на `:377`); то же в
  `readme_ru.md`. `CHANGELOG.md`, раздел `## v2.7.0` (не выпущен).

## Development Approach
- **testing approach**: Regular (code first, then tests)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change: `.venv/bin/pytest tests/ -q`; ruff/mypy on changed files must
  not add findings relative to `main`
- comments, docstrings, README and CHANGELOG describe the present behaviour only

## Testing Strategy
- **unit tests**: required for every task; pika and the Airflow connection are mocked as in the
  existing tests
- **integration tests**: `integration/` runs against the live broker (`.env`, vhost `test`):
  `.venv/bin/pytest integration/ -q`. It must pass at the end of the plan
- no UI, no e2e

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix

## Solution Overview
- **Умолчание `durable=True` везде:** `RMQHook.queue_declare`, `RMQHook.exchange_declare`,
  `RMQQueueManagementOperator` (параметр действует и на очередь, и на exchange). Временный
  exchange не устарел; умолчание `exchange_declare` меняется ради единообразия обоих declare
  хука и оператора — durable как норма. На проде временных exchange'ей нет; для чужих брокеров
  406 при повторном declare описан в Upgrade notes.
- **Отказ от временной non-exclusive очереди.** Активный declare (не `passive`) очереди с
  `durable=False` и `exclusive=False` — `ValueError`:
  - в хуке — в `queue_declare` до обращения к каналу; текст называет очередь, устаревшую функцию
    `transient_nonexcl_queues` (запрещена по умолчанию с RabbitMQ 4.3) и оба исправления:
    `durable=True` или `exclusive=True`;
  - в операторе — в `__init__` при `action="declare_queue"`; `durable` и `exclusive` не шаблонные,
    поэтому значения уже окончательные. Текст советует только `durable=True`: оператор закрывает
    соединение по завершении задачи, и exclusive-очередь живёт лишь пока задача выполняется.
    Ошибка возникает при создании оператора: для обычной задачи это импорт DAG'а, для mapped-задачи
    (`.partial(...).expand(...)`) — её выполнение; хук в этом случае ловит то же сочетание;
  - `queue_name` в операторе — шаблонное поле, в тексте ошибки он может быть нерендеренным;
  - пассивный declare проверку не проходит: он не создаёт очередь; `auto_delete` на правило не
    влияет.
- **QoS.** Одна функция проверки `qos` в `hooks/rmq.py`, её вызывают `RMQHook.__init__` и
  `RMQConsumeOperator.__init__`; она только отклоняет недопустимое и ничего не пишет в лог:
  - `prefetch_size` не равный `0` — `ValueError`: RabbitMQ его не реализует и закрывает соединение
    с `540 NOT_IMPLEMENTED`; источник — справочник AMQP 0-9-1 на rabbitmq.com
    (`basic.qos`, `prefetch-size`), ссылку проверить и указать в docstring;
  - `global_qos` с истинным значением (любым, не только `True`) брокеру не передаётся. WARNING
    пишет только `RMQHook.__init__` (то есть в задаче, при создании хука), а не оператор: его
    `__init__` выполняется при каждом парсинге DAG'а. Текст WARNING нейтральный: устаревшая
    функция RabbitMQ `global_qos`, запрещённая по умолчанию с 4.3, флаг не передаётся; задача
    работает дальше;
  - `get_channel` вызывает `basic_qos(prefetch_count=...)` без `prefetch_size` и `global_qos`
    (умолчания pika — `0` и `False`);
  - `None` и пустой словарь допустимы; неизвестные ключи не проверяются, как сейчас.
- **Документация.** README и CHANGELOG `v2.7.0`:
  - Changed — умолчание `durable=True` для очередей и exchange'ей; `global_qos` брокеру не
    передаётся (WARNING);
  - Upgrade notes — `ValueError` на `durable=False` без `exclusive=True` у очереди;
    `ValueError` на `prefetch_size != 0`; 406 `PRECONDITION_FAILED` при повторном declare ранее
    созданного временного exchange'а или очереди без явного `durable=False`.

## Technical Details
- Проверка в хуке: `if not passive and not durable and not exclusive: raise ValueError(...)`.
- Истинность `global_qos` — `bool(value)`; тесты покрывают `True`, `1`, `"true"`, `False`,
  отсутствие ключа.
- WARNING про `global_qos` пишется один раз на создание хука; оператор хранит `qos` как передан.
- Durability существующей очереди по AMQP проверяется активным повторным declare с
  `durable=True`: он проходит, только если очередь durable, иначе брокер отвечает 406
  `PRECONDITION_FAILED`. Пассивный declare durability не показывает.

## What Goes Where
- **Implementation Steps**: код хука и операторов, unit- и integration-тесты, README, CHANGELOG,
  бэклог.
- **Post-Completion**: проверка на Airflow 2.9.1, проверка продовых DAG'ов до выкатки.

## Implementation Steps

### Task 1: `RMQHook`: умолчание `durable=True`, отказ от временной non-exclusive очереди

**Files:**
- Modify: `airflow_provider_rmq/hooks/rmq.py`
- Modify: `tests/test_hook.py`

- [x] умолчание `durable=True` в `queue_declare` и `exchange_declare`; docstring'и описывают правило
- [x] `ValueError` в `queue_declare` до обращения к каналу при `passive=False`, `durable=False`,
  `exclusive=False`; текст по Solution Overview
- [x] тесты: `queue_declare` и `exchange_declare` с умолчаниями уходят в pika с `durable=True`;
  `durable=False, exclusive=True` проходит; `durable=False, exclusive=False` даёт `ValueError` и
  не трогает канал; `passive=True, durable=False` проходит; текст ошибки называет очередь и оба
  исправления
- [x] существующий тест `:407` передаёт `durable=True` явно и остаётся без изменений
- [x] прогнать тесты — должны пройти

### Task 2: `RMQQueueManagementOperator`: умолчание `durable=True` и отказ при создании оператора

**Files:**
- Modify: `airflow_provider_rmq/operators/rmq_management.py`
- Modify: `tests/test_management_operator.py`

- [x] умолчание `durable=True`; docstring параметра говорит, что он действует на очередь и exchange
- [x] `ValueError` в `__init__` при `action="declare_queue"`, `durable=False`, `exclusive=False`;
  текст советует `durable=True` и объясняет, почему exclusive-очередь оператору не подходит
- [x] тесты: `declare_queue` и `declare_exchange` с умолчаниями передают `durable=True`;
  запрещённое сочетание даёт `ValueError` в `__init__`; то же сочетание с `declare_exchange` и
  `delete_queue` проходит; `exclusive=True, durable=False` проходит; шаблонный `queue_name`
  (`"{{ params.q }}"`) не мешает проверке
- [x] существующие `test_declare_queue` / `test_declare_exchange` передают `durable=True` явно и
  остаются без изменений; умолчание и проверка меняются в одной задаче, иначе
  `TestValidation.test_all_valid_actions_accepted` упадёт
- [x] прогнать тесты — должны пройти

### Task 3: `qos`: без `global_qos`, отказ от `prefetch_size`

**Files:**
- Modify: `airflow_provider_rmq/hooks/rmq.py`
- Modify: `airflow_provider_rmq/operators/rmq_consume.py`
- Modify: `tests/test_hook.py`
- Modify: `tests/test_consume_operator.py`

- [x] функция разбора `qos` в `hooks/rmq.py`; `RMQHook.__init__` и `RMQConsumeOperator.__init__`
  её вызывают
- [x] `prefetch_size != 0` — `ValueError` со ссылкой на источник (в хуке и в операторе);
  истинный `global_qos` брокеру не передаётся, WARNING — только в `RMQHook.__init__`
- [x] `get_channel` вызывает `basic_qos` только с `prefetch_count`
- [x] docstring'и `qos` у хука и оператора перечисляют только `prefetch_count`
- [x] тесты: WARNING на `global_qos` `True`, `1`, `"true"` при создании хука, и `basic_qos` без
  `global_qos`; создание оператора с `global_qos=True` WARNING не пишет; `global_qos=False` и
  отсутствие ключа — без WARNING; `prefetch_size` `1` — `ValueError` в хуке и операторе;
  `prefetch_size=0` проходит; `qos=None` и `{}` проходят
- [x] прогнать тесты — должны пройти

### Task 4: Integration-тест объявлений и QoS на живом брокере

**Files:**
- Create: `integration/test_queue_declare.py`

- [x] `RMQQueueManagementOperator(action="declare_queue")` с умолчаниями на имени из
  `scratch.name(...)`, зарегистрированном через `scratch.register_queue(...)`: очередь создана
  (пассивный declare) и durable (активный повторный declare с `durable=True` проходит — см.
  Technical Details)
- [x] `RMQHook.queue_declare(name, exclusive=True)` с умолчанием `durable` — дважды на одном хуке:
  второй declare проходит (эквивалентность при повторном объявлении); имя из `scratch.name(...)`,
  регистрация для teardown
- [x] `RMQHook.queue_declare(name, durable=False, exclusive=True)` проходит
- [x] `RMQHook(qos={"prefetch_count": 5, "global_qos": True})` открывает канал и публикует/читает
  без ошибки
- [x] набор работает только по AMQP (`RMQ_TEST_URL`); Management API в тестах не используется
- [x] прогнать `.venv/bin/pytest integration/ -q` — должно пройти

### Task 5: Verify acceptance criteria
- [x] все требования из Overview реализованы
- [x] `.venv/bin/pytest tests/ -q` и `.venv/bin/pytest integration/ -q` проходят
- [x] `ruff check` и `mypy` по изменённым файлам не дают новых замечаний относительно `main`
- [x] grep по `airflow_provider_rmq/` и `docs/example_dags/`: `global_qos` брокеру нигде не
  передаётся, non-durable non-exclusive declare нет

### Task 6: [Final] Документация, CHANGELOG и бэклог
- [ ] `readme.md` и `readme_ru.md`:
  - `RMQHook`: `qos` — только `prefetch_count`, что делают `global_qos` и `prefetch_size`;
    `queue_declare` и `exchange_declare` — умолчание `durable=True`, правило для очередей;
  - `RMQConsumeOperator`: `qos`;
  - `RMQQueueManagementOperator`: умолчание `durable=True` для очереди и exchange'а, правило,
    ошибка при создании оператора;
  - фразу `:237` про transient queue не трогать: exclusive-временные очереди остаются допустимыми
- [ ] `CHANGELOG.md`, `v2.7.0`, по одной строке на пункт, ссылки только относительные — по
  Solution Overview
- [ ] `integration/README.md` (абзац про состав набора, `:80-84`) и
  `docs/backlog/no-broker-integration-tests.md` (раздел «Что есть»): описать третий файл
  `test_queue_declare.py` — что он покрывает (умолчания durable у оператора и хука,
  exclusive-очередь с повторным declare, QoS с `global_qos`); строка о проверенной версии брокера —
  та, на которой прогнан набор
- [ ] пересчитать ссылки на строки, которые сдвинули задачи 1 и 3, во всех пунктах `docs/backlog/`:
  `grep -rn 'hooks/rmq.py:\|rmq_consume.py:' docs/backlog/` — каждая ссылка должна указывать на
  ту же конструкцию, что до правок (в том числе `sync-decode-raises-on-binary-body`,
  `operator-sees-only-the-head-of-the-queue`, `error-verdict-redelivers-until-ttl`,
  `reliability-hardening-plan-stale`)
- [ ] пункт бэклога `docs/backlog/watcher-liveness-relies-on-management-metrics.md` с front matter
  `worth: later`, `where: airflow_provider_rmq/watcher/consumer.py:<строка _probe_consumers>`,
  `added: 2026-11-15`:
  - проверка живости watcher'а (`_probe_consumers`, `GET /api/consumers/{vhost}`) опирается на
    устаревший `management_metrics_collection` (на продовом 4.4.0 — `permitted_by_default`);
  - документация RabbitMQ не говорит, что отвечает `/api/consumers` при отключённом сборе метрик;
    ожидаемое следствие — пустой список и ложный вердикт «консьюмеров нет»;
  - что переводит пункт в работу: перевод функции в `denied_by_default` или отключение метрик на
    проде;
  - направления: опора на AMQP-проверку, обнаружение отключённой статистики
- [ ] перенести план в `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Обязательно до выкатки:**
- поискать в продовых DAG'ах явные `durable=False` без `exclusive=True` и `prefetch_size`, отличный
  от `0`: после обновления они дают ошибку при создании оператора

**Manual verification:**
- проверить на Airflow 2.9.1 (прод) импорт примеров DAG'ов из `docs/example_dags/`
- после прогона `integration/`: через Management API убедиться, что в vhost `test` не осталось
  очередей, и что `GET /api/deprecated-features/used` пуст. Использование `global_qos` этот
  эндпоинт не отслеживает (по документации RabbitMQ), поэтому для него доказательство — unit-тест
  на `basic_qos` без `global_qos`

**External system updates:**
- нет
