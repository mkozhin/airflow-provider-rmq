# Publisher confirms в RMQPublishOperator и проверка extra в синхронном хуке

## Overview
- `RMQPublishOperator` публикует через `RMQHook.basic_publish` без publisher confirms и без
  `mandatory`. Если сообщение не маршрутизируется (опечатка в `queue_name`, очередь не создана,
  нет биндинга), брокер молча его выбрасывает. Отвергнутое брокером сообщение и публикация в
  несуществующий exchange тоже могут оставить задачу зелёной. Задача зелёная, сообщения нет.
- Синхронный хук читает `heartbeat` и `blocked_connection_timeout` из `extra` без проверки:
  строка `"30"` роняет задачу с `TypeError` из pika, хотя асинхронный путь то же значение читает.
- План закрывает пункты бэклога `publish-operator-no-confirms` и `hook-extras-not-validated`, а
  с ними задачи 2–4 из `docs/plans/20260703-reliability-hardening.md`: форма ответа
  `queue_info`, защищённый `__del__`, WARNING на пустую публикацию.
- Совместимость: текущие DAG'и и подключения не требуют правок, новые параметры можно не
  задавать. Поведение меняется только там, где сообщение сейчас теряется молча.

## Context (from discovery)
- `airflow_provider_rmq/hooks/rmq.py`: `_get_connection_params` (`:123`), `get_channel`
  (`:168`), `__del__` (`:192`), `queue_info` (`:357`, ветка отсутствующей очереди `:377`),
  `basic_publish` (`:486`).
- `airflow_provider_rmq/operators/rmq_publish.py`: `template_fields = ("exchange",
  "routing_key", "message")` (`:24`), `__init__` (`queue_name` переключает на default exchange,
  `:76-78`), `execute` (`:92`), `_normalize_messages`.
- `airflow_provider_rmq/utils/amqp.py`: `_read_number` (`:53`), `_read_heartbeat` (`:95`,
  умолчание жёстко `DEFAULT_HEARTBEAT = 30`), `HEARTBEAT_KEY`.
- pika 1.4 `BlockingChannel`: после `confirm_delivery()` вызов `basic_publish` ждёт `Basic.Ack`
  и бросает `UnroutableError` (Return, затем Ack, при `mandatory=True`) или `NackError`;
  закрытие канала брокером (404 на несуществующий exchange, 403 без прав) всплывает из того же
  вызова как `ChannelClosedByBroker`, после чего `channel.is_closed` истинно и `get_channel()`
  открывает новый канал. Без confirms `mandatory` синхронно ничего не сообщает, а закрытие
  канала всплывает только на следующем вызове. Повторный `confirm_delivery()` на том же канале
  pika пишет ERROR'ом. При возврате сообщения pika пишет WARNING
  (`blocking_connection.py:1447-1451`) с полными свойствами сообщения (заголовки,
  `correlation_id`, `reply_to`, `message_id`) и первыми 255 символами `repr` тела; логгер pika
  в хуке выставлен на WARNING, так что строка попадает в лог задачи.
- Тесты: `tests/test_hook.py` (`TestConnectionParams`, `TestPublishAndConsume`,
  `TestQueueInfoChannelClosed.test_queue_info_nonexistent` `:402-415`),
  `tests/test_publish_operator.py` (фикстура `mock_hook`, `TestConnectionId` `:178-192`,
  проверки конструктора хука на `:183` и `:192`),
  `tests/test_amqp_utils.py`, `integration/` (живой брокер; `BrokerScratch` создаёт и убирает
  только очереди, фикстуры `rmq_conn_id`, `pika_channel`, `scratch`, `rmq_prefix`).
- README: раздел `RMQHook` (таблицы параметров и методов, `readme.md:130-186`, `basic_publish`
  `:161`, `queue_info` `:156`), раздел Connection Timing (`readme.md:104-124`: «Three optional
  Extra keys», таблица без `blocked_connection_timeout` `:116-120`, абзац «the two paths differ
  only in default» `:122`, правило отката «not a positive number» `:124`), раздел
  `RMQPublishOperator` (`readme.md:188`); то же в `readme_ru.md`.

## Development Approach
- **testing approach**: Regular (code first, then tests)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change: `.venv/bin/pytest tests/ -q`
- maintain backward compatibility: no existing DAG or connection needs edits
- comments and docstrings describe the present behavior only, never what was there before

## Testing Strategy
- **unit tests**: required for every task; pika and the Airflow connection are mocked, as in
  the existing `tests/test_hook.py` and `tests/test_publish_operator.py`
- **integration tests**: one new file in `integration/` against a real broker; it is gated on
  `RMQ_TEST_URL` and is run manually (Post-Completion)
- no UI, no e2e

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix

## Solution Overview
- **Хук.** `RMQHook(..., publisher_confirms: bool = False)`, новый аргумент — последним. При
  `True` `get_channel()` вызывает `channel.confirm_delivery()` ровно один раз на каждый новый
  канал, так что переоткрытый канал тоже работает в режиме подтверждений.
  `basic_publish(..., mandatory: bool = False)` передаёт флаг в pika. Умолчания хука сохраняют
  поведение кода, работающего с хуком напрямую.
- **Оператор.** `RMQPublishOperator(..., confirm: bool = True, mandatory: bool | None = None)`.
  - `confirm=True` по умолчанию: хук создаётся с `publisher_confirms=confirm`.
  - `mandatory` хранится как задан. Значение `None` разрешается в `execute()`, после рендера
    шаблонов, по отрендеренному `self.exchange`: `True` для default exchange (`""`, в том числе
    через `queue_name=`), иначе `False`. При `confirm=False` `None` разрешается в `False`. Явно
    заданное значение имеет приоритет. Несуществующая очередь при публикации прямо в неё всегда
    означает потерю, поэтому рабочий DAG этого не делает; для именованного exchange «никто не
    подписан» бывает нормой.
  - `mandatory=True` при `confirm=False` — `ValueError` в `__init__` (ошибка импорта DAG'а):
    эти параметры не шаблонные, а без подтверждений возврат синхронно не виден.
  - При `confirm=True` исключения из `basic_publish` превращаются в `AirflowException` с
    номером сообщения, exchange, routing key и исходным исключением в `__cause__`:
    - `UnroutableError`: «message N of M to exchange=… routing_key=… was returned by the
      broker: 312 NO_ROUTE» (`reply_code`/`reply_text` из `exc.messages[0].method`);
    - `NackError`: «… was rejected by the broker (nack)»;
    - `ChannelClosedByBroker`: «… broker closed the channel: <reply_code> <reply_text>».
  - При `confirm=False` исключения не оборачиваются: без подтверждений отказ всплывает на
    следующем сообщении, и номер указал бы не на то сообщение.
  - Сообщения до упавшего уже приняты брокером, ретрай задачи опубликует их снова: публикация
    at-least-once, это пишется в README. Сам вызов публикации не повторяется, повторяется
    только подключение, как сейчас.
  - Пустой список или `message=None`: WARNING «nothing to publish», задача успешна.
- **extra.** Значение `null` (`None`) в `heartbeat` или `blocked_connection_timeout` передаётся в
  pika как есть: heartbeat по предложению брокера, без тайм-аута блокировки. Остальные значения
  читаются через `_read_heartbeat(extras, default=SYNC_DEFAULT_HEARTBEAT)` и
  `_read_number(extras, "blocked_connection_timeout", 300, float, 0)`. У `_read_heartbeat`
  появляется параметр `default` (умолчание `DEFAULT_HEARTBEAT`), асинхронный путь вызывает его
  без изменений. `SYNC_DEFAULT_HEARTBEAT = 600` — константа в `hooks/rmq.py` с причиной в одну
  строку: `BlockingConnection` обслуживает heartbeat только внутри вызовов pika, и при 30 с
  задача, которая между вызовами работает дольше примерно минуты, получает обрыв соединения.
- **Мелочи хука.** `queue_info` для отсутствующей очереди отвечает с `consumer_count: 0`, той же
  формой, что для существующей. `__del__` глотает любое исключение: при завершении
  интерпретатора `pika` может быть выгружен частично.

## Technical Details
- Ожидание подтверждения в pika не имеет своего тайм-аута (`_flush_output` без срока,
  `blocking_connection.py:2339`). Под resource alarm его обрывает `blocked_connection_timeout`
  (300 с), при мёртвом линке — heartbeat. Но брокер может задерживать подтверждение на живом и
  не заблокированном соединении: quorum-очередь подтверждает сообщение только после записи на
  большинство реплик, и без кворума ack не приходит, `Connection.Blocked` не шлётся, heartbeat
  идёт. Тогда задача ждёт до своего `execution_timeout`, а без него — бессрочно. Без confirms
  такое сообщение тоже не было бы сохранено, задача просто завершилась бы зелёной. Своя граница
  в коде не вводится; README советует задавать `execution_timeout` задачам публикации.
- Подтверждение на каждое сообщение — один round-trip, порядка миллисекунды в локальной сети;
  для persistent-сообщений и quorum-очередей дольше, так что большой батч публикуется заметно
  медленнее.
- Что значит `Basic.Ack`: брокер принял сообщение. Если сообщение не смаршрутизировалось, а
  `mandatory=False`, ack тоже приходит, и сообщение выброшено. Для persistent-сообщения
  (`delivery_mode=2`) ack означает запись на диск только в durable-очередях; в transient-очереди
  сообщение на диск не пишется.
- Номер сообщения в тексте исключения — с единицы, «message N of M».

## What Goes Where
- **Implementation Steps**: код хука и оператора, unit-тесты, integration-тест, README,
  CHANGELOG, бэклог.
- **Post-Completion**: прогон `integration/` на живом брокере 4.3+, проверка на Airflow 2.9.1.

## Implementation Steps

### Task 1: Проверка `heartbeat` и `blocked_connection_timeout` в синхронном хуке

**Files:**
- Modify: `airflow_provider_rmq/utils/amqp.py`
- Modify: `airflow_provider_rmq/hooks/rmq.py`
- Modify: `tests/test_amqp_utils.py`
- Modify: `tests/test_hook.py`

- [x] добавить в `_read_heartbeat` параметр `default: int = DEFAULT_HEARTBEAT`, использовать его во
  всех ветках отката; асинхронный вызов (`utils/amqp.py:251`) не менять
- [x] константа `SYNC_DEFAULT_HEARTBEAT = 600` в `hooks/rmq.py` с причиной в одну строку
- [x] в `_get_connection_params`: явный `null` передаётся в pika как `None`; иначе `heartbeat`
  через `_read_heartbeat(extras, default=SYNC_DEFAULT_HEARTBEAT)`, `blocked_connection_timeout`
  через `_read_number(extras, "blocked_connection_timeout", 300, float, 0)`
- [x] тесты `_read_heartbeat` с `default`: отсутствующий ключ, нечисло, значение меньше секунды
  дают переданное умолчание
- [x] тесты хука: `"30"` даёт `30`; `"abc"` и `-5` дают 600 с WARNING; `0` даёт 0 с WARNING;
  без ключа — 600; `null` даёт `None` для обоих ключей; `"120"` для
  `blocked_connection_timeout` даёт `120.0`, мусор — 300 с WARNING
- [x] прогнать тесты — должны пройти

### Task 2: Publisher confirms и `mandatory` в `RMQHook`

**Files:**
- Modify: `airflow_provider_rmq/hooks/rmq.py`
- Modify: `tests/test_hook.py`

- [x] параметр `publisher_confirms: bool = False` последним в `RMQHook.__init__`, с docstring
- [x] в `get_channel()` при `publisher_confirms` вызывать `confirm_delivery()` один раз на каждый
  новый канал
- [x] `basic_publish(..., mandatory: bool = False)` передаёт флаг в `channel.basic_publish`;
  docstring называет исключения pika, которые всплывают в режиме confirms
- [x] тесты: `confirm_delivery` вызывается при флаге и не вызывается без него; повторный
  `get_channel()` на открытом канале не вызывает его снова; вызывается снова после переоткрытия
  закрытого канала; `mandatory` доходит до pika; без аргумента в pika уходит `mandatory=False`
- [x] прогнать тесты — должны пройти

### Task 3: `confirm` и `mandatory` в `RMQPublishOperator`

**Files:**
- Modify: `airflow_provider_rmq/operators/rmq_publish.py`
- Modify: `tests/test_publish_operator.py`

- [x] параметры `confirm: bool = True` и `mandatory: bool | None = None` с docstring;
  `ValueError` в `__init__` на `mandatory=True` при `confirm=False`
- [x] `execute` разрешает `mandatory` по отрендеренному `self.exchange` (правило из Solution
  Overview), создаёт хук с `publisher_confirms=self.confirm`, передаёт `mandatory` в каждый
  `basic_publish`
- [x] при `confirm=True` перехват `UnroutableError`, `NackError`, `ChannelClosedByBroker` с
  `AirflowException`, текст по типу исключения (с `reply_code`/`reply_text`), исходное
  исключение в `__cause__`; при `confirm=False` исключения не оборачиваются
- [x] WARNING при пустом списке сообщений
- [x] тесты разрешения `mandatory` по kwarg, ушедшему в `hook.basic_publish`: `queue_name=` → True;
  `exchange=""` → True; именованный exchange → False; `exchange="{{ params.ex }}"`, отрендеренный
  в `""` (через `render_template_fields` или присвоение `op.exchange = ""` перед `execute`) → True;
  явные True/False имеют приоритет; `confirm=False` без `mandatory` → False;
  `mandatory=True, confirm=False` → `ValueError`
- [x] тесты `execute`: хук создаётся с `publisher_confirms=True` по умолчанию и с `False` при
  `confirm=False`; `UnroutableError` на 3-м из 5 сообщений даёт `AirflowException` с «message 3
  of 5», `312 NO_ROUTE` и два успешных вызова до него; `NackError` даёт текст про nack;
  `ChannelClosedByBroker(404, "NOT_FOUND …")` даёт текст с кодом и причиной; при
  `confirm=False` то же исключение пробрасывается как есть; `message=None` и `[]` пишут WARNING
  и ничего не публикуют
- [x] обновить проверки аргументов конструктора хука в `TestConnectionId`
  (`tests/test_publish_operator.py:178-192`, вызовы `assert_called_once_with` на `:183` и `:192`)
  под `publisher_confirms=`
- [x] прогнать тесты — должны пройти

### Task 4: `queue_info` и `__del__`

**Files:**
- Modify: `airflow_provider_rmq/hooks/rmq.py`
- Modify: `tests/test_hook.py`

- [x] `queue_info` для отсутствующей очереди возвращает `consumer_count: 0`; docstring описывает
  одну форму ответа для обеих веток
- [x] `__del__` оборачивает проверку и `close()` в `try/except Exception: pass`
- [x] обновить ожидаемый словарь в `test_queue_info_nonexistent` (`tests/test_hook.py:402-415`)
- [x] тесты: форма ответа `queue_info` одинакова для обеих веток; `__del__` не бросает, когда
  `is_open` или `close()` бросают
- [x] прогнать тесты — должны пройти

### Task 5: Integration-тест публикации против живого брокера

**Files:**
- Create: `integration/test_publish_confirms.py`

- [x] публикация оператором в существующую очередь (`scratch`) проходит, сообщение в очереди
- [x] `queue_name=` на несуществующую очередь (имя с `rmq_prefix`) падает с `AirflowException`
  и `312 NO_ROUTE`
- [x] встроенный `amq.direct` с ключом `rmq_prefix…` без биндинга: при умолчаниях проходит, с
  `mandatory=True` падает
- [x] публикация в несуществующий exchange (имя с `rmq_prefix`) падает с `AirflowException` и
  кодом 404
- [x] очередь `scratch` с `{"x-max-length": 0, "x-overflow": "reject-publish"}`: публикация в неё
  падает с текстом про nack
- [x] файл собирается только при заданном `RMQ_TEST_URL` (без него — skip), как остальные тесты
  каталога

### Task 6: Verify acceptance criteria
- [x] все требования из Overview реализованы
- [x] изменены ожидания только у `test_queue_info_nonexistent` и у проверок аргументов хука в
  тестах оператора; остальные существующие тесты проходят без правок
- [x] полный прогон: `.venv/bin/pytest tests/ -q`
- [x] `ruff check` и `mypy` по изменённым файлам не дают новых замечаний относительно `main`

### Task 7: [Final] Документация и бэклог
- [ ] `readme.md` и `readme_ru.md`, раздел `RMQHook`: `publisher_confirms` в таблице параметров,
  `basic_publish(..., mandatory)` и исключения режима confirms в таблице методов, форма ответа
  `queue_info`
- [ ] `readme.md` и `readme_ru.md`, раздел `RMQPublishOperator`:
  - параметры `confirm` и `mandatory`, правило умолчания `mandatory` (default exchange — через
    `queue_name=` или `exchange=""` — даёт `True`);
  - когда задача теперь падает: несмаршрутизированное сообщение при `mandatory`, nack (например
    очередь с `x-overflow=reject-publish`), закрытие канала брокером (404 на exchange, 403);
    тексты отказа;
  - at-least-once при ретрае батча;
  - подтверждение на каждое сообщение замедляет большой батч; ожидание ack не ограничено,
    поэтому задачам публикации стоит задавать `execution_timeout`;
  - что значит успешный confirm (см. Technical Details: ack приходит и на выброшенное сообщение
    при `mandatory=False`, persistent пишется на диск только в durable-очереди);
  - при возврате pika пишет в лог задачи WARNING с полными свойствами сообщения, включая
    заголовки, и первыми 255 символами `repr` тела;
  - пустой `message` даёт WARNING «nothing to publish», задача успешна
- [ ] `readme.md` и `readme_ru.md`, раздел Connection Timing:
  - вводная фраза («Three optional Extra keys») и таблица: добавить `blocked_connection_timeout`
    (умолчание 300, только `RMQHook`, секунды до обрыва соединения, заблокированного брокером);
  - переписать абзац `:122`: пути отличаются умолчанием heartbeat и обработкой `null`
    (синхронный хук передаёт `null` в pika как есть, асинхронный путь берёт умолчание с WARNING);
    причина умолчания 600 — `BlockingConnection` обслуживает heartbeat только внутри вызовов
    pika, та же формулировка, что в комментарии к `SYNC_DEFAULT_HEARTBEAT`;
  - правило отката `:124` привести к коду: строковые числа принимаются, нечисло и значение ниже
    минимума заменяются умолчанием с WARNING, `0` допустим для `heartbeat` и
    `blocked_connection_timeout`
- [ ] `CHANGELOG.md`, раздел `v2.7.0`, по одной строке на пункт, ссылки только относительные:
  - Changed: `RMQPublishOperator` ждёт подтверждения брокера (`confirm=True`), большой батч
    публикуется медленнее; `mandatory` по умолчанию для публикации в default exchange; задача
    падает при nack и закрытии канала брокером;
  - Added: `RMQHook(publisher_confirms=…)`, `basic_publish(mandatory=…)`, WARNING на пустую
    публикацию;
  - Fixed: строковые и неверные `heartbeat` и `blocked_connection_timeout` в `extra`, форма
    `queue_info`, `__del__` при завершении интерпретатора;
  - Upgrade notes: публикация в default exchange (`queue_name=…` или `exchange=""`) падает, если
    очереди нет; задачам публикации стоит задать `execution_timeout`
- [ ] `git rm docs/backlog/publish-operator-no-confirms.md docs/backlog/hook-extras-not-validated.md`
- [ ] перенести план в `docs/plans/completed/`
- [ ] `docs/backlog/reliability-hardening-plan-stale.md`: задачи 2–4 в таблице отмечены сделанными
  со ссылкой на `docs/plans/completed/20261115-publish-confirms-and-hook-extras.md` и
  актуальными строками; убрать ссылки на удалённые пункты; в «Направлении решения» задачу 5
  (ленивый `WatcherSession`) переформулировать без «туда же» — она остаётся открытой, кандидат на
  маленький план «мелочи хука и моделей»

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification:**
- прогнать `integration/` на брокере RabbitMQ 4.3+ (лучше 4.4) и обновить строку «Проверено на
  RabbitMQ …» в `integration/README.md`
- проверить на Airflow 2.9.1 (прод): импорт примеров DAG'ов из `docs/example_dags/` и публикация
  одного сообщения

**External system updates:**
- в продовых DAG'ах с `RMQPublishOperator(queue_name=…)` проверить, что очередь существует до
  публикации: после обновления опечатка или ещё не созданная очередь роняет задачу
