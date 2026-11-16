# Таймауты `RMQTrigger` и параметр `trigger_mode` у `RMQSensor`

## Overview
- `RMQTrigger.run` (`airflow_provider_rmq/triggers/rmq.py:149-205`) подключается к брокеру и работает
  с очередью вызовами без границы по времени: `connect_robust` (`:166`), `connection.channel()`
  (`:169`), `declare_queue(passive=True)` (`:170`), `queue.get` (`:185`, pull) и `basic.consume`
  внутри `queue.iterator()` (`:134`, push). Если один из них не возвращается, задача висит в
  `deferred` без записи в лог задачи и без ошибки. Лечится только перезапуском triggerer'а, общего для
  всех отложенных задач.
- Реальные сценарии зависания:
  - TCP принят, AMQP-handshake молчит. На Python 3.10 (прод: `apache/airflow:2.9.1-python3.10`)
    фабрика `RobustConnection` повторяет такой handshake бесконечно, а `connect_robust` ждёт;
  - resource alarm во время подключения: `aio_pika` (и `RobustConnection`, и `Connection`) ждёт
    `connection.ready()` — снятия блокировки — вне таймаута подключения (`aio_pika/abc.py:715-719`,
    `:744`; ADR-0007);
  - канал или RPC не отвечают (`channel.open`, `queue.declare`, `basic.get`, `basic.consume`);
  - брокер отменил консьюмера push-режима (удалили очередь, сменился лидер quorum-очереди,
    перезапустился узел): итератор `aio_pika` ждёт вечно.
- Почему триггер уходит с `aio_pika` на `aiormq`. Robust-слой `aio_pika` сам переподключается, сам
  восстанавливает каналы (в т.ч. закрытые нами, вместе с их консьюмерами), повторяет ошибки
  аутентификации и 404 внутри своей фабрики, не показывая их вызывающему, и держит недоступное нам
  aiormq-соединение, пока идёт подключение. Повторять поверх него внутри триггера без утечек и без
  невидимых ошибок нельзя. Триггер владеет соединением сам:
  - каждая попытка — свой объект `aiormq.Connection`, созданный до первого `await`, и свой канал;
  - каждый шаг ограничен по времени; объект попытки закрывается при любом выходе из неё;
  - доставки оборачиваются в `aio_pika.IncomingMessage`, поэтому `match_and_ack`, фильтры и формат
    события не меняются;
  - `aiormq.connect()` не ждёт снятия alarm: триггер только читает, а брокер блокирует публикующие
    соединения.
- Решение (вариант A, выбран в обсуждении):
  - шаг, не уложившийся в границу, потеря соединения или канала и отмена консьюмера брокером дают
    WARNING с причиной, закрытие соединения попытки, паузу 15 → 30 → 60 → 60… с и новую попытку внутри
    того же `run()`;
  - задача остаётся `deferred` и продолжает сама, когда брокер ожил;
  - error-событие, как сейчас: неверный пароль (403 при подключении), нет доступа к vhost, 404 и 403
    на канале — на любой попытке (кроме 404 «home node … down» при перезапуске узла кластера — это
    повтор); отказ в подключении и обрыв handshake — на первой попытке; прочие
    исключения.
- Отличия от сегодняшнего поведения, все в сторону «ждать, а не падать»:
  - разрыв посреди pull-режима и отказ в подключении на второй и следующих попытках — новая попытка
    вместо ошибки;
  - в push-режиме переподключение делает сам триггер с WARNING, а не фабрика `RobustConnection`.
- Остаточное ограничение библиотеки: если брокер принял TCP и молчит на первом кадре handshake,
  отмена `aiormq.Connection.connect()` оставляет сокет открытым (`aiormq/connection.py:556-562`
  перехватывает только `EOFError`). С паузой 15 → 60 с это не больше одного сокета в минуту, пока длится
  такая ситуация; сокет закрывается, когда его закроет другая сторона. Фиксируется пунктом бэклога.
- Верхняя граница всего ожидания: `RMQSensor._defer` (`sensors/rmq.py:124-140`) передаёт в
  `self.defer(...)` собственный `timeout` сенсора. Сейчас он не передаётся, и в `deferrable=True`
  `timeout` сенсора не действует. Когда граница срабатывает, scheduler завершает задачу
  `TaskDeferralError("Trigger/execution timeout")`: FAILED с ретраями DAG'а (каждый ретрай снова
  ждёт полный `timeout`), при `soft_fail=True` — SKIPPED; `execute_complete` не вызывается. Если
  `message_wait_timeout` больше `timeout`, задача падает по `timeout`, а не пропускается.
- Тот же стык сенсор → триггер: `RMQSensor.mode` (`sensors/rmq.py:56`, `:91`) затирает `mode` базового
  `BaseSensorOperator` (`poke`/`reschedule`). Параметр с режимом `pull`/`push` переименовывается в
  `trigger_mode`; `mode` возвращается базовому классу. Имя `delivery_mode` не берётся: это свойство
  AMQP-сообщения (`delivery_mode=2`) в `RMQPublishOperator`.
- Совместимость:
  - текущие DAG'и работают без правок;
  - `mode="pull"`/`mode="push"` у `RMQSensor` продолжают работать с
    `AirflowProviderDeprecationWarning`; явный `mode=None` считается незаданным;
  - любое другое значение `mode`, кроме `poke`/`reschedule`/`pull`/`push`, теперь отвергает базовый
    класс при импорте DAG'а (сегодня принимается молча) — строка в CHANGELOG;
  - `RMQTrigger` сохраняет имя `mode` в `__init__` и `serialize()` — триггеры, уже лежащие в метабазе,
    восстанавливаются после обновления;
  - success-событие несёт те же поля (`body`, `headers`, `routing_key`, `exchange`);
  - прод: `RMQSensor` использует только `donstroy_price_watcher` (`deferrable=True`, `mode="push"`,
    `message_wait_timeout` до `end_hour`, `timeout` не задан → 7 дней по умолчанию Airflow).
    После обновления — предупреждение об устаревании; `timeout` в `defer()` его не задевает.
- Закрывает пункты бэклога `rmq-trigger-has-no-amqp-timeouts` и `sensor-mode-shadows-base-parameter`.

## Context (from discovery)
- `airflow_provider_rmq/triggers/rmq.py` (205 строк): `RMQTrigger.__init__` (`mode`,
  `message_wait_timeout`, `poll_interval`), `serialize()` (`:79-91`), `_handle_message` (ACK/NACK через
  `match_and_ack`, payload из `message.body` / `headers` / `routing_key` / `exchange`), `_run_push`
  (`:125-147`), `run` (`:149-205`, общий `except Exception` → error-событие).
- `airflow_provider_rmq/sensors/rmq.py`: параметр `mode` (`:56`), `self.mode = mode` после
  `super().__init__()` (`:84`, `:91`), `_miss_state` (`:99`), проверка `message_wait_timeout` только при
  `push` (`:111`), `_defer` (`:124-140`), `execute_complete` (`timeout` → `AirflowSkipException`, иначе
  `RuntimeError`), `poke` (`:151-201`, состояние промахов `:157-161`).
- `airflow_provider_rmq/watcher/consumer.py`: `_CLOSE_TIMEOUT` (`:72`), `_close_quietly` (`:640`).
  Упоминания: 8 `patch("airflow_provider_rmq.watcher.consumer._CLOSE_TIMEOUT", …)` в
  `tests/watcher/test_consumer.py` (`:436, 4341, 5764, 6884, 6963, 8776, 9853, 10084`), `consumer.py:80`,
  `:641`, `:667`, `:1353` (`:data:`_CLOSE_TIMEOUT``), `docs/adr/0007-connection-liveness-two-tier-check.md:228`,
  `:479`. Остальные помощники watcher'а (`_new_connection`, `_await_connected`, `_attached`,
  `_cancelled_by_broker`) обслуживают robust-пул watcher'а и остаются на месте.
- `airflow_provider_rmq/utils/amqp.py`: комментарий `DEFAULT_HEARTBEAT` (`:19-22`, «connect_robust
  reconnects»), `AmqpTimeouts` (`:44-52`, docstring `connect` «for connect_robust»), `get_amqp_timeouts`
  → `AmqpTimeouts(connect, rpc)` из `extra` (`connect_timeout`, по умолчанию 15 с; `rpc_timeout`, 30 с),
  `call_with_timeout` (`:187`, бросает вызов без ожидания отмены), `build_amqp_connection` (`:265`,
  URL с `heartbeat` в query; docstring говорит об `aio_pika` и `connect_robust`), `match`,
  `match_and_ack` (`:529`), `FilterMissState` (`:328`, `on_batch_done` `:434`, docstring `:441-444`
  «не чаще раза за прогон»).
- `airflow_provider_rmq/utils/backoff.py`: `next_backoff(current, maximum, minimum)` — удвоение паузы.
- `aiormq` 6.9.4 (зависимость `aio-pika`: `aiormq>=6.8,<7`; в `pyproject.toml` напрямую не указан):
  - `Connection(url, *, context=ssl_context)` — объект без I/O; `heartbeat` берётся из query URL;
  - `Connection.connect()` (`connection.py:532-623`): TCP, handshake; отмена после первого кадра
    закрывает writer и соединение (`except BaseException`), на первом кадре — нет; `ready()` не
    вызывается;
  - `Connection.close()` / `Base.close()` подавляет исключения и безопасна на неподключённом объекте
    (`base.py:123-141`);
  - `Connection.channel(publisher_confirms=..., timeout=)` (`:912`), `Channel.queue_declare(passive=)`,
    `basic_get(queue, no_ack=False, timeout=)` (`channel.py:473`, пустая очередь → доставка с
    `Basic.GetEmpty`), `basic_consume(queue, callback, consumer_tag=, timeout=)` (`:504`),
    `channel.consumers` — публичная карта тегов; `Basic.Cancel` от брокера убирает тег
    (`channel.py:392-397`); `channel.closing` / `connection.closing` — future закрытия с причиной;
  - иерархия: `AuthenticationError`, `ProbableAuthenticationError`, `IncompatibleProtocolError`,
    `ConnectionClosed`, `ConnectionNotAllowed` ⊂ `AMQPConnectionError` ⊂ `ConnectionError`;
    `ChannelAccessRefused` (403), `ChannelNotFoundEntity` (404) ⊂ `ChannelClosed`;
    `pamqp.exceptions.AMQPInternalError` — вне этой иерархии;
  - handshake: `Connection.Close` 403 на `StartOk` → `ProbableAuthenticationError`, другой код →
    `ConnectionClosed` (`connection.py:524-528`); `Connection.Close` на `Connection.Open` (530, нет
    доступа к vhost) → `AMQPInternalError` (`:521-523`); `AuthenticationError` — только когда нет
    общего механизма SASL (`:493-504`);
  - потеря по heartbeat: `__heartbeat` отменяет `_reader_task` (`connection.py:770-790`), соединение
    закрывается с причиной `CancelledError`, которая ставится и во все `channel.closing`
    (`base.py:48-64`).
- `aio_pika.IncomingMessage(delivered)` (`aio_pika/message.py:361`) строится из `DeliveredMessage`
  `basic_get` и `basic_consume`; `ack()`/`nack()` идут через `delivered.channel`.
- Тесты: `tests/test_trigger.py` (24 мока `connect_robust`, в т.ч. `:109`, `:269`, проверки kwargs
  `:300-339`), `tests/test_sensor.py` (`mode="push"` `:320-409`, текст ошибки `:354`,
  `test_the_base_sensor_mode_is_untouched` `:520-528`, `test_reschedule_never_reaches_the_threshold`
  `:549` с docstring'ом `:550-553` и ссылкой на пункт бэклога `:552`,
  `test_a_hundred_pokes_report_no_batch_line` `:569` с docstring'ом `:570-574` «`on_batch_done` is the
  operator's»), `tests/test_amqp_utils.py`, `tests/watcher/test_consumer.py`,
  `test_example_dags_dagbag`. Integration: `integration/conftest.py` (`rmq_conn_id` публикует
  подключение через `AIRFLOW_CONN_*`, `scratch` с префиксом, vhost `test`; docstring `aio_connection`
  `:309` «the path the watcher and the trigger take»), `integration/README.md`.
- Документация: `readme.md` / `readme_ru.md` — таблица ключей `extra` (`:117-122`: у
  `connect_timeout`/`rpc_timeout` «Used by» называет только watcher; строка `heartbeat` говорит о
  `connect_robust`), таблица параметров сенсора (`:456-457`), таблица pull/push (`:474`), абзац про
  reschedule со ссылкой на пункт бэклога (`:489`), примеры (`:501` `mode="reschedule"`, `:519`, `:529`
  `mode="push"`); `docs/example_dags/rmq_sensor_push.py` (`:3`, `:60`, `:104`); `CHANGELOG.md`
  (`v2.7.0` не выпущен, последний тег `v2.6.0`); `docs/backlog/airflow-3-migration-path.md` ссылается
  на `[[rmq-trigger-has-no-amqp-timeouts]]`.

## Development Approach
- **testing approach**: Regular (сначала код, затем тесты в той же задаче)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - tests are not optional - they are a required part of the checklist
  - write unit tests for new functions/methods
  - write unit tests for modified functions/methods
  - add new test cases for new code paths
  - update existing test cases if behavior changes
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change
- maintain backward compatibility
- комментарии и docstring'и описывают текущее поведение, без истории изменений; перенесённые в
  `utils/amqp.py` docstring'и не говорят о watcher'е как о единственном потребителе

## Testing Strategy
- **unit tests**: `pytest tests/ -q` — обязательны в каждой задаче. Триггер тестируется на фейках
  уровня `aiormq` (соединение, канал, доставки) и на локальном TCP-сервере для сценариев подключения.
- **integration tests**: `pytest integration/ -q` на живом RabbitMQ 4.4, только vhost `test`
  (`.env` с `RMQ_TEST_URL`); прод-vhost не используется.
- **совместимость с Airflow 2.9**: по исходникам Airflow 2.9.1 (тег в репозитории apache/airflow), а
  не только по venv 2.11.2, проверить: `AirflowProviderDeprecationWarning` в `airflow.exceptions`;
  `BaseOperator.defer(..., timeout=)`; исход истёкшего deferral-таймаута для сенсора
  (`check_trigger_timeouts`, `resume_execution`, `soft_fail`); что `BaseSensorOperator.timeout` —
  секунды `float` и при передаче `timedelta` (`_coerce_timeout`).
- **совместимость с нижней границей `aiormq`**: используемый API (`Connection(context=)`,
  `channel(publisher_confirms=, timeout=)`, `basic_get(timeout=)`, `basic_consume(consumer_tag=,
  timeout=)`, `channel.consumers`, `closing`) есть в выбранной нижней границе — проверить по исходникам
  её тега.
- e2e UI-тестов в проекте нет.

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview
- **Общий помощник закрытия — в `utils/amqp.py`.** `close_quietly` и `CLOSE_TIMEOUT` переезжают из
  watcher'а; watcher импортирует их оттуда, его поведение не меняется.
- **Шаг попытки — `_step(awaitable, timeout, name)`, помощник триггера.**
  - `task = ensure_future(awaitable)`; `done, _ = await asyncio.wait({task, connection.closing},
    timeout=min(timeout, остаток), return_when=FIRST_COMPLETED)`;
  - `CancelledError` из самого `asyncio.wait` — это отмена `run()`: `asyncio.wait` не пробрасывает
    исходы дочерних задач, поэтому другого источника у неё нет. `task` отменяется и бросается
    (`abandon_call` из `utils/amqp.py`), отмена пробрасывается;
  - `task` завершилась обычно или с исключением → результат или исключение (классификация ниже);
  - `task` завершилась отменой (так aiormq отдаёт `CancelledError` при закрытом соединении) или
    `connection.closing` завершился → `_ConnectionLost(reason=connection.closing.exception())`,
    причина читается через `exception()`, не `result()`;
  - ничего не завершилось → `task` бросается, `_StepTimeout(name, timeout)`;
  - так своя отмена `run()` и потеря соединения различаются механизмом, а не состоянием
    `connection.closing`, которое после закрытия остаётся завершённым до конца попытки;
  - паузы (`poll_interval`, пауза между попытками) — обычный `asyncio.sleep`: отмена в них всегда
    принадлежит `run()`.
- **Попытка = своё соединение и свой канал.**
  - `connection = aiormq.Connection(url, context=ssl_context)` — до первого `await`;
  - подключение — тот же `_step`, но без гонки с `connection.closing`: ждёт только задачу
    `connection.connect()` под границей `connect`. При неудаче handshake aiormq сам закрывает соединение
    и завершает `closing` исключением раньше, чем возвращается `connect()`, — гонка превратила бы отказ
    первой попытки в `_ConnectionLost`;
  - `channel = _step(connection.channel(publisher_confirms=False), rpc, "channel")`;
  - `_step(channel.queue_declare(queue, passive=True), rpc, "declare")`;
  - pull: `_step(channel.basic_get(queue, no_ack=False), rpc, "get")`; доставка с `GetEmpty` —
    очередь пуста, пауза `poll_interval`;
  - push: доставки кладутся колбэком `basic_consume` в локальную `asyncio.Queue`;
    `_step(channel.basic_consume(queue, callback, consumer_tag=tag), rpc, "consume")`;
  - каждая доставка → `aio_pika.IncomingMessage(delivered)` → вердикт `match()` один раз →
    `_step(match_and_ack(..., verdict=verdict), rpc, "ack")`: `match_and_ack` принимает уже посчитанный
    вердикт и не оценивает фильтр повторно (умолчание `None` — оценивает сам, как сейчас у watcher'а);
  - попытка в `try/finally`: `close_quietly(connection, ..., log)` при любом выходе, включая отмену
    `run()` triggerer'ом; закрытие соединения закрывает и его канал;
  - штатный выход push-попытки (совпадение, истёкший `message_wait_timeout`) перед закрытием
    возвращает непрочитанные доставки явно: `_step(channel.basic_cancel(tag), rpc, "cancel")`, затем
    `basic_nack(requeue=True)` для каждой доставки, оставшейся в локальной очереди, под той же границей.
    Quorum-очередь засчитывает в `delivery-limit` обрыв соединения, но не `basic.nack`; без явного
    возврата каждый удачный прогон списывал бы доставку со всех сообщений за совпавшим. Неудача этих
    шагов не меняет исход попытки — соединение просто закрывается. При обрыве соединения и отмене
    `run()` возврат делает брокер.
- Соединению задаётся имя: `?name=rmq-trigger:<queue>` в URL (aiormq передаёт его брокеру как
  `connection_name`) — по нему соединение видно в Management UI.
- **Ожидание push.** `asyncio.wait({get_task, channel.closing}, timeout=min(rpc, остаток),
  return_when=asyncio.FIRST_COMPLETED)`, где
  `get_task` — ожидание локальной очереди; его отмена ничего не шлёт брокеру. Каждый оборот:
  - доставка → обработка;
  - `channel.closing` завершился → `_ConnectionLost(reason=channel.closing.exception())`, как в
    `_step`: голый `CancelledError()` (потеря heartbeat) и голый `Exception()` (чистый EOF) — потеря
    соединения, причина из п. 1 (404, 403) — error-событие;
  - иначе `tag in channel.consumers` — тега нет → брокер отменил консьюмера → WARNING и новая попытка.
    Одной проверки достаточно: восстанавливать каналы некому, тег исчезает только по `Basic.Cancel`.
- **ACK совпавшего сообщения.** `basic.ack` не имеет ответа, и исход незавершённого ACK неизвестен:
  байты могли уйти в сокет до обрыва (тогда брокер сообщение удалил), а кадры, ещё стоявшие в очереди
  записи, при закрытии соединения отбрасываются (тогда сообщение вернётся). Поэтому **любая** неудача
  шага `ack` при вердикте `MATCH` — `_StepTimeout`, `_ConnectionLost`, `ChannelInvalidStateError`,
  `ConnectionError` — даёт success-событие с телом и WARNING «ACK not confirmed; the message may be
  delivered again»: возможен дубль, но не потеря. Неудача при другом вердикте — повторяемый исход.
- **Таймаут шага — своё исключение.** `_StepTimeout(step, timeout)` — исключение триггера, не
  наследник `OSError`. На Python 3.11+ `asyncio.TimeoutError` — это встроенный
  `TimeoutError ⊂ OSError`, и без своего класса таймаут подключения совпал бы с отказом в подключении.
- **Потеря соединения — своё исключение.** Соединение, потерянное по heartbeat, aiormq закрывает с
  причиной `CancelledError`: она ставится исключением в `connection.closing` и во все `channel.closing`,
  а шаг, ждавший ответа, завершается отменой. `_step` превращает это в `_ConnectionLost` (п. 3), а
  отмена самого `run()` приходит только из `await asyncio.wait(...)` и пробрасывается как есть после
  `finally`.
- **Классификация исходов** — в порядке проверки:
  1. error-событие на любой попытке:
     - `ProbableAuthenticationError` — aiormq поднимает её только на `Connection.Close` с кодом 403
       во время handshake, то есть неверный или сменённый пароль (`aiormq/connection.py:525-527`);
     - `AuthenticationError` — брокер не предлагает поддерживаемый механизм SASL;
     - `pamqp.exceptions.AMQPInternalError`, чей кадр (`exc.args[1]`) — `Connection.Close` с кодом 403
       или с кодом 530 и текстом об отказе в доступе к vhost (`access to vhost '…' refused …`) — ответ на
       `Connection.Open`, `connection.py:521-523`. `AMQPInternalError` на `Open` с другим кодом или
       текстом (541 «vhost is down» при перезапуске vhost, 320 при остановке узла, 530 из-за лимита
       соединений) — п. 2;
     - `ConnectionNotAllowed` (530 после handshake), `ChannelAccessRefused` (403),
       `ChannelNotFoundEntity` (404) — в т.ч. пришедшие причиной `channel.closing` /
       `connection.closing`. Текст ответа брокера у исключений aiormq — `str(exc)` / `exc.args[0]`,
       отдельного атрибута нет. Исключение: 404, в тексте ответа которого брокер пишет, что «домашний»
       узел durable classic-очереди недоступен (`home node '…' of durable queue '…' … is down or
       inaccessible`), — это перезапуск узла кластера, а не удалённая очередь: такой 404 идёт в п. 3;
  2. error-событие на первой попытке, повтор на следующих (причина — в WARNING): отказ в подключении
     (`ConnectionError` / `OSError` из `connect()`, кроме `_StepTimeout`), `IncompatibleProtocolError`
     `EOFError` и `StopAsyncIteration` (обрыв handshake: `FrameReceiver.get_frame()` поднимает
     `StopAsyncIteration` при EOF до первого кадра; так выглядит и перезапуск брокера), `ConnectionClosed` с другим
     кодом во время handshake (например 320 при остановке брокера), `AMQPInternalError` на
     `Connection.Open` вне п. 1;
  3. повтор: `_StepTimeout`; `_ConnectionLost` (причина — в WARNING; если причина сама из п. 1, она
     классифицируется по п. 1); закрытое
     соединение или канал (`AMQPConnectionError`, `ConnectionError`, `ChannelClosed` вне п. 1,
     `ChannelInvalidStateError`, `RuntimeError` закрытого соединения); отмена консьюмера брокером;
  4. всё остальное → error-событие, как сейчас.
  - `_StepTimeout` проверяется раньше п. 2; п. 1 — раньше п. 2–3.
  - WARNING: шаг или причина, `conn_id`, очередь, граница, номер попытки, пауза до следующей.
- **Пауза** между попытками — `next_backoff(current, maximum=60, minimum=15)`; сбрасывается после
  попытки, дошедшей до ожидания сообщения.
- **`message_wait_timeout`** в push-режиме — дедлайн от начала `run()` поверх всех попыток и пауз.
  Проверяется перед каждым шагом; граница каждого шага и паузы — `min(своя граница, остаток)`, кроме
  возврата буфера при штатном выходе (`basic_cancel` и NACK): он идёт под полной границей `rpc`, иначе
  буфер вернулся бы обрывом соединения. Перебег — не больше `rpc_timeout + CLOSE_TIMEOUT` (возврат
  буфера и закрытие соединения попытки; ~35 с при умолчаниях). Истёк → `{"status": "timeout"}`, как
  сейчас.
- **Сенсор.**
  - `trigger_mode: Literal["pull", "push"] | None = None`;
  - `mode` уходит базовому классу (`poke`/`reschedule`, проверяет базовый класс); явный `mode=None`
    удаляется из kwargs;
  - `mode="pull"`/`"push"` → `AirflowProviderDeprecationWarning`, значение переносится в
    `trigger_mode`, в базовый класс идёт умолчание `poke`;
  - заданы оба (`trigger_mode` и `mode` в `pull`/`push`) → `ValueError`;
  - `trigger_mode` проверяется явно;
  - `_defer` передаёт `trigger_mode` в `RMQTrigger(mode=...)` и
    `timeout=timedelta(seconds=self.timeout)` в `self.defer(...)`.
- **Видимость промахов в `reschedule`.** Каждый `poke` идёт на новом объекте, `FilterMissState`
  обнуляется. При `self.reschedule` в конце каждого `poke` вызывается `miss_state.on_batch_done(...)`,
  как у `RMQConsumeOperator`. Цена: строка WARNING на каждый `poke`, пока в голове очереди лежат чужие
  сообщения; каждый `poke` в `reschedule` — отдельная попытка задачи со своим логом, поэтому строка
  на попытку уместна. В режиме `poke` ничего не меняется: счётчик живёт на экземпляре между вызовами.

## Technical Details
- `utils/amqp.py`: `CLOSE_TIMEOUT = 5.0`, `close_quietly(closeable, what, logger, method="close") ->
  None` — перенос с одним изменением: WARNING пишется в логгер вызывающего (watcher передаёт свой
  модульный `log`, триггер — `airflow.task`), как у `FilterMissState`; docstring нейтральный.
- `pyproject.toml`: `aiormq` — прямая зависимость, диапазон совместим с `aio-pika` (`>=6.8,<7` или
  выше по итогам проверки нижней границы).
- `RMQTrigger`:
  - `__init__` / `serialize()` — набор и имена полей прежние;
  - тег консьюмера генерируется на попытку;
  - пауза — константы модуля (`15`, `60`), не параметры;
  - payload success-события строится из `IncomingMessage` так же, как сейчас.
- WARNING (логгер `airflow.task`, попадает в лог triggerer'а и задачи), пример:
  `RMQTrigger: connect to conn_id='rabbitmq' did not complete within 15s (attempt 3); retrying queue 'q' in 60s`.
- `RMQSensor.__init__`:
  ```python
  def __init__(self, *, queue_name, ..., trigger_mode=None, message_wait_timeout=None, ..., **kwargs):
      if "mode" in kwargs and kwargs["mode"] is None:
          del kwargs["mode"]
      if kwargs.get("mode") in ("pull", "push"):
          if trigger_mode is not None:
              raise ValueError(...)
          warnings.warn(..., AirflowProviderDeprecationWarning, stacklevel=2)
          trigger_mode = kwargs.pop("mode")
      super().__init__(**kwargs)
      self.trigger_mode = trigger_mode or "pull"
  ```
  - `message_wait_timeout` разрешён только при `trigger_mode == "push"`;
  - `template_fields` не меняются.
- `RMQSensor._defer`:
  `self.defer(trigger=RMQTrigger(..., mode=self.trigger_mode, ...), method_name="execute_complete",
  timeout=timedelta(seconds=self.timeout))`.

## What Goes Where
- **Implementation Steps** (`[ ]` checkboxes): код, тесты, документация в этом репозитории.
- **Post-Completion** (no checkboxes): правка продового DAG'а, наблюдение на проде.

## Implementation Steps

### Task 1: `close_quietly` и `CLOSE_TIMEOUT` — в `utils/amqp.py`

**Files:**
- Modify: `airflow_provider_rmq/utils/amqp.py`
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`
- Modify: `tests/test_amqp_utils.py`
- Modify: `docs/adr/0007-connection-liveness-two-tier-check.md`

- [x] перенести `_CLOSE_TIMEOUT` и `_close_quietly` в `utils/amqp.py` как `CLOSE_TIMEOUT` и
      `close_quietly(closeable, what, logger, method="close")`; WARNING — в переданный логгер;
      docstring нейтральный
- [x] `watcher/consumer.py` импортирует их и передаёт свой модульный `log` — строки и логгер прежние;
      ссылки `:data:`_CLOSE_TIMEOUT`` и упоминания `_close_quietly` в комментариях и docstring'ах
      (`:80`, `:641`, `:667`, `:1353` и прочие) указывают на новые имена
- [x] ADR-0007 (`:228`, `:479` и прочие упоминания) — новые имена; итоговый
      `grep -rnw --include='*.py' --include='*.md' "_close_quietly\|_CLOSE_TIMEOUT" docs airflow_provider_rmq tests`
      пуст, кроме `docs/plans/completed/` и этого файла плана
- [x] `_abandon_call` в `utils/amqp.py` становится публичным `abandon_call` (им пользуются
      `call_with_timeout` и `_step` триггера); обновить его вызовы и упоминания
- [x] `tests/watcher/test_consumer.py`: прямой импорт `_CLOSE_TIMEOUT` из `watcher.consumer`
      (`:408-416`) → `CLOSE_TIMEOUT` из `utils.amqp`; все 8 `patch(...consumer._CLOSE_TIMEOUT...)` →
      `airflow_provider_rmq.utils.amqp.CLOSE_TIMEOUT` (константу читает `close_quietly` из своего модуля);
      прочие упоминания `_close_quietly` в тестах — на новое имя; тест `:4341-4346`, проверяющий
      WARNING через `consumer.log`, проходит без изменения цели патча
- [x] тесты `close_quietly` в `tests/test_amqp_utils.py`: закрытие удалось; исключение → WARNING в
      переданный логгер и возврат; зависшее закрытие → возврат через `CLOSE_TIMEOUT`; `method="aclose"`
- [x] прогнать `pytest tests/ -q` — всё зелёное до задачи 2

### Task 2: `RMQTrigger`: попытка на своём `aiormq`-соединении

**Files:**
- Modify: `airflow_provider_rmq/triggers/rmq.py`
- Modify: `airflow_provider_rmq/utils/amqp.py`
- Modify: `pyproject.toml`
- Modify: `tests/test_trigger.py`
- Modify: `tests/test_amqp_utils.py`

- [x] `pyproject.toml`: прямая зависимость `aiormq` (диапазон — по проверке нижней границы из Testing
      Strategy)
- [x] помощник `_step(awaitable, timeout, name)` по Solution Overview: `asyncio.wait({task,
      connection.closing}, FIRST_COMPLETED)`; отмена из самого `asyncio.wait` — отмена `run()`,
      пробрасывается; отменённая задача или завершённый `closing` → `_ConnectionLost`; ничего не
      завершилось → `_StepTimeout`; брошенная задача отменяется и её исход читается (`abandon_call`)
- [x] попытка: `aiormq.Connection(url, context=ssl_context)`, `connect()`, `channel(publisher_confirms=
      False)`, `queue_declare(passive=True)` — каждый шаг через `_step` (`connect` / `rpc`);
      `try/finally` с `close_quietly(connection, ..., log)`, в том числе при отмене `run()`
- [x] pull: `basic_get(no_ack=False)` через `_step`; `GetEmpty` → пауза `poll_interval`; доставка →
      `IncomingMessage` → вердикт `match()` → `match_and_ack(..., verdict=)` через `_step`
- [x] `match_and_ack(message, msg_filter, miss_state, verdict=None)`: переданный вердикт (с причиной)
      используется вместо повторной оценки; тесты в `tests/test_amqp_utils.py` — с вердиктом фильтр не
      вызывается, без него поведение прежнее
- [x] push: `basic_consume` с колбэком в `asyncio.Queue` и тегом попытки через `_step`; ожидание через
      `asyncio.wait({get_task, channel.closing}, ..., return_when=FIRST_COMPLETED)`; проверка
      `tag in channel.consumers`; висящий `get_task` отменяется при выходе
- [x] любая неудача шага `ack` при вердикте `MATCH` (`_StepTimeout`, `_ConnectionLost`,
      `ChannelInvalidStateError`, `ConnectionError`) → success-событие с телом и WARNING
- [x] тест `_step` на настоящем `aiormq.Connection` против локального сервера, который закрывает
      сокет посреди шага: шаг завершается `_ConnectionLost` по `connection.closing`, а не по таймауту
- [x] перевести 24 мока `connect_robust` в `tests/test_trigger.py` (в т.ч. проверки kwargs `:300-339`) на
      фейки уровня `aiormq`
- [x] тесты успеха: pull и push доставляют совпавшее сообщение, payload прежний; `serialize()` —
      прежний набор полей; триггер из сериализованных kwargs с `mode` восстанавливается
- [x] тесты границ: молчащий handshake (локальный TCP-сервер, принимает и молчит) → таймаут
      `connect_timeout`, объект соединения закрыт; зависшие `channel()`, `queue_declare`, `basic_get`,
      `basic_consume`, ACK → таймаут `rpc_timeout`; отмена `run()` посреди ожидания закрывает соединение и
      не оставляет висящих задач
- [x] тесты ACK: таймаут ACK совпавшего сообщения и обрыв соединения во время этого ACK →
      success-событие и WARNING; таймаут NACK промаха → не success
- [x] тест ожидания push: сообщение, уже лежащее в локальной очереди, обрабатывается сразу, без
      ожидания `rpc_timeout`
- [x] тесты `_step`: отмена `run()` при уже закрытом соединении (соединение закрылось во время
      `poll_interval` и во время шага) → `CancelledError` пробрасывается, повтора нет; отменённая
      aiormq-задача при живом `run()` → `_ConnectionLost`
- [x] прогнать `pytest tests/ -q` — всё зелёное до задачи 3

### Task 3: `RMQTrigger`: цикл попыток, классификация, пауза и дедлайн

**Files:**
- Modify: `airflow_provider_rmq/triggers/rmq.py`
- Modify: `tests/test_trigger.py`

- [x] `_StepTimeout` проверяется раньше `OSError` (на 3.11+ `asyncio.TimeoutError ⊂ OSError`);
      `_ConnectionLost` — повтор, если его причина не из п. 1
- [x] 404 «home node … down» распознаётся по тексту ответа (`str(exc)`; у исключений aiormq нет атрибута
      `reply_text`) и идёт в п. 3
- [x] `AMQPInternalError` с кадром `Connection.Close` (`exc.args[1]`): 403 и 530 «access to vhost …
      refused» — п. 1; другие код или текст (541, 320, 530 лимита соединений) — п. 2
- [x] цикл попыток с классификацией в порядке из Solution Overview (п. 1 проверяется раньше п. 2–3:
      `ProbableAuthenticationError`, `ConnectionNotAllowed` — наследники `AMQPConnectionError` /
      `ConnectionError`; `AMQPInternalError` — по коду и тексту кадра, см. выше)
- [x] пауза через `next_backoff` 15 → 60 с, сброс после попытки, дошедшей до ожидания сообщения
- [x] push: `channel.closing` завершился → `_ConnectionLost(reason)` и классификация по его причине;
      тега нет в `channel.consumers` → WARNING «consumer cancelled by the broker» и новая попытка
- [x] push, штатный выход (совпадение, дедлайн): `basic_cancel` и `basic_nack(requeue=True)` для
      непрочитанных доставок под `rpc`, затем закрытие; неудача этих шагов исход не меняет
- [x] имя соединения `rmq-trigger:<queue>` через `?name=` в URL
- [x] `message_wait_timeout` (push) — дедлайн от начала `run()`, проверяется перед каждым шагом
      (включая паузу); граница шага — `min(своя, остаток)`; истёк → `{"status": "timeout"}`
- [x] docstring класса и `run()`: какие шаги ограничены, чем (`connect_timeout`, `rpc_timeout` из
      `extra`), что повторяется и что даёт ошибку, ACK совпавшего сообщения, перебег дедлайна не больше
      `rpc_timeout + CLOSE_TIMEOUT` (возврат буфера через `basic_cancel` и NACK, затем закрытие);
      docstring класса и параметров `mode` / `message_wait_timeout` (`triggers/rmq.py:17-36`, `:57-60`)
      описывают `basic_get` / `basic_consume` на `aiormq`, без `queue.get()` и `queue.iterator()`
- [x] тесты классификации: `ProbableAuthenticationError`, `AuthenticationError`, `AMQPInternalError` с
      `Connection.Close` 530, `ConnectionNotAllowed`, 403, 404 → error на первой и на третьей попытке;
      refused / `IncompatibleProtocolError` / `EOFError` / `ConnectionClosed` 320 при handshake → error на
      первой, повтор на второй; таймаут подключения на первой попытке → повтор (тест не зависит от
      версии Python: таймаут приходит как `_StepTimeout`); закрытие соединения посреди pull (в
      `basic_get` и между вызовами), закрытие канала с не-404/403 → повтор; прочее исключение → error
- [x] тесты потери по heartbeat: `connection.closing` и `channel.closing` с `CancelledError()`
      как исключением, шаг pull завершается отменой → `_ConnectionLost`, WARNING и повтор, `run()` не
      завершается; отмена самого `run()` → `CancelledError` пробрасывается, соединение закрыто
- [x] тесты push: тег пропал → WARNING и новая попытка, на ней 404 → error; `channel.closing` с 404 →
      error; 404 с текстом «home node … is down or inaccessible» (на passive declare, `basic_get`,
      `basic_consume` и причиной `channel.closing`) → WARNING и повтор, на следующей попытке очередь
      доступна → сообщение доставляется; `channel.closing` с `CancelledError()` (heartbeat) и с `Exception()` (EOF) → WARNING и
      повтор, `run()` не завершается; сообщение, пришедшее между оборотами, обрабатывается как обычно
- [x] тесты штатного выхода push: после совпадения при двух непрочитанных доставках в буфере —
      `basic_cancel`, затем `basic_nack(requeue=True)` для обеих, затем закрытие; то же при истёкшем
      `message_wait_timeout`; зависший `basic_cancel` не задерживает выход дольше `rpc`
- [x] тесты паузы и дедлайна: 15 → 30 → 60 → 60 и сброс (время подменено); дедлайн истекает во время
      паузы и между шагами попытки → timeout-событие; без буфера перебег не больше `CLOSE_TIMEOUT`, с
      зависшим `basic_cancel` — не больше `rpc_timeout + CLOSE_TIMEOUT`
- [x] тесты handshake на настоящем `aiormq.Connection` против локального сервера: `Connection.Close`
      320 после `StartOk`, `Connection.Close` 541 «vhost is down» и 320 в ответ на `Connection.Open`,
      EOF сразу после TCP-подключения — на первой попытке → error-событие (не `_ConnectionLost`), на
      второй → повтор; `Connection.Close` 530 «access to vhost '…' refused» на `Open` → error на любой
      попытке; 404 с текстом «home node … down» — распознаётся по `str(exc)` настоящего
      `ChannelNotFoundEntity`
- [x] прогнать `pytest tests/ -q` — всё зелёное до задачи 4
- ⚠️ ожидание push: буфер — `deque` и `asyncio.Event` (колбэк дописывает доставку и ставит событие),
  ожидание — `asyncio.wait({event.wait(), channel.closing}, ...)`; доставка снимается с буфера только
  после проверки дедлайна, поэтому необработанная остаётся в буфере для `basic_nack`

### Task 4: `RMQSensor`: `trigger_mode`, `timeout` в `defer()`, промахи в `reschedule`

**Files:**
- Modify: `airflow_provider_rmq/sensors/rmq.py`
- Modify: `airflow_provider_rmq/utils/amqp.py`
- Modify: `tests/test_sensor.py`

- [x] параметр `trigger_mode: Literal["pull", "push"] | None = None`; `mode` остаётся базовому классу;
      явный `mode=None` удаляется; `mode="pull"`/`"push"` → `AirflowProviderDeprecationWarning` и
      перенос в `trigger_mode`; оба заданы → `ValueError`; недопустимый `trigger_mode` → `ValueError`
- [x] проверка `message_wait_timeout` опирается на `trigger_mode`; тексты ошибок называют
      `trigger_mode`
- [x] `_defer`: `RMQTrigger(mode=self.trigger_mode, ...)` и `timeout=timedelta(seconds=self.timeout)` в
      `self.defer(...)`
- [x] `poke`: при `self.reschedule` в конце каждого вызова — `miss_state.on_batch_done(matched,
      evaluated, dropped_in_batch)` по этому `poke` (ERROR-вердикты не считаются в `evaluated`, как у
      оператора); docstring `on_batch_done` (`utils/amqp.py:441-444`) — что батч может быть и одним
      `poke` сенсора в `reschedule`; комментарий к `self._miss_state` (`sensors/rmq.py:96-98`) и
      docstring `FilterMissState` (`utils/amqp.py:333-334`) — состояние живёт между `poke` только в
      режиме `poke`, в `reschedule` каждый `poke` начинает с нуля
- [x] docstring класса и параметров: `trigger_mode`, `mode` базового класса, граница `timeout` в
      deferrable и её исход (FAILED с ретраями, SKIPPED при `soft_fail`); `message_wait_timeout`
      (`sensors/rmq.py:73-76`) — действует при `trigger_mode="push"`, перебег не больше
      `rpc_timeout + CLOSE_TIMEOUT` (отмена консьюмера и возврат непрочитанных сообщений)
- [x] обновить существующие тесты: `mode="push"` `:320-409` → `trigger_mode="push"` (и отдельный тест
      на предупреждение), текст ошибки `:354`, `test_the_base_sensor_mode_is_untouched` `:520-528`,
      docstring `test_reschedule_never_reaches_the_threshold` (`:550-553`, ссылка на пункт бэклога
      `:552`), docstring `test_a_hundred_pokes_report_no_batch_line` (`:570-574`, «`on_batch_done` is
      the operator's»)
- [x] новые тесты: умолчание (`trigger_mode="pull"`, `mode="poke"`, `reschedule` False); `mode="push"` →
      предупреждение, `trigger_mode="push"`, `mode="poke"`; `mode=None` → как без `mode`;
      `mode="reschedule"` + `deferrable=True` → в триггер уходит `pull`; `mode="bogus"` → ошибка базового
      класса; оба параметра → `ValueError`; `prepare_for_execution()` под `DebugExecutor` (единственный
      executor с `change_sensor_mode_to_reschedule`) переводит `mode` в `reschedule` и оставляет
      `trigger_mode="push"`
- [x] тесты `_defer`: `defer` получает `timeout=timedelta(seconds=sensor.timeout)` (и при `timeout`,
      заданном `timedelta`) и `RMQTrigger` с `mode=trigger_mode`; `message_wait_timeout` без `push` →
      `ValueError`
- [x] тесты `reschedule`: `poke` без совпадений → строка `on_batch_done`; в режиме `poke` строки по
      батчу нет
- [x] прогнать `pytest tests/ -q` — всё зелёное до задачи 5

### Task 5: Integration-тесты триггера на живом брокере

**Files:**
- Create: `integration/test_trigger.py`
- Modify: `integration/conftest.py`
- Modify: `integration/README.md`

- [x] `integration/conftest.py`: `_load_env` переносит из `.env` и `RMQ_TEST_MANAGEMENT_URL`; фикстура
      `rmq_management_url` — тест, которому она нужна, пропускается, если переменной нет, и
      пропускается с понятной причиной при 401/403 (пользователю нужен тег `management` или
      `monitoring`); vhost — тот же `test`; запросы к Management API затрагивают только соединения,
      которые тест нашёл по имени
  ⚠️ в этом окружении `RMQ_TEST_MANAGEMENT_URL` без учётных данных — тест закрытия соединения
  брокером скипается с причиной; клиент Management API проверен на локальном HTTP-фейке
- [x] pull и push: `RMQTrigger.run()` получает опубликованное сообщение в scratch-очереди и отдаёт
      success-событие с прежними полями
- [x] push на quorum-очереди (`scratch.declare_queue(..., arguments={"x-queue-type": "quorum"})`): за
      совпадающим сообщением лежат ещё два; после success-события оба на месте, и при следующем чтении
      у них нет заголовка `x-delivery-count` (возврат через `basic.nack` его не увеличивает, обрыв
      соединения увеличил бы)
- [x] push: очередь удаляется, пока триггер ждёт (подключение с `rpc_timeout` 2 с через свой
      `AIRFLOW_CONN_*`, константа паузы подменена на 0,1 с) → WARNING об отмене консьюмера, затем
      error-событие с 404 не позже чем через 10 с
- [x] push: соединение триггера закрывается со стороны брокера — `GET /api/vhosts/test/connections`,
      соединение с `client_properties.connection_name == "rmq-trigger:<scratch-очередь>"`, `DELETE
      /api/connections/{name}` только для него → WARNING и новая попытка, затем опубликованное
      сообщение доставляется; соединение фикстуры `scratch` не затронуто
- [x] push: `message_wait_timeout` 3 с без сообщений и с пустым буфером → timeout-событие не позже
      чем через 3 с + `CLOSE_TIMEOUT`
- [x] неверный пароль в `AIRFLOW_CONN_*` → error-событие на первой попытке
- [x] `integration/README.md`: что проверяет `test_trigger.py`; переменная `RMQ_TEST_MANAGEMENT_URL` в
      таблице переменных (формат, тег пользователя, тест пропускается без неё), фикстура
      `rmq_management_url` в таблице фикстур
- [x] прогнать `pytest integration/ -q` на vhost `test` — всё зелёное

### Task 6: Verify acceptance criteria
- [ ] все сценарии зависания из Overview ограничены по времени и видны в логе задачи
- [ ] исходы без повтора — по классификации (auth, vhost, 403, 404 всегда; отказ и обрыв handshake на
      первой попытке)
- [ ] после выхода и после отмены `run()` соединение попытки закрыто и висящих задач нет (кроме
      описанного ограничения `aiormq` на первом кадре handshake)
- [ ] сериализованный в метабазе триггер с `mode` восстанавливается; `mode="push"` у сенсора работает
      с предупреждением
- [ ] пункты проверки Airflow 2.9.1 и нижней границы `aiormq` из Testing Strategy подтверждены
- [ ] полный прогон: `pytest tests/ -q`
- [ ] integration: `pytest integration/ -q`

### Task 7: [Final] Документация, CHANGELOG и бэклог
- [ ] `readme.md` / `readme_ru.md`:
  - таблица ключей `extra` (`:117-122`): `connect_timeout`, `rpc_timeout` — «Used by»: RMQ Watcher и
    `RMQTrigger`; строка `heartbeat` верна для обоих асинхронных путей (триггер переподключается сам);
  - таблица параметров сенсора: `trigger_mode` вместо `mode`, строка про `mode` базового класса;
  - таблица pull/push и примеры: `trigger_mode="push"`;
  - пример с `mode="reschedule"` оставить — это параметр базового класса;
  - абзац `:489` — видимость промахов в `reschedule` (строка по каждому `poke`), без ссылки на пункт
    бэклога; режим в `reschedule` сам переводит только `DebugExecutor` (не `SequentialExecutor`);
  - строки про устройство триггера: `:476` («Periodic queue.get() + sleep»), `:565` («Uses aio_pika»),
    `:576` (очистка через `basic_cancel`) — под `aiormq`, `basic_get` / `basic_consume`, отмену
    консьюмера с возвратом непрочитанных сообщений и перебег до `rpc_timeout + CLOSE_TIMEOUT`;
  - раздел про триггер: границы `connect_timeout` / `rpc_timeout`, что повторяется с WARNING и что
    даёт ошибку, ACK совпавшего сообщения; `timeout` сенсора как верхняя граница ожидания в deferrable
    и её исход (FAILED с ретраями, SKIPPED при `soft_fail`; при `message_wait_timeout` > `timeout` —
    падение по `timeout`)
- [ ] `utils/amqp.py`: комментарий `DEFAULT_HEARTBEAT` (`:19-22`), docstring `AmqpTimeouts.connect`
      (`:48`) и `build_amqp_connection` (`:265`) описывают оба асинхронных потребителя, а не только
      `connect_robust`; docstring `aio_connection` в `integration/conftest.py:309` и строка таблицы
      фикстур `integration/README.md:99` — путь watcher'а, без триггера
- [ ] `docs/adr/0002-four-consume-loops-not-unified.md` (`:6`, `:100-103`): триггер — `aiormq`, pull и
      push, своё соединение на попытку
- [ ] новый `docs/adr/0011-trigger-owns-its-aiormq-connection.md`: почему у триггера своё
      `aiormq`-соединение на попытку, а не robust-пул из ADR-0007 (robust-слой восстанавливает каналы с
      консьюмерами, повторяет ошибки аутентификации и 404 внутри фабрики, держит недоступное
      соединение во время подключения); правило ACK совпавшего сообщения; ограничение `aiormq` на
      первом кадре handshake
- [ ] `docs/example_dags/rmq_sensor_push.py`: `trigger_mode="push"` (`:3`, `:60`, `:104`); абзац `:26-29`
      — перебег до `rpc_timeout + CLOSE_TIMEOUT`, исход истёкшего `message_wait_timeout` —
      `AirflowSkipException` (задача SKIPPED), а не `RuntimeError`
- [ ] тест, что `docs/example_dags/rmq_sensor_push.py` импортируется в `DagBag` без ошибок
      (`test_example_dags_dagbag` проверяет только файлы с `rmq_trigger`); прогнать `pytest tests/ -q`
- [ ] `CHANGELOG.md`, `v2.7.0`, по строке на пункт:
  - Changed: шаги `RMQTrigger` ограничены по времени (`connect_timeout`, `rpc_timeout`), при таймауте
    или потере соединения он пишет WARNING и подключается заново;
  - Changed: deferrable `RMQSensor` соблюдает свой `timeout`;
  - Changed: `RMQSensor` отвергает значения `mode` кроме `poke`/`reschedule` (и устаревших
    `pull`/`push`);
  - Added: `RMQSensor(trigger_mode=...)`;
  - Deprecated: `RMQSensor(mode="pull"|"push")`;
  - Fixed: push-триггер замечает консьюмера, отменённого брокером;
  - Fixed: `RMQSensor` в `reschedule` пишет строку о промахах по каждому `poke`
- [ ] новый пункт бэклога `docs/backlog/aiormq-silent-handshake-leaks-socket.md` (`worth: later`,
      `where: airflow_provider_rmq/triggers/rmq.py`): отмена `aiormq.Connection.connect()` на первом кадре
      handshake оставляет сокет; что решило бы (исправление в `aiormq`, своя фабрика транспорта) и почему
      отложено
- [ ] `git rm docs/backlog/rmq-trigger-has-no-amqp-timeouts.md docs/backlog/sensor-mode-shadows-base-parameter.md`;
      в `docs/backlog/airflow-3-migration-path.md` заменить ссылку `[[rmq-trigger-has-no-amqp-timeouts]]`
      фразой о том, что ограниченные по времени шаги уже есть в `RMQTrigger` и переносятся в
      `MessageQueueTrigger`
- [ ] пересверить ссылки бэклога после всех правок: `grep -rn "where:\|\.py:[0-9]" docs/backlog/` по
      файлам `watcher/consumer.py`, `utils/amqp.py`, `triggers/rmq.py`, `sensors/rmq.py` — каждое
      `where:` и каждая ссылка `файл:строка` в тексте пунктов указывает на ту же конструкцию в текущем
      коде; у нового пункта `aiormq-silent-handshake-leaks-socket` — `where:` со строкой
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**External system updates**:
- `project-mgcom/airflow_dags/rmq_watcher_factory.py` (`donstroy_price_watcher`): заменить
  `mode="push"` на `trigger_mode="push"` после выката версии провайдера.
- Перед выкатом проверить DAG'и из каталогов прода, которых нет локально (`bnmap`, `direct`, `price`,
  `provider`, `yandex_cloud`, `akvilon`), на `RMQSensor` с `deferrable=True` и заданным `timeout`
  меньше ожидаемого окна (теперь он действует) и на нестандартные значения `mode`.

**Manual verification**:
- После выката посмотреть лог triggerer'а за день работы `donstroy_price_watcher`: WARNING о
  переподключениях появляются только при реальных проблемах брокера.
