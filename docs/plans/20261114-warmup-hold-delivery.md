# Прогрев: держать доставку неподтверждённой, пока DAG не станет запускаемым

## Overview
- Пока DAG-процессор не наполнил `serialized_dag`, `trigger_dag` отвечает `DagNotFound`, и
  `_handle_immediate_delivery` делает `nack(requeue=True)` с растущей паузой. На quorum-очереди
  с `delivery-limit` (по умолчанию 20 на RabbitMQ 4.x; на 3.x — только если его задаёт
  политика) каждый такой возврат тратит единицу бюджета перевыдач. Прогрев в 13.5 минут — это
  около двадцати возвратов, и сообщение, лежавшее в очереди на старте шедулера, брокер
  отправляет в DLX или удаляет. Строка подписки всё это время показывает `listening`, счётчик
  `_NOT_READY_LIMIT = 25` до порога не доходит, и единственный след — WARNING на каждой десятой
  доставке (backlog `warmup-loses-the-message-invisibly`).
- Решение (вариант A): на `_DagNotReady` доставка остаётся неподтверждённой, и триггер этой же
  доставки повторяется внутри процесса с той же растущей паузой, пока DAG не станет
  запускаемым. Неподтверждённая доставка `delivery-limit` не тратит.
- Удержание ограничено порогом прогрева: 25 попыток подряд (`_NOT_READY_LIMIT`) или 20 минут
  с первой не-готовой попытки серии (`_NOT_READY_MAX_SECONDS`, по монотонным часам) — что
  наступит раньше. Временной предел держит запас до `consumer_timeout` брокера (30 минут по
  умолчанию), который отсчитывается от момента доставки, а не от начала удержания. На пороге
  поведение прежнее: `nack(requeue=True)` и строка `error` с текстом про отсутствующую
  сериализованную версию — потеря, если она дальше случится, видна в строке.
- Cooldown fire-очередь (`rmq_watcher.fire`, `_handle_fire_delivery`) не меняется: она
  объявляется без `x-queue-type`, то есть classic, у classic-очереди `delivery-limit` нет, а
  один fire-consumer обслуживает все cooldown-DAG, и удержание одного события остановило бы
  остальные. Ограничение (vhost с типом очереди по умолчанию quorum) документируется.

## Context (from discovery)
- `airflow_provider_rmq/watcher/consumer.py`:
  - `_TRIGGER_TIMEOUT` (96, 60 с на попытку триггера), `_DagNotReady` (164),
    `_NOT_READY_LIMIT = 25` (170-176), `_TRIGGER_BACKOFF_START/MAX` (193-194), `_Backoff`
    (547: `wait()` спит и удваивает паузу, `reset()`);
  - `_ConsumerState.not_ready_streak` (938-942, docstring «Deliveries answered in a row»);
  - `_sync_trigger` (261) превращает `DagNotFound` в `_DagNotReady`;
  - `_new_connection` (604) — `RobustConnection`; каналы — `RobustChannel`, после
    переподключения `RobustQueue.restore` подписывается заново тем же тегом, и тот же
    `QueueIterator` продолжает выдавать доставки нового канала;
  - `_consume_subscription` (3186-3330): `trigger_backoff` живёт через переподключения и не
    сбрасывается при attach; `state.not_ready_streak` обнуляется при attach; `set_qos` не
    вызывается — неподтверждённое окно не ограничено, все доставки окна стареют вместе;
  - `_handle_immediate_delivery` (3367-3445): ветка `_DagNotReady` — `nack(requeue=True)`,
    `not_ready_streak += 1`, на пороге `error`, иначе `listening` и лог (каждый десятый —
    WARNING, комментарий 3404-3407 называет его «единственным следом»), затем
    `backoff.wait()`; другая ошибка — `nack`, `error`, серия 0; успех — `backoff.reset()`,
    серия 0, `listening`, `ack`;
  - `_handle_fire_delivery` (3644) — тот же приём для fire-очереди, не меняется.
- aio-pika 9.6.2 (`.venv`): `IncomingMessage.channel` (`message.py` 397-401) бросает
  `ChannelInvalidStateError`, если aiormq-канал доставки закрыт; `ack`/`nack` на мёртвом канале
  тоже бросают; `QueueIterator.close` (`queue.py` ~488-505) при отмене делает `nack(requeue=True)`
  всем доставкам буфера; после закрытия канала `QueueIterator._on_close` (~472) не чистит буфер,
  и уже полученные доставки мёртвого канала продолжают выдаваться.
- Тесты: `tests/watcher/test_consumer.py`: `_make_fake_message` (93-104, голый `MagicMock`),
  блок не-готовности 8301-8650, в том числе `test_a_reattach_starts_the_count_again` (~8452) и
  `test_the_liveness_check_keeps_asking_about_a_warming_up_subscription` (~8597-8650).
- Документация: `readme.md` / `readme_ru.md` — «One trigger failure the status keeps quiet
  about», «Boundary of the silence» (~975-978), «`prefetch` is deliberately not set» (~981-982);
  `CHANGELOG.md` v2.7.0 (не выпущен); `docs/live-verification.md` (~755-760, 772-775);
  `docs/backlog/reattach-restarts-the-warmup-count.md` (ссылается на удаляемый пункт, на номера
  строк 3238/3430/3441 и на `test_a_reattach_starts_the_count_again`).

## Development Approach
- **testing approach**: Regular (code first, then tests)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change: `.venv/bin/python -m pytest -q tests/watcher/test_consumer.py`,
  full suite `.venv/bin/python -m pytest -q`; lint `.venv/bin/ruff check .` and
  `.venv/bin/mypy airflow_provider_rmq` — totals not above `main` (129 / 32)
- comments, docstrings, docs and test names describe the present only and state behaviour
  positively (global CLAUDE.md); only the CHANGELOG says what changed

## Testing Strategy
- **unit tests**: required for every task. The handler is tested with the fake message
  (`ack` / `nack` as `AsyncMock`), a mocked `_trigger_dag` with a `side_effect` sequence and the
  pause patched not to sleep, as the existing not-ready tests do. A closed delivery channel is
  simulated the way aio-pika presents it:
  `type(msg).channel = PropertyMock(side_effect=ChannelInvalidStateError(...))` (a per-test
  subclass or `patch.object` on the mock's type, so other tests are unaffected). Elapsed time is
  driven by patching the monotonic clock the handler reads.
- `_consume_subscription`-level tests (existing fake queue iterator) cover ordering and
  cancellation.
- e2e/UI тестов в проекте нет; поведение брокера (что удержание не тратит `delivery-limit`, что
  делает `consumer_timeout`, считается ли возврат при закрытии канала) — Post-Completion.

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview
- `_delivery_channel_open(message) -> bool`: обращается к `message.channel` внутри `try` и
  возвращает False на `aio_pika.exceptions.ChannelInvalidStateError`. `state.channel` для этой
  проверки не используется: `RobustChannel` после переподключения снова открыт, а тег
  удерживаемой доставки принадлежит закрытому aiormq-каналу.
- `_handle_immediate_delivery` — цикл попыток для одной доставки:
  1. перед каждой попыткой, включая первую: `_delivery_channel_open(message)` ложно → INFO
     «channel of the held delivery closed — the broker returns it to the queue», выход без
     `ack`/`nack`/триггера, серия не меняется. Доставка, оставшаяся в буфере от закрытого
     канала, так не запускает DAG; её копию брокер выдаст на новом канале, и она придёт как
     следующая доставка того же `QueueIterator` (обычно в том же attach);
  2. попытка триггера;
  3. `_DagNotReady`: `not_ready_streak += 1`; при первой не-готовой попытке серии запоминается
     `not_ready_since` (монотонные часы) в `_ConsumerState`;
     - порог не достигнут (`streak < _NOT_READY_LIMIT` и прошло меньше
       `_NOT_READY_MAX_SECONDS`): строка `listening` (как сейчас), лог «not serialized yet …
       holding the delivery, pausing Ns» (каждая десятая попытка серии — WARNING, остальные —
       INFO), `backoff.wait()`, следующая попытка той же доставки;
     - порог достигнут: проверка канала (как в п.1); `nack(requeue=True)`, `error` с прежним
       текстом, `backoff.wait()`, выход — прежнее поведение порога. Следующая доставка того же
       attach, если DAG всё ещё не готов, после одной попытки сразу идёт по этой ветке;
  4. другая ошибка: прежнее поведение — серия 0, `not_ready_since` сброшено, `nack`, `error`,
     пауза, выход;
  5. успех: прежнее поведение — `backoff.reset()`, серия 0, `not_ready_since` сброшено,
     `listening`, `ack`, метрика.
- `not_ready_since` сбрасывается везде, где сбрасывается серия (attach, успех, другая ошибка).
- Отмена задачи (`CancelledError` из паузы или из `_trigger_dag`) пробрасывается; удерживаемая
  доставка не получает `nack` от обработчика, буфер при закрытии итератора обрабатывает
  aio-pika, как и сейчас.
- Дубли: успешный триггер с последующим неуспешным `ack` возможен и сейчас (readme
  «Deduplication rests on the producer»); проверка канала перед каждой попыткой не даёт
  удержанию расширить это окно.
- Fire-путь, filter-miss-путь, публикация cooldown-заглушки не меняются.

## Technical Details
- Новые константы: `_NOT_READY_MAX_SECONDS = 1200.0`. Комментарий у `_NOT_READY_LIMIT` и
  `_NOT_READY_MAX_SECONDS` — худший случай: пауза может начинаться сразу с 60 с (backoff общий
  для доставок и переподключений), попытка длится до `_TRIGGER_TIMEOUT` (60 с), так что без
  временного предела 25 попыток могли бы занять до ~49 минут; предел в 20 минут оставляет
  запас до `consumer_timeout` 30 минут, который отсчитывается от момента доставки и стареет для
  всего неподтверждённого окна одновременно.
- `_ConsumerState`: поле `not_ready_since: float | None`; docstring `not_ready_streak` —
  «не-готовые попытки подряд, по удерживаемым доставкам этого attach».
- Монотонные часы: `time.monotonic` через имя модуля (например, `_monotonic = time.monotonic`
  или прямой вызов, который тест патчит), решение за исполнителем.
- Тексты логов: удержание — «holding the delivery»; тихий выход — INFO с dag_id, sub_id.
- Комментарий у WARNING каждой десятой попытки — в настоящем времени: длинное удержание
  остаётся видно в логе, отфильтрованном по WARNING.

## What Goes Where
- **Implementation Steps**: код, тесты, документация в этом репозитории.
- **Post-Completion**: проверка на стенде с RabbitMQ (quorum-очередь с `delivery-limit`,
  `consumer_timeout`), версия RabbitMQ и `consumer_timeout` прода, Airflow 2.9.1.

## Implementation Steps

### Task 1: Удерживать доставку во время прогрева в `_handle_immediate_delivery`

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`

- [ ] `_delivery_channel_open(message)` по Solution Overview; `_NOT_READY_MAX_SECONDS`;
      `_ConsumerState.not_ready_since` и сброс его везде, где сбрасывается серия
- [ ] цикл попыток в `_handle_immediate_delivery` по Solution Overview: проверка канала перед
      каждой попыткой и перед `nack` на пороге, тихий выход с INFO, удержание ниже порога,
      прежнее поведение на пороге, на успехе и на другой ошибке
- [ ] комментарии и docstring'и в настоящем времени: `_NOT_READY_LIMIT` и
      `_NOT_READY_MAX_SECONDS` (худший случай, `consumer_timeout`), `not_ready_streak`,
      `_handle_immediate_delivery`, комментарий у WARNING каждой десятой попытки (~3404-3407)
- [ ] переработать существующие тесты не-готовности (8301-8650) под удержание: ниже порога ни
      одного `nack`, одна доставка проходит несколько попыток. Отдельно:
      `test_a_reattach_starts_the_count_again` — переподключение моделируется закрытием канала
      удерживаемой доставки (`PropertyMock`) и новым attach; 
      `test_the_liveness_check_keeps_asking_about_a_warming_up_subscription` — подписка
      проверяется посреди удержания, а не после `nack`
- [ ] новые тесты обработчика: N не-готовых попыток, затем успех → один `ack`, ноль `nack`,
      серия 0, `backoff.reset()`; серия доходит до 25 → один `nack(requeue=True)`, `error`;
      временной предел наступает раньше 25 попыток (патч часов) → `nack`, `error`; после порога
      следующая доставка того же attach после одной попытки уходит в `nack`; успех после
      порога сбрасывает серию и делает `ack`; другая ошибка посреди удержания → `nack`,
      `error`, серия 0; канал закрыт перед первой попыткой → `_trigger_dag` не вызывается, ни
      `ack`, ни `nack`, серия не меняется, INFO; канал закрылся посреди удержания → то же;
      `state.channel` открыт, а канал доставки закрыт → тихий выход; канал закрыт на пороге →
      без `nack`; `CancelledError` из паузы и из `_trigger_dag` пробрасывается без `nack`;
      WARNING на 10-й и 20-й попытках одной доставки, текст «holding the delivery»; сумма пауз
      до порога при backoff, стартующем с `_TRIGGER_BACKOFF_MAX`, не превышает
      `_NOT_READY_MAX_SECONDS`
- [ ] тесты на уровне `_consume_subscription`: вторая доставка буфера обрабатывается после
      успешного удержания первой; отмена задачи посреди удержания — обработчик не делает
      `nack` удерживаемой доставке
- [ ] run tests - must pass before next task

### Task 2: Verify acceptance criteria
- [ ] во время прогрева доставка не возвращается в очередь до порога (тест)
- [ ] на пороге (по попыткам и по времени) поведение и строка `error` прежние (тест)
- [ ] доставка с закрытого канала не запускает DAG (тест)
- [ ] fire-путь не изменён (существующие тесты fire-очереди зелёные без правок)
- [ ] run full test suite: `.venv/bin/python -m pytest -q`
- [ ] `ruff` и `mypy` — счётчики не выше `main` (129 / 32)

### Task 3: [Final] Update documentation
- [ ] `readme.md` и `readme_ru.md` (синхронно, утвердительно):
      - «One trigger failure the status keeps quiet about» — доставка удерживается
        неподтверждённой, триггер повторяется с растущей паузой, строка `listening`;
      - «Boundary of the silence» — удерживаемая доставка не тратит `delivery-limit`; порог —
        25 попыток или 20 минут; возврат доставки при закрытии канала, `consumer_timeout` или
        остановке consumer'а (aio-pika возвращает весь буфер) — расход `delivery-limit` зависит
        от версии RabbitMQ, ссылка на сценарий live-verification; fire-очередь classic и
        `delivery-limit` не имеет, кроме vhost с типом очереди по умолчанию quorum;
      - «`prefetch` is deliberately not set» — время удержания вычитается из
        `consumer_timeout` для всего неподтверждённого окна; при большом бэклоге поднять
        `consumer_timeout`
- [ ] `CHANGELOG.md` v2.7.0: Fixed — событие, ждущее прогрева, удерживается неподтверждённым,
      не тратит `delivery-limit` quorum-очереди и не теряется без следа
- [ ] `docs/live-verification.md`: сценарий — quorum-очередь с `x-delivery-limit` 3, сообщение
      до старта шедулера, DAG-файл появляется позже → сообщение не уходит в DLX, DAG-ран
      создаётся, `x-delivery-count` не растёт; прогрев дольше порога → строка `error`; маленький
      `consumer_timeout` → канал закрыт брокером, consumer продолжает, записать, растёт ли
      `x-delivery-count`/`x-acquired-count`. Переписать пункт ограничений 772-775, который
      ссылается на удаляемый пункт бэклога; обновить ~755-760
- [ ] `docs/backlog/reattach-restarts-the-warmup-count.md`: убрать ссылку на удаляемый пункт,
      пересчитать номера строк и имя теста, описать последствие в новых терминах (каждый
      reattach начинает удержание заново)
- [ ] `git rm docs/backlog/warmup-loses-the-message-invisibly.md`
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification** (стенд, RabbitMQ той же версии, что в проде; Airflow 2.9.1):
- сценарий из `docs/live-verification.md`: удержание не увеличивает `x-delivery-count`,
  сообщение не уходит в DLX, DAG-ран создаётся после прогрева.
- `consumer_timeout`: удержание дольше него (маленький `consumer_timeout` на стенде) → брокер
  закрывает канал, consumer продолжает на новом канале, доставка не запускает DAG дважды;
  записать, увеличивает ли такой возврат счётчик доставок на этой версии RabbitMQ.

**Прод:**
- версия RabbitMQ и тип очередей подписок (quorum/classic, `delivery-limit` из политики): на
  3.x у quorum-очереди `delivery-limit` по умолчанию нет — от этого зависит, был ли дефект
  реальным для прода.
- `consumer_timeout` брокера и `x-consumer-timeout` политик очередей подписок: если меньше 30
  минут — порог удержания 20 минут его съедает; поднять или учесть.
