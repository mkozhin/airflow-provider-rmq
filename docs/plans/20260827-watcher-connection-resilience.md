# Живучесть watcher'а при разрывах соединения и сбоях инфраструктуры

## Overview

Watcher переживает разрыв связи с RabbitMQ, рестарт брокера, обрыв на прокси,
flow control и недоступность БД без ручного вмешательства: после устранения
внешней проблемы подписки восстанавливаются сами, а состояние, которое
показывает UI, соответствует тому, что видит брокер.

**Что произошло 2026-08-26 (инцидент, породивший план).** RabbitMQ перезагрузили.
Клиентское TCP-соединение не получило разрыв (AMQP heartbeat не согласован,
между Airflow и брокером стоит traefik, который держит клиентскую половину
открытой). `aio_pika` считал соединение живым, поэтому `connect_robust` не начал
реконнект; consumer-таска осталась висеть в `queue.iterator()` без исключений.
Брокер после рестарта про это соединение не знал: consumer'ов на очереди 0,
сообщения копились непрочитанными. `reconcile()` при этом исправно работал, но
судит о здоровье подписки по `entry.task.done()` (`consumer.py:190`) — таска не
завершена, значит «всё хорошо», и `_update_all_conn_counts` на том же основании
писал в БД `connected`. Страница Subscriptions показывала зелёный статус более
суток, до ручного рестарта шедулера. В логах за это время — ни одной записи.

**Ключевое свойство инцидента, определяющее дизайн:** соединение было
*зомби* — не закрытым, а притворяющимся живым. Поэтому ни проверка
`is_closed`, ни перезапуск таски поверх того же соединения проблему не решают:
всё, что пойдёт через это соединение, снова зависнет. Восстановление обязано
включать принудительное закрытие и пересоздание соединения, а зависание любой
операции по нему — считаться свидетельством смерти, а не отсутствием данных.

**Классы проблем, которые закрывает план:**

1. **Детект разрыва**: AMQP heartbeat для асинхронного пути и таймауты на всех
   сетевых операциях — чтобы разрыв превращался в исключение, а `connect_robust`
   получал повод переподключиться.
2. **Живучесть цикла**: ни один зависший `await` не блокирует reconcile навсегда;
   мёртвое или зомби-соединение выбрасывается из кеша; выход из итератора без
   исключения не превращается в busy-loop; flow control брокера не вешает
   consumer-таску.
3. **Watchdog**: подписка, которую брокер не видит, перезапускается принудительно
   вместе с пересозданием соединения, а не только по `task.done()`.
4. **Не терять события**: ACK после успешного `trigger_dag`, а не до; отказ
   триггера не превращается в горячий цикл редоставок; бинарный payload не
   роняет обработку после ACK.
5. **Честная наблюдаемость**: `status` в БД отражает состояние на стороне
   брокера, видно расхождение «сколько тасок запустили» и «сколько consumer'ов
   видит брокер», успешный старт и переподключение пишутся в лог.

**Отношение к плану `20260703-reliability-hardening.md`.** Тот план не начат и
остаётся как есть — этот план его не изменяет и не отменяет. Из него сюда
перенесены проработанные решения по темам, попадающим в скоуп живучести:
at-least-once в immediate-режиме (включая находку про `DagRunAlreadyExists`),
вынос синхронных вызовов в executor, graceful stop и диагностика `on_starting`,
часть statsd-метрик, а также decode-фикс в `triggers/rmq.py` (его Task 1) — там
строгий `decode("utf-8")` выполняется **после** ACK, то есть бинарное сообщение
теряется безвозвратно, что относится к классу проблем п.4 этого плана.

За планом 20260703 остаются: лидер-лок (HA), publisher confirms, гигиена
UI vs dag_file, валидация cooldown conn_id, decode-фикс в `hooks/rmq.py`
(там нет ACK-семантики, потери события не возникает) и метрика
`rmq_watcher.orphan_detected`. При его последующем исполнении пересекающиеся
задачи (там — Task 8, 10, 11, часть Task 1 и Task 14) окажутся уже
выполненными; сверить и отметить это — работа того плана, не этого.

## Context (from discovery)

- Затрагиваемые файлы: `airflow_provider_rmq/utils/amqp.py`,
  `utils/management.py`, `watcher/consumer.py`, `watcher/listener.py`,
  `watcher/models.py`, `watcher/views.py`,
  `watcher/templates/rmq_watcher/subscriptions.html`, `triggers/rmq.py`
  (общий `build_amqp_connection` и decode-фикс), `hooks/rmq.py` (только сверка
  имён ключей `extra`, не меняется).
- Ключевые места дефектов:
  - `utils/amqp.py:38-43` — AMQP URL собирается без query-параметров: ни
    `heartbeat`, ни таймаутов.
  - `consumer.py:365-397` — `_get_or_create_connection` кеширует соединение по
    `conn_id` и никогда не проверяет его состояние; `connect_robust` вызывается
    без `timeout`.
  - `consumer.py:190` — `reconcile` перезапускает подписку только по
    `entry.task.done()`.
  - `consumer.py:350-363` — `_update_all_conn_counts` пишет `connected` по числу
    незавершённых тасок, не обращаясь к брокеру; строка формируется **только**
    для `conn_id`, у которых есть хотя бы одна живая таска, поэтому при гибели
    всех тасок запись просто перестаёт обновляться.
  - `consumer.py:269-280`, `508-560` — provisioning-ветки (`_provision_cooldown`,
    `_provision_exchange_subs`) awaits `channel()`, declare и bind без таймаутов
    прямо внутри `reconcile`.
  - `consumer.py:599-607` — `while True` в `_consume_subscription` без задержки в
    ветке нормального выхода из итератора.
  - `consumer.py:616-623` — `publish` в pending-очередь без таймаута: при
    `connection.blocked` (memory/disk alarm) вешает consumer-таску.
  - `consumer.py:627-629` — `match_and_ack` подтверждает сообщение **до**
    `_trigger_dag`: ошибка триггера теряет событие.
  - `consumer.py:47-71` — `_sync_trigger` ловит `IntegrityError` (строка 70),
    тогда как при редоставке Airflow 2.9 бросает `DagRunAlreadyExists`.
  - `consumer.py` — `queue.iterator()` работает без `set_qos`: после перехода на
    ACK-после-триггера окно неподтверждённых сообщений становится
    неограниченным.
  - `triggers/rmq.py:90,96` — `match_and_ack` подтверждает сообщение, после чего
    строгий `decode("utf-8")` на бинарном payload бросает `UnicodeDecodeError`;
    событие уже подтверждено и теряется.
  - `listener.py:595-633` — тело итерации обёрнуто в `try/except Exception`
    **внутри** `while`; `asyncio.TimeoutError` в Python 3.11+ — алиас
    встроенного `TimeoutError` и подкласс `Exception`, поэтому таймаут,
    поставленный внутрь этого блока, будет молча поглощён.
  - `listener.py:635` — `_get_reconcile_interval()` (`listener.py:639-647`)
    вызывает `Variable.get`, то есть обращается к БД **вне** любого таймаута.
  - `listener.py:576-588` — `_run_loop` пересоздаёт event loop только по
    исключению из `_main`; `loop.close()` вызывается без
    `shutdown_default_executor()`.
  - `listener.py:544` — единственная строка, которую watcher пишет в лог при
    нормальной работе; успешный старт треда не логируется.
- Существующее состояние, которое переиспользуем вместо новых сущностей:
  - `RMQConnStatus.updated_at` (`models.py:58`) уже имеет `onupdate=func.now()`,
    то есть время последней записи статуса; отдельная колонка «время последнего
    цикла» была бы её дубликатом — вместо этого чинится узкий отбор `conn_id` в
    `_update_all_conn_counts`.
  - `hooks/rmq.py:139-147` уже читает `heartbeat` (дефолт 600) и
    `blocked_connection_timeout` (дефолт 300) из `extra` — синхронный путь
    настраиваем, асинхронный нет. Имена этих двух ключей в асинхронном пути
    берём те же.
- Паттерны проекта: graceful degradation в `reconcile` (сбой одной подписки не
  валит остальные), `run_in_executor` для синхронных вызовов, `OrphanTracker` как
  чистая алгебра множеств, дедупликация статусов через `_ConsumerState`.
- Тесты: pytest, `asyncio_mode = "auto"`, моки `aio_pika` в
  `tests/watcher/test_consumer.py` и `test_listener.py`, регрессия DagBag в
  `tests/watcher/test_example_dags_dagbag.py`. Существующие строгие ассерты на
  AMQP URL: `tests/test_amqp_utils.py:28,76`, `tests/test_trigger.py:298-300,360-367`.
- Airflow 3 вне скоупа (провайдер на ветке 2.x, pin `<3.0.0`).

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
- maintain backward compatibility: дефолты подобраны так, чтобы существующие
  установки получили защиту без правки Connection; поведенческие изменения
  (at-least-once, дефолтный heartbeat, prefetch) фиксируются в CHANGELOG
- новых регулярных записей в БД Airflow не появляется: диагностика кладётся в
  строку `rmq_conn_status`, которая и так перезаписывается каждый цикл

## Testing Strategy

- **unit tests**: обязательны в каждой задаче, в существующем стиле
  (`tests/watcher/test_consumer.py`, `test_listener.py`, `tests/test_amqp_utils.py`,
  `tests/test_trigger.py`)
- e2e-тестов в проекте нет; DagBag-регрессия example DAGs прогоняется в составе
  полного сьюта
- сценарии отказов моделируются моками:
  - зомби-соединение — мок, у которого `is_closed` возвращает `False`, а
    `channel()`/`declare_queue` никогда не резолвятся;
  - «немой» consumer — итератор без элементов при `consumer_count = 0` от
    Management API;
  - зависшая БД — `Variable.get`/сессия, блокирующиеся до таймаута теста.
- каждый сценарий инцидента должен иметь тест, доказывающий **восстановление**,
  а не только детект

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

Четыре независимых слоя защиты, каждый ловит то, что пропустил предыдущий:

1. **Детект разрыва (лечит корневую причину).** Heartbeat в AMQP URL: клиент сам
   замечает мёртвое соединение за ~2 интервала и `connect_robust` запускает
   реконнект. Таймауты на `connect`, `channel()`, `declare_queue`, bind и
   `publish` — включая provisioning-ветки внутри `reconcile`, чтобы ни одна
   сетевая операция не висела вечно. Регистрация `connection.blocked`/`unblocked`
   делает flow control видимым вместо тихого зависания публикации.
2. **Живучесть цикла (ловит зависания там, где heartbeat не помог).** Вся
   итерация reconcile выполняется под `asyncio.wait_for`, причём **снаружи**
   существующего `except Exception`, иначе таймаут был бы поглощён. Превышение
   бюджета времени доходит до `_run_loop`, который пересоздаёт event loop через
   30 с. Соединение, признанное мёртвым, закрывается и удаляется из кеша.
3. **Watchdog подписок (ловит зомби-соединение).** Раз в цикл проверяем, видит ли
   брокер нашего consumer'а. Отрицательный результат дважды подряд →
   принудительное закрытие соединения этого `conn_id` и пересоздание тасок.
   Зависание самой проверки считается отрицательным результатом, а не
   отсутствием данных: именно так выглядел инцидент.
4. **Честная наблюдаемость (делает следующий сбой видимым).** `status` пишется по
   результату проверки у брокера; рядом с «сколько тасок мы запустили» видно
   «сколько consumer'ов видит брокер»; строка статуса обновляется для каждого
   `conn_id`, включая те, где не осталось живых тасок; успешный старт и каждое
   переподключение логируются.

Плюс отдельная линия, не связанная с сетью, но той же природы («тихая потеря»):
ACK после успешного `trigger_dag`, backoff при устойчивом отказе триггера,
prefetch на consumer'е и снятие строгого decode с пути после ACK.

### Ключевые решения

- **Константы в коде + override через `extra`** (не только константы и не только
  `extra`): существующие Connection не имеют нужных ключей, поэтому дефолт должен
  давать защиту сам по себе; при этом сеть у всех разная, и число должно
  меняться без релиза провайдера. Ключи `heartbeat` и `blocked_connection_timeout`
  совпадают с теми, что уже понимает синхронный хук; `connect_timeout` и
  `rpc_timeout` — новые, у pika-пути аналогов нет.
- **Зависание AMQP-операции = свидетельство смерти соединения, а не «нет
  данных».** Ошибка Management API (HTTP, таймаут httpx) — действительно «нет
  данных», и на неё watchdog не реагирует. Но зависшая AMQP-проба означает ровно
  инцидентное состояние; трактовать её как «нет данных» значило бы не лечить
  главный сценарий.
- **Восстановление = пересоздание соединения, а не только таски.** Перезапуск
  таски поверх зомби-соединения бесполезен: новая таска получит из кеша тот же
  объект и повиснет так же.
- **Таймаут на итерацию, а не сторожевой тред**: `_run_loop` уже содержит готовый
  механизм восстановления, ему не хватает только повода сработать. Внешний тред
  ловил бы вдобавок полную блокировку event loop синхронным вызовом — эта угроза
  закрывается Task 8 (вынос синхронных вызовов в executor), поэтому второй
  механизм не нужен.
- **Цена срабатывания watchdog'а цикла принимается осознанно.** `TimeoutError`
  проходит через `finally: await self._manager.stop()`, то есть отменяет все
  таски и закрывает все соединения, после чего `_run_loop` ждёт 30 с. Один
  медленный цикл = пауза потребления по всем `conn_id`. Поэтому бюджет цикла
  берётся щедрым (`max(interval * 3, 300)`), а точечные зависания ловятся
  таймаутами слоя 1, которые срабатывают гораздо раньше и локально.
- **Отдельная колонка «время последнего цикла» не заводится**: `updated_at` с
  `onupdate=func.now()` уже несёт это значение. Настоящий дефект не в отсутствии
  колонки, а в том, что строка обновляется не для всех `conn_id`.
- **TCP keepalive в скоуп не входит**: `aio_pika`/`aiormq` не экспонируют socket
  options, а `client_properties` к сокету отношения не имеет. При `heartbeat=30`
  разрыв детектируется за ~60 с, и выигрыш keepalive маргинален. Решение
  фиксируется в ADR, чтобы к нему не возвращались.

## Technical Details

### Параметры соединения (`utils/amqp.py`)

```
DEFAULT_HEARTBEAT = 30            # сек; детект разрыва за ~60 с (2 пропуска)
DEFAULT_CONNECT_TIMEOUT = 15      # сек; на установку соединения
DEFAULT_RPC_TIMEOUT = 30          # сек; на channel()/declare/bind/publish
DEFAULT_BLOCKED_TIMEOUT = 300     # сек; ожидание снятия connection.blocked
```

- `build_amqp_connection` добавляет к URL query-строку с `heartbeat`, взятым из
  `extra` (ключ `heartbeat`) или из константы. Значение `0` допускается явно —
  оператор может выставить его осознанно, — но логируется WARNING: детект
  разрыва в этом случае не работает вовсе.
- `get_amqp_timeouts(conn_info) -> AmqpTimeouts` (dataclass с полями `connect`,
  `rpc`, `blocked`) — читает `extra`, подставляет дефолты, валидирует
  (положительные числа; мусор → дефолт + WARNING). Возвращается отдельно от
  URL, потому что это параметры вызовов, а не строки подключения.
- Изменение URL затрагивает `triggers/rmq.py` (deferrable-сенсор в push-режиме) —
  он использует тот же `build_amqp_connection` и получает heartbeat бесплатно,
  но существующие строгие ассерты на URL в тестах требуют обновления.
- `hooks/rmq.py` не меняется: pika уже получает `heartbeat` и
  `blocked_connection_timeout` из `extra`. Дефолт `heartbeat=600` там остаётся —
  синхронный хук живёт короткими сессиями внутри задачи.

### Соединения и таймауты (`consumer.py`)

- `_get_or_create_connection`: перед возвратом из кеша — проверка
  `connection.is_closed` (на быстром пути и под локом); закрытое соединение
  удаляется и создаётся заново. Это ловит честно закрытые соединения; зомби
  ловит watchdog (Task 5-6).
- Новый метод `_drop_connection(conn_id)` — best-effort `close()` под коротким
  `wait_for` и удаление из `self._connections`. Используется watchdog'ом и при
  фатальных ошибках соединения.
- `aio_pika.connect_robust(..., timeout=timeouts.connect)`.
- `asyncio.wait_for(..., timeout=timeouts.rpc)` вокруг всех AMQP-RPC:
  `connection.channel()`, `declare_queue`, `publish` — как в consumer-ветках
  (`_consume_subscription`, `_consume_fire_queue`), так и в provisioning-ветках
  (`_provision_cooldown`, `_provision_exchange_subs`, включая `_ensure_*`
  declare/bind). `TimeoutError` трактуется существующими ветками retry как
  транзиентная ошибка.
- `channel.set_qos(prefetch_count=_PREFETCH_COUNT)` (константа = 1) сразу после
  открытия канала в consumer-ветках: после перехода на ACK-после-триггера
  неограниченный prefetch означал бы лавину редоставок при обрыве.
- Регистрация `connection.blocked`/`connection.unblocked` (колбэки aio_pika):
  при блокировке — WARNING и статус `blocked` в `rmq_conn_status`, при снятии —
  возврат к обычному статусу. `timeouts.blocked` используется как верхняя
  граница ожидания снятия блокировки перед тем, как признать соединение
  непригодным.
- Ветка нормального выхода из `queue.iterator()` получает
  `await asyncio.sleep(_RECONNECT_DELAY)` перед новой итерацией `while True`.

### Проверка живости и восстановление (`consumer.py`, `utils/management.py`)

`_check_subscription_liveness(subscriptions) -> tuple[set[int], set[str]]`
возвращает `sub_id` на перезапуск и `conn_id` на пересоздание. Вызывается из
`reconcile()` после блока старта/отмены тасок.

- Для каждого `conn_id` с активными подписками:
  - есть `management_url` → один запрос списка очередей vhost'а; подписка жива,
    если `consumers > 0` по её очереди;
  - нет `management_url` (или `self._http_client is None`) → passive
    `queue_declare` на отдельном канале: успех означает, что соединение реально
    работает; результат применяется ко всем подпискам этого `conn_id`.
- **Классификация исходов проверки:**

  | Исход | Трактовка | Действие |
  |---|---|---|
  | Management API вернул данные, `consumers > 0` | жива | сброс счётчика |
  | Management API вернул данные, `consumers == 0` | мертва | +1 к счётчику |
  | Management API недоступен (HTTP-ошибка, таймаут httpx) | нет данных | WARNING, счётчик не трогать |
  | AMQP-проба прошла | жива | сброс счётчика |
  | AMQP-проба упала или зависла (`wait_for`) | мертва | +1 к счётчику |

- Кандидатом считается только подписка с `consumer_status == "listening"` и
  незавершённой таской: `connecting`/`error` восстанавливаются своим retry-циклом.
- Перезапуск — после **двух подряд** отрицательных проверок (счётчик в
  `_ActiveSub`), то есть не раньше двух reconcile-интервалов.
- Восстановление выполняется в порядке: `task.cancel()` для всех подписок этого
  `conn_id` → `_drop_connection(conn_id)` → создание новых тасок (они поднимут
  новое соединение через `_get_or_create_connection`). Плюс WARNING с причиной и
  `Stats.incr("rmq_watcher.consumer_restarted")`.

### Статусы (`consumer.py::_update_all_conn_counts`)

- Строка пишется для **каждого** `conn_id`, встречающегося в списке подписок, а
  не только для тех, где есть живая таска: иначе при гибели всех тасок запись
  залипает на последнем значении (ровно это и наблюдалось в инциденте).
- `status` определяется результатом проверки живости, а не числом тасок:
  - подтверждение брокером → `connected`;
  - отрицательная проверка → `error` с текстом причины;
  - «нет данных» → предыдущий `status` сохраняется (недоступность Management API
    не должна красить всё в красный);
  - активная блокировка → `blocked`.
- `consumer_count` сохраняет прежний смысл «сколько тасок мы запустили»;
  `broker_consumer_count` — «сколько consumer'ов видит брокер», `NULL` при «нет
  данных». Расхождение и есть искомый сигнал.

### Watchdog цикла (`listener.py`)

- Тело итерации выносится в `_run_cycle()`. Существующий
  `except Exception: log.exception(...)` перемещается **внутрь** `_run_cycle`,
  так что снаружи остаётся только `await asyncio.wait_for(self._run_cycle(), ...)`.
  Без этого `TimeoutError` (подкласс `Exception` в Python 3.11+) был бы поглощён
  и весь слой 2 оказался бы no-op.
- Бюджет `_cycle_timeout()` = `max(reconcile_interval * 3, 300)`,
  переопределяется переменной `rmq_watcher_cycle_timeout`.
- `reconcile_interval` и `cycle_timeout` читаются через `Variable.get`, то есть
  ходят в БД, и вычисляются **до** входа в `wait_for`. Поэтому оба кешируются в
  атрибутах и обновляются не чаще раза в N циклов, а сам вызов `Variable.get`
  идёт через executor под коротким `wait_for` с падением на последнее известное
  (или дефолтное) значение. Иначе зависшая БД вешает петлю в точке, которую
  watchdog принципиально не покрывает.
- Текущая фаза цикла (scan / sync / read subs / reconcile) пишется в атрибут
  перед каждым шагом и попадает в ERROR при таймауте вместе с длительностью.
- `finally: await self._manager.stop()` оборачивается собственным коротким
  `wait_for`; неудача логируется и не мешает пересозданию loop.
- `_run_loop` перед `loop.close()` выполняет `shutdown_default_executor()` под
  таймаутом: иначе потоки, заблокированные на мёртвой БД, накапливаются при
  каждом пересоздании петли.

### At-least-once в immediate-режиме (`consumer.py`, `triggers/rmq.py`)

- `_sync_trigger` возвращает `"triggered" | "skipped" | "duplicate"`; исключения
  пробрасывает, кроме двух, означающих дубль:
  - `airflow.exceptions.DagRunAlreadyExists` — основной случай при редоставке: в
    Airflow 2.9+ `trigger_dag` вызывает `DagRun.find_duplicate(...)` и бросает это
    исключение **до** INSERT, и оно **не** является подклассом `IntegrityError`,
    поэтому текущий `except IntegrityError` (`consumer.py:70`) реальную
    редоставку не ловит (та же ошибка есть в fire-консьюмере cooldown и
    исправляется здесь же, так как `_sync_trigger` общий);
  - `sqlalchemy.exc.IntegrityError` — страховка от гонки двух процессов.
- `_build_run_id(queue_name, message_id)`: при наличии AMQP `message_id` —
  детерминированный `rmq__{queue}__{message_id}`, иначе текущий timestamp-формат.
- Порядок в immediate-ветке: `_match` (без ACK) → при совпадении `_trigger_dag`
  → `message.ack()`; исключение из триггера → WARNING + NACK с requeue.
- **Backoff при отказе триггера.** `_nack_and_sleep` спит 0.1 с
  (`utils/amqp.py:62-68`) — это защита от горячей редоставки при НЕсовпадении
  фильтра, и для устойчивого отказа триггера она непригодна: ~10 редоставок/с
  исчерпают delivery-limit quorum-очереди (по умолчанию 20) примерно за две
  секунды, и сообщение уедет в dead-letter — то есть будет потеряно тем самым
  механизмом, который мы чиним. Поэтому ветка «триггер упал» получает
  собственный растущий backoff (1 с → удвоение → потолок 60 с, сброс при
  успехе), состояние живёт в `_consume_subscription`.
- Граница гарантии (фиксируется в README): исход `"skipped"` (DAG paused,
  inactive или отсутствует) — терминальный ACK. NACK здесь превратил бы паузу
  DAG'а в накопитель редоставок.
- Дедуп доверяет уникальности `message_id` продюсера: два разных сообщения с
  одинаковым `message_id` схлопнутся в один запуск.
- `triggers/rmq.py:96`: `message.body.decode("utf-8")` выполняется после ACK
  (`triggers/rmq.py:90`) и на бинарном payload бросает `UnicodeDecodeError`,
  теряя уже подтверждённое событие. Заменяется на
  `decode("utf-8", errors="replace")` — как в `utils/amqp.py::match`.

### Диагностика в UI (`models.py`, `views.py`, шаблон)

- В `rmq_conn_status` добавляется одна колонка `broker_consumer_count`
  (Integer, nullable).
- `create_all(checkfirst=True)` (`models.py:72`) новые колонки в существующую
  таблицу не добавляет, поэтому `ensure_table_exists` дополняется
  диалект-независимой миграцией: `sqlalchemy.inspect(engine).get_columns(...)` →
  `ALTER TABLE ... ADD COLUMN` (без `IF NOT EXISTS`, который есть только в
  PostgreSQL/MariaDB и упадёт синтаксической ошибкой на SQLite, где гоняются
  тесты) для отсутствующих колонок, всё под `try/except` с WARNING.
- На странице Subscriptions: возраст `updated_at` по каждому `conn_id`,
  предупреждающая пометка, когда он старше двух reconcile-интервалов, и
  `broker_consumer_count` рядом с `consumer_count` с пометкой расхождения.
- Вью живёт в процессе webserver'а и не имеет доступа к состоянию listener'а,
  поэтому интервал читает сама: `Variable.get("rmq_watcher_reconcile_interval")`
  под `try/except` с фолбэком на `_DEFAULT_RECONCILE_INTERVAL`.
- Время сравнивается как наивный UTC: `updated_at` заполняется `func.now()`
  (наивное серверное время БД), поэтому вью использует `datetime.utcnow()`;
  смешение naive и aware дало бы `TypeError` прямо в рендере.

### Метрики (`airflow.stats.Stats`, no-op без statsd)

`rmq_watcher.consumer_reconnect`, `rmq_watcher.consumer_restarted`,
`rmq_watcher.cycle_timeout`, `rmq_watcher.dag_triggered`. Вызовы обёрнуты так,
что сбой Stats не влияет на поток управления.

## What Goes Where

- **Implementation Steps**: изменения кода, тестов и документации этого репозитория.
- **Post-Completion**: проверка на живом стенде, настройки инфраструктуры
  (heartbeat брокера, idle-timeout traefik), релиз.

## Implementation Steps

### Task 1: Heartbeat и таймауты в параметрах AMQP-соединения

**Files:**
- Modify: `airflow_provider_rmq/utils/amqp.py`
- Modify: `tests/test_amqp_utils.py`
- Modify: `tests/test_trigger.py`

- [ ] добавить константы `DEFAULT_HEARTBEAT`, `DEFAULT_CONNECT_TIMEOUT`, `DEFAULT_RPC_TIMEOUT`, `DEFAULT_BLOCKED_TIMEOUT`
- [ ] `build_amqp_connection`: добавить в URL query-параметр `heartbeat` из `extra` (ключ `heartbeat`, как в `hooks/rmq.py:145`) или из константы
- [ ] `heartbeat=0` в `extra` принимается, но логируется WARNING о том, что детект разрыва отключён
- [ ] добавить `get_amqp_timeouts(conn_info)` → dataclass `AmqpTimeouts(connect, rpc, blocked)` с чтением `extra`, дефолтами и валидацией (нечисловое/неположительное → дефолт + WARNING)
- [ ] обновить существующие строгие ассерты на URL: `tests/test_amqp_utils.py:28,76` и `tests/test_trigger.py:298-300,360-367`
- [ ] тест: URL содержит `heartbeat` из дефолта и из `extra`; схема, credentials, порт и vhost не меняются
- [ ] тест: `heartbeat=0` → параметр в URL, WARNING залогирован
- [ ] тест: `get_amqp_timeouts` — дефолты, override из `extra`, мусорные значения (строка, отрицательное число, `None`)
- [ ] тест: экранирование credentials и vhost сохраняется при добавлении query-строки
- [ ] run tests - must pass before task 2

### Task 2: Таймауты AMQP-операций, prefetch и обработка flow control

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`

- [ ] `_get_or_create_connection`: проверять `connection.is_closed` на быстром пути и под локом; закрытое соединение удалять из `self._connections` и пересоздавать
- [ ] добавить `_drop_connection(conn_id)` — best-effort `close()` под коротким `wait_for` + удаление из кеша
- [ ] `connect_robust` вызывать с `timeout` из `get_amqp_timeouts`
- [ ] обернуть в `asyncio.wait_for` с `rpc`-таймаутом все AMQP-RPC в consumer-ветках: `channel()`, `declare_queue`, `publish` в cooldown-ветке
- [ ] обернуть тем же таймаутом AMQP-RPC в provisioning-ветках: `_provision_cooldown` (`consumer.py:269-280`) и `_provision_exchange_subs` (`consumer.py:508-560`), включая declare/bind внутри `_ensure_*`
- [ ] добавить `channel.set_qos(prefetch_count=_PREFETCH_COUNT)` (константа = 1) после открытия канала в consumer-ветках
- [ ] зарегистрировать колбэки `connection.blocked`/`unblocked`: WARNING и статус `blocked`, снятие блокировки возвращает обычный статус
- [ ] добавить `await asyncio.sleep(_RECONNECT_DELAY)` в ветке нормального выхода из `queue.iterator()` (защита от busy-loop)
- [ ] тест: закрытое соединение в кеше → создаётся новое, старое удалено из `_connections`
- [ ] тест: живое соединение в кеше → повторный `connect_robust` не вызывается
- [ ] тест: `_drop_connection` удаляет соединение из кеша даже когда `close()` зависает
- [ ] тест: зависший `channel()` → `TimeoutError` обрабатывается как транзиентная ошибка, таска уходит в retry и не завершается
- [ ] тест: зависший `declare` в провижининге не подвешивает `reconcile`
- [ ] тест: зависший `publish` в cooldown-ветке → таска не висит вечно
- [ ] тест: `set_qos` вызывается с ожидаемым prefetch
- [ ] тест: `connection.blocked` → статус `blocked`, `unblocked` → возврат к обычному
- [ ] тест: итератор завершился без исключения → пауза перед повторной подпиской (нет busy-loop)
- [ ] run tests - must pass before task 3

### Task 3: Колонка broker_consumer_count и диалект-независимая миграция

**Files:**
- Modify: `airflow_provider_rmq/watcher/models.py`
- Modify: `tests/watcher/test_models.py`

- [ ] добавить в `RMQConnStatus` колонку `broker_consumer_count` (Integer, nullable)
- [ ] расширить `upsert_conn_status` необязательным параметром для неё; сентинел «не менять» отличается от явного `None` («данных нет»)
- [ ] дополнить `ensure_table_exists` миграцией через `sqlalchemy.inspect(engine).get_columns(...)` + `ALTER TABLE ... ADD COLUMN` для отсутствующих колонок, под `try/except` с WARNING (без `IF NOT EXISTS` — он не поддерживается SQLite)
- [ ] тест: `upsert_conn_status` пишет и обновляет `broker_consumer_count`
- [ ] тест: вызов без параметра сохраняет ранее записанное значение; явный `None` записывает «нет данных»
- [ ] тест: `ensure_table_exists` на таблице без новой колонки добавляет её; повторный вызов не падает (SQLite)
- [ ] run tests - must pass before task 4

### Task 4: Watchdog итерации reconcile-цикла

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] вынести тело итерации `_main` в корутину `_run_cycle()`, **переместив существующий `except Exception` внутрь неё** — снаружи остаётся только `wait_for`, иначе `TimeoutError` будет поглощён
- [ ] вызывать `_run_cycle()` через `asyncio.wait_for` с бюджетом `_cycle_timeout()` = `max(reconcile_interval * 3, 300)`, переопределяемым переменной `rmq_watcher_cycle_timeout`
- [ ] кешировать `reconcile_interval` и `cycle_timeout` в атрибутах, обновляя не чаще раза в N циклов; сам `Variable.get` — через executor под коротким `wait_for` с фолбэком на последнее известное или дефолтное значение
- [ ] писать текущую фазу цикла в атрибут перед каждым шагом (scan / sync / read subs / reconcile)
- [ ] при `TimeoutError` — ERROR с фазой и длительностью, `Stats.incr("rmq_watcher.cycle_timeout")`, проброс наружу `_main`, чтобы `_run_loop` пересоздал event loop
- [ ] обернуть `await self._manager.stop()` в `finally` собственным коротким `wait_for`; неудачу логировать
- [ ] `_run_loop`: перед `loop.close()` выполнять `shutdown_default_executor()` под таймаутом (иначе потоки, заблокированные на мёртвой БД, накапливаются при каждом пересоздании петли)
- [ ] тест: `TimeoutError` не поглощается обработчиком цикла и доходит до `_run_loop` (регрессия на порядок вложенности try/except)
- [ ] тест: зависшая фаза цикла → петля пересоздаётся
- [ ] тест: зависший `Variable.get` → цикл продолжается с последним известным/дефолтным интервалом
- [ ] тест: `_cycle_timeout` учитывает интервал, нижнюю границу и Airflow-переменную
- [ ] тест: зависший `manager.stop()` не блокирует пересоздание loop
- [ ] тест: пересоздание петли не оставляет висящий executor
- [ ] run tests - must pass before task 5

### Task 5: Проверка живости подписок у брокера

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `airflow_provider_rmq/utils/management.py`
- Modify: `tests/watcher/test_consumer.py`
- Modify: `tests/test_management_utils.py`

- [ ] добавить в `utils/management.py` функцию получения `consumers` по очередям vhost'а через Management API (в стиле существующих вызовов)
- [ ] добавить `_check_subscription_liveness(subscriptions)` → `(sub_id на перезапуск, conn_id на пересоздание)`
- [ ] ветка с `management_url`: подписка жива при `consumers > 0`
- [ ] ветка без `management_url` (в том числе при `self._http_client is None`): passive `queue_declare` на отдельном канале под `wait_for`
- [ ] реализовать классификацию исходов из Technical Details: ошибка Management API = «нет данных» (счётчик не трогать); падение **или зависание** AMQP-пробы = отрицательная проверка
- [ ] считать кандидатом только подписку с `consumer_status == "listening"` и незавершённой таской
- [ ] требовать двух подряд отрицательных проверок (счётчик в `_ActiveSub`); успешная проверка сбрасывает счётчик
- [ ] тест: брокер сообщает 0 consumer'ов дважды подряд → подписка в наборе на перезапуск
- [ ] тест: одна отрицательная проверка → перезапуска нет
- [ ] тест: Management API недоступен → перезапусков нет, WARNING залогирован, счётчик не изменился
- [ ] тест: **passive declare зависает** → это отрицательная проверка; после двух циклов подписка признана мёртвой (сценарий инцидента)
- [ ] тест: `_http_client is None` → используется AMQP-проба, `reconcile` не падает
- [ ] тест: подписка в статусе `connecting`/`error` кандидатом не становится
- [ ] run tests - must pass before task 6

### Task 6: Восстановление — пересоздание соединения и честный статус

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`

- [ ] вызывать `_check_subscription_liveness` в конце `reconcile()`
- [ ] восстановление в порядке: `task.cancel()` для подписок этого `conn_id` → `_drop_connection(conn_id)` → создание новых тасок
- [ ] логировать WARNING с причиной перезапуска и инкрементировать `rmq_watcher.consumer_restarted`
- [ ] `_update_all_conn_counts`: писать строку для **каждого** `conn_id` из списка подписок, включая те, где не осталось живых тасок
- [ ] `_update_all_conn_counts`: определять `status` результатом проверки живости — `connected` при подтверждении, `error` при отрицательной проверке, сохранение прежнего при «нет данных», `blocked` при активной блокировке
- [ ] записывать `broker_consumer_count` (или `None` при «нет данных»)
- [ ] тест: после перезапуска мёртвой подписки `connect_robust` вызван заново, старое соединение удалено из кеша (регрессия на зомби-соединение)
- [ ] тест: брокер видит 0 consumer'ов → `status != "connected"`
- [ ] тест: «нет данных» → прежний `status` сохранён, `broker_consumer_count` записан как `None`
- [ ] тест: `conn_id` без живых тасок всё равно получает обновление строки
- [ ] тест: живые подписки не перезапускаются (существующее поведение `reconcile` не меняется)
- [ ] run tests - must pass before task 7

### Task 7: At-least-once, backoff и снятие строгого decode

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `airflow_provider_rmq/triggers/rmq.py`
- Modify: `tests/watcher/test_consumer.py`
- Modify: `tests/test_trigger.py`

- [ ] `_sync_trigger`: возвращает `"triggered" | "skipped" | "duplicate"`; `DagRunAlreadyExists` и `IntegrityError` → INFO + `"duplicate"`; прочие исключения пробрасываются
- [ ] `_build_run_id(queue_name, message_id)`: детерминированный `rmq__{queue}__{message_id}` при наличии `message_id`, иначе timestamp-формат
- [ ] immediate-ветка `_consume_subscription`: `_match` без ACK → при совпадении `_trigger_dag` → `message.ack()`; при исключении из триггера → WARNING + NACK с requeue
- [ ] добавить растущий backoff для ветки отказа триггера (1 с → удвоение → потолок 60 с, сброс при успехе) вместо фиксированных 0.1 с из `_nack_and_sleep`
- [ ] `_trigger_dag` принимает `message_id` и возвращает исход; удалить ставший неиспользуемым импорт `match_and_ack` в `consumer.py`
- [ ] инкрементировать `rmq_watcher.dag_triggered` при исходе `"triggered"`
- [ ] `triggers/rmq.py:96`: `decode("utf-8", errors="replace")` — бинарный payload после ACK не должен ронять обработку
- [ ] тест: успешный триггер → `ack` вызван после `trigger_dag` (проверка порядка)
- [ ] тест: `trigger_dag` бросает → `nack`+requeue, `ack` не вызван
- [ ] тест: повторный отказ триггера наращивает паузу и не даёт горячего цикла редоставок
- [ ] тест: `DagRunAlreadyExists` → исход `"duplicate"`, `ack`, `nack` не вызван
- [ ] тест: `IntegrityError` → исход `"duplicate"`, `ack`
- [ ] тест: `run_id` детерминирован при `message_id`, timestamp-fallback без него
- [ ] тест: paused/inactive DAG → исход `"skipped"`, терминальный `ack`
- [ ] тест: бинарный payload в триггере не бросает `UnicodeDecodeError`
- [ ] run tests - must pass before task 8

### Task 8: Вынос синхронных вызовов из event loop

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_listener.py`
- Modify: `tests/watcher/test_consumer.py`

- [ ] `_run_cycle`: `_scan_subscriptions` и `_sync_to_db` через `run_in_executor`
- [ ] `_run_cycle`: чтение `get_enabled_subscriptions` (синхронный SQLAlchemy-запрос в корутине) через `run_in_executor`
- [ ] `_ConsumerState.write` → `async def` с записью в БД через `run_in_executor`; обновить колл-сайты
- [ ] записи статусов в `reconcile` (`set_consumer_status` при удалении подписки, `_update_all_conn_counts`) и `upsert_conn_status` в error-ветке `_get_or_create_connection` — через `run_in_executor`
- [ ] тест: scan/sync/чтение подписок уходят через executor
- [ ] тест: `Variable.get` уходит через executor
- [ ] тест: `_ConsumerState.write` сохраняет дедупликацию статусов
- [ ] тест: записи статусов в `reconcile` идут через executor
- [ ] тест: зависший executor-вызов приводит к срабатыванию cycle timeout, а не к вечному ожиданию
- [ ] run tests - must pass before task 9

### Task 9: Graceful stop и диагностика жизненного цикла

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] `_main`: `asyncio.Event` для пробуждения; ссылки на loop и event хранятся в listener'е и обновляются при каждом пересоздании петли
- [ ] `before_stopping`: `threading.Event.set()` как авторитетный сигнал + `loop.call_soon_threadsafe(async_event.set)` под guard'ом (`loop.is_closed()` / `except RuntimeError`) + `thread.join(timeout=5)`
- [ ] `_main`: `asyncio.wait_for(event.wait(), timeout=interval)` вместо `asyncio.sleep(interval)`
- [ ] дополнить существующий лог `on_starting` (`listener.py:544-547`) явной причиной «watcher not started» при нераспознанном компоненте — не добавляя вторую запись рядом
- [ ] INFO-лог при успешном старте треда (с интервалом цикла и бюджетом таймаута)
- [ ] INFO-лог при каждом успешном (пере)подключении к брокеру с `conn_id` и именем очереди; инкремент `rmq_watcher.consumer_reconnect`
- [ ] тест: `before_stopping` будит петлю немедленно, `manager.stop()` вызван
- [ ] тест: `before_stopping` при закрытом loop не бросает, `threading.Event` выставлен
- [ ] тест: нераспознанный компонент → лог с причиной, тред не стартует
- [ ] тест: успешный старт треда логируется
- [ ] тест: успешное переподключение логируется и инкрементирует метрику
- [ ] run tests - must pass before task 10

### Task 10: Отображение живости на странице Subscriptions

**Files:**
- Modify: `airflow_provider_rmq/watcher/views.py`
- Modify: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`
- Modify: `tests/watcher/test_views.py`

- [ ] выводить возраст `updated_at` для каждого `conn_id`
- [ ] читать интервал во вью через собственный `Variable.get("rmq_watcher_reconcile_interval")` под `try/except` с фолбэком на `_DEFAULT_RECONCILE_INTERVAL`
- [ ] помечать предупреждением `conn_id`, у которого `updated_at` старше двух интервалов; сравнение вести в наивном UTC (`func.now()` пишет наивное серверное время)
- [ ] показывать `broker_consumer_count` рядом с `consumer_count` и помечать расхождение
- [ ] отображать `—` вместо числа, когда `broker_consumer_count` равен `NULL`
- [ ] тест: свежий `updated_at` → пометки нет; устаревший → пометка есть
- [ ] тест: наивная дата в колонке не вызывает `TypeError` при рендеринге
- [ ] тест: расхождение счётчиков отражается в выводе
- [ ] тест: `NULL` в `broker_consumer_count` не ломает рендеринг
- [ ] тест: недоступная Airflow-переменная → используется дефолтный интервал
- [ ] run tests - must pass before task 11

### Task 11: ADR по стратегии живучести

**Files:**
- Create: `docs/adr/0007-connection-liveness-two-tier-check.md`

- [ ] зафиксировать решение: живость подписки определяется по данным брокера, а не по `task.done()`; два уровня проверки (Management API и passive declare) и почему нужен fallback
- [ ] зафиксировать, что зависание AMQP-пробы трактуется как свидетельство смерти соединения, а ошибка Management API — как отсутствие данных
- [ ] зафиксировать, что восстановление включает пересоздание соединения, а не только перезапуск таски
- [ ] зафиксировать выбор таймаута на итерацию вместо сторожевого треда, принятую цену срабатывания (пауза потребления по всем `conn_id`) и условие пересмотра
- [ ] зафиксировать отказ от отдельной колонки «время последнего цикла» в пользу `updated_at` и почему TCP keepalive остался вне скоупа
- [ ] сослаться на инцидент 2026-08-26 как на источник решения

### Task 12: Verify acceptance criteria

- [ ] все пункты Overview реализованы, крайние случаи из Technical Details покрыты
- [ ] полный тестовый сьют: `pytest`
- [ ] DagBag-регрессия example DAGs проходит
- [ ] `ruff check .` без новых замечаний
- [ ] сквозная проверка сценария инцидента на моках: зомби-соединение (`is_closed == False`, операции не резолвятся) → watchdog признаёт подписку мёртвой за два цикла → соединение пересоздано → потребление возобновилось
- [ ] проверить, что при недоступном брокере статусы в БД становятся `error`, а не остаются `connected`
- [ ] проверить, что недоступность Management API не приводит ни к перезапускам, ни к смене статуса на `error`

### Task 13: [Final] Update documentation

- [ ] CHANGELOG: heartbeat по умолчанию, таймауты AMQP-операций, prefetch, обработка flow control, watchdog подписок и цикла, at-least-once в immediate-режиме, колонка `broker_consumer_count`
- [ ] readme.md: раздел про устойчивость соединения — доступные ключи `extra` (`heartbeat`, `blocked_connection_timeout` — общие с синхронным хуком; `connect_timeout`, `rpc_timeout` — только для асинхронного пути), что показывает страница Subscriptions, граница гарантии at-least-once (paused/inactive DAG = терминальный ACK, дедуп доверяет `message_id` продюсера)
- [ ] readme_ru.md: те же разделы
- [ ] CONTEXT.md: термины **Liveness watchdog** и **Cycle timeout** в глоссарий
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Проверка на стенде:**
- имитировать инцидент: оборвать TCP между Airflow и RabbitMQ так, чтобы клиент
  не получил RST (правило firewall с DROP, не REJECT), и убедиться, что consumer
  восстанавливается сам в пределах heartbeat + двух reconcile-интервалов, без
  рестарта контейнеров
- отдельно проверить рестарт брокера: восстановление должно происходить по тому
  же сценарию
- убедиться, что страница Subscriptions в момент обрыва показывает не-зелёное
  состояние, а не замерший `connected`
- проверить поведение при недоступной БД Airflow в момент прихода сообщения:
  сообщение возвращается в очередь с растущей паузой и обрабатывается после
  восстановления БД, не исчерпав delivery-limit
- проверить flow control: перевести брокер в состояние memory alarm и убедиться,
  что статус меняется на `blocked`, а consumer-таска не виснет

**Настройки инфраструктуры (вне репозитория):**
- проверить heartbeat на брокере (`rabbitmqctl environment | grep -i heartbeat`):
  при значении `0` серверная сторона heartbeat не согласует, и клиентский
  параметр остаётся единственной защитой
- проверить idle-timeout TCP-роутера traefik для AMQP: он должен быть заметно
  больше heartbeat-интервала, иначе прокси будет рвать здоровые соединения
- сверить, что `management_url` заполнен в используемых Airflow Connection —
  без него watchdog работает в ограниченном режиме (passive declare)
- при использовании quorum-очередей сверить `delivery-limit` с логикой backoff:
  сообщения, которые не удаётся обработать, уходят в dead-letter после
  исчерпания лимита

**Релиз:**
- версия провайдера с этими изменениями выкатывается на PyPI; после обновления
  рестарт шедулера обязателен (плагин загружается при старте процесса)
