# Правки по третьему аудиту ветки rmq-watcher-connection-resilience

## Overview

Третий независимый прогон ревью по ветке `rmq-watcher-connection-resilience`
дал 11 находок: 2 major, 6 minor, 3 pre-existing и 1 immaterial. План закрывает
их все — правками в коде и тестах там, где дефект принадлежит ветке, и записью
в `docs/backlog.md` там, где он существует и в `main`.

**Главное, ради чего план написан.** Обе major бьют в одно и то же свойство,
которое ветка обязана обеспечивать: статус подписки в UI должен соответствовать
тому, что видит брокер.

1. `_raised_while_cancelling` принимает обычную транзиентную ошибку за отмену.
   Consumer-таска завершается через ветку `except asyncio.CancelledError: return`
   — без записи статуса и без перезапуска, — а строка подписки продолжает
   показывать `listening` при отсутствующем consumer'е до следующего цикла
   reconcile. Это ровно тот ложный зелёный статус, ради устранения которого
   написана вся ветка, вернувшийся с другой стороны.

2. Кэш ответа Management API склеивает разных пользователей, поэтому conn_id,
   чьи consumer-теги не попали в чужой закэшированный ответ, получает
   отрицательный вердикт живости каждый цикл — при полностью здоровом
   consumer'е. Пересоздание соединения с перезапуском подписок при этом
   ограничено `_CYCLES_BEFORE_REDROP = 5` (`consumer.py:119`,
   `_may_drop_connection` в `consumer.py:2098`): между пересозданиями строка
   conn_id читается `degraded`.

**Формы исключений, определяющие дизайн правки.** Проверено запуском в
`.venv` репозитория:

```
1. чужое исключение внутри except CancelledError (форма aio_pika):
     cause=None            ctx=CancelledError  suppress=False   → отмена
2. raise X from None внутри except CancelledError:
     cause=None            ctx=CancelledError  suppress=True    → НЕ отмена
3. raise X from CancelledError (форма aiormq.TaskWrapper):
     cause=CancelledError  ctx=CancelledError  suppress=True    → НЕ отмена
4. голый raise внутри except CancelledError:
     это тот же объект CancelledError: cause=None, ctx=None
5. составная форма: отказ basic.cancel внутри __anext__ через TaskWrapper:
     cause=CancelledError  ctx=CancelledError  suppress=True    → отмена
```

Форма 1 — настоящий случай: `QueueIterator.__anext__`
(`.venv/.../aio_pika/queue.py:598-624`) отменяет consumer'а внутри обработки
`CancelledError`, и отказ `close()` выходит наружу вместо неё. Форма 4 — не
отдельный случай: голый `raise` перевозбуждает тот же объект, и он опознаётся
первым же `isinstance` на входе, не доходя до обхода цепочки.

Формы 2 и 3 — ложные срабатывания, и **флага `__suppress_context__`
недостаточно**, чтобы снять обе. `aiormq` доставляет ошибку разрыва
соединения ожидающему RPC не из сокета, а через
`FutureStore.reject_all` → `TaskWrapper.throw` → `.venv/.../aiormq/abc.py:44-46`:

```python
except asyncio.CancelledError as e:
    raise self._exception from e
```

то есть с `CancelledError` в `__cause__`. Обход, который ходит по `__cause__`
всегда, вернёт `True` и после правки.

Форма 5 — та же механика, но на настоящей отмене, и она задаёт границу
допустимого решения. `__anext__`, обрабатывая `CancelledError`, вызывает
`close()` → `Queue.cancel` → `aiormq.Channel.basic_cancel` → `Channel.rpc`,
помеченный `@task`. Разрыв соединения во время висящего `basic.cancel` даёт то
же `raise self._exception from e`, и наружу выходит исключение с
`cause=CancelledError, ctx=CancelledError, suppress=True`. Это **настоящая**
отмена — ровно случай, описанный в docstring функции (`consumer.py:558-560`) и
в `CHANGELOG.md:47`. Проверено запуском:

```
              по __cause__ ходим | не ходим
форма 5   ->        True         |  False
форма 2   ->        False        |  False
```

Отсюда решение: по `__cause__` ходить **всегда**, по `__context__` — только
при `not __suppress_context__`. Отказ от `__cause__` снял бы форму 3, но вернул
бы дефект `CHANGELOG.md:47` на форме 5. Форма 3 снимается не эвристикой, а
структурно — тем, что код доставки под неё не попадает.

**Почему это существенно для самой ветки.** `ConnectionError(f"{what} was
dropped while it connected") from None` (`consumer.py:1528`) написан затем,
чтобы drop соединения не выглядел отменой и таска его переретраила. Для
cooldown-подписки этот `ConnectionError` доходит до
`_raised_while_cancelling`, опознаётся как отмена и превращается обратно в
`CancelledError` — конверсия на 1528 обнуляется.

**Второй вход, форма 3.** Публикация плейсхолдера идёт по пути
`_handle_cooldown_delivery` → `_publish_pending` (`consumer.py:2560`) →
`_get_publish_channel` (`consumer.py:1377`) → `connection.channel()` → aiormq
`Channel.rpc`, помеченный `@task`, то есть исполняемый внутри `TaskWrapper`.
Разрыв publish-соединения во время этого RPC приходит вызывающему как
`AMQPConnectionError` с `CancelledError` в `__cause__` — внутри охраняемого
`async for` (`consumer.py:2401-2423`), — и таска завершается молча тем же
маршрутом. Отказ существует и переживает правку по `__suppress_context__`.

**Поэтому дизайн задачи 1 — не только флаг.** Эвристика существует ради одного
свойства `__anext__`: он способен подменить `CancelledError` ошибкой закрытия.
К коду доставки это свойство отношения не имеет, а обе ложные формы приходят
именно оттуда. Значит исключения, поднятые обработкой доставки, к эвристике
вообще не должны попадать — а сама эвристика правится флагом для того, что
остаётся под ней.

**Найдено при разборе плана, сверх отчёта.** Последняя запись статуса
исчезнувшей подписки не повторяется никогда: `_store_unwritten_statuses`
(`consumer.py:982`) обходит только `self._active`, а `_sync_consumer_tasks`
вынимает подписку из `_active` (`consumer.py:1017`) до записи
`_SUB_DISCONNECTED` (`consumer.py:1019`). Если та запись не прошла — а не
проходит она при недоступной метабазе, ради которой ветка и написана, — строка
навсегда остаётся с прежним статусом. Это и есть весь предмет задачи 5.

## Context (from discovery)

- Затрагиваемые файлы кода: `airflow_provider_rmq/watcher/consumer.py`
  (эвристика отмены, ключ кэша, пауза fire-цикла, реестр писателей статуса),
  `airflow_provider_rmq/utils/amqp.py` (комментарий у обработчика cast).
- Затрагиваемые тесты: `tests/watcher/test_consumer.py`,
  `tests/watcher/test_listener.py`, `tests/test_amqp_utils.py`.
- Затрагиваемая документация: `CHANGELOG.md`, `docs/backlog.md`,
  `docs/adr/0007-connection-liveness-two-tier-check.md`.
- Отчёт ревью: `.revmux/tasks/rmq-resilience-audit/01/report.md` (каталог
  `.revmux/` в `.gitignore`, в коммиты не идёт).
- Проверено запуском в `.venv`: поведение `__suppress_context__` у обоих
  случаев; `json.loads('{"heartbeat": 1e400}')` даёт `inf`, и `float(inf)` не
  бросает — `OverflowError` возникает только на JSON-целом, не представимом во
  float; `float("9" * 400)` даёт `inf`, поэтому тест на эту ветку требует
  целого литерала, а не строки.
- Watcher работает через `RobustConnection`, поэтому итератор очереди —
  `RobustQueueIterator` (`.venv/.../aio_pika/robust_queue.py:164`), который
  переопределяет `__anext__`, снимает `close_callbacks` канала и ждёт
  восстановления вместо `StopAsyncIteration`. Чистый выход у него наступает при
  намеренно закрытом соединении, при неприменимом восстановлении и по
  собственному таймауту ожидания — не при обычном закрытии канала брокером.
- Ошибка закрытия внутри `except (asyncio.TimeoutError, asyncio.CancelledError)`
  в `QueueIterator.__anext__` выходит наружу с `__suppress_context__ = False`
  (форма 1) либо, если отказал `basic.cancel` через `TaskWrapper`, с
  `cause=CancelledError, suppress=True` (форма 5).
- Прямого unit-теста `_raised_while_cancelling` в репозитории нет ни одного.
- Существующий хелпер `_QueueIterFailingCancel`
  (`tests/watcher/test_consumer.py:135`) поднимает **чужое** исключение внутри
  активного `except CancelledError` — это и есть форма 1, и он готовый
  тест-страж для задачи 1.
- `aiormq.abc.TaskWrapper.__inner` (`.venv/.../aiormq/abc.py:44-46`) делает
  `raise self._exception from e`, где `e` — `CancelledError`; так ошибка
  разрыва соединения доходит до любого ожидающего RPC.
- На пути `_publish_pending` (`consumer.py:2531`) достижимы **оба** класса
  отказа: таймаут AMQP-вызова и таймаут метабазы. Второй — потому что
  `_get_publish_channel` (`consumer.py:1377`) на медленном пути идёт в
  `_get_or_create_connection` → `_get_connection_info` (`consumer.py:1432`) →
  `pool.run(BaseHook.get_connection, ...)` под `_DB_TIMEOUT`; путь штатный,
  так как `_handle_publish_failure` после двух таймаутов подряд дропает
  publish-соединение, и следующая доставка идёт через его пересоздание.
- `_probe_by_management_api` в коде нет: ветка Management API — блок внутри
  `_probe_consumers` (`consumer.py:2224`), там же и docstring с посылкой про
  покрытие всего vhost (`consumer.py:2227-2243`, нужная фраза на 2236).
- Утверждение о времени жизни писателя записано **в четырёх** местах: docstring
  класса (`consumer.py:284-288`), ADR-0007 (`docs/adr/0007-...:408-409`),
  `CHANGELOG.md:21` и docstring фикстуры `fresh_status_writers`
  (`tests/watcher/test_consumer.py:276-281`).
- Реестр `_status_writers` (`consumer.py:365`) не очищается нигде в
  продакшн-коде — только в тестовой фикстуре `fresh_status_writers`.
- `_abandoned` (`consumer.py:877`) держит **таски**, а не `sub_id`: связи
  «брошенная таска → подписка» в коде нет, и любой механизм, которому она
  понадобится, начинается с её создания.
- `get_enabled_subscriptions` (`models.py:197`) фильтрует по `enabled=True`,
  поэтому переключение Disable/Enable в UI возвращает подписку с **тем же**
  `sub_id`.
- Версия `v2.4.0` в `CHANGELOG.md` не выпущена, и ни `_raised_while_cancelling`,
  ни `_consumer_cache`, ни `_status_writers` в `main` не существуют.

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
- каждая правка проверяется на способность падать: временно снять её и
  убедиться, что **краснеет каждый тест, который она чинит**, при этом
  тесты-стражи (те, что закрепляют неизменившееся поведение) остаются
  зелёными. Правка, после снятия которой не краснеет ничего, не имеет теста
- `ruff` не должен получить новых замечаний относительно базы ветки (сейчас 126,
  сверять `git stash` до/после)
- комментарии в коде описывают настоящее: ни один новый комментарий не
  ссылается на прежнее поведение, снятую проверку или переименование
- **CHANGELOG**: `v2.4.0` не выпущена, и все дефекты этого плана появились
  внутри неё. Новых пунктов `**Fixed:**` не добавляется — они описывали бы
  «до», которого не было ни у одного пользователя. Каждая правка уточняет тот
  существующий пункт, к предмету которого она относится

## Testing Strategy

- **unit tests**: обязательны в каждой задаче, в существующем стиле
  (`tests/watcher/test_consumer.py`, `test_listener.py`, `tests/test_amqp_utils.py`)
- e2e-тестов в проекте нет
- эвристика отмены (задача 1) покрывается на всех пяти формах из Overview,
  а не на одной: правка, сделанная под один случай, уже однажды разошлась с
  остальными. Форму 1 даёт готовый `_QueueIterFailingCancel`; форма 4
  проверяется как вход по `isinstance`, а не как обход цепочки
- сквозной тест на задачу 1 ставится на **cooldown**-путь: `_get_or_create_connection`
  вызывается вне охраняемого `try` (`consumer.py:2378` против `2401-2423`), поэтому
  на immediate-пути ошибка соединения и так уходит в `except Exception`
  (`consumer.py:2455`) и тест был бы зелёным до и после правки
- сквозные тесты задачи 1 моделируют отказ обеими формами — `from None` и
  `from CancelledError`: тест, написанный только под первую, показал бы правку
  подтверждённой при живом дефекте
- тесты на живость (задача 2) моделируют двух пользователей на одном
  `management_url` и vhost и проверяют, что второй conn_id не получает чужой
  ответ
- тест, проверяющий отсутствие эффекта, наблюдает вызов, а не его последствие:
  в `_wake_loop` исключение от `call_soon_threadsafe` глотается
  (`listener.py:707`), поэтому снятие проверки `loop.is_closed()` видно только
  через сам факт вызова
- тесты, которые выставляют атрибуты объекта под тестом, обязаны выставлять
  существующие: задача 7 существует потому, что два теста настраивали
  `_wakeup`/`_loop`, которых у листенера нет, и потому не проверяли ничего

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

Правки разделены по принадлежности дефекта:

1. **Дефекты ветки, влияющие на поведение** (задачи 1, 2, 5). Эвристика отмены,
   ключ кэша живости, недописанный финальный статус исчезнувшей подписки.
2. **Правка pre-existing поведения, оставленная в ветке** (задача 3). Пауза в
   fire-цикле отсутствует и в `main` (`_consume_fire_queue` там возвращается к
   `while True` без `sleep`, задержки стоят только в except-ветках), то есть
   формально она подпадает под правило задачи 9. Исключение сделано осознанно:
   fire-цикл — близнец подписочного, чью такую же паузу ветка уже добавила
   (`CHANGELOG.md:44`), и оставить один из двух без неё значит закрепить
   расхождение, которое эта же ветка и породила.
3. **Упрощение без изменения поведения** (задача 4). Возврат
   `_StatusWriter.store()`, который читают только тесты. Идёт **перед** задачей
   5: обе меняют `_StatusWriter`, и контракт `store()` сокращается до того, как
   задача 5 начнёт вызывать его из прохода по реестру.
4. **Тесты, не доказывающие ничего** (задачи 6–7). Две новые ветки heartbeat
   без покрытия и два пустых теста graceful-stop.
5. **Документация, расходящаяся с кодом** (задача 8). Комментарий про
   `1e400`, та же формулировка в `CHANGELOG.md`, ссылка на несуществующую
   константу в `docs/backlog.md`.
6. **Находки вне ветки** (задача 9). Три pre-existing уходят в
   `docs/backlog.md` без правок в коде: они есть и в `main`, и их исправление
   расширило бы диф ветки за пределы её темы.

### Ключевое решение по задаче 5

Задача 5 смешивала два разных предмета; в ветке остаётся только один из них.

**Поведенческий дефект — единственный:** финальный статус исчезнувшей подписки
не дописывается никогда. `_store_unwritten_statuses` (`consumer.py:982`)
обходит только `self._active`, а `_sync_consumer_tasks` вынимает подписку из
`_active` (`consumer.py:1017`) до записи `_SUB_DISCONNECTED`
(`consumer.py:1019`). Если та запись не прошла — а не проходит она при
недоступной метабазе, ради которой ветка и написана, — строка навсегда
остаётся с прежним статусом, чаще всего `listening` при отсутствующем
consumer'е.

**Правка соразмерна дефекту:** в конце `_store_unwritten_statuses` пройти
снимок `_status_writers`, снятый под `_status_writers_lock`, и для каждого
писателя с `has_pending` выполнить `writer.store` через `_cycle_executor` под
`_DB_TIMEOUT` — тем же способом, что `_ConsumerState.flush`
(`consumer.py:686-706`). Реестр процесс-глобальный, поэтому проход находит и
писателя подписки, которой в `_active` уже нет.

Почему этого достаточно и почему это безопасно:

- постановка «в очередь» не теряется при отмене цикла на записи — ставить
  нечего, признак дожима есть у самого писателя (`has_pending`);
- пересоздание петли переживается бесплатно: реестр живёт дольше менеджера;
- пересечение двух менеджеров безвредно — дожать чужой `_pending` есть ровно
  то, чего этот писатель и хочет, а `store()` сериализуется сам через
  `_storing` (`consumer.py:332-335`);
- `set_consumer_status` (`models.py:209`) — `filter_by(id=...).update(...)`, то
  есть на удалённой строке не совпадает ни с чем и не падает;
- проход по всем писателям делает лишним отдельный обход `self._active`: его
  писатели в реестре и так есть.

**Вывод писателей из обращения в эту ветку не входит.** Рост словаря
`_status_writers` — второй предмет, и он не поведенческий: объект из замка и
трёх полей на каждый когда-либо виденный `sub_id`, десятки записей на процесс.
Механизм вывода потребовал бы процесс-глобальной очереди, именования тасок,
скана `_abandoned`, `retire()` с четырьмя условиями и фиксированного порядка
двух замков — то есть нового разделяемого изменяемого состояния в модуле, где
каждая из трёх предыдущих попыток исправления уже порождала новый дефект того
же класса. Он и породил: при двух одновременно живых менеджерах писатель,
выданный одному, может быть выведен другим между получением и `record()`, и
тогда на одну строку пишут два писателя — старый может затереть более новый
`error` своим `listening`, то есть вернуть ложный зелёный статус, против
которого написана вся ветка.

Поэтому вывод писателей уходит в `docs/backlog.md` (задача 9) вместе с
обоснованием: рост реестра, потеря памяти `_stored` при возврате подписки и
требование к владению писателем при двух менеджерах.

**Утверждение о времени жизни писателя остаётся верным** — писатель по-прежнему
живёт столько же, сколько процесс. Ни docstring класса (`consumer.py:284-288`),
ни ADR-0007 (`docs/adr/0007-...:408-409`), ни `CHANGELOG.md:21`, ни docstring
фикстуры `fresh_status_writers` (`tests/watcher/test_consumer.py:276-281`)
править не требуется.

## Technical Details

**Задача 1** состоит из двух частей, и ни одна не заменяет другую.

*Часть A — вывести доставку из-под эвристики.* Тело `async for` получает
собственный `except`, помечающий исключение как поднятое обработкой доставки;
внешний обработчик (`consumer.py:2415`) при этой отметке перевозбуждает
исключение, не спрашивая эвристику. Это структурная граница: ни форма 3
(`aiormq`), ни любая будущая форма из кода доставки до эвристики не доходит.
Полная перестройка цикла на явный `await q_iter.__anext__()` для этого не
нужна — `async for` остаётся, отметка ставится во вложенном `except`.

*Часть B — исправить сам обход* в `_raised_while_cancelling`
(`consumer.py:574`) для того, что остаётся под эвристикой, то есть для
`__anext__`: переходить по `__context__` только при
`not current.__suppress_context__`. `raise asyncio.CancelledError from exc` в
обоих consumer-циклах не задет: такое исключение опознаётся первым же
`isinstance` на входе в цикл обхода.

По `__cause__` обход идёт **всегда** — это не свободный выбор: формы 3 и 5
неразличимы по цепочке исключений (обе `cause=CancelledError, suppress=True`),
и различает их только то, откуда исключение пришло. Форма 5 приходит из
`__anext__` и есть настоящая отмена; форма 3 приходит из кода доставки и
отменой не является. Поэтому граница проводится частью A по месту
возникновения, а не частью B по форме: эвристика на цепочке этого различить не
может в принципе, и попытка научить её этому — способ получить четвёртое
расхождение.

Отсюда же разделение ответственности при проверке: часть A закрывает форму 3,
часть B — форму 2, и снятие одной не должно ловиться тестом другой.

**Задача 2.** Ключ кэша `(management_url, vhost)` → `(management_url, vhost,
login)`. Ответ `/api/consumers/{vhost}` зависит от прав спрашивающего:
пользователю с тегом `management` RabbitMQ отдаёт только его собственные
каналы, весь список видят `monitoring`/`administrator`. Кэш живёт один цикл
(сбрасывается в `consumer.py:1943`), поэтому эффект не разовый: отрицательный
вердикт повторяется каждый цикл, а AMQP-fallback не включается, так как
HTTP-запрос успешен. Пересоздание соединения при этом ограничено
`_CYCLES_BEFORE_REDROP = 5` (`consumer.py:119`): `_judge_candidates`
спрашивает `_may_drop_connection` (`consumer.py:2098`) и при отказе не
перезапускает ничего, записывая вердикт как `degraded`. Ту же неверную посылку — «ответ
покрывает весь vhost и потому кешируется на цикл» — несут аннотация типа
(`consumer.py:867`), docstring `_probe_consumers` (`consumer.py:2227-2243`, фраза на `2236`)
и ADR-0007 (`docs/adr/0007-...:124`).

**Задача 3.** `await asyncio.sleep(_RECONNECT_DELAY)` в конце тела `try`
`_consume_fire_queue`, после блока `finally` (`consumer.py:2679`) — как в
`_consume_subscription` (`consumer.py:2433`).

**Задача 5.** Проход добавляется в конец `_store_unwritten_statuses`
(`consumer.py:982`). Снимок реестра снимается под `_status_writers_lock` и
отпускает его до первого `await`: держать замок через ожидание пула значит
блокировать `_status_writer()` всех остальных на время недоступной метабазы.
Отказ дожима логируется и не прерывает цикл — как в `_ConsumerState.flush`
(`consumer.py:686-706`). Новых полей и новых замков задача не вводит.

**Задача 6.** Две ветки без покрытия: `0 < value < 1` в `_read_heartbeat`
(`amqp.py:108`) и `OverflowError` в `_read_number` (`amqp.py:70`). Второй
случай воспроизводится **целым** литералом из 400 цифр, не строкой: `1e400`
читается json'ом как `inf` и ловится проверкой `math.isfinite`, а строка
`"9" * 400` даёт `inf` из `float()`, а не `OverflowError`.

**Задача 7.** У `RMQWatcherListener` цикл и его событие публикуются одним
кортежем `self._waker` (`listener.py:651`, `761`), и `_wake_loop`
(`listener.py:691`) читает только его. Тесты собирают пару
`listener._waker = (loop, wakeup)`. Проверка `loop.is_closed()`
(`listener.py:703`) наблюдаема только через сам вызов
`loop.call_soon_threadsafe`: исключение от него глотает `except RuntimeError`
двумя строками ниже (`listener.py:707`).

## What Goes Where

- **Implementation Steps** (`[ ]`): правки кода, тестов и документации в этом
  репозитории.
- **Post-Completion** (без чекбоксов): проверки на стенде, настройки инфраструктуры
  и решения о мерже — вне репозитория.

## Implementation Steps

### Task 1: Убрать доставку из-под эвристики отмены и исправить сам обход

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`
- Modify: `CHANGELOG.md`

- [x] часть A: дать телу `async for` (`consumer.py:2402-2413`) собственный
      `except`, помечающий исключение как поднятое обработкой доставки, и
      перевозбуждать его во внешнем обработчике (`consumer.py:2415`) без
      обращения к эвристике
- [x] часть A: то же для тела доставки fire-цикла (`_consume_fire_queue`,
      `consumer.py:2661-2674`) — как выравнивание близнецов, без собственного
      прослеженного отказа: `basic_ack`/`basic_nack` в aiormq не обёрнуты в
      `@task` (`aiormq/channel.py:537`, `558` против `@task` на `rpc`, `157`),
      получают исключение через `future.set_exception`, и формы 3 на этом пути
      нет. Пометка ставится, чтобы расхождение между близнецами не появилось
      снова
- [x] часть B: в `_raised_while_cancelling` (`consumer.py:574`) переходить по
      `__context__` только при `not current.__suppress_context__`; принять
      решение по `__cause__` согласно Technical Details и записать основание в
      docstring
- [x] дополнить docstring функции тем, что именно читается в цепочке и почему
      этого достаточно на 3.10 (без упоминания прежнего поведения)
- [x] тест: форма 2 — `ConnectionError(...) from None`, поднятый внутри
      `except asyncio.CancelledError`, отменой не считается
- [x] тест: форма 2 — `asyncio.TimeoutError() from None` из `call_with_timeout`
      в той же позиции отменой не считается
- [x] тест: форма 3 — `AMQPConnectionError`, поднятый как
      `raise X from CancelledError` (форма `aiormq.TaskWrapper`), отменой не
      считается
- [x] тест-страж: форма 1 — чужое исключение внутри
      `except asyncio.CancelledError` — по-прежнему считается отменой; строится
      на существующем `_QueueIterFailingCancel`
      (`tests/watcher/test_consumer.py:135`)
- [x] тест-страж: форма 4 — сам `CancelledError` — распознаётся входом по
      `isinstance`, не доходя до обхода цепочки
- [x] тест-страж: форма 5 — отказ `basic.cancel` внутри `__anext__`, пришедший
      через `TaskWrapper` (`cause=CancelledError`, `suppress=True`), **считается**
      отменой; это регрессионный тест на `CHANGELOG.md:47`, и без него отказ от
      обхода `__cause__` выглядел бы безопасным
- [x] тест на сквозной путь: **cooldown**-подписка, у которой публикация
      плейсхолдера в `_publish_pending` не уложилась в `rpc_timeout` и
      `call_with_timeout` поднял `TimeoutError` с подавленным контекстом,
      доходит до своего retry-цикла и пишет `error` — а не завершается молча
- [x] тест на сквозной путь: **cooldown**-подписка, у которой publish-соединение
      разорвано во время RPC внутри `_get_publish_channel` (форма 3),
      перезапускается, а не завершается молча
- [x] тест на сквозной путь: **cooldown**-подписка, у которой пересоздание
      publish-соединения упёрлось в `_DB_TIMEOUT` чтения Airflow-connection,
      перезапускается, а не завершается молча
- [x] тест на сквозной путь: подписка, у которой сам `__anext__` поднял форму 1,
      завершается как отменённая — путь, который остаётся под эвристикой после
      части A
- [x] уточнить существующий пункт `CHANGELOG.md:47` (эвристика отмены) вместо
      добавления нового
- [x] снять часть A: краснеют сквозные тесты на форму 3 и на `_DB_TIMEOUT` —
      их ловит только структурная граница, эвристика на этих формах отвечает
      «отмена»; вернуть
      - ⚠️ наблюдение при исполнении: краснеет только сквозной тест на форму 3.
        Отказ чтения Airflow-connection по `_DB_TIMEOUT` приходит из
        `call_with_timeout` как `TimeoutError` формы 2
        (`cause=None, ctx=CancelledError, suppress=True`), а не формы 3, поэтому
        его держит часть B, а не часть A. Тест на этот путь оставлен: он
        проходит настоящий маршрут `_get_publish_channel` →
        `_get_or_create_connection` → `_get_connection_info`
- [x] снять часть B: краснеют юнит-тесты на форму 2; сквозной тест на таймаут
      публикации при этом остаётся зелёным — после части A он от эвристики не
      зависит, и пинить им часть B нельзя; вернуть
- [x] в обоих случаях тесты-стражи, включая форму 5, остаются зелёными
- [x] run tests - must pass before next task

### Task 2: Включить пользователя Management API в ключ кэша живости

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `docs/adr/0007-connection-liveness-two-tier-check.md`
- Modify: `tests/watcher/test_consumer.py`
- Modify: `CHANGELOG.md`

- [x] расширить ключ кэша в `_probe_consumers` (`consumer.py:2224`, ключ кэша на `2265`) до
      `(management_url, vhost, login)`, обновить аннотацию типа
      (`consumer.py:867`) и комментарий над ней (`consumer.py:866`), который
      называет ключ словами
- [x] исправить docstring `_probe_consumers` (`consumer.py:2227-2243`, фраза на `2236`): ответ
      зависит от прав спрашивающего, поэтому переиспользуется только между
      conn_id с той же учётной записью
- [x] исправить то же утверждение в ADR-0007 (`docs/adr/0007-...:124`)
- [x] тест: два conn_id с одним `management_url` и vhost, но разными login,
      получают каждый свой ответ, и второй не судится по первому
- [x] тест-страж: два conn_id с одинаковыми URL, vhost и login по-прежнему
      делают один HTTP-запрос на цикл
- [x] уточнить `CHANGELOG.md:16` — единственное место в changelog, где посылка
      сформулирована явно («one `GET /api/consumers/{vhost}` is shared by every
      `conn_id` pointing at the same broker and vhost within a cycle»)
- [x] проверить docstring'и тестов `test_one_management_request_serves_every_conn_id_of_a_vhost`
      и `test_the_consumer_cache_lives_for_one_cycle_only`
      (`tests/watcher/test_consumer.py:4501`, `4527`) — они повторяют ту же посылку
      - ⚠️ ссылки на строки устарели: тесты стоят на `4823` и `4848`. Первый
        переименован в `test_one_management_request_serves_conn_ids_of_one_account`
        и задаёт обоим conn_id один login явно — иначе он не закреплял бы то,
        что проверяет
- [x] снять правку: краснеет тест на двух пользователей, тест-страж остаётся
      зелёным; вернуть
      - ⚠️ наблюдение при исполнении: со снятым login в ключе
        `test_two_logins_on_one_vhost_each_get_their_own_answer` краснеет
        (`conn_b` получает чужой ответ, HTTP-запрос всего один, и его consumer
        получает «negative check 1 of 2»), а оба стража —
        `test_one_management_request_serves_conn_ids_of_one_account` и
        `test_the_consumer_cache_lives_for_one_cycle_only` — остаются зелёными
- [x] run tests - must pass before next task

### Task 3: Дать fire-циклу паузу перед переподпиской

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`
- Modify: `CHANGELOG.md`

- [x] добавить `await asyncio.sleep(_RECONNECT_DELAY)` в конце тела `try`
      `_consume_fire_queue` (`consumer.py:2679`). Комментарий формулировать по
      состояниям чистого выхода `RobustQueueIterator` (намеренно закрытое
      соединение, неприменимое восстановление, таймаут ожидания канала), а не
      «канал закрыт брокером»: итератор здесь robust-вариант
      - ⚠️ ссылка на строку устарела: конец тела `try` — `consumer.py:2766`
        (после `finally`, закрывающего канал fire-консьюмера)
- [x] тест: итератор fire-очереди, завершающийся сразу и подряд, не крутит цикл
      без задержки — переподписка происходит после `_RECONNECT_DELAY`
      (`test_the_fire_iterator_ending_pauses_before_resubscribing`)
- [x] тест-страж: отмена fire-таски во время этой паузы завершает её штатно
      (`test_cancelling_the_fire_task_inside_a_pause_ends_it_quietly`)
- [x] уточнить существующий пункт `CHANGELOG.md:44` — он описывает ту же паузу
      для подписочного цикла и после этой правки относится к обоим
- [x] снять правку: краснеет тест на задержку, тест-страж остаётся зелёным;
      вернуть
      - ⚠️ наблюдение при исполнении: со снятой паузой краснеют **оба** теста, и
        это не дефект второго. Пауза стоит внутри тела `try`, поэтому отмену в
        ней принимает `except asyncio.CancelledError: return` и таска возвращает
        `None`. Пауз в except-ветках это не касается: они сами являются
        обработчиками, и соседний `except asyncio.CancelledError` их отмену не
        ловит — таска завершается отменённой. Страж, дожидающийся паузы и
        требующий `outcome is None`, тем самым закрепляет и место паузы; зелёным
        без правки он мог бы остаться только отказавшись от этого требования
- [x] run tests - must pass before next task

### Task 4: Убрать возврат `store()`, который читают только тесты

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`

- [x] сделать `_StatusWriter.store()` возвращающим `None` и убрать из docstring
      абзац про возвращаемое значение, оставив описание того, что метод делает
      при уже идущей записи
- [x] переписать `tests/watcher/test_consumer.py:6473` на наблюдаемое поведение:
      завершение `.result(timeout=1)` и есть «воркер отдан сразу»
      - ⚠️ ссылка на строку устарела: тест
        `test_a_write_finding_the_writer_busy_gives_its_worker_straight_back`
        стоит на `6891`. Наблюдаемое: второй вызов возвращается за секунду и
        второй записи не начинает (`commits == ["listening"]`)
- [x] переписать `tests/watcher/test_consumer.py:6495` на наблюдаемое поведение:
      каждый записанный статус доходит до строки
      - ⚠️ ссылка на строку устарела: тест
        `test_writes_that_arrive_in_order_all_land` стоит на `6926`. Наблюдаемое:
        порядок в `landed`, пустой `has_pending` после каждой записи и `stored`
- [x] тест: запись, начатая вторым вызовом при идущем первом, не теряет
      отложенный статус
      (`test_a_status_noted_while_a_write_runs_is_taken_by_that_write`)
- [x] run tests - must pass before next task

### Task 5: Дописывать финальный статус исчезнувшей подписки

**Files:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`
- Modify: `CHANGELOG.md`

- [x] в конце `_store_unwritten_statuses` (`consumer.py:1044`) пройти снимок
      `_status_writers`, снятый под `_status_writers_lock` и отпустивший его до
      первого `await`, и для каждого писателя с `has_pending` выполнить
      `writer.store` через `self._cycle_executor` под `_DB_TIMEOUT`
      - ⚠️ ссылка на строку устарела: метод стоит на `consumer.py:1044`
- [x] убрать ставший избыточным отдельный обход `self._active`: его писатели
      входят в снимок реестра
      - ➕ вместе с обходом `self._active` снят и обход `self._fire_state`
        (у фаер-состояния `sub_id` всегда `None`, и его `flush` возвращался
        сразу), а `_ConsumerState.flush` удалён: после снятия обоих обходов у
        метода не осталось вызывающих
- [x] логировать отказ дожима и продолжать цикл, как это делает
      `_ConsumerState.flush` (`consumer.py:686-706`)
- [x] объяснить в docstring, почему проход идёт по реестру, а не по активным
      подпискам: строка исчезнувшей подписки переживает её саму, и дописать её
      больше некому
- [x] тест: подписка удалена, запись `disconnected` не прошла — следующий цикл
      дописывает её статус
      (`test_the_next_cycle_writes_the_status_of_a_subscription_that_is_gone`)
- [x] тест: две неудачи подряд не теряют статус — третий цикл всё ещё дожимает
      (`test_two_failed_cycles_in_a_row_do_not_lose_the_status`)
- [x] тест: отмена цикла на записи `_SUB_DISCONNECTED` не теряет статус —
      следующий цикл его дописывает
      (`test_a_cycle_cancelled_on_the_final_write_does_not_lose_it`)
- [x] тест: проход не ломается на подписке, строки которой в БД уже нет
      (`test_the_pass_survives_a_row_that_is_no_longer_there`)
- [x] тест-страж: писатель, у которого нечего дожимать, обращения к БД не
      вызывает (`test_a_writer_with_nothing_left_does_not_touch_the_database`)
- [x] тест-страж: дожим не запускает вторую параллельную запись, когда первая
      ещё идёт
      (`test_the_pass_does_not_start_a_second_write_beside_a_running_one`)
- [x] уточнить существующий пункт `CHANGELOG.md:49` — он описывает дожим
      незаписанного статуса и после этой правки относится и к подписке, которой
      уже нет
- [x] снять проход по реестру, убедиться, что краснеют первые три теста и не
      краснеют стражи; вернуть
      - ⚠️ наблюдение при исполнении: со снятым проходом краснеют **четыре**
        теста, а не три. Четвёртый — `test_the_pass_survives_a_row_that_is_no
        _longer_there`: подписки, чьей строки уже нет, нет и в `_active`,
        поэтому обход активных до её писателя не доходит и записи не случается
        вовсе. Оба стража остаются зелёными, как и существующий
        `test_the_cycle_carries_a_dropped_status_into_the_row`
- [x] run tests - must pass before next task


### Task 6: Покрыть обе новые ветки heartbeat

**Files:**
- Modify: `tests/test_amqp_utils.py`

- [x] тест: `{"heartbeat": 0.5}` даёт URL с `heartbeat=DEFAULT_HEARTBEAT` и
      WARNING про интервал короче секунды
      (`test_a_heartbeat_shorter_than_a_second_falls_back_to_default`)
- [x] тест: `{"heartbeat": <400-значное целое>}` даёт тот же fallback, а не
      исключение из `build_amqp_connection`
      (`test_a_heartbeat_too_large_for_a_float_falls_back_to_default`)
- [x] снять `if 0 < value < 1` и `OverflowError` по очереди, убедиться, что
      краснеет ровно соответствующий тест, вернуть
      - ⚠️ ссылки на строки в задаче устарели: тесты добавлены в класс
        `TestHeartbeatInUrl` перед `test_ssl_url_keeps_query`
      - проверено по очереди: со снятым `if 0 < value < 1` краснеет только
        тест на 0.5 (URL приходит с `heartbeat=0`, то есть с опт-аутом), со
        снятым `OverflowError` — только тест на 400-значное целое
        (`OverflowError` выходит из `build_amqp_connection` наружу); второй тест
        в каждом прогоне остаётся зелёным
- [x] run tests - must pass before next task

### Task 7: Починить тесты graceful-stop, которые ничего не проверяют

**Files:**
- Modify: `tests/watcher/test_listener.py`

- [x] в `test_before_stopping_survives_a_loop_that_is_already_closed`
      (`test_listener.py:2460`) собирать `listener._waker = (loop, wakeup)` и
      наблюдать сам вызов: подменить `loop.call_soon_threadsafe` на `MagicMock`
      и проверить, что он не вызван
- [x] в `test_before_stopping_survives_a_loop_closing_under_it`
      (`test_listener.py:2473`) — **другое**: собрать `listener._waker`, оставить
      `loop.is_closed()` → `False` и
      `call_soon_threadsafe.side_effect = RuntimeError`, проверить
      `assert_called_once()` и что `before_stopping` не бросил. Это единственное
      покрытие `except RuntimeError`, и подмена на пустой мок его снимает
- [x] в `test_a_cycle_that_stopped_the_watcher_does_not_wait_at_all`
      (`test_listener.py:2448`) дать живой loop и `asyncio.Event`, чтобы
      мгновенный возврат нельзя было объяснить отсутствием waker'а, и снизить
      `_reconcile_interval` до пары секунд — иначе снятие проверки `_stop_event`
      выявляется только через 30 с
- [x] снять из `_wake_loop` проверку `loop.is_closed()` (`listener.py:703`),
      убедиться, что тест краснеет, вернуть
      - проверено: краснеет только
        `test_before_stopping_survives_a_loop_that_is_already_closed`
        (`call_soon_threadsafe` вызван), остальные четыре теста класса зелёные
- [x] снять `except RuntimeError` (`listener.py:707`), убедиться, что тест
      краснеет, вернуть
      - проверено: краснеет только
        `test_before_stopping_survives_a_loop_closing_under_it`
        (`RuntimeError` выходит из `before_stopping` наружу)
- [x] снять из `_wait_for_next_cycle` проверку `_stop_event`
      (`listener.py:792`), убедиться, что тест краснеет, вернуть
      - проверено: краснеет только
        `test_a_cycle_that_stopped_the_watcher_does_not_wait_at_all`
        (ожидание длится весь интервал в 2 с)
- [x] run tests - must pass before next task

### Task 8: Привести документацию в соответствие с кодом

**Files:**
- Modify: `airflow_provider_rmq/utils/amqp.py`
- Modify: `CHANGELOG.md`
- Modify: `docs/backlog.md`

- [x] исправить комментарий у `except (TypeError, ValueError, OverflowError)`
      (`amqp.py:71`): из cast'а выходит JSON-целое, не представимое во float;
      `1e400` читается как `inf` и отсекается проверкой `math.isfinite` ниже
      - ⚠️ ссылка на строку устарела: обработчик стоит на `amqp.py:70`,
        комментарий — на `71-75`
- [x] исправить ту же формулировку в `CHANGELOG.md:56`
      - ⚠️ пункт свёрнут: в `main` числовых парсеров нет вовсе
        (`utils/amqp.py` там 91 строка, без `_read_number`), поэтому дефект
        появился внутри `v2.4.0`. Итоговая формулировка ушла в пункт
        `**Added:**` про `connect_timeout`/`rpc_timeout` и в пункт про
        heartbeat — уже с верным разделением: `OverflowError` даёт только
        JSON-целое, не представимое во float, а `inf` отсекает проверка
        конечности
- [x] заменить в `docs/backlog.md:144` ссылку на несуществующий `_NACK_SLEEP` на
      реальное место: пауза 0.1 с внутри `nack_and_sleep` (`utils/amqp.py:290`)
      - ⚠️ ссылка на строку устарела: `nack_and_sleep` стоит на `amqp.py:284`,
        сама пауза — на `289`
- [x] решить по каждому пункту `**Fixed:**` отдельно, а не диапазоном: сворачивать
      в `**Added:**` только те, чьё поведение в `main` отсутствует. Проверять
      через `git show main:<файл>`, а не по номеру строки
      - ⚠️ отклонение от посылки плана: по `main` проверены все 25 пунктов
        `**Fixed:**`, и воспроизводимых в `v2.3.0` оказалось **четырнадцать**, а
        не пять. Свёрнуто одиннадцать: таймаут как баунд на вызывающего,
        растущая пауза fire-пути, потеря статуса брошенной записью, ожидание
        коннекта под замком своего `conn_id`, дожим незаписанного статуса,
        `inf`/`nan` в парсерах, ранний выход стража по `_pending`, отмена
        коннекта под вызывающим, бюджет `stop()`, статус publish-роли и
        `OverflowError` в extras — вся эта механика в `main` отсутствует.
        Остальные (недоставленный плейсхолдер, `listening` до подтверждения
        `basic.consume`, неподтверждённая fire-доставка, незакрытые каналы,
        коннект под alarm, утечка соединения при отмене коннекта,
        `DagRunAlreadyExists`, `replace_microseconds`, ACK до декодирования в
        push-режиме и пять названных ниже) воспроизводятся в `main` дословно
      - ➕ пункт про сравнение только по статусу переписан так, чтобы не
        начинаться со ссылки «the same guard» на свёрнутый соседний пункт
- [x] явно оставить как `**Fixed:**` пункты, воспроизводимые в выпущенной
      `v2.3.0`: `CHANGELOG.md:44` (нет паузы после чистого выхода итератора),
      `:45` (пул отдаёт соединение с незавершённым reconnect — наблюдалось в
      проде), `:46` (fire consumer стартует на любом объекте пула), `:47`
      (отменённая таска переживает отмену), `:52` (страж сравнивал только
      статус)
      - ⚠️ все пять проверены по `main` и оставлены; после свёртки они стоят на
        `CHANGELOG.md:41-45`
- [x] run tests - must pass before next task

### Task 9: Записать находки вне ветки в backlog

**Files:**
- Modify: `docs/backlog.md`

- [x] запись: нерезолвящиеся аргументы декоратора молча становятся дефолтами
      (`listener.py:497` — `continue` по неудавшемуся `ast.literal_eval`);
      `conn_id=RMQ_CONN` даёт подписку на `rmq_default`, `filter_data=FILTER` —
      подписку без фильтра, `cooldown=COOLDOWN` — immediate-режим; с вариантами
      решения и связью с существующим пунктом про
      `_parse_rmq_trigger_decorator` (`docs/backlog.md:59`)
      - ⚠️ ссылка на строку устарела: `continue` стоит на `listener.py:496`,
        сама функция — на `456`, кортеж `_RMQ_TRIGGER_KWARGS` — на `426-436`.
        Ссылка на соседний пункт беклога (`docs/backlog.md:59`) верна
      - ➕ сверх плана записана граница случая: exchange-режим защищён
        частично — при нелитеральных **обоих** `routing_keys` и
        `routing_key_ids` `build_subscriptions` бросает `ValueError` и подписка
        пропускается с WARNING; при одном из двух набор ключей молча урезается
- [x] запись: замена `self._stop_event` в `_start()` (`listener.py:722`) сразу
      после `join(timeout=10)` без проверки результата может оживить уходящий
      watcher — старый `_run_loop` читает поле динамически и увидит новое
      несведённое событие
      - ссылки проверены и верны: `_start()` — `listener.py:714`, `join` —
        `721`, подмена события — `722`; поле читается на `738`, `755` и `767`
- [x] запись: две ветки `_provision_one_exchange_sub` (`consumer.py:1596` и
      `1605`) описывают одну ситуацию, но возвращают разное, из-за чего dag_id
      не попадает в трекер орфанов и WARNING про осиротевшую очередь не выдаётся
      - ⚠️ ссылки на строки устарели: метод стоит на `consumer.py:1636`, ветка
        `management_url is None` — `1662` с `return True` на `1669`, ветка
        логина/пароля — `1671` с `return False` на `1678`; `mark_provisioned`
        у вызывающего — `1816-1817`, орфан-проверка — `1284`
      - ➕ третья ветка того же метода (отказ Management API,
        `consumer.py:1693`) уже отвечает `True`, то есть верный ответ написан
        дважды из трёх — записано как обоснование направления решения
- [x] запись: реестр `_status_writers` (`consumer.py:365`) растёт всю жизнь
      процесса — `_sync_to_db` (`listener.py:1160-1199`) пересоздаёт
      `dag_file`-подписки с новыми `sub_id`, и каждый мёртвый id навсегда
      удерживает писателя с текстом ошибки. Записать разобранные требования к
      выводу: писатель выводится только свободным; при двух одновременно живых
      менеджерах нужно владение писателем, а не разовая отметка обращения —
      иначе писателя, выданного одному менеджеру, выведет другой между
      получением и `record()`, и на одну строку начнут писать двое; возврат
      подписки через Disable/Enable даёт тот же `sub_id` (`models.py:197`) и
      теряет память `_stored`. Указать, что рост не поведенческий — объект из
      замка и трёх полей на `sub_id`, десятки записей на процесс
      - ⚠️ ссылка на строку устарела: `_sync_to_db` стоит на
        `listener.py:1161-1202`. Остальные верны: реестр — `consumer.py:365`,
        `get_enabled_subscriptions` — `models.py:197-199`, `_abandoned` —
        `consumer.py:917`, поля писателя — `consumer.py:293-295`
      - ➕ в запись добавлено, что поведенческую половину уже закрывает проход
        по реестру в `_store_unwritten_statuses` (`consumer.py:1021`, снимок на
        `1043-1044`) — задача 5 этого же плана
- [x] в каждой записи указать, воспроизводится ли находка в `main`, и почему
      она не правится в этой ветке
      - проверено через `git show main:<файл>`: первые три находки
        воспроизводятся в `main` дословно (тот же цикл kwargs, `_start()` на
        `listener.py:560-574`, те же два возврата в
        `_provision_one_exchange_sub`); четвёртая в `main` отсутствует —
        `_StatusWriter` и `_status_writers` появились внутри невыпущенной
        `v2.4.0`, поэтому записана как известное свойство текущего дизайна
- [x] run tests - must pass before next task
      - `1000 passed, 1 skipped`; `ruff` — 135 замечаний, как и до правки
        (изменение только в `docs/backlog.md`)

### Task 10: Verify acceptance criteria

- [ ] проверить, что ни один путь из Overview не оставляет подписку с зелёным
      статусом при отсутствующем consumer'е
- [ ] проверить, что `_raised_while_cancelling` даёт ожидаемый ответ на всех
      пяти формах исключения из Overview
- [ ] проверить, что ни одно исключение из кода доставки не доходит до
      эвристики отмены
- [ ] проверить, что в `CHANGELOG.md` не появилось пунктов о дефектах, которых
      не было ни в одном выпуске
- [ ] run full test suite: `pytest`
- [ ] сверить `ruff` с базой ветки: новых замечаний нет
- [ ] проверить, что `.revmux/` не попал ни в один коммит

### Task 11: [Final] Update documentation

- [ ] сверить `readme.md` и `readme_ru.md` с изменившимся поведением
- [ ] сверить `docs/adr/0007-connection-liveness-two-tier-check.md` целиком —
      задача 2 правит его точечно
- [ ] обновить `CONTEXT.md`, если появились новые точки входа
- [ ] переместить этот план в `docs/plans/completed/`

## Post-Completion
*Требует ручного вмешательства или внешних систем — без чекбоксов*

**Проверка на стенде:**
- проверки Post-Completion из `docs/plans/completed/20260827-watcher-connection-resilience.md`
  не проводились ни разу; ветка на живом RabbitMQ не проверялась
- сценарий, породивший план 20260827: рестарт брокера при живом TCP-соединении
  через прокси — убедиться, что подписки восстанавливаются сами
- сценарий находки задачи 2 воспроизводится только на брокере с двумя
  пользователями без тега `monitoring` на одном vhost

**Настройки инфраструктуры:**
- у AMQP-entrypoint traefik не задан `respondingTimeouts`, поэтому действует
  дефолт v3 `readTimeout=60s`; для AMQP нужен `0s`
- конфиг `rabbitmq.conf` не смонтирован в контейнер, поэтому заданный в нём
  `management.path_prefix` не применяется

**Решение о мерже:**
- три раунда ревью подряд дали находки, порождённые правками предыдущего
  раунда, и одна из сегодняшних major — следствие правки первого раунда,
  гасящая правку второго. После исполнения этого плана стоит прогнать ещё один
  раунд, прежде чем считать ветку закрытой
- ветка не смержена и не запушена: из контейнера нет SSH к удалённому репозиторию
