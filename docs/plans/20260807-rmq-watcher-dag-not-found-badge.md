# UI-бейдж «dag not found» на странице RMQ Watcher Subscriptions

## Обзор

Страница RMQ Watcher Subscriptions показывает список подписок на очереди
RabbitMQ и их статус (`listening`/`connecting`/`error`), но никак не
проверяет, что `dag_id` подписки вообще соответствует какому-то реальному,
активному Airflow DAG. Subscription с несуществующим/переименованным/
удалённым `dag_id` выглядит рабочей (`status: listening` — коннекшн и
консьюмер реальные), но при совпадающем сообщении реальный DAG никогда не
запускается.

Сигнал об этом сегодня есть, но только реактивный и в логах:
`consumer.py::_sync_trigger` (строки 47-71) запрашивает
`DagModel.filter_by(dag_id=..., is_active=True, is_paused=False)` перед
вызовом `trigger_dag()`; если запрос ничего не находит, пишется
`log.warning("DAG %s not found, inactive or paused — message acked,
skipping trigger", dag_id)` и сообщение ACK-ается без запуска чего-либо.
Срабатывает только когда реально приходит и потребляется подходящее
сообщение — то есть пока такое сообщение не пришло, ничего не подсказывает,
что подписка сломана.

Этот план добавляет **проактивный** сигнал: бейдж `⚠ dag not found` прямо
на странице Subscriptions, рядом с любым `dag_id`, не соответствующим
активному Airflow DAG — видимый сразу, без ожидания первого сообщения и без
грепа логов Scheduler'а.

**Откуда взялся этот план и что он НЕ покрывает.** Изначально это была
часть плана `docs/plans/completed/20260807-ast-dag-id-constant-resolution.md`
(AST-резолвинг `dag_id` из модульных констант в `@dag(...)`) — вынесена в
отдельный план по итогам ревью. Причины разделения:
- Бейдж не помогает именно в том сценарии, который решает AST-план: если
  `dag_id=` в DAG-файле вообще не резолвится статически, subscription не
  регистрируется совсем — строка просто отсутствует на странице
  Subscriptions, бейджу нечего подсвечивать. Единственный сигнал для ЭТОГО
  случая — WARNING в логе Scheduler'а (см. AST-план).
- Бейдж реально помогает для **другого** класса случаев: `dag_id` успешно
  зарезолвился (литерал/константа в `dag_file`, или это ручная `source='ui'`
  subscription с опечаткой), но соответствующий Airflow DAG переименован,
  удалён, или никогда не существовал.
- Функционально независим от AST-плана — может реализовываться в любом
  порядке, не требует его изменений в коде.

**Известный временный false positive.** Только что добавленный DAG-файл, ещё
не распарсенный Scheduler'ом, отсутствует в `DagModel` — его subscription
получит бейдж, пока парсер не догонит. Это осознанно не лечится (см.
Technical Details — та же гонка объясняет, почему бейдж read-only и не
делает авто-disable), но должно быть проговорено в README, чтобы не читалось
как баг.

Этот план:
1. Добавляет `get_known_dag_ids()` — хелпер, запрашивающий множество
   `dag_id` активных Airflow DAG (`models.py`).
2. Встраивает результат в `RMQWatcherView.subscriptions()` (`views.py`).
3. Рендерит бейдж `⚠ dag not found` в шаблоне `subscriptions.html` рядом с
   любым `dag_id`, отсутствующим в этом множестве.

Явно вне скоупа:
- `is_paused` не проверяется — см. Technical Details, отдельная оговорка.
- Ничего не меняется в поведении самого триггера сообщений (`consumer.py`)
  — это чисто UI-слой, read-only диагностика.

## Контекст (по итогам разведки)

- `airflow_provider_rmq/watcher/models.py` — `WatcherSession` привязана к
  `airflow.settings.engine`, то есть к **той же** базе, что и собственные
  метаданные Airflow (`_make_session_factory`, ~строка 61-63). Query-хелперы
  вроде `get_conn_statuses` (~161-163) следуют простому паттерну: принимают
  `session`, возвращают данные, без commit. Импорты Airflow внутри этих
  функций делаются лениво (`from airflow.settings import engine` внутри тела
  функции), а не на уровне модуля — новые импорты моделей Airflow должны
  следовать той же конвенции.
- `airflow_provider_rmq/watcher/views.py` — `RMQWatcherView.subscriptions()`
  (строки 110-121): открывает `with WatcherSession() as session:`, достаёт
  `subs`/`conn_statuses`, закрывает блок, и **только после этого**, уже вне
  `with`, вызывает `self.render_template(...)`. Это значит, что
  `subs`/`conn_statuses` — ORM-объекты, чьи атрибуты Jinja читает уже после
  того, как сессия закрылась (см. Technical Details — важно для правильного
  дизайна обработки сбоя `get_known_dag_ids`).
- `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html` —
  есть две ветки строк (сгруппированная `SubscriptionGroup`, одиночная
  `RMQSubscription`), обе рендерят `{{ item.dag_id }}` в первой `<td>`.
- `tests/watcher/test_views.py` — мок-хелпер `_session_ctx()` переиспользует
  один и тот же мок `session.query.return_value` независимо от того, какая
  модель запрашивается. Проверено эмпирически: незамоканная цепочка
  `session.query(DagModel...).filter(...).all()` на этом моке НЕ падает —
  `MagicMock` сам авто-конфигурирует `__iter__`, без явной настройки
  `.all()` детерминированно отдаёт пустую последовательность. То есть
  существующие тесты в `TestSubscriptionsList` не сломаются молча даже без
  патча — но всё равно стоит явно замокать `get_known_dag_ids` в них, чтобы
  view-тесты оставались изолированным unit-тестом и не тянули по пути
  реальный `from airflow.models import DagModel` из `models.py`.
  Существующий `_make_sub()` в этом же файле возвращает `MagicMock()`, у
  которого `item.is_group` авто-создаётся truthy — важно для теста
  рендеринга шаблона (см. Technical Details).
- `tests/watcher/test_models.py` — фикстура `session` создаёт только
  таблицы `WatcherBase.metadata`, не знает про `DagModel` (отдельный,
  принадлежащий Airflow declarative base) — нужна отдельная фикстура.
- `docs/plans/20260703-reliability-hardening.md` (всё ещё открыт, не
  замёржен) пересекается по `models.py`+`test_models.py` (его Task 5
  «Ленивый WatcherSession», Task 6 — защита источника), `views.py`+
  `test_views.py` (Task 6), `CHANGELOG.md` (Task 4), `readme.md`/
  `readme_ru.md` (Task 16). Функционального конфликта нет — использование
  `with WatcherSession() as session:` и паттерн `get_*`-хелперов
  совместимы с описанным там переходом на ленивую фабрику — но
  исполнителям стоит проверить, какой план приземлится первым, и перед
  стартом второго сверить актуальные diff'ы конфликтующих файлов.
- `docs/plans/completed/20260807-ast-dag-id-constant-resolution.md` — смежный,
  но функционально не зависимый план (см. Обзор). **Реализован и замёржен в
  `main` (`c684b54`)** — то есть это не будущая координация, а уже
  свершившийся факт, который меняет состояние двух файлов из списка Task 5:
  - `readme.md:539` / `readme_ru.md:538` — он **уже добавил** абзац про
    нерезолвимый `dag_id`, причём с формулировкой «не появляется на странице
    вовсе — **не «с бейджем»**, а полностью отсутствует». Эта фраза написана
    когда бейджа ещё не существовало и сейчас висит без референта: Task 5
    должен **отредактировать** её (противопоставив новому бейджу), а не
    дописывать рядом второй абзац на ту же тему;
  - `CHANGELOG.md` — он завёл секцию `## v2.3.0`, которая **не имеет тега**
    (последний тег — `v2.2.0`, версия считается `setuptools_scm` по тегам),
    то есть фактически является unreleased-секцией. Отдельной секции
    `## Unreleased` в этом файле нет и не было — конвенции такой у репозитория
    нет, все секции именованы версиями.

## Подход к разработке

- **Подход к тестам**: Regular — реализуем задачу, затем пишем/обновляем её
  тесты в рамках той же задачи, и только потом переходим дальше.
- Каждую задачу (включая её тесты) доводим до конца, прежде чем начинать
  следующую.
- **Каждая задача с изменением кода ДОЛЖНА включать новые/обновлённые
  тесты** — это не опционально.
- **Все тесты должны проходить перед началом следующей задачи.**
- Обновлять этот файл плана, если скоуп меняется по ходу реализации.

## Стратегия тестирования

- Unit-тесты для `get_known_dag_ids` — реальная in-memory SQLite БД, без
  моков, по конвенции всех остальных тестов хелперов в `test_models.py`.
- Тесты view-слоя через существующий Flask-mock паттерн в
  `tests/watcher/test_views.py`.
- Реальный рендеринг Jinja-шаблона через отдельный `jinja2.Environment` (без
  полного контекста Flask-AppBuilder) — единственный способ доказать, что
  бейдж реально появляется в HTML, а не только доходит до kwargs
  `render_template`. Полный Flask-AppBuilder e2e не нужен.
- Команды тестов (сверено): `pytest tests/` — ровно так вызывает CI
  (`.github/workflows/publish.yml`); `pytest tests/watcher/` — для быстрого
  цикла по ходу задач. `ruff`/`mypy` настроены в `pyproject.toml`, но в CI
  **не запускаются** — этот план их и не требует.

## Отслеживание прогресса

- Отмечать выполненные пункты `[x]` сразу по готовности.
- Новые обнаруженные задачи добавлять с префиксом ➕.
- Проблемы/блокеры документировать с префиксом ⚠️.

## Обзор решения

Три слоя, каждый независимо тестируемый:

1. **Query-хелпер** (`models.py`): `get_known_dag_ids()` запрашивает
   `DagModel.is_active` (тот же движок БД, что и у таблиц watcher'а).
2. **View wiring** (`views.py`): результат передаётся в шаблон через
   **отдельную** `WatcherSession`, изолированную от сессии с
   `subs`/`conn_statuses` — см. Technical Details, почему это важно.
3. **UI** (`subscriptions.html`): бейдж `⚠ dag not found` рядом с любым
   `dag_id`, отсутствующим в множестве известных активных DAG.

## Технические детали

### `get_known_dag_ids` (models.py)

```python
def get_known_dag_ids(session: Session) -> set[str]:
    """Return dag_ids of all Airflow DAGs currently known to be active."""
    from airflow.models import DagModel
    return {
        row[0]
        for row in session.query(DagModel.dag_id)
        .filter(DagModel.is_active.is_(True))
        .all()
    }
```

Ленивый импорт `airflow.models`, по той же конвенции, что уже используют
`_make_session_factory`/`ensure_table_exists` в этом же файле.

### `views.py` + шаблон

Страница Subscriptions сейчас трогает только собственные таблицы watcher'а.
Это добавляет её первую жёсткую рантайм-зависимость от
`airflow.models.DagModel` — чисто косметический бейдж не должен уметь
превращать рабочую admin-страницу в 500-ю ошибку (дрейф схемы, сбой БД или
будущий мажорный апгрейд Airflow — всё это правдоподобно: Airflow 3, как
известно, переименовывает флаги вроде `is_active`, а пин Airflow в этом
репозитории не вечно будет `<3.0.0`).

**Отдельная сессия, не общая с `subs`/`conn_statuses`, и без
`session.rollback()`.** Раннее рассматривался вариант звать
`get_known_dag_ids(session)` в конце того же `with WatcherSession() as
session:`, что и `subs`/`conn_statuses`, и на сбой отвечать
`session.rollback()`. Это заводило тонкий баг: `render_template(...)` в
реальном коде вызывается **после** закрытия `with`-блока (см. Контекст —
`views.py:110-121`), то есть `subs`/`conn_statuses` — ORM-объекты, чьи
атрибуты Jinja читает уже после того, как сессия закрылась. Обычное
закрытие сессии (`Session.close()`, без ошибок) это не ломает — уже
загруженные значения атрибутов остаются доступны на detached объекте. Но
`session.rollback()` **истекает** (`expire`) все объекты в сессии как часть
отката транзакции — то есть `subs`/`conn_statuses`, которые уже были
успешно загружены ДО сбоя `get_known_dag_ids`, стали бы expired, а попытка
Jinja прочитать `item.dag_id` после закрытия сессии дала бы
`DetachedInstanceError` вместо рендеринга страницы. То есть сам fallback на
случай сбоя ломал бы страницу вместо того чтобы просто выключить бейдж.

Правильное решение — полностью изолировать сессию:

```python
try:
    with WatcherSession() as dag_session:
        known_dag_ids = get_known_dag_ids(dag_session)
except Exception:
    log.warning("Failed to look up known Airflow dag_ids for the "
                "'dag not found' badge — badge disabled for this request",
                exc_info=True)
    known_dag_ids = None
```

Этот блок идёт **после** того, как основной `with WatcherSession() as
session: ...` блок (с `subs`/`conn_statuses`) уже закрылся — отдельная
`Session`, отдельный identity map, отдельная транзакция. Сбой (и то, что
`Session.__exit__`/`close()` внутренне делает при выходе из `with
dag_session:` с исключением) никак не может истечь или повредить объекты
из первой, уже закрытой сессии — они просто не связаны. `session.rollback()`
здесь **не нужен и не должен вызываться** — ни на `session`, ни явно на
`dag_session` (закрытие `with`-блока само корректно освобождает
`dag_session` при исключении). Дополнительное соединение к той же БД для
одной страницы, открываемой нечасто (admin UI) — пренебрежимо дешёвая цена
за то, чтобы не задевать состояние уже успешно использованной сессии.

`subscriptions()` передаёт `known_dag_ids` в `render_template`. Шаблон
рендерит, в обеих ветках строк, сразу после `{{ item.dag_id }}`:

```html
{% if known_dag_ids is defined and known_dag_ids is not none and item.dag_id not in known_dag_ids %}
  <span class="label label-warning"
        title="No active Airflow DAG found for this dag_id — a matching message will not trigger a real pipeline run. For dag_file subscriptions, check that dag_id= in @dag(...) resolves to a string literal or a simple module-level constant.">
    ⚠ dag not found
  </span>
{% endif %}
```

`is defined and` — на случай, если `known_dag_ids` вообще не попадёт в
контекст рендера (например при будущем рефакторинге вызова
`render_template` в другом месте). **Важно правильно понимать, от чего
именно защищает этот guard: не от исключения, а от тихого ложного
срабатывания.** Проверено на реально установленном jinja2 3.1.6 (и по
исходникам: `jinja2.runtime.Undefined.__iter__`, runtime.py:898, отдаёт
**пустой** итератор; падает только `StrictUndefined`, runtime.py:1060-1062,
а вебсервер Airflow его не включает):

```
{% if known is not none and x not in known %}BADGE{% endif %}   → 'BADGE'
{% if known is defined and known is not none and x not in known %} → ''
```

То есть без `is defined` шаблон **не падает** — `Undefined is not none`
даёт `True`, `x not in Undefined` тоже `True` (членство в пустом
итераторе), и бейдж `⚠ dag not found` тихо появляется на **каждой** строке
таблицы. Это ровно тот сценарий, ради которого guard и нужен: исполнителю,
который решит проверить «а правда ли упадёт?», код покажет отсутствие
краха, и `is defined` будет выглядеть лишним — а его удаление даст ложные
бейджи на всех подписках. Регрессионный тест на это — последний чекбокс
Task 3 («контекст без ключа → без бейджа и без исключения»).

Гейтинг на `known_dag_ids is not none` означает, что сбой запроса скрывает
бейдж целиком, а не ложно помечает каждую строку. **Пустое, но успешно
полученное** множество (например, свежее окружение, где активных DAG
действительно ноль) считается достоверным результатом — в этом случае все
subscription'ы реально «висят в воздухе», так что бейдж должен показываться
на всех строках; это отличается от «запрос не удался» использованием `None`
(а не `set()`) как сентинела сбоя.

Бейдж только read-only — никакого авто-disable, никакого авто-delete
(только что добавленный DAG может ещё не появиться в `DagModel`; трактовать
эту гонку как ошибку было бы активно неверно). `is_paused` не проверяется —
paused DAG всё ещё существует и является другим, легитимным состоянием.
Важная оговорка: `consumer.py::_sync_trigger` (см. Обзор) сегодня фильтрует
`is_paused=False` наравне с `is_active=True` — то есть подписка на реально
существующий, но **paused** DAG тоже никогда фактически не сработает, тем
же WARNING-путём, что и для отсутствующего DAG. Бейдж это сознательно не
покрывает: пауза — частое, обратимое, никак не «сломанное» состояние (сам
факт паузы уже виден на странице DAG в основном UI Airflow), и подсвечивать
её как «dag not found» было бы вводящим в заблуждение в противоположную
сторону. Это существующее поведение — просто фиксируем его здесь, чтобы не
удивляться на Post-Completion, если paused DAG не триггерится несмотря на
«зелёный» бейдж.

## Что куда идёт

- **Implementation Steps**: все изменения кода/тестов/документации ниже —
  всё в пределах этого репозитория.
- **Post-Completion**: ручная проверка на реальном инстансе Airflow.

## Implementation Steps

### Task 0: Preflight (координация)

- [ ] **Дефолт на момент написания: этот план приземляется первым.** `docs/plans/20260703-reliability-hardening.md` не начат (нет коммитов против него), и его пересекающиеся задачи не трогают `subscriptions()`: Task 5 (ленивая фабрика `WatcherSession`) совместим с `with WatcherSession() as session:`, Task 6 правит `create`/`edit`/`edit_group`, Task 4 — `CHANGELOG.md`, Task 16 — оба readme. Подтвердить, что это всё ещё так (`git log --oneline` по конфликтующим файлам), и зафиксировать решение здесь. Если порядок изменится и reliability-план приземлится раньше — заново сверить актуальные diff'ы `models.py`/`views.py`/`test_models.py`/`test_views.py`/`CHANGELOG.md`/`readme.md`/`readme_ru.md`, не полагаясь на описания «как было» в обоих планах
- [ ] сверить состояние файлов, уже изменённых замёрженным AST-планом (см. Контекст): `readme.md:539`, `readme_ru.md:538` (абзац про нерезолвимый `dag_id` с фразой «не «с бейджем»») и нетегированная секция `## v2.3.0` в `CHANGELOG.md` — Task 5 рассчитывает именно на это состояние

### Task 1: Хелпер `get_known_dag_ids` в models.py

**Files:**
- Modify: `airflow_provider_rmq/watcher/models.py`
- Modify: `tests/watcher/test_models.py`
- Create: `docs/adr/0006-badge-dag-lookup-not-unified-with-sync-trigger.md`

- [ ] добавить `get_known_dag_ids(session: Session) -> set[str]` по спеке из Technical Details — ленивый импорт `from airflow.models import DagModel` внутри функции, `filter(DagModel.is_active.is_(True))`
- [ ] **зафиксировать в ADR, почему фильтр намеренно НЕ совпадает с `consumer.py::_sync_trigger`** (`is_active` только, против `is_active=True, is_paused=False` там). Сейчас это обоснование живёт лишь в Technical Details этого плана, а Task 5 унесёт файл в `docs/plans/completed/` — через полгода расхождение будет выглядеть недосмотром, а тест на paused DAG — произвольным. В репозитории уже есть ровно такая конвенция: `docs/adr/0001..0005` — короткие ADR в стиле «…не унифицировать». Завести `docs/adr/0006-*.md` (на английском, как остальные ADR) + двухстрочный комментарий в самой `get_known_dag_ids` со ссылкой на тест `test_..._paused_dag_is_included`
- [ ] существующая фикстура `session` в `test_models.py` создаёт только таблицы `WatcherBase.metadata` — она **не знает** про `DagModel` (отдельный, принадлежащий Airflow declarative base). Добавить в том же файле отдельную фикстуру `session_with_dagmodel`, построенную так же, как `session` (in-memory SQLite, function-scoped, дропается в teardown), но дополнительно выполняющую `DagModel.__table__.create(engine, checkfirst=True)` на том же движке перед тем как отдать session. Teardown зеркалит существующую фикстуру (`WatcherBase.metadata.drop_all`) — отдельно дропать таблицу `dag` не нужно и не следует: движок in-memory и создаётся заново на каждый тест
- [ ] импортировать `DagModel` **внутри** тела фикстуры/тестов, а не на уровне модуля: `test_models.py` сегодня не импортирует Airflow вообще, а модульный `from airflow.models import DagModel` втянет в быстрый unit-модуль весь ORM Airflow (плюс `RemovedInAirflow3Warning`). Это та же ленивая конвенция, которой следует сам `models.py`
- [ ] добавить тесты в `TestGetKnownDagIds` (реальная БД, без моков — по конвенции всех остальных тестов хелперов в этом файле): возвращаются только строки с `is_active=True`; строки с `is_active=False` исключаются; пустая таблица возвращает `set()`; **`DagModel(is_active=True, is_paused=True)` — включается в результат** (закрепляет тестом осознанное решение из Technical Details не фильтровать по `is_paused`, а не только держать его в комментарии — без этого теста будущая правка может незаметно добавить `is_paused=False` и тихо изменить задокументированную семантику)
- [ ] прогнать `pytest tests/watcher/test_models.py` — должно проходить перед Task 2

### Task 2: Встраивание `known_dag_ids` в view Subscriptions

**Files:**
- Modify: `airflow_provider_rmq/watcher/views.py`
- Modify: `tests/watcher/test_views.py`

- [ ] импортировать `get_known_dag_ids` в `views.py` рядом с существующим импортом `get_conn_statuses`
- [ ] в `RMQWatcherView.subscriptions()` **после** того, как основной `with WatcherSession() as session: ...` блок (с `subs`/`conn_statuses`) уже закрылся, открыть **отдельный** `with WatcherSession() as dag_session:` и вызвать в нём `get_known_dag_ids(dag_session)`, обернув весь этот блок в `try/except Exception` по спеке из Technical Details (`known_dag_ids = None` + `log.warning(..., exc_info=True)` при сбое — **без** `session.rollback()`, он не нужен и не должен вызываться ни на одной из сессий: изоляция через отдельную сессию — единственный правильный механизм, см. подробное обоснование в Technical Details про `DetachedInstanceError`), и передать `known_dag_ids` в `self.render_template(...)` (метод самого FAB-вью, а не свободная функция Flask `render_template` — в тестах он мокается как `view.render_template`, импортировать в `views.py` из Flask ничего для этого не нужно)
- [ ] **обновить три существующих теста `TestSubscriptionsList`** (`test_subscriptions_list_returns_200`, `test_subscriptions_list_shows_conn_status`, `test_subscriptions_list_shows_consumer_status_badge`), добавив им `patch("airflow_provider_rmq.watcher.views.get_known_dag_ids", return_value=set())` — эмпирически проверено: без патча `known_dag_ids` посчитается по-настоящему, но не упадёт (`session` там `MagicMock`, а `MagicMock` сама авто-конфигурирует `__iter__` и без явной настройки `filter(...).all()` детерминированно отдаёт пустую последовательность) — патч нужен не чтобы избежать падения, а чтобы view-тесты оставались изолированным unit-тестом и не тянули по пути реальный `from airflow.models import DagModel` из `models.py`
- [ ] добавить тест `test_subscriptions_list_passes_known_dag_ids` — замокать `get_known_dag_ids` так, чтобы вернуть множество, не содержащее `dag_id` какого-то subscription, проверить что `render_template` получает `known_dag_ids` соответствующим образом (имя теста намеренно не «shows_badge» — фактическое появление бейджа в HTML проверяется отдельно, в Task 3; этот тест — только про то, что view прокидывает `known_dag_ids` в kwargs)
- [ ] добавить тест, подтверждающий, что при `dag_id`, присутствующем в `known_dag_ids`, поведение не меняется (нет регрессии на happy path)
- [ ] добавить тест `test_subscriptions_list_known_dag_ids_none_on_lookup_failure` — замокать `get_known_dag_ids` так, чтобы бросала исключение, проверить (через `caplog`), что `subscriptions()` всё равно рендерится (исключение не пробрасывается), `render_template` получает `known_dag_ids=None`, и что `log.warning(..., exc_info=True)` реально был вызван — иначе сам сбой диагностики останется незамеченным
- [ ] прогнать `pytest tests/watcher/test_views.py` — должно проходить перед Task 3

### Task 3: Бейдж `⚠ dag not found` в шаблоне + тест реального рендеринга

**Files:**
- Modify: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`
- Modify: `tests/watcher/test_views.py`

- [ ] в `subscriptions.html` добавить бейдж `⚠ dag not found` сразу после `{{ item.dag_id }}` в **обеих** ветках (grouped-строка и single-строка), с условием `{% if known_dag_ids is defined and known_dag_ids is not none and item.dag_id not in known_dag_ids %}` — именно с `is defined and`, а не только `is not none`. **Не «иначе упадёт»** (проверено на jinja2 3.1.6: не падает, см. Technical Details): без `is defined` отсутствие `known_dag_ids` в контексте даёт `Undefined is not none` → `True` и `item.dag_id not in Undefined` → `True` (членство в пустом итераторе), то есть бейдж тихо появляется на **каждой** строке. Guard защищает от ложного срабатывания, а не от исключения — не удалять его «как ненужный», убедившись что краха нет
- [ ] настроить `TestSubscriptionsTemplateRendering` — окружение `jinja2.Environment` напрямую (без полного контекста Flask-AppBuilder), `loader = ChoiceLoader([FileSystemLoader(".../watcher/templates"), DictLoader({"rmq_watcher/base.html": "{% block content %}{% endblock %}"})])` (`{% extends base_template %}` резолвит имя через loader, одного `FileSystemLoader` недостаточно), заглушки в `env.globals` под реальные сигнатуры вызовов из шаблона: `get_flashed_messages` → `lambda **kw: []`, `url_for` → `lambda *a, **kw: "#"`, `csrf_token` → `lambda: "test-token"` (фильтр `| tojson` — встроенный в Jinja2, регистрации не требует)
- [ ] **⚠ в каждом вызове `render(...)` обязательно передавать `base_template="rmq_watcher/base.html"`** (значение должно совпадать с ключом `DictLoader` выше). Первая строка шаблона — `{% extends base_template %}`, где `base_template` не имя файла, а **контекстная переменная** (её в реальном рантайме подставляет Flask-AppBuilder). Без неё `render(...)` падает с `UndefinedError: 'base_template' is undefined` ещё до того, как что-либо отрендерится — то есть без этого пункта весь харнесс Task 3 не работает, а не просто отдаёт неверный HTML
- [ ] в этом же классе завести настоящие (не `MagicMock`) фикстуры строк — существующий `_make_sub()` возвращает `MagicMock()`, у которого `item.is_group` авто-создаётся truthy, из-за чего в шаблоне `{% if item.is_group is defined and item.is_group %}` любая строка попадала бы в grouped-ветку: grouped-строка — реальный `SubscriptionGroup(...)` из `views.py`; single-строка — объект без атрибута `is_group` вовсе (реальный `RMQSubscription(...)` или `types.SimpleNamespace`)
- [ ] тест: неизвестный `dag_id` в grouped-строке → `"dag not found"` присутствует в HTML
- [ ] тест: неизвестный `dag_id` в single-строке → `"dag not found"` присутствует в HTML (отдельно от grouped — это и есть то, что ловит потенциальный copy-paste промах между двумя ветками шаблона)
- [ ] тест: известный `dag_id` → `"dag not found"` отсутствует
- [ ] тест: `known_dag_ids=None` → отсутствует для всех строк
- [ ] тест: контекст рендера вовсе без ключа `known_dag_ids` → рендерится без исключения и без бейджа (проверяет `is defined`)
- [ ] прогнать `pytest tests/watcher/test_views.py` — должно проходить перед Task 4

### Task 4: Проверка критериев приёмки

- [ ] **проверить на уровне исходной задачи, а не пересказом тестов**: подписка, созданная через UI с опечаткой в `dag_id`, и `dag_file`-подписка на переименованный/удалённый DAG обе получают бейдж; при этом grouped-строка бейджится по `dag_id` группы, а single-строка — по своему собственному. Это единственный критерий здесь, сформулированный в терминах проблемы из Обзора — остальные три пересказывают уже написанные тесты
- [ ] проверить: сбой `get_known_dag_ids` отключает бейдж, а не бросает исключение (тест из Task 2)
- [ ] проверить: бейдж появляется в HTML в обеих ветках шаблона для неизвестного `dag_id`, и отсутствует для известного (тесты из Task 3)
- [ ] прогнать полный набор: `pytest tests/` (совпадает с вызовом в CI, `.github/workflows/publish.yml`)

### Task 5: [Final] Обновление документации

**Files:**
- Modify: `readme.md`
- Modify: `readme_ru.md`
- Modify: `CHANGELOG.md`

- [ ] **описание бейджа — в раздел про UI, а не в "How it works"**: `readme.md`, `### Subscription Management` (~строка 719) — короткий абзац: любой subscription, чей `dag_id` не соответствует активному Airflow DAG, помечается бейджем `⚠ dag not found`. Проговорить два ограничения: (а) `is_paused` не проверяется — paused DAG бейджа не получит, хотя сообщение его тоже не запустит (см. Technical Details/ADR 0006); (б) только что добавленный DAG может временно бейджиться, пока Scheduler его не распарсил — это ожидаемо, не баг
- [ ] **⚠ отредактировать уже существующий абзац**, а не дописывать второй на ту же тему: `readme.md:539` (и `readme_ru.md:538`) уже содержит текст, добавленный замёрженным AST-планом, с фразой «не появляется на странице Subscriptions вовсе — **не «с бейджем»**, а полностью отсутствует». Она писалась когда бейджа не существовало и сейчас ссылается в пустоту. Переформулировать так, чтобы она явно противопоставлялась **новому, реально существующему** бейджу: нерезолвимый `dag_id` — единственный случай, который бейдж не ловит, потому что подсвечивать нечего (строки нет)
- [ ] отзеркалить обе правки в `readme_ru.md`: новый абзац — в `### Управление подписками` (~строка 717), правку существующего — на строке 538
- [ ] добавить запись в `CHANGELOG.md`: **Added** — страница Subscriptions теперь помечает бейджем `⚠ dag not found` любой subscription, чей `dag_id` не соответствует ни одному активному Airflow DAG. **Дописывать в существующую секцию `## v2.3.0`** — она заведена AST-планом и не имеет тега (`git tag` → последний `v2.2.0`), то есть является unreleased-секцией. Секции `## Unreleased` в этом файле нет и заводить её не надо — конвенции такой у репозитория нет. Перед правкой сверить `git tag`: если к этому моменту `v2.3.0` уже выпущен — завести `## v2.4.0` над ней
- [ ] прогнать `pytest tests/` ещё раз — должно проходить
- [ ] перенести этот план в `docs/plans/completed/`

## Post-Completion
*Пункты, требующие ручного вмешательства или внешних систем — без чекбоксов, только информационно*

**Ручная проверка** (на реальном/staging-инстансе Airflow с этим провайдером):
- перезапустить Scheduler после деплоя этого изменения; убедиться, что
  страница RMQ Watcher Subscriptions рендерится без ошибок и бейдж
  `⚠ dag not found` визуально присутствует/отсутствует как ожидается на
  паре заведомо корректных и заведомо неверных (например, UI-подписка с
  опечаткой в `dag_id`) строк
- намеренно вызвать сбой `get_known_dag_ids` (например временно
  переименовав/недоступной БД в тестовом окружении) и убедиться, что
  страница остаётся рабочей, просто без бейджей

**Обновления во внешних системах**: не требуются.
