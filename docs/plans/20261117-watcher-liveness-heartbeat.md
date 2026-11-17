# Живость watcher'а: heartbeat в метабазе и баннер в UI

## Overview

- Watcher-процесс, умерший сам (OOM, segfault, `kill -9`), не перезапускается до рестарта
  шедулера (ADR 0008), а подписки после этого молча не обрабатываются: строки
  `consumer_status` застывают на последнем значении, обычно `listening`. Сигналы сейчас —
  StatsD-счётчик `rmq_watcher.cycle_completed`, которому нужен внешний мониторинг, и отметка
  `last_reconcile_at` в блоке Connections страницы Subscriptions, которая одна на `conn_id`
  и при нескольких репликах прячет мёртвую за живой.
- План делает смерть watcher'а видимой средствами самого Airflow, без внешних систем, без
  отдельных DAG'ов и без кода в процессе шедулера:
  - каждый запуск watcher-процесса ведёт свою строку в новой таблице
    `rmq_watcher_heartbeat` и обновляет её в конце каждого прохода цикла;
  - главная страница Airflow (список DAG'ов) показывает всем пользователям баннер штатным
    механизмом `UIAlert` / `DASHBOARD_UIALERTS`, когда при включённых подписках живого
    watcher'а нет или одна из реплик умерла;
  - страница Subscriptions показывает панель по репликам, а статусы подписок при
    отсутствии живого watcher'а — как «неизвестно».
- Вне плана, отдельными решениями: перезапуск умершего процесса (супервизор) и
  push-уведомления о смерти; ограничение памяти watcher'а (неограниченный prefetch как
  вероятная причина OOM).

## Context (from discovery)

- `airflow_provider_rmq/watcher/models.py`: `WatcherBase`, `RMQSubscription`,
  `RMQConnStatus` (:66, одна строка на `conn_id`, `last_reconcile_at` :81),
  `ensure_table_exists` (:148, `create_all` + `_add_missing_columns`), `WatcherSession` (:96).
- `airflow_provider_rmq/watcher/listener.py`:
  - `_VARIABLE_TIMEOUT = 15.0` (:57), `_STOP_TIMEOUT = 30.0` (:132),
    `_CHILD_STOP_TIMEOUT = 5.0` (:165, через столько шедулер шлёт SIGKILL после SIGTERM),
    `_CYCLE_POOL_WORKERS = 4` (:175);
  - `run` (:723), `_ensure_pools` (:742), `_run_loop` (:776, перезапуск `_main` после краша
    в том же процессе с паузой 30 с), `_main` (:799, `incr("rmq_watcher.cycle_completed")`
    после прохода в бюджете, `finally: _stop_manager` :831-832), `_run_cycle` (:854,
    проглатывает `_StopRequested` :916-917), `_cycle_step` (:923, первой строкой
    `_raise_if_stopping` :984), `_refresh_settings` (:1038).
- `airflow_provider_rmq/utils/executor.py`: `BoundedExecutor`, `PoolSaturated` (:93).
- `airflow_provider_rmq/watcher/tunables.py`: `DEFAULT_RECONCILE_INTERVAL = 60`,
  `cycle_timeout`, `MIN_CYCLE_TIMEOUT = 300`, `stale_after(interval, budget)` (:68).
- `airflow_provider_rmq/watcher/views.py`: `_stale_after` (:299, читает Variables),
  `_build_conn_status_rows` (:322, отрицательный возраст ограничен нулём :345),
  `method_permission_name` (:364), страница `/subscriptions` (:375).
- `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`: блок Connections
  с пометкой устаревшего `last_reconcile_at` (:86).
- `airflow_provider_rmq/watcher/plugin.py`: верхнеуровневый импорт `views` (:11-18) — модуль
  загружается во всех процессах Airflow; `_bp` (:37); права Op (:49-55);
  `_serves_the_web_ui(app)` (:106, отличает webserver от CLI FAB, который тоже регистрирует
  блюпринт); `_bp.record_once(_grant_op_access)` (:216); `on_load` (:232, DDL в каждом
  процессе, см. `docs/backlog/plugin-on-load-runs-ddl-in-every-process.md`).
- Airflow (сверено по 2.11.2): `airflow.www.utils.UIAlert` (`__init__` присваивает
  `message`, `category`, `roles`, `html`; `should_show(appbuilder)`),
  `airflow.settings.DASHBOARD_UIALERTS`, отбор `should_show` в `www/views.py` (:1070-1071),
  вывод `m.message` / `m.category` в `www/templates/airflow/dags.html` (:84-85) — при рендере,
  позже отбора. Для 2.9.1 сверяется в Task 1.
- Тесты: `tests/watcher/test_models.py`, `test_listener.py`, `test_views.py`,
  `test_plugin.py`.
- Документация: `readme.md` / `readme_ru.md`, раздел «The watcher process» (абзац «A watcher
  process that dies is not restarted», `readme.md:973`, и «Liveness metric»), раздел про
  страницу Subscriptions; `docs/adr/0008-watcher-child-process.md` (раздел «Перезапуска нет,
  живость показывает метрика», :311); `CHANGELOG.md` (неопубликованный раздел `## v2.7.0`);
  `docs/backlog/watcher-child-death-unnoticed.md`.

## Development Approach

- **testing approach**: Regular (код, затем тесты в той же задаче)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - write unit tests for new functions/methods
  - write unit tests for modified functions/methods
  - add new test cases for new code paths
  - update existing test cases if behavior changes
  - tests cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change: `.venv/bin/python -m pytest tests/ -q`
- maintain backward compatibility: новая таблица создаётся существующим механизмом схемы;
  её отсутствие или недоступность БД не роняет ни watcher, ни страницы

## Testing Strategy

- **unit tests**: required for every task
- оценка живости — чистая функция, тестируется таблицей сценариев без БД
- модель и запись heartbeat — на SQLite-сессии, как существующие тесты `test_models.py`
- цикл и остановка — через существующие фикстуры `test_listener.py`
- `UIAlert` и страница — через фикстуры `test_views.py` / `test_plugin.py`
- e2e UI-тестов в проекте нет; проверки на живом Airflow — в Post-Completion

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview

- **Строка на запуск процесса.** Watcher-процесс при старте создаёт `WatcherIdentity`
  (UUID, hostname, PID, `started_at`) и ведёт строку `rmq_watcher_heartbeat` с этим `id`.
  Перезапуск `_main` внутри процесса (`_run_loop`) строку не меняет: живость — свойство
  процесса. Hostname и PID — для человека, а не для идентичности: в контейнерах они
  повторяются.
- **Heartbeat — конец прохода в бюджете.** Строка обновляется там же, где растёт
  `cycle_completed`: проход, упёршийся в бюджет, отметку не двигает. Фонового пинга нет —
  он показывал бы живым зависший цикл.
- **Порог устаревания пишет сам watcher.** С каждой отметкой строка получает
  `stale_after_seconds` — срок, за который этот процесс с его действующими настройками
  обязан оставить следующую отметку. Webserver не читает Variables для оценки живости, и
  реплики с разными настройками оцениваются каждая по своему сроку.
- **Штатная остановка помечается первой.** `stopped_at` записывается первым действием
  `finally` в `_main`, когда остановка запрошена, — до `_stop_manager`, мимо `_cycle_step`,
  с границей около 2 с: шедулер даёт процессу 5 с до SIGKILL.
- **Оценка — одна чистая функция.** `assess_liveness` по строкам, текущему времени,
  признаку «есть включённые подписки» и признаку «есть свежий `last_reconcile_at`»
  возвращает вердикт; её вызывают баннер, панель и отображение статусов, поэтому они не
  расходятся.
- **Баннер — штатный `UIAlert`, только в webserver'е.** `_bp.record_once` с проверкой
  `_serves_the_web_ui(state.app)` вызывает `_install_dashboard_alert`, который лениво
  импортирует `UIAlert`, определяет подкласс и добавляет экземпляр в
  `airflow.settings.DASHBOARD_UIALERTS`. Модули плагина `airflow.www.utils` на уровне модуля
  не импортируют, поэтому другие процессы этот код не загружают.
- **Чистка — отдельно и без гарантий.** После коммита своей отметки живой watcher отдельной
  транзакцией удаляет чужие строки, чья последняя отметка старше суток; её сбой не трогает
  отметку. Строку умершей реплики можно убрать и вручную — кнопкой на странице Subscriptions.

## Technical Details

### Таблица `rmq_watcher_heartbeat`

| колонка | тип | смысл |
|---|---|---|
| `id` | `String(36)`, PK | UUID запуска процесса |
| `hostname` | `String(250)` | `socket.gethostname()` |
| `pid` | `Integer` | PID watcher-процесса |
| `started_at` | `DateTime` | старт процесса |
| `last_cycle_at` | `DateTime`, nullable | конец последнего прохода в бюджете |
| `stale_after_seconds` | `Float` | срок следующей отметки при действующих настройках |
| `stopped_at` | `DateTime`, nullable | штатная остановка |

Время — naive UTC из процесса, как у `last_reconcile_at`. Таблица создаётся
`ensure_table_exists` (`create_all`).

`stale_after_seconds` = `stale_after(interval, budget)` + `_VARIABLE_TIMEOUT` (чтение
настроек вне бюджета) + граница записи heartbeat, где `interval` и `budget` — действующие
значения процесса после `_refresh_settings`.

**Последняя отметка** строки — `stopped_at`, иначе `last_cycle_at`, иначе `started_at`.

### Вердикт живости

- Строка **остановлена**, если `stopped_at` задан.
- Строка **жива**, если не остановлена и `now − последняя отметка ≤ stale_after_seconds`.
  Отрицательный возраст (сдвиг часов между хостами) считается нулём.
- Строка **мертва**, если не остановлена и не жива.
- Мёртвая строка **заменена**, если есть строка с тем же `hostname`, чей `started_at`
  позже последней отметки мёртвой: процесс запущен после того, как прежний замолчал
  (рестарт шедулера в том же контейнере). Одновременные реплики на одном hostname друг
  друга не заменяют.

| состояние | условие | баннер |
|---|---|---|
| `unused` | включённых подписок нет | нет |
| `legacy` | включённые подписки есть, строк heartbeat нет, есть свежий `last_reconcile_at` | нет (watcher версии без heartbeat; на панели — пояснение) |
| `never` | включённые подписки есть, строк heartbeat нет, свежего `last_reconcile_at` нет | красный |
| `down` | включённые подписки есть, строки есть, живых нет | красный |
| `degraded` | есть живые и есть незаменённые мёртвые | жёлтый |
| `ok` | есть живые, незаменённых мёртвых нет | нет |

«Свежий `last_reconcile_at`» для `legacy`/`never` — моложе `_stale_after()` страницы; он
считается только когда строк heartbeat нет, то есть Variables читаются лишь в этом
переходном случае.

Тексты баннера — без hostname и PID (их видят все пользователи, включая
`AUTH_ROLE_PUBLIC`); подробности — на панели Subscriptions:

- `never`: «RMQ watcher has not reported since the subscriptions were enabled — subscriptions
  are not consumed.»
- `down`: «RMQ watcher has not completed a cycle since `<время самой свежей отметки>` —
  subscriptions are not consumed and event-triggered DAGs do not run.» Если все строки
  остановлены штатно — «RMQ watcher stopped at `<время>` and no Scheduler runs it.»
- `degraded`: «One RMQ watcher replica has not completed a cycle since `<время>`; other
  replicas are consuming.»

Каждый текст ссылается на `/rmq-watcher/subscriptions`. Баннер видят все пользователи
(`roles=None`).

### Запись из цикла

Все записи heartbeat — шаг с одним именем `"heartbeat"`, чтобы зависшая запись занимала не
больше одного воркера cycle-пула из четырёх.

- **Старт процесса.** При первом входе в `_main` процесса (не при перезапусках `_main`) —
  upsert строки (`id`, `hostname`, `pid`, `started_at`, `stale_after_seconds`) через
  `_cycle_step` с собственной границей. Upsert не сбрасывает `last_cycle_at`. Сбой —
  WARNING, цикл стартует.
- **Конец прохода в бюджете** (рядом с `incr("rmq_watcher.cycle_completed")`), если
  остановка не запрошена: upsert `last_cycle_at` и `stale_after_seconds` через
  `_cycle_step`, коммит; затем отдельной транзакцией best-effort чистка чужих строк, чья
  последняя отметка старше суток. Upsert, а не UPDATE: строку, удалённую кнопкой Forget
  или не вставленную при старте, следующий проход восстанавливает. Сбой записи,
  `_StepInFlight`, `PoolSaturated` — WARNING, цикл продолжается; `_StopRequested` — тихо.
- **Штатная остановка.** Первым действием `finally` в `_main`, если `_stop_event.is_set()`:
  `record_heartbeat_stop`, поданный прямо в cycle-пул (`submit` + `call_with_timeout(
  asyncio.wrap_future(...), 2 с)`), мимо `_raise_if_stopping` и учёта шагов; затем
  `_stop_manager`. `PoolSaturated`, таймаут и сбой — WARNING; строка тогда просто
  устареет. Краш `_main` (остановка не запрошена) `stopped_at` не ставит.

### Баннер в webserver'е

- `_install_dashboard_alert(state)` через `_bp.record_once`: выходит, если
  `_serves_the_web_ui(state.app)` ложно; лениво импортирует `UIAlert`, определяет
  `_WatcherLivenessAlert` и перепривязывает список:
  `settings.DASHBOARD_UIALERTS = [*settings.DASHBOARD_UIALERTS, alert]` (список мог быть
  задан кортежем в `airflow_local_settings`); повторный вызов второй экземпляр не
  добавляет.
- `_WatcherLivenessAlert` не вызывает `UIAlert.__init__`: задаёт `roles = None`,
  `html = True`, а `message` и `category` — свойства без записи, читающие снимок вердикта
  из `flask.g`.
- `should_show(self, *args, **kwargs)` берёт вердикт из кэша и кладёт его в `flask.g`;
  `message` и `category` при рендере читают только `flask.g` и в БД не ходят, так что
  отбор и текст одного запроса не расходятся.
- Кэш — неизменяемая пара `(expires_at, verdict)`, заменяемая целиком, 30 с на процесс
  воркера webserver'а; перечтение под `threading.Lock`, чтобы промах кэша не дал залп
  запросов. Промах — один SELECT по `rmq_watcher_heartbeat`, один `EXISTS` по включённым
  подпискам и, только при пустой таблице heartbeat, чтение `last_reconcile_at` и
  `_stale_after()`.
- Сбой чтения (нет таблицы, БД недоступна) — баннер не показывается, WARNING раз на период
  кэша: главная страница Airflow не падает из-за плагина.
- Подставляемое в HTML время экранируется.

### Страница Subscriptions

- Панель «Watcher processes» над блоком Connections: строки heartbeat (hostname, PID,
  started, последняя отметка и её возраст, срок, состояние alive / dead / replaced /
  stopped), пояснение для `legacy`, тот же баннер вверху страницы.
- Кнопка «Forget» у мёртвой строки: POST-эндпоинт с CSRF, право `"forget": "delete"` в
  `method_permission_name`; для живой и остановленной строки эндпоинт отказывает.
- При вердикте `down` или `never` статусы подписок и `conn_id` выводятся как `unknown` с
  подсказкой «watcher is not running — last recorded: `<status>`».

### Что делает документация явным

- Пересоздание контейнера, в котором watcher не успел записать `stopped_at` (SIGKILL), и
  смена hostname оставляют мёртвую строку: при живых репликах — жёлтый баннер до кнопки
  Forget или до чистки через сутки.
- Жёлтый баннер о мёртвой реплике при живых соседях гаснет сам через сутки, когда строку
  удаляет чистка, — даже если реплика не поднялась.
- Один проход, упёршийся в бюджет, может на короткое время дать `down` у живого процесса:
  перезапуск `_main` ждёт 30 с и добавляет время остановки менеджера.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): код, тесты, документация в этом репозитории.
- **Post-Completion** (no checkboxes): проверка на живом Airflow, выкладка на прод.

## Implementation Steps

### Task 1: Сверить `UIAlert` / `DASHBOARD_UIALERTS` и `record_once` с Airflow 2.9.1

**Files:**
- Modify: `docs/plans/20261117-watcher-liveness-heartbeat.md`

- [ ] по исходникам Airflow 2.9.1 (тег `2.9.1` в apache/airflow) подтвердить: атрибуты,
      которые `UIAlert.__init__` присваивает, сигнатуру `should_show`, что
      `views.Airflow.index` фильтрует `settings.DASHBOARD_UIALERTS` вызовом `should_show`
      на каждый запрос, что `dags.html` читает `m.message` и `m.category` при рендере,
      что `flask.g` доступен в обоих местах одного запроса
- [ ] подтвердить, что `_serves_the_web_ui(state.app)` отличает webserver от CLI FAB в 2.9.1
      так же, как для `_grant_op_access`
- [ ] записать расхождения с 2.11.2 в этот план (⚠️) и поправить Technical Details
- [ ] тестов задача не добавляет: она меняет только план

### Task 2: Модель `RMQWatcherHeartbeat` и функции записи

**Files:**
- Modify: `airflow_provider_rmq/watcher/models.py`
- Modify: `tests/watcher/test_models.py`

- [ ] frozen-dataclass `WatcherIdentity(id, hostname, pid, started_at)`
- [ ] модель `RMQWatcherHeartbeat` (`rmq_watcher_heartbeat`, колонки из Technical Details)
- [ ] `record_heartbeat_start(session, identity, stale_after_seconds)` — upsert, не
      сбрасывающий `last_cycle_at`; `record_heartbeat_cycle(session, identity,
      last_cycle_at, stale_after_seconds)` — upsert
- [ ] `prune_heartbeats(session, keep_id, older_than)` — удаление чужих строк по последней
      отметке; `record_heartbeat_stop(session, id, stopped_at)`;
      `forget_heartbeat(session, id)`; `get_heartbeats(session)`
- [ ] тесты: таблица создаётся `ensure_table_exists`; upsert старта поверх строки с
      `last_cycle_at` его сохраняет; upsert прохода поверх существующей и отсутствующей
      строки; чистка не трогает свою строку и свежие чужие и удаляет старые по каждой из
      трёх отметок; stop и forget
- [ ] run tests — must pass before task 3

### Task 3: Оценка живости `assess_liveness`

**Files:**
- Create: `airflow_provider_rmq/watcher/liveness.py`
- Create: `tests/watcher/test_liveness.py`

- [ ] `LivenessVerdict` (состояние `unused | legacy | never | down | degraded | ok`; строки
      по категориям alive / dead / replaced / stopped; время для текста)
- [ ] `assess_liveness(rows, now, has_enabled_subscriptions, has_fresh_reconcile)` — правила
      из Technical Details: порог из строки, отрицательный возраст как ноль, «заменена» по
      `hostname` и `started_at` позже последней отметки
- [ ] `banner_text(verdict)` и `banner_category(verdict)` — тексты без hostname и PID
- [ ] тесты: каждое состояние таблицы; граница порога (ровно `stale_after_seconds` — жива);
      строка без `last_cycle_at` до и после порога от `started_at`; отрицательный возраст;
      у двух строк разные пороги
- [ ] тесты: рестарт в том же контейнере — заменена; две одновременные реплики на одном
      hostname, одна умирает — `degraded`, не заменена; смена hostname — не заменена;
      все строки остановлены — `down` с текстом об остановке; пустая таблица при свежем и
      устаревшем `last_reconcile_at` — `legacy` и `never`
- [ ] run tests — must pass before task 4

### Task 4: Запись heartbeat из цикла watcher'а

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] `WatcherIdentity` создаётся один раз на процесс в `run`; срок
      `stale_after_seconds` считается из действующих interval и бюджета после
      `_refresh_settings`
- [ ] старт процесса: при первом входе в `_main` — шаг `"heartbeat"` с
      `record_heartbeat_start`; сбой — WARNING
- [ ] конец прохода в бюджете без запрошенной остановки: шаг `"heartbeat"` с
      `record_heartbeat_cycle`, затем best-effort `prune_heartbeats` отдельной транзакцией;
      `_StopRequested` — тихо, сбой, `_StepInFlight`, `PoolSaturated` — WARNING
- [ ] остановка: первым действием `finally` в `_main` при `_stop_event.is_set()` —
      `record_heartbeat_stop` прямой подачей в cycle-пул с границей 2 с, затем
      `_stop_manager`
- [ ] тесты: проход в бюджете двигает `last_cycle_at` и пишет срок; проход, упёршийся в
      бюджет, — нет; смена интервала меняет срок в строке со следующего прохода;
      перезапуск `_main` после краша сохраняет `id`, `started_at` и не пишет старт заново
- [ ] тесты: при выставленном stop event `stopped_at` записывается, раньше
      `manager.stop()`, и зависший `manager.stop()` этому не мешает; краш `_main`
      `stopped_at` не ставит; остановка не даёт WARNING о heartbeat прохода
- [ ] тесты: сбой записи и сбой чистки не прерывают цикл и не мешают `cycle_completed`;
      сбой чистки не откатывает отметку; зависшая запись не подаётся повторно;
      `PoolSaturated` при остановке — WARNING без исключения
- [ ] run tests — must pass before task 5

### Task 5: Баннер на главной странице Airflow

**Files:**
- Modify: `airflow_provider_rmq/watcher/plugin.py`
- Create: `airflow_provider_rmq/watcher/dashboard_alert.py`
- Create: `tests/watcher/test_dashboard_alert.py`
- Modify: `tests/watcher/test_plugin.py`

- [ ] `dashboard_alert.py`: чтение вердикта с кэшем (неизменяемая пара, 30 с, `Lock` на
      перечтение; `last_reconcile_at` и `_stale_after()` — только при пустой таблице
      heartbeat); сбой чтения — вердикт «не показывать» и WARNING раз на период
- [ ] `install_dashboard_alert(app)`: ленивый импорт `UIAlert`, подкласс без вызова
      `UIAlert.__init__`, `should_show(*args, **kwargs)` со снимком в `flask.g`,
      `message`/`category` из `flask.g`, перепривязка `DASHBOARD_UIALERTS`, без
      повторного экземпляра
- [ ] `plugin.py`: `_bp.record_once`, вызывающий установку только при
      `_serves_the_web_ui(state.app)`
- [ ] тесты: `should_show` и текст для каждого состояния; кэш не перечитывает БД в течение
      периода; кэш, истёкший между `should_show` и `message`, не меняет текст запроса;
      сбой чтения скрывает баннер без исключения; подставленное время экранируется
- [ ] тесты: установка добавляет ровно один экземпляр и сохраняет существующие элементы,
      в том числе заданные кортежем; CLI-приложение FAB баннер не ставит;
      `import airflow_provider_rmq.watcher.plugin` не загружает `airflow.www.utils` в
      `sys.modules`
- [ ] run tests — must pass before task 6

### Task 6: Панель реплик и статусы подписок на странице Subscriptions

**Files:**
- Modify: `airflow_provider_rmq/watcher/views.py`
- Modify: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`
- Modify: `tests/watcher/test_views.py`

- [ ] панель «Watcher processes»: строки heartbeat с состоянием, возрастом отметки и сроком;
      пояснение для `legacy`; баннер с тем же текстом вверху страницы
- [ ] эндпоинт `forget` (POST, CSRF), `"forget": "delete"` в `method_permission_name`;
      отказ для живой и остановленной строки
- [ ] при вердикте `down` или `never` статусы подписок и `conn_id` выводятся как `unknown`
      с подсказкой о последнем записанном статусе
- [ ] тесты: панель для одной и нескольких реплик; forget мёртвой строки ролью Op с
      `can_delete`; отказ forget для живой строки и без права; подмена статусов при
      `down`/`never` и её отсутствие при `ok`/`degraded`/`legacy`
- [ ] run tests — must pass before task 7

### Task 7: Verify acceptance criteria

- [ ] каждое состояние таблицы и сценарий из Technical Details покрыт тестом Task 3–6
      (сверить по списку: смерть одной реплики, штатная остановка, рестарт в том же
      контейнере, webserver новее шедулера, недоступная БД, без включённых подписок)
- [ ] процесс шедулера и процессы, не обслуживающие UI, не импортируют `airflow.www.utils`
      из-за плагина (тест Task 5)
- [ ] run full test suite: `.venv/bin/python -m pytest tests/ -q`
- [ ] run linters: `.venv/bin/ruff check airflow_provider_rmq tests` (новых ошибок нет)

### Task 8: [Final] Update documentation

- [ ] `readme.md` и `readme_ru.md`, раздел «The watcher process»: абзац о неперезапуске
      (`readme.md:973`) описывает баннер на главной и панель на Subscriptions; новый абзац
      о heartbeat — срок из строки, состояния, кнопка Forget, суточная чистка и то, что
      жёлтый баннер гаснет с ней сам, мёртвая строка после пересоздания контейнера, краткий
      `down` после прохода, упёршегося в бюджет; абзац «Liveness metric» описывает метрику
      как дополнение для внешнего мониторинга
- [ ] новый `docs/adr/0012-watcher-liveness-heartbeat.md`: строка на запуск процесса,
      heartbeat в конце прохода, срок от watcher'а, `stopped_at` до остановки менеджера,
      `UIAlert` с ленивым импортом; отвергнутые варианты — поллинг из хуков шедулера,
      внешний мониторинг, health-check DAG, flash на каждой странице, общая строка
      `rmq_watcher_conn_status`, фоновый пинг
- [ ] `docs/adr/0008-watcher-child-process.md`: раздел о живости (:311) в настоящем времени
      — процесс не перезапускается, живость показывают heartbeat (ADR 0012) и метрика
- [ ] `CHANGELOG.md`, раздел `## v2.7.0` → Added: по одной строке на пункт (баннер на
      главной, панель реплик и Forget, таблица `rmq_watcher_heartbeat`)
- [ ] `docs/backlog/watcher-child-death-unnoticed.md`: сузить до неперезапуска и отсутствия
      push-уведомлений
- [ ] ссылки в документации — только относительные
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification (тестовый стенд, vhost `test`):**
- одна реплика: `kill -9` watcher-процесса → красный баннер на главной не позже срока из
  строки; статусы подписок — `unknown`; рестарт шедулера → баннер исчезает
- `docker stop` шедулера → строка со `stopped_at`, баннер «stopped»
- webserver с новой версией при шедулере со старой → баннера нет, на панели пояснение
  `legacy`
- вид баннера в светлой и тёмной теме Airflow UI

**Deployment:**
- выкладка на прод (2.9.1) — с согласия владельца; таблица создаётся при первом старте
  процесса Airflow с новой версией провайдера

**Следующие решения (вне плана):**
- супервизор с перезапуском умершего watcher'а и push-уведомлением о смерти
- ограничение памяти watcher'а (prefetch) как устранение вероятной причины OOM
