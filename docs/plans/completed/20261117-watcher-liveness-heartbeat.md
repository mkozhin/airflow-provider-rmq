# Живость watcher'а: heartbeat в метабазе и баннер в UI

## Overview

- Watcher-процесс, умерший сам (OOM, segfault, `kill -9`), не перезапускается до рестарта
  шедулера (ADR 0008), а подписки после этого молча не обрабатываются: строки
  `consumer_status` застывают на последнем значении, обычно `listening`. Сигналы сейчас —
  StatsD-счётчик `rmq_watcher.cycle_completed`, которому нужен внешний мониторинг, и отметка
  `last_reconcile_at` в блоке Connections страницы Subscriptions, которую надо открыть,
  чтобы увидеть.
- План делает остановку обработки подписок видимой средствами самого Airflow, без внешних
  систем, без отдельных DAG'ов и без кода в процессе шедулера:
  - каждый запуск watcher-процесса ведёт свою строку в новой таблице
    `rmq_watcher_heartbeat` и обновляет её в конце каждого прохода цикла;
  - главная страница Airflow (список DAG'ов) показывает всем пользователям красный баннер
    штатным механизмом `UIAlert` / `DASHBOARD_UIALERTS`, когда при включённых подписках ни
    один watcher не подтверждает работу;
  - страница Subscriptions показывает историю watcher-процессов, а статусы подписок, когда
    ни один watcher не подтверждает работу и свежей отметки `last_reconcile_at` нет
    (`down`), — как «неизвестно».
- Вопрос, на который отвечает баннер, — «работает ли хоть один watcher», а не «у каждого ли
  шедулера свой watcher». Связать строку watcher'а с её шедулером без кода в процессе
  шедулера нельзя: в таблице `job` нет ни PID, ни токена запуска, а `on_starting` срабатывает
  раньше, чем у `Job` появляется id. Смерть watcher'а одной реплики из нескольких сообщения не
  останавливает — их обрабатывают watcher'ы остальных реплик; она видна на панели, баннера не
  даёт.
- Вне плана, отдельными решениями: перезапуск умершего процесса (супервизор),
  push-уведомления о смерти, тревога по отдельной реплике в HA; ограничение памяти watcher'а
  (неограниченный prefetch как вероятная причина OOM).

## Context (from discovery)

- `airflow_provider_rmq/watcher/models.py`: `WatcherBase` и его таблицы, `RMQConnStatus`
  (:66, одна строка на `conn_id`, `last_reconcile_at` :81), `ensure_table_exists` (:148,
  `create_all` по метаданным `WatcherBase` + `_add_missing_columns`), `WatcherSession` (:96).
- `airflow_provider_rmq/watcher/listener.py`:
  - `_VARIABLE_TIMEOUT = 15.0` (:57), `_STEP_TIMEOUT = 60.0` (:68), `_STOP_TIMEOUT = 30.0`
    (:132), `_CHILD_STOP_TIMEOUT = 5.0` (:165, через столько шедулер шлёт SIGKILL после
    SIGTERM), `_CYCLE_POOL_WORKERS = 4` (:175);
  - `request_stop` (:682, выставляет событие остановки и будит цикл; безопасен из любого
    потока и до старта цикла); `run` (:723), `_ensure_pools` (:742), `_run_loop` (:776,
    перезапуск `_main` после краша в том же процессе с паузой 30 с), `_main` (:799:
    `_refresh_settings` внутри `while` перед каждым проходом,
    `incr("rmq_watcher.cycle_completed")` после прохода в бюджете, `finally: _stop_manager`
    :831-832), `_run_cycle` (:854, reconcile, начатый до остановки, доигрывает до конца;
    проглатывает `_StopRequested` :916-917), `_cycle_step` (:923, первой строкой
    `_raise_if_stopping` :956; уже поданную попытку не отзывает), `_refresh_settings` (:1038).
- `airflow_provider_rmq/watcher/runner.py`: `_run_watcher` (:445) — основной поток ждёт
  сигнал остановки и вызывает `listener.request_stop()` (:474-477), а при смене `getppid()`
  — тоже `listener.request_stop()` (:482-486); смерть шедулера приходит и как SIGTERM через
  `PR_SET_PDEATHSIG` (:258-277).
- `airflow_provider_rmq/utils/executor.py`: `BoundedExecutor`, `PoolSaturated` (:16).
- `airflow_provider_rmq/watcher/tunables.py`: `DEFAULT_RECONCILE_INTERVAL = 60`,
  `cycle_timeout`, `MIN_CYCLE_TIMEOUT = 300`, `stale_after(interval, budget)` (:68).
- `airflow_provider_rmq/watcher/views.py`: `_stale_after` (:299, читает Variables),
  `_build_conn_status_rows` (:322, naive UTC и отрицательный возраст как ноль :327-343),
  страница `/subscriptions` (:375).
- `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`: блок Connections
  с пометкой устаревшего `last_reconcile_at` (:86).
- `airflow_provider_rmq/watcher/plugin.py`: верхнеуровневый импорт `views` (:11-18) — модуль
  загружается во всех процессах Airflow; `_bp` (:37); `_serves_the_web_ui(app)` (:106,
  отличает webserver от CLI FAB, который тоже регистрирует блюпринт); образец колбэка
  `_grant_op_access(state)` (:122) и его регистрация `_bp.record_once(_grant_op_access)`
  (:216); `on_load` (:232, `ensure_table_exists` в каждом процессе).
- Airflow (сверено по 2.9.1 и 2.11.2, расхождений в используемом API нет):
  - `airflow.www.utils.UIAlert`: `__init__` присваивает `category`, `roles`, `html`,
    `message` (`Markup(message) if html else message`); `should_show(self, appbuilder)` —
    один позиционный аргумент; `html` после `__init__` нигде не читается;
  - `airflow.settings.DASHBOARD_UIALERTS` — модульный список; `import_local_settings`
    копирует имена из `airflow_local_settings` в глобалы `airflow.settings`, так что там
    может оказаться и кортеж;
  - `views.Airflow.index` (`@expose("/home")`) на каждый запрос строит список
    `[fm for fm in settings.DASHBOARD_UIALERTS if fm.should_show(get_airflow_app().appbuilder)]`
    (2.9.1 `www/views.py:1001-1003`, 2.11.2 :1070-1072), читая `settings.DASHBOARD_UIALERTS`
    через модуль (`from airflow import settings`), и передаёт его в `render_template` того же
    вызова — отбор и рендер идут в одном контексте запроса, `flask.g` у них общий;
  - `www/templates/airflow/dags.html` (:84-85, одинаково) выводит
    `show_message(m.message, m.category)`; макрос `_messages.html` подставляет
    `{{ category }}` в класс и `{{ content }}` в тело с автоэкранированием Flask для `.html`;
  - `airflow.utils.net.get_hostname()` в обеих версиях одинаков:
    `conf.getimport("core", "hostname_callable", fallback="airflow.utils.net.getfqdn")()`;
  - `create_app` вызывает `init_appbuilder_views` (регистрирует `views.Airflow`, эндпоинт
    `Airflow.index`) раньше `init_plugins` (регистрирует блюпринты плагинов) — в 2.9.1 и
    2.11.2. FAB CLI (`airflow users` / `roles` / `sync-perm`) в 2.9.1 приходит из
    `apache-airflow-providers-fab>=1.0.2`: `get_application_builder` вызывает
    `init_appbuilder` и `init_plugins`, но не `init_appbuilder_views`, а индекс-вид FAB по
    умолчанию — `IndexView.index`. Поэтому `_serves_the_web_ui(state.app)` в колбэке
    `record_once` отличает webserver от CLI FAB одинаково для `_grant_op_access` и для
    колбэка баннера.
- Тесты: `tests/watcher/test_models.py`, `test_listener.py`, `test_runner.py`,
  `test_views.py`, `test_plugin.py`; образец проверки импортов отдельным интерпретатором —
  `TestDagFileScanImportFootprint` в `tests/watcher/test_dag_file_scan.py`.
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
- maintain backward compatibility: отсутствие новой таблицы или недоступность БД не роняет
  ни watcher, ни страницы

## Testing Strategy

- **unit tests**: required for every task
- оценка живости — чистая функция, тестируется таблицей сценариев без БД, плюс один тест
  на строках, прочитанных из БД
- модель и запись heartbeat — на SQLite-сессии, как существующие тесты `test_models.py`
- цикл и остановка — через фикстуры `test_listener.py` и `test_runner.py`
- `UIAlert` и страница — через фикстуры `test_views.py` / `test_plugin.py`
- след импорта плагина — отдельным интерпретатором (`subprocess.run([sys.executable, "-c",
  probe])`), как `TestDagFileScanImportFootprint`
- e2e UI-тестов в проекте нет; проверки на живом Airflow — в Post-Completion

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview

- **Строка на запуск процесса.** Watcher-процесс в `run` создаёт `WatcherIdentity` (UUID,
  hostname, PID, `started_at`) и ведёт строку `rmq_watcher_heartbeat` с этим `id`.
  Перезапуск `_main` внутри процесса (`_run_loop`) строку не меняет.
- **Heartbeat — конец прохода в бюджете, срок — абсолютное время.** Строка обновляется там
  же, где растёт `cycle_completed`, и получает `due_at` — момент, к которому этот процесс с
  его действующими настройками обязан оставить следующую отметку. Фонового пинга нет — он
  показывал бы живым зависший цикл.
- **Вердикт — есть ли подтверждённая работа.** Строка жива, пока не остановлена и не прошёл
  её `due_at`. Есть живая строка с завершённым проходом — watcher работает. Живых строк нет,
  но `last_reconcile_at` свежий — работа идёт без heartbeat (watcher версии провайдера без
  него, webserver обновлён раньше шедулера) или только что прекратилась; баннера нет, пока
  отметка не устарела. Ничего свежего нет — красный баннер. Таблица `job` для вердикта не
  читается, строки между собой не сопоставляются.
- **Штатная остановка помечается по сигналу.** Runner после штатного сигнала сначала
  вызывает `request_stop` (событие и пробуждение цикла), затем `mark_stopped`, который
  отдельным коротким потоком записывает `stopped_at` с ожиданием не дольше 2 с. Смерть
  шедулера (смена `getppid()`, SIGTERM от `PR_SET_PDEATHSIG`) отметку не ставит. Отметка
  нужна панели и тексту баннера, чтобы отличать «остановлен» от «умер».
- **Таблица вне `WatcherBase`.** `RMQWatcherHeartbeat` объявлена на своей декларативной базе,
  и её создаёт `ensure_heartbeat_table()`, который вызывают только watcher-процесс и
  webserver. `on_load` по-прежнему создаёт таблицы `WatcherBase`, и DDL новой таблицы в
  процессе шедулера не выполняется.
- **Баннер — штатный `UIAlert`, только в webserver'е.** Колбэк `record_once` в `plugin.py`
  по образцу `_grant_op_access` проверяет `_serves_the_web_ui(state.app)` и вызывает
  `dashboard_alert.install_dashboard_alert()`, который лениво импортирует `UIAlert`,
  определяет подкласс и добавляет экземпляр в `airflow.settings.DASHBOARD_UIALERTS`. Модули
  плагина `airflow.www.utils` на уровне модуля не импортируют.
- **Чистка — отдельно и без гарантий.** После коммита своей отметки живой watcher отдельной
  транзакцией удаляет чужие строки, чья последняя отметка старше суток.

## Technical Details

### Таблица `rmq_watcher_heartbeat`

| колонка | тип | смысл |
|---|---|---|
| `id` | `String(36)`, PK | UUID запуска процесса |
| `hostname` | `String(500)` | `airflow.utils.net.get_hostname()` |
| `pid` | `Integer` | PID watcher-процесса |
| `started_at` | `DateTime` | момент создания идентичности в `run` |
| `last_cycle_at` | `DateTime`, nullable | момент записи отметки последнего прохода в бюджете |
| `due_at` | `DateTime` | крайний срок следующей отметки при действующих настройках |
| `stopped_at` | `DateTime`, nullable | штатная остановка |

- Время — naive UTC из процесса, как у `last_reconcile_at`.
- Своя декларативная база (`HeartbeatBase`), не `WatcherBase`; `ensure_heartbeat_table()` —
  `create_all` её метаданных, идемпотентный, с флагом готовности как у
  `ensure_table_exists`.
- Upsert'ы старта, срока и прохода `stopped_at` не пишут и не стирают; его пишет только
  `record_heartbeat_stop` — upsert с полной идентичностью (`id`, `hostname`, `pid`,
  `started_at`, `due_at`, `stopped_at`), поэтому отметка остановки ложится и на строку,
  которую запись старта ещё не успела закоммитить.
- Все upsert'ы heartbeat переживают параллельную вставку того же `id`: вставка, упавшая на
  `IntegrityError` (строку только что закоммитил другой писатель), откатывается и
  повторяется как UPDATE тех же полей. Так отметка остановки, гонящаяся со стартовой
  записью, не теряется ни на одном диалекте метабазы.
- **Последняя отметка** строки — `stopped_at`, иначе `last_cycle_at`, иначе `started_at`.

### Срок `due_at`

Обозначения: `I` — действующий интервал, `B` — действующий бюджет прохода, `V` —
`_VARIABLE_TIMEOUT` (чтение настроек вне бюджета), `H` — граница одной записи heartbeat
(`_HEARTBEAT_TIMEOUT = 10` с), `P` — граница чистки (`_PRUNE_TIMEOUT = 10` с). `now`
берётся перед записью, поэтому в каждую формулу входят остаток текущей записи (`H`) и
следующая запись (`H`).

- **Строка старта** пишется после первого `_refresh_settings` процесса, перед первым
  проходом: `due_at = now + H + B + H`.
- **Перед проходом**, если после `_refresh_settings` бюджет вырос против того, с которым
  считан сохранённый срок: `due_at = now + H + B + H`, `last_cycle_at` не трогается.
- **Отметка прохода**: `last_cycle_at = now`, `due_at = now + H + P + I + V + B + H`.

### Вердикт живости

- Строка **жива**, если `stopped_at` пуст и `now ≤ due_at`.
- **Свежий `last_reconcile_at`** — самая свежая отметка в `rmq_watcher_conn_status` моложе
  `_stale_after() + V + _RECONCILE_TAIL` (`_RECONCILE_TAIL = 60` с — работа прохода после
  отметки: запись статусов и запись heartbeat); читается только когда живых строк нет или
  таблицы heartbeat ещё нет.

Состояния проверяются сверху вниз, первое совпавшее — вердикт:

| состояние | условие | баннер | статусы подписок |
|---|---|---|---|
| `unused` | включённых подписок нет | нет | как есть |
| `ok` | есть живая строка с `last_cycle_at` | нет | как есть |
| `starting` | есть живые строки, ни у одной нет `last_cycle_at` | нет | как есть |
| `unconfirmed` | живых строк нет, свежий `last_reconcile_at` есть | нет (на панели пояснение) | как есть |
| `down` | живых строк нет, свежего `last_reconcile_at` нет | красный | `unknown` |

`unconfirmed` покрывает watcher версии провайдера без heartbeat, webserver, обновлённый
раньше шедулера, и первые минуты после остановки последнего watcher'а: свежая отметка
`last_reconcile_at` переходит в `down`, как только устаревает.

Тексты баннера `down` — без hostname и PID (их видят все пользователи, включая
`AUTH_ROLE_PUBLIC`); подробности — на панели Subscriptions. Время — самая свежая последняя
отметка среди строк:

- строк heartbeat нет: «RMQ watcher is not running — subscriptions are not consumed and
  event-triggered DAGs do not run.»
- самая свежая строка остановлена штатно: «RMQ watcher stopped at `<время>` and no
  Scheduler runs it — subscriptions are not consumed.»
- иначе: «RMQ watcher has not completed a cycle since `<время>` — subscriptions are not
  consumed and event-triggered DAGs do not run.»

Каждый текст ссылается на `/rmq-watcher/subscriptions`. Баннер видят все пользователи
(`roles=None`).

### Запись из watcher-процесса

Записи старта, срока и прохода — шаг `_cycle_step` с одним именем `"heartbeat"` и границей
`H`, чтобы зависшая запись занимала не больше одного воркера cycle-пула из четырёх; чистка —
шаг `"heartbeat_prune"` с границей `P`.

- **Подготовка.** В `run` — только `WatcherIdentity` (`started_at` — момент создания).
  Таблицу создаёт сама запись heartbeat: каждый шаг `"heartbeat"`, пока флаг готовности не
  стоит, сначала вызывает `ensure_heartbeat_table()` — в cycle-пуле, под границей `H`, а не
  в потоке цикла. Сбой — WARNING, повтор на следующей записи.
- **Старт процесса.** После первого `_refresh_settings` процесса, перед первым проходом —
  upsert строки старта. Перезапуск `_main` строку старта не пишет заново.
- **Перед проходом** — запись нового `due_at`, если бюджет вырос.
- **Конец прохода в бюджете** (рядом с `incr("rmq_watcher.cycle_completed")`), если
  остановка не запрошена: upsert `last_cycle_at` и `due_at`, коммит; затем отдельной
  транзакцией best-effort чистка чужих строк, чья последняя отметка старше суток. Upsert, а
  не UPDATE: строку, не вставленную при старте, следующий проход восстанавливает.
- Сбой записи, `_StepInFlight`, `PoolSaturated` — WARNING, цикл продолжается;
  `_StopRequested` — тихо.
- **Штатная остановка.** `request_stop` не меняется. Новый `mark_stopped()` при первом
  вызове, если идентичность процесса уже создана, запускает поток, который своей сессией
  пишет `record_heartbeat_stop`, и ждёт его не дольше 2 с; повторные вызовы отметку не
  пишут. Runner в ветке полученного сигнала вызывает сначала `request_stop()`, затем
  `mark_stopped()` — только если `getppid()` всё ещё равен PID шедулера: SIGTERM от
  `PR_SET_PDEATHSIG` приходит уже после смены родителя. Ветка смены `getppid()`,
  `_parent_is_gone` и краш `_main` отметку не ставят. Сбой и таймаут отметки — WARNING.

### Баннер в webserver'е

- `plugin.py`: колбэк `_install_liveness_alert(state)` по образцу `_grant_op_access` —
  выходит, если `_serves_the_web_ui(state.app)` ложно, иначе вызывает
  `dashboard_alert.install_dashboard_alert()`; регистрация `_bp.record_once(...)`.
- `dashboard_alert.install_dashboard_alert()`: вызывает `ensure_heartbeat_table()` (сбой —
  WARNING), лениво импортирует `UIAlert`, определяет `_WatcherLivenessAlert` и
  перепривязывает список: `settings.DASHBOARD_UIALERTS = [*settings.DASHBOARD_UIALERTS,
  alert]` (список мог быть задан кортежем в `airflow_local_settings`); повторный вызов
  второй экземпляр не добавляет.
- `_WatcherLivenessAlert` не вызывает `UIAlert.__init__`: задаёт `roles = None`, а `message`
  и `category` — свойства без записи, читающие снимок вердикта из `flask.g`. `message`
  возвращает `markupsafe.Markup`, собранный `Markup(template).format(...)`: ссылка остаётся
  разметкой, подставленное время экранируется.
- `should_show(self, *args, **kwargs)` (Airflow 2.9.1–2.11.2 передаёт один позиционный
  `appbuilder`) берёт вердикт из кэша, кладёт его в `flask.g` и возвращает `True` только для
  `down`; `message` и `category` при рендере читают только `flask.g` и в БД не ходят, так что
  отбор и текст одного запроса не расходятся. Атрибут `html` не задаётся: Airflow читает его
  только в `UIAlert.__init__`.
- Кэш — неизменяемая пара `(expires_at, verdict)`, заменяемая целиком, 30 с на процесс
  воркера webserver'а; перечтение под `threading.Lock`, чтобы промах кэша не дал залп
  запросов. Промах — пока флаг готовности не стоит, `ensure_heartbeat_table()`; затем
  SELECT по `rmq_watcher_heartbeat`, `EXISTS` по включённым подпискам и, только когда живых
  строк нет, самая свежая `last_reconcile_at` и `_stale_after()`.
- Таблицы heartbeat нет (создание не удалось) — вердикт считается как при пустой таблице,
  по одной `last_reconcile_at`: баннер не теряется из-за неудачного DDL.
- Сбой чтения (БД недоступна) — баннер не показывается, WARNING раз на период кэша: главная
  страница Airflow не падает из-за плагина.

### Страница Subscriptions

- Панель «Watcher processes» над блоком Connections: строки heartbeat за сутки (hostname,
  PID, started, последняя отметка и её возраст, срок, состояние alive / starting / dead /
  stopped); пояснение для `unconfirmed`; баннер с тем же текстом вверху страницы.
- Мёртвая строка (не остановлена, срок прошёл) выделена на панели: в HA это watcher одной
  реплики, который не работает, пока другие обрабатывают подписки, или след шедулера,
  ушедшего без штатной остановки.
- При вердикте `down` статусы подписок и `conn_id` выводятся как `unknown` с подсказкой
  «watcher is not running — last recorded: `<status>`».

### Что делает документация явным

- Баннер говорит «ни один watcher не подтверждает работу»; смерть watcher'а одной реплики при
  работающих других баннера не даёт и видна на панели Subscriptions как мёртвая строка.
- После остановки последнего watcher'а без отметки `stopped_at` баннер появляется, когда
  проходит его `due_at`; при свежем `last_reconcile_at` — когда устаревает и она.
- Один проход, упёршийся в бюджет, может на короткое время дать `down` у живого процесса:
  перезапуск `_main` ждёт 30 с и добавляет время остановки менеджера.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): код, тесты, документация в этом репозитории.
- **Post-Completion** (no checkboxes): проверка на живом Airflow, выкладка на прод.

## Implementation Steps

### Task 1: Сверить используемые API с Airflow 2.9.1

**Files:**
- Modify: `docs/plans/20261117-watcher-liveness-heartbeat.md`

- [x] по исходникам Airflow 2.9.1 (тег `2.9.1` в apache/airflow) подтвердить: атрибуты,
      которые `UIAlert.__init__` присваивает, сигнатуру `should_show`, что
      `views.Airflow.index` фильтрует `settings.DASHBOARD_UIALERTS` вызовом `should_show`
      на каждый запрос, что `dags.html` читает `m.message` и `m.category` при рендере с
      автоэкранированием, что `flask.g` доступен в обоих местах одного запроса
- [x] подтвердить для 2.9.1 `airflow.utils.net.get_hostname()` и то, что
      `_serves_the_web_ui(state.app)` отличает webserver от CLI FAB так же, как для
      `_grant_op_access`
- [x] записать расхождения с 2.11.2 в этот план (⚠️) и поправить Technical Details
      (расхождений в используемом API нет — различаются только номера строк `views.py`;
      сверка записана в Context, уточнение `should_show` / `html` — в «Баннер в webserver'е»)
- [x] тестов задача не добавляет: она меняет только план

### Task 2: Модель `RMQWatcherHeartbeat` и функции записи

**Files:**
- Modify: `airflow_provider_rmq/watcher/models.py`
- Modify: `tests/watcher/test_models.py`

- [x] frozen-dataclass `WatcherIdentity(id, hostname, pid, started_at)`
- [x] `HeartbeatBase`, модель `RMQWatcherHeartbeat` (колонки из Technical Details),
      `ensure_heartbeat_table()`
- [x] `record_heartbeat_start(session, identity, due_at)`,
      `record_heartbeat_due(session, identity, due_at)`,
      `record_heartbeat_cycle(session, identity, last_cycle_at, due_at)` — upsert'ы, не
      трогающие `stopped_at`; старт не сбрасывает `last_cycle_at`
- [x] `record_heartbeat_stop(session, identity, due_at, stopped_at)` — upsert с полной
      идентичностью; `prune_heartbeats(session, keep_id, older_than)` — по последней
      отметке; `get_heartbeats(session, since)`
- [x] тесты: `ensure_heartbeat_table` создаёт таблицу и идемпотентен; `ensure_table_exists`
      её не создаёт; upsert старта поверх строки с `last_cycle_at` и со `stopped_at` их
      сохраняет; upsert прохода поверх существующей и отсутствующей строки; запись срока не
      трогает `last_cycle_at`; stop на существующей и на отсутствующей строке
- [x] тесты: вставка, упавшая на `IntegrityError` (строка с тем же `id` появилась между
      SELECT и INSERT), повторяется как UPDATE — для stop `stopped_at` ложится, для старта
      `stopped_at` не стирается
- [x] тесты: чистка не трогает свою строку и свежие чужие и удаляет старые по каждой из
      трёх отметок
- [x] run tests — must pass before task 3

### Task 3: Оценка живости `assess_liveness`

**Files:**
- Create: `airflow_provider_rmq/watcher/liveness.py`
- Create: `tests/watcher/test_liveness.py`

- [x] `LivenessVerdict` (состояние `unused | ok | starting | unconfirmed | down`; строки с
      состоянием alive / starting / dead / stopped; самая свежая последняя отметка и
      признак «она — штатная остановка»)
- [x] `assess_liveness(heartbeats, now, has_enabled_subscriptions, has_fresh_reconcile)` —
      правила из Technical Details
- [x] `banner_text(verdict)`, `masks_statuses(verdict)` — три текста `down` без hostname и
      PID; подмена статусов только для `down`
- [x] тесты: каждое состояние таблицы; граница `due_at` (ровно `due_at` — жива); строка со
      `stopped_at` не жива при любом `due_at`; несколько строк — одна живая с проходом даёт
      `ok`, даже если другие мертвы; каждый из трёх текстов `down`
- [x] тесты сценариев: единственный watcher убит (`kill -9`) — `down` после `due_at`;
      `docker stop` шедулера — `down` с текстом про остановку после устаревания
      `last_reconcile_at`; рестарт шедулера — `starting`, затем `ok`, мёртвая строка
      прежнего запуска не мешает; webserver обновлён раньше шедулера (строк нет, свежий
      `last_reconcile_at`) — `unconfirmed`; строк нет, отметка устарела — `down`
- [x] тест на строках, прочитанных из БД (`get_heartbeats` с настоящей строкой): без
      `TypeError`, `ok`
- [x] run tests — must pass before task 4

### Task 4: Запись heartbeat из watcher-процесса

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `airflow_provider_rmq/watcher/runner.py`
- Modify: `tests/watcher/test_listener.py`
- Modify: `tests/watcher/test_runner.py`

- [x] `run`: `WatcherIdentity` (hostname — `get_hostname()`); константы
      `_HEARTBEAT_TIMEOUT`, `_PRUNE_TIMEOUT`; шаг `"heartbeat"` вызывает
      `ensure_heartbeat_table()`, пока флаг готовности не стоит
- [x] старт: после первого `_refresh_settings` процесса, перед первым проходом — шаг
      `"heartbeat"` с `record_heartbeat_start` и `due_at = now + H + B + H`; перезапуск
      `_main` старт не пишет
- [x] перед проходом: если бюджет вырос против сохранённого срока — шаг `"heartbeat"` с
      `record_heartbeat_due`
- [x] конец прохода в бюджете без запрошенной остановки: шаг `"heartbeat"` с
      `record_heartbeat_cycle`, затем шаг `"heartbeat_prune"`; `_StopRequested` — тихо,
      сбой, `_StepInFlight`, `PoolSaturated` — WARNING
- [x] `mark_stopped()`: при первом вызове и созданной идентичности — поток с
      `record_heartbeat_stop` своей сессией, ожидание не дольше 2 с, сбой и таймаут —
      WARNING; `request_stop` не меняется
- [x] runner, ветка полученного сигнала: `request_stop()`, затем `mark_stopped()` только
      если `getppid()` равен PID шедулера; ветка смены `getppid()` — только `request_stop()`
- [x] тесты (`test_listener.py`): строка старта несёт срок из переопределённого бюджета
      (Variable `rmq_watcher_cycle_timeout`); рост бюджета публикует новый срок до прохода и
      не двигает `last_cycle_at`; проход в бюджете двигает `last_cycle_at` и `due_at`;
      проход, упёршийся в бюджет, — нет; перезапуск `_main` после краша сохраняет `id` и
      `started_at`
- [x] тесты (`test_listener.py`): `mark_stopped` пишет `stopped_at`, пока reconcile ещё
      идёт, и не ждёт дольше 2 с при зависшей записи; повторный вызов второй записи не
      делает; вызов до `run` ничего не пишет; stop, пришедший до подачи записи старта, —
      запись старта не подаётся; stop, пришедший, пока запись старта идёт в воркере, —
      итоговая строка со `stopped_at`; проход, записанный после отметки, `stopped_at` не
      стирает; краш `_main` `stopped_at` не ставит; остановка не даёт WARNING о heartbeat
      прохода
- [x] тесты (`test_listener.py`): сбой записи и сбой чистки не прерывают цикл и не мешают
      `cycle_completed`; сбой чистки не откатывает отметку; зависшая запись не подаётся
      повторно; сбой и зависание `ensure_heartbeat_table` внутри шага старта — WARNING по
      границе `H`, цикл стартует, следующая запись создаёт таблицу
- [x] тесты (`test_runner.py`): штатный SIGTERM при живом родителе — `request_stop`, затем
      `mark_stopped`; смена `getppid()` и SIGTERM после смены родителя — только
      `request_stop`
- [x] run tests — must pass before task 5

### Task 5: Баннер на главной странице Airflow

**Files:**
- Create: `airflow_provider_rmq/watcher/dashboard_alert.py`
- Modify: `airflow_provider_rmq/watcher/plugin.py`
- Create: `tests/watcher/test_dashboard_alert.py`
- Modify: `tests/watcher/test_plugin.py`

- [x] `dashboard_alert.py`: чтение вердикта с кэшем (неизменяемая пара, 30 с, `Lock` на
      перечтение; `ensure_heartbeat_table()` на промахе, пока флаг не стоит; без таблицы —
      вердикт по одной `last_reconcile_at`; `last_reconcile_at` и порог — только без живых
      строк); сбой чтения — вердикт «не показывать» и WARNING раз на период
- [x] тесты: создание таблицы, не удавшееся при установке, повторяется на следующем промахе
      кэша; без таблицы и с устаревшей `last_reconcile_at` — `down`; отметка
      `last_reconcile_at` возрастом `I + B + 10` с ещё свежая
- [x] `install_dashboard_alert()`: `ensure_heartbeat_table()`, ленивый импорт `UIAlert`,
      подкласс без вызова `UIAlert.__init__`, `should_show(*args, **kwargs)` со снимком в
      `flask.g`, `message` — `Markup` из `flask.g`, `category` из `flask.g`, перепривязка
      `DASHBOARD_UIALERTS`, без повторного экземпляра
- [x] `plugin.py`: колбэк `_install_liveness_alert(state)` с проверкой
      `_serves_the_web_ui(state.app)` и `_bp.record_once(_install_liveness_alert)`
- [x] тесты: `should_show` истинен только для `down`; `message` — экземпляр `Markup` с
      настоящей ссылкой, а подставленное время экранировано; кэш не перечитывает БД в течение
      периода; кэш, истёкший между `should_show` и `message`, не меняет текст запроса; сбой
      чтения скрывает баннер без исключения
- [x] тесты: установка добавляет ровно один экземпляр и сохраняет существующие элементы,
      в том числе заданные кортежем; CLI-приложение FAB баннер не ставит
- [x] тест отдельным интерпретатором (как `TestDagFileScanImportFootprint`):
      `import airflow_provider_rmq.watcher.plugin` не загружает `airflow.www.utils`
- [x] run tests — must pass before task 6

### Task 6: Панель процессов и статусы подписок на странице Subscriptions

**Files:**
- Modify: `airflow_provider_rmq/watcher/views.py`
- Modify: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`
- Modify: `tests/watcher/test_views.py`

- [x] панель «Watcher processes»: строки heartbeat за сутки с состоянием, возрастом
      последней отметки и сроком, мёртвые строки выделены; пояснение для `unconfirmed`;
      баннер с тем же текстом вверху страницы
- [x] при вердикте `down` статусы подписок и `conn_id` выводятся как `unknown` с подсказкой
      о последнем записанном статусе
- [x] тесты: панель для одного и нескольких процессов, живой и мёртвой строки; подмена
      статусов при `down` и её отсутствие при `ok`/`starting`/`unconfirmed`; сбой чтения
      heartbeat не роняет страницу
- [x] run tests — must pass before task 7

### Task 7: Verify acceptance criteria

- [x] каждое состояние таблицы и каждый сценарий из Task 3 покрыт тестом
      (`tests/watcher/test_liveness.py`: `TestStates`, `TestScenarios`, `TestRowsReadFromDatabase`)
- [x] тест отдельным интерпретатором из Task 5 проходит: плагин не загружает
      `airflow.www.utils`; `ensure_table_exists` (вызываемый `on_load` в шедулере) таблицу
      heartbeat не создаёт
- [x] run full test suite: `.venv/bin/python -m pytest tests/ -q`
- [x] run linters: `.venv/bin/ruff check airflow_provider_rmq tests` (новых ошибок нет)

### Task 8: [Final] Update documentation

- [x] `readme.md` и `readme_ru.md`, раздел «The watcher process»: абзац о неперезапуске
      (`readme.md:973`) описывает баннер на главной и панель на Subscriptions; новый абзац о
      heartbeat — вопрос баннера («ни один watcher не подтверждает работу»), состояния,
      задержка до `due_at` и устаревания `last_reconcile_at`, мёртвая строка реплики в HA на
      панели, суточная чистка, краткий `down` после прохода, упёршегося в бюджет; абзац
      «Liveness metric» описывает метрику как дополнение для внешнего мониторинга
- [x] новый `docs/adr/0012-watcher-liveness-heartbeat.md`: строка на запуск процесса, срок
      `due_at` от watcher'а, вердикт «есть ли подтверждённая работа», `stopped_at` по
      штатному сигналу, отдельная декларативная база таблицы, `UIAlert` с ленивым импортом;
      отвергнутые варианты — тревога по каждому шедулеру через сопоставление строк с
      таблицей `job` (связи нет без кода в шедулере, и любое правило по времени ошибается при
      репликах на одном hostname), поллинг из хуков шедулера, внешний мониторинг,
      health-check DAG, flash на каждой странице, общая строка `rmq_watcher_conn_status`,
      фоновый пинг
- [x] `docs/adr/0008-watcher-child-process.md`: раздел о живости (:311) в настоящем времени
      — процесс не перезапускается, работу показывают heartbeat (ADR 0012) и метрика
- [x] `CHANGELOG.md`, раздел `## v2.7.0` → Added: по одной строке на пункт (баннер на
      главной, панель процессов на Subscriptions, таблица `rmq_watcher_heartbeat`)
- [x] `docs/backlog/watcher-child-death-unnoticed.md`: сузить до неперезапуска, отсутствия
      push-уведомлений и тревоги по отдельной реплике в HA
- [x] ссылки в документации — только относительные
- [x] move this plan to `docs/plans/completed/` (left for the harness, which moves the plan after all phases)

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification (тестовый стенд, vhost `test`):**
- одна реплика: `kill -9` watcher-процесса → красный баннер на главной после `due_at`;
  статусы подписок — `unknown`; рестарт шедулера → баннер исчезает
- `docker stop` шедулера → строка со `stopped_at` на панели, баннер «stopped» после
  устаревания `last_reconcile_at`
- webserver с новой версией при шедулере со старой → баннера нет, на панели пояснение
  `unconfirmed`
- вид баннера в светлой и тёмной теме Airflow UI

**Deployment:**
- выкладка на прод (2.9.1) — с согласия владельца; таблица создаётся при первом старте
  watcher-процесса или webserver'а с новой версией провайдера

**Следующие решения (вне плана):**
- супервизор с перезапуском умершего watcher'а и push-уведомлением о смерти
- тревога по отдельной реплике в HA — требует явной связи watcher'а со своим `SchedulerJob`
- ограничение памяти watcher'а (prefetch) как устранение вероятной причины OOM
