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
  - webserver сверяет живые watcher'ы с живыми шедулерами из таблицы `job` Airflow: у
    каждого живого шедулера должен быть живой watcher;
  - главная страница Airflow (список DAG'ов) показывает всем пользователям баннер штатным
    механизмом `UIAlert` / `DASHBOARD_UIALERTS`, когда при включённых подписках живого
    watcher'а нет или у живого шедулера его нет;
  - страница Subscriptions показывает панель по хостам и процессам, а статусы подписок при
    отсутствии живого watcher'а — как «неизвестно».
- Вне плана, отдельными решениями: перезапуск умершего процесса (супервизор) и
  push-уведомления о смерти; ограничение памяти watcher'а (неограниченный prefetch как
  вероятная причина OOM).

## Context (from discovery)

- `airflow_provider_rmq/watcher/models.py`: `WatcherBase`, `RMQSubscription`,
  `RMQConnStatus` (:66, одна строка на `conn_id`, `last_reconcile_at` :81),
  `ensure_table_exists` (:148, `create_all` + `_add_missing_columns`), `WatcherSession` (:96).
- `airflow_provider_rmq/watcher/listener.py`:
  - `_VARIABLE_TIMEOUT = 15.0` (:57), `_STEP_TIMEOUT = 60.0` (:68), `_STOP_TIMEOUT = 30.0`
    (:132), `_CHILD_STOP_TIMEOUT = 5.0` (:165, через столько шедулер шлёт SIGKILL после
    SIGTERM), `_CYCLE_POOL_WORKERS = 4` (:175);
  - `request_stop` (:682, выставляет событие остановки и будит цикл; безопасен из любого
    потока и до старта цикла); `run` (:723), `_ensure_pools` (:742),
    `_run_loop` (:776, перезапуск `_main` после краша в том же процессе с паузой 30 с),
    `_main` (:799: `_refresh_settings` внутри `while` перед каждым проходом,
    `incr("rmq_watcher.cycle_completed")` после прохода в бюджете, `finally: _stop_manager`
    :831-832), `_run_cycle` (:854, reconcile, начатый до остановки, доигрывает до конца;
    проглатывает `_StopRequested` :916-917), `_cycle_step` (:923, первой строкой
    `_raise_if_stopping` :956), `_refresh_settings` (:1038).
- `airflow_provider_rmq/watcher/runner.py`: `_run_watcher` (:445) — основной поток ждёт
  сигнал остановки и вызывает `listener.request_stop()` (:474-477), а при смене `getppid()`
  — тоже `listener.request_stop()` (:482-486); смерть шедулера приходит и как SIGTERM через
  `PR_SET_PDEATHSIG` (:258-277).
- `airflow_provider_rmq/utils/executor.py`: `BoundedExecutor`, `PoolSaturated` (:16).
- `airflow_provider_rmq/watcher/tunables.py`: `DEFAULT_RECONCILE_INTERVAL = 60`,
  `cycle_timeout`, `MIN_CYCLE_TIMEOUT = 300`, `stale_after(interval, budget)` (:68).
- `airflow_provider_rmq/watcher/views.py`: `_stale_after` (:299, читает Variables),
  `_build_conn_status_rows` (:322, отрицательный возраст ограничен нулём :343),
  `method_permission_name` (:364), страница `/subscriptions` (:375).
- `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`: блок Connections
  с пометкой устаревшего `last_reconcile_at` (:86).
- `airflow_provider_rmq/watcher/plugin.py`: верхнеуровневый импорт `views` (:11-18) — модуль
  загружается во всех процессах Airflow; `_bp` (:37); `_serves_the_web_ui(app)` (:106,
  отличает webserver от CLI FAB, который тоже регистрирует блюпринт); образец колбэка
  `_grant_op_access(state)` (:122) и его регистрация `_bp.record_once(_grant_op_access)`
  (:216); `on_load` (:232, DDL в каждом процессе, см.
  `docs/backlog/plugin-on-load-runs-ddl-in-every-process.md`).
- Airflow (сверено по 2.11.2):
  - `airflow.jobs.job.Job`: `job_type`, `state`, `hostname` (пишется
    `airflow.utils.net.get_hostname()`), `latest_heartbeat`, `start_date`, `is_alive()`
    (`state == running` и heartbeat моложе `[scheduler] scheduler_health_check_threshold`
    для `SchedulerJob`);
  - `airflow.www.utils.UIAlert` (`__init__` присваивает `message`, `category`, `roles`,
    `html`; `message = Markup(message) if html else message`; `should_show(appbuilder)`),
    `airflow.settings.DASHBOARD_UIALERTS`, отбор `should_show` в `www/views.py`
    (:1070-1071), вывод `m.message` / `m.category` в `www/templates/airflow/dags.html`
    (:84-85) с автоэкранированием — при рендере, позже отбора.
  Для 2.9.1 сверяется в Task 1.
- Тесты: `tests/watcher/test_models.py`, `test_listener.py`, `test_views.py`,
  `test_plugin.py`; образец проверки импортов отдельным интерпретатором —
  `TestDagFileScanImportFootprint` в `tests/watcher/test_dag_file_scan.py` и
  `tests/watcher/test_runner.py:213`.
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

- **Строка на запуск процесса.** Watcher-процесс при старте создаёт `WatcherIdentity`
  (UUID, hostname, PID, `started_at`) и ведёт строку `rmq_watcher_heartbeat` с этим `id`.
  Hostname — `airflow.utils.net.get_hostname()`, то же значение, что шедулер пишет в свою
  строку `job`. Перезапуск `_main` внутри процесса (`_run_loop`) строку не меняет.
- **Heartbeat — конец прохода в бюджете, срок — абсолютное время.** Строка обновляется там
  же, где растёт `cycle_completed`, и получает `due_at` — момент, к которому этот процесс
  с его действующими настройками обязан оставить следующую отметку. Фонового пинга нет —
  он показывал бы живым зависший цикл. Живость строки webserver судит по её `due_at`;
  Variables он читает только когда живых строк нет — чтобы понять, свежа ли отметка
  `last_reconcile_at`.
- **Тревога — шедулер без живого watcher'а, а не мёртвая строка.** Webserver берёт строки
  `SchedulerJob` из таблицы `job` за окно чистки — живые, стартующие и завершённые — и
  строки heartbeat и на каждом hostname закрепляет за каждым шедулером, от старого к
  новому, самую старую свободную строку, начатую, пока этот шедулер жил. Шедулер запускает
  watcher один раз, поэтому строка достаётся своему шедулеру; строки остановленных и
  стартующих шедулеров заняты ими, а шедулер без своей строки (старая версия, watcher не
  записал строку, строку удалила чистка) строку, начатую после его конца, не забирает. Тревогу затем дают только живые шедулеры, прошедшие стартовую отсрочку:
  закреплённая живая строка — watcher работает, закреплённая неживая — watcher этого
  шедулера умер, не закреплённой нет — watcher ни разу не отчитался (старая версия
  провайдера или не стартовал). Нет живых шедулеров — тревога об этом, что бы ни было в
  строках. Строки ушедших шедулеров показываются на панели и тревоги не поднимают.
  Поэтому ни чистка строк, ни `stopped_at` на тревогу не влияют.
- **Штатная остановка помечается по сигналу.** Runner после штатного сигнала остановки
  сначала вызывает `request_stop` (событие и пробуждение цикла), затем `mark_stopped`,
  который отдельным коротким потоком записывает `stopped_at` с ожиданием не дольше 2 с,
  независимо от идущего reconcile. Смерть шедулера (смена `getppid()`, SIGTERM от
  `PR_SET_PDEATHSIG`) отметку не ставит. Отметка нужна панели, чтобы отличать «остановлен»
  от «умер».
- **Оценка — одна чистая функция.** `assess_liveness` по строкам heartbeat, живым
  `SchedulerJob`, текущему времени, признаку «есть включённые подписки» и признаку «есть
  свежий `last_reconcile_at`» возвращает вердикт; её вызывают баннер, панель и
  отображение статусов, поэтому они не расходятся.
- **Баннер — штатный `UIAlert`, только в webserver'е.** Колбэк `record_once` в `plugin.py`
  по образцу `_grant_op_access` проверяет `_serves_the_web_ui(state.app)` и вызывает
  `dashboard_alert.install_dashboard_alert()`, который лениво импортирует `UIAlert`,
  определяет подкласс и добавляет экземпляр в `airflow.settings.DASHBOARD_UIALERTS`. Модули
  плагина `airflow.www.utils` на уровне модуля не импортируют.
- **Чистка — отдельно и без гарантий.** После коммита своей отметки живой watcher отдельной
  транзакцией удаляет чужие строки, чья последняя отметка старше суток. На тревогу это не
  влияет.

## Technical Details

### Таблица `rmq_watcher_heartbeat`

| колонка | тип | смысл |
|---|---|---|
| `id` | `String(36)`, PK | UUID запуска процесса |
| `hostname` | `String(500)` | `airflow.utils.net.get_hostname()` |
| `pid` | `Integer` | PID watcher-процесса |
| `started_at` | `DateTime` | старт процесса |
| `last_cycle_at` | `DateTime`, nullable | момент записи отметки последнего прохода в бюджете |
| `due_at` | `DateTime` | крайний срок следующей отметки при действующих настройках |
| `stopped_at` | `DateTime`, nullable | штатная остановка |

Время — naive UTC из процесса, как у `last_reconcile_at`. Таблица создаётся
`ensure_table_exists` (`create_all`). Ни один upsert heartbeat не пишет `stopped_at`: его
пишет только `record_heartbeat_stop`.

`Job.start_date` объявлен `UtcDateTime` и читается с часовым поясом;
`get_scheduler_jobs` приводит его к naive UTC, как `_build_conn_status_rows`
(`views.py:327-341`) приводит `last_reconcile_at`, чтобы `assess_liveness` сравнивал
значения одного вида.

**Последняя отметка** строки — `stopped_at`, иначе `last_cycle_at`, иначе `started_at`.

### Срок `due_at`

Обозначения: `I` — действующий интервал, `B` — действующий бюджет прохода, `V` —
`_VARIABLE_TIMEOUT` (чтение настроек вне бюджета), `H` — граница одной записи heartbeat
(`_HEARTBEAT_TIMEOUT = 10` с), `P` — граница чистки (`_PRUNE_TIMEOUT = 10` с).

`now` берётся перед записью, поэтому в каждую формулу входят остаток текущей записи (`H`)
и следующая запись (`H`).

- **Строка старта** пишется после первого `_refresh_settings` процесса, перед первым
  проходом: `due_at = now + H + B + H`.
- **Перед проходом**, если после `_refresh_settings` бюджет вырос против того, с которым
  считан сохранённый срок: `due_at = now + H + B + H`, `last_cycle_at` не трогается.
- **Отметка прохода**: `last_cycle_at = now`, `due_at = now + H + P + I + V + B + H` — после
  неё идут остаток записи, чистка, ожидание интервала, чтение настроек, проход и запись
  следующей отметки.

### Вердикт живости

- Строка heartbeat **жива**, если `stopped_at` пуст и `now ≤ due_at`.
- В сопоставлении участвуют строки `job` с `job_type = "SchedulerJob"`, у которых
  `latest_heartbeat` (иначе `start_date`) моложе окна чистки heartbeat (сутки), — живые,
  стартующие и завершённые. Шедулер **жив**, если его строка проходит `Job.is_alive()`.
  Живой шедулер **обязан иметь watcher**, если его `start_date` старше стартовой отсрочки
  `_START_GRACE = 300` с (процессу watcher'а нужно импортировать Airflow и плагины до
  первой записи).
- **Сопоставление на хосте** H: все участвующие шедулеры H перебираются от самого старого
  `start_date` к самому новому, и каждый закрепляет за собой самую старую ещё не
  закреплённую строку H, начатую в пределах его жизни:
  `start_date − T ≤ started_at ≤ верх`, где `T = _MATCH_TOLERANCE` (60 с). У живого
  шедулера верхней границы нет. У неживого верх — `min(конец + T, start_date следующего
  шедулера H)`, где конец — `end_date`, иначе `latest_heartbeat`: watcher стартует из
  `Job.__init__`, через секунды после старта своего шедулера, поэтому строка, начатая после
  старта преемника, принадлежит преемнику, даже если она попадает в допуск после конца
  предшественника. Допуск снизу
  покрывает повторную установку `start_date` в `Job.prepare_for_execution` после
  `on_starting` и секундное округление `DateTime` в MySQL. Предусловие: `started_at`
  берётся в `run`, после импорта Airflow в дочернем процессе, — то есть заведомо позже
  первой установки `start_date`. Фильтры «жив» и «обязан» применяются после
  сопоставления, поэтому строки остановленных и стартующих шедулеров остаются за ними.
  Обязанный шедулер в итоге:
  - **обслужен** — закреплённая строка жива;
  - **без watcher'а** — закреплённая строка не жива (watcher умер или остановлен);
  - **не отчитался** — закрепить нечего: watcher этого шедулера не писал heartbeat ни разу
    (старая версия провайдера или не стартовал).
- **Свежий `last_reconcile_at`** — моложе `_stale_after()` страницы; считается только когда
  живых строк heartbeat нет, то есть Variables читаются лишь в этом случае.

Состояния проверяются сверху вниз, первое совпавшее — вердикт:

| состояние | условие | баннер | статусы подписок |
|---|---|---|---|
| `unused` | включённых подписок нет | нет | как есть |
| `no_scheduler` | живых шедулеров нет | красный | `unknown`, если и живых строк нет; иначе как есть |
| `starting` | живые шедулеры есть, обязанных нет (все в отсрочке) | нет | как есть |
| `ok` | все обязанные шедулеры обслужены | нет | как есть |
| `degraded` | есть живые строки, и есть шедулер без watcher'а или не отчитавшийся | жёлтый | как есть |
| `legacy` | живых строк нет, все необслуженные шедулеры — не отчитавшиеся, свежий `last_reconcile_at` есть | нет (на панели пояснение) | как есть |
| `mixed` | живых строк нет, есть и не отчитавшиеся, и без watcher'а, свежий `last_reconcile_at` есть | жёлтый | как есть |
| `never` | строк heartbeat нет совсем | красный | `unknown` |
| `down` | всё остальное: живых строк нет, свежей отметки старой версии нет | красный | `unknown` |

`legacy` и `mixed` требуют хотя бы одного не отчитавшегося шедулера: свежая отметка
`last_reconcile_at` засчитывается старой версии только когда есть шедулер, чей watcher
heartbeat не пишет. Без такого шедулера свежая отметка — след только что остановленного
нового watcher'а и тревогу не снимает.

Тексты баннера — без hostname и PID (их видят все пользователи, включая
`AUTH_ROLE_PUBLIC`); подробности — на панели Subscriptions:

- `never`: «RMQ watcher has not reported since the subscriptions were enabled — subscriptions
  are not consumed.»
- `down`: «RMQ watcher has not completed a cycle since `<время самой свежей отметки>` —
  subscriptions are not consumed and event-triggered DAGs do not run.»
- `no_scheduler`, строки heartbeat есть: «No Scheduler is running the RMQ watcher since
  `<время самой свежей отметки>` — subscriptions are not consumed.»
- `no_scheduler`, строк heartbeat нет: «No Scheduler is running the RMQ watcher —
  subscriptions are not consumed.»
- `degraded`: «A Scheduler has no running RMQ watcher (not started, stopped responding, or an
  older provider version); other watchers are consuming.»
- `mixed`: «A Scheduler's RMQ watcher stopped responding; a watcher of an older provider
  version is still consuming.»

Каждый текст ссылается на `/rmq-watcher/subscriptions`. Баннер видят все пользователи
(`roles=None`).

### Запись из watcher-процесса

Записи старта, срока и прохода — шаг `_cycle_step` с одним именем `"heartbeat"` и границей
`H`, чтобы зависшая запись занимала не больше одного воркера cycle-пула из четырёх; чистка —
шаг `"heartbeat_prune"` с границей `P`.

- **Старт процесса.** После первого `_refresh_settings` процесса, перед первым проходом —
  upsert строки (`id`, `hostname`, `pid`, `started_at`, `due_at`). Upsert не сбрасывает
  `last_cycle_at`. Перезапуск `_main` строку старта не пишет заново.
- **Перед проходом** — запись нового `due_at`, если бюджет вырос.
- **Конец прохода в бюджете** (рядом с `incr("rmq_watcher.cycle_completed")`), если
  остановка не запрошена: upsert `last_cycle_at` и `due_at`, коммит; затем отдельной
  транзакцией best-effort чистка чужих строк, чья последняя отметка старше суток. Upsert, а
  не UPDATE: строку, не вставленную при старте, следующий проход восстанавливает.
- Сбой записи, `_StepInFlight`, `PoolSaturated` — WARNING, цикл продолжается;
  `_StopRequested` — тихо.
- **Штатная остановка.** `request_stop` не меняется: событие остановки и пробуждение цикла.
  Новый `mark_stopped()` при первом вызове, если идентичность процесса уже создана,
  запускает поток, который своей сессией пишет `record_heartbeat_stop`, и ждёт его не
  дольше 2 с; повторные вызовы отметку не пишут. Runner в ветке полученного сигнала
  вызывает сначала `request_stop()`, затем `mark_stopped()` — только если `getppid()` всё
  ещё равен PID шедулера: SIGTERM от `PR_SET_PDEATHSIG` приходит уже после смены
  родителя. Ветка смены `getppid()`, `_parent_is_gone` и краш `_main` отметку не ставят.
  Событие выставлено раньше отметки, поэтому цикл получает остановку без задержки.
  `record_heartbeat_stop` — upsert с полной идентичностью (`id`, `hostname`, `pid`,
  `started_at`, `due_at`, `stopped_at`): запись старта, которая к этому моменту уже идёт в
  воркере и закоммитится позже, `stopped_at` не трогает и не стирает, а не начатую
  `_cycle_step` при выставленном событии не подаёт. Сбой и таймаут отметки — WARNING;
  строка тогда просто не будет помечена остановленной.

### Баннер в webserver'е

- `plugin.py`: колбэк `_install_liveness_alert(state)` по образцу `_grant_op_access` —
  выходит, если `_serves_the_web_ui(state.app)` ложно, иначе вызывает
  `dashboard_alert.install_dashboard_alert()`; регистрация `_bp.record_once(...)`.
- `dashboard_alert.install_dashboard_alert()`: лениво импортирует `UIAlert`, определяет
  `_WatcherLivenessAlert` и перепривязывает список:
  `settings.DASHBOARD_UIALERTS = [*settings.DASHBOARD_UIALERTS, alert]` (список мог быть
  задан кортежем в `airflow_local_settings`); повторный вызов второй экземпляр не
  добавляет.
- `_WatcherLivenessAlert` не вызывает `UIAlert.__init__`: задаёт `roles = None`, а `message`
  и `category` — свойства без записи, читающие снимок вердикта из `flask.g`. `message`
  возвращает `markupsafe.Markup`, собранный `Markup(template).format(...)`: ссылка остаётся
  разметкой, подставленное время экранируется.
- `should_show(self, *args, **kwargs)` берёт вердикт из кэша и кладёт его в `flask.g`;
  `message` и `category` при рендере читают только `flask.g` и в БД не ходят, так что
  отбор и текст одного запроса не расходятся.
- Кэш — неизменяемая пара `(expires_at, verdict)`, заменяемая целиком, 30 с на процесс
  воркера webserver'а; перечтение под `threading.Lock`, чтобы промах кэша не дал залп
  запросов. Промах — SELECT по `rmq_watcher_heartbeat`, SELECT строк `SchedulerJob` за окно
  (с `start_date`, приведённым к naive UTC),
  `EXISTS` по включённым подпискам и, только когда живых строк heartbeat нет, чтение
  `last_reconcile_at` и `_stale_after()`.
- Сбой чтения (нет таблицы, БД недоступна) — баннер не показывается, WARNING раз на период
  кэша: главная страница Airflow не падает из-за плагина.

### Страница Subscriptions

- Панель «Watcher processes» над блоком Connections: по каждому хосту — живые шедулеры с
  их состоянием сопоставления (обслужен / без watcher'а / не отчитался / в отсрочке) и
  строки heartbeat этого хоста (PID, started, последняя отметка и её возраст, срок,
  состояние alive / dead / stopped, закреплена ли за живым шедулером); пояснение для
  `legacy` и `mixed`; баннер с тем же текстом вверху страницы.
- При вердикте `no_scheduler`, `never` или `down` статусы подписок и `conn_id` выводятся
  как `unknown` с подсказкой «watcher is not running — last recorded: `<status>`».

### Что делает документация явным

- Тревога означает «у живого шедулера нет живого watcher'а» или «живых watcher'ов нет»;
  строки, не закреплённые за живым шедулером, — следы ушедших шедулеров: они видны на
  панели сутки и тревоги не поднимают.
- Шедулер, чей watcher ни разу не писал heartbeat, при свежей отметке `last_reconcile_at`
  считается работающим на старой версии провайдера.
- Шедулер, у которого зависает собственный цикл, перестаёт считаться живым, и недостачу на
  его хосте баннер не показывает — об этом сообщает собственное предупреждение Airflow
  о шедулере.
- Один проход, упёршийся в бюджет, может на короткое время дать недостачу у живого
  процесса: перезапуск `_main` ждёт 30 с и добавляет время остановки менеджера.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): код, тесты, документация в этом репозитории.
- **Post-Completion** (no checkboxes): проверка на живом Airflow, выкладка на прод.

## Implementation Steps

### Task 1: Сверить используемые API с Airflow 2.9.1

**Files:**
- Modify: `docs/plans/20261117-watcher-liveness-heartbeat.md`

- [ ] по исходникам Airflow 2.9.1 (тег `2.9.1` в apache/airflow) подтвердить: атрибуты,
      которые `UIAlert.__init__` присваивает, сигнатуру `should_show`, что
      `views.Airflow.index` фильтрует `settings.DASHBOARD_UIALERTS` вызовом `should_show`
      на каждый запрос, что `dags.html` читает `m.message` и `m.category` при рендере с
      автоэкранированием, что `flask.g` доступен в обоих местах одного запроса
- [ ] подтвердить для 2.9.1: колонки `Job` (`job_type`, `state`, `hostname`,
      `latest_heartbeat`, `start_date`), `Job.is_alive()` и порог
      `scheduler_health_check_threshold`, `airflow.utils.net.get_hostname()` как источник
      `Job.hostname`
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
- [ ] `record_heartbeat_start(session, identity, due_at)` — upsert, не сбрасывающий
      `last_cycle_at`; `record_heartbeat_due(session, identity, due_at)`;
      `record_heartbeat_cycle(session, identity, last_cycle_at, due_at)` — upsert
- [ ] `prune_heartbeats(session, keep_id, older_than)` — удаление чужих строк по последней
      отметке; `record_heartbeat_stop(session, identity, due_at, stopped_at)` — upsert с
      полной идентичностью; `get_heartbeats(session)`; `get_scheduler_jobs(session, since)`
      — строки `SchedulerJob` за окно (hostname; `start_date`, `end_date`,
      `latest_heartbeat`, приведённые к naive UTC; признак `is_alive()`)
- [ ] тесты: таблица создаётся `ensure_table_exists`; upsert старта поверх строки с
      `last_cycle_at` его сохраняет, поверх строки со `stopped_at` — тоже; upsert прохода
      поверх существующей и отсутствующей строки; запись срока не трогает `last_cycle_at`;
      чистка не трогает свою строку и свежие чужие и удаляет старые по каждой из трёх
      отметок; stop на существующей и на отсутствующей строке
- [ ] тесты: `get_scheduler_jobs` возвращает живой, стартующий и завершённый `SchedulerJob`
      из окна с верным признаком живости, пропускает строки старше окна и другого
      `job_type`; `start_date`, `end_date`, `latest_heartbeat` возвращаются naive UTC
- [ ] run tests — must pass before task 3

### Task 3: Оценка живости `assess_liveness`

**Files:**
- Create: `airflow_provider_rmq/watcher/liveness.py`
- Create: `tests/watcher/test_liveness.py`

- [ ] `LivenessVerdict` (состояние `unused | no_scheduler | starting | ok | degraded | legacy
      | mixed | never | down`; по каждому хосту — шедулеры с результатом сопоставления и
      строки с признаком закрепления; время для текста)
- [ ] `assess_liveness(heartbeats, scheduler_jobs, now, has_enabled_subscriptions,
      has_fresh_reconcile)` — правила из Technical Details: `due_at`, стартовая отсрочка,
      сопоставление шедулеров и строк на хосте, порядок состояний сверху вниз
- [ ] `banner_text(verdict)`, `banner_category(verdict)`, `masks_statuses(verdict)` — тексты
      без hostname и PID (у `no_scheduler` — два варианта, со строками и без), подмена
      статусов для `never`, `down` и для `no_scheduler` без живых строк
- [ ] тесты: каждое состояние таблицы; граница `due_at` (ровно `due_at` — жива); все живые
      шедулеры в отсрочке — `starting` без баннера при любом `last_reconcile_at`; строка с
      `started_at` на 30 с раньше `start_date` закрепляется, на 120 с раньше — нет
- [ ] тест на строках, прочитанных из БД (настоящий `Job` через `get_scheduler_jobs` и
      настоящая строка heartbeat): `assess_liveness` отрабатывает без `TypeError` и даёт `ok`
- [ ] тесты: watcher умер, шедулер жив — `down` при одной реплике, `degraded` при двух;
      рестарт шедулера в том же контейнере (старый `SchedulerJob` не жив, новая строка
      начата после старта нового) — `ok`, хотя мёртвая строка осталась; две реплики на
      одном hostname — нужны две закреплённые живые строки, смерть одной даёт `degraded`
- [ ] тесты сопоставления на одном hostname: A жив, младший B остановлен (его строка
      `stopped`, новее строки A) — `ok`, строка A закреплена за A; B передеплоен как B' —
      `ok`; watcher A мёртв, B перезапускается и ещё в отсрочке — не `ok` (`degraded` или
      `down`), живая строка B' за A не закрепляется; watcher B' записался раньше watcher'а A
      при почти одновременном старте — вердикт верен
- [ ] тесты границы жизни на одном hostname: завершённый шедулер старой версии без строки и
      следующий с живой строкой — `ok`, строка не уходит завершённому; завершённый
      шедулер, чью строку удалила чистка, и перезапущенный — `ok`; завершённый шедулер без
      `end_date` — граница по `latest_heartbeat`; рестарт с паузой 5 с, у старого шедулера
      строки нет (обновление со старой версии и после краша) — строка нового закреплена за
      новым, `ok`; рестарт с паузой 5 с, у старого своя мёртвая строка — `ok`; строка,
      начатая через 30 с после конца шедулера без преемника на хосте, закрепляется за ним,
      через 120 с — нет
- [ ] тесты: `docker stop` единственного шедулера (строка `stopped`, `SchedulerJob` не жив,
      свежий `last_reconcile_at`) — `no_scheduler`, а не `legacy`; `kill -9` единственного
      шедулера (строка ещё живая до `due_at`) — `no_scheduler` без подмены статусов; живых
      шедулеров нет и строк нет — `no_scheduler` с текстом без времени
- [ ] тесты: смешанные версии — старый шедулер без строк на своём хосте, новый остановлен,
      свежий `last_reconcile_at` — `legacy`; старый и новый шедулер на одном hostname, новый
      watcher умер, свежий `last_reconcile_at` — `mixed`, статусы не подменяются; старый
      шедулер на одном хосте и мёртвый новый watcher на другом при свежей отметке —
      `mixed`; не отчитавшийся шедулер при живых новых watcher'ах — `degraded`; пустая
      таблица при свежем и устаревшем `last_reconcile_at` — `legacy` и `never`
- [ ] run tests — must pass before task 4

### Task 4: Запись heartbeat из watcher-процесса

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `airflow_provider_rmq/watcher/runner.py`
- Modify: `tests/watcher/test_listener.py`
- Modify: `tests/watcher/test_runner.py`

- [ ] `WatcherIdentity` создаётся один раз на процесс в `run` (hostname —
      `get_hostname()`); константы `_HEARTBEAT_TIMEOUT`, `_PRUNE_TIMEOUT`
- [ ] старт: после первого `_refresh_settings` процесса, перед первым проходом — шаг
      `"heartbeat"` с `record_heartbeat_start` и `due_at = now + H + B + H`; перезапуск `_main`
      старт не пишет
- [ ] перед проходом: если бюджет вырос против сохранённого срока — шаг `"heartbeat"` с
      `record_heartbeat_due`
- [ ] конец прохода в бюджете без запрошенной остановки: шаг `"heartbeat"` с
      `record_heartbeat_cycle` (`last_cycle_at` — момент записи, `due_at` по формуле), затем
      шаг `"heartbeat_prune"`; `_StopRequested` — тихо, сбой, `_StepInFlight`,
      `PoolSaturated` — WARNING
- [ ] `mark_stopped()`: при первом вызове и созданной идентичности — поток с
      `record_heartbeat_stop` своей сессией, ожидание не дольше 2 с, сбой и таймаут —
      WARNING; `request_stop` не меняется
- [ ] runner, ветка полученного сигнала: `request_stop()`, затем `mark_stopped()` только
      если `getppid()` равен PID шедулера; ветка смены `getppid()` — только `request_stop()`
- [ ] тесты: строка старта несёт срок из переопределённого бюджета (Variable
      `rmq_watcher_cycle_timeout`); рост бюджета публикует новый срок до прохода и не
      двигает `last_cycle_at`; проход в бюджете двигает `last_cycle_at` и `due_at`; проход,
      упёршийся в бюджет, — нет; перезапуск `_main` после краша сохраняет `id` и
      `started_at`
- [ ] тесты (`test_listener.py`): `mark_stopped` пишет `stopped_at`, пока reconcile ещё
      идёт, и не ждёт дольше 2 с при зависшей записи; повторный вызов второй записи не
      делает; вызов до `run` ничего не пишет; stop, пришедший до подачи записи старта, —
      запись старта не подаётся; stop, пришедший, пока запись старта уже идёт в воркере, —
      итоговая строка со `stopped_at`; проход, записанный после отметки, `stopped_at` не
      стирает; краш `_main` `stopped_at` не ставит; остановка не даёт WARNING о heartbeat
      прохода
- [ ] тесты (`test_runner.py`): штатный SIGTERM при живом родителе — `request_stop`, затем
      `mark_stopped`; смена `getppid()` и SIGTERM после смены родителя — только
      `request_stop`
- [ ] тесты: сбой записи и сбой чистки не прерывают цикл и не мешают `cycle_completed`;
      сбой чистки не откатывает отметку; зависшая запись не подаётся повторно
- [ ] run tests — must pass before task 5

### Task 5: Баннер на главной странице Airflow

**Files:**
- Create: `airflow_provider_rmq/watcher/dashboard_alert.py`
- Modify: `airflow_provider_rmq/watcher/plugin.py`
- Create: `tests/watcher/test_dashboard_alert.py`
- Modify: `tests/watcher/test_plugin.py`

- [ ] `dashboard_alert.py`: чтение вердикта с кэшем (неизменяемая пара, 30 с, `Lock` на
      перечтение; `last_reconcile_at` и `_stale_after()` — только без живых строк
      heartbeat); сбой чтения — вердикт «не показывать» и WARNING раз на период
- [ ] `install_dashboard_alert()`: ленивый импорт `UIAlert`, подкласс без вызова
      `UIAlert.__init__`, `should_show(*args, **kwargs)` со снимком в `flask.g`,
      `message` — `Markup` из `flask.g`, `category` из `flask.g`, перепривязка
      `DASHBOARD_UIALERTS`, без повторного экземпляра
- [ ] `plugin.py`: колбэк `_install_liveness_alert(state)` с проверкой
      `_serves_the_web_ui(state.app)` и `_bp.record_once(_install_liveness_alert)`
- [ ] тесты: `should_show` и текст для каждого состояния; `message` — экземпляр `Markup`
      с настоящей ссылкой, а подставленное время экранировано; кэш не перечитывает БД в
      течение периода; кэш, истёкший между `should_show` и `message`, не меняет текст
      запроса; сбой чтения скрывает баннер без исключения
- [ ] тесты: установка добавляет ровно один экземпляр и сохраняет существующие элементы,
      в том числе заданные кортежем; CLI-приложение FAB баннер не ставит
- [ ] тест отдельным интерпретатором (как `TestDagFileScanImportFootprint`):
      `import airflow_provider_rmq.watcher.plugin` не загружает `airflow.www.utils`
- [ ] run tests — must pass before task 6

### Task 6: Панель хостов и статусы подписок на странице Subscriptions

**Files:**
- Modify: `airflow_provider_rmq/watcher/views.py`
- Modify: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`
- Modify: `tests/watcher/test_views.py`

- [ ] панель «Watcher processes»: по хостам — живые шедулеры с результатом сопоставления,
      строки heartbeat с состоянием, возрастом отметки, сроком и признаком закрепления;
      пояснение для `legacy` и `mixed`; баннер с тем же текстом вверху страницы
- [ ] при вердикте `no_scheduler`, `never` или `down` статусы подписок и `conn_id`
      выводятся как `unknown` с подсказкой о последнем записанном статусе
- [ ] тесты: панель для одного и нескольких хостов, мёртвая строка ушедшего шедулера
      показана без тревоги; подмена статусов при `no_scheduler`/`never`/`down` и её
      отсутствие при `starting`/`ok`/`degraded`/`legacy`/`mixed`
- [ ] run tests — must pass before task 7

### Task 7: Verify acceptance criteria

- [ ] каждое состояние таблицы и каждый сценарий из Task 3 покрыт тестом (сверить по
      списку: смерть watcher'а при одной и двух репликах, рестарт шедулера в том же
      контейнере, две реплики на одном hostname, остановка всех шедулеров, все шедулеры в
      отсрочке, смешанные версии на разных хостах и на одном, webserver новее шедулера,
      недоступная БД, без включённых подписок, рост бюджета, смерть шедулера без отметки
      остановки, `start_date` из настоящего `Job`)
- [ ] тест отдельным интерпретатором из Task 5 проходит: плагин не загружает
      `airflow.www.utils`
- [ ] run full test suite: `.venv/bin/python -m pytest tests/ -q`
- [ ] run linters: `.venv/bin/ruff check airflow_provider_rmq tests` (новых ошибок нет)

### Task 8: [Final] Update documentation

- [ ] `readme.md` и `readme_ru.md`, раздел «The watcher process»: абзац о неперезапуске
      (`readme.md:973`) описывает баннер на главной и панель на Subscriptions; новый абзац о
      heartbeat — сверка с живыми шедулерами, состояния, стартовая отсрочка, суточная
      чистка, краткая недостача после прохода, упёршегося в бюджет, зависший шедулер;
      абзац «Liveness metric» описывает метрику как дополнение для внешнего мониторинга
- [ ] новый `docs/adr/0012-watcher-liveness-heartbeat.md`: строка на запуск процесса, срок
      `due_at` от watcher'а, тревога как недостача watcher'ов у живых шедулеров из таблицы
      `job` с сопоставлением шедулера и строки на хосте, `stopped_at` только по штатному
      сигналу (`mark_stopped`), `UIAlert` с ленивым импортом; отвергнутые
      варианты — поллинг из хуков шедулера, внешний мониторинг, health-check DAG, flash на
      каждой странице, общая строка `rmq_watcher_conn_status`, фоновый пинг, тревога по
      мёртвым строкам с правилом замены по hostname
- [ ] `docs/adr/0008-watcher-child-process.md`: раздел о живости (:311) в настоящем времени
      — процесс не перезапускается, живость показывают heartbeat (ADR 0012) и метрика
- [ ] `CHANGELOG.md`, раздел `## v2.7.0` → Added: по одной строке на пункт (баннер на
      главной, панель хостов на Subscriptions, таблица `rmq_watcher_heartbeat`)
- [ ] `docs/backlog/watcher-child-death-unnoticed.md`: сузить до неперезапуска и отсутствия
      push-уведомлений
- [ ] ссылки в документации — только относительные
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification (тестовый стенд, vhost `test`):**
- одна реплика: `kill -9` watcher-процесса → красный баннер на главной не позже `due_at`;
  статусы подписок — `unknown`; рестарт шедулера → баннер исчезает
- `docker stop` шедулера → строка со `stopped_at` на панели, баннер `no_scheduler`;
  `kill -9` шедулера → тот же баннер, как только `SchedulerJob` перестаёт быть живым
  (`scheduler_health_check_threshold`), строка без `stopped_at`
- webserver с новой версией при шедулере со старой → баннера нет, на панели пояснение
  `legacy`
- вид баннера в светлой и тёмной теме Airflow UI

**Deployment:**
- выкладка на прод (2.9.1) — с согласия владельца; таблица создаётся при первом старте
  процесса Airflow с новой версией провайдера

**Следующие решения (вне плана):**
- супервизор с перезапуском умершего watcher'а и push-уведомлением о смерти
- ограничение памяти watcher'а (prefetch) как устранение вероятной причины OOM
