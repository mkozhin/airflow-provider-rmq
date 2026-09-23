# Watcher в дочернем процессе шедулера

## Overview

- Цикл RMQ Watcher работает в отдельном дочернем процессе шедулера, запущенном через
  `subprocess.Popen` (fork+exec). В самом процессе `airflow scheduler` плагин не
  создаёт ни потоков, ни пулов, ни event loop.
- Решаемая проблема — `docs/incidents/20260923-scheduler-hang.md`. Airflow делает
  `fork()` из процесса шедулера: пул отправки задач `CeleryExecutor`
  (`_send_tasks_to_celery`, `ProcessPoolExecutor`) и запуск `DagFileProcessorManager`
  (`scheduler_job_runner.py:839`). Это безопасно только в однопоточном процессе. По
  `py-spy dump` на проде все фоновые потоки шедулера — потоки плагина
  (`rmq-watcher`, `rmq-watcher-cycle_*`, `rmq-watcher-consumer_*`, `asyncio_0`), и
  дочерний процесс fork может унаследовать захваченную ими блокировку и зависнуть.
- Установка не меняется: `pip install`, без правок docker compose и без отдельных
  сервисов. Под шедулером в дереве процессов появляется процесс
  `python -m airflow_provider_rmq.watcher.runner`.
- Поведение watcher'а для пользователя то же: те же подписки, переменные, страница
  Subscriptions, строки в логе контейнера шедулера.

## Context (from discovery)

- `airflow_provider_rmq/watcher/listener.py`:
  - `RMQWatcherListener.__init__` (стр. 891) создаёт пулы `BoundedExecutor`
    (стр. 952-953); потоки пулов появляются только при первом `submit`
    (`utils/executor.py:59`), так что сам экземпляр потоков не создаёт;
  - `on_starting` (стр. 976) определяет шедулер и зовёт `_start` (стр. 1032), который
    создаёт stop event и поднимает поток `rmq-watcher` с `_run_loop` (стр. 1055);
    INFO-строка об интервале и бюджете цикла — стр. 1047-1053;
  - `before_stopping` (стр. 996) ставит stop event, будит цикл (`_wake_loop`) и
    джойнит поток с `_JOIN_TIMEOUT = 5.0` (стр. 123);
  - `_run_loop` пересоздаёт event loop после падения `_main` с паузой
    `self._stop_event.wait(timeout=30)` (стр. 1074);
  - `_main` (стр. 1077) — цикл reconcile; `_run_cycle` сам перехватывает
    `_StepInFlight` и `Exception` (стр. 1178-1181), наружу выходит только таймаут
    цикла;
  - `manager.stop()` ограничен `_STOP_TIMEOUT = 30.0` (стр. 113).
- `airflow_provider_rmq/watcher/plugin.py:221` — `listeners = [RMQWatcherListener()]`:
  экземпляр создаётся при импорте плагина во всех компонентах Airflow (scheduler,
  webserver, triggerer, worker).
- `airflow_provider_rmq/utils/executor.py:44-50` — at-exit join потоков пулов без
  таймаута: `sys.exit` процесса с застрявшим вызовом в пуле не завершается.
- `airflow/jobs/job.py:119` (Airflow 2.9.1): `on_starting` вызывается в
  `Job.__init__` в основном потоке, до форков health-сервера и
  `DagFileProcessorManager` — процесс шедулера в этот момент ещё однопоточный.
- `airflow_provider_rmq/utils/metrics.py:8` — `incr()` для StatsD-метрик.
- Несколько шедулеров: watcher уже рассчитан на параллельные экземпляры
  (`listener.py:1709,1765,1834`, `models.py:176`); один дочерний процесс на шедулер
  сохраняет эту семантику.
- Тесты: `tests/watcher/test_listener.py` (десятки тестов вызывают `_run_cycle()` и
  `_main()` напрямую), `tests/watcher/test_plugin.py`, `tests/conftest.py`; запуск
  `pytest tests/`.
- Прод: Airflow 2.9.1, Python 3.10.14, CeleryExecutor, docker compose, dumb-init с
  `DUMB_INIT_SETSID=1` (SIGTERM уходит всей группе процессов контейнера). Локальный
  venv — Airflow 2.11.2; нижняя граница совместимости — 2.9.1.
- `docs/plans/20260831-listener-generation-ownership.md` — отдельный незавершённый
  план про пересечение поколений потока watcher'а в одном объекте listener'а.
  Механизм, о котором он написан (`_start`, поток, `_JOIN_TIMEOUT`), этот план
  удаляет; судьба того плана решается отдельно, его файл здесь не меняется.

## Development Approach

- **testing approach**: Regular (код задачи, затем тесты в той же задаче)
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
- maintain backward compatibility: подписки, таблицы, переменные, страница
  Subscriptions и существующие метрики не меняются; меняется только процесс, в котором
  идёт цикл
- комментарии, докстринги и документация описывают текущую модель и не упоминают, как
  было раньше

## Testing Strategy

- **unit tests**: required for every task (see Development Approach above)
- **integration test**: настоящий `subprocess.Popen` runner'а с подменённым циклом —
  старт, выход по SIGTERM, выход после смерти родителя (задача 5)
- **e2e tests**: UI-тестов в проекте нет; проверка на живом стенде — в Post-Completion

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview

- **Процесс шедулера.** `RMQWatcherListener` в нём тонкий: `on_starting` запускает
  дочерний процесс, `before_stopping` его останавливает. `Popen` делает fork+exec на
  уровне C (`_posixsubprocess`): между fork и exec в дочернем процессе не выполняется
  Python-код, поэтому запуск безопасен при любом состоянии родителя. `close_fds=True`
  (по умолчанию): дочерний процесс не держит сокетов и пайпов шедулера.
- **Дочерний процесс** — чистый интерпретатор. `import airflow` поднимает конфиг,
  ORM, секреты, StatsD и консольное логирование из того же окружения контейнера. У
  процесса свой движок SQLAlchemy: до `pool_size + max_overflow` соединений с
  метабазой на каждый шедулер сверх соединений самого шедулера.
- **Сигналы без обработчиков.** Runner первой строкой блокирует SIGTERM и SIGINT
  (`signal.pthread_sigmask`), и эту маску наследуют все потоки процесса. Цикл
  watcher'а идёт в отдельном потоке runner'а; основной поток ждёт сигнал в
  `signal.sigtimedwait` и, получив его, обычным кодом просит цикл остановиться.
  Python-обработчик сигнала не выполняется никогда, поэтому сигнал не может прервать
  код, держащий блокировку (`threading.Event`, буфер stdout), а сигнал, пришедший до
  запуска цикла, остаётся в очереди ядра и не теряется. Поток внутри дочернего
  процесса безопасен: сам дочерний процесс `fork()` не делает.
- **Смерть родителя.** PID шедулера родитель передаёт в окружении. Runner ставит
  `prctl(PR_SET_PDEATHSIG, SIGTERM)` и сразу сравнивает `os.getppid()` с переданным
  PID: несовпадение значит, что шедулер умер до установки, и runner выходит. Дальше
  ядро само присылает SIGTERM при завершении основного потока шедулера, а цикл
  дополнительно сверяет `getppid()` перед каждым проходом. Пайп с EOF для этого не
  подходит: fork'и шедулера (пул отправки, менеджер разбора DAG'ов) наследуют его
  конец, и EOF не приходит, пока они живы. PDEATHSIG привязан к потоку, запустившему
  процесс, — это основной поток шедулера (`Job.__init__`).
- **Выход.** Основной поток runner'а ждёт поток цикла ограниченное время, затем
  `logging.shutdown()` и `os._exit()`. At-exit join потоков пулов не выполняется, и
  застрявший в пуле вызов не держит процесс.
- **Остановка из шедулера.** `before_stopping`: SIGTERM, ожидание
  `_CHILD_STOP_TIMEOUT` (5 с), затем SIGKILL. SIGKILL обрывает `manager.stop()`,
  бюджет которого 30 с; неподтверждённые доставки брокер возвращает в очередь. При
  `docker stop` dumb-init присылает runner'у SIGTERM напрямую, вместе с шедулером;
  повторная просьба об остановке безвредна.
- **Смерть самого процесса watcher'а** (OOM, segfault, SIGKILL) не приводит к
  перезапуску: перезапускать из шедулера нечем без потока или обработчика SIGCHLD.
  Умерший процесс виден в `docker top` как `<defunct>` под шедулером до его
  остановки. Сигнал живости — StatsD-счётчик `rmq_watcher.cycle_completed`: он растёт
  на каждом проходе цикла, дошедшем до конца, включая проходы, в которых отдельные
  шаги упали и были записаны в лог. Счётчик говорит о том, что процесс жив и цикл
  крутится, а не о том, что всё здорово; правило «нет прироста N минут» каждый
  настраивает сам.
- **Потоковая схема в шедулере удаляется полностью**, без настройки для возврата.
  Откат — предыдущая версия пакета.
- **Рекурсия исключена** маркером окружения `AIRFLOW_PROVIDER_RMQ_WATCHER_CHILD=1`:
  listener в процессе с маркером ничего не запускает. `Job` дочерний процесс не
  создаёт, так что `on_starting` там не вызывается; маркер — страховка на случай,
  если в дочернем процессе загрузятся плагины (`core.lazy_load_plugins=False` или
  вызов `get_listener_manager()` на пути `trigger_dag`).

## Technical Details

- **Константы окружения** — в `runner.py`, listener импортирует их оттуда.
  `runner.py` на верхнем уровне импортирует только stdlib; listener и Airflow
  импортируются внутри `main()`:
  - `CHILD_ENV_MARKER = "AIRFLOW_PROVIDER_RMQ_WATCHER_CHILD"`;
  - `PARENT_PID_ENV = "AIRFLOW_PROVIDER_RMQ_WATCHER_PARENT_PID"`.
- **Запуск** (`on_starting` → `_spawn_child`, только шедулер):
  - маркер в окружении → ничего не делать;
  - `self._child` жив (`poll() is None`) → WARNING, второй процесс не запускается;
  - вызов не из основного потока → WARNING (PDEATHSIG сработает при завершении этого
    потока, а не процесса), процесс всё равно запускается;
  - окружение: `os.environ` + маркер + `PARENT_PID_ENV=str(os.getpid())` +
    `PYTHONPATH` = каталог, из которого импортирован пакет
    (`Path(airflow_provider_rmq.__file__).parents[1]`), перед существующим
    `PYTHONPATH`; каталоги dags/plugins/config дочерний процесс добавляет сам при
    инициализации Airflow;
  - `subprocess.Popen([sys.executable, "-m", "airflow_provider_rmq.watcher.runner"],
    env=env, stdin=subprocess.DEVNULL)`; stdout/stderr наследуются (строки watcher'а
    идут в `docker logs` шедулера);
  - INFO с PID дочернего процесса; при исключении `Popen` — ERROR, шедулер
    продолжает работу без watcher'а.
- **Остановка** (`before_stopping` → `_stop_child`):
  - процесса нет → ничего; процесс уже завершён → `wait()` и код выхода в лог;
  - иначе `terminate()` → `wait(timeout=_CHILD_STOP_TIMEOUT)` → при `TimeoutExpired`
    WARNING, `kill()` → `wait()`; код выхода в лог.
- **Listener в дочернем процессе:**
  - `_stop_event = threading.Event()` создаётся в `__init__`;
  - `run(parent_pid: int | None = None)` — запоминает `_parent_pid`, пишет
    INFO-строку об интервале и бюджете цикла (сейчас стр. 1047-1053) и выполняет
    `_run_loop` в текущем потоке;
  - `request_stop()` — stop event + `_wake_loop()`; вызывается обычным кодом, не из
    обработчика сигнала; идемпотентен.
- **Runner** (`airflow_provider_rmq/watcher/runner.py`, `main()`):
  1. `signal.pthread_sigmask(SIG_BLOCK, {SIGTERM, SIGINT})` — первой строкой;
  2. `parent = int(os.environ[PARENT_PID_ENV])`; нет или не число → сообщение в stderr,
     `os._exit(2)`;
  3. `_set_parent_death_signal()` — `prctl(PR_SET_PDEATHSIG, SIGTERM)` через
     `ctypes.CDLL(None, use_errno=True)` на Linux; ошибка — сообщение, работа
     продолжается; не Linux — no-op;
  4. `os.getppid() != parent` → `os._exit(0)`;
  5. `import airflow`, импорт listener'а; INFO с PID процесса и PID шедулера;
  6. `listener = RMQWatcherListener()`; поток `rmq-watcher` с
     `target=listener.run, args=(parent,)`, `daemon=True`;
  7. цикл ожидания: `signal.sigtimedwait({SIGTERM, SIGINT}, 1.0)`; сигнал → INFO,
     `listener.request_stop()`, выход из цикла; поток цикла завершился сам (смена
     родителя в `_main`) → выход из цикла;
  8. `thread.join(timeout=_EXIT_JOIN_TIMEOUT)` (с запасом над `_STOP_TIMEOUT`); поток
     жив → WARNING;
  9. `logging.shutdown()`, `os._exit(0)`; исключение на шагах 5-8 → запись в лог,
     `logging.shutdown()`, `os._exit(1)`.
- **Цикл** (`_main`):
  - перед каждым проходом: `_parent_pid` задан и `os.getppid() != _parent_pid` →
    ERROR о завершении шедулера, stop event, выход;
  - после того как `_run_cycle` вернул управление — `incr("rmq_watcher.cycle_completed")`;
    при таймауте цикла счётчик не растёт.
- **`_CHILD_STOP_TIMEOUT = 5.0`** — ожидание дочернего процесса в `before_stopping`.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): изменения кода, тесты, ADR,
  документация в репозитории
- **Post-Completion** (no checkboxes): проверка на стенде с Airflow 2.9.1, выкатка на
  прод, наблюдение

## Implementation Steps

### Task 1: API цикла для дочернего процесса: `run`, `request_stop`, проверка родителя, метрика

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] `_stop_event` создаётся в `__init__`; `_start` продолжает работать (до задачи 3
      он переиспользует или пересоздаёт событие, существующие тесты остаются зелёными)
- [ ] `run(parent_pid=None)`: сохраняет `_parent_pid`, пишет INFO-строку об интервале
      и бюджете цикла, выполняет `_run_loop` в текущем потоке
- [ ] `request_stop()`: stop event + `_wake_loop()`, идемпотентен, работает до
      запуска цикла
- [ ] в `_main` перед каждым проходом — проверка `getppid()` против `_parent_pid`
      (ERROR, stop event, выход)
- [ ] после возврата `_run_cycle` — `incr("rmq_watcher.cycle_completed")`
- [ ] тесты: смена `getppid` останавливает цикл; `_parent_pid=None` родителя не
      проверяет; `request_stop()` до `run()` — `run()` сразу возвращается;
      повторный `request_stop()` безвреден
- [ ] тесты: `cycle_completed` растёт, когда `_run_cycle` вернул управление (в том
      числе если внутри шаг упал и был записан в лог), и не растёт на таймауте цикла
- [ ] run tests - must pass before next task

### Task 2: Точка входа дочернего процесса `runner`

**Files:**
- Create: `airflow_provider_rmq/watcher/runner.py`
- Create: `tests/watcher/test_runner.py`

- [ ] константы `CHILD_ENV_MARKER`, `PARENT_PID_ENV`, `_EXIT_JOIN_TIMEOUT`; на верхнем
      уровне модуля только stdlib
- [ ] `_set_parent_death_signal()` через `ctypes` (Linux; иначе no-op)
- [ ] `main()` по шагам из Technical Details: маска сигналов, PID родителя,
      PDEATHSIG, проверка `getppid`, инициализация Airflow, поток цикла, ожидание в
      `sigtimedwait`, ограниченный join, `logging.shutdown()` + `os._exit()`;
      `if __name__ == "__main__": main()`
- [ ] тесты `_set_parent_death_signal`: вызов `prctl` с `PR_SET_PDEATHSIG`/`SIGTERM`
      (замоканный `ctypes`), ошибка `prctl` — сообщение без падения, не-Linux — no-op
- [ ] тесты `main()` (замоканы `os._exit`, `sigtimedwait`, listener): маска
      выставлена до всего остального; нет или битый `PARENT_PID_ENV` → код 2; родитель
      сменился до установки → выход без запуска цикла; сигнал → `request_stop()` и
      join; поток цикла завершился сам → выход без сигнала; поток не завершился за
      `_EXIT_JOIN_TIMEOUT` → WARNING и `os._exit`; исключение при инициализации →
      код 1
- [ ] run tests - must pass before next task

### Task 3: Listener запускает и останавливает дочерний процесс вместо потока

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] `on_starting`: маркер в окружении → ничего; для шедулера — `_spawn_child()` по
      Technical Details (дубль при живом процессе — WARNING; вызов не из основного
      потока — WARNING; ошибка `Popen` — ERROR)
- [ ] `before_stopping` → `_stop_child()`: SIGTERM, `wait(_CHILD_STOP_TIMEOUT)`, затем
      SIGKILL + `wait()`, код выхода в лог
- [ ] удалить потоковую схему в шедулере: `_start`, поле `_thread`, `_JOIN_TIMEOUT` и
      ветки `before_stopping`, работавшие с потоком; докстринг класса описывает
      процессную схему
- [ ] тесты `on_starting`: аргументы `Popen`, окружение (маркер, `PARENT_PID_ENV`,
      `PYTHONPATH` с каталогом пакета перед существующим значением,
      `stdin=DEVNULL`); не-шедулер не запускает процесс; маркер блокирует запуск;
      дубль при живом процессе; вызов не из основного потока; ошибка `Popen`
- [ ] тесты `before_stopping`: выход по SIGTERM; таймаут → SIGKILL; процесс уже
      завершён (путь, по которому dumb-init остановил runner раньше шедулера);
      `before_stopping` без процесса
- [ ] удалить или переписать тесты потоковой схемы
- [ ] run tests - must pass before next task

### Task 4: Регрессионный тест — в процессе шедулера нет потоков плагина

**Files:**
- Modify: `tests/watcher/test_listener.py`

- [ ] тест: множество потоков до и после `on_starting` для шедулера (замоканный
      `Popen`) совпадает; event loop не создан; ни один пул не получил задачу
- [ ] тест: то же для последовательности `on_starting` → `before_stopping` →
      `on_starting`
- [ ] run tests - must pass before next task

### Task 5: Интеграционный тест реального процесса runner'а

**Files:**
- Create: `tests/watcher/test_runner_process.py`
- Create: `tests/watcher/runner_stub.py`

- [ ] `runner_stub.py` — тестовая точка входа: подменяет `_main` listener'а корутиной,
      которая печатает маркер старта (`flush=True`) и ждёт stop event, пишет маркер
      выхода в файл из переменной окружения перед `os._exit`, затем зовёт
      `runner.main()`
- [ ] тест: процесс стартует и печатает маркер; SIGTERM → код выхода 0 и маркер
      выхода
- [ ] тест смерти родителя: промежуточный процесс запускает stub с его PID в
      `PARENT_PID_ENV`; `kill -9` промежуточного → маркер выхода появляется
      (PDEATHSIG); завершение проверяется по файлу-маркеру, а не по `os.kill(pid, 0)`
- [ ] тест: SIGTERM, отправленный сразу после старта, до запуска цикла, не теряется
- [ ] тест: stub с маркером окружения, вызывающий `on_starting` шедулера, не порождает
      внука; то же при `AIRFLOW__CORE__LAZY_LOAD_PLUGINS=False`
- [ ] тесты помечены как Linux-only (`skipif`), таймауты не меньше 30 с на старт с
      `import airflow`
- [ ] run tests - must pass before next task

### Task 6: Комментарии и докстринги под процессную модель

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `airflow_provider_rmq/utils/executor.py`
- Modify: `airflow_provider_rmq/watcher/consumer.py`

- [ ] найти упоминания модели «поток внутри шедулера»
      (`grep -rn -i "watcher thread\|scheduler process\|loop thread\|background thread"
      airflow_provider_rmq`), в том числе `listener.py:951, 1064-1071, 1233, 1576` и
      `utils/executor.py:29, 49`
- [ ] переписать найденное в терминах текущей модели (цикл в потоке дочернего
      процесса шедулера); обоснования, которые держались на процессе шедулера
      (например, import lock у `listener.py:1576`), проверить и сформулировать заново
- [ ] run tests - must pass before next task

### Task 7: ADR — watcher в дочернем процессе

**Files:**
- Create: `docs/adr/0008-watcher-child-process.md`

- [ ] контекст: fork в процессе шедулера, потоки плагина, ссылка на
      `docs/incidents/20260923-scheduler-hang.md`
- [ ] решение: дочерний процесс через `Popen` из `on_starting` в основном потоке;
      PID шедулера в окружении, PDEATHSIG + `getppid`; сигналы через маску и
      `sigtimedwait`; выход через `os._exit`; без перезапуска, метрика
      `cycle_completed` как сигнал живости; удаление потоковой схемы
- [ ] следствия: отдельный пул соединений с метабазой; SIGKILL через 5 с обрывает
      `manager.stop()`; dumb-init присылает SIGTERM всей группе; мёртвый процесс виден
      как `<defunct>`; `close_fds=True`
- [ ] отклонённые варианты: отдельный сервис в compose (меняет установку),
      `multiprocessing` spawn (зависимость от `__main__` шедулера),
      `register_at_fork` (не защищает блокировки уровня C), пайп с EOF (fork'и
      шедулера держат его конец), обработчики сигналов (прерывают код, держащий
      блокировку), чтение PID родителя в дочернем процессе (гонка со смертью родителя)

### Task 8: Verify acceptance criteria

- [ ] в процессе шедулера после `on_starting` нет потоков, event loop и задач в пулах
      плагина
- [ ] цикл watcher'а работает в дочернем процессе и завершается по SIGTERM, по смерти
      шедулера и при SIGTERM до старта цикла
- [ ] повторный `on_starting`, ошибка `Popen`, вызов не из основного потока, пакет из
      папки plugins обработаны
- [ ] `rmq_watcher.cycle_completed` растёт на каждом проходе, дошедшем до конца, и не
      растёт на таймауте
- [ ] код не использует API Airflow новее 2.9.1
- [ ] run full test suite: `pytest tests/`
- [ ] `ruff check` и `mypy` по изменённым файлам

### Task 9: [Final] Update documentation

- [ ] `readme.md` и `readme_ru.md`: раздел «How it works» — цикл в дочернем процессе
      шедулера, процесс в дереве и `<defunct>` при его смерти, строки в логе шедулера,
      метрика `rmq_watcher.cycle_completed` (живость, а не здоровье) и правило на её
      отсутствие, отдельный пул соединений с метабазой; места, описывающие цикл внутри
      процесса шедулера (в том числе `readme.md:891, 977`)
- [ ] `CONTEXT.md`: описание Watcher — дочерний процесс шедулера
- [ ] `CHANGELOG.md`: новая секция `## v2.6.0` с изменением процессной модели и
      метрикой
- [ ] `docs/live-verification.md`: сценарий 12 (остановка и рестарт scheduler'а) —
      ожидаемое поведение дочернего процесса; сценарий смерти шедулера по `kill -9`;
      как выглядит мёртвый watcher в `docker top`
- [ ] `docs/incidents/20260923-scheduler-hang.md`: ссылка на ADR 0008 в разделе про
      решение со стороны плагина
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Проверка на стенде с Airflow 2.9.1:**
- `docker top` шедулера: процесс `python -m airflow_provider_rmq.watcher.runner` под
  `airflow scheduler`;
- `py-spy dump` процесса шедулера: только `MainThread`; у процесса runner'а — основной
  поток в `sigtimedwait` и потоки watcher'а;
- строки watcher'а видны в `docker logs` шедулера и в Loki по прежним селекторам;
- `docker restart` шедулера: старый процесс runner'а завершается, новый стартует;
- `kill -9` процесса шедулера: процесс runner'а завершается сам;
- метрика `rmq_watcher.cycle_completed` приходит в Prometheus;
- число соединений с метабазой выросло на пул runner'а и не растёт дальше.

**Прод:**
- выкатка новой версии плагина; `AIRFLOW__CELERY__SYNC_PARALLELISM` можно оставить
  как есть или вернуть по умолчанию — после выкатки fork в шедулере безопасен;
- правило алерта на отсутствие прироста `rmq_watcher.cycle_completed`;
- наблюдение за 503 шедулера по запросам из инцидента.
