# Зависание основного цикла шедулера Airflow при работающем RMQ Watcher

**Статус:** причина не установлена. Главный кандидат — дедлок после `fork()` при
отправке задач в Celery: дочерний процесс наследует захваченную блокировку буфера
`sys.stdout`, в который пишут потоки watcher'а. Механизм подтверждён по исходникам
CPython 3.10 и воспроизведён синтетически, но равноправной альтернативой остаётся
ожидание result backend без таймаута, хотя проверки на проде её ослабили. Все
условия гипотезы 1 на проде выполнены, а все фоновые потоки процесса шедулера
принадлежат плагину: без него процесс однопоточный и fork при отправке безопасен.
Различить гипотезы может только стек потоков, снятый во время зависания. Разбор
продолжается в этом репозитории.

**Среда:** сервер `airflow-server` (машина `vps257381`, 6 CPU), Airflow **2.9.1**,
Python **3.10.14**, CeleryExecutor, брокер Redis, метабаза Postgres. Каждый сервис в
своём контейнере docker compose на базе официального compose-файла Airflow, логи
контейнеров через драйвер `json-file`. Result backend — Postgres
(`AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://…` в общем окружении compose).
Пакеты в контейнере шедулера: `apache-airflow-providers-celery` **3.6.2**, celery
5.4.0, kombu 5.3.7, SQLAlchemy 1.4.52, psycopg2-binary 2.9.9, httpx 0.28.1.
В процессе шедулера работает плагин `airflow_provider_rmq` версии **v2.5.0**: поток
`rmq-watcher` с собственным event loop и пулы потоков `BoundedExecutor`
`rmq-watcher-cycle` (4) и `rmq-watcher-consumer` (32).

## Симптом

В UI висит баннер:

> The scheduler does not appear to be running. Last heartbeat was received 2 hours ago.

При этом контейнер `airflow-airflow-scheduler-1` не падает и не перезапускается сам:

- `/health` шедулера каждые 30 с отвечает **503 Service Unavailable**. Health-сервер —
  отдельный дочерний процесс (`Process(target=serve_health_check)` в
  `airflow/cli/commands/scheduler_command.py:89`), он лишь читает heartbeat из
  метабазы. Его ответы не говорят о состоянии процесса шедулера;
- основной цикл `scheduler_job_runner` не пишет в лог ни одной строки;
- поток watcher'а работает как обычно: раз в минуту `Using connection ID 'rabbitmq'`
  и `HTTP Request: GET https://mb.realcombi.mgcom.ru/api/consumers/realcom "200 OK"`,
  а также предупреждения `amqp.py:366 … missed the filter`.

Живость самого процесса шедулера доказывают строки watcher'а: процесс жив, основной
поток заблокирован, фоновые потоки работают. Лечится только ручным перезапуском
контейнера.

## Эпизоды

По логам Loki (`/health` пишется в лог с 24.08, это весь срок хранения) за 30 дней
было ровно два эпизода. Больше 503 от шедулера не встречалось.

| # | Последняя строка основного цикла | Первый 503 | Перезапуск вручную | Простой |
|---|---|---|---|---|
| 1 | 19.09 03:27:49.652 UTC | 03:28:34 | 07:19:53 (SIGTERM) | ~3,9 ч |
| 2 | 23.09 05:00:50.037 UTC | 05:01:33 | 07:58:15 (SIGTERM) | ~3 ч |

Аптайм процесса перед эпизодом: для первого — меньше суток (18.09 шедулер
перезапускался трижды), для второго — ~4 суток (с перезапуска 19.09 07:19).

Метрика `airflow_scheduler_heartbeat` (StatsD → Prometheus) показывает те же два
провала: счётчик остановился на 24205 и 28473, затем серия пропала. Gauge очереди
executor'а застыл на 3 и 9, но он снимается до отправки и места остановки не
уточняет. Провалы 30.08 и 31.08 к делу не относятся: в те же окна пропадал и
`node_load1` этого хоста, то есть терялись метрики, а шедулер работал (503 нет).
С 31.08 по 14.09 метрик с хоста нет совсем, так что за этот период опираться можно
только на логи.

## Где именно встаёт цикл

Оба раза последняя строка основного цикла — хвост пачки отправки задач в Celery:

```
{scheduler_job_runner.py:639} INFO - Sending TaskInstanceKey(...) to executor with priority … and queue default
{base_executor.py:149} INFO - Adding to queue: ['airflow', 'tasks', 'run', …]
```

Отправка при этом **доходит до брокера целиком**: воркер пишет `execute_command[…] received`
на каждую задачу.

| Эпизод | `Adding to queue` в последней пачке | `received` у воркера | Время `received` |
|---|---|---|---|
| 19.09 | 3 (03:27:49.652) | 3 | 49.730–49.752 |
| 23.09 | 9 (05:00:50.031–.037) | 9 | 50.156–50.234 |

В нормальной итерации (например, 23.09 05:00:31) через ~0,3 с после отправки идут
`Received executor event with state queued` и `Setting external_id`. В обоих
эпизодах этих строк нет.

В Airflow 2.9.1 порядок такой (`scheduler_job_runner.py:977-979`):
`executor.heartbeat()` → `_process_executor_events()`, и первое, что делает
`_process_executor_events`, — пишет `Received executor event` (стр. 689), ещё до
обращения к БД. Значит, цикл стоит **внутри `executor.heartbeat()`**. На уровне INFO
этот вызов после `Adding to queue` при успехе ничего не пишет. Внутри него
(celery-провайдер 3.6.2):

1. `_send_tasks_to_celery` (`celery_executor.py:329-345`): для пачки больше одной
   задачи — `ProcessPoolExecutor`, на Linux это `fork()`; выход из `with` ждёт
   завершения всех дочерних процессов без таймаута;
2. для каждой отправленной задачи — `result.state` и `update_task_state`
   (`celery_executor.py:326-327`): запрос к result backend;
3. `sync()` → `BulkStateFetcher.get_many()` → запрос к result backend
   (`celery_executor_utils.py:273-290`) без обёртки `OPERATION_TIMEOUT`.

Доставка задач в брокер исключает зависание до публикации, но не различает эти три
места.

## Совпадение с циклом watcher'а

Ежеминутная итерация watcher'а (чтение коннекшена `rabbitmq` из метабазы и
HTTP-запрос к management API) оба раза попала в ~100 мс от остановки:

| Эпизод | Последний `Adding to queue` | watcher: `get_connection('rabbitmq')` | watcher: HTTP 200 |
|---|---|---|---|
| 19.09 | 03:27:49.652 | 03:27:49.683 | 03:27:49.739 |
| 23.09 | 05:00:50.037 | 05:00:49.996 | 05:00:50.093 |

Само по себе совпадение слабое. На полных контрольных сутках без зависаний:

| Дата (UTC) | Версия | Задач | Пачек | Пачек > 1 задачи | Из них в ±100 мс от строки watcher'а |
|---|---|---:|---:|---:|---:|
| 03.09 | v2.4.0 | 13 060 | 9 176 | 2 550 | 11 |
| 22.09 | v2.5.0 | 13 042 | 8 734 | 2 807 | 13 |

То есть такие совпадения случаются около десятка раз в сутки и обычно проходят без
последствий. Примеры: 05.09 02:02:29.596 (v2.4.0) — две задачи, HTTP-строка watcher'а
в .598, события executor'а в .745; 23.09 04:53:47.955 (v2.5.0) — две задачи,
`get_connection` в 48.024, HTTP в 48.075, события в 48.170. Если причина связана с
watcher'ом, опасное окно много уже 100 мс.

Совпадение также не различает гипотезы: и дедлок после fork, и общий с watcher'ом
ресурс одинаково предсказывают, что остановка придётся на его активность.

Место в коде, откуда идут строки watcher'а:

- `airflow_provider_rmq/watcher/consumer.py:1955` —
  `pool.run(BaseHook.get_connection, conn_id)` в пуле `BoundedExecutor`
  (отсюда `base.py:84 Using connection ID 'rabbitmq'`);
- `airflow_provider_rmq/watcher/consumer.py:2952` — `_probe_consumers`, вызывает
  `get_queue_consumers` (`airflow_provider_rmq/utils/management.py:52`) через
  `httpx.AsyncClient` (отсюда `_client.py:1740 HTTP Request: GET …/api/consumers/realcom`).

Обе строки уходят в `sys.stdout` из фоновых потоков: консольный handler шедулера —
`RedirectStdHandler` (`airflow/config_templates/airflow_local_settings.py:95-97`,
`airflow/utils/log/logging_mixin.py:218-254`), обычный `StreamHandler`, который после
каждой записи делает `flush`.

## Связь с версией плагина

По номеру строки в `RMQWatcherListener.on_starting` видно, какая версия грузилась
при каждом старте шедулера:

| Старт шедулера | `listener.py:<N>` | Версия |
|---|---|---|
| 25.08, 28.08 | 544 | до v2.4.0 |
| 02.09 | 665 | v2.4.0 |
| 14.09 15:38 | 986 | v2.5.0 |
| 18.09 (×3), 19.09, 23.09 | 986 | v2.5.0 |

Оба зависания пришлись на v2.5.0. На v2.4.0 (~12 суток, часть времени без метрик) и
раньше зависаний по логам нет. При двух событиях это корреляция, а не доказательство.

Ежеминутный цикл watcher'а с `get_connection` и `GET /api/consumers`, пулы
`BoundedExecutor`, watchdog консьюмеров и пробы брокера есть уже в v2.4.0 (коммит
`2b0c56c` входит в оба тега). Поток `rmq-watcher` с event loop, чтением коннекшена в
executor'е и `httpx.AsyncClient` есть с v2.3.0. `git diff v2.4.0 v2.5.0 --
airflow_provider_rmq/watcher/consumer.py` — 47 строк логики промахов фильтра.
В v2.5.0 появились `ack_unmatched`, состояние промахов фильтра и новые записи в лог
из потоков watcher'а, в том числе WARNING `missed the filter`
(`airflow_provider_rmq/utils/amqp.py:366`); `listener.py` вырос с 1203 до 1997 строк.

Нагрузка на отправку в обоих периодах одинакова (таблица выше: ~13 тыс. задач и
~2,5–2,8 тыс. многозадачных пачек в сутки), так что разница между версиями ею не
объясняется.

## Гипотезы

### 1. Дедлок после `fork()` на блокировке буфера `sys.stdout` (основная)

**Условия на проде** (`/opt/airflow/config/airflow.cfg`, подключён через
`AIRFLOW_CONFIG`):

| Параметр | Значение | Следствие |
|---|---|---|
| `[celery] sync_parallelism` | `0` | `max(1, cpu_count() - 1)` (`celery_executor.py:261-263`); `cpu_count()` видит 6 CPU хоста, пул отправки — до 5 процессов |
| `[core] mp_start_method` | не задан | `mp_context` не передаётся, на Linux это `fork` |
| `[celery] operation_timeout` | `1.0` | `apply_async` в дочернем процессе прерывается через 1 с |
| `[logging] logging_config_class` | пусто | стандартная конфигурация: консольный `RedirectStdHandler` пишет в `sys.stdout` |
| `[core] parallelism` / `[scheduler] max_tis_per_query` | `32` / `16` | пачки отправки до 16 задач |

В контейнере шедулера Python 3.10.14, `PYTHONUNBUFFERED` не задан, буфер
`sys.stdout` — `BufferedWriter` (см. «Проверено на проде»). Все условия гипотезы
выполнены.

**Механизм (CPython 3.10).**

1. `fork()` копирует в дочерний процесс только вызывающий поток, а все блокировки —
   в том состоянии, в каком они были в момент fork.
2. `BufferedWriter` у `sys.stdout` хранит блокировку и идентификатор потока-владельца
   без проверки PID (`Modules/_io/bufferedio.c:269-311`). После fork эта блокировка не
   переинициализируется; `logging` переинициализирует только собственные блокировки
   handler'ов (`Lib/logging/__init__.py:254-264`).
3. Родитель сбрасывает stdout перед fork (`multiprocessing/popen_fork.py:16`), но не
   удерживает блокировку до самого `os.fork()` (`popen_fork.py:66`). Если между этим
   сбросом и fork фоновый поток watcher'а или его пула начал писать строку лога, в
   дочерний процесс попадает захваченная блокировка, владельца которой там нет.
4. Дочерний процесс спокойно отправляет задачу в брокер (`send_task_to_executor`
   блокировку stdout не трогает) и отдаёт результат родителю.
5. На выходе `BaseProcess._bootstrap` вызывает `util._flush_std_streams()`
   (`multiprocessing/process.py:334`) → `sys.stdout.flush()` → ждёт блокировку вечно.
   `os._exit()` (`popen_fork.py:73`) до этого не доходит.
6. Родитель, уже получивший все результаты, висит в `ProcessPoolExecutor.shutdown(wait=True)`
   → join manager-потока (`concurrent/futures/process.py:780`) → join каждого
   дочернего процесса (`:526`), без таймаута.

**С фактами сходится:** все задачи дошли до воркера, а обработки событий нет;
срабатывает редко — нужно попасть записью в лог в окно между сбросом и fork; процессы
пула стартуют до передачи им заданий (`concurrent/futures/process.py:674-705`),
поэтому значима запись, начатая до первых `received`: 19.09 это `get_connection`
(HTTP-строка в 49.739 появилась уже после первого `received` в 49.730), 23.09 —
HTTP-строка.

**Воспроизведение.** Синтетические скрипты (фоновый поток пишет в stdout-пайп,
основной гоняет `ProcessPoolExecutor`) зависают на Python 3.8, 3.10 и 3.12: все
результаты получены, родитель в `shutdown`, дочерние процессы в
`_flush_std_streams` ← `_bootstrap`. С небуферизованным stdout (`PYTHONUNBUFFERED=1`)
зависаний нет за тысячи итераций; `write_through=True` у `TextIOWrapper` не спасает,
нужен именно stdout без `BufferedWriter`. Окно в части прогонов расширено
искусственно, так что частоту на проде это не оценивает.

**Слабые места.**

- Совпадения записей watcher'а с многозадачной пачкой случаются около десятка раз в
  сутки без последствий. Это согласуется с узким окном «сброс → fork», но и не
  подтверждает его.
- Не объяснено, почему нет эпизодов на v2.4.0 при той же нагрузке и том же цикле
  watcher'а. Возможные ответы: случайность при двух событиях или дополнительные
  записи в лог из потоков watcher'а в v2.5.0.
- Кроме сброса stdout, дочерний процесс перед `os._exit()` выполняет
  `threading._shutdown()` и `multiprocessing.util._exit_function()`
  (`multiprocessing/process.py:317,332`), а они запускают очистку `ThreadPoolExecutor`
  (`concurrent/futures/thread.py:23-43`). Другая унаследованная блокировка на выходе
  не исключена, хотя лучшего кандидата не найдено. Аналогичная блокировка есть у
  stderr.

### 2. Ожидание result backend без таймаута

После отправки heartbeat обращается к result backend: `result.state` для каждой
задачи (`celery_executor.py:327`) и `sync()` → `mget` или SQL-запрос
(`celery_executor_utils.py:276,284-287`), без `OPERATION_TIMEOUT`. Result backend на
Postgres (`db+postgresql://…`), `result_backend_sqlalchemy_engine_options` пуст, то
есть собственных таймаутов у движка backend'а нет. Зависшее соединение или
блокировка остановит цикл в том же месте, и логи этого не отличат от гипотезы 1.
Совпадение с watcher'ом эта гипотеза не объясняет, но при около десятке безопасных
совпадений в сутки его объяснять и не обязательно.

Против этой гипотезы:

- Postgres всё время простоя обслуживал других клиентов: воркер выполнял задачи и
  записывал их результаты в ту же таблицу `celery_taskmeta`. Чтение из неё может
  ждать часами только при удерживаемой `ACCESS EXCLUSIVE`-блокировке, а она
  остановила бы и запись результатов воркером.
- libpq по умолчанию включает TCP keepalive; при системных значениях по умолчанию
  (`tcp_keepalive_time` 7200 с, 9 проб по 75 с) мёртвое соединение обнаруживается
  примерно через 2 ч 11 мин и завершается ошибкой. Эпизод 2 длился ~3 ч, эпизод 1 —
  ~3,9 ч без восстановления. В контейнере шедулера значения стандартные:
  `tcp_keepalive_time=7200`, `tcp_keepalive_intvl=75`, `tcp_keepalive_probes=9`.

Остаётся зависание на стороне клиента, не связанное с сервером; без стека его не
исключить.

### 3. Основной поток блокируется ресурсом, общим с watcher'ом

Watcher ходит в метабазу через тот же движок SQLAlchemy, что и основной цикл
(`airflow_provider_rmq/watcher/models.py:91-96`: `sessionmaker(bind=airflow.settings.engine)`),
плюс общие логирование и `StatsD`-клиент. Watcher на мгновение захватывает общий
ресурс и не отпускает его (или теряет соединение), а основной поток ждёт без таймаута.
Эта гипотеза хуже объясняет, почему шедулер застревает именно после отправки в
Celery, но исключать её без стека нельзя.

### Что опровергнуто

- **Хуки listener'а в основном потоке.** `RMQWatcherListener` реализует только
  `on_starting` и `before_stopping` (`airflow_provider_rmq/watcher/listener.py:975,995`);
  в цикл планирования и обработку событий executor'а плагин не встраивается.
- **Захват GIL watcher'ом.** Watcher продолжает писать в лог каждую минуту, значит
  GIL не удерживается постоянно.
- **Блокировка на Python-уровне во время самой отправки.** `send_task_to_executor`
  оборачивает `apply_async` в `timeout(OPERATION_TIMEOUT)` (SIGALRM,
  `celery_executor_utils.py:215`); такое зависание прервалось бы с
  `Task Timeout Error` в логе. К тому же все задачи дошли до брокера.

### Что исключено

- **Redis/брокер:** задачи доставлены, Redis в эти минуты штатно делает `BGSAVE`.
- **Воркер:** принял и выполнял задачи.
- **Postgres как источник блокировок dag_run:** `could not obtain lock on row in
  relation "dag_run" … FOR UPDATE NOWAIT` идут постоянно и в штатное время. Это
  нормальный шум Airflow, который проверяет, не обрабатывает ли dag_run другой
  процесс. Состояние соединения шедулера во время простоя (`pg_stat_activity`,
  `pg_locks`) не снималось, так что Postgres как result backend (гипотеза 2) этим не
  исключён.
- **Падение процесса, OOM, segfault:** контейнер не перезапускался, watcher
  продолжал работать. В журнале ядра хоста (`journalctl -k`, хранится с 08.06) в
  окнах 19.09 03:20–03:40 UTC и 23.09 04:55–05:10 UTC записей нет, `dmesg` не
  содержит OOM, segfault и hung_task.

## Проверено на проде (23.09)

Условия гипотезы 1 в контейнере шедулера:

```bash
C=airflow-airflow-scheduler-1
docker exec $C python --version                                        # Python 3.10.14
docker exec $C env | grep PYTHONUNBUFFERED                             # пусто
docker exec $C python -c "import sys; print(type(sys.stdout.buffer).__name__)"  # BufferedWriter
```

`docker exec` получает то же окружение контейнера (образ и compose), что и процесс
шедулера, а его stdout — тоже пайп. Вместе с конфигурацией (см. «Условия на проде»)
это значит, что все условия гипотезы 1 выполнены.

### Потоки процесса шедулера

Дерево процессов контейнера (`docker top $C -eo pid,ppid,comm,args`):

| Процесс | Роль |
|---|---|
| `dumb-init -- /entrypoint scheduler` | init контейнера |
| `airflow scheduler`, дочерний у `dumb-init` | **процесс шедулера**, основной цикл |
| `airflow scheduler`, дочерний у шедулера, старт вместе с ним | health-сервер |
| `airflow scheduler -- DagFileProcessorManager` | менеджер разбора DAG'ов, fork при старте шедулера (`scheduler_job_runner.py:839`), однопоточный |
| `airflow scheduler - DagFileProcessor <файл>` | разбор файла DAG'а, дочерний у менеджера |

Стек здорового процесса шедулера (`py-spy dump --pid <PID шедулера>`) — 7 потоков,
столько же задач в `/proc/<pid>/task`, то есть других нативных потоков нет:

| Поток | Чей |
|---|---|
| `MainThread` | цикл шедулера (`_run_scheduler_loop`, `scheduler_job_runner.py:998`) |
| `rmq-watcher` | event loop watcher'а (`airflow_provider_rmq/watcher/listener.py:1060`) |
| `rmq-watcher-cycle_0`, `rmq-watcher-cycle_1` | пул `BoundedExecutor` цикла watcher'а |
| `rmq-watcher-consumer_0`, `rmq-watcher-consumer_1` | пул `BoundedExecutor` консьюмеров |
| `asyncio_0` | стандартный executor event loop'а watcher'а (DNS-резолв через `loop.getaddrinfo`); в процессе шедулера нет другого event loop |

Все фоновые потоки процесса шедулера принадлежат плагину. Без него процесс
однопоточный, и `fork()` в `_send_tasks_to_celery` безопасен. Плагин — необходимое
условие гипотез 1 и 3; сам дефект — `fork()` в многопоточном процессе на стороне
celery-провайдера Airflow, но на этом сервере многопоточным процесс шедулера делает
только плагин.

## Что снять при следующем зависании (до перезапуска!)

Без стеков причину не доказать. Порядок действий на хосте, от root:

```bash
# 1. Дерево процессов: висят ли под шедулером дочерние процессы пула
docker top airflow-airflow-scheduler-1 -eo pid,ppid,comm,args

# 2. Где ждёт каждый поток шедулера и каждый дочерний процесс (без ptrace)
ps -L -p "$SCHEDULER_HOST_PID" -o pid,tid,comm,wchan:32
cat /proc/"$CHILD_HOST_PID"/wchan

# 3. Стеки родителя и дочерних процессов
py-spy dump --pid "$SCHEDULER_HOST_PID"
py-spy dump --native --pid "$CHILD_HOST_PID"
```

PID шедулера — процесс `airflow scheduler`, у которого `ppid` равен PID `dumb-init`;
не `DagFileProcessorManager` и не PID 1 контейнера. Дочерние процессы пула отправки
выглядят в `docker top` так же, как health-сервер (`airflow scheduler`, дочерний у
шедулера), но у них свежие PID и они появляются только в момент отправки; в
зависании их будет несколько (до 5). Для сравнения есть стек здорового шедулера
(см. «Потоки процесса шедулера»). `py-spy` с хоста работает по host-PID и не требует
`cap_add: [SYS_PTRACE]` в compose; добавление capability пересоздаёт контейнер и
стирает улики, поэтому делать это можно только заранее.

Запасной способ без py-spy: `docker exec airflow-airflow-scheduler-1 kill -QUIT <pid>`.
В 2.9.1 шедулер ставит обработчик `SIGQUIT`, печатающий стеки всех потоков
(`airflow/cli/commands/daemon_utils.py:84`, `airflow/utils/cli.py:317-332`). Он пишет
в stdout, поэтому, если завис сам stdout, дамп может не выйти. `SIGUSR2` печатает
состояние executor'а, а не стеки.

Как читать результат:

- родитель стоит в `concurrent/futures/process.py` `shutdown`/`join` внутри
  `_send_tasks_to_celery`, а дочерний процесс — в `_flush_std_streams` /
  `_enter_buffered_busy` — подтверждена гипотеза 1; стек с `--native` покажет, чья
  блокировка, если дочерний процесс стоит где-то ещё;
- родитель в `update_task_state`, `get_many`, SQL или чтении сокета, зависших
  дочерних процессов пула нет — гипотеза 2; дальше `pg_stat_activity`/`pg_locks` по
  соединению шедулера;
- родитель на `acquire()` / `pool.connect` вне этих мест — гипотеза 3, по стеку потока
  `rmq-watcher` и пулов видно, кто держит ресурс.

## Как проверить без ожидания следующего эпизода

- **Отключить fork в отправке:** `AIRFLOW__CELERY__SYNC_PARALLELISM=1` в окружении
  шедулера, тогда отправка идёт последовательно в том же процессе. Опрос состояний
  с Postgres result backend идёт через `_get_many_from_db_backend` без пула
  процессов, так что при этом значении CeleryExecutor не делает fork вообще.
  Самый широкий обход риска fork при отправке. Влияние на скорость отправки
  проверить: на пачках из единиц задач последовательная отправка может оказаться
  не медленнее fork большого процесса.
- **Отключить буфер stdout:** `PYTHONUNBUFFERED=1` в окружении шедулера. Точечный
  обход варианта с блокировкой буфера stdout; другие риски fork он не снимает.
- Для причинного эксперимента эти настройки включать **по одной**, иначе результат
  не различит гипотезы. Если важнее надёжность, чем доказательство, допустимы обе.
  Несколько недель без повторения будут аргументом в пользу обхода, но не
  доказательством конкретной блокировки.
- **Воспроизвести на стенде:** шедулер с v2.5.0 на Python 3.10, `sync_parallelism` > 1,
  stdout в буферизованный пайп, как в docker, без `PYTHONUNBUFFERED`; DAG, который
  раз в секунду ставит пачки задач; ускоренный цикл watcher'а
  (`RECONCILE_INTERVAL_VAR` поменьше), чтобы участить совпадения.
- **Откат на v2.4.0** не проверяет гипотезу 1: весь ежеминутный цикл watcher'а там
  тот же. Откат проверяет только совокупность изменений v2.5.0. DAG'и с
  `ack_unmatched=` на v2.4.0 не распарсятся.

Со стороны плагина: снижение объёма логирования из потоков watcher'а лишь уменьшает
вероятность, `QueueHandler` с фоновым writer'ом гонку не устраняет, а подменять из
плагина stdout, start method или чужие at-fork колбэки нельзя — плагин не
контролирует записи Airflow, httpx и сторонние блокировки. Стратегическое решение —
запускать watcher отдельным процессом через spawn/exec, чтобы в процессе шедулера
не было его потоков. Дешёвая мера — предупреждение в логе при старте watcher'а,
если отправка в Celery использует fork-пул.

Стратегическое решение принято в [ADR 0008](../adr/0008-watcher-child-process.md)
и вошло в v2.6.0: цикл watcher'а работает в дочернем процессе шедулера
`python -c <bootstrap> airflow_provider_rmq.watcher.runner`, который listener запускает через
`subprocess.Popen` (fork и exec в C, без Python-кода между ними). В процессе
шедулера у плагина нет ни потоков, ни event loop, ни пулов, и условие гипотез 1 и 3
со стороны плагина снято. Гипотезу 2 это решение не затрагивает.

## Страховка на время разбора

Встроенный health-сервер отдаёт 503 при зависании, и healthcheck на нём настроен в
compose: `curl --fail http://localhost:8974/health` раз в 30 с. Политика
`restart: always` перезапускает контейнер только при завершении процесса, а статус
`unhealthy` на неё не влияет — поэтому зависший шедулер стоит часами. Для
автоматического перезапуска нужен `autoheal` или аналог. Перезапуск сократит простой с часов до минут, но причину не
лечит и стирает улики. Поэтому скрипт, который перезапускает контейнер, должен
сначала сохранить дамп: `docker top`, `wchan` по всем потокам и дочерним процессам,
`py-spy dump` с хоста.

## Где смотреть данные

Логи и метрики лежат в observability-stack (скилл `observability`, Loki/Prometheus).

```bash
# провалы heartbeat
sum(increase(airflow_scheduler_heartbeat[5m]))

# число CPU хоста
count(count by (cpu) (node_cpu_seconds_total{host="airflow-server"}))

# эпизоды зависания по часам
{host="airflow-server",container_name="airflow-airflow-scheduler-1"} |= "code 503"

# основной цикл без шума health-сервера и watcher'а
{host="airflow-server",container_name="airflow-airflow-scheduler-1"}
  !~ "GET /health|code 503|consumers/realcom|Using connection ID .rabbitmq.|amqp.py"

# пачки отправки и строки watcher'а для подсчёта совпадений; граница пачки —
# "Setting the following tasks to queued state", размер — число следующих "Adding to queue"
{host="airflow-server",container_name="airflow-airflow-scheduler-1"}
  |~ "Adding to queue:|Setting the following tasks to queued state:|Using connection ID .rabbitmq.|HTTP Request: GET .*api/consumers|missed the filter"

# доставка задач воркеру
{host="airflow-server",container_name="airflow-airflow-worker-1"} |= "] received"

# версия плагина при старте шедулера
{host="airflow-server",container_name="airflow-airflow-scheduler-1"} |= "on_starting: component=Job"
```

Через REST API (403) недоступны список провайдеров и конфиг; версия
celery-провайдера получена от эксплуатации, конфиг — из
`/opt/airflow/config/airflow.cfg` на сервере.
