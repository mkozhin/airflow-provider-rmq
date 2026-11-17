---
worth: yes
where: airflow_provider_rmq/watcher/plugin.py:262
added: 2026-09-02
---
# `on_load` плагина делает DDL-проверку в каждом task-процессе

**Контекст.** `RMQWatcherPlugin.on_load` (`airflow_provider_rmq/watcher/plugin.py:261-267`)
без всяких условий вызывает `ensure_table_exists()` (`models.py:208-222`):
`create_all(checkfirst=True)` по двум таблицам плюс `inspect(engine).get_columns`
по каждой и `ALTER TABLE` при расхождении. Пишет в эти таблицы только процесс
watcher'а — дочерний процесс шедулера (`airflow_provider_rmq.watcher.runner`).
Самому шедулеру они не нужны, webserver их только читает. Таблицу
`rmq_watcher_heartbeat` `on_load` не трогает: её создают процесс watcher'а и
webserver, когда она им впервые нужна (ADR 0012). Но `on_load` — хук
загрузки плагина, а плагины грузит каждый процесс Airflow.

Цепочка проверена на Airflow 2.11.2:

- `airflow tasks run` → `task_command.py:452`
  `get_listener_manager().hook.on_starting(...)`;
- `get_listener_manager()` → `integrate_listener_plugins()`
  (`listeners/listener.py:74`) → `ensure_plugins_loaded()`
  (`plugins_manager.py:588`);
- `register_plugin()` → `plugin_instance.on_load()` (`plugins_manager.py:242`).

То есть каждый запуск каждой задачи, на каждом воркере, начинается с
`create_all` и инспекции таблиц watcher'а. То же в triggerer'е, в standalone
dag-processor'е, в webserver'е и в самом шедулере.

То же и в процессе watcher'а. `_prepare_airflow` в runner'е вызывает
`plugins_manager.ensure_plugins_loaded` (`runner.py:167-176`, `339-343`), и
`on_load` выполняет DDL в основном потоке, пока процесс готовит Airflow. У этой
подготовки нет своего лимита времени (`runner.py:52-53`), поэтому `ALTER TABLE`,
вставший в очередь за ACCESS EXCLUSIVE блокировкой, задерживает старт петли,
пока блокировку не отпустят. Под `_MIGRATION_TIMEOUT` и отложенными повторами
`_ensure_schema` (`listener.py:1329-1367`) этот вызов не попадает: тот же
`ensure_table_exists` там идёт в пуле цикла под собственным таймаутом.

**Что это стоит.**

- Четыре–шесть запросов к метабазе на старт каждой задачи, прежде чем задача
  сделала что-либо своё. На установке с тысячами задач в час это тысячи лишних
  обращений в час к таблицам, которые задачам не нужны.
- Пользователю БД, под которым работают воркеры, нужны права на `CREATE TABLE`
  и `ALTER TABLE`. Без них `on_load` ловит исключение и пишет
  `log.exception` — в лог **каждой** задачи, на каждом старте.
- В каждый task-процесс импортируются `aio_pika`, `httpx`, `consumer.py` со
  всеми зависимостями: `plugin.py` тянет `listener.py`, тот — `consumer.py`.
  Это время импорта и память на процесс, ради кода, который в задаче не
  исполняется. Импорты стоят на верхнем уровне `plugin.py` (строки 11 и 18);
  модуль баннера `dashboard_alert` `plugin.py` импортирует лениво, в колбэке
  блюпринта, который выполняется только в webserver'е.
- Одновременный `create_all` из нескольких процессов при первом старте
  (webserver, шедулер, воркеры разом) — гонка `checkfirst`, которая на Postgres
  даёт `relation already exists` у проигравшего; ловится тем же `except`, но
  шумит.

**Почему это важно.** Проблема без симптома в UI, но с ценой на каждой задаче и
с требованием к правам, о котором README не говорит. Первое, что увидит
администратор с раздельными ролями БД, — трейсбек про `CREATE TABLE` в логе
каждой задачи.

**Направления решения.**

1. *`on_load` ничего не делает с базой.* Миграция и так повторяется в петле
   watcher'а на каждом цикле, пока схема не готова (`_ensure_schema`,
   `listener.py:1329`), — это единственное место, где она нужна и где она уже
   устойчива к недоступной и к заблокированной базе. Таблицы `WatcherBase`
   webserver'у создавать не нужно: он их только читает; таблицу heartbeat он
   создаёт сам при первом чтении живости, без `on_load`.
2. *Ленивые импорты в `plugin.py`.* `RMQWatcherListener` и `RMQWatcherView`
   импортировать по месту, чтобы task-процесс не поднимал `aio_pika` и
   `httpx`. Плагин при этом остаётся зарегистрированным, а его тяжёлая часть
   загружается там, где исполняется.

Предусловие пункта 1 выполнено: вью переживает отсутствие таблиц.
`subscriptions()` (`views.py:551-583`) читает `RMQSubscription` под `try`, на
отказе откатывает транзакцию и отдаёт страницу с заметкой про схему на месте
блока подписок, а блок Connections — со своей заметкой. Webserver стоит и на
базе, в которой таблиц ещё нет.

Если `on_load` всё же должен что-то делать, выходить сразу везде, кроме
процесса watcher'а: его `on_starting` узнаёт по `_runner.CHILD_ENV_MARKER` в
окружении (`listener.py:526`). Тест: `on_load` не обращается к движку.
Существующий тест `on_load` проверяет только то, что исключение из
`ensure_table_exists` не всплывает (`tests/watcher/test_plugin.py:63`).
