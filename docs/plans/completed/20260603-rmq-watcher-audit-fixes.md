# RMQ Watcher Audit Fixes (v2.0.6)

## Overview

Аудит проекта выявил баги в плагине RMQ Watcher. Часть из них уже исправлена в коммитах
(`@hookimpl` добавлен, CSRF-токены исправлены, DagBag заменён на AST).
Этот план фиксирует оставшиеся реальные баги за один релиз v2.0.6.

## Context (from discovery)

- **Файлы**: `airflow_provider_rmq/watcher/listener.py`, `consumer.py`, `models.py`, `views.py`, `plugin.py`
- **Тесты**: `tests/watcher/test_listener.py`, `test_consumer.py`, `test_models.py`, `test_views.py`, `test_plugin.py`
- **Паттерн**: каждый модуль покрыт тестами, используются `pytest` + `pytest-mock`
- **Команда тестов**: `pytest tests/` (из корня проекта)
- **Текущая версия**: 2.0.5 (теги: v2.0.4, v2.0.5 уже на GitHub)

## Development Approach

- **Testing approach**: Regular (код → тесты)
- Каждую задачу выполнять полностью перед переходом к следующей
- Тесты обязательны, охватывают success и error сценарии
- Все тесты должны проходить до перехода к следующей задаче

## Progress Tracking

- `[x]` — задача выполнена
- `➕` — новая задача обнаружена в процессе
- `⚠️` — блокер

## Баги (подтверждены против текущего HEAD)

| ID | Файл | Критичность | Суть | Статус |
|---|---|---|---|---|
| L0 | listener.py | ~~CRITICAL~~ | `@hookimpl` | ✅ уже в коде |
| L2 | listener.py | HIGH | Двойной `on_starting` → два параллельных цикла | ❌ не исправлен |
| L3 | listener.py | HIGH | Крэш за пределами внутреннего try → поток мёртв | ❌ не исправлен |
| M2 | models.py | CRITICAL | `upsert_subscription` перетирает `enabled` из UI | ❌ не исправлен |
| C5 | consumer.py | HIGH | Бинарное тело → UnicodeDecodeError → бесконечный redelivery | ❌ не исправлен |
| V1 | views.py | LOW | `is_dag_file` не передаётся в `create` при ошибке — не краш (Jinja2 default Undefined → False), но явная передача делает код предсказуемым | ❌ не исправлен |
| P1 | plugin.py | HIGH | `ensure_table_exists` без try/except в `on_load` | ❌ не исправлен |
| C3 | consumer.py | MEDIUM | `consumer_status` не сбрасывается при отключении подписки | ❌ не исправлен |
| C4 | consumer.py | LOW | `datetime.utcnow()` deprecated в Python 3.12+ | ❌ не исправлен |
| PY1 | pyproject.toml | ~~LOW~~ | tenacity — `hooks/rmq.py` его использует, удалять нельзя | ✅ не баг |
| DagBag | test_listener.py | LOW | Стейл-тест патчит DagBag, которого нет в коде | ❌ стейл |

---

## Implementation Steps

### Task 1: Fix listener.py — защита от двойного запуска и перезапуск при крэше

Адресует баги: **L2** (HIGH), **L3** (HIGH)

> **Контекст кода**: `_main()` (строки 131-158) уже содержит `while not self._stop_event.is_set()`
> с внутренним `try/except` для каждого reconcile-цикла. `_run_loop()` вызывает `_main()` один раз —
> если `_main()` выйдет из-за исключения вне внутреннего try (например, крэш asyncio loop, ошибка
> в `_manager.start()`), поток завершится без перезапуска.

**Файлы:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

**Изменения:**

- [x] Убедиться что `from airflow.listeners import hookimpl` присутствует в импортах и `@hookimpl` стоит на `on_starting` и `before_stopping` — эти изменения уже в рабочем дереве (не закоммичены), войдут в коммит этой задачи
- [x] В `_start()` (строка 112) добавить улучшенный guard — два случая: поток жив и не остановлен (дубль — игнорировать), поток жив но `_stop_event` уже выставлен (предыдущий lifecycle завершается — дождаться и стартовать заново):
  ```python
  if self._thread is not None and self._thread.is_alive():
      if self._stop_event is None or not self._stop_event.is_set():
          log.warning("RMQ Watcher thread already running — ignoring duplicate on_starting")
          return
      # Previous lifecycle is shutting down — wait briefly then start fresh
      log.info("RMQ Watcher: waiting for previous thread to stop...")
      self._thread.join(timeout=10)
  ```
- [x] Переписать `_run_loop()` (строки 121-129): обернуть вызов `_main()` в `while not self._stop_event.is_set()` с `_stop_event.wait(timeout=30)` после исключения и `log.info("RMQ Watcher loop stopped")` при выходе. Каждая итерация создаёт новый event loop, создаёт новый `self._manager`.
  ```python
  def _run_loop(self) -> None:
      while not self._stop_event.is_set():
          loop = asyncio.new_event_loop()
          asyncio.set_event_loop(loop)
          try:
              loop.run_until_complete(self._main())
          except Exception:
              log.exception("RMQ Watcher loop crashed — restarting in 30s")
          finally:
              loop.close()
          if not self._stop_event.is_set():
              self._stop_event.wait(timeout=30)
      log.info("RMQ Watcher loop stopped")
  ```
- [x] Убедиться что `_main()` при выходе через `finally` вызывает `await self._manager.stop()` — это уже есть в строке 158, проверить что `self._manager` не будет `None` при повторном входе в `_main()`
- [x] Написать тест: двойной вызов `listener.on_starting(component)` — `_start()` вызывается дважды, но только один поток создаётся (проверить через `threading.enumerate()` или mock `threading.Thread`)
- [x] Написать тест: `_run_loop` перезапускается после исключения. Ключевой момент: `_run_loop` вызывает `_stop_event.wait(timeout=30)` между итерациями — чтобы тест не висел 30 секунд, `_stop_event` нужно выставить до второго вызова `_main`. Реализовать через `side_effect`: первый вызов `_main` → `raise Exception("crash")`, второй вызов → `self._stop_event.set()` → return нормально. Тогда `wait(30)` мгновенно возвращает (событие уже выставлено). Проверить что `_main` был вызван дважды.
- [x] Очистить стейл-тест `test_extract_subscriptions_returns_empty_list_on_dagbag_error`: убрать патч `airflow.models.DagBag` (не используется в коде), переписать тест как проверку что несуществующий файл → OSError → возвращает `[]`
- [x] `pytest tests/watcher/test_listener.py` — все тесты проходят

---

### Task 2: Fix models.py — сохранение enabled при dag_file reconcile

Адресует баг: **M2** (CRITICAL)

> **Контекст кода**: `upsert_subscription` (строки 92-110) при update существующей записи безусловно
> устанавливает `sub.enabled = enabled`. `_sync_to_db` вызывает её для dag_file подписок с дефолтом
> `enabled=True`, перетирая значение, установленное пользователем через UI.

**Файлы:**
- Modify: `airflow_provider_rmq/watcher/models.py`
- Modify: `tests/watcher/test_models.py`

**Изменения:**

- [x] В `upsert_subscription` найти блок обновления существующей записи (строки 103-109).
  Изменить только строку `sub.enabled = enabled` — добавить условие:
  ```python
  if sub is None:
      sub = RMQSubscription(dag_id=dag_id, queue_name=queue_name, conn_id=conn_id, enabled=enabled)
      session.add(sub)
  elif source == "ui":
      sub.enabled = enabled
  # dag_file source: сохраняем текущее значение enabled из БД
  # Остальные поля обновляются для обоих источников:
  sub.filter_data = filter_data or {}
  sub.source = source
  sub.trigger_mode = trigger_mode
  sub.group_key = group_key
  sub.cooldown = cooldown
  ```
  Убедиться что `filter_data`, `source`, `trigger_mode`, `group_key`, `cooldown` обновляются для ВСЕХ записей (не только ui), как и раньше
- [x] Написать тест: `upsert_subscription` с `source="dag_file"` на существующей записи с `enabled=False` — `enabled` остаётся `False`
- [x] Написать тест: `upsert_subscription` с `source="ui"` на существующей записи с `enabled=False`, вызов с `enabled=True` — `enabled` становится `True`
- [x] Написать тест: новая запись с `source="dag_file"` — `enabled` устанавливается из аргумента
- [x] `pytest tests/watcher/test_models.py` — все тесты проходят

---

### Task 3: Fix consumer.py — бинарные тела, сброс статуса, datetime

Адресует баги: **C5** (HIGH), **C3** (MEDIUM), **C4** (LOW)

> **Контекст C3**: При `reconcile` подписка исключается из `active_subs` если она отключена
> (`enabled=False`). Задача для неё отменяется через `task.cancel()`. Статус `consumer_status`
> в БД остаётся `"listening"` — пользователь видит устаревшее состояние.
> Строка удалённой через UI подписки уже удалена из БД, `set_consumer_status` на неё — no-op.
> Исправляем только сценарий "отключена через toggle" (строка существует, статус нужно сбросить).

**Файлы:**
- Modify: `airflow_provider_rmq/watcher/consumer.py`
- Modify: `tests/watcher/test_consumer.py`

**Изменения:**

- [x] **C5** (`_trigger_dag`, строка 251): заменить `.decode("utf-8")` на `.decode("utf-8", errors="replace")`
- [x] **C3** (`reconcile`, строки 116-125): после `asyncio.gather` для `to_remove` добавить сброс статуса для каждого sub_id, только если задача не завершилась по CancelledError с удалением строки (проще: всегда вызывать, no-op если строки нет):
  ```python
  for sub_id in to_remove:
      try:
          with WatcherSession() as session:
              set_consumer_status(session, sub_id, "disconnected")
              session.commit()
      except Exception:
          pass
      self._sub_conn_ids.pop(sub_id, None)
  ```
- [x] **C4** (`_build_run_id`, строка 28): добавить `timezone` в импорт: `from datetime import datetime, timezone` и заменить `datetime.utcnow()` на `datetime.now(timezone.utc)`
- [x] Написать тест: `_trigger_dag` с `message.body` содержащим невалидный UTF-8 (`b"\xff\xfe"`) — не бросает исключение, `conf["body"]` является строкой
- [x] Написать тест: `reconcile` с удалёнными sub_id вызывает `set_consumer_status("disconnected")` для каждого из них
- [x] `pytest tests/watcher/test_consumer.py` — все тесты проходят

---

### Task 4: Fix views.py + plugin.py — is_dag_file контекст и on_load guard

Адресует баги: **V1** (HIGH), **P1** (HIGH)

> **Контекст V1**: шаблон использует `is_dag_file` в 6 местах без `is defined` guard. В Jinja2
> (режим `Undefined`, не `StrictUndefined`) undefined переменная в `{% if %}` → `False` → полей
> readonly нет, поля `required`. Это не краш, но явная передача делает контракт читаемым
> и безопасным при смене конфигурации Jinja2.

**Файлы:**
- Modify: `airflow_provider_rmq/watcher/views.py`
- Modify: `airflow_provider_rmq/watcher/plugin.py`
- Modify: `tests/watcher/test_views.py`
- Modify: `tests/watcher/test_plugin.py`

**Изменения:**

- [x] Прочитать `templates/rmq_watcher/subscription_form.html` и найти все места использования `is_dag_file` — убедиться что они не защищены `is defined`
- [x] **V1**: в методе `create` view, все `return self.render_template("rmq_watcher/subscription_form.html", sub=None)` заменить на `..., sub=None, is_dag_file=False)` (строки с ошибкой валидации: ~57, ~63, и ~80)
- [x] **P1**: в `plugin.py` добавить `import logging` + `log = logging.getLogger(__name__)`, обернуть `ensure_table_exists()` в `try/except Exception` с `log.exception("RMQ Watcher: failed to create DB tables on plugin load")`
- [x] Написать тест views: POST `/rmq-watcher/subscriptions/create` с пустым `dag_id` возвращает HTTP 200 (рендерит форму снова, не 500 UndefinedError)
- [x] Написать тест plugin: `on_load` при исключении в `ensure_table_exists` — исключение не всплывает (перехватывается и логируется)
- [x] `pytest tests/watcher/test_views.py tests/watcher/test_plugin.py` — все тесты проходят

---

### Task 5: Verify acceptance criteria

- [x] `pytest tests/` — все тесты проходят (ноль failures) — 284 passed
- [x] `@hookimpl` на обоих методах listener: строки 96 и 103
- [x] Guard от двойного старта: `self._thread.is_alive()` в `_start()` строка 113
- [x] `enabled` не перезаписывается для dag_file: условие `elif source == "ui"` в `upsert_subscription` строка 105
- [x] Нет голых `{{ csrf_token() }}` в шаблонах — вывод пустой
- [x] `datetime.utcnow` не осталось — нет вхождений в `airflow_provider_rmq/`
- [x] `tenacity` НЕ трогали: строки 12 и 32 в `hooks/rmq.py`

---

### Task 6: [Final] CHANGELOG, коммит, тег v2.0.6

**Файлы:**
- Modify: `CHANGELOG.md`

- [ ] Добавить раздел `## v2.0.6` в `CHANGELOG.md` с описанием всех 7 исправлений (L2, L3, M2, C5, V1, P1, C3+C4)
- [ ] `git add` всех изменённых файлов и один `git commit` с заголовком и телом из 4–5 пунктов (по одному на ключевое исправление)
- [ ] `git tag -a v2.0.6 -m "v2.0.6 — ..."` с кратким описанием
- [ ] Push main и тег (только с явного разрешения пользователя)
- [ ] `mv docs/plans/20260603-rmq-watcher-audit-fixes.md docs/plans/completed/`

---

## Post-Completion

**Ручная верификация в Airflow:**
- Перезапустить scheduler-контейнер после установки v2.0.6
- В логах scheduler увидеть: `RMQWatcherListener.on_starting: component=SchedulerJobRunner`
- Убедиться что в Admin → Connections существует нужный conn_id (RabbitMQ connection). Без него статус будет `error` — это не баг плагина, а отсутствие конфигурации.
- Создать подписку через UI, указав корректный conn_id → статус меняется с `connecting` на `listening`
- Выключить dag_file подписку через UI → через 60+ сек убедиться что она остаётся выключенной (M2 fix)
- Отправить бинарное сообщение в очередь → DAG запускается без бесконечного retry (C5 fix)
