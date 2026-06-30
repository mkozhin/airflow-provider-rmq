# Watcher DAG: фабрика дагов для дебаунса очередей RabbitMQ

## Overview

Реализовать паттерн **watcher DAG** — event-driven запуск основного DAG-а при
получении уведомлений от стороннего сервиса через RabbitMQ.

**Проблема:** несколько площадок (1–N, максимум известен) отправляют уведомления
в очередь с разницей 0–5 минут. Нельзя запускать целевой DAG на каждое сообщение —
нужно одно срабатывание на "пачку". Количество сообщений в пачке заранее неизвестно.

**Решение:**
- Watcher ловит первое сообщение через `RMQSensor(mode="push", deferrable=True)`
- Выжидает `window_minutes` через `TimeDeltaSensorAsync` (дебаунс-окно, слот не занят)
- Дренирует очередь `RMQConsumeOperator` — забирает всё накопившееся
- Один раз триггерит целевой DAG
- Перезапускает себя — до конца рабочего дня (`end_hour`)
- Следующий день: `schedule` запускает автоматически

**Конфиг** — Python-словарь прямо в файле (version-controlled, легко добавить клиента):
```python
# start_hour и end_hour — в UTC
WATCHERS = [
    {
        "client": "client_1",
        "queue": "processing.trigger.client_1",
        "target_dag": "main_dag_client_1",
        "window_minutes": 5,
        "start_hour": 6,   # UTC: 6:00 UTC = 9:00 MSK
        "end_hour": 15,    # UTC: 15:00 UTC = 18:00 MSK
    },
]
```

**Что НЕ нужно:** Redpanda Connect не участвует — сторонний сервис пишет
в RabbitMQ напрямую.

## Context

### Репозиторий провайдера RabbitMQ

**Путь:** `~/PycharmProjects/apache-airflow-provider-rmq`

Провайдер — **библиотека** с готовыми операторами и сенсорами для RabbitMQ.
При реализации боевого DAG-а нужно смотреть туда, чтобы понимать доступный функционал.
Далее в плане пути вида `[provider]/foo/bar` означают файлы относительно этого корня.

**Доступные компоненты провайдера:**
- `[provider]/airflow_provider_rmq/sensors/rmq.py` — `RMQSensor(deferrable=True, mode="push")`
  push-режим реализован (коммит `4e80825`)
- `[provider]/airflow_provider_rmq/operators/rmq_consume.py` — `RMQConsumeOperator`
- `[provider]/airflow_provider_rmq/operators/rmq_publish.py` — `RMQPublishOperator`
- `[provider]/airflow_provider_rmq/operators/rmq_management.py` — `RMQQueueManagementOperator`
- `[provider]/airflow_provider_rmq/hooks/rmq.py` — `RMQHook` (прямой доступ к RabbitMQ)
- `[provider]/docs/example_dags/` — готовые примеры использования каждого компонента

### Где создаются файлы

| Что | Где | Зачем |
|---|---|---|
| Пример watcher DAG | `[provider]/docs/example_dags/rmq_watcher_factory.py` | Демо внутри репозитория провайдера |
| **Боевой watcher DAG** | определяется при реализации в целевом проекте | Реальное использование |
| **Боевой main DAG** | определяется при реализации в целевом проекте | Реальное использование |

### Внешние зависимости (стандартный Airflow)

- `airflow.operators.trigger_dagrun.TriggerDagRunOperator`
- `airflow.sensors.time_delta.TimeDeltaSensorAsync` (Airflow ≥ 2.4)
- **Airflow:** `>=2.7.0`, Python `>=3.10`

## Development Approach

- **Testing approach:** Regular (код → тесты)
- Это example DAGs — демонстрационный код, unit-тесты для них не обязательны
- Каждый task завершается полностью перед следующим

## Testing Strategy

- Example DAGs проверяются импортом (`python -c "import ..."`) и ручным запуском
- Новые утилиты в пакете (если появятся) покрываются unit-тестами

## What Goes Where

- **Implementation Steps** — создание файлов в этом репозитории
- **Post-Completion** — ручная проверка на реальном Airflow + RabbitMQ

---

## Implementation Steps

### Task 1: Watcher DAG factory

**Files:**
- Create: боевой DAG в папке текущего проекта (путь определяется при реализации)
- Create: `[provider]/docs/example_dags/rmq_watcher_factory.py` (пример, опционально)

- [ ] Добавить `WATCHERS` конфиг-список в начало файла с полями `client`, `queue`,
  `target_dag`, `window_minutes`, `start_hour`, `end_hour`; добавить комментарий
  "добавить нового клиента = добавить один словарь"
- [ ] Написать `_alert_on_failure(context)` — логирует ошибку без авто-рестарта
- [ ] Написать фабричную функцию `make_watcher_dag(client, queue_name, target_dag_id,
  window_minutes, start_hour, end_hour)` возвращающую DAG объект; добавить комментарий
  почему используется фабрика вместо `@dag` (нужна динамическая генерация N DAG-ов из конфига)
- [ ] Шаг 1 внутри фабрики: `RMQSensor(task_id="wait_for_message",
  queue_name=queue_name, deferrable=True, mode="push", rmq_conn_id="rmq_default")`
- [ ] Шаг 2: `TimeDeltaSensorAsync(task_id="debounce_window",
  delta=timedelta(minutes=window_minutes))` — ждёт `data_interval_end + window_minutes`
  без занятия worker-слота; **не передавать** `deferrable=True` (избыточно,
  класс уже deferrable по определению); добавить комментарий: на scheduled-запуске
  (первый run дня) `data_interval_end` уже в прошлом — сенсор срабатывает мгновенно,
  что ожидаемо (очередь ещё пустая, drain ничего не заберёт)
- [ ] Шаг 3: `RMQConsumeOperator(task_id="drain_queue", queue_name=queue_name,
  max_messages=50, rmq_conn_id="rmq_default")` — дренирует всё накопившееся
- [ ] Шаг 4: `TriggerDagRunOperator(task_id="trigger_target",
  trigger_dag_id=target_dag_id, wait_for_completion=False, reset_dag_run=False)`;
  добавить комментарий: каждый trigger создаёт уникальный `execution_date`,
  коллизии крайне маловероятны; `max_active_runs=1` на целевом DAG означает
  что новый run встанет в очередь пока предыдущий не завершится
- [ ] Шаг 5: `@task def maybe_retrigger()` — если `datetime.now(utc).hour < end_hour`
  вызывает `trigger_dag(dag_id=self_dag_id)`; иначе логирует "окно закрыто, следующий
  старт по расписанию"; добавить комментарий что `end_hour` в UTC
- [ ] DAG параметры фабрики: `dag_id=f"watcher_{client}"`,
  `schedule=f"0 {start_hour} * * *"`, `max_active_runs=1`, `catchup=False`,
  `start_date=datetime(2024, 1, 1)`, `tags=["example", "rmq", "watcher"]`,
  `on_failure_callback=_alert_on_failure`; добавить комментарий что `start_hour`
  в UTC
- [ ] В конце файла: цикл `for cfg in WATCHERS: globals()[f"watcher_{cfg['client']}"]
  = make_watcher_dag(**cfg)`
- [ ] Проверить синтаксис из корня `[provider]`:
  `python -m py_compile docs/example_dags/rmq_watcher_factory.py` без ошибок

---

### Task 2: Пример целевого DAG-а

**Files:**
- Create: `[provider]/docs/example_dags/rmq_main_dag_example.py`

- [ ] Создать `main_dag_client_1` с `schedule="0 8 * * *"`, `max_active_runs=1`,
  `catchup=False`
- [ ] Один `@task def process_data()` — заглушка с `log.info`
- [ ] Docstring: объяснить что DAG запускается по расписанию в 8:00 И дополнительно
  через watcher при получении новых данных в течение дня

---

### Task 3: Документация

**Files:**
- Modify: `[provider]/readme.md`
- Modify: `[provider]/readme_ru.md`
- Modify: `[provider]/CHANGELOG.md`

- [ ] Добавить секцию **Watcher DAG Pattern** в `readme.md`: описание паттерна,
  схема потока (ASCII), ссылка на `rmq_watcher_factory.py`
- [ ] То же в `readme_ru.md` на русском
- [ ] Обновить `CHANGELOG.md` — добавить запись о новом example DAG

---

### Task 4: Проверка критериев приёмки

- [ ] Синтаксис без ошибок (из корня `[provider]`):
  `python -m py_compile docs/example_dags/rmq_watcher_factory.py`
  и `python -m py_compile docs/example_dags/rmq_main_dag_example.py`
- [ ] Airflow видит все watcher-DAG-и из `WATCHERS` (проверить через `airflow dags list`)
- [ ] `TimeDeltaSensorAsync` доступен: `python -c "from airflow.sensors.time_delta import TimeDeltaSensorAsync"`
- [ ] `maybe_retrigger` не вызывает self-trigger после `end_hour`
- [ ] На scheduled-запуске (первый run) `TimeDeltaSensorAsync` срабатывает мгновенно,
  drain ничего не забирает — поведение ожидаемое

---

### Task 5: [Final] Документация и перемещение плана

- [ ] Переместить план в `[provider]/docs/plans/completed/`

---

## Technical Details

### Поток выполнения

```
09:00  schedule → watcher_client_1 run #1 стартует
         RMQSensor(push, deferrable) → Triggerer держит AMQP-соединение, 0 слотов

10:15  RabbitMQ пушит сообщение → Triggerer будит task
         TimeDeltaSensorAsync → ждёт до 10:20, 0 слотов
10:20  RMQConsumeOperator → забирает накопившиеся сообщения (0–N штук)
         TriggerDagRunOperator → main_dag_client_1 запущен (или поставлен в очередь)
         maybe_retrigger → 10 < 18 → trigger watcher_client_1 run #2

10:20  run #2 стартует → RMQSensor(push) → Triggerer, 0 слотов
  ...
18:05  maybe_retrigger → 18 >= 18 → стоп

09:00  следующего дня → schedule → run #N стартует
```

### Почему `TimeDeltaSensorAsync` а не sleep

`TimeDeltaSensorAsync` ждёт `data_interval_end + delta`. При triggered-запусках
(self-trigger через `trigger_dag()`) `data_interval_end == logical_date == время
триггера` → 5-минутное окно без занятия worker-слота.

**Исключение: первый scheduled-запуск дня** (в `start_hour`). У него
`data_interval_end = start_hour + 1 период расписания`, что уже в будущем или прошлом
в зависимости от реализации Airflow. На практике сенсор срабатывает мгновенно —
это нормально, очередь ещё пуста, drain вернёт 0 сообщений, watcher просто
самоперезапустится и начнёт слушать очередь.

При 10 параллельных watcher-ах: 10 async coroutine в Triggerer, 0 worker-слотов.

### Self-trigger через Airflow API

```python
from airflow.api.common.trigger_dag import trigger_dag
from datetime import datetime, timezone

@task
def maybe_retrigger(self_dag_id: str, end_hour: int) -> None:
    if datetime.now(tz=timezone.utc).hour < end_hour:
        trigger_dag(dag_id=self_dag_id, conf={})
    else:
        log.info("Time window closed, watcher resumes tomorrow via schedule")
```

### Параметры WATCHERS

| Поле | Тип | Описание |
|---|---|---|
| `client` | str | Идентификатор → `dag_id = f"watcher_{client}"` |
| `queue` | str | Имя очереди RabbitMQ |
| `target_dag` | str | `dag_id` целевого DAG-а |
| `window_minutes` | int | Дебаунс-окно (обычно 5) |
| `start_hour` | int | Час старта по расписанию (0–23) |
| `end_hour` | int | После этого часа петля не продолжается |

### on_failure_callback (без авто-рестарта)

```python
def _alert_on_failure(context: dict) -> None:
    log.error(
        "Watcher DAG %s failed at task %s. Restart manually if needed.",
        context["dag"].dag_id,
        context["task_instance"].task_id,
    )
```

Авто-рестарт намеренно не реализован — при бесконечно падающей задаче
авто-рестарт создаёт цикл. При ошибке — пользователь видит алерт в логах
и перезапускает watcher вручную.

## Post-Completion

**Ручная проверка:**
- Запустить `watcher_client_1` вручную в Airflow UI
- Опубликовать 2–3 тестовых сообщения в очередь с интервалом 1 мин через `RMQPublishOperator`
- Убедиться что `main_dag_client_1` запустился ровно один раз
- Проверить что после `end_hour` watcher не перезапускает себя
- Проверить что на следующий день в `start_hour` watcher стартует по расписанию

**Зависимости:**
- `TimeDeltaSensorAsync` из `airflow.sensors.time_delta` — доступен с Airflow 2.4 (у нас `>=2.7` — ок)
- Push-режим RMQSensor — реализован в коммите `4e80825`
- Сторонний сервис пишет напрямую в RabbitMQ очередь `processing.trigger.<client>`
