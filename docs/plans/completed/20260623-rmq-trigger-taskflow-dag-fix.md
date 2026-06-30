# Фикс присоединения подписок @rmq_trigger к TaskFlow DAG (@dag/@task)

## Overview

`@rmq_trigger(...)`, применённый над TaskFlow-декоратором `@dag(...)` (паттерн,
используемый во ВСЕХ примерах репозитория, в readme.md/readme_ru.md и в
docstring самого декоратора), ломает импорт DAG-файла в настоящем Airflow:

```python
@rmq_trigger(queue="orders")
@dag(dag_id="my_dag", schedule=None)
def my_dag():
    ...

my_dag()
```

падает на строке `@rmq_trigger(...)` с `AttributeError: 'function' object has
no attribute 'dag_id'`.

**Причина:** `airflow.decorators.dag(...)` возвращает не объект `DAG`, а
обычную функцию-фабрику (`factory`, см. `airflow/models/dag.py:4244`) —
реальный `DAG` появляется только в момент её вызова (`my_dag()` в конце
файла). Когда `@rmq_trigger` навешан *над* `@dag(...)`, его внутренняя
функция `decorator(dag)` получает именно эту фабрику и сразу пытается
прочитать `dag.dag_id` — атрибута пока не существует.

**Подтверждено эмпирически** настоящим `airflow.models.dagbag.DagBag`,
а не только ручным импортом: `docs/example_dags/rmq_watcher_triggered_dag.py`
(уже в `main` до этой сессии) падает с той же ошибкой. Баг не пойман 563
тестами, потому что:
- `tests/watcher/test_decorators.py` тестирует декоратор в изоляции через
  `MagicMock()` с заранее выставленным `.dag_id` — реальный путь через
  `@dag(...)` никогда не воспроизводится.
- Реальный механизм обнаружения подписок (`listener.py`,
  `_extract_subscriptions_from_file`) **никогда не импортирует DAG-файл** —
  он статически разбирает исходный текст через `ast.parse`. Это осознанное
  архитектурное решение (см. docstring метода): импорт DAG-файла занял бы
  Python import lock и мог бы вызвать дедлок с собственной обработкой
  Scheduler'а.

Из этого следует ключевой факт: атрибут `dag._rmq_subscriptions`, который
выставляет декоратор, **не читается нигде в продакшен-коде** — только в
собственных юнит-тестах `decorators.py`. То есть рантайм-поведение
декоратора сейчас не несёт практической нагрузки для самого механизма
подписок, но обязано не ломать импорт DAG, потому что именно из-за этой
поломки сам DAG не появляется в Airflow вообще (DagBag регистрирует Import
Error на весь файл).

**Цель плана:** сделать так, чтобы `@rmq_trigger`, применённый над
`@dag(...)`/`@task`-style TaskFlow DAG, корректно импортировался в настоящем
Airflow 2.9+ (ветка 2.x), не теряя при этом обратной совместимости с
текущим прямым использованием на готовом объекте `DAG`
(`with DAG(...) as dag: ...; rmq_trigger(...)(dag)`), и закрыть пробел в
тестах, из-за которого баг был невидим.

## Context

- **Основной файл:** `airflow_provider_rmq/watcher/decorators.py` —
  `rmq_trigger()` → `decorator(dag)` (строки 126–152) безусловно читает
  `dag.dag_id` сразу при декорировании.
- **Подтверждение, что `_rmq_subscriptions` мёртв для прода:**
  `grep -rn "_rmq_subscriptions"` по всему репозиторию (кроме тестов) даёт
  совпадения только в самом `decorators.py`.
- **Реальный механизм подписок:** `airflow_provider_rmq/watcher/listener.py`
  — `_extract_subscriptions_from_file` (строка 315) парсит исходный текст
  через `ast`, никогда не выполняя файл. `_parse_rmq_trigger_decorator`
  (строка 70) извлекает только литеральные kwargs через `ast.literal_eval` —
  значит, исправление декоратора НЕ требует никаких изменений в `listener.py`
  или `subscription_builder.py`.
- **Тесты, которые нужно сохранить без изменений:**
  `tests/watcher/test_decorators.py` — все 25 тестов используют
  `_make_dag()` (MagicMock с явным `.dag_id`) — это естественно попадёт в
  "eager"-ветку нового кода, поведение не меняется.
- **Существующий, уже смерженный пример с этим багом:**
  `docs/example_dags/rmq_watcher_triggered_dag.py`.
- **Несвязанная, отдельная находка (не в скоупе этого плана):**
  `docs/example_dags/rmq_dlq_setup.py` падает в `DagBag` с
  `TypeError: unsupported operand type(s) for >>: 'list' and 'list'` —
  не имеет отношения к `@rmq_trigger`/подпискам, фиксируется отдельно при
  необходимости (см. Post-Completion).
- **Рассмотренные и отклонённые альтернативы:**
  - *Убрать рантайм-мутацию совсем* (декоратор — чистая валидация без
    побочных эффектов): проще, но ломает задокументированный контракт
    ("decorator appends entries to dag._rmq_subscriptions") и требует
    переписать все 25 тестов декоратора без выигрыша в надёжности.
  - *Запретить стекинг над `@dag`, разрешить только `with DAG(...) as dag`*:
    откатывает репозиторий от TaskFlow-стиля, который используется
    исключительно везде (`readme.md`, все example DAG) — несоразмерный объём
    правок документации/примеров ради более простого декоратора.

## Development Approach

- **Тестирование:** Regular (код → тесты), как в предыдущей сессии
  (`exchange-trigger-subscriptions`).
- Каждый task завершается полностью (включая тесты) перед следующим.
- Обратная совместимость — обязательна: ни один из существующих 563 тестов
  не должен измениться или начать падать.
- Создание новых example DAG (cooldown-2-очереди, jetstat-exchange) —
  **отдельная задача после этого плана**, не включается сюда.

## Testing Strategy

- **Unit-тесты** на новую ветку логики в `decorators.py` (фабрика без
  `.dag_id`, неверный порядок декораторов, параметризованная фабрика,
  некорректный вход) — `tests/watcher/test_decorators.py`.
- **Регрессионный интеграционный тест** через настоящий
  `airflow.models.dagbag.DagBag` против `docs/example_dags/` — единственный
  вид теста, способный поймать именно этот класс багов (импорт DAG-файла
  целиком), которого в репозитории сейчас не существует.

## Progress Tracking

- Отмечать `[x]` сразу по завершении пункта.
- Новые задачи помечать префиксом ➕, блокеры — ⚠️.

## Solution Overview

В `decorators.py` `decorator(dag_or_factory)` разветвляется по duck-typing
`hasattr(dag_or_factory, "dag_id")`:

1. **Уже готовый `DAG`-инстанс** (атрибут `dag_id` есть — классический
   `with DAG(...) as dag:` или мок в тестах) → присоединить подписки сразу,
   как сейчас. Поведение не меняется, ни один существующий тест не трогаем.
2. **Невызванная TaskFlow-фабрика от `@dag(...)`** (атрибута `dag_id` нет, но
   объект `callable`) → оборачивается в `functools.wraps`-обёртку: при
   вызове обёртки сначала вызывается исходная фабрика (получаем настоящий
   `DAG`), затем к результату присоединяются подписки, затем результат
   возвращается. Поскольку TaskFlow-конвенция требует немедленного вызова
   фабрики в том же файле (`my_dag()` в конце), ошибка валидации всё равно
   проявляется при первом же импорте файла — просто на пару строк ниже.
3. **Ни то, ни другое** (не DAG, не callable) → понятный `TypeError`.
4. Если обёртка вызвана, но результат вызова исходной фабрики не имеет
   `.dag_id` (типичный случай — декораторы перепутаны местами:
   `@dag(...) @rmq_trigger(...) def f(): ...`) → понятный `TypeError` с
   подсказкой про правильный порядок, а не невнятный `AttributeError` на
   `None` где-то внутри `with DAG(...)`.

Рекурсивная природа оборачивания корректно поддерживает произвольный
стекинг нескольких `@rmq_trigger` друг на друге и поверх параметризуемых
DAG-фабрик без специального кода для этих случаев.

## Technical Details

### Новая структура `decorators.py`

```python
from __future__ import annotations

import functools
from typing import Any

from airflow_provider_rmq.watcher.subscription_builder import (
    build_subscriptions,
    has_exchange_conflict,
)


def rmq_trigger(
    queue=None, queues=None, conn_id="rmq_default", filter_data=None, cooldown=0,
    *, exchange=None, routing_keys=None, routing_key_ids=None, routing_key_status="*",
):
    """... (текущий docstring + новый раздел "Decorator order and DAG types") ..."""

    def _attach(dag_obj):
        new_subs = build_subscriptions(
            dag_id=dag_obj.dag_id, queue=queue, queues=queues, exchange=exchange,
            routing_keys=routing_keys, routing_key_ids=routing_key_ids,
            routing_key_status=routing_key_status, conn_id=conn_id,
            filter_data=filter_data, cooldown=cooldown,
        )
        if not hasattr(dag_obj, "_rmq_subscriptions"):
            dag_obj._rmq_subscriptions = []
        if has_exchange_conflict(dag_obj._rmq_subscriptions, new_subs):
            raise ValueError(
                "Multiple @rmq_trigger(exchange=...) decorators on DAG "
                f"{dag_obj.dag_id!r} are not supported — they would all resolve "
                f"to the same 'rmq_watcher.sub.{dag_obj.dag_id}' queue. Use one "
                "decorator call with the union of routing keys instead."
            )
        dag_obj._rmq_subscriptions.extend(new_subs)
        return dag_obj

    def decorator(dag_or_factory):
        if hasattr(dag_or_factory, "dag_id"):
            return _attach(dag_or_factory)

        if not callable(dag_or_factory):
            raise TypeError(
                "@rmq_trigger must decorate a DAG instance or a callable "
                f"@dag(...) factory function, got {type(dag_or_factory)!r}."
            )

        @functools.wraps(dag_or_factory)
        def wrapper(*args, **kwargs):
            result = dag_or_factory(*args, **kwargs)
            if not hasattr(result, "dag_id"):
                raise TypeError(
                    "@rmq_trigger wrapped a callable that did not produce a DAG "
                    f"instance (got {type(result)!r}). Make sure @rmq_trigger is "
                    "the outermost decorator, placed above @dag(...), not below it."
                )
            return _attach(result)

        return wrapper

    return decorator
```

### Почему не нужны изменения в `subscription_builder.py` / `listener.py`

`build_subscriptions()` использует `dag_id` только для построения
`queue_name = f"rmq_watcher.sub.{dag_id}"` в exchange-режиме — `dag_obj.dag_id`
доступен в `_attach()` независимо от того, какой веткой (eager/lazy) мы туда
попали. AST-парсер в `listener.py` вообще не видит рантайм-объекты — он
получает `dag_id` из текста `@dag(dag_id=...)` отдельной функцией
`_extract_dag_id_from_decorators`. Обе системы остаются полностью
независимыми, как и были спроектированы.

### Почему отложенная валидация не ослабляет проверки

Для прямого `DAG`-инстанса валидация (через `build_subscriptions`) происходит
сразу при декорировании — без изменений. Для TaskFlow-фабрики валидация
происходит в момент вызова `wrapper(...)`, но TaskFlow-конвенция (везде в
этом репозитории) обязывает вызывать фабрику немедленно в конце того же
файла (`my_dag()`) — значит ошибка всё равно проявляется при первом же
импорте файла Airflow'ом, просто на этапе вызова, а не декорирования.

## What Goes Where

- **Implementation Steps** — изменения в `decorators.py`, новые/обновлённые
  тесты, регрессионный DagBag-тест, запись в CHANGELOG.
- **Post-Completion** — создание двух новых example DAG (отдельная задача
  после этого плана) и опциональный фикс несвязанного бага в
  `rmq_dlq_setup.py`.

## Implementation Steps

### Task 1: Ленивое присоединение подписок к TaskFlow DAG-фабрике

**Files:**
- Modify: `airflow_provider_rmq/watcher/decorators.py`
- Modify: `tests/watcher/test_decorators.py`

- [x] Вынести текущую логику присоединения подписок (build_subscriptions +
  has_exchange_conflict + extend) в приватную функцию `_attach(dag_obj)`
  внутри `rmq_trigger`, как описано в Technical Details
- [x] В `decorator(dag_or_factory)` реализовать ветвление
  `hasattr(dag_or_factory, "dag_id")` → eager `_attach`; иначе при
  `callable` → ленивая обёртка через `functools.wraps`; иначе `TypeError`
- [x] В обёртке: если результат вызова исходной фабрики не имеет
  `.dag_id`, бросить `TypeError` с понятной подсказкой про порядок
  декораторов
- [x] Обновить docstring `rmq_trigger`: добавить раздел о том, что
  поддерживаются и готовый `DAG`-инстанс, и невызванная `@dag(...)`-фабрика,
  и что `@rmq_trigger` обязан быть выше `@dag(...)` в стеке декораторов
- [x] Написать тест: невызванная фабрика (обычная функция без `.dag_id`)
  оборачивается, и `_rmq_subscriptions` появляется на результате только
  после вызова обёртки
- [x] Написать тест: стекинг `@rmq_trigger(exchange=...)` поверх
  TaskFlow-фабрики — `ValueError` возникает в момент вызова обёртки, а не
  при декорировании
- [x] Написать тест: параметризованная фабрика, вызванная дважды с разными
  kwargs, создаёт два независимых DAG-объекта с собственными
  `_rmq_subscriptions`
- [x] Написать тест: неверный порядок (`@dag(...)` ниже `@rmq_trigger(...)`,
  то есть `rmq_trigger` декорирует обычную функцию, которая возвращает не
  DAG) — ожидается понятный `TypeError`, не `AttributeError`
- [x] Написать тест: полностью некорректный вход (например, `None` или
  `"string"`) — `TypeError`
- [x] Прогнать `pytest tests/watcher/test_decorators.py -q` — все 25
  существующих + новые тесты проходят без изменений в существующих

### Task 2: Регрессионный тест через настоящий Airflow DagBag

**Files:**
- Create: `tests/watcher/test_example_dags_dagbag.py`

- [x] Загрузить `docs/example_dags/` через
  `airflow.models.dagbag.DagBag(dag_folder=..., include_examples=False, safe_mode=False)`
- [x] Собрать список файлов, использующих `rmq_trigger` (например, файлы,
  содержащие подстроку `"rmq_trigger"` в исходном тексте)
- [x] Assert: ни один из этих файлов не присутствует в `bag.import_errors`
- [x] Явно задокументировать в тесте, что `rmq_dlq_setup.py` намеренно
  исключён из проверки (несвязанный, отдельный баг — см. Context)
- [x] Учесть, что фильтр по подстроке `"rmq_trigger"` подхватит и два
  черновика новых example DAG (`rmq_watcher_cooldown_multi_queue.py`,
  `rmq_watcher_jetstat_exchange.py`), которые на момент этого плана лежат в
  `docs/example_dags/` как untracked файлы — тест должен требовать их
  безошибочного импорта так же, как и для закоммиченных файлов
- [x] Прогнать тест — должен быть зелёным после Task 1; временно
  откатить фикс из Task 1 и убедиться, что тест ловит регресс (sanity-check),
  затем вернуть фикс

### Task 3: [Final] CHANGELOG и финальная проверка

**Files:**
- Modify: `CHANGELOG.md`

- [x] Добавить запись о фиксе: `@rmq_trigger` теперь корректно работает
  при стекинге над TaskFlow `@dag(...)` (раньше ломал импорт DAG с
  `AttributeError` на `dag.dag_id`)
- [x] Прогнать полный набор тестов: `pytest tests/ -q` — все проходят
  (563 существующих + новые из Task 1–2; фактически 573 passed)
- [x] Вручную прогнать `DagBag` по `docs/example_dags/` ещё раз, убедиться
  в нулевом количестве import errors для файлов с `rmq_trigger` (только
  несвязанный `rmq_dlq_setup.py` в `import_errors`, как и ожидалось)
- [x] Переместить этот план в `docs/plans/completed/` (note: фактическое
  перемещение файла выполняет оркестратор в конце всего workflow, после
  фаз review — не выполняется в рамках этой итерации)

## Post-Completion

**Следующий шаг (отдельная задача, не в этом плане):**
- Создать два новых example DAG: подписка на две очереди с cooldown-задержкой
  и exchange-mode подписка на конкретный id отчёта Jetstat (`succeeded`).
  Черновики уже подготовлены в текущей сессии и валидированы через
  `_parse_rmq_trigger_decorator` — после фикса из этого плана нужно
  подтвердить их импорт через настоящий `DagBag` и добавить в репозиторий.

**Несвязанная находка, опционально:**
- `docs/example_dags/rmq_dlq_setup.py` падает в `DagBag` с
  `TypeError: unsupported operand type(s) for >>: 'list' and 'list'`
  (строка ~176, `[delete_main_queue, delete_dlq_queue] >> [delete_main_ex, delete_dlx_ex]`).
  Не связано с `@rmq_trigger`/подписками — фиксировать отдельно по решению
  пользователя.
