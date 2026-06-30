# AST Parser: читать dag_id из @dag(dag_id=...) вместо имени функции

## Overview

AST-парсер в `_extract_subscriptions_from_file` всегда берёт `dag_id` из имени
Python-функции (`node.name`). Если пользователь задаёт явный `dag_id=` в декораторе
`@dag(dag_id='my_dag')`, а функция называется иначе (например `get_params_dag`),
подписка регистрируется с неверным `dag_id` и сообщения из очереди не запускают DAG.

Цель — сделать так, чтобы `dag_id` в подписке совпадал с тем, что отображается
в Airflow UI (т.е. с реальным `dag_id` DAG-а).

## Context

- **Основной файл**: `airflow_provider_rmq/watcher/listener.py`
  - функция `_parse_rmq_trigger_decorator` (строки 27–71) — парсит `@rmq_trigger`
  - метод `_extract_subscriptions_from_file` (строки 238–268) — строка 266 жёстко берёт `node.name`
  - строки 247–249: в docstring есть явная пометка «Limitation»
- **Тесты**: `tests/watcher/test_listener.py`
  - класс `TestScanSubscriptions` — тесты на `_extract_subscriptions_from_file` через моки
  - `test_extract_subscriptions_returns_empty_list_on_ioerror` — единственный тест,
    вызывающий `_extract_subscriptions_from_file` напрямую с реальным файлом

## Development Approach

- **Тестирование**: Regular (код → затем тесты)
- Один логический шаг за раз, тесты обязательны перед следующим шагом
- Обратная совместимость: если `dag_id` не удаётся извлечь статически → fallback на `node.name`

## Solution Overview

Добавить отдельную вспомогательную функцию `_extract_dag_id_from_decorators(decorators)`
рядом с `_parse_rmq_trigger_decorator`. Функция сканирует список декораторов одной функции,
ищет вызов `@dag(...)` и пытается извлечь `dag_id=` как строковый литерал.

Возможные случаи:
| Ситуация | Результат |
|---|---|
| `@dag(dag_id='my_dag')` | `'my_dag'` |
| `@dag()` без dag_id | `None` → fallback на имя функции |
| `@dag(dag_id=SOME_VAR)` (нелитерал) | `None` + warning → fallback на имя функции |
| `@dag(dag_id=123)` (литерал, но не str) | `None` (тихий fallback, предупреждение не нужно) |
| нет декоратора `@dag` | `None` → fallback на имя функции |

## Technical Details

### Новая функция

```python
def _extract_dag_id_from_decorators(decorators: list[ast.expr]) -> str | None:
    """Return explicit dag_id from @dag(dag_id='...') or None for fallback to function name."""
    for dec in decorators:
        if not isinstance(dec, ast.Call):
            continue
        func = dec.func
        is_dag = (
            (isinstance(func, ast.Name) and func.id == "dag")
            or (isinstance(func, ast.Attribute) and func.attr == "dag")
        )
        if not is_dag:
            continue
        for kw in dec.keywords:
            if kw.arg == "dag_id":
                try:
                    value = ast.literal_eval(kw.value)
                    if isinstance(value, str):
                        return value
                except (ValueError, TypeError):
                    log.warning(
                        "rmq_trigger: dag_id= is not a string literal — falling back to function name"
                    )
                break  # нашли dag_id= но не смогли — не продолжаем
    return None
```

Поведение:
- Обрабатывает как `@dag(...)`, так и `decorators.dag(...)` (атрибутный доступ)
- Работает одинаково для `FunctionDef` и `AsyncFunctionDef` (оба имеют `decorator_list`)
- При `dag_id=VARIABLE` (нелитерал) → `ValueError` → warning + fallback
- При `dag_id=123` (литерал, не str) → тихий fallback (не предупреждаем, это редкость)
- `break` после первого найденного `dag_id=` — первый `@dag` и первый `dag_id` приоритетны

### Изменение в `_extract_subscriptions_from_file`

```python
# было:
sub["dag_id"] = node.name

# стало:
sub["dag_id"] = _extract_dag_id_from_decorators(node.decorator_list) or node.name
```

Также удалить «Limitation» комментарий из docstring.

### Механика тестов (Task 2 и Task 3)

**Task 2 — unit-тесты через `ast.parse`:**
Функция принимает `list[ast.expr]` (decorator_list), не модуль. Тесты строятся так:
```python
import ast

def _decorators(src: str) -> list:
    return ast.parse(src).body[0].decorator_list

# пример:
decorators = _decorators("@dag(dag_id='my_dag')\ndef f(): pass")
result = _extract_dag_id_from_decorators(decorators)
assert result == "my_dag"
```

**Task 3 — интеграционные тесты с реальными файлами:**
Использовать pytest `tmp_path` fixture, записывать реальный `.py`-файл, вызывать
`listener._extract_subscriptions_from_file(str(dag_file))` напрямую (не через моки).

## Implementation Steps

### Task 1: Добавить `_extract_dag_id_from_decorators` и обновить парсер

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`

- [x] добавить функцию `_extract_dag_id_from_decorators(decorators)` перед `_parse_rmq_trigger_decorator` (строка ~27)
- [x] обновить строку 266: `sub["dag_id"] = _extract_dag_id_from_decorators(node.decorator_list) or node.name`
- [x] убрать «Limitation» пометку из docstring `_extract_subscriptions_from_file`

### Task 2: Unit-тесты для `_extract_dag_id_from_decorators`

**Files:**
- Modify: `tests/watcher/test_listener.py`

- [x] добавить вспомогательную функцию `_decorators(src)` в начале файла (или в классе): парсит строку и возвращает `decorator_list` первой функции
- [x] добавить класс `TestExtractDagId` с тестами:
  - [x] `@dag(dag_id='my_dag')` → `'my_dag'`
  - [x] `decorators.dag(dag_id='my_dag')` (атрибутный доступ) → `'my_dag'`
  - [x] `@dag()` без dag_id → `None`
  - [x] нет декоратора `@dag` → `None`
  - [x] `@dag(dag_id=VARIABLE)` (нелитерал) → `None`
  - [x] `@dag(dag_id=123)` (литерал, не str) → `None`
  - [x] async-функция `async def f()` с `@dag(dag_id='my_dag')` → `'my_dag'`
- [x] запустить тесты — все должны проходить (8/8)

### Task 3: Интеграционные тесты `_extract_subscriptions_from_file`

**Files:**
- Modify: `tests/watcher/test_listener.py`

- [x] добавить тесты в `TestExtractSubscriptionsFromFile` через `tmp_path`:
  - [x] DAG-файл с `@dag(dag_id='explicit')` + `@rmq_trigger` → подписка с `dag_id='explicit'`
  - [x] DAG-файл без явного `dag_id=` + `@rmq_trigger` → fallback на имя функции
  - [x] DAG-файл с `@dag(dag_id=SOME_VAR)` + `@rmq_trigger` → fallback на имя функции
- [x] запустить полный набор тестов — 299 passed

### Task 4: Verify + CHANGELOG + завершение

- [ ] убедиться что все тесты проходят (их стало больше чем было — это ожидаемо)
- [ ] добавить раздел `## v2.0.9` в `CHANGELOG.md` с пунктом об исправлении
- [ ] переместить план в `docs/plans/completed/`

## Post-Completion

**Ручная проверка**:
- Задеплоить версию в Airflow
- Создать тестовый DAG с `@dag(dag_id='custom_id')` и `@rmq_trigger`
- Убедиться что через ~60 с в UI подписок появится `dag_id='custom_id'`, а не имя функции
- Отправить сообщение в очередь и проверить что запускается нужный DAG
