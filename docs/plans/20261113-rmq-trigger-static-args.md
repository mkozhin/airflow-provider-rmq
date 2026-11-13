# Статически читаемые аргументы `@rmq_trigger`

## Overview

- Watcher находит подписки, разбирая DAG-файлы через AST, и не выполняет их. Аргумент
  `@rmq_trigger`, который не является литералом (`conn_id=RMQ_CONN`, `filter_data=FILTER`,
  `cooldown=get_cooldown()`), watcher выбрасывает и строит подписку с умолчанием этого
  параметра. Подписка в UI выглядит рабочей (`listening`), а слушает `rmq_default` вместо
  нужного брокера, работает без фильтра, в immediate-режиме вместо cooldown или
  возвращает отвергнутые сообщения в очередь. Единственный след — WARNING в логе
  шедулера (backlog: `rmq-trigger-kwargs-silently-dropped`).
- Позиционные аргументы после первого (`conn_id`, `filter_data`, `cooldown` в
  сигнатуре `rmq_trigger(queue, queues, conn_id, filter_data, cooldown, *, ...)`) парсер
  не читает вовсе и не сообщает об этом.
- Решение из трёх частей:
  1. watcher резолвит константы верхнего уровня самого DAG-файла любого литерального
     типа — голым именем, позиционно, внутри литералов списков и словарей, через
     `**CONST`;
  2. всё, что статически не читается, watcher отказывается регистрировать для всего
     DAG'а (ERROR в логе) вместо подстановки умолчаний;
  3. сам декоратор при импорте DAG прогоняет свой вызов тем же парсером и сравнивает
     результат с фактическими аргументами и `dag_id`. Нечитаемость или расхождение —
     исключение, DAG не импортируется, Airflow показывает плашку «Broken DAG». Ломается
     весь DAG: подписка, работающая не так, как написано в файле, хуже явно сломанного
     DAG'а.
- Импортированные константы (`from common.rmq import RMQ_CONN`) не резолвятся: watcher
  разбирает только сам DAG-файл.

## Context (from discovery)

- `airflow_provider_rmq/watcher/listener.py`:
  - `_SCOPE_BOUNDARIES`, `_outer_parts` (194), `_module_scope_nodes` (218) — обход
    области модуля;
  - `_collect_module_constants` (238) — собирает только строковые константы верхнего
    уровня, с правилом «имя связано ровно один раз» и отключением при `import *`;
  - `_DagIdSentinel`/`_UNRESOLVED_DAG_ID` (354), `_extract_dag_id_from_decorators` (361),
    `_resolve_function_dag_id` (717) — резолвинг `dag_id` с фильтром «константа объявлена
    выше функции», запретом констант для вложенных функций и WARNING'ом watcher'а;
  - `_RMQ_TRIGGER_KWARGS` (514), `_is_rmq_trigger_call` (528), `_scanned_queue_label`
    (545), `_UNREADABLE_KWARG_OUTCOMES`, `_warn_unreadable_kwarg` (590),
    `_parse_rmq_trigger_decorator` (633) — чтение аргументов через `ast.literal_eval`,
    `continue` на нечитаемом (688) и на неизвестном ключе (679-680), только `node.args[0]`
    из позиционных;
  - `RMQWatcherListener._extract_subscriptions_from_file` (1937) — чтение файла в utf-8,
    `ast.walk` по всем `FunctionDef`/`AsyncFunctionDef`, конфликт стекинга `exchange=`
    (WARNING, первый вызов остаётся).
  - Модуль импортирует `consumer` (`aio_pika`, `httpx`), `models`, `runner`.
- `airflow_provider_rmq/watcher/decorators.py` — `rmq_trigger`, `_attach` вызывает
  `build_subscriptions`; ветки готового `DAG` и неисполненной TaskFlow-фабрики.
- `airflow_provider_rmq/watcher/subscription_builder.py` — `build_subscriptions`,
  `has_exchange_conflict`; общий для декоратора и watcher'а.
- `airflow_provider_rmq/watcher/models.py:30` — уникальный ключ подписки
  `(dag_id, queue_name, conn_id)`.
- Тесты: `tests/watcher/test_listener.py` (в т.ч. `test_nested_function_with_literal_dag_id_still_resolves`,
  1647), `tests/watcher/test_decorators.py` (около 40 прямых вызовов
  `rmq_trigger(...)(dag)`), `tests/watcher/test_example_dags_dagbag.py` (импорт примеров
  через `DagBag`), `tests/watcher/test_subscription_builder.py`.
- Документация: `readme.md:626-630`, docstring `rmq_trigger` (раздел «Decorator order and
  DAG types», абзац «dag_id must be statically resolvable»), блоки «Gotcha: literal
  arguments only» в `docs/example_dags/rmq_watcher_jetstat_exchange.py:65-80` и
  `docs/example_dags/rmq_watcher_cooldown_multi_queue.py:48-66`.
- Прод на Airflow 2.9.1, dev venv на 2.11.2, Python 3.10.

## Development Approach

- **testing approach**: Regular (код, затем тесты в той же задаче)
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
- run tests after each change: `pytest tests/ -q`
- обратная совместимость: DAG-файлы с литеральными аргументами ведут себя как прежде,
  включая DAG'и во вложенных фабриках и под `if`/`for`/`with`/`try` модульного уровня;
  нелитеральные аргументы, прежде молча подменявшиеся умолчанием, становятся ошибкой
  импорта — это и есть цель изменения

## Testing Strategy

- **unit tests**: required for every task (see Development Approach above)
- AST-часть тестируется на исходном тексте (`ast.parse` строки) без файлов
- чистая проверка декоратора — на файлах в `tmp_path` без `DagBag`
- подключение проверки в декоратор — на файлах в `tmp_path`, импортируемых через
  `DagBag`, потому что ей нужен настоящий call site
- e2e/UI-тестов в проекте нет

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview

### Один парсер на два процесса

Чтение `@rmq_trigger` переезжает из `listener.py` в отдельный модуль
`airflow_provider_rmq/watcher/dag_file_scan.py`. Он зависит только от стандартной
библиотеки и `subscription_builder`, поэтому декоратор может импортировать его внутри
DAG-процессора, не таща за собой `listener`, `consumer` (`aio_pika`, `httpx`), `models` и
`runner`.

Watcher и декоратор пользуются одними и теми же функциями:
- `_read_and_parse(path)` — чтение файла в utf-8 и `ast.parse`; файл, который watcher не
  декодирует, не проходит и проверку декоратора;
- `_iter_trigger_functions(tree)` — обход `ast.walk` по всем `FunctionDef`/
  `AsyncFunctionDef` с их пригодными константами (`_usable_constants`: пустой словарь
  для вложенных функций, фильтр «объявлена выше» для функций модульного уровня);
- `_static_dag_id(func, constants)` — чистый резолвер `dag_id` без логирования; WARNING
  остаётся в обёртке watcher'а;
- `_read_function_triggers(func, constants, dag_id)` — чтение всех вызовов `rmq_trigger`
  функции и сборка подписок с отказом на уровне функции.

### Что читается

| Форма | Пример | Результат |
|---|---|---|
| литерал | `conn_id="rmq_prod"` | читается |
| константа файла | `conn_id=RMQ_CONN` | читается |
| позиционный аргумент | `rmq_trigger(QUEUE, None, RMQ_CONN)` | читается, позиции 0–4 |
| имя внутри литерала | `queues=[Q1, Q2]`, `filter_data={"body.type": PAID}` | читается |
| распаковка константы | `**RMQ_OPTS` (словарь со строковыми ключами) | читается |
| импорт | `from x import RMQ_CONN` | отказ |
| вызов, атрибут, `os.environ[...]` | `conn_id=get_conn()` | отказ |
| f-строка, конкатенация | `queue=f"{ENV}.orders"` | отказ |
| `*args`, `**expr` не из констант | `**load_opts()` | отказ |
| неизвестный ключ, повтор параметра | `queues_=...`, `rmq_trigger("q", queue="r")` | отказ |
| изменяемая константа, которую файл меняет | `FILTER["a"]["b"] = 1`, `QUEUES.append(...)` | отказ |

Константа пригодна, если: присвоена на верхнем уровне литеральным выражением, связана в
области модуля ровно один раз, объявлена строкой выше функции, функция — модульного
уровня, в файле нет `import *` (действующие правила). Новое правило касается только
констант, значение которых содержит `list` или `dict`: такое имя не пригодно, если
где-либо в файле оно стоит корнем цепочки `Subscript`/`Attribute` в контексте
`Store`/`Del` (присваивание, `del`, `+=`, цель `for`/`with`) или корнем цепочки, на которой
вызывается метод (`FILTER["a"].update(...)`, `QUEUES.append(...)`). Строки и числа
неизменяемы, поэтому `DAG_ID.lower()` константу не отравляет.

`dag_id` по-прежнему принимается только строкой: нестроковая константа (`None`, `0`,
`False`) в `dag_id=` нерезолвима — проверка «не строка» идёт раньше проверки на пустое
значение.

### Отказ вместо умолчания — на уровне функции

Если у функции нечитаем хоть один вызов `rmq_trigger` или вызовы конфликтуют по стекингу
`exchange=`, watcher не регистрирует ни одной подписки этой функции и пишет ERROR с
`dag_id`, путём файла, параметрами и причинами. Так watcher повторяет поведение
декоратора, который в этих случаях ломает импорт всего DAG'а. Исходы
`registered`/`registered_unpacking` исчезают вместе с подменой умолчаниями. Отказ
`build_subscriptions` (например, `cooldown=-1`) по-прежнему пишется WARNING'ом со
своим текстом.

Подписка, уже лежащая в БД, после отказа пропадает из доверенного скана и уходит
действующим путём промахов (`_ABSENT_GRACE_CYCLES`), как подписка удалённого
декоратора.

### Проверка в декораторе

`rmq_trigger(...)` при вызове запоминает call site — `sys._getframe(1)`: имя файла и
`f_lineno`. На Python 3.10 для многострочного и стекового вызова `f_lineno` указывает
на первую строку вызова. Проверка выполняется в `_attach`, где известен `dag_id` (для
TaskFlow-фабрики это момент вызова фабрики, всё ещё при импорте файла):

1. построить подписки из фактических аргументов; `ValueError` билдера уходит наружу без
   изменений, как сейчас;
2. прочитать и разобрать файл `_read_and_parse`; недоступен или не разбирается (zip-архив,
   кодировка не utf-8) → ошибка «watcher не сможет прочитать этот файл»;
3. найти через `_iter_trigger_functions` вызов `rmq_trigger` в `decorator_list`, чей
   диапазон строк `[lineno, end_lineno]` содержит `f_lineno`. Не найден → ошибка
   «`rmq_trigger` применён не как декоратор над функцией; watcher видит только форму
   `@rmq_trigger(...)` над `@dag(...)`». Сюда попадают прямой вызов
   `rmq_trigger(...)(dag)` и импорт под другим именем (`rmq_trigger as rt`) — watcher
   таких подписок не видит;
4. прочитать все вызовы этой функции `_read_function_triggers`; отказ → ошибка с
   перечнем параметров и причин;
5. `_static_dag_id` функции не вычисляется или не равен `dag_obj.dag_id` → ошибка;
6. сравнить статические подписки этого вызова с подписками из шага 1 с учётом типов
   (`json.dumps(..., sort_keys=True)` обоих списков, чтобы `1`, `1.0` и `True`
   различались) → расхождение → ошибка с перечнем различающихся полей;
7. `_attach` использует подписки из шага 1.

Ошибка — `RMQTriggerNotStaticError(ValueError)` с текстом на английском, как остальные
сообщения провайдера. DAG-процессор превращает её в import error, Airflow показывает
плашку «Broken DAG». Файл читается и разбирается на каждый вызов `_attach` без кэша: в
DAG-процессоре это один-два разбора одного файла.

### Ограничение

Декоратор проверяет только вызовы, которые действительно выполняются при импорте.
Декоратор в фабрике, которую файл не вызывает, или в ветке `if`, которая не исполняется,
watcher по-прежнему видит, а декоратор не проверяет. Для таких мест действует только
отказ watcher'а.

## Technical Details

- `dag_file_scan.py` получает из `listener.py`: `_SCOPE_BOUNDARIES`, `_outer_parts`,
  `_module_scope_nodes`, `_collect_module_constants`, `_DagIdSentinel`,
  `_UNRESOLVED_DAG_ID`, `_extract_dag_id_from_decorators`, `_RMQ_TRIGGER_KWARGS`,
  `_is_rmq_trigger_call`, `_scanned_queue_label`, `_UNREADABLE_KWARG_OUTCOMES`,
  `_warn_unreadable_kwarg`, `_parse_rmq_trigger_decorator`, `_resolve_function_dag_id`.
  В задаче 4 последние четыре заменяются новым кодом.
- Новые функции `dag_file_scan.py`:
  - `_collect_module_constants(tree) -> tuple[dict[str, tuple[Any, int]], set[int]]` —
    значения любого типа, который даёт `ast.literal_eval`, с защитой от мутации для
    `list`/`dict`;
  - `_resolve_static(node, constants) -> Any | _Unresolved` — рекурсивно: `Constant`,
    `Name` из констант, `List`/`Tuple`, `Dict` (ключ `None`, то есть `{**X}`, — отказ),
    `UnaryOp(USub/UAdd)` над числом; результат — глубокая копия;
  - `_read_rmq_trigger_call(call, constants) -> _CallRead` —
    `_CallRead(kwargs: dict, unreadable: list[tuple[str, str]])`, второй элемент —
    `(параметр, причина)`. Позиционные маппятся на
    `("queue", "queues", "conn_id", "filter_data", "cooldown")`; больше пяти позиционных,
    `Starred`, неизвестный явный ключ, `**NAME` не с dict-константой, неизвестный ключ в
    распаковке, повтор параметра между позиционными, ключевыми и `**` — нечитаемо;
  - `_read_and_parse(path) -> ast.Module`, `_usable_constants(...)`,
    `_iter_trigger_functions(tree)`, `_static_dag_id(func, constants)`,
    `_read_function_triggers(func, constants, dag_id) -> _FunctionRead`
    (`subscriptions`, `problems`);
  - `RMQTriggerNotStaticError(ValueError)`, `_verify_static_readability(path, lineno,
    dag_id, runtime_subs)`.
- `listener.py`: `_extract_subscriptions_from_file` работает через `_read_and_parse`,
  `_iter_trigger_functions`, `_static_dag_id` (с WARNING'ом watcher'а при нерезолвимом
  `dag_id` и наличии `@rmq_trigger`) и `_read_function_triggers` (ERROR при отказе).
- `decorators.py`: `rmq_trigger` захватывает `(filename, f_lineno)` до создания
  `decorator`; `_attach` строит подписки из фактических аргументов, вызывает
  `_verify_static_readability` и продолжает с подписками из фактических аргументов.
- Тесты, вызывающие `rmq_trigger(...)(dag)` напрямую, подменяют
  `decorators._verify_static_readability` autouse-фикстурой в `test_decorators.py`;
  переключателя в рабочем коде нет.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): изменения кода, тесты, документация и
  бэклог в этом репозитории
- **Post-Completion** (no checkboxes): аудит DAG-файлов и подписок прода перед
  выкаткой, ручная проверка плашки «Broken DAG» на Airflow 2.9.1

## Implementation Steps

### Task 1: Вынести статическое чтение DAG-файла в `dag_file_scan.py`

**Files:**
- Create: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] перенести в `dag_file_scan.py` все имена из первого пункта Technical Details без
      изменения поведения
- [ ] `listener.py` импортирует перенесённое из `dag_file_scan`;
      `_extract_subscriptions_from_file` остаётся методом listener'а
- [ ] перенаправить импорты и `patch`-цели тестов (`...listener.log` для предупреждений
      парсера и т.п.) на новый модуль
- [ ] тест в подпроцессе: после `import airflow_provider_rmq.watcher.dag_file_scan` в
      `sys.modules` нет `airflow_provider_rmq.watcher.listener`, `.consumer`, `.models`
- [ ] run tests - must pass before next task

### Task 2: Константы любого литерального типа и защита от мутации

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Create: `tests/watcher/test_dag_file_scan.py`

- [ ] `_collect_module_constants` хранит значения любого типа, который даёт
      `ast.literal_eval`
- [ ] для значений, содержащих `list`/`dict`: пометить непригодными имена, стоящие
      корнем цепочки `Subscript`/`Attribute` в `Store`/`Del` и корнем цепочки с вызовом
      метода — обход всего дерева
- [ ] `_extract_dag_id_from_decorators` принимает для `dag_id` только строку; проверка
      «не строка» раньше проверки на пустое значение
- [ ] тесты: строка/число/bool/None/список/кортеж/словарь собираются; двойное связывание,
      `global`, walrus, `import *` — как прежде
- [ ] тесты мутации: `X[k] = v`, `del X[k]`, `X[k] += 1`, `X["a"]["b"] = v`,
      `X["a"].update(...)`, `X.append(...)`, `for X[k] in ...` делают имя непригодным;
      строковая константа с `DAG_ID.lower()` в файле остаётся пригодной
- [ ] тесты `dag_id`: `DAG_ID = None`/`0`/`False` в `dag_id=` нерезолвимы
- [ ] run tests - must pass before next task

### Task 3: Чтение вызова `rmq_trigger` через константы

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Modify: `tests/watcher/test_dag_file_scan.py`

- [ ] `_resolve_static`: литералы, имена констант, вложенные списки/кортежи/словари,
      унарный минус над числом; всё остальное — нечитаемо с причиной
- [ ] `_read_rmq_trigger_call`: ключевые аргументы, позиционные 0–4, `**NAME` с
      dict-константой; правила отказа из Technical Details
- [ ] `_usable_constants` — общий фильтр по области и строке для `dag_id` и аргументов
- [ ] тесты успеха: голое имя, позиционный `QUEUE`, позиционные `conn_id`/`filter_data`/
      `cooldown`, `queues=[Q1, Q2]`, `filter_data={"body.type": PAID}`, `**RMQ_OPTS`,
      смесь `**RMQ_OPTS` и явных аргументов, атрибутная форма `decorators.rmq_trigger(...)`
- [ ] тесты отказа с причиной: импортированное имя, вызов, атрибут, f-строка,
      конкатенация, `{**X}`, `*args`, `**load()`, неизвестный ключ, повтор параметра
      (позиционный + ключевой, ключевой + `**`), константа ниже функции, константа во
      вложенной функции, мутируемая константа
- [ ] тест: значение из словаря констант копируется — изменение результата не меняет
      словарь
- [ ] run tests - must pass before next task

### Task 4: Отказ на уровне функции в скане watcher'а

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`
- Modify: `tests/watcher/test_dag_file_scan.py`

- [ ] `_read_and_parse`, `_iter_trigger_functions`, `_static_dag_id`,
      `_read_function_triggers` в `dag_file_scan.py`; `_resolve_function_dag_id`,
      `_parse_rmq_trigger_decorator`, `_warn_unreadable_kwarg`,
      `_UNREADABLE_KWARG_OUTCOMES` заменяются ими
- [ ] `_read_function_triggers`: нечитаемый вызов или конфликт стекинга `exchange=` —
      ни одной подписки функции, проблемы перечислены; отказ `build_subscriptions` —
      как сейчас, WARNING с его текстом
- [ ] `_extract_subscriptions_from_file` работает через новые функции; ERROR при отказе,
      WARNING про нерезолвимый `dag_id` — в обёртке watcher'а
- [ ] переписать тесты, ожидавшие регистрацию с умолчанием, и тест конфликта стекинга
      `exchange=` (первый вызов остаётся) на ожидание отказа всей функции и ERROR
- [ ] тесты: `conn_id=RMQ_CONN` (константа файла) даёт подписку с `conn_id="rmq_prod"`;
      `conn_id=get_conn()` — ни одной; два стековых вызова, читаемый и нечитаемый, — ни
      одной подписки функции; вложенная фабрика с литералами и DAG под модульным `if`
      дают подписки как прежде
- [ ] тест через `_sync_to_db`: строка, чей декоратор стал нечитаемым, считает промахи и
      удаляется по истечении `_ABSENT_GRACE_CYCLES`, как строка удалённого декоратора
- [ ] run tests - must pass before next task

### Task 5: Чистая проверка читаемости вызова

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Create: `tests/watcher/test_static_readability.py`

- [ ] `RMQTriggerNotStaticError(ValueError)`
- [ ] `_verify_static_readability(path, lineno, dag_id, runtime_subs)`: шаги 2–6 из
      «Проверка в декораторе», своё сообщение на каждый шаг, сравнение через
      `json.dumps(..., sort_keys=True)`
- [ ] тесты на файлах в `tmp_path` без `DagBag`: литералы и константы проходят; файл не
      читается, не utf-8, вызов не найден, вызов нечитаем, `dag_id` не вычисляется или
      другой, значения расходятся (в т.ч. `1` против `True`) — ошибка с ожидаемым текстом
- [ ] тесты поиска: многострочный вызов, несколько `@rmq_trigger` на одной функции,
      вложенная фабрика, DAG под модульным `if`
- [ ] run tests - must pass before next task

### Task 6: Подключить проверку в декоратор

**Files:**
- Modify: `airflow_provider_rmq/watcher/decorators.py`
- Modify: `tests/watcher/test_decorators.py`
- Create: `tests/watcher/test_decorator_static_check.py`

- [ ] `rmq_trigger` захватывает `(filename, f_lineno)` вызывающего кадра
- [ ] `_attach`: подписки из фактических аргументов (его `ValueError` без изменений) →
      `_verify_static_readability` → дальше с подписками из фактических аргументов
- [ ] autouse-фикстура в `test_decorators.py`, подменяющая
      `decorators._verify_static_readability`, для прямых вызовов `rmq_trigger(...)(dag)`
- [ ] тесты через `DagBag` на файлах в `tmp_path`: литералы, константы, вложенная
      фабрика, DAG под модульным `if` импортируются; f-строка, вызов, импортированная
      константа, мутированная константа, декоратор в цикле, `dag_id` из импорта,
      `rmq_trigger(...)(dag)` в DAG-файле, `rmq_trigger as rt`, стековые читаемый и
      нечитаемый вызовы, неизвестный ключ — import error с ожидаемым текстом
- [ ] тест: TaskFlow-фабрика, константа которой меняется между строкой декоратора и
      вызовом `my_dag()`, — import error
- [ ] тест: невалидный литерал (`cooldown=-1`) даёт import error с текстом
      `build_subscriptions`, а не `RMQTriggerNotStaticError`
- [ ] `tests/watcher/test_example_dags_dagbag.py` проходит без подмены
- [ ] run tests - must pass before next task

### Task 7: Verify acceptance criteria

- [ ] нелитеральный, но статически читаемый аргумент даёт подписку с правильным
      значением
- [ ] нечитаемый аргумент: watcher не строит ни одной подписки DAG'а и пишет ERROR, DAG
      не импортируется, плашка «Broken DAG» с понятным текстом
- [ ] позиционные `conn_id`/`filter_data`/`cooldown` читаются
- [ ] DAG'и с литералами во вложенных фабриках и под модульным `if` работают как прежде
- [ ] run full test suite: `pytest tests/ -q`
- [ ] `ruff check airflow_provider_rmq tests` и `mypy airflow_provider_rmq` не добавляют
      новых замечаний

### Task 8: [Final] Update documentation

- [ ] `readme.md:626-630`: какие формы аргументов и `dag_id` читаются, что нечитаемость
      ломает импорт DAG'а
- [ ] docstring `rmq_trigger`: раздел «Decorator order and DAG types» переписать —
      watcher видит только форму `@rmq_trigger(...)` над `@dag(...)`, прямой вызов на
      готовом `DAG` даёт ошибку импорта; абзац про статически читаемые `dag_id` и
      аргументы
- [ ] переписать блоки «Gotcha: literal arguments only» в
      `docs/example_dags/rmq_watcher_jetstat_exchange.py` и
      `docs/example_dags/rmq_watcher_cooldown_multi_queue.py`
- [ ] бэклог: `git rm docs/backlog/rmq-trigger-kwargs-silently-dropped.md`; переписать
      `docs/backlog/ast-dag-id-resolution-incomplete.md` — f-строки, конкатенация,
      импортированные константы, `@dag(**DAG_KWARGS)` и фабрики по-прежнему не
      резолвятся, но на DAG'е с `@rmq_trigger` дают видимую ошибку импорта; `**CONST`
      резолвится для `rmq_trigger`, но не для `@dag(...)`
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Перед выкаткой на прод:**
- просмотреть DAG-файлы прода с `@rmq_trigger` на нечитаемые аргументы (импортированные
  константы, вызовы, f-строки, `rmq_trigger(...)(dag)`, импорт под другим именем): после
  выкатки такие DAG'и перестанут импортироваться. Сейчас их подписки уже работают не
  так, как написано в файле, или не существуют вовсе
- найти подписки, чей `conn_id` сейчас подменён умолчанием (`conn_id=КОНСТАНТА` в файле,
  `rmq_default` в БД). Ключ подписки — `(dag_id, queue_name, conn_id)`, поэтому после
  выкатки появится новая строка с правильным `conn_id`, а старая с `rmq_default` проживёт
  ещё `_ABSENT_GRACE_CYCLES` + 1 сканов со своим consumer'ом: возможен двойной запуск
  DAG'а, флаг `enabled` и правки из UI на новую строку не переносятся. Такие старые
  строки выключить или удалить вручную в момент выкатки

**Ручная проверка:**
- на Airflow 2.9.1: DAG с `conn_id=get_conn()` показывает «Broken DAG» с текстом
  ошибки; после исправления на константу файла плашка пропадает, подписка появляется
  на странице Subscriptions с правильным `conn_id`
