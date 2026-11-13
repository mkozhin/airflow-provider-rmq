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
  2. если в DAG-файле хоть один вызов `@rmq_trigger` статически не читается или
     отвергнут, watcher не регистрирует ни одной подписки этого файла (ERROR в логе)
     вместо подстановки умолчаний;
  3. сам декоратор при импорте DAG прогоняет свой вызов тем же парсером и сравнивает
     результат с фактическими аргументами и `dag_id`. Нечитаемость или расхождение —
     исключение, импорт файла обрывается, Airflow показывает плашку «Broken DAG» и
     теряет все DAG'и этого файла. Подписка, работающая не так, как написано в файле,
     хуже явно сломанного файла.
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
    распаковка `**` (676-681), пропуск неизвестного ключа (682-683), `continue` на
    нечитаемом (688), только `node.args[0]` из позиционных, отказ `build_subscriptions` —
    WARNING и пропуск одного вызова;
  - `RMQWatcherListener._get_dags_folder` (1721) и `_scan_subscriptions` — перечень
    файлов `glob(DAGS_FOLDER/**/*.py)`, скрытые каталоги в него не попадают;
  - `RMQWatcherListener._extract_subscriptions_from_file` (1937) — чтение файла в utf-8,
    `ast.walk` по всем `FunctionDef`/`AsyncFunctionDef`, конфликт стекинга `exchange=`
    (WARNING, первый вызов остаётся); `None` означает «файл не разобран», список —
    подписки файла.
  - Модуль импортирует `consumer` (`aio_pika`, `httpx`), `models`, `runner`.
- `airflow_provider_rmq/watcher/decorators.py` — `rmq_trigger`, `_attach` вызывает
  `build_subscriptions`; ветки готового `DAG` и неисполненной TaskFlow-фабрики.
- `airflow_provider_rmq/watcher/subscription_builder.py` — `build_subscriptions`,
  `has_exchange_conflict`; общий для декоратора и watcher'а.
- `airflow_provider_rmq/watcher/models.py:30` — уникальный ключ подписки
  `(dag_id, queue_name, conn_id)`; `upsert_subscription` (190) находит строку по ключу
  независимо от `source` и перезаписывает `source`, `filter_data`, `group_key`,
  `cooldown`, `ack_unmatched`.
- `airflow_provider_rmq/watcher/consumer.py:279-287` — доставка для отсутствующего,
  неактивного или поставленного на паузу DAG'а подтверждается и пропадает.
- Тесты: `tests/watcher/test_listener.py` (в т.ч. `test_nested_function_with_literal_dag_id_still_resolves`,
  1647), `tests/watcher/test_decorators.py` (около 40 прямых вызовов
  `rmq_trigger(...)(dag)`), `tests/watcher/test_example_dags_dagbag.py` (импорт примеров
  через `DagBag`), `tests/watcher/test_subscription_builder.py`.
- Документация: `readme.md:628-630` и `readme_ru.md:628-630` (абзацы про `dag_id` и про
  литеральные аргументы), `docs/live-verification.md:703-708`, абзацы про стекинг
  `exchange=` с веткой готового `DAG` (`readme.md:769`, `readme_ru.md:768`), docstring
  `rmq_trigger` (`:param exchange:` в `decorators.py:68-73`, раздел «Decorator order and
  DAG types», абзац «dag_id must be statically resolvable»), блоки «Gotcha: literal
  arguments only» в `docs/example_dags/rmq_watcher_jetstat_exchange.py:65-82` и
  `docs/example_dags/rmq_watcher_cooldown_multi_queue.py:46-67`.
- `docs/backlog/*.md` — около 15 пунктов ссылаются на строки `listener.py` ниже
  переносимого блока (в `where:` и в тексте), `ast-dag-id-resolution-incomplete.md` — на
  функцию, которая переезжает в `dag_file_scan.py`.
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
- `_read_and_parse(path)` — чтение файла байтами и `ast.parse(raw_bytes, filename=path)`:
  BOM и объявление кодировки PEP 263 обрабатываются так же, как при импорте; файл,
  который не декодируется или не разбирается, не проходит ни скан watcher'а, ни
  проверку декоратора;
- `_iter_trigger_functions(tree)` — обход `ast.walk` по всем `FunctionDef`/
  `AsyncFunctionDef` с их пригодными константами (`_usable_constants`: пустой словарь
  для вложенных функций, фильтр «объявлена выше» для функций модульного уровня);
- `_static_dag_id(func, constants)` — чистый резолвер `dag_id` без логирования;
  нерезолвимый `dag_id` на функции с `@rmq_trigger` — проблема, которую собирает
  `_read_file_triggers` и watcher пишет ERROR'ом;
- `_read_function_triggers(func, constants, dag_id)` — чтение всех вызовов `rmq_trigger`
  одной функции и сборка её подписок с перечнем проблем;
- `_read_file_triggers(tree)` — подписки всего файла и прочитанные вызовы (строки,
  `dag_id`, подписки каждого) или отказ всему файлу.

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
| константа-список/словарь, используемая не только в `rmq_trigger` | `alias = FILTER`, `FILTER["a"] = 1`, `helper(QUEUES)` | отказ |
| любое упоминание `rmq_trigger` не как вызова в `decorator_list` функции | `rmq_trigger(queue="b")(dag)`, `import ... as rt`, `rt = decorators.rmq_trigger`, `partial(rmq_trigger, ...)` | отказ всему файлу |

Константа пригодна, если: присвоена на верхнем уровне литеральным выражением, связана в
области модуля ровно один раз, объявлена строкой выше функции, функция — модульного
уровня, в файле нет `import *` (действующие правила). Новое правило касается только
констант, значение которых содержит `list` или `dict`: такое имя пригодно, только если
каждое его упоминание в файле, кроме самого присваивания, стоит внутри аргументов вызова
`rmq_trigger` (включая `**NAME`). Любое другое упоминание — алиас, передача в функцию,
чтение по индексу, вызов метода, запись — делает константу непригодной. Правило
намеренно консервативное: изменение через алиас или функцию статически не отследить, а
непригодная константа даёт явный отказ, а не тихое расхождение. Строки и числа
неизменяемы, на них правило не распространяется, поэтому `DAG_ID.lower()` константу не
отравляет.

`dag_id` из константы принимается только строкой: нестроковая константа (`None`, `0`,
`False`) в `dag_id=` нерезолвима — при поиске константы проверка «не строка» идёт
раньше проверки на пустое значение. Литерал сохраняет нынешний порядок: пустой литерал
(`@dag(dag_id=None)`) означает «имя функции», как в Airflow.

### Отказ вместо умолчания — на уровне файла

Исключение декоратора обрывает импорт модуля, и DAG-процессор теряет все DAG'и файла,
а не только тот, на котором стоит нечитаемый вызов. Watcher повторяет это и не
регистрирует ни одной подписки файла, если в нём:
- хоть один вызов `rmq_trigger` в `decorator_list` нечитаем;
- вызовы одной функции конфликтуют по стекингу `exchange=`;
- `build_subscriptions` отвергает хоть один вызов (например, `cooldown=-1`);
- `dag_id` функции с `@rmq_trigger` не вычисляется;
- вызов `rmq_trigger` стоит в `decorator_list` ниже вызова `dag` — тогда
  `rmq_trigger` оборачивает обычную функцию, и вызов фабрики при импорте падает с
  `TypeError` (`decorators.py:220-227`) ещё до проверки декоратора;
- имя `rmq_trigger` или атрибут `.rmq_trigger` упоминается иначе, чем как `func` вызова,
  который сам является элементом `decorator_list` функции: прямой
  `rmq_trigger(...)(dag)`, импорт под другим именем, `rt = decorators.rmq_trigger`,
  `partial(rmq_trigger, ...)`, передача в функцию. Импорт под собственным именем
  (`from ... import rmq_trigger`) допустим. Вызов через любую такую обёртку watcher не
  узнаёт, а декоратор на нём ломает импорт.

ERROR содержит путь, `dag_id`, параметры и причины, включая текст билдера.
`_extract_subscriptions_from_file` возвращает для такого файла пустой список, а не
`None`: файл прочитан и разобран, подписок в нём нет. Исходы
`registered`/`registered_unpacking` исчезают вместе с подменой умолчаниями.

Иначе соседний DAG сломанного файла сохранил бы подписку: Airflow деактивирует его,
watcher продолжает потреблять, а `consumer.py:279-287` подтверждает и теряет каждое
сообщение для неактивного DAG'а, при этом строка в UI показывает `listening`.

Подписки, уже лежащие в БД, после отказа пропадают из доверенного скана и уходят
действующим путём промахов (`_ABSENT_GRACE_CYCLES`), как подписки удалённого
декоратора.

WARNING про нерезолвимый `dag_id` на функции с `@rmq_trigger` становится ERROR'ом и
больше не советует заводить подписку вручную на странице Subscriptions: такой DAG
теперь не импортируется, и подписка на него потребляла бы сообщения впустую.

### Проверка в декораторе

`rmq_trigger(...)` при вызове запоминает call site — `sys._getframe(1)`: имя файла и
`f_lineno`. На Python 3.10 для многострочного и стекового вызова `f_lineno` указывает
на первую строку вызова. Проверка выполняется в `_attach`, где известен `dag_id` (для
TaskFlow-фабрики это момент вызова фабрики, всё ещё при импорте файла):

1. построить подписки из фактических аргументов; `ValueError` билдера уходит наружу без
   изменений, как сейчас;
2. проверить, что watcher вообще сканирует файл call site: путь оканчивается на `.py`,
   лежит внутри `DAGS_FOLDER` и ни один компонент пути после `DAGS_FOLDER`, включая имя
   файла, не начинается с точки — как у `glob(DAGS_FOLDER/**/*.py)`, который пропускает
   и скрытые каталоги, и файлы вроде `.x.py` (Airflow такие файлы грузит). Сравнение по
   `os.path.abspath` без разворота симлинков, потому что glob отдаёт пути под
   `DAGS_FOLDER` как есть. Иначе → ошибка
   «вызов находится в файле, который watcher не сканирует». Сюда попадает фабрика с
   `@rmq_trigger`, объявленная в `plugins/`, пакете из `PYTHONPATH` или скрытом
   каталоге и вызванная из DAG-файла: Airflow такой DAG загрузит, а подписки не появится
   никогда;
3. прочитать и разобрать файл `_read_and_parse`; недоступен или не разбирается (zip-архив,
   кодировка, которую `ast.parse` не принимает) → ошибка «watcher не сможет прочитать
   этот файл». Затем прогнать `_read_file_triggers` по всему файлу: любая проблема, за
   которую watcher откажет файлу, — ошибка с её перечнем. Так декоратор и watcher
   совпадают по всему файлу, а не только по функции этого вызова: неиспользуемый
   `import ... as rt` или нечитаемый вызов в фабрике, которую файл не вызывает, ломают
   импорт, а не молча оставляют файл без подписок;
   Прямой вызов `rmq_trigger(...)(dag)`, алиас, нечитаемый вызов, нерезолвимый `dag_id`
   и неверный порядок декораторов всегда останавливаются здесь, с текстом шага 3;
4. найти через `_iter_trigger_functions` вызов `rmq_trigger` в `decorator_list`, чей
   диапазон строк `[lineno, end_lineno]` содержит `f_lineno`. Не найден → ошибка
   «`rmq_trigger` вызван через имя, которого watcher не узнаёт; watcher видит только
   форму `@rmq_trigger(...)` над `@dag(...)`». Сюда доходят только формы, которых шаг 3
   не видит: переэкспорт под другим именем из чужого модуля (`from common import trig`),
   `getattr(module, "rmq_trigger")`;
5. `_static_dag_id` функции не равен `dag_obj.dag_id` → ошибка (вычислимость уже
   проверил шаг 3);
6. сравнить статические подписки найденного вызова — `build_subscriptions` по
   `_read_rmq_trigger_call(call).kwargs` с `dag_id` из шага 5 — с подписками из шага 1,
   без ключей `dag_id`/`group_key`, которые watcher добавляет к строкам сам. Сравнение —
   рекурсивное `_same_value(a, b)` с учётом типов: скаляры равны при
   `type(a) is type(b)` и `a == b`, так что `1`, `1.0` и `True` различаются; списки и
   кортежи — поэлементно; словари — по совпадению множеств ключей (ключ сравнивается
   вместе с типом, `1` и `"1"` разные) и значениям без сортировки ключей, поэтому
   словарь со смешанными ключами (`{1: "a", "b": "c"}`), который билдер принимает, не
   ломает сравнение → расхождение → ошибка с перечнем различающихся полей;
7. `_attach` использует подписки из шага 1.

Ошибка — `RMQTriggerNotStaticError(ValueError)` с текстом на английском, как остальные
сообщения провайдера. DAG-процессор превращает её в import error, Airflow показывает
плашку «Broken DAG». Файл читается и разбирается на каждый вызов `_attach` без кэша: в
DAG-процессоре это один-два разбора одного файла.

Шаг 2 сравнивает с `airflow.settings.DAGS_FOLDER`. Проверка DAG'ов через
`DagBag(dag_folder=<путь>)` — в тестах провайдера и в CI пользователей — должна выставлять
`DAGS_FOLDER` (`AIRFLOW__CORE__DAGS_FOLDER` или подмена `airflow.settings.DAGS_FOLDER`)
на ту же папку, иначе каждый DAG с `@rmq_trigger` получит ошибку «файл не сканируется
watcher'ом». README говорит это явно.

### Ограничения

Декоратор срабатывает, только если хоть один `@rmq_trigger` файла выполняется при
импорте; тогда шаг 3 проверяет весь файл. Остаются случаи, где watcher видит больше,
чем Airflow создаёт:
- читаемый вызов в коде, который при импорте не исполняется (фабрика, которую файл не
  вызывает, ветка `if`, ложная в этом окружении), или в файле из `.airflowignore`, даёт
  подписку на DAG, которого Airflow не создаёт: доставки подтверждаются и теряются в
  `consumer.py:279-287`, а строку помечает бейдж «dag not found» на странице
  Subscriptions. Это поведение существует и сейчас;
- файл, в котором ни один `@rmq_trigger` не выполняется при импорте, декоратор не
  проверяет; если watcher ему откажет, DAG'и файла работают без подписок, о чём пишет
  ERROR watcher'а.

`rmq_trigger`, переэкспортированный из другого модуля под другим именем
(`from common import trig`), watcher не узнаёт: `@trig(...)` для него не вызов
`rmq_trigger`. Декоратор на таком вызове ломает импорт (шаг 4), а прочие подписки файла
watcher продолжает регистрировать. Это закрывает только аудит перед выкаткой.

`@dag(...)` на функции с `@rmq_trigger` читается, только если `dag` — декоратор Airflow
(`airflow.decorators.dag` или `airflow.models.dag.dag`, по имени или через модуль).
Одноимённая обёртка `dag` из другого модуля (`from company import dag`) может дать DAG'у
другой `dag_id`, чем виден в файле, поэтому такой файл отвергается в обоих процессах — даже
если обёртка передаёт `dag_id` без изменений: скан её не исполняет и доказать этого не может.

Фабрика с `@rmq_trigger`, объявленная вне `DAGS_FOLDER` и вызванная из DAG-файла, ломает
импорт этого файла (шаг 2), но статически watcher связь файла с фабрикой не видит: прочие
подписки файла он продолжает регистрировать, и их доставки теряются для
деактивированных DAG'ов. Это закрывает только аудит перед выкаткой.

## Technical Details

- `dag_file_scan.py` получает из `listener.py`: `_SCOPE_BOUNDARIES`, `_outer_parts`,
  `_module_scope_nodes`, `_collect_module_constants`, `_DagIdSentinel`,
  `_UNRESOLVED_DAG_ID`, `_extract_dag_id_from_decorators`, `_RMQ_TRIGGER_KWARGS`,
  `_is_rmq_trigger_call`, `_scanned_queue_label`, `_UNREADABLE_KWARG_OUTCOMES`,
  `_warn_unreadable_kwarg`, `_parse_rmq_trigger_decorator`, `_resolve_function_dag_id`.
  В задаче 4 `_scanned_queue_label`, `_UNREADABLE_KWARG_OUTCOMES`,
  `_warn_unreadable_kwarg`, `_parse_rmq_trigger_decorator` и `_resolve_function_dag_id`
  заменяются новым кодом, и у модуля не остаётся логгера: ничего в нём не логирует.
  `_DagIdSentinel`, `_UNRESOLVED_DAG_ID` и `_extract_dag_id_from_decorators` сведены в
  один `_static_dag_id(func, constants) -> str | None`.
- Новые функции `dag_file_scan.py`:
  - `_collect_module_constants(tree) -> tuple[dict[str, tuple[Any, int]], set[int]]` —
    значения, которые `_resolve_static` читает без констант: JSON-скаляры и списки,
    кортежи, словари из них; константа со значением, содержащим `list`/`dict`, пригодна,
    только если все её упоминания стоят внутри аргументов вызовов `rmq_trigger`;
  - `_resolve_static(node, constants) -> Any`, отказ — исключение `_Unreadable` с
    причиной, которое `_read_rmq_trigger_call` ловит один раз на аргумент. Рекурсивно:
    `Constant` JSON-скалярного типа (`str`, `int`, `float`, `bool`, `None`), `Name` из
    констант (значение константы как есть, без копии), `List`/`Tuple`, `Dict` (ключ
    `None`, то есть `{**X}`, — отказ), `UnaryOp(USub/UAdd)` над `int`/`float`. Литерал
    множества, `bytes`, комплексное число и всё остальное — отказ: подписка хранится
    как JSON;
  - `_read_rmq_trigger_call(call, constants) -> _CallRead` —
    `_CallRead(kwargs: dict, unreadable: list[tuple[str, str]])`, второй элемент —
    `(параметр, причина)`. Позиционные маппятся на
    `("queue", "queues", "conn_id", "filter_data", "cooldown")`; больше пяти позиционных,
    `Starred`, неизвестный явный ключ, `**NAME` не с dict-константой, неизвестный ключ в
    распаковке, повтор параметра между позиционными, ключевыми и `**` — нечитаемо;
  - `_read_and_parse(path) -> ast.Module`, `_usable_constants(...)`,
    `_iter_trigger_functions(tree)`, `_static_dag_id(func, constants)`,
    `_read_function_triggers(func, constants, dag_id) -> _TriggerRead`
    (`subscriptions`, `problems`, `calls`: для каждого прочитанного вызова — первая и
    последняя строка, `dag_id` и подписки `build_subscriptions`; отказ
    `build_subscriptions` — тоже проблема с текстом билдера);
  - `_NameOrigins(tree)` — к какому импорту привязано имя в декораторах функции: имя
    разрешается в области видимости, где выполняется декоратор (объемлющая функция
    целиком, класс — только непосредственно объемлющий, затем модуль); на уровне модуля
    учитываются привязки выше строки `def` (вне цикла), а для DAG-функции внутри фабрики
    (`def` уровня модуля вне цикла) — привязки выше `def` фабрики и те ниже, если файл
    не упоминает фабрику, упоминает её не как вызываемое в `ast.Call` (`make = create_dag`,
    `FACTORIES.append(create_dag)`) или хотя бы один вызов выполняется не до привязки
    (после неё, в функции, классе, лямбде, включении или цикле; `dag = create_dag()`
    выполняет вызов до привязки); `:=` ниже фабрики учитывается всегда; `global` в любой функции файла
    считается привязкой; локальные имена других функций, лямбд и включений не учитываются;
  - `_trigger_origin_problems(tree, origins)` — вызов `rmq_trigger` в `decorator_list` читается,
    только если имя связано абсолютным импортом из
    `airflow_provider_rmq.watcher.decorators` (или `airflow_provider_rmq.watcher`), а
    `<module>.rmq_trigger` — если `<module>` связан импортом с одним из этих модулей;
    одноимённый `rmq_trigger` другого модуля, относительный импорт и `import *` —
    проблема файла;
  - `_dag_origin_problems(tree, origins)` — то же для `@dag(...)` на функции с `@rmq_trigger`:
    читается только `dag`, связанный абсолютным импортом с `airflow.decorators.dag` или
    `airflow.models.dag.dag` (оба пути есть в Airflow 2.9 и указывают на одну функцию), и
    `<module>.dag` с `<module>`, связанным с `airflow.decorators` или `airflow.models.dag`;
    иное — проблема файла;
  - `_read_file_triggers(tree) -> _TriggerRead` (`subscriptions`, `problems`, `calls`): проблема
    хоть одной функции, нерезолвимый `dag_id` на функции с `@rmq_trigger`, упоминание
    имени `rmq_trigger` или атрибута `.rmq_trigger` иначе, чем как `func` вызова в
    `decorator_list` функции (кроме импорта под собственным именем), проблема
    `_trigger_origin_problems` — пустые `subscriptions` у всего файла; то же, если
    `rmq_trigger` в `decorator_list` стоит ниже вызова `dag` или ниже первого вызова
    `rmq_trigger` стоит декоратор, не являющийся вызовом `rmq_trigger` или `dag`
    (`_other_decorator_problems`);
  - `_watcher_scans(path, dags_folder) -> bool` — тот же отбор, что у
    `glob(DAGS_FOLDER/**/*.py)`: `.py`, внутри папки по `os.path.abspath` (а если нет —
    по `os.path.realpath` обоих путей, для модуля, импортированного через другое
    написание той же папки), ни один компонент пути после папки, включая имя файла, не
    начинается с точки;
  - `_analyse_file(path)` — результат `_read_file_triggers` (или исключение чтения и
    разбора) в словаре модуля по абсолютному пути и версии файла
    `(st_ino, st_mtime_ns, st_size)`: файл читается и разбирается один раз на версию,
    сколько бы вызовов `@rmq_trigger` в нём ни проверялось;
  - `RMQTriggerNotStaticError(ValueError)`, `_verify_static_readability(path, lineno,
    dag_id, runtime_subs, dags_folder)` — шаги 3–5 ищут свой вызов в `calls` анализа
    файла и сравнивают его `dag_id` и подписки, второго чтения вызова нет.
- `listener.py`: `_extract_subscriptions_from_file` работает через `_read_and_parse` и
  `_read_file_triggers`, при проблемах пишет ERROR и возвращает пустой список.
- `decorators.py`: `rmq_trigger` захватывает `(filename, f_lineno)` до создания
  `decorator`; `_attach` строит подписки из фактических аргументов, вызывает
  `_verify_static_readability` с `airflow.settings.DAGS_FOLDER` и продолжает с подписками
  из фактических аргументов.
- `rmq_trigger(...)` на готовом `DAG` сразу бросает `TypeError`, без проверки файла:
  такой вызов скан не видит никогда.
- Тесты `test_decorators.py` применяют `rmq_trigger(...)` к фабрикам из тестового
  модуля и подменяют `decorators._verify_static_readability` autouse-фикстурой;
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

- [x] перенести в `dag_file_scan.py` все имена из первого пункта Technical Details без
      изменения поведения
- [x] `listener.py` импортирует перенесённое из `dag_file_scan`;
      `_extract_subscriptions_from_file` остаётся методом listener'а
- [x] перенаправить импорты и `patch`-цели тестов (`...listener.log` для предупреждений
      парсера и т.п.) на новый модуль
- [x] тест в подпроцессе: после `import airflow_provider_rmq.watcher.dag_file_scan` в
      `sys.modules` нет `airflow_provider_rmq.watcher.listener`, `.consumer`, `.models`
- [x] docstring модуля `subscription_builder.py` (3-9) и упоминания расположения парсера
      в перенесённых docstring'ах и комментариях указывают на `dag_file_scan.py`
- [x] run tests - must pass before next task

### Task 2: Константы любого литерального типа и защита от мутации

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Create: `tests/watcher/test_dag_file_scan.py`
- Modify: `tests/watcher/test_listener.py`

- [x] `_collect_module_constants` хранит значения любого типа, который даёт
      `ast.literal_eval`; `test_non_string_constant_ignored` (`DAG_ID = 123` → `{}`)
      переписать под новое поведение сбора
- [x] для значений, содержащих `list`/`dict`: имя пригодно, только если каждое его
      упоминание в файле, кроме присваивания, стоит внутри аргументов вызова
      `rmq_trigger` — обход всего дерева
- [x] `_extract_dag_id_from_decorators` принимает константу для `dag_id` только строкой;
      при поиске константы проверка «не строка» раньше проверки на пустое значение;
      ветка литерала сохраняет порядок «сначала пустое значение»
- [x] docstring'и `_collect_module_constants` (только строки, WARNING у вызывающего) и
      `_extract_dag_id_from_decorators` (константы как `dict[str, str]`) описывают
      значения любого типа и правило для `list`/`dict`
- [x] тесты: строка/число/bool/None/список/кортеж/словарь собираются; двойное связывание,
      `global`, walrus, `import *` — как прежде
- [x] тесты изменяемых констант: `X[k] = v`, `del X[k]`, `X["a"]["b"] = v`,
      `X.append(...)`, `alias = X` (и запись через `alias`), `helper(X)`, чтение `X["a"]`
      вне `rmq_trigger` делают имя непригодным; `X` только в `filter_data=X` и в `**X`
      остаётся пригодным; строковая константа с `DAG_ID.lower()` в файле остаётся
      пригодной
- [x] тесты `dag_id`: `DAG_ID = None`/`0`/`False` в `dag_id=` нерезолвимы;
      `@dag(dag_id=None)` литералом по-прежнему даёт имя функции
      (`test_falsy_literal_dag_id_returns_none` проходит без изменений)
- [x] run tests - must pass before next task

### Task 3: Чтение вызова `rmq_trigger` через константы

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Modify: `tests/watcher/test_dag_file_scan.py`

- [x] `_resolve_static`: литералы, имена констант, вложенные списки/кортежи/словари,
      унарный минус над числом; всё остальное — нечитаемо с причиной
- [x] `_read_rmq_trigger_call`: ключевые аргументы, позиционные 0–4, `**NAME` с
      dict-константой; правила отказа из Technical Details
- [x] `_usable_constants` — общий фильтр по области и строке для `dag_id` и аргументов
- [x] тесты успеха: голое имя, позиционный `QUEUE`, позиционные `conn_id`/`filter_data`/
      `cooldown`, `queues=[Q1, Q2]`, `filter_data={"body.type": PAID}`, `**RMQ_OPTS`,
      смесь `**RMQ_OPTS` и явных аргументов, атрибутная форма `decorators.rmq_trigger(...)`
- [x] тесты отказа с причиной: импортированное имя, вызов, атрибут, f-строка,
      конкатенация, `{**X}`, `*args`, `**load()`, неизвестный ключ, повтор параметра
      (позиционный + ключевой, ключевой + `**`), константа ниже функции, константа во
      вложенной функции, мутируемая константа
- [x] тест: значение из словаря констант копируется — изменение результата не меняет
      словарь
- [x] run tests - must pass before next task

### Task 4: Отказ на уровне файла в скане watcher'а

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`
- Modify: `tests/watcher/test_dag_file_scan.py`

- [x] `_read_and_parse`, `_iter_trigger_functions`, `_static_dag_id`,
      `_read_function_triggers`, `_read_file_triggers` в `dag_file_scan.py`;
      `_resolve_function_dag_id`, `_parse_rmq_trigger_decorator`,
      `_warn_unreadable_kwarg`, `_UNREADABLE_KWARG_OUTCOMES` заменяются ими
- [x] `_read_function_triggers`: нечитаемый вызов, конфликт стекинга `exchange=` или
      отказ `build_subscriptions` — проблема с причиной (для билдера — его текст)
- [x] `_read_and_parse` читает байты и разбирает `ast.parse(raw_bytes, filename=path)`
- [x] `_read_file_triggers`: проблема хоть одной функции, нерезолвимый `dag_id` на
      функции с `@rmq_trigger`, упоминание имени `rmq_trigger` или атрибута
      `.rmq_trigger` иначе, чем как `func` вызова в `decorator_list` функции (импорт под
      собственным именем допустим), `rmq_trigger` ниже `dag` в `decorator_list` — ни одной
      подписки файла
- [x] docstring `_extract_subscriptions_from_file` и комментарии, ссылающиеся на
      `_parse_rmq_trigger_decorator` (`_is_rmq_trigger_call`, проверка в
      `_extract_dag_id_from_decorators`), описывают отказ всему файлу и ERROR
- [x] `_extract_subscriptions_from_file` работает через новые функции; при проблемах —
      ERROR с путём, `dag_id`, параметрами и причинами и пустой список (не `None`);
      сообщение про нерезолвимый `dag_id` не советует заводить подписку в UI
- [x] переписать тесты, ожидавшие регистрацию с умолчанием, пропуск одного вызова при
      отказе `build_subscriptions` и тест конфликта стекинга `exchange=` (первый вызов
      остаётся) на ожидание отказа всего файла и ERROR
- [x] тесты: `conn_id=RMQ_CONN` (константа файла) даёт подписку с `conn_id="rmq_prod"`;
      `conn_id=get_conn()` — ни одной; два стековых вызова, читаемый и нечитаемый, — ни
      одной подписки файла; стековые `queue="a"` и `queue="b", cooldown=-1` — ни одной, в
      ERROR текст билдера; файл с двумя DAG'ами, один из которых нечитаем, — ни одной
      подписки; читаемый TaskFlow-DAG рядом с `rmq_trigger(queue="b")(DAG("B"))`, рядом с
      `from ... import rmq_trigger as rt` (использованным и неиспользованным), рядом с
      `rt = decorators.rmq_trigger` и рядом с `RMQ = partial(rmq_trigger, ...)` — ни одной
      подписки файла; `from ... import rmq_trigger` и `decorators.rmq_trigger(...)` в
      `decorator_list` проходят; DAG с `@dag` над `@rmq_trigger` рядом с читаемым DAG'ом —
      ни одной подписки файла; файл в UTF-8 с BOM разбирается; вложенная фабрика с
      литералами и DAG под модульным `if` дают подписки как прежде
- [x] тест через `_sync_to_db`: строки файла, в котором один декоратор стал нечитаемым,
      считают промахи и удаляются по истечении `_ABSENT_GRACE_CYCLES`, как строки
      удалённого декоратора
- [x] run tests - must pass before next task

### Task 5: Чистая проверка читаемости вызова

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Create: `tests/watcher/test_static_readability.py`

- [x] `RMQTriggerNotStaticError(ValueError)`, `_watcher_scans(path, dags_folder)`
- [x] `_verify_static_readability(path, lineno, dag_id, runtime_subs, dags_folder)`:
      шаги 2–6 из «Проверка в декораторе», своё сообщение на каждый шаг, сравнение
      статических подписок найденного вызова без `dag_id`/`group_key` через `_same_value`
- [x] тесты `_same_value`: `1`/`1.0`/`True` различаются, `{1: x}` и `{"1": x}` различаются,
      одинаковый словарь со смешанными ключами `{1: "a", "b": "c"}` равен себе без
      исключения, вложенные списки и словари сравниваются поэлементно
- [x] тесты `_watcher_scans`: файл в корне и в подкаталоге — да; вне папки, в каталоге с
      точкой, файл `.x.py` в корне и в подкаталоге, не `.py` — нет; результат совпадает
      с `glob(DAGS_FOLDER/**/*.py)` на дереве в `tmp_path`, где есть все эти случаи
- [x] тесты на файлах в `tmp_path` без `DagBag`: литералы и константы проходят; файл вне
      `DAGS_FOLDER`, в скрытом каталоге или с точкой в имени, файл не читается, не
      декодируется, в файле есть проблема `_read_file_triggers` вне функции этого
      вызова (неиспользуемый алиас, нечитаемый вызов в невызываемой фабрике), вызов не
      найден (переэкспорт под другим именем — текст шага 4), вызов нечитаем или `dag_id`
      не вычисляется (текст шага 3), `dag_id` другой (шаг 5), значения расходятся (в т.ч.
      `1` против `True`, шаг 6) — ошибка с ожидаемым текстом
- [x] тесты поиска: многострочный вызов, несколько `@rmq_trigger` на одной функции,
      вложенная фабрика, DAG под модульным `if`; файл в UTF-8 с BOM проходит
- [x] run tests - must pass before next task

### Task 6: Подключить проверку в декоратор

**Files:**
- Modify: `airflow_provider_rmq/watcher/decorators.py`
- Modify: `tests/watcher/test_decorators.py`
- Modify: `tests/watcher/test_example_dags_dagbag.py`
- Create: `tests/watcher/test_decorator_static_check.py`

- [x] `rmq_trigger` захватывает `(filename, f_lineno)` вызывающего кадра
- [x] `_attach`: подписки из фактических аргументов (его `ValueError` без изменений) →
      `_verify_static_readability` → дальше с подписками из фактических аргументов
- [x] autouse-фикстура в `test_decorators.py`, подменяющая
      `decorators._verify_static_readability`, для прямых вызовов `rmq_trigger(...)(dag)`
- [x] тесты через `DagBag` подменяют `airflow.settings.DAGS_FOLDER` на папку, которую
      грузят (`tmp_path`), — без этого шаг 2 отвергает каждый файл
- [x] тесты через `DagBag` на файлах в `tmp_path`: литералы, константы, вложенная
      фабрика, DAG под модульным `if` импортируются; f-строка, вызов, импортированная
      константа, мутированная константа, декоратор в цикле, `dag_id` из импорта,
      `rmq_trigger(...)(dag)` в DAG-файле, `rmq_trigger as rt` (в том числе
      неиспользуемый рядом с литеральным `@rmq_trigger`), `@RMQ(...)` с
      `RMQ = partial(rmq_trigger, ...)`, стековые читаемый и нечитаемый вызовы,
      неизвестный ключ — import error с текстом шага 3 (перечень проблем файла);
      `@trig(...)` при `from helper import trig`, где `helper.py` вне `DAGS_FOLDER`
      переэкспортирует `rmq_trigger`, — import error с текстом шага 4
- [x] тест: TaskFlow-фабрика, константа которой меняется между строкой декоратора и
      вызовом `my_dag()`, — import error
- [x] тест: фабрика с `@rmq_trigger` в модуле вне `DAGS_FOLDER`, вызванная из DAG-файла, —
      import error «файл не сканируется watcher'ом»
- [x] тест: файл с двумя DAG'ами, один нечитаем, — import error всего файла, `DagBag` не
      содержит ни одного из них
- [x] тест: невалидный литерал (`cooldown=-1`) даёт import error с текстом
      `build_subscriptions`, а не `RMQTriggerNotStaticError`
- [x] фикстура `dagbag` в `tests/watcher/test_example_dags_dagbag.py` подменяет
      `airflow.settings.DAGS_FOLDER` на `EXAMPLE_DAGS_DIR`; проверку
      `_verify_static_readability` не подменяет — все примеры проходят её как есть
- [x] тест: `DagBag` по папке примеров без подмены `DAGS_FOLDER` даёт import error «файл не
      сканируется watcher'ом» — так пользователь видит ненастроенный CI
- [x] run tests - must pass before next task

### Task 7: Verify acceptance criteria

- [x] нелитеральный, но статически читаемый аргумент даёт подписку с правильным
      значением
- [x] нечитаемый аргумент: watcher не строит ни одной подписки файла и пишет ERROR, файл
      не импортируется, плашка «Broken DAG» с понятным текстом
- [x] вызов из файла, который watcher не сканирует, — ошибка импорта
- [x] позиционные `conn_id`/`filter_data`/`cooldown` читаются
- [x] DAG'и с литералами во вложенных фабриках и под модульным `if` работают как прежде
- [x] run full test suite: `pytest tests/ -q`
- [x] `ruff check airflow_provider_rmq tests` и `mypy airflow_provider_rmq` не добавляют
      новых замечаний

### Task 8: [Final] Update documentation

- [x] `readme.md:628-630` и `readme_ru.md:628-630` (абзац 626 про доверенный скан не
      трогать): какие формы аргументов и `dag_id`
      читаются, что нечитаемость ломает импорт всего файла, что `@rmq_trigger` должен
      стоять в файле, который сканирует watcher (внутри `DAGS_FOLDER`, без точки в
      начале имени каталога или файла), что проверка DAG'ов через
      `DagBag(dag_folder=...)` в CI должна выставлять `DAGS_FOLDER` на ту же папку
- [x] `readme.md:769` и `readme_ru.md:768`: убрать ветку «готовый инстанс `DAG`» из абзаца
      про стекинг `exchange=`
- [x] docstring `rmq_trigger`: `:param exchange:` (`decorators.py:68-73`) без ветки
      готового `DAG`; раздел «Decorator order and DAG types» переписать — watcher видит
      только форму `@rmq_trigger(...)` над `@dag(...)` в файле внутри `DAGS_FOLDER`,
      прямой вызов на готовом `DAG` даёт ошибку импорта; абзац про статически читаемые
      `dag_id` и аргументы; в разделе cooldown — инфраструктура поднимается на
      наименьшем `conn_id` cooldown-подписок (`decorators.py:119` называет «первое
      найденное соединение»); `TypeError` при неверном порядке декораторов возникает при
      вызове фабрики, а не при декорировании (`decorators.py:162`)
- [x] `docs/live-verification.md:703-708`: нечитаемый аргумент — import error и отказ
      watcher'а всему файлу с ERROR; ссылку на удаляемый
      `rmq-trigger-kwargs-silently-dropped.md` убрать
- [x] переписать блоки «Gotcha: literal arguments only» целиком:
      `docs/example_dags/rmq_watcher_jetstat_exchange.py:65-82` и
      `docs/example_dags/rmq_watcher_cooldown_multi_queue.py:46-67`
- [x] перенаправить ссылки `listener.py:N` в `docs/backlog/` (`where:` и текст) на строки
      после задач 1 и 4; ссылки на перенесённые функции, включая `where:`
      `ast-dag-id-resolution-incomplete.md`, — на `dag_file_scan.py`
- [x] бэклог: `git rm docs/backlog/rmq-trigger-kwargs-silently-dropped.md`; переписать
      `docs/backlog/ast-dag-id-resolution-incomplete.md` — f-строки, конкатенация,
      импортированные константы, `@dag(**DAG_KWARGS)` и фабрики по-прежнему не
      резолвятся, но на DAG'е с `@rmq_trigger` дают видимую ошибку импорта; `**CONST`
      резолвится для `rmq_trigger`, но не для `@dag(...)`; ссылку
      `[[rmq-trigger-kwargs-silently-dropped]]` (строка 55) убрать
- [x] move this plan to `docs/plans/completed/` — переносит harness после завершения
      плана; путь файла в ветке не меняется

## Post-Completion

*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Перед выкаткой на прод.** Основа аудита — прогнать новый `_read_file_triggers` по
каждому файлу папки DAG'ов прода и сравнить результат с таблицей
`rmq_watcher_subscriptions`: он находит нечитаемые вызовы и любые упоминания
`rmq_trigger` не в позиции декоратора. Скан не находит фабрики с `@rmq_trigger` вне
`DAGS_FOLDER` и `rmq_trigger`, переэкспортированный из другого модуля под другим именем, —
их искать grep'ом по `rmq_trigger` в модулях, которые импортируют DAG-файлы. CI, проверяющий DAG'и через `DagBag`, должен выставлять `DAGS_FOLDER` до
выкатки, иначе отметит все DAG'и с `@rmq_trigger` сломанными.

- **Файлы, которые перестанут импортироваться.** DAG-файлы с `@rmq_trigger`, где есть
  нечитаемые аргументы (импортированные константы, вызовы, f-строки, изменяемые
  константы, используемые не только в `rmq_trigger`), `rmq_trigger(...)(dag)`,
  `rmq_trigger` под другим именем, `dag` не из Airflow (`from company import dag` на
  функции с `@rmq_trigger`, даже обёртка, не меняющая `dag_id`; искать grep'ом
  `import dag` в DAG-файлах), любой другой декоратор ниже `@rmq_trigger` (даже с
  `functools.wraps`; искать декораторы между `@rmq_trigger` и `def`), привязка `dag` на
  уровне модуля ниже фабрики с вложенным `@rmq_trigger`, если фабрика вызывается после неё, передаётся дальше без вызова (`FACTORIES.append(create_dag)`) или не упоминается в своём файле вовсе (искать `with DAG(...) as dag:` и `dag = ...` ниже фабрик), или нерезолвимый `dag_id` (f-строка, конкатенация,
  `@dag(**KW)`, импортированная константа, константа во вложенной фабрике; в логе
  шедулера — `cannot statically resolve dag_id`). Ломается весь файл, включая соседние
  DAG'и без `@rmq_trigger`. Сюда же файлы, вызывающие фабрики с `@rmq_trigger` вне
  `DAGS_FOLDER`, в скрытых каталогах или в файлах с точкой в имени.
- **UI-строки на DAG'и сломанных файлов.** Любая строка `source='ui'`, чей `dag_id` живёт
  в файле из предыдущего пункта, включая соседние DAG'и без `@rmq_trigger`. Такие строки
  особенно вероятны для прямого вызова и вызова через алиас (watcher их не видел, и
  подписку заводили вручную) и для нерезолвимого `dag_id` (прежний WARNING советовал
  завести подписку в UI). После выкатки DAG деактивируется, а UI-строка продолжит
  потреблять: `consumer.py:279-287` подтвердит и потеряет каждое сообщение, `_sync_to_db`
  UI-строки не трогает. Для каждого такого файла — исправить его до выкатки или
  выключить эти строки в момент выкатки.
- **`dag_file`-строки сломанных файлов.** Скан отдаёт для такого файла пустой список, но
  его существующие строки живут, пока их не удалит счётчик промахов: 2 цикла для
  неактивного DAG'а, иначе 3 (по 60 с по умолчанию). Если Airflow деактивирует DAG
  раньше, доставки в этом окне подтверждаются и теряются. В момент выкатки выключить
  все включённые подписки каждого ломающегося файла, независимо от `source`, или
  исправить файл заранее.
- **UI-строки, которые заберёт скан.** `upsert_subscription` находит строку по
  `(dag_id, queue_name, conn_id)` независимо от `source`. Если ключ, который скан
  начнёт читать, совпадёт с UI-строкой, она молча станет `dag_file`: её значения
  перезапишутся из файла, редактировать её в UI станет нельзя, а при пропаже из скана
  её удалит счётчик промахов. Если ключ отличается только `conn_id`, рядом с UI-строкой
  появится второй consumer. Такие строки сверить с файлом и удалить или привести к
  нему до выкатки (связанный пункт бэклога — `ui-subscription-taken-over-by-scan`).
- **Ключ поменяется: задвоение.** Подписки, чей `conn_id` сейчас подменён умолчанием
  (`conn_id=КОНСТАНТА` или позиционный `rmq_trigger("q", None, "rmq_prod")` в файле,
  `rmq_default` в БД). После выкатки появится новая строка с правильным `conn_id`, а
  старая проживёт ещё `_ABSENT_GRACE_CYCLES` + 1 сканов со своим consumer'ом: возможен
  двойной запуск DAG'а, `enabled` и правки из UI на новую строку не переносятся. Старые
  строки выключить или удалить вручную в момент выкатки.
- **Значения поменяются на месте.** Строки, сохраняющие ключ, у которых
  `filter_data`/`cooldown`/`ack_unmatched` заданы константой или позиционно и до сих пор
  подменялись умолчанием. Первый скан после выкатки перезапишет их: фильтр начнёт
  отвергать сообщения (при `ack_unmatched=False` они крутятся в очереди), `cooldown`
  переключит подписку в режим отложенного запуска через pending-очередь,
  `ack_unmatched=True` начнёт подтверждать и терять несовпавшие сообщения — это
  необратимо. Каждую такую строку проверить, что файл действительно хочет этих
  значений.
- **Биндинги exchange-подписок поменяются на месте.** Нечитаемые `routing_keys`,
  `routing_key_ids`, `routing_key_status` сейчас отбрасываются, и подписка строится из
  остального: `routing_key_ids=["job"], routing_key_status=STATUS` даёт `job.*`. После
  выкатки `STATUS = "succeeded"` превратит биндинг в `job.succeeded`, и сверка биндингов
  снимет `*` — часть сообщений перестанет приходить; константные `routing_keys` рядом с
  литеральными `routing_key_ids` добавят биндинги и новый трафик. Ключ строки
  (`rmq_watcher.sub.{dag_id}`) при этом не меняется, так что изменение молчаливое. Для
  exchange-подписок, где routing-параметры заданы константой, позиционно или через
  `**CONST`, до выкатки сравнить биндинги, которые сверка добавит и снимет, с текущими.
- **Подписки, которые появятся впервые.** Вызовы, где очередь или exchange заданы
  константой (`queue=ORDERS_Q`, позиционный `QUEUE`, `queues=[Q1, Q2]`,
  `routing_keys=RKS`), сейчас не регистрируют ничего. После выкатки это новые
  `dag_file`-строки: consumer прицепится к очереди, где может лежать многомесячный
  бэклог (каждое сообщение — запуск DAG'а), на очереди, которую читает другое
  приложение, заберёт и подтвердит часть доставок, а exchange-режим объявит на проде
  новые exchange, очереди и биндинги. Новая строка создаётся включённой, и consumer
  стартует в том же цикле, поэтому выключить её «при первом появлении» не успеть. Для
  каждого ключа без существующей строки до выкатки проверить глубину очереди и её
  потребителей, затем либо осушить очередь, либо заранее завести строку с этим ключом
  выключенной: скан `dag_file` сохраняет `enabled` существующей строки
  (`upsert_subscription`, `models.py:217-219`), и подписку включают вручную после
  проверки.
- **Cooldown на одном `conn_id`.** Cooldown-инфраструктура поднимается только на
  `min(conn_id)` cooldown-подписок (`consumer.py:1606-1615`); доставки на других
  `conn_id` возвращаются в очередь, и DAG не срабатывает никогда
  (`_report_split_cooldown`, `consumer.py:1664-1686`). Читаемые после выкатки
  `conn_id`/`cooldown` могут развести cooldown-подписки по нескольким `conn_id` — и
  временно, пока старая строка `rmq_default` живёт рядом с новой, и навсегда. Если
  `min()` сменится, pending-сообщения на старом vhost уйдут в `rmq_watcher.fire` без
  consumer'а. До выкатки собрать множество `(conn_id, cooldown > 0)` после выкатки и
  убедиться, что все cooldown-подписки на одном `conn_id`, а fire-`conn_id` либо не
  меняется, либо его pending-очереди осушены.

**Ручная проверка:**
- на Airflow 2.9.1: DAG с `conn_id=get_conn()` показывает «Broken DAG» с текстом
  ошибки; после исправления на константу файла плашка пропадает, подписка появляется
  на странице Subscriptions с правильным `conn_id`
