# AST-резолвинг dag_id для модульных констант

## Обзор

`RMQWatcherListener` сканирует DAG-файлы через `ast` (никогда их не исполняя,
чтобы не ловить import lock Scheduler'а), чтобы связать декораторы
`@rmq_trigger` с правильным `dag_id`. Сейчас он резолвит `dag_id=` только
если это строковый литерал (`dag_id='my_dag'`). Любое другое значение —
включая очень частый паттерн модульной константы (`DAG_NAME = 'my_dag'` ...
`dag_id=DAG_NAME`) — заставляет парсер откатываться на имя декорированной
python-функции. Получившийся subscription регистрируется под dag_id,
который не соответствует никакому реальному Airflow DAG. При этом в UI RMQ
Watcher он выглядит рабочим (`status: listening`), что активно вводит в
заблуждение: коннекшн и консьюмер реальные, но реальный DAG при совпадающем
сообщении никогда не запускается.

Если быть точным насчёт того, какие сигналы об этом есть уже сегодня, до
этого плана — их **два**, оба лог-only:
1. `_extract_dag_id_from_decorators` уже сегодня ловит `ValueError` от
   `ast.literal_eval(<Name>)` и пишет `log.warning("rmq_trigger: dag_id= is
   not a string literal — falling back to function name")` перед откатом на
   имя функции — то есть откат не полностью тихий. Но этот warning
   **не** пишется на каждом reconcile-цикле: `_scan_subscriptions` пропускает
   файлы с неизменившимся `mtime`, так что `_extract_subscriptions_from_file`
   (и этот warning) реально выполняется только при первом цикле после
   старта/рестарта Scheduler'а и при изменении самого файла. Плюс у него два
   изъяна: он ничего не говорит про путь файла/какая это функция, и —
   важнее — он срабатывает для **любого** `@dag(dag_id=<не-литерал>)` в
   **любом** DAG-файле, который сканирует watcher, даже если на этой функции
   вообще нет `@rmq_trigger` — то есть сегодня это ещё и источник
   постороннего шума в логах для DAG, никак не связанных с этим плагином.
2. `consumer.py::_sync_trigger` (строки 47-71) запрашивает
   `DagModel.filter_by(dag_id=..., is_active=True, is_paused=False)` перед
   вызовом `trigger_dag()`; если запрос ничего не находит, пишется
   `log.warning("DAG %s not found, inactive or paused — message acked,
   skipping trigger", dag_id)` и сообщение ACK-ается без запуска чего-либо.
   Это реактивный сигнал — срабатывает только когда реально приходит и
   потребляется подходящее сообщение.

Этот план не изобретает сигнал с нуля: он переносит и уточняет warning (1) —
теперь с путём файла и именем функции, и только когда `@rmq_trigger` реально
присутствует, убирая посторонний шум.

Баг найден на реальном DAG'е, `donstroy_pipeline_spark_all.py` (внешний
репозиторий `project-donstroy`): `DAG_NAME = 'donstroy_pipeline_spark_all'`
на уровне модуля, `dag_id=DAG_NAME` в `@dag(...)`, функция называется
`data_proc`. Subscription тихо регистрировался под `dag_id="data_proc"`,
которого не существует — DAG никогда не триггерился из RabbitMQ.

Этот план:
1. Учит AST-парсер резолвить простые модульные строковые константы,
   на которые ссылается `dag_id=`.
2. Убирает тихий, активно-неверный fallback на имя функции для случая, когда
   `dag_id=` указан, но не резолвится — заменяет его на пропуск регистрации
   + явный warning, указывающий на UI как на ручной запасной путь.

Явно вне скоупа этого плана (отдельные задачи на будущее, здесь не
решаются и никаких файлов под них этот план не правит):
- f-строки / конкатенация / `dag_id=`, вычисляемый вызовом функции
  (настоящие dag-factory паттерны, генерирующие несколько DAG с разными
  `dag_id` по ходу итерации).
- **Импортированные константы** (`from common.constants import DAG_NAME`,
  затем `dag_id=DAG_NAME`) — вероятно, самый частый реальный вариант после
  локальной модульной константы, но `_collect_module_constants` в принципе
  его не видит (импорт меняет счётчик связывания имени и потому поражает
  резолвинг любой ранее найденной top-level константы с тем же именем, но
  сама по себе значением не становится). После этого плана такие DAG
  переходят в skip+warning (безопасное поведение, просто не автоматическое).
- **Разбор содержимого распаковки** `@dag(**{"dag_id": "real"})` /
  `@dag(**DAG_KWARGS)`. План детектирует наличие распаковки и честно
  возвращает `_UNRESOLVED_DAG_ID` (skip + warning), но не пытается извлечь
  `dag_id` даже из литерального словаря — это отдельная, более сложная
  задача.
- **UI-бейдж «⚠ dag not found»** для subscription, чей `dag_id` не
  соответствует ни одному активному Airflow DAG — вынесен в отдельный план
  `docs/plans/20260807-rmq-watcher-dag-not-found-badge.md`. Причины
  разделения: (а) бейдж не помогает именно в сценарии, который решает этот
  план — если `dag_id=` вообще не резолвится, subscription не
  регистрируется совсем, строка просто отсутствует на странице, бейджу
  нечего подсвечивать; бейдж — самостоятельная, полезная, но другая фича
  (ловит переименованные/удалённые DAG и опечатки в `source='ui'`
  подписках); (б) он добавляет отдельную рантайм-зависимость от
  `airflow.models.DagModel` в `views.py` и заметно расширяет пересечение с
  открытым `docs/plans/20260703-reliability-hardening.md`
  (`models.py`/`views.py`/`test_views.py`/`CHANGELOG.md`); (в) разделение
  позволяет этому плану (фиксу реального репортнутого бага) быть меньше и
  быстрее пройти реализацию/ревью независимо.

`donstroy_pipeline_spark_all.py` находится в другом репозитории
(`project-donstroy`) и этим планом **не трогается** — как только этот план
задеплоится, DAG сам корректно зарезолвится (`DAG_NAME` — это ровно случай
«простой модульной строковой константы», под который план добавляет
поддержку).

## Контекст (по итогам разведки)

- `airflow_provider_rmq/watcher/listener.py` — `_extract_dag_id_from_decorators`
  (строки ~31-54), `_parse_rmq_trigger_decorator` (~70-128, содержит свою
  дублирующуюся проверку `is_rmq` через `ast.Call`, которую стоит вынести в
  общий хелпер), `_extract_subscriptions_from_file` (~315-383, докстринг на
  ~324-325 документирует текущее, скоро-неверное, поведение fallback'а;
  фактическая строка fallback'а — `listener.py:360`:
  `dag_id = _extract_dag_id_from_decorators(node.decorator_list) or
  node.name`, идиома `or`, важно при замене на явные проверки — см.
  Technical Details).
- `tests/watcher/test_listener.py` — в `TestExtractDagId` уже есть тест
  `test_non_literal_dag_id_returns_none` (`dag_id=VARIABLE`, без резолвинга
  констант), который кодирует *текущее* неверное поведение и должен
  измениться. `TestExtractSubscriptionsFromFile.test_fallback_to_function_name_when_dag_id_is_variable`
  — ближайший существующий аналог donstroy-бага — та же форма
  (`DAG_ID = 'runtime_name'` на уровне модуля, `dag_id=DAG_ID`) — и сейчас
  утверждает неверный результат (`dag_id == "variable_dag"`, имя функции).
  Это становится regression-тестом для этого плана: после фикса он должен
  утверждать `dag_id == "runtime_name"`.
- `CHANGELOG.md` — **v2.0.9** документирует ровно то поведение, которое этот
  план отменяет: *"falls back to the function name for non-literal values
  (e.g. `dag_id=VARIABLE`)"*. Этому плану нужна собственная запись в
  changelog, отмечающая это как изменение (почти breaking: существующие
  неверные `dag_file`-подписки удаляются `_sync_to_db` на следующем
  reconcile после апгрейда). На момент написания плана верхний раздел файла
  — уже выпущенный `## v2.2.0`, секции `Unreleased` нет.
- Документация лежит в `readme.md` и `readme_ru.md` (нижний регистр, в этом
  репозитории нет `README.md`) — обе зеркалят одну и ту же структуру
  разделов и нуждаются в одинаковом добавлении.
- `docs/plans/20260703-reliability-hardening.md` (всё ещё открыт, не
  замёржен) пересекается по файлам: **`listener.py`+`test_listener.py`**
  (его Tasks 7 «Takeover ui-строк сканом», 10 «Разгрузка event loop», 11
  «Graceful stop», 13 «Интеграция лидер-лока», 14 «Statsd-метрики» — те же
  файлы, что и Tasks 1-3 этого плана), **`CHANGELOG.md`** (его Task 4), и
  **`readme.md`/`readme_ru.md`** (его Task 16, финальный doc-таск, как и
  Task 5 этого плана). Функционального конфликта нет — структура
  `_extract_subscriptions_from_file` из этого плана совместима с описанными
  там изменениями — но при пересечении разница особенно заметна.
  Исполнителям стоит явно зафиксировать, какой план приземляется первым, и
  перед стартом второго — сверить актуальные diff'ы конфликтующих файлов, а
  не полагаться на описания «как было» в обоих планах.
- **Смежный, но отдельный план**: `docs/plans/20260807-rmq-watcher-dag-not-found-badge.md`
  (UI-бейдж «⚠ dag not found») изначально был частью этого плана и вынесен
  отдельно — см. Обзор. Он не зависит функционально от этого плана (может
  реализовываться в любом порядке), но затрагивает `models.py`/`views.py`/
  `test_views.py`/`CHANGELOG.md` — при параллельной работе над обоими
  планами те же правила координации, что и с reliability-hardening.

## Подход к разработке

- **Подход к тестам**: Regular — реализуем задачу, затем пишем/обновляем её
  тесты в рамках той же задачи, и только потом переходим дальше.
- Каждую задачу (включая её тесты) доводим до конца, прежде чем начинать
  следующую.
- Изменения небольшие и точечные; `donstroy_pipeline_spark_all.py` и любые
  файлы вне этого репозитория не трогаем.
- **Каждая задача с изменением кода ДОЛЖНА включать новые/обновлённые
  тесты** — это не опционально.
- **Все тесты должны проходить перед началом следующей задачи.**
- Обновлять этот файл плана, если скоуп меняется по ходу реализации.

## Стратегия тестирования

- Unit-тесты на каждую новую/изменённую функцию (`tests/watcher/test_listener.py`).
- Интеграционные тесты с реальными временными DAG-файлами, по существующему
  паттерну `TestExtractSubscriptionsFromFile` (фикстура `tmp_path`).
- В проекте нет e2e-фреймворка для UI — не актуально для этого плана (он не
  трогает UI-слой).
- Команда тестов: `pytest tests/watcher/` (в начале Task 1 сверить точный
  вызов по `pyproject.toml`/конфигу CI, если он отличается).

## Отслеживание прогресса

- Отмечать выполненные пункты `[x]` сразу по готовности.
- Новые обнаруженные задачи добавлять с префиксом ➕.
- Проблемы/блокеры документировать с префиксом ⚠️.

## Обзор решения

Два аддитивных слоя, каждый независимо тестируемый:

1. **Резолвинг констант** (`listener.py`): новый проход
   `_collect_module_constants` строит карту `{имя: (значение, lineno)}` один
   раз на файл; ссылки `dag_id=NAME` резолвятся по ней с учётом позиции
   (константа должна быть присвоена раньше декорируемой функции).
2. **Явное состояние «не резолвится»** (`listener.py`): новый сентинел
   `_UNRESOLVED_DAG_ID` различает «`dag_id` вообще не задан» (можно
   безопасно использовать имя функции — это совпадает с собственным
   поведением Airflow по умолчанию для `@dag(...)`) и «`dag_id` задан, но
   статически не резолвится» (гадать небезопасно — пропускаем регистрацию,
   логируем warning со ссылкой на UI). Значение `dag_id` берётся и из
   позиционного `@dag("id")`, и из `@dag(dag_id="id")` — в сигнатуре
   Airflow это `POSITIONAL_OR_KEYWORD`-параметр, и позиционная форма
   сегодня вообще не обрабатывается (см. Technical Details).

## Технические детали

### Сентинел и резолвинг констант (listener.py)

Используем enum-сентинел вместо голого `object()`, чтобы mypy корректно
сужал `str | _DagIdSentinel | None` после проверки `is` (в репозитории
`mypy>=1.5` как dev-зависимость; сейчас не встроен в CI, но нет причины
специально заводить дыру в типах):

```python
import enum


class _DagIdSentinel(enum.Enum):
    UNRESOLVED = enum.auto()


_UNRESOLVED_DAG_ID = _DagIdSentinel.UNRESOLVED
```
**История этого куска (для контекста, зачем алгоритм именно такой)**. Версия 1
резолвила «последнее присваивание побеждает» на весь файл — неверно для DAG,
объявленного между двумя присваиваниями (реальный Python видит значение в
момент выполнения декоратора, а не последнее в файле). Версия 2 («ровно одно
присваивание — иначе нерезолвится») чинила это, но считала «повторным
присваиванием» только формы, подходящие под извлечение значения: `AugAssign`,
tuple-unpacking, chained-присваивание, присваивание внутри `if` имя не
поражали. Версия 3 перечисляла top-level формы вручную (`_bound_names` +
рекурсия через control-flow) — прототип показал, что перечень раз за разом
неполон: проскакивали `except ... as NAME`, `global NAME`, walrus,
capture-паттерны `match`/`case`. Версия 4 (scope-blind `ast.walk` по всему
дереву) закрыла полноту, но «отравляла» константу любым одноимённым
связыванием в **чужой** области видимости — параметром функции, атрибутом в
теле класса, локальной переменной, таргетом comprehension, — то есть теряла
вполне валидные константы; плюс всё равно имела дыру на `from x import *`.

Итоговая версия сочетает обе гарантии: **полный перечень форм связывания** и
**корректный учёт областей видимости**. Полнота перечня получена не угадыванием,
а интроспекцией: обходом всех подклассов `ast.AST` и их `_fields` на предмет
полей, несущих имя (`id`/`name`/`names`/`arg`/`asname`/`rest`). Области
видимости учитываются обходом, который **обрывается на границах вложенных
областей** (`FunctionDef`/`AsyncFunctionDef`/`ClassDef`/`Lambda` и все виды
comprehension — в Python 3 у comprehension своя область): само имя такого узла
в module scope связывается и учитывается, а его тело — нет. Отдельно
обрабатываются два случая, которые обход по областям иначе бы упустил:

- `global NAME` внутри любой функции — функция вправе переприсвоить модульное
  имя, поэтому ищется по **всему** дереву и всегда поражает;
- `from module import *` — набор импортируемых имён статически неизвестен,
  поэтому при любом wildcard-импорте резолвинг констант для файла отключается
  целиком (возвращается пустая карта). Иначе `DAG_ID = "local"` +
  `from settings import *` резолвился бы в `"local"`, хотя импорт мог его
  переприсвоить — ровно тот ложноположительный неверный `dag_id`, который этот
  план призван исключить.

Прототип итоговой версии прогнан на 24 поражающих формах (включая wildcard,
`except as`, `global`, все три match-паттерна, вложенный `for` внутри `if`, и
три «коварных» walrus-случая: в обычном `if`, **внутри comprehension** и
**в дефолте параметра функции** — последние два обрезались бы границей
области, если бы не отдельный проход по `NamedExpr` и обход `_outer_parts`)
и на 7 валидных (одноимённый параметр, атрибут класса, локальная переменная,
таргет comprehension, параметр lambda, константа в дефолте параметра): дыр
нет, лишних потерь нет, реальный `donstroy_pipeline_spark_all.py` резолвится
корректно.

Разделение обязанностей внутри функции: (1) **значение** берётся только из
безусловного top-level `Assign`/`AnnAssign` — единственной формы, про которую
можно быть уверенным, что она выполнится именно так; (2) **признаки
неоднозначности** (poison) считаются по всему module scope, как описано выше.

Нужен импорт `from collections.abc import Iterator` (или использовать
`typing.Iterator`, если так принято в модуле).

```python
# Узлы, чьё тело — отдельная область видимости: имя самого узла связывается
# в объемлющей области, но его содержимое на module scope не влияет.
_SCOPE_BOUNDARIES = (
    ast.FunctionDef,
    ast.AsyncFunctionDef,
    ast.ClassDef,
    ast.Lambda,
    ast.ListComp,
    ast.SetComp,
    ast.DictComp,
    ast.GeneratorExp,
)


def _outer_parts(node: ast.AST) -> Iterator[ast.AST]:
    """Sub-expressions of a scope-boundary node that still evaluate in the
    ENCLOSING scope — decorators, parameter defaults, class bases/keywords,
    return annotations, and a comprehension's outermost iterable. Everything
    else inside the boundary belongs to the new scope.
    """
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        yield from node.decorator_list
        yield from node.args.defaults
        yield from (d for d in node.args.kw_defaults if d is not None)
        if node.returns is not None:
            yield node.returns
    elif isinstance(node, ast.ClassDef):
        yield from node.decorator_list
        yield from node.bases
        yield from (kw.value for kw in node.keywords)
    elif isinstance(node, ast.Lambda):
        yield from node.args.defaults
        yield from (d for d in node.args.kw_defaults if d is not None)
    elif isinstance(node, (ast.ListComp, ast.SetComp, ast.DictComp, ast.GeneratorExp)):
        if node.generators:
            yield node.generators[0].iter   # evaluated in the enclosing scope


def _module_scope_nodes(tree: ast.Module) -> Iterator[ast.AST]:
    """Yield every node that executes in the module's own scope.

    Like ``ast.walk``, but prunes at nested-scope boundaries: a
    ``FunctionDef``/``ClassDef``/``Lambda``/comprehension node is yielded
    itself (so the name it binds in the enclosing scope is counted) and its
    ``_outer_parts`` are still traversed, but its body is not — bindings in
    there belong to a different scope and must not poison a module-level
    constant.
    """
    stack: list[ast.AST] = list(tree.body)
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, _SCOPE_BOUNDARIES):
            stack.extend(_outer_parts(node))
            continue
        stack.extend(ast.iter_child_nodes(node))


def _collect_module_constants(tree: ast.Module) -> dict[str, tuple[str, int]]:
    """Collect simple top-level ``NAME = "literal string"`` assignments
    (plus annotated ``NAME: str = "literal"``), each paired with its line
    number so callers can enforce "only usable before this point in the
    file" — see the wiring below.

    A name is kept only if it is bound **exactly once** in the module's own
    scope, and that one binding is the top-level literal assignment itself.
    The binding forms counted below are the complete set of ``ast`` fields
    that can bind a name (derived by introspecting every ``ast.AST``
    subclass's ``_fields`` for name-bearing entries, not by guessing):
    ``Name`` in ``Store``/``Del`` (``=``, ``+=``, tuple-unpack, chained
    assignment, ``for`` targets, ``with ... as``, walrus, ``del``, and the
    PEP 695 ``type X = ...`` alias name, which is itself a ``Name`` in
    ``Store``), ``alias`` (imports), ``FunctionDef``/``AsyncFunctionDef``/
    ``ClassDef`` names, ``ExceptHandler.name``, ``MatchAs``/``MatchStar``
    names, ``MatchMapping.rest``. ``arg`` (parameters) is deliberately NOT
    counted — parameters live in the function's own scope, which
    ``_module_scope_nodes`` already excludes.

    Three cases the scope-pruned walk would otherwise miss are handled
    explicitly: ``global NAME`` anywhere in the file (a function may rebind a
    module-level name) and walrus ``:=`` anywhere (PEP 572 binds it in the
    *enclosing* scope, so one inside a comprehension still hits module level)
    both always poison; ``from module import *`` disables constant
    resolution for the whole file — the imported names are
    statically unknown, so any earlier literal could have been silently
    replaced.

    Refusing to resolve an ambiguous name is safe (the caller skips
    registration and logs a WARNING); guessing wrong is not — same principle
    as ``_UNRESOLVED_DAG_ID`` below.
    """
    binding_counts: dict[str, int] = {}
    has_wildcard_import = False

    def _bump(name: str) -> None:
        binding_counts[name] = binding_counts.get(name, 0) + 1

    for node in _module_scope_nodes(tree):
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            _bump(node.id)
        elif isinstance(node, ast.alias):
            if node.name == "*" and node.asname is None:
                has_wildcard_import = True
            else:
                _bump(node.asname or node.name.split(".")[0])
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            _bump(node.name)
        elif isinstance(node, ast.ExceptHandler) and node.name:
            _bump(node.name)
        elif isinstance(node, (ast.MatchAs, ast.MatchStar)) and node.name:
            _bump(node.name)
        elif isinstance(node, ast.MatchMapping) and node.rest:
            _bump(node.rest)

    # Two forms that a scope-pruned walk cannot see reliably, both scanned
    # over the WHOLE tree (over-counting here is safe — it only turns a
    # constant unresolvable):
    #   * `global NAME` — a function may rebind a module-level name;
    #   * walrus `:=` — per PEP 572 it binds in the ENCLOSING scope, so a
    #     walrus inside a comprehension binds at module level even though
    #     the comprehension body itself is a separate scope.
    for node in ast.walk(tree):
        if isinstance(node, ast.Global):
            for name in node.names:
                _bump(name)
        elif isinstance(node, ast.NamedExpr) and isinstance(node.target, ast.Name):
            _bump(node.target.id)

    if has_wildcard_import:
        return {}

    constants: dict[str, tuple[str, int]] = {}
    for node in tree.body:  # value extraction: top-level ONLY, unconditional
        if isinstance(node, ast.Assign):
            if len(node.targets) != 1:
                continue
            target, value_node = node.targets[0], node.value
        elif isinstance(node, ast.AnnAssign):
            if node.value is None:
                continue
            target, value_node = node.target, node.value
        else:
            continue
        if not isinstance(target, ast.Name):
            continue
        name = target.id
        if binding_counts.get(name, 0) != 1:
            continue
        try:
            value = ast.literal_eval(value_node)
        except (ValueError, TypeError):
            continue
        if isinstance(value, str):
            constants[name] = (value, node.lineno)
    return constants
```

Возвращается `dict[str, tuple[str, int]]` (значение + номер строки), а не
плоский `dict[str, str]` — резолвинг для конкретной функции должен ещё
проверить `lineno < номер_строки_этой_функции`, чтобы не резолвить
константу, присвоенную ПОСЛЕ декорируемой функции (use-before-definition в
реальном Python привёл бы к `NameError` при импорте файла — DAG вообще не
появился бы в Airflow, а без этой проверки статический анализ резолвил бы
правдоподобный, но никогда не существовавший `dag_id`). Позиционная
проверка и распаковка `(value, lineno)` → `str` под сигнатуру
`_extract_dag_id_from_decorators(decorators, constants: dict[str, str])`
происходят на стороне вызывающего кода — см. «Встраивание в
`_extract_subscriptions_from_file`» ниже — сама
`_extract_dag_id_from_decorators` по-прежнему принимает плоский
`dict[str, str]` и не знает о номерах строк.

**⚠ Позиционный `dag_id` — обязательно поддержать.** Сегодня
`_extract_dag_id_from_decorators` перебирает **только** `dec.keywords`
(`listener.py:43`), а в сигнатуре Airflow `dag_id` — самый первый параметр
и он `POSITIONAL_OR_KEYWORD` (проверено через `inspect.signature` на
установленном Airflow: `dag_id | POSITIONAL_OR_KEYWORD | default=''`). То
есть `@dag("my_real_id", schedule=None)` — совершенно легальная и
распространённая запись, и при ней текущий код возвращает `None`, а
вызывающий берёт имя функции — **это ровно тот тихий неверный `dag_id`,
ради устранения которого написан весь план**. Поэтому источник значения
надо брать так: если у декоратора есть `dec.args`, то `dec.args[0]` — это
`dag_id`; иначе искать `dag_id=` среди `dec.keywords`. Дальше — одна и та
же лесенка резолвинга (литерал → константа → `_UNRESOLVED_DAG_ID`) и одно и
то же правило про `""`, независимо от того, позиционный это аргумент или
именованный.

**⚠ «`@dag` не распознан» ≠ «`dag_id` не задан».** Сегодня оба состояния
сливаются в `None`, и вызывающий код одинаково берёт имя функции. Для
первого это неверно: предикат `is_dag` (`listener.py:37-40`) сопоставляет
только `ast.Name(id="dag")` и `ast.Attribute(attr="dag")`, поэтому
`from airflow.decorators import dag as airflow_dag` + `@airflow_dag(dag_id=DAG_NAME)`
не распознаётся как `@dag(...)` вовсе — и функция молча регистрируется под
именем функции. Это остаточный экземпляр ровно того тихого класса, который
план закрывает, поэтому состояния надо развести: **`None` возвращается
только когда `@dag(...)`-вызов распознан, но `dag_id` в нём не задан**
(легальный случай — Airflow сам берёт имя функции); если распознанного
`@dag(...)`-вызова среди декораторов нет вообще — вернуть
`_UNRESOLVED_DAG_ID`. Отслеживать алиасы импорта не нужно: достаточно
честно сказать «не знаю» и пропустить с warning'ом. Существующий кейс
`@rmq_trigger(...)` + `@dag(schedule=None)` (без `dag_id`) при этом не
страдает — `@dag` там распознаётся, возвращается `None`, имя функции
берётся как и раньше (проверено).

**⚠ Распаковка `**kwargs`/`*args` — обязательно считать нерезолвимой.**
`@dag(**{"dag_id": "real_id"})` и `@dag(**DAG_KWARGS)` — легальные вызовы;
в AST у них `keywords=[keyword(arg=None, value=...)]` (проверено), то есть
поиск `dag_id` среди `kw.arg` ничего не найдёт. Если на этом остановиться,
функция вернёт `None`, вызывающий возьмёт имя функции — снова тот же тихий
неверный `dag_id`. Правило: **если `dag_id` не найден явно, но у декоратора
есть распаковка** (`any(kw.arg is None for kw in dec.keywords)` или
`ast.Starred` среди `dec.args`), доказать отсутствие `dag_id` невозможно →
вернуть `_UNRESOLVED_DAG_ID` (skip + warning), а не `None`. Сам разбор
содержимого распаковки (даже литерального `**{"dag_id": ...}`) —
сознательно вне скоупа этого плана.

Поведение `_extract_dag_id_from_decorators(decorators, constants=None)`
(«значение `dag_id`» ниже — это `dec.args[0]`, если он есть, иначе
`dag_id=` из `dec.keywords`):

| значение `dag_id` в `@dag(...)` | резолвится в литерал | резолвится через `constants` | иначе |
|---|---|---|---|
| `@dag(...)` не распознан вовсе | — | — | вернуть `_UNRESOLVED_DAG_ID` (см. ниже про алиасы) |
| `@dag(...)` распознан, `dag_id` не задан, распаковки нет | — | — | вернуть `None` (вызывающий код использует имя функции — корректно, совпадает с поведением Airflow по умолчанию) |
| не задано, но есть `**kwargs`/`*args` | — | — | вернуть `_UNRESOLVED_DAG_ID` (отсутствие недоказуемо) |
| задано, значение falsy (`""`, `None`, `False`, `0`) | вернуть `None` | вернуть `None` | — |
| задано, значение truthy | вернуть литерал, если это `str` | вернуть `constants[name]` | вернуть `_UNRESOLVED_DAG_ID` |

**Про falsy-значения**: сам Airflow (`airflow/models/dag.py`,
`def dag(dag_id: str = "", ...)`, строка 4256: `with DAG(dag_id or
f.__name__, ...)`) резолвит реальный dag_id как `dag_id or f.__name__` —
то есть **любое** falsy-значение (не только `""`, но и `None`, `False`,
`0`) в самом Airflow означает «взять имя функции». Сегодня существующая
строка `... or node.name` воспроизводит это автоматически; при переходе на
явные проверки `is` семантику надо **сохранить**: если `ast.literal_eval`
дал falsy-значение, `_extract_dag_id_from_decorators` возвращает `None`
(имя функции), а не `_UNRESOLVED_DAG_ID` — иначе корректно работающий
сегодня DAG потеряет подписку.

Для **truthy** нестроковых литералов (`dag_id=123`) поведение меняется:
раньше это тихо маппилось в `None`/имя функции, теперь — в
`_UNRESOLVED_DAG_ID`, потому что значение задано, оно не falsy, и имя
функции здесь такая же неверная догадка, как и для нерезолвящегося `Name`
(реальный Airflow подставил бы `123` как dag_id, что почти наверняка не то,
чего хотел автор).

Внутри самой `_extract_dag_id_from_decorators` логирования не происходит —
решение, логировать ли warning, принимает вызывающий код, потому что только
он знает путь файла и присутствует ли реально декоратор `@rmq_trigger`
(не связанный с этим `@dag(dag_id=<динамика>)` без `@rmq_trigger` — не
забота этого плагина, и должен оставаться безмолвным).

### Отвергнутая альтернатива: брать `dag_id` из `DagModel.fileloc`

Напрашивается не-AST путь: `consumer.py::_sync_trigger` уже импортирует
`DagModel` и ходит в БД через `WatcherSession` из фонового треда, а
`_scan_subscriptions` знает точный `path` файла — запрос
`DagModel.fileloc == path` вернул бы **реальный** `dag_id`, посчитанный
самим Airflow, и разом закрыл бы все динамические формы (импортированные
константы, f-строки, конкатенацию, вызовы функций), которые этот план
сознательно оставляет за бортом.

Отвергнуто, но осознанно, и вот почему:
- **Chicken-and-egg на первом деплое.** `DagModel` заполняется, только когда
  Scheduler уже распарсил файл. Watcher сканирует те же файлы независимо и
  может опередить парсер — тогда запрос вернёт пусто, и поведение станет
  недетерминированным (то резолвится, то нет, в зависимости от гонки).
- **Файлы с несколькими DAG.** `fileloc` не уникален: один файл легко даёт
  несколько строк `DagModel`. Сопоставить конкретную декорированную функцию
  с конкретной строкой без AST всё равно нельзя — то есть AST-анализ нужен
  как основа в любом случае, и вопрос лишь в том, добавлять ли БД-запрос
  сверху.
- **Смена природы зависимости.** Сейчас скан — чистая функция от текста
  файла, без обращений к БД; это делает его дешёвым, тестируемым на
  `tmp_path`-файлах и безопасным для фонового треда. Запрос в БД на каждый
  файл в цикле реконсиляции — уже другой профиль нагрузки и другой класс
  отказов.

Как **fallback перед skip'ом** (только когда AST не смог) идея выглядит
привлекательнее, но это отдельная фича со своими гонками и тестами — не
часть фикса конкретного репортнутого бага. Зафиксировано здесь, чтобы
отказ был явным, а не выглядел пробелом в разборе.

### Общий хелпер `_is_rmq_trigger_call`

`_parse_rmq_trigger_decorator` уже содержит инлайновую проверку `ast.Call` +
`Name`/`Attribute` для `rmq_trigger`. Выносим её в
`_is_rmq_trigger_call(node: ast.expr) -> bool`, чтобы
`_extract_subscriptions_from_file` мог переиспользовать её для решения,
заслуживает ли нерезолвящийся `dag_id` warning (логировать должны только
функции, реально декорированные `@rmq_trigger`).

### Встраивание в `_extract_subscriptions_from_file`

`_collect_module_constants(tree)` должна вызываться **внутри второго `try` —
того, что защищён широким `except Exception`** (блок начинается с `try:
result: list[dict] = []` примерно на строке 355), а не сразу после `ast.parse` —
`ast.parse` находится в своём узком блоке `except (SyntaxError, OSError,
UnicodeDecodeError)`, который это не покрывает, а докстринг этой функции
гарантирует, что она никогда не бросает исключение (`_scan_subscriptions`
полагается на это, чтобы всё равно зафиксировать mtime файла).
`_collect_module_constants` уже глушит `ValueError`/`TypeError` на уровне
каждого узла внутри себя, но патологический литерал теоретически может
бросить что-то другое (например `RecursionError`) — держим это за той же
защитной сеткой, что и остальной парсинг файла.

```python
try:
    module_constants = _collect_module_constants(tree)  # {name: (value, lineno)}
    # Only functions whose decorators are evaluated in module scope may use
    # the module-level constant map. A nested function (or a method) can be
    # shadowed by a local/class-scope binding of the same name, in which case
    # the module value is simply wrong — see the "вложенные функции" note
    # below.
    module_scope_function_ids = {
        id(node)
        for node in _module_scope_nodes(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    result: list[dict] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        if id(node) not in module_scope_function_ids:
            usable_constants: dict[str, str] = {}   # nested: refuse to resolve
        else:
            # Position filter: a constant assigned AFTER this function's own
            # line must not be used to resolve its dag_id= — see
            # _collect_module_constants's "use-before-definition" rationale.
            usable_constants = {
                name: value
                for name, (value, lineno) in module_constants.items()
                if lineno < node.lineno
            }
        dag_id = _extract_dag_id_from_decorators(node.decorator_list, usable_constants)
        if dag_id is _UNRESOLVED_DAG_ID:
            if any(_is_rmq_trigger_call(d) for d in node.decorator_list):
                log.warning(
                    "rmq_trigger: cannot statically resolve dag_id for function %r "
                    "in %s — dag_id= is not a string literal or a simple "
                    "module-level string constant defined earlier in the file. "
                    "Subscription NOT registered from dag_file; for queue=/queues= "
                    "subscriptions, create it manually via the RMQ Watcher UI "
                    "(source='ui') instead — the UI form does not support "
                    "exchange=/routing-key subscriptions, so for those dag_id= "
                    "must be made statically resolvable.",
                    node.name, path,
                )
            continue
        if dag_id is None:
            dag_id = node.name
        ...
```

**⚠ Вложенные функции: карту констант им передавать нельзя.**
`_extract_subscriptions_from_file` обходит дерево через `ast.walk`, а он
находит **все** `FunctionDef`, включая вложенные. Если DAG объявлен внутри
функции-фабрики, локальное имя перекрывает модульное, и модульное значение
для него просто неверно:

```python
DAG_ID = "module-id"

def make_dag():
    DAG_ID = "local-id"

    @rmq_trigger(queue="q")
    @dag(dag_id=DAG_ID)          # реальный Airflow получит "local-id"
    def data_proc():
        pass

    return data_proc()

built_dag = make_dag()
```

Без защиты `usable_constants` содержал бы `DAG_ID = "module-id"` (коллектор
внутрь `make_dag` не заходит и локального переприсваивания не видит), и
подписка зарегистрировалась бы под `"module-id"` — тихий неверный `dag_id`,
то есть ровно то, что план обязан исключать. Проверено прототипом на этом
самом примере. То же и для метода класса, если имя приходит из class
namespace.

Защита минимальная: множество `id()` функций, найденных
`_module_scope_nodes` (то есть тех, чьи декораторы вычисляются в module
scope), и всем остальным передаётся пустая карта — `dag_id=NAME` у них
даёт `_UNRESOLVED_DAG_ID` → skip + warning. Полноценный лексический
резолвинг вложенных областей не нужен: отказ здесь безопасен, а такой DAG
и так попадает в категорию «dag-factory», для которой план изначально не
обещает автоматической регистрации.

Файл, который раньше мис-регистрировал subscription с неверным dag_id,
теперь для этой функции не регистрирует **ничего**, пока не будет
исправлен. `_sync_to_db` уже удаляет `dag_file`-записи, которых больше нет
в скане, так что старая неверная запись пропадает на следующем reconcile
безо всякого дополнительного кода.

**Оговорка про exchange-mode**: подсказка «создайте subscription вручную
через UI» работает только для `queue=`/`queues=` — форма создания в UI
(`subscription_form.html`) имеет поля `dag_id`, `queue_name`, `conn_id`,
`cooldown`, `filter_data`, `enabled`, но **не** `exchange`/`routing_keys`;
их нет и в модели `RMQSubscription` (проверено по шаблону формы,
`views.py` и `models.py`). Если нерезолвящийся `dag_id` встретится на
DAG с `@rmq_trigger(exchange=...)`, единственный выход — сделать `dag_id=`
статически резолвимым (литерал или простая модульная константа до функции);
через UI такую подписку не восстановить. Текст WARNING выше уже это
проговаривает.

### Видимость сломанного DAG после изменения (осознанный отказ)

После этого плана у DAG с нерезолвящимся `dag_id` не остаётся **ни одного**
видимого артефакта: строки на странице Subscriptions нет (подписка не
создаётся), а WARNING пишется только когда файл реально пересканируется —
после рестарта Scheduler'а или изменения mtime. До изменения был хотя бы
видимый, пусть и неверный, ряд в таблице.

Рассмотренные и **отклонённые** смягчения:
- повторять WARNING раз в N reconcile-циклов для затронутых путей — требует
  хранить состояние между циклами (по сути ещё один tracker), а логи и так
  не то место, где пользователь ищет «почему мой DAG не триггерится»;
- логировать в конце скана сводку «M файлов пропущено» — дёшево, но само по
  себе не подсказывает, какие именно файлы и что с ними делать, то есть
  добавляет шум, не добавляя диагностики.

Причина отказа: правильное место для такого сигнала — UI, а не лог, и это
ровно то, чем занимается отдельный план
`docs/plans/20260807-rmq-watcher-dag-not-found-badge.md`. Он, впрочем, этот
конкретный случай тоже не покрывает (нет строки — нечего подсвечивать), так
что честная формулировка такая: **временно, до появления UI-механизма для
«файл просканирован, но подписка не создана», единственный сигнал — WARNING
в логе Scheduler'а**. Это явно проговаривается в README (Task 7), чтобы не
выглядело недосмотром.

## Что куда идёт

- **Implementation Steps**: все изменения кода/тестов/документации ниже —
  всё в пределах этого репозитория.
- **Post-Completion**: ручная проверка на реальном инстансе Airflow;
  изменений в `project-donstroy` не требуется (см. Обзор).

## Implementation Steps

**Принцип разбиения**: после Task 3 код консистентен end-to-end — вызывающая
строка `listener.py:360` уже приведена к явным `is`-проверкам в той же
задаче, что вводит `_UNRESOLVED_DAG_ID`. Это не косметика: `_UNRESOLVED_DAG_ID`
truthy, а `build_subscriptions` (`subscription_builder.py`) принимает
`dag_id: str`, но **никак его не валидирует** — использует только в
`f"rmq_watcher.sub.{dag_id}"`. Если оставить `or node.name` хотя бы на одну
задачу, сентинел молча уйдёт в `_sync_to_db` как значение `dag_id` (а в
exchange-режиме создаст очередь `rmq_watcher.sub._DagIdSentinel.UNRESOLVED`).
Tasks 4-5 после этого — чистые расширения экстрактора, вызывающий код они
не меняют.

Поэтому Task 3 намеренно крупнее остальных (~12 чекбоксов против ориентира
~5). В неё сведено **всё, что определяет трёхвариантный контракт возврата**
(`str` / `None` / `_UNRESOLVED_DAG_ID`) — включая правило «нераспознанный
`@dag(...)` → `_UNRESOLVED_DAG_ID`, — и приведение вызывающего кода к нему.
Разносить это по задачам вредно с двух сторон: контракт описан в Technical
Details одной таблицей, и реализовывать его половинами означает, что таблица
какое-то время описывает несуществующее поведение; а оставить `or node.name`
хотя бы на одну задачу — значит пустить truthy-сентинел в `build_subscriptions`
(см. выше). Позиционный аргумент (Task 4) и распаковка (Task 5) вынесены
отдельно именно потому, что они контракт не меняют — только расширяют набор
входов, которые в него попадают.

### Task 0: Preflight (координация и baseline)

- [x] **Решение: этот план приземляется первым.** На момент выполнения Task 0 (2026-08-08) `docs/plans/20260703-reliability-hardening.md` и `docs/plans/20260807-rmq-watcher-dag-not-found-badge.md` не находятся в работе (нет активных изменений/коммитов по ним) — этот план единственный реально исполняемый прямо сейчас, поэтому он и приземляется первым. Если один из конфликтующих планов (пересечение по `listener.py`/`test_listener.py`/`CHANGELOG.md`/`readme.md`/`readme_ru.md`) стартует позже — его исполнителю **не полагаться** на текстовые описания «как было» в обоих планах (они устареют после этого плана), а заново сверить актуальные diff'ы/состояние перечисленных файлов относительно того, что оставит этот план.
- [x] **Decision: v2.3.0.** Аргумент решающий: изменение может тихо удалить ранее работавшие `dag_file`-подписки (`_sync_to_db` уберёт их на первом reconcile после апгрейда, если `dag_id=` у них не резолвится статически новым парсером иначе, чем раньше) — это поведенческое, почти breaking изменение, что соответствует minor-бампу, а не patch (`v2.2.1`). Это значение будет использовано как есть в записи `CHANGELOG.md` в Task 7.
- [x] baseline сохранён: `./.venv/bin/ruff check airflow_provider_rmq/watcher/listener.py tests/watcher/test_listener.py > /tmp/ruff_baseline.txt 2>&1` и `./.venv/bin/mypy airflow_provider_rmq/watcher/listener.py > /tmp/mypy_baseline.txt 2>&1` — оба выполнены из `.venv` (venv существует в корне репозитория, путь подтверждён)
- [x] сравнение с ожиданиями: **совпадает полностью**. Ruff: `listener.py` — чистый, `test_listener.py` — ровно 1 ошибка `F401` (неиспользуемый импорт `pytest`, строка 9). Mypy: ровно 12 ошибок в 5 файлах (`utils/filters.py`, `utils/amqp.py`, `watcher/models.py` ×2, `watcher/consumer.py` ×2, `watcher/listener.py` ×4), в `listener.py` — ровно 4 ошибки `Item "None" of "Event | None" has no attribute ...` на строках 198, 207, 208, 215 (внутри диапазона 198-215, заявленного планом). Отклонений от ожидаемого baseline нет.

### Task 1: `_collect_module_constants` (сбор кандидатов на резолвинг)

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] добавить `_SCOPE_BOUNDARIES`, `_outer_parts(node)` + генератор `_module_scope_nodes(tree)` и `_collect_module_constants(tree) -> dict[str, tuple[str, int]]` строго по коду из Technical Details: обход обрывается на границах вложенных областей (`FunctionDef`/`AsyncFunctionDef`/`ClassDef`/`Lambda`/все comprehension) — сам узел учитывается, тело нет; poison-ветки: `Name` в `Store`/`Del`, `alias`, `FunctionDef`/`AsyncFunctionDef`/`ClassDef`, `ExceptHandler.name`, `MatchAs`/`MatchStar`, `MatchMapping.rest`; **`arg` НЕ считается** (параметры живут в своей области, обход туда не заходит); отдельно — `ast.Global` и `ast.NamedExpr` (walrus) по всему дереву и флаг wildcard-импорта, при котором возвращается пустая карта. Значение берётся только из безусловных top-level `Assign`/`AnnAssign`; кандидат принимается при poison-счётчике ровно 1; вернуть `(значение, lineno)` для позиционной фильтрации в Task 3
- [ ] в докстринге зафиксировать, откуда взята полнота перечня (интроспекция `_fields` подклассов `ast.AST` на поля, несущие имя — не угадывание) и почему `global`/wildcard обрабатываются отдельно от обхода по областям
- [ ] тесты, базовые кейсы: простая строковая константа (единственная запись — резолвится, с корректным `lineno`), annotated-присваивание (`NAME: str = 'x'`), игнорирование нестроковой константы, игнорирование нелитерального RHS (например вызов функции)
- [ ] тесты, формы переприсваивания (все → нерезолвится): повторное присваивание литералом (`DAG_ID = "first"; DAG_ID = "second"`), `AugAssign` (`DAG_ID += "_daily"`), tuple-unpacking (`DAG_ID, Q = "real", "q"`), chained-присваивание (`DAG_ID = OTHER = "real"`), повторный импорт (`from settings import DAG_ID`), `import x as DAG_ID`, `def`/`class` с тем же именем, `del DAG_ID`, `for DAG_ID in ...` на верхнем уровне, `with o() as DAG_ID`, условное переприсваивание внутри `if` (`DAG_ID = "old"`, затем `if C: DAG_ID = "new"` → ни `"old"`, ни `"new"`)
- [ ] тест (можно одним `@pytest.mark.parametrize`) на экзотические формы связывания в **module scope**, которые более ранние версии перечня пропускали: `except Exception as DAG_ID`, `global DAG_ID` + присваивание внутри функции, walrus `if (DAG_ID := ...)`, `case DAG_ID:` (`MatchAs`), `case [*DAG_ID]:` (`MatchStar`), `case {"k": _, **DAG_ID}:` (`MatchMapping.rest`), вложенный `for DAG_ID in ...` внутри `if` — все роняют константу
- [ ] **тест на wildcard-импорт**: `DAG_ID = "local"` + `from settings import *` → карта констант пуста (резолвинг для файла отключён целиком). Без этого `"local"` резолвился бы, хотя импорт мог переприсвоить имя — ложноположительный неверный `dag_id`
- [ ] **тесты на walrus, который обход по областям иначе бы пропустил** (PEP 572 связывает `:=` в объемлющей области): `xs = [(DAG_ID := i) for i in y]` (внутри comprehension), `def g(x=(DAG_ID := "other")): pass` (в дефолте параметра), `@deco(DAG_ID := "n")` (в выражении декоратора) — все роняют константу. Именно ради них нужны `_outer_parts` и отдельный проход по `NamedExpr`
- [ ] тесты на то, что связывание в **чужой** области видимости константу НЕ роняет (это и есть выигрыш scope-aware обхода — все пять проверены прототипом): одноимённый параметр функции (`def process(DAG_NAME): ...`), атрибут в теле класса (`class C: DAG_NAME = "b"`), локальная переменная внутри функции (`def f(): DAG_NAME = "loc"`), таргет comprehension (`[DAG_NAME for DAG_NAME in y]`), параметр lambda (`lambda DAG_NAME: ...`) → во всех случаях модульная `DAG_NAME = "real"` резолвится в `"real"`
- [ ] регрессионный тест на форму реального donstroy-файла: модульная `DAG_NAME = "donstroy_pipeline_spark_all"`, где то же имя используется **как значение по умолчанию параметра** (`def create_cluster(..., dag_name: str = DAG_NAME)`) — это `Name(Load)`, не связывание, поэтому константа обязана резолвиться корректно (прототип это подтвердил на настоящем файле)
- [ ] отдельная ветка под PEP 695 (`type DAG_ID = int`) не нужна: имя алиаса — обычный `ast.Name` в `Store`, первая poison-ветка его уже считает (проверено). Тест при желании возможен через строку в `ast.parse(...)` + `pytest.mark.skipif(sys.version_info < (3, 12))` — прямой синтаксис в теле теста нельзя, CI гоняет и 3.10/3.11, там это `SyntaxError` на этапе парсинга файла
- [ ] прогнать `pytest tests/watcher/test_listener.py` — должно проходить перед Task 2

### Task 2: Общий хелпер `_is_rmq_trigger_call` (чистый рефакторинг)

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] вынести инлайновую проверку `ast.Call` + `Name`/`Attribute` «это вызов `rmq_trigger(...)`?» из `_parse_rmq_trigger_decorator` в `_is_rmq_trigger_call(node: ast.expr) -> bool`; переключить `_parse_rmq_trigger_decorator` на неё — поведение не меняется, это подготовка к Task 3, где хелпер нужен для решения, логировать ли warning
- [ ] добавить контрактный тест `TestIsRmqTriggerCall` — bare `rmq_trigger(...)`, `decorators.rmq_trigger(...)` (Attribute-доступ), посторонний вызов `some_other_call(...)` → `False`, не-`Call` узел (например голый `@some_name`) → `False`
- [ ] прогнать `pytest tests/watcher/test_listener.py` — должно проходить перед Task 3

### Task 3: Сентинел + трёхвариантный контракт + приведение вызывающего кода (feature работает end-to-end)

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] добавить enum `_DagIdSentinel` + модульный `_UNRESOLVED_DAG_ID = _DagIdSentinel.UNRESOLVED` (по Technical Details — enum, а не голый `object()`, чтобы mypy корректно сужал `str | _DagIdSentinel | None` после проверок `is`)
- [ ] обновить `_extract_dag_id_from_decorators` — новая сигнатура `def _extract_dag_id_from_decorators(decorators: list[ast.expr], constants: dict[str, str] | None = None) -> str | _DagIdSentinel | None:` (в модуле уже есть `from __future__ import annotations`, так что union-синтаксис безопасен на 3.10): принимать `constants`, резолвить `dag_id=NAME` по ней, возвращать `_UNRESOLVED_DAG_ID`, когда `dag_id=` задан, но не резолвится (нелитеральные выражения и truthy нестроковые литералы вроде `dag_id=123`); возвращать `None`, **только** когда `@dag(...)`-вызов распознан, но `dag_id` в нём не задан, **или** когда резолвленное значение falsy (`""`, `None`, `False`, `0` — реальный Airflow делает `dag_id or f.__name__`, см. Technical Details); убрать внутренний `log.warning` — логирование переезжает к вызывающему коду в этой же задаче **Если распознанного `@dag(...)`-вызова среди декораторов нет вообще — возвращать `_UNRESOLVED_DAG_ID`, а не `None`** (см. Technical Details про алиасированный `@dag`: сегодня оба состояния сливаются в `None` и дают тихий откат на имя функции).
- [ ] обновить докстринг: трёхвариантный контракт возврата (`str` / `None` / `_UNRESOLVED_DAG_ID`), почему fallback на имя функции безопасен только в случае `None`, и **асимметрия falsy**: литерал `@dag(dag_id=None)` даёт `None` (имя функции, как в Airflow), а `DAG_ID = None` + `dag_id=DAG_ID` даёт `UNRESOLVED` (в `constants` попадают только `str`) — направление ошибки безопасное, но расхождение намеренное и должно быть зафиксировано
- [ ] **⚠ заменить вызывающую строку `listener.py:360`** — сейчас там `dag_id = _extract_dag_id_from_decorators(node.decorator_list) or node.name` (идиома `or`). Заменить на явные проверки из сниппета в Technical Details: `if dag_id is _UNRESOLVED_DAG_ID:` → warning (через `_is_rmq_trigger_call` из Task 2) + `continue`; затем `if dag_id is None: dag_id = node.name`. **Обязательно в этой же задаче**: `_UNRESOLVED_DAG_ID` truthy, а `build_subscriptions` не валидирует `dag_id`, поэтому оставленный `or` молча пропустит сентинел в БД
- [ ] в том же месте: вызвать `_collect_module_constants(tree)` один раз **внутри второго `try` — того, что защищён широким `except Exception`** (не сразу после `ast.parse`: тот находится в своём узком `try`, который эту функцию не покрывает), и для каждого `FunctionDef`/`AsyncFunctionDef` передавать позиционно-отфильтрованную карту (`{name: value for name, (value, lineno) in module_constants.items() if lineno < node.lineno}`) — константа, присвоенная после функции, для неё не резолвится (use-before-definition)
- [ ] **⚠ guard для вложенных функций** (единственная unsafe-дыра, найденная при финальной сверке): `ast.walk` находит и вложенные `FunctionDef`, а модульная карта для них неверна — локальное имя перекрывает модульное. Построить `module_scope_function_ids = {id(n) for n in _module_scope_nodes(tree) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}` и передавать позиционно-отфильтрованную карту **только** функциям из этого множества; всем остальным — пустой `{}`, чтобы `dag_id=NAME` дал `_UNRESOLVED_DAG_ID` → skip + warning
- [ ] regression-тест `test_nested_function_does_not_use_module_constants` — DAG объявлен внутри функции-фабрики с локальным переприсваиванием того же имени (пример в Technical Details) → `result == []` и WARNING, **не** регистрация под модульным значением
- [ ] обновить докстринг `_extract_subscriptions_from_file` (сейчас: "dag_id is taken from the explicit dag_id= argument of @dag(...) when it is a string literal; otherwise falls back to the decorated function name") — описать новое трёхвариантное поведение
- [ ] обновить существующие тесты: `test_non_literal_dag_id_returns_none` → `test_non_literal_dag_id_without_constant_returns_unresolved` (проверять `is _UNRESOLVED_DAG_ID`); `test_non_string_literal_dag_id_returns_none` → `test_non_string_literal_dag_id_returns_unresolved`; `test_fallback_to_function_name_when_dag_id_is_variable` → `test_dag_id_resolved_from_module_level_constant` с тем же телом DAG-файла и проверкой `result[0]["dag_id"] == "runtime_name"` (прямой regression-тест на donstroy-баг)
- [ ] **обновить ещё два существующих теста, которые иначе гарантированно упадут на новом контракте** (в них нет распознанного `@dag(...)`-вызова, значит по новому правилу возвращается `_UNRESOLVED_DAG_ID`, а не `None`): `test_no_dag_decorator_returns_none` (`tests/watcher/test_listener.py:56`, `@some_other_decorator`) и `test_empty_decorator_list_returns_none` (`:73`, пустой список декораторов) → переименовать в `..._returns_unresolved` и проверять `is _UNRESOLVED_DAG_ID` (строка таблицы «`@dag(...)` не распознан вовсе»)
- [ ] unit-тесты контракта: `test_variable_dag_id_resolved_via_constants`, `test_variable_dag_id_not_in_constants_returns_unresolved`, и параметризованный falsy-тест по **всем** falsy-значениям, которые может вернуть `ast.literal_eval`: `""`, `None`, `False`, `0`, `0.0`, `[]`, `{}`, `()` — все → `None` (имя функции), а не `_UNRESOLVED_DAG_ID`; спецификация обещает семантику `dag_id or f.__name__` для любого falsy, а не только для четырёх перечисленных
- [ ] file-level тесты: `test_unresolvable_dag_id_with_rmq_trigger_skips_and_warns` (`@rmq_trigger(...)` + `@dag(dag_id=f"prefix_{1+1}")` → `result == []`, WARNING); `test_unresolvable_dag_id_without_rmq_trigger_stays_silent` (без `@rmq_trigger` → `result == []` и ни одной записи про этот файл/функцию — обернуть в `with caplog.at_level(logging.WARNING):`, не проверять голым `caplog.records == []`, это упадёт на постороннем шуме логгеров); `test_use_before_definition_not_visible_to_earlier_function` (`@dag(dag_id=DAG_ID)` + `DAG_ID = "x"` **после** функции → не резолвится; в реальном Python это `NameError` при импорте)
- [ ] **тест изоляции skip'а**: `test_unresolvable_function_does_not_drop_other_subscriptions_in_same_file` — один файл, две функции с `@rmq_trigger`: у первой `dag_id=CONST` (резолвится), у второй `dag_id=f"..."` (нет) → `len(result) == 1`, ровно один WARNING. Проверяет, что `continue` пропускает **одну функцию**, а не обнуляет весь файл
- [ ] в warning-тестах проверять конкретные подстроки, а не точную формулировку 7-строчного сообщения: уровень `WARNING`, наличие `str(dag_file)` в тексте, имя функции, подстрока `"UI"` — этого достаточно, чтобы поймать регрессию, и не ломается от переформулировки
- [ ] тест на **exchange-mode с резолвнутой константой** (самый дорогой по последствиям путь — из него получается имя очереди, а осиротевшие `rmq_watcher.sub.*` разгребаются вручную, см. Post-Completion): `DAG_ID = "real_id"` + `@rmq_trigger(exchange=..., routing_key_ids=[...])` + `@dag(dag_id=DAG_ID)` → `queue_name == "rmq_watcher.sub.real_id"`. Шаблон — существующий `test_exchange_subscription_gets_correct_queue_name_and_group_key`
- [ ] тест на **алиасированный `@dag`**: `from airflow.decorators import dag as airflow_dag` + `@airflow_dag(dag_id=DAG_ID)` + `@rmq_trigger(...)` → `result == []` и WARNING (распознанного `@dag(...)` нет → `_UNRESOLVED_DAG_ID` → skip). **Не** регистрация под именем функции
- [ ] тест-антирегрессия на легальный случай «`@dag` распознан, `dag_id` не задан»: существующий `test_fallback_to_function_name_when_no_dag_id` (`@rmq_trigger(queue='q2')` + `@dag(schedule_interval=None)`) должен продолжать давать имя функции — проверено, `is_dag` там `True`, поэтому возвращается `None`, а не сентинел
- [ ] прогнать `pytest tests/watcher/test_listener.py` (весь файл) — должно проходить перед Task 4

### Task 4: Позиционный `dag_id`

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] в `_extract_dag_id_from_decorators` брать значение так: **сначала** проверить `ast.Starred` среди `dec.args` — если есть, сразу вернуть `_UNRESOLVED_DAG_ID` (значение позиции недоказуемо, см. Task 5; без этого приоритета `@dag(*ARGS, dag_id="real")` взял бы `Starred` как значение и потерял бы вполне резолвимый kwarg, а `test_star_args_returns_unresolved` проходил бы случайно — через `ValueError` в `literal_eval`, а не по замыслу); **затем** если `dec.args` непусто → `dec.args[0]`, иначе искать `dag_id=` среди `dec.keywords`; далее — та же лесенка резолвинга (литерал → константа → `UNRESOLVED`) и то же правило про falsy. Сейчас функция перебирает только `dec.keywords` (`listener.py:43`), а в сигнатуре Airflow `dag_id` — первый параметр и он `POSITIONAL_OR_KEYWORD` (проверено через `inspect.signature`), то есть `@dag("my_real_id", schedule=None)` легален и сегодня даёт тихий откат на имя функции — ровно тот баг, который чинит этот план
- [ ] unit-тесты: `test_positional_string_literal_dag_id` (`@dag("my_dag")` → `"my_dag"`), `test_positional_variable_dag_id_resolved_via_constants` (`@dag(DAG_NAME)` при `constants={"DAG_NAME": "resolved"}` → `"resolved"`), `test_positional_unresolvable_dag_id_returns_unresolved` (`@dag(SOME_VAR)` → `_UNRESOLVED_DAG_ID`), `test_positional_empty_string_dag_id_returns_none` (`@dag("")` → `None`)
- [ ] file-level тесты (проверяют всю связку с `usable_constants` и sentinel-веткой, а не только сам экстрактор): `test_positional_literal_dag_id_from_file` — `@rmq_trigger(queue='q')` + `@dag("real_id")` на функции с другим именем → `result[0]["dag_id"] == "real_id"`; `test_positional_constant_dag_id_from_file` — `DAG_ID = "real_id"` + `@dag(DAG_ID)` → то же
- [ ] прогнать `pytest tests/watcher/test_listener.py` — должно проходить перед Task 5

### Task 5: Распаковка `**kwargs` → `_UNRESOLVED_DAG_ID`

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] в `_extract_dag_id_from_decorators`: если явный `dag_id` не найден, но есть `**`-распаковка (`any(kw.arg is None for kw in dec.keywords)`) — вернуть `_UNRESOLVED_DAG_ID`, а не `None`. (`*args` уже покрыт приоритетной проверкой `ast.Starred` из Task 4 — здесь дублировать не нужно, только добавить тест.) `@dag(**{"dag_id": "real_id"})` и `@dag(**DAG_KWARGS)` в AST дают `keyword(arg=None, ...)` (проверено), поиск по `kw.arg` их не видит, и без этого правила получится откат на имя функции — тот же тихий баг
- [ ] unit-тесты: `test_dict_unpacking_returns_unresolved` (`@dag(**{"dag_id": "real"})`), `test_name_unpacking_returns_unresolved` (`@dag(**DAG_KWARGS)`), `test_star_args_returns_unresolved` (`@dag(*ARGS)`) → все `_UNRESOLVED_DAG_ID`
- [ ] file-level тест `test_kwargs_unpacking_skips_and_warns` — `@rmq_trigger(...)` + `@dag(**DAG_KWARGS)` → `result == []` и WARNING (а не тихая регистрация под именем функции)
- [ ] прогнать `pytest tests/watcher/test_listener.py` — должно проходить перед Task 6

### Task 6: Проверка критериев приёмки

- [ ] проверить: временный DAG-файл, устроенный ровно как `donstroy_pipeline_spark_all.py` (модульная `DAG_NAME = '...'`, `dag_id=DAG_NAME`, `@rmq_trigger(queue=...)` над `@dag(...)`), резолвится в корректный `dag_id`, а не в имя функции — покрыто переименованным regression-тестом из Task 3, здесь повторно подтвердить по имени
- [ ] проверить: нерезолвящийся `dag_id=` при наличии `@rmq_trigger` не регистрирует ничего и логирует warning; без `@rmq_trigger` — остаётся полностью безмолвным (тесты из Task 3)
- [ ] проверить: use-before-definition не резолвится; skip изолирован в пределах одной функции (тесты из Task 3)
- [ ] проверить: позиционный `@dag("id")` и `@dag(CONST)` резолвятся; распаковка даёт skip + warning (тесты из Tasks 4-5)
- [ ] прогнать полный набор: `pytest tests/ -v` (ровно так вызывает CI, `.github/workflows/publish.yml:27`)
- [ ] **ruff** — прогнать `./.venv/bin/ruff check airflow_provider_rmq/watcher/listener.py tests/watcher/test_listener.py` (оба файла одной командой, как и baseline в Task 0) и сравнить с `/tmp/ruff_baseline.txt`: в `listener.py` должно остаться ноль ошибок, в `tests/watcher/test_listener.py` — не больше существующей одной (неиспользуемый импорт `pytest`). **Не исправлять** существующие ошибки в остальном проекте — вне скоупа
- [ ] **mypy** — прогнать `./.venv/bin/mypy airflow_provider_rmq/watcher/listener.py` и сравнить с `/tmp/mypy_baseline.txt`: **новых ошибок быть не должно**. Если появится ошибка в сужении типов вокруг `_UNRESOLVED_DAG_ID` — значит обоснование enum-сентинела (Technical Details) не сработало; исправить до завершения задачи
- [ ] учесть, что `ruff`/`mypy` **в CI не запускаются вообще** (там только `pytest tests/ -v`) — эти два гейта чисто локальные, «CI поймает» здесь не работает
- [ ] убедиться, что ни один другой тестовый файл не ссылается на старый двухвариантный (`str | None`) контракт `_extract_dag_id_from_decorators`: `grep -rn "_extract_dag_id_from_decorators" tests/`

### Task 7: [Final] Обновление документации

**Files:**
- Modify: `readme.md`
- Modify: `readme_ru.md`
- Modify: `airflow_provider_rmq/watcher/decorators.py`
- Modify: `CHANGELOG.md`
- Modify: `docs/example_dags/rmq_watcher_cooldown_multi_queue.py`
- Modify: `docs/example_dags/rmq_watcher_jetstat_exchange.py`
- Modify: `docs/example_dags/rmq_watcher_triggered_dag.py`
- Modify: `docs/plans/20260807-rmq-watcher-dag-not-found-badge.md`
- Move: `docs/plans/20260807-ast-dag-id-constant-resolution.md` → `docs/plans/completed/20260807-ast-dag-id-constant-resolution.md`

- [ ] в `readme.md`, раздел "How it works" (RMQ Watcher Plugin), добавить короткий абзац после предложения про reconciliation loop: `dag_id` в `@dag(...)` (позиционный или именованный) должен быть строковым литералом или простой модульной строковой константой, определённой раньше декорируемой функции, чтобы AST-скан `dag_file` его нашёл; если это не так — subscription **не появляется на странице Subscriptions вовсе** (не «с бейджем», а совсем отсутствует), единственный сигнал — WARNING в логе Scheduler'а после рестарта или изменения файла (не на каждом reconcile-цикле — только когда файл реально пересканируется)
- [ ] отзеркалить тот же абзац в разделе "Как это работает" файла `readme_ru.md`
- [ ] в докстринге `rmq_trigger` в `decorators.py` добавить краткую заметку (рядом с "Decorator order and DAG types") про то же ограничение — чтобы это было видно из тултипа в IDE, а не только из README
- [ ] добавить запись в `CHANGELOG.md` под номером версии, согласованным в Task 0: **Added** — `dag_id` в `@dag(...)` теперь резолвит простые модульные строковые константы (`DAG_NAME = 'x'` → `dag_id=DAG_NAME`), включая позиционную форму `@dag(DAG_NAME)`, а не только строковые литералы; **Changed** — нерезолвящийся `dag_id` (не литерал и не простая модульная константа — импортированные константы, f-строки, конкатенация, вызовы функций, распаковка `**kwargs`) больше не откатывается на имя функции: subscription пропускается с WARNING. **Обязательно проговорить в этом же пункте**, что удаляются не только заведомо неверные подписки: если имя python-функции случайно **совпадало** с реальным `dag_id` (например `DAG_NAME = "my_dag"` импортирован из другого модуля, а функция названа `def my_dag():`), сегодня такая подписка работает корректно, а после апгрейда исчезнет — вернуть литерал/локальную модульную константу в `dag_id` или создать подписку через UI
- [ ] в заметках "Gotcha: literal arguments only" (`docs/example_dags/rmq_watcher_cooldown_multi_queue.py:46-53`, `docs/example_dags/rmq_watcher_jetstat_exchange.py:65-71`) **добавить** уточняющую фразу — про `dag_id` там сейчас вообще ничего не сказано, обе заметки описывают только аргументы `@rmq_trigger(...)`, так что «убирать» нечего: дописать, что `dag_id` в `@dag(...)` теперь резолвит простые модульные константы, тогда как аргументы самого `@rmq_trigger(...)` остаются literal-only и по-прежнему пропускаются молча — поведение `_parse_rmq_trigger_decorator` этот план не меняет
- [ ] ➕ (существующий doc-долг, не следствие этого плана, но правится в тех же абзацах) в `docs/example_dags/rmq_watcher_triggered_dag.py` (строки ~14-15) поправить утверждение шага 3, что цикл каждые 60 с «re-reads DAG files», и аналогичную фразу «re-parsed ... on every reconcile cycle» в двух файлах выше — неизменившиеся файлы не перечитываются (mtime-гейт). В `readme.md:537` формулировка уже корректная («mtime-based — only changed files are re-parsed»), там править нечего; в `readme_ru.md` соответствующее место тоже уже корректно
- [ ] прогнать `pytest tests/ -v` ещё раз после правок, смежных с документацией/комментариями в коде
- [ ] обновить ссылки на этот план в `docs/plans/20260807-rmq-watcher-dag-not-found-badge.md` (строки 29, 101, 324) на новый путь `docs/plans/completed/...` — иначе после переноса они станут битыми и будут указывать на несуществующий активный план. Если badge-план к этому моменту уже приземлился и переехал сам — скорректировать по факту
- [ ] перенести этот план в `docs/plans/completed/`

## Post-Completion
*Пункты, требующие ручного вмешательства или внешних систем — без чекбоксов, только информационно*

**Ручная проверка** (на реальном/staging-инстансе Airflow с этим провайдером):
- убедиться, что DAG, устроенный как `donstroy_pipeline_spark_all.py`
  (модульная константа `DAG_NAME`, `dag_id=DAG_NAME`), подхватывает
  корректный `dag_id` в таблице Subscriptions после этого апгрейда, без
  каких-либо изменений в самом `project-donstroy`
- проверить логи Scheduler'а после первого reconcile-цикла после деплоя на
  предмет нового WARNING-сообщения — на случай, если какой-то другой DAG в
  парке машин тихо полагался на старый (неверный) fallback на имя функции —
  таким DAG'ам после этого апгрейда нужно будет поправить `dag_id=` (или
  завести subscription вручную через UI)
- если среди мис-зарегистрированных (под именем функции) подписок была
  exchange-mode или с `cooldown > 0` — после удаления строки `_sync_to_db` в
  RabbitMQ останутся осиротевшие очереди: `rmq_watcher.sub.{имя_функции}` с
  биндингами (exchange-mode) и/или `rmq_watcher.pending.{имя_функции}`
  (per-dag_id очередь-таймер cooldown, см. `CONTEXT.md`) — провайдер их не
  удаляет автоматически. **Не рассчитывать на WARNING от
  orphan-tracker**: `OrphanTracker` хранит `_provisioned` только в памяти
  текущего `RMQConsumerManager` и узнаёт о dag_id лишь через
  `mark_provisioned` — после рестарта Scheduler'а (а деплой этого изменения
  как раз его и подразумевает) новый tracker про старую очередь ничего не
  знает и промолчит. Проверять напрямую через RabbitMQ Management UI/API:
  искать очереди `rmq_watcher.sub.*` и `rmq_watcher.pending.*`, чьё имя совпадает с **именем
  python-функции**, а не с реальным `dag_id`, и удалять вручную
  (`rabbitmqadmin delete queue name=rmq_watcher.sub.<имя_функции>` и/или
  `rabbitmqadmin delete queue name=rmq_watcher.pending.<имя_функции>`)

**Обновления во внешних системах**:
- в `project-donstroy` или любом другом потребляющем репозитории для
  основного бага изменений не требуется (случай модульной константы
  резолвится автоматически); любой DAG, полагающийся на *по-настоящему*
  динамический `dag_id=` (f-строки, вызовы функций, dag-factory-циклы,
  импортированные константы, распаковка `**kwargs`), после апгрейда
  потеряет свой `dag_file`-subscription и потребует ручной UI-регистрации —
  до тех пор, пока поддержка соответствующих форм не будет добавлена
  отдельной задачей
