# Список DAG-файлов watcher-а — как у Airflow, с видимыми ошибками обхода

## Overview
- `_scan_subscriptions` собирает файлы через `glob(DAGS_FOLDER/**/*.py)`. Отсюда два дефекта
  (backlog `unreadable-subdir-passes-as-trusted`, `scan-ignores-airflowignore`):
  - `glob` молча теряет нечитаемый подкаталог. Перечисление без него признаётся доверенным,
    подписки поддерева без единого сообщения уходят в counted-miss и удаляются, а docstring
    `_scan_subscriptions` обещает, что прежний разбор сохранится;
  - набор файлов не совпадает с тем, что грузит Airflow: `glob` не учитывает `.airflowignore`
    (watcher регистрирует подписку из файла, DAG-а которого не существует, и consumer
    подтверждает и теряет сообщения) и пропускает скрытые файлы и каталоги, которые Airflow
    загружает (там `@rmq_trigger` сейчас даёт Broken DAG).
- Решение: собственный обход папки, повторяющий `airflow.utils.file._find_path_from_directory`
  (`os.walk(followlinks=True)`, `.airflowignore` в каждом каталоге, синтаксис из
  `[core] dag_ignore_file_syntax`, правила Airflow `_RegexpIgnoreRule` / `_GlobIgnoreRule`).
  Ошибка ввода-вывода на самом каталоге (листинг, `.airflowignore`, симлинк-каталог с
  недоступной целью) делает этот каталог нечитаемым; ошибка на отдельном файле остаётся
  ошибкой этого файла; ни одна ошибка обхода не роняет цикл.
- Декоратор применяет тот же `.airflowignore` к файлу, где вызван `@rmq_trigger`: фабрика в
  игнорируемом каталоге, которую импортирует DAG-файл, даёт Broken DAG «the RMQ watcher does
  not scan this file», а не молча теряет подписки.
- Нечитаемый каталог: файлы, которые watcher раньше видел под ним, идут той же дорогой, что
  нечитаемый файл (`_file_unreadable`). Прежний разбор держится `_UNREADABLE_FILE_LIMIT` циклов
  с WARNING, файлы входят в долю для вердикта доверия, дальше их ключи уходят обычным
  counted-miss. Сам каталог получает WARNING один раз, когда становится нечитаемым, и INFO,
  когда обход снова его прочитал. Каталог, нечитаемый с самого старта (`lost+found`), ничего
  не блокирует.

## Context (from discovery)
- `airflow_provider_rmq/watcher/listener.py`: `_get_dags_folder` (проверка корня),
  `_scan_subscriptions` (glob, mtime-кэш, вердикт доверия `_TRUSTED_SCAN_FRACTION`,
  `_UNTRUSTED_SCAN_LIMIT`, вытеснение путей), `_file_unreadable`, `_untrusted_listing_reason`;
  `except Exception` цикла (`listener.py:898`) роняет весь цикл на любом исключении скана.
- `airflow_provider_rmq/watcher/dag_file_scan.py`: `_watcher_scans` + `_is_scanned_below`
  (правило «`.py` внутри DAGS_FOLDER, без компонента с точкой»), текст ошибки в
  `_verify_static_readability` (шаг 1).
- Airflow `airflow/utils/file.py`: код `_IgnoreRule` … `find_path_from_directory` в 2.9.1 и
  2.11.2 идентичен (сверено, отличаются только docstring'и). `os.walk` в нём без `onerror`, так
  что `list_py_file_paths` первый дефект не закрывает. Цикл симлинков → `RuntimeError`.
  Неподдерживаемый синтаксис → `ValueError`. Файл берётся при `path.is_file()` и
  `path.suffix == ".py"` (или zip). Синтаксис по умолчанию у `find_path_from_directory`
  вычисляется один раз при импорте модуля.
- Python 3.10: `Path.is_file()` / `is_dir()` глотают только ENOENT, ENOTDIR, EBADF, ELOOP;
  в каталоге с `r` без `x` они бросают `PermissionError`, на ESTALE/EIO — `OSError`.
  `GitWildMatchPattern` бросает `GitIgnorePatternError` (`ValueError`) на строках вроде `!`, `\`.
  `_RegexpIgnoreRule.match` (re2) бросает `UnicodeEncodeError` на имени не в UTF-8.
- `os.walk(followlinks=True)` в 3.10: `onerror` вызывается только при сбое `scandir(top)`.
  Если `entry.is_dir()` бросает `OSError` (симлинк на каталог с недоступной целью, ESTALE/EIO,
  файловая система с DT_UNKNOWN), запись молча попадает в список файлов, и в неё не заходят.
- Airflow `find_dag_file_paths` ловит исключение `path.is_file()` пофайлово («Error while
  examining») и пропускает только этот файл.
- Фабрика с `@rmq_trigger` в отдельном модуле, который импортирует DAG-файл, — поддерживаемый
  случай (readme, ADR 0009, `rmq_static_check_factories` в
  `tests/watcher/test_decorator_static_check.py`). Проверка декоратора идёт по файлу вызова
  `rmq_trigger(...)`, то есть по фабрике.
- Тесты: `tests/watcher/test_listener.py` патчит `listener.glob.glob` (~38 мест);
  `tests/watcher/test_static_readability.py` сверяет `_watcher_scans` с glob;
  `tests/watcher/test_decorator_static_check.py:578` тестирует скрытую папку как отказ;
  footprint-тест импорта — `tests/watcher/test_dag_file_scan.py:48`.
- Ограничение: `dag_file_scan.py` импортирует только stdlib + `subscription_builder` (он
  выполняется в DAG processor при каждом импорте) на уровне модуля. Новый обход живёт
  отдельно и импортирует `airflow` лениво, внутри функции, как `_get_dags_folder`;
  `_watcher_scans` импортирует `dag_folder_listing` тоже внутри функции — к этому моменту
  DAG-файл уже импортировал `airflow`.

## Development Approach
- **testing approach**: Regular (code first, then tests)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change: `.venv/bin/python -m pytest -q tests/watcher`
- comments, docstrings and test names describe the present only (global CLAUDE.md); history
  goes to commits

## Testing Strategy
- **unit tests**: required for every task.
- обход тестируется на реальных каталогах в `tmp_path`: каталоги с правами 000, 644 (`r` без
  `x`) и 311 (`x` без `r`) — такие тесты пропускаются под root, где `chmod` не отнимает доступ;
  `.airflowignore` в обоих синтаксисах, вложенные `.airflowignore`, отрицание `!` в
  glob-синтаксисе, невалидный glob-паттерн, `.airflowignore` не в UTF-8, имя файла не в UTF-8
  под regexp-`.airflowignore`, симлинк на каталог, симлинк на каталог с недоступной целью,
  `.py`-симлинк в каталог 000 рядом с нормальными файлами (в корне и в подкаталоге), цикл
  симлинков, скрытые файлы, файл с именем ровно `.py`, битый симлинк `.py`, `.zip` рядом,
  `DAGS_FOLDER` со слешем на конце.
- сверка обхода и проверки декоратора: на том же дереве для каждого `.py`-файла
  `is_listed(path, folder)` равно `path in list_dag_files(folder).files`.
- сверка с Airflow, главный тест на расхождение: для каждого синтаксиса набор `files` нового
  обхода равен `{p for p in find_path_from_directory(folder, ".airflowignore",
  ignore_file_syntax=syntax) if Path(p).is_file() and Path(p).suffix == ".py"}`; синтаксис
  обхода задаётся `monkeypatch.setenv("AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX", syntax)`. Плюс
  одна проверка для синтаксиса по умолчанию против
  `list_py_file_paths(folder, safe_mode=False, include_examples=False)`. Дерево сверки включает
  каталог 000 (Airflow его молча пропускает, обход тоже не даёт из него файлов); нечитаемый
  `.airflowignore` и цикл симлинков в сверку не входят — там Airflow падает.
- e2e/UI тестов в проекте нет.

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview
- Новый модуль `airflow_provider_rmq/watcher/dag_folder_listing.py` с функцией
  `list_dag_files(dags_folder) -> DagFolderListing`.
- `_scan_subscriptions` берёт из листинга `files` вместо glob. После цикла stat/parse пути из
  `_last_mtimes` / `_cached_subs` / `_read_failures`, лежащие под нечитаемым каталогом, ровно
  один раз добавляются в `current_files` и в `failures` с его ошибкой. Дальше их ведёт
  существующий код: доля читаемого, вердикт, `_file_unreadable`, вытеснение. Ни один путь под
  нечитаемым каталогом не вытесняется в этом цикле.
- Ошибки, при которых Airflow сам не получает списка (цикл симлинков, неподдерживаемый
  `dag_ignore_file_syntax`, невалидный glob-паттерн, `.airflowignore` не в UTF-8), и нечитаемый
  корень (включая его `.airflowignore`) становятся `_UntrustedScan` до вердикта, как сбой корня
  в `_get_dags_folder`: счётчик недоверия не тратится и не пополняется.
- Декоратор: `_watcher_scans` — «`.py` внутри DAGS_FOLDER (abspath, затем realpath) и не
  отсеян `.airflowignore`». Правило про точку уходит. Отсев считает
  `dag_folder_listing.is_listed(path, dags_folder)` по цепочке каталогов от DAGS_FOLDER до
  файла теми же правилами, что и обход; `dag_file_scan` импортирует его лениво, внутри
  функции, так что импорт `dag_file_scan` по-прежнему не тянет `airflow`. Так фабрика в
  игнорируемом каталоге даёт Broken DAG. Цена: прямой импорт игнорируемого файла внутри
  DAGS_FOLDER (pytest на тестовой копии в `tests/` под `.airflowignore`) тоже падает;
  `DagBag` такие файлы не импортирует.
- zip: остаётся отказ. Watcher архивы не читает, `@rmq_trigger` в zip падает Broken DAG
  («cannot read this file»), это документируется.
- `safe_mode` Airflow не повторяется: файл, который принимает `_read_file_triggers`,
  импортирует `dag` из `airflow.decorators` / `airflow.models.dag`, значит в нём всегда есть
  слова `airflow` и `dag`, и эвристика Airflow по умолчанию его пропускает. Пользовательский
  `[core] might_contain_dag_callable` может отсеять файл, который watcher читает, — это
  ограничение документируется.

## Technical Details
- `DagFolderListing` (NamedTuple): `files: set[str]` (пути в написании обхода:
  `os.path.join(root, name)` от `dags_folder`); `unreadable_dirs: dict[str, Exception]` (путь
  каталога в том же написании → ошибка).
  `recovered: set[str]` — каталоги из переданного `recheck`, которые обход в этот раз прочитал
  без ошибки.
- `list_dag_files(dags_folder, recheck=frozenset())`; `recheck` — нечитаемые каталоги прошлого
  цикла.
- `DagFolderListingError(Exception)` — один класс, причина в сообщении.
- Обход `os.walk(dags_folder, followlinks=True, onerror=...)`; `onerror` кладёт
  `exc.filename` в `unreadable_dirs`. Для каждого `root` — три уровня ошибок:
  - **уровень каталога**, в одном `try/except OSError`: `<root>/.airflowignore` (`is_file()`,
    `open`, `read()`; строки — `ifile.read().split("\n")`, комментарии срезаются
    `re2.sub(r"\s*#.*", "", line)`, как у Airflow), компиляция правил
    `rule_type.compile(line, Path(dags_folder), ignore_file_path)` (`None` отбрасывается, дубли
    удаляются с сохранением порядка, наследование по `patterns_by_dir[Path(root).resolve()]` —
    всё как в `_find_path_from_directory`). `OSError` → `unreadable_dirs[root] = exc`,
    `dirs[:] = []`, файлы каталога не берутся: что в нём игнорировать, неизвестно;
  - **уровень подкаталога**: отсев `rule_type.match(Path(root) / sd, patterns)`; `OSError` из
    `match` (`is_dir()` у glob-паттерна на `/`) → этот подкаталог в `unreadable_dirs` и убран из
    `dirs`; повтор `(Path(root) / sd).resolve()` в `patterns_by_dir` → `DagFolderListingError`
    (цикл);
  - **уровень записи**: каждая запись из `files` `os.walk`, не отсеянная правилами:
    - имя с `Path(name).suffix == ".py"` (и не `.airflowignore`): `Path(...).is_file()` True →
      в `files`; `OSError` из `is_file()` или из `match` → тоже в `files`, чтобы `getmtime` в
      `_scan_subscriptions` провалился и повёл файл путём `_file_unreadable`, как сейчас;
      False (битый симлинк, каталог с именем `x.py`) → не берётся;
    - остальные записи: `OSError` из `match` → `unreadable_dirs[p] = exc` (glob-паттерн на `/`
      зовёт `is_dir()`, и на симлинк-каталоге с недоступной целью он падает раньше всего
      остального); дальше `os.path.islink(p)` или `os.lstat(p)` показывает каталог →
      `os.stat(p)`; `OSError` с errno не из ENOENT/ENOTDIR/ELOOP (и `OSError` самого `lstat`,
      кроме ENOENT) → `unreadable_dirs[p] = exc`: это каталог, в который `os.walk` не зашёл,
      потому что не смог его опознать. ENOENT/ENOTDIR/ELOOP — битая ссылка, такой каталог не
      существует;
  - каталог из `recheck`, прочитанный без ошибки уровня каталога, → `recovered`;
  - любое исключение, кроме `OSError`, из чтения `.airflowignore`, `compile` или `match`
    (`ValueError` на невалидном паттерне, `UnicodeDecodeError` / `UnicodeEncodeError`) →
    `DagFolderListingError`: Airflow на этом же падает и не получает списка.
  - **инвариант**: из `list_dag_files` выходит только `DagFolderListingError`. Каждый
    `OSError` обхода принадлежит одному из трёх уровней выше и превращается в нечитаемый
    каталог или в файл, который провалится на `getmtime`; ни один вызов файловой системы
    (`is_file`, `is_dir`, `stat`, `lstat`, `open`, `match`, `resolve`) не стоит вне такого
    уровня. `resolve` остаётся нестрогим, как у Airflow: ошибку `lstat` внутри него stdlib
    глотает, и каталог обходится как обычно.
- `is_listed(path, dags_folder) -> bool` для декоратора: путь — `.py` под DAGS_FOLDER в
  написании abspath, иначе realpath (как `_watcher_scans`). Выбранное написание задаёт базу
  для всего вызова: в ветке realpath правила компилируются с `base_dir =
  Path(os.path.realpath(dags_folder))`, и путь, и каталоги цепочки тоже в realpath, иначе
  `relative_to` у regexp-правил бросает `ValueError`; цепочка каталогов от DAGS_FOLDER
  до каталога файла, на каждом шаге читается `.airflowignore` и дополняются наследуемые
  правила, проверяется, что каталог не отсеян правилами родителя; в конце — что не отсеян сам
  файл. Любой `OSError` (чтение `.airflowignore`, `is_dir()` внутри `match`), исключение
  `compile`/`match` и неизвестный синтаксис → `DagFolderListingError`; из `is_listed`
  выходит только оно, декоратор превращает его в `RMQTriggerNotStaticError` с причиной.
- Синтаксис: `conf.get("core", "DAG_IGNORE_FILE_SYNTAX", fallback="regexp")`, сравнение с
  учётом регистра, как у Airflow; `glob` → `_GlobIgnoreRule`, `regexp` или пусто →
  `_RegexpIgnoreRule`, иначе `DagFolderListingError`.
- `_scan_subscriptions`:
  - `DagFolderListingError` → `_UntrustedScan(<причина>)`;
  - корень в `unreadable_dirs` (сравнение по `os.path.normpath`) → `_UntrustedScan(... cannot
    be listed ...)`;
  - «путь под каталогом»: `path.startswith(os.path.join(dir_path, ""))`, как в
    `_is_scanned_below`; пути в кэше и каталоги из обхода в одном написании, потому что оба
    получены от одного обхода (`DAGS_FOLDER` со слешем на конце покрыт тестом);
  - `self._unreadable_dirs: set[str]` прошлого цикла передаётся как `recheck`: каталог, впервые
    попавший в набор, → WARNING с ошибкой; оставшийся → DEBUG; из `recovered` → INFO «readable
    again»; выпавший иначе (удалён, игнорируется, ушёл под нечитаемого предка) → только
    убирается из набора, без сообщения о восстановлении.
- Сообщение `_verify_static_readability` шаг 1: «The watcher reads only *.py files inside
  DAGS_FOLDER (<folder>) that .airflowignore does not exclude», без фразы про точку.

## What Goes Where
- **Implementation Steps**: код, тесты, документация в этом репозитории.
- **Post-Completion**: ручная проверка на Airflow 2.9.1, аудит прода (скрытые каталоги,
  `.airflowignore` с `@rmq_trigger`, тяжёлые скрытые каталоги).

## Implementation Steps

### Task 1: Обход папки DAG-файлов по правилам Airflow

**Files:**
- Create: `airflow_provider_rmq/watcher/dag_folder_listing.py`
- Create: `tests/watcher/test_dag_folder_listing.py`

- [x] `DagFolderListing`, `DagFolderListingError`, `list_dag_files(dags_folder, recheck)` и
      `is_listed(path, dags_folder)` по Technical Details; импорт `airflow.configuration.conf`,
      правил `airflow.utils.file` и `re2` внутри функций
- [x] docstring модуля: обход повторяет `_find_path_from_directory`; отличия — ошибки ввода-вывода
      делают каталог нечитаемым, берутся только `.py`, zip и `safe_mode` не повторяются
- [x] тесты успеха: плоская папка, вложенные каталоги, скрытые файл (`.x.py`) и каталог
      (`.hidden/`) входят, `.airflowignore` regexp и glob (с `!`-отрицанием), вложенный
      `.airflowignore` наследует правила родителя, симлинк на каталог проходится; файл с именем
      ровно `.py`, битый симлинк `.py` и `.zip` не входят; `DAGS_FOLDER` со слешем на конце
- [x] тесты ошибок (skip под root): каталог 000, 644 и 311 → в `unreadable_dirs`, соседние
      файлы на месте, цикл не падает; нечитаемый `.airflowignore` → каталог в
      `unreadable_dirs`, его файлов нет; симлинк на каталог, у цели которого предок потерял `x`
      → симлинк в `unreadable_dirs`; `.py`-симлинк в каталог 000 рядом с нормальными файлами
      (в корне и в подкаталоге) → он в `files`, соседи в `files`, каталог не в
      `unreadable_dirs`; битый симлинк без `.py` → ничего; `recheck` с каталогом, который снова
      читается, → он в `recovered`, с каталогом под новым нечитаемым предком → нет; цикл
      симлинков, неизвестный синтаксис, невалидный glob-паттерн, `.airflowignore` не в UTF-8,
      имя не в UTF-8 под regexp-`.airflowignore` → `DagFolderListingError`; glob-синтаксис с
      паттерном `name/` и симлинк-каталог с недоступной целью → он в `unreadable_dirs`, обход
      не падает
- [x] тест инварианта, две части:
      - перебор: для дерева со всеми видами записей (файл `.py`, файл не `.py`, каталог,
        симлинк-каталог, `.airflowignore`) и обоих синтаксисов, с glob-паттерном на `/`, по
        очереди подменять `Path.is_file`, `Path.is_dir`, `os.stat`, `os.lstat`, `open` так,
        чтобы они бросали `OSError` на одном пути; утверждение одно — `list_dag_files` не
        бросает ничего, кроме `DagFolderListingError`. Где путь окажется, здесь не
        проверяется: часть подмен до кода обхода не доходит (`os.lstat` внутри нестрогого
        `Path.resolve` / `os.path.realpath` глотает stdlib, `Path.is_dir` при regexp-синтаксисе
        не вызывается, `Path.stat` в 3.10 не видит патч `os.stat`);
      - учёт пути, на явном списке случаев, где ошибка доходит до кода обхода:
        `Path.is_file` на `.py` → путь в `files`; `Path.is_dir` из `match` при glob-паттерне на
        `/` на подкаталоге и на симлинк-записи → путь в `unreadable_dirs`; собственный
        `os.stat` обхода на симлинк-записи → путь в `unreadable_dirs`; `open` файла
        `.airflowignore` → его каталог в `unreadable_dirs`
- [x] тесты `is_listed`: файл в корне, во вложенном каталоге, под игнорируемым каталогом, сам
      игнорируемый файл, отрицание в glob, вне DAGS_FOLDER, через realpath симлинк-DAGS_FOLDER
      с непустым regexp- и glob-`.airflowignore` (и игнорируемый, и не игнорируемый файл); сверка с
      `list_dag_files` по Testing Strategy
- [x] тест сверки по Testing Strategy: оба синтаксиса против `find_path_from_directory` с
      явным `ignore_file_syntax`, синтаксис по умолчанию против `list_py_file_paths`
- [x] run tests - must pass before next task

### Task 2: `_scan_subscriptions` на новом обходе, нечитаемые каталоги как нечитаемые файлы

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [x] заменить `glob.glob(...)` на `list_dag_files(dags_folder)`; `DagFolderListingError` и
      нечитаемый корень → `_UntrustedScan` до вердикта; убрать неиспользуемый `import glob`
- [x] после цикла stat/parse добавить известные пути под каждым нечитаемым каталогом в
      `current_files` и в `failures` (ошибка каталога, mtime `None`) — ровно один раз, без stat
      и разбора
- [x] `self._unreadable_dirs` как `recheck` и логирование: WARNING при появлении каталога,
      DEBUG пока он нечитаем, INFO только для `recovered`
- [x] `_untrusted_listing_reason`: назвать нечитаемые каталоги в причине, когда они есть
- [x] привести к тому, что код гарантирует, docstring'и `_get_dags_folder` и
      `_scan_subscriptions` (нечитаемый подкаталог: прежний разбор его файлов держится как у
      нечитаемого файла; `.airflowignore`; скрытые файлы) и комментарий в `listener.py:1198`
      («The file is in the glob»)
- [x] тестовый хелпер `_listing(*files)` → `DagFolderListing(set(files), {})`; перевести патчи
      `listener.glob.glob` на `listener.list_dag_files` без изменения смысла тестов;
      переименовать в формулировки про листинг: `test_empty_glob_*` (903, 929, 949, 5131), 882,
      1131; `test_empty_setting_keeps_glob_from_running` (1153-1163) проверяет, что
      `list_dag_files` не вызывается; тест 1178-1189, проверяющий поведение самого
      `glob.glob`, переписать на нечитаемый корень/подкаталог через новый обход
- [x] новые тесты: подкаталог с ранее разобранным файлом становится нечитаемым → подписки этого
      цикла на месте, WARNING по каталогу и «holding its previous parse» по файлу, путь не
      вытеснен, файл посчитан в доле один раз; следующий цикл → подписки сброшены
      (counted-miss); доступ вернулся → INFO, файл перечитан; каталог нечитаем со старта →
      скан доверенный, один WARNING по каталогу, ни одного по файлам; большое нечитаемое
      поддерево роняет долю → `_UntrustedScan` с именем каталога в причине; нечитаемый
      `.airflowignore` корня и `DagFolderListingError` → `_UntrustedScan`, счётчик недоверия не
      тронут; файл под `.airflowignore` не даёт подписку; файл в скрытом каталоге даёт;
      `DAGS_FOLDER` со слешем на конце сопоставляет пути под нечитаемым каталогом; симлинк на
      каталог, цель которого стала недоступна, держит прежний разбор файлов под ним; один
      `.py`, на котором падает `is_file()`, в корне или в подкаталоге — ошибка только этого
      файла, соседи и доверие скана не затронуты; дочерний нечитаемый каталог под ставшим
      нечитаемым родителем не даёт INFO «readable again»
- [x] run tests - must pass before next task

### Task 3: Проверка декоратора без правила точки

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Modify: `airflow_provider_rmq/watcher/decorators.py`
- Modify: `tests/watcher/test_static_readability.py`
- Modify: `tests/watcher/test_decorator_static_check.py`
- Modify: `tests/watcher/test_dag_file_scan.py`

- [x] `_watcher_scans` / `_is_scanned_below`: «`.py` внутри DAGS_FOLDER (abspath, затем
      realpath) и `is_listed`», без правила точки; `dag_folder_listing` импортируется внутри
      функции; `DagFolderListingError` → `RMQTriggerNotStaticError` с причиной
- [x] текст шага 1 в `_verify_static_readability` по Technical Details
- [x] docstring `rmq_trigger` в `decorators.py` (189-191): файл, который сканирует watcher, —
      `.py` в DAGS_FOLDER, не отсеянный `.airflowignore`; без правила точки
- [x] `test_static_readability.py`: перевести на настоящее время тесты правила точки —
      параметры `.x.py` и `.hidden/a.py` (117-121), исключение `.linked` (164-173), 189-190,
      199, `test_file_in_a_hidden_directory` и `test_file_name_starting_with_a_dot` (349-357)
      → True; убрать glob-комментарий 201-203; сверку перевести с glob на `list_dag_files`:
      `scanned == listed` для простого дерева, дерева с `.airflowignore` и
      симлинк-подкаталога; для симлинк-DAGS_FOLDER — `listed <= scanned` и равенство по
      realpath, как сейчас (ветка realpath в `_watcher_scans` по замыслу принимает оба
      написания); добавить: файл под `.airflowignore` → False
- [x] `test_decorator_static_check.py:578`: имя и docstring в настоящем времени («DagBag loads
      a hidden folder and the watcher reads its subscription»), проверка — загрузка без ошибки и
      совпадение подписок; добавить `DagBag` на папке с `.airflowignore`: игнорируемый файл не
      импортируется, остальные грузятся; DAG-файл, импортирующий фабрику с `@rmq_trigger` из
      игнорируемого `common/`, → Broken DAG с «does not scan»; та же фабрика в неигнорируемом
      каталоге → грузится
- [x] footprint-тест (`test_dag_file_scan.py:48`): после свежего импорта `dag_file_scan` в
      `sys.modules` нет ни `airflow`, ни `airflow_provider_rmq.watcher.dag_folder_listing`
- [x] run tests - must pass before next task

### Task 4: Verify acceptance criteria
- [ ] нечитаемый подкаталог виден в логе (WARNING по каталогу) и держит прежний разбор, как
      нечитаемый файл; ни одна ошибка ввода-вывода обхода не роняет цикл
- [ ] файл под `.airflowignore` не даёт подписку; скрытый файл даёт; zip даёт Broken DAG;
      фабрика с `@rmq_trigger` под `.airflowignore` даёт Broken DAG
- [ ] ошибка на одном файле остаётся ошибкой файла; симлинк-каталог с недоступной целью
      держит прежний разбор
- [ ] набор файлов совпадает с Airflow (тесты сверки зелёные для обоих синтаксисов)
- [ ] run full test suite: `.venv/bin/python -m pytest -q`
- [ ] `ruff check` и `mypy` по затронутым файлам: счётчики не выше базовых (120 / 32)

### Task 5: [Final] Update documentation
- [ ] `readme.md` и `readme_ru.md` (синхронно): абзац «`@rmq_trigger` must also sit in a file
      the watcher scans» — набор файлов как у Airflow (`.airflowignore`, скрытые входят), zip
      не читается и даёт Broken DAG, `might_contain_dag_callable` не повторяется; `@rmq_trigger`
      в файле под `.airflowignore` (фабрика в `common/`) даёт Broken DAG, прямой импорт такого
      файла внутри DAGS_FOLDER тоже; поведение при
      нечитаемом подкаталоге; новый `.airflowignore`, скрывающий больше половины папки,
      проходит `_UNTRUSTED_SCAN_LIMIT` недоверенных циклов, как любое реальное сокращение
- [ ] `CHANGELOG.md` v2.7.0: Changed — скрытые файлы и каталоги читаются, файлы под
      `.airflowignore` нет, `@rmq_trigger` в файле под `.airflowignore` обрывает импорт; Fixed —
      нечитаемый подкаталог виден в логе и держит прежний разбор своих файлов, как нечитаемый
      файл; переписать пункт про правило точки
- [ ] `docs/adr/0009-rmq-trigger-static-check-at-import.md`: переписать в настоящем времени
      пункт про скрытый каталог и файл с точкой (78-79)
- [ ] `docs/adr/0010-watcher-lists-dag-folder-like-airflow.md`: свой обход вместо
      `list_py_file_paths` (нет `onerror`), приватные правила Airflow (сверены для 2.9.1 и
      2.11.2), zip, `safe_mode` и `might_contain_dag_callable` не повторяются; декоратор
      применяет `.airflowignore` к файлу вызова (фабрика в игнорируемом каталоге), цена —
      прямой импорт игнорируемого файла внутри DAGS_FOLDER падает
- [ ] `CONTEXT.md`: уточнить «Trusted scan» про нечитаемые подкаталоги
- [ ] `docs/live-verification.md`: шаг ручной проверки (chmod подкаталога, `.airflowignore`)
- [ ] `git rm` `docs/backlog/unreadable-subdir-passes-as-trusted.md` и
      `docs/backlog/scan-ignores-airflowignore.md`
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification** (Airflow 2.9.1):
- `chmod 000` подкаталога с DAG-ом под `@rmq_trigger` → WARNING по каталогу и «holding its
  previous parse», через цикл подписки уходят counted-miss; вернуть права → INFO, подписка
  восстановлена.
- `.airflowignore` с каталогом, где лежит копия DAG-файла → вторая подписка не появляется.
- DAG с `@rmq_trigger` в скрытом каталоге → не Broken DAG, подписка на странице.

**Аудит прода перед выкаткой:**
- скрытые каталоги в DAGS_FOLDER с `*.py` (`find $DAGS_FOLDER -path '*/.*' -name '*.py'`):
  их подписки появятся впервые. Мусор (`.ipynb_checkpoints`, копии) — в `.airflowignore`.
- тяжёлые скрытые каталоги внутри DAGS_FOLDER (`.git`, `.venv`, `.tox`, `node_modules`):
  watcher не применяет `safe_mode`, поэтому разберёт каждый `.py` в них, и первый цикл может
  выйти за `_STEP_TIMEOUT`. Добавить их в `.airflowignore` до выкатки.
- файлы с `@rmq_trigger` под `.airflowignore`: DAG-файлы — их подписки (если были) уйдут
  counted-miss; модули-фабрики, которые импортируют DAG-файлы, — эти DAG-файлы станут Broken DAG.
  Найти до выкатки и вынести фабрики из игнорируемых каталогов (или убрать каталог из
  `.airflowignore`).
- тесты и CI, импортирующие напрямую файл с `@rmq_trigger` под `.airflowignore` внутри
  DAGS_FOLDER, начнут падать на импорте.
