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
  Любая ошибка ввода-вывода на каталоге делает этот каталог нечитаемым, а не роняет цикл.
- Нечитаемый каталог: файлы, которые watcher раньше видел под ним, идут той же дорогой, что
  нечитаемый файл (`_file_unreadable`). Прежний разбор держится `_UNREADABLE_FILE_LIMIT` циклов
  с WARNING, файлы входят в долю для вердикта доверия, дальше их ключи уходят обычным
  counted-miss. Сам каталог получает WARNING один раз, когда становится нечитаемым, и INFO,
  когда снова читается. Каталог, нечитаемый с самого старта (`lost+found`), ничего не
  блокирует.

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
- Тесты: `tests/watcher/test_listener.py` патчит `listener.glob.glob` (~38 мест);
  `tests/watcher/test_static_readability.py` сверяет `_watcher_scans` с glob;
  `tests/watcher/test_decorator_static_check.py:578` тестирует скрытую папку как отказ;
  footprint-тест импорта — `tests/watcher/test_dag_file_scan.py:48`.
- Ограничение: `dag_file_scan.py` импортирует только stdlib + `subscription_builder` (он
  выполняется в DAG processor при каждом импорте). Новый обход живёт отдельно и импортирует
  `airflow` лениво, внутри функции, как `_get_dags_folder`.

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
  glob-синтаксисе, невалидный glob-паттерн, `.airflowignore` не в UTF-8, симлинк на каталог,
  цикл симлинков, скрытые файлы, файл с именем ровно `.py`, битый симлинк `.py`, `.zip` рядом,
  `DAGS_FOLDER` со слешем на конце.
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
- Декоратор: `_watcher_scans` сводится к «`.py` внутри DAGS_FOLDER» (abspath, затем realpath).
  Правило про точку уходит. `.airflowignore` декоратор не читает: такой файл Airflow не
  импортирует, DAG-а нет, рассогласования нет, а прямой импорт тестовой копии (pytest, CI)
  продолжает работать.
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
- `DagFolderListingError(Exception)` — один класс, причина в сообщении.
- Обход `os.walk(dags_folder, followlinks=True, onerror=...)`; `onerror` кладёт
  `exc.filename` в `unreadable_dirs`. Для каждого `root` вся работа каталога — в одном
  `try/except OSError`, файлы набираются в локальный список и добавляются в `files` только при
  успехе:
  - `<root>/.airflowignore`: `is_file()`, `open`, `read()`; строки — `ifile.read().split("\n")`,
    комментарии срезаются `re2.sub(r"\s*#.*", "", line)`, как у Airflow;
  - правила компилируются `rule_type.compile(line, Path(dags_folder), ignore_file_path)`,
    `None` отбрасывается, дубли удаляются с сохранением порядка, наследование по
    `patterns_by_dir[Path(root).resolve()]` — всё как в `_find_path_from_directory`;
  - отсев подкаталогов `rule_type.match(Path(root) / sd, patterns)`; повтор
    `(Path(root) / sd).resolve()` в `patterns_by_dir` → `DagFolderListingError` (цикл);
  - файл берётся, если имя не `.airflowignore`, `Path(name).suffix == ".py"`, не отсеян
    правилами и `Path(...).is_file()`;
  - `OSError` в любом из шагов → `unreadable_dirs[root] = exc`, `dirs[:] = []`, файлы каталога
    не берутся (что в нём игнорировать и какие файлы в нём есть, неизвестно);
  - `ValueError` из `compile` (невалидный паттерн) и `UnicodeDecodeError` при чтении
    `.airflowignore` → `DagFolderListingError`: Airflow на этом же падает и не получает списка.
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
  - `self._unreadable_dirs: set[str]` прошлого цикла: каталог, впервые попавший в набор, →
    WARNING с ошибкой; оставшийся → DEBUG; выпавший → INFO «readable again».
- Сообщение `_verify_static_readability` шаг 1: «The watcher reads only *.py files inside
  DAGS_FOLDER (<folder>)», без фразы про точку.

## What Goes Where
- **Implementation Steps**: код, тесты, документация в этом репозитории.
- **Post-Completion**: ручная проверка на Airflow 2.9.1, аудит прода (скрытые каталоги,
  `.airflowignore` с `@rmq_trigger`, тяжёлые скрытые каталоги).

## Implementation Steps

### Task 1: Обход папки DAG-файлов по правилам Airflow

**Files:**
- Create: `airflow_provider_rmq/watcher/dag_folder_listing.py`
- Create: `tests/watcher/test_dag_folder_listing.py`

- [ ] `DagFolderListing`, `DagFolderListingError`, `list_dag_files(dags_folder)` по Technical
      Details; импорт `airflow.configuration.conf`, правил `airflow.utils.file` и `re2` внутри
      функции
- [ ] docstring модуля: обход повторяет `_find_path_from_directory`; отличия — ошибки ввода-вывода
      делают каталог нечитаемым, берутся только `.py`, zip и `safe_mode` не повторяются
- [ ] тесты успеха: плоская папка, вложенные каталоги, скрытые файл (`.x.py`) и каталог
      (`.hidden/`) входят, `.airflowignore` regexp и glob (с `!`-отрицанием), вложенный
      `.airflowignore` наследует правила родителя, симлинк на каталог проходится; файл с именем
      ровно `.py`, битый симлинк `.py` и `.zip` не входят; `DAGS_FOLDER` со слешем на конце
- [ ] тесты ошибок (skip под root): каталог 000, 644 и 311 → в `unreadable_dirs`, соседние
      файлы на месте, цикл не падает; нечитаемый `.airflowignore` → каталог в
      `unreadable_dirs`, его файлов нет; цикл симлинков, неизвестный синтаксис, невалидный
      glob-паттерн, `.airflowignore` не в UTF-8 → `DagFolderListingError`
- [ ] тест сверки по Testing Strategy: оба синтаксиса против `find_path_from_directory` с
      явным `ignore_file_syntax`, синтаксис по умолчанию против `list_py_file_paths`
- [ ] run tests - must pass before next task

### Task 2: `_scan_subscriptions` на новом обходе, нечитаемые каталоги как нечитаемые файлы

**Files:**
- Modify: `airflow_provider_rmq/watcher/listener.py`
- Modify: `tests/watcher/test_listener.py`

- [ ] заменить `glob.glob(...)` на `list_dag_files(dags_folder)`; `DagFolderListingError` и
      нечитаемый корень → `_UntrustedScan` до вердикта; убрать неиспользуемый `import glob`
- [ ] после цикла stat/parse добавить известные пути под каждым нечитаемым каталогом в
      `current_files` и в `failures` (ошибка каталога, mtime `None`) — ровно один раз, без stat
      и разбора
- [ ] `self._unreadable_dirs` и логирование: WARNING при появлении каталога, DEBUG пока он
      нечитаем, INFO при восстановлении
- [ ] `_untrusted_listing_reason`: назвать нечитаемые каталоги в причине, когда они есть
- [ ] привести к тому, что код гарантирует, docstring'и `_get_dags_folder` и
      `_scan_subscriptions` (нечитаемый подкаталог: прежний разбор его файлов держится как у
      нечитаемого файла; `.airflowignore`; скрытые файлы) и комментарий в `listener.py:1198`
      («The file is in the glob»)
- [ ] тестовый хелпер `_listing(*files)` → `DagFolderListing(set(files), {})`; перевести патчи
      `listener.glob.glob` на `listener.list_dag_files` без изменения смысла тестов;
      переименовать в формулировки про листинг: `test_empty_glob_*` (903, 929, 949, 5131), 882,
      1131; `test_empty_setting_keeps_glob_from_running` (1153-1163) проверяет, что
      `list_dag_files` не вызывается; тест 1178-1189, проверяющий поведение самого
      `glob.glob`, переписать на нечитаемый корень/подкаталог через новый обход
- [ ] новые тесты: подкаталог с ранее разобранным файлом становится нечитаемым → подписки этого
      цикла на месте, WARNING по каталогу и «holding its previous parse» по файлу, путь не
      вытеснен, файл посчитан в доле один раз; следующий цикл → подписки сброшены
      (counted-miss); доступ вернулся → INFO, файл перечитан; каталог нечитаем со старта →
      скан доверенный, один WARNING по каталогу, ни одного по файлам; большое нечитаемое
      поддерево роняет долю → `_UntrustedScan` с именем каталога в причине; нечитаемый
      `.airflowignore` корня и `DagFolderListingError` → `_UntrustedScan`, счётчик недоверия не
      тронут; файл под `.airflowignore` не даёт подписку; файл в скрытом каталоге даёт;
      `DAGS_FOLDER` со слешем на конце сопоставляет пути под нечитаемым каталогом
- [ ] run tests - must pass before next task

### Task 3: Проверка декоратора без правила точки

**Files:**
- Modify: `airflow_provider_rmq/watcher/dag_file_scan.py`
- Modify: `tests/watcher/test_static_readability.py`
- Modify: `tests/watcher/test_decorator_static_check.py`
- Modify: `tests/watcher/test_dag_file_scan.py`

- [ ] `_watcher_scans` / `_is_scanned_below`: «`.py` внутри DAGS_FOLDER» (abspath, затем
      realpath), без правила точки; docstring говорит, почему `.airflowignore` не учитывается
- [ ] текст шага 1 в `_verify_static_readability` без фразы про точку
- [ ] `test_static_readability.py`: перевести на настоящее время тесты правила точки —
      параметры `.x.py` и `.hidden/a.py` (117-121), исключение `.linked` (164-173), 189-190,
      199, `test_file_in_a_hidden_directory` и `test_file_name_starting_with_a_dot` (349-357)
      → теперь True; убрать glob-комментарий 201-203; сверку перевести с glob на
      `list_dag_files`: на деревьях без `.airflowignore` `scanned == listed` (включая
      симлинк-подкаталог и симлинк-DAGS_FOLDER), с `.airflowignore` — односторонне «каждый
      файл листинга сканируется»; добавить: файл под `.airflowignore` → True
- [ ] `test_decorator_static_check.py:578`: имя и docstring в настоящем времени («DagBag loads
      a hidden folder and the watcher reads its subscription»), проверка — загрузка без ошибки и
      совпадение подписок; добавить `DagBag` на папке с `.airflowignore`: игнорируемый файл не
      импортируется, остальные грузятся
- [ ] footprint-тест (`test_dag_file_scan.py:48`): после свежего импорта `dag_file_scan` в
      `sys.modules` нет ни `airflow`, ни `airflow_provider_rmq.watcher.dag_folder_listing`
- [ ] run tests - must pass before next task

### Task 4: Verify acceptance criteria
- [ ] нечитаемый подкаталог виден в логе (WARNING по каталогу) и держит прежний разбор, как
      нечитаемый файл; ни одна ошибка ввода-вывода обхода не роняет цикл
- [ ] файл под `.airflowignore` не даёт подписку; скрытый файл даёт; zip даёт Broken DAG
- [ ] набор файлов совпадает с Airflow (тесты сверки зелёные для обоих синтаксисов)
- [ ] run full test suite: `.venv/bin/python -m pytest -q`
- [ ] `ruff check` и `mypy` по затронутым файлам: счётчики не выше базовых (120 / 32)

### Task 5: [Final] Update documentation
- [ ] `readme.md` и `readme_ru.md` (синхронно): абзац «`@rmq_trigger` must also sit in a file
      the watcher scans» — набор файлов как у Airflow (`.airflowignore`, скрытые входят), zip
      не читается и даёт Broken DAG, `might_contain_dag_callable` не повторяется; поведение при
      нечитаемом подкаталоге; новый `.airflowignore`, скрывающий больше половины папки,
      проходит `_UNTRUSTED_SCAN_LIMIT` недоверенных циклов, как любое реальное сокращение
- [ ] `CHANGELOG.md` v2.7.0: Changed — скрытые файлы и каталоги читаются, файлы под
      `.airflowignore` нет; Fixed — нечитаемый подкаталог виден в логе и держит прежний разбор
      своих файлов, как нечитаемый файл; переписать пункт про правило точки
- [ ] `docs/adr/0009-rmq-trigger-static-check-at-import.md`: переписать в настоящем времени
      пункт про скрытый каталог и файл с точкой (78-79)
- [ ] `docs/adr/0010-watcher-lists-dag-folder-like-airflow.md`: свой обход вместо
      `list_py_file_paths` (нет `onerror`), приватные правила Airflow (сверены для 2.9.1 и
      2.11.2), zip, `safe_mode` и `might_contain_dag_callable` не повторяются, декоратор не
      читает `.airflowignore`
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
- файлы с `@rmq_trigger` под `.airflowignore`: их подписки (если были) уйдут counted-miss.
