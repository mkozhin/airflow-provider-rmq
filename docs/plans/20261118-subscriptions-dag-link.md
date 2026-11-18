# Ссылка на страницу DAG'а со страницы подписок

## Overview
- На странице Subscriptions (`RMQWatcherView.subscriptions`) имя DAG'а в первой колонке становится ссылкой на страницу этого DAG'а в Airflow.
- Ссылка ведёт на `url_for('Airflow.dag', dag_id=…)`, то есть на `/dags/<dag_id>`. Airflow перенаправляет этот адрес на Grid.
- Ссылка ставится, если DAG не помечен как отсутствующий. Строка с бейджем «⚠ dag not found» показывает имя простым текстом, как сейчас.
- Остальное на странице не меняется: колонки, бейджи, статусы, действия.

## Context (from discovery)
- `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`: имя DAG'а выводится в двух ветках — в групповой строке (около строки 212) и в одиночной (около строки 275). Бейдж «⚠ dag not found» стоит в каждой ветке рядом с именем; его условие: `active_dag_ids is defined and active_dag_ids is not none and item.dag_id not in active_dag_ids`.
- `active_dag_ids` передаёт вью (`views.py`, около строки 606): множество активных `dag_id` или `None`, если поиск DAG'ов в метабазе не удался.
- Endpoint `Airflow.dag` (`/dags/<string:dag_id>`) есть в Airflow 2.9.1 (`airflow/www/views.py:2777`) и в 2.11.2; он перенаправляет на `Airflow.grid`.
- `tests/watcher/test_views.py`: `_render_subscriptions_html` отрисовывает настоящий шаблон через отдельный `jinja2.Environment`; `url_for` в нём — заглушка, возвращающая `"#"`. Фикстуры строк: `_make_group`, `_make_single_row`; класс тестов `TestSubscriptionsTemplateRendering`.

## Development Approach
- **testing approach**: Regular (сначала шаблон, затем тесты)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run tests after each change
- maintain backward compatibility

## Testing Strategy
- **unit tests**: отрисовка настоящего шаблона через `_render_subscriptions_html` в `tests/watcher/test_views.py`; проверяется HTML, а не kwargs `render_template`.
- **e2e tests**: в проекте их нет.

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview
- Правка только в шаблоне: в обеих ветках имя DAG'а оборачивается в `<a href="{{ url_for('Airflow.dag', dag_id=item.dag_id) }}">`, если DAG не помечен как отсутствующий.
- В каждой ветке существующее условие бейджа записывается один раз: `{% set dag_missing = active_dag_ids is defined and active_dag_ids is not none and item.dag_id not in active_dag_ids %}`. Бейдж выводится при `dag_missing`, ссылка — при `not dag_missing`, так что они всегда взаимоисключающие.
- Ссылка есть, когда `active_dag_ids` не передан в контекст, равен `None` или содержит `item.dag_id`. При неудавшемся поиске DAG'ов бейджа нет, и ссылка ставится: для отсутствующего DAG'а Airflow сам покажет своё сообщение.
- Ссылка открывается в той же вкладке, как имя DAG'а на главной странице Airflow.
- Python-код, модели и вью не меняются.

## Technical Details
- Обе ветки шаблона получают одинаковую разметку ячейки; дублирование двух веток сохраняется, как во всём шаблоне.
- `dag_id` экранируется `url_for` и автоэкранированием Jinja, как остальные значения на странице. Тестовый `jinja2.Environment` собран без autoescape, поэтому тест на экранирование в нём не пишется.
- Заглушка `url_for` в тестовом рендере возвращает детерминированную строку с endpoint и аргументами: `lambda endpoint, **kw: "/" + endpoint + "?" + urlencode(sorted(kw.items()))`. Ни один существующий тест не опирается на прежнее значение `"#"`.

## What Goes Where
- **Implementation Steps**: шаблон, тесты, CHANGELOG.
- **Post-Completion**: ручная проверка на тестовом стенде.

## Implementation Steps

### Task 1: Ссылка на DAG в ячейке имени DAG'а

**Files:**
- Modify: `airflow_provider_rmq/watcher/templates/rmq_watcher/subscriptions.html`
- Modify: `tests/watcher/test_views.py`

- [x] в групповой ветке `subscriptions.html` записать `{% set dag_missing = … %}` из существующего условия бейджа; при `not dag_missing` вывести `item.dag_id` ссылкой на `url_for('Airflow.dag', dag_id=item.dag_id)`, при `dag_missing` — простым текстом с бейджем
- [x] то же в одиночной ветке
- [x] в `test_views.py` заменить заглушку `url_for` на `lambda endpoint, **kw: "/" + endpoint + "?" + urlencode(sorted(kw.items()))`; существующие тесты отрисовки остаются зелёными
- [x] тесты: активный DAG — точный `href` на `Airflow.dag` с его `dag_id` в групповой и в одиночной строке; у групповой и одиночной строки разные `dag_id`
- [x] тесты: DAG с бейджем «⚠ dag not found» — ссылки нет, имя и бейдж на месте, в групповой и в одиночной строке; имя и отсутствие ссылки проверяются по первой `<td>` строки (её текст содержит `dag_id`, в ней нет `<a`), а не по всему HTML: `dag_id` встречается на странице ещё и в `confirm` кнопки Delete
- [x] тест: `active_dag_ids=None` — ссылка есть, бейджа нет
- [x] тест: `active_dag_ids` не передан в контекст — ссылка есть, бейджа нет
- [x] run tests: `.venv/bin/pytest tests/watcher/test_views.py -q` — must pass before task 2

### Task 2: Verify acceptance criteria
- [ ] verify all requirements from Overview are implemented
- [ ] verify edge cases are handled (отсутствующий DAG, неудавшийся поиск DAG'ов)
- [ ] run full test suite: `.venv/bin/pytest -q`
- [ ] run linter: `.venv/bin/ruff check .` — не больше 127 находок (столько на main на 2026-11-18)

### Task 3: [Final] Update documentation
- [ ] CHANGELOG v2.7.0, раздел Added: одна строка — имя DAG'а на странице Subscriptions ведёт на страницу DAG'а в Airflow
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- на тестовом стенде открыть Subscriptions, кликнуть по имени активного DAG'а — открывается его Grid
- строка с несуществующим `dag_id` показывает имя без ссылки и бейдж «⚠ dag not found»
