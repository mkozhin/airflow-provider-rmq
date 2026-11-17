---
worth: yes
where: .github/workflows/publish.yml:13
added: 2026-08-09
---
# CI не проверяет нижнюю границу поддерживаемого Airflow

**Контекст.** `pyproject.toml:29` объявляет поддержку
`apache-airflow>=2.9.0,<3.0.0`, но матрица CI
(`.github/workflows/publish.yml:13`) варьирует только версии Python, а шаг
установки (`publish.yml:24`) — `pip install -e ".[dev]"` без пина Airflow и
без constraints-файла — ставит ту версию, которая отрезолвится на момент
прогона. Фактически тестируется одна схема БД и один набор внутренних API, и
это последний 2.x, а не 2.9.1, на котором работает прод.

Сам workflow запускается только на push тега `v*` (`publish.yml:3-6`): ни pull
request, ни push в `main` тестов не запускают. Регрессия совместимости
обнаруживается в момент выпуска, когда job `publish` уже ждёт зелёного `test`.

**Почему это важно.** Провайдер напрямую импортирует и инстанцирует
внутренние ORM-модели Airflow (`DagModel` в `watcher/consumer.py:282` и
`watcher/models.py:398`, вызов `airflow.api.common.trigger_dag` в
`watcher/consumer.py:278`). Такие обращения — первое, что ломается между
минорными версиями Airflow, и покрытие одной версией это не ловит. Зелёный
прогон на 2.11 ничего не доказывает про 2.9.1.

**Смежные пробелы того же файла.** `ruff check` в CI не запускается вовсе:
на `ruff check .` сейчас 129 замечаний, и их никто не сторожит. Тестов против
живого брокера в CI нет — это вынесено отдельно в
[[no-broker-integration-tests]].

**Направление решения (не проработано).** Все правки — один файл
`.github/workflows/publish.yml`, их стоит делать одним заходом:

- триггеры `pull_request` и `push` в `main` для job'ы `test`, тег оставить
  только для `publish`;
- матрица `airflow-version: [2.9.1, latest]`: нижняя точка совпадает с продом
  и ставится с constraints-файлом этой версии
  (`constraints-2.9.1/constraints-${python}.txt`), иначе pip подтянет
  зависимости, с которыми прод не живёт; верхняя — последний 2.x со своими
  constraints;
- шаг `ruff check --output-format github` с зафиксированным baseline;
- job интеграционных тестов из [[no-broker-integration-tests]].
