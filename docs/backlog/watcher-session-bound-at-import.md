---
worth: later
where: airflow_provider_rmq/watcher/models.py:96
added: 2026-11-17
---
# Фабрика `WatcherSession` привязывается к движку Airflow при импорте модуля

`WatcherSession = _make_session_factory()` (`watcher/models.py:96`) создаётся при импорте и
берёт `airflow.settings.engine` в этот момент. Модуль импортирует `plugin.py`, то есть это
происходит в каждом процессе Airflow, загружающем плагины. Импорт до `configure_orm` даёт
`sessionmaker(bind=None)`, а пересозданный движок (например, `settings.reconfigure_orm`)
остаётся у фабрики старым.

На проде не проявлялось: в watcher-процессе Airflow готовится до импорта
(`runner._prepare_airflow`). Ленивая фабрика (создание при первом вызове) убирает
зависимость от порядка импорта. Это задача 5 плана
`docs/plans/completed/20260703-reliability-hardening.md`.

Значение не решено: открыт вопрос, есть ли путь, где модуль импортируется до
`configure_orm` или движок пересоздаётся, — без такого пути правка остаётся гигиеной.
