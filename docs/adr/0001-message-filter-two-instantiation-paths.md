# Два пути создания MessageFilter — намеренно, не дублирование

`MessageFilter` создаётся двумя разными способами в кодовой базе: через конструктор
с `filter_callable` (в sync-контекстах: sensor poke, consume operator) и через
`MessageFilter.deserialize()` (в async/cross-process контекстах: trigger, watcher consumer).
Это не дублирование — это отражение фундаментального ограничения: callable-фильтры
не сериализуемы и не могут пересекать границу процесса (triggerer, background thread).
Унификация невозможна без смешения двух семантически разных контекстов.

## Considered Options

Рассматривалась унификация через общий factory или расширение `deserialize()` для
поддержки callable. Отклонено: callable нельзя передать через JSON/pickle безопасно,
а значит deserialize-путь принципиально не может поддерживать callable. Два пути
останутся двумя путями независимо от рефакторинга.
