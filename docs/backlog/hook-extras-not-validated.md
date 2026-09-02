---
worth: yes
where: airflow_provider_rmq/hooks/rmq.py:145
added: 2026-09-02
---
# Синхронный хук читает `extra` без проверки, а асинхронный путь — с проверкой

**Контекст.** `RMQHook._get_connection_params`
(`airflow_provider_rmq/hooks/rmq.py`) берёт `heartbeat` и
`blocked_connection_timeout` из `extra` как есть:
`extras.get("heartbeat", 600)`. Асинхронный путь в `utils/amqp.py` читает те
же ключи через `_read_number` и `_read_heartbeat`: нечисло и отрицательное
значение — WARNING и умолчание, `0` — WARNING про отключённое обнаружение
обрыва, значение меньше секунды — умолчание. ADR-0003 оставил модули
раздельными, но не обязывал их читать одно и то же поле по-разному.

**Что ломается.** Поле Extra в форме подключения — JSON, и значение легко
оказывается строкой: `{"heartbeat": "30"}`. pika `ConnectionParameters`
проверяет тип и бросает `TypeError` из `_get_connection_params`, то есть
изнутри `_connect` под tenacity — но `TypeError` не входит в список
повторяемых, так что задача падает сразу с трейсбеком pika, а не с сообщением
про неверный `extra`. Тот же `extra` для watcher'а работает: `_read_number`
приводит строку к числу. Одно подключение, два поведения.

Из того же плана `docs/plans/20260703-reliability-hardening.md` (задачи 1–2,
см. [[reliability-hardening-plan-stale]]), про тот же файл:

- `consume_messages` декодирует тело строгим `utf-8`; бинарное сообщение
  роняет оператор `UnicodeDecodeError` после того, как сообщение уже забрано
  из очереди (без auto_ack оно вернётся при закрытии канала, но задача
  красная). Триггер и watcher уже используют `errors="replace"`;
- `queue_info` для отсутствующей очереди отвечает без ключа
  `consumer_count` — форма ответа зависит от ветки;
- `__del__` не защищён от исключений при завершении интерпретатора, когда
  `pika` может быть уже частично выгружен.

**Почему это важно.** Всё это мелко по отдельности и заметно вместе: хук — то,
с чем пользователь провайдера сталкивается первым, и его отказы должны
называть причину. Правка механическая: `_read_heartbeat(extras)` и
`_read_number(extras, "blocked_connection_timeout", 300, float, 0)` уже
существуют и импортируются из `utils/amqp.py`, куда хук и так ходит за
портами.

Тесты: строковый `heartbeat` из `extra` даёт число и WARNING; `heartbeat: 0`
даёт WARNING; бинарное тело в `consume_messages`; форма `queue_info` для
несуществующей очереди.
