---
worth: yes
where: airflow_provider_rmq/hooks/rmq.py:145
added: 2026-09-02
---
# Синхронный хук читает `extra` без проверки, а асинхронный путь — с проверкой

**Контекст.** `RMQHook._get_connection_params`
(`airflow_provider_rmq/hooks/rmq.py:145-146`) берёт `heartbeat` и
`blocked_connection_timeout` из `extra` как есть:
`extras.get("heartbeat", 600)`. Асинхронный путь в `utils/amqp.py` читает те
же ключи через `_read_number` и `_read_heartbeat`: нечисло и отрицательное
значение — WARNING и умолчание, `0` — WARNING про отключённое обнаружение
обрыва, значение меньше секунды — умолчание. ADR-0003 оставил модули
раздельными, но не обязывал их читать одно и то же поле по-разному.

Расходятся и умолчания: без ключа в `extra` синхронный хук ставит heartbeat
600 с, а асинхронный путь — `DEFAULT_HEARTBEAT = 30`. Одно подключение без
`heartbeat` в `extra` у оператора замечает обрыв линка примерно через полчаса,
у watcher'а и триггера — примерно через полторы минуты.

**Что ломается.** Поле Extra в форме подключения — JSON, и значение легко
оказывается строкой: `{"heartbeat": "30"}`. pika `ConnectionParameters`
проверяет тип и бросает `TypeError` (`heartbeat must be an int or a callable
function`; для `blocked_connection_timeout` — `must be a Real number`) из
`_get_connection_params`, то есть изнутри `_connect` под tenacity. `TypeError`
не входит в список повторяемых (`hooks/rmq.py:152-156`), так что задача падает
сразу с трейсбеком pika, а не с сообщением про неверный `extra`. Тот же `extra`
для watcher'а работает: `_read_number` приводит строку к числу. Одно
подключение, два поведения.

Из задачи 2 плана `docs/plans/20260703-reliability-hardening.md`
(см. [[reliability-hardening-plan-stale]]), про тот же файл:

- `queue_info` для отсутствующей очереди отвечает без ключа
  `consumer_count` (`hooks/rmq.py:377`) — форма ответа зависит от ветки;
- `__del__` (`hooks/rmq.py:192-195`) не защищён от исключений при завершении
  интерпретатора, когда `pika` может быть уже частично выгружен.

Строгое декодирование тела в `consume_messages` — другой вопрос: его замена
меняет решения ACK/NACK при умолчаниях, и разбирается она в
[[sync-decode-raises-on-binary-body]].

**Почему это важно.** Всё это мелко по отдельности и заметно вместе: хук — то,
с чем пользователь провайдера сталкивается первым, и его отказы должны
называть причину. Правка механическая: `_read_heartbeat(extras)` и
`_read_number(extras, "blocked_connection_timeout", 300, float, 0)` уже
существуют в `utils/amqp.py`, откуда хук и так импортирует порты. Переход на
`_read_heartbeat` меняет умолчание heartbeat у синхронного хука с 600 на 30 —
это строка в CHANGELOG.

Тесты: строковый `heartbeat` из `extra` даёт число и WARNING; `heartbeat: 0`
даёт WARNING; умолчание heartbeat одинаково на обоих путях; форма
`queue_info` для несуществующей очереди; `__del__` не бросает при исключении
из `is_open`/`close()`.
