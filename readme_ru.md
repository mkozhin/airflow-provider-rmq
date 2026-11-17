<h1 align="center">
  Apache Airflow Provider for RabbitMQ
</h1>
<h3 align="center">
  Реактивный запуск DAG из очередей RabbitMQ — плюс хуки, операторы, сенсоры и отложенные триггеры.
</h3>

<p align="center">
  <a href="#установка">Установка</a> &bull;
  <a href="#настройка-подключения">Подключение</a> &bull;
  <a href="#компоненты">Компоненты</a> &bull;
  <a href="#примеры-dag">Примеры</a> &bull;
  <a href="#участие-в-разработке">Участие</a>
</p>

---

*Powered by [Claude Code](https://claude.ai/code)*

---

## Обзор

`airflow-provider-rmq` — провайдер для Apache Airflow × RabbitMQ. Возможности:

- **Реактивный запуск DAG** — [плагин RMQ Watcher](#плагин-rmq-watcher) автоматически запускает DAG при поступлении сообщения в очередь, без поллинга и без занятия слотов воркеров
- Публикация сообщений в обменники (exchanges) и очереди с подтверждениями издателя (publisher confirms)
- Потребление сообщений с фильтрацией по заголовкам и пользовательским функциям
- Ожидание конкретных сообщений с помощью сенсоров (режимы `poke` и `reschedule`, а также отложенный/deferrable режим)
- Отложенный сенсор в режимах **pull** (периодический опрос) и **push** (доставка брокером через `basic_consume`) — выбор зависит от требований к задержке
- Полное управление очередями и обменниками (создание, удаление, очистка, привязка, отвязка)
- SSL/TLS подключения
- Помощник для настройки Dead Letter Queue (DLQ)
- Настройка QoS (prefetch)

### Требования

| Зависимость | Версия |
|---|---|
| Apache Airflow | `>=2.9.0, <3.0.0` |
| pika | `>=1.3.0, <2.0.0` |
| aio-pika | `>=9.0.0, <10.0.0` |
| aiormq | `>=6.8.0, <7.0.0` |
| tenacity | `>=8.0.0` |
| httpx | `>=0.27` |
| Python | `>=3.10` |
| RabbitMQ | `>=4.3` |

На RabbitMQ 4.3+ возврат в очередь через `basic.nack` не идёт в счёт `delivery-limit` quorum-очереди; на более ранних версиях повторные возвраты могут увести сообщение в dead-letter или удалить его, поэтому они не поддерживаются.

---

## Установка

### Установка из PyPI

```bash
pip install airflow-provider-rmq
```

### Сборка из исходников

```bash
git clone https://github.com/mkozhin/airflow-provider-rmq.git
cd airflow-provider-rmq
pip install build
python -m build
pip install dist/airflow_provider_rmq-*.whl
```

---

## Настройка подключения

Создайте новое подключение в интерфейсе Airflow (**Admin > Connections**):

| Поле | Значение | Описание |
|---|---|---|
| Connection Id | `rmq_default` | Любой уникальный ID |
| Connection Type | `AMQP` | Зарегистрирован провайдером |
| Host | `localhost` | Хост сервера RabbitMQ |
| Port | `5672` | `5671` для SSL |
| Login | `guest` | Имя пользователя RabbitMQ |
| Password | `guest` | Пароль RabbitMQ |
| Schema | `/` | Виртуальный хост (vhost) |

### Настройка SSL/TLS

Добавьте SSL-настройки в поле **Extra** в формате JSON:

```json
{
  "ssl_enabled": true,
  "ca_certs": "/path/to/ca.pem",
  "certfile": "/path/to/client-cert.pem",
  "keyfile": "/path/to/client-key.pem",
  "cert_reqs": "CERT_REQUIRED"
}
```

Хук также предоставляет пользовательские виджеты для SSL-полей (`ssl_enabled`, `ca_certs`, `certfile`, `keyfile`), видимые в форме подключения Airflow.

Установите `"cert_reqs": "CERT_NONE"` для отключения проверки сертификатов (не рекомендуется для продакшена).

### Тайминги подключения

Четыре необязательных ключа поля **Extra** управляют таймингами AMQP. Все они задаются обычными JSON-значениями рядом с SSL-настройками:

```json
{
  "heartbeat": 30,
  "connect_timeout": 15,
  "rpc_timeout": 30,
  "blocked_connection_timeout": 300
}
```

| Ключ | По умолчанию | Кто использует | Значение |
|---|---|---|---|
| `heartbeat` | `30` на асинхронном пути, `600` в `RMQHook` | любое AMQP-соединение | Секунды между heartbeat-кадрами AMQP. На асинхронном пути `aiormq` допускает три пропущенных интервала и проверяет каждые полинтервала, поэтому разрыв превращается в исключение примерно за `3 × heartbeat` секунд — около 90 с при значении по умолчанию. После этого robust-соединение watcher'а переподключается, а `RMQTrigger` закрывает попытку и начинает новую |
| `connect_timeout` | `15` | RMQ Watcher, `RMQTrigger` | Секунды на установку асинхронного соединения: TCP-подключение и AMQP-handshake |
| `rpc_timeout` | `30` | RMQ Watcher, `RMQTrigger` | Секунды на один асинхронный AMQP-вызов — `channel()`, declare, bind, publish; в `RMQTrigger` ещё `basic_get`, `basic_consume`, ACK/NACK и `basic_cancel` |
| `blocked_connection_timeout` | `300` | `RMQHook` | Секунды, которые соединение, заблокированное брокером (resource alarm), может оставаться заблокированным, прежде чем хук разорвёт его с исключением. `0` разрывает его сразу, как только брокер его заблокирует, и пишется WARNING'ом; `null` ждёт без срока |

`heartbeat` — один и тот же ключ на обоих путях. Пути различаются в двух вещах:

- **Дефолт.** `RMQHook` берёт `600`: `BlockingConnection` из pika обслуживает heartbeat только внутри вызовов pika, и при коротком интервале задача, которая между вызовами работает дольше примерно минуты, получает обрыв соединения. Асинхронный путь берёт `30`, потому что watcher держит соединение сутками и должен быстро замечать умершую связь.
- **`null`.** `RMQHook` передаёт `null` в `heartbeat` или `blocked_connection_timeout` в pika как есть: heartbeat идёт по предложению брокера, а заблокированное соединение ждёт без срока. Асинхронный путь считает `null` в `heartbeat` негодным значением и берёт дефолт с WARNING'ом.

Значение `"heartbeat": 0` принимается как осознанный отказ от механизма и пишется WARNING'ом: без heartbeat'ов тихо умершая связь (рестарт брокера за прокси, который держит клиентскую половину открытой) остаётся незамеченной.

У `connect_timeout` и `rpc_timeout` нет аналога в `RMQHook` — там pika работает по собственной логике повторов. Каждый ключ принимает число или строку с числом (`"30"`). Значение, не являющееся конечным числом или меньшее минимума ключа, заменяется дефолтом с WARNING'ом. Минимум — `0` для `heartbeat` и `blocked_connection_timeout` и любое значение больше нуля для `connect_timeout` и `rpc_timeout`; `heartbeat` от нуля до одной секунды или больше `65535` (наибольшего значения, которое переносит AMQP) тоже заменяется дефолтом.

---

## Компоненты

### RMQHook

**Импорт:** `from airflow_provider_rmq.hooks.rmq import RMQHook`

Основной хук для всех взаимодействий с RabbitMQ. Использует pika `BlockingConnection` с автоматической логикой повторных попыток (tenacity). Соединение закрывается автоматически, когда объект хука уничтожается сборщиком мусора, поэтому вызывать `close()` вручную не нужно. Контекстный менеджер (`with`) также поддерживается.

#### Параметры конструктора

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `vhost` | `str \| None` | `None` | Нет | Переопределить виртуальный хост из подключения |
| `qos` | `dict \| None` | `None` | Нет | Настройки QoS: `{"prefetch_count": 10}` — число неподтверждённых сообщений, которые брокер доставляет консьюмеру; это единственный ключ, который уходит брокеру. Истинный `global_qos` не передаётся, хук пишет WARNING: prefetch на канал — устаревшая функция RabbitMQ `global_qos`, запрещённая по умолчанию с RabbitMQ 4.3. `prefetch_size`, отличный от `0`, даёт `ValueError`: RabbitMQ не реализует ограничение по размеру ([соответствие AMQP 0-9-1](https://www.rabbitmq.com/docs/specification)) |
| `retry_count` | `int` | `3` | Нет | Количество попыток переподключения |
| `retry_delay` | `float` | `1.0` | Нет | Базовая задержка (секунды) между попытками (экспоненциальный рост) |
| `publisher_confirms` | `bool` | `False` | Нет | Переводить каждый канал, который открывает хук, включая переоткрытый, в режим подтверждений издателя: `basic_publish` ждёт, пока брокер подтвердит каждое сообщение |

#### Основные методы

| Метод | Описание |
|---|---|
| `get_channel()` | Возвращает pika `BlockingChannel` (создаёт подключение лениво; открывает новый канал, если текущий закрыт) |
| `queue_declare(queue_name, passive, durable, exclusive, auto_delete, arguments)` | Объявить очередь. `durable` по умолчанию `True`. Активное объявление (`passive=False`) с `durable=False` требует `exclusive=True`: очередь одновременно non-durable и non-exclusive опирается на устаревшую функцию RabbitMQ `transient_nonexcl_queues`, запрещённую по умолчанию с RabbitMQ 4.3, и даёт `ValueError` до обращения к брокеру. `auto_delete` на правило не влияет. Объявление существующей очереди с другим `durable` падает с 406 `PRECONDITION_FAILED`; передавайте флаг, с которым очередь создана |
| `queue_delete(queue_name, if_unused, if_empty)` | Удалить очередь |
| `queue_bind(queue, exchange, routing_key, arguments)` | Привязать очередь к обменнику |
| `queue_unbind(queue, exchange, routing_key, arguments)` | Отвязать очередь от обменника |
| `queue_purge(queue_name)` | Удалить все сообщения из очереди |
| `queue_info(queue_name)` | Получить информацию об очереди через пассивное объявление: словарь с ключами `queue`, `message_count`, `consumer_count` и `exists`. Для отсутствующей очереди — те же ключи, оба счётчика `0` и `exists=False` |
| `exchange_declare(exchange, exchange_type, passive, durable, auto_delete, internal, arguments)` | Объявить обменник. `durable` по умолчанию `True`. Объявление существующего обменника с другим `durable` падает с 406 `PRECONDITION_FAILED`; передавайте флаг, с которым обменник создан |
| `exchange_delete(exchange, if_unused)` | Удалить обменник |
| `exchange_bind(destination, source, routing_key, arguments)` | Привязать обменник к обменнику |
| `exchange_unbind(destination, source, routing_key, arguments)` | Отвязать обменник от обменника |
| `basic_publish(exchange, routing_key, body, properties, mandatory)` | Опубликовать сообщение. `mandatory=True` (по умолчанию `False`) просит брокер вернуть сообщение, которое не попало ни в одну очередь. При `publisher_confirms=True` вызов ждёт подтверждения брокера и бросает `pika.exceptions.UnroutableError` (сообщение возвращено), `NackError` (сообщение отвергнуто) или `ChannelClosedByBroker` (например, 404 на несуществующий обменник, 403 без прав; следующий `get_channel()` открывает новый канал). Без подтверждений вызов возвращается, как только сообщение записано в сокет, о возврате сообщения не сообщается, а закрытие канала брокером всплывает на одном из следующих вызовов |
| `consume_messages(queue_name, max_messages, auto_ack, inactivity_timeout)` | Потребить сообщения из очереди |
| `ack(delivery_tag)` | Подтвердить сообщение |
| `nack(delivery_tag, requeue)` | Отклонить сообщение |
| `build_dlq_arguments(dlx_exchange, dlx_routing_key, message_ttl)` | Статический метод: собрать `x-*` аргументы для поддержки DLQ |
| `test_connection()` | Проверить подключение (используется UI Airflow) |
| `close()` | Закрыть канал и подключение |

#### Пример использования

```python
from airflow_provider_rmq.hooks.rmq import RMQHook

hook = RMQHook(rmq_conn_id="rmq_default")
info = hook.queue_info("my_queue")
print(f"Сообщений в очереди: {info['message_count']}")

hook.basic_publish(
    exchange="",
    routing_key="my_queue",
    body='{"key": "value"}',
)
# Соединение закроется автоматически при выходе hook из области видимости
```

---

### RMQPublishOperator

**Импорт:** `from airflow_provider_rmq.operators.rmq_publish import RMQPublishOperator`

Публикует одно или несколько сообщений в RabbitMQ и по умолчанию ждёт, пока брокер подтвердит каждое (см. [Подтверждения публикации](#подтверждения-публикации)). Поддерживает строки, словари (автоматическая сериализация в JSON) и списки.

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `exchange` | `str` | `""` | Нет | Обменник для публикации (пустая строка = обменник по умолчанию) |
| `routing_key` | `str` | `""` | Нет | Ключ маршрутизации сообщения |
| `message` | `str \| list[str] \| dict \| list[dict] \| None` | `None` | Нет | Тело сообщения. Словари сериализуются в JSON |
| `queue_name` | `str \| None` | `None` | Нет | Ярлык: устанавливает `exchange=""` и `routing_key=queue_name` |
| `content_type` | `str \| None` | `None` | Нет | AMQP заголовок content type (например, `"application/json"`) |
| `delivery_mode` | `int \| None` | `None` | Нет | `1` = непостоянное, `2` = постоянное (persistent) |
| `headers` | `dict \| None` | `None` | Нет | Пользовательские AMQP заголовки |
| `priority` | `int \| None` | `None` | Нет | Приоритет сообщения (0-9) |
| `expiration` | `str \| None` | `None` | Нет | TTL сообщения в миллисекундах (строка, например `"60000"`) |
| `correlation_id` | `str \| None` | `None` | Нет | Идентификатор корреляции |
| `reply_to` | `str \| None` | `None` | Нет | Имя очереди для ответа |
| `message_id` | `str \| None` | `None` | Нет | Идентификатор сообщения |
| `confirm` | `bool` | `True` | Нет | Подтверждения издателя: ждать, пока брокер подтвердит каждое сообщение, и ронять задачу, если брокер вернул, отверг или не принял сообщение |
| `mandatory` | `bool \| None` | `None` | Нет | Просить брокер вернуть сообщение, которое не попало ни в одну очередь. `None` разрешается после рендера шаблонов: `True` для обменника по умолчанию (`exchange=""` или `queue_name=`) при `confirm=True`, иначе `False`. `True` при `confirm=False` даёт `ValueError` при создании оператора (для обычной задачи — при импорте DAG) |

**Шаблонные поля:** `exchange`, `routing_key`, `message`

`message=None` или пустой список пишет WARNING `Message is empty, nothing to publish to exchange='…' routing_key='…'`; задача завершается успешно, не подключаясь к брокеру.

#### Подтверждения публикации

При `confirm=True` каждое сообщение ждёт подтверждения брокера, прежде чем уходит следующее. Если брокер не принял сообщение, задача падает с `AirflowException`, где указаны номер сообщения в пакете, обменник и ключ маршрутизации; исключение pika сохраняется в `__cause__`.

| Ответ брокера | Текст отказа |
|---|---|
| Возврат: ни одна очередь не подходит под ключ маршрутизации, а `mandatory` включён (например, `queue_name=` указывает на несуществующую очередь) | `message 3 of 5 to exchange='' routing_key='orders' was returned by the broker: 312 NO_ROUTE` |
| Отказ (nack), например заполненная очередь с `x-max-length` и `x-overflow=reject-publish` | `message 3 of 5 to exchange='' routing_key='orders' was rejected by the broker (nack)` |
| Закрытие канала: 404 на несуществующий обменник, 403 без прав | `message 1 of 1 to exchange='events' routing_key='orders.new' was not published, broker closed the channel: 404 NOT_FOUND - no exchange 'events' in vhost '/'` |

- **Дефолт `mandatory`.** В обменнике по умолчанию сообщение в несуществующую очередь всегда теряется, поэтому там `mandatory` включён. Для именованного обменника отсутствие очереди под ключ бывает нормой, поэтому `mandatory` выключен; задайте `mandatory=True`, чтобы и там падать на несмаршрутизированном сообщении.
- **Что значит подтверждение.** Брокер принял сообщение. Несмаршрутизированное сообщение при `mandatory=False` тоже подтверждается — и выбрасывается. Persistent-сообщение (`delivery_mode=2`) к моменту подтверждения записано на диск только в durable-очередях; transient-очередь его на диск не пишет.
- **Возвращённые сообщения в логе.** WARNING pika о возвращённом сообщении, в котором есть заголовки, `correlation_id`, `reply_to`, `message_id` и начало тела, в лог не попадает: в заголовках и теле могут быть учётные данные или персональные данные. Падение задачи называет сообщение, обменник, ключ маршрутизации и код ответа.
- **Ретрай публикует заново.** Сообщения до упавшего уже лежат в брокере, и ретрай задачи публикует весь пакет снова: публикация at-least-once, поэтому потребителям стоит переносить дубли, например по бизнес-ключу в теле каждого сообщения. `message_id` сообщения не различает: он задаётся один раз в операторе и не является шаблонным полем, поэтому у всех сообщений задачи во всех запусках он одинаковый. Сам вызов публикации не повторяется, повторяется только подключение.
- **Цена и ожидание.** Каждое сообщение стоит одного round-trip до брокера — порядка миллисекунды в локальной сети, дольше для persistent-сообщений и quorum-очередей, — поэтому большой пакет публикуется заметно медленнее. У ожидания подтверждения нет своего срока: quorum-очередь, потерявшая большинство реплик, задерживает подтверждение на живом и не заблокированном соединении, и задача ждёт до своего `execution_timeout`. Задавайте задачам публикации `execution_timeout`.

При `confirm=False` задача не ждёт брокер: возвращённое или отвергнутое сообщение остаётся незамеченным, публикация в несуществующий обменник может оставить задачу зелёной, а закрытие канала брокером всплывает на одном из следующих сообщений как само исключение pika.

#### Пример использования

```python
# Публикация словаря в очередь
RMQPublishOperator(
    task_id="publish",
    queue_name="my_queue",
    message={"event": "order_created", "id": 42},
    delivery_mode=2,
    headers={"x-source": "airflow"},
)

# Публикация пакета сообщений в обменник
RMQPublishOperator(
    task_id="publish_batch",
    exchange="events",
    routing_key="orders.new",
    message=[
        {"id": 1, "item": "widget"},
        {"id": 2, "item": "gadget"},
    ],
)
```

---

### RMQConsumeOperator

**Импорт:** `from airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator`

Потребляет сообщения из очереди RabbitMQ. Подходящие сообщения подтверждаются (ACK) и возвращаются через XCom. Сообщение, которое фильтр прочитал и отверг, по умолчанию отклоняется (NACK) с `requeue=True`, а при `ack_unmatched=True` подтверждается и исчезает; сообщение, которое фильтр не смог вычислить, возвращается в очередь независимо от параметра. См. [Промахи фильтра и `ack_unmatched`](#промахи-фильтра-и-ack_unmatched).

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `queue_name` | `str` | — | **Да** | Имя очереди для потребления |
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `max_messages` | `int` | `100` | Нет | Максимальное количество сообщений за выполнение |
| `filter_headers` | `dict[str, Any] \| None` | `None` | Нет | Словарь AMQP заголовков для фильтрации. Поддерживает ключи `body.*` для фильтрации по JSON-телу (например, `{"body.data.status": "active"}`) |
| `filter_callable` | `Callable[[Any, str], bool] \| None` | `None` | Нет | Пользовательская функция фильтрации `(properties, body_str) -> bool` |
| `qos` | `dict \| None` | `None` | Нет | Настройки QoS: `{"prefetch_count": 10}` — единственный ключ, который уходит брокеру. `prefetch_size`, отличный от `0`, даёт `ValueError` при создании оператора; истинный `global_qos` брокеру не передаётся, а хук пишет WARNING в задаче. См. [`RMQHook`](#rmqhook) |
| `ack_unmatched` | `bool` | `False` | Нет | Подтверждать сообщение, отвергнутое фильтром, вместо возврата в очередь. Сообщение уходит из брокера навсегда — на подтверждение DLX не срабатывает. Обязан быть `bool` |

**Шаблонные поля:** `queue_name`

**Возвращает:** `list[dict]` — список подошедших сообщений, каждое с ключами: `body`, `headers`, `routing_key`, `exchange`

> **`ack_unmatched=True` удаляет сообщения чужих потребителей.** На очереди, которую слушает подписка watcher'а или любой другой потребитель, каждое сообщение, отвергнутое фильтром этой задачи, подтверждается и исчезает: сосед продолжает показывать `listening`, ошибок не сообщает, а его DAG просто никогда не запускается. Страница Subscriptions помечает бейджем очередь, разделяемую двумя подписками, но про операторы и сенсоры она не знает ничего — с их стороны единственная защита это абзац. Включайте параметр только для очереди, которая принадлежит этой задаче.

**Один батч за прогон таски.** `execute()` делает ровно один `consume_messages`, поэтому задача видит не больше `max_messages` сообщений из головы очереди и ничего за ними — в том числе при `ack_unmatched=True`. Параметр меняет другое: отвергнутые уходят из очереди насовсем, следующий прогон читает следующий батч, и сообщение, лежащее за полутора сотнями чужих, находится за два-три прогона, а не никогда. Счётчики промахов живут один прогон и, значит, один батч, поэтому порог в сотню промахов подряд недостижим при меньшем `max_messages`; вместо него у оператора один WARNING по батчу — о том, что не совпало ничего, о числе удалённых за батч, или об обоих сразу в одной строке. В этот счёт идёт только то, по чему фильтр вынес вердикт: нечитаемое тело о `filter_data` не говорит ничего.

#### Пример использования

```python
# Потребление с фильтром по заголовкам
RMQConsumeOperator(
    task_id="consume_orders",
    queue_name="orders",
    filter_headers={"x-type": "order"},
    max_messages=50,
    qos={"prefetch_count": 10},
)

# Потребление с фильтром по телу сообщения
RMQConsumeOperator(
    task_id="consume_active",
    queue_name="events",
    filter_headers={"body.status": "active"},
)

# Потребление с пользовательской функцией фильтрации
def large_orders(properties, body: str) -> bool:
    import json
    data = json.loads(body)
    return data.get("amount", 0) > 1000

RMQConsumeOperator(
    task_id="consume_large",
    queue_name="orders",
    filter_callable=large_orders,
)
```

#### Обработка сообщений через TaskFlow API

`RMQConsumeOperator` возвращает `list[dict]` через XCom. Используйте `consume.output` в `@task`-функции для доступа к каждому сообщению:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.operators.rmq_consume import RMQConsumeOperator

@dag(...)
def my_pipeline():
    consume = RMQConsumeOperator(
        task_id="consume",
        queue_name="orders",
        max_messages=50,
    )

    @task
    def process_messages(messages: list[dict]) -> list[dict]:
        results = []
        for msg in messages:
            body = msg["body"]          # тело сообщения (str)
            headers = msg["headers"]    # AMQP заголовки (dict)
            rk = msg["routing_key"]     # ключ маршрутизации
            exchange = msg["exchange"]  # обменник-источник
            log.info("Сообщение: body=%s, headers=%s", body, headers)

            data = json.loads(body)
            results.append(data)
        return results

    processed = process_messages(consume.output)
    processed >> next_task  # передать результаты дальше
```

---

### RMQQueueManagementOperator

**Импорт:** `from airflow_provider_rmq.operators.rmq_management import RMQQueueManagementOperator`

Выполняет операции управления очередями и обменниками в RabbitMQ.

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `action` | `str` | — | **Да** | Действие (см. таблицу ниже) |
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `queue_name` | `str \| None` | `None` | Условно | Имя очереди (обязательно для операций с очередями) |
| `durable` | `bool` | `True` | Нет | Объявленная очередь или обменник переживает перезапуск брокера. Действует на `declare_queue` и `declare_exchange`. Объявление существующей очереди или обменника с другим `durable` падает с 406 `PRECONDITION_FAILED`; передавайте флаг, с которым ресурс создан |
| `exclusive` | `bool` | `False` | Нет | Очередь эксклюзивна для этого подключения. Оператор закрывает подключение по завершении задачи, поэтому эксклюзивная очередь живёт, только пока задача выполняется |
| `auto_delete` | `bool` | `False` | Нет | Ресурс удаляется, когда больше не используется |
| `exchange_name` | `str \| None` | `None` | Условно | Имя обменника (обязательно для операций с обменниками) |
| `exchange_type` | `str` | `"direct"` | Нет | Тип обменника: `direct`, `fanout`, `topic`, `headers` |
| `internal` | `bool` | `False` | Нет | Обменник не может принимать сообщения напрямую |
| `if_unused` | `bool` | `False` | Нет | Удалять только если нет потребителей/привязок |
| `if_empty` | `bool` | `False` | Нет | Удалять очередь только если она пуста |
| `routing_key` | `str` | `""` | Нет | Ключ маршрутизации для bind/unbind |
| `arguments` | `dict \| None` | `None` | Нет | Дополнительные `x-*` аргументы (например, настройки DLQ) |
| `source_exchange` | `str \| None` | `None` | Условно | Обменник-источник для exchange bind/unbind |

**Шаблонные поля:** `queue_name`, `exchange_name`, `routing_key`, `arguments`

**Очереди durable.** `action="declare_queue"` с `durable=False` и `exclusive=False` даёт `ValueError` при создании оператора: такая очередь опирается на устаревшую функцию RabbitMQ `transient_nonexcl_queues`, запрещённую по умолчанию с RabbitMQ 4.3. У обычной задачи ошибка возникает при импорте DAG'а, у mapped-задачи (`.partial(...).expand(...)`) — при её выполнении. `queue_name` в тексте ошибки может быть нерендеренным шаблоном. Объявляйте очередь с `durable=True` — это умолчание.

#### Поддерживаемые действия

| Действие | Обязательные параметры | Описание |
|---|---|---|
| `declare_queue` | `queue_name` | Создать очередь |
| `delete_queue` | `queue_name` | Удалить очередь |
| `purge_queue` | `queue_name` | Удалить все сообщения из очереди |
| `bind_queue` | `queue_name`, `exchange_name` | Привязать очередь к обменнику |
| `unbind_queue` | `queue_name`, `exchange_name` | Отвязать очередь от обменника |
| `declare_exchange` | `exchange_name` | Создать обменник |
| `delete_exchange` | `exchange_name` | Удалить обменник |
| `bind_exchange` | `exchange_name`, `source_exchange` | Привязать обменник к обменнику |
| `unbind_exchange` | `exchange_name`, `source_exchange` | Отвязать обменник от обменника |

#### Пример использования

```python
# Создать durable очередь
RMQQueueManagementOperator(
    task_id="create_queue",
    action="declare_queue",
    queue_name="my_queue",
    durable=True,
)

# Создать topic обменник и привязать очередь
RMQQueueManagementOperator(
    task_id="create_exchange",
    action="declare_exchange",
    exchange_name="events",
    exchange_type="topic",
    durable=True,
)

RMQQueueManagementOperator(
    task_id="bind",
    action="bind_queue",
    queue_name="my_queue",
    exchange_name="events",
    routing_key="orders.*",
)
```

---

### RMQSensor

**Импорт:** `from airflow_provider_rmq.sensors.rmq import RMQSensor`

Ожидает сообщение в очереди RabbitMQ, соответствующее условиям фильтрации. Поддерживает режимы `poke` и `reschedule` из `BaseSensorOperator` и отложенный (deferrable) режим.

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `queue_name` | `str` | — | **Да** | Имя очереди для мониторинга |
| `rmq_conn_id` | `str` | `"rmq_default"` | Нет | ID подключения Airflow |
| `filter_headers` | `dict[str, Any] \| None` | `None` | Нет | Фильтр по заголовкам/телу (словарь) |
| `filter_callable` | `Callable \| None` | `None` | Нет | Пользовательская функция фильтрации. **Не поддерживается с `deferrable=True`** |
| `deferrable` | `bool` | `False` | Нет | Использовать отложенный режим (освобождает слот воркера) |
| `poke_batch_size` | `int` | `100` | Нет | Максимум сообщений за один цикл проверки |
| `poke_interval` | `float` | `60` | Нет | Секунды между проверками (наследуется от BaseSensorOperator) |
| `timeout` | `float` | `604800` | Нет | Максимум секунд ожидания до ошибки (наследуется от BaseSensorOperator). При `deferrable=True` ограничивает всё ожидание в triggerer'е (см. [Отложенный (deferrable) режим](#отложенный-deferrable-режим)) |
| `mode` | `str` | `"poke"` | Нет | `"poke"` или `"reschedule"` (наследуется от BaseSensorOperator, значение проверяет он). `"pull"` и `"push"` принимаются как устаревшее написание `trigger_mode` с `AirflowProviderDeprecationWarning`, в том числе как `"mode"` в `default_args` DAG'а или сенсора; явный `trigger_mode` важнее значения из `default_args` |
| `trigger_mode` | `Literal["pull", "push"] \| None` | `None` (= `"pull"`) | Нет | Как триггер читает очередь при `deferrable=True`: `"pull"` = `basic_get` в цикле, `"push"` = доставка брокером через `basic_consume`. Без `deferrable=True` не используется. Вместе с `mode="pull"`/`"push"` — `ValueError` |
| `message_wait_timeout` | `float \| None` | `None` | Нет | Максимум секунд ожидания в push-режиме. `None` = ждать до `timeout` сенсора (см. [Отложенный (deferrable) режим](#отложенный-deferrable-режим)). Только с `trigger_mode="push"`. Поддерживает Jinja-шаблоны и XCom |
| `ack_unmatched` | `bool` | `False` | Нет | Подтверждать сообщение, отвергнутое фильтром, вместо возврата в очередь. Сообщение уходит из брокера навсегда — на подтверждение DLX не срабатывает. Обязан быть `bool` |

**Шаблонные поля:** `queue_name`, `message_wait_timeout`

**Возвращает:** `dict | None` — подошедшее сообщение с ключами: `body`, `headers`, `routing_key`, `exchange`

#### Отложенный (deferrable) режим

При `deferrable=True` сенсор передаёт выполнение в процесс triggerer через `RMQTrigger`. Это освобождает слот воркера на время ожидания, что более эффективно при длительном ожидании.

**`timeout` ограничивает всё ожидание.** Сенсор передаёт свой `timeout` в deferral, поэтому ожидание в triggerer'е — вместе со всеми переподключениями триггера — заканчивается через `timeout` секунд. Scheduler тогда завершает задачу с `TaskDeferralError("Trigger/execution timeout")`: задача **FAILED** и перезапускается по ретраям DAG'а, и каждый ретрай снова ждёт полный `timeout`, а при `soft_fail=True` — **SKIPPED**. `execute_complete` не вызывается. `message_wait_timeout` больше `timeout` не срабатывает никогда: задача падает по `timeout`, а не пропускается. `timeout` со сроком дальше 9999 года оставляет отложенное ожидание без ограничения; `float("inf")` отвергает сам `BaseSensorOperator`.

**Ограничение:** `filter_callable` нельзя использовать с `deferrable=True`, так как Python-функции не могут быть сериализованы для передачи в triggerer. Используйте `filter_headers` вместо этого.

#### Pull vs Push режим

Параметр `trigger_mode` (актуален только при `deferrable=True`) определяет способ получения сообщений триггером:

| | `trigger_mode="pull"` (по умолчанию) | `trigger_mode="push"` |
|---|---|---|
| Механизм | `basic_get` в цикле, пауза `poll_interval` на пустой очереди | Подписка через `basic_consume` |
| Задержка | До `poll_interval` секунд | Мгновенно — брокер доставляет сразу |
| Idle-нагрузка | Опрос даже при пустой очереди | Нет активности до прихода сообщения |
| Когда использовать | Простота, предсказуемость | Минимальная задержка, тихие очереди |

> **Поведение при таймауте:** когда `message_wait_timeout` истекает, сенсор бросает `AirflowSkipException` — таск помечается как **SKIPPED** (не FAILED), downstream-таски пропускаются. `on_failure_callback` не вызывается. Это делает `message_wait_timeout` безопасным для плановых остановок (например, конец рабочего дня) без ложных алертов. Срок отсчитывается от начала ожидания поверх всех переподключений, а событие приходит не позже чем через `rpc_timeout` + 5 с (`CLOSE_TIMEOUT`) после него: триггер отменяет консьюмера, возвращает в очередь сообщения, которые брокер уже прислал, а триггер не прочитал, и закрывает соединение.

> **Quorum-очереди:** при умолчании `ack_unmatched=False` отвергнутое сообщение отклоняется с `requeue=True` (`basic.nack`). `delivery-limit` quorum-очереди считает только `basic.reject` и неудавшиеся доставки, поэтому сообщение, которое фильтр раз за разом отвергает, возвращается в голову очереди и вычисляется заново, сколько угодно раз. Актуально для обоих режимов. При `ack_unmatched=True` на выделенной очереди сообщение вместо этого подтверждается и не передоставляется вовсе.

> **Остаток батча всегда возвращается в очередь.** За один `poke` читается до `poke_batch_size` сообщений, и чтение прекращается на первом совпадении. Сообщения за ним фильтр не видел, поэтому они отклоняются с `requeue=True` независимо от `ack_unmatched`, и следующий `poke` увидит их снова.

> **`ack_unmatched=True` удаляет сообщения чужих потребителей.** На очереди, которую слушает подписка watcher'а или любой другой потребитель, каждое сообщение, отвергнутое фильтром этого сенсора, подтверждается и исчезает: сосед продолжает показывать `listening`, ошибок не сообщает, а его DAG просто никогда не запускается. Страница Subscriptions помечает бейджем очередь, разделяемую двумя подписками, но про операторы и сенсоры она не знает ничего — с их стороны единственная защита это абзац. Включайте параметр только для очереди, которая принадлежит этой задаче.

**Один батч за `poke`, и где виден промах.** Каждый `poke` читает один батч из головы очереди и не видит ничего за ним; при `ack_unmatched=True` отвергнутые уходят из очереди, поэтому сообщение за большим чужим бэклогом достаётся за несколько `poke`, а не никогда. В режиме `poke` счётчики промахов живут на экземпляре сенсора, который `BaseSensorOperator.execute` переиспользует между `poke`, поэтому сотня промахов подряд даёт WARNING при любом `poke_batch_size`. В режиме `reschedule` `AirflowRescheduleException` завершает таск-инстанс после каждого `poke`, и следующий `poke` идёт на новом объекте, поэтому счётчики каждый раз начинают с нуля. Там каждый `poke`, который прочитал сообщения и не нашёл среди них подходящего или подтвердил и выбросил часть из них, пишет одну строку WARNING о своём батче в лог своего запуска задачи — как `RMQConsumeOperator` о своём прогоне. Сенсор работает в режиме `reschedule`, когда DAG задаёт `mode="reschedule"`; кроме того, `BaseSensorOperator.prepare_for_execution()` сам переводит его туда под `DebugExecutor`. `trigger_mode` в обоих случаях остаётся заданным.

#### Пример использования

```python
# Классический poke-режим с пользовательским фильтром
RMQSensor(
    task_id="wait_for_order",
    queue_name="orders",
    filter_callable=lambda props, body: "urgent" in body,
    poke_interval=10,
    timeout=300,
    mode="reschedule",
)

# Отложенный pull-режим (по умолчанию)
RMQSensor(
    task_id="wait_for_event",
    queue_name="events",
    filter_headers={"x-type": "payment"},
    deferrable=True,
    timeout=600,
)

# Отложенный push-режим — брокер доставляет мгновенно, таймаут 60 сек
RMQSensor(
    task_id="wait_for_event_push",
    queue_name="events",
    filter_headers={"x-type": "payment"},
    deferrable=True,
    trigger_mode="push",
    message_wait_timeout=60,
    timeout=120,
)

# Динамический таймаут через XCom — например, сколько секунд осталось до конца рабочего дня
RMQSensor(
    task_id="wait_for_message",
    queue_name="events",
    deferrable=True,
    trigger_mode="push",
    message_wait_timeout="{{ ti.xcom_pull(task_ids='compute_timeout') }}",
)
```

#### Обработка результата сенсора через TaskFlow API

`RMQSensor` возвращает `dict | None` через XCom. Используйте `sensor.output` в `@task`-функции для доступа к найденному сообщению:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.sensors.rmq import RMQSensor

@dag(...)
def my_pipeline():
    wait = RMQSensor(
        task_id="wait_for_event",
        queue_name="events",
        filter_headers={"x-type": "payment"},
        deferrable=True,
    )

    @task
    def handle_event(message: dict):
        log.info("Получено: %s", message)
        return message

    handle_event(wait.output)
```

---

### RMQTrigger

**Импорт:** `from airflow_provider_rmq.triggers.rmq import RMQTrigger`

Асинхронный триггер для отложенного режима сенсора. Работает с брокером напрямую через `aiormq`, на своём соединении и своём канале для каждой попытки. Обычно не используется напрямую — `RMQSensor` с `deferrable=True` создаёт его автоматически.

#### Параметры

| Параметр | Тип | По умолчанию | Обязательный | Описание |
|---|---|---|---|---|
| `rmq_conn_id` | `str` | — | **Да** | ID подключения Airflow |
| `queue_name` | `str` | — | **Да** | Очередь для мониторинга |
| `filter_data` | `dict \| None` | `None` | Нет | Сериализованный фильтр из `MessageFilter.serialize()` |
| `poll_interval` | `float` | `5.0` | Нет | Секунды между опросами при пустой очереди (только pull-режим) |
| `mode` | `Literal["pull", "push"]` | `"pull"` | Нет | Режим доставки: `"pull"` = `basic_get` в цикле, `"push"` = `basic_consume`. `RMQSensor` передаёт сюда свой `trigger_mode` |
| `message_wait_timeout` | `float \| None` | `None` | Нет | Максимум секунд ожидания в push-режиме, от начала `run()` поверх всех попыток и пауз между ними. Timeout-событие приходит не позже чем через `rpc_timeout` + `CLOSE_TIMEOUT` (5 с) после срока: триггер завершает ACK или NACK доставки, которая уже у него в руках, отменяет консьюмера, возвращает непрочитанные доставки в очередь через NACK и закрывает соединение. В pull-режиме не используется |
| `ack_unmatched` | `bool` | `False` | Нет | Подтверждать сообщение, отвергнутое фильтром, вместо возврата в очередь. Сообщение уходит из брокера навсегда — на подтверждение DLX не срабатывает. Обязан быть `bool` |

#### Границы, повторы и ошибки

**Каждый вызов брокера ограничен по времени.** Каждая попытка открывает своё соединение — в Management UI оно называется `rmq-trigger:<очередь>` — и канал на нём, и закрывает соединение, чем бы попытка ни закончилась, в том числе при отмене задачи. Handshake идёт под `connect_timeout`; открытие канала, пассивный declare очереди, `basic_get`, `basic_consume`, каждый ACK/NACK и `basic_cancel` — под `rpc_timeout` (см. [Тайминги подключения](#тайминги-подключения)). В push-режиме закрытый канал сразу прерывает ожидание доставки, а каждые `rpc_timeout` секунд без доставки триггер проверяет, не отменил ли брокер консьюмера. Resource alarm подключение не задерживает: триггер только читает, а брокер блокирует публикующие соединения.

**Что повторяется.** Шаг, не уложившийся в границу, потерянное соединение или канал (heartbeat, перезапуск брокера, сброшенный сокет) и консьюмер, отменённый брокером (очередь удалили, сменился лидер quorum-очереди, перезапустился узел), завершают попытку строкой WARNING в логе задачи, например:

```
RMQTrigger: connect did not complete within 15s on conn_id='rabbitmq' (attempt 3); retrying queue 'q' in 60s
```

Пауза перед следующей попыткой — 15 с, затем 30 с, затем 60 с для каждой следующей; после попытки, дошедшей до ожидания сообщения, она снова начинается с 15 с. Задача всё это время остаётся отложенной и продолжает сама, когда брокер ожил; всё ожидание ограничивает `timeout` сенсора (см. [Отложенный (deferrable) режим](#отложенный-deferrable-режим)).

**Что даёт ошибку.** Триггер отдаёт error-событие — сенсор тогда падает с `RuntimeError` — на любой попытке при неверном или сменённом пароле (403 во время handshake), отсутствии доступа к vhost'у (530 на `Connection.Open`), 403, 404 или 405 RESOURCE_LOCKED на очереди (эксклюзивная очередь чужого соединения, которая исчезает вместе с ним), соединении, закрытом брокером с 540 NOT_IMPLEMENTED (например, `basic_get` на stream-очереди), 501 FRAME_ERROR или 503 COMMAND_INVALID, 406 PRECONDITION_FAILED в ответ на `basic_consume` (push-режим на stream-очереди, которая не принимает консьюмера без ограничения prefetch) и любом исключении, кроме перечисленных здесь. 404, в тексте которого брокер пишет, что «домашний» узел durable classic-очереди недоступен, — это перезапуск узла, а не пропавшая очередь, и он повторяется. Отказ в подключении и оборванный handshake (EOF, `Connection.Close` посреди него, остановленный vhost) — ошибка на первой попытке (неверный хост или порт, брокера нет) и повтор на всех следующих.

**Совпавшее сообщение подтверждается до success-события.** Если этот ACK не подтверждён — истекла граница или под ним пропало соединение, — событие всё равно отдаётся, со строкой WARNING `ACK not confirmed … the message may be delivered again`: кадр мог дойти до брокера, а мог и не дойти, поэтому возможен дубль, но не потеря. Если кадр заведомо не записан — соединение или канал закрылись до ACK, либо ACK не прошёл на закрытом канале до записи кадра, — попытка повторяется: брокер вернёт сообщение в очередь. В push-режиме закрытое соединение или канал завершает попытку до того, как прочитана следующая доставка из буфера: брокер уже вернул доставки буфера в очередь. Доставка, взятая до истечения `message_wait_timeout`, получает на свой ACK или NACK полный `rpc_timeout`. В push-режиме совпадение, истёкший `message_wait_timeout` и отмена триггера (истёк `timeout` сенсора, задачу очистили, triggerer останавливается) отменяют консьюмера и возвращают в очередь доставки, которые брокер уже прислал, одним `basic.nack` с `multiple=True` до закрытия соединения: quorum-очередь засчитывает в `delivery-limit` доставку, брошенную вместе с соединением, а NACK — нет. У push-консьюмера нет ограничения prefetch, поэтому брокер присылает ему все готовые сообщения очереди, и они ждут в памяти triggerer'а, пока триггер их не прочитает или не вернёт. Success-событие отдаётся до выхода из консьюмера и закрытия соединения: триггер, отменённый во время этой уборки — остановка triggerer'а, очистка задачи, истёкший таймаут отложенного ожидания, — своё событие уже передал. Исключение — отмена, пришедшая, пока ACK совпавшего сообщения в пути: `aiormq` уже поставил кадр в очередь записи, сообщение подтверждено, а события нет. При таймауте или очистке задачи событие и так некому принять; остановка triggerer'а ровно в это окно теряет сообщение.

**Брокер, который не начинает handshake.** Если брокер принял TCP-подключение и так и не прислал первый кадр handshake, `aiormq` оставляет этот сокет открытым после того, как шаг подключения брошен, — пока его не закроет другая сторона. Когда пауза между попытками дошла до 60 с, это не больше одного сокета в минуту, пока брокер в таком состоянии.

---

### MessageFilter (утилита)

**Импорт:** `from airflow_provider_rmq.utils.filters import MessageFilter`

Проверяет, соответствует ли сообщение RabbitMQ заданным условиям фильтрации. Используется внутри операторами и сенсорами.

#### Режимы фильтрации

1. **Фильтрация по заголовкам** (`filter_headers`): словарь пар ключ-значение, которым должны соответствовать заголовки сообщения.
   - Обычные ключи проверяют словарь `properties.headers`
   - Ключи, начинающиеся с `body.`, обходят JSON-тело сообщения (например, `{"body.data.status": "active"}`)

2. **Фильтрация функцией** (`filter_callable`): `fn(properties, body_str) -> bool`

Оба режима можно комбинировать (логика И: оба условия должны выполниться), и порядок вычисления именно такой: сначала условия по словарю, а функция — только там, где словарь совпал или его нет.

`matches(properties, body) -> bool` отвечает на вопрос «да или нет». Коду, которому нужно отличить отказ фильтра от сбоя вычисления, служит `evaluate(properties, body) -> tuple[FilterVerdict, str | None]`: вердикт — это `FilterVerdict.MATCH`, `MISS` или `ERROR` (`from airflow_provider_rmq.utils.filters import FilterVerdict`), а второй элемент несёт тип и текст исключения за `ERROR` и `None` в остальных случаях. Именно об этих трёх вердиктах таблица ниже.

> **Изменение поведения для внешнего кода, зовущего `MessageFilter.matches()` напрямую.** Исключение из `filter_callable` по-прежнему подавляется и по-прежнему читается как несовпадение, но его тип, текст и трейсбек уходят теперь на уровень **DEBUG**, а не WARNING. Внутри провайдера видимую строку ставят счётчики промахов цикла потребления: они называют очередь и троттлят серию — см. ниже. Внешнему коду, который полагался на WARNING, стоит логировать сбой самому внутри `filter_callable`: записи уходят на логгер `airflow.task`, и понижение его уровня до DEBUG вернёт их ценой всех остальных записей лога задачи.

---

### Промахи фильтра и `ack_unmatched`

Каждый цикл потребления в провайдере — `RMQConsumeOperator`, `RMQSensor`, `RMQTrigger` и watcher — приходит к одному из трёх вердиктов о сообщении:

| Вердикт | Что означает | Что происходит с сообщением |
|---|---|---|
| **совпало** | фильтр его принял | подтверждается, и сообщение идёт дальше — возвращается через XCom или запускает DAG |
| **промах** | фильтр прочитал сообщение и отверг | при `ack_unmatched=False` (умолчание) отклоняется с `requeue=True`; при `ack_unmatched=True` подтверждается и исчезает |
| **сбой** | фильтр не удалось вычислить: `filter_callable` бросил исключение либо на ключе `body.*` тело не разобралось как JSON | всегда отклоняется с `requeue=True`, что бы ни говорил `ack_unmatched`, — поэтому тело, которое не разбирается никогда, передоставляется до истечения TTL |

`ack_unmatched` — `bool` с умолчанием `False`, то есть в точности поведение всех прежних версий. Он есть у `@rmq_trigger(...)`, у `RMQConsumeOperator`, у `RMQSensor`, у `RMQTrigger` и чекбоксом на странице Subscriptions. Каждый из этих конструкторов отвергает не-`bool`: строка `"false"` истинна, а параметр уничтожает сообщения.

```python
@rmq_trigger(
    queue="orders.airflow-trigger",       # очередь, принадлежащая этой подписке
    filter_data={"filter_headers": {"event": "trigger"}},
    ack_unmatched=True,                   # всё остальное подтверждается и удаляется
)
@dag(schedule=None)
def orders_dag():
    ...
```

**Зачем он нужен.** При умолчании сообщение, которое не принимает ни один фильтр, из очереди не выходит вовсе: брокер возвращает его в голову, потребитель получает его снова, снова отвергает, и круг идёт примерно десять раз в секунду до истечения TTL сообщения — порядка 300 000 передоставок за восьмичасовой TTL, замерено на рабочей установке. `ack_unmatched=True` не замедляет круг, а убирает его: каждое сообщение, по которому фильтр вынес вердикт, доставляется один раз. Единственное, что сюда не попадает, — сообщение, которое фильтр не смог прочитать вовсе; про него абзац ниже.

**Подтверждённое сообщение не попадает в DLQ.** Dead-lettering срабатывает на reject, истечение TTL и переполнение, а подтверждение — ничто из этого. Очередь с `x-dead-letter-exchange` не сохраняет копию того, что удалил `ack_unmatched`: сообщение уходит из брокера целиком.

**Сбой вычисления фильтра — не промах.** И исключение из `filter_callable`, и тело, которое `json.loads` отказывается разобрать при запросе ключа `body.*`, означают, что сообщение не прочитано, — поэтому оно возвращается в очередь и в ветку подтверждения не попадает никогда. Минута недоступности внешнего сервиса стоит передоставок, а не батча сообщений. Оба сбоя достижимы у оператора и сенсора; в watcher'е и триггере функция не переживает сериализацию ([ADR 0001](docs/adr/0001-message-filter-two-instantiation-paths.md)), поэтому там остаётся только неразобранное тело.

**Где в гарантии дыра: тело, которое не разбирается никогда.** Недоступность кончается, и на следующем обороте сообщение проходит, а вот обрезанное или не-JSON тело — постоянное свойство самого сообщения: оно даёт сбой каждый раз одинаково. Возврат в очередь здесь выбран намеренно — подтверждение удалило бы сообщение, которого фильтр не читал, ровно то, чего `ack_unmatched` обещает не делать, — и цена этого в том, что и на classic-, и на quorum-очереди (на RabbitMQ 4.3+ возврат через nack не ограничен) одно такое сообщение держит круг передоставок в ~10 оборотов в секунду до истечения своего TTL, независимо от `ack_unmatched`. Видимой эту ситуацию делает серия WARNING'ов, описанная ниже; выход — TTL на очереди, dead-letter-обменник, которым этот TTL кормится, или фильтр, не запрашивающий ключ `body.*` на очереди, где тела не JSON. Ограниченный выход, который такое сообщение в конце концов подтверждает, записан в `docs/backlog/error-verdict-redelivers-until-ttl.md` и в это поведение намеренно не входит.

**Одна очередь — одна подписка.** Две включённые подписки на одном `queue_name` и `conn_id` — это два конкурирующих потребителя, и брокер раскладывает доставки между ними round-robin. Сообщение, подходящее обоим фильтрам, — а при пустом `filter_data` это всякое сообщение — подтверждает тот потребитель, которому оно досталось, и DAG второй подписки не запускается. Так было и раньше, уникальный ключ строки подписки этого по-прежнему не запрещает, поэтому здесь рекомендация, а не запрет: установка со строго непересекающимися фильтрами работает как работала. При `ack_unmatched=True` цена выше — сообщение, которое принимает только соседний фильтр, удаляется в момент, когда попадает на подтверждающего потребителя, сосед остаётся в `listening`, и нигде не появляется ни одной ошибки. Поэтому страница Subscriptions помечает бейджем любую разделяемую очередь, а там, где одна из подписок подтверждает, текст жёстче. Fan-out делается отдельными очередями и биндингами, а не двумя подписками на одной.

**Что промах пишет в лог.** Ничего на каждое сообщение: промах фильтра — штатный режим работы, и строка на каждый стоила бы строки на каждое чужое сообщение. Вместо этого счётчики одного цикла потребления дают:

- WARNING на каждой степени десяти промахов подряд начиная со ста (100, 1 000, 10 000 …) — с именем очереди, с указанием, вернулись сообщения в очередь или удалены, и с подсказкой проверить фильтр. Совпадение обрывает серию и обнуляет счётчик.
- WARNING на каждой степени десяти *удалённых* сообщений начиная со ста; этот счётчик не обнуляет ничто. Поток из девяноста девяти промахов и одного совпадения до первого порога не доходит никогда, удаляя при этом девяносто девять сообщений из каждой сотни, — и виден он именно здесь. Когда оба счётчика берут одно и то же число на одном сообщении, это число уже названо строкой выше, и вторая не выходит.
- WARNING на **первом** сбое вычисления фильтра и дальше на каждой степени десяти начиная со ста, с типом и текстом исключения. Совпадение эту серию не обнуляет — её обнуляет только сотня исходов подряд без единого сбоя. Сбойное сообщение возвращается в очередь при любом `ack_unmatched`, поэтому приходит снова через 0,1 с, и строка на каждый оборот записала бы в лог сам цикл передоставок.
- только в watcher'е — метрики `rmq_watcher.filter_missed` и `rmq_watcher.filter_errored`, по одной на вердикт. У оператора, сенсора и триггера метрической обвязки нет, и префикс `rmq_watcher` был бы в них неправдой.

Насколько далеко достаёт счётчик, задаёт срок жизни владельца: consumer-таска в watcher'е (переживает переприцепления), один `run()` в триггере, один прогон таски у оператора, экземпляр сенсора между его `poke`. Два места, где порог в сотню недостижим, названы выше в разделах `RMQConsumeOperator` и `RMQSensor` вместе с тем, что стоит там вместо него.

**Чего это не меняет.** Оператор и сенсор по-прежнему читают за забор один батч из головы очереди и заглянуть за него не могут: смотреть дальше головы значило бы удерживать чужие сообщения неподтверждёнными, а это упирается в собственный `qos.prefetch_count` задачи и вредит всем остальным потребителям очереди. Ответ для такого случая — `ack_unmatched=True` на очереди, принадлежащей подписке или задаче, а не батч побольше.

---

## Плагин RMQ Watcher

**Плагин RMQ Watcher** инвертирует обычный паттерн с сенсором: вместо того чтобы DAG ждал сообщения, сообщение в RabbitMQ само *вызывает* запуск DAG — без поллинга, без `deferred`-тасков, без потребления ресурсов воркеров.

### Как это работает

Watcher работает в дочернем процессе Scheduler'а — `python -c <bootstrap> airflow_provider_rmq.watcher.runner`, который listener плагина (Airflow Listener API) запускает при старте Scheduler'а и останавливает при его остановке, см. [Процесс watcher'а](#процесс-watcherа). В этом процессе идёт asyncio-цикл, который подписывается на очереди через AMQP `basic_consume`. При поступлении подходящего сообщения `trigger_dag()` вызывается напрямую внутри процесса. Одно соединение `RobustConnection` на `conn_id` разделяется между всеми подписками к одному кластеру, а cooldown-публикация идёт по отдельному второму соединению (см. [Устойчивость соединения](#устойчивость-соединения)).

Каждые 60 секунд (настраивается через Airflow Variable `rmq_watcher_reconcile_interval`) цикл reconciliation пересканирует DAG-файлы на наличие декораторов `@rmq_trigger` (инкрементально по mtime — перепарсиваются только изменившиеся файлы) и синхронизирует подписки в БД. Обходится папка `airflow.settings.DAGS_FOLDER` — тот же путь, из которого DAG'и разбирает Scheduler.

Читаются в ней те `*.py`-файлы, которые из этой папки загружает Airflow. Обход заходит в каталоги-симлинки, читает `.airflowignore` каждого каталога в синтаксисе `[core] dag_ignore_file_syntax` (`regexp` или `glob`) и применяет его правила к этому каталогу и ко всему ниже собственными классами правил Airflow, а скрытые каталоги и файлы вроде `.x.py` берёт наравне с остальными, как их загружает Airflow. Две части листинга Airflow не повторяются. `.zip`-архив с DAG'ами не читается: watcher разбирает только исходный текст на Python, и `@rmq_trigger` внутри архива обрывает импорт (см. ниже). Эвристика `safe_mode` Airflow не применяется: разбирается каждый `.py`-файл перечисления, поэтому тяжёлому каталогу внутри `DAGS_FOLDER` — `.git`, `.venv`, `node_modules` — место в `.airflowignore`. Файл, который скан принимает, импортирует `dag` из Airflow, поэтому эвристика по умолчанию его всегда пропускает; пользовательский `[core] might_contain_dag_callable` может отсеять файл, который watcher читает, и тогда watcher регистрирует подписки DAG'а, которого Airflow не загружает. Обоснование — в [ADR 0010](docs/adr/0010-watcher-lists-dag-folder-like-airflow.md).

Пропажа ключа подписки из скана событием не считается: строка остаётся на месте — тот же `id`, та же consumer-таска, тот же `enabled`, — а цикл считает промахи и пишет WARNING с остатком попыток. Три доверенных скана без ключа строка переживает, на четвёртом — удаляется. Включённая подписка, чей `dag_id` Airflow к этому моменту уже не считает активным, удаляется быстрее — на третьем промахе: подходящее сообщение по такой подписке подтверждается (ACK) без единого запуска, поэтому её consumer стоит дороже, чем сама строка. Выключенная подписка полную отсрочку получает всегда, независимо от состояния DAG'а: consumer'а у неё нет, сообщения копятся в очереди, а досрочное удаление стоило бы ей флага `enabled` — вернувшийся ключ создал бы строку заново включённой. На нескольких шедулерах включённая строка неактивного DAG'а может уйти на один засчитанный промах раньше — единицу счётчику способна оставить гонка двух шедулеров, а не настоящий промах, — и стоит это нового `id`, перезапуска consumer-таски и начатого заново счётчика прогрева: скан пересоздаёт строку такой же включённой, какой она была. Выключенной строки это не касается: и условие досрочного удаления, и предикат его `DELETE` требуют `enabled`.

Скану, который утверждением о коде не является, цикл не верит вовсе: шаг синхронизации пропускается целиком, и ни одна строка промаха не считает. Так читаются недоступная, пустая строкой, несуществующая и нечитаемая папка DAG'ов, а также перечисление, из которого прочиталось меньше половины того, что папка, как известно, держала, — перечисление без единого файла и перечисление, назвавшее файлы, из которых не прочитался ни один, в том числе. Чем именно файл отказал, роли не играет: оборванная выкладка оставляет файлы обрезанными (читаются, но не разбираются), потерявшее доступ хранилище — не отдающими stat, и то и другое говорит одно и то же: на диске сейчас лежит не код. Один сломанный файл среди многих — это утверждение о коде, и такое перечисление остаётся доверенным. Перечисление, которое им не является, недоверено три цикла подряд и принимается на четвёртом: и папка, опустевшая по-настоящему, и файловая система, которая уже не вернётся, должны когда-то стать правдой. Принятое сжатие опускает известный размер папки до себя: папка, законно потерявшая большую часть файлов, дальше меряется новым размером, следующий обвал получает все три цикла целиком, а установка с пустой навсегда папкой DAG'ов принимается и больше о себе не напоминает вместо WARNING'а каждый цикл. Папка, файлы которой перестали читаться совсем, заканчивает тем же: перечислению, которому цикл однажды сдался, он не отказывает снова — это уже оплаченное установившееся состояние, — и недоверие начинается заново, только когда какой-нибудь файл прочитается. В остальном известный размер — это максимум: папка, отваливающаяся по куску за цикл, сравнивается с тем, что держала, а не с собственным прошлым шагом. Отказ самой папки перечисления не даёт вовсе и бюджет оставляет ровно там, где он был: пополняй он его, папка, чередующая нечитаемость с обвалом, держала бы недоверие сколько угодно, а шаг синхронизации не выполнился бы больше никогда. Процессу, который ещё не поверил ни одному перечислению, сравнивать не с чем, и он берёт то, что дают, кроме перечисления без единого прочитанного файла: между стартом шедулера в полусмонтированный том DAG'ов и удалениями стоит только отсрочка в три скана. Причина пишется в лог ERROR'ом; остальные шаги — чтение подписок, reconcile, проверка живости, запись статусов — идут как обычно, от строк в БД, с одной оговоркой: метаданные exchange живут только в скане, поэтому недоверенный цикл берёт их из последнего доверенного скана, а на шедулере, который ни одного доверенного скана ещё не закончил, их нет вовсе — ни провижининга sub queue и биндингов, ни строки в логе: WARNING о сироте пишется только для ключей, которые процесс провижинил сам, поэтому consumer поднимается на `rmq_watcher.sub.{dag_id}` против необъявленной очереди, и до первого доверенного скана об этом не сообщает ничто. Файл, чьё содержимое не поддалось разбору или не отдалось на чтение, один цикл держит свой прошлый разбор, а дальше уходит в общий счёт промахов. Ключ, вернувшийся после удаления своей строки, отмечается WARNING'ом с именем подписки: скан создаёт новую строку со значением `enabled` по умолчанию, поэтому подписка, выключенная до удаления, снова начинает потреблять. Сверяется эта строка с удалениями, которые шедулер сделал сам и всё ещё держит в памяти, поэтому рестарт между удалением и возвратом — как и другой шедулер в HA, пересоздающий удалённое первым, — оставляет от пересоздания только INFO-строки счётчика промахов.

Каталог внутри папки, который обход не может прочитать, — нет прав, отказавший или устаревший mount, симлинк, чей целевой каталог недоступен, — прячет свои файлы, а не говорит об их отсутствии. Симлинк, чья цель недоступна вовсе, считается таким каталогом, даже когда указывает на файл: пока цель недоступна, их не различить. Каталог получает в логе Scheduler'а один WARNING, когда становится нечитаемым (`RMQ Watcher: DAG folder directory <path> cannot be read …`), строки DEBUG, пока таким остаётся, и INFO `… is readable again`, когда обход снова его прочитал. Каждый DAG-файл, который watcher знает под ним, считается файлом перечисления, который не прочитался, — ровно как нечитаемый файл: один цикл держит прошлый разбор со своим WARNING (`… holding its previous parse …`), входит в долю, половина которой должна прочитаться, а дальше его ключи уходят в счёт промахов. Поддерево, достаточно большое, чтобы опустить прочитанную долю ниже половины, делает перечисление недоверенным, и ERROR называет нечитаемые каталоги — первые пять и число остальных. Каталог, нечитаемый с самого старта watcher'а, ничего не держит — файлов под ним watcher не знает — и получает только WARNING. Ошибка ввода-вывода на отдельном `.py`-файле остаётся ошибкой этого файла. То, на чём весь листинг падает у самого Airflow, — цикл симлинков, неподдерживаемый `dag_ignore_file_syntax`, невалидный glob-паттерн, `.airflowignore`, который не декодируется, имя файла, которое не кодируют regexp-правила, — и нечитаемый `.airflowignore` корня папки перечисления не дают вовсе: как и у неперечислимой папки, цикл недоверен, а бюджет недоверенных перечислений остаётся там, где был. Новый `.airflowignore`, отсекающий больше половины папки, — такое же сокращение, как любое другое: три цикла недоверия, на четвёртом принимается, и строки отсечённых файлов уходят путём подсчёта промахов.

Скан читает исходный текст DAG-файла и никогда его не исполняет, поэтому от `@rmq_trigger(...)` он видит только то, что написано в самом файле:

- `dag_id` в `@dag(...)` (позиционный или именованный аргумент) — строковый литерал или строковая константа файла; `@dag(...)` без `dag_id` называет DAG именем функции, как это делает Airflow. `dag` должен вызываться под собственным именем — `@dag(...)` или `@<module>.dag(...)`: при `from airflow.decorators import dag as airflow_dag` скан не находит `@dag(...)`, не может вычислить `dag_id` и отвергает файл. Кроме того, это должен быть декоратор Airflow: голое имя `dag`, импортированное из `airflow.decorators` (или `airflow.models.dag`), или `<module>.dag`, где `<module>` связан импортом с `airflow.decorators` или `airflow.models.dag` (`from airflow import decorators`, `import airflow.decorators`). На функции с `@rmq_trigger` одноимённый `dag` другого модуля (`from company import dag` — обёртка, добавляющая префикс к `dag_id`, или передающая его без изменений), относительный импорт и файл с `from ... import *` отвергаются: такой декоратор может дать DAG другой `dag_id`, чем виден в файле. Импорт должен быть тем, что имя значит там, где выполняется декоратор: перепривязка `dag` на уровне модуля выше декорируемой функции, `global dag` в любой функции файла, `dag` фабрики, внутри которой объявлена вложенная DAG-функция, или перепривязка `dag` ниже в цикле уровня модуля тоже отвергают файл, а привязка, которую декоратор не видит, — параметр `dag` (`@task def report(dag=None)`), `dag = context["dag"]` в колбэке, `dag = my_dag()` или `with DAG(...) as dag:` ниже декорируемой функции — значения не имеет. Для DAG-функции, объявленной непосредственно в фабрике — обычной `def` уровня модуля вне цикла, без декораторов, не `async def` и без `yield` в теле, — привязка на уровне модуля ниже `def` фабрики значения не имеет, если файл упоминает фабрику только в вызове и каждый такой вызов выполняется один раз до привязки: `dag = create_dag()` ниже фабрики или `with DAG(...) as dag:` ниже последнего вызова фабрики. Такая привязка отвергает файл, если фабрика вызывается после неё или внутри функции, тела класса, лямбды, включения или цикла, где может выполниться позже; если файл передаёт фабрику дальше, не вызывая её (`make = create_dag`, `FACTORIES.append(create_dag)`, `register(create_dag)`); и если файл не упоминает фабрику вовсе — её может импортировать и вызвать другой DAG-файл уже после того, как этот модуль выполнился. Любая привязка на уровне модуля ниже фабрики отвергает файл и тогда, когда DAG-функция объявлена в функции или классе внутри фабрики (`def outer(): def create_dag(): ...; return create_dag`), или когда фабрика — `async def`, генератор (`yield dag_obj`) или несёт декоратор (`@register`): вызов такой фабрики может выполнить `def` DAG-функции уже после привязки. То же относится к `rmq_trigger` и к именам модулей в `<module>.rmq_trigger` и `<module>.dag`;
- каждый аргумент `@rmq_trigger(...)`, именованный или позиционный (`queue`, `queues`, `conn_id`, `filter_data`, `cooldown` — в этом порядке), — литерал, константа файла, литерал списка, кортежа или словаря, собранный из них (`queues=[Q1, Q2]`, `filter_data={"body.type": PAID}`), знак перед числом, а также `**OPTS`, где `OPTS` — словарь-константа файла, ключи которого — параметры `rmq_trigger`.

Константа файла — имя, которому литерал присвоен ровно один раз на верхнем уровне DAG-файла, выше декорируемой функции. Сама функция должна быть модульного уровня — DAG, объявленный внутри функции-фабрики, читает только литералы, — а в файле с `from ... import *` констант нет. Константа, содержащая список или словарь, не может упоминаться в файле нигде, кроме аргументов `rmq_trigger(...)`: любое другое упоминание — `alias = FILTER`, `FILTER["a"] = 1`, `helper(QUEUES)`, чтение по индексу — может изменить её там, где скан за этим не уследит, поэтому скан её не читает. Не читаются и: импортированное имя (`from common.rmq import RMQ_CONN`), вызов, атрибут, `os.environ[...]`, f-строка, конкатенация, `*args`, `**` от чего угодно, кроме словаря-константы, неизвестный именованный аргумент, параметр, переданный дважды, а также значения, которых не может хранить подписка, записанная как JSON, — множество, литерал bytes или комплексного числа. `rmq_trigger` скан узнаёт только как вызов-декоратор функции — `@rmq_trigger(...)` или `@<module>.rmq_trigger(...)`, — стоящий над `@dag(...)` и импортированный из провайдера под собственным именем: `from airflow_provider_rmq.watcher.decorators import rmq_trigger` (или `from airflow_provider_rmq.watcher import rmq_trigger`), а для формы с атрибутом — `<module>`, связанный импортом с `airflow_provider_rmq.watcher.decorators` или `airflow_provider_rmq.watcher` (`from airflow_provider_rmq.watcher import decorators`, `import airflow_provider_rmq.watcher.decorators as d`). Одноимённый `rmq_trigger` другого модуля (`from common_rmq import rmq_trigger`, за которым стоит `partial(rmq_trigger, conn_id=...)` или функция-обёртка), относительный импорт (`from .common import rmq_trigger`) и файл с `from ... import *` отвергаются: такой декоратор может получить другие аргументы, чем показывает файл. `import ... as rt`, `rt = decorators.rmq_trigger` и `partial(rmq_trigger, ...)` в DAG-файле доходят до декоратора путями, за которыми скан не следит, а `rmq_trigger(...)(dag)` на готовом `DAG` сразу бросает `TypeError`. Ниже первого `@rmq_trigger(...)` функции могут стоять только `@rmq_trigger(...)` и `@dag(...)`: любой другой декоратор там может изменить DAG незаметно для скана — декоратор ниже `@dag(...)` без `functools.wraps` переименовывает функцию, по имени которой назван DAG без `dag_id`, декоратор между `@rmq_trigger` и `@dag` может переименовать DAG или вернуть что-то другое, — и отвергает файл. Декораторы выше первого `@rmq_trigger(...)` оборачивают то, что он возвращает, и значения не имеют.

Файл, который скан не может прочитать целиком, отвергается целиком. Если у любого `@rmq_trigger` в нём есть аргумент, который скан не читает, `dag_id`, который он не может вычислить, аргументы, которые отвергает `build_subscriptions`, или вторая `exchange=`-подписка на том же DAG, если `@rmq_trigger` стоит ниже `@dag(...)` или ниже `@rmq_trigger` стоит другой декоратор, если его `rmq_trigger` не из провайдера или его `dag` не из Airflow, или файл упоминает `rmq_trigger` иначе, чем этим вызовом-декоратором, — не регистрируется ни одна подписка файла, а в логе Scheduler'а появляется ERROR с путём к файлу и каждой проблемой: DAG, строка, параметр и причина. ERROR пишется при старте watcher'а и при каждом изменении файла, а не на каждом цикле. Строки, которые файл зарегистрировал раньше, уходят путём подсчёта промахов, описанным выше, как строки удалённого декоратора. Импорт файла падает по тем же причинам: пока строится DAG, `@rmq_trigger` прогоняет код скана по своему файлу и бросает `RMQTriggerNotStaticError` (это `ValueError`, импортируется из `airflow_provider_rmq.watcher`), если watcher отверг бы файл, не узнал бы вызов, вычислил бы другой `dag_id` или прочитал бы значения, отличные от полученных декоратором, — например, константу, изменённую между строкой декоратора и вызовом фабрики. Airflow записывает для файла ошибку импорта и показывает её в плашке Broken DAG с перечнем проблем, а все DAG'и файла пропадают, включая те, на которых `@rmq_trigger` нет: подписка, работающая вопреки объявленному в файле, хуже файла, который явно не загрузился. Проверка охватывает весь файл, но выполняется только тогда, когда при импорте исполняется хотя бы один `@rmq_trigger` этого файла. В файле, где каждый вызов стоит в фабрике, которую файл не вызывает, или под `if`, ложным в этом окружении, единственный сигнал — ERROR watcher'а. `rmq_trigger`, переэкспортированный другим модулем под другим именем (`from common import trig`), для скана вовсе не `rmq_trigger`: `@trig(...)` обрывает импорт, а watcher продолжает регистрировать прочие подписки файла.

Кроме того, `@rmq_trigger` должен стоять в файле, который сканирует watcher: в `*.py`-файле внутри `DAGS_FOLDER`, которого не отсекает ни один `.airflowignore`, — в перечислении, описанном выше, скрытые каталоги и файлы включительно. Вызов в любом другом месте обрывает импорт с ошибкой «is in a file the RMQ watcher does not scan» — в том числе фабрика с `@rmq_trigger`, объявленная в `plugins/`, в пакете из `PYTHONPATH` или в каталоге папки, который отсекает `.airflowignore` (общий пакет `common/`, указанный там), и вызванная из DAG-файла: Airflow такой DAG загрузил бы, а watcher его подписку не увидел бы никогда. Проверка относится к файлу вызова `rmq_trigger(...)`, поэтому прямой импорт файла под `.airflowignore` внутри `DAGS_FOLDER` — тест, импортирующий копию DAG-файла из игнорируемого `tests/`, — падает так же; `DagBag` такие файлы не импортирует вовсе. `.airflowignore` на пути к файлу, который не читается или не применяется, обрывает импорт с ошибкой «the RMQ watcher cannot tell whether it scans this file» и называет причину, а файл внутри `.zip`-архива — с ошибкой «the RMQ watcher cannot read this file». Проверка сравнивает место вызова с `airflow.settings.DAGS_FOLDER` процесса, импортирующего файл, поэтому CI-задача или тест, загружающие DAG'и через `DagBag(dag_folder=<путь>)`, должны направить `DAGS_FOLDER` на ту же папку — `AIRFLOW__CORE__DAGS_FOLDER=<путь>` в окружении или `airflow.settings.DAGS_FOLDER`, выставленный на неё до создания `DagBag`. Иначе каждый DAG с `@rmq_trigger` падает с этой ошибкой импорта. Модуль, импортированный через другое написание пути к папке — запись `sys.path` с её реальным путём или другую символическую ссылку на неё, — считается лежащим внутри неё. Проверка читает каждый файл один раз на версию на диске (inode, время изменения, размер), сколько бы вызовов `@rmq_trigger` в файле ни было, и читает файл таким, каким он лежит на диске в момент построения DAG: файл, подменённый между импортом и проверкой, — например, обновление git-sync посреди импорта, — может уронить этот один разбор, а следующий разбор импортирует новую версию без ошибки.

### Быстрый старт

**Шаг 1 — добавьте декоратор к DAG:**

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.watcher.decorators import rmq_trigger

@rmq_trigger(queue="orders", conn_id="rmq_default")
@dag(schedule=None)
def orders_dag():
    @task
    def process(**context):
        conf = context["dag_run"].conf
        print(f"Тело: {conf['body']}, Заголовки: {conf['headers']}")
    process()

orders_dag()
```

`@rmq_trigger` должен быть самым внешним декоратором, прямо над `@dag(...)` (как показано выше) — если поставить его ниже, при импорте возникнет `TypeError` вместо тихого бездействия.

**Шаг 2** — перезапустите Scheduler. Плагин активируется автоматически, дополнительная конфигурация не требуется.

**Шаг 3** — опубликуйте сообщение в очередь `orders` — DAG запустится в течение нескольких секунд.

### Несколько очередей и cooldown

Подпишите один DAG на несколько очередей и ограничьте частоту запусков параметром `cooldown`:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.watcher.decorators import rmq_trigger

@rmq_trigger(
    queues=["orders", "payments"],  # сообщение из любой очереди запускает DAG
    cooldown=300,                    # cooldown 300 с — DAG запускается один раз за окно
    conn_id="rmq_default",
)
@dag(dag_id="my_dag", schedule=None)
def my_dag():
    @task
    def process(**context):
        conf = context["dag_run"].conf
        # conf["source"] == "cooldown" при запуске через механизм cooldown
        # conf["body"] и conf["headers"] пусты — исходные данные теряются в DLX-цепочке
        print(conf["source"])
    process()

my_dag()
```

**Как работает cooldown:**

- При поступлении первого подходящего сообщения плагин публикует TTL-маркер в `rmq_watcher.pending.{dag_id}` (очередь без потребителей, `x-max-length=1`, DLX в `rmq_watcher.fire`).
- Через N секунд маркер истекает и маршрутизируется в `rmq_watcher.fire`; fire consumer вызывает `trigger_dag()` с идемпотентным run_id.
- Дополнительные сообщения в течение окна cooldown подтверждаются (ACK) молча — pending-очередь отклоняет повторный publish (`x-overflow=reject-publish`).
- Вся RMQ-инфраструктура (exchange и очередь `rmq_watcher.fire`, а также per-DAG очереди `rmq_watcher.pending.*`) создаётся плагином автоматически при запуске.

**Ограничения:**
- Все cooldown-подписки одного DAG делят одну pending-очередь и один таймер.
- Все cooldown-DAG должны использовать один `conn_id` / vhost.
- `conf["body"]` и `conf["headers"]` пусты при запуске через cooldown — исходные данные сообщения теряются в DLX-цепочке.
- Изменение `cooldown` в DAG-файле вступает в силу на следующем reconcile-цикле (по умолчанию 60 с); уже запущенные таймеры в RMQ не затрагиваются.
- Cooldown-DAG, который Airflow не сериализует никогда — удалён из папки, сломан ошибкой импорта, заведён только на другом инстансе, — оставляет своё fire-событие ходить по `rmq_watcher.fire` столько, сколько живёт процесс, а все строки подписок этого DAG'а показывают `listening`. Собственной строки у fire-консьюмера нет, поэтому единственное место, где это видно, — лог шедулера: INFO на каждое событие и WARNING на каждое десятое.

**Права доступа RabbitMQ (только при cooldown):**

При `cooldown > 0` пользователю Airflow необходимы права configure/write/read на шаблон ресурсов `rmq_watcher.*` помимо прав на прикладные очереди:

```
rabbitmqctl set_permissions -p <vhost> <user> "^(rmq_watcher\\..*|your-queue.*)$" "^(rmq_watcher\\..*|your-queue.*)$" "^(rmq_watcher\\..*|your-queue.*)$"
```

Это охватывает exchange `rmq_watcher.fire`, очередь `rmq_watcher.fire` и очереди `rmq_watcher.pending.<dag_id>`, которые создаёт механизм cooldown.

### Триггеры в режиме exchange

Вместо подписки на заранее созданную и привязанную вручную очередь (`queue=`/`queues=`), DAG может подписаться напрямую на topic-обменник. Когда указан `exchange=`, провайдер полностью берёт на себя RMQ-инфраструктуру — ручное создание очереди и внешняя YAML-таблица маршрутов не нужны:

```python
from airflow.decorators import dag, task
from airflow_provider_rmq.watcher.decorators import rmq_trigger

# Routing key в форме Jetstat: декартово произведение id × status
@rmq_trigger(
    exchange="jetstat.airflow",
    routing_key_ids=["670f877702775c2de8325b1f"],
    routing_key_status="succeeded",   # по умолчанию "*" = любой статус
)
@dag(dag_id="jetstat_succeeded", schedule=None)
def jetstat_succeeded_dag():
    @task
    def process(**context):
        conf = context["dag_run"].conf
        print(conf["routing_key"])  # "670f877702775c2de8325b1f.succeeded"
    process()

jetstat_succeeded_dag()

# Произвольные routing key любой формы (не привязаны к форме id/status)
@rmq_trigger(exchange="some.other.exchange", routing_keys=["region.eu.alert"])
@dag(dag_id="region_alerts", schedule=None)
def region_alerts_dag():
    ...
```

Обе формы можно комбинировать в одном вызове — итоговый набор routing key является объединением `routing_keys` и декартова произведения `routing_key_ids` × `routing_key_status`.

**Что провайдер создаёт автоматически** на каждом reconcile-цикле:

- Сам обменник (topic, durable, с `alternate-exchange` для немаршрутизируемых сообщений)
- Выделенную очередь `rmq_watcher.sub.{dag_id}` — **одна общая очередь на DAG**, потребляется точно так же, как любая подписка через `queue=`
- Привязки (bindings) между этой очередью и обменником, синхронизированные с routing key, объявленными в декораторе на текущий момент (сравниваются с реальным состоянием привязок в RabbitMQ через Management HTTP API — а не с тем, что хранится в БД Airflow)
- Страховочные механизмы: немаршрутизируемые сообщения попадают в `{exchange}.unrouted` (TTL 8ч); каждое промаршрутизированное сообщение зеркалируется в `{exchange}.log` (catch-all привязка `#`, TTL 8ч) для последующего логирования/аудита

**Extra-параметр подключения — `management_url`:** эндпоинт Management HTTP API брокера. Режиму exchange он нужен для чтения текущих привязок (в AMQP 0-9-1 нет операции «показать мои привязки»), а watchdog живости пользуется им для подписок любого типа. Добавьте его в тот же Airflow Connection, который используется для AMQP:

```json
{
  "management_url": "https://rabbitmq.example.com"
}
```

Учётной записи нужен тег `management` вдобавок к её AMQP-правам — пользователю без тега Management API отвечает 401/403, что бы ни выдал ему `set_permissions`:

```
rabbitmqctl set_user_tags <user> management
```

Ответ Management API определяется правами учётной записи, которая спрашивает: пользователю с тегом `management` показывают только каналы его собственных соединений, а `monitoring` и `administrator` видят весь vhost. На учётной записи минимальных прав, описанной выше, watcher видит поэтому своих consumer'ов и ничьих больше.

Те же `login`/`password` из подключения повторно используются для вызова Management API. Если `management_url` не задан, bind-diff пропускается на каждом цикле (логируется как ERROR) — очередь всё равно объявляется и потребляется как обычно, но привязки никогда не создаются/обновляются.

`management_url` заодно выбирает пробу живости для **каждой** подписки — и `queue=`, и `exchange=`. С ним проверка читает `GET /api/consumers/{vhost}`, различает отдельные consumer-теги и показывает число в колонке Broker consumers. Без него проверка откатывается на passive-declare очереди: он разом ручается за всех consumer'ов этого `conn_id` (или разом их осуждает) и не возвращает никакого числа, поэтому колонка Broker consumers остаётся `—` даже при положительном вердикте. После двух подряд неудачных обращений к Management API по одному `conn_id` (неверный URL, учётная запись без тега `management`, 404, таймаут) проверка переходит на тот же passive declare, а не остаётся слепой. Тот же двухцикловый предел действует и на `conn_id`, чьё Airflow-соединение вообще не читается — переименовали, удалили, пока подписки на него ссылаются, или secrets backend раз за разом отказывает: тогда спрашивают само соединение из пула, которому строка метаданных не нужна.

**Без стекинга — один DAG, один обменник.** Несколько декораторов `@rmq_trigger(exchange=...)` на одном DAG вызывают `ValueError` — все они привели бы к одной и той же очереди `rmq_watcher.sub.{dag_id}`, и последний обработанный декоратор молча победил бы. Ошибка возникает в момент вызова фабрики `@dag(...)` (например, `jetstat_succeeded_dag()` в конце файла, как в примере выше) — во время импорта, до регистрации DAG, — а watcher отвергает все подписки такого файла. Используйте один вызов декоратора с объединением routing key, либо подписывайтесь на разные обменники из разных DAG. Чтобы потреблять из нескольких обменников в одном DAG, используйте `queue=`/`queues=` с очередями, созданными и привязанными вручную.

**Права RabbitMQ (только для режима exchange):** в дополнение к шаблону `rmq_watcher\..*`, уже требуемому для cooldown, пользователю Airflow в RMQ нужны:

```
# configure: объявление обменника / alternate-exchange / его очередей
rabbitmqctl set_permissions -p <vhost> <user> "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted|\\.log)?|...)$" \
  "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted|\\.log)?|...)$" \
  "^(rmq_watcher\\..*|jetstat\\.airflow(\\.unrouted)?|...)$"
```

`configure`/`write` нужны на `{exchange}(.unrouted|.log)?`; `read` дополнительно нужен на `{exchange}(.unrouted)?`, потому что привязка очереди *от* обменника требует права read на обменник-источник, а не только configure на целевую очередь. Замените `jetstat.airflow` на имя, которое реально передаётся в `exchange=`.

**Миграция с `queue=` на `exchange=`:** переключение существующей подписки не убирает за собой старую инфраструктуру — старая, созданная вручную очередь **не** удаляется автоматически и остаётся без консьюмера после повторного деплоя DAG-файла с `exchange=`. Удалите её вручную после того, как убедитесь, что миграция работает корректно.

**Переименование DAG:** изменение `dag_id` приводит к созданию новой очереди/привязок `rmq_watcher.sub.{new_dag_id}` на следующем reconcile-цикле. Старая `rmq_watcher.sub.{old_dag_id}` становится сиротой (orphan) — её метаданных подписки больше нет ни в одном распарсенном DAG-файле (см. ADR-0005) — и **не** удаляется автоматически. Удалите её вручную.

**Мониторинг:**

- RabbitMQ Management UI — `rmq_watcher.sub.{dag_id}` должна показывать `consumer count > 0`, когда подписка DAG активна
- Логи Airflow — WARNING для осиротевших очередей/привязок `rmq_watcher.sub.*` (с подсказкой `rabbitmqadmin delete queue ...`); ERROR для пропущенного bind-diff (Management API недоступен) или конфликта свойств обменника (`PRECONDITION_FAILED` — имя обменника уже занято чем-то другим с другими свойствами). WARNING о сироте идёт впереди удаления: `exchange`/`routing_keys` берутся только из текущего скана, поэтому подписка, чьего ключа нет в доверенном скане, читается как осиротевшая с первого же промаха — за четыре цикла до того, как её строка уйдёт, — и всё это время её биндинги не сверяются, хотя строка и её consumer живы

**Откат (rollback):** удалите `exchange=`/`routing_keys=`/`routing_key_ids=`/`routing_key_status=` из декоратора и передеплойте. `rmq_watcher.sub.{dag_id}` становится сиротой — в логах появляется WARNING с первого доверенного скана без ключа, за несколько циклов до того, как удалится сама строка подписки; TTL (8ч) ограничивает неконтролируемый рост, а ручная очистка выполняется по подсказке из текста WARNING. Сам обменник и его очереди `.unrouted`/`.log` откатом **не** затрагиваются (ими могут всё ещё пользоваться другие DAG).

### Payload, передаваемый в DAG

```python
conf = context["dag_run"].conf
# Немедленный запуск (cooldown=0 или без cooldown):
# {
#     "source":          "immediate",
#     "body":            "<тело сообщения в UTF-8>",
#     "headers":         {"key": "value", ...},
#     "routing_key":     "orders.created",
#     "queue":           "orders",
#     "subscription_id": 42,
# }
# Триггеры в режиме exchange (exchange=) используют точно такую же форму "immediate" —
# "queue" — это всегда rmq_watcher.sub.{dag_id} (а не имя обменника), а "routing_key" —
# это фактический подошедший routing key (например, "<id>.<status>" для формы routing_key_ids).
#
# Cooldown-запуск (после истечения TTL в rmq_watcher.fire):
# {
#     "source":          "cooldown",
#     "dag_id":          "<dag_id>",
#     "body":            "",        # пусто — тело исходного сообщения не сохраняется
#     "headers":         {},        # пусто — заголовки исходного сообщения не сохраняются
#     "routing_key":     "<dag_id>",
#     "queue":           "rmq_watcher.fire",
#     "subscription_id": None,
# }
```

### Управление подписками

| Способ | Описание |
|---|---|
| Декоратор `@rmq_trigger` | Infrastructure as Code — подписка живёт в DAG-файле, управляется через git |
| Airflow UI по адресу `/rmq-watcher/subscriptions` | Создание, редактирование, включение/отключение, удаление (только для UI-подписок) |
| Прямая запись в БД | Для автоматизации через Terraform / скрипты (`source='ui'`) |

Подписки типа `dag_file` — **только для чтения** в UI: reconciliation перезаписывает БД из кода каждые 60 с. Через UI можно изменить только переключатель `enabled`.

Выключение `dag_file`-подписки переживает сбой чтения DAG-файлов. Цикл, скану которого нельзя верить — папка DAG'ов пропала на секунду под git-sync'ом, лишилась прав или опустела, — синхронизацию пропускает вовсе; доверенный скан без её ключа даёт строке отсрочку в три цикла, и выключенная строка получает эту отсрочку целиком, даже когда Airflow к тому моменту уже не считает её DAG активным. Всё это время строка та же самая, а вместе с ней и снятый оператором флаг. Отсрочка не бессрочна: ключ, промахнувшийся мимо четвёртого доверенного скана подряд, строку удаляет — и декоратор, вернувшийся позже, создаёт её заново включённой.

Подписки в режиме exchange отображаются в UI как любая другая подписка `dag_file` — только по имени их очереди (`rmq_watcher.sub.{dag_id}`). Метаданные `exchange`/`routing_keys` там не показываются; единственным источником истины для них остаётся файл DAG.

Любая подписка, чей `dag_id` не соответствует активному Airflow DAG, помечается на странице бейджем `⚠ dag not found`. Это **односторонний** сигнал: его наличие означает реальную проблему — подходящее сообщение будет подтверждено (ACK) без какого-либо запуска, — а его **отсутствие ничего не гарантирует**. Три причины, по которым бейдж может пропустить сломанную подписку: `is_paused` не проверяется, поэтому DAG на паузе никогда не получит бейдж, хотя подходящее сообщение всё равно будет ACK-нуто без запуска — точно так же, как для отсутствующего DAG (см. [ADR 0006](docs/adr/0006-badge-dag-lookup-not-unified-with-sync-trigger.md)); только что добавленный DAG может недолго быть помечен бейджем, пока его не распарсит Scheduler; а только что удалённый или переименованный DAG, наоборот, какое-то время остаётся без бейджа, пока не обновится `DagModel.is_active`. Последние два случая — ожидаемая задержка, а не баг.

Очередь, которую слушает больше одной **включённой** подписки того же `conn_id`, помечается бейджем `⚠ shared queue`; групповая строка называет, какие именно из её очередей разделяются, и читается как `⚠ shared queue: q1, q2`. Выключенная строка потребителя не поднимает и бейджа не получает. Бейдж стоит на обеих строках пары: каждая из них задета одинаково. Там, где у одной из подписок включён `ack_unmatched`, к бейджу добавляется ` — messages deleted`, и подсказка называет цену: сообщение, которое принимает только соседний фильтр, удаляется в момент, когда попадает на подтверждающего потребителя, а сосед продолжает показывать `listening`. Колонка `ack_unmatched` в таблице показывает `on` у подписки, подтверждающей свои промахи.

Та же настройка — чекбокс на форме подписки, доступный во всех трёх её сценариях: создание, редактирование и редактирование группы, — и она переживает сохранение группы, которое переписывает её строки заново. У группы чекбокс стоит сразу за все её строки: он отмечен, только когда подтверждают все очереди группы, и сохранение пишет это одно значение в каждую строку. Поэтому группа, строки которой расходятся, рисуется снятой, а под чекбоксом выходит подсказка об этом расхождении: колонка таблицы читает такую группу как подтверждающую, потому что подтверждает одна из очередей, а форма отвечает наоборот намеренно — сохранение, которое поправило один только cooldown, не должно включать безвозвратное удаление той очереди, где его не было. Отклонённая отправка — отрицательный cooldown, `filter_data` не в JSON — перерисовывает чекбокс из отправленного значения, а не из сохранённой строки, поэтому только что снятая галочка возвращается снятой.

Сохранение подписки, разделяющей очередь с другой включённой, поднимает `flash`-предупреждение с именем очереди; то же делает включение подписки тумблером. Предупреждение выходит после сохранения, а не до него: форма рисует `dag_id`, список очередей, `conn_id`, cooldown и `filter_data` из сохранённой строки, поэтому контур подтверждения перерисовал бы их из базы и потерял бы ровно тот ввод, который подтверждают.

### Доступ к странице подписок

Страница и пункт меню принадлежат двум ролям — Admin и Op. Viewer и User не видят ни пункта, ни категории `RabbitMQ`, под которой он висит, а прямой заход на `/rmq-watcher/subscriptions` отвечает отказом.

Admin получает страницу от самого Airflow: тот отдаёт этой роли все не-DAG'овые права на каждой синхронизации ролей. Op получает её от провайдера — на каждом старте веб-сервера плагин выдаёт роли `can_read`, `can_create`, `can_edit` и `can_delete` на ресурс `RMQ Subscriptions` плюс `menu_access` на `Subscriptions` и на `RabbitMQ`. Выдача идёт при каждом старте, поэтому обновление провайдера открывает страницу роли Op само, без ручных действий.

На чистой метабазе нужны два старта: роль `Op` Airflow создаёт уже после того, как отработал колбэк плагина, поэтому первый старт роли не находит, пишет предупреждение и оставляет выдачу следующему старту. Выдача работает при auth-менеджере FAB — он же стоит по умолчанию; при другом `auth_manager` выдавать права некому, и колбэк вместо этого пишет предупреждение.

> **Предупреждение.** В эти права входят создание и удаление подписок, то есть доступ к странице — это право заставить произвольный DAG запускаться по сообщению из очереди. Установка, где роли Op такое право не положено, пишет `grant_op_access = false` в секцию `[rmq_watcher]` файла `airflow.cfg` — или задаёт `AIRFLOW__RMQ_WATCHER__GRANT_OP_ACCESS=false` в окружении веб-сервера — и перезапускает веб-сервер.

Переключатель читает и исполняет только веб-сервер. Блюпринт плагина регистрирует каждое приложение, собирающее appbuilder, — FAB-команды за `airflow users`, `airflow roles` и `airflow sync-perm` собирают его тоже, — а права страницы принадлежат тому приложению, которое эту страницу подаёт, поэтому выдача спрашивает про собственный веб-интерфейс Airflow и отходит в сторону там, где его нет. Команда, запущенная в сессии без этого переключателя, оставляет права роли в покое, а не отвечает значением, которого оператор там не задавал.

Переключатель управляет доступом, а не только выдачей: ложное значение **снимает** те же шесть прав с роли Op на следующем старте веб-сервера. Это говорят `false`, `off`, `no`, `n` и `0`, а также значение в написании, которого переключатель не знает: непрочитанный ответ страницу закрывает, а не оставляет открытой. Выдавать или отбирать их руками при истинном или незаданном значении бессмысленно — старт вернёт ответ провайдера на место.

Это опция конфигурации Airflow, а не Airflow Variable, потому что роли `Op` Airflow отдаёт создание, изменение и удаление Variables. Переключатель, лежащий там, — это переключатель, который сама ограничиваемая им роль вернёт себе в `true` и заберёт страницу обратно на ближайшем перезапуске веб-сервера; `airflow.cfg` и окружение процесса недосягаемы для любой роли из веб-интерфейса, а именно это и требуется от переключателя, отбирающего привилегию.

`menu_access` выдаётся на **отображаемые** имена пункта и его категории, а меню FAB плоское. Поэтому другой установленный плагин, назвавший свой пункт `Subscriptions` или свою категорию `RabbitMQ`, разделяет эти два права, и отзыв закроет роли Op его меню заодно с этим.

Любая другая роль открывает страницу через CLI Airflow:

```bash
airflow roles add-perms MyRole -a can_read -r "RMQ Subscriptions"
airflow roles add-perms MyRole -a menu_access -r "Subscriptions"
airflow roles add-perms MyRole -a menu_access -r "RabbitMQ"
```

Без `menu_access` на категорию пункт не появится в меню даже при двух остальных выданных правах. Без `can_create`, `can_edit` и `can_delete` страница по-прежнему рисует все восемь своих управляющих элементов — шаблон прав не проверяет, — и каждый из них отвечает отказом.

### Процесс watcher'а

В самом процессе Scheduler'а у плагина нет ни потоков, ни event loop, ни пулов потоков. Airflow делает `fork()` из процесса Scheduler'а — пул `CeleryExecutor`, отправляющий задачи в Celery, менеджер разбора DAG'ов, — а fork безопасен только из процесса с одним потоком, поэтому цикл watcher'а работает в собственном процессе; обоснование — в [ADR 0008](docs/adr/0008-watcher-child-process.md). Listener запускает его в `on_starting`, из основного потока Scheduler'а, через `subprocess.Popen` с окружением Scheduler'а, так что процесс читает ту же конфигурацию Airflow, метабазу, бэкенд секретов и настройки StatsD. Установка — обычный `pip install`: ни отдельного сервиса, ни отдельной команды; один Scheduler — один процесс watcher'а под ним, который в `docker top` контейнера Scheduler'а виден как `python -c <bootstrap> airflow_provider_rmq.watcher.runner` под `airflow scheduler`. Bootstrap — программа в одну строку, которая выполняет модуль runner'а так же, как `python -m`. Прежде чем что-либо импортировать, она убирает из пути поиска модулей рабочий каталог Scheduler'а — обычно `AIRFLOW_HOME`, — который `python -c` ставит в его начало и которого нет в пути самого Scheduler'а, так что лежащие там `runpy.py`, `logging.py` или пакет с именем этого пакета в процессе watcher'а не импортируются; сам рабочий каталог у процесса тот же, что у Scheduler'а, и относительные пути в окружении разрешаются так же. Для пакета, положенного в папку plugins, а не установленного через pip, bootstrap добавляет папку plugins в конец пути поиска модулей, где она и у Scheduler'а, так что модуль в ней с именем модуля стандартной библиотеки или установленного пакета — `plugins/secrets/` со своим бэкендом секретов — не подменяет его ни в одном из процессов. До запуска цикла процесс загружает конфигурацию провайдеров и плагины Airflow — так же, как они загружены у Scheduler'а до запуска его job'ов. Загрузка идёт в основном потоке процесса — там же, где свои плагины грузит Scheduler, — так что плагин, который при импорте ставит обработчик сигнала или wakeup fd сигналов, загружается в обоих процессах одинаково. Тем временем отдельный поток уже ждёт сигнал остановки и следит за Scheduler'ом, так что плагин, зависший на импорте, не мешает процессу остановиться; поток цикла стартует только после того, как загрузка закончилась. Процесс работает и на Linux, и на macOS; сигнал о смерти родителя из раздела **Остановка** есть только в Linux, на других системах смерть Scheduler'а замечают только сверки родителя — потока, который следит за загрузкой плагинов, затем основного потока процесса раз в секунду и цикла перед каждым проходом. stdout и stderr у него Scheduler'а, поэтому все строки watcher'а попадают в лог контейнера Scheduler'а, как и строки самого процесса:

| Строка | Значение |
|---|---|
| `RMQ Watcher process started with PID … (scheduler PID …)` | Scheduler запустил процесс |
| `RMQ Watcher process … started for the scheduler process …` | Процесс импортировал Airflow и запускает цикл |
| `RMQ Watcher run … (host …, PID …) keeps its heartbeat in rmq_watcher_heartbeat` | Цикл запущен; идентификатор запуска — ключ строки этого процесса в таблице heartbeat (см. **Heartbeat** ниже) |
| `RMQ Watcher process received SIGTERM — stopping the watcher` | Пришла просьба об остановке; цикл перестаёт ждать текущий шаг, не начинает следующих и останавливает consumer'ов |
| `RMQ Watcher: stop requested — ending this cycle …` | Остановка пришла посреди цикла; остаток цикла, reconcile в том числе, пропускается, и цикл переходит к остановке consumer'ов |
| `RMQ Watcher: the scheduler process … has exited (parent is now …) — stopping the watcher` | ERROR — Scheduler умер, не остановив процесс, и цикл заметил это перед очередным проходом |
| `RMQ Watcher process: the scheduler process … has exited (parent is now …) — stopping the watcher` | ERROR — Scheduler умер, не остановив процесс, и основной поток процесса заметил это между двумя ожиданиями сигнала остановки |
| `RMQ Watcher process … exited with code …` | Scheduler остановил процесс и собрал его код выхода |
| `RMQ Watcher process … had already exited with code …` | К моменту остановки Scheduler'ом процесса уже не было — обычная строка при `docker stop` с `dumb-init`, который шлёт ему SIGTERM вместе со Scheduler'ом |
| `RMQ Watcher process … still running 5s after SIGTERM — killing it` | WARNING — процесс не вышел за 5 с; listener шлёт SIGKILL |
| `RMQ Watcher process … not reaped 2s after SIGKILL — leaving it behind, the scheduler goes on stopping` | ERROR — процесс застрял в ядре, например на зависшем NFS; Scheduler останавливается без него |
| `RMQ Watcher process …: the loop is still running 45s after the stop — exiting without it` | stderr, в обход логирования Airflow, блокировку которого может держать поток цикла, — цикл не вернул управление за 45 с после остановки; процесс выходит без него |
| `RMQ Watcher process …: logging did not shut down within 2s — exiting without it` | stderr — поток, который процесс оставляет, держит блокировку обработчика логов; процесс выходит, не дожидаясь его |
| `RMQ Watcher loop failed — exiting` | ERROR с трассировкой — исключение вышло из потока цикла; код выхода 1 |
| `RMQ Watcher process failed — exiting` | ERROR с трассировкой — упала подготовка процесса или ожидание сигнала остановки; код выхода 1 |
| `RMQ Watcher process …: AIRFLOW_PROVIDER_RMQ_WATCHER_PARENT_PID does not hold the scheduler's PID — not starting the watcher` | stderr, до настройки логирования Airflow — процесс запущен без PID'а Scheduler'а; код выхода 2 |
| `RMQ Watcher process …: the scheduler process … exited before the watcher started` | stderr — Scheduler умер, пока процесс запускался; код выхода 0 |
| `RMQ Watcher process …: received SIGTERM while Airflow was being prepared — exiting` | stderr — остановка пришла, пока процесс ещё импортировал Airflow и загружал плагины; он выходит сразу, цикл не запускался; код выхода 0 |
| `RMQ Watcher process …: the scheduler process … exited while Airflow was being prepared — exiting` | stderr — Scheduler умер, пока процесс ещё импортировал Airflow и загружал плагины; код выхода 0 |
| `RMQ Watcher process …: prctl(PR_SET_PDEATHSIG) failed: …` или `… cannot request the parent-death signal …` | stderr — ядро отказало в сигнале о смерти родителя; проверки родителя в основном потоке процесса, раз в секунду, и в цикле, перед каждым проходом, всё равно завершат процесс, когда Scheduler'а не станет |
| `RMQWatcherListener in process …, a fork of the scheduler process … — the watcher process belongs to the scheduler, nothing to start or stop here` | В логе задачи LocalExecutor'а — супервизор задачи, форкнутый от Scheduler'а, наследует listener и оставляет процесс watcher'а Scheduler'у |

Коды выхода процесса: 0 — остановлен сигналом или смертью Scheduler'а; 1 — упала подготовка процесса или работа цикла; 2 — запущен без PID'а Scheduler'а. `-9` в строке Scheduler'а `exited with code` значит, что listener убил процесс SIGKILL'ом; отрицательный код в строке `had already exited` — номер сигнала, которым процесс завершился сам: `-9` у OOM killer'а, `-14` (SIGALRM) — собственный срок выхода процесса: выход застрял — на stderr, который никто не читает, на обработчике логов, на базе — на 60 с после того, как процесс решил выйти, и ядро завершило его.

**Остановка.** При остановке Scheduler'а listener (`before_stopping`) шлёт процессу SIGTERM и ждёт 5 с, пока тот остановит consumer'ов и выйдет, затем шлёт SIGKILL и ждёт ещё 2 с. Процесс, не собранный и после этого, пишется в лог ERROR'ом с PID и остаётся как есть: остановка Scheduler'а никогда не ждёт watcher дольше 7 с. SIGKILL обрывает собственную остановку consumer'ов, и неподтверждённые доставки брокер возвращает в очереди. При `docker stop` с `dumb-init` и `DUMB_INIT_SETSID=1` процесс watcher'а получает SIGTERM от init напрямую, вместе с Scheduler'ом, и повторная просьба listener'а после него безвредна. Получив SIGTERM сам, процесс ждёт до 45 с, пока цикл остановит consumer'ов — их собственный бюджет 30 с, — и затем выходит без него. Блокирующий шаг цикла, который ещё идёт, цикл не дожидается — вызов остаётся в своём воркере до выхода процесса — и следующих шагов, reconcile в том числе, не начинает, так что consumer'ам достаётся весь их бюджет. Reconcile, который остановка застала уже идущим, не прерывается: он доходит до конца, в пределах бюджета цикла, и только потом consumer'ы останавливаются; reconcile дольше этих 45 с оставляет процесс выходить без их остановки, и строки подписок не помечаются `disconnected`. На чём бы выход ни застрял после этого — пайп stderr, который никто не читает, блокировка обработчика логов, база, не отвечающая на закрытие соединения, — через 60 с после решения выйти процесса нет: прежде чем написать или залогировать на выходе хоть одну строку, он просит ядро взвести будильник, SIGALRM, чьё действие по умолчанию завершает процесс, в чём бы ни застряли его потоки. Исключение одно — плагин, который при импорте ставит собственный обработчик SIGALRM: этот обработчик действует, пока плагины не загружены, и остановка, пришедшая во время загрузки, обходится без срока — между ней и выходом одна строка в stderr. Когда загрузка вернула управление, процесс возвращает SIGALRM действие по умолчанию и снимает таймер SIGALRM, оставленный плагином, и с этого момента 60 с — самое большее, на сколько watcher может пережить Scheduler, который его уже не ждёт или умер. Scheduler, умерший без остановки — `kill -9`, OOM killer, — всё равно уводит watcher за собой: на Linux ядро шлёт процессу SIGTERM, как только завершается основной поток Scheduler'а, а там, где ядро в этом сигнале отказало или, как на macOS, такого сигнала нет, процесс каждую секунду, а цикл перед каждым проходом сверяет PID родителя с PID'ом Scheduler'а и завершается с одной из ERROR-строк из таблицы выше. Остановка или смерть Scheduler'а, пришедшая, пока процесс ещё импортирует Airflow и загружает плагины, завершает его сразу, сколько бы эта загрузка ни длилась: отдельный поток ждёт сигнал и проверяет родителя с самого начала, а цикла, который надо останавливать, ещё нет.

**Другие сигналы.** Процесс watcher'а игнорирует SIGHUP, SIGQUIT, SIGUSR1 и SIGUSR2. Он входит в группу процессов Scheduler'а, а `dumb-init` с `DUMB_INIT_SETSID=1` пересылает любой полученный сигнал всей группе, так что `docker kill -s USR2`, которым у Scheduler'а просят дамп состояния executor'а, или `-s QUIT` ради стеков его потоков доходит и до watcher'а — и тот продолжает работать. SIGTERM и SIGINT останавливают его, как описано выше. Программа, которую запускает процесс watcher'а, — команда опции `_cmd` конфигурации Airflow — стартует с этими четырьмя сигналами игнорируемыми; программа, ставящая свой обработчик, получает их как обычно. Стеки потоков самого watcher'а показывает `py-spy dump --pid <PID watcher'а>`.

**Умерший процесс watcher'а не перезапускается.** Падение внутри цикла восстанавливается на месте — event loop пересоздаётся через 30 с. Процесс, умерший целиком — OOM, segfault, `kill -9`, — остаётся мёртвым до следующего рестарта Scheduler'а: перезапуск потребовал бы потока или обработчика `SIGCHLD` в процессе Scheduler'а, то есть ровно того, чего у плагина в нём нет. До рестарта он виден в `docker top` как `<defunct>` под `airflow scheduler`, страница Subscriptions показывает его запуск как `dead` в блоке Watcher processes и помечает каждый `Last reconcile`, как только тот становится старше одного интервала плюс одного бюджета цикла, а если watcher был единственным — главная страница Airflow показывает всем пользователям красный баннер о том, что подписки не обрабатываются.

**Heartbeat.** Каждый запуск процесса watcher'а ведёт одну строку таблицы `rmq_watcher_heartbeat`: идентификатор запуска (UUID), хост (`get_hostname()` Airflow), PID, время старта, время последней отметки о завершённом цикле, момент, к которому должна прийти следующая отметка, и момент штатной остановки. Падение цикла, восстановленное внутри процесса, сохраняет и запуск, и его строку, а пересозданный цикл записывает новый срок перед своим первым проходом — один раз на записанный старт или отмеченный цикл: цикл, снова упавший до отметки, срока не записывает, поэтому строка запуска, чьи циклы раз за разом упираются в бюджет, просрочивается. Перед первым циклом запуск записывает срок, покрывающий этот цикл; каждый цикл, уложившийся в бюджет, записывает свою отметку и срок следующей: через один reconcile-интервал, один бюджет цикла, чтение переменных (15 с) и 30 с на запись heartbeat, чистку и следующую запись — `405` секунд при незаданных переменных. Когда переменные увеличивают бюджет цикла, новый срок записывается до цикла, который пойдёт с этим бюджетом. Фонового пинга нет: зависший цикл не оставляет отметок, и его строка просрочивается так же, как строка умершего процесса. Запись heartbeat ограничена 10 с; неудачная или не уложившаяся в срок запись — WARNING, и цикл продолжает обрабатывать подписки. Записи heartbeat и чистка — такие же шаги пула цикла, как шаги самого цикла, и учитываются в `rmq_watcher.cycle_step_timed_out` и `rmq_watcher.cycle_step_skipped` наравне с ними. На SIGTERM или SIGINT, пока Scheduler ещё его родитель, процесс записывает `stopped_at` из отдельного потока и ждёт эту запись не дольше 2 с; watcher, завершившийся из-за смерти Scheduler'а, отметки остановки не оставляет, и его строка просрочивается. После каждого записанного цикла watcher удаляет строки других запусков, чей срок прошёл, а последняя отметка старше суток. Таблицу создают процесс watcher'а и веб-сервер — каждый, когда она впервые ему нужна; создание, не удавшееся потому, что другой процесс только что создал таблицу, считается выполненным. Сам процесс Scheduler'а DDL для неё не выполняет. Обоснование — в [ADR 0012](docs/adr/0012-watcher-liveness-heartbeat.md).

**Баннер на главной странице.** Баннер отвечает на вопрос, подтверждает ли хоть один watcher, что подписки обрабатываются, а не на вопрос, есть ли свой watcher у каждого Scheduler'а. Запуск жив, пока у него нет штатной остановки и его срок не прошёл. Вердикт — первое из выполненных условий:

| Состояние | Условие | Главная страница | Статусы подписок |
|---|---|---|---|
| `unused` | Ни одна подписка не включена | — | Как записаны |
| `ok` | Живой запуск отметил завершённый цикл | — | Как записаны |
| `starting` | Запуски живы, ни один ещё не завершил цикл | — | Как записаны |
| `unconfirmed` | Живых запусков нет, но последний `last_reconcile_at` свежий | — (пояснение на странице Subscriptions) | Как записаны |
| `down` | Живых запусков нет, свежего `last_reconcile_at` нет | Красный баннер | `unknown` |

`last_reconcile_at` свежий, пока он моложе одного reconcile-интервала плюс одного бюджета цикла плюс 75 с на чтение переменных и остаток цикла после отметки — `435` секунд при незаданных переменных. Состояние `unconfirmed` покрывает watcher версии провайдера, которая не пишет heartbeat, веб-сервер, обновлённый раньше Scheduler'а, и первые минуты после остановки последнего watcher'а; оно переходит в `down`, как только отметка устаревает. Баннер — штатное оповещение Airflow на главной странице (списке DAG'ов), его видят все пользователи, включая публичную роль, поэтому в нём нет ни хоста, ни PID; он сообщает, что watcher не работает, либо что он остановлен в указанное время и ни один Scheduler его не запускает, либо что он не завершил ни одного цикла с указанного времени, и ссылается на страницу Subscriptions за подробностями. Когда последний watcher умирает без отметки остановки, баннер появляется после того, как прошёл его срок и устарела отметка `last_reconcile_at`, — примерно через 7 минут после его последнего завершённого цикла при незаданных переменных; после штатной остановки — когда устареет отметка. Каждый воркер веб-сервера переиспользует свой вердикт 30 с, так что к обеим задержкам добавляется до 30 с. Если веб-сервер не может прочитать базу, баннер не показывается — главная страница отрисовывается как обычно, — а в лог раз в 30 с пишется WARNING. При нескольких Scheduler'ах смерть watcher'а одного из них, пока остальные обрабатывают подписки, баннера не даёт, потому что подписки продолжают обрабатываться: его строка видна как `dead` на странице Subscriptions. Цикл, исчерпавший бюджет, отметки не оставляет, а event loop пересоздаётся через 30 с, после остановки consumer'ов; пересозданный цикл записывает новый срок перед своим первым проходом. В промежутке строка работающего процесса может быть просрочена до минуты, и баннер появится тогда, только если устарела и `last_reconcile_at`. Срок записывает хост watcher'а, а сравнивает с ним часы веб-сервера, поэтому часы хостов Scheduler'а и веб-сервера должны совпадать (NTP): часы веб-сервера, ушедшие вперёд на несколько секунд, на столько же сокращают каждый срок. Подписки, объявленные в DAG-файлах, попадают в таблицу подписок только через скан watcher'а, поэтому на свежей установке, чей watcher умер до первого цикла, вердикт — `unused`, и баннера нет. Обоснование — в [ADR 0012](docs/adr/0012-watcher-liveness-heartbeat.md).

**Метрика живости.** Баннеру ничего вне Airflow не нужно; внешней системе мониторинга достаётся ещё и StatsD-счётчик. `rmq_watcher.cycle_completed` (уходит под `[metrics] statsd_prefix` Airflow, по умолчанию `airflow.rmq_watcher.cycle_completed`) растёт на единицу на каждом reconcile-цикле, дошедшем до конца, — включая цикл, в котором отдельные шаги упали и были записаны в лог. Цикл, исчерпавший бюджет, сюда не засчитывается; он засчитывается в `rmq_watcher.cycle_timed_out`. Счётчик говорит о том, что процесс жив и его цикл крутится, а не о том, что watcher здоров: подписка, застрявшая в `error`, или соединение, приговорённое проверкой живости, его рост не останавливают, и видно их на странице Subscriptions. Правило на его остановку каждая установка настраивает сама; окно длиннее одного reconcile-интервала плюс одного бюджета цикла — `60 + 300` секунд при незаданных переменных — не даёт ему сработать на медленном, но работающем цикле. Такое правило и сообщает о смерти watcher'а одного Scheduler'а из нескольких, которую баннер оставляет странице Subscriptions: правило нужно своё на каждый Scheduler, потому что у каждого свой процесс watcher'а, и один из них может умереть, пока остальные работают. Измерения, называющего Scheduler, у счётчика нет, поэтому его должен добавить сборщик метрик — StatsD-агент или экспортер на каждом хосте, помечающий полученное хостом или экземпляром; один сборщик на все Scheduler'ы сливает их приросты в один ряд, и мёртвый watcher в нём не виден, пока жив хоть один другой.

**Соединения с метабазой.** У процесса watcher'а свой движок SQLAlchemy, который Airflow собирает из тех же настроек `[database]`, поэтому каждый Scheduler держит к метабазе до `sql_alchemy_pool_size + sql_alchemy_max_overflow` (по умолчанию 5 + 10) соединений сверх собственных. Под это стоит рассчитать `max_connections` базы или пулера перед ней.

**Память.** Процесс watcher'а — второй интерпретатор Airflow в контейнере Scheduler'а: он импортирует Airflow, провайдеры и плагины, как и Scheduler, и сверх того держит неподтверждённые доставки consumer'ов. Под него нужен запас в лимите памяти контейнера — при тесном лимите OOM killer заберёт один из двух процессов, а убитый так процесс watcher'а не перезапускается.

### Устойчивость соединения

Процесс watcher'а держит долгоживущие AMQP-соединения всё время работы Scheduler'а, поэтому рестарт брокера, тихо оборвавшийся TCP-канал, resource alarm или недоступная БД для него — обычная погода: после устранения внешней проблемы подписки восстанавливаются сами. Дизайн описан в [ADR 0007](docs/adr/0007-connection-liveness-two-tier-check.md); кратко — четыре слоя.

**1 — Heartbeat и таймауты на каждый вызов.** У любого асинхронного соединения есть `heartbeat` (по умолчанию 30 с), а каждый AMQP-вызов — connect, `channel()`, declare, bind, publish — выполняется под `connect_timeout`/`rpc_timeout` (см. [Тайминги подключения](#тайминги-подключения)). Умершая связь превращается в исключение, а не в вызов, который ждёт вечно. Границу connect'у ставит сам watcher, а не библиотека: `connect_robust(timeout=…)` покрывает TCP-connect и AMQP-handshake и на этом заканчивается, а следующее за ними ожидание — пока брокер объявит соединение разблокированным — остаётся снаружи и под resource alarm длится столько же, сколько сам alarm. Не уложившийся в границу connect не отменяют, а оставляют в полёте, и его соединение остаётся в пуле: отмена испортила бы объект соединения навсегда, а выбрасывание оставило бы открытое соединение к брокеру, которое некому закрыть. Соединения каждого `conn_id` строятся под собственным локом, и лок покрывает старт попытки, а не ожидание её: сколько бы вызывающих ни хотели соединение, брокера просят об одной попытке, и ждут они её рядом, а не по очереди. Переставший отвечать брокер стоит каждому вызывающему одного `connect_timeout` и задерживает только свои `conn_id`. Из пула выдаётся лишь пригодное соединение, а не просто незакрытое: незавершённое переподключение оставляет объект без транспорта — он считает себя открытым и роняет любой вызов через себя, — и такой объект заменяется, а не достаётся следующему вызывающему.

**2 — Watchdog живости подписок.** Каждый reconcile-цикл watcher спрашивает у брокера, зарегистрированы ли ещё его consumer'ы. Каждая подписка подключается под собственным тегом `rmq_watcher.{hostname}.{pid}.{sub_id}.{nonce}` (у fire-консьюмера cooldown вместо `{sub_id}` стоит `fire`), и проверка ищет именно этот тег — чужой consumer на той же очереди, второй DAG, подписанный на неё, или вторая реплика scheduler'а в HA за нашего поручиться не могут, а `{nonce}`, свой у каждого подключения к очереди, не даёт поручиться за сменившую его таску призраку, оставшемуся у брокера после не вернувшегося `close()`. Тег присваивается — и подписка объявляется `listening` — только после того, как брокер подтвердил регистрацию, а сама регистрация идёт под `rpc_timeout`: не вернувшийся `basic.consume` иначе оставил бы проверку искать тег, которого у брокера никогда не было. Если задан `management_url`, тег ищется в ответе `GET /api/consumers/{vhost}`; если нет — watcher делает passive `queue_declare` на отдельном канале, и его вердикт распространяется разом на всех consumer'ов этого `conn_id`. В обоих случаях тег дополнительно сверяется с каналом, на котором клиент его держит: RabbitMQ отменяет нашего consumer'а — удалили очередь, сменился лидер quorum-очереди или потока, перезапустился узел с classic-очередью под клиентом, подключённым к другому узлу — кадром `basic.cancel`, который не закрывает ни соединение, ни канал, поэтому любая проба этого соединения продолжает проходить успешно, пока никто не потребляет; пропавший из нашего собственного канала тег и есть то, что выносит приговор. Две подряд отрицательные проверки (то есть не раньше двух reconcile-интервалов) отменяют ровно те подписки, чей тег брокер не подтвердил, и сбрасывают общее для них соединение — перезапуск одной только таски выдал бы ей из пула то же самое зомби-соединение. Строка статуса второй проверки не ждёт: `conn_id`, чьих consumer'ов брокер сейчас не держит, сразу получает `error` с причиной и номером проверки. Подтверждённые подписки того же `conn_id` не отменяются и не перезапускаются: соединение выдёргивается у них из-под ног, они переподключаются собственным retry-циклом и показывают сброс транзиентной ошибкой consumer'а (WARNING называет их поимённо). Fire-консьюмер уходит вместе с соединением, на котором работает: выданный при старте объект соединения он держит всю жизнь; reconcile-цикл поднимает его заново — на том соединении, что лежит в пуле сейчас, — когда объект под ним подменили, и переносит его вслед за сменой cooldown-`conn_id`. `conn_id`, у которого fire-консьюмер оказался единственным прицепленным потребителем, пересоздаётся по одному его вердикту: спросить на этом соединении больше не о ком — его подписки ведёт собственный retry-цикл, — а перезапуск иначе получал бы из пула тот же объект каждый цикл. `conn_id`, у которого таски живы, но ни одна не доходит до `listening` два цикла подряд, не даёт проверке ни одного кандидата — тогда спрашивают само соединение, отвечает ли оно на RPC: молчание пересоздаёт его, ответ оставляет на месте, и строка говорит, что отказ находится за соединением, а не в нём (подписка, застрявшая в `error` из-за падающего триггера, подключена к исправному брокеру). Недоступность Management API — это «нет данных», а не вердикт: WARNING в лог, счётчики не трогаются, сохранённый статус остаётся — но лишь до второй неудачи подряд, на которой вступает passive declare; с нечитаемым Airflow-соединением поступают так же. А вот зависшая AMQP-проба считается признаком смерти. Один и тот же `conn_id` не пересоздаётся чаще, чем раз в 5 циклов; вердикт, задержанный этим ограничителем, пишется в лог и отражается статусом `degraded`.

**3 — Таймаут цикла.** Вся итерация reconcile выполняется в бюджете `max(reconcile_interval × 3, 300)` секунд. Его превышение останавливает менеджер и пересоздаёт event loop через 30 с, с ERROR'ом, называющим фазу (`migrate` / `scan` / `sync` / `read subs` / `reconcile`) и её длительность. Бюджет намеренно щедрый — срабатывание останавливает потребление по всем `conn_id`, — а точечные зависания ловятся таймаутами первого слоя, которые срабатывают гораздо раньше и только для той подписки, которой принадлежит вызов. У каждого блокирующего шага цикла (скан DAG-файлов, синхронизация в БД, чтение подписок) есть собственный бюджет в 60 секунд и единственная попытка в полёте, поэтому не отвечающая БД стоит одного цикла, а не всей петли, и не заполняет четырёхворкерный пул цикла застрявшими копиями одного и того же вызова.

**4 — Публикация на собственном соединении.** Cooldown-публикация идёт по отдельному, лениво поднимаемому соединению на каждый `conn_id`. Под memory- или disk-alarm RabbitMQ блокирует публикующие соединения тем, что перестаёт читать из сокета, — иначе вместе с публикацией встали бы подтверждения всех потребителей этого же соединения; вдобавок соединение, помеченное брокером как заблокированное, перестаёт писать и на стороне клиента (`aiormq` придерживает собственную запись до `connection.unblocked`), поэтому именно разведение оставляет `basic.ack` в движении: потребляющее соединение не публикует ничего и пометки не получает. Чего разведение не покрывает — соединение, поднятое *во время* алярма: годным оно станет только после его снятия, ради чего в первом слое и стоит клиентская граница на connect. Плейсхолдер, который брокер вернул как немаршрутизируемый — pending-очереди нет, и запускать DAG после окна будет некому, — возвращает доставку в очередь, а не подтверждает её. Два подряд таймаута публикации пересоздают только публикующее соединение и не задевают потребление, а публикация, которая продолжает падать, выдерживает растущую паузу (1 с с удвоением до 60 с) перед переподключением подписки. Каждая неудачная попытка возвращает доставку в очередь, а возврат через nack в счёт `delivery-limit` quorum-очереди не идёт, поэтому доставка переживает alarm любой длины; паузы тем временем не дают возврату крутиться на полной скорости. Fire-консьюмер cooldown работает на наименьшем `conn_id` среди cooldown-подписок, что не даёт порядку строк из БД перебрасывать его с брокера на брокер; cooldown-подписки, разложенные по нескольким `conn_id`, логируются как ERROR — pending-очереди живут только на одном брокере.

**Переменные Airflow:**

| Переменная | По умолчанию | Значение |
|---|---|---|
| `rmq_watcher_reconcile_interval` | `60` | Секунды между reconcile-циклами |
| `rmq_watcher_cycle_timeout` | `max(interval × 3, 300)` | Секунды, которые может занять один цикл, прежде чем event loop будет пересоздан |

Обе перечитываются в начале каждого цикла в отдельном пуле потоков под собственным коротким таймаутом, так что изменённая переменная вступает в силу со следующего цикла; чтение, всё ещё занимающее воркер, не даёт начаться следующему, а по собственному таймауту петля его бросает и остаётся при уже действующих значениях, а не подвешивается; БД, которая на чтение отвечает отказом, неотличима от незаданной переменной — читается «значения нет», и в силу вступают встроенные умолчания, пока следующий цикл не прочтёт настоящее.

**Конфигурация Airflow:**

| Опция | По умолчанию | Значение |
|---|---|---|
| `[rmq_watcher] grant_op_access` | `true` | Держит ли роль Op права [страницы подписок](#доступ-к-странице-подписок) |

Читается один раз за старт веб-сервера, когда выставляются права страницы, и читается из `airflow.cfg` и окружения веб-сервера — и больше ниоткуда: обходы `_cmd` и `_secret`, которые дотянулись бы до shell-команды или до secrets-бэкенда, Airflow чтит только для перечисленных им чувствительных опций, а этой среди них нет. Поэтому чтение не трогает ни сеть, ни базу, и в нём нечему зависнуть, пока веб-сервер ещё собирает своё приложение. Происходит оно раньше, чем что-либо трогается, и ответ читается в сторону отказа. `1`, `t`, `true`, `y`, `yes` и `on` держат страницу открытой; `0`, `f`, `false`, `n`, `no` и `off` её закрывают — регистр, окружающие пробелы и хвостовой комментарий после `#` значения не имеют. Значение, написанное ни в одном из этих написаний, страницу тоже закрывает, а само значение называется предупреждением в логе: у переключателя одна работа — отбирать привилегию, поэтому то, что прочесть не удалось, получает ответ в сторону отказа, а не выдачи. Умолчанием `true` читается только опция, которую никто не задавал вовсе. Опция объявлена в метаданных конфигурации провайдера, поэтому страница Configuration в Airflow, `airflow config list` и `airflow config get-value rmq_watcher grant_op_access` показывают её вместе со значением по умолчанию и на установке, где её никто не задавал.

**Что показывает страница Subscriptions.** Блок Connections по адресу `/rmq-watcher/subscriptions` показывает по строке на каждый `conn_id`:

| Колонка | Значение |
|---|---|
| Status | `connected` — брокер подтвердил все наши consumer-теги на этом `conn_id`; `error` — отрицательный вердикт: consumer, которого брокер не подтвердил, полное отсутствие работающих тасок этого `conn_id`, неудачная попытка соединения либо живые таски, из которых ни одна не доходит до `listening`, при том что соединение отвечает на RPC, — отказ находится за соединением (причина в Last Error); `degraded` — отрицательный вердикт задержан ограничителем частоты пересозданий, и соединение оставлено на месте вместо очередного пересоздания; `unknown` — по этому `conn_id` ещё ни одна проверка не выносила вердикта. Статус идёт только за вердиктом: без вердикта строка сохраняет записанное в ней значение, а число запущенных watcher'ом тасок не красит её в зелёный никогда |
| Consumers | Сколько живых consumer-тасок watcher держит на этом `conn_id`, включая cooldown-fire-таску, если она работает здесь же |
| Broker consumers | Сколько consumer'ов видит брокер на очередях тех подписок, которые действительно проверялись, то есть чей собственный статус — `listening`, плюс `rmq_watcher.fire`, если fire-консьюмер работает на этом `conn_id`. `—` означает, что числа нет вовсе: passive-declare-проба его не возвращает никогда, поэтому без `management_url` колонка остаётся `—` даже при положительном вердикте. Число, отличающееся от предыдущей колонки, подсвечивается: меньше — подписка ещё не подключилась (поднимается или отходит после ошибки) и потому не проверялась, больше — вторая реплика scheduler'а под теми же учётными данными или, на учётной записи с тегом `monitoring`/`administrator`, которая только и делает чужие каналы видимыми, другие клиенты на тех же очередях |
| Last reconcile | Возраст последнего цикла по этому `conn_id`, с пометкой, если он старше одного reconcile-интервала плюс полный бюджет цикла (`60 + 300` секунд, когда обе переменные не заданы): отметка ставится в конце цикла, после чего петля ждёт интервал, — это и есть предельный возраст для watcher'а, работающего ровно так, как настроен. Помеченная строка означает, что петля застряла, перезапускается или не работает вовсе, — либо что этот `conn_id` больше не упомянут ни одной включённой подпиской |

Строка пишется для **каждого** `conn_id` из списка подписок, включая тот, у которого умерли все таски. Счётчики и `last_reconcile_at` обновляются каждый цикл — именно по ним видно, что петля жива; `status` сохраняет последнее записанное значение, если проверка не дала данных.

Строки не удаляются. `conn_id`, который больше не упомянут ни одной включённой подпиской, перестаёт получать отметку и сохраняет возраст, накопленный к этому моменту, — строка остаётся помеченной навсегда, в том числе после документированного способа приостановить потребление: выключения последней подписки этого `conn_id` в UI. Включение любой из них возобновляет отметки со следующего цикла.

Колонка Status в таблице Subscriptions ниже на странице — другая: она показывает собственную таску одной подписки (`connecting`, `listening`, `error`, `disconnected`), тогда как блок Connections выше показывает общий для этих тасок `conn_id` (в сгруппированной строке видно, сколько подписок группы находятся в `listening`).

**Блок Watcher processes.** Над блоком Connections страница показывает строки heartbeat, которые учитываются: каждый живой запуск (не остановлен, срок не прошёл) и не больше 50 остальных запусков, чей срок не прошёл или чья последняя отметка не старше суток. Строки идут от самой свежей отметки к самой старой, живые и остальные вперемешку; столбцы — Host, PID, Started, Last mark с её возрастом, Due и State. Состояние — `alive` (срок не прошёл, цикл завершён), `starting` (срок не прошёл, ни один цикл ещё не завершён), `dead` (срок прошёл без штатной остановки — строка выделена) или `stopped` (остановлен по сигналу своего Scheduler'а; его срок показан как `—`). Строка `dead` рядом со строкой `alive` — это watcher одной реплики Scheduler'а, который не работает, пока остальные обрабатывают подписки, или след Scheduler'а, ушедшего без штатной остановки watcher'а; живой watcher удаляет её, когда её срок прошёл и с последней отметки минули сутки. При `unconfirmed` блок поясняет, почему баннера нет. При `down` вверху страницы стоит тот же баннер, что и на главной, а записанные статусы строк `conn_id` и включённых подписок выводятся как `unknown`, с последним записанным статусом во всплывающей подсказке: пока ни один watcher не работает, их никто не обновляет; выключенная подписка сохраняет свой `disabled`. Блок читается заново при каждом запросе страницы. Неудачное чтение заменяет его уведомлением и оставляет все статусы такими, как они записаны.

**Миграция схемы.** Таблица статусов держит колонки `broker_consumer_count` и `last_reconcile_at` наравне с остальными, а таблица подписок — `absent_cycles`, счётчик промахов reconcile-цикла; одна идемпотентная миграция добавляет в конкретной БД то, чего в ней нет: она смотрит на живые таблицы и выполняет `ALTER TABLE … ADD COLUMN` только для отсутствующего. Плагин запускает её один раз при загрузке, а петля watcher'а повторяет в начале каждого цикла (единственная попытка в полёте, с растущим backoff'ом между неудачами), пока миграция не пройдёт, — так что недоступная в момент загрузки БД не оставляет процесс работать против устаревшей таблицы. Пока таблица отстаёт, читающий её блок — Connections или Subscriptions — показывает короткое уведомление вместо ошибки рендеринга страницы; пока стоит уведомление о подписках, кнопки создания подписки на странице нет: её форма пишет через то же отображение, которое прочитать не удалось.

### Гарантия доставки (immediate-режим)

Для подписок с `cooldown=0` watcher подтверждает доставку только **после** того, как `trigger_dag` создал DAG run:

- Упавший триггер NACK-ается с requeue, пишется WARNING'ом и повторяется с растущим backoff'ом (1 с, удвоение до потолка 60 с); статус подписки становится `error` с причиной, поэтому подписка, переставшая запускать DAG'и, видна на странице. Один ответ `trigger_dag` из-под этого правила выведен, и ему посвящён следующий пункт.
- **Единственный отказ триггера, о котором статус молчит:** DAG, у которого Airflow ещё не держит сериализованной версии, — `DagNotFound` из `trigger_dag`, то самое состояние, в котором только что запустившийся шедулер находится, пока DAG-процессор не разобрал файлы. Соединение, канал и регистрация consumer'а целы, и доставка ждёт остальной Airflow, поэтому строка держится в `listening` — именно этим consumer и занят, и именно этот статус оставляет подписку кандидатом проверки живости. `error`, оставленный более ранним, не связанным отказом, снимается первой же несериализованной доставкой, а не тянется через весь прогрев, поэтому и строка `conn_id`, все подписки которого прогреваются, остаётся зелёной. Сама доставка при этом возвращается в очередь и выдерживает ту же растущую паузу. Молчание ограничено: 25 таких доставок подряд — уже отказ сам по себе, и строка получает `error`, называющий отсутствующую сериализованную версию, а собственный текст Airflow `Dag id ... not found` остаётся за ним. Счётчик меряет один непрерывный прогрев — его начинают заново и переподключение, и любая доставка, которая прошла или упала по другой причине, — а каждая десятая несериализованная доставка пишется WARNING'ом. Возврат через nack в счёт `delivery-limit` quorum-очереди не идёт, поэтому доставка возвращается, пока DAG не сериализуется и ран не запустится, или пока счётчик не дойдёт до 25 и строка не получит `error`. Fire-консьюмер cooldown поступает с тем же ответом так же и держит свой статус в `listening`: собственной строки у него нет, но именно этот статус оставляет его кандидатом проверки живости, поэтому fire-консьюмер, переставший потреблять во время прогрева, будет замечен. Один такой консьюмер обслуживает все cooldown-DAG'и, поэтому его события считаются по `dag_id` только ради уровня записи в логе — каждое десятое идёт WARNING'ом. Cooldown-DAG, который не сериализуется никогда, виден поэтому только в логе шедулера.
- Дедупликация опирается на продюсера. Если у сообщения есть AMQP `message_id`, run id детерминирован (`rmq__{queue}__{message_id}`, с приведением к безопасному алфавиту и усечением с добавлением хеша, когда не влезает), поэтому редоставка попадает на уже созданный ран и подтверждается как дубль. Два *разных* сообщения с одинаковым `message_id` при этом схлопнутся в один DAG run. Без `message_id` в run id идёт timestamp, и редоставка запускает DAG повторно.
- **Граница гарантии:** DAG на паузе, неактивный или отсутствующий приводит к терминальному ACK — событие отбрасывается намеренно, с WARNING'ом в логе. NACK превратил бы поставленный на паузу DAG в накопитель редоставок.
- Сообщение, отвергнутое фильтром подписки, NACK-ается с requeue и возвращается в голову очереди — если только подписка не попросила `ack_unmatched`: тогда оно подтверждается и исчезает (см. [ADR 0002](docs/adr/0002-four-consume-loops-not-unified.md) и [Промахи фильтра и `ack_unmatched`](#промахи-фильтра-и-ack_unmatched)). Сообщение, которое фильтр не смог вычислить, возвращается в очередь в обоих случаях.

**`prefetch` намеренно не задаётся.** При умолчании `ack_unmatched=False` промахи фильтра возвращаются в голову очереди, поэтому любое конечное окно prefetch рано или поздно заполнилось бы ими целиком, и потребление встало бы навсегда — при статусе `connected`. Окно не задаётся вовсе, а не выставляется для подписок, подтверждающих свои промахи, потому что `ack_unmatched=True` сценарий не закрывает: сообщение, на котором фильтр не удалось вычислить, возвращается в очередь независимо от того, о чём просила подписка, и одного такого хватает, чтобы конечное окно со временем заполнилось. Цена решения принимается явно: число неподтверждённых доставок задаёт prefetch брокера по умолчанию, а он не ограничен, пока в RabbitMQ не задан `default_consumer_prefetch` ([consumer prefetch](https://www.rabbitmq.com/docs/consumer-prefetch#configurable-default-prefetch)), поэтому очередь с накопленным backlog'ом вычитывается в память процесса watcher'а, а старейшая доставка может упереться в `consumer_timeout` брокера (по умолчанию 30 минут), после чего RabbitMQ закрывает канал с `PRECONDITION_FAILED` и возвращает всё окно в очередь. Если на отслеживаемой очереди ожидается большой backlog, увеличьте `consumer_timeout` в конфигурации RabbitMQ. Конечный `default_consumer_prefetch` даёт watcher'у ровно то конечное окно, о котором сказано выше, поэтому на брокерах, обслуживающих watcher, его оставляют незаданным.

### Лучшие практики

- Используйте **выделенную очередь** для каждого триггера DAG (например, `orders.airflow-trigger` отдельно от `orders`). Это исключает NACK-циклы на quorum queues и интерференцию с другими консьюмерами.
- **Одна очередь — одна подписка.** Очередь, принадлежащая единственной подписке, — та конфигурация, ради которой провайдер сделан, и единственная, где `ack_unmatched=True` безопасен. Две включённые подписки на одном `queue_name` и `conn_id` конкурируют за доставки и помечаются бейджем на странице Subscriptions — см. [Промахи фильтра и `ack_unmatched`](#промахи-фильтра-и-ack_unmatched).
- Чтобы приостановить потребление сообщений без остановки DAG: **отключите подписку** в UI, а не ставьте DAG на паузу. Пауза DAG подтверждает (ack) сообщения без их обработки.
- В **HA-режиме с несколькими scheduler'ами** каждый активный scheduler запускает свой watcher-процесс. Их консьюмеры читают одни и те же очереди и конкурируют за доставки, поэтому брокер отдаёт каждое сообщение одному из них, а повторно сообщение приходит — с риском дубля, описанным ниже, — только если его доставка не подтверждена, как и при одном scheduler'е. Строки статуса подписок все реплики пишут одни и те же, поэтому строка может показывать состояние чужой реплики. Свой reconcile-цикл он тоже запускает, а счётчик промахов `dag_file`-подписки у них общий: N шедулеров тратят отсрочку в три скана в N раз быстрее, поэтому на трёх и более подписка может удалиться в пределах одного reconcile-интервала после пропажи ключа. Чего один шедулер сделать не может — отменить работу другого: каждая запись счётчика, и промах, и удаление строки, к которому счёт привёл, условна относительно прочитанного значения, поэтому шедулер, чьё значение другой уже сдвинул, не пишет ничего. Повторную доставку отсекает детерминированный `run_id`: у подписок с `cooldown > 0` он есть всегда (`rmq_cooldown__{dag_id}__{message_id}`), а в immediate-режиме — всякий раз, когда продюсер проставляет AMQP `message_id` (`rmq__{queue}__{message_id}`); в обоих случаях триггер попадает в уже созданный DAG run. Без `message_id` immediate-режим кладёт в `run_id` временную метку, и повторная доставка запускает DAG снова — вот там и нужен `max_active_runs=1` как минимальная мера защиты.

---

## Примеры DAG

Пакет включает несколько примеров DAG в `docs/example_dags/`. Все примеры используют **TaskFlow API** (декораторы `@dag` / `@task`) и демонстрируют **обработку полученных сообщений** в downstream-тасках через XCom.

| DAG | Описание |
|---|---|
| `rmq_example_basic` | Публикация, ожидание, потребление, обработка сообщений, очистка |
| `rmq_publish_advanced` | Продвинутая публикация со всеми AMQP-свойствами, пакетная отправка, topic exchange |
| `rmq_consume_with_filters` | Фильтры по заголовкам, по телу, пользовательские функции, QoS — с пошаговой обработкой сообщений |
| `rmq_sensor_deferrable` | Отложенный сенсор в pull-режиме с фильтрацией по заголовкам и обработкой сообщений |
| `rmq_sensor_push` | Отложенный сенсор в **push-режиме** — брокер доставляет сообщения мгновенно через `basic_consume` |
| `rmq_watcher_demo` | **Плагин RMQ Watcher** — DAG, запускаемый реактивно сообщениями из RabbitMQ через `@rmq_trigger`; также работает по расписанию |
| `rmq_pipeline_start` / `rmq_pipeline_finish` | Паттерн блокировки пайплайна — предотвращение параллельных запусков |
| `rmq_dlq_setup` | Настройка инфраструктуры Dead Letter Queue с DLX, TTL, exchange-to-exchange привязками |
| `rmq_watcher_orders_payments_cooldown` | Cooldown-триггер на несколько очередей — debounce всплеска сообщений в один запуск DAG |
| `rmq_watcher_jetstat_report_succeeded` | Exchange-mode триггер — подписка прямо на topic exchange (routing key вида id × status для Jetstat) |

---

## Структура репозитория

```
airflow-provider-rmq/
├── airflow_provider_rmq/
│   ├── __init__.py                  # Метаданные провайдера и get_provider_info()
│   ├── hooks/
│   │   └── rmq.py                   # RMQHook
│   ├── operators/
│   │   ├── rmq_publish.py           # RMQPublishOperator
│   │   ├── rmq_consume.py           # RMQConsumeOperator
│   │   └── rmq_management.py        # RMQQueueManagementOperator
│   ├── sensors/
│   │   └── rmq.py                   # RMQSensor
│   ├── triggers/
│   │   └── rmq.py                   # RMQTrigger
│   ├── utils/
│   │   ├── amqp.py                  # build_amqp_connection(), get_amqp_timeouts(), call_with_timeout(), FilterMissState, handle_miss(), match_and_ack()
│   │   ├── executor.py              # BoundedExecutor (пул потоков для блокирующих вызовов)
│   │   ├── filters.py               # MessageFilter, FilterVerdict
│   │   ├── management.py            # Клиент Management HTTP API (биндинги, consumer'ы очереди)
│   │   ├── metrics.py               # Счётчики Stats, no-op без statsd
│   │   └── ssl.py                   # build_ssl_context()
│   └── watcher/
│       ├── decorators.py            # @rmq_trigger
│       ├── subscription_builder.py  # build_subscriptions(), has_exchange_conflict()
│       ├── dag_file_scan.py         # статическое чтение @rmq_trigger из DAG-файлов; RMQTriggerNotStaticError
│       ├── subscription_form.py     # parse_cooldown(), parse_filter_data() (разбор формы UI)
│       ├── models.py                # RMQSubscription, RMQConnStatus, RMQWatcherHeartbeat, WatcherSession
│       ├── tunables.py              # настройки, управляющие watcher'ом, и их дефолты
│       ├── consumer.py              # RMQConsumerManager
│       ├── orphan_tracker.py        # OrphanTracker (детект орфанов cooldown- и exchange-режима)
│       ├── listener.py              # RMQWatcherListener (запуск и остановка процесса watcher'а; цикл watcher'а)
│       ├── runner.py                # Точка входа процесса watcher'а (python -c <bootstrap> airflow_provider_rmq.watcher.runner)
│       ├── liveness.py              # assess_liveness(): подтверждает ли работу хоть один процесс watcher'а
│       ├── dashboard_alert.py       # Баннер живости watcher'а на главной странице (UIAlert Airflow)
│       ├── views.py                 # RMQWatcherView (Flask-AppBuilder UI)
│       └── plugin.py                # RMQWatcherPlugin (AirflowPlugin)
├── docs/
│   ├── adr/                         # Записи об архитектурных решениях
│   └── example_dags/                # Примеры DAG
├── tests/                           # Модульные тесты
├── integration/                     # Тесты против живого брокера (по требованию, вне testpaths)
├── CHANGELOG.md
├── pyproject.toml
└── readme.md
```

---

## Запуск тестов

```bash
# Установка dev-зависимостей
pip install -e ".[dev]"

# Запуск всех тестов
pytest tests/

# Запуск конкретного модуля тестов
pytest tests/test_trigger.py -v
```

```bash
# Интеграционные тесты против живого брокера (по требованию)
pytest integration/ -q
```

Каталог `integration/` лежит вне `testpaths`, поэтому ни один обычный прогон — `pytest`, `pytest tests/` или CI — на брокер не уходит. Он запускается только явным указанием каталога, целиком скипается без `RMQ_TEST_URL` и отказывается работать, если в URL не указан отдельный vhost — умолчательный `/` не годится. Всё созданное несёт префикс прогона `rmqtest.{pid}.{uuid8}.` и удаляется в teardown. Переменные, фикстуры и правила безопасности — в [integration/README.md](integration/README.md).

---

## Лицензия

Apache License 2.0. См. [LICENSE](LICENSE) для подробностей.
