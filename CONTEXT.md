# RMQ Watcher

Airflow-провайдер, который реактивно запускает DAG run'ы по сообщениям из RabbitMQ. Фоновый
процесс (**Watcher**) внутри Scheduler читает декларации `@rmq_trigger(...)` из DAG-файлов,
синхронизирует их с БД Airflow и держит живые consumer-задачи на RabbitMQ.

## Language

**Subscription**:
Связь "одна очередь → один DAG" с фильтром сообщений и опциональным cooldown. Единица учёта в
таблице `rmq_watcher_subscriptions`; один consumer-таск на подписку.
_Avoid_: trigger, binding (binding — это другое, см. ниже)

**Queue-mode trigger**:
`@rmq_trigger(queue=...)` / `queues=[...]` — режим подписки, где очередь заранее вручную создаёт
и биндит оператор; провайдер только читает из неё (`passive`-declare).
_Avoid_: classic mode, manual mode

**Exchange-mode trigger**:
`@rmq_trigger(exchange=..., routing_keys=... | routing_key_ids=...)` — режим подписки, где
exchange, выделенная очередь на DAG и все биндинги между ними декларирует и поддерживает сам
провайдер, а не оператор.
_Avoid_: topic mode, auto mode

**Routing key**:
AMQP topic-ключ (точечно-сегментированная строка), по которому exchange-mode подписка
биндится на exchange. Желаемый набор полностью вычисляется из декоратора на каждом
reconcile-цикле.
_Avoid_: route, key (без уточнения)

**Binding**:
Связь между exchange и очередью по конкретному routing key/паттерну. Источник истины для
exchange-mode подписок — само RabbitMQ (через Management API), не БД Airflow.
_Avoid_: subscription (см. выше — это разные понятия)

**Bind-diff**:
Сверка желаемого набора routing key (из декоратора) с текущими живыми биндингами очереди
(через Management HTTP API), выполняемая каждый reconcile-цикл; разница применяется обычным
AMQP `bind`/`unbind`.

**Sub queue**:
`rmq_watcher.sub.{dag_id}` — общая очередь на DAG для exchange-mode подписок, провижинится
провайдером, активно потребляется (в отличие от pending queue).
_Avoid_: trigger queue

**Pending queue**:
`rmq_watcher.pending.{dag_id}` — таймер cooldown-механизма (`x-max-length=1` + TTL + DLX в
`rmq_watcher.fire`); не имеет ни одного consumer'а, существует только чтобы дождаться TTL и
передать сигнал дальше.

**Orphaned subscription**:
DAG_id, чьи RMQ-артефакты (pending queue или sub queue + биндинги) продолжают существовать в
RabbitMQ после того, как соответствующая подписка исчезла из БД (DAG удалён/переименован, или
файл временно не парсится). Watcher не удаляет и не отвязывает такие артефакты автоматически —
только логирует WARNING с подсказкой на ручную очистку.

**Publish connection**:
Второе AMQP-соединение на каждый `conn_id`, живущее в пуле под ролью `publish`: по нему
публикуются cooldown-плейсхолдеры. Разведено с потребляющим соединением намеренно — под
resource alarm брокер перестаёт читать сокет публикующего соединения, и на общем соединении это
остановило бы `basic.ack` всех сидящих на нём потребителей.
_Avoid_: отдельное подключение Airflow, отдельный `conn_id` (запись Connection одна и та же,
второе только соединение к брокеру)

**Liveness watchdog**:
Проверка каждого reconcile-цикла, зарегистрирован ли ещё наш consumer у брокера: поиск
собственного `consumer_tag` подписки в ответе Management API, а без `management_url` — passive
`queue_declare` по соединению. Два подряд отрицательных вердикта отменяют только те подписки,
чей тег брокер не подтвердил, и сбрасывают общее соединение `conn_id`: подтверждённые соседи по
этому соединению не отменяются и не перезапускаются, они переподключаются собственным
retry-циклом. Недоступность Management API — это «нет данных», а не вердикт. Судит о живости
именно брокер: незавершённая consumer-таска не доказывает ничего.
_Avoid_: healthcheck, ping (проверяется регистрация нашего consumer'а, а не доступность хоста)

**Degraded**:
Статус `conn_id`, означающий, что проверка живости вынесла отрицательный вердикт, но
ограничитель частоты пересозданий не дал сбросить соединение.
_Avoid_: error (`error` — вердикт, по которому действие выполнено; `degraded` — вердикт, по
которому действие сознательно не выполнено)

**Cycle timeout**:
Бюджет времени на одну итерацию цикла watcher'а — `max(reconcile_interval × 3, 300)` секунд,
переопределяется Airflow-переменной `rmq_watcher_cycle_timeout`. Его превышение пересоздаёт
event loop целиком и потому останавливает потребление по всем `conn_id`: это верхний слой
защиты от зависаний, а точечные ловятся таймаутами отдельных AMQP-вызовов.
_Avoid_: reconcile interval (это другое — период между циклами, а не бюджет одного)

## Relationships

- Один **DAG** может иметь несколько **Subscription** (стек `@rmq_trigger`), каждая — свой
  consumer-таск.
- **Subscription** — это ровно **Queue-mode trigger** ИЛИ **Exchange-mode trigger**, не оба
  одновременно в рамках одного вызова декоратора.
- **Exchange-mode trigger** определяет один **Routing key** набор → разворачивается в один
  или несколько **Binding** между exchange и **Sub queue**.
- **Bind-diff** поддерживает **Binding** в соответствии с декоратором; не трогает саму
  **Subscription** в БД.
- **Liveness watchdog** судит о конкретной **Subscription** и лечит одно соединение; **Cycle
  timeout** судит о цикле целиком и пересоздаёт event loop, то есть задевает все подписки
  сразу — поэтому его бюджет намеренно щедрый.
- Несколько **Exchange-mode trigger** на одном DAG запрещены (одна **Sub queue** на dag_id) —
  валидируется как ошибка, а не молча игнорируется.

## Example dialogue

> **Dev:** "Если я уберу `@rmq_trigger(exchange=...)` из DAG-файла, **Binding** между exchange
> и **Sub queue** сразу пропадёт?"
> **Maintainer:** "Нет — **Subscription** в БД удалится и consumer-таск остановится, но
> **Binding** останется. Это станет **Orphaned subscription**: просто WARNING в логе и TTL на
> **Sub queue**, которая со временем сама очистится. Анбиндить сразу нельзя — иначе временная
> синтаксическая ошибка при сохранении файла рвала бы живой маршрут трафика."

## Flagged ambiguities

- "Подписка" в разговоре иногда используется и для **Subscription** (строка в БД), и для
  **Binding** (связь exchange↔очередь) — это разные сущности с разным источником истины
  (БД Airflow vs RabbitMQ). При неоднозначности уточнять явно.
