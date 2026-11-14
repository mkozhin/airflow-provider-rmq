---
worth: yes
where: airflow_provider_rmq/watcher/models.py:224
added: 2026-09-02
---
# UI-подписка с ключом декоратора молча переходит скану и удаляется вместе с ним

**Симптом.** `upsert_subscription` (`airflow_provider_rmq/watcher/models.py:190`)
ищет строку по ключу `(dag_id, queue_name, conn_id)` без учёта `source` и
безусловно пишет `sub.source = source` вместе с `filter_data`, `cooldown`,
`group_key` и `ack_unmatched` (`models.py:223-228`). Два пути заходят в него с
разными `source`, и каждый перезаписывает результат другого. Docstring
`_sync_to_db` при этом утверждает «Never touches ui-sourced subscriptions»
(`airflow_provider_rmq/watcher/listener.py:1565`), что для UI-строки с ключом
декоратора неверно.

**Трасса 1, скан забирает UI-строку.** Оператор создал подписку на странице
Subscriptions: DAG `orders`, очередь `orders.in`, фильтр по заголовку.
Позже в DAG-файл добавляют `@rmq_trigger(queue="orders.in")` без фильтра.
Следующий цикл `_sync_to_db` (`listener.py:1696`) делает upsert с
`source="dag_file"`: строка та же, `source` становится `dag_file`, фильтр
заменяется пустым. С этого цикла подписка пропускает всё, а на странице стала
read-only. Через месяц декоратор убирают — и `_sync_to_db` удаляет строку как
исчезнувшую из кода на четвёртом доверенном скане подряд без её ключа
(`listener.py:1630`). Подписка, которую оператор создал руками, исчезает
через четыре цикла, и в логе о ней сказано как о `dag_file`-строке.

Тем же upsert'ом скан пишет в строку `ack_unmatched` из декоратора
(`listener.py:1704`). UI-строка с включённым `ack_unmatched` под декоратором без
него начинает возвращать отвергнутые фильтром сообщения в очередь; декоратор с
`ack_unmatched=True` поверх UI-строки без него начинает подтверждать и терять их.

**Трасса 2, UI переписывает dag_file-строку, а скан возвращает.** Подписка
живёт в коде. Оператор нажимает Create на странице с тем же `dag_id`,
очередью и `conn_id`, но с другим фильтром: `create` (`views.py:451`, upsert на 497)
через `upsert_subscription` находит dag_file-строку, ставит `source="ui"` и новый
фильтр. Через 60 секунд скан возвращает `source="dag_file"` и старый фильтр.
Правка «прижилась» на минуту и откатилась. Никакого сообщения оператор не получает:
форма ответила «Subscription created».

`edit_group` (`views.py:716`) приходит к тому же: для группы без `dag_file`-строк
он удаляет её строки и делает `flush` (`views.py:806-807`), затем пишет каждую
очередь через `upsert_subscription` с `source="ui"` (`views.py:811`), и новый ключ,
совпавший с `dag_file`-строкой, забирает эту строку. `edit` (`views.py:525`) идёт
мимо `upsert_subscription` и правит ORM-объект напрямую (`views.py:584-586`):
смена ключа UI-строки на ключ существующей `dag_file`-строки упирается в
`uq_rmq_sub_dag_queue_conn` на `commit` (`views.py:597`), и необработанный
`IntegrityError` отдаёт ошибку вместо понятного отказа. Захвата строки там нет, но
и объяснения оператору нет. Защита на уровне модели эти вью не закроет, нужна
проверка ключа в самих вью.

**Почему это важно.** Обе стороны тихие. Первая теряет конфигурацию оператора
и в итоге саму подписку, вторая создаёт видимость, что правка сделана. Правило
«код — источник истины» само по себе верное, но оно должно быть объявлено, а
не молча исполнено.

**Направление решения.** Описано как задачи 6 и 7 плана
`docs/plans/20260703-reliability-hardening.md` (см.
[[reliability-hardening-plan-stale]]):

- `upsert_subscription` отказывает `ValueError` попытке `ui`-upsert поверх
  `dag_file`-строки; направление `ui → dag_file` разрешено;
- `create`, `edit`, `edit_group` проверяют коллизию нового ключа с
  `dag_file`-строкой до записи и отвечают flash-ошибкой «this subscription is
  managed by @rmq_trigger in the DAG file»; в `edit_group` проверка обязана
  стоять до `delete`+`flush`;
- `_sync_to_db` при поглощении `ui`-строки пишет WARNING «taking over UI
  subscription» один раз, а не на каждом цикле, и его docstring называет это
  поглощение вместо «Never touches ui-sourced subscriptions».

Тесты: оба направления upsert; `edit_group` при коллизии не удаляет группу и не
коммитит; `edit` при коллизии отвечает flash-ошибкой без `IntegrityError`;
повторный скан без повторного WARNING.
