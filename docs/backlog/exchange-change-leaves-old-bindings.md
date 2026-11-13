---
worth: yes
where: airflow_provider_rmq/utils/management.py:49
added: 2026-08-30
---
# Смена exchange в декораторе оставляет старые биндинги

**Симптом.** `_provision_one_exchange_sub` (`airflow_provider_rmq/watcher/consumer.py:2131`)
сверяет биндинги через `get_current_bindings(..., exchange, ...)` (`consumer.py:2178`),
где `exchange` — тот, что назван в декораторе сейчас. Ответ Management API
фильтруется по нему (`b.get("source") == exchange`,
`airflow_provider_rmq/utils/management.py:49`), а `_sync_bindings`
(`consumer.py:3908`) снимает биндинги только с переданного exchange'а. Поэтому в
сверку попадают только биндинги нового exchange'а: недостающие добавляются,
лишние на **прежнем** exchange не видны и не удаляются. Прежний exchange нигде не
хранится — метаданные exchange-подписки берутся из текущего скана
(`exchange_meta`, `listener.py:878`).

Трасса. DAG в exchange-режиме переключают с `jetstat` на `jetstat_v2`. Имя
очереди — `rmq_watcher.sub.{dag_id}` — не меняется, orphan-трекинг ключуется по
`dag_id`, так что и он ничего не замечает. Биндинг `jetstat → rmq_watcher.sub.{dag_id}`
остаётся в брокере, и сообщения, публикуемые в старый exchange, продолжают
доходить до очереди и запускать DAG — сколько угодно долго, пока биндинг не
снимут руками.

**Почему это важно.** DAG запускается по событиям источника, от которого его
сознательно отписали. Со стороны UI и логов всё выглядит штатно: подписка
`listening`, биндинги нового exchange'а на месте.

**Куда копать.** Помнить предыдущий сконфигурированный exchange (в строке
подписки или в отдельной таблице) и снимать его биндинги при смене, либо
сверять биндинги очереди по всем source-exchange'ам, которые Management API
отдаёт для неё, а не только по текущему.

Смежное по той же функции: [[exchange-provision-branches-disagree]].

Смежное по объявлению exchange'а: [[exchange-declared-not-passive]].
