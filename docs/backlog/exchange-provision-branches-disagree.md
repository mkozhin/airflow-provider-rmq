---
worth: yes
where: airflow_provider_rmq/watcher/consumer.py:2111
added: 2026-08-29
---
# Две одинаковые ветки `_provision_one_exchange_sub` возвращают разное

**Симптом.** `RMQConsumerManager._provision_one_exchange_sub`
(`airflow_provider_rmq/watcher/consumer.py:2076`) объявляет sub-очередь
exchange-подписки (`_ensure_sub_queue`, `consumer.py:2097`) и затем синхронизирует
её биндинги. Обе предпосылки bind-diff проверяются подряд и описывают одну
ситуацию — Management API спросить нечем:

```python
if management_url is None:                                   # consumer.py:2102
    log.error("management_url not set on conn_id=%r — skipping bind-diff "
              "for DAG %r (queue %s%s still declared, will retry next cycle)", ...)
    return True                                              # consumer.py:2109

if conn_info.login is None or conn_info.password is None:     # consumer.py:2111
    log.error("conn_id=%r has no login/password set — skipping bind-diff "
              "for DAG %r (queue %s%s still declared, will retry next cycle)", ...)
    return False                                             # consumer.py:2118
```

Тексты почти совпадают, и оба верны: `_ensure_sub_queue` отработал выше, очередь
объявлена, bind-diff пропущен до следующего цикла. Возвраты противоположны.

Возврат — не отчёт об успехе, он управляет трекером орфанов у вызывающего:

```python
if provisioned:
    self._exchange_tracker.mark_provisioned({dag_id})        # consumer.py:2256-2257
```

**Трасса.** У `conn_id` задан `management_url`, но в Airflow Connection нет логина и
пароля. Очередь `rmq_watcher.sub.{dag_id}` создана, а `dag_id` **не попадает** в
`_exchange_tracker`. Позже подписку убирают из DAG-файла:
`_check_orphaned_exchange_bindings` (`consumer.py:1710`) не находит `dag_id` среди
провиженных, и WARNING про осиротевшую очередь не выдаётся — хотя очередь на брокере
объявлена и остаётся там с биндингами.

Это ровно тот единственный сигнал, ради которого трекер существует: ADR-0005
(`docs/adr/0005-exchange-orphan-no-auto-unbind.md`) отказывается снимать биндинги
автоматически и оставляет оператору предупреждение с готовой командой очистки.
Третья ветка того же метода — отказ самого Management API (`consumer.py:2126-2133`) —
отвечает `True`, то есть верный ответ для «bind-diff не сделан, очередь есть» здесь
уже написан дважды из трёх.

**Почему это важно.** Отказ тихий и накопительный: очередь с биндингами живёт на
брокере, TTL 8 часов (`_EXCHANGE_TTL_MS`, `consumer.py:190`) удаляет из неё
сообщения, но не её саму, а единственное уведомление оператору подавлено.
Конфигурация, при которой это происходит — `management_url` в `extra` есть,
логин/пароль на подключении не заданы, — не экзотическая: её даёт любое подключение,
собранное «по частям».

**Направление решения (не проработано).** Свести обе ветки к одному ответу `True` —
очередь объявлена, и это всё, что `mark_provisioned` означает; несинхронизированные
биндинги починятся на следующем цикле, как и в ветке без `management_url`. Заодно
стоит перечитать сам контракт возврата: docstring метода описывает его как «следует
ли пометить `dag_id` провиженным», и три ветки из трёх, где bind-diff пропущен,
должны отвечать одинаково. Проверить, нет ли других ранних выходов после
`_ensure_sub_queue`. Тест — подписка на conn_id без учётных данных, затем её
удаление, и ожидаемый WARNING про осиротевшую очередь.

Смежное по той же функции: [[exchange-change-leaves-old-bindings]].
