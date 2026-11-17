---
worth: yes
where: airflow_provider_rmq/watcher/consumer.py:2152
added: 2026-08-29
---
# Две одинаковые ветки `_provision_one_exchange_sub` возвращают разное

**Симптом.** `RMQConsumerManager._provision_one_exchange_sub`
(`airflow_provider_rmq/watcher/consumer.py:2117`) объявляет sub-очередь
exchange-подписки (`_ensure_sub_queue`, `consumer.py:2138`) и затем синхронизирует
её биндинги. Обе предпосылки bind-diff проверяются подряд и описывают одну
ситуацию — Management API спросить нечем:

```python
if management_url is None:                                   # consumer.py:2143
    log.error("management_url not set on conn_id=%r — skipping bind-diff "
              "for DAG %r (queue %s%s still declared, will retry next cycle)", ...)
    return True                                              # consumer.py:2150

if conn_info.login is None or conn_info.password is None:     # consumer.py:2152
    log.error("conn_id=%r has no login/password set — skipping bind-diff "
              "for DAG %r (queue %s%s still declared, will retry next cycle)", ...)
    return False                                             # consumer.py:2159
```

Тексты почти совпадают, и оба верны: `_ensure_sub_queue` отработал выше, очередь
объявлена, bind-diff пропущен до следующего цикла. Возвраты противоположны.

Возврат — не отчёт об успехе, он управляет трекером орфанов у вызывающего:

```python
if provisioned:
    self._exchange_tracker.mark_provisioned({dag_id})        # consumer.py:2297-2298
```

**Трасса.** У `conn_id` задан `management_url`, но в Airflow Connection нет логина и
пароля. Очередь `rmq_watcher.sub.{dag_id}` создана, а `dag_id` **не попадает** в
`_exchange_tracker`. Позже подписку убирают из DAG-файла:
`_check_orphaned_exchange_bindings` (`consumer.py:1751`) не находит `dag_id` среди
провиженных, и WARNING про осиротевшую очередь не выдаётся — хотя очередь на брокере
объявлена и остаётся там с биндингами.

Это ровно тот единственный сигнал, ради которого трекер существует: ADR-0005
(`docs/adr/0005-exchange-orphan-no-auto-unbind.md`) отказывается снимать биндинги
автоматически и оставляет оператору предупреждение с готовой командой очистки.
Третья ветка того же метода — отказ самого Management API (`consumer.py:2167-2174`) —
отвечает `True`, то есть верный ответ для «bind-diff не сделан, очередь есть» здесь
уже написан дважды из трёх.

**Почему это важно.** Отказ тихий и накопительный: очередь с биндингами живёт на
брокере, TTL 8 часов (`_EXCHANGE_TTL_MS`, `consumer.py:209`) удаляет из неё
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

Возврат `False` закреплён тестами:
`test_missing_login_logs_error_and_returns_false_without_raising` и
`test_missing_password_logs_error_and_returns_false_without_raising`
(`tests/watcher/test_consumer.py:3375`, `:3400`) проверяют, что `dag_id` **не**
попадает в `_exchange_tracker._provisioned` (`:3397`, `:3421`). Исправление
переворачивает эти утверждения вместе с кодом, иначе тесты его откатят.

Смежное по той же функции: [[exchange-change-leaves-old-bindings]].

Ещё два пункта про ту же инфраструктуру, найденные аудитом 2026-09-02:
[[exchange-declared-not-passive]] — активный declare чужого exchange'а даёт
`PRECONDITION_FAILED` навсегда; [[exchange-log-queue-unbounded]] — очередь
`.log` без `x-max-length`.
