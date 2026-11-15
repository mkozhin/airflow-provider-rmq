---
worth: yes
where: docs/plans/20260703-reliability-hardening.md:211
added: 2026-09-02
---
# План reliability-hardening наполовину выполнен другими планами и не отражает этого

**Контекст.** `docs/plans/20260703-reliability-hardening.md` — 16 задач по
итогам ревью 2026-07-03. В его чек-листе (`## Implementation Steps`, с :211)
116 пунктов, и ни один не отмечен выполненным. Часть задач закрыта планами
connection-resilience, watcher-audit-fixes, status-truthfulness,
watcher-child-process и publish-confirms-and-hook-extras иначе, чем он
описывает. Исполнять его как есть нельзя: половина шагов переделает уже
сделанное, другая половина ссылается на строки, имена и устройство watcher'а,
которых нет.

Главное расхождение — устройство watcher'а. Задачи 11 и 13 (:366-379, :396-412)
описывают петлю как поток внутри шедулера: `before_stopping` выставляет
`threading.Event`, будит loop через `call_soon_threadsafe` и делает
`thread.join(timeout=5)`; лидер-лок встраивается в `listener._main` и
передаётся при graceful stop. Watcher же — дочерний процесс шедулера
(ADR-0008): `before_stopping` вызывает `_stop_child()`
(`watcher/listener.py:554-555`), а `_run_loop`/`_main` (`listener.py:776`,
`:799`) исполняются в потоке `rmq-watcher` внутри этого процесса
(`watcher/runner.py:469`). Шаги задачи 11 к этому не применимы, а дизайн
лидер-лока должен исходить из того, что лок держит процесс watcher'а: по одному
на каждую реплику шедулера, с его собственной остановкой по SIGTERM и гибелью
вместе с родителем, после которой соединение с арбитром рвёт брокер. Кроме
того, план создаёт `docs/adr/0006-leader-lock-via-rmq-exclusive-queue.md`, а
номер 0006 занят `0006-badge-dag-lookup-not-unified-with-sync-trigger.md`.

Сверка задач с кодом:

| задача | состояние |
|---|---|
| 1. decode `errors="replace"` | сделано в триггере (`triggers/rmq.py:119`) и в watcher'е (`watcher/consumer.py:3762`, `utils/amqp.py:306`); **не** сделано в `RMQHook.consume_messages` (`hooks/rmq.py:712`) — это изменение решений ACK/NACK, разбирается в [[sync-decode-raises-on-binary-body]] |
| 2. `queue_info` с `consumer_count`, guard в `__del__`, warning на пустой publish | сделано планом `docs/plans/completed/20261115-publish-confirms-and-hook-extras.md` (`hooks/rmq.py:513-521`, `hooks/rmq.py:309-319`, `operators/rmq_publish.py:130-136`) |
| 3–4. publisher confirms в хуке и операторе | сделано планом `docs/plans/completed/20261115-publish-confirms-and-hook-extras.md`: `confirm_delivery()` в `get_channel()` (`hooks/rmq.py:284-285`), `basic_publish(..., mandatory)` (`hooks/rmq.py:630`), `confirm`/`mandatory` и `AirflowException` с номером сообщения в операторе (`operators/rmq_publish.py:117-210`) |
| 5. ленивый `WatcherSession` | не сделано, фабрика создаётся при импорте (`watcher/models.py:96`) |
| 6–7. защита `source` и takeover-warning | не сделано → [[ui-subscription-taken-over-by-scan]] |
| 8. at-least-once, `DagRunAlreadyExists`, детерминированный `run_id` | сделано (`_build_run_id` и `_sync_trigger`, `watcher/consumer.py:249-305`) |
| 9. валидация cooldown `conn_id` | частично: ERROR при разъезде есть (`_report_split_cooldown`, `watcher/consumer.py:1663`), исключения лишних подписок из активного набора и `error` в их строках нет |
| 10. вынос I/O и БД в executor | сделано, двумя пулами `BoundedExecutor` (`watcher/listener.py:750-754`) |
| 11. graceful stop, диагностика `on_starting` | сделано в форме дочернего процесса (`_stop_child`, `_waker`, `watcher/runner.py`); шаги плана описывают поток |
| 12–13. лидер-лок через exclusive-очередь | не сделано; на него ссылается [[status-cache-stale-across-replicas]] как на одно из двух направлений |
| 14. statsd-метрики | сделано в другом составе: `dag_triggered`, `consumer_attached`, `consumer_restarted`, `cycle_completed`, `cycle_timed_out`, `cycle_step_timed_out`, `cycle_step_skipped`, `tasks_abandoned`, `filter_missed`, `filter_errored`, `sync_superseded`; нет `consumer_reconnect`, `orphan_detected` |
| 15–16. приёмка, документация | привязаны к остальному |

**Почему это важно.** План — единственное место, где лидер-лок описан с
проработанным дизайном (арбитр, не-robust соединение, поведение при
недоступности арбитра). Пока он лежит в `docs/plans/` как невыполненный, он
выглядит очередью работы, которой на две трети нет, и одновременно прячет
действительно нужный дизайн внутри устаревшего списка, часть которого
описывает watcher-поток, а не watcher-процесс.

**Направление решения.** Не исполнять, а разобрать:

- задача 1 — в [[sync-decode-raises-on-binary-body]];
- задачи 2–4 сделаны планом
  `docs/plans/completed/20261115-publish-confirms-and-hook-extras.md`;
- задача 5 (ленивый `WatcherSession`) открыта — кандидат на маленький план
  «мелочи хука и моделей»;
- задачи 6–7 — уже вынесены в [[ui-subscription-taken-over-by-scan]], план на
  них пишется по этому пункту;
- задачи 12–13 — отдельный план по лидер-локу, написанный под дочерний процесс
  watcher'а, со свободным номером ADR и с решением, стоит ли он дороже второго
  направления из [[status-cache-stale-across-replicas]] (версионированная
  запись строки), и с учётом того, что Airflow 3 даёт распределение триггеров
  между triggerer'ами бесплатно ([[airflow-3-migration-path]]);
- задача 9 — дописать в план лидер-лока или закрыть отдельным пунктом;
- сам файл после этого — в `completed/` с пометкой «разобран», чтобы дизайн
  лидер-лока остался доступен по ссылке.
