---
worth: later
where: airflow_provider_rmq/utils/metrics.py:8
added: 2026-11-17
---
# У watcher'а нет счётчиков на отказ `trigger_dag`, переподключения и сироты

Watcher отдаёт StatsD-счётчики цикла, консьюмеров и фильтров (`incr`, `utils/metrics.py`).
Не считаются:

- отказ `trigger_dag` и отказ публикации в pending-очередь — сейчас они видны только в
  строке подписки в UI и в логе;
- переподключения консьюмера отдельно от первого подключения (`consumer_attached` считает
  оба);
- найденные сироты (`orphan_detected` из задачи 14 плана
  `docs/plans/completed/20260703-reliability-hardening.md`).

Значение не решено: счётчики полезны, только когда метрики Airflow собираются и на них
есть правила, а на проде их пока нет ([[watcher-child-death-unnoticed]]).
