---
worth: yes
where: airflow_provider_rmq/sensors/rmq.py:47
added: 2026-09-02
---
# `RMQSensor.mode` затеняет `mode` базового сенсора Airflow

**Симптом.** `BaseSensorOperator` принимает `mode` со значениями `poke` и
`reschedule`, проверяет его в `__init__` и читает свойством `reschedule`
(`airflow/sensors/base.py:166, 225-233, 422-424`). `RMQSensor`
(`airflow_provider_rmq/sensors/rmq.py`) объявляет параметр с тем же именем и
значениями `pull`/`push` — режим доставки для deferrable-триггера — и после
`super().__init__()` пишет `self.mode = mode` (`rmq.py:75`), затирая значение
базового класса.

Проверено на Airflow 2.11.2:

```
RMQSensor(task_id="a", queue_name="q").mode            → "pull", reschedule False
RMQSensor(..., mode="reschedule").mode                 → "reschedule", reschedule True
RMQSensor(..., deferrable=True, mode="reschedule").mode → "reschedule"  (уезжает в триггер)
RMQSensor(..., mode="bogus")                            → принимается без ошибки
```

Проверка базового класса проходит по умолчанию `poke` и после этого
перезаписывается, поэтому любое значение принимается молча. Работает всё по
стечению обстоятельств: `pull` и `push` не равны `reschedule`, значит сенсор
идёт по poke-ветке, и это совпадает с задуманным.

**Где это ломается.**

- `mode="reschedule"` вместе с `deferrable=True` передаёт `reschedule` в
  `RMQTrigger`, тот не находит `push` и молча работает в pull-режиме.
- `SequentialExecutor` и `DebugExecutor` сами переписывают режим сенсора:
  `task.mode = "reschedule"` (`base.py:412-418`). Сенсор с `mode="push"` на
  таких executor'ах — в тестах, в `airflow standalone` — тихо становится
  pull-режимом.
- Пример в README для poke-режима использует `mode="reschedule"` как параметр
  базового класса, а таблица параметров того же README описывает `mode` как
  `pull`/`push`. Один параметр с двумя значениями в одном документе.
- В сериализованном DAG'е и в UI поле `mode` показывает `pull`, что для
  читающего «режим сенсора Airflow» выглядит как повреждённое значение.

**Почему это важно.** Публичная сигнатура оператора расходится с базовым
классом, на который она опирается, и это невозможно исправить без изменения
API. Чем дольше живёт, тем дороже.

**Направление решения.** Переименовать в `delivery_mode` (или `trigger_mode`),
оставив `mode` базовому классу. Период совместимости: если пришёл `mode` со
значением `pull` или `push` — `DeprecationWarning`, перекладывание в
`delivery_mode`, и в базовый класс уходит `poke`. Явно проверять допустимые
значения обоих параметров. Тесты на все четыре комбинации и на
`SequentialExecutor`. Запись в CHANGELOG как поведенческое изменение.

**Побочный эффект переименования: сенсор теряет видимость промаха.** `RMQSensor`
держит `FilterMissState` полем экземпляра, и порог в сто промахов подряд берётся
только потому, что `BaseSensorOperator.execute` зовёт `poke` на одном и том же
объекте. В режиме `reschedule` `AirflowRescheduleException` завершает
таск-инстанс, и следующий `poke` идёт на новом объекте: обнуляются **оба**
счётчика — и `streak`, и `dropped`. Порога по батчу у сенсора нет
(`on_batch_done` зовёт только оператор), поэтому при `poke_batch_size < 100`
строка порога не выходит вовсе. С `ack_unmatched=True` это значит удаление
сообщений без единой строки в логе.

Сегодня `reschedule` достижим только через executor
(`SequentialExecutor`, `DebugExecutor`), а переименование сделает его штатно
доступным пользователю. Поэтому вместе с переименованием нужен ответ на
видимость: состояние, переживающее таск-инстанс, либо порог, не опирающийся на
время жизни объекта.
