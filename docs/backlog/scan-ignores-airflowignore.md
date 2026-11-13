---
worth: later
where: airflow_provider_rmq/watcher/listener.py:1783
added: 2026-09-02
---
# Скан подписок читает файлы, которые сам Airflow не загружает

**Контекст.** `_scan_subscriptions` (`airflow_provider_rmq/watcher/listener.py:1758`)
берёт папку из `airflow.settings.DAGS_FOLDER` (`_get_dags_folder`, `listener.py:1721`)
— того же источника, что и шедулер, — а список файлов в ней собирает через
`glob.glob(os.path.join(dags_folder, "**", "*.py"), recursive=True)`
(`listener.py:1782-1784`). Airflow для того же использует
`airflow.utils.file.list_py_file_paths`, и у него
другой набор: он учитывает `.airflowignore` в каждой директории (синтаксис по
`[core] dag_ignore_file_syntax`, regexp или glob), читает `.zip`-пакеты и в
`safe_mode` пропускает файлы без слов `dag` и `airflow` внутри. Папка у обоих
одна, расходится только набор файлов в ней:

- файл в директории с `.airflowignore` — Airflow его не парсит, DAG'а из него
  не существует, а скан его читает и регистрирует подписку;
- `.zip` с DAG'ами — Airflow загружает, скан не видит; подписка из декоратора
  внутри архива никогда не появится, и в логе об этом ничего.

**Трасса для первого случая.** В репозитории DAG'ов лежит `tests/` с копией
DAG-файла для юнит-тестов и `.airflowignore` с `tests/`. Копия несёт тот же
`@rmq_trigger(queue="orders.in")`, но `dag_id` в ней `test_orders`. Скан
регистрирует вторую подписку на ту же очередь. Её consumer забирает половину
сообщений (брокер раздаёт round-robin), `_sync_trigger`
(`airflow_provider_rmq/watcher/consumer.py:261`) не находит DAG `test_orders`,
отвечает `skipped` (`consumer.py:285-290`), и сообщение подтверждается — событие
потеряно. На странице у строки будет бейдж `dag not found`, но потеря уже
идёт, по одному сообщению из двух.

**Чего не хватает для решения.** Неизвестно, держит ли кто-то реальные файлы
с `@rmq_trigger` в игнорируемых директориях или в zip-пакетах. Если нет —
достаточно документировать ограничение. Если да — заменить `glob` на
`list_py_file_paths(dags_folder, safe_mode=False, include_examples=False)`:
одна строка, тот же набор файлов, что у шедулера, zip включительно (`ast` их
читать не умеет, для архивов нужен `zipfile` + `ast.parse` по каждому
`.py` внутри).
