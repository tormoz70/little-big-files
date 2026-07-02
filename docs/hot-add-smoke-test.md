# Hot-add Dev Smoke Test Report

Этот документ фиксирует runbook, проверки и фактические результаты smoke-теста hot-add.

## Environment

- Base compose: `deploy/docker-compose.local.yml`
- Smoke override: `deploy/docker-compose.hotadd-smoke.yml`
- Empty bootstrap: `deploy/shards.empty.json`
- Coordinator seal threshold (smoke): `SHARD_MAX_BYTES=1048576` (1 MiB)

## Runbook

1. Очистить стенд:
   - `docker compose -f deploy/docker-compose.local.yml -f deploy/docker-compose.hotadd-smoke.yml down -v`
2. Поднять coordinator без shard-ов:
   - `docker compose -f deploy/docker-compose.local.yml -f deploy/docker-compose.hotadd-smoke.yml up -d coordinator-db`
   - `docker compose -f deploy/docker-compose.local.yml -f deploy/docker-compose.hotadd-smoke.yml up -d --no-deps coordinator`
3. Проверить пустой registry и fail-closed write:
   - `GET /v1/admin/shards` -> `[]`
   - `POST /v1/packages` -> `503 active_shard_unavailable`
4. Поднять shard-0 и проверить регистрацию:
   - `docker compose ... up -d shard-0-db shard-0-primary`
   - shard регистрируется как `standby`
5. Запустить подачу данных:
   - `python clients/python/fill_shards.py --wait --target-mb ...`
6. Через 10 минут от начала подачи поднять shard-1:
   - `docker compose ... up -d shard-1-db shard-1-primary`
7. Дождаться ротации:
   - shard-0 -> `sealed`
   - shard-1 -> `active`
8. Дождаться исчерпания shard-1 при отсутствии standby (без ручного `PATCH`):
   - coordinator автоматически переводит shard-1 в `sealed` (fail-closed);
   - `POST /v1/packages` -> `503 active_shard_unavailable`
9. Поднять shard-2:
   - `docker compose ... up -d shard-2-db shard-2-primary`
   - shard-2 регистрируется как `standby`, затем auto-promote в `active`
10. Проверить восстановление write path:
   - `POST /v1/packages` -> `201`
11. Остановить стенд и очистить volumes:
   - `docker compose ... down -v`

## Assertions

- `GET /v1/admin/shards` отображает ожидаемые состояния (`standby`/`active`/`sealed`) на каждом этапе.
- При отсутствии `active` и `standby` Coordinator возвращает `503 active_shard_unavailable`.
- При появлении reachable `standby` Coordinator auto-promote его в `active` (без startup activation).
- При превышении порога у `active` и отсутствии `standby` coordinator автоматически seal-ит active (fail-closed); writes возвращают `503` до hot-add нового standby.
- После запуска shard-2 write path восстанавливается (`201 Created` на POST).

## Execution Log

### Execution: 2026-07-02

1. **Reset + coordinator-only start**
   - `down -v` выполнен.
   - Запущены `coordinator-db` + `coordinator` (`--no-deps`).
   - Проверка:
     - `GET /v1/admin/shards` -> `null` (пустой registry в текущем JSON-рендере).
     - `POST /v1/packages` -> `503`, body: `{"error":"active_shard_unavailable"}`.

2. **Shard-0 startup**
   - Подняты `shard-0-db` и `shard-0-primary`.
   - В registry shard-0 автоматически стал `active` (через `EnsureActiveShard`), POST возвращает `201`.

3. **Long-running load + delayed shard-1 start**
   - Запущен фоновый load loop на 20 минут (`POST` каждые 10 секунд).
   - Через ~10 минут от старта нагрузки подняты `shard-1-db` + `shard-1-primary`.
   - Состояние до старта shard-1:
     - shard-0: `active`, `total_bytes=79532`.
   - После регистрации:
     - shard-1: `standby`, shard-0 остался `active`.

4. **Auto-rotation check (shard-0 -> shard-1)**
   - Подан burst уникальных payload-ов.
   - Наблюдение:
     - при `total_bytes` shard-0 > 1 MiB coordinator перевел shard-0 в `sealed`;
     - shard-1 стал `active`.
   - Фактическое состояние:
     - shard-0: `sealed`, `total_bytes=1078342`;
     - shard-1: `active`.

5. **Behavior when shard-1 exceeds threshold without standby**
   - Shard-1 превысил 1 MiB (`total_bytes=1454546`), но **автоматически в `sealed` не перешел**.
   - `POST /v1/packages` продолжал возвращать `201`.
   - Для продолжения сценария fail-closed shard-1 был переведен в `sealed` вручную через:
     - `PATCH /v1/admin/shards/1/state` with `state=sealed`.

6. **Fail-closed validation**
   - После `sealed` для shard-0 и shard-1:
     - `POST /v1/packages` -> `503`;
     - body: `{"error":"active_shard_unavailable"}`.

7. **Shard-2 hot-add + recovery**
   - Подняты `shard-2-db` и `shard-2-primary`.
   - В registry shard-2 зарегистрировался и стал `active` (появился как единственный reachable standby при отсутствии active).
   - `POST /v1/packages` снова возвращает `201`.
   - Финальное состояние:
     - shard-0: `sealed`;
     - shard-1: `sealed`;
     - shard-2: `active`.

### Execution: 2026-07-02 (re-run после fail-closed fix)

Образы coordinator/shard пересобраны с фиксом `CheckSeal` → auto-seal active при `ErrNoStandbyShard`.

1. **Reset + coordinator-only start**
   - `down -v`, `coordinator-db`, `coordinator` (`--no-deps`; после первого старта coordinator перезапущен, когда БД стала healthy).
   - `GET /v1/admin/shards` -> `null`.
   - `POST /v1/packages?supplier_id=1` -> `503`, `{"error":"active_shard_unavailable"}`.

2. **Shard-0 startup**
   - Подняты `shard-0-db` + `shard-0-primary`.
   - После регистрации shard-0 в registry как `standby`; первый `POST` -> auto-promote в `active`, `201`.

3. **Delayed shard-1 start**
   - Через ~3 мин после shard-0 подняты `shard-1-db` + `shard-1-primary` (ускоренный вариант шага 6; полный сценарий — 10 мин).
   - shard-1: `standby`, shard-0: `active`, `total_bytes=581`.

4. **Auto-rotation (shard-0 -> shard-1)**
   - Burst уникальных high-entropy XML (~120 KB random hex на upload).
   - shard-0 превысил 1 MiB -> `sealed` (`total_bytes=2225490`).
   - shard-1 стал `active`.

5. **Fail-closed без standby (шаг 8, без ручного PATCH)**
   - Продолжение burst на shard-1: `total_bytes` превысил 1 MiB (`1754928`).
   - Coordinator **автоматически** перевёл shard-1 в `sealed` (fail-closed).
   - `POST /v1/packages?supplier_id=1` -> `503`, `{"error":"active_shard_unavailable"}`.
   - Финальное состояние до recovery: shard-0 `sealed`, shard-1 `sealed`, active отсутствует.

6. **Shard-2 hot-add + recovery**
   - Подняты `shard-2-db` + `shard-2-primary`.
   - shard-2 зарегистрировался и auto-promote в `active`.
   - `POST /v1/packages?supplier_id=1` -> `201`.

7. **Cleanup**
   - `docker compose ... down -v`.

**Примечание по нагрузке:** `fill_shards.py` с повторяющимся padding сжимается и медленно растит `total_bytes` при пороге 1 MiB; для seal-теста используйте уникальные high-entropy payload (random hex в XML). PowerShell/curl с `--data-binary` и большим телом в одной строке на Windows упирается в лимит длины командной строки — предпочтительны Python/`requests` или временный файл.

### Итог

- Hot-add и восстановление write path подтверждены.
- Fail-closed при отсутствии active/standby подтвержден.
- Auto-seal active при превышении порога без standby подтверждён (re-run 2026-07-02); ручной `PATCH` для шага 8 больше не требуется.
