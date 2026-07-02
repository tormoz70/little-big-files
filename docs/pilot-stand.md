# Опытная эксплуатация: параметры и инструкция развертывания

Документ фиксирует рекомендуемый контур для опытной эксплуатации `little-big-files` на виртуальных машинах.

Проект в текущем виде ориентирован на Docker Compose и локальные/single-host манифесты. Поэтому для multi-host пилота используется тот же набор сервисов, но с разнесением по ВМ и реальными hostname в bootstrap.

---

## 1. Рекомендуемая схема пилота

### Вариант B (рекомендуется): 3-4 ВМ

| VM | Сервисы | vCPU | RAM | Диск |
|----|---------|------|-----|------|
| `lbf-coord` | `coordinator`, `coordinator-db`, `prometheus`, `grafana` (+ внешний reverse proxy/TLS) | 4 | 8 GB | 100 GB SSD |
| `lbf-shard-0` | `shard-0-primary`, `shard-0-db` (active) | 8 | 16 GB | NVMe 500 GB-1 TB |
| `lbf-shard-1` | `shard-1-primary`, `shard-1-db` (standby) | 4 | 8 GB | 200 GB SSD |
| `lbf-shard-2` *(опц.)* | `shard-2-primary`, `shard-2-db` (standby) | 4 | 8 GB | 200 GB SSD |

Реплика для шарда на пилоте опциональна. Базовый контур можно запускать без replica и shard-sync.

### Альтернативы

- **Минимум (1 ВМ):** все сервисы на одном хосте, только для короткого пилота и малой нагрузки.
- **Расширенный HA (5+ ВМ):** по отдельной replica ВМ на каждый shard + `shard-sync`.

---

## 2. Сетевой профиль и доступы

Клиенты работают только с Coordinator.

| Endpoint | Назначение | Доступ |
|----------|------------|--------|
| `https://lbf.example.org` | API Coordinator (`/v1/...`) | внешние клиенты |
| `http://lbf-coord.internal:8080/metrics` | метрики coordinator | внутренняя сеть |
| `http://lbf-shard-N.internal:8080` | shard API/metrics/internal | только из внутренней сети |
| `http://lbf-coord.internal:3000` | Grafana | ops |
| `http://lbf-coord.internal:9090` | Prometheus | ops |

Рекомендуется ограничить firewall так, чтобы shard `:8080` был доступен только с `lbf-coord`.

---

## 3. Параметры стенда (pilot baseline)

### Coordinator

| Переменная | Значение для пилота | Комментарий |
|------------|---------------------|-------------|
| `HTTP_ADDR` | `:8080` | API |
| `COORDINATOR_PG_DSN` | `postgres://.../coordinator` | отдельная БД coordinator |
| `COORDINATOR_BOOTSTRAP` | путь к `shards.pilot.json` | реестр шардов |
| `CLUSTER_KEY` | `...` | общий ключ для startup registration и mutating admin API |
| `SHARD_MAX_BYTES` | `107374182400` (100 GB) | порог seal |
| `SEAL_CHECK_INTERVAL` | `30s`-`5m` | частота проверки active |
| `MAX_BODY_BYTES` | `67108864` (64 MB) | лимит POST |

### Shard (primary)

| Переменная | Значение для пилота |
|------------|---------------------|
| `SHARD_ID` | optional fallback for standalone start |
| `SHARD_ROLE` | `primary` |
| `SHARD_READ_ONLY` | `false` (до seal) |
| `SHARD_UUID` | стабильный UUID шарда |
| `SHARD_CLUSTER_KEY` | ключ кластера (тот же, что `CLUSTER_KEY`) |
| `SHARD_ADVERTISE_URL` | URL, по которому coordinator ходит в shard |
| `SHARD_STARTUP_STATE` | `standby` (coordinator отклоняет `active`/`sealed` при startup registration) |
| `COORDINATOR_URL` | `http://lbf-coord.internal:8080` |
| `PG_DSN` | своя shard-N БД |
| `DATA_DIR` | `/data/segments` |
| `ROCKSDB_PATH` | `/data/rocksdb` |
| `DEDUP_BACKEND` | `rocksdb` |
| `COMPRESSION_ENABLED` | `true` |

### Replica (опционально)

| Переменная | Значение |
|------------|----------|
| `SHARD_ROLE` | `replica` |
| `SHARD_READ_ONLY` | `true` |
| `SYNC_PRIMARY_URL` | `http://lbf-shard-N.internal:8080` |
| `CLUSTER_KEY` | тот же ключ кластера для `/v1/internal/segments` |

---

## 4. Bootstrap для pilot

Пример `shards.pilot.json` (без реплик):

```json
[
  {
    "shard_id": 0,
    "state": "active",
    "primary_url": "http://lbf-shard-0.internal:8080"
  },
  {
    "shard_id": 1,
    "state": "standby",
    "primary_url": "http://lbf-shard-1.internal:8080"
  },
  {
    "shard_id": 2,
    "state": "standby",
    "primary_url": "http://lbf-shard-2.internal:8080"
  }
]
```

Если replica используется, добавляется `replica_url` для sealed-read.

---

## 5. Порядок развертывания

1. Подготовить ВМ: Docker Engine 24+, Docker Compose v2, `curl`, `jq`.
2. Развернуть `coordinator-db` и `coordinator` на `lbf-coord`.
3. На shard-ВМ поднять `shard-N-db` и `shard-N-primary`.
4. Перед стартом coordinator положить `shards.pilot.json` и указать `COORDINATOR_BOOTSTRAP`.
5. Поднять observability (`prometheus`, `grafana`) на `lbf-coord`.
6. Проверить:
   - `GET /v1/admin/shards`
   - `GET /metrics` на coordinator и shard
   - тестовый `POST /v1/packages?supplier_id=...`

Mutating admin операции выполняются с `cluster_key`:

- `POST /v1/admin/shards` — idempotent startup registration по `shard_uuid` (вход всегда `standby`; при отсутствии `active` coordinator может автоматически promote первый reachable `standby`)
- `PATCH /v1/admin/shards/{id}/state` — отдельный recovery/failover сценарий (`standby -> active` только с `confirm=true`)
- `POST /v1/admin/seal-rotate` — manual rotate (требует `cluster_key` в body или `X-Cluster-Key`)

`COORDINATOR_BOOTSTRAP` используется как seed-реестр: после первого создания shard rows coordinator не перезаписывает runtime state (`active/sealed/standby`) при рестарте.

При недоступности `active` coordinator работает в режиме fail-closed: `POST /v1/packages` возвращает `503 active_shard_unavailable` до появления reachable `standby` (или ручного failover).

---

## 6. Бэкапы и восстановление

Для опытной эксплуатации обязательно:

- ежедневный `pg_dump` для `coordinator-db` и всех `shard-N-db`;
- snapshot/backup директорий `DATA_DIR` (segments);
- backup sidecar-файлов (`segment_*.idx`, `ingest_journal.ndjson`, dictionaries sidecar);
- регулярный тест восстановления:

```bash
# dry-run по умолчанию
recovery-tool
recovery-tool --apply
```

---

## 7. Ограничения текущей реализации

На текущем этапе в репозитории нет полноценной автоматизации multi-host:

- нет `docker-compose` для кросс-ВМ оркестрации "из коробки";
- нет auto-failover для shard/coordinator;
- нет встроенного TLS/auth в приложении (рекомендуется закрывать через reverse proxy).

---

## 8. Операционный чеклист первого дня

- проверить доступность `coordinator` и `shard` health/metrics;
- убедиться, что в `shard_registry` есть `active` и `standby`;
- выполнить контрольный `POST` и `GET /original`;
- проверить Grafana-панели ingest/error/seal;
- проверить свободное место на active-шарде и порог `SHARD_MAX_BYTES`;
- запустить и проверить nightly backup.

