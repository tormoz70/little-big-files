# Тестовый стенд little-big-files (Фаза 4)

Подробное описание развёртывания **шардированного** стенда с Coordinator, volume-based seal и зеркалированием primary/replica. Для локальной разработки без шардов см. [README.md](../README.md) (`docker-compose.yml` + один `server`).

---

## 1. Назначение

Стенд предназначен для:

- проверки **сквозного API** через Coordinator (как у клиентов в prod);
- проверки **volume-based шардирования**: запись на active → seal → запись на standby;
- проверки **зеркалирования**: segment sync primary → replica, чтение sealed шарда с replica;
- нагрузочных прогонов (dedup, WriteBuffer, compression) перед выходом на пик ~1000 POST/с;
- отладки ops-сценариев: seal, rotate, admin API.

Клиенты **никогда** не ходят напрямую на shard — только на Coordinator `:8080`.

---

## 1.1. Локальный стенд (3 шарда × 50 MB)

Быстрый вариант для Docker Desktop / локальной VM: **3 шарда**, порог seal **50 MB**, **10 поставщиков** и Python-клиенты.

| Параметр | Значение |
|----------|----------|
| Compose | [deploy/docker-compose.local.yml](../deploy/docker-compose.local.yml) |
| Bootstrap | [deploy/shards.local.json](../deploy/shards.local.json) |
| `SHARD_MAX_BYTES` | 52 428 800 (50 MB) |
| Coordinator | `localhost:8080` |
| Поставщики | `supplier_id` 1001–1010 ([clients/python/suppliers.py](../clients/python/suppliers.py)) |
| Тестовые ZIP | [examples/](../examples/) (12 файлов EKB) |

### Запуск

```bash
make docker-local
# или:
docker compose -f deploy/docker-compose.local.yml up -d --build
```

### Загрузка данных (Python)

```bash
cd clients/python
python -m venv .venv && .venv\Scripts\activate   # Windows
pip install -r requirements.txt

# одна загрузка всех examples на 10 поставщиков
python upload_examples.py --wait

# много раундов → seal shard 0 → 1 → 2
python upload_examples.py --wait --repeat 300 --verify-read
```

Подробнее: [clients/python/README.md](../clients/python/README.md).

### Начальные состояния шардов

| shard_id | state   |
|----------|---------|
| 0        | active  |
| 1        | standby |
| 2        | standby |

### Проверка

```bash
curl -s http://localhost:8080/v1/admin/shards
curl -s http://localhost:8080/metrics | head
make stand-upload
```

### Мониторинг (Prometheus + Grafana)

| URL | Назначение |
|-----|------------|
| http://localhost:3000 | Grafana (`admin` / `lbf`), дашборд **LBF Local Stand** |
| http://localhost:9090 | Prometheus UI |
| `http://localhost:8080/metrics` | метрики Coordinator |

Подробнее: [deploy/observability/README.md](../deploy/observability/README.md).

Остановка с очисткой volumes:

```bash
make docker-local-down
```

---

## 2. Архитектура

```mermaid
flowchart TB
    client[Клиенты / нагрузочный генератор] --> coord[Coordinator :8080]

    subgraph coord_layer [Coordinator]
        cdb[(coordinator-db)]
        reg[shard_registry]
        gpi[global_package_index]
    end

    coord --> cdb
    coord --> reg
    coord --> gpi

    coord -->|POST write| s0p[shard-0-primary]
    coord -->|GET read sealed| s0r[shard-0-replica]

    s0p --> s0db[(shard-0-db)]
    s0r --> s0db
    s0sync[shard-0-sync] -->|HTTP segments| s0p
    s0sync --> s0r

    coord -.->|после seal| s1p[shard-1-primary standby→active]
    s1p --> s1db[(shard-1-db)]
    s1sync[shard-1-sync] --> s1r[shard-1-replica]
```

### Состояния шардов (начальная конфигурация)

| shard_id | state   | Назначение                          |
|----------|---------|-------------------------------------|
| 0        | active  | Принимает все записи                |
| 1        | standby | Пустой, активируется после seal #0  |

Реестр задаётся в [deploy/shards.bootstrap.json](../deploy/shards.bootstrap.json) и используется как seed при старте Coordinator (существующие runtime состояния в `shard_registry` не перезаписываются).

---

## 3. Требования к VM

### Минимум (функциональные тесты)

| Ресурс | Значение |
|--------|----------|
| vCPU   | 4        |
| RAM    | 8 GB     |
| Диск   | 100 GB SSD |
| ОС     | Ubuntu 22.04/24.04, Debian 12 |
| ПО     | Docker Engine 24+, Compose v2 |

### Рекомендуется (нагрузка + seal-тесты)

| Ресурс | Значение |
|--------|----------|
| vCPU   | 8        |
| RAM    | 16 GB    |
| Диск   | 250 GB SSD |
| Сеть   | 1 Gbit   |

### Почему столько контейнеров

`docker-compose.sharded.yml` по умолчанию поднимает **6 сервисов** (без профиля `replica`):

| Группа | Сервисы | Кол-во |
|--------|---------|--------|
| Coordinator | coordinator-db, coordinator | 2 |
| Shard 0 | db, primary | 2 |
| Shard 1 | db, primary | 2 |
| **Итого** | | **6** |

С профилем `replica` добавляются `shard-*-replica` и `shard-*-sync` (итого 10 сервисов).

Каждый шард — **изолированный** PostgreSQL и отдельные volumes для сегментов/RocksDB (на стенде dedup backend = `memory`).

---

## 4. Состав стенда (контейнеры)

### 4.1. Coordinator

| Сервис | Образ / сборка | Порт наружу | Роль |
|--------|----------------|-------------|------|
| `coordinator-db` | postgres:16-alpine | — (только docker network) | Глобальный индекс |
| `coordinator` | `deploy/Dockerfile` target `coordinator` | **8080** | API + маршрутизация + seal loop |

**Coordinator PG** (`coordinator` DB):

- `shard_registry` — состояние шардов, URL primary/replica;
- `global_package_index` — global_id → shard_id + local_id;
- `global_xml_index` — заготовка под будущий XML hash lookup (в MVP не заполняется).

### 4.2. Shard 0 (active)

| Сервис | SHARD_ID | SHARD_ROLE | SHARD_READ_ONLY | Volumes |
|--------|----------|------------|-----------------|---------|
| `shard-0-db` | — | — | — | shard0-pg |
| `shard-0-primary` | 0 | primary | false | shard0-segments, shard0-rocksdb |
| `shard-0-replica` | 0 | replica | **true** | shard0-replica-segments, shard0-replica-rocksdb |
| `shard-0-sync` | — | — | — | пишет в shard0-replica-segments |

### 4.3. Shard 1 (standby)

Аналогично shard 0, но `SHARD_ID=1`, state в bootstrap = `standby`. После seal shard 0 становится `sealed`, shard 1 — `active`.

### 4.4. Внутренние порты

Все shard-сервера слушают `:8080` **внутри** docker network. Снаружи опубликован только Coordinator `8080`. Прямой доступ к шардам для отладки:

```bash
docker compose -f deploy/docker-compose.sharded.yml exec shard-0-primary wget -qO- http://localhost:8080/v1/internal/stats
```

---

## 5. Потоки данных

### 5.1. Write (POST /v1/packages)

```mermaid
sequenceDiagram
    participant C as Client
    participant CO as Coordinator
    participant A as shard-N-primary
    participant PG as coordinator-db

    C->>CO: POST /v1/packages?supplier_id=
    CO->>CO: ActiveShard() → shard 0
    CO->>A: proxy POST (local ingest)
    A->>A: dedup + segments + shard PG
    A-->>CO: 201 {package_id: local}
    CO->>CO: global_id = (shard_id << 48) | local_id
    CO->>PG: INSERT global_package_index
    CO-->>C: 201 {package_id: global_id}
```

**Важно:**

- `package_id` в ответе клиенту — **глобальный** (64 bit).
- `file_id` остаётся **локальным** внутри шарда; в URL download Coordinator подставляет global package_id + local file_id.
- Dedup (Bloom + memory index) работает **только внутри active шарда** между всеми supplier_id.

### 5.2. Read (GET)

| Запрос | Маршрут Coordinator |
|--------|---------------------|
| `GET /v1/packages/{global_id}` | `shard_id = global_id >> 48` → proxy на shard |
| `GET /v1/packages/{global_id}/files/{file_id}` | то же + local file_id |
| `GET /v1/packages/{global_id}/original` | shortcut на original file |

**Выбор URL шарда:**

- `active` → **primary_url**
- `sealed` → **replica_url** (если задан), иначе primary

### 5.3. Seal и ротация

Триггер (автоматический, каждые `SEAL_CHECK_INTERVAL`):

```
IF active_shard.total_bytes >= SHARD_MAX_BYTES:
    POST active/v1/internal/seal     → read-only
    shard_registry: active → sealed
    standby → active
```

На стенде `SHARD_MAX_BYTES=1073741824` (**1 GB**) — seal можно получить без терабайтов данных.

**Ручной seal** (без ожидания порога):

```bash
curl -s -X POST http://localhost:8080/v1/admin/seal-rotate \
  -H "Content-Type: application/json" \
  -d '{"cluster_key":"lbf-sharded-cluster-key"}'
```

После ротации новые POST идут на shard 1; чтение старых пакетов — по global_id с shard_id=0 (sealed, через replica).

### 5.4. Hot-add через API (автоматический standby)

Регистрация shard (идемпотентно по `shard_uuid`):

```bash
curl -s -X POST http://localhost:8080/v1/admin/shards \
  -H "Content-Type: application/json" \
  -d '{
    "cluster_key": "lbf-local-cluster-key",
    "shard_uuid": "33333333-3333-3333-3333-333333333333",
    "primary_url": "http://shard-3-primary:8080"
  }' | jq .
```

Hot-add завершается на `standby`: новый shard появляется в реестре и дальше активируется только обычной ротацией Coordinator (`seal-rotate`) или отдельным recovery/manual failover.

### 5.5. Manual failover: standby -> active

Ручное переключение standby -> active (с подтверждением) используется как recovery-путь:

```bash
curl -s -X PATCH http://localhost:8080/v1/admin/shards/3/state \
  -H "Content-Type: application/json" \
  -d '{
    "cluster_key": "lbf-local-cluster-key",
    "state": "active",
    "confirm": true
  }' | jq .
```

### 5.6. Зеркалирование (MVP стенда)

| Слой | Механизм на стенде | Prod (TODO) |
|------|-------------------|-------------|
| Сегменты | `shard-*-sync` копирует файлы по HTTP `/v1/internal/segments` каждые 15s | rsync/lsyncd |
| PostgreSQL | Replica использует **тот же** PG primary (общий DSN) | Streaming replication |
| RocksDB / Bloom | Отдельные volumes; rebuild из PG при старте (`DEDUP_REBUILD_ON_START`) | Checkpoint copy |

Replica **не принимает POST** (`SHARD_ROLE=replica` + `SHARD_READ_ONLY=true` → HTTP 403).

---

## 6. Глобальный package_id

```
┌────────────────┬────────────────────────────────────────────────┐
│  shard_id      │           local_package_id                       │
│  16 bit        │           48 bit                                 │
└────────────────┴────────────────────────────────────────────────┘
```

Пример: shard 0, local_id 42:

```
global_id = (0 << 48) | 42 = 42
```

Shard 1, local_id 5:

```
global_id = (1 << 48) | 5 = 281474976710661
```

Проверка в ответе POST:

```bash
curl -s -X POST "http://localhost:8080/v1/packages?supplier_id=1" \
  -d '<?xml version="1.0"?><doc/>' | jq .package_id
```

---

## 7. Переменные окружения

### Coordinator

| Variable | Значение на стенде | Описание |
|----------|-------------------|----------|
| `HTTP_ADDR` | `:8080` | Публичный API |
| `COORDINATOR_PG_DSN` | `postgres://lbf:lbf@coordinator-db:5432/coordinator?...` | PG Coordinator |
| `COORDINATOR_BOOTSTRAP` | `/app/deploy/shards.bootstrap.json` | Начальный реестр шардов |
| `COORDINATOR_MIGRATIONS_PATH` | `/app/migrations/coordinator` | SQL миграции |
| `CLUSTER_KEY` | `lbf-...` | Общий ключ для регистрации shard и mutating admin API |
| `SHARD_MAX_BYTES` | `1073741824` (1 GB) | Порог seal |
| `SEAL_CHECK_INTERVAL` | `30s` | Период опроса stats active шарда |
| `MAX_BODY_BYTES` | `67108864` (64 MB) | Лимит тела POST |

### Shard server (primary / replica)

| Variable | Primary | Replica |
|----------|---------|---------|
| `SHARD_ID` | 0 или 1 | то же |
| `SHARD_ROLE` | `primary` | `replica` |
| `SHARD_READ_ONLY` | `false` | `true` |
| `SHARD_UUID` | UUID шарда | обычно не используется на replica |
| `SHARD_CLUSTER_KEY` | `lbf-...` | общий ключ кластера |
| `SHARD_ADVERTISE_URL` | URL primary shard | — |
| `SHARD_STARTUP_STATE` | `standby` | Startup registration; `active`/`sealed` отклоняются coordinator |
| `COORDINATOR_URL` | `http://coordinator:8080` | — |
| `PG_DSN` | shard-N-db | **тот же** (MVP) |
| `DATA_DIR` | `/data/segments` | отдельный volume |
| `DEDUP_BACKEND` | `memory` | `memory` |
| `COMPRESSION_ENABLED` | `true` | `true` |
| `WRITE_BUFFER_MAX_BYTES` | 4 MB (default) | — |
| `LARGE_ZIP_ASYNC_UNPACK` | `true` (default) | отключён на replica* |

\*На replica unpack queue не стартует (`SHARD_ROLE=replica`).

### shard-sync

| Variable | Пример |
|----------|--------|
| `SYNC_PRIMARY_URL` | `http://shard-0-primary:8080` |
| `CLUSTER_KEY` | `lbf-...` (доступ к `/v1/internal/segments`) |
| `DATA_DIR` | `/data/segments` (volume replica) |
| `SYNC_INTERVAL` | `15s` |

---

## 8. Развёртывание

### 8.1. Подготовка VM

```bash
sudo apt update && sudo apt install -y docker.io docker-compose-plugin git jq curl
sudo usermod -aG docker $USER
# перелогиниться
```

### 8.2. Запуск

```bash
git clone https://github.com/tormoz70/little-big-files.git
cd little-big-files
make docker-sharded
# или:
docker compose -f deploy/docker-compose.sharded.yml up -d --build
```

Первый build занимает 3–10 мин (скачивание Go modules + сборка 3 бинарников).

### 8.3. Проверка здоровья

```bash
# Все контейнеры running
docker compose -f deploy/docker-compose.sharded.yml ps

# Реестр шардов
curl -s http://localhost:8080/v1/admin/shards | jq

# Stats active шарда (изнутри)
docker compose -f deploy/docker-compose.sharded.yml exec shard-0-primary \
  wget -qO- http://localhost:8080/v1/internal/stats | jq
```

Ожидаемый `shards`:

```json
[
  { "shard_id": 0, "state": "active", ... },
  { "shard_id": 1, "state": "standby", ... }
]
```

---

## 9. Сценарии проверки

### 9.1. Базовый ingest + read

```bash
# POST XML
RESP=$(curl -s -X POST "http://localhost:8080/v1/packages?supplier_id=2447&filename=test.xml" \
  -d '<?xml version="1.0"?><seans ver="3.2.0"></seans>')
echo "$RESP" | jq .
GID=$(echo "$RESP" | jq -r .package_id)

# GET manifest
curl -s "http://localhost:8080/v1/packages/$GID" | jq .

# GET bytes
curl -s "http://localhost:8080/v1/packages/$GID/original"
```

### 9.2. Dedup (100 одинаковых POST)

Один physical blob на active шарде, 100 разных global package_id:

```bash
BODY='<?xml version="1.0"?><seans></seans>'
for i in $(seq 1 100); do
  curl -s -o /dev/null -w "%{http_code}\n" \
    -X POST "http://localhost:8080/v1/packages?supplier_id=1" -d "$BODY"
done
```

Проверка на primary:

```bash
docker compose -f deploy/docker-compose.sharded.yml exec shard-0-db \
  psql -U lbf -d lbf_shard0 -c "SELECT COUNT(*) FROM content_blobs;"
```

### 9.3. Small ZIP (sync unpack)

```bash
python3 - <<'PY' | curl -s -X POST "http://localhost:8080/v1/packages?supplier_id=1&filename=pkg.zip" \
  --data-binary @- | jq .unpack_status
import zipfile, io
b = io.BytesIO()
with zipfile.ZipFile(b, 'w') as z:
    z.writestr('a.xml', b'<?xml version="1.0"?><a/>')
print(b.getvalue().decode('latin-1'), end='')
PY
```

Ожидание: `"unpack_status": "ok"`, `storage_mode`: `zip_with_members`.

### 9.4. Seal + read со sealed шарда

```bash
# Принудительная ротация
curl -s -X POST http://localhost:8080/v1/admin/seal-rotate \
  -H "Content-Type: application/json" \
  -d '{"cluster_key":"lbf-sharded-cluster-key"}'

# Запомнить GID пакета до ротации (из сценария 9.1)
curl -s "http://localhost:8080/v1/packages/$GID/original"

# Новый POST должен попасть на shard 1
curl -s -X POST "http://localhost:8080/v1/packages?supplier_id=2" \
  -d '<?xml version="1.0"?><after-seal/>' | jq .package_id
# global_id >> 48 должно быть 1
```

### 9.5. Replica отклоняет write

```bash
docker compose -f deploy/docker-compose.sharded.yml exec shard-0-replica \
  wget -qO- --post-data='<?xml version="1.0"?><x/>' \
  --header='Content-Type: application/octet-stream' \
  'http://localhost:8080/v1/packages?supplier_id=1' || true
# Ожидание: HTTP 403
```

---

## 10. Нагрузочное тестирование

Пример с [vegeta](https://github.com/tsenart/vegeta):

```bash
echo 'POST http://localhost:8080/v1/packages?supplier_id=1001' | \
  vegeta attack -duration=30s -rate=100 \
  -body=<(printf '<?xml version="1.0"?><load/>') | vegeta report
```

Для приближения к 1000 pkg/s:

- увеличить vCPU/RAM VM;
- на shard выставить `DEDUP_BACKEND=rocksdb` (образ `server-rocksdb`, см. Dockerfile);
- вынести PG на faster disk / tune `shared_buffers`.

Метрики Prometheus доступны на **`GET /metrics`** у Coordinator и каждого shard. На локальном стенде — Grafana `:3000` и Prometheus `:9090` (см. [§1.1](#11-локальный-стенд-3-шарда--50-mb)).

Дополнительно: логи Coordinator/shard и `supplier_stats` в shard PG.

---

## 11. Volumes и данные

| Volume | Содержимое | Потеря при `docker volume rm` |
|--------|------------|-------------------------------|
| coordinator-pg | Глобальный индекс | Потеря маршрутизации global_id |
| shard0-pg | Метаданные shard 0 | Потеря packages/files/blobs |
| shard0-segments | Физические blobs shard 0 | **Невосстановимая** без backup |
| shard0-replica-segments | Копия сегментов | Пересync с primary |
| shard1-* | Аналогично для shard 1 | |

Полный сброс стенда:

```bash
docker compose -f deploy/docker-compose.sharded.yml down -v
```

---

## 12. Ограничения MVP (vs production)

| Возможность | Стенд | Production target |
|-------------|-------|-------------------|
| Volume-based seal | ✅ | ✅ |
| Global package_id | ✅ | ✅ |
| Read sealed via replica URL | ✅ | ✅ |
| HTTP segment sync | ✅ | rsync/lsyncd continuous |
| PG replication | ❌ shared DB | Streaming replication |
| RocksDB sync | ❌ rebuild | Checkpoint |
| Failover auto-promote | ❌ | Patroni / ops |
| `GET /xml/{hash}` fan-out | ❌ | global_xml_index + fan-out |
| TLS / auth | ❌ | reverse proxy + mTLS |
| `/metrics` Prometheus | ✅ (локальный стенд) | ✅ |

---

## 13. Troubleshooting

| Симптом | Возможная причина | Действие |
|---------|------------------|----------|
| Coordinator 502 на POST | Shard primary не поднялся | `docker compose logs shard-0-primary` |
| `no active shard` | Пустой shard_registry | Проверить bootstrap JSON, перезапустить coordinator |
| GET 404 после seal | Replica segments не synced | `docker compose logs shard-0-sync`, проверить volumes |
| Seal не срабатывает | `total_bytes < SHARD_MAX_BYTES` | Уменьшить `SHARD_MAX_BYTES` или вызвать `POST /v1/admin/seal-rotate` с `cluster_key` |
| `active_shard_unavailable` (POST 503) | active shard недоступен по сети | Проверить `primary_url`, `GET /v1/internal/stats`, затем выполнить recovery/failover через `PATCH /v1/admin/shards/{id}/state` |
| Duplicate POST медленный | Dedup memory + PG | Для нагрузки — RocksDB backend |
| Coordinator не стартует | PG not ready | Дождаться healthcheck coordinator-db |

Логи:

```bash
docker compose -f deploy/docker-compose.sharded.yml logs -f coordinator
docker compose -f deploy/docker-compose.sharded.yml logs -f shard-0-primary
```

---

## 14. Связанные документы

- [sharding-model.md](sharding-model.md) — модель volume-based шардирования
- [architecture.md](architecture.md) — алгоритмы dedup, API, форматы
- [stack.md](stack.md) — стэк и фазы
- [pilot-stand.md](pilot-stand.md) — параметры и запуск для опытной эксплуатации на ВМ
- [deploy/docker-compose.sharded.yml](../deploy/docker-compose.sharded.yml) — манифест стенда
- [deploy/shards.bootstrap.json](../deploy/shards.bootstrap.json) — начальный реестр

---

## 15. Быстрая шпаргалка

```bash
# Поднять
make docker-sharded

# Статус шардов
curl -s localhost:8080/v1/admin/shards | jq .

# POST пакета
curl -s -X POST "localhost:8080/v1/packages?supplier_id=1" \
  -d '<?xml version="1.0"?><doc/>' | jq .

# Seal вручную
curl -s -X POST localhost:8080/v1/admin/seal-rotate \
  -H "Content-Type: application/json" \
  -d '{"cluster_key":"lbf-sharded-cluster-key"}'

# Остановить и удалить данные
docker compose -f deploy/docker-compose.sharded.yml down -v
```

## 16. Recovery после потери PostgreSQL

Если metadata в PostgreSQL повреждена или утеряна, используйте `recovery-tool`:

```bash
# dry-run (по умолчанию)
recovery-tool

# применение в PostgreSQL + rebuild dedup index
recovery-tool --apply
```

Для корректного восстановления в `DATA_DIR` должны быть:

| Артефакт | Назначение |
|----------|------------|
| `segment_XXXX.idx` | blob index: hash, offset, segment, sizes |
| `ingest_journal.ndjson` | packages + package_files |
| `../dictionaries/current.json` | актуальный Zstd dictionary sidecar |

### supplier_id для набора ekb_work2

| Источник | supplier_id в API |
|----------|-------------------|
| `2447` | `2447` |
| `1577-1601` | `1577` (агрегированный поставщик для стенда) |

Диапазон `supplier_id` в API: `1..1000000`.
