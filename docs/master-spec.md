# Master Spec: little-big-files

| | |
|---|---|
| **Статус** | Normative (as-built + product contract) |
| **Версия** | 1.0 |
| **Дата** | 2026-08-03 |
| **Стек** | Go 1.22+, PostgreSQL 14+, HTTP REST |
| **Лицензия** | MIT |

Этот документ — **человекочитаемый product contract**: зачем система существует, какие инварианты обязательны, что входит в контракт API/данных, а что намеренно вне scope.

Для Cursor/агентов авторитетный SoT — [`.ai/master-spec.yaml`](../.ai/master-spec.yaml) (persona, guardrails, code anchors, verification); краткий pointer — [`.cursor/rules/master-spec.mdc`](../.cursor/rules/master-spec.mdc). Оба артефакта должны оставаться согласованы при смене контракта.

Детали реализации, стендов и trade-offs вынесены в связанные документы (§17).

---

## 1. Цель продукта

`little-big-files` — **append-only content-addressed storage** для высоконагруженного приёма XML/ZIP пакетов (контур EKB/поставщиков) с **прозрачной дедупликацией**.

Клиент всегда получает новый логический `package_id`. Физически одинаковые XML (и прочие blobs) хранятся один раз и переиспользуются через `ref_count`. В потоках с повторяющимся XML между поставщиками ожидаемая экономия диска — **70–90%**.

### 1.1. Проблема

| Фактор | Описание |
|--------|----------|
| Объём | Сотни миллионов XML; типичный posted000 **~400–700 B**, outlier до **~16 KB** |
| Нагрузка | Write-heavy, peak **~1000 pkg/s** |
| Пакеты | ZIP с 1 XML (~85%) или bulk ZIP (до ~85 XML); редкие ZIP на МБ с тысячами XML |
| Паттерн | Одни и те же XML приходят от разных suppliers в разных пакетах |
| Чтение | **Очень редкое** (анализ, разбор конфликтов) |
| Удаления | Нет — только append |

### 1.2. Ценностное предложение

1. Прозрачный dedup без сигнала клиенту (`201` + новые ID при любом успешном upload).
2. ZIP = оригинал + members (small sync / large async).
3. Volume-based sharding (rolling seal) без rehash/миграции при scale-out.
4. Operational guardrails: disk write gate `507`, CRC32C на read, recovery tooling, Prometheus.

---

## 2. Область действия (in / out of scope)

### 2.1. In scope (текущий контракт)

- HTTP REST `/v1/packages` (upload/download metadata + bytes).
- Content-addressed blobs в append-only segment files.
- Package-level и blob-level dedup (SHA-256).
- Zstd dictionary compression для XML (клиенту — логические bytes).
- Single-node и coordinator-sharded deployment.
- Hot-add standby shard, seal/rotate по `SHARD_MAX_BYTES`.
- Primary/replica segment sync через HTTP sidecar (`shard-sync`).
- Async unpack large ZIP + recovery scan по `storage_mode=raw_large`.
- Prometheus metrics, structured logs.

### 2.2. Out of scope (явно)

| Тема | Статус |
|------|--------|
| gRPC / Kafka / async `202 Accepted` на upload | Не поддерживается |
| Публичный lookup по XML content hash | Не реализован (`global_xml_index` — schema stub) |
| Удаление / update / GC сегментов | Append-only; orphans допустимы |
| Cross-shard blob dedup после seal | Приемлема вторая копия на новом active |
| Авто-failover primary→replica (promote) | Manual / future |
| Streaming PG replication в compose | Target; MVP — shared PG / sidecar segments |
| Object storage tier для `archived` | Опциональный future |

---

## 3. Роли и режимы развёртывания

### 3.1. Исполняемые компоненты

| Бинарь | Роль |
|--------|------|
| `cmd/server` | Shard / standalone: API, ingestion, storage, dedup, compression, unpack |
| `cmd/coordinator` | Write→active, read routing, registry, seal/rotate, global package index |
| `cmd/shard-sync` | Sidecar: sync segment files primary→replica |
| `cmd/rebuild-index` | Rebuild hot dedup index из `content_blobs` |
| `cmd/recovery-tool` | Rebuild metadata из segment index + ingest journal |
| `cmd/migrate` | SQL migrations |

### 3.2. Deployment modes

| Mode | Когда | Поведение |
|------|-------|-----------|
| `single-node` | Нет `COORDINATOR_URL` / `DEPLOYMENT_MODE=single-node` | Один server = полный write/read path |
| `sharded` | Есть coordinator | Клиенты ходят на coordinator; shards — data plane |

Профили Docker: `docker-single-node`, `docker-sharded`, `docker-sharded-replica`, `docker-local` — см. README / [test-stand.md](test-stand.md).

---

## 4. Функциональные требования (FR)

### FR-1. Прозрачная загрузка пакета

- `POST /v1/packages?supplier_id=` принимает XML или ZIP.
- При успехе всегда **`201 Created`** с новыми `package_id` / `file_id`.
- Клиент **не** получает признаков dedup (`canonical_package_id`, `ref_count`, `content_hash` скрыты).
- `supplier_id` берётся **только** из query/header, не из имени файла.

### FR-2. Package-level clone

- `package_hash = SHA-256(POST body)`.
- Если canonical package с тем же hash уже есть: новый row `packages` + copy `package_files` + `ref_count++` на blobs; без повторной записи сегмента для тех же bytes.

### FR-3. Blob-level StoreOrRef

- `content_hash = SHA-256(logical bytes)`.
- Hot path: Bloom → memory/RocksDB/postgres index → PG `content_blobs`.
- Новый blob: encode (optional Zstd) → append segment → insert metadata.
- Concurrent first-write разрешается атомарно (`InsertBlobOrIncrement` / `ON CONFLICT`).

### FR-4. XML payload

- `payload_type=xml`, `storage_mode=single`.
- Один `package_files` с `role=original`.

### FR-5. Small ZIP

Условие small: `len(body) ≤ ZIP_THRESHOLD_SIZE` **и** `entry_count ≤ ZIP_THRESHOLD_COUNT` (defaults: 100 KiB / 100).

- Store original ZIP (`role=original`).
- Sync unpack members → `StoreOrRef` каждый XML (`role=member`).
- При ошибке unpack: store `_unpack_error` text (`role=unpack_error`), `unpack_status=failed`.
- Итог: `storage_mode=zip_with_members` (или эквивалент с error file).

### FR-6. Large ZIP

- Сразу store original, `storage_mode=raw_large`, ответ `unpack_status=pending` (если async включён).
- Background workers распаковывают; после успеха — `zip_with_members`, members пропагируются на clone packages.
- Durable recovery: периодический scan `ListPendingLargePackages()` переэнкьюит `raw_large`.

### FR-7. Read API

- `GET /v1/packages/{id}` — manifest (metadata + file list).
- `GET /v1/packages/{id}/files/{file_id}` — logical bytes файла.
- `GET /v1/packages/{id}/original` — shortcut на original.
- На read: CRC32C verify (если `VERIFY_CHECKSUM=true`); compressed `XML2` прозрачно decompress.

### FR-8. Coordinator routing

- Write всегда на **единственный** active primary.
- Read: `global_id = (shard_id << 48) | local_id`; proxy на shard; для sealed предпочтителен `replica_url` при наличии.
- Seal при `total_bytes ≥ SHARD_MAX_BYTES`: active→sealed, standby→active.
- Hot-add: shard регистрируется только как `standby`; promote через seal-rotate / auto-promote если active отсутствует.

### FR-9. Disk write gate

- При `MIN_FREE_DISK_BYTES > 0` и недостатке свободного места — отказ записи с **`507 Insufficient Storage`**.
- Resume с hysteresis (`DISK_RESUME_HYSTERESIS_BYTES`).

---

## 5. Нефункциональные требования (NFR)

| ID | Требование | Целевое значение |
|----|------------|------------------|
| NFR-W | Write throughput | ≥ **1000 POST/s** peak (WriteBuffer + RocksDB hot index в prod) |
| NFR-R | Read latency | Не оптимизируется как hot path; p99 приемлем для редкого анализа |
| NFR-D | Durability | Segment batch fsync; PG WAL; journal fsync; CRC32C на record |
| NFR-A | Availability (write) | Fail-closed без active shard (`503`); auto-promote reachable standby |
| NFR-S | Storage savings | 70–90% при высоком cross-supplier duplicate rate |
| NFR-I | Integrity | CRC32C на payload; verify on read по умолчанию |
| NFR-Z | Zip bomb guard | `MAX_UNPACKED_ZIP_BYTES` (default 512 MiB) + streaming member unpack |
| NFR-B | Body limit | `MAX_BODY_BYTES` (default 64 MiB) → `413` |
| NFR-O | Observability | `/metrics` Prometheus; slog |
| NFR-C | Cluster auth | Internal/admin endpoints — shared `CLUSTER_KEY` (constant-time compare) |

**RPO/RTO (целевые, operational):** RPO до ~1 ч (частота backup WAL/segments); RTO до ~4 ч (restore + index rebuild). Точные SLO — предмет пилота.

---

## 6. Инварианты системы

Эти правила **нельзя** нарушать без смены major версии контракта.

1. **Transparent IDs** — успешный POST всегда создаёт новый logical package; клиент не видит dedup internals.
2. **Append-only** — нет update/delete сегментов; orphan bytes после проигравшей гонки допустимы.
3. **Logical hash** — `content_hash` считается по несжатым bytes; на диске payload может быть `XML2`.
4. **Single active writer** — в sharded mode пишет только один active primary.
5. **Seal immutability** — sealed/replica не принимают public write (`ShardGuard`).
6. **PG source of truth** — PostgreSQL владеет metadata и `content_blobs`; hot index rebuildable.
7. **Global id layout** — `[16 bit shard_id][48 bit local_package_id]`.
8. **Startup registration** — hot-add принимает только `startup_state=standby` (или пусто).
9. **CRC contract** — record = magic + size + payload + CRC32C; неизвестный magic / bad CRC = invalid.
10. **No cross-seal dedup requirement** — дубликат XML на новом active после seal допустим.

---

## 7. API-контракт

### 7.1. Public (client-facing)

| Method | Path | Success | Notes |
|--------|------|---------|-------|
| `POST` | `/v1/packages?supplier_id=&filename=` | `201` | Body: XML or ZIP |
| `GET` | `/v1/packages/{id}` | `200` JSON | Manifest |
| `GET` | `/v1/packages/{id}/files/{file_id}` | `200` bytes | Download |
| `GET` | `/v1/packages/{id}/original` | `200` bytes | Original blob |
| `GET` | `/metrics` | `200` | Prometheus |

**Типичные ошибки:** `400` (bad input), `404` (not found), `413` (body too large), `500`, `503` (no active shard / internal disabled), `507` (disk gate), `403` (replica/read-only write).

**Ответ `201` (минимум):**

```json
{
  "package_id": "...",
  "file_id": "...",
  "unpack_status": "ok|pending|failed|skipped"
}
```

`unpack_status` опционален / зависит от payload.

### 7.2. Shard internal (cluster)

Требуют `X-Cluster-Key` или `Authorization: Bearer`; без ключа — disabled (`503`).

| Method | Path | Назначение |
|--------|------|------------|
| `GET` | `/v1/internal/stats` | bytes, role, read-only |
| `POST` | `/v1/internal/seal` | primary → read-only |
| `GET` | `/v1/internal/segments` | list for sync |
| `GET` | `/v1/internal/segments/{name}` | download raw segment |

### 7.3. Coordinator admin

Mutating ops требуют `cluster_key` (body и/или header).

| Method | Path | Назначение |
|--------|------|------------|
| `GET` | `/v1/admin/shards` | registry list |
| `POST` | `/v1/admin/shards` | register/upsert by UUID (`standby` only) |
| `POST` | `/v1/admin/seal-rotate` | seal active + activate standby |
| `PATCH` | `/v1/admin/shards/{id}/state` | safe manual transition |

Public package endpoints на coordinator — proxy с теми же путями `/v1/packages...`.

---

## 8. Модель данных

### 8.1. Shard / standalone PostgreSQL

**`content_blobs`** — уникальные физические объекты:

- PK `content_hash` (SHA-256 logical)
- `size`, `stored_size`, `segment_id`, `offset`, `ref_count`, `first_seen_at`

**`packages`** — логические пакеты:

- `supplier_id`, `package_hash` (**не** UNIQUE), `payload_type` (`xml`|`zip`)
- `storage_mode` (`single`|`zip_with_members`|`raw_large`)
- `canonical_package_id` (nullable), `file_count`, `unpack_error`

**`package_files`** — связи:

- `role`: `original` | `member` | `unpack_error`
- unique: один `original` и один `unpack_error` на package

Также: `supplier_stats`, `compression_dictionary`.

### 8.2. Coordinator PostgreSQL

- `shard_registry` — state machine, URLs, `shard_uuid`, `total_bytes`
- `global_package_index` — routing/audit по global id
- `global_xml_index` — **не используется** публичным API (reserved)

### 8.3. Файловое хранилище (`DATA_DIR`)

```text
segments/segment_NNNN.dat|.idx
segments/ingest_journal.ndjson
dictionaries/current.json + dict_*.zdict
rocksdb/   # optional hot index
```

**Record on disk:**

```text
[4 magic][4 payload_size][payload][4 CRC32C(payload)]
```

| Magic | Meaning |
|-------|---------|
| `XML1` | raw XML |
| `XML2` | zstd-compressed XML |
| `ZIP1` | ZIP |
| `ERR1` | unpack error text |

Segment footer при seal: `record_count | total_size | reserved | FOOT`.

---

## 9. Алгоритмы (нормативное поведение)

### 9.1. ProcessPackage (write)

```
package_hash = SHA-256(body)
IF canonical := FindCanonical(package_hash):
    clone package + files; ref_count++; RETURN 201
Detect XML | ZIP | reject
IF XML: StoreOrRef(body); package single/original; RETURN 201
IF ZIP small: StoreOrRef(zip); unpack members|error; RETURN 201
IF ZIP large: StoreOrRef(zip); raw_large; enqueue unpack; RETURN 201 pending
```

### 9.2. StoreOrRef

```
h = SHA-256(logical)
IF exists: ref_count++; RETURN h
encode/compress → append segment (+ index) → InsertBlobOrIncrement → hot Put
```

### 9.3. Seal / rotate

```
IF active.total_bytes >= SHARD_MAX_BYTES AND standby reachable:
    POST seal(active)
    registry: active→sealed; standby→active
```

Если active отсутствует — promote первый reachable standby (fail-open для write availability при наличии standby; иначе `503`).

---

## 10. Безопасность (нормативные требования)

| Контроль | Требование |
|----------|------------|
| Body limit | `MAX_BODY_BYTES` → `413` |
| Payload detect | ZIP=`PK…`, XML=leading `<` / `<?xml`; иначе `400` |
| Zip path traversal | skip dirs; skip names with `..`; no FS extract |
| Zip bomb | streaming members + `MAX_UNPACKED_ZIP_BYTES` |
| Cluster secret | internal + mutating admin; constant-time compare |
| Write guard | reject POST on replica / read-only |
| Network trust | public shard API предполагается в trusted network; иначе нужен edge auth |
| Content-Disposition | filename из user/ZIP metadata — sanitization обязательна для hardened prod |

**Не является целью:** end-user authN/authZ на public upload (это слой внешнего gateway).

---

## 11. Отказоустойчивость и recovery

| Сценарий | Ожидаемое поведение |
|----------|---------------------|
| Crash после segment write, до PG commit | Orphan bytes; клиент не видит blob |
| Crash после PG commit, до journal | Runtime OK (PG SoT); journal может отставать |
| Partial segment tail | Startup truncate to last valid CRC record |
| Crash mid large unpack | Остаётся `raw_large` → recover re-enqueue; unpack idempotent |
| Corrupt journal last line | Reader игнорирует truncated trailing line |
| Active shard down | Auto-promote standby if reachable; else `503` |
| Sealed primary down (read) | Prefer replica_url |

Инструменты: `recovery-tool`, `rebuild-index`, `shard-sync`.

---

## 12. Наблюдаемость

Минимальный набор сигналов:

- HTTP request rate / latency / status
- Shard total bytes, read-only flag
- Blob logical vs stored vs referenced
- Unpack: enqueued / dropped / recovered
- Coordinator: seal events, shard up/failures, proxy errors
- Disk gate trips (`507`)

Дашборды/стенд: [deploy/observability](../deploy/observability/README.md), [test-stand.md](test-stand.md).

---

## 13. Конфигурация (ключевые knobs)

| Env | Default | Спека |
|-----|---------|-------|
| `DEPLOYMENT_MODE` | auto | `single-node` \| `sharded` |
| `PG_DSN` | local | Shard metadata |
| `DATA_DIR` | `./data/segments` | Segments/journal |
| `HTTP_ADDR` | `:8080` | Listen |
| `MAX_BODY_BYTES` | 64 MiB | Upload cap |
| `MAX_UNPACKED_ZIP_BYTES` | 512 MiB | Unpacked ZIP cap |
| `ZIP_THRESHOLD_SIZE` | 100 KiB | Large ZIP by size |
| `ZIP_THRESHOLD_COUNT` | 100 | Large ZIP by entries |
| `LARGE_ZIP_ASYNC_UNPACK` | true | Async path |
| `WRITE_BUFFER_MAX_BYTES` | 4 MiB | Batch flush |
| `WRITE_BUFFER_INTERVAL` | 100 ms | Batch flush |
| `VERIFY_CHECKSUM` | true | CRC on read |
| `COMPRESSION_ENABLED` | true | Zstd XML |
| `DEDUP_BACKEND` | `postgres` | `postgres`\|`memory`\|`rocksdb` |
| `SHARD_MAX_BYTES` | 500 GiB | Seal threshold |
| `CLUSTER_KEY` | unset | Admin/internal secret |
| `COORDINATOR_URL` | unset | Auto-register shard |
| `MIN_FREE_DISK_BYTES` | 0 | Disk gate (0=off) |

Полный список — [implementation.md §15](implementation.md).

---

## 14. Критерии приёмки (acceptance)

Система соответствует master-spec, если:

1. **Dedup transparency:** два одинаковых POST → два разных `package_id`, одинаковые bytes на GET; один physical blob (`ref_count≥2`).
2. **XML round-trip:** upload XML → GET original/file → byte-identical payload.
3. **Small ZIP:** original + members доступны; duplicate member XML across packages не дублирует blob.
4. **Large ZIP:** `201` с `pending`; после unpack members появляются; crash mid-unpack восстанавливается scan'ом.
5. **Limits:** body > max → `413`; unpacked ZIP > max → controlled failure (не OOM).
6. **Integrity:** повреждённый CRC на read детектится при `VERIFY_CHECKSUM=true`.
7. **Single-node smoke:** `make docker-single-node` + POST/GET проходит.
8. **Sharded smoke:** write через coordinator; `GET /v1/admin/shards` показывает active; seal-rotate переключает writer.
9. **Hot-add:** новый shard регистрируется как standby; после rotate принимает writes.
10. **Disk gate:** при низком free space write → `507`.
11. **Internal auth:** без cluster key internal seal/segments недоступны.
12. **Replica:** `SHARD_ROLE=replica` отклоняет public POST.

---

## 15. Дорожная карта относительно фаз

| Фаза | Содержание | Статус относительно кода |
|------|------------|--------------------------|
| 1 | Segments + PG + HTTP + StoreOrRef | Done |
| 2 | WriteBuffer + Zstd dictionary | Done |
| 3 | Bloom + RocksDB hot index | Done (default backend postgres; rocksdb в compose) |
| 4 | Coordinator, volume sharding, replica sync, hot-add | Done (MVP); auto-failover primary promote — future |
| 5+ | Compaction, archived tier, xml hash API, hardened auth edge | Backlog |

---

## 16. Известные ограничения (зафиксированы)

1. Нет публичного XML-hash lookup (`global_xml_index` stub).
2. Coordinator→shard public proxy без cluster key (OK только в private network).
3. Segment files без CRC trailer от pre-CRC версий несовместимы.
4. Orphan segment bytes не компактируются online.
5. Filename в `Content-Disposition` требует дополнительного sanitization для hardened prod.
6. Cross-seal duplicate physical copies допустимы by design.

---

## 17. Связанные документы

| Документ | Роль |
|----------|------|
| [../.ai/master-spec.yaml](../.ai/master-spec.yaml) | **Agent SoT**: persona, guardrails, anchors, verification |
| [../.cursor/rules/master-spec.mdc](../.cursor/rules/master-spec.mdc) | Always-on Cursor pointer + hard-rules summary |
| [architecture.md](architecture.md) | Концепция, trade-offs, исторические алгоритмы |
| [implementation.md](implementation.md) | As-built: последовательности, storage, FT, security |
| [sharding-model.md](sharding-model.md) | Volume seal, hot-add, mirroring |
| [stack.md](stack.md) | Технологический стек |
| [test-stand.md](test-stand.md) | Локальный стенд и сценарии |
| [pilot-stand.md](pilot-stand.md) | Пилотная эксплуатация |
| [hot-add-smoke-test.md](hot-add-smoke-test.md) | Smoke hot-add |
| [../README.md](../README.md) | Quick start |

---

## 18. Глоссарий

| Термин | Определение |
|--------|-------------|
| **Transparent dedup** | Клиент всегда видит новый package; shared storage скрыт |
| **Canonical package** | Первый package с данным `package_hash` |
| **Active shard** | Единственный writer в кластере |
| **Seal** | Перевод шарда в read-only по объёму |
| **StoreOrRef** | Записать blob или увеличить `ref_count` |
| **Logical bytes** | Payload до compression / как у клиента |
| **Global package id** | 64-bit: shard (16) + local (48) |
| **raw_large** | Large ZIP до завершения async unpack |

---

**Владелец изменений:** изменения API, инвариантов (§6) или acceptance (§14) требуют обновления **`.ai/master-spec.yaml`** и этого `docs/master-spec.md` в том же PR/коммите, что и код (плюс сводку в `.cursor/rules/master-spec.mdc`, если меняются hard rules).
