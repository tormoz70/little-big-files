# Технологический стэк

Актуальная сводка по реализации **little-big-files**. Детали API и алгоритмов — в [architecture.md](architecture.md) и плане реализации.

## Язык и рантайм

| Компонент | Выбор |
|-----------|-------|
| Язык | **Go 1.21+** |
| Сборка | `go mod`, Makefile |
| Контейнеры | Docker multi-stage (CGO для RocksDB) |
| Локальная инфра | `docker-compose` — PostgreSQL 16, volumes для сегментов |

## Хранение

| Слой | Технология | Назначение |
|------|------------|------------|
| Физические данные | Append-only **сегменты** (NVMe / HDD) | Blobs: ZIP, XML, `_unpack_error.txt` |
| Метаданные | **PostgreSQL 14+** | `packages`, `package_files`, `content_blobs` |
| Hot index dedup | **RocksDB** | Кеш `content_hash → location` (Фаза 3) |
| Bloom filter | `bits-and-blooms/bloom/v3` | Предфильтр перед RocksDB / PG |
| Сжатие на диске | **Zstd** + dictionary | Фаза 2; клиенту отдаются оригинальные bytes |

PostgreSQL — **source of truth** для метаданных и `content_blobs`. RocksDB дублирует hot lookup и перестраивается из PG при старте.

## API

| | |
|---|---|
| Протокол | **HTTP REST** `/v1/...` |
| Роутер | `chi` или `net/http` + middleware |
| Формат | JSON (metadata) + raw bytes (download) |

**Вне scope:** gRPC, Kafka/Rabbit, async `202 Accepted`.

### Основные endpoints

- `POST /v1/packages?supplier_id=` — приём ZIP/XML, всегда `201`
- `GET /v1/packages/{id}` — manifest
- `GET /v1/packages/{id}/files/{file_id}` — bytes файла
- `GET /v1/packages/{id}/original` — shortcut на original
- `GET /metrics` — Prometheus

`supplier_id` — **только из запроса** (query/header), не из имени файла.

## Go-библиотеки

| Компонент | Библиотека |
|-----------|------------|
| HTTP | `go-chi/chi/v5` или stdlib |
| PostgreSQL | `jackc/pgx/v5`, `golang-migrate/migrate` |
| RocksDB | `linxGnu/grocksdb` (CGO, `librocksdb-dev`) |
| Zstd | `klauspost/compress/zstd` |
| Bloom | `bits-and-blooms/bloom/v3` |
| ZIP | `archive/zip` (stdlib) |
| Конфиг | `caarlos0/env` |
| Логи | `log/slog` |
| Тесты | `stretchr/testify`, `testcontainers/testcontainers-go` |

## Хеширование

| Ключ | Функция | Где |
|------|---------|-----|
| `package_hash` | **SHA-256** | Тело POST; lookup дубликата пакета |
| `content_hash` | **SHA-256** | Dedup blob (`StoreOrRef`) |

XXH3-128 **не используется** — единый SHA-256 для пакетов и контента.

## Наблюдаемость и нагрузка

| | |
|---|---|
| Метрики | Prometheus (`/metrics`) |
| Логи | Structured `slog` |
| Load balancer | nginx / HAProxy |
| Нагрузочные тесты | k6 / vegeta |
| Цель prod | **≥ 1000 POST/сек** (с WriteBuffer + RocksDB) |

Опционально: **PgBouncer** (transaction mode) перед PostgreSQL.

## Масштабирование (Фаза 4)

| Компонент | Технология |
|-----------|------------|
| Coordinator | Go-сервис + PostgreSQL global index |
| Шарды | Volume-based seal: active (NVMe) → sealed (HDD) |
| Репликация | PG streaming replication, sync сегментов (`rsync`/`lsyncd`), RocksDB checkpoint |

Подробнее: [sharding-model.md](sharding-model.md).

## Фазы внедрения стэка

| Фаза | Компоненты |
|------|------------|
| **1** | Go + PG + сегменты + HTTP API + `StoreOrRef` (PG lookup) |
| **2** | WriteBuffer (4 MB / 100 ms), Zstd dictionary |
| **3** | RocksDB + Bloom — **обязательно до prod 1000 pkg/s** |
| **4** | Coordinator, volume sharding, зеркалирование |

## Структура репозитория (план)

```
little-big-files/
├── cmd/server/           # HTTP API + ingestion
├── cmd/coordinator/      # Фаза 4
├── cmd/rebuild-index/    # Recovery RocksDB из PG
├── internal/
│   ├── api/
│   ├── ingestion/
│   ├── storage/          # segments, BlobStore.StoreOrRef
│   ├── metadata/         # pgx repository
│   ├── dedup/            # Bloom, RocksDB
│   └── compress/         # Zstd
├── migrations/
├── deploy/docker-compose.yml
└── docs/
```
