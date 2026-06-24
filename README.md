# little-big-files

Content-addressed storage for EKB XML/ZIP packages with transparent deduplication.

## Features

- `POST /v1/packages?supplier_id=` — always returns `201` with new `package_id` / `file_id`
- Duplicate uploads share physical blobs (invisible to clients)
- ZIP: stores original + unpacked members (small ZIP sync; **large ZIP async** — see below)
- `GET /v1/packages/{id}`, `/files/{file_id}`, `/original`

## Quick start

```bash
docker compose -f deploy/docker-compose.yml up -d
go run ./cmd/server
```

Environment:

| Variable | Default |
|----------|---------|
| `PG_DSN` | `postgres://lbf:lbf@localhost:5432/lbf?sslmode=disable` |
| `DATA_DIR` | `./data/segments` |
| `HTTP_ADDR` | `:8080` |
| `LARGE_ZIP_ASYNC_UNPACK` | `true` — background unpack for large ZIP |
| `UNPACK_WORKERS` | `2` |
| `ZIP_THRESHOLD_SIZE` | `102400` (100 KB) |
| `ZIP_THRESHOLD_COUNT` | `100` files |
| `WRITE_BUFFER_MAX_BYTES` | `4194304` (4 MB) — batch segment writes |
| `WRITE_BUFFER_INTERVAL` | `100ms` — max delay before flush |
| `COMPRESSION_ENABLED` | `true` — Zstd dictionary compression for XML |
| `COMPRESSION_MIN_SIZE` | `64` — minimum payload size to compress |
| `EXAMPLES_DIR` | `./examples` — ZIP samples for dictionary training |
| `DEDUP_BACKEND` | `memory` — `memory`, `postgres` (PG-only), `rocksdb` (needs `-tags rocksdb`) |
| `ROCKSDB_PATH` | `./data/rocksdb` |
| `BLOOM_EXPECTED_ITEMS` | `1000000` |
| `BLOOM_FALSE_POSITIVE` | `0.001` |
| `DEDUP_REBUILD_ON_START` | `true` — reload Bloom+index from `content_blobs` |

## Build & test

```bash
make build
make test
```

## Docs

- [architecture.md](docs/architecture.md)
- [stack.md](docs/stack.md)
- [sharding-model.md](docs/sharding-model.md)
