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
| `VERIFY_CHECKSUM` | `true` — verify per-record CRC32C on read |
| `UNPACK_RECOVER_INTERVAL` | `1m` — re-scan packages stuck in `raw_large` and re-enqueue unpack |
| `COMPRESSION_ENABLED` | `true` — Zstd dictionary compression for XML |
| `COMPRESSION_MIN_SIZE` | `64` — minimum payload size to compress |
| `EXAMPLES_DIR` | `./examples` — ZIP samples for dictionary training |
| `DEDUP_BACKEND` | `memory` — `memory`, `postgres` (PG-only), `rocksdb` (needs `-tags rocksdb`) |
| `ROCKSDB_PATH` | `./data/rocksdb` |
| `BLOOM_EXPECTED_ITEMS` | `1000000` |
| `BLOOM_FALSE_POSITIVE` | `0.001` |
| `DEDUP_REBUILD_ON_START` | `true` — reload Bloom+index from `content_blobs` |

## Phase 4: sharded test stand

Volume-based sharding with Coordinator. Replica per shard is **optional** (Docker profile `replica`).

```bash
make docker-sharded
curl -X POST "http://localhost:8080/v1/packages?supplier_id=1" -d '<?xml version="1.0"?><doc/>'
curl http://localhost:8080/v1/admin/shards
```

With primary/replica mirroring and HTTP segment sync:

```bash
make docker-sharded-replica
```

### Local stand: 3 shards × 50 MB + Python clients

```bash
make docker-local
make stand-upload
# or: python clients/python/upload_examples.py --wait --repeat 300
```

With replicas: `make docker-local-replica`

Ten test suppliers (`1001`–`1010`), example ZIPs from `examples/`. Grafana `:3000` (admin/lbf), Prometheus `:9090`. See [clients/python/README.md](clients/python/README.md) and [docs/test-stand.md](docs/test-stand.md#11-локальный-стенд-3-шарда--50-mb).

| Variable | Default | Role |
|----------|---------|------|
| `COORDINATOR_PG_DSN` | coordinator PG | Global index |
| `CLUSTER_KEY` | unset | Shared secret for shard registration/admin mutation |
| `COORDINATOR_BOOTSTRAP` | `./deploy/shards.bootstrap.json` | Shard registry |
| `SHARD_MAX_BYTES` | 500 GB | Seal trigger |
| `SEAL_CHECK_INTERVAL` | `30s` | Auto seal poll |
| `SHARD_ID` | `0` | Shard instance id |
| `SHARD_ROLE` | `primary` | `primary` / `replica` |
| `SHARD_READ_ONLY` | `false` | Sealed / replica writes blocked |
| `SHARD_UUID` | unset | Stable shard identity for startup registration |
| `SHARD_CLUSTER_KEY` | unset | Cluster key sent by shard to coordinator |
| `SHARD_ADVERTISE_URL` | unset | Coordinator-facing URL for this shard |
| `SHARD_STARTUP_STATE` | `standby` | Desired initial state on first registration |
| `COORDINATOR_URL` | unset | Coordinator base URL for shard auto-registration |
| `SYNC_PRIMARY_URL` | — | Replica segment sync source (shard-sync) |

Architecture: clients → **Coordinator :8080** → active shard primary. Reads from sealed shards use **replica_url** when set in bootstrap; otherwise the sealed **primary** serves reads.

Hot-add and manual switching API:

- `POST /v1/admin/shards` — shard startup registration / idempotent upsert by `shard_uuid`
- `PATCH /v1/admin/shards/{id}/state` — safe manual state transition (`standby -> active` requires `confirm=true`)

Shard-internal endpoints (`/v1/internal/*`: stats, seal, raw segment download/sync) require the cluster key, sent as `X-Cluster-Key: <key>` (or `Authorization: Bearer <key>`). The key is taken from `CLUSTER_KEY`, falling back to `SHARD_CLUSTER_KEY`. If neither is configured these endpoints are disabled (`503`). `shard-sync` sends the key automatically.

**Подробное описание стенда:** [docs/test-stand.md](docs/test-stand.md) — VM, контейнеры, сценарии проверки, seal, troubleshooting.

See also [sharding-model.md](docs/sharding-model.md).

## Build & test

```bash
make build
make test
make test-coverage
```

Integration tests (PostgreSQL):

```bash
make docker-up
PG_DSN=postgres://lbf:lbf@localhost:5432/lbf?sslmode=disable make test-integration
```

## Docs

- [test-stand.md](docs/test-stand.md) — **тестовый стенд Ф4** (развёртывание, сценарии)
- [pilot-stand.md](docs/pilot-stand.md) — **опытная эксплуатация на ВМ** (параметры и инструкция)
- [architecture.md](docs/architecture.md)
- [stack.md](docs/stack.md)
- [sharding-model.md](docs/sharding-model.md)
