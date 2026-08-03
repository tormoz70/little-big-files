# little-big-files

[![CI](https://github.com/tormoz70/little-big-files/actions/workflows/ci.yml/badge.svg)](https://github.com/tormoz70/little-big-files/actions/workflows/ci.yml)
[![Go](https://img.shields.io/badge/Go-1.22-00ADD8?logo=go&logoColor=white)](https://go.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![GitHub stars](https://img.shields.io/github/stars/tormoz70/little-big-files?style=social)](https://github.com/tormoz70/little-big-files/stargazers)

**Content-addressed Go storage for high-volume XML/ZIP ingestion with transparent deduplication and volume-based sharding.**

Clients always receive a new `package_id` on upload; identical XML blobs are stored once behind the scenes. In workloads with repeated XML across suppliers and packages, physical storage savings can reach **70–90%**. Built for write-heavy, read-rare pipelines (~1000 pkg/s peak) with optional coordinator-based sharding, hot-add, and Prometheus observability.

## Why little-big-files

- **Transparent dedup** — every upload gets a new logical package; shared content is invisible to clients
- **Content-addressed segments** — append-only storage with Bloom filter + hash index (memory, PostgreSQL, or RocksDB)
- **ZIP-aware ingestion** — stores original archive plus unpacked members; large ZIPs unpack asynchronously
- **Zstd dictionary compression** — tuned for small XML payloads
- **Volume-based sharding** — rolling seal when a shard fills; coordinator routes writes to the active shard
- **Single-node or clustered** — one-command Docker profiles from dev to multi-shard test stand
- **Operational guardrails** — disk-space write gate (`507`), CRC32C verification on read, recovery tooling

## Architecture

```mermaid
flowchart LR
  clients[Clients]
  coord[Coordinator :8080]
  active[Active shard primary]
  sealed[Sealed shards]
  replica[Optional replicas]

  clients --> coord
  coord --> active
  active -->|"seal on volume limit"| sealed
  sealed --> replica
  coord -->|"reads"| sealed
  coord -->|"reads"| replica
```

Write path: clients → **Coordinator** → active shard primary. When a shard exceeds its volume limit it is sealed; reads from sealed shards prefer **replica_url** when configured, otherwise the sealed primary serves reads.

Details: [architecture.md](docs/architecture.md) · [sharding-model.md](docs/sharding-model.md) · [implementation.md](docs/implementation.md)

## Quick start

Single-node profile (coordinator + one shard, good for local dev):

```bash
make docker-single-node
```

Upload and read back:

```bash
curl -X POST "http://localhost:8080/v1/packages?supplier_id=1" \
  -H "Content-Type: application/xml" \
  -d '<?xml version="1.0"?><doc/>'

# Response: {"package_id":"...","file_id":"..."}
curl "http://localhost:8080/v1/packages/{package_id}"
```

PowerShell:

```powershell
$body = '<?xml version="1.0"?><doc/>'
$r = Invoke-RestMethod -Method Post -Uri "http://localhost:8080/v1/packages?supplier_id=1" -ContentType "application/xml" -Body $body
Invoke-RestMethod -Uri "http://localhost:8080/v1/packages/$($r.package_id)"
```

### Other deployment profiles

| Profile | Command | Use case |
|---------|---------|----------|
| Single-node | `make docker-single-node` | Dev, pilot, one host |
| Sharded | `make docker-sharded` | Multi-shard coordinator routing |
| Sharded + replica | `make docker-sharded-replica` | Primary/replica segment sync |
| Local test stand | `make docker-local` | 3 shards × 50 MB + Grafana/Prometheus |

Sharded smoke test:

```bash
make docker-sharded
curl -X POST "http://localhost:8080/v1/packages?supplier_id=1" -d '<?xml version="1.0"?><doc/>'
curl http://localhost:8080/v1/admin/shards
```

Local stand with Python upload client: `make docker-local && make stand-upload` — see [clients/python/README.md](clients/python/README.md).

## Configuration

Key environment variables (full list in [implementation.md](docs/implementation.md)):

| Variable | Default | Description |
|----------|---------|-------------|
| `PG_DSN` | `postgres://lbf:lbf@localhost:5432/lbf?sslmode=disable` | Shard metadata database |
| `DATA_DIR` | `./data/segments` | Segment files on disk |
| `HTTP_ADDR` | `:8080` | Listen address |
| `DEDUP_BACKEND` | `postgres` | `postgres`, `memory` (tests/dev), or `rocksdb` (Docker `server-rocksdb` image) |
| `MAX_UNPACKED_ZIP_BYTES` | `512MiB` | Max total uncompressed ZIP member bytes per unpack |
| `DEPLOYMENT_MODE` | auto | `single-node` or `sharded` |
| `COORDINATOR_URL` | unset | Enables shard auto-registration |
| `CLUSTER_KEY` | unset | Shared secret for admin/internal endpoints |
| `SHARD_MAX_BYTES` | 500 GB | Volume limit before seal |
| `MIN_FREE_DISK_BYTES` | `0` | Disk gate threshold (`507` when exceeded) |
| `COMPRESSION_ENABLED` | `true` | Zstd dictionary compression for XML |

Coordinator and shard-specific variables (`COORDINATOR_PG_DSN`, `SHARD_UUID`, `SYNC_PRIMARY_URL`, etc.) are documented in [test-stand.md](docs/test-stand.md).

## API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/packages?supplier_id=` | Upload XML or ZIP; always `201` with new IDs |
| `GET` | `/v1/packages/{id}` | Package metadata |
| `GET` | `/v1/packages/{id}/files/{file_id}` | File content |
| `GET` | `/v1/packages/{id}/original` | Original uploaded bytes |
| `GET` | `/v1/admin/shards` | Shard registry (sharded mode) |

Admin hot-add and seal-rotate APIs: [implementation.md](docs/implementation.md) · operational runbooks: [test-stand.md](docs/test-stand.md)

## Build & test

```bash
make build
make test
make test-coverage
```

Integration tests (requires PostgreSQL):

```bash
make docker-up
PG_DSN=postgres://lbf:lbf@localhost:5432/lbf?sslmode=disable make test-integration
```

## Documentation

- [test-stand.md](docs/test-stand.md) — deployment, scenarios, seal, troubleshooting
- [pilot-stand.md](docs/pilot-stand.md) — pilot VM operations
- [implementation.md](docs/implementation.md) — architecture, storage, fault tolerance, security
- [architecture.md](docs/architecture.md) — design rationale and data flow
- [stack.md](docs/stack.md) — technology choices
- [sharding-model.md](docs/sharding-model.md) — volume sharding model

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Bug reports and feature requests welcome via GitHub Issues.

## License

[MIT](LICENSE)
