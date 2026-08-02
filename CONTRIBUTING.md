# Contributing

Thanks for your interest in little-big-files.

## Development setup

Requirements: Go 1.22+, Docker, Make.

```bash
git clone https://github.com/tormoz70/little-big-files.git
cd little-big-files
make build
make test
```

### Local stack

```bash
make docker-single-node   # coordinator + one shard
make docker-local         # 3-shard test stand with observability
```

### Integration tests

```bash
make docker-up
PG_DSN=postgres://lbf:lbf@localhost:5432/lbf?sslmode=disable make test-integration
```

## Pull requests

1. Fork the repo and create a branch from `main`.
2. Keep changes focused; one logical change per PR.
3. Run `make test` (and integration tests if you touch metadata/storage paths).
4. Update docs when behavior or configuration changes.
5. Describe what changed and how you tested it in the PR body.

## Reporting issues

Use the issue templates for bugs and feature requests. Include:

- steps to reproduce
- expected vs actual behavior
- relevant logs, env vars, and deployment profile (`docker-single-node`, `docker-sharded`, etc.)

## Code style

Follow existing patterns in the codebase. Run `go fmt ./...` before committing.
