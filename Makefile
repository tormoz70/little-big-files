.PHONY: build test run migrate migrate-all docker-up rebuild-index

build:
	go build -o bin/server ./cmd/server
	go build -o bin/coordinator ./cmd/coordinator
	go build -o bin/rebuild-index ./cmd/rebuild-index
	go build -o bin/recovery-tool ./cmd/recovery-tool
	go build -o bin/shard-sync ./cmd/shard-sync

build-rocksdb:
	go build -tags rocksdb -o bin/server ./cmd/server
	go build -tags rocksdb -o bin/rebuild-index ./cmd/rebuild-index

test:
	go test ./...

test-coverage:
	go test ./... -coverprofile=coverage.out
	go tool cover -func=coverage.out

test-integration:
	go test -tags integration ./internal/metadata/... -count=1

run: build
	PG_DSN=postgres://lbf:lbf@localhost:5432/lbf?sslmode=disable DATA_DIR=./data/segments ./bin/server

migrate:
	go run ./cmd/migrate --metadata

migrate-all:
	go run ./cmd/migrate --metadata --coordinator

rebuild-index:
	go run ./cmd/rebuild-index

docker-up:
	docker compose -f deploy/docker-compose.yml up -d

docker-sharded:
	docker compose -f deploy/docker-compose.sharded.yml up -d --build

docker-single-node:
	docker compose -f deploy/docker-compose.single-node.yml up -d --build

docker-sharded-replica:
	docker compose -f deploy/docker-compose.sharded.yml -f deploy/docker-compose.sharded.replica.yml --profile replica up -d --build

docker-local:
	docker compose -f deploy/docker-compose.local.yml up -d --build

docker-local-replica:
	docker compose -f deploy/docker-compose.local.yml -f deploy/docker-compose.local.replica.yml --profile replica up -d --build

docker-local-down:
	docker compose -f deploy/docker-compose.local.yml down -v

stand-reset: docker-local-down docker-local

stand-upload:
	cd clients/python && pip install -q -r requirements.txt && python upload_examples.py --wait

stand-upload-ekb:
	cd clients/python && pip install -q -r requirements.txt && python upload_ekb_work.py --wait --verify-read --count 100
