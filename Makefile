.PHONY: build test run migrate docker-up rebuild-index

build:
	go build -o bin/server ./cmd/server
	go build -o bin/rebuild-index ./cmd/rebuild-index

build-rocksdb:
	go build -tags rocksdb -o bin/server ./cmd/server
	go build -tags rocksdb -o bin/rebuild-index ./cmd/rebuild-index

test:
	go test ./...

run: build
	PG_DSN=postgres://lbf:lbf@localhost:5432/lbf?sslmode=disable DATA_DIR=./data/segments ./bin/server

migrate:
	go run ./cmd/server

rebuild-index:
	go run ./cmd/rebuild-index

docker-up:
	docker compose -f deploy/docker-compose.yml up -d
