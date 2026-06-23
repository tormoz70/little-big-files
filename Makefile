.PHONY: build test run migrate docker-up

build:
	go build -o bin/server ./cmd/server

test:
	go test ./...

run: build
	PG_DSN=postgres://lbf:lbf@localhost:5432/lbf?sslmode=disable DATA_DIR=./data/segments ./bin/server

migrate:
	go run ./cmd/server

docker-up:
	docker compose -f deploy/docker-compose.yml up -d
