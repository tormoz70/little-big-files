FROM golang:1.22-bookworm AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /server ./cmd/server
RUN go build -o /coordinator ./cmd/coordinator
RUN go build -o /shard-sync ./cmd/shard-sync
RUN go build -o /rebuild-index ./cmd/rebuild-index
RUN go build -o /recovery-tool ./cmd/recovery-tool

FROM debian:bookworm-slim AS server
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /server /app/server
COPY migrations /app/migrations
COPY examples /app/examples
EXPOSE 8080
ENTRYPOINT ["/app/server"]

FROM debian:bookworm-slim AS coordinator
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /coordinator /app/coordinator
COPY migrations/coordinator /app/migrations/coordinator
COPY deploy/shards.bootstrap.json /app/deploy/shards.bootstrap.json
EXPOSE 8080
ENTRYPOINT ["/app/coordinator"]

FROM debian:bookworm-slim AS shard-sync
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /shard-sync /app/shard-sync
ENTRYPOINT ["/app/shard-sync"]

# Legacy single-node image with optional RocksDB (build: --target server-rocksdb --build-arg TAGS=rocksdb)
FROM builder AS builder-rocksdb
RUN apt-get update && apt-get install -y librocksdb-dev
RUN go build -tags rocksdb -o /server ./cmd/server

FROM debian:bookworm-slim AS server-rocksdb
RUN apt-get update && apt-get install -y librocksdb7.8 ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder-rocksdb /server /app/server
COPY migrations /app/migrations
COPY examples /app/examples
ENV DEDUP_BACKEND=rocksdb
ENTRYPOINT ["/app/server"]
