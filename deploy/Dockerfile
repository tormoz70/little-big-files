# Production image with RocksDB (optional). Default dev build uses memory backend.
FROM golang:1.22-bookworm AS builder
RUN apt-get update && apt-get install -y librocksdb-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -tags rocksdb -o /server ./cmd/server
RUN go build -tags rocksdb -o /rebuild-index ./cmd/rebuild-index

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y librocksdb7.8 ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /server /app/server
COPY --from=builder /rebuild-index /app/rebuild-index
COPY migrations /app/migrations
ENV DEDUP_BACKEND=rocksdb
EXPOSE 8080
ENTRYPOINT ["/app/server"]
