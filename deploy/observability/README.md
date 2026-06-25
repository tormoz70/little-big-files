# Observability (локальный стенд)

Prometheus + Grafana для мониторинга Coordinator и shard-сервисов.

## Запуск

Входит в локальный стенд:

```bash
make docker-local
```

| Сервис     | URL                         | Логин        |
|------------|-----------------------------|--------------|
| Grafana    | http://localhost:3000       | admin / lbf  |
| Prometheus | http://localhost:9090       | —            |
| Metrics    | http://localhost:8080/metrics | Coordinator |

## Что собирается

### HTTP (Coordinator + shards)

- `lbf_http_requests_total{method,route,code}`
- `lbf_http_request_duration_seconds{method,route}`

### Shard

- `lbf_shard_total_bytes{shard_id,role}` — размер segment-файлов на диске
- `lbf_shard_read_only{shard_id,role}` — 1 если шард sealed/read-only
- `lbf_shard_blob_logical_bytes` — SUM(`content_blobs.size`), несжатый размер уникальных blob
- `lbf_shard_blob_stored_bytes` — SUM(`stored_size`), байты записей на диске (header + payload)
- `lbf_shard_blob_referenced_logical_bytes` — SUM(`size * ref_count`), логический объём с dedup-ссылками
- `lbf_shard_compression_ratio` — `logical / stored` (legacy, >1 = сжатие помогло)
- `lbf_shard_compression_savings_percent` — **экономия в %**: `(1 − stored/logical) × 100`
- `lbf_shard_storage_efficiency_ratio` — `referenced_logical / segment_bytes` (dedup + compression)

```promql
# экономия от сжатия, % (удобнее ratio 1.80 → ~44%)
lbf_shard_compression_savings_percent{job="shards", role="primary"}
```

### Coordinator (только на Coordinator, отдельный Prometheus registry)

- `lbf_coordinator_shard_info{shard_id,state}` — реестр шардов
- `lbf_coordinator_shard_bar_bytes{shard_id,state}` — последние 4 шарда по `shard_id` (панель **Last 4 shards by state**, кольцевые gauge)
- `lbf_coordinator_active_shard_id` — текущий active shard (метрика в Prometheus, на дашборде не выводится)
- `lbf_coordinator_shard_max_bytes` — порог seal (50 MB на локальном стенде)

Coordinator-метрики **не** отдаются shard-серверами — только `GET /metrics` на Coordinator.

## Grafana

Дашборд **LBF Local Stand** подхватывается автоматически из `grafana/dashboards/lbf-local.json`.

Полезные запросы в Prometheus UI:

```promql
lbf_coordinator_shard_bytes{state="active"} / lbf_coordinator_shard_max_bytes
sum(rate(lbf_http_requests_total{job="coordinator",route="/v1/packages"}[1m]))
```

## Конфигурация

- [prometheus.yml](prometheus.yml) — scrape coordinator + 6 shard instances
- [grafana/provisioning/](grafana/provisioning/) — datasource и dashboards

Перезагрузка Prometheus после правки конфига:

```bash
curl -X POST http://localhost:9090/-/reload
```
