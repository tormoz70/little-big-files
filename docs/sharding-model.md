# Модель шардирования (volume-based)

> Обновлено под вводную: чтение редкое; шардирование по **объёму** (rolling seal), не по `supplier_id`. См. также [architecture.md](architecture.md) §7.

## Суть

- **Write** — всегда на один **active** шард (текущий поток данных)
- **Seal** — когда шард заполнен (`SHARD_MAX_BYTES`) → read-only, «остывает»
- **Read** — редко, с любого шарда через **Coordinator** + global index
- **Зеркалирование** — primary + replica на каждый шард (MVP: HTTP sidecar sync)

---

## 1. Жизненный цикл шарда

```mermaid
stateDiagram-v2
    [*] --> standby: provision
    standby --> active: activate
    active --> sealed: total_bytes >= SHARD_MAX_BYTES
    sealed --> archived: optional cold migrate
    archived --> [*]: retention policy
```

| State | Write | Read | Tier |
|-------|-------|------|------|
| `standby` | Нет | Нет | — |
| `active` | **Да (единственный)** | Да | NVMe |
| `sealed` | Нет | Да (редко) | HDD |
| `archived` | Нет | Очень редко | S3 / object storage |

---

## 2. Физическая модель

```mermaid
flowchart TB
    client[Clients] --> coord[Coordinator :8080]

    subgraph coord_layer [Coordinator]
        gpi[(global_package_index)]
        sr[shard_registry]
    end

    coord --> gpi
    coord --> sr

    coord -->|write| activeP[Shard 2 Primary]
    coord -->|read rare| sealedP[Shard 0 Primary]
    coord -->|read rare| sealedR[Shard 0 Replica]

    activeP -.->|mirror| activeR[Shard 2 Replica]
    sealedP -.->|mirror| sealedR
```

---

## 3. Write path

```mermaid
sequenceDiagram
    participant Client
    participant Coord as Coordinator
    participant Active as Active Shard Primary
    participant Idx as Coordinator PG

    Client->>Coord: POST /v1/packages
    Coord->>Coord: resolve ActiveShard()
    Coord->>Active: proxy write
    Active-->>Coord: local_id + stats
    Coord->>Idx: insert global_package_index
    Coord-->>Client: global_package_id
```

Все поставщики пишут на **один** active шард → дедупликация работает **между suppliers** в текущем потоке.

---

## 4. Seal и ротация

```mermaid
sequenceDiagram
    participant Coord as Coordinator
    participant Old as Active Shard
    participant New as Standby Shard

    Old->>Coord: total_bytes >= SHARD_MAX_BYTES
    Coord->>Old: seal (read-only, fsync, checkpoint)
    Coord->>Coord: shard_registry: old→sealed
    Coord->>New: activate
    Note over Coord,New: Новые writes только на New
```

**Добавление шарда = seal + activate.** Rehash и миграция данных не нужны.

---

## 4.1. Hot-add нового шарда

```mermaid
sequenceDiagram
    participant Admin as Администратор
    participant Shard as Новый шард
    participant Coord as Coordinator
    participant CPG as Coordinator DB
    participant Active as Текущий active shard

    Admin->>Shard: Подготовить VM и установить софт
    Admin->>Shard: Настроить CLUSTER_KEY, COORDINATOR_URL, SHARD_UUID, SHARD_ADVERTISE_URL
    Admin->>Shard: Запустить shard
    Shard->>Shard: Инициализация storage, DB, HTTP server
    Shard->>Coord: POST /v1/admin/shards<br/>{shard_uuid, cluster_key, primary_url, startup_state=standby}
    Coord->>Coord: Проверить cluster_key и shard_uuid
    Coord->>CPG: Найти shard по shard_uuid

    alt UUID уже зарегистрирован
        CPG-->>Coord: Existing shard row
        Coord->>CPG: Обновить URLs и last_seen_at
        Coord-->>Shard: 200 OK<br/>{shard_id, state}
    else Новый UUID
        Coord->>CPG: INSERT shard_registry<br/>state=standby
        CPG-->>Coord: Назначен shard_id
        Coord-->>Shard: 201 Created<br/>{shard_id, state=standby}
    end

    Shard->>Shard: Применить назначенный shard_id
    Shard-->>Admin: Запущен как standby

    Note over Admin,Shard: Роль администратора завершена после запуска shard
    Note over Coord,Shard: Hot-add завершен: shard в кластере как standby

    Coord->>Active: Позже, при ротации: POST /v1/internal/seal
    Active->>Active: Перейти в read-only
    Active-->>Coord: 200 OK, sealed

    Coord->>Shard: Позже, при ротации: GET /v1/internal/stats
    Shard-->>Coord: 200 OK, reachable standby
    Coord->>CPG: Transaction:<br/>old active -> sealed<br/>standby -> active
    CPG-->>Coord: Commit OK

    Note over Coord,Shard: Новые writes идут на добавленный shard после ротации
```

Hot-add безопасный путь: администратор подготавливает VM и запускает shard с `CLUSTER_KEY`, `COORDINATOR_URL`, `SHARD_UUID` и `SHARD_ADVERTISE_URL`. Новый shard при старте сам регистрируется в Coordinator и автоматически входит в кластер как `standby`. Coordinator отклоняет startup registration с `startup_state=active`/`sealed`, чтобы исключить обход ротации. Если `active` отсутствует, Coordinator автоматически promotes первый reachable `standby`; иначе новый shard остается `standby` до обычной ротации, где Coordinator sealed текущий `active` и переводит standby в `active`.

---

## 5. Read path (редкий)

```mermaid
flowchart TD
    req[Read Request] --> type{Тип}

    type -->|GET /packages/id| decode["shard_id = id >> 48"]
    decode --> proxy[Proxy на shard primary/replica]

    type -->|GET /xml/hash| noxml[Not implemented in MVP]

    type -->|query by supplier+time| gpi[global_package_index]
    gpi --> proxy
```

Чтение редкое → fan-out по всем sealed шардам допустим как fallback.

---

## 6. Глобальный package_id

```
┌────────────────┬────────────────────────────────────────────────┐
│  shard_id      │           local_package_id                       │
│  16 bit        │           48 bit                                 │
└────────────────┴────────────────────────────────────────────────┘
```

---

## 7. Зеркалирование

| Компонент | Primary → Replica |
|-----------|-------------------|
| PostgreSQL | Shared DB в local/test стенде; streaming replication — target |
| Segments | HTTP `shard-sync` sidecar |
| RocksDB | Rebuild на replica при старте |
| Bloom | Rebuild из metadata при старте |

- **Write** → primary active shard only
- **Read** (конфликты, анализ) → replica sealed shard (меньше нагрузка)

---

## 8. Дедупликация

| Сценарий | Поведение |
|----------|-----------|
| Два supplier, один XML, active shard | 1 копия (dedup) |
| Тот же XML после seal на новом active | 2 копии (приемлемо) |
| Чтение старого XML | В MVP только по `global_id`; XML hash lookup не реализован |

---

## 9. Отличие от supplier_id sharding

| | supplier_id % N | volume-based (текущая модель) |
|--|-----------------|--------------------------------|
| Добавление шарда | Rehash + миграция | Seal + activate |
| Cross-supplier dedup | Только внутри шарда | Внутри active (все suppliers) |
| «Остывание» | Нет естественного | Sealed = cold |
| Чтение | Нужен supplier_id для XML | Global index / fan-out |
| Hot-spot | По supplier | По времени (один writer) |

---

## 10. Конфигурация

```
SHARD_MAX_BYTES=536870912000   # 500 GB
COORDINATOR_PG_DSN=...
SHARD_ROLE=primary|replica
```
