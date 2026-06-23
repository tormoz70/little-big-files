# Архитектура системы хранения XML-данных с дедупликацией

> **Реализация (Фаза 1):** content-addressed storage, прозрачная dedup, HTTP API `/v1/packages`. Стэк: [stack.md](stack.md). Модель шардов: [sharding-model.md](sharding-model.md).

## 1. Ключевые выводы из вводных

| Параметр | Значение |
|----------|----------|
| Основной объем | Сотни миллионов XML; типичный posted000 **~400–700 B**, outlier до **16 KB** |
| Частота | Высокая, потоковая (~**1000 pkg/s** на пике) |
| Пакеты | ZIP с 1 XML (~85%) или bulk ZIP (до ~85 XML) |
| Редкие кейсы | ZIP на несколько МБ с тысячами XML (пару раз в год) |
| **Критический паттерн** | **Прозрачная dedup:** клиент всегда получает новый `package_id`, физически — shared blobs |
| Удаления | Нет, только append |
| Требования | Хранить в оригинальном виде (текст); ZIP = original + members |
| **Чтение** | **Очень редко** — анализ, разбор конфликтов |
| **Шардирование** | По **объёму** на шарде (rolling seal); см. [sharding-model.md](sharding-model.md) |

> **Ключевой инсайт:** Дедупликация XML — это не оптимизация, а **основной механизм экономии**. Если одни и те же XML шлют разные поставщики в разных ZIP, экономия может достигать **70-90% объема**. Дедупликация на **active** шарде работает **между всеми поставщиками** в текущем потоке записи.

> **Паттерн доступа:** write-heavy, read-rare. Это позволяет использовать volume-based шардирование с глобальным индексом на Coordinator и чтением с любого sealed шарда без оптимизации hot-read path.

## 2. Общая архитектура

```
┌─────────────────────────────────────────────────────────────────┐
│                        CLIENTS / SUPPLIERS                       │
│              (XML files, ZIP packages via API)                   │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                     INGESTION SERVICE                            │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │ Protocol     │  │ ZIP          │  │ Routing &          │    │
│  │ Adapter      │→ │ Detector &   │→ │ Classification     │    │
│  │ (HTTP/gRPC)  │  │ Unpacker     │  │ (small/large ZIP)  │    │
│  └──────────────┘  └──────────────┘  └────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   DEDUPLICATION ENGINE                           │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │ Hash         │  │ Bloom Filter │  │ Hash Index         │    │
│  │ Calculator   │→ │ (fast check) │→ │ (RocksDB)          │    │
│  └──────────────┘  └──────────────┘  └────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                     STORAGE ENGINE                               │
│  ┌────────────────────────┐  ┌──────────────────────────────┐  │
│  │ Segment Manager        │  │ Write Buffer (batching)      │  │
│  │ (append-only segments) │  │ (memory → disk flush)        │  │
│  └────────────────────────┘  └──────────────────────────────┘  │
│                              ↓                                   │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ SEGMENTS (append-only files, 1-4 GB each)                  │ │
│  │ [XML1][XML2][XML3]...[XMLn][ZIP1]...[XMLn+1]...            │ │
│  └────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────────┐
│                   METADATA STORE (PostgreSQL)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────┐    │
│  │ packages     │  │ package_files│  │ content_blobs      │    │
│  └──────────────┘  └──────────────┘  └────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
```

## 3. Компоненты системы

### 3.1. Ingestion Service

**Назначение:** Прием входящих данных, нормализация, классификация.

**Алгоритм работы:**

1. Принимает данные через протокольный адаптер (HTTP REST/gRPC/очередь)
2. Определяет тип: одиночный XML или ZIP
3. Если ZIP — анализирует размер и количество файлов
4. Маршрутизирует в один из двух потоков:
   - **Small package flow** (распаковка + дедупликация)
   - **Large package flow** (хранение как есть)

**Критерии маршрутизации:**

```
IF zip_size > THRESHOLD_SIZE (100 KB) 
   OR file_count > THRESHOLD_COUNT (100):
    → Large package flow
ELSE:
    → Small package flow
```

### 3.2. Deduplication Engine

**Назначение:** Определение, является ли XML дубликатом уже сохраненного.

**Трехуровневая проверка (от быстрой к медленной):**

```
УРОВЕНЬ 1: Bloom Filter (O(1), ~10 ns)
  ↓ miss → новый файл
  ↓ hit  → УРОВЕНЬ 2
  
УРОВЕНЬ 2: Hash Index lookup (O(1), ~1-10 μs)
  ↓ not found → новый файл
  ↓ found     → УРОВЕНЬ 3
  
УРОВЕНЬ 3: Size verification (O(1), ~100 ns)
  ↓ size differs → коллизия хеша → новый файл
  ↓ size matches → ДУБЛИКАТ (используем ссылку)
```

### 3.3. Storage Engine

**Назначение:** Физическая запись и чтение данных в append-only сегменты.

**Ключевые свойства:**

- Только append операции (никаких update/delete)
- Batched writes (буферизация в памяти перед сбросом на диск)
- Sequential I/O (максимальная пропускная способность диска)

### 3.4. Metadata Store (PostgreSQL)

**Назначение:** Хранение бизнес-метаданных, связей пакет-XML, статистики.

**Не хранит:** указатели на физическое расположение XML (это делает Hash Index в RocksDB для скорости).

**В распределённой конфигурации (§7.2):** на каждом шарде — локальная PostgreSQL (метаданные шарда). На **Coordinator** — отдельная легковесная PostgreSQL с глобальным индексом (`global_package_index`, `global_xml_index`, `shard_registry`) для маршрутизации редких read-запросов.

## 4. Алгоритмы

### 4.1. Алгоритм хеширования

| Ключ | Функция | Назначение |
|------|---------|------------|
| `package_hash` | **SHA-256** тела POST | Package-level dedup (clone refs) |
| `content_hash` | **SHA-256** bytes blob | Blob-level dedup (`StoreOrRef`) |

### 4.2. StoreOrRef (Write Path)

```
FUNCTION StoreOrRef(data, record_type):
    content_hash = SHA-256(data)
    IF content_blobs.exists(content_hash):
        ref_count++
        RETURN content_hash
    location = segment.append(data)
    INSERT content_blobs(content_hash, location, ref_count=1)
    RETURN content_hash
```

### 4.3. Алгоритм обработки пакета (POST /v1/packages)

```
FUNCTION process_package(body, supplier_id):
    package_hash = SHA-256(body)

    IF canonical = packages.find_first(package_hash):
        new_id = packages.insert(supplier_id, canonical_package_id=canonical)
        clone package_files refs from canonical → ref_count++ on each blob
        RETURN 201 new_id   # без 409, без сигнала dedup клиенту

    IF payload is XML:
        hash = StoreOrRef(body)
        INSERT package + package_files(role=original)
        RETURN 201

    IF payload is ZIP:
        hash_zip = StoreOrRef(body)
        IF large (>100KB or >100 files):
            INSERT package(storage_mode=raw_large) + original only
        ELSE:
            TRY unpack ZIP
            IF fail:
                StoreOrRef(_unpack_error.txt)
                INSERT original + unpack_error; unpack_status=failed in JSON
            ELSE:
                FOR each member: StoreOrRef(member)
                INSERT original + members; unpack_status=ok
        RETURN 201
```

**Прозрачность:** клиент не видит `canonical_package_id`, `ref_count`, `content_hash`. Повторная загрузка → новые `package_id` / `file_id`, те же bytes на GET.

### 4.3.1. Алгоритм обработки пакета (ZIP) — legacy pseudocode

<details>
<summary>Устаревший pseudocode (до v1.2)</summary>

```
FUNCTION process_package(raw_data, supplier_id):
    package_hash = SHA-256(raw_data)  # для аудита
    
    # Проверяем, не получали ли мы уже этот ZIP
    IF metadata_store.package_exists(package_hash):
        RETURN {status: 'duplicate_package', existing_id: ...}
    
    # Определяем режим обработки
    IF is_zip(raw_data):
        file_count = zip_count_files(raw_data)
        zip_size = len(raw_data)
        
        IF zip_size > THRESHOLD_SIZE OR file_count > THRESHOLD_COUNT:
            RETURN process_large_zip(raw_data, supplier_id, package_hash)
        ELSE:
            RETURN process_small_zip(raw_data, supplier_id, package_hash)
    ELSE:
        # Одиночный XML
        RETURN process_single_xml(raw_data, supplier_id, package_hash)
```

</details>

### 4.4. Алгоритм обработки маленького ZIP (legacy)

```
FUNCTION process_small_zip(zip_data, supplier_id, package_hash):
    # Открываем транзакцию в metadata_store
    BEGIN TRANSACTION
    
    package_id = metadata_store.create_package(
        supplier_id, package_hash, storage_mode='unpacked'
    )
    
    # Буфер для batch write
    write_buffer = []
    xml_refs = []
    
    FOR each entry IN zip_entries(zip_data):
        xml_data = zip_read(entry)
        
        # Дедупликация
        result = process_xml(xml_data, package_id, entry.filename)
        xml_refs.append(result)
        
        # Если новый файл — добавляем в буфер записи
        IF result.status == 'new':
            write_buffer.append(xml_data)
    
    # Batch write всех новых XML одним проходом
    IF write_buffer IS NOT EMPTY:
        locations = storage_engine.batch_append(write_buffer)
        hash_index.batch_put(locations)
    
    metadata_store.finalize_package(package_id, xml_refs)
    COMMIT
    
    RETURN {
        status: 'ok', 
        package_id: package_id, 
        stats: {
            total: len(xml_refs), 
            new: count(status='new'), 
            duplicates: count(status='duplicate')
        }
    }
```

### 4.5. Алгоритм обработки большого ZIP

```
FUNCTION process_large_zip(zip_data, supplier_id, package_hash):
    # Стратегия: храним оригинал ZIP как единый объект
    location = storage_engine.append(zip_data)
    
    package_id = metadata_store.create_package(
        supplier_id, package_hash, 
        storage_mode='raw',
        location=location,
        note='Large package stored as-is'
    )
    
    # Опционально: асинхронная фоновая распаковка
    IF async_processing_enabled:
        enqueue_background_task('unpack_and_index_large_zip', package_id)
    
    RETURN {status: 'ok', package_id: package_id, mode: 'raw'}
```

### 4.6. Алгоритм чтения (Read Path)

```
FUNCTION read_xml(xml_key):
    # xml_key — это бизнес-ключ или hash
    location = hash_index.get(xml_key)
    
    IF location IS NULL:
        RETURN {status: 'not_found'}
    
    # Single read по известному offset
    data = storage_engine.read(
        location.segment_id, 
        location.offset, 
        location.size
    )
    
    # Опциональная проверка целостности
    IF verify_integrity:
        IF XXH3-128(data) != xml_key:
            RAISE DataCorruptionError()
    
    RETURN {status: 'ok', data: data}

FUNCTION read_package(package_id):
    package = metadata_store.get_package(package_id)
    
    IF package.storage_mode == 'raw':
        # Читаем оригинальный ZIP
        zip_data = storage_engine.read(
            package.location.segment_id,
            package.location.offset,
            package.location.size
        )
        RETURN {type: 'zip', data: zip_data}
    
    ELSE IF package.storage_mode == 'unpacked':
        # Читаем все XML пакета
        refs = metadata_store.get_package_contents(package_id)
        xml_files = []
        
        FOR ref IN refs:
            xml_data = storage_engine.read(
                ref.location.segment_id,
                ref.location.offset,
                ref.location.size
            )
            xml_files.append({
                filename: ref.original_filename, 
                data: xml_data
            })
        
        RETURN {type: 'xml_collection', files: xml_files}
```

### 4.7. Алгоритм Batch Write (оптимизация I/O)

```
CLASS WriteBuffer:
    buffer = []
    buffer_size = 0
    MAX_BUFFER_SIZE = 4 MB
    MAX_FLUSH_INTERVAL = 100 ms
    
    FUNCTION append(xml_data):
        buffer.append(xml_data)
        buffer_size += len(xml_data)
        
        IF buffer_size >= MAX_BUFFER_SIZE:
            flush()
    
    FUNCTION flush():
        IF buffer IS EMPTY:
            RETURN
        
        # Группируем в один сегмент
        segment = segment_manager.get_active_segment()
        base_offset = segment.current_offset
        
        # Формируем единый буфер для записи
        write_block = []
        locations = []
        current_offset = base_offset
        
        FOR xml_data IN buffer:
            header = pack('<I', len(xml_data))  # 4-byte size prefix
            write_block.append(header)
            write_block.append(xml_data)
            
            locations.append(Location(
                segment_id=segment.id,
                offset=current_offset + 4,  # после header
                size=len(xml_data)
            ))
            current_offset += 4 + len(xml_data)
        
        # Одна системная запись вместо тысяч
        segment.write(concat(write_block))
        segment.fsync()  # или group commit
        
        RETURN locations
```

**Эффект:** Вместо 10 000 syscall `write()` на пачку XML — один `write()` на 4 МБ. Пропускная способность NVMe возрастает с ~100K IOPS до нескольких GB/s.

## 5. Форматы данных

### 5.1. Формат сегмента

```
┌─────────────────────────────────────────────────────────────┐
│  segment_0001.dat                                           │
├─────────────────────────────────────────────────────────────┤
│  [RECORD 1]                                                 │
│    [4 bytes: magic = 0x584D4C31 "XML1"]                    │
│    [4 bytes: size = N]                                      │
│    [N bytes: XML data]                                      │
│    [16 bytes: XXH3-128 hash]  # для быстрой верификации     │
│                                                             │
│  [RECORD 2]                                                 │
│    [4 bytes: magic]                                         │
│    [4 bytes: size]                                          │
│    [N bytes: XML data]                                      │
│    [16 bytes: hash]                                         │
│                                                             │
│  ...                                                        │
│                                                             │
│  [RECORD K] (ZIP or XML)                                    │
│    [4 bytes: magic = 0x5A495031 "ZIP1" or "XML1"]          │
│    [4 bytes: size]                                          │
│    [N bytes: data]                                          │
│    [16 bytes: hash]                                         │
│                                                             │
│  [SEGMENT FOOTER]                                           │
│    [4 bytes: record_count]                                  │
│    [8 bytes: total_size]                                    │
│    [16 bytes: segment_hash]                                 │
│    [4 bytes: footer_magic = 0x464F4F54 "FOOT"]             │
└─────────────────────────────────────────────────────────────┘
```

**Обоснование формата:**

- **Magic number:** защита от чтения "мусора" при сбое
- **Size prefix:** позволяет читать запись без сканирования
- **Hash в конце:** быстрая верификация без повторного чтения
- **Footer:** для валидации сегмента и восстановления после сбоя

### 5.2. Формат записи в Hash Index (RocksDB)

```
KEY:   [16 bytes: XXH3-128 hash]
VALUE: [4 bytes: segment_id] 
       [8 bytes: offset] 
       [4 bytes: size]
       [1 byte: record_type]  # 0=XML, 1=ZIP, 2=compressed
       [8 bytes: created_at]  # unix timestamp ms

Total: 41 byte на запись
```

**Экономия:** 16 байт на ключ вместо 32 (SHA-256) = 50% экономии места в индексе.

### 5.3. Структура Bloom Filter

**Параметры:**

- Ожидаемое количество элементов: 1 млрд
- Допустимая вероятность false positive: 0.1% (0.001)
- Размер: ~1.7 GB (рассчитывается по формуле `m = -n*ln(p)/(ln(2))^2`)

**Реализация:**

- Разбит на 16 сегментов по ~100 MB
- Каждый сегмент — отдельный файл на диске
- Загружается в RAM при старте
- Периодически перестраивается (раз в сутки) при достижении емкости

### 5.4. Структура метаданных в PostgreSQL (v1.2)

```sql
CREATE TABLE content_blobs (
    content_hash    BYTEA PRIMARY KEY,     -- SHA-256(bytes)
    size            INT NOT NULL,
    segment_id      INT NOT NULL,
    offset          BIGINT NOT NULL,
    ref_count       BIGINT NOT NULL DEFAULT 1,
    first_seen_at   TIMESTAMPTZ NOT NULL
);

CREATE TABLE packages (
    id                   BIGSERIAL PRIMARY KEY,
    supplier_id          INT NOT NULL,
    received_at          TIMESTAMPTZ NOT NULL,
    package_hash         BYTEA NOT NULL,     -- SHA-256(POST body), NOT UNIQUE
    payload_type         VARCHAR(10) NOT NULL,
    storage_mode         VARCHAR(20) NOT NULL,
    original_filename    VARCHAR(255),
    canonical_package_id BIGINT REFERENCES packages(id),
    file_count           INT NOT NULL DEFAULT 0,
    unpack_error         TEXT
);

CREATE TABLE package_files (
    id                BIGSERIAL PRIMARY KEY,
    package_id        BIGINT NOT NULL REFERENCES packages(id),
    blob_hash         BYTEA NOT NULL REFERENCES content_blobs(content_hash),
    role              VARCHAR(15) NOT NULL,  -- original | member | unpack_error
    original_filename VARCHAR(255),
    sequence_number   INT
);
```

**Read path:** `file_id` → `package_files.blob_hash` → `content_blobs` → segment read → bytes клиенту.

### 5.4.1. Устаревшая схема (xml_registry)

<details>
<summary>Legacy schema</summary>

```sql
-- Реестр уникальных XML (один раз на уникальный контент)
CREATE TABLE xml_registry (
    hash            BYTEA PRIMARY KEY,      # XXH3-128, 16 байт
    size            INT NOT NULL,
    segment_id      INT NOT NULL,
    offset          BIGINT NOT NULL,
    first_seen_at   TIMESTAMP NOT NULL,
    reference_count BIGINT DEFAULT 1,       # сколько раз встречается
    slow_hash       BYTEA                   # SHA-256, только при коллизиях
);

-- Реестр пакетов
CREATE TABLE packages (
    id              BIGSERIAL PRIMARY KEY,
    supplier_id     INT NOT NULL,
    received_at     TIMESTAMP NOT NULL,
    package_hash    BYTEA NOT NULL,         # SHA-256 оригинала
    storage_mode    VARCHAR(20) NOT NULL,   # 'raw' | 'unpacked' | 'single'
    
    # Для 'raw' и 'single'
    location_segment_id  INT,
    location_offset      BIGINT,
    location_size        INT,
    
    # Статистика
    file_count      INT,
    new_count       INT,                    # сколько новых XML
    duplicate_count INT,                    # сколько дубликатов
    total_xml_size  BIGINT,
    
    UNIQUE(package_hash)
);

-- Связь пакет ↔ XML (только для 'unpacked')
CREATE TABLE package_contents (
    package_id      BIGINT REFERENCES packages(id),
    xml_hash        BYTEA REFERENCES xml_registry(hash),
    original_filename VARCHAR(255),
    sequence_number INT,                    # порядок в ZIP
    
    PRIMARY KEY (package_id, sequence_number)
);

-- Индексы
CREATE INDEX idx_packages_supplier ON packages(supplier_id, received_at DESC);
CREATE INDEX idx_packages_hash ON packages(package_hash);
CREATE INDEX idx_package_contents_xml ON package_contents(xml_hash);

-- Статистика по поставщикам (для аналитики дедупликации)
CREATE TABLE supplier_stats (
    supplier_id         INT PRIMARY KEY,
    total_packages      BIGINT DEFAULT 0,
    total_xml_sent      BIGINT DEFAULT 0,
    unique_xml_count    BIGINT DEFAULT 0,
    dedup_ratio         DECIMAL(5,2),       # % дубликатов
    last_activity       TIMESTAMP
);
```

</details>

## 6. Read/Write Path (v1.2)

**Write:** `POST /v1/packages` → `package_hash` lookup → clone refs **или** `StoreOrRef` blobs → PG transaction.

**Read:** `GET /v1/packages/{id}/files/{file_id}` → `package_files` → `content_blobs` → segment.

Клиент не видит dedup; duplicate POST всегда `201`.

## 6.1. Read/Write Path (legacy diagram)

```
                    ┌──────────────────┐
                    │ Incoming XML/ZIP │
                    └────────┬─────────┘
                             ↓
                    ┌──────────────────┐
                    │ Protocol Adapter │
                    └─────────────────┘
                             ↓
                    ┌──────────────────┐
                    │ Type Detection   │
                    │ (XML vs ZIP)     │
                    └─────────────────┘
                             ↓
              ┌──────────────┴──────────────┐
              ↓                             ↓
     ┌────────────────┐           ┌────────────────┐
     │ Single XML /   │           │ Large ZIP      │
     │ Small ZIP      │           │                │
     └────────┬───────┘           └────────┬───────┘
              ↓                             ↓
     ┌────────────────┐           ┌────────────────┐
     │ Unpack ZIP     │           │ Append ZIP     │
     │ (if needed)    │           │ as single blob │
     └────────┬───────┘           └────────┬───────┘
              ↓                             ↓
     ┌─────────────────────────────────────────────┐
     │  FOR each XML:                              │
     │    1. XXH3-128(xml)                         │
     │    2. Bloom Filter check                    │
     │    3. IF hit → Hash Index lookup            │
     │    4. IF found → size verify                │
     │    5. IF duplicate → skip write             │
     │    6. IF new → add to write buffer          │
     └────────┬────────────────────────────────────┘
              ↓
     ┌────────────────┐
     │ Batch Flush    │
     │ (4 MB or 100ms)│
     └────────┬───────┘
              ↓
     ┌─────────────────────────────────────────────┐
     │  Single syscall write() to active segment   │
     │  + fsync (group commit)                     │
     └────────┬────────────────────────────────────┘
              ↓
     ┌─────────────────────────────────────────────┐
     │  Batch update:                              │
     │    - Hash Index (RocksDB)                   │
     │    - Bloom Filter                           │
     │    - PostgreSQL (xml_registry,              │
     │      packages, package_contents)            │
     └─────────────────────────────────────────────┘
```

### Read Path

> **Контекст:** чтение выполняется редко (анализ, конфликты). В single-node — прямой доступ. В распределённой конфигурации — через **Coordinator** (§7.2): lookup в global index или fan-out по sealed шардам.

```
     ┌──────────────────┐
     │ Read Request     │
     │ (by xml_key or   │
     │  package_id)     │
     └────────┬─────────┘
              ↓
     ┌────────┴─────────┐
     ↓                  ↓
┌─────────┐      ┌──────────────┐
│ By XML  │      │ By Package   │
│ key     │      │              │
└────┬────┘      └──────┬───────┘
     ↓                  ↓
┌──────────────┐ ┌──────────────────┐
│ Hash Index   │ │ PostgreSQL:      │
│ lookup       │ │ get package      │
│ (RocksDB)    │ │ metadata         │
└──────┬───────┘ └──────┬───────────┘
       ↓                ↓
       ↓         ┌──────┴───────┐
       ↓         ↓              ↓
       ↓   ┌──────────┐  ┌──────────┐
       ↓   │ mode=raw │  │ mode=    │
       ↓   │          │  │ unpacked │
       ↓   └────┬─────  └─────────┘
       ↓        ↓             ↓
       ↓   ┌────────┐  ┌────────────┐
       ↓   │ Single │  │ For each   │
       ↓   │ read   │  │ XML:       │
       ↓   └────┬───┘  │ Hash Index │
       ↓        ↓      │ lookup     │
       ↓        │      └──────┬─────┘
       ↓        │             ↓
       └─────────────────────┴───┐
                                  ↓
                    ┌──────────────────────┐
                    │ Storage Engine:      │
                    │ seek() + read()      │
                    │ from segment file    │
                    └──────────┬───────────┘
                               ↓
                    ┌──────────────────────┐
                    │ Optional: verify     │
                    │ hash integrity       │
                    └──────────┬───────────┘
                               ↓
                    ┌──────────────────────┐
                    │ Return data          │
                    └──────────────────────┘
```

## 7. Масштабирование

### 7.1. Вертикальное (Single Node)

**Ограничения одного сервера:**

- Сегменты: до 100 TB на один сервер (ограничение дискового пространства)
- Hash Index (RocksDB): до 10-20 млрд записей на NVMe
- Write throughput: 1-3 GB/s на один NVMe
- Read IOPS: 500K-1M на NVMe

### 7.2. Горизонтальное масштабирование (volume-based sharding)

**Стратегия:** шардирование по **объёму данных** на шарде, не по `supplier_id`. Все записи идут на один **active** шард; при заполнении шард **sealed** (read-only) и активируется следующий. Sealed шарды «остывают» — почти не читаются.

Подробная модель: [sharding-model.md](sharding-model.md).

```
┌─────────────────────────────────────────────────────────────┐
│                      COORDINATOR                             │
│  global_package_index │ global_xml_index │ shard_registry   │
└──────────────────────────────┬──────────────────────────────┘
                               │
          write (always)       │        read (rare)
               ↓               │               ↓
┌──────────────────┐   ┌─────┴─────┐   ┌──────────────────┐
│  Sealed Shard 0  │   │  Active   │   │  Sealed Shard 1  │
│  [read-only,HDD] │   │  Shard N  │   │  [read-only,HDD] │
│  Primary◄─►Replica│   │ Primary◄─►Replica│ Primary◄─►Replica│
│  Ingest─Dedup    │   │  (hot,NVMe)│   │                  │
│  Storage─RocksDB │   │  ALL writes│   │                  │
│  Postgres        │   │            │   │                  │
└──────────────────┘   └───────────┘   └──────────────────┘
```

**Жизненный цикл шарда:**

| Состояние | Write | Read | Хранилище |
|-----------|-------|------|-----------|
| `standby` | Нет | Нет | — |
| `active` | **Да (единственный)** | Да | NVMe |
| `sealed` | Нет | Да (редко) | HDD |
| `archived` | Нет | Очень редко | Object storage (опц.) |

**Условие seal (ротации):**

```
IF active_shard.total_bytes >= SHARD_MAX_BYTES:
    seal(active_shard)     # read-only, fsync, checkpoint, sync replica
    activate(next_shard)   # pre-provisioned standby → active
```

Добавление шарда **не требует rehash и миграции** — новый пустой шард просто становится active.

**Маршрутизация:**

| Операция | Правило |
|----------|---------|
| Write `POST /packages` | Всегда на **active** primary |
| Read `GET /packages/{global_id}` | `shard_id = global_id >> 48` → proxy на шард |
| Read `GET /xml/{hash}` | Lookup `global_xml_index`; fallback: parallel fan-out (допустимо — чтение редкое) |
| Query по supplier + период | `global_package_index` → batch read с нужных шардов |

**Глобальный package_id (64 bit):**

```
[16 bit: shard_id][48 bit: local_package_id]
```

**Дедупликация в volume-based модели:**

- **Active шард:** полный dedup (Bloom + RocksDB) **между всеми поставщиками** — покрывает критический паттерн cross-supplier дубликатов в текущем потоке
- **Между sealed шардами:** данные immutable, dedup не выполняется
- **Тот же XML после seal на новом active:** возможна вторая физическая копия — **приемлемо** (cold storage дешёвый, чтение редкое)

### 7.3. Зеркалирование шардов

Каждый шард — пара **primary + replica**:

| Компонент | Механизм репликации |
|-----------|---------------------|
| PostgreSQL | Streaming replication |
| Segments | rsync/lsyncd на fsync; batch sync при seal |
| RocksDB | Checkpoint → copy на replica |
| Bloom filter | Copy `bloom.dat` при seal |

**Политика:**

- Write → только primary **active** шарда
- Read (анализ, конфликты) → replica sealed шарда (снижает нагрузку)
- Seal блокируется до синхронизации replica

### 7.4. Coordinator (глобальный индекс)

Легковесная PostgreSQL на Coordinator:

```sql
-- Реестр шардов
CREATE TABLE shard_registry (
    shard_id      SMALLINT PRIMARY KEY,
    state         VARCHAR(10) NOT NULL,  -- standby | active | sealed | archived
    primary_url   TEXT NOT NULL,
    replica_url   TEXT,
    total_bytes   BIGINT DEFAULT 0,
    sealed_at     TIMESTAMP
);

-- Индекс пакетов (для редкого чтения и запросов по supplier/времени)
CREATE TABLE global_package_index (
    global_id     BIGINT PRIMARY KEY,
    shard_id      SMALLINT NOT NULL,
    local_id      BIGINT NOT NULL,
    supplier_id   INT NOT NULL,
    received_at   TIMESTAMP NOT NULL,
    package_hash  BYTEA NOT NULL
);

-- Индекс XML (быстрый lookup по hash)
CREATE TABLE global_xml_index (
    hash          BYTEA PRIMARY KEY,
    shard_id      SMALLINT NOT NULL,
    first_seen_at TIMESTAMP NOT NULL
);
```

Обновляется Coordinator при каждой успешной записи на active shard.

### 7.5. Масштабирование Hash Index внутри шарда

Если RocksDB на одном шарде не вмещает все записи:

- **Range partitioning:** по первым байтам хеша
- **Tiered storage:** hot на NVMe (active), cold на HDD (sealed)

## 8. Отказоустойчивость

### 8.1. Durability (сохранность данных)

**Write-ahead log (WAL) для метаданных:**

- PostgreSQL: встроенный WAL
- RocksDB: встроенный WAL
- Сегменты: fsync после каждого batch

**Group commit:**

- Несколько записей группируются в один fsync
- Уменьшает нагрузку на диск в 10-100 раз
- Задержка: до 1-10 ms (настраивается)

### 8.2. Recovery после сбоя

**Сценарий: crash во время batch write**

1. При старте проверяем footer каждого сегмента
2. Если footer поврежден → сегмент обрезается до последней валидной записи
3. Hash Index перестраивается из сегментов (full scan)
4. PostgreSQL уже имеет WAL → метаданные консистентны

**Сценарий: потеря сегмента**

1. Обнаруживается при чтении (checksum mismatch)
2. Помечаем записи как "lost" в специальном индексе
3. Данные невосстановимы (но это append-only, бэкапы сегментов обязательны)

### 8.3. Бэкапы

**Стратегия:**

- Сегменты: инкрементальные бэкапы (restic/borg) — дедупликация на уровне бэкапов
- RocksDB: снапшоты через `Checkpoint` API
- PostgreSQL: PITR (Point-in-Time Recovery)

**RPO/RTO:**

- RPO: до 1 часа (зависит от частоты бэкапов WAL)
- RTO: до 4 часов (восстановление из бэкапа + rebuild индекса)

### 8.4. Отказоустойчивость шардов (зеркалирование)

**Сценарий: падение primary active шарда**

1. Coordinator перестаёт маршрутизировать write на упавший шард
2. Promote replica → primary (manual или автоматика — Phase 5)
3. Обновить `shard_registry.primary_url`
4. Продолжить write (при потере несинхронизированных данных — replay из WAL replica)

**Сценарий: падение sealed шарда при редком read**

1. Coordinator маршрутизирует read на **replica**
2. Primary sealed шарда можно восстановить из replica + backup segments

**Сценарий: seal во время записи**

1. Дождаться завершения активных batch write
2. Final fsync + RocksDB checkpoint
3. Дождаться replica sync (`replica_lag < threshold`)
4. Только then: `active → sealed`, `standby → active`

## 9. Оценка эффективности

### 9.1. Экономия места от дедупликации

**Сценарий:** 100 млн XML по 100 байт, из них 30% — уникальные

| Метрика | Без дедупликации | С дедупликацией | Экономия |
|---------|------------------|-----------------|----------|
| XML данные | 10 GB | 3 GB | 70% |
| Индекс (RocksDB) | — | ~4 GB (100M × 41 байт) | — |
| PostgreSQL metadata | — | ~10 GB | — |
| **Итого** | **10 GB** | **~17 GB** | **... но** |

> **Подождите!** Индекс и метаданные **больше**, чем сами данные. Это важный инсайт.

### 9.2. Переоценка: когда дедупликация выгодна

**Дедупликация выгодна, когда:**

- XML большие (от 1 КБ)
- Коэффициент дедупликации высокий (>50%)
- Поставщики шлют одни и те же данные часто

**Дедупликация НЕ выгодна, когда:**

- XML по 50 байт (индекс больше данных)
- Все XML уникальны
- Низкая нагрузка на диск, высокая на CPU

### 9.3. Альтернативная стратегия: Dictionary Compression

Для XML по 50-100 байт **словарное сжатие эффективнее дедупликации**:

```
Без сжатия: 100 байт × 100M = 10 GB
Dictionary compression (5x): 20 байт × 100M = 2 GB
Индекс: 0 (ключи в PostgreSQL)
Итого: 2 GB + 2 GB (PostgreSQL) = 4 GB
```

> **Рекомендация:** Использовать **комбинацию**:
>
> 1. Dictionary compression для всех XML (5-10x сжатие)
> 2. Дедупликация **только для XML > 1 KB** (где индекс окупается)
> 3. Для маленьких XML — только сжатие + ключи в PostgreSQL

## 10. Итоговая архитектура (финальная версия)

```
┌─────────────────────────────────────────────────────────────┐
│                      COORDINATOR                             │
│  Write → active shard │ Read → any shard │ Global index PG    │
└──────────────────────────────┬──────────────────────────────┘
                               ↓ write
┌─────────────────────────────────────────────────────────────┐
│                        INGESTION (active shard)              │
│  Protocol Adapter → ZIP Detector → Router                   │
└─────────────────────────────────────────────────────────────┘
                              ↓
              ┌───────────────┴───────────────┐
              ↓                               ↓
     ┌────────────────┐             ┌────────────────┐
     │ Small XML/ZIP  │             │ Large ZIP      │
     │ (< 100 KB)     │             │ (≥ 100 KB)     │
     └────────┬───────┘             └────────┬───────┘
              ↓                               ↓
     ┌────────────────┐             ┌────────────────┐
     │ Dictionary     │             │ Append as-is   │
     │ Compression    │             │ to segment     │
     │ (5-10x)        │             │                │
     └────────┬───────┘             └────────┬───────┘
              ↓                               ↓
     ┌────────────────                       │
     │ IF size > 1KB: │                       │
     │   Dedup check  │                       │
     │ ELSE:          │                       │
     │   Just write   │                       │
     └────────┬───────┘                       │
              ↓                               ↓
     ┌────────────────────────────────────────────┐
     │        BATCH WRITE BUFFER (4 MB)           │
     │        Single syscall → segment            │
     └────────────────────────────────────────────┘
                              ↓
     ┌────────────────────────────────────────────┐
     │ SEGMENTS (append-only, 1-4 GB each)        │
     │ Compressed XML + Raw large ZIPs            │
     │ [active: NVMe] [sealed: HDD, read-only]    │
     └────────────────────────────────────────────┘
                              ↓
     ┌────────────────────────────────────────────┐
     │ SHARD LOCAL: PostgreSQL + RocksDB + Bloom  │
     │ COORDINATOR: global_package/xml index      │
     └────────────────────────────────────────────┘
                              ↓
     ┌────────────────────────────────────────────┐
     │ MIRRORING: primary ◄──► replica per shard │
     └────────────────────────────────────────────┘
```

**Ротация:** при `total_bytes >= SHARD_MAX_BYTES` → seal → следующий shard становится active.

## 11. Ключевые решения и trade-offs

| Решение | Обоснование | Альтернатива |
|---------|-------------|--------------|
| XXH3-128 для хеша | Скорость в 50 раз выше SHA-256 | SHA-256 (если нужна крипто-стойкость) |
| Append-only сегменты | Простота, нет GC, быстрый write | B-Tree (сложнее, но с update) |
| Batch write | Уменьшение syscall в 1000x | Per-record write (медленно) |
| Dictionary compression | 5-10x для мелких XML | Дедупликация (неэффективна для 50 байт) |
| Пороговое правило для ZIP | Баланс между экономией и сложностью | Всегда распаковывать (дорого для больших) |
| PostgreSQL для метаданных | ACID, зрелая, знакомая | RocksDB для всего (сложнее) |
| Volume-based sharding | Rolling seal по объёму; данные остывают; нет rehash | Шардирование по supplier_id (миграция при scale-out) |
| Зеркалирование шардов | HA write path; read с replica sealed шардов | Single node без replica |
| Coordinator + global index | Редкое чтение; маршрутизация без fan-out в common case | Scatter-gather на каждый read |

## 12. Дорожная карта внедрения

### Фаза 1 (MVP):

- Append-only сегменты без сжатия и дедупликации
- PostgreSQL для всего (и метаданные, и указатели)
- Базовая обработка ZIP

### Фаза 2 (Оптимизация):

- Dictionary compression для XML
- Пороговое правило для больших ZIP
- Batch write

### Фаза 3 (Дедупликация):

- Bloom Filter + RocksDB Hash Index
- Дедупликация для XML > 1 KB
- Статистика дедупликации по поставщикам

### Фаза 4 (Масштабирование):

- Volume-based sharding: active → sealed по `SHARD_MAX_BYTES`
- Coordinator + global index (package, xml, shard_registry)
- Зеркалирование: primary + replica на каждый шард
- Cold tier для sealed шардов (HDD)
- Мониторинг per-shard (seal events, replica lag, bytes per state)

## 13. Приложения

### A. Глоссарий

| Термин | Определение |
|--------|-------------|
| **Дедупликация** | Механизм устранения дублирования данных путем хранения только уникальных блоков |
| **Content-Defined Chunking (CDC)** | Алгоритм разбиения данных на чанки на основе их содержимого, а не фиксированных границ |
| **Bloom Filter** | Вероятностная структура данных для быстрой проверки принадлежности элемента множеству |
| **Append-only** | Паттерн хранения, при котором данные только добавляются, но не изменяются и не удаляются |
| **Batch write** | Группировка нескольких операций записи в одну системную операцию для повышения производительности |
| **XXH3** | Быстрая некриптографическая хеш-функция семейства XXH |
| **Coordinator** | Компонент маршрутизации: write на active shard, read с любого шарда, глобальный индекс |
| **Seal** | Перевод шарда в read-only при достижении лимита объёма; данные «остывают» |
| **Active shard** | Единственный шард, принимающий записи в текущий момент |
| **Sealed shard** | Заполненный read-only шард на cold storage; читается редко |

### B. Рекомендуемые технологии

| Компонент | Технология | Альтернативы |
|-----------|------------|--------------|
| Hash Index | RocksDB | LevelDB, LMDB, BadgerDB |
| Database | PostgreSQL 14+ | CockroachDB, TiDB |
| Coordinator DB | PostgreSQL 14+ (легковесный индекс) | etcd + custom index |
| Bloom Filter | pybloom-live, bloom-filter2 | Cuckoo filter |
| Compression | Zstd, LZ4 | Snappy, Brotli |
| Hash Function | SHA-256 | xxhash (XXH3-128) |
| Backup | restic, borg | rclone, rsync + zfs snapshots |

### C. Метрики для мониторинга

**Производительность:**

- Write throughput (MB/s)
- Read latency (p50, p95, p99)
- IOPS (read/write)
- Batch flush frequency

**Эффективность дедупликации:**

- Deduplication ratio (unique/total)
- Bloom filter false positive rate
- Hash index size vs data size
- Storage savings (GB, %)

**Надежность:**

- fsync latency
- WAL lag
- Segment corruption rate
- Recovery time
- Replica lag per shard
- Seal duration and success rate

**Шардирование (Фаза 4):**

- `shard_bytes{state}` — объём active vs sealed
- `coordinator_seal_total` — количество seal events
- `shard_mirror_lag_seconds` — отставание replica
- `read_fanout_duration_seconds` — latency fan-out read (редкий кейс)
- `shard_last_read_at` — «остывание» sealed шардов

**Бизнес-метрики:**

- Packages per supplier
- Duplicate rate by supplier
- Average package size
- Peak load times

---

**Версия документа:** 1.1  
**Дата:** 2026-06-23  
**Статус:** Draft  
**Изменения v1.1:** volume-based sharding (rolling seal), редкое чтение, Coordinator, зеркалирование шардов. См. [sharding-model.md](sharding-model.md).
