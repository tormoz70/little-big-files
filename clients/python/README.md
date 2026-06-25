# Python clients — локальный стенд

Клиенты для загрузки тестовых ZIP из `examples/` через Coordinator.

## Поставщики (10)

| ID   | Имя            | Регион  |
|------|----------------|---------|
| 1001 | ekb-north-01   | north   |
| 1002 | ekb-north-02   | north   |
| 1003 | ekb-south-01   | south   |
| 1004 | ekb-south-02   | south   |
| 1005 | ekb-west-01    | west    |
| 1006 | ekb-west-02    | west    |
| 1007 | ekb-east-01    | east    |
| 1008 | ekb-east-02    | east    |
| 1009 | ekb-central-01 | central |
| 1010 | ekb-central-02 | central |

## Быстрый старт

```bash
# 1. Поднять стенд (3 шарда × 50 MB seal)
docker compose -f deploy/docker-compose.local.yml up -d --build

# 2. Python env
cd clients/python
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # Linux/macOS
pip install -r requirements.txt

# 3. Одна загрузка всех examples (по кругу на 10 поставщиков)
python upload_examples.py --wait

# 4. Много загрузок examples (dedup — seal может не сработать!)
python upload_examples.py --wait --repeat 300

# 5. Заполнить active shard до seal (уникальные XML ~100 KB)
python fill_shards.py --wait --target-mb 55

# 6. Нагрузка (случайные файлы из ekb_work2, 10 org-поставщиков)
python load_test.py --rate 10 --duration 120

# 6b. Нагрузка на examples/*.zip (синтетические supplier 1001-1010)
python load_test.py --examples --rate 10 --duration 120
```

## Скрипты

| Скрипт | Назначение |
|--------|------------|
| `lbf_client.py` | HTTP-клиент Coordinator |
| `suppliers.py` | 10 тестовых supplier_id |
| `upload_examples.py` | Загрузка `examples/*.zip` |
| `fill_shards.py` | Уникальные XML для seal (обход dedup) |
| `load_test.py` | Случайная нагрузка из `ekb_work2` (или `--examples`) |

## Параметры upload_examples.py

```
--base-url http://localhost:8080
--repeat 300          # раундов (zip × supplier по кругу)
--wait                # ждать готовности Coordinator
--verify-read         # проверить GET /original
--delay 0.05          # пауза между POST
```

## Проверка seal

```bash
curl -s http://localhost:8080/v1/admin/shards | python -m json.tool
```

После заполнения active шарда (~50 MB) Coordinator переводит его в `sealed` и активирует следующий standby.

**Важно:** повторная загрузка одних и тех же `examples/*.zip` почти не увеличивает `total_bytes` из‑за dedup. Для проверки seal используйте `fill_shards.py`.

## Остановка

```bash
docker compose -f deploy/docker-compose.local.yml down -v
```

## Мониторинг

Grafana: http://localhost:3000 (`admin` / `lbf`), дашборд **LBF Local Stand**.  
Prometheus: http://localhost:9090.  
Подробнее: [deploy/observability/README.md](../../deploy/observability/README.md).
