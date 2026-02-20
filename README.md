# ROG Supplier Ingestion Pipeline

This project refactors the original `ldit_tool` into a production-style supplier data ingestion and normalization service aligned with ROG LLC's e-commerce operations.


ROG manages 1,000+ SKUs and processes weekly order volumes across multiple suppliers and channels (including TikTok Shop). The pipeline supports supplier feed variability, validates data before persistence, and uses caching + scheduled sync to reduce stale inventory risk and avoid unnecessary reprocessing.

## Architecture
- `app/ingestion/loaders.py`: Load CSV/JSON/TXT supplier feeds.
- `app/ingestion/normalizer.py`: Map supplier-specific keys into canonical product/order fields.
- `app/models/pydantic_models.py`: Validate product/order records and API contracts.
- `app/db/models.py`: SQLAlchemy table definitions for `products` and `orders`.
- `app/db/repository.py`: Upsert logic for products (`sku + supplier_id`) and orders (`order_id`).
- `app/ingestion/cache.py`: Redis keying by supplier + type + path + file hash.
- `app/ingestion/pipeline.py`: Orchestrates load -> normalize -> validate -> persist -> cache.
- `app/api/routes.py`: `POST /ingest` and `GET /health` endpoints.
- `app/scheduler/jobs.py`: APScheduler daily sync job scanning `data/incoming/`.
- `app/main.py`: FastAPI startup/shutdown wiring, schema creation, scheduler bootstrap.

## Canonical schemas
### Product
- `sku`
- `price`
- `quantity`
- `supplier_id`
- `status`

### Order
- `order_id`
- `sku`
- `quantity`
- `supplier_id`
- `status`
- `price` (optional)

## Validation rules
- SKU pattern: `^[A-Za-z0-9_-]{3,40}$`
- Product status: `active | inactive | backorder | discontinued`
- Order status: `pending | processing | shipped | cancelled | returned`
- Product `price > 0`, `quantity >= 0`
- Order `quantity > 0`, optional `price > 0`

## Supported feed formats
- CSV: header-based rows
- JSON: list of objects or single-key wrapped list
- TXT: line-based `key:value,key:value`

## Key normalization aliases
- `item_sku|sku_code -> sku`
- `qty|stock|inventory -> quantity`
- `vendor|supplier -> supplier_id`
- `state -> status`
- `unit_price|cost -> price`
- `order|order_number|id -> order_id`

## API
### POST `/ingest`
Example request:
```bash
curl -X POST http://localhost:8000/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "data/incoming/supplier_a_products.csv",
    "supplier_id": "supplier_a",
    "record_type": "product"
  }'
```

### GET `/health`
```bash
curl http://localhost:8000/health
```

## Scheduler
- Runs inside FastAPI process.
- Cron expression from `SCHEDULE_CRON` (default: `0 2 * * *`).
- Scans `data/incoming/` daily and ingests each supported file.

## Redis cache behavior
- Cache key format: `ingest:{supplier_id}:{record_type}:{file_path}:{sha256(file_bytes)}`
- Cached files are skipped during TTL window (`CACHE_TTL_SECONDS`, default `86400`).
- If Redis is unavailable, ingestion continues without cache enforcement.

## Run with Docker
1. Copy `.env.example` to `.env`.
2. Start services:
```bash
docker compose up --build
```
3. Mount feed files into `data/incoming/` and trigger `/ingest` or wait for scheduled sync.

## Local tests
```bash
pip install -r requirements.txt
pytest -q
```

## Notes
- Uses `Base.metadata.create_all` at startup (no Alembic in this iteration).
- No auth layer is included in this pass.
