import logging
import time
import uuid
from pathlib import Path

from pydantic import ValidationError

from app.db.repository import upsert_orders, upsert_products
from app.db.session import get_db_session
from app.ingestion.cache import RedisCache
from app.ingestion.loaders import load_records
from app.ingestion.normalizer import normalize_records
from app.models.pydantic_models import OrderIn, ProductIn, RunSummary


logger = logging.getLogger(__name__)
ALLOWED_INGEST_ROOT = Path("data/incoming").resolve()


def _resolve_ingest_path(file_path: str) -> Path:
    resolved_path = Path(file_path).resolve()

    try:
        resolved_path.relative_to(ALLOWED_INGEST_ROOT)
    except ValueError as exc:
        raise ValueError("file_path must be under data/incoming") from exc

    if not resolved_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    return resolved_path


def run_pipeline(file_path: str, supplier_id: str, record_type: str, cache: RedisCache) -> RunSummary:
    run_id = str(uuid.uuid4())
    start = time.time()
    path = _resolve_ingest_path(file_path)

    logger.info(
        "pipeline.start",
        extra={
            "run_id": run_id,
            "supplier_id": supplier_id,
            "record_type": record_type,
            "file_path": str(path),
        },
    )

    if record_type not in {"product", "order"}:
        raise ValueError("record_type must be 'product' or 'order'")

    file_hash = cache.file_hash(path)
    cache_key = cache.build_key(supplier_id, record_type, path, file_hash)
    if cache.exists(cache_key):
        logger.info("pipeline.cached_skip", extra={"run_id": run_id, "file_path": str(path)})
        return RunSummary(
            run_id=run_id,
            status="completed",
            processed=0,
            inserted=0,
            rejected=0,
            skipped_cached=True,
            errors=[],
        )

    raw_records = load_records(path)
    normalized = normalize_records(raw_records, supplier_id=supplier_id, record_type=record_type)

    valid_rows: list[dict] = []
    errors: list[dict] = []

    for index, row in enumerate(normalized, start=1):
        try:
            validated = ProductIn(**row) if record_type == "product" else OrderIn(**row)
            valid_rows.append(validated.model_dump())
        except (ValidationError, ValueError, TypeError) as exc:
            error = {"index": index, "error": str(exc)}
            logger.warning("pipeline.validation_error", extra={"run_id": run_id, **error})
            errors.append(error)

    inserted = 0
    if valid_rows:
        with get_db_session() as session:
            if record_type == "product":
                inserted = upsert_products(session, valid_rows)
            else:
                inserted = upsert_orders(session, valid_rows)

    if valid_rows or (not normalized and not errors):
        cache.set(cache_key)

    elapsed_ms = int((time.time() - start) * 1000)
    logger.info(
        "pipeline.persist_summary",
        extra={
            "run_id": run_id,
            "supplier_id": supplier_id,
            "record_type": record_type,
            "file_path": str(path),
            "processed": len(normalized),
            "inserted": inserted,
            "rejected": len(errors),
            "elapsed_ms": elapsed_ms,
        },
    )

    return RunSummary(
        run_id=run_id,
        status="completed",
        processed=len(normalized),
        inserted=inserted,
        rejected=len(errors),
        skipped_cached=False,
        errors=errors,
    )