from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_session
from app.ingestion.cache import RedisCache
from app.ingestion.pipeline import run_pipeline
from app.models.pydantic_models import HealthStatus, IngestRequest, RunSummary


router = APIRouter()
cache_instance: RedisCache | None = None


def get_cache() -> RedisCache:
    if cache_instance is None:
        raise RuntimeError("Cache is not initialized")
    return cache_instance


@router.post("/ingest", response_model=RunSummary)
def ingest_feed(payload: IngestRequest, cache: RedisCache = Depends(get_cache)) -> RunSummary:
    try:
        return run_pipeline(payload.file_path, payload.supplier_id, payload.record_type, cache)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {exc}") from exc


@router.get("/health", response_model=HealthStatus)
def health(cache: RedisCache = Depends(get_cache), session: Session = Depends(get_session)) -> HealthStatus:
    db_status = "ok"
    redis_status = "ok"

    try:
        session.execute(text("SELECT 1"))
    except Exception:
        db_status = "down"

    if not cache.ping():
        redis_status = "down"

    overall = "ok" if db_status == "ok" and redis_status == "ok" else "degraded"
    return HealthStatus(status=overall, db=db_status, redis=redis_status)
