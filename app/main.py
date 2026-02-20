from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api import routes
from app.config import settings
from app.db.base import Base
from app.db.session import engine
from app.ingestion.cache import RedisCache
from app.logging_config import configure_logging
from app.scheduler.jobs import SupplierSyncScheduler


configure_logging()
cache = RedisCache(settings.redis_url, settings.cache_ttl_seconds)
routes.cache_instance = cache
scheduler = SupplierSyncScheduler(cache)


@asynccontextmanager
async def lifespan(app: FastAPI):
    Base.metadata.create_all(bind=engine)
    scheduler.start()
    try:
        yield
    finally:
        scheduler.shutdown()


app = FastAPI(title=settings.app_name, lifespan=lifespan)
app.include_router(routes.router)
