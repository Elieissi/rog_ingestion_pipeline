import logging
from pathlib import Path

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import settings
from app.ingestion.cache import RedisCache
from app.ingestion.pipeline import run_pipeline


logger = logging.getLogger(__name__)
SUPPORTED_EXTENSIONS = {".csv", ".json", ".txt"}


def _resolve_feed_metadata(file_path: Path) -> tuple[str, str] | None:
    stem_parts = file_path.stem.lower().split("_")
    if len(stem_parts) < 2:
        return None

    supplier_id = stem_parts[0]
    tokens = set(stem_parts[1:])
    if "order" in tokens or "orders" in tokens:
        return supplier_id, "order"
    if "product" in tokens or "products" in tokens:
        return supplier_id, "product"
    return None


class SupplierSyncScheduler:
    def __init__(self, cache: RedisCache):
        self.cache = cache
        self.scheduler = BackgroundScheduler()

    def _run_sync(self) -> None:
        logger.info("scheduler.run_start")
        incoming_dir = Path("data/incoming")
        if not incoming_dir.exists():
            logger.info("scheduler.run_end", extra={"processed_files": 0, "skipped_files": 0})
            return

        processed = 0
        skipped = 0
        for file_path in incoming_dir.iterdir():
            if file_path.suffix.lower() not in SUPPORTED_EXTENSIONS:
                continue

            metadata = _resolve_feed_metadata(file_path)
            if metadata is None:
                skipped += 1
                logger.warning(
                    "scheduler.file_skipped",
                    extra={
                        "file_path": str(file_path),
                        "reason": "filename must include supplier and product(s)|order(s) tokens",
                    },
                )
                continue

            supplier_id, record_type = metadata
            try:
                run_pipeline(str(file_path), supplier_id, record_type, self.cache)
            except Exception as exc:
                logger.exception("pipeline.failed", extra={"file_path": str(file_path), "error": str(exc)})
            processed += 1

        logger.info("scheduler.run_end", extra={"processed_files": processed, "skipped_files": skipped})

    def start(self) -> None:
        self.scheduler.add_job(
            self._run_sync,
            CronTrigger.from_crontab(settings.schedule_cron),
            id="daily_supplier_sync",
            replace_existing=True,
        )
        self.scheduler.start()

    def shutdown(self) -> None:
        self.scheduler.shutdown(wait=False)
