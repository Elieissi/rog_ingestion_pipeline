import hashlib
import logging
from pathlib import Path

import redis


logger = logging.getLogger(__name__)


class RedisCache:
    def __init__(self, redis_url: str, ttl_seconds: int):
        self.ttl_seconds = ttl_seconds
        try:
            self.client = redis.Redis.from_url(redis_url, decode_responses=True)
            self.client.ping()
        except Exception:
            self.client = None
            logger.warning("cache.unavailable")

    def is_available(self) -> bool:
        return self.client is not None

    def ping(self) -> bool:
        if not self.client:
            return False
        try:
            return bool(self.client.ping())
        except Exception:
            return False

    def file_hash(self, file_path: Path) -> str:
        file_bytes = file_path.read_bytes()
        return hashlib.sha256(file_bytes).hexdigest()

    def build_key(self, supplier_id: str, record_type: str, file_path: Path, file_hash: str) -> str:
        return f"ingest:{supplier_id}:{record_type}:{file_path}:{file_hash}"

    def exists(self, key: str) -> bool:
        if not self.client:
            return False
        try:
            return bool(self.client.exists(key))
        except Exception:
            logger.warning("cache.exists_failed", extra={"key": key})
            return False

    def set(self, key: str) -> None:
        if not self.client:
            return
        try:
            self.client.setex(key, self.ttl_seconds, "1")
        except Exception:
            logger.warning("cache.set_failed", extra={"key": key})
