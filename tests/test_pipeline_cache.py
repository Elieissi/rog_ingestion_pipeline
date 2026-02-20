from contextlib import contextmanager
from pathlib import Path

import pytest

from app.ingestion.pipeline import run_pipeline


class FakeCache:
    def __init__(self):
        self.keys: set[str] = set()

    def file_hash(self, file_path: Path) -> str:
        return "same-hash"

    def build_key(self, supplier_id: str, record_type: str, file_path: Path, file_hash: str) -> str:
        return f"ingest:{supplier_id}:{record_type}:{file_path}:{file_hash}"

    def exists(self, key: str) -> bool:
        return key in self.keys

    def set(self, key: str) -> None:
        self.keys.add(key)


class DummySession:
    pass


@pytest.fixture
def supplier_file(tmp_path: Path) -> Path:
    file_path = tmp_path / "supplier_products.csv"
    file_path.write_text("sku,price,quantity,status\nSKU-1,10.50,5,active\n", encoding="utf-8")
    return file_path


def test_pipeline_cache_skips_second_ingestion(monkeypatch, supplier_file: Path):
    cache = FakeCache()

    @contextmanager
    def fake_get_db_session():
        yield DummySession()

    monkeypatch.setattr("app.ingestion.pipeline.get_db_session", fake_get_db_session)
    monkeypatch.setattr("app.ingestion.pipeline.upsert_products", lambda session, rows: len(rows))

    first = run_pipeline(str(supplier_file), "supplier_a", "product", cache)
    second = run_pipeline(str(supplier_file), "supplier_a", "product", cache)

    assert first.processed == 1
    assert first.inserted == 1
    assert first.skipped_cached is False
    assert second.skipped_cached is True
    assert second.processed == 0


def test_pipeline_reprocesses_when_hash_changes(monkeypatch, tmp_path: Path):
    class ChangingHashCache(FakeCache):
        def __init__(self):
            super().__init__()
            self.counter = 0

        def file_hash(self, file_path: Path) -> str:
            self.counter += 1
            return f"hash-{self.counter}"

    cache = ChangingHashCache()

    file_path = tmp_path / "supplier_products.csv"
    file_path.write_text("sku,price,quantity,status\nSKU-1,10.50,5,active\n", encoding="utf-8")

    @contextmanager
    def fake_get_db_session():
        yield DummySession()

    monkeypatch.setattr("app.ingestion.pipeline.get_db_session", fake_get_db_session)
    monkeypatch.setattr("app.ingestion.pipeline.upsert_products", lambda session, rows: len(rows))

    first = run_pipeline(str(file_path), "supplier_a", "product", cache)
    second = run_pipeline(str(file_path), "supplier_a", "product", cache)

    assert first.skipped_cached is False
    assert second.skipped_cached is False
    assert second.processed == 1
