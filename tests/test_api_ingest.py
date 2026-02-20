from fastapi.testclient import TestClient

from app.api import routes
from app.main import app
from app.models.pydantic_models import RunSummary


def test_ingest_endpoint_success(monkeypatch):
    def fake_run_pipeline(file_path, supplier_id, record_type, cache):
        return RunSummary(
            run_id="123",
            status="completed",
            processed=3,
            inserted=3,
            rejected=0,
            skipped_cached=False,
            errors=[],
        )

    monkeypatch.setattr("app.api.routes.run_pipeline", fake_run_pipeline)

    client = TestClient(app)
    response = client.post(
        "/ingest",
        json={
            "file_path": "data/incoming/supplier_a_products.csv",
            "supplier_id": "supplier_a",
            "record_type": "product",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["processed"] == 3
    assert payload["inserted"] == 3


def test_ingest_endpoint_bad_extension_returns_400(monkeypatch):
    def fake_run_pipeline(file_path, supplier_id, record_type, cache):
        raise ValueError("Unsupported file type: .xml")

    monkeypatch.setattr("app.api.routes.run_pipeline", fake_run_pipeline)

    client = TestClient(app)
    response = client.post(
        "/ingest",
        json={
            "file_path": "data/incoming/supplier_a_products.xml",
            "supplier_id": "supplier_a",
            "record_type": "product",
        },
    )

    assert response.status_code == 400


def test_ingest_endpoint_internal_errors_are_sanitized(monkeypatch):
    def fake_run_pipeline(file_path, supplier_id, record_type, cache):
        raise RuntimeError("sensitive backend exception")

    monkeypatch.setattr("app.api.routes.run_pipeline", fake_run_pipeline)

    client = TestClient(app)
    response = client.post(
        "/ingest",
        json={
            "file_path": "data/incoming/supplier_a_products.csv",
            "supplier_id": "supplier_a",
            "record_type": "product",
        },
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "Ingestion failed"


def test_health_degraded_when_dependencies_down(monkeypatch):
    class BrokenCache:
        def ping(self):
            return False

    class BrokenSession:
        def execute(self, query):
            raise RuntimeError("db unavailable")

    routes.cache_instance = BrokenCache()
    app.dependency_overrides[routes.get_session] = lambda: BrokenSession()

    client = TestClient(app)
    response = client.get("/health")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "degraded"
    assert payload["db"] == "down"
    assert payload["redis"] == "down"


def test_health_returns_503_when_cache_uninitialized():
    routes.cache_instance = None

    client = TestClient(app)
    response = client.get("/health")

    assert response.status_code == 503
    assert response.json()["detail"] == "Cache is unavailable"