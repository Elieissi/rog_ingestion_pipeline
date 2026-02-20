from pathlib import Path

from app.scheduler.jobs import _resolve_feed_metadata


def test_resolve_feed_metadata_preserves_supplier_with_underscores():
    supplier_id, record_type = _resolve_feed_metadata(Path("supplier_a_products.csv"))
    assert supplier_id == "supplier_a"
    assert record_type == "product"


def test_resolve_feed_metadata_orders_suffix():
    supplier_id, record_type = _resolve_feed_metadata(Path("acme_us_orders.json"))
    assert supplier_id == "acme_us"
    assert record_type == "order"


def test_resolve_feed_metadata_invalid_name_returns_none():
    assert _resolve_feed_metadata(Path("unknownfile.csv")) is None