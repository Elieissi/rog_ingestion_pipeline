from app.ingestion.normalizer import normalize_record, normalize_records


def test_normalizer_maps_aliases_to_canonical_keys():
    row = {
        "item_sku": "SKU-1",
        "qty": "11",
        "vendor": "supplier_z",
        "state": "ACTIVE",
        "unit_price": "13.45",
    }
    normalized = normalize_record(row, default_supplier_id="fallback", record_type="product")
    assert normalized["sku"] == "SKU-1"
    assert normalized["quantity"] == 11
    assert normalized["supplier_id"] == "supplier_z"
    assert normalized["status"] == "active"
    assert str(normalized["price"]) == "13.45"


def test_normalizer_order_adds_order_id():
    row = {
        "order_number": "ORD-9",
        "sku_code": "SKU-9",
        "quantity": "2",
        "supplier": "sup_9",
        "status": "processing",
    }
    normalized = normalize_record(row, default_supplier_id="fallback", record_type="order")
    assert normalized["order_id"] == "ORD-9"
    assert normalized["sku"] == "SKU-9"


def test_normalizer_batch_uses_fallback_supplier():
    rows = [{"sku": "SKU-1", "inventory": "4", "status": "inactive", "price": "9.99"}]
    normalized = normalize_records(rows, supplier_id="fallback_supplier", record_type="product")
    assert normalized[0]["supplier_id"] == "fallback_supplier"


def test_normalizer_uses_deterministic_alias_precedence():
    row = {
        "sku": "PRIMARY",
        "item_sku": "SECONDARY",
        "supplier_id": "supplier_primary",
        "vendor": "supplier_secondary",
        "quantity": "1",
        "price": "4.00",
        "status": "active",
    }

    normalized = normalize_record(row, default_supplier_id="fallback", record_type="product")

    assert normalized["sku"] == "PRIMARY"
    assert normalized["supplier_id"] == "supplier_primary"