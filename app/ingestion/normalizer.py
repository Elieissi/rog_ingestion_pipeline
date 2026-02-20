from decimal import Decimal

KEY_ALIASES = {
    "sku": ("sku", "item_sku", "sku_code"),
    "quantity": ("quantity", "qty", "stock", "inventory"),
    "supplier_id": ("supplier_id", "vendor", "supplier"),
    "status": ("status", "state"),
    "price": ("price", "unit_price", "cost"),
    "order_id": ("order_id", "order", "order_number", "id"),
}


def _pick_value(record: dict, canonical_key: str):
    for source_key in KEY_ALIASES[canonical_key]:
        if source_key in record:
            return record[source_key]
    return None


def _normalize_value(value):
    if isinstance(value, str):
        return value.strip()
    return value


def normalize_record(record: dict, default_supplier_id: str, record_type: str) -> dict:
    cleaned = {k.strip().lower(): _normalize_value(v) for k, v in record.items() if str(k).strip()}

    normalized = {
        "sku": _pick_value(cleaned, "sku"),
        "quantity": _pick_value(cleaned, "quantity"),
        "supplier_id": _pick_value(cleaned, "supplier_id") or default_supplier_id,
        "status": (_pick_value(cleaned, "status") or "").lower(),
        "price": _pick_value(cleaned, "price"),
    }

    if record_type == "order":
        normalized["order_id"] = _pick_value(cleaned, "order_id")

    if normalized.get("quantity") not in (None, ""):
        try:
            normalized["quantity"] = int(normalized["quantity"])
        except (TypeError, ValueError):
            # Keep original value so Pydantic can surface a structured validation error.
            pass

    if normalized.get("price") not in (None, ""):
        try:
            normalized["price"] = Decimal(str(normalized["price"]))
        except (ArithmeticError, TypeError, ValueError):
            # Keep original value for downstream validation.
            pass
    elif record_type == "product":
        normalized["price"] = None

    return normalized


def normalize_records(records: list[dict], supplier_id: str, record_type: str) -> list[dict]:
    return [normalize_record(record, supplier_id, record_type) for record in records]