import pytest
from pydantic import ValidationError

from app.models.pydantic_models import OrderIn, ProductIn


def test_product_valid_sku_accepts_expected_pattern():
    product = ProductIn(
        sku="ROG_SKU-123",
        price="19.99",
        quantity=5,
        supplier_id="supplier_a",
        status="active",
    )
    assert product.sku == "ROG_SKU-123"


def test_product_invalid_sku_rejected():
    with pytest.raises(ValidationError):
        ProductIn(
            sku="bad sku",
            price="19.99",
            quantity=5,
            supplier_id="supplier_a",
            status="active",
        )


def test_product_missing_fields_rejected():
    with pytest.raises(ValidationError):
        ProductIn(sku="AAA-1", price="9.99", quantity=2, supplier_id="", status="active")


def test_product_invalid_type_rejected():
    with pytest.raises(ValidationError):
        ProductIn(
            sku="AAA-1",
            price="not-a-number",
            quantity="abc",
            supplier_id="supplier_a",
            status="active",
        )


def test_order_invalid_status_rejected():
    with pytest.raises(ValidationError):
        OrderIn(
            order_id="ORD-1",
            sku="AAA-1",
            quantity=1,
            supplier_id="supplier_a",
            status="unknown",
        )
