from decimal import Decimal
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

SKU_PATTERN = r"^[A-Za-z0-9_-]{3,40}$"


class ProductIn(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    sku: str = Field(..., pattern=SKU_PATTERN)
    price: Decimal = Field(..., gt=0)
    quantity: int = Field(..., ge=0)
    supplier_id: str = Field(..., min_length=1)
    status: Literal["active", "inactive", "backorder", "discontinued"]


class OrderIn(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True)

    order_id: str = Field(..., min_length=1)
    sku: str = Field(..., pattern=SKU_PATTERN)
    quantity: int = Field(..., gt=0)
    supplier_id: str = Field(..., min_length=1)
    status: Literal["pending", "processing", "shipped", "cancelled", "returned"]
    price: Optional[Decimal] = None

    @field_validator("price")
    @classmethod
    def validate_price(cls, value: Optional[Decimal]) -> Optional[Decimal]:
        if value is not None and value <= 0:
            raise ValueError("price must be greater than 0 when provided")
        return value


class IngestRequest(BaseModel):
    file_path: str
    supplier_id: str = Field(..., min_length=1)
    record_type: Literal["product", "order"]


class RunSummary(BaseModel):
    run_id: str
    status: str
    processed: int
    inserted: int
    rejected: int
    skipped_cached: bool
    errors: list[dict]


class HealthStatus(BaseModel):
    status: str
    db: str
    redis: str


__all__ = [
    "HealthStatus",
    "IngestRequest",
    "OrderIn",
    "ProductIn",
    "RunSummary",
    "SKU_PATTERN",
    "ValidationError",
]
