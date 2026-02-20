from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from app.db.models import Order, Product


def upsert_products(session: Session, items: Iterable[dict]) -> int:
    rows = list(items)
    if not rows:
        return 0

    if session.bind and session.bind.dialect.name == "postgresql":
        stmt = insert(Product).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_products_sku_supplier",
            set_={
                "price": stmt.excluded.price,
                "quantity": stmt.excluded.quantity,
                "status": stmt.excluded.status,
                "updated_at": stmt.excluded.updated_at,
            },
        )
        session.execute(stmt)
        session.commit()
        return len(rows)

    # Fallback for non-Postgres test environments.
    inserted = 0
    for row in rows:
        existing = session.execute(
            select(Product).where(
                Product.sku == row["sku"],
                Product.supplier_id == row["supplier_id"],
            )
        ).scalar_one_or_none()
        if existing:
            existing.price = row["price"]
            existing.quantity = row["quantity"]
            existing.status = row["status"]
        else:
            session.add(Product(**row))
        inserted += 1
    session.commit()
    return inserted


def upsert_orders(session: Session, items: Iterable[dict]) -> int:
    rows = list(items)
    if not rows:
        return 0

    if session.bind and session.bind.dialect.name == "postgresql":
        stmt = insert(Order).values(rows)
        stmt = stmt.on_conflict_do_update(
            index_elements=["order_id"],
            set_={
                "sku": stmt.excluded.sku,
                "quantity": stmt.excluded.quantity,
                "supplier_id": stmt.excluded.supplier_id,
                "status": stmt.excluded.status,
                "price": stmt.excluded.price,
            },
        )
        session.execute(stmt)
        session.commit()
        return len(rows)

    inserted = 0
    for row in rows:
        existing = session.execute(select(Order).where(Order.order_id == row["order_id"])).scalar_one_or_none()
        if existing:
            existing.sku = row["sku"]
            existing.quantity = row["quantity"]
            existing.supplier_id = row["supplier_id"]
            existing.status = row["status"]
            existing.price = row.get("price")
        else:
            session.add(Order(**row))
        inserted += 1
    session.commit()
    return inserted
