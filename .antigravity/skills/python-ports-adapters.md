# Skill: Python Hexagonal Ports & Data Mapping

## Objective
Decouple persistence and external integrations from the domain using Protocols/ABCs and separate persistence schemas.

## Core Rules
1. **Ports (Domain/Application Boundaries)**:
   - Define Repository and Service contracts using `typing.Protocol` or `abc.ABC`.
   - Repositories must operate strictly on Aggregate Roots, simulating an in-memory set.

2. **Data Mapper Pattern (No Active Record)**:
   - Separate the SQLAlchemy/Tortoise/Django Table model from the Domain Entity.
   - Infrastructure must provide explicit mapper functions: `to_domain(db_model) -> DomainEntity` and `to_persistence(domain_entity) -> DBModel`.

3. **Unit of Work**:
   - Coordinate transactions across repositories using a Unit of Work context manager protocol.

## Code Example: Protocol & SQLAlchemy Implementation
```python
# 1. Port in domain/ports.py
from typing import Protocol, Optional
from uuid import UUID
from domain.order import Order

class OrderRepository(Protocol):
    def get_by_id(self, order_id: UUID) -> Optional[Order]:
        ...

    def save(self, order: Order) -> None:
        ...

# 2. Adapter in infrastructure/repositories.py
from sqlalchemy.orm import Session
from domain.order import Order
from infrastructure.models import OrderORMModel

class SqlAlchemyOrderRepository:
    def __init__(self, session: Session) -> None:
        self._session = session

    def get_by_id(self, order_id: UUID) -> Optional[Order]:
        row = self._session.query(OrderORMModel).filter_by(id=str(order_id)).first()
        return self._to_domain(row) if row else None

    def save(self, order: Order) -> None:
        orm_obj = self._to_persistence(order)
        self._session.merge(orm_obj)

    @staticmethod
    def _to_domain(orm: OrderORMModel) -> Order:
        return Order(id=UUID(orm.id), ...)

    @staticmethod
    def _to_persistence(domain: Order) -> OrderORMModel:
        return OrderORMModel(id=str(domain.id), ...)