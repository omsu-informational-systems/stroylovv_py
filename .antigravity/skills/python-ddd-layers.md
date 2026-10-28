# Skill: Python DDD Layers & Boundary Enforcement

## Objective
Enforce strict separation between Domain, Application, Infrastructure, and Interface layers in Python.

## Core Rules
1. **Domain Layer (`domain/`)**:
   - Zero third-party runtime dependencies (no SQLAlchemy, no Django ORM, no Pydantic, no HTTP libraries).
   - Use standard library only: `dataclasses`, `typing`, `abc`, `uuid`, `datetime`.
   - Never import from `application`, `infrastructure`, or `interfaces`.
   - Pure business logic, Domain Exceptions, Value Objects, Entities, Aggregate Roots, and Repository Protocols.

2. **Application Layer (`application/`)**:
   - Contains Use Cases / Command Handlers / Query Handlers.
   - Coordinates Domain objects and Port interfaces.
   - Accepts primitive/DTO inputs, produces DTO outputs. Does not interact with HTTP/CLI directly.

3. **Infrastructure Layer (`infrastructure/`)**:
   - Implements ports/protocols defined in `domain/` or `application/`.
   - Houses SQLAlchemy mappers, Redis caches, HTTP clients, message brokers.
   - Responsible for converting persistence models to Domain Entities and vice versa (Data Mappers).

4. **Interface / Presentation Layer (`interfaces/` or `api/`)**:
   - FastAPI routers, Click/Typer CLI, webhooks.
   - Handles HTTP serialization (Pydantic models live here, NOT in the domain).
   - Invokes Application Use Cases and translates domain errors into HTTP/CLI status codes.

## Code Example: Dependency Direction Check
```python
# CORRECT (domain/order.py)
from dataclasses import dataclass
from uuid import UUID

@dataclass
class Order:
    id: UUID
    ...

# STRICTLY FORBIDDEN in domain/
from sqlalchemy.orm import declarative_base  # No ORM models in domain
from pydantic import BaseModel             # Keep Pydantic in API/Application DTOs
from fastapi import HTTPException          # No framework exceptions