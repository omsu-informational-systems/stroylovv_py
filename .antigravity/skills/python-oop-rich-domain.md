# Skill: Python Rich Domain Models & Clean OOP

## Objective
Prevent Anemic Domain Models (models with only attributes/getters/setters). Enforce "Tell, Don't Ask" and explicit invariants.

## Core Rules
1. **Value Objects**:
   - Define using `@dataclass(frozen=True)` or `NamedTuple` to enforce immutability.
   - Must validate invariants inside `__post_init__`.
   - Equality is based strictly on attributes.

2. **Entities & Aggregates**:
   - Mark mutable identity objects via `@dataclass(kw_only=True)`.
   - Never expose public mutations without invariants.
   - Do not allow external code to set attributes directly (e.g. `order.status = "PAID"` is forbidden). Use expressive domain methods: `order.mark_as_paid(payment_ref)`.
   - Aggregate Roots protect the internal state of child entities.

3. **Typing & Strictness**:
   - Always use type annotations (`typing.Self`, `typing.Protocol`, `typing.NewType`).
   - Use custom Domain Exceptions derived from a base `DomainException`, never generic `ValueError` or `Exception`.

## Code Example: Rich Model vs Anemic Model
```python
# GOOD: Rich Domain Entity + Value Object
from dataclasses import dataclass
from decimal import Decimal
from typing import Self
from domain.exceptions import InsufficientFundsError, InvalidAmountError

@dataclass(frozen=True)
class Money:
    amount: Decimal
    currency: str

    def __post_init__(self) -> None:
        if self.amount < Decimal("0"):
            raise InvalidAmountError("Money amount cannot be negative")

    def add(self, other: Self) -> Self:
        if self.currency != other.currency:
            raise ValueError("Currency mismatch")
        return Money(self.amount + other.amount, self.currency)

@dataclass(kw_only=True)
class Wallet:
    id: str
    balance: Money

    def withdraw(self, amount: Money) -> None:
        if self.balance.amount < amount.amount:
            raise InsufficientFundsError("Insufficient funds for withdrawal")
        self.balance = Money(self.balance.amount - amount.amount, self.balance.currency)