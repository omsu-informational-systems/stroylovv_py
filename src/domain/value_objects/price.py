from dataclasses import dataclass

from src.domain.exceptions.base import InvalidPriceError


@dataclass(frozen=True)
class Price:
    """
    Value Object: цена биржевого актива.

    Инвариант: цена строго положительна (> 0).
    """

    value: float

    def __post_init__(self) -> None:
        if self.value <= 0:
            raise InvalidPriceError(f"Цена должна быть строго положительной, получено: {self.value}")

    def __float__(self) -> float:
        return float(self.value)

