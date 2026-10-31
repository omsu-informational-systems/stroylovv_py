from dataclasses import dataclass

from src.domain.exceptions.base import InvalidConfidenceError


@dataclass(frozen=True)
class Confidence:
    """
    Value Object: сила пробоя ценового коридора.

    Инвариант: значение находится в диапазоне [0.0, 1.0].
    """

    value: float

    def __post_init__(self) -> None:
        if not (0.0 <= self.value <= 1.0):
            raise InvalidConfidenceError(
                f"Confidence должна быть в пределах [0.0, 1.0], получено: {self.value}"
            )

    def __float__(self) -> float:
        return float(self.value)

