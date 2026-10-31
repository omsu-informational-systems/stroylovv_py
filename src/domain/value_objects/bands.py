from dataclasses import dataclass

from src.domain.exceptions.base import InvalidPriceError


@dataclass(frozen=True)
class DonchianBands:
    """
    Value Object: границы ценового коридора Дончяна.

    Инвариант: верхняя граница не может быть ниже нижней (upper >= lower).
    """

    upper: float
    lower: float

    def __post_init__(self) -> None:
        if self.upper < self.lower:
            raise InvalidPriceError(
                f"Upper band ({self.upper}) не может быть меньше Lower band ({self.lower})"
            )

    @property
    def width(self) -> float:
        """Ширина канала."""
        return self.upper - self.lower

