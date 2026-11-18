from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

from src.domain.exceptions.base import InvalidPriceError


@dataclass(frozen=True)
class Price:
    """
    Value Object: цена биржевого актива.

    Использует десятичную арифметику (Decimal) для точного хранения
    цен и денежных сумм из десятичных строк протокола платформы.
    Инвариант: цена конечна и строго положительна (> 0).
    """

    amount: Decimal

    def __init__(self, value: Decimal | float | int | str) -> None:
        if isinstance(value, bool):
            raise InvalidPriceError(f"Некорректный тип цены: {value!r}")
        try:
            dec = value if isinstance(value, Decimal) else Decimal(str(value).strip())
        except (InvalidOperation, AttributeError) as exc:
            raise InvalidPriceError(f"Некорректный формат цены: {value!r}") from exc

        if not dec.is_finite() or dec <= Decimal("0"):
            raise InvalidPriceError(
                f"Цена должна быть строго положительной, получено: {value}"
            )
        object.__setattr__(self, "amount", dec)

    @property
    def value(self) -> float:
        """Числовое представление цены для статистических расчётов."""
        return float(self.amount)

    @property
    def decimal_value(self) -> Decimal:
        """Точное десятичное значение цены."""
        return self.amount

    def to_decimal_str(self) -> str:
        """Представление цены в виде десятичной строки (формат протокола)."""
        return format(self.amount, "f")

    def __float__(self) -> float:
        return float(self.amount)
