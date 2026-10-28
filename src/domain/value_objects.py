"""
Value Objects доменного слоя.

Правила DDD-скилла:
- только stdlib (dataclasses, typing, decimal, uuid, datetime)
- frozen=True для иммутабельности
- валидация инвариантов в __post_init__
- никаких Pydantic / SQLAlchemy / FastAPI импортов
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from src.domain.exceptions import InvalidPriceError


class Signal(str, Enum):
    """Торговый сигнал стратегии."""

    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class CandleInterval(str, Enum):
    """Временной интервал свечи."""

    DAILY = "1d"


@dataclass(frozen=True)
class Price:
    """
    Value Object: цена актива.

    Инвариант: значение строго положительно.
    """

    value: float

    def __post_init__(self) -> None:
        if self.value <= 0:
            raise InvalidPriceError(f"Цена должна быть положительной, получено: {self.value}")


@dataclass(frozen=True)
class Candle:
    """
    Value Object: биржевая свеча (OHLCV).

    Инварианты:
    - high >= low (иначе данные некорректны)
    - все цены строго положительны (делегировано Price)
    """

    ticker: str
    open: Price
    high: Price
    low: Price
    close: Price
    volume: float

    def __post_init__(self) -> None:
        if self.high.value < self.low.value:
            raise InvalidPriceError(
                f"High ({self.high.value}) не может быть меньше Low ({self.low.value})"
            )
        if self.volume < 0:
            raise InvalidPriceError(f"Объём не может быть отрицательным: {self.volume}")


@dataclass(frozen=True)
class DonchianBands:
    """
    Value Object: границы канала Дончяна.

    Содержит верхнюю и нижнюю границы, вычисленные по скользящему окну.
    """

    upper: float
    lower: float

    def __post_init__(self) -> None:
        if self.upper < self.lower:
            raise InvalidPriceError(
                f"Upper ({self.upper}) не может быть меньше Lower ({self.lower})"
            )

    @property
    def width(self) -> float:
        """Ширина канала."""
        return self.upper - self.lower


@dataclass(frozen=True)
class TradeSignal:
    """
    Value Object: результат анализа свечи.

    Содержит торговый сигнал, границы канала и метрику силы пробоя.
    """

    ticker: str
    signal: Signal
    close: float
    bands: DonchianBands
    confidence: float  # [0.0, 1.0]
