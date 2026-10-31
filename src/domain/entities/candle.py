from dataclasses import dataclass
from datetime import datetime, timezone

from src.domain.exceptions.base import InvalidCandleDataError
from src.domain.value_objects.interval import CandleInterval
from src.domain.value_objects.price import Price


@dataclass(kw_only=True)
class Candle:
    """
    Доменная сущность: биржевая свеча (OHLCV).

    Инкапсулирует бизнес-правила валидации котировок.
    """

    ticker: str
    timestamp: datetime
    open: Price
    high: Price
    low: Price
    close: Price
    volume: float
    interval: CandleInterval = CandleInterval.DAILY

    def __post_init__(self) -> None:
        self._validate_invariants()

    def _validate_invariants(self) -> None:
        if not self.ticker or not self.ticker.strip():
            raise InvalidCandleDataError("Тикер инструмента не может быть пустым")

        if self.high.value < self.low.value:
            raise InvalidCandleDataError(
                f"High ({self.high.value}) не может быть меньше Low ({self.low.value})"
            )

        if self.volume < 0:
            raise InvalidCandleDataError(f"Объём не может быть отрицательным: {self.volume}")

        if self.high.value < self.open.value or self.high.value < self.close.value:
            raise InvalidCandleDataError(
                f"High ({self.high.value}) должен быть максимальным значением свечи"
            )

        if self.low.value > self.open.value or self.low.value > self.close.value:
            raise InvalidCandleDataError(
                f"Low ({self.low.value}) должен быть минимальным значением свечи"
            )

