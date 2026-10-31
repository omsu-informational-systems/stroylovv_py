from dataclasses import dataclass
from datetime import datetime

from src.domain.value_objects.bands import DonchianBands
from src.domain.value_objects.confidence import Confidence
from src.domain.value_objects.price import Price
from src.domain.value_objects.signal import Signal


@dataclass(kw_only=True)
class TradeRecommendation:
    """
    Доменная сущность: торговая рекомендация.

    Результат оценки рыночной свечи алгоритмом стратегии пробоя.
    """

    ticker: str
    timestamp: datetime
    signal: Signal
    close: Price
    bands: DonchianBands
    confidence: Confidence

    @property
    def upper_band(self) -> float:
        return self.bands.upper

    @property
    def lower_band(self) -> float:
        return self.bands.lower

    @property
    def close_value(self) -> float:
        return self.close.value

    @property
    def confidence_value(self) -> float:
        return self.confidence.value

