from typing import Protocol, runtime_checkable

from src.domain.entities.candle import Candle
from src.domain.entities.recommendation import TradeRecommendation


@runtime_checkable
class StrategyPort(Protocol):
    """Порт алгоритмической стратегии анализа свечей."""

    def evaluate(self, candle: Candle) -> TradeRecommendation:
        """Оценить свечу и вернуть торговую рекомендацию."""
        ...

