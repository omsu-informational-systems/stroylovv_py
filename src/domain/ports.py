"""
Порты (Protocol-интерфейсы) доменного слоя.

Правило DDD / ports-adapters скилла:
- только typing.Protocol или abc.ABC
- нет импортов из infrastructure / application / api
- конкретные реализации — в infrastructure/
"""

from typing import Protocol

from src.domain.value_objects import Candle, TradeSignal


class StrategyPort(Protocol):
    """Контракт торговой стратегии (входящий порт)."""

    def evaluate(self, candle: Candle) -> TradeSignal:
        """Оценить свечу и вернуть торговый сигнал."""
        ...
