from dataclasses import dataclass
from datetime import datetime

from src.domain.events.base import DomainEvent
from src.domain.value_objects.signal import Signal


@dataclass(frozen=True)
class RecommendationCreatedEvent(DomainEvent):
    """Событие: сформирована торговая рекомендация."""

    ticker: str = ""
    timestamp: datetime | None = None
    signal: Signal = Signal.HOLD
    close: float = 0.0
    upper_band: float = 0.0
    lower_band: float = 0.0
    confidence: float = 0.0

