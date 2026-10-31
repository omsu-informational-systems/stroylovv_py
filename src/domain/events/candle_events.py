from dataclasses import dataclass
from datetime import datetime

from src.domain.events.base import DomainEvent


@dataclass(frozen=True)
class CandleReceivedEvent(DomainEvent):
    """Событие: получена биржевая свеча."""

    ticker: str = ""
    timestamp: datetime | None = None
    close: float = 0.0

