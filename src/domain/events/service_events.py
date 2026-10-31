from dataclasses import dataclass
from datetime import datetime

from src.domain.events.base import DomainEvent


@dataclass(frozen=True)
class HeartbeatTriggeredEvent(DomainEvent):
    """Событие: сформирован сигнал состояния сервиса (heartbeat)."""

    service_name: str = ""
    timestamp: datetime | None = None

