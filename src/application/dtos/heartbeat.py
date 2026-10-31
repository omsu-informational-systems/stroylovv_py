from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class HeartbeatDTO:
    """DTO сигнала состояния сервиса."""

    service_name: str
    timestamp: datetime

