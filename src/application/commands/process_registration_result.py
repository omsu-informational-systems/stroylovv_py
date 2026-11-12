from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ProcessRegistrationResultCommand:
    """Команда обработки ответа платформы на регистрацию сервиса."""

    service_name: str
    success: bool
    reason: str = ""
    timestamp: datetime | None = None
