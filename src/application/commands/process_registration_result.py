from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ProcessRegistrationResultCommand:
    """Команда обработки ответа платформы account.registration.result."""

    service_name: str
    success: bool
    reason: str = ""
    status: str = ""
    account_id: str = ""
    group_id: str = ""
    strategy_id: str = ""
    causation_id: str | None = None
    secret_key: str = ""
    timestamp: datetime | None = None
