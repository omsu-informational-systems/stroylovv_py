from dataclasses import dataclass
from datetime import datetime


@dataclass(kw_only=True)
class ServiceRegistration:
    """
    Доменная сущность: статус регистрации микросервиса на платформе.
    """

    service_name: str
    registered_at: datetime | None = None
    is_active: bool = False

    def activate(self, timestamp: datetime) -> None:
        """Активировать регистрацию по ответу платформы."""
        self.is_active = True
        self.registered_at = timestamp

