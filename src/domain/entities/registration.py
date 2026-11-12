from dataclasses import dataclass
from datetime import datetime


@dataclass(kw_only=True)
class ServiceRegistration:
    """
    Доменная сущность: статус регистрации микросервиса на платформе.

    Отслеживает стабильные идентификаторы сервиса, студента, учебной группы
    и состояние подтверждения/отказа платформы.
    """

    service_name: str
    account_name: str = ""
    group_id: str = ""
    strategy_name: str = ""
    registered_at: datetime | None = None
    is_active: bool = False
    is_rejected: bool = False
    rejection_reason: str = ""

    def activate(self, timestamp: datetime) -> None:
        """Подтвердить успешную регистрацию платформой."""
        self.is_active = True
        self.is_rejected = False
        self.rejection_reason = ""
        self.registered_at = timestamp

    def reject(self, reason: str, timestamp: datetime | None = None) -> None:
        """Зафиксировать отказ в регистрации с указанием понятной причины."""
        self.is_active = False
        self.is_rejected = True
        self.rejection_reason = reason.strip() or "Причина отказа не указана платформой"
        self.registered_at = timestamp

    @property
    def can_publish_recommendations(self) -> bool:
        """
        Флаг разрешения публикации торговых рекомендаций.
        При отказе платформы публикация строго блокируется.
        """
        return not self.is_rejected
