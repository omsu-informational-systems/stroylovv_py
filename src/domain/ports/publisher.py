from typing import Protocol, runtime_checkable

from src.domain.entities.recommendation import TradeRecommendation
from src.domain.entities.registration import ServiceRegistration


@runtime_checkable
class RecommendationPublisherPort(Protocol):
    """Порт публикации торговых рекомендаций."""

    async def publish_recommendation(self, recommendation: TradeRecommendation) -> None:
        """Опубликовать торговую рекомендацию с подтверждением доставки брокером."""
        ...


@runtime_checkable
class HeartbeatPublisherPort(Protocol):
    """Порт публикации сигнала состояния (heartbeat) сервиса."""

    async def publish_heartbeat(self, registration: ServiceRegistration) -> None:
        """Опубликовать heartbeat сервиса с актуальными метриками процесса."""
        ...


@runtime_checkable
class RegistrationPublisherPort(Protocol):
    """Порт отправки запроса на регистрацию сервиса."""

    async def publish_registration(self, registration: ServiceRegistration) -> str:
        """
        Опубликовать запрос на регистрацию микросервиса со стабильными идентификаторами.

        Возвращает eventId отправленного события account.registration.request.
        """
        ...
