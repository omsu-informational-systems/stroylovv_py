from typing import Protocol, runtime_checkable

from src.domain.entities.recommendation import TradeRecommendation


@runtime_checkable
class RecommendationPublisherPort(Protocol):
    """Порт публикации торговых рекомендаций."""

    async def publish_recommendation(self, recommendation: TradeRecommendation) -> None:
        """Опубликовать торговую рекомендацию."""
        ...


@runtime_checkable
class HeartbeatPublisherPort(Protocol):
    """Порт публикации сигнала состояния (heartbeat) сервиса."""

    async def publish_heartbeat(self, service_name: str) -> None:
        """Опубликовать heartbeat сервиса."""
        ...


@runtime_checkable
class RegistrationPublisherPort(Protocol):
    """Порт отправки запроса на регистрацию сервиса."""

    async def publish_registration(self, service_name: str) -> None:
        """Опубликовать запрос на регистрацию микросервиса."""
        ...

