import logging

from src.domain.entities.recommendation import TradeRecommendation
from src.domain.entities.registration import ServiceRegistration
from src.domain.ports.publisher import (
    HeartbeatPublisherPort,
    RecommendationPublisherPort,
    RegistrationPublisherPort,
)
from src.infrastructure.config.settings import Settings
from src.infrastructure.kafka.client import KafkaConnectionManager
from src.infrastructure.mappers.event_mapper import EventMapper
from src.infrastructure.mappers.recommendation_mapper import RecommendationMapper

logger = logging.getLogger(__name__)


class KafkaPublisherAdapter(
    RecommendationPublisherPort,
    HeartbeatPublisherPort,
    RegistrationPublisherPort,
):
    """
    Адаптер публикации сообщений в защищённую Kafka (Infrastructure Layer).

    Реализует порты:
    - RecommendationPublisherPort -> strategy.recommendation.created
    - HeartbeatPublisherPort -> account.service.heartbeat
    - RegistrationPublisherPort -> account.registration.request
    """

    def __init__(
        self,
        connection_manager: KafkaConnectionManager,
        settings: Settings,
        registration: ServiceRegistration,
    ) -> None:
        self._manager = connection_manager
        self._settings = settings
        self._registration = registration

    async def publish_recommendation(self, recommendation: TradeRecommendation) -> None:
        """Публикация торговой рекомендации в тему strategy.recommendation.created."""
        payload_json = RecommendationMapper.to_kafka_json(
            recommendation=recommendation,
            registration=self._registration,
            secret_key=self._settings.effective_secret_key,
            schema_version=self._settings.schema_version,
        )
        # Ключ сообщения Kafka: eventId исходной свечи (согласно эталонному протоколу)
        message_key = (
            recommendation.source_event_id or recommendation.recommendation_id
        ).encode("utf-8")

        await self._send(
            topic=self._settings.topic_recommendation_created,
            key=message_key,
            value=payload_json.encode("utf-8"),
        )
        logger.info(
            "Рекомендация подтверждена брокером (%s): instrument=%s, seq=%s, signal=%s, confidence=%.4f, qty=%d",
            self._settings.topic_recommendation_created,
            recommendation.ticker,
            recommendation.sequence_number,
            recommendation.signal.value,
            recommendation.confidence_value,
            recommendation.suggested_quantity,
        )

    async def publish_heartbeat(self, registration: ServiceRegistration) -> None:
        """Публикация сигнала состояния в тему account.service.heartbeat."""
        payload_json = EventMapper.to_heartbeat_json(
            registration=registration,
            secret_key=self._settings.effective_secret_key,
            app_version=self._settings.app_version,
            schema_version=self._settings.schema_version,
        )
        await self._send(
            topic=self._settings.topic_heartbeat,
            key=registration.service_name.encode("utf-8"),
            value=payload_json.encode("utf-8"),
        )
        logger.debug(
            "Heartbeat отправлен (%s): processed=%d, published=%d, lastSeq=%s",
            self._settings.topic_heartbeat,
            registration.processed_candles,
            registration.published_recommendations,
            registration.last_processed_sequence_number,
        )

    async def publish_registration(self, registration: ServiceRegistration) -> str:
        """Публикация запроса регистрации в тему account.registration.request."""
        event_id, payload_json = EventMapper.to_registration_envelope(
            registration=registration,
            secret_key=self._settings.effective_secret_key,
            schema_version=self._settings.schema_version,
        )
        await self._send(
            topic=self._settings.topic_registration_request,
            key=registration.service_name.encode("utf-8"),
            value=payload_json.encode("utf-8"),
        )
        logger.info(
            "Запрос регистрации подтверждён брокером (%s): eventId=%s, serviceName=%s, accountId=%s, groupId=%s, strategyId=%s",
            self._settings.topic_registration_request,
            event_id,
            registration.service_name,
            registration.account_id,
            registration.group_id,
            registration.strategy_id,
        )
        return event_id

    async def _send(self, topic: str, key: bytes, value: bytes) -> None:
        if self._manager.producer is None:
            logger.debug(
                "Kafka Producer не активен, сообщение пропущено (тема: %s)",
                topic,
            )
            return

        await self._manager.publish(topic=topic, key=key, value=value)
