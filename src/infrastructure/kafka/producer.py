import logging

from src.domain.entities.recommendation import TradeRecommendation
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
    Адаптер публикации сообщений в Kafka (Infrastructure Layer).

    Реализует порты:
    - RecommendationPublisherPort
    - HeartbeatPublisherPort
    - RegistrationPublisherPort
    """

    def __init__(
        self,
        connection_manager: KafkaConnectionManager,
        settings: Settings,
    ) -> None:
        self._manager = connection_manager
        self._settings = settings

    async def publish_recommendation(self, recommendation: TradeRecommendation) -> None:
        """Публикация торговой рекомендации в тему strategy.recommendation.created."""
        payload_json = RecommendationMapper.to_kafka_json(
            recommendation=recommendation,
            service_name=self._settings.app_name,
            secret_key=self._settings.secret_key,
            schema_version=self._settings.schema_version,
        )
        await self._send(
            topic=self._settings.topic_recommendation_created,
            key=recommendation.ticker.encode("utf-8"),
            value=payload_json.encode("utf-8"),
        )
        logger.info(
            "Рекомендация опубликована в %s: ticker=%s, signal=%s, close=%.2f, confidence=%.4f",
            self._settings.topic_recommendation_created,
            recommendation.ticker,
            recommendation.signal.value,
            recommendation.close_value,
            recommendation.confidence_value,
        )

    async def publish_heartbeat(self, service_name: str) -> None:
        """Публикация сигнала состояния в тему account.service.heartbeat."""
        payload_json = EventMapper.to_heartbeat_json(
            service_name=service_name,
            secret_key=self._settings.secret_key,
            schema_version=self._settings.schema_version,
        )
        await self._send(
            topic=self._settings.topic_heartbeat,
            key=service_name.encode("utf-8"),
            value=payload_json.encode("utf-8"),
        )
        logger.debug("Heartbeat отправлен в тему %s", self._settings.topic_heartbeat)

    async def publish_registration(
        self,
        service_name: str,
        account_name: str = "",
        group_id: str = "",
        strategy_name: str = "",
    ) -> None:
        """Публикация запроса регистрации в тему account.registration.request."""
        acc_name = account_name or self._settings.account_name
        grp_id = group_id or self._settings.group_id
        strat_name = strategy_name or self._settings.strategy_name

        payload_json = EventMapper.to_registration_json(
            service_name=service_name,
            secret_key=self._settings.secret_key,
            account_name=acc_name,
            group_id=grp_id,
            strategy_name=strat_name,
            schema_version=self._settings.schema_version,
        )
        await self._send(
            topic=self._settings.topic_registration_request,
            key=service_name.encode("utf-8"),
            value=payload_json.encode("utf-8"),
        )
        logger.info(
            "Запрос регистрации отправлен в %s: serviceName=%s, accountName=%s, groupId=%s, strategyName=%s",
            self._settings.topic_registration_request,
            service_name,
            acc_name,
            grp_id,
            strat_name,
        )

    async def _send(self, topic: str, key: bytes, value: bytes) -> None:
        producer = self._manager.producer
        if producer is None:
            logger.debug(
                "Kafka Producer не активен, сообщение пропущено (тема: %s, ключ: %s)",
                topic,
                key.decode("utf-8", errors="ignore"),
            )
            return

        await producer.send_and_wait(topic=topic, key=key, value=value)
