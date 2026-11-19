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
        
        # 1. DEBUG: Что мы пытаемся отправить (помогает отладить маппинг)
        logger.debug(
            "Подготовка к публикации рекомендации: ticker=%s, seq=%s, signal=%s, confidence=%.4f, qty=%d",
            recommendation.ticker,
            recommendation.sequence_number,
            recommendation.signal.value,
            recommendation.confidence_value,
            recommendation.suggested_quantity,
        )

        # 2. Маппинг и формирование ключа
        payload_json = RecommendationMapper.to_kafka_json(
            recommendation=recommendation,
            registration=self._registration,
            secret_key=self._settings.effective_secret_key,
            schema_version=self._settings.schema_version,
        )
        
        source_id = recommendation.source_event_id or recommendation.recommendation_id
        message_key = source_id.encode("utf-8")

        # 3. Отправка (Если producer=None, _send теперь выбросит исключение)
        await self._send(
            topic=self._settings.topic_recommendation_created,
            key=message_key,
            value=payload_json.encode("utf-8"),
        )

        # 4. INFO: Честный лог об успехе (выполняется ТОЛЬКО если _send не выбросил ошибку)
        logger.info(
            "Рекомендация успешно отправлена в Kafka (%s): ticker=%s, seq=%s, signal=%s, confidence=%.4f, qty=%d",
            self._settings.topic_recommendation_created,
            recommendation.ticker,
            recommendation.sequence_number,
            recommendation.signal.value,
            recommendation.confidence_value,
            recommendation.suggested_quantity,
        )

        # 5. WARNING: Проактивная проверка бизнес-аномалий
        if recommendation.signal.value != "HOLD" and recommendation.suggested_quantity == 0:
            logger.warning(
                "Аномалия в рекомендации: сигнал %s, но suggested_quantity=0. "
                "Возможно, стратегия не рассчитала объём (ticker=%s, seq=%s)",
                recommendation.signal.value,
                recommendation.ticker,
                recommendation.sequence_number,
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
        # Исправлена формулировка на технически точную
        logger.info(
            "Запрос регистрации отправлен в Kafka (%s): eventId=%s, serviceName=%s, accountId=%s, groupId=%s, strategyId=%s",
            self._settings.topic_registration_request,
            event_id,
            registration.service_name,
            registration.account_id,
            registration.group_id,
            registration.strategy_id,
        )
        return event_id

    async def _send(self, topic: str, key: bytes, value: bytes) -> None:
        """Внутренний метод отправки. Fail-fast при отсутствии продюсера."""
        if self._manager.producer is None:
            # КРИТИЧЕСКИ ВАЖНО: Выбрасываем ошибку, а не молча возвращаем None.
            # Это гарантирует, что логи "успешно отправлено" выше будут правдивыми.
            raise RuntimeError(f"Kafka Producer не активен, отправка в тему '{topic}' невозможна")

        await self._manager.publish(topic=topic, key=key, value=value)