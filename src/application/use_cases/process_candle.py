import logging
import time

from src.application.commands.process_candle import ProcessCandleCommand
from src.application.dtos.recommendation import TradeRecommendationDTO
from src.domain.entities.candle import Candle
from src.domain.entities.registration import ServiceRegistration
from src.domain.ports.publisher import RecommendationPublisherPort
from src.domain.ports.strategy import StrategyPort
from src.domain.value_objects.interval import CandleInterval
from src.domain.value_objects.price import Price

logger = logging.getLogger(__name__)


class ProcessCandleUseCase:
    """
    Сценарий использования: обработка биржевой свечи (market.candle.closed).

    Координирует:
    1. Проверку дедупликации по eventId (идемпотентность в рамках процесса).
    2. Создание доменной сущности Candle с десятичной валидацией Price.
    3. Оценку свечи доменной стратегией StrategyPort.
    4. Привязку контекста события (correlationId, causationId/sourceEventId, runId, sequenceNumber).
    5. Публикацию рекомендации через RecommendationPublisherPort (если регистрация не отклонена).
    6. Обновление счётчиков телеметрии для heartbeat.
    """

    def __init__(
        self,
        strategy: StrategyPort,
        publisher: RecommendationPublisherPort | None = None,
        registration: ServiceRegistration | None = None,
        default_order_quantity: int = 1,
    ) -> None:
        self._strategy = strategy
        self._publisher = publisher
        self._registration = registration
        self._default_order_quantity = max(1, default_order_quantity)

    async def execute(self, command: ProcessCandleCommand) -> TradeRecommendationDTO | None:
        if (
            self._registration is not None
            and command.event_id
            and self._registration.is_duplicate_event(command.event_id)
        ):
            logger.debug(
                "Пропуск повторно полученного события свечи eventId=%s (ticker=%s, seq=%s)",
                command.event_id,
                command.ticker,
                command.sequence_number,
            )
            return None

        started = time.perf_counter()

        interval = (
            CandleInterval(command.interval)
            if command.interval in CandleInterval._value2member_map_
            else CandleInterval.DAILY
        )

        candle = Candle(
            ticker=command.ticker,
            timestamp=command.timestamp,
            open=Price(command.open),
            high=Price(command.high),
            low=Price(command.low),
            close=Price(command.close),
            volume=command.volume,
            interval=interval,
        )

        recommendation = self._strategy.evaluate(candle)
        elapsed_ms = int((time.perf_counter() - started) * 1000)

        recommendation.enrich_context(
            source_event_id=command.event_id,
            correlation_id=command.correlation_id,
            run_id=command.run_id,
            sequence_number=command.sequence_number,
            processing_time_ms=elapsed_ms,
            default_order_quantity=self._default_order_quantity,
        )

        published = False
        if self._registration is not None and not self._registration.can_publish_recommendations:
            logger.warning(
                "Публикация рекомендации (%s: %s) отклонена: регистрация сервиса заблокирована платформой. Причина: '%s'",
                recommendation.ticker,
                recommendation.signal.value,
                self._registration.rejection_reason,
            )
        elif self._publisher is not None:
            try:
                await self._publisher.publish_recommendation(recommendation)
                published = True
            except Exception as exc:
                logger.error("Ошибка при публикации рекомендации в брокер: %s", exc)
                raise

        if self._registration is not None:
            self._registration.mark_candle_processed(
                event_id=command.event_id,
                sequence_number=command.sequence_number,
                published=published,
            )

        return TradeRecommendationDTO(
            ticker=recommendation.ticker,
            timestamp=recommendation.timestamp,
            signal=recommendation.signal.value,
            close=recommendation.close_value,
            upper_band=recommendation.upper_band,
            lower_band=recommendation.lower_band,
            confidence=recommendation.confidence_value,
        )
