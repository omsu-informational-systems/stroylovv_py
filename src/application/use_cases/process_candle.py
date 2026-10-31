import logging

from src.application.commands.process_candle import ProcessCandleCommand
from src.application.dtos.recommendation import TradeRecommendationDTO
from src.domain.entities.candle import Candle
from src.domain.ports.publisher import RecommendationPublisherPort
from src.domain.ports.strategy import StrategyPort
from src.domain.value_objects.interval import CandleInterval
from src.domain.value_objects.price import Price

logger = logging.getLogger(__name__)


class ProcessCandleUseCase:
    """
    Сценарий использования: обработка биржевой свечи.

    Координирует создание доменной сущности Candle,
    вызов доменной стратегии StrategyPort
    и публикацию результата через RecommendationPublisherPort.
    """

    def __init__(
        self,
        strategy: StrategyPort,
        publisher: RecommendationPublisherPort | None = None,
    ) -> None:
        self._strategy = strategy
        self._publisher = publisher

    async def execute(self, command: ProcessCandleCommand) -> TradeRecommendationDTO:
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

        if self._publisher is not None:
            try:
                await self._publisher.publish_recommendation(recommendation)
            except Exception as exc:
                logger.error("Ошибка при публикации рекомендации в брокер: %s", exc)
                raise

        return TradeRecommendationDTO(
            ticker=recommendation.ticker,
            timestamp=recommendation.timestamp,
            signal=recommendation.signal.value,
            close=recommendation.close_value,
            upper_band=recommendation.upper_band,
            lower_band=recommendation.lower_band,
            confidence=recommendation.confidence_value,
        )

