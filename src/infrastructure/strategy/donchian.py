from src.domain.entities.candle import Candle
from src.domain.entities.channel import DonchianChannel
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.ports.strategy import StrategyPort


class DonchianStrategyAdapter(StrategyPort):
    """
    Адаптер стратегии канала Дончяна (Infrastructure Layer).

    Реализует доменный порт StrategyPort.
    """

    def __init__(self, window: int = 20) -> None:
        self._channel = DonchianChannel(window=window)

    @property
    def window(self) -> int:
        return self._channel.window

    def evaluate(self, candle: Candle) -> TradeRecommendation:
        recommendation, _event = self._channel.process_candle(candle)
        return recommendation

