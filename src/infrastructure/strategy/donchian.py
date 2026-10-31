from src.domain.entities.candle import Candle
from src.domain.entities.channel import DonchianChannel
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.ports.strategy import StrategyPort
from src.infrastructure.config.settings import Settings, settings as default_settings


class DonchianStrategyAdapter(StrategyPort):
    """
    Адаптер стратегии канала Дончяна (Infrastructure Layer).

    Реализует доменный порт StrategyPort.
    Параметр окна загружается напрямую из конфигурации Settings (.env).
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        window: int | None = None,
    ) -> None:
        cfg = settings or default_settings
        self._channel = DonchianChannel(
            window=window if window is not None else cfg.strategy_window
        )

    @property
    def window(self) -> int:
        return self._channel.window

    def evaluate(self, candle: Candle) -> TradeRecommendation:
        recommendation, _event = self._channel.process_candle(candle)
        return recommendation
