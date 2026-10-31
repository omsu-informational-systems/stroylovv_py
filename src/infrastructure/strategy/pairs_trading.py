from src.domain.entities.candle import Candle
from src.domain.entities.pair import PairsTradingModel
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.ports.strategy import StrategyPort
from src.infrastructure.config.settings import Settings, settings as default_settings


class PairsTradingStrategyAdapter(StrategyPort):
    """
    Адаптер стратегии парного трейдинга (Infrastructure Layer).

    Реализует StrategyPort. Все параметры (тикеры, окно, z-пороги)
    загружаются напрямую из конфигурации Settings (.env).
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        primary_ticker: str | None = None,
        benchmark_ticker: str | None = None,
        window: int | None = None,
        z_entry: float | None = None,
        z_exit: float | None = None,
    ) -> None:
        cfg = settings or default_settings
        self._model = PairsTradingModel(
            primary_ticker=primary_ticker or cfg.pairs_primary_ticker,
            benchmark_ticker=benchmark_ticker or cfg.pairs_benchmark_ticker,
            window=window if window is not None else cfg.pairs_window,
            z_entry=z_entry if z_entry is not None else cfg.pairs_z_entry,
            z_exit=z_exit if z_exit is not None else cfg.pairs_z_exit,
        )

    @property
    def primary_ticker(self) -> str:
        return self._model.primary_ticker

    @property
    def benchmark_ticker(self) -> str:
        return self._model.benchmark_ticker

    @property
    def window(self) -> int:
        return self._model.window

    def evaluate(self, candle: Candle) -> TradeRecommendation:
        recommendation, _event = self._model.process_candle(candle)
        return recommendation
