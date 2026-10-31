from src.domain.entities.candle import Candle
from src.domain.entities.hybrid_basket import HybridTrioModel
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.ports.strategy import StrategyPort
from src.infrastructure.config.settings import Settings, settings as default_settings


class HybridTrioStrategyAdapter(StrategyPort):
    """
    Адаптер гибридной стратегии (Infrastructure Layer).

    Реализует StrategyPort:
    - primary_ticker + benchmark_ticker: парный арбитраж с подтверждением тренда
    - momentum_ticker: импульсный трендовый моментум с фильтром объёма
    - прочие тикеры: защита капитала (HOLD)

    Все тикеры и параметры загружаются напрямую из конфигурации Settings (.env).
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        primary_ticker: str | None = None,
        benchmark_ticker: str | None = None,
        momentum_ticker: str | None = None,
        donchian_window: int | None = None,
        pairs_window: int | None = None,
        z_entry: float | None = None,
        z_exit: float | None = None,
        volume_multiplier: float | None = None,
    ) -> None:
        cfg = settings or default_settings
        self._model = HybridTrioModel(
            primary_ticker=primary_ticker or cfg.hybrid_primary_ticker,
            benchmark_ticker=benchmark_ticker or cfg.hybrid_benchmark_ticker,
            momentum_ticker=momentum_ticker or cfg.hybrid_momentum_ticker,
            donchian_window=donchian_window if donchian_window is not None else cfg.hybrid_donchian_window,
            pairs_window=pairs_window if pairs_window is not None else cfg.hybrid_pairs_window,
            z_entry=z_entry if z_entry is not None else cfg.hybrid_z_entry,
            z_exit=z_exit if z_exit is not None else cfg.hybrid_z_exit,
            volume_multiplier=volume_multiplier if volume_multiplier is not None else cfg.hybrid_volume_multiplier,
        )

    @property
    def primary_ticker(self) -> str:
        return self._model.primary_ticker

    @property
    def benchmark_ticker(self) -> str:
        return self._model.benchmark_ticker

    @property
    def momentum_ticker(self) -> str:
        return self._model.momentum_ticker

    def evaluate(self, candle: Candle) -> TradeRecommendation:
        recommendation, _event = self._model.process_candle(candle)
        return recommendation
