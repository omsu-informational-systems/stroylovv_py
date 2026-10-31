from src.domain.entities.candle import Candle
from src.domain.entities.pair import PairsTradingModel
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.ports.strategy import StrategyPort


class PairsTradingStrategyAdapter(StrategyPort):
    """
    Адаптер стратегии парного трейдинга (Infrastructure Layer).

    Реализует StrategyPort для связки SBER (бенчмарк) и T (первичный торгуемый актив).
    """

    def __init__(
        self,
        primary_ticker: str = "T",
        benchmark_ticker: str = "SBER",
        window: int = 30,
        z_entry: float = 1.8,
        z_exit: float = 0.5,
    ) -> None:
        self._model = PairsTradingModel(
            primary_ticker=primary_ticker,
            benchmark_ticker=benchmark_ticker,
            window=window,
            z_entry=z_entry,
            z_exit=z_exit,
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

