from src.domain.entities.candle import Candle
from src.domain.entities.hybrid_basket import HybridTrioModel
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.ports.strategy import StrategyPort


class HybridTrioStrategyAdapter(StrategyPort):
    """
    Адаптер гибридной стратегии для связки SBER + T-Bank + OZON (Infrastructure Layer).

    Реализует StrategyPort:
    - SBER + T: парный арбитраж с подтверждением тренда Сбера
    - OZON: импульсный трендовый моментум с фильтром объёма
    - Прочие: защита капитала (HOLD)
    """

    def __init__(
        self,
        donchian_window: int = 20,
        pairs_window: int = 30,
        z_entry: float = 1.8,
        z_exit: float = 0.5,
        volume_multiplier: float = 1.25,
    ) -> None:
        self._model = HybridTrioModel(
            donchian_window=donchian_window,
            pairs_window=pairs_window,
            z_entry=z_entry,
            z_exit=z_exit,
            volume_multiplier=volume_multiplier,
        )

    def evaluate(self, candle: Candle) -> TradeRecommendation:
        recommendation, _event = self._model.process_candle(candle)
        return recommendation

