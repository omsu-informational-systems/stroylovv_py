from datetime import datetime, timezone
import pytest

from src.domain.entities.candle import Candle
from src.domain.entities.hybrid_basket import HybridTrioModel
from src.domain.exceptions.base import InvalidPairConfigError
from src.domain.ports.strategy import StrategyPort
from src.domain.value_objects.price import Price
from src.domain.value_objects.signal import Signal
from src.infrastructure.strategy.hybrid_trio import HybridTrioStrategyAdapter


def _candle(
    ticker: str,
    price: float,
    volume: float = 1000.0,
    high: float | None = None,
    low: float | None = None,
) -> Candle:
    now = datetime.now(timezone.utc)
    h = high if high is not None else price + 0.5
    l = low if low is not None else price - 0.5
    return Candle(
        ticker=ticker,
        timestamp=now,
        open=Price(price),
        high=Price(h),
        low=Price(l),
        close=Price(price),
        volume=volume,
    )


def test_hybrid_trio_invariants() -> None:
    with pytest.raises(InvalidPairConfigError):
        HybridTrioModel(donchian_window=1)

    with pytest.raises(InvalidPairConfigError):
        HybridTrioModel(pairs_window=2)

    with pytest.raises(InvalidPairConfigError):
        HybridTrioModel(z_entry=-1.0)

    with pytest.raises(InvalidPairConfigError):
        HybridTrioModel(z_entry=1.5, z_exit=1.6)


def test_hybrid_foreign_tickers_hold() -> None:
    model = HybridTrioModel()
    for foreign_ticker in ["GAZP", "MAGN", "MOEX", "TATN"]:
        rec, _ = model.process_candle(_candle(foreign_ticker, 150.0))
        assert rec.ticker == foreign_ticker
        assert rec.signal == Signal.HOLD
        assert rec.confidence_value == 0.0


def test_hybrid_weekend_trap_protection() -> None:
    model = HybridTrioModel()
    # Zero volume on OZON, SBER, T
    for ticker in ["OZON", "SBER", "T"]:
        rec, _ = model.process_candle(_candle(ticker, 200.0, volume=0.0))
        assert rec.signal == Signal.HOLD

        # Flat frozen candle (high == low)
        rec2, _ = model.process_candle(_candle(ticker, 200.0, high=200.0, low=200.0))
        assert rec2.signal == Signal.HOLD


def test_hybrid_ozon_volume_breakout() -> None:
    window = 3
    model = HybridTrioModel(donchian_window=window, volume_multiplier=1.2)

    # 1. Прогрев 3 свечи в диапазоне 3500..3550 со средним объёмом 1000
    for _ in range(window):
        rec, _ = model.process_candle(_candle("OZON", 3520.0, volume=1000.0, high=3550.0, low=3500.0))
        assert rec.signal == Signal.HOLD

    # 2. Ложный пробой: цена пробила 3550 (close=3600), но объём низкий (500 < 1200) -> HOLD
    rec_false, _ = model.process_candle(_candle("OZON", 3600.0, volume=500.0, high=3610.0, low=3540.0))
    assert rec_false.signal == Signal.HOLD

    # 3. Истинный пробой: цена пробила 3550 (close=3650) и объём высокий (2000 >= 1200) -> BUY
    rec_true, _ = model.process_candle(_candle("OZON", 3650.0, volume=2000.0, high=3660.0, low=3590.0))
    assert rec_true.signal == Signal.BUY
    assert rec_true.confidence_value > 0.0


def test_hybrid_banking_synergy_double_confirmation() -> None:
    p_window = 4
    d_window = 3
    model = HybridTrioModel(
        donchian_window=d_window,
        pairs_window=p_window,
        z_entry=1.5,
        z_exit=0.4,
    )

    # 1. Разогрев связки SBER + T
    for _ in range(p_window):
        model.process_candle(_candle("SBER", 290.0, high=292.0, low=288.0))
        model.process_candle(_candle("T", 270.0, high=272.0, low=268.0))

    # 2. SBER совершает сильный импульс вверх (close=305 > upper=292) -> sber_trending_up = True
    model.process_candle(_candle("SBER", 305.0, high=306.0, low=295.0))

    # 3. В этот же момент Т-Банк отстаёт (T=250 -> Z-score сильно отрицательный)
    rec_t, _ = model.process_candle(_candle("T", 250.0, high=252.0, low=248.0))

    # Сработала синергия (Double confirmation: Trend SBER + Spread T) -> СУПЕР-BUY с confidence = 1.0!
    assert rec_t.signal == Signal.BUY
    assert rec_t.confidence_value == 1.0


def test_hybrid_strategy_adapter_protocol() -> None:
    adapter = HybridTrioStrategyAdapter()
    assert isinstance(adapter, StrategyPort)

    rec = adapter.evaluate(_candle("GAZP", 115.0))
    assert rec.ticker == "GAZP"
    assert rec.signal == Signal.HOLD

