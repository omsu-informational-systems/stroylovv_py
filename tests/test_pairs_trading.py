from datetime import datetime, timezone
import pytest

from src.domain.entities.candle import Candle
from src.domain.entities.pair import PairsTradingModel
from src.domain.exceptions.base import InvalidPairConfigError
from src.domain.ports.strategy import StrategyPort
from src.domain.value_objects.price import Price
from src.domain.value_objects.signal import Signal
from src.infrastructure.strategy.pairs_trading import PairsTradingStrategyAdapter


def test_pairs_trading_model_invariants() -> None:
    # Empty primary ticker
    with pytest.raises(InvalidPairConfigError):
        PairsTradingModel(primary_ticker="", benchmark_ticker="SBER", window=5)

    # Identical tickers
    with pytest.raises(InvalidPairConfigError):
        PairsTradingModel(primary_ticker="SBER", benchmark_ticker="sber", window=5)

    # Window < 3
    with pytest.raises(InvalidPairConfigError):
        PairsTradingModel(primary_ticker="T", benchmark_ticker="SBER", window=2)

    # z_entry <= 0
    with pytest.raises(InvalidPairConfigError):
        PairsTradingModel(primary_ticker="T", benchmark_ticker="SBER", window=5, z_entry=-1.0)

    # z_exit >= z_entry
    with pytest.raises(InvalidPairConfigError):
        PairsTradingModel(primary_ticker="T", benchmark_ticker="SBER", window=5, z_entry=1.5, z_exit=1.5)


def _make_candle(
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


def test_pairs_trading_foreign_ticker_hold() -> None:
    model = PairsTradingModel(primary_ticker="T", benchmark_ticker="SBER", window=5)
    foreign_candle = _make_candle("GAZP", 120.0)

    rec, evt = model.process_candle(foreign_candle)
    assert rec.ticker == "GAZP"
    assert rec.signal == Signal.HOLD
    assert rec.confidence_value == 0.0


def test_pairs_trading_weekend_zero_volume_trap() -> None:
    model = PairsTradingModel(primary_ticker="T", benchmark_ticker="SBER", window=5)

    # Zero volume candle (weekend/flatline)
    weekend_candle = _make_candle("T", 270.0, volume=0.0)
    rec, _ = model.process_candle(weekend_candle)
    assert rec.signal == Signal.HOLD

    # Frozen candle (high == low)
    frozen_candle = _make_candle("T", 270.0, volume=100.0, high=270.0, low=270.0)
    rec2, _ = model.process_candle(frozen_candle)
    assert rec2.signal == Signal.HOLD


def test_pairs_trading_divergence_signals() -> None:
    window = 5
    model = PairsTradingModel(
        primary_ticker="T",
        benchmark_ticker="SBER",
        window=window,
        z_entry=1.5,
        z_exit=0.4,
    )

    # 1. Разогреваем модель нормальным спредом
    base_t = [270.0, 270.0, 270.0, 270.0, 270.0]
    base_sber = [290.0, 290.0, 290.0, 290.0, 290.0]

    for p_t, p_sber in zip(base_t, base_sber, strict=False):
        model.process_candle(_make_candle("SBER", p_sber))
        rec, _ = model.process_candle(_make_candle("T", p_t))
        # Во время прогрева сигнал HOLD
        assert rec.signal == Signal.HOLD

    # 2. Моделируем аномальное отставание Т-Банка (T падает до 250, SBER держится на 290)
    # Ratio резко падает -> Z-score сильно отрицательный (< -1.5)
    model.process_candle(_make_candle("SBER", 290.0))
    rec_t, _ = model.process_candle(_make_candle("T", 250.0))

    assert rec_t.signal == Signal.BUY
    assert rec_t.confidence_value > 0.0

    # Проверяем зеркальный сигнал для SBER (он переоценен относительно Т)
    rec_sber, _ = model.process_candle(_make_candle("SBER", 290.0))
    assert rec_sber.signal == Signal.SELL

    # 3. Моделируем возврат спреда к средней (Т-Банк откупают обратно до 270.0)
    # Заполняем историю нормализованным спредом до схлопывания
    for _ in range(5):
        model.process_candle(_make_candle("SBER", 290.0))
        rec_norm, _ = model.process_candle(_make_candle("T", 270.0))

    # Спред нормализовался -> сигнал HOLD (фиксация прибыли)
    assert rec_norm.signal == Signal.HOLD


def test_pairs_trading_strategy_adapter_protocol() -> None:
    adapter = PairsTradingStrategyAdapter(
        primary_ticker="T",
        benchmark_ticker="SBER",
        window=10,
    )
    assert isinstance(adapter, StrategyPort)
    assert adapter.primary_ticker == "T"
    assert adapter.benchmark_ticker == "SBER"

    candle = _make_candle("SBER", 295.0)
    rec = adapter.evaluate(candle)
    assert rec.ticker == "SBER"
    assert rec.signal == Signal.HOLD
