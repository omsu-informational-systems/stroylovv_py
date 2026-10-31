from datetime import datetime, timezone
import pytest

from src.domain.entities.candle import Candle
from src.domain.entities.channel import DonchianChannel
from src.domain.exceptions.base import (
    InvalidCandleDataError,
    InvalidConfidenceError,
    InvalidPriceError,
    InvalidWindowSizeError,
)
from src.domain.value_objects.bands import DonchianBands
from src.domain.value_objects.confidence import Confidence
from src.domain.value_objects.interval import CandleInterval
from src.domain.value_objects.price import Price
from src.domain.value_objects.signal import Signal


def test_price_value_object() -> None:
    p = Price(100.5)
    assert p.value == 100.5
    assert float(p) == 100.5

    with pytest.raises(InvalidPriceError):
        Price(0.0)

    with pytest.raises(InvalidPriceError):
        Price(-10.0)


def test_confidence_value_object() -> None:
    c = Confidence(0.75)
    assert float(c) == 0.75

    with pytest.raises(InvalidConfidenceError):
        Confidence(-0.01)

    with pytest.raises(InvalidConfidenceError):
        Confidence(1.01)


def test_donchian_bands_value_object() -> None:
    bands = DonchianBands(upper=110.0, lower=95.0)
    assert bands.width == 15.0

    with pytest.raises(InvalidPriceError):
        DonchianBands(upper=90.0, lower=100.0)


def test_candle_entity_invariants() -> None:
    now = datetime.now(timezone.utc)
    candle = Candle(
        ticker="GAZP",
        timestamp=now,
        open=Price(100.0),
        high=Price(105.0),
        low=Price(95.0),
        close=Price(102.0),
        volume=10000.0,
        interval=CandleInterval.DAILY,
    )
    assert candle.ticker == "GAZP"
    assert candle.high.value == 105.0

    # High < Low
    with pytest.raises(InvalidCandleDataError):
        Candle(
            ticker="GAZP",
            timestamp=now,
            open=Price(100.0),
            high=Price(94.0),
            low=Price(95.0),
            close=Price(96.0),
            volume=1000.0,
        )

    # Empty ticker
    with pytest.raises(InvalidCandleDataError):
        Candle(
            ticker="",
            timestamp=now,
            open=Price(100.0),
            high=Price(105.0),
            low=Price(95.0),
            close=Price(100.0),
            volume=100.0,
        )


def test_donchian_channel_aggregate_breakouts() -> None:
    channel = DonchianChannel(window=3)
    now = datetime.now(timezone.utc)

    # 1. Заполняем окно свечами в диапазоне 100..105
    for _ in range(3):
        candle = Candle(
            ticker="SBER",
            timestamp=now,
            open=Price(101.0),
            high=Price(105.0),
            low=Price(98.0),
            close=Price(102.0),
            volume=5000.0,
        )
        rec, event = channel.process_candle(candle)
        assert rec.signal == Signal.HOLD

    # 2. Пробой вверх: Close=108 > Upper=105 -> BUY
    buy_candle = Candle(
        ticker="SBER",
        timestamp=now,
        open=Price(103.0),
        high=Price(110.0),
        low=Price(102.0),
        close=Price(108.0),
        volume=15000.0,
    )
    rec, event = channel.process_candle(buy_candle)
    assert rec.signal == Signal.BUY
    assert rec.confidence_value > 0.0
    assert event.signal == Signal.BUY

    # 3. Пробой вниз: Close=90 < Lower=98 -> SELL
    sell_candle = Candle(
        ticker="SBER",
        timestamp=now,
        open=Price(100.0),
        high=Price(101.0),
        low=Price(88.0),
        close=Price(90.0),
        volume=20000.0,
    )
    rec_sell, event_sell = channel.process_candle(sell_candle)
    assert rec_sell.signal == Signal.SELL
    assert rec_sell.confidence_value > 0.0


def test_donchian_channel_invalid_window() -> None:
    with pytest.raises(InvalidWindowSizeError):
        DonchianChannel(window=1)

