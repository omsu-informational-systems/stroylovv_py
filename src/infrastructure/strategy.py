"""
Реализация стратегии Дончяна (Infrastructure Layer).

Правило DDD-скилла: Infrastructure реализует порты/протоколы из domain/.
Правило OOP-скилла: никакой логики в конструкторе, инварианты в __post_init__.
Правило ports-adapters-скилла: этот модуль — Adapter, а StrategyPort — Port.
"""

from __future__ import annotations

from collections import deque

from src.domain.exceptions import InvalidWindowSizeError
from src.domain.value_objects import Candle, DonchianBands, Signal, TradeSignal


class DonchianChannelStrategy:
    """
    Адаптер стратегии пробоя канала Дончяна.

    Реализует StrategyPort. Хранит скользящее окно High/Low
    по каждому тикеру в оперативной памяти (deque с maxlen=N).

    SRP: класс отвечает только за расчёт сигнала Дончяна.
    OCP: новые стратегии реализуют StrategyPort без изменения этого класса.
    """

    DEFAULT_WINDOW = 20

    def __init__(self, window: int = DEFAULT_WINDOW) -> None:
        if window < 2:
            raise InvalidWindowSizeError(
                f"Размер окна должен быть >= 2, получено: {window}"
            )
        self._window = window
        self._highs: dict[str, deque[float]] = {}
        self._lows: dict[str, deque[float]] = {}

    def evaluate(self, candle: Candle) -> TradeSignal:
        """
        Оценить входящую свечу и вернуть торговый сигнал.

        Алгоритм:
        1. Обновляем скользящее окно High/Low для данного тикера.
        2. Вычисляем границы канала Дончяна.
        3. Определяем сигнал по пробою границы ценой закрытия.
        4. Вычисляем метрику confidence (нормализованная сила пробоя).
        """
        self._update_window(candle)
        bands = self._calculate_bands(candle.ticker)
        signal = self._classify_signal(candle.close.value, bands)
        confidence = self._calculate_confidence(candle.close.value, bands, signal)

        return TradeSignal(
            ticker=candle.ticker,
            signal=signal,
            close=candle.close.value,
            bands=bands,
            confidence=confidence,
        )

    # ------------------------------------------------------------------ #
    # Приватные методы (единственная ответственность каждого — SRP)
    # ------------------------------------------------------------------ #

    def _update_window(self, candle: Candle) -> None:
        """Добавить High/Low свечи в скользящее окно тикера."""
        ticker = candle.ticker
        if ticker not in self._highs:
            self._highs[ticker] = deque(maxlen=self._window)
            self._lows[ticker] = deque(maxlen=self._window)
        self._highs[ticker].append(candle.high.value)
        self._lows[ticker].append(candle.low.value)

    def _calculate_bands(self, ticker: str) -> DonchianBands:
        """Вычислить текущие границы канала Дончяна для тикера."""
        return DonchianBands(
            upper=max(self._highs[ticker]),
            lower=min(self._lows[ticker]),
        )

    @staticmethod
    def _classify_signal(close: float, bands: DonchianBands) -> Signal:
        """Классифицировать сигнал по положению цены закрытия."""
        if close > bands.upper:
            return Signal.BUY
        if close < bands.lower:
            return Signal.SELL
        return Signal.HOLD

    @staticmethod
    def _calculate_confidence(close: float, bands: DonchianBands, signal: Signal) -> float:
        """
        Метрика силы пробоя, нормализованная к ширине канала.

        Возвращает 0.0 для HOLD и значение в (0.0, 1.0] для BUY/SELL.
        """
        if bands.width == 0 or signal is Signal.HOLD:
            return 0.0
        if signal is Signal.BUY:
            return min((close - bands.upper) / bands.width, 1.0)
        # Signal.SELL
        return min((bands.lower - close) / bands.width, 1.0)
