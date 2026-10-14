"""
Модуль торговых стратегий.

Принцип OCP (Open/Closed): новые стратегии реализуют абстрактный
интерфейс TradingStrategy без изменения существующего кода.
"""

from abc import ABC, abstractmethod
from collections import deque
from datetime import datetime, timezone

from src.schemas import CandleMessage, Signal, TradeRecommendation


class TradingStrategy(ABC):
    """Абстрактный базовый класс торговой стратегии (ISP / OCP)."""

    @abstractmethod
    def process_candle(self, candle: CandleMessage) -> TradeRecommendation:
        """Обработать свечу и вернуть торговую рекомендацию."""
        ...


class DonchianChannelStrategy(TradingStrategy):
    """
    Стратегия пробоя канала Дончяна.

    Логика:
    - Close > Upper Band (max high за N свечей) → BUY
    - Close < Lower Band (min low  за N свечей) → SELL
    - Иначе                                      → HOLD

    Принцип SRP: класс отвечает только за расчёт сигнала.
    Состояние изолировано по тикерам через словарь deque.
    """

    def __init__(self, window: int = 20) -> None:
        if window < 2:
            raise ValueError(f"Размер окна должен быть >= 2, получено: {window}")
        self._window = window
        # Хранение скользящих окон High и Low по каждому тикеру (потокобезопасно
        # для single-thread async; для multi-thread нужен asyncio.Lock)
        self._highs: dict[str, deque[float]] = {}
        self._lows: dict[str, deque[float]] = {}

    def _get_or_create_deques(self, ticker: str) -> tuple[deque[float], deque[float]]:
        """Ленивая инициализация скользящих окон для нового тикера."""
        if ticker not in self._highs:
            self._highs[ticker] = deque(maxlen=self._window)
            self._lows[ticker] = deque(maxlen=self._window)
        return self._highs[ticker], self._lows[ticker]

    def _calculate_confidence(
        self,
        close: float,
        upper: float,
        lower: float,
        signal: Signal,
    ) -> float:
        """
        Метрика силы пробоя — насколько цена вышла за границу канала.

        Нормализована к ширине канала, зажата в [0, 1].
        """
        channel_width = upper - lower
        if channel_width == 0:
            return 0.0
        if signal is Signal.BUY:
            return min((close - upper) / channel_width, 1.0)
        if signal is Signal.SELL:
            return min((lower - close) / channel_width, 1.0)
        return 0.0

    def process_candle(self, candle: CandleMessage) -> TradeRecommendation:
        """Обработать входящую свечу и вернуть торговую рекомендацию."""
        highs, lows = self._get_or_create_deques(candle.ticker)

        # Обновляем окно ПРЕДЫДУЩИМИ значениями (пробой оценивается
        # относительно уже сформированного канала)
        highs.append(candle.high)
        lows.append(candle.low)

        upper_band = max(highs)
        lower_band = min(lows)

        if candle.close > upper_band:
            signal = Signal.BUY
        elif candle.close < lower_band:
            signal = Signal.SELL
        else:
            signal = Signal.HOLD

        confidence = self._calculate_confidence(
            close=candle.close,
            upper=upper_band,
            lower=lower_band,
            signal=signal,
        )

        return TradeRecommendation(
            ticker=candle.ticker,
            timestamp=datetime.now(tz=timezone.utc),
            signal=signal,
            close=candle.close,
            upper_band=upper_band,
            lower_band=lower_band,
            confidence=confidence,
        )
