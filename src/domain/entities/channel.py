from collections import deque
from datetime import datetime, timezone

from src.domain.entities.candle import Candle
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.events.recommendation_events import RecommendationCreatedEvent
from src.domain.exceptions.base import InvalidWindowSizeError
from src.domain.value_objects.bands import DonchianBands
from src.domain.value_objects.confidence import Confidence
from src.domain.value_objects.signal import Signal


class DonchianChannel:
    """
    Доменный агрегат / модель канала Дончяна.

    Отвечает за:
    - Потокобезопасное хранение скользящего окна экстремумов (High/Low) за N свечей.
    - Определение торгового сигнала (BUY/SELL/HOLD) относительно сформированного коридора цен.
    - Расчёт силы пробоя (confidence).
    """

    DEFAULT_WINDOW: int = 20

    def __init__(self, window: int = DEFAULT_WINDOW) -> None:
        if window < 2:
            raise InvalidWindowSizeError(
                f"Размер окна должен быть >= 2, получено: {window}"
            )
        self._window: int = window
        self._highs: dict[str, deque[float]] = {}
        self._lows: dict[str, deque[float]] = {}

    @property
    def window(self) -> int:
        return self._window

    def _get_or_create_windows(self, ticker: str) -> tuple[deque[float], deque[float]]:
        if ticker not in self._highs:
            self._highs[ticker] = deque(maxlen=self._window)
            self._lows[ticker] = deque(maxlen=self._window)
        return self._highs[ticker], self._lows[ticker]

    def _calculate_confidence(
        self,
        close: float,
        bands: DonchianBands,
        signal: Signal,
    ) -> float:
        """Расчёт нормализованной силы пробоя [0.0, 1.0]."""
        if bands.width <= 0 or signal is Signal.HOLD:
            return 0.0
        if signal is Signal.BUY:
            return min((close - bands.upper) / bands.width, 1.0)
        # Signal.SELL
        return min((bands.lower - close) / bands.width, 1.0)

    def process_candle(
        self,
        candle: Candle,
    ) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        """
        Обработать свечу:
        1. Проверяем пробой цены закрытия относительно УЖЕ сформированного окна (N свечей).
        2. Если накоплено меньше N свечей, сигнал — HOLD.
        3. Обновляем скользящие окна текущими экстремумами.
        """
        highs, lows = self._get_or_create_windows(candle.ticker)

        close_val = candle.close.value

        # Защита от "стены выходных" и пустых свечей симулятора (volume <= 0 или high == low)
        if candle.volume <= 0.0 or candle.high.value == candle.low.value:
            upper = max(highs) if highs else candle.high.value
            lower = min(lows) if lows else candle.low.value
            bands = DonchianBands(upper=max(upper, lower), lower=min(upper, lower))
            now = datetime.now(tz=timezone.utc)
            recommendation = TradeRecommendation(
                ticker=candle.ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                bands=bands,
                confidence=Confidence(0.0),
            )
            event = RecommendationCreatedEvent(
                ticker=candle.ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=close_val,
                upper_band=bands.upper,
                lower_band=bands.lower,
                confidence=0.0,
            )
            return recommendation, event

        if len(highs) < self._window:
            # Окно еще не заполнено — сигнал HOLD
            upper = max(highs) if highs else candle.high.value
            lower = min(lows) if lows else candle.low.value
            bands = DonchianBands(upper=upper, lower=lower)
            signal = Signal.HOLD
            confidence_val = 0.0
        else:
            # Канал сформирован по предыдущим N свечам
            upper = max(highs)
            lower = min(lows)
            bands = DonchianBands(upper=upper, lower=lower)

            if close_val > bands.upper:
                signal = Signal.BUY
            elif close_val < bands.lower:
                signal = Signal.SELL
            else:
                signal = Signal.HOLD

            confidence_val = self._calculate_confidence(close_val, bands, signal)

        # Обновляем окно текущей свечой для последующих расчетов
        highs.append(candle.high.value)
        lows.append(candle.low.value)

        confidence = Confidence(confidence_val)
        now = datetime.now(tz=timezone.utc)

        recommendation = TradeRecommendation(
            ticker=candle.ticker,
            timestamp=now,
            signal=signal,
            close=candle.close,
            bands=bands,
            confidence=confidence,
        )

        event = RecommendationCreatedEvent(
            ticker=candle.ticker,
            timestamp=now,
            signal=signal,
            close=close_val,
            upper_band=bands.upper,
            lower_band=bands.lower,
            confidence=confidence_val,
        )

        return recommendation, event
