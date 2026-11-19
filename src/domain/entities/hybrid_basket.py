import logging
from collections import deque
from datetime import datetime, timezone
import math

from src.domain.entities.candle import Candle
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.events.recommendation_events import RecommendationCreatedEvent
from src.domain.exceptions.base import InvalidPairConfigError
from src.domain.value_objects.bands import DonchianBands
from src.domain.value_objects.confidence import Confidence
from src.domain.value_objects.price import Price
from src.domain.value_objects.signal import Signal

logger = logging.getLogger(__name__)

class HybridTrioModel:
    def __init__(
        self,
        primary_ticker: str,
        benchmark_ticker: str,
        momentum_ticker: str,
        donchian_window: int,
        pairs_window: int,
        z_entry: float = 1.8,
        z_exit: float = 0.5,
        volume_multiplier: float = 1.25,
        min_std_ratio: float = 0.0005,
    ) -> None:
        p_tick = primary_ticker.strip().upper()
        b_tick = benchmark_ticker.strip().upper()
        m_tick = momentum_ticker.strip().upper()

        if not p_tick or not b_tick or not m_tick:
            raise InvalidPairConfigError("Тикеры активов не могут быть пустыми")

        if len({p_tick, b_tick, m_tick}) != 3:
            raise InvalidPairConfigError("Все три тикера гибридной корзины должны быть уникальными")

        if donchian_window < 2:
            raise InvalidPairConfigError("donchian_window должно быть >= 2")
        if pairs_window < 3:
            raise InvalidPairConfigError("pairs_window должно быть >= 3")
        if z_entry <= 0:
            raise InvalidPairConfigError("z_entry должно быть > 0")
        if z_exit < 0 or z_exit >= z_entry:
            raise InvalidPairConfigError("z_exit должно быть в диапазоне [0, z_entry)")

        self._primary: str = p_tick
        self._benchmark: str = b_tick
        self._momentum: str = m_tick
        self._supported_tickers: set[str] = {p_tick, b_tick, m_tick}

        self._donchian_window = donchian_window
        self._pairs_window = pairs_window
        self._z_entry = z_entry
        self._z_exit = z_exit
        self._vol_mult = volume_multiplier
        self._min_std_ratio = min_std_ratio

        # Состояние истории свечей по тикерам
        self._highs: dict[str, deque[float]] = {}
        self._lows: dict[str, deque[float]] = {}
        self._volumes: dict[str, deque[float]] = {}

        # Состояние для связки Primary + Benchmark
        self._last_prices: dict[str, float] = {}
        self._ratios: deque[float] = deque(maxlen=pairs_window)
        self._primary_position: Signal = Signal.HOLD
        self._benchmark_trending_up: bool = False

    def process_candle(self, candle: Candle) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        ticker = candle.ticker.upper()
        now = datetime.now(tz=timezone.utc)

        if ticker not in self._supported_tickers:
            return self._build_recommendation(candle, now, Signal.HOLD, 0.0)

        # Защита от флэта и нулевых объёмов
        if candle.volume <= 0.0 or candle.high.value == candle.low.value:
            logger.debug("Пропуск свечи (flat/zero volume): ticker=%s, vol=%s", ticker, candle.volume)
            return self._build_recommendation(candle, now, Signal.HOLD, 0.0)

        self._ensure_history(ticker)

        if ticker == self._momentum:
            return self._process_momentum(candle, now)

        return self._process_pairs(candle, now)

    def _ensure_history(self, ticker: str) -> None:
        if ticker not in self._highs:
            self._highs[ticker] = deque(maxlen=self._donchian_window)
            self._lows[ticker] = deque(maxlen=self._donchian_window)
            self._volumes[ticker] = deque(maxlen=self._donchian_window)

    def _process_momentum(self, candle: Candle, now: datetime) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        ticker = self._momentum
        highs = self._highs[ticker]
        lows = self._lows[ticker]
        vols = self._volumes[ticker]
        close_val = candle.close.value

        if len(highs) < self._donchian_window:
            highs.append(candle.high.value)
            lows.append(candle.low.value)
            vols.append(candle.volume)
            return self._build_recommendation(candle, now, Signal.HOLD, 0.0, reason=f"Накопление окна (моментум): {len(highs)}/{self._donchian_window}")

        upper = max(highs)
        lower = min(lows)
        bands = DonchianBands(upper=upper, lower=lower)
        avg_vol = sum(vols) / len(vols)

        signal = Signal.HOLD
        confidence = 0.0
        reason = "Удержание (нет пробоя или объёма)"

        if close_val > upper and candle.volume >= (avg_vol * self._vol_mult):
            signal = Signal.BUY
            confidence = min(1.0, (close_val - upper) / (bands.width or 1.0) * 2.0)
            reason = f"Пробой канала Дончиана с объёмом {candle.volume:.1f} > {avg_vol * self._vol_mult:.1f}"
        elif close_val < (upper + lower) / 2.0:
            signal = Signal.HOLD
            confidence = 0.0

        highs.append(candle.high.value)
        lows.append(candle.low.value)
        vols.append(candle.volume)

        return self._build_recommendation(candle, now, signal, confidence, upper, lower, reason)

    def _process_pairs(self, candle: Candle, now: datetime) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        ticker = candle.ticker.upper()
        self._last_prices[ticker] = candle.close.value

        if self._primary in self._last_prices and self._benchmark in self._last_prices:
            current_ratio = self._last_prices[self._primary] / self._last_prices[self._benchmark]
            self._ratios.append(current_ratio)  

        highs = self._highs[ticker]
        lows = self._lows[ticker]
        vols = self._volumes[ticker]

        if len(self._ratios) < self._pairs_window or len(highs) < self._donchian_window:
            highs.append(candle.high.value)
            lows.append(candle.low.value)
            vols.append(candle.volume)
            return self._build_recommendation(
                candle, now, Signal.HOLD, 0.0, 
                reason=f"Накопление окна (пары): ratios={len(self._ratios)}/{self._pairs_window}, highs={len(highs)}/{self._donchian_window}"
            )

        n = len(self._ratios)
        mean_r = sum(self._ratios) / n
        var_r = sum((r - mean_r) ** 2 for r in self._ratios) / n
        std_r = math.sqrt(var_r)

        if std_r < self._min_std_ratio:
            highs.append(candle.high.value)
            lows.append(candle.low.value)
            vols.append(candle.volume)
            return self._build_recommendation(
                candle, now, Signal.HOLD, 0.0, 
                reason=f"Низкая волатильность спреда: std={std_r:.6f} < min={self._min_std_ratio}"
            )

        current_r = self._last_prices.get(self._primary, 1.0) / (self._last_prices.get(self._benchmark, 1.0) or 1.0)
        z_score = (current_r - mean_r) / std_r

        if ticker == self._benchmark and len(highs) >= self._donchian_window:
            bench_upper = max(highs)
            bench_lower = min(lows)
            if candle.close.value > bench_upper:
                self._benchmark_trending_up = True
            elif candle.close.value < (bench_upper + bench_lower) / 2.0:
                self._benchmark_trending_up = False

        bench_trending_up = self._benchmark_trending_up
        signal = Signal.HOLD
        confidence = 0.0
        reason = "Удержание (Z-score в нейтральной зоне)"

        if ticker == self._primary:
            if z_score <= -self._z_entry:
                signal = Signal.BUY
                confidence = 1.0 if bench_trending_up else min(1.0, abs(z_score) / 3.0)
                reason = f"Primary отстал (Z={z_score:.2f}), Benchmark тренд={'UP' if bench_trending_up else 'DOWN'}"
                self._primary_position = Signal.BUY
            elif z_score >= self._z_entry:
                signal = Signal.SELL
                confidence = min(1.0, abs(z_score) / 3.0)
                reason = f"Primary перегрет (Z={z_score:.2f})"
                self._primary_position = Signal.SELL
            elif abs(z_score) <= self._z_exit:
                self._primary_position = Signal.HOLD
            else:
                signal = self._primary_position
                confidence = min(1.0, abs(z_score) / 3.0)
                reason = f"Следование позиции (Z={z_score:.2f})"

        elif ticker == self._benchmark:
            if z_score >= self._z_entry:
                signal = Signal.BUY
                confidence = min(1.0, abs(z_score) / 3.0)
                reason = f"Benchmark пробой вверх (Z={z_score:.2f})"
            elif z_score <= -self._z_entry:
                signal = Signal.HOLD if bench_trending_up else Signal.SELL
                confidence = min(1.0, abs(z_score) / 3.0)
                reason = f"Benchmark пробой вниз (Z={z_score:.2f}), тренд={'UP(игнор)' if bench_trending_up else 'DOWN'}"

        upper_band = candle.close.value * (1 + (std_r / (mean_r or 1.0)) * self._z_entry)
        lower_band = max(0.01, candle.close.value * (1 - (std_r / (mean_r or 1.0)) * self._z_entry))

        highs.append(candle.high.value)
        lows.append(candle.low.value)
        vols.append(candle.volume)

        return self._build_recommendation(candle, now, signal, confidence, upper_band, lower_band, reason)

    def _build_recommendation(
        self,
        candle: Candle,
        timestamp: datetime,
        signal: Signal,
        confidence: float,
        upper: float | None = None,
        lower: float | None = None,
        reason: str = "",
    ) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        """Создаёт сущность и ОБОГАЩАЕТ её контекстом (исправление бага с qty=0)."""
        bands = DonchianBands(
            upper=max(upper or candle.high.value, lower or candle.low.value),
            lower=min(upper or candle.high.value, lower or candle.low.value)
        )
        conf_vo = Confidence(round(confidence, 4))
        
        rec = TradeRecommendation(
            ticker=candle.ticker.upper(),
            timestamp=timestamp,
            signal=signal,
            close=candle.close,
            bands=bands,
            confidence=conf_vo,
            source_event_id=getattr(candle, 'event_id', None),
            correlation_id=getattr(candle, 'correlation_id', None),
            run_id=getattr(candle, 'run_id', None),
            sequence_number=getattr(candle, 'sequence_number', None),
        )
        
        # КРИТИЧЕСКИ ВАЖНО: Вызываем enrich_context, чтобы проставить suggested_quantity и reason
        rec.enrich_context(
            default_order_quantity=1,
            reason=reason or rec.reason,
        )

        evt = RecommendationCreatedEvent(
            ticker=rec.ticker,
            timestamp=rec.timestamp,
            signal=rec.signal,
            close=rec.close.value,
            upper_band=rec.upper_band,
            lower_band=rec.lower_band,
            confidence=rec.confidence_value,
        )
        return rec, evt