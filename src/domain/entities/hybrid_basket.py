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


class HybridTrioModel:
    """
    Доменный агрегат гибридной стратегии для трио активов:
    - primary_ticker (по умолчанию T): актив парной связки.
    - benchmark_ticker (по умолчанию SBER): бенчмарк финсектора и индикатор тренда.
    - momentum_ticker (по умолчанию OZON): ростовой лидер, торгуемый по трендовому пробою с фильтром объёма.

    Все параметры и тикеры полностью настраиваются через параметры конструктора (передаются из env).
    """

    def __init__(
        self,
        primary_ticker: str = "T",
        benchmark_ticker: str = "SBER",
        momentum_ticker: str = "OZON",
        donchian_window: int = 20,
        pairs_window: int = 30,
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

    @property
    def primary_ticker(self) -> str:
        return self._primary

    @property
    def benchmark_ticker(self) -> str:
        return self._benchmark

    @property
    def momentum_ticker(self) -> str:
        return self._momentum

    @property
    def supported_tickers(self) -> set[str]:
        return set(self._supported_tickers)

    def _ensure_history(self, ticker: str) -> None:
        if ticker not in self._highs:
            self._highs[ticker] = deque(maxlen=self._donchian_window)
            self._lows[ticker] = deque(maxlen=self._donchian_window)
            self._volumes[ticker] = deque(maxlen=self._donchian_window)

    def process_candle(
        self,
        candle: Candle,
    ) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        ticker = candle.ticker.upper()
        now = datetime.now(tz=timezone.utc)

        # 1. Нецелевые тикеры (не входящие в настроенную корзину) -> безопасный HOLD
        if ticker not in self._supported_tickers:
            return self._build_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        # 2. Защита от свечей выходного дня и замороженного рынка (Weekend & Flatline Trap)
        if candle.volume <= 0.0 or candle.high.value == candle.low.value:
            return self._build_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        self._ensure_history(ticker)

        # 3. Маршрутизация на алгоритмы
        if ticker == self._momentum:
            return self._process_momentum(candle, now)

        # Для пары primary / benchmark -> Синергетическая связка
        return self._process_pairs(candle, now)

    def _process_momentum(
        self,
        candle: Candle,
        now: datetime,
    ) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        ticker = self._momentum
        highs = self._highs[ticker]
        lows = self._lows[ticker]
        vols = self._volumes[ticker]

        close_val = candle.close.value

        if len(highs) < self._donchian_window:
            highs.append(candle.high.value)
            lows.append(candle.low.value)
            vols.append(candle.volume)
            return self._build_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        upper = max(highs)
        lower = min(lows)
        bands = DonchianBands(upper=upper, lower=lower)
        avg_vol = sum(vols) / len(vols)

        signal = Signal.HOLD
        confidence = 0.0

        # Пробой с институциональным объёмом
        if close_val > upper and candle.volume >= (avg_vol * self._vol_mult):
            signal = Signal.BUY
            confidence = min(1.0, (close_val - upper) / (bands.width or 1.0) * 2.0)
        elif close_val < (upper + lower) / 2.0:
            # Выход по средней линии канала (Trailing Exit)
            signal = Signal.HOLD
            confidence = 0.0

        # Обновляем историю
        highs.append(candle.high.value)
        lows.append(candle.low.value)
        vols.append(candle.volume)

        return self._build_recommendation(
            ticker=ticker,
            timestamp=now,
            signal=signal,
            close=candle.close,
            upper=bands.upper,
            lower=bands.lower,
            confidence=confidence,
        )

    def _process_pairs(
        self,
        candle: Candle,
        now: datetime,
    ) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        ticker = candle.ticker.upper()
        self._last_prices[ticker] = candle.close.value

        # Обновляем отношение цен Primary / Benchmark
        if self._primary in self._last_prices and self._benchmark in self._last_prices:
            bench_p = self._last_prices[self._benchmark]
            if bench_p > 0 and ticker == self._primary:
                ratio = self._last_prices[self._primary] / bench_p
                self._ratios.append(ratio)

        highs = self._highs[ticker]
        lows = self._lows[ticker]
        vols = self._volumes[ticker]

        # Недостаточно данных для окна парного трейдинга
        if len(self._ratios) < self._pairs_window or len(highs) < self._donchian_window:
            highs.append(candle.high.value)
            lows.append(candle.low.value)
            vols.append(candle.volume)
            return self._build_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        # Расчет Z-score спреда
        n = len(self._ratios)
        mean_r = sum(self._ratios) / n
        var_r = sum((r - mean_r) ** 2 for r in self._ratios) / n
        std_r = math.sqrt(var_r)

        if std_r < self._min_std_ratio:
            signal = Signal.HOLD
            confidence = 0.0
            z_score = 0.0
        else:
            current_r = self._last_prices.get(self._primary, 1.0) / (
                self._last_prices.get(self._benchmark, 1.0) or 1.0
            )
            z_score = (current_r - mean_r) / std_r

        # Обновление статуса тренда Benchmark до добавления текущей свечи в окно
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

        if ticker == self._primary:
            if z_score <= -self._z_entry:
                # Primary аномально отстал
                signal = Signal.BUY
                if bench_trending_up:
                    # Двойное подтверждение: Benchmark пробивает вверх + Primary отстает
                    confidence = 1.0
                else:
                    confidence = min(1.0, abs(z_score) / 3.0)
                self._primary_position = Signal.BUY
            elif z_score >= self._z_entry:
                signal = Signal.SELL
                confidence = min(1.0, abs(z_score) / 3.0)
                self._primary_position = Signal.SELL
            elif abs(z_score) <= self._z_exit:
                signal = Signal.HOLD
                confidence = 0.0
                self._primary_position = Signal.HOLD
            else:
                signal = self._primary_position
                confidence = min(1.0, abs(z_score) / 3.0)

        elif ticker == self._benchmark:
            if z_score >= self._z_entry:
                signal = Signal.BUY
                confidence = min(1.0, abs(z_score) / 3.0)
            elif z_score <= -self._z_entry:
                # Если Benchmark сам в аптренде, не шортим его против тренда
                signal = Signal.HOLD if bench_trending_up else Signal.SELL
                confidence = min(1.0, abs(z_score) / 3.0)
            elif abs(z_score) <= self._z_exit:
                signal = Signal.HOLD
                confidence = 0.0

        upper_band = candle.close.value * (1 + (std_r / (mean_r or 1.0)) * self._z_entry)
        lower_band = max(0.01, candle.close.value * (1 - (std_r / (mean_r or 1.0)) * self._z_entry))

        highs.append(candle.high.value)
        lows.append(candle.low.value)
        vols.append(candle.volume)

        return self._build_recommendation(
            ticker=ticker,
            timestamp=now,
            signal=signal,
            close=candle.close,
            upper=upper_band,
            lower=lower_band,
            confidence=confidence,
        )

    def _build_recommendation(
        self,
        ticker: str,
        timestamp: datetime,
        signal: Signal,
        close: Price,
        upper: float,
        lower: float,
        confidence: float,
    ) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        bands = DonchianBands(upper=max(upper, lower), lower=min(upper, lower))
        conf_vo = Confidence(round(confidence, 4))
        rec = TradeRecommendation(
            ticker=ticker,
            timestamp=timestamp,
            signal=signal,
            close=close,
            bands=bands,
            confidence=conf_vo,
        )
        evt = RecommendationCreatedEvent(
            ticker=ticker,
            timestamp=timestamp,
            signal=signal,
            close=close.value,
            upper_band=bands.upper,
            lower_band=bands.lower,
            confidence=conf_vo.value,
        )
        return rec, evt
