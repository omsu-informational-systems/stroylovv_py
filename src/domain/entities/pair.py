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


class PairsTradingModel:
    """
    Доменный агрегат парного арбитража (Statistical Arbitrage / Pairs Trading).

    Ориентирован на сильно скоррелированные пары российского фондового рынка:
    SBER (Сбербанк, бенчмарк) и T (Т-Технологии / ТКС Банк, первичный торгуемый актив).

    Бизнес-правила:
    - Расчёт динамического отношения цен (Ratio = Price(Primary) / Price(Benchmark)).
    - Расчёт Z-score отклонения спреда от скользящей средней (mu, sigma).
    - Генерация рыночно-нейтральных сигналов:
      - Z <= -z_entry: Primary аномально отстал -> BUY Primary, SELL Benchmark
      - Z >= +z_entry: Primary аномально перегрет -> SELL Primary, BUY Benchmark
      - |Z| <= z_exit: спред нормализовался -> HOLD (фиксация прибыли)
    - Защита от ловушки выходных дней / закрытого рынка (Weekend / Flatline Trap):
      если volume <= 0 или high == low, рынок заморожен -> сигнал HOLD.
    """

    def __init__(
        self,
        primary_ticker: str,
        benchmark_ticker: str,
        window: int,
        z_entry: float = 1.8,
        z_exit: float = 0.5,
        min_std_ratio: float = 0.0005,
    ) -> None:
        if not primary_ticker or not primary_ticker.strip():
            raise InvalidPairConfigError("Тикер primary_ticker не может быть пустым")
        if not benchmark_ticker or not benchmark_ticker.strip():
            raise InvalidPairConfigError("Тикер benchmark_ticker не может быть пустым")
        if primary_ticker.upper() == benchmark_ticker.upper():
            raise InvalidPairConfigError("Тикеры пары должны быть различными")
        if window < 3:
            raise InvalidPairConfigError(
                f"Размер окна парного трейдинга должен быть >= 3, получено: {window}"
            )
        if z_entry <= 0:
            raise InvalidPairConfigError(
                f"Порог z_entry должен быть положительным, получено: {z_entry}"
            )
        if z_exit < 0 or z_exit >= z_entry:
            raise InvalidPairConfigError(
                f"Порог z_exit ({z_exit}) должен быть в диапазоне [0, z_entry)"
            )

        self._primary: str = primary_ticker.strip().upper()
        self._benchmark: str = benchmark_ticker.strip().upper()
        self._window: int = window
        self._z_entry: float = z_entry
        self._z_exit: float = z_exit
        self._min_std_ratio: float = min_std_ratio

        self._last_prices: dict[str, float] = {}
        self._ratios: deque[float] = deque(maxlen=window)
        self._current_position: Signal = Signal.HOLD

    @property
    def primary_ticker(self) -> str:
        return self._primary

    @property
    def benchmark_ticker(self) -> str:
        return self._benchmark

    @property
    def window(self) -> int:
        return self._window

    @property
    def current_ratio(self) -> float | None:
        if self._primary in self._last_prices and self._benchmark in self._last_prices:
            bench_price = self._last_prices[self._benchmark]
            if bench_price > 0:
                return self._last_prices[self._primary] / bench_price
        return None

    def process_candle(
        self,
        candle: Candle,
    ) -> tuple[TradeRecommendation, RecommendationCreatedEvent]:
        ticker = candle.ticker.upper()
        now = datetime.now(tz=timezone.utc)
        close_val = candle.close.value

        # 1. Если свеча от стороннего инструмента (не входящего в пару), держим нейтралитет
        if ticker not in (self._primary, self._benchmark):
            return self._create_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        # 2. Фильтр "стены выходных / нерабочих часов" (Weekend / Flatline Trap):
        # Если свеча имеет нулевой объем или high == low (цена заморожена на месте),
        # категорически не совершаем сделок
        if candle.volume <= 0 or candle.high.value == candle.low.value:
            return self._create_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        # 3. Обновляем последнюю цену инструмента
        self._last_prices[ticker] = close_val

        # 4. Если нет актуальных цен для обоих инструментов, окно не готово
        if self._primary not in self._last_prices or self._benchmark not in self._last_prices:
            return self._create_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        bench_price = self._last_prices[self._benchmark]
        if bench_price <= 0:
            return self._create_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        current_ratio = self._last_prices[self._primary] / bench_price
        if ticker == self._primary:
            self._ratios.append(current_ratio)

        # 5. Накопление достаточного окна для статистической значимости
        if len(self._ratios) < self._window:
            return self._create_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        # 6. Расчёт средней (mean) и стандартного отклонения (std) спреда
        n = len(self._ratios)
        mean_ratio = sum(self._ratios) / n
        variance = sum((r - mean_ratio) ** 2 for r in self._ratios) / n
        std_ratio = math.sqrt(variance)

        # Если волатильность спреда микроскопическая (рынок замер), избегаем ложных сделок
        if std_ratio < self._min_std_ratio:
            return self._create_recommendation(
                ticker=ticker,
                timestamp=now,
                signal=Signal.HOLD,
                close=candle.close,
                upper=candle.high.value,
                lower=candle.low.value,
                confidence=0.0,
            )

        # 7. Расчёт Z-score
        z_score = (current_ratio - mean_ratio) / std_ratio

        # 8. Логика торговых сигналов
        signal = Signal.HOLD
        confidence_val = 0.0

        if ticker == self._primary:
            # Для Т-Банка (Primary):
            if z_score <= -self._z_entry:
                # Т-Банк аномально отстал от Сбера -> BUY T
                signal = Signal.BUY
                confidence_val = min(1.0, abs(z_score) / 3.0)
                self._current_position = Signal.BUY
            elif z_score >= self._z_entry:
                # Т-Банк аномально перегрет относительно Сбера -> SELL T
                signal = Signal.SELL
                confidence_val = min(1.0, abs(z_score) / 3.0)
                self._current_position = Signal.SELL
            elif abs(z_score) <= self._z_exit:
                # Спред схлопнулся к норме -> выход из позиции
                signal = Signal.HOLD
                confidence_val = 0.0
                self._current_position = Signal.HOLD
            else:
                # В нейтральной зоне удерживаем текущую позицию
                signal = self._current_position
                confidence_val = min(1.0, abs(z_score) / 3.0)

        elif ticker == self._benchmark:
            # Для Сбербанка (Benchmark):
            if z_score >= self._z_entry:
                # Сбер отстал от Т-Банка -> BUY SBER
                signal = Signal.BUY
                confidence_val = min(1.0, abs(z_score) / 3.0)
            elif z_score <= -self._z_entry:
                # Сбер обогнал Т-Банк -> SELL SBER
                signal = Signal.SELL
                confidence_val = min(1.0, abs(z_score) / 3.0)
            elif abs(z_score) <= self._z_exit:
                signal = Signal.HOLD
                confidence_val = 0.0

        # Границы для совместимости с протоколом платформы
        band_spread = close_val * (std_ratio / mean_ratio) * self._z_entry
        upper_band = close_val + band_spread
        lower_band = max(0.01, close_val - band_spread)

        return self._create_recommendation(
            ticker=ticker,
            timestamp=now,
            signal=signal,
            close=candle.close,
            upper=upper_band,
            lower=lower_band,
            confidence=confidence_val,
        )

    def _create_recommendation(
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
