from dataclasses import dataclass, field
from datetime import datetime
import uuid

from src.domain.value_objects.bands import DonchianBands
from src.domain.value_objects.confidence import Confidence
from src.domain.value_objects.price import Price
from src.domain.value_objects.signal import Signal


@dataclass(kw_only=True)
class TradeRecommendation:
    """
    Доменная сущность: торговая рекомендация.

    Результат оценки рыночной свечи алгоритмом стратегии.
    Хранит как доменные параметры сигнала, так и контекст трассировки
    исходного события свечи (source_event_id, correlation_id, run_id, sequence_number).
    """

    ticker: str
    timestamp: datetime
    signal: Signal
    close: Price
    bands: DonchianBands
    confidence: Confidence

    recommendation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    suggested_quantity: int = 0
    data_quality: str = "OK"
    reason: str = ""
    processing_time_ms: int = 0

    source_event_id: str | None = None
    correlation_id: str | None = None
    run_id: str | None = None
    sequence_number: int | None = None

    def enrich_context(
        self,
        *,
        source_event_id: str | None = None,
        correlation_id: str | None = None,
        run_id: str | None = None,
        sequence_number: int | None = None,
        processing_time_ms: int = 0,
        default_order_quantity: int = 1,
        reason: str = "",
    ) -> None:
        """Привязать контекст исходного события свечи и расчётные параметры рекомендации."""
        self.source_event_id = source_event_id
        self.correlation_id = correlation_id
        self.run_id = run_id
        self.sequence_number = sequence_number
        self.processing_time_ms = max(0, int(processing_time_ms))

        if self.signal is Signal.HOLD:
            self.suggested_quantity = 0
        else:
            self.suggested_quantity = max(1, int(default_order_quantity))

        if reason:
            self.reason = reason
        elif not self.reason:
            self.reason = self._build_default_reason()

    def _build_default_reason(self) -> str:
        if self.signal is Signal.BUY:
            return (
                f"Сигнал BUY по {self.ticker}: close={self.close.to_decimal_str()}, "
                f"confidence={self.confidence_value:.4f}"
            )
        if self.signal is Signal.SELL:
            return (
                f"Сигнал SELL по {self.ticker}: close={self.close.to_decimal_str()}, "
                f"confidence={self.confidence_value:.4f}"
            )
        return f"Удержание позиции (HOLD) по {self.ticker}: close={self.close.to_decimal_str()}"

    @property
    def upper_band(self) -> float:
        return self.bands.upper

    @property
    def lower_band(self) -> float:
        return self.bands.lower

    @property
    def close_value(self) -> float:
        return self.close.value

    @property
    def confidence_value(self) -> float:
        return self.confidence.value
