from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


@dataclass(frozen=True)
class ProcessCandleCommand:
    """Команда на обработку закрытой свечи (market.candle.closed)."""

    ticker: str
    timestamp: datetime
    open: Decimal | float | str
    high: Decimal | float | str
    low: Decimal | float | str
    close: Decimal | float | str
    volume: float
    interval: str = "1d"

    # Контекст события из общей JSON-обертки платформы
    event_id: str | None = None
    correlation_id: str | None = None
    run_id: str | None = None
    sequence_number: int | None = None
