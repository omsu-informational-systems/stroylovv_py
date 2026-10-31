from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class CandleDTO:
    """DTO биржевой свечи."""

    ticker: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    interval: str = "1d"

