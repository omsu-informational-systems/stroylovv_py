from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class TradeRecommendationDTO:
    """DTO торговой рекомендации."""

    ticker: str
    timestamp: datetime
    signal: str
    close: float
    upper_band: float
    lower_band: float
    confidence: float

