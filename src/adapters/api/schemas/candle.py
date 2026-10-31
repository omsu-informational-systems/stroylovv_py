from datetime import datetime

from pydantic import BaseModel, Field, PositiveFloat


class CandleRequestSchema(BaseModel):
    """Схема входящей свечи через HTTP эндпоинт."""

    ticker: str
    timestamp: datetime
    open: PositiveFloat
    high: PositiveFloat
    low: PositiveFloat
    close: PositiveFloat
    volume: PositiveFloat
    interval: str = "1d"


class RecommendationResponseSchema(BaseModel):
    """Схема ответа с торговой рекомендацией."""

    ticker: str
    timestamp: datetime
    signal: str
    close: float
    upper_band: float
    lower_band: float
    confidence: float = Field(ge=0.0, le=1.0)

