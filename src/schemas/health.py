from datetime import datetime

from pydantic import BaseModel, Field, PositiveFloat

from src.schemas.market import CandleInterval, ComponentDetail, OverallStatus, Signal


class HealthResponse(BaseModel):
    """Ответ эндпоинта GET /health."""

    status: OverallStatus
    service: str
    version: str
    components: dict[str, ComponentDetail] = Field(default_factory=dict)


class CandleMessage(BaseModel):
    """Входящее сообщение со свечными данными из RabbitMQ."""

    ticker: str
    timestamp: datetime
    open: PositiveFloat
    high: PositiveFloat
    low: PositiveFloat
    close: PositiveFloat
    volume: PositiveFloat
    interval: CandleInterval


class TradeRecommendation(BaseModel):
    """Торговая рекомендация, публикуемая в RabbitMQ."""

    ticker: str
    timestamp: datetime
    signal: Signal
    close: float
    upper_band: float
    lower_band: float
    confidence: float = Field(ge=0.0, le=1.0, description="Сила пробоя [0, 1]")