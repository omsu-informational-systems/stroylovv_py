from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class ProcessRecommendationResultCommand:
    """Команда обработки ответа платформы strategy.recommendation.result."""

    account_id: str
    strategy_id: str
    instrument: str
    sequence_number: int | None = None
    recommendation_id: str = ""
    causation_id: str | None = None
    execution_status: str = ""
    reason: str = ""
    timestamp: datetime | None = None
