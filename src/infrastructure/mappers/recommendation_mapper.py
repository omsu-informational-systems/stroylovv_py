from datetime import datetime, timezone
import json
from typing import Any

from src.domain.entities.recommendation import TradeRecommendation


class RecommendationMapper:
    """Маппер доменной торговой рекомендации в JSON-сообщение для Kafka."""

    @staticmethod
    def to_kafka_dict(
        recommendation: TradeRecommendation,
        service_name: str,
        secret_key: str,
    ) -> dict[str, Any]:
        return {
            "eventType": "strategy.recommendation.created",
            "secretKey": secret_key,
            "serviceName": service_name,
            "ticker": recommendation.ticker,
            "timestamp": recommendation.timestamp.astimezone(timezone.utc).isoformat(),
            "signal": recommendation.signal.value,
            "close": recommendation.close_value,
            "upperBand": recommendation.upper_band,
            "lowerBand": recommendation.lower_band,
            "confidence": round(recommendation.confidence_value, 4),
        }

    @staticmethod
    def to_kafka_json(
        recommendation: TradeRecommendation,
        service_name: str,
        secret_key: str,
    ) -> str:
        data = RecommendationMapper.to_kafka_dict(
            recommendation=recommendation,
            service_name=service_name,
            secret_key=secret_key,
        )
        return json.dumps(data, ensure_ascii=False)

