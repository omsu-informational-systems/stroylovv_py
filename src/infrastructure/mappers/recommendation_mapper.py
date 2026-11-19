import json
from typing import Any

from src.domain.entities.recommendation import TradeRecommendation
from src.domain.entities.registration import ServiceRegistration
from src.infrastructure.mappers.event_mapper import build_envelope


class RecommendationMapper:
    EVENT_TYPE = "strategy.recommendation.created"

    @staticmethod
    def to_kafka_dict(
        recommendation: TradeRecommendation,
        registration: ServiceRegistration,
        schema_version: str = "1.0",
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "recommendationId": recommendation.recommendation_id,
            "sourceEventId": recommendation.source_event_id,
            "runId": recommendation.run_id,
            "accountId": registration.account_id,
            "groupId": registration.group_id,
            "strategyId": registration.strategy_id,
            "instrument": recommendation.ticker,
            "sequenceNumber": recommendation.sequence_number,
            "signal": recommendation.signal.value,
            "confidence": round(recommendation.confidence_value, 4),
            "suggestedQuantity": recommendation.suggested_quantity,
            "dataQuality": recommendation.data_quality,
            "reason": recommendation.reason,
            "processingTimeMs": recommendation.processing_time_ms,
            "metadata": {
                "close": recommendation.close.to_decimal_str(),
                "upperBand": f"{recommendation.upper_band:.4f}",
                "lowerBand": f"{recommendation.lower_band:.4f}",
            },
        }

        return build_envelope(
            event_type=RecommendationMapper.EVENT_TYPE,
            producer=registration.service_name,
            payload=payload,          
            schema_version=schema_version,
            correlation_id=recommendation.correlation_id,
            causation_id=recommendation.source_event_id,
            # secret_key=secret_key, <-- УБРАНО, если build_envelope это позволяет
        )

    @staticmethod
    def to_kafka_json(
        recommendation: TradeRecommendation,
        registration: ServiceRegistration,
        secret_key: str,  # Оставляем здесь, чтобы не ломать вызовы извне (например, из адаптера)
        schema_version: str = "1.0",
    ) -> str:
        # Вызываем без secret_key, так как мы убрали его из сигнатуры to_kafka_dict
        data = RecommendationMapper.to_kafka_dict(
            recommendation=recommendation,
            registration=registration,
            schema_version=schema_version,
        )
        return json.dumps(data, ensure_ascii=False)