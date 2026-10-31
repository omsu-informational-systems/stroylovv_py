from fastapi import APIRouter, Depends, status

from src.adapters.api.dependencies import get_health_use_case, get_kafka_manager
from src.adapters.api.schemas.enums import ComponentStatus, OverallStatus
from src.adapters.api.schemas.health import ComponentDetail, HealthResponse
from src.application.use_cases.health_check import HealthCheckUseCase
from src.infrastructure.kafka.client import KafkaConnectionManager

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Проверка работоспособности сервиса",
    description="Возвращает статус сервиса и его инфраструктурных компонентов.",
)
async def health_check(
    use_case: HealthCheckUseCase = Depends(get_health_use_case),
    kafka_manager: KafkaConnectionManager = Depends(get_kafka_manager),
) -> HealthResponse:
    kafka_status = "UP" if kafka_manager.is_connected else "DOWN"
    dto = use_case.execute(
        components_status={
            "kafka": (kafka_status, "Connection status to secured Kafka broker"),
        }
    )

    return HealthResponse(
        status=OverallStatus(dto.status),
        service=dto.service,
        version=dto.version,
        components={
            k: ComponentDetail(
                status=ComponentStatus(v.status),
                details=v.details,
            )
            for k, v in dto.components.items()
        },
    )

