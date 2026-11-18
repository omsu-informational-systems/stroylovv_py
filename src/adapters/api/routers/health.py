from fastapi import APIRouter, Depends, status

from src.adapters.api.dependencies import (
    get_health_use_case,
    get_kafka_manager,
    get_service_registration,
)
from src.adapters.api.schemas.enums import ComponentStatus, OverallStatus
from src.adapters.api.schemas.health import ComponentDetail, HealthResponse
from src.application.use_cases.health_check import HealthCheckUseCase
from src.domain.entities.registration import ServiceRegistration
from src.infrastructure.kafka.client import KafkaConnectionManager

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Проверка работоспособности сервиса",
    description="Возвращает статус сервиса и его инфраструктурных компонентов без авторизации.",
)
async def health_check(
    use_case: HealthCheckUseCase = Depends(get_health_use_case),
    kafka_manager: KafkaConnectionManager = Depends(get_kafka_manager),
    registration: ServiceRegistration = Depends(get_service_registration),
) -> HealthResponse:
    kafka_status = "UP" if kafka_manager.is_connected else "DOWN"
    kafka_details = (
        "Connected to secured Kafka broker"
        if kafka_manager.is_connected
        else "Kafka connection unavailable"
    )

    reg_status = registration.status_label
    if registration.is_active:
        reg_details = "Registered on platform"
    elif registration.is_rejected:
        reg_details = "Registration rejected by platform"
    else:
        reg_details = "Awaiting registration confirmation"

    dto = use_case.execute(
        components_status={
            "kafka": (kafka_status, kafka_details),
            "registration": (reg_status, reg_details),
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
