from fastapi import APIRouter, status

from src.core.config import settings
from src.schemas import HealthResponse, OverallStatus

router = APIRouter()


@router.get(
    "",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Проверка работоспособности сервиса",
    description="Возвращает статус сервиса и его компонентов.",
)
async def health_check() -> HealthResponse:
    """Эндпоинт проверки состояния сервиса (liveness probe)."""
    return HealthResponse(
        status=OverallStatus.UP,
        service=settings.app_name,
        version=settings.app_version,
        components={},
    )