"""
Роутер эндпоинта GET /health.

Правило DDD-скилла: Interface Layer вызывает Application Use Cases
и переводит доменные ошибки в HTTP-статусы.
Для первой работы компоненты пусты — components: {}.
"""

from fastapi import APIRouter, status

from src.api.schemas import HealthResponse, OverallStatus
from src.core.config import settings

router = APIRouter(tags=["Health"])


@router.get(
    "/health",
    response_model=HealthResponse,
    status_code=status.HTTP_200_OK,
    summary="Проверка работоспособности сервиса",
    description=(
        "Возвращает статус сервиса и его компонентов. "
        "Не требует авторизации. "
        "HTTP 200 означает, что процесс запущен и отвечает на запросы."
    ),
)
async def health_check() -> HealthResponse:
    """Liveness probe — сервис жив и отвечает на запросы."""
    return HealthResponse(
        status=OverallStatus.UP,
        service=settings.app_name,
        version=settings.app_version,
        components={},
    )
