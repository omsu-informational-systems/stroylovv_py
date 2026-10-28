"""
API-схемы (Pydantic DTO) для слоя представления.

Правило DDD-скилла: Pydantic-модели живут в api/interfaces,
а НЕ в domain/.

Правило SRP: этот файл содержит только HTTP-контракты для /health.
"""

from pydantic import BaseModel, Field

from src.api.schemas.enums import ComponentStatus, OverallStatus


class ComponentDetail(BaseModel):
    """Состояние одного компонента сервиса."""

    status: ComponentStatus
    details: str | None = None


class HealthResponse(BaseModel):
    """Тело ответа GET /health согласно спецификации платформы."""

    status: OverallStatus
    service: str
    version: str
    components: dict[str, ComponentDetail] = Field(default_factory=dict)
