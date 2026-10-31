from pydantic import BaseModel, Field

from src.adapters.api.schemas.enums import ComponentStatus, OverallStatus


class ComponentDetail(BaseModel):
    """Детализация состояния компонента."""

    status: ComponentStatus
    details: str | None = None


class HealthResponse(BaseModel):
    """Схема ответа эндпоинта GET /health."""

    status: OverallStatus
    service: str
    version: str
    components: dict[str, ComponentDetail] = Field(default_factory=dict)

