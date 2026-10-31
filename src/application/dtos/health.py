from dataclasses import dataclass, field


@dataclass(frozen=True)
class ComponentDetailDTO:
    """DTO детального состояния компонента."""

    status: str
    details: str | None = None


@dataclass(frozen=True)
class HealthDTO:
    """DTO общего состояния сервиса."""

    status: str
    service: str
    version: str
    components: dict[str, ComponentDetailDTO] = field(default_factory=dict)

