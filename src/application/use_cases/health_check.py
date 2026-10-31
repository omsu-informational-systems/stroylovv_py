from src.application.dtos.health import ComponentDetailDTO, HealthDTO


class HealthCheckUseCase:
    """Сценарий использования: проверка общего состояния сервиса и компонентов."""

    def __init__(self, service_name: str, service_version: str) -> None:
        self._service_name = service_name
        self._service_version = service_version

    def execute(
        self,
        components_status: dict[str, tuple[str, str | None]] | None = None,
    ) -> HealthDTO:
        components: dict[str, ComponentDetailDTO] = {}
        overall_status = "UP"

        if components_status:
            for name, (status, details) in components_status.items():
                components[name] = ComponentDetailDTO(status=status, details=details)
                if status in ("DOWN", "DEGRADED"):
                    # Сервис жив и отвечает на запросы по HTTP, но зависит от внешних компонентов
                    overall_status = "DEGRADED"

        return HealthDTO(
            status=overall_status,
            service=self._service_name,
            version=self._service_version,
            components=components,
        )
