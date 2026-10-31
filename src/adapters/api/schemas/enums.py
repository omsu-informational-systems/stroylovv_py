from enum import Enum


class OverallStatus(str, Enum):
    """Общий статус микросервиса."""

    UP = "UP"
    DEGRADED = "DEGRADED"
    DOWN = "DOWN"


class ComponentStatus(str, Enum):
    """Статус компонента сервиса."""

    UP = "UP"
    DOWN = "DOWN"
    UNKNOWN = "UNKNOWN"

