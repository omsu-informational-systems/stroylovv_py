"""
Перечисления для API-схем (слой представления).

Отделены от domain/value_objects, так как здесь Signal/Status
являются HTTP-контрактами, а не доменными типами.
"""

from enum import Enum


class OverallStatus(str, Enum):
    """Общий статус сервиса в ответе /health."""

    UP = "UP"
    DEGRADED = "DEGRADED"


class ComponentStatus(str, Enum):
    """Статус отдельного компонента сервиса."""

    UP = "UP"
    DOWN = "DOWN"
    UNKNOWN = "UNKNOWN"
