from enum import Enum

from pydantic import BaseModel


class OverallStatus(str, Enum):
    """Общий статус сервиса."""

    UP = "UP"
    DEGRADED = "DEGRADED"
    DOWN = "DOWN"


class ComponentStatus(str, Enum):
    """Статус отдельного компонента сервиса."""

    UP = "UP"
    DOWN = "DOWN"
    UNKNOWN = "UNKNOWN"


class ComponentDetail(BaseModel):
    """Детальная информация о состоянии компонента."""

    status: ComponentStatus
    details: str | None = None


class Signal(str, Enum):
    """Торговый сигнал стратегии Дончяна."""

    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"


class CandleInterval(str, Enum):
    """Интервал свечи."""

    DAILY = "1d"