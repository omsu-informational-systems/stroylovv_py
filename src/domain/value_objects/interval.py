from enum import Enum


class CandleInterval(str, Enum):
    """Таймфрейм / интервал свечи."""

    DAILY = "1d"
    ONE_HOUR = "1h"
    FOUR_HOURS = "4h"

