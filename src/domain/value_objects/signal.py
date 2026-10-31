from enum import Enum


class Signal(str, Enum):
    """Торговый сигнал стратегии пробоя диапазона."""

    BUY = "BUY"
    SELL = "SELL"
    HOLD = "HOLD"

