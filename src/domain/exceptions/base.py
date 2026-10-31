"""Доменные исключения сервиса.

Чистый Python (stdlib), базовое исключение DomainError.
"""


class DomainError(Exception):
    """Базовое доменное исключение."""


class InvalidPriceError(DomainError):
    """Цена не может быть отрицательной или нулевой."""


class InvalidCandleDataError(DomainError):
    """Данные биржевой свечи нарушают бизнес-инварианты (high < low, volume < 0)."""


class InvalidWindowSizeError(DomainError):
    """Размер скользящего окна канала должен быть >= 2."""


class InvalidConfidenceError(DomainError):
    """Сила сигнала (confidence) должна находиться в диапазоне [0.0, 1.0]."""


class InvalidPairConfigError(DomainError):
    """Некорректная конфигурация парной стратегии (одинаковые тикеры или окно < 2)."""

