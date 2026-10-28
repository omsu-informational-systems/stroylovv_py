"""
Доменные исключения сервиса.

Правило DDD-скилла: все исключения наследуются от DomainError,
а не от встроенных ValueError/Exception.
"""


class DomainError(Exception):
    """Базовое доменное исключение."""


class InvalidWindowSizeError(DomainError):
    """Размер скользящего окна не соответствует допустимому диапазону."""


class InvalidPriceError(DomainError):
    """Цена не может быть отрицательной или нулевой."""
