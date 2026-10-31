from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class RegistrationDTO:
    """DTO запроса на регистрацию микросервиса."""

    service_name: str
    timestamp: datetime


@dataclass(frozen=True)
class RegistrationResultDTO:
    """DTO ответа на запрос регистрации."""

    service_name: str
    success: bool
    message: str = ""

