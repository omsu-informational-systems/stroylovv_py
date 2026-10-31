from dataclasses import dataclass


@dataclass(frozen=True)
class RegisterServiceCommand:
    """Команда отправки запроса на регистрацию сервиса на платформе."""

    service_name: str

