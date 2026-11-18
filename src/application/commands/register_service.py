from dataclasses import dataclass


@dataclass(frozen=True)
class RegisterServiceCommand:
    """Команда отправки запроса на регистрацию сервиса на платформе."""

    service_name: str
    account_id: str = ""
    account_name: str = ""
    group_id: str = ""
    strategy_id: str = ""
    strategy_name: str = ""
    description: str = ""
