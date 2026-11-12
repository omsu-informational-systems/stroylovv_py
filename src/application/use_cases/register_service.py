import logging

from src.application.commands.register_service import RegisterServiceCommand
from src.domain.ports.publisher import RegistrationPublisherPort

logger = logging.getLogger(__name__)


class RegisterServiceUseCase:
    """Сценарий использования: отправка запроса на регистрацию сервиса."""

    def __init__(self, publisher: RegistrationPublisherPort) -> None:
        self._publisher = publisher

    async def execute(self, command: RegisterServiceCommand) -> None:
        try:
            await self._publisher.publish_registration(
                service_name=command.service_name,
                account_name=command.account_name,
                group_id=command.group_id,
                strategy_name=command.strategy_name,
            )
            logger.info(
                "Запрос на регистрацию сервиса '%s' успешно отправлен (студент: %s, группа: %s, стратегия: %s)",
                command.service_name,
                command.account_name,
                command.group_id,
                command.strategy_name,
            )
        except Exception as exc:
            logger.error("Не удалось отправить запрос регистрации сервиса: %s", exc)
            raise
