import logging

from src.application.commands.register_service import RegisterServiceCommand
from src.domain.entities.registration import ServiceRegistration
from src.domain.ports.publisher import RegistrationPublisherPort

logger = logging.getLogger(__name__)


class RegisterServiceUseCase:
    """Сценарий использования: отправка запроса на регистрацию сервиса (account.registration.request)."""

    def __init__(
        self,
        publisher: RegistrationPublisherPort,
        registration: ServiceRegistration,
    ) -> None:
        self._publisher = publisher
        self._registration = registration

    async def execute(self, command: RegisterServiceCommand | None = None) -> str:
        try:
            event_id = await self._publisher.publish_registration(self._registration)
            self._registration.record_registration_requested(event_id)
            logger.info(
                "Запрос на регистрацию сервиса '%s' отправлен (eventId=%s, accountId=%s, groupId=%s, strategyId=%s)",
                self._registration.service_name,
                event_id,
                self._registration.account_id,
                self._registration.group_id,
                self._registration.strategy_id,
            )
            return event_id
        except Exception as exc:
            logger.error("Не удалось отправить запрос регистрации сервиса: %s", exc)
            raise
