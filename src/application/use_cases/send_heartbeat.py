import logging

from src.application.commands.send_heartbeat import SendHeartbeatCommand
from src.domain.entities.registration import ServiceRegistration
from src.domain.ports.publisher import HeartbeatPublisherPort

logger = logging.getLogger(__name__)


class SendHeartbeatUseCase:
    """
    Сценарий использования: публикация сигнала состояния сервиса (account.service.heartbeat).

    Отправляется только после подтверждения регистрации платформой.
    """

    def __init__(
        self,
        publisher: HeartbeatPublisherPort,
        registration: ServiceRegistration,
    ) -> None:
        self._publisher = publisher
        self._registration = registration

    async def execute(self, command: SendHeartbeatCommand | None = None) -> bool:
        if not self._registration.can_send_heartbeat:
            return False
        try:
            await self._publisher.publish_heartbeat(self._registration)
            return True
        except Exception as exc:
            logger.error("Не удалось отправить heartbeat сервиса: %s", exc)
            raise
