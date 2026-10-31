import logging

from src.application.commands.send_heartbeat import SendHeartbeatCommand
from src.domain.ports.publisher import HeartbeatPublisherPort

logger = logging.getLogger(__name__)


class SendHeartbeatUseCase:
    """Сценарий использования: публикация heartbeat сервиса."""

    def __init__(self, publisher: HeartbeatPublisherPort) -> None:
        self._publisher = publisher

    async def execute(self, command: SendHeartbeatCommand) -> None:
        try:
            await self._publisher.publish_heartbeat(command.service_name)
        except Exception as exc:
            logger.error("Не удалось отправить heartbeat сервиса: %s", exc)
            raise

