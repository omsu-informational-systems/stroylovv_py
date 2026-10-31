import logging

from src.domain.ports.consumer import EventConsumerPort

logger = logging.getLogger(__name__)


class ConsumerWorker:
    """Фоновый воркер чтения свечей из Kafka."""

    def __init__(self, consumer: EventConsumerPort) -> None:
        self._consumer = consumer

    async def start(self) -> None:
        logger.info("Запуск ConsumerWorker...")
        await self._consumer.start()

    async def stop(self) -> None:
        logger.info("Остановка ConsumerWorker...")
        await self._consumer.stop()

