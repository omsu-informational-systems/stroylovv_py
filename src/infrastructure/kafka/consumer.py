import asyncio
import logging

from aiokafka import AIOKafkaConsumer

from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.domain.ports.consumer import EventConsumerPort
from src.infrastructure.config.settings import Settings
from src.infrastructure.kafka.client import KafkaConnectionManager
from src.infrastructure.mappers.candle_mapper import CandleMapper

logger = logging.getLogger(__name__)


class KafkaConsumerService(EventConsumerPort):
    """
    Адаптер потребления свечных сообщений из защищенной Kafka (Infrastructure Layer).

    - Подписка на CANDLE_TOPIC (по умолчанию market.candle.default).
    - Ручной коммит смещения (enable.auto.commit = false) после успешного расчета.
    - Изоляция ошибок одного сообщения от остановки всего цикла чтения.
    """

    def __init__(
        self,
        connection_manager: KafkaConnectionManager,
        process_candle_use_case: ProcessCandleUseCase,
        settings: Settings,
    ) -> None:
        self._manager = connection_manager
        self._use_case = process_candle_use_case
        self._settings = settings
        self._consumer: AIOKafkaConsumer | None = None
        self._running: bool = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Запуск фонового потребления сообщений."""
        if self._running:
            return

        topics = [self._settings.candle_topic]
        self._consumer = self._manager.create_consumer(topics=topics)
        if self._consumer is None:
            logger.info("Kafka Consumer отключен (нет KAFKA_PASSWORD).")
            return

        try:
            await self._consumer.start()
            self._running = True
            logger.info(
                "Kafka Consumer запущен. Подписка на темы: %s, группа: %s",
                topics,
                self._settings.kafka_group_id,
            )
            self._task = asyncio.create_task(self._consume_loop())
        except Exception as exc:
            logger.error("Ошибка при старте Kafka Consumer: %s", exc)
            self._running = False
            if self._consumer:
                await self._consumer.stop()
                self._consumer = None

    async def stop(self) -> None:
        """Корректная остановка фонового консьюмера."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as exc:
                logger.warning("Ошибка при остановке Kafka Consumer: %s", exc)
            finally:
                self._consumer = None
        logger.info("Kafka Consumer остановлен.")

    async def _consume_loop(self) -> None:
        """Цикл чтения и обработки свечей."""
        assert self._consumer is not None
        while self._running:
            try:
                async for message in self._consumer:
                    if not self._running:
                        break

                    try:
                        command = CandleMapper.parse_kafka_message(message.value)
                        await self._use_case.execute(command)

                        # At-least-once: подтверждаем смещение только после успешной обработки
                        if not self._settings.kafka_enable_auto_commit:
                            await self._consumer.commit()

                    except Exception as err:
                        logger.error(
                            "Ошибка при обработке сообщения из Kafka (offset=%s): %s",
                            message.offset,
                            err,
                        )
            except asyncio.CancelledError:
                break
            except Exception as loop_err:
                logger.error("Сбой в цикле чтения Kafka: %s. Повтор через 5 секунд...", loop_err)
                await asyncio.sleep(5)

