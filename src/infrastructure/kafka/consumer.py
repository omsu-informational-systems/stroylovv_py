import asyncio
import logging

from aiokafka import AIOKafkaConsumer

from src.application.commands.process_registration_result import (
    ProcessRegistrationResultCommand,
)
from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.application.use_cases.process_registration_result import (
    ProcessRegistrationResultUseCase,
)
from src.domain.ports.consumer import EventConsumerPort
from src.infrastructure.config.settings import Settings
from src.infrastructure.kafka.client import KafkaConnectionManager
from src.infrastructure.mappers.candle_mapper import CandleMapper
from src.infrastructure.mappers.event_mapper import EventMapper

logger = logging.getLogger(__name__)


class KafkaConsumerService(EventConsumerPort):
    """
    Адаптер потребления сообщений из защищенной Kafka (Infrastructure Layer).

    - Подписка на тему свечей (market.candle.closed в CANDLE_TOPIC)
      и тему результата регистрации (account.registration.result).
    - Автоматическое восстановление соединения при перезапуске брокера без падения сервиса.
    - Структурированное логирование получения каждой свечи и статуса регистрации.
    - Ручной коммит смещения (enable.auto.commit = false) после успешной обработки.
    """

    def __init__(
        self,
        connection_manager: KafkaConnectionManager,
        process_candle_use_case: ProcessCandleUseCase,
        settings: Settings,
        process_registration_result_use_case: ProcessRegistrationResultUseCase | None = None,
    ) -> None:
        self._manager = connection_manager
        self._use_case = process_candle_use_case
        self._settings = settings
        self._reg_use_case = process_registration_result_use_case
        self._consumer: AIOKafkaConsumer | None = None
        self._running: bool = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Запуск фонового потребления сообщений с автоматическим переподключением."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._consume_loop())
        logger.info(
            "Фоновый сервис Kafka Consumer инициализирован (темы: %s, %s; группа: %s)",
            self._settings.candle_topic,
            self._settings.topic_registration_result,
            self._settings.kafka_group_id,
        )

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
        logger.info("Kafka Consumer успешно остановлен.")

    async def _consume_loop(self) -> None:
        """Отказоустойчивый цикл чтения и обработки входящих событий."""
        topics = [self._settings.candle_topic, self._settings.topic_registration_result]

        while self._running:
            if not self._settings.kafka_password:
                logger.info("Kafka Consumer ожидает конфигурации (KAFKA_PASSWORD не задан в .env).")
                await asyncio.sleep(10)
                continue

            try:
                if self._consumer is None:
                    self._consumer = self._manager.create_consumer(topics=topics)

                if self._consumer is None:
                    await asyncio.sleep(5)
                    continue

                logger.info(
                    "Подключение Kafka Consumer к %s (темы: %s, группа: %s)...",
                    self._settings.kafka_bootstrap_servers,
                    topics,
                    self._settings.kafka_group_id,
                )
                await self._consumer.start()
                logger.info("Kafka Consumer успешно подключен к брокеру и ожидает сообщений.")

                async for message in self._consumer:
                    if not self._running:
                        break

                    try:
                        # 1. Обработка ответа на регистрацию (account.registration.result)
                        if message.topic == self._settings.topic_registration_result:
                            svc_name, sec_key, success, reason, ts = EventMapper.parse_registration_result(message.value)
                            is_our_service = (
                                (svc_name and svc_name == self._settings.app_name)
                                or (sec_key and sec_key == self._settings.secret_key)
                            )
                            if is_our_service:
                                logger.info(
                                    "Получен результат регистрации нашего сервиса: serviceName=%s, secretKey=%s, success=%s, reason='%s' (partition=%s, offset=%s)",
                                    svc_name,
                                    sec_key,
                                    success,
                                    reason,
                                    message.partition,
                                    message.offset,
                                )
                                if self._reg_use_case:
                                    await self._reg_use_case.execute(
                                        ProcessRegistrationResultCommand(
                                            service_name=svc_name,
                                            secret_key=sec_key,
                                            success=success,
                                            reason=reason,
                                            timestamp=ts,
                                        )
                                    )
                            else:
                                logger.debug(
                                    "Пропуск результата регистрации другого сервиса: serviceName=%s, secretKey=%s (offset=%s)",
                                    svc_name,
                                    sec_key,
                                    message.offset,
                                )

                        # 2. Обработка входящей свечи (market.candle.closed)
                        else:
                            command = CandleMapper.parse_kafka_message(message.value)
                            logger.info(
                                "Получена свеча: eventType=market.candle.closed, ticker=%s, timestamp=%s, close=%.2f, volume=%.0f (topic=%s, partition=%s, offset=%s)",
                                command.ticker,
                                command.timestamp.isoformat(),
                                command.close,
                                command.volume,
                                message.topic,
                                message.partition,
                                message.offset,
                            )
                            await self._use_case.execute(command)

                        # At-least-once: фиксация смещения после обработки
                        if not self._settings.kafka_enable_auto_commit:
                            await self._consumer.commit()

                    except Exception as msg_err:
                        logger.error(
                            "Ошибка при обработке сообщения из Kafka (topic=%s, partition=%s, offset=%s): %s",
                            message.topic,
                            message.partition,
                            message.offset,
                            msg_err,
                        )

            except asyncio.CancelledError:
                break
            except Exception as loop_err:
                logger.warning(
                    "Временная недоступность брокера Kafka (%s): %s. Автоматическое переподключение через 5 секунд...",
                    self._settings.kafka_bootstrap_servers,
                    loop_err,
                )
                if self._consumer:
                    try:
                        await self._consumer.stop()
                    except Exception:
                        pass
                    self._consumer = None
                await asyncio.sleep(5)
