import asyncio
import logging
import time
from typing import Any

from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.application.use_cases.process_recommendation_result import (
    ProcessRecommendationResultUseCase,
)
from src.application.use_cases.process_registration_result import (
    ProcessRegistrationResultUseCase,
)
from src.application.use_cases.register_service import RegisterServiceUseCase
from src.domain.entities.registration import ServiceRegistration
from src.domain.ports.consumer import EventConsumerPort
from src.infrastructure.config.settings import Settings
from src.infrastructure.kafka.client import KafkaConnectionManager
from src.infrastructure.mappers.candle_mapper import CandleMapper
from src.infrastructure.mappers.event_mapper import EventMapper

logger = logging.getLogger(__name__)


class KafkaConsumerService(EventConsumerPort):
    """
    Адаптер потребления сообщений из защищённой Kafka (Infrastructure Layer).

    Реализует протокол платформы с двумя раздельными группами потребителей:
    1. `<KAFKA_GROUP_ID>.answers` -> account.registration.result, strategy.recommendation.result
       - Подписывается и дожидается назначения разделов ДО отправки запроса регистрации.
       - Отправляет account.registration.request и ожидает подтверждения REGISTERED.
       - Читает результаты исполнения рекомендаций (strategy.recommendation.result).
    2. `<KAFKA_GROUP_ID>.candles` -> <CANDLE_TOPIC>
       - Подписывается ТОЛЬКО после подтверждения регистрации.
       - Обрабатывает только события eventType == market.candle.closed.
       - Фиксирует смещение конкретной записи (commit(message=msg, asynchronous=False))
         строго ПОСЛЕ подтверждённой публикации рекомендации.
    """

    def __init__(
        self,
        connection_manager: KafkaConnectionManager,
        process_candle_use_case: ProcessCandleUseCase,
        settings: Settings,
        registration: ServiceRegistration,
        register_service_use_case: RegisterServiceUseCase | None = None,
        process_registration_result_use_case: ProcessRegistrationResultUseCase | None = None,
        process_recommendation_result_use_case: ProcessRecommendationResultUseCase | None = None,
    ) -> None:
        self._manager = connection_manager
        self._candle_use_case = process_candle_use_case
        self._settings = settings
        self._registration = registration
        self._register_use_case = register_service_use_case
        self._reg_result_use_case = process_registration_result_use_case
        self._rec_result_use_case = process_recommendation_result_use_case

        self._answers_consumer: Any | None = None
        self._candles_consumer: Any | None = None
        self._running: bool = False
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Запуск фонового цикла регистрации и потребления сообщений."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._consume_loop())
        logger.info(
            "Фоновый сервис Kafka Consumer запущен (answers=%s, candles=%s, candle_topic=%s)",
            self._settings.kafka_answers_group_id,
            self._settings.kafka_candles_group_id,
            self._settings.candle_topic,
        )

    async def stop(self) -> None:
        """Корректная остановка потребителей Kafka."""
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        await self._close_consumers()
        logger.info("Kafka Consumer успешно остановлен.")

    async def _close_consumers(self) -> None:
        self._manager.set_consumer_connected(False)
        for attr in ("_candles_consumer", "_answers_consumer"):
            consumer = getattr(self, attr)
            if consumer is not None:
                try:
                    await asyncio.to_thread(consumer.close)
                except Exception as exc:
                    logger.warning("Ошибка при закрытии Kafka Consumer (%s): %s", attr, exc)
                finally:
                    setattr(self, attr, None)

    @staticmethod
    def _poll_one_sync(consumer: Any, timeout: float) -> Any | None:
        msg = consumer.poll(timeout)
        if msg is None:
            return None
        if msg.error():
            raise RuntimeError(f"Ошибка Kafka Consumer: {msg.error().code()}")
        return msg

    @staticmethod
    def _commit_sync(consumer: Any, message: Any) -> None:
        consumer.commit(message=message, asynchronous=False)

    @staticmethod
    def _commit_async(consumer: Any, message: Any) -> None:
        consumer.commit(message=message, asynchronous=True)

    async def _wait_assignment(self, consumer: Any, label: str) -> None:
        """Дождаться назначения разделов consumer-у (согласно протоколу до отправки регистрации)."""
        deadline = time.monotonic() + self._settings.kafka_assignment_timeout_seconds
        while self._running:
            assigned = await asyncio.to_thread(consumer.assignment)
            if assigned:
                logger.info(
                    "Назначены разделы для группы '%s' (%d партиций).",
                    label,
                    len(assigned),
                )
                return
            if time.monotonic() > deadline:
                raise TimeoutError(f"Не назначены разделы темы для группы '{label}'")
            await asyncio.to_thread(consumer.poll, 0.2)

    async def _handle_answer_message(self, consumer: Any, message: Any) -> bool:
        """
        Обработать одно сообщение из answers-группы и зафиксировать смещение.

        Чужие или невалидные сообщения в общей теме ответов пропускаются быстро
        (asynchronous commit), а собственный ответ фиксируется синхронно.
        Возвращает True, если обработан собственный ответ account.registration.result.
        """
        raw = message.value()
        if raw is None:
            await asyncio.to_thread(self._commit_async, consumer, message)
            return False

        topic = message.topic()
        is_own_registration = False
        is_own_recommendation = False

        try:
            if topic == self._settings.topic_registration_result:
                cmd = EventMapper.parse_registration_result(raw)
                if self._reg_result_use_case is not None:
                    is_own_registration = await self._reg_result_use_case.execute(cmd)
            elif topic == self._settings.topic_recommendation_result:
                cmd = EventMapper.parse_recommendation_result(raw)
                if self._rec_result_use_case is not None:
                    is_own_recommendation = await self._rec_result_use_case.execute(cmd)
        except Exception as parse_err:
            logger.warning(
                "Пропуск некорректного сообщения в теме ответов %s (offset=%s): %s",
                topic,
                message.offset(),
                parse_err,
            )
            await asyncio.to_thread(self._commit_async, consumer, message)
            return False

        if is_own_registration or is_own_recommendation:
            await asyncio.to_thread(self._commit_sync, consumer, message)
        else:
            await asyncio.to_thread(self._commit_async, consumer, message)
        return is_own_registration

    async def _perform_registration_handshake(self) -> bool:
        """
        Выполнить полный handshake регистрации:
        1. Подписать answers-consumer и дождаться назначения разделов.
        2. Отправить account.registration.request.
        3. Ожидать собственный account.registration.result (REGISTERED / REJECTED).
        """
        answer_topics = [
            self._settings.topic_registration_result,
            self._settings.topic_recommendation_result,
        ]
        self._answers_consumer = self._manager.create_consumer(
            group_id=self._settings.kafka_answers_group_id
        )
        if self._answers_consumer is None:
            return False

        await asyncio.to_thread(self._answers_consumer.subscribe, answer_topics)
        await self._wait_assignment(
            self._answers_consumer, self._settings.kafka_answers_group_id
        )
        self._manager.set_consumer_connected(True)

        if self._register_use_case is not None:
            await self._register_use_case.execute()

        deadline = time.monotonic() + self._settings.registration_timeout_seconds
        while self._running:
            if time.monotonic() > deadline:
                raise TimeoutError(
                    "Нет ответа регистрации (account.registration.result); "
                    "проверьте настройки ключа и тем с преподавателем"
                )
            msg = await asyncio.to_thread(self._poll_one_sync, self._answers_consumer, 0.2)
            if msg is None:
                continue

            is_own = await self._handle_answer_message(self._answers_consumer, msg)
            if is_own:
                return self._registration.is_active

        return False

    async def _handle_candle_message(self, consumer: Any, message: Any) -> None:
        """
        Обработать одно сообщение из темы свечей:
        - Если eventType != market.candle.closed -> коммит и пропуск.
        - Если market.candle.closed -> обработка и подтверждённая публикация, затем sync commit.
        - При ошибке публикации смещение НЕ подтверждается!
        """
        raw = message.value()
        if raw is None:
            await asyncio.to_thread(self._commit_sync, consumer, message)
            return

        event_type, command = CandleMapper.parse_kafka_envelope(raw)
        if command is None:
            logger.debug(
                "Пропуск события с типом eventType=%s в теме свечей (offset=%s)",
                event_type,
                message.offset(),
            )
            await asyncio.to_thread(self._commit_sync, consumer, message)
            return

        logger.info(
            "Получена свеча: eventType=%s, ticker=%s, seq=%s, close=%s, volume=%.0f (partition=%s, offset=%s)",
            event_type,
            command.ticker,
            command.sequence_number,
            command.close,
            command.volume,
            message.partition(),
            message.offset(),
        )

        # Обрабатываем свечу и публикуем рекомендацию с подтверждением доставки брокером
        await self._candle_use_case.execute(command)

        # At-Least-Once: фиксируем смещение ТОЛЬКО после успешной обработки и подтверждённой публикации
        await asyncio.to_thread(self._commit_sync, consumer, message)

    async def _consume_loop(self) -> None:
        """Отказоустойчивый цикл регистрации и чтения потоков Kafka."""
        while self._running:
            if not self._settings.is_kafka_configured:
                logger.info(
                    "Kafka Consumer ожидает конфигурации (KAFKA_USERNAME / KAFKA_PASSWORD не заданы)."
                )
                await asyncio.sleep(10)
                continue

            if not self._manager.is_producer_connected:
                await self._manager.start_producer()
                if not self._manager.is_producer_connected:
                    await asyncio.sleep(5)
                    continue

            try:
                registered = await self._perform_registration_handshake()
                if not self._running:
                    break

                if not registered:
                    logger.error(
                        "Регистрация отклонена платформой. Чтение свечей не запускается."
                    )
                    # Остаёмся в пассивном режиме, сохраняя HTTP /health доступным (DEGRADED)
                    await asyncio.sleep(30)
                    continue

                # Регистрация подтверждена -> подписываемся на тему свечей отдельной группой
                self._candles_consumer = self._manager.create_consumer(
                    group_id=self._settings.kafka_candles_group_id
                )
                if self._candles_consumer is None:
                    await asyncio.sleep(5)
                    continue

                await asyncio.to_thread(
                    self._candles_consumer.subscribe, [self._settings.candle_topic]
                )
                logger.info(
                    "Регистрация подтверждена; подписка на тему свечей '%s' (группа: %s).",
                    self._settings.candle_topic,
                    self._settings.kafka_candles_group_id,
                )

                while self._running:
                    # 1. Читаем ответы платформы (strategy.recommendation.result и др.)
                    answer_msg = await asyncio.to_thread(
                        self._poll_one_sync, self._answers_consumer, 0.05
                    )
                    if answer_msg is not None:
                        try:
                            await self._handle_answer_message(self._answers_consumer, answer_msg)
                        except Exception as ans_err:
                            logger.error(
                                "Ошибка обработки ответа платформы (topic=%s, offset=%s): %s",
                                answer_msg.topic(),
                                answer_msg.offset(),
                                ans_err,
                            )

                    # 2. Читаем поток закрытых свечей
                    candle_msg = await asyncio.to_thread(
                        self._poll_one_sync, self._candles_consumer, 0.1
                    )
                    if candle_msg is not None:
                        try:
                            await self._handle_candle_message(self._candles_consumer, candle_msg)
                        except Exception as msg_err:
                            logger.error(
                                "Ошибка обработки свечи (topic=%s, partition=%s, offset=%s); "
                                "смещение НЕ зафиксировано: %s",
                                candle_msg.topic(),
                                candle_msg.partition(),
                                candle_msg.offset(),
                                msg_err,
                            )

            except asyncio.CancelledError:
                break
            except ImportError:
                logger.error(
                    "Библиотека confluent-kafka не установлена. Остановка цикла Kafka Consumer."
                )
                break
            except Exception as loop_err:
                logger.warning(
                    "Временная недоступность брокера Kafka (%s): %s. "
                    "Повторное подключение через 5 секунд...",
                    self._settings.kafka_bootstrap_servers,
                    loop_err,
                )
                await self._close_consumers()
                await asyncio.sleep(5)
