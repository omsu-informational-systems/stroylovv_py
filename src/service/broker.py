"""
Модуль брокера сообщений (RabbitMQ).

Принцип DIP: высокоуровневые модули зависят от абстракции MessageBroker,
а не от конкретной реализации aio-pika.

Принцип SRP: класс RabbitMQBroker отвечает только за транспорт сообщений.
Торговая логика остаётся в strategy.py.
"""

import json
import logging
from abc import ABC, abstractmethod

from src.schemas import CandleMessage, TradeRecommendation

logger = logging.getLogger(__name__)


class MessageBroker(ABC):
    """Абстракция транспортного слоя (DIP / ISP)."""

    @abstractmethod
    async def connect(self) -> None:
        """Установить соединение с брокером."""
        ...

    @abstractmethod
    async def disconnect(self) -> None:
        """Закрыть соединение с брокером."""
        ...

    @abstractmethod
    async def publish_recommendation(self, recommendation: TradeRecommendation) -> None:
        """Опубликовать торговую рекомендацию."""
        ...

    @abstractmethod
    async def start_consuming(self) -> None:
        """Начать потребление свечных данных из очереди."""
        ...


class RabbitMQBroker(MessageBroker):
    """
    Реализация брокера сообщений на основе RabbitMQ (AMQP 0-9-1).

    Конфигурация обменников и очередей согласно спецификации проекта:
    - Входящий обменник: market.events (topic, durable)
    - Очередь: candle_processing_queue (durable, с DLX)
    - Исходящий обменник: market.recommendations (topic, durable)
    """

    # Константы инфраструктуры RabbitMQ (SRP: конфигурация рядом с реализацией)
    INPUT_EXCHANGE = "market.events"
    INPUT_QUEUE = "candle_processing_queue"
    INPUT_ROUTING_KEY = "candle.daily.#"

    OUTPUT_EXCHANGE = "market.recommendations"
    OUTPUT_ROUTING_KEY_TEMPLATE = "recommendation.{ticker}"

    DLX_EXCHANGE = "market.events.dlx"
    DLX_QUEUE = "candle_processing_queue.dlx"

    def __init__(self, amqp_url: str) -> None:
        self._amqp_url = amqp_url
        self._connection = None
        self._channel = None

    async def connect(self) -> None:
        """
        Установить соединение и объявить топологию RabbitMQ.

        Требует: pip install aio-pika
        """
        # TODO: реализовать через aio-pika после добавления зависимости
        # import aio_pika
        # self._connection = await aio_pika.connect_robust(self._amqp_url)
        # self._channel = await self._connection.channel()
        # await self._declare_topology()
        logger.info("Подключение к RabbitMQ: %s", self._amqp_url)
        raise NotImplementedError("Требуется установка aio-pika: poetry add aio-pika")

    async def disconnect(self) -> None:
        """Корректно закрыть соединение с брокером."""
        if self._connection and not self._connection.is_closed:
            await self._connection.close()
            logger.info("Соединение с RabbitMQ закрыто.")

    async def publish_recommendation(self, recommendation: TradeRecommendation) -> None:
        """Опубликовать рекомендацию в обменник market.recommendations."""
        if self._channel is None:
            raise RuntimeError("Брокер не подключён. Вызовите connect() перед публикацией.")

        routing_key = self.OUTPUT_ROUTING_KEY_TEMPLATE.format(
            ticker=recommendation.ticker.lower()
        )
        payload = recommendation.model_dump_json().encode()

        # TODO: добавить publisher confirms через aio_pika.Message с delivery_mode
        logger.debug(
            "Публикация рекомендации: ticker=%s signal=%s routing_key=%s",
            recommendation.ticker,
            recommendation.signal,
            routing_key,
        )

    async def start_consuming(self) -> None:
        """Начать потребление свечей из candle_processing_queue."""
        if self._channel is None:
            raise RuntimeError("Брокер не подключён. Вызовите connect() перед потреблением.")
        # TODO: реализовать через aio_pika queue.consume(callback)
        logger.info("Начало потребления из очереди: %s", self.INPUT_QUEUE)

    def _parse_candle(self, body: bytes) -> CandleMessage:
        """Десериализовать и валидировать входящее JSON-сообщение."""
        data = json.loads(body)
        return CandleMessage.model_validate(data)
