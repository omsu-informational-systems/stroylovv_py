import logging
import ssl
from typing import Any

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from src.infrastructure.config.settings import Settings

logger = logging.getLogger(__name__)


class KafkaConnectionManager:
    """
    Менеджер соединения с брокером Kafka (Infrastructure Layer).

    Настраивает защищенное соединение SASL_SSL + SCRAM-SHA-256:
    - bootstrap.servers = omsu.7bits.it:9094
    - security.protocol = SASL_SSL
    - sasl.mechanism = SCRAM-SHA-256
    - sasl.username = student-key-05
    - sasl.password = секрет из конфигурации
    - проверка TLS-сертификата сервера omsu.7bits.it
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._is_producer_connected: bool = False
        self._is_consumer_connected: bool = False

    @property
    def is_connected(self) -> bool:
        return self._is_producer_connected or self._is_consumer_connected

    @property
    def producer(self) -> AIOKafkaProducer | None:
        return self._producer

    @property
    def consumer(self) -> AIOKafkaConsumer | None:
        return self._consumer

    def _create_ssl_context(self) -> ssl.SSLContext:
        """Создание TLS контекста с системным набором доверенных сертификатов."""
        ctx = ssl.create_default_context()
        ctx.check_hostname = True
        ctx.verify_mode = ssl.CERT_REQUIRED
        return ctx

    async def start_producer(self) -> None:
        """Инициализация и запуск защищенного Kafka Producer."""
        if not self._settings.kafka_password:
            logger.warning(
                "KAFKA_PASSWORD не задан. Kafka Producer не запущен (режим разработки/тестирования)."
            )
            return

        try:
            ssl_ctx = self._create_ssl_context()
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                security_protocol=self._settings.kafka_security_protocol,
                sasl_mechanism=self._settings.kafka_sasl_mechanism,
                sasl_plain_username=self._settings.kafka_username,
                sasl_plain_password=self._settings.kafka_password,
                ssl_context=ssl_ctx,
            )
            await self._producer.start()
            self._is_producer_connected = True
            logger.info(
                "Kafka Producer успешно подключен к %s как %s",
                self._settings.kafka_bootstrap_servers,
                self._settings.kafka_username,
            )
        except Exception as exc:
            self._is_producer_connected = False
            logger.error("Не удалось подключить Kafka Producer: %s", exc)

    async def stop_producer(self) -> None:
        """Корректная остановка Producer."""
        if self._producer:
            try:
                await self._producer.stop()
            except Exception as exc:
                logger.warning("Ошибка при остановке Kafka Producer: %s", exc)
            finally:
                self._is_producer_connected = False
                self._producer = None

    def create_consumer(self, topics: list[str]) -> AIOKafkaConsumer | None:
        """Создать настроенный AIOKafkaConsumer для указанных тем."""
        if not self._settings.kafka_password:
            logger.warning("KAFKA_PASSWORD не задан. Kafka Consumer не создан.")
            return None

        ssl_ctx = self._create_ssl_context()
        return AIOKafkaConsumer(
            *topics,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            security_protocol=self._settings.kafka_security_protocol,
            sasl_mechanism=self._settings.kafka_sasl_mechanism,
            sasl_plain_username=self._settings.kafka_username,
            sasl_plain_password=self._settings.kafka_password,
            ssl_context=ssl_ctx,
            group_id=self._settings.kafka_group_id,
            auto_offset_reset=self._settings.kafka_auto_offset_reset,
            enable_auto_commit=self._settings.kafka_enable_auto_commit,
        )

