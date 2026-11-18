import asyncio
import logging
import threading
from typing import Any

from src.infrastructure.config.settings import Settings

logger = logging.getLogger(__name__)


class KafkaConnectionManager:
    """
    Менеджер соединения с защищённым брокером Kafka (Infrastructure Layer).

    Использует библиотеку confluent-kafka (librdkafka) с параметрами:
    - bootstrap.servers = omsu.7bits.it:9094
    - security.protocol = SASL_SSL
    - sasl.mechanism = SCRAM-SHA-256
    - enable.ssl.certificate.verification = True
    - ssl.endpoint.identification.algorithm = https
    - опциональный ssl.ca.location (KAFKA_CA_LOCATION)
    - enable.idempotence = True для Producer
    - enable.auto.commit = False, enable.auto.offset.store = False,
      allow.auto.create.topics = False для Consumer
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._producer: Any | None = None
        self._producer_lock = threading.Lock()
        self._is_producer_connected: bool = False
        self._is_consumer_connected: bool = False

    @property
    def is_connected(self) -> bool:
        return self._is_producer_connected or self._is_consumer_connected

    @property
    def is_producer_connected(self) -> bool:
        return self._is_producer_connected

    @property
    def is_consumer_connected(self) -> bool:
        return self._is_consumer_connected

    def set_consumer_connected(self, connected: bool) -> None:
        self._is_consumer_connected = connected

    @property
    def producer(self) -> Any | None:
        return self._producer

    def resolve_ca_location(self) -> str | None:
        """
        Определить путь к доверенному набору CA-сертификатов.

        Приоритет:
        1. Явный KAFKA_CA_LOCATION из переменных окружения / .env.
        2. Набор доверенных корневых сертификатов certifi (необходим на Windows,
           где встроенный OpenSSL в confluent-kafka не читает хранилище Windows).
        Проверка TLS-сертификата и имени хоста при этом всегда остаётся включённой.
        """
        explicit = self._settings.kafka_ca_location.strip()
        if explicit:
            return explicit
        try:
            import certifi

            return certifi.where()
        except ImportError:
            return None

    def build_base_config(self) -> dict[str, Any]:
        """Сформировать базовую защищённую конфигурацию confluent-kafka."""
        config: dict[str, Any] = {
            "bootstrap.servers": self._settings.kafka_bootstrap_servers,
            "security.protocol": self._settings.kafka_security_protocol,
            "sasl.mechanism": self._settings.kafka_sasl_mechanism,
            "sasl.username": self._settings.kafka_username,
            "sasl.password": self._settings.kafka_password,
            "enable.ssl.certificate.verification": True,
            "ssl.endpoint.identification.algorithm": "https",
        }
        ca_location = self.resolve_ca_location()
        if ca_location:
            config["ssl.ca.location"] = ca_location
        return config

    def build_producer_config(self) -> dict[str, Any]:
        """Конфигурация идемпотентного Producer."""
        return {
            **self.build_base_config(),
            "enable.idempotence": True,
        }

    def build_consumer_config(self, group_id: str) -> dict[str, Any]:
        """Конфигурация надёжного Consumer с ручным подтверждением смещений."""
        return {
            **self.build_base_config(),
            "group.id": group_id,
            "auto.offset.reset": self._settings.kafka_auto_offset_reset,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }

    def check_connection_sync(
        self,
        timeout: float = 10.0,
        *,
        verbose: bool = False,
    ) -> tuple[int, list[str]]:
        """
        Проверить TLS, SCRAM-SHA-256 авторизацию и наличие обязательных тем
        без регистрации аккаунта (режим --check).

        Возвращает: (общее_количество_тем, список_отсутствующих_обязательных_тем).
        """
        from confluent_kafka import Producer

        broker_errors: list[str] = []

        def _on_error(err: Any) -> None:
            broker_errors.append(str(err))

        cfg: dict[str, Any] = {**self.build_producer_config(), "error_cb": _on_error}
        if verbose:
            cfg["debug"] = "broker,security,protocol"

        diag_logger = logging.getLogger("librdkafka.check")
        captured_logs: list[str] = []

        class _CaptureHandler(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                msg = record.getMessage()
                if "password" not in msg.lower():
                    captured_logs.append(msg)

        handler = _CaptureHandler()
        diag_logger.addHandler(handler)
        diag_logger.setLevel(logging.DEBUG if verbose else logging.WARNING)
        diag_logger.propagate = False

        producer = Producer(cfg, logger=diag_logger)
        try:
            metadata = producer.list_topics(timeout=timeout)
        except Exception as exc:
            # Прокачиваем очередь обратных вызовов, чтобы error_cb и logger получили причину сбоя
            producer.poll(0.2)
            details = list(dict.fromkeys(broker_errors + captured_logs))
            if details:
                raise RuntimeError(f"{exc}\nДетали librdkafka:\n  - " + "\n  - ".join(details)) from exc
            raise
        finally:
            diag_logger.removeHandler(handler)

        available = metadata.topics
        missing = sorted(
            topic
            for topic in self._settings.expected_topics
            if topic not in available or available[topic].error is not None
        )
        return len(available), missing

    async def start_producer(self) -> None:
        """Инициализация, проверка метаданных и запуск защищённого Kafka Producer."""
        if not self._settings.is_kafka_configured:
            logger.warning(
                "KAFKA_USERNAME или KAFKA_PASSWORD не заданы. "
                "Kafka Producer не запущен (режим локальной разработки/тестирования)."
            )
            return

        try:
            await asyncio.to_thread(self._start_producer_sync)
        except ImportError:
            self._is_producer_connected = False
            logger.error(
                "Библиотека confluent-kafka не установлена. Выполните: poetry install"
            )
        except Exception as exc:
            self._is_producer_connected = False
            self._producer = None
            logger.error("Не удалось подключить Kafka Producer: %s", exc)

    def _start_producer_sync(self) -> None:
        from confluent_kafka import Producer

        producer = Producer(self.build_producer_config())
        metadata = producer.list_topics(timeout=10)
        available = metadata.topics
        missing = sorted(
            topic
            for topic in self._settings.expected_topics
            if topic not in available or available[topic].error is not None
        )
        if missing:
            logger.warning(
                "Некоторые ожидаемые темы отсутствуют на брокере: %s",
                ", ".join(missing),
            )
        self._producer = producer
        self._is_producer_connected = True
        logger.info(
            "Kafka Producer успешно подключён к %s (TLS + %s; доступно тем: %d)",
            self._settings.kafka_bootstrap_servers,
            self._settings.kafka_sasl_mechanism,
            len(available),
        )

    async def stop_producer(self) -> None:
        """Корректная остановка Producer с досылкой буферизованных сообщений."""
        if self._producer is not None:
            try:
                await asyncio.to_thread(self._flush_and_clear_producer_sync)
            except Exception as exc:
                logger.warning("Ошибка при остановке Kafka Producer: %s", exc)
            finally:
                self._is_producer_connected = False
                self._producer = None

    def _flush_and_clear_producer_sync(self) -> None:
        with self._producer_lock:
            if self._producer is not None:
                self._producer.flush(self._settings.kafka_flush_timeout_seconds)
                self._producer = None

    def publish_sync(self, topic: str, key: bytes, value: bytes) -> None:
        """
        Синхронная публикация сообщения с обязательным подтверждением доставки брокером.

        Если брокер не подтвердил приём сообщения, выбрасывает RuntimeError —
        это гарантирует, что смещение свечи не будет зафиксировано.
        """
        with self._producer_lock:
            if self._producer is None:
                raise RuntimeError("Kafka Producer не подключён")

            delivered: list[Any] = []

            def _on_delivery(err: Any, _msg: Any) -> None:
                delivered.append(err)

            self._producer.produce(
                topic,
                key=key,
                value=value,
                on_delivery=_on_delivery,
            )
            remaining = self._producer.flush(self._settings.kafka_flush_timeout_seconds)
            if remaining or not delivered or delivered[0] is not None:
                detail = delivered[0] if delivered else "таймаут ожидания подтверждения"
                raise RuntimeError(
                    f"Не подтверждена публикация в тему '{topic}': {detail}"
                )

    async def publish(self, topic: str, key: bytes, value: bytes) -> None:
        """Асинхронная обёртка подтверждённой публикации сообщения."""
        await asyncio.to_thread(self.publish_sync, topic, key, value)

    def create_consumer(self, group_id: str) -> Any | None:
        """Создать настроенный confluent-kafka Consumer для указанной технической группы."""
        if not self._settings.is_kafka_configured:
            logger.warning("Учётные данные Kafka не заданы. Consumer не создан.")
            return None

        from confluent_kafka import Consumer

        return Consumer(self.build_consumer_config(group_id=group_id))
