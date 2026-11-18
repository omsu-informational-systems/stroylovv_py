from pathlib import Path

from pydantic import AnyHttpUrl, EmailStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
ENV_FILE_PATH = PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    """
    Настройки приложения и интеграции с защищенной Kafka платформы.

    Загружаются из переменных окружения или локального файла .env.
    Секреты (KAFKA_PASSWORD, KAFKA_USERNAME, ACCOUNT_SECRET_KEY) никогда не
    хардкодятся в коде и не выводятся в repr/логи.
    """

    model_config = SettingsConfigDict(
        env_file=(str(ENV_FILE_PATH), ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_nested_delimiter="__",
    )

    # --- Метаданные микросервиса и стабильные идентификаторы студента ---
    app_name: str = "breakout-strategy"
    app_version: str = "1.0.0"
    service_name: str = ""
    account_id: str = "stroylov-v"
    account_name: str = "Виталий Стройлов"
    group_id: str = "МИБ-401"
    strategy_id: str = "hybrid-breakout-stroylov"
    strategy_name: str = "Hybrid Breakout & Pairs Strategy"
    service_description: str = (
        "Автоматизированный сервис торговой стратегии пробоя каналов Дончяна и парного арбитража"
    )
    schema_version: str = "1.0"

    # --- HTTP сервер ---
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    log_level: str = "INFO"

    # --- Параметры торговой стратегии ---
    strategy_type: str = "hybrid"  # "hybrid" (гибрид), "pairs" (пары) или "donchian" (Дончян)
    strategy_window: int = 20
    default_order_quantity: int = 1

    # Настройки парного трейдинга (SBER + T)
    pairs_primary_ticker: str = "T"
    pairs_benchmark_ticker: str = "SBER"
    pairs_window: int = 30
    pairs_z_entry: float = 1.8
    pairs_z_exit: float = 0.5

    # Настройки гибридного ансамбля
    hybrid_primary_ticker: str = "T"
    hybrid_benchmark_ticker: str = "SBER"
    hybrid_momentum_ticker: str = "OZON"
    hybrid_donchian_window: int = 20
    hybrid_pairs_window: int = 30
    hybrid_z_entry: float = 1.8
    hybrid_z_exit: float = 0.5
    hybrid_volume_multiplier: float = 1.25

    # --- Подключение к защищенной Kafka платформы (SASL_SSL + SCRAM-SHA-256) ---
    kafka_bootstrap_servers: str = "omsu.7bits.it:9094"
    kafka_security_protocol: str = "SASL_SSL"
    kafka_sasl_mechanism: str = "SCRAM-SHA-256"
    kafka_username: str = ""
    kafka_password: str = ""
    kafka_ca_location: str = ""
    kafka_group_id: str = "strategy-breakout-v1"
    kafka_auto_offset_reset: str = "earliest"
    kafka_enable_auto_commit: bool = False

    # Поле secretKey протокола платформы (совпадает с ключом пары / KAFKA_USERNAME; секрет пары сюда НЕ передаётся)
    account_secret_key: str = ""
    secret_key: str = ""

    # Темы сообщений Kafka
    topic_registration_request: str = "account.registration.request"
    topic_registration_result: str = "account.registration.result"
    topic_heartbeat: str = "account.service.heartbeat"
    candle_topic: str = "market.candle.default"
    topic_recommendation_created: str = "strategy.recommendation.created"
    topic_recommendation_result: str = "strategy.recommendation.result"

    # Интервалы и таймауты
    heartbeat_interval_seconds: int = 5
    registration_timeout_seconds: int = 30
    kafka_assignment_timeout_seconds: int = 30
    kafka_flush_timeout_seconds: float = 5.0

    # Контактная информация OpenAPI
    contact_name: str = "StroylovV"
    contact_email: EmailStr = "spartan9611@gmail.com"
    contact_url: AnyHttpUrl = "https://github.com/StroylovV"

    @property
    def effective_service_name(self) -> str:
        """Стабильное имя сервиса (совпадает с producer в обёртке событий)."""
        return self.service_name.strip() or self.app_name.strip()

    @property
    def effective_secret_key(self) -> str:
        """
        Ключ пары для поля secretKey в сообщениях платформы.

        По спецификации совпадает с KAFKA_USERNAME (секрет пары сюда не передаётся).
        """
        return (
            self.account_secret_key.strip()
            or self.secret_key.strip()
            or self.kafka_username.strip()
        )

    @property
    def kafka_answers_group_id(self) -> str:
        """Отдельная техническая группа Kafka для чтения ответов платформы."""
        return f"{self.kafka_group_id}.answers"

    @property
    def kafka_candles_group_id(self) -> str:
        """Отдельная техническая группа Kafka для чтения потока свечей."""
        return f"{self.kafka_group_id}.candles"

    @property
    def is_kafka_configured(self) -> bool:
        """Заданы ли реальные учётные данные для подключения к защищённой Kafka."""
        user = self.kafka_username.strip()
        pwd = self.kafka_password.strip()
        if not user or not pwd:
            return False
        if (user.startswith("<") and user.endswith(">")) or (
            pwd.startswith("<") and pwd.endswith(">")
        ):
            return False
        return True

    @property
    def expected_topics(self) -> set[str]:
        """Полный набор обязательных тем брокера Kafka."""
        return {
            self.candle_topic,
            self.topic_registration_request,
            self.topic_registration_result,
            self.topic_heartbeat,
            self.topic_recommendation_created,
            self.topic_recommendation_result,
        }

    def __repr__(self) -> str:
        return (
            f"Settings(service_name={self.effective_service_name!r}, "
            f"account_id={self.account_id!r}, "
            f"group_id={self.group_id!r}, "
            f"strategy_id={self.strategy_id!r}, "
            f"kafka_bootstrap_servers={self.kafka_bootstrap_servers!r}, "
            f"kafka_group_id={self.kafka_group_id!r}, "
            f"candle_topic={self.candle_topic!r})"
        )


settings = Settings()
