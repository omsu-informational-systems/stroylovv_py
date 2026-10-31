from pydantic import AnyHttpUrl, EmailStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Настройки приложения и интеграций.

    Загружаются из переменных окружения или .env файла.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_nested_delimiter="__",
    )

    # --- Метаданные микросервиса ---
    app_name: str = "breakout-strategy"
    app_version: str = "1.0.0"

    # --- HTTP сервер ---
    app_host: str = "0.0.0.0"
    app_port: int = 8000
    log_level: str = "INFO"

    # --- Параметры торговой стратегии ---
    strategy_window: int = 20

    # --- Подключение к защищенной Kafka платформы ---
    kafka_bootstrap_servers: str = "omsu.7bits.it:9094"
    kafka_security_protocol: str = "SASL_SSL"
    kafka_sasl_mechanism: str = "SCRAM-SHA-256"
    kafka_username: str = "student-key-05"
    kafka_password: str = ""
    kafka_group_id: str = "candles-student-key-05"
    kafka_auto_offset_reset: str = "earliest"
    kafka_enable_auto_commit: bool = False

    # Поле secretKey для совместимости протокола платформы (содержит student-key-05)
    secret_key: str = "student-key-05"

    # Темы сообщений
    topic_registration_request: str = "account.registration.request"
    topic_registration_result: str = "account.registration.result"
    topic_heartbeat: str = "account.service.heartbeat"
    candle_topic: str = "market.candle.default"
    topic_recommendation_created: str = "strategy.recommendation.created"
    topic_recommendation_result: str = "strategy.recommendation.result"

    # Интервалы фоновых процессов
    heartbeat_interval_seconds: int = 10

    # Контактная информация
    contact_name: str = "StroylovV"
    contact_email: EmailStr = "spartan9611@gmail.com"
    contact_url: AnyHttpUrl = "https://github.com/StroylovV"


settings = Settings()

