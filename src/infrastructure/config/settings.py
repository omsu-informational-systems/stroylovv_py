from pathlib import Path

from pydantic import AnyHttpUrl, EmailStr, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
ENV_FILE_PATH = PROJECT_ROOT / ".env"


class Settings(BaseSettings):
    """
    Настройки приложения и интеграций.

    Загружаются из переменных окружения или .env файла.
    """

    model_config = SettingsConfigDict(
        env_file=(str(ENV_FILE_PATH), ".env"),
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
    strategy_type: str = "hybrid"  # "hybrid" (гибрид), "pairs" (пары) или "donchian" (Дончян)
    strategy_window: int = 20

    # Настройки парного трейдинга (SBER + T)
    pairs_primary_ticker: str = "T"
    pairs_benchmark_ticker: str = "SBER"
    pairs_window: int = 30
    pairs_z_entry: float = 1.8
    pairs_z_exit: float = 0.5

    # Настройки гибридного ансамбля (полностью управляются из .env)
    hybrid_primary_ticker: str = "T"
    hybrid_benchmark_ticker: str = "SBER"
    hybrid_momentum_ticker: str = "OZON"
    hybrid_donchian_window: int = 20
    hybrid_pairs_window: int = 30
    hybrid_z_entry: float = 1.8
    hybrid_z_exit: float = 0.5
    hybrid_volume_multiplier: float = 1.25

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

