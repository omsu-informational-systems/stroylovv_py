from pydantic import AnyHttpUrl, EmailStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Настройки приложения, загружаемые из переменных окружения / .env файла."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
        env_nested_delimiter="__",  
    )

    # --- Метаданные сервиса ---
    app_name: str = "Breakout-strategy"
    app_version: str = "1.0.0"

    # --- Сеть ---
    app_host: str = "0.0.0.0"
    app_port: int = 8000

    # --- Логирование ---
    log_level: str = "INFO"

    # --- Контактная информация (OpenAPI) ---
    contact_name: str = "StroylovV"
    contact_email: EmailStr = "spartan9611@gmail.com"
    contact_url: AnyHttpUrl = "https://github.com/StroylovV"


settings = Settings()