"""
Точка входа приложения FastAPI.

Содержит:
- настройку логирования
- lifespan (graceful startup / shutdown)
- сборку приложения FastAPI
- блок __main__ для локального запуска через python -m src.main
"""

from collections.abc import AsyncIterator
import logging

import uvicorn
from fastapi import FastAPI

from src.api import api_router
from src.core.config import settings

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Управление жизненным циклом приложения (graceful startup / shutdown)."""
    logger.info("Запуск сервиса '%s' v%s ...", settings.app_name, settings.app_version)
    yield
    logger.info("Остановка сервиса '%s' ...", settings.app_name)


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    contact={
        "name": settings.contact_name,
        "email": settings.contact_email,
        "url": str(settings.contact_url),
    },
    lifespan=lifespan,
)

app.include_router(api_router)


if __name__ == "__main__":
    uvicorn.run(
        "src.main:app",
        host=settings.app_host,
        port=settings.app_port,
        log_level=settings.log_level.lower(),
        reload=True,
    )
