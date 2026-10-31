from collections.abc import AsyncIterator
import logging

import uvicorn
from fastapi import FastAPI

from src.adapters.api.dependencies import (
    get_kafka_manager,
    get_publisher,
    get_settings,
    get_strategy,
)
from src.adapters.api.routers import api_router
from src.adapters.workers.heartbeat_worker import HeartbeatWorker
from src.application.commands.register_service import RegisterServiceCommand
from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.application.use_cases.register_service import RegisterServiceUseCase
from src.application.use_cases.send_heartbeat import SendHeartbeatUseCase
from src.infrastructure.kafka.consumer import KafkaConsumerService

settings = get_settings()

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Управление жизненным циклом сервиса (FastAPI Lifespan)."""
    logger.info("Запуск сервиса '%s' v%s...", settings.app_name, settings.app_version)

    kafka_manager = get_kafka_manager()
    publisher = get_publisher()
    strategy = get_strategy()

    # 1. Подключение к защищенной Kafka
    try:
        await kafka_manager.start_producer()
    except Exception as exc:
        logger.warning("Не удалось инициализировать Kafka Producer: %s", exc)

    # 2. Отправка запроса на регистрацию сервиса
    register_use_case = RegisterServiceUseCase(publisher=publisher)
    try:
        await register_use_case.execute(
            RegisterServiceCommand(service_name=settings.app_name)
        )
    except Exception as exc:
        logger.warning("Не удалось отправить регистрацию: %s", exc)

    # 3. Запуск воркера периодического heartbeat
    heartbeat_use_case = SendHeartbeatUseCase(publisher=publisher)
    heartbeat_worker = HeartbeatWorker(
        use_case=heartbeat_use_case,
        service_name=settings.app_name,
        interval_seconds=settings.heartbeat_interval_seconds,
    )
    heartbeat_worker.start()

    # 4. Запуск Kafka Consumer свечей
    process_candle_use_case = ProcessCandleUseCase(
        strategy=strategy,
        publisher=publisher,
    )
    consumer_service = KafkaConsumerService(
        connection_manager=kafka_manager,
        process_candle_use_case=process_candle_use_case,
        settings=settings,
    )
    try:
        await consumer_service.start()
    except Exception as exc:
        logger.warning("Не удалось запустить Kafka Consumer: %s", exc)

    yield

    logger.info("Остановка сервиса '%s'...", settings.app_name)
    await heartbeat_worker.stop()
    await consumer_service.stop()
    await kafka_manager.stop_producer()
    logger.info("Сервис '%s' успешно остановлен.", settings.app_name)


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

# Подключение роутеров адаптеров (Presentation Layer)
app.include_router(api_router)


if __name__ == "__main__":
    uvicorn.run(
        "src.main:app",
        host=settings.app_host,
        port=settings.app_port,
        log_level=settings.log_level.lower(),
        reload=True,
    )
