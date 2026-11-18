from collections.abc import AsyncIterator
import logging

import uvicorn
from fastapi import FastAPI

from src.adapters.api.dependencies import (
    get_kafka_manager,
    get_process_candle_use_case,
    get_process_recommendation_result_use_case,
    get_process_registration_result_use_case,
    get_register_service_use_case,
    get_send_heartbeat_use_case,
    get_service_registration,
    get_settings,
)
from src.adapters.api.routers import api_router
from src.adapters.workers.heartbeat_worker import HeartbeatWorker
from src.infrastructure.kafka.consumer import KafkaConsumerService

settings = get_settings()

logging.basicConfig(
    level=settings.log_level.upper(),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Управление жизненным циклом сервиса (FastAPI Lifespan)."""
    logger.info(
        "Запуск сервиса '%s' v%s...",
        settings.effective_service_name,
        settings.app_version,
    )

    kafka_manager = get_kafka_manager()
    registration = get_service_registration()
    register_use_case = get_register_service_use_case()
    process_reg_result_use_case = get_process_registration_result_use_case()
    process_rec_result_use_case = get_process_recommendation_result_use_case()
    process_candle_use_case = get_process_candle_use_case()
    heartbeat_use_case = get_send_heartbeat_use_case()

    # 1. Подключение к защищённой Kafka (Producer + проверка TLS/SCRAM-SHA-256 метаданных)
    try:
        await kafka_manager.start_producer()
    except Exception as exc:
        logger.warning(
            "Не удалось инициализировать Kafka Producer (временная недоступность): %s",
            exc,
        )

    # 2. Запуск Kafka Consumer:
    #    - подписка answers-группы и ожидание назначения разделов ДО отправки регистрации
    #    - отправка account.registration.request и ожидание REGISTERED
    #    - после подтверждения: подписка candles-группы на CANDLE_TOPIC
    consumer_service = KafkaConsumerService(
        connection_manager=kafka_manager,
        process_candle_use_case=process_candle_use_case,
        settings=settings,
        registration=registration,
        register_service_use_case=register_use_case,
        process_registration_result_use_case=process_reg_result_use_case,
        process_recommendation_result_use_case=process_rec_result_use_case,
    )
    try:
        await consumer_service.start()
    except Exception as exc:
        logger.warning("Не удалось запустить Kafka Consumer: %s", exc)

    # 3. Запуск воркера периодического heartbeat (начнёт отправку после подтверждения регистрации)
    heartbeat_worker = HeartbeatWorker(
        use_case=heartbeat_use_case,
        interval_seconds=settings.heartbeat_interval_seconds,
    )
    heartbeat_worker.start()

    yield

    logger.info("Остановка сервиса '%s'...", settings.effective_service_name)
    await heartbeat_worker.stop()
    await consumer_service.stop()
    await kafka_manager.stop_producer()
    logger.info("Сервис '%s' успешно остановлен.", settings.effective_service_name)


app = FastAPI(
    title=settings.effective_service_name,
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
        reload=False,
    )
