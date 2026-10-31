from functools import lru_cache

from fastapi import Depends

from src.application.use_cases.health_check import HealthCheckUseCase
from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.domain.ports.publisher import RecommendationPublisherPort
from src.domain.ports.strategy import StrategyPort
from src.infrastructure.config.settings import Settings, settings
from src.infrastructure.kafka.client import KafkaConnectionManager
from src.infrastructure.kafka.producer import KafkaPublisherAdapter
from src.infrastructure.strategy.donchian import DonchianStrategyAdapter


@lru_cache
def get_settings() -> Settings:
    return settings


@lru_cache
def get_strategy() -> StrategyPort:
    cfg = get_settings()
    return DonchianStrategyAdapter(window=cfg.strategy_window)


@lru_cache
def get_kafka_manager() -> KafkaConnectionManager:
    return KafkaConnectionManager(settings=get_settings())


@lru_cache
def get_publisher() -> KafkaPublisherAdapter:
    return KafkaPublisherAdapter(
        connection_manager=get_kafka_manager(),
        settings=get_settings(),
    )


def get_process_candle_use_case(
    strategy: StrategyPort = Depends(get_strategy),
    publisher: RecommendationPublisherPort = Depends(get_publisher),
) -> ProcessCandleUseCase:
    return ProcessCandleUseCase(strategy=strategy, publisher=publisher)


def get_health_use_case(
    cfg: Settings = Depends(get_settings),
) -> HealthCheckUseCase:
    return HealthCheckUseCase(
        service_name=cfg.app_name,
        service_version=cfg.app_version,
    )

