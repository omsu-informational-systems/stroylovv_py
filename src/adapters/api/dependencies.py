from functools import lru_cache

from fastapi import Depends

from src.application.use_cases.health_check import HealthCheckUseCase
from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.application.use_cases.process_registration_result import (
    ProcessRegistrationResultUseCase,
)
from src.application.use_cases.register_service import RegisterServiceUseCase
from src.domain.entities.registration import ServiceRegistration
from src.domain.ports.publisher import RecommendationPublisherPort
from src.domain.ports.strategy import StrategyPort
from src.infrastructure.config.settings import Settings, settings
from src.infrastructure.kafka.client import KafkaConnectionManager
from src.infrastructure.kafka.producer import KafkaPublisherAdapter
from src.infrastructure.strategy.donchian import DonchianStrategyAdapter
from src.infrastructure.strategy.hybrid_trio import HybridTrioStrategyAdapter
from src.infrastructure.strategy.pairs_trading import PairsTradingStrategyAdapter


@lru_cache
def get_settings() -> Settings:
    return settings


@lru_cache
def get_service_registration() -> ServiceRegistration:
    cfg = get_settings()
    return ServiceRegistration(
        service_name=cfg.app_name,
        account_name=cfg.account_name,
        group_id=cfg.group_id,
        strategy_name=cfg.strategy_name,
    )


@lru_cache
def get_strategy() -> StrategyPort:
    cfg = get_settings()
    stype = cfg.strategy_type.lower()
    if stype == "hybrid":
        return HybridTrioStrategyAdapter(settings=cfg)
    if stype == "pairs":
        return PairsTradingStrategyAdapter(settings=cfg)
    return DonchianStrategyAdapter(settings=cfg)


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
    registration: ServiceRegistration = Depends(get_service_registration),
) -> ProcessCandleUseCase:
    return ProcessCandleUseCase(
        strategy=strategy,
        publisher=publisher,
        registration=registration,
    )


def get_process_registration_result_use_case(
    registration: ServiceRegistration = Depends(get_service_registration),
) -> ProcessRegistrationResultUseCase:
    return ProcessRegistrationResultUseCase(registration=registration)


def get_register_service_use_case(
    publisher: KafkaPublisherAdapter = Depends(get_publisher),
) -> RegisterServiceUseCase:
    return RegisterServiceUseCase(publisher=publisher)


def get_health_use_case(
    cfg: Settings = Depends(get_settings),
) -> HealthCheckUseCase:
    return HealthCheckUseCase(
        service_name=cfg.app_name,
        service_version=cfg.app_version,
    )
