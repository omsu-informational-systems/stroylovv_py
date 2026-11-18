from functools import lru_cache

from fastapi import Depends

from src.application.use_cases.health_check import HealthCheckUseCase
from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.application.use_cases.process_recommendation_result import (
    ProcessRecommendationResultUseCase,
)
from src.application.use_cases.process_registration_result import (
    ProcessRegistrationResultUseCase,
)
from src.application.use_cases.register_service import RegisterServiceUseCase
from src.application.use_cases.send_heartbeat import SendHeartbeatUseCase
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
        service_name=cfg.effective_service_name,
        account_id=cfg.account_id,
        account_name=cfg.account_name,
        group_id=cfg.group_id,
        strategy_id=cfg.strategy_id,
        strategy_name=cfg.strategy_name,
        description=cfg.service_description,
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
        registration=get_service_registration(),
    )


def get_process_candle_use_case(
    strategy: StrategyPort = Depends(get_strategy),
    publisher: RecommendationPublisherPort = Depends(get_publisher),
    registration: ServiceRegistration = Depends(get_service_registration),
) -> ProcessCandleUseCase:
    cfg = get_settings()
    reg = (
        registration
        if isinstance(registration, ServiceRegistration)
        else get_service_registration()
    )
    strat = strategy if isinstance(strategy, StrategyPort) else get_strategy()
    pub = publisher if isinstance(publisher, RecommendationPublisherPort) else get_publisher()
    return ProcessCandleUseCase(
        strategy=strat,
        publisher=pub,
        registration=reg,
        default_order_quantity=cfg.default_order_quantity,
    )


def get_process_registration_result_use_case(
    registration: ServiceRegistration = Depends(get_service_registration),
) -> ProcessRegistrationResultUseCase:
    reg = (
        registration
        if isinstance(registration, ServiceRegistration)
        else get_service_registration()
    )
    cfg = get_settings()
    return ProcessRegistrationResultUseCase(
        registration=reg,
        expected_secret_key=cfg.effective_secret_key,
    )


def get_process_recommendation_result_use_case(
    registration: ServiceRegistration = Depends(get_service_registration),
) -> ProcessRecommendationResultUseCase:
    reg = (
        registration
        if isinstance(registration, ServiceRegistration)
        else get_service_registration()
    )
    return ProcessRecommendationResultUseCase(registration=reg)


def get_register_service_use_case(
    publisher: KafkaPublisherAdapter = Depends(get_publisher),
    registration: ServiceRegistration = Depends(get_service_registration),
) -> RegisterServiceUseCase:
    pub = publisher if isinstance(publisher, KafkaPublisherAdapter) else get_publisher()
    reg = (
        registration
        if isinstance(registration, ServiceRegistration)
        else get_service_registration()
    )
    return RegisterServiceUseCase(publisher=pub, registration=reg)


def get_send_heartbeat_use_case(
    publisher: KafkaPublisherAdapter = Depends(get_publisher),
    registration: ServiceRegistration = Depends(get_service_registration),
) -> SendHeartbeatUseCase:
    pub = publisher if isinstance(publisher, KafkaPublisherAdapter) else get_publisher()
    reg = (
        registration
        if isinstance(registration, ServiceRegistration)
        else get_service_registration()
    )
    return SendHeartbeatUseCase(publisher=pub, registration=reg)


def get_health_use_case(
    cfg: Settings = Depends(get_settings),
) -> HealthCheckUseCase:
    conf = cfg if isinstance(cfg, Settings) else get_settings()
    return HealthCheckUseCase(
        service_name=conf.effective_service_name,
        service_version=conf.app_version,
    )
