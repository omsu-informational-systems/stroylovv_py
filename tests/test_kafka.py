from datetime import datetime, timezone
from decimal import Decimal
import json
import pytest

from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.entities.registration import ServiceRegistration
from src.infrastructure.config.settings import Settings
from src.infrastructure.kafka.client import KafkaConnectionManager
from src.infrastructure.kafka.consumer import KafkaConsumerService
from src.infrastructure.strategy.donchian import DonchianStrategyAdapter


def test_kafka_security_config_matches_platform_spec() -> None:
    cfg = Settings(
        _env_file=None,
        kafka_bootstrap_servers="omsu.7bits.it:9094",
        kafka_username="example-key",
        kafka_password="example-secret-password",
        kafka_group_id="strategy-stroylov-v1",
        kafka_ca_location="/etc/ssl/certs/ca-certificates.crt",
    )
    manager = KafkaConnectionManager(settings=cfg)

    producer_cfg = manager.build_producer_config()
    assert producer_cfg["bootstrap.servers"] == "omsu.7bits.it:9094"
    assert producer_cfg["security.protocol"] == "SASL_SSL"
    assert producer_cfg["sasl.mechanism"] == "SCRAM-SHA-256"
    assert producer_cfg["sasl.username"] == "example-key"
    assert producer_cfg["sasl.password"] == "example-secret-password"
    assert producer_cfg["enable.ssl.certificate.verification"] is True
    assert producer_cfg["ssl.endpoint.identification.algorithm"] == "https"
    assert producer_cfg["ssl.ca.location"] == "/etc/ssl/certs/ca-certificates.crt"
    assert producer_cfg["enable.idempotence"] is True

    answers_cfg = manager.build_consumer_config(cfg.kafka_answers_group_id)
    candles_cfg = manager.build_consumer_config(cfg.kafka_candles_group_id)
    assert answers_cfg["group.id"] == "strategy-stroylov-v1.answers"
    assert candles_cfg["group.id"] == "strategy-stroylov-v1.candles"
    assert answers_cfg["auto.offset.reset"] == "earliest"
    assert answers_cfg["enable.auto.commit"] is False
    assert answers_cfg["enable.auto.offset.store"] is False
    assert answers_cfg["allow.auto.create.topics"] is False

    # Секреты и ключи никогда не попадают в repr
    rep = repr(cfg)
    assert "example-secret-password" not in rep
    assert "example-key" not in rep

    # effective_secret_key автоматически совпадает с kafka_username
    assert cfg.effective_secret_key == "example-key"

    # Без явного KAFKA_CA_LOCATION автоматически используется доверенный набор certifi
    cfg_default_ca = Settings(
        _env_file=None,
        kafka_username="example-key",
        kafka_password="example-secret-password",
    )
    default_base = KafkaConnectionManager(settings=cfg_default_ca).build_base_config()
    assert default_base["enable.ssl.certificate.verification"] is True
    assert default_base["ssl.endpoint.identification.algorithm"] == "https"
    assert "ssl.ca.location" in default_base
    assert default_base["ssl.ca.location"].endswith("cacert.pem")


class _FakeKafkaMessage:
    def __init__(self, topic: str, payload: dict, offset: int = 1, partition: int = 0) -> None:
        self._topic = topic
        self._raw = json.dumps(payload).encode("utf-8")
        self._offset = offset
        self._partition = partition

    def topic(self) -> str:
        return self._topic

    def value(self) -> bytes:
        return self._raw

    def offset(self) -> int:
        return self._offset

    def partition(self) -> int:
        return self._partition

    def error(self):
        return None


class _FakeConsumer:
    def __init__(self) -> None:
        self.committed: list[tuple[int, bool]] = []

    def commit(self, *, message, asynchronous: bool = True) -> None:
        self.committed.append((message.offset(), asynchronous))


@pytest.mark.asyncio
async def test_candle_offset_not_committed_when_publish_fails() -> None:
    class FailingPublisher:
        async def publish_recommendation(self, rec: TradeRecommendation) -> None:
            raise RuntimeError("Broker delivery not confirmed")

    cfg = Settings(kafka_username="k", kafka_password="p")
    reg = ServiceRegistration(service_name="test-svc", account_id="acc-1")
    reg.activate(datetime.now(timezone.utc))

    use_case = ProcessCandleUseCase(
        strategy=DonchianStrategyAdapter(window=2),
        publisher=FailingPublisher(),  # type: ignore[arg-type]
        registration=reg,
    )
    service = KafkaConsumerService(
        connection_manager=KafkaConnectionManager(cfg),
        process_candle_use_case=use_case,
        settings=cfg,
        registration=reg,
    )

    fake_consumer = _FakeConsumer()
    candle_msg = _FakeKafkaMessage(
        topic="market.candle.default",
        offset=42,
        payload={
            "eventId": "evt-42",
            "eventType": "market.candle.closed",
            "schemaVersion": "1.0",
            "correlationId": "corr-42",
            "payload": {
                "runId": "run-1",
                "instrument": {"ticker": "SBER"},
                "sequenceNumber": 42,
                "candleTime": "2026-10-14T10:00:00Z",
                "open": "280.00",
                "high": "282.00",
                "low": "279.00",
                "close": "281.50",
                "volume": 1000,
            },
        },
    )

    with pytest.raises(RuntimeError, match="Broker delivery not confirmed"):
        await service._handle_candle_message(fake_consumer, candle_msg)

    # Смещение НЕ должно быть зафиксировано при ошибке публикации!
    assert fake_consumer.committed == []


@pytest.mark.asyncio
async def test_candle_offset_committed_synchronously_after_confirmed_publish() -> None:
    published: list[TradeRecommendation] = []

    class OkPublisher:
        async def publish_recommendation(self, rec: TradeRecommendation) -> None:
            published.append(rec)

    cfg = Settings(kafka_username="k", kafka_password="p")
    reg = ServiceRegistration(service_name="test-svc", account_id="acc-1")
    reg.activate(datetime.now(timezone.utc))

    use_case = ProcessCandleUseCase(
        strategy=DonchianStrategyAdapter(window=2),
        publisher=OkPublisher(),  # type: ignore[arg-type]
        registration=reg,
    )
    service = KafkaConsumerService(
        connection_manager=KafkaConnectionManager(cfg),
        process_candle_use_case=use_case,
        settings=cfg,
        registration=reg,
    )

    fake_consumer = _FakeConsumer()
    candle_msg = _FakeKafkaMessage(
        topic="market.candle.default",
        offset=77,
        payload={
            "eventId": "evt-77",
            "eventType": "market.candle.closed",
            "schemaVersion": "1.0",
            "correlationId": "corr-77",
            "payload": {
                "runId": "run-1",
                "instrument": {"ticker": "SBER"},
                "sequenceNumber": 77,
                "candleTime": "2026-10-14T10:00:00Z",
                "open": "280.00",
                "high": "282.00",
                "low": "279.00",
                "close": "281.50",
                "volume": 1000,
            },
        },
    )

    await service._handle_candle_message(fake_consumer, candle_msg)

    assert len(published) == 1
    assert published[0].close.decimal_value == Decimal("281.50")
    assert fake_consumer.committed == [(77, False)]
