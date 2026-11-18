from datetime import datetime, timezone
from decimal import Decimal
import json

from src.domain.entities.recommendation import TradeRecommendation
from src.domain.entities.registration import ServiceRegistration
from src.domain.value_objects.bands import DonchianBands
from src.domain.value_objects.confidence import Confidence
from src.domain.value_objects.price import Price
from src.domain.value_objects.signal import Signal
from src.infrastructure.mappers.candle_mapper import CandleMapper
from src.infrastructure.mappers.event_mapper import EventMapper
from src.infrastructure.mappers.recommendation_mapper import RecommendationMapper


def _sample_registration() -> ServiceRegistration:
    return ServiceRegistration(
        service_name="strategy-stroylov",
        account_id="stroylov-v",
        account_name="Виталий Стройлов",
        group_id="МИБ-401",
        strategy_id="hybrid-breakout-stroylov",
        strategy_name="Hybrid Breakout",
        description="Проверка обмена с платформой",
    )


def test_candle_mapper_parse_platform_envelope() -> None:
    raw_json = json.dumps({
        "eventId": "595a23b4-9eb0-4589-850b-0d775b03dabc",
        "eventType": "market.candle.closed",
        "schemaVersion": "1.0",
        "occurredAt": "2026-10-14T08:00:00Z",
        "correlationId": "run-example-SBER-000051",
        "causationId": None,
        "producer": "market-data-loader",
        "payload": {
            "runId": "run-001",
            "instrument": {"ticker": "sber"},
            "sequenceNumber": 51,
            "candleTime": "2024-05-10T10:00:00Z",
            "open": "281.40",
            "high": "283.00",
            "low": "280.10",
            "close": "282.50",
            "volume": 250000,
            "interval": "1d",
        },
    }).encode("utf-8")

    event_type, cmd = CandleMapper.parse_kafka_envelope(raw_json)
    assert event_type == "market.candle.closed"
    assert cmd is not None
    assert cmd.ticker == "SBER"
    assert cmd.event_id == "595a23b4-9eb0-4589-850b-0d775b03dabc"
    assert cmd.correlation_id == "run-example-SBER-000051"
    assert cmd.run_id == "run-001"
    assert cmd.sequence_number == 51
    assert cmd.open == Decimal("281.40")
    assert cmd.close == Decimal("282.50")
    assert cmd.volume == 250000.0
    assert cmd.timestamp == datetime(2024, 5, 10, 10, 0, 0, tzinfo=timezone.utc)


def test_candle_mapper_skips_non_candle_event_type() -> None:
    raw_json = json.dumps({
        "eventId": "evt-other",
        "eventType": "market.run.started",
        "schemaVersion": "1.0",
        "payload": {},
    }).encode("utf-8")

    event_type, cmd = CandleMapper.parse_kafka_envelope(raw_json)
    assert event_type == "market.run.started"
    assert cmd is None


def test_recommendation_mapper_platform_envelope() -> None:
    now = datetime.now(timezone.utc)
    reg = _sample_registration()
    rec = TradeRecommendation(
        ticker="SBER",
        timestamp=now,
        signal=Signal.BUY,
        close=Price("282.50"),
        bands=DonchianBands(upper=280.0, lower=270.0),
        confidence=Confidence(0.85),
    )
    rec.enrich_context(
        source_event_id="candle-evt-1",
        correlation_id="run-example-SBER-000051",
        run_id="run-001",
        sequence_number=51,
        processing_time_ms=2,
        default_order_quantity=3,
    )

    result_json = RecommendationMapper.to_kafka_json(
        recommendation=rec,
        registration=reg,
        secret_key="EXAMPLE-KEY",
        schema_version="1.0",
    )
    parsed = json.loads(result_json)

    # Проверка общей JSON-обёртки
    assert parsed["eventId"] != "candle-evt-1"
    assert parsed["eventType"] == "strategy.recommendation.created"
    assert parsed["schemaVersion"] == "1.0"
    assert parsed["occurredAt"].endswith("Z")
    assert parsed["correlationId"] == "run-example-SBER-000051"
    assert parsed["causationId"] == "candle-evt-1"
    assert parsed["producer"] == "strategy-stroylov"

    # Проверка payload рекомендации
    payload = parsed["payload"]
    assert payload["recommendationId"] == rec.recommendation_id
    assert payload["sourceEventId"] == "candle-evt-1"
    assert payload["runId"] == "run-001"
    assert payload["secretKey"] == "EXAMPLE-KEY"
    assert payload["accountId"] == "stroylov-v"
    assert payload["groupId"] == "МИБ-401"
    assert payload["strategyId"] == "hybrid-breakout-stroylov"
    assert payload["instrument"] == "SBER"
    assert payload["sequenceNumber"] == 51
    assert payload["signal"] == "BUY"
    assert payload["confidence"] == 0.85
    assert payload["suggestedQuantity"] == 3
    assert payload["dataQuality"] == "OK"
    assert payload["processingTimeMs"] == 2
    assert payload["metadata"]["close"] == "282.50"
    assert "secret" not in json.dumps(payload["metadata"]).lower()


def test_event_mapper_registration_and_heartbeat_envelopes() -> None:
    reg = _sample_registration()
    reg.mark_candle_processed(event_id="evt-1", sequence_number=51, published=True)

    # 1. Запрос регистрации
    event_id, reg_json = EventMapper.to_registration_envelope(
        registration=reg,
        secret_key="EXAMPLE-KEY",
        schema_version="1.0",
    )
    parsed_reg = json.loads(reg_json)
    assert parsed_reg["eventId"] == event_id
    assert parsed_reg["eventType"] == "account.registration.request"
    assert parsed_reg["schemaVersion"] == "1.0"
    assert parsed_reg["occurredAt"].endswith("Z")
    assert parsed_reg["correlationId"] == event_id
    assert parsed_reg["causationId"] is None
    assert parsed_reg["producer"] == "strategy-stroylov"

    reg_payload = parsed_reg["payload"]
    assert reg_payload["secretKey"] == "EXAMPLE-KEY"
    assert reg_payload["accountId"] == "stroylov-v"
    assert reg_payload["accountName"] == "Виталий Стройлов"
    assert reg_payload["groupId"] == "МИБ-401"
    assert reg_payload["strategyId"] == "hybrid-breakout-stroylov"
    assert reg_payload["strategyName"] == "Hybrid Breakout"
    assert reg_payload["serviceName"] == "strategy-stroylov"
    assert reg_payload["description"] == "Проверка обмена с платформой"

    # 2. Heartbeat
    hb_json = EventMapper.to_heartbeat_json(
        registration=reg,
        secret_key="EXAMPLE-KEY",
        app_version="1.0.0",
        schema_version="1.0",
    )
    parsed_hb = json.loads(hb_json)
    assert parsed_hb["eventType"] == "account.service.heartbeat"
    assert parsed_hb["schemaVersion"] == "1.0"
    assert parsed_hb["occurredAt"].endswith("Z")
    assert parsed_hb["producer"] == "strategy-stroylov"

    hb_payload = parsed_hb["payload"]
    assert hb_payload["secretKey"] == "EXAMPLE-KEY"
    assert hb_payload["accountId"] == "stroylov-v"
    assert hb_payload["groupId"] == "МИБ-401"
    assert hb_payload["strategyId"] == "hybrid-breakout-stroylov"
    assert hb_payload["serviceName"] == "strategy-stroylov"
    assert hb_payload["instanceId"] == reg.instance_id
    assert hb_payload["status"] == "UP"
    assert hb_payload["startedAt"].endswith("Z")
    assert hb_payload["sentAt"].endswith("Z")
    assert hb_payload["processedCandles"] == 1
    assert hb_payload["publishedRecommendations"] == 1
    assert hb_payload["lastProcessedSequenceNumber"] == 51
    assert hb_payload["metadata"] == {"appVersion": "1.0.0"}


def test_event_mapper_parse_registration_and_recommendation_results() -> None:
    approved_bytes = json.dumps({
        "eventId": "res-1",
        "eventType": "account.registration.result",
        "schemaVersion": "1.0",
        "occurredAt": "2026-10-28T13:43:22.960822Z",
        "correlationId": "req-1",
        "causationId": "req-1",
        "producer": "platform",
        "payload": {
            "secretKey": "EXAMPLE-KEY",
            "accountId": "stroylov-v",
            "groupId": "МИБ-401",
            "strategyId": "hybrid-breakout-stroylov",
            "serviceName": "strategy-stroylov",
            "status": "REGISTERED",
            "reason": "Account service has been registered.",
        },
    }).encode("utf-8")

    cmd_ok = EventMapper.parse_registration_result(approved_bytes)
    assert cmd_ok.causation_id == "req-1"
    assert cmd_ok.account_id == "stroylov-v"
    assert cmd_ok.service_name == "strategy-stroylov"
    assert cmd_ok.status == "REGISTERED"
    assert cmd_ok.success is True

    rejected_bytes = json.dumps({
        "eventId": "res-2",
        "eventType": "account.registration.result",
        "schemaVersion": "1.0",
        "causationId": "req-2",
        "payload": {
            "secretKey": "EXAMPLE-KEY",
            "accountId": "stroylov-v",
            "serviceName": "strategy-stroylov",
            "status": "REJECTED",
            "reason": "Unknown registration key",
        },
    }).encode("utf-8")

    cmd_rej = EventMapper.parse_registration_result(rejected_bytes)
    assert cmd_rej.success is False
    assert cmd_rej.status == "REJECTED"
    assert cmd_rej.reason == "Unknown registration key"

    rec_result_bytes = json.dumps({
        "eventId": "rec-res-1",
        "eventType": "strategy.recommendation.result",
        "schemaVersion": "1.0",
        "occurredAt": "2026-10-28T13:45:00Z",
        "causationId": "rec-evt-1",
        "payload": {
            "accountId": "stroylov-v",
            "strategyId": "hybrid-breakout-stroylov",
            "instrument": "SBER",
            "sequenceNumber": 51,
            "recommendationId": "rec-uuid-1",
            "accountExecution": {"status": "EXECUTED"},
        },
    }).encode("utf-8")

    rec_cmd = EventMapper.parse_recommendation_result(rec_result_bytes)
    assert rec_cmd.account_id == "stroylov-v"
    assert rec_cmd.strategy_id == "hybrid-breakout-stroylov"
    assert rec_cmd.instrument == "SBER"
    assert rec_cmd.sequence_number == 51
    assert rec_cmd.recommendation_id == "rec-uuid-1"
    assert rec_cmd.execution_status == "EXECUTED"
