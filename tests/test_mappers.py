from datetime import datetime, timezone
import json

from src.domain.entities.recommendation import TradeRecommendation
from src.domain.value_objects.bands import DonchianBands
from src.domain.value_objects.confidence import Confidence
from src.domain.value_objects.price import Price
from src.domain.value_objects.signal import Signal
from src.infrastructure.mappers.candle_mapper import CandleMapper
from src.infrastructure.mappers.event_mapper import EventMapper
from src.infrastructure.mappers.recommendation_mapper import RecommendationMapper


def test_candle_mapper_parse_kafka_json() -> None:
    raw_json = json.dumps({
        "eventType": "market.candle.closed",
        "ticker": "gazp",
        "timestamp": "2026-10-14T10:00:00Z",
        "open": 105.79,
        "high": 105.80,
        "low": 105.78,
        "close": 105.80,
        "volume": 250000.0,
        "interval": "1d",
    }).encode("utf-8")

    cmd = CandleMapper.parse_kafka_message(raw_json)
    assert cmd.ticker == "GAZP"
    assert cmd.close == 105.80
    assert cmd.high == 105.80
    assert cmd.volume == 250000.0


def test_recommendation_mapper() -> None:
    now = datetime.now(timezone.utc)
    rec = TradeRecommendation(
        ticker="SBER",
        timestamp=now,
        signal=Signal.BUY,
        close=Price(250.0),
        bands=DonchianBands(upper=248.0, lower=230.0),
        confidence=Confidence(0.85),
    )

    result_json = RecommendationMapper.to_kafka_json(
        recommendation=rec,
        service_name="breakout-strategy",
        secret_key="student-key-05",
    )
    parsed = json.loads(result_json)
    assert parsed["eventType"] == "strategy.recommendation.created"
    assert parsed["secretKey"] == "student-key-05"
    assert parsed["serviceName"] == "breakout-strategy"
    assert parsed["ticker"] == "SBER"
    assert parsed["signal"] == "BUY"
    assert parsed["upperBand"] == 248.0
    assert parsed["confidence"] == 0.85


def test_event_mapper() -> None:
    heartbeat_json = EventMapper.to_heartbeat_json("my-svc", "student-key-05")
    parsed_hb = json.loads(heartbeat_json)
    assert parsed_hb["eventType"] == "account.service.heartbeat"
    assert parsed_hb["secretKey"] == "student-key-05"

    reg_json = EventMapper.to_registration_json("my-svc", "student-key-05")
    parsed_reg = json.loads(reg_json)
    assert parsed_reg["eventType"] == "account.registration.request"
    assert parsed_reg["secretKey"] == "student-key-05"

