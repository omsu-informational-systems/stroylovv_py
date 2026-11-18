from decimal import Decimal
import json
from typing import Any

from src.application.commands.process_candle import ProcessCandleCommand
from src.infrastructure.mappers.event_mapper import parse_iso_timestamp


class CandleMapper:
    """
    Data Mapper для входящих сообщений свечей (market.candle.closed) из Kafka.

    Поддерживает стандартную обёртку платформы:
    - eventId, eventType, correlationId, occurredAt, payload
    - payload.instrument.ticker (или payload.ticker)
    - payload.runId, payload.sequenceNumber, payload.candleTime
    - десятичные строковые цены ("281.40") через Decimal
    """

    EXPECTED_EVENT_TYPE = "market.candle.closed"

    @staticmethod
    def extract_event_type(data: dict[str, Any]) -> str:
        return str(data.get("eventType", "")).strip()

    @staticmethod
    def parse_kafka_message(payload_bytes: bytes) -> ProcessCandleCommand:
        data = json.loads(payload_bytes.decode("utf-8"))
        return CandleMapper.from_dict(data)

    @staticmethod
    def parse_kafka_envelope(payload_bytes: bytes) -> tuple[str, ProcessCandleCommand | None]:
        """
        Разобрать сырое сообщение Kafka.

        Если eventType задан и отличается от market.candle.closed,
        возвращает (event_type, None) для безопасного пропуска сообщения.
        """
        data = json.loads(payload_bytes.decode("utf-8"))
        event_type = CandleMapper.extract_event_type(data)
        if event_type and event_type != CandleMapper.EXPECTED_EVENT_TYPE:
            return event_type, None
        return event_type or CandleMapper.EXPECTED_EVENT_TYPE, CandleMapper.from_dict(data)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> ProcessCandleCommand:
        # Извлекаем payload из обёртки платформы (с поддержкой legacy candle/data/плоского формата)
        if isinstance(data.get("payload"), dict):
            payload = data["payload"]
        elif isinstance(data.get("candle"), dict):
            payload = data["candle"]
        elif isinstance(data.get("data"), dict):
            payload = data["data"]
        else:
            payload = data

        # Извлекаем тикер из payload.instrument.ticker или payload.ticker
        instrument_raw = payload.get("instrument")
        if isinstance(instrument_raw, dict):
            ticker = str(instrument_raw.get("ticker", "")).strip().upper()
        elif isinstance(instrument_raw, str) and instrument_raw.strip():
            ticker = instrument_raw.strip().upper()
        else:
            ticker = str(payload.get("ticker", "")).strip().upper()

        # Время свечи: приоритет candleTime (историческое/рыночное время свечи), затем timestamp / occurredAt
        raw_ts = (
            payload.get("candleTime")
            or payload.get("timestamp")
            or data.get("occurredAt")
        )
        timestamp = parse_iso_timestamp(raw_ts)

        # Цены парсим через Decimal (в протоколе передаются десятичными строками, например "281.40")
        open_price = Decimal(str(payload["open"]).strip())
        high_price = Decimal(str(payload["high"]).strip())
        low_price = Decimal(str(payload["low"]).strip())
        close_price = Decimal(str(payload["close"]).strip())
        volume_raw = payload.get("volume", 0)
        volume = float(Decimal(str(volume_raw).strip())) if volume_raw is not None else 0.0

        seq_raw = payload.get("sequenceNumber")
        sequence_number = int(seq_raw) if seq_raw is not None else None

        run_id_raw = payload.get("runId")
        run_id = str(run_id_raw) if run_id_raw is not None else None

        event_id_raw = data.get("eventId")
        event_id = str(event_id_raw) if event_id_raw is not None else None

        correlation_id_raw = data.get("correlationId")
        correlation_id = str(correlation_id_raw) if correlation_id_raw is not None else None

        interval = str(payload.get("interval") or payload.get("timeframe") or "1d").strip()

        return ProcessCandleCommand(
            ticker=ticker,
            timestamp=timestamp,
            open=open_price,
            high=high_price,
            low=low_price,
            close=close_price,
            volume=volume,
            interval=interval,
            event_id=event_id,
            correlation_id=correlation_id,
            run_id=run_id,
            sequence_number=sequence_number,
        )
