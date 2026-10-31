from datetime import datetime, timezone
import json
from typing import Any

from src.application.commands.process_candle import ProcessCandleCommand


class CandleMapper:
    """Маппер для входящих сообщений свечей из Kafka."""

    @staticmethod
    def parse_kafka_message(payload_bytes: bytes) -> ProcessCandleCommand:
        data = json.loads(payload_bytes.decode("utf-8"))
        return CandleMapper.from_dict(data)

    @staticmethod
    def from_dict(data: dict[str, Any]) -> ProcessCandleCommand:
        # Извлекаем вложенный объект, если свеча завернута в поле 'candle' или 'data'
        candle_data = data.get("candle", data.get("data", data))

        raw_ts = candle_data.get("timestamp")
        if isinstance(raw_ts, str):
            # Поддержка формата ISO 8601
            try:
                timestamp = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            except ValueError:
                timestamp = datetime.now(timezone.utc)
        elif isinstance(raw_ts, (int, float)):
            timestamp = datetime.fromtimestamp(raw_ts, tz=timezone.utc)
        else:
            timestamp = datetime.now(timezone.utc)

        return ProcessCandleCommand(
            ticker=str(candle_data.get("ticker", "")).strip().upper(),
            timestamp=timestamp,
            open=float(candle_data["open"]),
            high=float(candle_data["high"]),
            low=float(candle_data["low"]),
            close=float(candle_data["close"]),
            volume=float(candle_data.get("volume", 0.0)),
            interval=str(candle_data.get("interval", "1d")),
        )

