from datetime import datetime, timezone
import json
from typing import Any
import uuid

from src.application.commands.process_recommendation_result import (
    ProcessRecommendationResultCommand,
)
from src.application.commands.process_registration_result import (
    ProcessRegistrationResultCommand,
)
from src.domain.entities.registration import ServiceRegistration


def utc_now_iso() -> str:
    """Текущее время UTC в формате ISO 8601 с суффиксом Z."""
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def format_utc_iso(dt: datetime) -> str:
    """Форматировать datetime в ISO 8601 UTC с суффиксом Z."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def parse_iso_timestamp(raw_ts: Any) -> datetime:
    """Надёжный разбор временной метки ISO 8601 (включая Z и наносекунды) или epoch."""
    if isinstance(raw_ts, str) and raw_ts.strip():
        try:
            clean_ts = raw_ts.strip().replace("Z", "+00:00")
            if "." in clean_ts:
                prefix, rest = clean_ts.split(".", 1)
                if "+" in rest:
                    frac, tz = rest.split("+", 1)
                    clean_ts = f"{prefix}.{frac[:6]}+{tz}"
                elif "-" in rest:
                    frac, tz = rest.split("-", 1)
                    clean_ts = f"{prefix}.{frac[:6]}-{tz}"
                else:
                    clean_ts = f"{prefix}.{rest[:6]}"
            parsed = datetime.fromisoformat(clean_ts)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            return datetime.now(timezone.utc)
    if isinstance(raw_ts, (int, float)):
        return datetime.fromtimestamp(raw_ts, tz=timezone.utc)
    return datetime.now(timezone.utc)


def build_envelope(
    *,
    event_type: str,
    producer: str,
    payload: dict[str, Any],
    schema_version: str = "1.0",
    correlation_id: str | None = None,
    causation_id: str | None = None,
    event_id: str | None = None,
) -> dict[str, Any]:
    """Сформировать стандартную JSON-обёртку сообщения платформы."""
    eid = event_id or str(uuid.uuid4())
    return {
        "eventId": eid,
        "eventType": event_type,
        "schemaVersion": schema_version,
        "occurredAt": utc_now_iso(),
        "correlationId": correlation_id or eid,
        "causationId": causation_id,
        "producer": producer,
        "payload": payload,
    }


class EventMapper:
    """
    Data Mapper для служебных событий платформы:
    - account.registration.request
    - account.registration.result
    - account.service.heartbeat
    - strategy.recommendation.result
    """

    @staticmethod
    def to_registration_envelope(
        registration: ServiceRegistration,
        secret_key: str,
        schema_version: str = "1.0",
    ) -> tuple[str, str]:
        """
        Сформировать событие account.registration.request в стандартной обёртке.

        Возвращает кортеж (event_id, json_str).
        """
        payload: dict[str, Any] = {
            "secretKey": secret_key,
            "accountId": registration.account_id,
            "accountName": registration.account_name,
            "groupId": registration.group_id,
            "strategyId": registration.strategy_id,
            "strategyName": registration.strategy_name,
            "serviceName": registration.service_name,
        }
        if registration.description:
            payload["description"] = registration.description

        envelope = build_envelope(
            event_type="account.registration.request",
            producer=registration.service_name,
            payload=payload,
            schema_version=schema_version,
        )
        return str(envelope["eventId"]), json.dumps(envelope, ensure_ascii=False)

    @staticmethod
    def to_heartbeat_json(
        registration: ServiceRegistration,
        secret_key: str,
        app_version: str = "1.0.0",
        schema_version: str = "1.0",
    ) -> str:
        """
        Сформировать событие account.service.heartbeat в стандартной обёртке.

        В metadata передаются только несекретные сведения (версия приложения).
        """
        payload: dict[str, Any] = {
            "secretKey": secret_key,
            "accountId": registration.account_id,
            "groupId": registration.group_id,
            "strategyId": registration.strategy_id,
            "serviceName": registration.service_name,
            "instanceId": registration.instance_id,
            "status": "UP",
            "startedAt": format_utc_iso(registration.started_at),
            "sentAt": utc_now_iso(),
            "processedCandles": registration.processed_candles,
            "publishedRecommendations": registration.published_recommendations,
            "lastProcessedSequenceNumber": registration.last_processed_sequence_number,
            "metadata": {"appVersion": app_version},
        }
        envelope = build_envelope(
            event_type="account.service.heartbeat",
            producer=registration.service_name,
            payload=payload,
            schema_version=schema_version,
        )
        return json.dumps(envelope, ensure_ascii=False)

    @staticmethod
    def parse_registration_result(payload_bytes: bytes) -> ProcessRegistrationResultCommand:
        """
        Десериализация сообщения account.registration.result от платформы.

        Поддерживает стандартную обёртку платформы (с causationId и payload)
        и обратную совместимость с плоским JSON.
        """
        data = json.loads(payload_bytes.decode("utf-8"))
        inner = data.get("payload") if isinstance(data.get("payload"), dict) else data

        causation_id = data.get("causationId")
        service_name = str(inner.get("serviceName", data.get("serviceName", ""))).strip()
        account_id = str(inner.get("accountId", data.get("accountId", ""))).strip()
        group_id = str(inner.get("groupId", data.get("groupId", ""))).strip()
        strategy_id = str(inner.get("strategyId", data.get("strategyId", ""))).strip()
        secret_key = str(inner.get("secretKey", data.get("secretKey", ""))).strip()

        raw_status = str(inner.get("status", data.get("status", ""))).strip().upper()
        raw_success = inner.get("success", data.get("success"))
        if raw_status:
            success = raw_status == "REGISTERED"
        elif isinstance(raw_success, bool):
            success = raw_success
            raw_status = "REGISTERED" if success else "REJECTED"
        else:
            success = False
            raw_status = "REJECTED"

        reason = str(
            inner.get(
                "reason",
                inner.get("message", inner.get("error", data.get("reason", ""))),
            )
        ).strip()

        raw_ts = data.get("occurredAt") or data.get("timestamp") or inner.get("timestamp")
        ts = parse_iso_timestamp(raw_ts)

        return ProcessRegistrationResultCommand(
            service_name=service_name,
            account_id=account_id,
            group_id=group_id,
            strategy_id=strategy_id,
            causation_id=str(causation_id) if causation_id is not None else None,
            secret_key=secret_key,
            status=raw_status,
            success=success,
            reason=reason,
            timestamp=ts,
        )

    @staticmethod
    def parse_recommendation_result(payload_bytes: bytes) -> ProcessRecommendationResultCommand:
        """Десериализация сообщения strategy.recommendation.result от платформы."""
        data = json.loads(payload_bytes.decode("utf-8"))
        inner = data.get("payload") if isinstance(data.get("payload"), dict) else data

        instrument_raw = inner.get("instrument", "")
        if isinstance(instrument_raw, dict):
            instrument = str(instrument_raw.get("ticker", "")).strip().upper()
        else:
            instrument = str(instrument_raw).strip().upper()

        account_execution = inner.get("accountExecution")
        execution_status = ""
        reason = ""
        if isinstance(account_execution, dict):
            execution_status = str(account_execution.get("status", "")).strip()
            reason = str(account_execution.get("reason", "")).strip()
        if not execution_status:
            execution_status = str(inner.get("status", "")).strip()
        if not reason:
            reason = str(inner.get("reason", "")).strip()

        seq_raw = inner.get("sequenceNumber")
        seq = int(seq_raw) if seq_raw is not None else None

        causation_id = data.get("causationId")
        raw_ts = data.get("occurredAt") or data.get("timestamp")

        return ProcessRecommendationResultCommand(
            account_id=str(inner.get("accountId", "")).strip(),
            strategy_id=str(inner.get("strategyId", "")).strip(),
            instrument=instrument,
            sequence_number=seq,
            recommendation_id=str(inner.get("recommendationId", "")).strip(),
            causation_id=str(causation_id) if causation_id is not None else None,
            execution_status=execution_status,
            reason=reason,
            timestamp=parse_iso_timestamp(raw_ts),
        )
