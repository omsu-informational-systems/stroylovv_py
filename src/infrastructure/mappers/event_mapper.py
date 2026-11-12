from datetime import datetime, timezone
import json
from typing import Any


class EventMapper:
    """Маппер служебных событий платформы (регистрация, heartbeat, результат регистрации)."""

    @staticmethod
    def to_heartbeat_json(
        service_name: str,
        secret_key: str,
        schema_version: str = "1.0.0",
    ) -> str:
        payload = {
            "schemaVersion": schema_version,
            "eventType": "account.service.heartbeat",
            "secretKey": secret_key,
            "serviceName": service_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def to_registration_json(
        service_name: str,
        secret_key: str,
        account_name: str = "",
        group_id: str = "",
        strategy_name: str = "",
        schema_version: str = "1.0.0",
    ) -> str:
        payload = {
            "schemaVersion": schema_version,
            "eventType": "account.registration.request",
            "secretKey": secret_key,
            "accountName": account_name,
            "groupId": group_id,
            "serviceName": service_name,
            "strategyName": strategy_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def parse_registration_result(payload_bytes: bytes) -> tuple[str, str, bool, str, datetime]:
        """
        Десериализация сообщения account.registration.result от платформы.

        Поддерживает как обертку event/payload платформы, так и плоский JSON.
        Возвращает: (service_name, secret_key, success, reason, timestamp)
        """
        data = json.loads(payload_bytes.decode("utf-8"))
        inner = data.get("payload") if isinstance(data.get("payload"), dict) else data

        service_name = str(inner.get("serviceName", data.get("serviceName", ""))).strip()
        secret_key = str(inner.get("secretKey", data.get("secretKey", ""))).strip()

        raw_success = inner.get("success", data.get("success"))
        if isinstance(raw_success, bool):
            success = raw_success
        else:
            raw_status = str(inner.get("status", data.get("status", ""))).strip().upper()
            success = raw_status in ("REGISTERED", "SUCCESS", "OK", "APPROVED", "CONFIRMED")

        reason = str(
            inner.get(
                "reason",
                inner.get("message", inner.get("error", data.get("reason", ""))),
            )
        ).strip()

        raw_ts = data.get("occurredAt") or data.get("timestamp") or inner.get("timestamp")
        if isinstance(raw_ts, str):
            try:
                clean_ts = raw_ts.replace("Z", "+00:00")
                if "." in clean_ts:
                    prefix, rest = clean_ts.split(".", 1)
                    if "+" in rest:
                        frac, tz = rest.split("+", 1)
                        clean_ts = f"{prefix}.{frac[:6]}+{tz}"
                    else:
                        clean_ts = f"{prefix}.{rest[:6]}"
                ts = datetime.fromisoformat(clean_ts)
            except ValueError:
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        return service_name, secret_key, success, reason, ts
