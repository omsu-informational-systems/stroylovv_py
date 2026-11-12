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
    def parse_registration_result(payload_bytes: bytes) -> tuple[str, bool, str, datetime]:
        """
        Десериализация сообщения account.registration.result от платформы.

        Возвращает: (service_name, success, reason, timestamp)
        """
        data = json.loads(payload_bytes.decode("utf-8"))
        service_name = str(data.get("serviceName", "")).strip()

        raw_success = data.get("success")
        if isinstance(raw_success, bool):
            success = raw_success
        else:
            raw_status = str(data.get("status", "")).strip().upper()
            success = raw_status in ("SUCCESS", "OK", "APPROVED", "CONFIRMED")

        reason = str(data.get("reason", data.get("message", data.get("error", "")))).strip()

        raw_ts = data.get("timestamp")
        if isinstance(raw_ts, str):
            try:
                ts = datetime.fromisoformat(raw_ts.replace("Z", "+00:00"))
            except ValueError:
                ts = datetime.now(timezone.utc)
        else:
            ts = datetime.now(timezone.utc)

        return service_name, success, reason, ts
