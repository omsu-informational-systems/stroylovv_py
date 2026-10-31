from datetime import datetime, timezone
import json
from typing import Any


class EventMapper:
    """Маппер служебных событий платформы (регистрация, heartbeat)."""

    @staticmethod
    def to_heartbeat_json(service_name: str, secret_key: str) -> str:
        payload = {
            "eventType": "account.service.heartbeat",
            "secretKey": secret_key,
            "serviceName": service_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return json.dumps(payload, ensure_ascii=False)

    @staticmethod
    def to_registration_json(service_name: str, secret_key: str) -> str:
        payload = {
            "eventType": "account.registration.request",
            "secretKey": secret_key,
            "serviceName": service_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        return json.dumps(payload, ensure_ascii=False)

