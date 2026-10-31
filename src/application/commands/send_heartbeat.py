from dataclasses import dataclass


@dataclass(frozen=True)
class SendHeartbeatCommand:
    """Команда отправки сигнала состояния (heartbeat)."""

    service_name: str

