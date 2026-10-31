from datetime import datetime, timezone
import pytest

from src.application.commands.process_candle import ProcessCandleCommand
from src.application.commands.send_heartbeat import SendHeartbeatCommand
from src.application.use_cases.health_check import HealthCheckUseCase
from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.application.use_cases.send_heartbeat import SendHeartbeatUseCase
from src.infrastructure.strategy.donchian import DonchianStrategyAdapter


@pytest.mark.asyncio
async def test_process_candle_use_case() -> None:
    strategy = DonchianStrategyAdapter(window=2)
    use_case = ProcessCandleUseCase(strategy=strategy, publisher=None)

    now = datetime.now(timezone.utc)
    # Свеча 1 (формирование окна 1/2) -> HOLD
    cmd1 = ProcessCandleCommand(
        ticker="GAZP",
        timestamp=now,
        open=100.0,
        high=105.0,
        low=95.0,
        close=100.0,
        volume=1000.0,
    )
    rec1 = await use_case.execute(cmd1)
    assert rec1.ticker == "GAZP"
    assert rec1.signal == "HOLD"

    # Свеча 2 (формирование окна 2/2) -> HOLD
    cmd2 = ProcessCandleCommand(
        ticker="GAZP",
        timestamp=now,
        open=100.0,
        high=105.0,
        low=95.0,
        close=101.0,
        volume=1200.0,
    )
    rec2 = await use_case.execute(cmd2)
    assert rec2.signal == "HOLD"

    # Свеча 3: пробой вверх (close=112 > max_high=105) -> BUY
    cmd3 = ProcessCandleCommand(
        ticker="GAZP",
        timestamp=now,
        open=102.0,
        high=115.0,
        low=101.0,
        close=112.0,
        volume=2000.0,
    )
    rec3 = await use_case.execute(cmd3)
    assert rec3.signal == "BUY"
    assert rec3.confidence > 0.0


def test_health_check_use_case() -> None:
    uc = HealthCheckUseCase(service_name="test-svc", service_version="0.1.0")
    res = uc.execute({"kafka": ("UP", "Connected")})
    assert res.status == "UP"
    assert res.components["kafka"].status == "UP"

    res_down = uc.execute({"kafka": ("DOWN", "Connection refused")})
    assert res_down.status == "DEGRADED"


@pytest.mark.asyncio
async def test_send_heartbeat_use_case() -> None:
    published_events = []

    class FakePublisher:
        async def publish_heartbeat(self, service_name: str) -> None:
            published_events.append(service_name)

    use_case = SendHeartbeatUseCase(publisher=FakePublisher())  # type: ignore
    await use_case.execute(SendHeartbeatCommand(service_name="test-service"))
    assert published_events == ["test-service"]
