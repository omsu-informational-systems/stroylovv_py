from datetime import datetime, timezone
import pytest

from src.application.commands.process_candle import ProcessCandleCommand
from src.application.commands.process_recommendation_result import (
    ProcessRecommendationResultCommand,
)
from src.application.commands.process_registration_result import (
    ProcessRegistrationResultCommand,
)
from src.application.use_cases.health_check import HealthCheckUseCase
from src.application.use_cases.process_candle import ProcessCandleUseCase
from src.application.use_cases.process_recommendation_result import (
    ProcessRecommendationResultUseCase,
)
from src.application.use_cases.process_registration_result import (
    ProcessRegistrationResultUseCase,
)
from src.application.use_cases.register_service import RegisterServiceUseCase
from src.application.use_cases.send_heartbeat import SendHeartbeatUseCase
from src.domain.entities.recommendation import TradeRecommendation
from src.domain.entities.registration import ServiceRegistration
from src.infrastructure.strategy.donchian import DonchianStrategyAdapter


@pytest.mark.asyncio
async def test_process_candle_use_case_and_deduplication() -> None:
    published: list[TradeRecommendation] = []

    class FakeRecPublisher:
        async def publish_recommendation(self, rec: TradeRecommendation) -> None:
            published.append(rec)

    reg = ServiceRegistration(
        service_name="breakout-strategy",
        account_id="stroylov-v",
        group_id="МИБ-401",
        strategy_id="donchian-v1",
    )
    reg.activate(datetime.now(timezone.utc))

    strategy = DonchianStrategyAdapter(window=2)
    use_case = ProcessCandleUseCase(
        strategy=strategy,
        publisher=FakeRecPublisher(),  # type: ignore[arg-type]
        registration=reg,
        default_order_quantity=2,
    )

    now = datetime.now(timezone.utc)
    # Свеча 1 (формирование окна 1/2) -> HOLD, qty == 0
    cmd1 = ProcessCandleCommand(
        ticker="GAZP",
        timestamp=now,
        open="100.00",
        high="105.00",
        low="95.00",
        close="100.00",
        volume=1000.0,
        event_id="evt-1",
        correlation_id="corr-1",
        run_id="run-1",
        sequence_number=1,
    )
    rec1 = await use_case.execute(cmd1)
    assert rec1 is not None
    assert rec1.ticker == "GAZP"
    assert rec1.signal == "HOLD"
    assert published[-1].suggested_quantity == 0
    assert reg.processed_candles == 1
    assert reg.published_recommendations == 1
    assert reg.last_processed_sequence_number == 1

    # Дубликат eventId="evt-1" -> игнорируется, повторной публикации нет
    dup = await use_case.execute(cmd1)
    assert dup is None
    assert len(published) == 1
    assert reg.processed_candles == 1

    # Свеча 2 (формирование окна 2/2) -> HOLD
    cmd2 = ProcessCandleCommand(
        ticker="GAZP",
        timestamp=now,
        open="100.00",
        high="105.00",
        low="95.00",
        close="101.00",
        volume=1200.0,
        event_id="evt-2",
        sequence_number=2,
    )
    rec2 = await use_case.execute(cmd2)
    assert rec2 is not None
    assert rec2.signal == "HOLD"

    # Свеча 3: пробой вверх (close=112 > max_high=105) -> BUY, qty == 2
    cmd3 = ProcessCandleCommand(
        ticker="GAZP",
        timestamp=now,
        open="102.00",
        high="115.00",
        low="101.00",
        close="112.00",
        volume=2000.0,
        event_id="evt-3",
        correlation_id="corr-3",
        run_id="run-1",
        sequence_number=3,
    )
    rec3 = await use_case.execute(cmd3)
    assert rec3 is not None
    assert rec3.signal == "BUY"
    assert rec3.confidence > 0.0
    assert published[-1].suggested_quantity == 2
    assert published[-1].source_event_id == "evt-3"
    assert published[-1].correlation_id == "corr-3"
    assert reg.processed_candles == 3
    assert reg.last_processed_sequence_number == 3


def test_health_check_use_case() -> None:
    uc = HealthCheckUseCase(service_name="test-svc", service_version="0.1.0")
    res = uc.execute({"kafka": ("UP", "Connected")})
    assert res.status == "UP"
    assert res.components["kafka"].status == "UP"

    res_down = uc.execute({"kafka": ("DOWN", "Connection refused")})
    assert res_down.status == "DEGRADED"


@pytest.mark.asyncio
async def test_send_heartbeat_only_after_registration() -> None:
    published_regs: list[ServiceRegistration] = []

    class FakeHeartbeatPublisher:
        async def publish_heartbeat(self, registration: ServiceRegistration) -> None:
            published_regs.append(registration)

    reg = ServiceRegistration(service_name="test-service", account_id="acc-1")
    use_case = SendHeartbeatUseCase(
        publisher=FakeHeartbeatPublisher(),  # type: ignore[arg-type]
        registration=reg,
    )

    # До регистрации heartbeat не отправляется
    assert await use_case.execute() is False
    assert len(published_regs) == 0

    # После активации heartbeat отправляется
    reg.activate(datetime.now(timezone.utc))
    assert await use_case.execute() is True
    assert len(published_regs) == 1
    assert published_regs[0].service_name == "test-service"


@pytest.mark.asyncio
async def test_register_service_use_case_records_event_id() -> None:
    calls: list[ServiceRegistration] = []

    class FakeRegPublisher:
        async def publish_registration(self, registration: ServiceRegistration) -> str:
            calls.append(registration)
            return "reg-event-123"

    reg = ServiceRegistration(
        service_name="breakout-strategy",
        account_id="stroylov-v",
        account_name="Виталий Стройлов",
        group_id="МИБ-401",
        strategy_id="hybrid-breakout-stroylov",
        strategy_name="hybrid-breakout",
    )
    uc = RegisterServiceUseCase(
        publisher=FakeRegPublisher(),  # type: ignore[arg-type]
        registration=reg,
    )

    event_id = await uc.execute()
    assert event_id == "reg-event-123"
    assert reg.registration_event_id == "reg-event-123"
    assert len(calls) == 1


@pytest.mark.asyncio
async def test_process_registration_result_filters_foreign_and_updates_state() -> None:
    reg = ServiceRegistration(
        service_name="breakout-strategy",
        account_id="stroylov-v",
        account_name="Виталий Стройлов",
        group_id="МИБ-401",
        strategy_id="hybrid-breakout-stroylov",
    )
    reg.record_registration_requested("req-own-1")
    uc = ProcessRegistrationResultUseCase(registration=reg)

    # Чужой ответ (другой causationId) -> игнорируется
    matched_foreign = await uc.execute(
        ProcessRegistrationResultCommand(
            service_name="other-service",
            account_id="other-student",
            causation_id="req-other-999",
            status="REJECTED",
            success=False,
            reason="Foreign rejection",
        )
    )
    assert matched_foreign is False
    assert reg.is_rejected is False

    # Собственный отказ -> блокирует публикацию
    matched_rej = await uc.execute(
        ProcessRegistrationResultCommand(
            service_name="breakout-strategy",
            account_id="stroylov-v",
            causation_id="req-own-1",
            status="REJECTED",
            success=False,
            reason="Invalid key",
        )
    )
    assert matched_rej is True
    assert reg.is_active is False
    assert reg.is_rejected is True
    assert reg.can_publish_recommendations is False

    # Собственное подтверждение -> активирует
    matched_ok = await uc.execute(
        ProcessRegistrationResultCommand(
            service_name="breakout-strategy",
            account_id="stroylov-v",
            causation_id="req-own-1",
            status="REGISTERED",
            success=True,
            reason="Account service has been registered.",
        )
    )
    assert matched_ok is True
    assert reg.is_active is True
    assert reg.is_rejected is False
    assert reg.can_publish_recommendations is True


@pytest.mark.asyncio
async def test_process_recommendation_result_filters_own() -> None:
    reg = ServiceRegistration(
        service_name="breakout-strategy",
        account_id="stroylov-v",
        strategy_id="hybrid-breakout-stroylov",
    )
    uc = ProcessRecommendationResultUseCase(registration=reg)

    assert (
        await uc.execute(
            ProcessRecommendationResultCommand(
                account_id="other-user",
                strategy_id="other-strat",
                instrument="SBER",
                sequence_number=10,
                execution_status="EXECUTED",
            )
        )
        is False
    )

    assert (
        await uc.execute(
            ProcessRecommendationResultCommand(
                account_id="stroylov-v",
                strategy_id="hybrid-breakout-stroylov",
                instrument="SBER",
                sequence_number=10,
                recommendation_id="rec-1",
                execution_status="EXECUTED",
            )
        )
        is True
    )


@pytest.mark.asyncio
async def test_process_candle_blocks_on_rejection() -> None:
    published: list[TradeRecommendation] = []

    class FakeRecPublisher:
        async def publish_recommendation(self, rec: TradeRecommendation) -> None:
            published.append(rec)

    reg = ServiceRegistration(service_name="breakout-strategy")
    reg.reject("Платформа отклонила доступ")

    strategy = DonchianStrategyAdapter(window=2)
    use_case = ProcessCandleUseCase(
        strategy=strategy,
        publisher=FakeRecPublisher(),  # type: ignore[arg-type]
        registration=reg,
    )

    cmd = ProcessCandleCommand(
        ticker="SBER",
        timestamp=datetime.now(timezone.utc),
        open="250.00",
        high="255.00",
        low="245.00",
        close="250.00",
        volume=1000.0,
    )
    res = await use_case.execute(cmd)
    assert res is not None
    assert res.ticker == "SBER"
    assert len(published) == 0
