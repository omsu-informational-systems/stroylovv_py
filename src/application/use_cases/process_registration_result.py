from datetime import datetime, timezone
import logging

from src.application.commands.process_registration_result import (
    ProcessRegistrationResultCommand,
)
from src.domain.entities.registration import ServiceRegistration

logger = logging.getLogger(__name__)


class ProcessRegistrationResultUseCase:
    """
    Сценарий использования: обработка ответа платформы account.registration.result.

    - Игнорирует чужие ответы (проверка causationId и собственных идентификаторов).
    - Никогда не выводит secretKey в журнал.
    - При status == REGISTERED активирует регистрацию.
    - При status == REJECTED фиксирует причину и блокирует публикацию рекомендаций.
    """

    def __init__(
        self,
        registration: ServiceRegistration,
        expected_secret_key: str = "",
    ) -> None:
        self._registration = registration
        self._expected_secret_key = expected_secret_key

    async def execute(self, command: ProcessRegistrationResultCommand) -> bool:
        if not self._registration.matches_registration_result(
            causation_id=command.causation_id,
            service_name=command.service_name,
            account_id=command.account_id,
            strategy_id=command.strategy_id,
        ):
            return False

        if command.secret_key and self._expected_secret_key:
            if command.secret_key != self._expected_secret_key:
                return False

        ts = command.timestamp or datetime.now(timezone.utc)
        if command.success:
            self._registration.activate(timestamp=ts)
            logger.info(
                "[РЕГИСТРАЦИЯ ПОДТВЕРЖДЕНА] Сервис '%s' зарегистрирован (accountId=%s, groupId=%s, strategyId=%s, status=%s).",
                self._registration.service_name,
                self._registration.account_id,
                self._registration.group_id,
                self._registration.strategy_id,
                command.status or "REGISTERED",
            )
        else:
            self._registration.reject(reason=command.reason, timestamp=ts)
            logger.error(
                "[ОТКАЗ В РЕГИСТРАЦИИ] Платформа отклонила регистрацию сервиса '%s' (status=%s). "
                "Причина: %s. Публикация рекомендаций ЗАБЛОКИРОВАНА.",
                self._registration.service_name,
                command.status or "REJECTED",
                self._registration.rejection_reason,
            )
        return True
