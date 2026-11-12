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

    При успехе: активирует статус регистрации и разрешает публикацию рекомендаций.
    При отказе: фиксирует понятную причину отказа, блокирует публикацию рекомендаций
    и записывает структурированный журнал ошибки.
    """

    def __init__(self, registration: ServiceRegistration) -> None:
        self._registration = registration

    async def execute(self, command: ProcessRegistrationResultCommand) -> None:
        ts = command.timestamp or datetime.now(timezone.utc)
        if command.success:
            self._registration.activate(timestamp=ts)
            logger.info(
                "[РЕГИСТРАЦИЯ ПОДТВЕРЖДЕНА] Сервис '%s' успешно зарегистрирован на платформе. "
                "Студент: '%s', Группа: '%s', Стратегия: '%s'. Публикация рекомендаций РАЗРЕШЕНА.",
                command.service_name,
                self._registration.account_name,
                self._registration.group_id,
                self._registration.strategy_name,
            )
        else:
            self._registration.reject(reason=command.reason, timestamp=ts)
            logger.error(
                "[ОТКАЗ В РЕГИСТРАЦИИ] Платформа отклонила регистрацию сервиса '%s'! "
                "Причина отказа: %s. Публикация торговых рекомендаций ЗАБЛОКИРОВАНА.",
                command.service_name,
                self._registration.rejection_reason,
            )

