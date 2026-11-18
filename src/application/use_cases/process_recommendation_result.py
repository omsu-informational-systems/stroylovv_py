import logging

from src.application.commands.process_recommendation_result import (
    ProcessRecommendationResultCommand,
)
from src.domain.entities.registration import ServiceRegistration

logger = logging.getLogger(__name__)


class ProcessRecommendationResultUseCase:
    """
    Сценарий использования: обработка ответа платформы strategy.recommendation.result.

    Игнорирует чужие ответы и логирует статус исполнения собственных рекомендаций
    без вывода конфиденциальных ключей.
    """

    def __init__(self, registration: ServiceRegistration) -> None:
        self._registration = registration

    async def execute(self, command: ProcessRecommendationResultCommand) -> bool:
        if not self._registration.matches_recommendation_result(
            account_id=command.account_id,
            strategy_id=command.strategy_id,
        ):
            return False

        logger.info(
            "Результат рекомендации: instrument=%s, sequenceNumber=%s, recommendationId=%s, status=%s%s",
            command.instrument,
            command.sequence_number,
            command.recommendation_id,
            command.execution_status or "UNKNOWN",
            f", reason='{command.reason}'" if command.reason else "",
        )
        return True
