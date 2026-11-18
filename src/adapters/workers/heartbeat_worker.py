import asyncio
import logging

from src.application.use_cases.send_heartbeat import SendHeartbeatUseCase

logger = logging.getLogger(__name__)


class HeartbeatWorker:
    """
    Фоновый воркер периодической отправки сигнала состояния (account.service.heartbeat).

    Отправляет heartbeat примерно раз в 5 секунд после подтверждения регистрации
    независимо от наличия активного запуска.
    """

    def __init__(
        self,
        use_case: SendHeartbeatUseCase,
        interval_seconds: int = 5,
    ) -> None:
        self._use_case = use_case
        self._interval = max(1, interval_seconds)
        self._task: asyncio.Task[None] | None = None
        self._running: bool = False

    def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())
        logger.info("Heartbeat worker запущен (интервал %s с).", self._interval)

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Heartbeat worker остановлен.")

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self._use_case.execute()
            except Exception as exc:
                logger.warning("Ошибка в цикле heartbeat: %s", exc)

            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break
