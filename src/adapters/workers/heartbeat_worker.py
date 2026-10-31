import asyncio
import logging

from src.application.commands.send_heartbeat import SendHeartbeatCommand
from src.application.use_cases.send_heartbeat import SendHeartbeatUseCase

logger = logging.getLogger(__name__)


class HeartbeatWorker:
    """Фоновый воркер периодической отправки heartbeat платформе."""

    def __init__(
        self,
        use_case: SendHeartbeatUseCase,
        service_name: str,
        interval_seconds: int = 10,
    ) -> None:
        self._use_case = use_case
        self._service_name = service_name
        self._interval = interval_seconds
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
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Heartbeat worker остановлен.")

    async def _run_loop(self) -> None:
        command = SendHeartbeatCommand(service_name=self._service_name)
        while self._running:
            try:
                await self._use_case.execute(command)
            except Exception as exc:
                logger.debug("Ошибка в цикле heartbeat: %s", exc)

            try:
                await asyncio.sleep(self._interval)
            except asyncio.CancelledError:
                break

