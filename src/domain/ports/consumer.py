from typing import Protocol, runtime_checkable


@runtime_checkable
class EventConsumerPort(Protocol):
    """Порт фонового потребления событий из брокера."""

    async def start(self) -> None:
        """Запустить фоновое чтение сообщений."""
        ...

    async def stop(self) -> None:
        """Остановить чтение сообщений."""
        ...

