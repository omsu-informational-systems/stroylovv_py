from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime, timezone
import uuid


@dataclass(kw_only=True)
class ServiceRegistration:
    """
    Доменная сущность: идентификация, состояние регистрации и метрики процесса сервиса.

    Инкапсулирует:
    - Стабильные идентификаторы сервиса и стратегии (сохраняются между перезапусками).
    - Идентификатор текущего процесса (instance_id) и время запуска (started_at).
    - Состояние регистрации на платформе (pending / registered / rejected).
    - Счётчики обработанных свечей, опубликованных рекомендаций и последний sequenceNumber.
    - Ограниченный кэш обработанных eventId для защиты от дублей в рамках процесса.
    """

    service_name: str
    account_id: str = ""
    account_name: str = ""
    group_id: str = ""
    strategy_id: str = ""
    strategy_name: str = ""
    description: str = ""

    instance_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    registration_event_id: str | None = None
    registered_at: datetime | None = None
    is_active: bool = False
    is_rejected: bool = False
    rejection_reason: str = ""

    max_seen_events: int = 4096
    _processed_candles: int = field(default=0, repr=False)
    _published_recommendations: int = field(default=0, repr=False)
    _last_processed_sequence_number: int | None = field(default=None, repr=False)
    _seen_event_ids: OrderedDict[str, bool] = field(default_factory=OrderedDict, repr=False)

    def record_registration_requested(self, event_id: str) -> None:
        """Зафиксировать eventId отправленного запроса регистрации для сопоставления ответа."""
        cleaned = event_id.strip()
        if cleaned:
            self.registration_event_id = cleaned

    def activate(self, timestamp: datetime) -> None:
        """Подтвердить успешную регистрацию платформой (status == REGISTERED)."""
        self.is_active = True
        self.is_rejected = False
        self.rejection_reason = ""
        self.registered_at = timestamp

    def reject(self, reason: str, timestamp: datetime | None = None) -> None:
        """Зафиксировать отказ в регистрации (status == REJECTED) с указанием причины."""
        self.is_active = False
        self.is_rejected = True
        self.rejection_reason = reason.strip() or "Причина отказа не указана платформой"
        self.registered_at = timestamp

    def matches_registration_result(
        self,
        *,
        causation_id: str | None = None,
        service_name: str = "",
        account_id: str = "",
        strategy_id: str = "",
    ) -> bool:
        """
        Проверить, относится ли ответ account.registration.result именно к нашему сервису.

        При наличии registration_event_id приоритетно проверяет causationId.
        Если registration_event_id не задан (например, в тестах или при восстановлении),
        сопоставляет собственные стабильные идентификаторы.
        """
        if self.registration_event_id and causation_id:
            return causation_id == self.registration_event_id

        if account_id and self.account_id and account_id != self.account_id:
            return False
        if strategy_id and self.strategy_id and strategy_id != self.strategy_id:
            return False
        if service_name and self.service_name and service_name != self.service_name:
            return False

        return bool(
            (service_name and service_name == self.service_name)
            or (account_id and self.account_id and account_id == self.account_id)
        )

    def matches_recommendation_result(
        self,
        *,
        account_id: str = "",
        strategy_id: str = "",
    ) -> bool:
        """Проверить, относится ли ответ strategy.recommendation.result к нашему сервису."""
        if not account_id or not strategy_id:
            return False
        return account_id == self.account_id and strategy_id == self.strategy_id

    def is_duplicate_event(self, event_id: str | None) -> bool:
        """Проверить, было ли событие свечи с данным eventId уже обработано в этом процессе."""
        if not event_id:
            return False
        return event_id in self._seen_event_ids

    def mark_candle_processed(
        self,
        event_id: str | None = None,
        sequence_number: int | None = None,
        *,
        published: bool = False,
    ) -> None:
        """Зафиксировать успешную обработку свечи и (опционально) публикацию рекомендации."""
        if event_id:
            self._seen_event_ids[event_id] = True
            self._seen_event_ids.move_to_end(event_id)
            while len(self._seen_event_ids) > self.max_seen_events:
                self._seen_event_ids.popitem(last=False)

        self._processed_candles += 1
        if published:
            self._published_recommendations += 1
        if sequence_number is not None:
            self._last_processed_sequence_number = sequence_number

    @property
    def processed_candles(self) -> int:
        return self._processed_candles

    @property
    def published_recommendations(self) -> int:
        return self._published_recommendations

    @property
    def last_processed_sequence_number(self) -> int | None:
        return self._last_processed_sequence_number

    @property
    def can_publish_recommendations(self) -> bool:
        """
        Флаг разрешения публикации торговых рекомендаций.
        При отказе платформы публикация строго блокируется.
        """
        return not self.is_rejected

    @property
    def can_send_heartbeat(self) -> bool:
        """Сигнал состояния отправляется только после успешной регистрации."""
        return self.is_active and not self.is_rejected

    @property
    def status_label(self) -> str:
        """Текстовый статус регистрации для диагностики /health."""
        if self.is_active:
            return "UP"
        if self.is_rejected:
            return "DOWN"
        return "UNKNOWN"
