"""
Утилита отправки запроса регистрации account.registration.request и ожидания ответа
account.registration.result от учебной платформы.

Соблюдает порядок протокола:
1. Подключает Producer (TLS + SCRAM-SHA-256).
2. Подписывает answers-consumer (<KAFKA_GROUP_ID>.answers) на account.registration.result
   и дожидается назначения разделов ДО отправки запроса регистрации.
3. Отправляет account.registration.request с подтверждением доставки брокером.
4. Читает account.registration.result, сопоставляя causationId == eventId запроса,
   подтверждает смещение и выводит результат (REGISTERED / REJECTED) без вывода секретов.

Запуск:
    poetry run python -m scripts.register_account
    poetry run python -m scripts.register_account --dry-run
"""

import argparse
import asyncio
import json
import sys
import time

from src.application.use_cases.process_registration_result import (
    ProcessRegistrationResultUseCase,
)
from src.application.use_cases.register_service import RegisterServiceUseCase
from src.domain.entities.registration import ServiceRegistration
from src.infrastructure.config.settings import settings
from src.infrastructure.kafka.client import KafkaConnectionManager
from src.infrastructure.kafka.producer import KafkaPublisherAdapter
from src.infrastructure.mappers.event_mapper import EventMapper


def _build_registration() -> ServiceRegistration:
    return ServiceRegistration(
        service_name=settings.effective_service_name,
        account_id=settings.account_id,
        account_name=settings.account_name,
        group_id=settings.group_id,
        strategy_id=settings.strategy_id,
        strategy_name=settings.strategy_name,
        description=settings.service_description,
    )


def _print_masked_envelope(registration: ServiceRegistration) -> None:
    _, raw_json = EventMapper.to_registration_envelope(
        registration=registration,
        secret_key="***MASKED***",
        schema_version=settings.schema_version,
    )
    print(json.dumps(json.loads(raw_json), ensure_ascii=False, indent=2))


async def _run_registration(dry_run: bool) -> int:
    registration = _build_registration()

    print("=" * 72)
    print("РЕГИСТРАЦИЯ СЕРВИСА НА ПЛАТФОРМЕ (account.registration.request)")
    print("=" * 72)
    print(f"Адрес брокера:    {settings.kafka_bootstrap_servers}")
    print(f"serviceName:      {registration.service_name}")
    print(f"accountId:        {registration.account_id}")
    print(f"accountName:      {registration.account_name}")
    print(f"groupId:          {registration.group_id}")
    print(f"strategyId:       {registration.strategy_id}")
    print(f"strategyName:     {registration.strategy_name}")
    print(f"Answers group.id: {settings.kafka_answers_group_id}")
    print("-" * 72)

    if dry_run:
        print("Режим --dry-run (сообщение НЕ отправляется в Kafka, secretKey скрыт):")
        _print_masked_envelope(registration)
        return 0

    if not settings.is_kafka_configured:
        print("ОШИБКА: KAFKA_USERNAME и/или KAFKA_PASSWORD не заданы в .env")
        return 1

    manager = KafkaConnectionManager(settings=settings)
    publisher = KafkaPublisherAdapter(
        connection_manager=manager,
        settings=settings,
        registration=registration,
    )
    register_uc = RegisterServiceUseCase(publisher=publisher, registration=registration)
    reg_result_uc = ProcessRegistrationResultUseCase(
        registration=registration,
        expected_secret_key=settings.effective_secret_key,
    )

    answers = None
    try:
        print("[1/4] Подключение Producer (TLS + SCRAM-SHA-256)...")
        await manager.start_producer()
        if not manager.is_producer_connected:
            print("ОШИБКА: не удалось подключить Kafka Producer (проверьте доступность порта 9094 и учётные данные).")
            return 1

        print(f"[2/4] Подписка на '{settings.topic_registration_result}' и ожидание назначения разделов...")
        answers = manager.create_consumer(group_id=settings.kafka_answers_group_id)
        if answers is None:
            print("ОШИБКА: не удалось создать answers-consumer.")
            return 1

        await asyncio.to_thread(answers.subscribe, [settings.topic_registration_result])
        deadline = time.monotonic() + settings.kafka_assignment_timeout_seconds
        while not await asyncio.to_thread(answers.assignment):
            if time.monotonic() > deadline:
                raise TimeoutError("Не назначены разделы темы account.registration.result")
            await asyncio.to_thread(answers.poll, 0.2)
        print("      Разделы назначены.")

        print(f"[3/4] Отправка запроса в '{settings.topic_registration_request}'...")
        event_id = await register_uc.execute()
        print(f"      Запрос подтверждён брокером (eventId={event_id}).")

        print(
            f"[4/4] Ожидание ответа в '{settings.topic_registration_result}' "
            f"(до {settings.registration_timeout_seconds} с)..."
        )
        res_deadline = time.monotonic() + settings.registration_timeout_seconds
        skipped_foreign = 0
        while True:
            if time.monotonic() > res_deadline:
                print(
                    f"ОШИБКА: нет ответа account.registration.result за отведённое время "
                    f"(пропущено чужих ответов: {skipped_foreign}). "
                    "Проверьте настройки ключа и темы по умолчанию у преподавателя."
                )
                return 1

            msg = await asyncio.to_thread(answers.poll, 0.2)
            if msg is None:
                continue
            if msg.error():
                raise RuntimeError(f"Ошибка Kafka Consumer: {msg.error().code()}")

            raw = msg.value()
            if raw is None:
                await asyncio.to_thread(answers.commit, message=msg, asynchronous=True)
                continue

            try:
                cmd = EventMapper.parse_registration_result(raw)
            except Exception as parse_err:
                skipped_foreign += 1
                await asyncio.to_thread(answers.commit, message=msg, asynchronous=True)
                continue

            matched = await reg_result_uc.execute(cmd)
            if matched:
                await asyncio.to_thread(answers.commit, message=msg, asynchronous=False)
                print("-" * 72)
                if registration.is_active:
                    print(
                        f"УСПЕХ: status={cmd.status or 'REGISTERED'}, "
                        f"accountId={registration.account_id}, "
                        f"groupId={registration.group_id}, "
                        f"reason='{cmd.reason}'"
                    )
                    print("=" * 72)
                    return 0
                print(
                    f"ОТКЛОНЕНО: status={cmd.status or 'REJECTED'}, "
                    f"reason='{registration.rejection_reason}'"
                )
                print("=" * 72)
                return 1

            skipped_foreign += 1
            await asyncio.to_thread(answers.commit, message=msg, asynchronous=True)

    except Exception as exc:
        print(f"ОШИБКА регистрации: {exc}")
        return 1
    finally:
        if answers is not None:
            try:
                await asyncio.to_thread(answers.close)
            except Exception:
                pass
        await manager.stop_producer()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Отправка account.registration.request и ожидание account.registration.result."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Показать сформированный JSON-запрос (с маскировкой secretKey) без отправки в Kafka.",
    )
    args = parser.parse_args()
    return asyncio.run(_run_registration(dry_run=args.dry_run))


if __name__ == "__main__":
    sys.exit(main())
