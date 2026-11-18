"""
Утилита диагностики подключения к защищённому брокеру Apache Kafka (режим --check).

Проверяет сетевую доступность, TLS-шифрование (с проверкой сертификата и имени хоста),
SASL SCRAM-SHA-256 авторизацию и наличие всех обязательных тем платформы.
НЕ регистрирует аккаунт и НЕ выводит секреты/ключи в консоль.

Запуск:
    poetry run python -m scripts.check_connection --check
    poetry run python -m scripts.check_connection --check --debug
"""

import argparse
import socket
import ssl
import sys

from src.infrastructure.config.settings import settings
from src.infrastructure.kafka.client import KafkaConnectionManager


def _check_tcp_and_tls(bootstrap: str, ca_location: str | None) -> bool:
    """Предварительная проверка TCP-доступности и TLS-рукопожатия до вызова Kafka."""
    first_broker = bootstrap.split(",")[0].strip()
    host, _, port_str = first_broker.partition(":")
    port = int(port_str or "9094")

    ctx = ssl.create_default_context(cafile=ca_location)
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED

    try:
        with socket.create_connection((host, port), timeout=5.0) as raw_sock:
            with ctx.wrap_socket(raw_sock, server_hostname=host) as tls_sock:
                cert = tls_sock.getpeercert() or {}
                not_after = cert.get("notAfter", "неизвестно")
                print(
                    f"[1/2] TCP + TLS OK: {host}:{port} "
                    f"(протокол={tls_sock.version()}, сертификат до: {not_after})"
                )
                return True
    except ssl.SSLCertVerificationError as exc:
        print(f"[1/2] ОШИБКА проверки TLS-сертификата ({host}:{port}): {exc}")
        return False
    except OSError as exc:
        print(f"[1/2] ОШИБКА TCP/сети ({host}:{port}): {exc}")
        return False


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Проверка TLS, SCRAM-SHA-256 авторизации и метаданных тем Kafka."
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Получить метаданные брокера без регистрации аккаунта.",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Включить подробную отладку librdkafka (broker/security/protocol).",
    )
    args = parser.parse_args()

    print("=" * 72)
    print("ДИАГНОСТИКА ПОДКЛЮЧЕНИЯ К ЗАЩИЩЁННОЙ KAFKA (SASL_SSL + SCRAM-SHA-256)")
    print("=" * 72)
    print(f"Адрес брокера:           {settings.kafka_bootstrap_servers}")
    print(f"Протокол безопасности:   {settings.kafka_security_protocol}")
    print(f"Механизм SASL:           {settings.kafka_sasl_mechanism}")
    print(f"Группа ответов:          {settings.kafka_answers_group_id}")
    print(f"Группа свечей:           {settings.kafka_candles_group_id}")
    print(f"Тема свечей:             {settings.candle_topic}")
    print(f"Имя сервиса (producer):  {settings.effective_service_name}")

    manager = KafkaConnectionManager(settings=settings)
    ca_location = manager.resolve_ca_location()
    print(f"CA-сертификаты:          {ca_location or 'системные по умолчанию'}")
    print("-" * 72)

    if not settings.is_kafka_configured:
        print("ОШИБКА: KAFKA_USERNAME и/или KAFKA_PASSWORD не заданы в переменных окружения / .env")
        return 1

    if not _check_tcp_and_tls(settings.kafka_bootstrap_servers, ca_location):
        return 1

    try:
        topics_count, missing = manager.check_connection_sync(
            timeout=10.0, verbose=args.debug
        )
    except ImportError:
        print("ОШИБКА: библиотека confluent-kafka не установлена. Выполните: poetry install")
        return 1
    except Exception as exc:
        print(f"[2/2] ОШИБКА Kafka/SASL: {exc}")
        return 1

    print(
        f"[2/2] OK: TLS и SCRAM-SHA-256 авторизация работают; доступно тем на брокере: {topics_count}"
    )
    if missing:
        print("ПРЕДУПРЕЖДЕНИЕ: отсутствуют обязательные темы (уточните настройки у преподавателя):")
        for topic in missing:
            print(f"  - {topic}")
        return 1

    print("OK: Все 6 обязательных тем платформы доступны.")
    print("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
