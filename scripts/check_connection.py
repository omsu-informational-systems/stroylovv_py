"""
Утилита диагностики подключения к брокеру Apache Kafka.
Проверяет сетевую доступность, TLS-шифрование, SASL-аутентификацию,
работоспособность Producer и Consumer без вывода конфиденциальных данных.
"""
import asyncio
import sys

from src.infrastructure.config.settings import settings
from src.infrastructure.kafka.client import KafkaConnectionManager


async def main() -> int:
    print("=" * 70)
    print("ДИАГНОСТИКА ПОДКЛЮЧЕНИЯ К APACHE KAFKA (УЧЕБНЫЙ СТЕНД)")
    print("=" * 70)
    print(f"Адрес брокера:        {settings.kafka_bootstrap_servers}")
    print(f"Протокол безопасности: {settings.kafka_security_protocol}")
    print(f"Механизм SASL:        {settings.kafka_sasl_mechanism}")
    print(f"Логин (username):     {settings.kafka_username}")
    print(f"Имя группы консьюмера: {settings.kafka_group_id}")
    print(f"Топик свечей:         {settings.candle_topic}")
    print(f"Топик регистрации:    {settings.topic_registration_request}")
    print("-" * 70)

    if not settings.kafka_password:
        print("❌ ОШИБКА: KAFKA_PASSWORD не задан в .env файле!")
        return 1

    manager = KafkaConnectionManager(settings=settings)

    # 1. Проверка Kafka Producer
    print("[1/2] Проверка подключения Kafka Producer...")
    try:
        await manager.start_producer()
        if not manager.is_connected or manager.producer is None:
            print("❌ Не удалось подключить Producer.")
            return 1

        print("  ✅ Producer успешно подключен и авторизован!")
        candle_parts = await manager.producer.partitions_for(settings.candle_topic)
        reg_parts = await manager.producer.partitions_for(settings.topic_registration_request)
        print(f"  ✅ Топик '{settings.candle_topic}' доступен (партиций: {len(candle_parts) if candle_parts else 0})")
        print(f"  ✅ Топик '{settings.topic_registration_request}' доступен (партиций: {len(reg_parts) if reg_parts else 0})")
        await manager.stop_producer()
    except Exception as exc:
        print(f"❌ Ошибка подключения Producer: {exc}")
        return 1

    # 2. Проверка Kafka Consumer
    print("\n[2/2] Проверка подключения Kafka Consumer...")
    topics = [settings.candle_topic, settings.topic_registration_result]
    consumer = manager.create_consumer(topics=topics)
    if consumer is None:
        print("❌ Не удалось создать Consumer.")
        return 1

    try:
        await consumer.start()
        all_topics = await consumer.topics()
        print(f"  ✅ Consumer успешно подключен! Доступно топиков на брокере: {len(all_topics)}")
        await consumer.stop()
    except Exception as exc:
        print(f"❌ Ошибка подключения Consumer: {exc}")
        return 1

    print("-" * 70)
    print("ИТОГ: Все проверки пройдены успешно! Соединение с Kafka полностью работает.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

