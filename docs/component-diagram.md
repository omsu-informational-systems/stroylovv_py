# Крупноузловая компонентная схема сервиса (Component Diagram)

Схема отображает внутренние компоненты микросервиса `breakout-strategy`, сгруппированные по слоям Domain-Driven Design (DDD), их зоны ответственности и основные потоки данных.

```mermaid
flowchart TB
    %% Внешние триггеры
    ExtKafkaIn["Kafka Topics:<br/>• CANDLE_TOPIC (market.candle.closed)<br/>• account.registration.result"]
    ExtHttpIn["HTTP Клиент / Docker:<br/>• GET /health"]
    ExtKafkaOut["Kafka Topics:<br/>• account.registration.request<br/>• account.service.heartbeat<br/>• strategy.recommendation.created"]

    subgraph PresentationLayer ["1. Presentation / Delivery Layer (Адаптеры входа)"]
        ConsumerService["KafkaConsumerService<br/>• Чтение топиков свечей и регистрации<br/>• Защита от сбоев брокера (reconnect loop)<br/>• Ручной коммит смещения (At-Least-Once)"]
        HeartbeatWorker["HeartbeatWorker<br/>• Фоновый периодический таймер (10с)<br/>• Инициация сигналов доступности"]
        HttpRouter["FastAPI HTTP Router<br/>• GET /health эндпоинт"]
    end

    subgraph ApplicationLayer ["2. Application Layer (Сценарии использования / Use Cases)"]
        UCOrchestrator["ProcessCandleUseCase<br/>• Создание доменной свечи<br/>• Запрос оценки стратегии<br/>• Проверка допуска к публикации"]
        UCRegResult["ProcessRegistrationResultUseCase<br/>• Обработка подтверждения/отказа платформы<br/>• Управление допуском к торгам"]
        UCRegister["RegisterServiceUseCase<br/>• Формирование команды регистрации"]
        UCHb["SendHeartbeatUseCase<br/>• Формирование команды пульса"]
        UCHc["HealthCheckUseCase<br/>• Агрегация статусов компонентов"]
    end

    subgraph DomainLayer ["3. Domain Layer (Ядро бизнес-логики — Pure Python stdlib)"]
        subgraph DomainEntities ["Доменные сущности & Модели"]
            EntCandle["Candle (Сущность свечи)<br/>• Инварианты: High >= Low, Close, Open > 0"]
            EntRec["TradeRecommendation<br/>• Сигналы: BUY / SELL / HOLD<br/>• Метрика confidence [0.0, 1.0]"]
            EntReg["ServiceRegistration<br/>• Статус допуска: is_active, is_rejected<br/>• Защита от публикации при отказе"]
            ModelHybrid["HybridTrioModel / Donchian<br/>• Скользящие окна Donchian (20 свечей)<br/>• Z-score коинтеграции парного спреда<br/>• Защита от 'пурпурной стены' выходных"]
        end

        subgraph DomainPorts ["Доменные порты (Protocols)"]
            PortStrategy["StrategyPort<br/>evaluate(candle) -> recommendation"]
            PortRecPub["RecommendationPublisherPort<br/>publish_recommendation(...)"]
            PortRegPub["RegistrationPublisherPort<br/>publish_registration(...)"]
            PortHbPub["HeartbeatPublisherPort<br/>publish_heartbeat(...)"]
        end
    end

    subgraph InfrastructureLayer ["4. Infrastructure Layer (Интеграции и адаптеры выхода)"]
        KafkaConn["KafkaConnectionManager<br/>• aiokafka Producer / Consumer<br/>• SASL_SSL + SCRAM-SHA-256<br/>• TLS верификация сертификата"]
        KafkaPub["KafkaPublisherAdapter<br/>• Реализация портов публикации<br/>• Асинхронная отправка с acks='all'"]
        Mappers["Data Mappers (Сериализация/Десериализация)<br/>• CandleMapper: парсинг JSON -> Command<br/>• RecommendationMapper: домен -> JSON<br/>• EventMapper: регистрация & heartbeat JSON"]
        StrategyAdapters["Strategy Adapters<br/>• HybridTrioStrategyAdapter<br/>• DonchianStrategyAdapter"]
        Config["Settings (.env)<br/>• Динамическая конфигурация<br/>• Защита учетных данных"]
    end

    %% Потоки данных: Вход
    ExtKafkaIn -->|"Байты сообщений"| ConsumerService
    ExtHttpIn -->|"HTTP запрос"| HttpRouter
    HttpRouter --> UCHc

    ConsumerService -->|"Свечной payload"| Mappers
    Mappers -->|"ProcessCandleCommand"| UCOrchestrator

    ConsumerService -->|"account.registration.result"| Mappers
    Mappers -->|"ProcessRegistrationResultCommand"| UCRegResult
    UCRegResult -->|"activate() / reject()"| EntReg

    HeartbeatWorker -->|"Тик таймера"| UCHb
    UCRegister -->|"publish_registration"| PortRegPub

    %% Потоки данных: Доменная логика
    UCOrchestrator -->|"Создает сущность"| EntCandle
    UCOrchestrator -->|"Проверяет допуск"| EntReg
    UCOrchestrator -->|"Вызывает стратегию"| PortStrategy
    PortStrategy -.->|"Реализует"| StrategyAdapters
    StrategyAdapters -->|"Выполняет расчет"| ModelHybrid
    ModelHybrid -->|"Формирует"| EntRec

    %% Потоки данных: Публикация
    UCOrchestrator -->|"Передает рекомендацию"| PortRecPub
    UCHb -->|"Передает heartbeat"| PortHbPub

    PortRecPub -.->|"Реализует"| KafkaPub
    PortRegPub -.->|"Реализует"| KafkaPub
    PortHbPub -.->|"Реализует"| KafkaPub

    KafkaPub -->|"Сериализация через mappers"| Mappers
    KafkaPub -->|"Отправка через aiokafka"| KafkaConn
    KafkaConn -->|"Защищенный TLS сокет"| ExtKafkaOut

    Config -.->|"Параметры и топики"| KafkaConn
    Config -.->|"Параметры окон"| StrategyAdapters
```

---

## Зоны ответственности компонентов

### 1. Presentation / Delivery Layer
- **`KafkaConsumerService`:** непрерывное получение сообщений из брокера Kafka по темам `CANDLE_TOPIC` и `account.registration.result`. Изолирует сетевые сбои: при падении или перезапуске брокера не завершает процесс, а ожидает доступности и восстанавливает подписку.
- **`HeartbeatWorker`:** автономный фоновый процесс, с заданной периодичностью отправляющий сигналы жизнедеятельности сервиса для платформы.
- **`FastAPI HTTP Router`:** легковесный веб-адаптер для опроса работоспособности (`GET /health`) внешними системами мониторинга.

### 2. Application Layer
- **`ProcessCandleUseCase`:** координирует цепочку обработки поступающей свечи: валидация, расчет стратегии, проверка статуса регистрации и отправка рекомендации.
- **`ProcessRegistrationResultUseCase`:** обрабатывает вердикт платформы. При отказе сохраняет понятную причину и блокирует публикацию рекомендаций. При успехе активирует флаг доступа.
- **`RegisterServiceUseCase`:** формирует запрос на регистрацию при запуске сервиса со стабильными метаданными студента и стратегии.
- **`SendHeartbeatUseCase` / `HealthCheckUseCase`:** обеспечивают наблюдаемость и диагностику компонентов.

### 3. Domain Layer (Ядро чистой архитектуры)
- **`Candle`:** инкапсулирует свойства свечи, гарантирует валидность цен и таймстемпа.
- **`TradeRecommendation`:** представляет результат анализа рынка (`BUY`, `SELL`, `HOLD`), границы ценового коридора и математическую уверенность `confidence`.
- **`ServiceRegistration`:** доменный агрегат состояния допуска к торгам. Содержит метод `can_publish_recommendations`.
- **`HybridTrioModel`:** доменный алгоритм, объединяющий статистический арбитраж на банковской паре `SBER` + `T` с импульсным пробоем каналов Дончяна на `OZON`. Защищен от фиктивных свечей выходного дня.
- **Порты (`StrategyPort`, `RecommendationPublisherPort`, `RegistrationPublisherPort`, `HeartbeatPublisherPort`):** абстрактные интерфейсы взаимодействия по принципу Dependency Inversion (DIP).

### 4. Infrastructure Layer
- **`KafkaConnectionManager`:** управление TLS-контекстом (проверка сертификата сервера `omsu.7bits.it`) и соединениями `aiokafka.AIOKafkaProducer` / `aiokafka.AIOKafkaConsumer`.
- **`KafkaPublisherAdapter`:** реализация доменных портов отправки сообщений в соответствующие топики Kafka.
- **`CandleMapper`, `RecommendationMapper`, `EventMapper`:** взаимная трансляция форматов платформы (включая общее поле `schemaVersion` и структуру событий) и внутренних DTO/сущностей.
- **`Settings`:** безопасная загрузка конфигурации из `.env` без раскрытия секретов.
