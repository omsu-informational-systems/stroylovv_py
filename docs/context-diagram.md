# Контекстная схема системы (Context Diagram)

Контекстная схема описывает границы сервиса торговой стратегии (`breakout-strategy`), его пользователей, внешние системы и протоколы взаимодействия.

```mermaid
flowchart TB
    subgraph Users ["Пользователи"]
        Student["Студент / Разработчик<br/>(Виталий Стройлов, группа МИБ-401)"]
        Teacher["Преподаватель / Администратор<br/>(Куратор курса)"]
    end

    subgraph CoreService ["Границы разрабатываемого проекта"]
        StrategyService["Сервис торговой стратегии<br/>(breakout-strategy)<br/>• Расчет каналов Дончяна & гибридного арбитража<br/>• Генерация сигналов BUY / SELL / HOLD<br/>• Отказоустойчивое чтение и публикация"]
    end

    subgraph ExternalSystems ["Внешние системы и окружение"]
        KafkaBroker["Брокер сообщений Apache Kafka<br/>(omsu.7bits.it:9094, SASL_SSL SCRAM-SHA-256)<br/>Центральная шина обмена событиями"]
        Platform["Преподавательская платформа<br/>(Trading & Evaluation Platform)<br/>• Регистрация студентов и сервисов<br/>• Ведение виртуального портфеля<br/>• Оценка финансового результата"]
        CandleFeed["Генератор рыночных данных<br/>(Market Data Feed / Replay Engine)<br/>• Поставка биржевых свечей MOEX"]
        Orchestrator["Система мониторинга и контейнеризации<br/>(Docker / Health Probe)"]
    end

    %% Взаимодействия пользователей
    Student -->|"1. Настройка окружения (.env), запуск контейнера, анализ журналов"| StrategyService
    Teacher -->|"2. Выдача ключа доступа, модерация аккаунтов, оценка стратегии"| Platform
    Teacher -->|"3. Запуск воспроизведения потока котировок (Replay)"| CandleFeed

    %% Потоки свечей
    CandleFeed -->|"4. Публикация market.candle.closed"| KafkaBroker
    KafkaBroker -->|"5. Потребление market.candle.closed (CANDLE_TOPIC)"| StrategyService

    %% Регистрация и Heartbeat
    StrategyService -->|"6. Публикация account.registration.request"| KafkaBroker
    KafkaBroker -->|"7. Чтение account.registration.request"| Platform
    Platform -->|"8. Публикация account.registration.result"| KafkaBroker
    KafkaBroker -->|"9. Потребление account.registration.result"| StrategyService
    StrategyService -->|"10. Периодическая публикация account.service.heartbeat (каждые 10с)"| KafkaBroker
    KafkaBroker -->|"11. Мониторинг активности сервиса"| Platform

    %% Торговые рекомендации
    StrategyService -->|"12. Публикация strategy.recommendation.created (BUY/SELL/HOLD)"| KafkaBroker
    KafkaBroker -->|"13. Учет сигналов и исполнение сделок в виртуальном портфеле"| Platform

    %% Healthcheck
    Orchestrator -->|"14. HTTP GET /health (Liveness/Readiness Probe)"| StrategyService
```

---

## Описание взаимодействий

| № | Источник | Приемник | Протокол / Топик | Назначение и передаваемые данные |
| :- | :--- | :--- | :--- | :--- |
| **1** | Студент | `breakout-strategy` | CLI / Git / Docker | Конфигурирование параметров стратегии через `.env`, локальный запуск и просмотр структурированных логов. |
| **2** | Преподаватель | Платформа | Web UI / Admin API | Выдача персональной пары «ключ / секрет», согласование топика свечей, проверка списка зарегистрированных студентов. |
| **3** | Преподаватель | Candle Feed | Внутренний триггер | Запуск стенда Replay исторических срезов Мосбиржи. |
| **4** | Candle Feed | Kafka Broker | Kafka (`market.candle.default`) | Поставка потока сформированных дневных свечей (`market.candle.closed`). |
| **5** | Kafka Broker | `breakout-strategy` | Kafka (Consumer group `candles-student-key-05`) | Чтение свечей целевых тикеров (`SBER`, `T`, `OZON` и др.) с ручным подтверждением смещения (At-Least-Once). |
| **6-7**| `breakout-strategy` | Платформа | Kafka (`account.registration.request`) | Запрос регистрации при старте сервиса со стабильными идентификаторами (`secretKey`, `accountName`, `groupId`, `serviceName`, `strategyName`). |
| **8-9**| Платформа | `breakout-strategy` | Kafka (`account.registration.result`) | Ответ на запрос регистрации (`success: bool`, `reason: str`). При отказе сервис блокирует публикацию рекомендаций. |
| **10-11**| `breakout-strategy`| Платформа | Kafka (`account.service.heartbeat`) | Периодический сигнал жизнедеятельности сервиса (каждые 10 секунд). |
| **12-13**| `breakout-strategy`| Платформа | Kafka (`strategy.recommendation.created`) | Отправка торговых рекомендаций (`BUY`, `SELL`, `HOLD`) с указанием расчетных границ канала и метрики `confidence`. |
| **14**| Orchestrator | `breakout-strategy` | HTTP (`GET /health`) | Проверка состояния сервиса и доступности брокера Kafka. |

