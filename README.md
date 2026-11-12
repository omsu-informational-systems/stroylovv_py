# Breakout Strategy Service

Отказоустойчивый серверный микросервис на Python (FastAPI + aiokafka + Clean Architecture / DDD) для обработки потока биржевых свечей, регистрации на обучающей платформе и генерации торговых рекомендаций (`BUY`, `SELL`, `HOLD`) на основе стратегии пробоя диапазона (Donchian Channels) и гибридного ансамбля статистического арбитража.

* **Автор:** Виталий Стройлов (учебная группа МОБ-301)
* **Репозиторий:** [omsu-informational-systems/stroylovv_py](https://github.com/omsu-informational-systems/stroylovv_py)
* **Проект в системе управления задачами:** [GitHub Issues & Tasks](https://github.com/omsu-informational-systems/stroylovv_py/issues)
* **План работ и критерии готовности (Kanban Board):** [docs/project-tasks.md](docs/project-tasks.md)
* **Проектная документация:** [docs/project-description.md](docs/project-description.md)
* **Архитектурные схемы в Git:**
  - 🗺️ **[Контекстная схема (Context Diagram)](docs/context-diagram.md)** — взаимодействие сервиса, платформы, пользователей и брокера Kafka.
  - 🧩 **[Крупноузловая компонентная схема (Component Diagram)](docs/component-diagram.md)** — слои DDD, зоны ответственности компонентов и потоки данных.

---

## 1. Задачи проекта и критерии готовности

План работ разбит на этапы в соответствии с требованиями учебной платформы (подробные критерии приемки каждой задачи описаны в [docs/project-tasks.md](docs/project-tasks.md)):

1. **TASK-01:** Подключение к брокеру Kafka по защищенному протоколу SASL_SSL / SCRAM-SHA-256.
2. **TASK-02:** Отправка запроса `account.registration.request` со стабильными идентификаторами студента (`accountName`), учебной группы (`groupId`), сервиса (`serviceName`), стратегии (`strategyName`) и ключа `secretKey`, а также обработка `account.registration.result`.
3. **TASK-03:** Потребление потока закрытых свечей `market.candle.closed` из топика `CANDLE_TOPIC` с уникальным `group.id` (`candles-student-key-05`) и At-Least-Once семантикой.
4. **TASK-04:** Публикация базовой торговой рекомендации `HOLD` в топик `strategy.recommendation.created` при формировании окна и во флэте.
5. **TASK-05:** Алгоритмический расчет ценовых коридоров Дончяна и гибридного арбитража (`SBER` + `T` + `OZON`).
6. **TASK-06:** Отказоустойчивость: автоматическое переподключение к брокеру при сетевых сбоях и перезапусках без остановки процесса сервиса.

---

## 2. Структура событий и протокол платформы

Все сообщения содержат общую структуру и заголовок `schemaVersion: "1.0.0"`:

| Событие (`eventType`) | Топик Kafka | Действие сервиса | Назначение |
| :--- | :--- | :---: | :--- |
| `account.registration.request` | `account.registration.request` | Публикация | Запрос регистрации сервиса со стабильными данными студента и стратегии |
| `account.registration.result` | `account.registration.result` | Чтение | Обработка ответа. При отказе сервис блокирует публикацию рекомендаций |
| `account.service.heartbeat` | `account.service.heartbeat` | Публикация | Периодический сигнал доступности сервиса (каждые 10 секунд) |
| `market.candle.closed` | `CANDLE_TOPIC` (`market.candle.default`) | Чтение | Поток закрытых биржевых свечей для алгоритмического анализа |
| `strategy.recommendation.created` | `strategy.recommendation.created` | Публикация | Торговые рекомендации `BUY` / `SELL` / `HOLD` |

---

## 3. Требования к окружению и запуск

### Требования
* **Docker** (версия 24.0+) и **Docker Compose**
* *Либо локально:* **Python 3.12+** и менеджер зависимостей **Poetry 2.0+**

### Настройка конфигурации (.env)
Создайте локальный файл `.env` в корне проекта (файл добавлен в `.gitignore` и никогда не попадает в репозиторий):
```ini
APP__NAME=breakout-strategy
ACCOUNT_NAME=Виталий Стройлов
GROUP_ID=МОБ-301
STRATEGY_NAME=hybrid-breakout

KAFKA_BOOTSTRAP_SERVERS=omsu.7bits.it:9094
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_USERNAME=student-key-05
KAFKA_PASSWORD=<ваш_выданный_секрет_от_преподавателя>
SECRET_KEY=student-key-05
KAFKA_GROUP_ID=candles-student-key-05
CANDLE_TOPIC=market.candle.default
```

> [!IMPORTANT]
> В поле `SECRET_KEY` указывается выданный ключ (логин), а не пароль.
> Пароль `KAFKA_PASSWORD` хранится исключительно в локальном `.env` и никогда не публикуется в Git, журналы или скриншоты.

---

### Запуск сервиса

**Локальный запуск через Poetry:**
```bash
poetry install
poetry run python -m src.main
```

**Сборка и запуск в Docker:**
```bash
docker compose up --build -d
```

**Проверка работоспособности (Healthcheck):**
```bash
curl http://localhost:8000/health
```

**Запуск полного набора модульных и интеграционных тестов:**
```bash
poetry run pytest
```
