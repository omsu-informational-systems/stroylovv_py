# Breakout Strategy Service

Отказоустойчивый серверный микросервис на Python (FastAPI + aiokafka + Clean Architecture / DDD) для обработки потока биржевых свечей, регистрации на обучающей платформе и генерации торговых рекомендаций (`BUY`, `SELL`, `HOLD`) на основе стратегии пробоя диапазона (Donchian Channels) и гибридного ансамбля статистического арбитража.

* **Автор:** Виталий Стройлов (учебная группа МИБ-401)
* **Репозиторий:** [omsu-informational-systems/stroylovv_py](https://github.com/omsu-informational-systems/stroylovv_py)
* **Проект в системе управления задачами:** [GitHub Issues & Tasks](https://github.com/omsu-informational-systems/stroylovv_py/issues)
* **План работ и критерии готовности (Kanban Board):** [docs/project-tasks.md](docs/project-tasks.md)
* **Проектная документация:** [docs/project-description.md](docs/project-description.md)
* **Архитектурные схемы в Git:**
  -  **[Контекстная схема (Context Diagram)](docs/context-diagram.md)** — взаимодействие сервиса, платформы, пользователей и брокера Kafka.
  -  **[Крупноузловая компонентная схема (Component Diagram)](docs/component-diagram.md)** — слои DDD, зоны ответственности компонентов и потоки данных.

---

## Торговая стратегия: Пробой диапазона (Donchian Breakout)

Сервис отслеживает границы ценового коридора по скользящему окну за последние 20 периодов:
1. **Upper Band:** Максимальная цена `High` за предыдущие 20 свечей.
2. **Lower Band:** Минимальная цена `Low` за предыдущие 20 свечей.
3. **Сигнал BUY:** Цена закрытия текущей свечи пробивает верхнюю границу (`Close > Upper_Band`).
4. **Сигнал SELL:** Цена закрытия текущей свечи пробивает нижнюю границу (`Close < Lower_Band`).
5. **Сигнал HOLD:** Цена находится внутри диапазона (флэт/консолидация) либо накоплено менее 20 свечей истории.

---

## 2. Структура событий и протокол платформы

* **Docker** (версия 24.0+)
* *Либо локально:* **Python 3.12+** и **Poetry 1.8+**

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

**Быстрая диагностика подключения к Kafka:**
```bash
poetry run python -m scripts.check_connection
```

**Проверка работоспособности (Healthcheck):**
```bash
curl http://localhost:8000/health
```

**Запуск полного набора модульных и интеграционных тестов:**
```bash
poetry run pytest
```
