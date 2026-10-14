FROM python:3.12-slim AS builder

WORKDIR /app

# Установка Poetry в изолированный слой
ENV POETRY_VERSION=1.8.3 \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml poetry.lock* ./

# Установка зависимостей без dev-пакетов непосредственно в системный python сборщика
RUN poetry install --only main --no-root

# Финальный runtime-образ
FROM python:3.12-slim AS runner

WORKDIR /app

# Создание непривилегированного пользователя
RUN groupadd -r appuser && useradd -r -g appuser -s /bin/false appuser

# Копирование установленных пакетов из слоя builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY src/ /app/src/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8000

USER appuser

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]