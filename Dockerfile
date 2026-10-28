# ─────────────────────────────────────────────
# Stage 1: builder — устанавливаем зависимости
# ─────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

# Переменные окружения для Poetry
ENV POETRY_VERSION=1.8.3 \
    POETRY_HOME="/opt/poetry" \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

ENV PATH="$POETRY_HOME/bin:$PATH"

# Устанавливаем только curl для загрузки Poetry (без gcc — не нужен для чистого Python)
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && curl -sSL https://install.python-poetry.org | python3 - \
    && rm -rf /var/lib/apt/lists/*

# Копируем только манифест зависимостей — слой кешируется при неизменном pyproject.toml
COPY pyproject.toml poetry.lock* ./

# Устанавливаем только production-зависимости, без корня пакета
RUN poetry install --only main --no-root --no-ansi

# ─────────────────────────────────────────────
# Stage 2: runner — минимальный runtime-образ
# ─────────────────────────────────────────────
FROM python:3.12-slim AS runner

WORKDIR /app

# Непривилегированный пользователь (defence-in-depth)
RUN groupadd --system appgroup && useradd --system --gid appgroup --no-create-home appuser

# Копируем установленные пакеты из builder-слоя
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Копируем только исходный код сервиса
COPY src/ ./src/

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"

CMD ["python", "-m", "uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000"]
