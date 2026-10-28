"""
Интеграционные тесты эндпоинта GET /health.

Проверяет:
- HTTP 200
- Content-Type: application/json
- Структуру тела ответа согласно спецификации платформы
"""

import pytest
from fastapi.testclient import TestClient

from src.main import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


def test_health_returns_200(client: TestClient) -> None:
    response = client.get("/health")
    assert response.status_code == 200


def test_health_content_type_is_json(client: TestClient) -> None:
    response = client.get("/health")
    assert "application/json" in response.headers["content-type"]


def test_health_response_schema(client: TestClient) -> None:
    data = client.get("/health").json()

    assert data["status"] in ("UP", "DEGRADED")
    assert isinstance(data["service"], str)
    assert isinstance(data["version"], str)
    assert isinstance(data["components"], dict)


def test_health_no_secrets_in_response(client: TestClient) -> None:
    """Ответ не должен содержать секреты или трассировку стека."""
    text = client.get("/health").text
    assert "traceback" not in text.lower()
    assert "password" not in text.lower()
    assert "secret" not in text.lower()
