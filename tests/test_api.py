from fastapi.testclient import TestClient
import pytest

from src.main import app


@pytest.fixture(scope="module")
def client() -> TestClient:
    return TestClient(app)


def test_process_candle_api_endpoint(client: TestClient) -> None:
    payload = {
        "ticker": "T",
        "timestamp": "2026-10-14T10:00:00Z",
        "open": 270.0,
        "high": 272.0,
        "low": 269.0,
        "close": 271.0,
        "volume": 250000.0,
        "interval": "1m",
    }
    response = client.post("/market/candles", json=payload)
    assert response.status_code == 200
    data = response.json()
    assert data["ticker"] == "T"
    assert data["signal"] in ["BUY", "SELL", "HOLD"]


def test_process_candle_invalid_payload(client: TestClient) -> None:
    # Отрицательная цена
    payload = {
        "ticker": "GAZP",
        "timestamp": "2026-10-14T10:00:00Z",
        "open": -10.0,
        "high": 110.0,
        "low": 104.0,
        "close": 108.0,
        "volume": 250000.0,
        "interval": "1d",
    }
    response = client.post("/market/candles", json=payload)
    assert response.status_code in (422, 400)

