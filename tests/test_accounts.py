import pytest
from unittest.mock import AsyncMock, patch
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.main import app
from app.database import get_db
from app.models import Base
from app.kafka_producer import kafka_service

# SQLite In-memory engine for unit testing
SQLALCHEMY_DATABASE_URL = "sqlite:///:memory:"

engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base.metadata.create_all(bind=engine)


def override_get_db():
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db
client = TestClient(app)


@pytest.fixture(autouse=True)
def clean_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)


def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"
    assert "kafka" in data


@patch.object(kafka_service, "send_event", new_callable=AsyncMock)
def test_create_account_success(mock_send_event):
    payload = {
        "user_id": "usr-123e4567-e89b-12d3-a456-426614174000",
        "account_type": "SAVINGS",
        "balance": 150000.50,
        "currency": "COP",
        "user_email": "usuario@ejemplo.com"
    }

    response = client.post("/accounts", json=payload)
    assert response.status_code == 201

    data = response.json()
    assert data["user_id"] == payload["user_id"]
    assert data["account_type"] == "SAVINGS"
    assert float(data["balance"]) == 150000.50
    assert data["currency"] == "COP"
    assert data["status"] == "ACTIVE"
    assert "id" in data
    assert "account_number" in data
    assert "created_at" in data
    assert "updated_at" in data

    # Verify Kafka event was dispatched asynchronously via BackgroundTasks
    mock_send_event.assert_called_once()
    call_kwargs = mock_send_event.call_args.kwargs
    assert call_kwargs["topic"] == "account-created-topic"
    event_payload = call_kwargs["value"]
    assert event_payload["event"] == "ACCOUNT_CREATED"
    assert event_payload["data"]["id"] == data["id"]
    assert event_payload["data"]["account_number"] == data["account_number"]
    assert event_payload["data"]["user_email"] == "usuario@ejemplo.com"


@patch.object(kafka_service, "send_event", new_callable=AsyncMock)
def test_create_account_duplicate_account_number(mock_send_event):
    payload = {
        "user_id": "usr-12345",
        "account_number": "ACC-999888777",
        "account_type": "CHECKING"
    }

    # First creation -> OK
    response1 = client.post("/accounts", json=payload)
    assert response1.status_code == 201

    # Second creation with identical account_number -> 409 Conflict
    response2 = client.post("/accounts", json=payload)
    assert response2.status_code == 409
    assert "Ya existe una cuenta con el número" in response2.json()["detail"]


def test_get_account_by_id():
    # Create account
    create_payload = {
        "user_id": "usr-999",
        "account_type": "SAVINGS"
    }
    create_res = client.post("/accounts", json=create_payload)
    account_id = create_res.json()["id"]

    # Get account
    get_res = client.get(f"/accounts/{account_id}")
    assert get_res.status_code == 200
    assert get_res.json()["id"] == account_id
    assert get_res.json()["user_id"] == "usr-999"
