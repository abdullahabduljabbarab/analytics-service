import pytest
from fastapi.testclient import TestClient

from app.main import app, get_analytics_store
from app.store import InMemoryStore


@pytest.fixture
def store():
    """A fresh in-memory store per test, shared between the API dependency and
    any direct admin refresh/rebuild calls in the test."""
    return InMemoryStore()


@pytest.fixture
def client(store):
    app.dependency_overrides[get_analytics_store] = lambda: store
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()
