from __future__ import annotations

import pytest

fastapi = pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from yelp_text_to_sql.api import app


def test_health_endpoint_returns_ok() -> None:
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert "database_engine" in payload


def test_schema_endpoint_returns_tables() -> None:
    client = TestClient(app)

    response = client.get("/schema")

    assert response.status_code == 200
    payload = response.json()
    assert "tables" in payload
    assert "business" in payload["tables"]
