from __future__ import annotations

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from yelp_text_to_sql.database import QueryResult
from yelp_text_to_sql.prompt_schema import PromptBundle
from yelp_text_to_sql.sql_generation import SQLGenerationResult


@pytest.fixture
def prompt_bundle() -> PromptBundle:
    """Return a simple reusable prompt bundle for pipeline tests."""
    return PromptBundle(
        system_prompt="Schema prompt for tests",
        user_prompt="Generate SQL for this question:\nTest question",
        schema_loaded=True,
    )


@pytest.fixture
def make_query_result():
    """Return a small helper for creating QueryResult objects."""

    def _make(
        *,
        rows: list[dict] | None = None,
        executed: bool = False,
        error: str | None = None,
        message: str = "Test message",
    ) -> QueryResult:
        return QueryResult(
            rows=rows or [],
            executed=executed,
            error=error,
            message=message,
        )

    return _make


@pytest.fixture
def make_generation_result():
    """Return a helper for creating SQLGenerationResult objects."""

    def _make(
        *,
        sql: str = "",
        raw_response: str = "",
        notes: str = "Test note",
        explanation: str = "",
    ) -> SQLGenerationResult:
        return SQLGenerationResult(
            sql=sql,
            raw_response=raw_response,
            notes=notes,
            explanation=explanation,
        )

    return _make
