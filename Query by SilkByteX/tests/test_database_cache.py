from __future__ import annotations

from yelp_text_to_sql import database


def test_execute_sql_reuses_cached_result_for_identical_sql(monkeypatch) -> None:
    config = database.DatabaseConfig(
        engine="hive",
        hive_host="127.0.0.1",
        hive_port=10000,
        hive_database="yelp_db",
        hive_auth="NONE",
    )
    execution_calls: list[str] = []

    monkeypatch.setattr(database, "load_database_config", lambda: config)
    monkeypatch.setattr(database, "_validate_database_config", lambda current_config: None)

    def fake_execute_with_hive(sql: str, current_config: database.DatabaseConfig) -> database.QueryResult:
        execution_calls.append(sql)
        return database.QueryResult(
            rows=[{"city": "Phoenix", "business_count": 100}],
            executed=True,
            message="Hive query completed successfully. Returned 1 row(s).",
        )

    monkeypatch.setattr(database, "_execute_with_hive", fake_execute_with_hive)
    database.clear_query_result_cache()

    first_result = database.execute_sql("SELECT city, COUNT(*) AS business_count FROM business GROUP BY city LIMIT 10")
    second_result = database.execute_sql("SELECT city, COUNT(*) AS business_count FROM business GROUP BY city LIMIT 10")

    assert first_result.executed is True
    assert second_result.executed is True
    assert len(execution_calls) == 1
    assert "cache" in second_result.message.lower()
