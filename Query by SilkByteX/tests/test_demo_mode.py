from __future__ import annotations

from yelp_text_to_sql import pipeline


def test_demo_sql_mapping_supports_project_example_questions() -> None:
    assert pipeline.get_demo_sql("Show the first 5 businesses") == "SELECT * FROM business LIMIT 5"
    assert pipeline.get_demo_sql("Count the number of reviews") == "SELECT COUNT(*) AS review_count FROM rating"
    assert "review_year" in pipeline.get_demo_sql("Count the number of reviews per year")
    assert "business_count" in pipeline.get_demo_sql("Show the top 10 cities by number of businesses")
    assert "review_count" in pipeline.get_demo_sql("Show the top 10 users by review count")


def test_demo_sql_mapping_handles_small_formatting_changes() -> None:
    sql = pipeline.get_demo_sql("Show the first 5 businesses!")

    assert sql == "SELECT * FROM business LIMIT 5"


def test_demo_mode_returns_supported_query_result_without_live_services(
    monkeypatch,
    make_query_result,
) -> None:
    monkeypatch.setattr(
        pipeline,
        "execute_sql",
        lambda sql: make_query_result(
            rows=[{"business_id": "b1", "name": "Cafe"}],
            executed=True,
            message="Demo query worked",
        ),
    )

    result = pipeline.run_natural_language_query(
        "Show the first 5 businesses",
        use_demo_mode=True,
    )

    assert result.used_demo_mode is True
    assert result.success is True
    assert result.status == "success"
    assert result.final_sql == "SELECT * FROM business LIMIT 5"
    assert result.rows == [{"business_id": "b1", "name": "Cafe"}]
    assert result.retry_happened is False


def test_demo_mode_returns_clear_message_for_unsupported_question() -> None:
    result = pipeline.run_natural_language_query(
        "Show me businesses with the highest stars in each city",
        use_demo_mode=True,
    )

    assert result.used_demo_mode is True
    assert result.success is False
    assert result.status == "demo_question_not_supported"
    assert "not supported" in result.error_message.lower()
    assert "Show the first 5 businesses" in result.result_message
