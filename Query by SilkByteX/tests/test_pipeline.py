from __future__ import annotations

from yelp_text_to_sql import pipeline, sql_generation
from yelp_text_to_sql.config import AppConfig


def test_pipeline_returns_input_error_for_blank_question() -> None:
    result = pipeline.run_natural_language_query("   ", use_demo_mode=False)

    assert result.success is False
    assert result.status == "input_error"
    assert "enter a natural-language question" in result.error_message.lower()
    assert result.retry_happened is False


def test_pipeline_live_mode_returns_generation_error_when_sql_is_missing(
    monkeypatch,
    prompt_bundle,
    make_generation_result,
) -> None:
    monkeypatch.setattr(pipeline, "build_prompt_bundle", lambda question: prompt_bundle)
    monkeypatch.setattr(
        pipeline,
        "generate_sql",
        lambda question, system_prompt, recent_context=None: make_generation_result(
            sql="",
            notes="Live SQL generation is not configured yet. Missing: DEEPSEEK_MODEL.",
        ),
    )

    result = pipeline.run_natural_language_query(
        "Show the first 5 businesses",
        use_demo_mode=False,
    )

    assert result.success is False
    assert result.status == "generation_error"
    assert result.error_message == "Live SQL generation is not configured yet."
    assert result.retry_happened is False


def test_pipeline_live_mode_success_without_retry(
    monkeypatch,
    prompt_bundle,
    make_generation_result,
    make_query_result,
) -> None:
    monkeypatch.setattr(pipeline, "build_prompt_bundle", lambda question: prompt_bundle)
    monkeypatch.setattr(
        pipeline,
        "generate_sql",
        lambda question, system_prompt, recent_context=None: make_generation_result(
            sql="```sql\nSELECT 1 AS value\n```",
            notes="SQL generated successfully.",
            explanation=(
                "This asks the database for one tiny row with the number 1 in it. "
                "It is a simple way to prove the SQL ran successfully."
            ),
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "execute_sql",
        lambda sql: make_query_result(
            rows=[{"value": 1}],
            executed=True,
            message="Query succeeded",
        ),
    )

    result = pipeline.run_natural_language_query(
        "Show the first 5 businesses",
        use_demo_mode=False,
    )

    assert result.success is True
    assert result.status == "success"
    assert result.generated_sql == "SELECT 1 AS value"
    assert result.final_sql == "SELECT 1 AS value"
    assert result.generated_sql_explanation.startswith("This asks the database")
    assert result.final_sql_explanation.startswith("This asks the database")
    assert result.corrected_sql == ""
    assert result.retry_happened is False
    assert result.rows == [{"value": 1}]
    assert result.retry_status == "Succeeded on attempt 1."


def test_pipeline_live_mode_retries_once_and_uses_corrected_sql(
    monkeypatch,
    prompt_bundle,
    make_generation_result,
    make_query_result,
) -> None:
    monkeypatch.setattr(pipeline, "build_prompt_bundle", lambda question: prompt_bundle)
    monkeypatch.setattr(
        pipeline,
        "generate_sql",
        lambda question, system_prompt, recent_context=None: make_generation_result(
            sql="SELECT bad_column FROM business",
            notes="Initial SQL generated",
            explanation=(
                "This first try asks the database for a column that does not really exist. "
                "That mistake is why the database sends back an error."
            ),
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "generate_corrected_sql",
        lambda question, failed_sql, database_error, system_prompt, recent_context=None: make_generation_result(
            sql="SELECT name FROM business",
            notes="Corrected SQL generated",
            explanation=(
                "The fix switches to the real name column in the business table. "
                "That lets the database return business names instead of failing."
            ),
        ),
    )

    query_results = [
        make_query_result(
            error="Unknown column bad_column",
            message="First query failed",
        ),
        make_query_result(
            rows=[{"name": "Cafe"}],
            executed=True,
            message="Corrected query succeeded",
        ),
    ]

    def fake_execute_sql(sql: str):
        return query_results.pop(0)

    monkeypatch.setattr(pipeline, "execute_sql", fake_execute_sql)

    result = pipeline.run_natural_language_query(
        "Show the first 5 businesses",
        use_demo_mode=False,
    )

    assert result.success is True
    assert result.retry_happened is True
    assert result.generated_sql == "SELECT bad_column FROM business"
    assert result.corrected_sql == "SELECT name FROM business"
    assert result.final_sql == "SELECT name FROM business"
    assert result.generated_sql_explanation.startswith("This first try")
    assert result.corrected_sql_explanation.startswith("The fix switches")
    assert result.final_sql_explanation.startswith("The fix switches")
    assert result.rows == [{"name": "Cafe"}]
    assert result.retry_status == "Succeeded on corrected attempt 2."


def test_pipeline_passes_recent_context_into_generation(
    monkeypatch,
    prompt_bundle,
    make_generation_result,
    make_query_result,
) -> None:
    captured_context: list[dict] = []
    recent_context = [
        {
            "question": "Show the top 10 cities by number of businesses",
            "sql": (
                "SELECT city, COUNT(*) AS business_count "
                "FROM business GROUP BY city ORDER BY business_count DESC LIMIT 10"
            ),
            "rows": [{"city": "Phoenix", "business_count": 100}],
            "error": "",
            "message": "Query succeeded",
        }
    ]

    monkeypatch.setattr(pipeline, "build_prompt_bundle", lambda question: prompt_bundle)

    def fake_generate_sql(question, system_prompt, recent_context=None):
        captured_context.extend(recent_context or [])
        return make_generation_result(
            sql="SELECT city FROM business WHERE stars = 5 LIMIT 10",
            notes="SQL generated successfully.",
            explanation=(
                "This keeps looking at businesses and adds a filter for 5-star entries. "
                "Then it returns a short list of matching cities."
            ),
        )

    monkeypatch.setattr(pipeline, "generate_sql", fake_generate_sql)
    monkeypatch.setattr(
        pipeline,
        "execute_sql",
        lambda sql: make_query_result(
            rows=[{"city": "Phoenix"}],
            executed=True,
            message="Query succeeded",
        ),
    )

    result = pipeline.run_natural_language_query(
        "Now filter those to only show 5-star businesses",
        use_demo_mode=False,
        recent_context=recent_context,
    )

    assert result.success is True
    assert captured_context == recent_context


def test_pipeline_emits_progress_updates_in_order(
    monkeypatch,
    prompt_bundle,
    make_generation_result,
    make_query_result,
) -> None:
    seen_updates: list[tuple[str, str]] = []

    monkeypatch.setattr(pipeline, "build_prompt_bundle", lambda question: prompt_bundle)
    monkeypatch.setattr(
        pipeline,
        "generate_sql",
        lambda question, system_prompt, recent_context=None: make_generation_result(
            sql="SELECT 1 AS value",
            notes="SQL generated successfully.",
            explanation="This returns one row with the number 1 in it.",
        ),
    )
    monkeypatch.setattr(
        pipeline,
        "execute_sql",
        lambda sql: make_query_result(
            rows=[{"value": 1}],
            executed=True,
            message="Query succeeded",
        ),
    )

    result = pipeline.run_natural_language_query(
        "Show the first 5 businesses",
        use_demo_mode=False,
        progress_callback=lambda phase, note: seen_updates.append((phase, note)),
    )

    assert result.success is True
    assert [phase for phase, _note in seen_updates] == [
        pipeline.PIPELINE_PHASE_USER_INTENT,
        pipeline.PIPELINE_PHASE_SCHEMA_MAPPING,
        pipeline.PIPELINE_PHASE_SQL_GENERATION,
        pipeline.PIPELINE_PHASE_DATA_EXECUTION,
    ]
    assert all(note for _phase, note in seen_updates)


def test_generate_sql_handles_missing_live_config_without_real_api(monkeypatch) -> None:
    monkeypatch.setattr(
        sql_generation,
        "load_config",
        lambda: AppConfig(deepseek_api_key="", deepseek_model=""),
    )
    monkeypatch.setattr(sql_generation, "has_live_model_config", lambda config: False)

    result = sql_generation.generate_sql(
        "Generate SQL for this question:\nShow the first 5 businesses",
        "Schema prompt",
    )

    assert result.sql == ""
    assert "Live SQL generation is not configured yet." in result.notes
    assert "DEEPSEEK_API_KEY" in result.notes
