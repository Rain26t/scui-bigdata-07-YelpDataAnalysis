from __future__ import annotations

from yelp_text_to_sql import sql_generation


def test_extract_sql_and_explanation_reads_json_payload() -> None:
    sql_text, explanation_text = sql_generation._extract_sql_and_explanation(
        """
        {
            "sql": "SELECT AVG(stars) AS average_stars FROM business",
            "explanation": "This looks at the stars numbers in the business table. It averages them together to give one simple score. Extra sentence."
        }
        """
    )

    assert sql_text == "SELECT AVG(stars) AS average_stars FROM business"
    assert explanation_text == (
        "This looks at the stars numbers in the business table. "
        "It averages them together to give one simple score."
    )


def test_extract_sql_and_explanation_falls_back_to_plain_sql() -> None:
    sql_text, explanation_text = sql_generation._extract_sql_and_explanation(
        "```sql\nSELECT * FROM business LIMIT 5\n```"
    )

    assert sql_text == "SELECT * FROM business LIMIT 5"
    assert explanation_text == ""


def test_build_generation_request_text_includes_recent_context() -> None:
    request_text = sql_generation._build_generation_request_text(
        "Now filter those to only show 5-star businesses",
        recent_context=[
            {
                "question": "Show the top 10 cities by number of businesses",
                "sql": (
                    "SELECT city, COUNT(*) AS business_count "
                    "FROM business GROUP BY city ORDER BY business_count DESC LIMIT 10"
                ),
                "rows": [
                    {"city": "Phoenix", "business_count": 100},
                    {"city": "Toronto", "business_count": 98},
                ],
                "error": "",
                "message": "Query succeeded",
            }
        ],
    )

    assert "Recent conversation memory:" in request_text
    assert "Show the top 10 cities by number of businesses" in request_text
    assert "Returned 2 row(s)." in request_text
    assert "Now filter those to only show 5-star businesses" in request_text


def test_match_golden_query_template_detects_yearly_review_counts() -> None:
    template = sql_generation._get_golden_query_template("Count the number of reviews per year")

    assert template is not None
    assert template.key == "reviews_per_year"
    assert "FROM rating" in template.sql


def test_generate_sql_uses_golden_query_fast_path(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for golden-query intents.")

    monkeypatch.setattr(sql_generation, "_request_model_text", fail_if_called)

    result = sql_generation.generate_sql(
        "Rank most frequent Chinese restaurants",
        system_prompt="unused in the golden fast path",
    )

    assert "array_contains(categories, 'Chinese')" in result.sql
    assert "golden query cheat sheet fast path" in result.notes.lower()


def test_generate_sql_uses_turnaround_merchants_fast_path(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for golden-query intents.")

    monkeypatch.setattr(sql_generation, "_request_model_text", fail_if_called)

    result = sql_generation.generate_sql(
        "Find turnaround merchants",
        system_prompt="unused in the golden fast path",
    )

    assert "DATE_SUB(CURRENT_DATE(), 365)" in result.sql
    assert "rating_improvement" in result.sql


def test_generate_sql_uses_category_synergy_fast_path(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for golden-query intents.")

    monkeypatch.setattr(sql_generation, "_request_model_text", fail_if_called)

    result = sql_generation.generate_sql(
        "Show the top category synergy pairs",
        system_prompt="unused in the golden fast path",
    )

    assert "EXPLODE(categories)" in result.sql
    assert "left_side.category < right_side.category" in result.sql


def test_generate_sql_uses_top_reviewers_fast_path(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for golden-query intents.")

    monkeypatch.setattr(sql_generation, "_request_model_text", fail_if_called)

    result = sql_generation.generate_sql(
        "Identify top reviewers based on review_count",
        system_prompt="unused in the golden fast path",
    )

    assert "FROM users" in result.sql
    assert "ORDER BY review_count DESC" in result.sql


def test_generate_sql_uses_elite_impact_fast_path(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for golden-query intents.")

    monkeypatch.setattr(sql_generation, "_request_model_text", fail_if_called)

    result = sql_generation.generate_sql(
        "Identify elite status impact (average word count and useful votes BEFORE vs AFTER becoming elite).",
        system_prompt="unused in the golden fast path",
    )

    assert "first_elite_year" in result.sql
    assert "useful_vote_lift" in result.sql


def test_generate_sql_uses_yearly_stats_fast_path_with_schema_safe_tip_placeholder(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for golden-query intents.")

    monkeypatch.setattr(sql_generation, "_request_model_text", fail_if_called)

    result = sql_generation.generate_sql(
        "Compute the yearly statistics of new users, number of reviews, elite users, tips, and check-ins.",
        system_prompt="unused in the golden fast path",
    )

    assert "CAST(NULL AS BIGINT) AS tip_count_unavailable" in result.sql
    assert "checkin_count" in result.sql


def test_generate_sql_uses_checkins_per_year_fast_path(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for golden-query intents.")

    monkeypatch.setattr(sql_generation, "_request_model_text", fail_if_called)

    result = sql_generation.generate_sql(
        "Count the number of check-ins per year",
        system_prompt="unused in the golden fast path",
    )

    assert "FROM checkin" in result.sql
    assert "YEAR(TO_TIMESTAMP(TRIM(checkin_ts))) AS checkin_year" in result.sql


def test_generate_sql_uses_top_merchants_combined_metrics_fast_path(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for golden-query intents.")

    monkeypatch.setattr(sql_generation, "_request_model_text", fail_if_called)

    result = sql_generation.generate_sql(
        "Identify the top 5 merchants based on combined metrics of rating frequency, average rating, and check-in frequency",
        system_prompt="unused in the golden fast path",
    )

    assert "combined_rank_score" in result.sql
    assert "rating_frequency_rank + average_rating_rank + checkin_frequency_rank" in result.sql


def test_generate_sql_uses_post_review_checkin_dropoff_fast_path(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for golden-query intents.")

    monkeypatch.setattr(sql_generation, "_request_model_text", fail_if_called)

    result = sql_generation.generate_sql(
        "Analyze post-review check-in drop-off: percentage drop in check-ins following a sudden spike in 1-star reviews",
        system_prompt="unused in the golden fast path",
    )

    assert "one_star_review_count >= 5" in result.sql
    assert "checkin_drop_percent" in result.sql


def test_research_hypothesis_template_detects_weather_prompt() -> None:
    template = sql_generation._get_research_hypothesis_template(
        "Does weather affect restaurant ratings?"
    )

    assert template is not None
    assert template.key == "weather_mood_hypothesis"
    assert "weather_csv_path" in template.code


def test_generate_general_chat_reply_uses_research_hypothesis_fast_path(monkeypatch) -> None:
    def fail_if_called(*_args, **_kwargs):
        raise AssertionError("The model should not be called for research-hypothesis prompts.")

    monkeypatch.setattr(sql_generation, "_request_model_messages", fail_if_called)

    reply = sql_generation.generate_general_chat_reply("cursed storefronts")

    assert "CURSED STOREFRONTS HYPOTHESIS" in reply
    assert "walkscore_api_key" in reply
