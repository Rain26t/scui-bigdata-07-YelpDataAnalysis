from __future__ import annotations

from yelp_text_to_sql import prompt_schema


def test_text_to_sql_system_prompt_includes_pdf_grounding_sections() -> None:
    prompt_text = prompt_schema.build_text_to_sql_system_prompt()

    assert "Full Yelp schema:" in prompt_text
    assert (
        "TABLE: business (business_id, name, address, city, state, postal_code, latitude, longitude, stars, review_count, is_open, attributes, categories, hours)"
        in prompt_text
    )
    assert (
        "TABLE: rating (review_id, user_id, business_id, stars, date, text, useful, funny, cool)"
        in prompt_text
    )
    assert (
        "TABLE: users (user_id, name, review_count, yelping_since, friends, useful, funny, cool, fans, elite, average_stars)"
        in prompt_text
    )
    assert "TABLE: checkin (business_id, date)" in prompt_text
    assert "Business glossary and dataset semantics:" in prompt_text
    assert "Common question families from the project brief:" in prompt_text
    assert "Scope guardrails:" in prompt_text
    assert "merchant, business, storefront, and restaurant" in prompt_text
    assert "top cities or states by merchant count" in prompt_text
    assert "Do not invent external tables or join paths" in prompt_text


def test_text_to_sql_system_prompt_includes_cuisine_and_rating_semantics() -> None:
    prompt_text = prompt_schema.build_text_to_sql_system_prompt()

    assert 'The broad restaurant category is "Restaurants".' in prompt_text
    assert "business.stars is the average rating of a business." in prompt_text
    assert "rating.stars is the rating inside one individual review." in prompt_text
    assert "users joining each year" in prompt_text


def test_text_to_sql_system_prompt_includes_performance_constraints() -> None:
    prompt_text = prompt_schema.build_text_to_sql_system_prompt()

    assert "Strict output rules:" in prompt_text
    assert "Output ONLY raw executable SQL. No markdown, no backtick wrappers, no explanation text." in prompt_text
    assert "ALWAYS include a LIMIT clause (default LIMIT 20) unless the user explicitly asks for all results." in prompt_text
    assert "The review table is named rating, NOT review. Never use FROM review." in prompt_text
    assert "Filter early with WHERE before applying GROUP BY or JOIN." in prompt_text
    assert "Performance rules are mandatory:" in prompt_text
    assert "Default to LIMIT 20 for ordinary result sets." in prompt_text
    assert "Filter early with WHERE clauses" in prompt_text
    assert "Never use SELECT * inside subqueries or CTEs" in prompt_text


def test_text_to_sql_system_prompt_includes_dataset_quirks_and_examples() -> None:
    prompt_text = prompt_schema.build_text_to_sql_system_prompt()

    assert "Critical dataset quirks you must obey:" in prompt_text
    assert "array_contains(business.categories, 'Pizza')" in prompt_text
    assert "business.review_count > 50" in prompt_text
    assert "business.is_open uses 1 for open and 0 for closed" in prompt_text
    assert "The review table is named rating, not review." in prompt_text
    assert 'User: "What are the top 5 pizza places in Las Vegas?"' in prompt_text
    assert "WHERE b.name LIKE '%Starbucks%'" in prompt_text


def test_text_to_sql_system_prompt_includes_golden_query_cheat_sheet() -> None:
    prompt_text = prompt_schema.build_text_to_sql_system_prompt()

    assert "Golden Query Cheat Sheet:" in prompt_text
    assert 'USER INTENT: "Count the number of reviews per year"' in prompt_text
    assert "YEAR(TO_DATE(date)) AS review_year" in prompt_text
    assert 'USER INTENT: "Rank most frequent Chinese restaurants"' in prompt_text
    assert "array_contains(categories, 'Chinese')" in prompt_text
    assert 'USER INTENT: "Analyze the number of users joining each year"' in prompt_text
    assert 'USER INTENT: "Identify elite status impact (average word count and useful votes BEFORE vs AFTER becoming elite)."' in prompt_text
    assert 'USER INTENT: "Analyze the distribution of ratings (1-5 stars)"' in prompt_text
    assert 'USER INTENT: "Count the number of check-ins per year"' in prompt_text
    assert 'USER INTENT: "Analyze post-review check-in drop-off: percentage drop in check-ins following a sudden spike in 1-star reviews"' in prompt_text


def test_text_to_sql_system_prompt_includes_project_definitions() -> None:
    prompt_text = prompt_schema.build_text_to_sql_system_prompt()

    assert "Critical project definitions you must memorize:" in prompt_text
    assert "WHERE state IN ('AL', 'AK', 'AZ'" in prompt_text
    assert "the parent restaurant category is exactly 'Restaurants'" in prompt_text
    assert "recent_avg - historical_avg >= 1.0" in prompt_text
    assert "cat1 < cat2 to avoid duplicates" in prompt_text
    assert "COUNT(stars) > 50" in prompt_text
    assert "current live schema does not include a tips table" in prompt_text
