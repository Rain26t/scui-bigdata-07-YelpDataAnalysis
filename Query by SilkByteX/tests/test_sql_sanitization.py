from __future__ import annotations

from yelp_text_to_sql.sql_sanitization import sanitize_sql


def test_sanitize_sql_extracts_sql_from_markdown_fence() -> None:
    raw_sql = """
    Here is your query:

    ```sql
    SELECT name FROM business LIMIT 5
    ```
    """

    assert sanitize_sql(raw_sql) == "SELECT name FROM business LIMIT 5"


def test_sanitize_sql_keeps_plain_sql_unchanged() -> None:
    raw_sql = "SELECT COUNT(*) AS review_count FROM rating"

    assert sanitize_sql(raw_sql) == raw_sql


def test_sanitize_sql_returns_empty_string_for_blank_input() -> None:
    assert sanitize_sql("   ") == ""


def test_sanitize_sql_uses_first_sql_code_block_when_multiple_exist() -> None:
    raw_sql = """
    ```sql
    SELECT city FROM business LIMIT 3
    ```

    ```sql
    SELECT name FROM users LIMIT 3
    ```
    """

    assert sanitize_sql(raw_sql) == "SELECT city FROM business LIMIT 3"


def test_sanitize_sql_removes_explanation_text_around_the_query() -> None:
    raw_sql = """
    SQL:
    SELECT COUNT(*) AS review_count
    FROM rating;

    Explanation: this counts all review rows.
    """

    assert sanitize_sql(raw_sql) == "SELECT COUNT(*) AS review_count FROM rating"


def test_sanitize_sql_returns_empty_string_when_no_query_is_possible() -> None:
    raw_sql = "-- TODO: The schema does not include the category mapping needed for this question."

    assert sanitize_sql(raw_sql) == ""
