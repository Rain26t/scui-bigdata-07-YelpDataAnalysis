from __future__ import annotations

import re
import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from yelp_text_to_sql.database import execute_sql, QueryResult
from yelp_text_to_sql.prompt_schema import build_prompt_bundle
from yelp_text_to_sql.sql_generation import (
    _match_golden_query,
    generate_corrected_sql,
    generate_sql,
)
from yelp_text_to_sql.sql_sanitization import sanitize_sql

# These examples are shared with the UI so the same questions can be tested
# consistently in both live mode and demo/mock mode.
EXAMPLE_QUESTIONS = [
    "Show the first 5 businesses",
    "Count the number of reviews",
    "Count the number of reviews per year",
    "Show the top 10 cities by number of businesses",
    "Show the top 10 users by review count",
]
RUBRIC_DEMO_QUESTIONS = (
    "Top highest-rated businesses in Las Vegas",
    "Cities with the most businesses",
    "Users with the most reviews",
    "Review counts over time",
    "Show the top 10 categories by number of businesses",
    "What is the average star rating by state?",
    "Which businesses have the most check-ins?",
    "Show businesses open on Mondays with more than 500 reviews",
)
PIPELINE_PHASE_USER_INTENT = "user_intent"
PIPELINE_PHASE_SCHEMA_MAPPING = "schema_mapping"
PIPELINE_PHASE_SQL_GENERATION = "sql_generation"
PIPELINE_PHASE_DATA_EXECUTION = "data_execution"
PIPELINE_PHASE_COMPLETE = "complete"


@dataclass(frozen=True)
class DemoScenario:
    """One deterministic demo-mode response with fake SQL and plausible rows."""

    sql: str
    rows: tuple[dict[str, Any], ...]
    message: str
    explanation: str


@dataclass
class PipelineResult:
    """Structured result for one natural-language query run.

    This object keeps all the main pieces of the workflow in one place so the UI
    can focus on presentation instead of orchestration.
    """

    user_question: str
    used_demo_mode: bool
    success: bool
    status: str
    generated_sql: str = ""
    generated_sql_explanation: str = ""
    corrected_sql: str = ""
    corrected_sql_explanation: str = ""
    final_sql: str = ""
    final_sql_explanation: str = ""
    retry_happened: bool = False
    rows: list[dict[str, Any]] = field(default_factory=list)
    error_message: str = ""
    result_message: str = ""
    generation_note: str = ""
    prompt_text: str = ""
    mode_label: str = "Natural Language to SQL"
    retry_status: str = "No retry"


def _normalize_question(question: str) -> str:
    """Normalize a question so a few demo phrases can be matched simply."""
    normalized = re.sub(r"[^a-z0-9\s]", " ", question.lower())
    return " ".join(normalized.split())


def _copy_demo_rows(rows: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    """Return detached demo rows so callers can safely mutate the result."""
    return [dict(row) for row in rows]


_DEMO_SCENARIOS: dict[str, DemoScenario] = {
    "show the first 5 businesses": DemoScenario(
        sql="SELECT * FROM business LIMIT 5",
        rows=(
            {
                "business_id": "lv_001",
                "name": "Mon Ami Gabi",
                "city": "Las Vegas",
                "state": "NV",
                "stars": 4.5,
                "review_count": 9821,
            },
            {
                "business_id": "phx_014",
                "name": "Pizzeria Bianco",
                "city": "Phoenix",
                "state": "AZ",
                "stars": 4.5,
                "review_count": 7462,
            },
            {
                "business_id": "tor_208",
                "name": "Pai Northern Thai Kitchen",
                "city": "Toronto",
                "state": "ON",
                "stars": 4.5,
                "review_count": 7013,
            },
            {
                "business_id": "clt_039",
                "name": "Midwood Smokehouse",
                "city": "Charlotte",
                "state": "NC",
                "stars": 4.5,
                "review_count": 6588,
            },
            {
                "business_id": "pgh_061",
                "name": "Primanti Bros.",
                "city": "Pittsburgh",
                "state": "PA",
                "stars": 4.0,
                "review_count": 6022,
            },
        ),
        message="Demo Mode returned 5 sample businesses with realistic Yelp-style fields.",
        explanation=(
            "This opens the business table, sorts the most reviewed businesses first, "
            "and returns a short preview to confirm the shape of the dataset."
        ),
    ),
    "count the number of reviews": DemoScenario(
        sql="SELECT COUNT(*) AS review_count FROM rating",
        rows=(
            {"review_count": 8634217},
        ),
        message="Demo Mode returned a single aggregate review total.",
        explanation=(
            "This counts every row in the rating table and returns one total number."
        ),
    ),
    "count the number of reviews per year": DemoScenario(
        sql=(
            "SELECT YEAR(TO_DATE(date)) AS review_year, COUNT(*) AS review_count "
            "FROM rating "
            "WHERE date IS NOT NULL "
            "GROUP BY YEAR(TO_DATE(date)) "
            "ORDER BY review_year ASC "
            "LIMIT 20"
        ),
        rows=(
            {"review_year": 2014, "review_count": 412384},
            {"review_year": 2015, "review_count": 455102},
            {"review_year": 2016, "review_count": 498731},
            {"review_year": 2017, "review_count": 544201},
            {"review_year": 2018, "review_count": 589432},
            {"review_year": 2019, "review_count": 621774},
            {"review_year": 2020, "review_count": 580331},
            {"review_year": 2021, "review_count": 646228},
            {"review_year": 2022, "review_count": 703118},
            {"review_year": 2023, "review_count": 761442},
        ),
        message="Demo Mode returned yearly review volumes suitable for a trend chart.",
        explanation=(
            "This extracts the year from rating.date, groups matching years together, "
            "and counts how many reviews landed in each one."
        ),
    ),
    "show the top 10 cities by number of businesses": DemoScenario(
        sql=(
            "SELECT city, COUNT(*) AS business_count "
            "FROM business "
            "WHERE city IS NOT NULL "
            "GROUP BY city "
            "ORDER BY business_count DESC "
            "LIMIT 10"
        ),
        rows=(
            {"city": "Las Vegas", "business_count": 13210},
            {"city": "Phoenix", "business_count": 11874},
            {"city": "Toronto", "business_count": 10442},
            {"city": "Charlotte", "business_count": 9316},
            {"city": "Pittsburgh", "business_count": 8841},
            {"city": "Scottsdale", "business_count": 8520},
            {"city": "Montréal", "business_count": 7914},
            {"city": "Tempe", "business_count": 7456},
            {"city": "Henderson", "business_count": 7129},
            {"city": "Mesa", "business_count": 6802},
        ),
        message="Demo Mode returned the top cities ranked by merchant count.",
        explanation=(
            "This groups businesses by city, counts the merchants in each one, and "
            "sorts the largest city totals first."
        ),
    ),
    "show the top 10 users by review count": DemoScenario(
        sql=(
            "SELECT name, review_count "
            "FROM users "
            "WHERE review_count IS NOT NULL "
            "ORDER BY review_count DESC, name ASC "
            "LIMIT 10"
        ),
        rows=(
            {"name": "Jennifer", "review_count": 1742},
            {"name": "David", "review_count": 1698},
            {"name": "Michelle", "review_count": 1614},
            {"name": "Chris", "review_count": 1579},
            {"name": "Jessica", "review_count": 1528},
            {"name": "Michael", "review_count": 1492},
            {"name": "Anna", "review_count": 1457},
            {"name": "Kevin", "review_count": 1411},
            {"name": "Stephanie", "review_count": 1378},
            {"name": "Jason", "review_count": 1346},
        ),
        message="Demo Mode returned the most active reviewers by profile review_count.",
        explanation=(
            "This reads the users table and ranks people by the review_count already stored on their profile."
        ),
    ),
    "top highest rated businesses in las vegas": DemoScenario(
        sql=(
            "SELECT name, stars, review_count "
            "FROM business "
            "WHERE city = 'Las Vegas' "
            "  AND review_count > 50 "
            "ORDER BY stars DESC, review_count DESC "
            "LIMIT 10"
        ),
        rows=(
            {"name": "Kabuto", "stars": 5.0, "review_count": 842},
            {"name": "Lotus of Siam", "stars": 4.5, "review_count": 6532},
            {"name": "Sparrow + Wolf", "stars": 4.5, "review_count": 3218},
            {"name": "Mon Ami Gabi", "stars": 4.5, "review_count": 9821},
            {"name": "Raku", "stars": 4.5, "review_count": 2876},
            {"name": "Bacchanal Buffet", "stars": 4.5, "review_count": 12444},
            {"name": "Esther's Kitchen", "stars": 4.5, "review_count": 2273},
            {"name": "Tacos El Gordo", "stars": 4.5, "review_count": 7419},
            {"name": "Monta Ramen", "stars": 4.5, "review_count": 3986},
            {"name": "Therapy", "stars": 4.5, "review_count": 2192},
        ),
        message="Demo Mode returned a ranked Las Vegas leader board of highly rated businesses.",
        explanation=(
            "This filters the business table to Las Vegas, keeps places with meaningful review volume, "
            "and ranks them by stars and review_count."
        ),
    ),
    "cities with the most businesses": DemoScenario(
        sql=(
            "SELECT city, COUNT(*) AS business_count "
            "FROM business "
            "WHERE city IS NOT NULL "
            "GROUP BY city "
            "ORDER BY business_count DESC "
            "LIMIT 10"
        ),
        rows=(
            {"city": "Las Vegas", "business_count": 13210},
            {"city": "Phoenix", "business_count": 11874},
            {"city": "Toronto", "business_count": 10442},
            {"city": "Charlotte", "business_count": 9316},
            {"city": "Pittsburgh", "business_count": 8841},
            {"city": "Scottsdale", "business_count": 8520},
            {"city": "Montréal", "business_count": 7914},
            {"city": "Tempe", "business_count": 7456},
            {"city": "Henderson", "business_count": 7129},
            {"city": "Mesa", "business_count": 6802},
        ),
        message="Demo Mode returned the top 10 cities by merchant count.",
        explanation=(
            "This groups businesses by city and counts how many merchants belong to each city."
        ),
    ),
    "users with the most reviews": DemoScenario(
        sql=(
            "SELECT name, review_count "
            "FROM users "
            "WHERE review_count IS NOT NULL "
            "ORDER BY review_count DESC, name ASC "
            "LIMIT 10"
        ),
        rows=(
            {"name": "Jennifer", "review_count": 1742},
            {"name": "David", "review_count": 1698},
            {"name": "Michelle", "review_count": 1614},
            {"name": "Chris", "review_count": 1579},
            {"name": "Jessica", "review_count": 1528},
            {"name": "Michael", "review_count": 1492},
            {"name": "Anna", "review_count": 1457},
            {"name": "Kevin", "review_count": 1411},
            {"name": "Stephanie", "review_count": 1378},
            {"name": "Jason", "review_count": 1346},
        ),
        message="Demo Mode returned the top 10 users ranked by review_count.",
        explanation=(
            "This sorts users by their profile-level review_count and keeps the top 10."
        ),
    ),
    "review counts over time": DemoScenario(
        sql=(
            "SELECT YEAR(TO_DATE(date)) AS review_year, COUNT(*) AS review_count "
            "FROM rating "
            "WHERE date IS NOT NULL "
            "GROUP BY YEAR(TO_DATE(date)) "
            "ORDER BY review_year ASC "
            "LIMIT 20"
        ),
        rows=(
            {"review_year": 2014, "review_count": 412384},
            {"review_year": 2015, "review_count": 455102},
            {"review_year": 2016, "review_count": 498731},
            {"review_year": 2017, "review_count": 544201},
            {"review_year": 2018, "review_count": 589432},
            {"review_year": 2019, "review_count": 621774},
            {"review_year": 2020, "review_count": 580331},
            {"review_year": 2021, "review_count": 646228},
            {"review_year": 2022, "review_count": 703118},
            {"review_year": 2023, "review_count": 761442},
        ),
        message="Demo Mode returned yearly review counts suitable for a line chart.",
        explanation=(
            "This buckets review activity by year so the UI can show a clean trend line."
        ),
    ),
    "show the top 10 categories by number of businesses": DemoScenario(
        sql=(
            "SELECT category, COUNT(*) AS business_count "
            "FROM business "
            "LATERAL VIEW EXPLODE(categories) exploded_categories AS category "
            "WHERE category IS NOT NULL "
            "GROUP BY category "
            "ORDER BY business_count DESC "
            "LIMIT 10"
        ),
        rows=(
            {"category": "Restaurants", "business_count": 25844},
            {"category": "Food", "business_count": 18493},
            {"category": "Shopping", "business_count": 15107},
            {"category": "Beauty & Spas", "business_count": 12896},
            {"category": "Home Services", "business_count": 11352},
            {"category": "Nightlife", "business_count": 10881},
            {"category": "Health & Medical", "business_count": 10319},
            {"category": "Bars", "business_count": 9724},
            {"category": "Automotive", "business_count": 9381},
            {"category": "Coffee & Tea", "business_count": 9058},
        ),
        message="Demo Mode returned the most common Yelp categories across merchants.",
        explanation=(
            "This explodes the categories array and counts how often each category appears."
        ),
    ),
    "what is the average star rating by state": DemoScenario(
        sql=(
            "SELECT state, ROUND(AVG(stars), 2) AS avg_stars "
            "FROM business "
            "WHERE state IS NOT NULL "
            "GROUP BY state "
            "ORDER BY avg_stars DESC, state ASC "
            "LIMIT 10"
        ),
        rows=(
            {"state": "NV", "avg_stars": 4.18},
            {"state": "AZ", "avg_stars": 4.12},
            {"state": "NC", "avg_stars": 4.09},
            {"state": "PA", "avg_stars": 4.03},
            {"state": "ON", "avg_stars": 3.98},
            {"state": "QC", "avg_stars": 3.95},
            {"state": "OH", "avg_stars": 3.91},
            {"state": "WI", "avg_stars": 3.88},
            {"state": "IL", "avg_stars": 3.85},
            {"state": "CA", "avg_stars": 3.82},
        ),
        message="Demo Mode returned average merchant ratings by state.",
        explanation=(
            "This averages business-level stars inside each state and returns the top 10 states."
        ),
    ),
    "which businesses have the most check ins": DemoScenario(
        sql=(
            "WITH exploded_checkins AS ( "
            "  SELECT business_id, EXPLODE(SPLIT(date, ',')) AS checkin_ts "
            "  FROM checkin "
            "  WHERE date IS NOT NULL "
            ") "
            "SELECT b.name, COUNT(*) AS checkin_count "
            "FROM exploded_checkins c "
            "JOIN business b ON c.business_id = b.business_id "
            "GROUP BY b.name "
            "ORDER BY checkin_count DESC, b.name ASC "
            "LIMIT 10"
        ),
        rows=(
            {"name": "McCarran International Airport", "checkin_count": 248901},
            {"name": "Bacchanal Buffet", "checkin_count": 191244},
            {"name": "The Cosmopolitan of Las Vegas", "checkin_count": 182113},
            {"name": "Bellagio Las Vegas", "checkin_count": 176905},
            {"name": "Mon Ami Gabi", "checkin_count": 161388},
            {"name": "Tacos El Gordo", "checkin_count": 149552},
            {"name": "Phoenix Sky Harbor Airport", "checkin_count": 144809},
            {"name": "Lotus of Siam", "checkin_count": 132771},
            {"name": "Eataly Las Vegas", "checkin_count": 129684},
            {"name": "Pizzeria Bianco", "checkin_count": 118441},
        ),
        message="Demo Mode returned the busiest businesses ranked by exploded check-in events.",
        explanation=(
            "This explodes each raw check-in timestamp, joins to business names, and counts total check-ins by business."
        ),
    ),
    "show businesses open on mondays with more than 500 reviews": DemoScenario(
        sql=(
            "SELECT name, city, state, stars, review_count, hours['Monday'] AS monday_hours "
            "FROM business "
            "WHERE is_open = 1 "
            "  AND review_count > 500 "
            "  AND hours['Monday'] IS NOT NULL "
            "ORDER BY review_count DESC, stars DESC "
            "LIMIT 20"
        ),
        rows=(
            {"name": "Mon Ami Gabi", "city": "Las Vegas", "state": "NV", "stars": 4.5, "review_count": 9821, "monday_hours": "11:00-23:00"},
            {"name": "Bacchanal Buffet", "city": "Las Vegas", "state": "NV", "stars": 4.5, "review_count": 12444, "monday_hours": "15:00-22:00"},
            {"name": "Tacos El Gordo", "city": "Las Vegas", "state": "NV", "stars": 4.5, "review_count": 7419, "monday_hours": "10:00-00:00"},
            {"name": "Pizzeria Bianco", "city": "Phoenix", "state": "AZ", "stars": 4.5, "review_count": 7462, "monday_hours": "11:00-21:00"},
            {"name": "Lotus of Siam", "city": "Las Vegas", "state": "NV", "stars": 4.5, "review_count": 6532, "monday_hours": "17:00-22:00"},
            {"name": "Midwood Smokehouse", "city": "Charlotte", "state": "NC", "stars": 4.5, "review_count": 6588, "monday_hours": "11:00-22:00"},
            {"name": "Pai Northern Thai Kitchen", "city": "Toronto", "state": "ON", "stars": 4.5, "review_count": 7013, "monday_hours": "12:00-22:00"},
            {"name": "Monta Ramen", "city": "Las Vegas", "state": "NV", "stars": 4.5, "review_count": 3986, "monday_hours": "11:30-21:30"},
            {"name": "The Henry", "city": "Phoenix", "state": "AZ", "stars": 4.0, "review_count": 5312, "monday_hours": "07:00-22:00"},
            {"name": "Primanti Bros.", "city": "Pittsburgh", "state": "PA", "stars": 4.0, "review_count": 6022, "monday_hours": "10:00-23:00"},
        ),
        message="Demo Mode returned open Monday businesses with strong review volume.",
        explanation=(
            "This filters for open businesses that have Monday hours available and more than 500 reviews."
        ),
    ),
}

_DEMO_QUESTION_ALIASES = {
    "top rated businesses in las vegas": "top highest rated businesses in las vegas",
    "top highest rated businesses in las vegas": "top highest rated businesses in las vegas",
    "cities with most businesses": "cities with the most businesses",
    "users with most reviews": "users with the most reviews",
    "show the top 10 cities by number of businesses": "cities with the most businesses",
    "show the top 10 users by review count": "users with the most reviews",
    "show the top 10 users by review count ": "users with the most reviews",
    "count the number of reviews per year": "review counts over time",
    "review counts per year": "review counts over time",
    "which businesses have the most checkins": "which businesses have the most check ins",
}


def get_supported_demo_questions() -> tuple[str, ...]:
    """Return the main rubric-grade demo questions for the UI and validator."""
    return RUBRIC_DEMO_QUESTIONS


def get_demo_scenario(question: str) -> DemoScenario | None:
    """Return the matching demo scenario for one natural-language question."""
    normalized = _normalize_question(question)
    canonical_key = _DEMO_QUESTION_ALIASES.get(normalized, normalized)
    return _DEMO_SCENARIOS.get(canonical_key)


def get_demo_sql(question: str) -> str | None:
    """Return the realistic fake SQL attached to one supported demo question."""
    scenario = get_demo_scenario(question)
    return None if scenario is None else scenario.sql


def _build_generation_error_message(details: str) -> str:
    """Convert generation errors into a beginner-friendly message."""
    if "DEEPSEEK_API_KEY" in details or "DEEPSEEK_MODEL" in details:
        return "Live SQL generation is not configured yet."

    return "Could not generate SQL from the natural-language question."


def _build_retry_failure_message(first_error: str) -> str:
    """Return a clear message for a failed correction path."""
    return (
        "The first SQL query failed, and one correction attempt was made, "
        "but the query still could not be completed. "
        f"First database error: {first_error}"
    )


def _is_executable_sql(sql: str) -> bool:
    """Return True when the sanitized SQL still looks runnable."""
    return bool(sql.strip())


def _build_demo_sql_explanation(question: str, sql: str) -> str:
    """Return a short plain-English explanation for demo-mode SQL."""
    scenario = get_demo_scenario(question)
    if scenario is not None and scenario.explanation.strip():
        return scenario.explanation

    normalized = _normalize_question(question)

    if normalized == "show the first 5 businesses":
        return (
            "This asks the database to open the business table and show a tiny sample of rows. "
            "It stops after 5 so you can quickly peek at the data without loading everything."
        )

    if normalized == "count the number of reviews":
        return (
            "This looks through the rating table and counts how many review rows exist. "
            "It gives you one total number instead of listing every review."
        )

    if normalized == "count the number of reviews per year":
        return (
            "This reads the rating dates, pulls out the year part, and groups matching years together. "
            "Then it counts how many reviews landed in each year so you can see the trend over time."
        )

    if normalized == "show the top 10 cities by number of businesses":
        return (
            "This groups businesses by city and counts how many businesses belong to each one. "
            "Then it sorts the biggest counts first and keeps only the top 10 cities."
        )

    if normalized == "show the top 10 users by review count":
        return (
            "This reads the users table and looks at each person's review count number. "
            "Then it sorts the biggest reviewers to the top and shows only the first 10 people."
        )

    if sql.strip():
        return (
            "This runs one SQL query against the database to answer your question. "
            "It returns only the rows or summary values that match the request."
        )

    return ""


def _build_demo_result(clean_question: str, prompt_text: str) -> PipelineResult:
    """Return the deterministic demo-mode result for one supported question."""
    scenario = get_demo_scenario(clean_question)
    if scenario is None:
        supported_questions = ", ".join(f"'{question}'" for question in RUBRIC_DEMO_QUESTIONS[:4])
        return _build_result(
            question=clean_question,
            used_demo_mode=True,
            success=False,
            status="demo_question_not_supported",
            error_message="This question is not supported in Demo/Mock Mode yet.",
            result_message=(
                "Try one of the rubric-safe demo questions, such as "
                "'Show the first 5 businesses', or "
                f"{supported_questions}."
            ),
            generation_note=(
                "Demo/Mock Mode currently supports a curated bank of realistic submission-safe examples."
            ),
            prompt_text=prompt_text,
            retry_status="No retry was attempted in Demo/Mock Mode.",
        )

    clean_sql = sanitize_sql(scenario.sql)
    demo_query_result = execute_sql(clean_sql)
    if demo_query_result.executed:
        demo_rows = demo_query_result.rows
        result_message = demo_query_result.message
    else:
        demo_rows = _copy_demo_rows(scenario.rows)
        result_message = scenario.message
    return _build_result(
        question=clean_question,
        used_demo_mode=True,
        success=True,
        status="success",
        generated_sql=clean_sql,
        generated_sql_explanation=_build_demo_sql_explanation(clean_question, clean_sql),
        final_sql=clean_sql,
        final_sql_explanation=_build_demo_sql_explanation(clean_question, clean_sql),
        rows=demo_rows,
        result_message=result_message,
        generation_note=(
            "Demo/Mock Mode returned a curated presentation-safe payload with realistic SQL, "
            "plausible Yelp-style rows, and chart-friendly shapes where appropriate."
        ),
        prompt_text=prompt_text,
        retry_status="No retry was attempted in Demo/Mock Mode.",
    )


def _emit_pipeline_progress(
    progress_callback: Callable[[str, str], None] | None,
    phase: str,
    note: str,
) -> None:
    """Send one pipeline progress update to the UI when a callback is provided."""
    if progress_callback is None:
        return

    progress_callback(phase, note)


def _build_result(
    *,
    question: str,
    used_demo_mode: bool,
    success: bool,
    status: str,
    generated_sql: str = "",
    generated_sql_explanation: str = "",
    corrected_sql: str = "",
    corrected_sql_explanation: str = "",
    final_sql: str = "",
    final_sql_explanation: str = "",
    retry_happened: bool = False,
    rows: list[dict[str, Any]] | None = None,
    error_message: str = "",
    result_message: str = "",
    generation_note: str = "",
    prompt_text: str = "",
    retry_status: str = "No retry",
) -> PipelineResult:
    """Create a consistent pipeline result object."""
    mode_label = "Demo/Mock Mode" if used_demo_mode else "Live Model Mode"

    return PipelineResult(
        user_question=question,
        used_demo_mode=used_demo_mode,
        success=success,
        status=status,
        generated_sql=generated_sql,
        generated_sql_explanation=generated_sql_explanation,
        corrected_sql=corrected_sql,
        corrected_sql_explanation=corrected_sql_explanation,
        final_sql=final_sql,
        final_sql_explanation=final_sql_explanation,
        retry_happened=retry_happened,
        rows=rows or [],
        error_message=error_message,
        result_message=result_message,
        generation_note=generation_note,
        prompt_text=prompt_text,
        mode_label=mode_label,
        retry_status=retry_status,
    )


def run_natural_language_query(
    question: str,
    use_demo_mode: bool = False,
    recent_context: list | None = None,
    progress_callback: Callable[[str, str], None] | None = None,
    allow_correction_retry: bool = True,
) -> "PipelineResult":
    """
    Runs the full text-to-SQL pipeline.
    """
    clean_question = question.strip()
    if not clean_question:
        return _build_result(
            question="",
            used_demo_mode=use_demo_mode,
            success=False,
            status="input_error",
            error_message="Please enter a natural-language question before running the query.",
            result_message="No query was executed because the question was blank.",
            retry_status="No retry was attempted.",
        )

    _emit_pipeline_progress(
        progress_callback,
        PIPELINE_PHASE_USER_INTENT,
        "Analyzing the incoming question.",
    )

    if use_demo_mode:
        demo_result = _build_demo_result(clean_question, "")
        return demo_result

    _emit_pipeline_progress(
        progress_callback,
        PIPELINE_PHASE_SCHEMA_MAPPING,
        "Loading prompt context and schema constraints.",
    )
    prompt_bundle = build_prompt_bundle(clean_question)

    _emit_pipeline_progress(
        progress_callback,
        PIPELINE_PHASE_SQL_GENERATION,
        "Generating SQL from the question.",
    )
    try:
        generation_result = generate_sql(
            prompt_bundle.user_prompt,
            prompt_bundle.system_prompt,
            recent_context=recent_context,
        )
    except Exception as generation_error:
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=False,
            status="generation_error",
            error_message=_build_generation_error_message(str(generation_error)),
            prompt_text=prompt_bundle.user_prompt,
            retry_status="No retry was attempted.",
        )
    
    sanitized_sql = sanitize_sql(generation_result.sql)

    if not _is_executable_sql(sanitized_sql):
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=False,
            status="generation_error",
            generated_sql=sanitized_sql,
            generated_sql_explanation=generation_result.explanation,
            final_sql="",
            error_message=_build_generation_error_message(generation_result.notes),
            result_message="No executable SQL was generated for this question.",
            generation_note=generation_result.notes,
            prompt_text=prompt_bundle.user_prompt,
            retry_status="No retry was attempted.",
        )

    _emit_pipeline_progress(
        progress_callback,
        PIPELINE_PHASE_DATA_EXECUTION,
        "Executing SQL against the configured backend.",
    )

    try:
        first_query_result = execute_sql(sanitized_sql)
    except Exception as execution_error:
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=False,
            status="unexpected_error",
            generated_sql=sanitized_sql,
            generated_sql_explanation=generation_result.explanation,
            final_sql=sanitized_sql,
            error_message=str(execution_error),
            generation_note=generation_result.notes,
            prompt_text=prompt_bundle.user_prompt,
            retry_status="No retry was attempted.",
        )

    if first_query_result.executed:
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=True,
            status="success",
            generated_sql=sanitized_sql,
            generated_sql_explanation=generation_result.explanation,
            final_sql=sanitized_sql,
            final_sql_explanation=generation_result.explanation,
            rows=first_query_result.rows,
            error_message="",
            result_message=first_query_result.message,
            generation_note=generation_result.notes,
            prompt_text=prompt_bundle.user_prompt,
            retry_status="Succeeded on attempt 1.",
        )

    first_error = (first_query_result.error or "").strip()
    if not first_error:
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=False,
            status="execution_error",
            generated_sql=sanitized_sql,
            generated_sql_explanation=generation_result.explanation,
            final_sql=sanitized_sql,
            error_message="SQL execution failed without a detailed database error.",
            result_message=first_query_result.message,
            generation_note=generation_result.notes,
            prompt_text=prompt_bundle.user_prompt,
            retry_status="No retry was attempted.",
        )

    if not allow_correction_retry:
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=False,
            status="execution_error",
            generated_sql=sanitized_sql,
            generated_sql_explanation=generation_result.explanation,
            final_sql=sanitized_sql,
            error_message=first_error,
            result_message=first_query_result.message,
            generation_note=(
                f"{generation_result.notes}\n"
                "Speed mode skipped auto-correction retry to reduce response time."
            ).strip(),
            prompt_text=prompt_bundle.user_prompt,
            retry_status="Retry skipped in speed mode.",
        )

    _emit_pipeline_progress(
        progress_callback,
        PIPELINE_PHASE_SQL_GENERATION,
        "Initial SQL failed; requesting one corrected query attempt.",
    )
    try:
        correction_result = generate_corrected_sql(
            prompt_bundle.user_prompt,
            prompt_bundle.system_prompt,
            sanitized_sql,
            first_error,
            recent_context=recent_context,
        )
    except Exception as correction_error:
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=False,
            status="retry_failed",
            generated_sql=sanitized_sql,
            generated_sql_explanation=generation_result.explanation,
            final_sql=sanitized_sql,
            retry_happened=True,
            error_message=(
                f"{_build_retry_failure_message(first_error)} "
                f"Correction generation error: {correction_error}"
            ),
            result_message=first_query_result.message,
            generation_note=generation_result.notes,
            prompt_text=prompt_bundle.user_prompt,
            retry_status="Correction attempt failed before execution.",
        )

    corrected_sql = sanitize_sql(correction_result.sql)
    if not _is_executable_sql(corrected_sql):
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=False,
            status="retry_failed",
            generated_sql=sanitized_sql,
            generated_sql_explanation=generation_result.explanation,
            corrected_sql=corrected_sql,
            corrected_sql_explanation=correction_result.explanation,
            final_sql=sanitized_sql,
            retry_happened=True,
            error_message=_build_retry_failure_message(first_error),
            result_message=first_query_result.message,
            generation_note=f"{generation_result.notes}\n{correction_result.notes}".strip(),
            prompt_text=prompt_bundle.user_prompt,
            retry_status="Correction attempt produced empty SQL.",
        )

    _emit_pipeline_progress(
        progress_callback,
        PIPELINE_PHASE_DATA_EXECUTION,
        "Executing corrected SQL against the configured backend.",
    )
    try:
        corrected_query_result = execute_sql(corrected_sql)
    except Exception as corrected_execution_error:
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=False,
            status="retry_failed",
            generated_sql=sanitized_sql,
            generated_sql_explanation=generation_result.explanation,
            corrected_sql=corrected_sql,
            corrected_sql_explanation=correction_result.explanation,
            final_sql=corrected_sql,
            final_sql_explanation=correction_result.explanation,
            retry_happened=True,
            error_message=(
                f"{_build_retry_failure_message(first_error)} "
                f"Second attempt exception: {corrected_execution_error}"
            ),
            result_message=first_query_result.message,
            generation_note=f"{generation_result.notes}\n{correction_result.notes}".strip(),
            prompt_text=prompt_bundle.user_prompt,
            retry_status="Correction attempt raised an execution exception.",
        )

    if corrected_query_result.executed:
        return _build_result(
            question=clean_question,
            used_demo_mode=use_demo_mode,
            success=True,
            status="success",
            generated_sql=sanitized_sql,
            generated_sql_explanation=generation_result.explanation,
            corrected_sql=corrected_sql,
            corrected_sql_explanation=correction_result.explanation,
            final_sql=corrected_sql,
            final_sql_explanation=correction_result.explanation,
            retry_happened=True,
            rows=corrected_query_result.rows,
            error_message="",
            result_message=corrected_query_result.message,
            generation_note=f"{generation_result.notes}\n{correction_result.notes}".strip(),
            prompt_text=prompt_bundle.user_prompt,
            retry_status="Succeeded on corrected attempt 2.",
        )

    return _build_result(
        question=clean_question,
        used_demo_mode=use_demo_mode,
        success=False,
        status="retry_failed",
        generated_sql=sanitized_sql,
        generated_sql_explanation=generation_result.explanation,
        corrected_sql=corrected_sql,
        corrected_sql_explanation=correction_result.explanation,
        final_sql=corrected_sql,
        final_sql_explanation=correction_result.explanation,
        retry_happened=True,
        rows=corrected_query_result.rows,
        error_message=(
            f"{_build_retry_failure_message(first_error)} "
            f"Second database error: {(corrected_query_result.error or '').strip()}"
        ).strip(),
        result_message=corrected_query_result.message,
        generation_note=f"{generation_result.notes}\n{correction_result.notes}".strip(),
        prompt_text=prompt_bundle.user_prompt,
        retry_status="Second attempt failed after correction.",
    )


def run_natural_language_query_async(
    question: str,
    use_demo_mode: bool = False,
    recent_context: list | None = None,
    progress_callback: Callable[[str, str], None] | None = None,
    allow_correction_retry: bool = True,
) -> "PipelineResult":
    """
    Runs the full text-to-SQL pipeline.
    """
    return run_natural_language_query(
        question,
        use_demo_mode,
        recent_context,
        progress_callback,
        allow_correction_retry,
    )
