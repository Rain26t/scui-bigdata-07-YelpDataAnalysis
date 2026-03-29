from __future__ import annotations

from fuzzywuzzy import fuzz

from yelp_text_to_sql.golden_queries import GOLDEN_QUERIES

MIN_FUZZY_MATCH_SCORE = 80


def fuzzy_match_golden_query(question: str) -> str | None:
    """
    Uses fuzzy string matching to find a similar question in the golden queries.

    Args:
        question: The user's question.

    Returns:
        The matching SQL query if a sufficiently similar question is found, otherwise None.
    """
    for golden_question, golden_sql in GOLDEN_QUERIES.items():
        score = fuzz.ratio(question.lower(), golden_question.lower())
        if score >= MIN_FUZZY_MATCH_SCORE:
            return golden_sql
    return None
