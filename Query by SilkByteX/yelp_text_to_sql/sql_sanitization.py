from __future__ import annotations

import re
import textwrap


SQL_START_PATTERN = re.compile(r"\b(WITH|SELECT)\b", flags=re.IGNORECASE)
FENCE_PATTERN = re.compile(r"```(?:sql)?\s*(.*?)```", flags=re.IGNORECASE | re.DOTALL)
LABEL_PATTERN = re.compile(r"^\s*(sql|sql query|query|generated sql|answer)\s*:\s*", flags=re.IGNORECASE)
EXPLANATION_SPLIT_PATTERN = re.compile(
    r"\b(Explanation|Note|Summary|Why this works|This query|The query|Result)\b\s*:?",
    flags=re.IGNORECASE,
)


def _strip_markdown_fences(text: str) -> str:
    """Extract the first fenced SQL block or return the original text."""
    match = FENCE_PATTERN.search(text)
    if match:
        return match.group(1).strip()
    return text


def _collapse_whitespace(text: str) -> str:
    """Collapse all whitespace to single spaces for safe execution strings."""
    return " ".join(text.split())


def sanitize_sql(raw_sql: str) -> str:
    """Return one clean single-line SQL string ready for execution."""
    text = textwrap.dedent(raw_sql or "").strip()
    if not text:
        return ""

    text = _strip_markdown_fences(text)
    text = LABEL_PATTERN.sub("", text).strip()

    sql_match = SQL_START_PATTERN.search(text)
    if not sql_match:
        return ""

    text = text[sql_match.start() :].strip()
    text = EXPLANATION_SPLIT_PATTERN.split(text, maxsplit=1)[0].strip()
    text = text.split("```", 1)[0].strip()
    text = text.split(";", 1)[0].strip()
    text = text.rstrip(";").strip()

    if not text:
        return ""

    return _collapse_whitespace(text)
