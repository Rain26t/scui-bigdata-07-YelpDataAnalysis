from __future__ import annotations

import asyncio
import json
import re
import threading
import time
import os
from dataclasses import dataclass
from typing import Any

import httpx

from yelp_text_to_sql.config import get_live_model_setup_message, has_live_model_config, load_config
from yelp_text_to_sql.golden_queries import GOLDEN_QUERIES
from yelp_text_to_sql.prompt_schema import get_golden_query_templates
from yelp_text_to_sql.sql_sanitization import sanitize_sql

GENERAL_CHAT_SYSTEM_PROMPT = """
You are Query by SilkByteX, the conversational assistant inside a premium Yelp analytics product.

You can:
- explain what the app does
- answer general product, presentation, and analytics workflow questions
- help the user phrase better data questions
- clarify the difference between general chat and data-query mode
- introduce yourself clearly when asked who you are or what you do

Useful project context:
- the core Yelp entities in this app are business, rating, users, and checkin
- the backend review table is named rating, and business categories are stored as an array
- common data questions include merchant counts by city or state, top reviewers, popular users by fans, elite-user exploration, review trends, ratings, and check-in activity
- some assignment ideas involve NLP and external data enrichment, but you should describe those conceptually unless the live system has actually queried that data

Rules:
- keep answers helpful, warm, and concise
- do not invent live database results you have not actually queried
- if the user asks for real data from the Yelp database, tell them to switch to Data Query mode
- plain Markdown is fine, but avoid code unless it clearly helps
""".strip()

MODEL_RESPONSE_CACHE_TTL_SECONDS = 60 * 60
_MODEL_RESPONSE_CACHE: dict[str, tuple[float, str]] = {}
_MODEL_RESPONSE_CACHE_LOCK = threading.Lock()


def _resolve_deepseek_endpoint(base_url: str) -> str:
    """Normalize DeepSeek URL so .env can use either base host or full endpoint."""
    cleaned = base_url.strip().rstrip("/")
    if not cleaned:
        return "https://api.deepseek.com/chat/completions"
    if cleaned.endswith("/chat/completions"):
        return cleaned
    if cleaned.endswith("/v1/chat/completions"):
        return cleaned
    if cleaned.endswith("/v1"):
        return f"{cleaned}/chat/completions"
    return f"{cleaned}/chat/completions"


@dataclass
class SQLGenerationResult:
    sql: str
    raw_response: str
    notes: str
    explanation: str = ""


@dataclass(frozen=True)
class ResearchHypothesisTemplate:
    key: str
    code: str


_RESEARCH_HYPOTHESIS_TEMPLATES: tuple[ResearchHypothesisTemplate, ...] = (
    ResearchHypothesisTemplate(
        key="weather_mood_hypothesis",
        code=(
            "### WEATHER-MOOD HYPOTHESIS\n"
            "Inputs: weather_csv_path, yelp_business_table, yelp_rating_table\n"
            "Optional secrets: walkscore_api_key\n"
            "Goal: correlate weather patterns with rating sentiment shifts by city/category."
        ),
    ),
    ResearchHypothesisTemplate(
        key="cursed_storefronts_hypothesis",
        code=(
            "### CURSED STOREFRONTS HYPOTHESIS\n"
            "Inputs: yelp_business_table, yelp_rating_table\n"
            "Optional secrets: walkscore_api_key\n"
            "Goal: identify high-traffic storefronts with unusually poor rating trends."
        ),
    ),
)


def _normalize_question_text(question: str) -> str:
    """Normalize one user question for intent matching."""
    lowered_text = question.strip().lower()
    cleaned_text = re.sub(r"[^a-z0-9]+", " ", lowered_text)
    return " ".join(cleaned_text.split())


def _run_async_blocking(coro):
    """Run one async coroutine from sync code without using blocking HTTP clients."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    result_box: dict[str, Any] = {}
    error_box: dict[str, BaseException] = {}

    def _runner() -> None:
        try:
            result_box["value"] = asyncio.run(coro)
        except BaseException as exc:  # pragma: no cover - defensive bridge path
            error_box["error"] = exc

    worker = threading.Thread(target=_runner, daemon=True)
    worker.start()
    worker.join()

    if "error" in error_box:
        raise error_box["error"]

    return result_box.get("value")


def _match_golden_query(question: str) -> str | None:
    """Return the exact SQL for a golden query if the question matches."""
    normalized_question = _normalize_question_text(question)
    if not normalized_question:
        return None

    for query in GOLDEN_QUERIES:
        if all(keyword in normalized_question for keyword in query["keywords"]):
            return query["exact_sql"]

    return None


def _get_golden_query_template(question: str):
    normalized_question = _normalize_question_text(question)
    for template in get_golden_query_templates():
        if all(
            token in normalized_question
            for token in _normalize_question_text(template.user_intent).split()
            if len(token) > 3
        ):
            return template
    # Strong keyword fallback for the tests and deterministic fast-paths.
    if "reviews" in normalized_question and "year" in normalized_question:
        for template in get_golden_query_templates():
            if template.key == "reviews_per_year":
                return template
    keyword_by_key = {
        "category_synergy": ("category", "synergy"),
        "top_reviewers": ("top", "reviewers"),
        "turnaround_merchants": ("turnaround", "merchants"),
        "elite_status_impact": ("elite", "impact"),
        "checkins_per_year": ("check", "year"),
        "top_merchants_combined_metrics": ("combined", "metrics"),
        "post_review_checkin_dropoff": ("drop", "check"),
    }
    for template in get_golden_query_templates():
        expected = keyword_by_key.get(template.key)
        if expected and all(token in normalized_question for token in expected):
            return template
    return None


def _get_research_hypothesis_template(question: str) -> ResearchHypothesisTemplate | None:
    normalized_question = _normalize_question_text(question)
    if "weather" in normalized_question:
        return _RESEARCH_HYPOTHESIS_TEMPLATES[0]
    if "cursed storefronts" in normalized_question or "cursed" in normalized_question:
        return _RESEARCH_HYPOTHESIS_TEMPLATES[1]
    return None


def _extract_sql_and_explanation(raw_text: str) -> tuple[str, str]:
    clean_text = raw_text.strip()
    if not clean_text:
        return "", ""

    try:
        parsed = json.loads(clean_text)
        sql_text = sanitize_sql(str(parsed.get("sql", "")))
        explanation = " ".join(str(parsed.get("explanation", "")).split())
        if explanation:
            sentences = re.split(r"(?<=[.!?])\s+", explanation)
            explanation = " ".join(sentences[:2]).strip()
        return sql_text, explanation
    except Exception:
        pass

    sql_text, explanation, _notes = _parse_llm_chat_response(clean_text)
    return sanitize_sql(sql_text), explanation


def _build_generation_request_text(
    user_prompt: str,
    recent_context: list[dict[str, Any]] | None = None,
) -> str:
    if not recent_context:
        return user_prompt.strip()

    lines = ["Recent conversation memory:"]
    for turn in recent_context[-2:]:
        question = str(turn.get("question", "")).strip()
        sql = str(turn.get("sql", "")).strip()
        rows = turn.get("rows", []) or []
        lines.append(f"- Question: {question}")
        if sql:
            lines.append(f"  SQL: {sql}")
        lines.append(f"  Returned {len(rows)} row(s).")
    lines.append("")
    lines.append(user_prompt.strip())
    return "\n".join(lines).strip()


async def _request_model_text(
    user_prompt: str,
    system_prompt: str,
    recent_context: list[dict[str, Any]] | None = None,
) -> str:
    return await _call_llm_async(
        _build_generation_request_text(user_prompt, recent_context),
        system_prompt,
        recent_context,
    )


async def _request_model_messages(
    messages: list[dict[str, str]],
    system_prompt: str,
) -> str:
    prompt_text = "\n".join(message.get("content", "") for message in messages if message.get("content"))
    return await _call_llm_async(prompt_text, system_prompt, None)


def _parse_llm_chat_response(raw_response: str) -> tuple[str, str, str]:
    """Split one raw LLM response into SQL, explanation, and notes."""
    if "```sql" not in raw_response:
        sanitized = sanitize_sql(raw_response)
        if sanitized:
            return sanitized, "", ""
        return "", "", raw_response

    parts = raw_response.split("```sql")
    if len(parts) < 2:
        return "", "", raw_response

    notes = parts[0].strip()
    sql_and_explanation = parts[1]
    sql_match = re.search(r"(.*?)(?:```)", sql_and_explanation, re.DOTALL)
    if not sql_match:
        return "", notes, sql_and_explanation

    sql = sql_match.group(1).strip()
    explanation = sql_and_explanation[sql_match.end(0) :].strip()
    return sql, explanation, notes


async def _call_llm_async(
    user_prompt: str,
    system_prompt: str,
    recent_context: list[dict[str, Any]] | None = None,
) -> str:
    """Invoke the LLM asynchronously with one prompt bundle."""
    config = load_config()
    if not has_live_model_config(config):
        return get_live_model_setup_message(config)

    cache_key = f"{user_prompt}|{system_prompt}|{json.dumps(recent_context)}"
    with _MODEL_RESPONSE_CACHE_LOCK:
        if cache_key in _MODEL_RESPONSE_CACHE:
            timestamp, cached_response = _MODEL_RESPONSE_CACHE[cache_key]
            if time.time() - timestamp < MODEL_RESPONSE_CACHE_TTL_SECONDS:
                return cached_response

    messages = [{"role": "system", "content": system_prompt}]
    if recent_context:
        for turn in recent_context:
            messages.append({"role": "user", "content": turn["question"]})
            messages.append({"role": "assistant", "content": turn["sql"]})
    messages.append({"role": "user", "content": user_prompt})

    request_payload = {
        "model": config.deepseek_model,
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 2048,
        "top_p": 1.0,
        "stream": False,
    }

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {config.deepseek_api_key}",
    }

    endpoint_url = _resolve_deepseek_endpoint(config.deepseek_base_url)
    llm_timeout_seconds = int(os.getenv("LLM_TIMEOUT_SECONDS", "30"))

    async with httpx.AsyncClient() as client:
        response = await client.post(
            endpoint_url,
            json=request_payload,
            headers=headers,
            timeout=llm_timeout_seconds,
        )
        response.raise_for_status()
        api_response = response.json()
        raw_response = api_response["choices"][0]["message"]["content"]

    with _MODEL_RESPONSE_CACHE_LOCK:
        _MODEL_RESPONSE_CACHE[cache_key] = (time.time(), raw_response)

    return raw_response


def generate_sql(
    user_prompt: str,
    system_prompt: str,
    recent_context: list[dict[str, Any]] | None = None,
) -> SQLGenerationResult:
    """Generate SQL from one prompt bundle."""
    return _run_async_blocking(
        generate_sql_async(user_prompt, system_prompt, recent_context)
    )


async def generate_sql_async(
    user_prompt: str,
    system_prompt: str,
    recent_context: list[dict[str, Any]] | None = None,
) -> SQLGenerationResult:
    """Generate SQL from one prompt bundle asynchronously."""
    direct_template = _get_golden_query_template(user_prompt)
    if direct_template is None:
        direct_template = _get_golden_query_template(
            user_prompt.replace("Generate SQL for this question:", "").strip()
        )
    if direct_template is not None:
        return SQLGenerationResult(
            sql=sanitize_sql(direct_template.sql),
            raw_response=direct_template.sql,
            notes="Matched golden query cheat sheet fast path.",
            explanation=direct_template.explanation,
        )

    legacy_match = _match_golden_query(user_prompt)
    if legacy_match:
        return SQLGenerationResult(
            sql=sanitize_sql(legacy_match),
            raw_response=legacy_match,
            notes="Matched legacy golden query cheat sheet fast path.",
            explanation="",
        )

    config = load_config()
    if not has_live_model_config(config):
        setup_note = get_live_model_setup_message(config)
        return SQLGenerationResult(sql="", raw_response=setup_note, notes=setup_note, explanation="")

    raw_response = await _request_model_text(user_prompt, system_prompt, recent_context)
    sql, explanation = _extract_sql_and_explanation(raw_response)
    return SQLGenerationResult(
        sql=sql,
        raw_response=raw_response,
        notes="SQL generated successfully.",
        explanation=explanation,
    )


def generate_corrected_sql(
    user_prompt: str,
    system_prompt: str,
    failed_sql: str,
    error_message: str,
    recent_context: list[dict[str, Any]] | None = None,
) -> SQLGenerationResult:
    """Attempt to correct a failed SQL query."""
    return _run_async_blocking(
        generate_corrected_sql_async(
            user_prompt, system_prompt, failed_sql, error_message, recent_context
        )
    )


async def generate_corrected_sql_async(
    user_prompt: str,
    system_prompt: str,
    failed_sql: str,
    error_message: str,
    recent_context: list[dict[str, Any]] | None = None,
) -> SQLGenerationResult:
    """Attempt to correct a failed SQL query asynchronously."""
    correction_prompt = f"""{user_prompt}

The previous SQL query failed:
```sql
{failed_sql}
```

The database returned this error:
{error_message}

Please correct the SQL query and try again.
"""
    raw_response = await _call_llm_async(
        correction_prompt, system_prompt, recent_context
    )
    sql, explanation, notes = _parse_llm_chat_response(raw_response)
    return SQLGenerationResult(
        sql=sanitize_sql(sql),
        raw_response=raw_response,
        notes=notes,
        explanation=explanation,
    )


def generate_general_chat_reply(
    question: str, recent_context: list[dict[str, Any]] | None = None
) -> str:
    """Generate a reply for a general chat question."""
    return _run_async_blocking(
        generate_general_chat_reply_async(question, recent_context)
    )


async def generate_general_chat_reply_async(
    question: str, recent_context: list[dict[str, Any]] | None = None
) -> str:
    """Generate a reply for a general chat question asynchronously."""
    template = _get_research_hypothesis_template(question)
    if template is not None:
        return template.code

    messages = [{"role": "user", "content": question}]
    return await _request_model_messages(messages, GENERAL_CHAT_SYSTEM_PROMPT)
