# Forcing a recompile to clear stale cache
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
import queue
import re
import threading
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from yelp_text_to_sql.config import load_config

# --- Database Connection Pool ---
DB_ENGINE = None
SPARK_SESSION = None
_SPARK_SESSION_LOCK = threading.Lock()
DEFAULT_QUERY_TIMEOUT_SECONDS = int(os.getenv("YELP_SQL_QUERY_TIMEOUT_SECONDS", "300") or "300")
_QUERY_RESULT_CACHE: dict[str, QueryResult] = {}
_TABLE_NAME_CACHE: dict[str, set[str]] = {}


@dataclass
class DatabaseConfig:
    engine: str = "hive"
    hive_host: str = ""
    hive_port: int = 10000
    hive_database: str = "default"
    hive_auth: str = "NONE"
    hive_username: str = ""
    hive_password: str = ""
    spark_master: str = "local[*]"
    spark_app_name: str = "YelpTextToSQL"
    spark_warehouse_dir: str = ""
    hive_metastore_uri: str = ""
    spark_sql_catalog_implementation: str = "hive"


def clear_query_result_cache() -> None:
    _QUERY_RESULT_CACHE.clear()
    _TABLE_NAME_CACHE.clear()


def load_database_config() -> DatabaseConfig:
    host = os.getenv("HIVE_HOST", "").strip()
    return DatabaseConfig(
        engine=os.getenv("YELP_SQL_ENGINE", "hive").strip().lower() or "hive",
        hive_host=host,
        hive_port=int(os.getenv("HIVE_PORT", "10000") or "10000"),
        hive_database=os.getenv("HIVE_DATABASE", "default").strip() or "default",
        hive_auth=os.getenv("HIVE_AUTH", "NONE").strip().upper() or "NONE",
        hive_username=os.getenv("HIVE_USERNAME", "").strip(),
        hive_password=os.getenv("HIVE_PASSWORD", "").strip(),
        spark_master=os.getenv("SPARK_MASTER", "local[*]").strip() or "local[*]",
        spark_app_name=os.getenv("SPARK_APP_NAME", "YelpTextToSQL").strip() or "YelpTextToSQL",
        spark_warehouse_dir=os.getenv("SPARK_WAREHOUSE_DIR", "").strip(),
        hive_metastore_uri=os.getenv("HIVE_METASTORE_URI", "").strip(),
        spark_sql_catalog_implementation=(
            os.getenv("SPARK_SQL_CATALOG_IMPLEMENTATION", "hive").strip() or "hive"
        ),
    )


def _validate_database_config(current_config: DatabaseConfig) -> None:
    if current_config.engine == "hive" and not current_config.hive_host and not load_config().database_uri:
        raise ValueError("Hive host is not configured.")
    if current_config.engine not in {"hive", "spark"}:
        raise ValueError("Unsupported database engine. Use YELP_SQL_ENGINE=hive or spark.")

def get_db_engine():
    """Initializes and returns a persistent database engine."""
    global DB_ENGINE
    if DB_ENGINE is None:
        app_config = load_config()
        db_uri = app_config.database_uri
        if not db_uri:
            raise ValueError("Database URI is not configured.")
        DB_ENGINE = create_engine(db_uri, pool_size=10, max_overflow=20)
    return DB_ENGINE


def _get_spark_session(current_config: DatabaseConfig):
    """Create and cache one SparkSession for SQL execution."""
    global SPARK_SESSION
    with _SPARK_SESSION_LOCK:
        if SPARK_SESSION is not None:
            return SPARK_SESSION
        try:
            from pyspark.sql import SparkSession
        except Exception as exc:  # pragma: no cover - import depends on runtime env
            raise RuntimeError(
                "PySpark is not installed. Install pyspark to use YELP_SQL_ENGINE=spark."
            ) from exc

        builder = SparkSession.builder.appName(current_config.spark_app_name).master(
            current_config.spark_master
        )
        if current_config.spark_sql_catalog_implementation:
            builder = builder.config(
                "spark.sql.catalogImplementation",
                current_config.spark_sql_catalog_implementation,
            )
        if current_config.spark_warehouse_dir:
            builder = builder.config("spark.sql.warehouse.dir", current_config.spark_warehouse_dir)
        if current_config.hive_metastore_uri:
            builder = builder.config("hive.metastore.uris", current_config.hive_metastore_uri)

        SPARK_SESSION = builder.enableHiveSupport().getOrCreate()
        if current_config.hive_database:
            SPARK_SESSION.sql(f"USE {current_config.hive_database}")
        return SPARK_SESSION

def get_database_config_diagnostics():
    return {}, None


def _normalize_live_table_names(sql: str) -> str:
    """Normalize SQL text without forcing one table naming convention."""
    return str(sql)


def _execute_raw_sql_for_rows(sql: str, current_config: DatabaseConfig) -> list[dict[str, Any]]:
    """Execute one raw SQL and return row dicts without wrapping QueryResult."""
    if current_config.engine == "spark":
        spark = _get_spark_session(current_config)
        dataframe = spark.sql(sql)
        return [row.asDict(recursive=True) for row in dataframe.collect()]
    engine = get_db_engine()
    with engine.connect() as connection:
        result = connection.execute(text(sql))
        return [dict(row) for row in result.mappings()]


def _list_table_names(current_config: DatabaseConfig) -> set[str]:
    """Return lowercase table names visible in the configured database."""
    cache_key = f"{current_config.engine}:{current_config.hive_database}"
    cached = _TABLE_NAME_CACHE.get(cache_key)
    if cached is not None:
        return cached
    rows = _execute_raw_sql_for_rows("SHOW TABLES", current_config)
    names: set[str] = set()
    for row in rows:
        for value in row.values():
            candidate = str(value).strip().lower()
            if candidate:
                names.add(candidate)
    _TABLE_NAME_CACHE[cache_key] = names
    return names


def _resolve_review_table_alias(sql: str, current_config: DatabaseConfig) -> str:
    """Resolve review/rating table naming so queries work across different ETL outputs."""
    normalized = _normalize_live_table_names(sql)
    lowered_sql = normalized.lower()
    if " review" not in lowered_sql and "`review`" not in lowered_sql and " rating" not in lowered_sql and "`rating`" not in lowered_sql:
        return normalized

    try:
        table_names = _list_table_names(current_config)
    except Exception:
        return normalized

    has_review = "review" in table_names
    has_rating = "rating" in table_names
    if has_review and not has_rating:
        return re.sub(
            r"(?i)\b(from|join|describe|table)\s+`?rating`?\b",
            lambda match: f"{match.group(1)} review",
            normalized,
        )
    if has_rating and not has_review:
        return re.sub(
            r"(?i)\b(from|join|describe|table)\s+`?review`?\b",
            lambda match: f"{match.group(1)} rating",
            normalized,
        )
    return normalized

@dataclass
class QueryResult:
    rows: list[dict[str, Any]] = field(default_factory=list)
    executed: bool = False
    error: str | None = None
    message: str = ""


def execute_sql(sql: str) -> QueryResult:
    """Executes a SQL query against the database using the connection pool."""
    if not isinstance(sql, str) or not sql.strip():
        return QueryResult(
            rows=[], executed=False, error="Invalid SQL provided", message="Empty or invalid SQL string."
        )
    current_config = load_database_config()
    try:
        _validate_database_config(current_config)
    except Exception as config_error:
        return QueryResult(rows=[], executed=False, error=str(config_error), message="Database configuration is invalid.")

    sql = _resolve_review_table_alias(sql, current_config)
    if sql in _QUERY_RESULT_CACHE:
        cached = _QUERY_RESULT_CACHE[sql]
        return QueryResult(
            rows=[dict(row) for row in cached.rows],
            executed=cached.executed,
            error=cached.error,
            message=f"{cached.message} (cache hit)",
        )

    def _run_query() -> QueryResult:
        try:
            if current_config.engine == "spark":
                return _execute_with_spark(sql, current_config)
            return _execute_with_hive(sql, current_config)
        except SQLAlchemyError as e:
            return QueryResult(rows=[], executed=False, error=str(e), message="Database execution failed.")
        except Exception as e:
            return QueryResult(rows=[], executed=False, error=str(e), message="An unexpected error occurred.")

    timeout_seconds = max(1, DEFAULT_QUERY_TIMEOUT_SECONDS)
    result_queue: queue.Queue[QueryResult] = queue.Queue(maxsize=1)

    def _runner() -> None:
        result_queue.put(_run_query())

    worker = threading.Thread(target=_runner, daemon=True)
    worker.start()
    worker.join(timeout=timeout_seconds)

    if worker.is_alive():
        return QueryResult(
            rows=[],
            executed=False,
            error=(
                f"Query timed out after {timeout_seconds} seconds. "
                "Try a smaller date range, add LIMIT, or run a more selective query."
            ),
            message="Database query timed out.",
        )

    if not result_queue.empty():
        final_result = result_queue.get()
        if final_result.executed:
            _QUERY_RESULT_CACHE[sql] = QueryResult(
                rows=[dict(row) for row in final_result.rows],
                executed=True,
                error=final_result.error,
                message=final_result.message,
            )
        return final_result

    return QueryResult(
        rows=[],
        executed=False,
        error="The database worker ended unexpectedly without returning a result.",
        message="Database execution failed.",
    )


def _execute_with_hive(sql: str, current_config: DatabaseConfig) -> QueryResult:
    rows = _execute_raw_sql_for_rows(sql, current_config)
    return QueryResult(
        rows=rows,
        executed=True,
        message=f"Hive query completed successfully. Returned {len(rows)} row(s).",
    )


def _execute_with_spark(sql: str, current_config: DatabaseConfig) -> QueryResult:
    rows = _execute_raw_sql_for_rows(sql, current_config)
    return QueryResult(
        rows=rows,
        executed=True,
        message=f"Spark SQL completed successfully. Returned {len(rows)} row(s).",
    )

def execute_sql_async(sql: str) -> QueryResult:
    """Executes a SQL query against the database using the connection pool."""
    return execute_sql(sql)

def run_test_query() -> QueryResult:
    """Run one fixed beginner-friendly test query.

    Use this before building any LLM flow. It helps confirm:
    1. your Python code can reach the SQL engine
    2. the business table exists
    3. rows can come back in a simple list-of-dicts format
    4. your `.env` backend settings are valid enough to run a real query

    Recommended first live validation command:
    - python3 yelp_text_to_sql/database.py
    """
    test_sql = "SELECT * FROM business LIMIT 1"
    return execute_sql(test_sql)

def run_test_query_async() -> QueryResult:
    """Run one fixed beginner-friendly test query."""
    return run_test_query()

def describe_table_schema(table_name: str) -> QueryResult:
    """Describes the schema of a given table."""
    return execute_sql(f"DESCRIBE {table_name}")

def describe_table_schema_async(table_name: str) -> QueryResult:
    """Describes the schema of a given table."""
    return describe_table_schema(table_name)

def get_table_counts() -> dict[str, int]:
    """Fetches and caches the row counts for all major tables."""
    # This is a simplified example. In a real app, you'd cache this.
    tables = ["business", "review", "users", "checkin"]
    counts = {}
    for table in tables:
        result = execute_sql(f"SELECT COUNT(*) as count FROM {table}")
        if result.executed and result.rows:
            counts[table] = result.rows[0]['count']
    return counts
