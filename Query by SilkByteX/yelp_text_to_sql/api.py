# Forcing a recompile to clear stale cache
from __future__ import annotations

from dataclasses import asdict
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from yelp_text_to_sql.config import AppConfig, has_live_model_config, load_config
from yelp_text_to_sql.database import (
    QueryResult,
    execute_sql,
    run_test_query,
    describe_table_schema,
)
from yelp_text_to_sql.pipeline import PipelineResult, run_natural_language_query
from yelp_text_to_sql.prompt_schema import build_prompt_bundle, build_schema_prompt_text
from yelp_text_to_sql.schema_definitions import (
    get_schema_verification_checklist,
    get_table_schemas,
)
from yelp_text_to_sql.sql_generation import generate_sql


class SQLExecutionRequest(BaseModel):
    sql: str = Field(..., min_length=1)


class DescribeTableRequest(BaseModel):
    table_name: str = Field(..., min_length=1)


class GenerateSQLRequest(BaseModel):
    question: str = Field(..., min_length=1)
    recent_context: list[dict[str, Any]] | None = None


class TextToSQLRequest(BaseModel):
    question: str = Field(..., min_length=1)
    use_demo_mode: bool = False
    recent_context: list[dict[str, Any]] | None = None


def _serialize_query_result(result: QueryResult) -> dict[str, Any]:
    return asdict(result)


def _serialize_pipeline_result(result: PipelineResult) -> dict[str, Any]:
    return asdict(result)


def _serialize_config(config: AppConfig) -> dict[str, Any]:
    return {
        "app_title": config.app_title,
        "page_icon": config.page_icon,
        "default_question": config.default_question,
        "deepseek_model": config.deepseek_model,
        "deepseek_base_url": config.deepseek_base_url,
        "debug_mode": config.debug_mode,
        "has_deepseek_api_key": bool(config.deepseek_api_key),
    }


def create_app() -> FastAPI:
    app = FastAPI(
        title="Query by Silkbyte X API",
        version="1.0.0",
        description=(
            "FastAPI wrapper around the SilkbyteX Query Yelp Text-to-SQL pipeline so external "
            "clients such as PySpark jobs can call the same backend used by Streamlit."
        ),
    )

    @app.get("/")
    def root() -> dict[str, Any]:
        config = load_config()
        return {
            "service": "yelp-text-to-sql-api",
            "status": "ok",
            "docs_url": "/docs",
            "redoc_url": "/redoc",
            "live_model_configured": has_live_model_config(config),
        }

    @app.get("/health")
    def health() -> dict[str, Any]:
        config = load_config()
        database_config = type("obj", (object,), {"engine": "unknown", "hive_host": "", "hive_port": "", "hive_database": "", "hive_auth": "", "spark_master": "", "spark_app_name": "", "spark_warehouse_dir": "", "hive_metastore_uri": "", "spark_sql_catalog_implementation": "", "config_errors": []})
        database_issue = None
        return {
            "status": "ok",
            "app": _serialize_config(config),
            "live_model_configured": has_live_model_config(config),
            "database_engine": database_config.engine,
            "database_ready": database_issue is None,
            "database_issue": (
                None
                if database_issue is None
                else {
                    "error": database_issue.error,
                    "message": database_issue.message,
                }
            ),
        }

    @app.get("/config")
    def config_summary() -> dict[str, Any]:
        config = load_config()
        database_config = type("obj", (object,), {"engine": "unknown", "hive_host": "", "hive_port": "", "hive_database": "", "hive_auth": "", "spark_master": "", "spark_app_name": "", "spark_warehouse_dir": "", "hive_metastore_uri": "", "spark_sql_catalog_implementation": "", "config_errors": []})
        database_issue = None
        return {
            "app": _serialize_config(config),
            "database": {
                "engine": database_config.engine,
                "hive_host": database_config.hive_host,
                "hive_port": database_config.hive_port,
                "hive_database": database_config.hive_database,
                "hive_auth": database_config.hive_auth,
                "spark_master": database_config.spark_master,
                "spark_app_name": database_config.spark_app_name,
                "spark_warehouse_dir": database_config.spark_warehouse_dir,
                "hive_metastore_uri": database_config.hive_metastore_uri,
                "spark_sql_catalog_implementation": database_config.spark_sql_catalog_implementation,
                "config_errors": database_config.config_errors,
                "validation_issue": (
                    None
                    if database_issue is None
                    else {
                        "error": database_issue.error,
                        "message": database_issue.message,
                    }
                ),
            },
        }

    @app.get("/schema")
    def schema() -> dict[str, Any]:
        return {
            "tables": get_table_schemas(),
            "verification_checklist": get_schema_verification_checklist(),
            "schema_prompt_text": build_schema_prompt_text(),
        }

    @app.post("/describe-table")
    def describe_table(request: DescribeTableRequest) -> dict[str, Any]:
        return _serialize_query_result(describe_table_schema(request.table_name))

    @app.post("/run-test-query")
    def run_test() -> dict[str, Any]:
        return _serialize_query_result(run_test_query())

    @app.post("/execute-sql")
    def execute_sql_endpoint(request: SQLExecutionRequest) -> dict[str, Any]:
        return _serialize_query_result(execute_sql(request.sql))

    @app.post("/generate-sql")
    def generate_sql_endpoint(request: GenerateSQLRequest) -> dict[str, Any]:
        prompt_bundle = build_prompt_bundle(request.question)
        generation_result = generate_sql(
            prompt_bundle.user_prompt,
            prompt_bundle.system_prompt,
            recent_context=request.recent_context,
        )
        return {
            "question": request.question,
            "schema_loaded": prompt_bundle.schema_loaded,
            "system_prompt": prompt_bundle.system_prompt,
            "user_prompt": prompt_bundle.user_prompt,
            "sql": generation_result.sql,
            "explanation": generation_result.explanation,
            "notes": generation_result.notes,
            "raw_response": generation_result.raw_response,
        }

    @app.post("/text-to-sql")
    def text_to_sql(request: TextToSQLRequest) -> dict[str, Any]:
        result = run_natural_language_query(
            request.question,
            use_demo_mode=request.use_demo_mode,
            recent_context=request.recent_context,
        )
        return _serialize_pipeline_result(result)

    return app


app = create_app()
