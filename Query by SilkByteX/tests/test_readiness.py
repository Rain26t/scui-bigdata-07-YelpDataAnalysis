from __future__ import annotations

from yelp_text_to_sql import ui


def test_extract_describe_column_names_filters_headers_and_metadata() -> None:
    rows = [
        {"col_name": "# col_name", "data_type": "data_type"},
        {"col_name": "business_id", "data_type": "string"},
        {"col_name": "name", "data_type": "string"},
        {"col_name": "", "data_type": ""},
        {"col_name": "# Partition Information", "data_type": ""},
        {"col_name": "partition_col", "data_type": "string"},
    ]

    assert ui._extract_describe_column_names(rows) == ["business_id", "name"]


def test_compare_schema_columns_reports_missing_and_extra_columns() -> None:
    comparison = ui._compare_schema_columns(
        ["business_id", "name", "city"],
        ["business_id", "name", "state"],
    )

    assert comparison["missing_columns"] == ["city"]
    assert comparison["extra_columns"] == ["state"]
    assert comparison["matched_column_count"] == 2
    assert comparison["expected_column_count"] == 3
    assert comparison["live_column_count"] == 3


def test_should_auto_fallback_to_demo_mode_only_after_live_failures() -> None:
    assert ui._should_auto_fallback_to_demo_mode(
        used_demo_mode=False,
        success=False,
        status="retry_failed",
    )
    assert not ui._should_auto_fallback_to_demo_mode(
        used_demo_mode=True,
        success=False,
        status="retry_failed",
    )
    assert not ui._should_auto_fallback_to_demo_mode(
        used_demo_mode=False,
        success=True,
        status="success",
    )
    assert not ui._should_auto_fallback_to_demo_mode(
        used_demo_mode=False,
        success=False,
        status="input_error",
    )
