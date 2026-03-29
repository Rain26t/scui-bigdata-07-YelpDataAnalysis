from __future__ import annotations

import pandas as pd

from yelp_text_to_sql import charts


def test_find_chart_columns_returns_bar_for_label_and_numeric_columns() -> None:
    dataframe = pd.DataFrame(
        [
            {"city": "Las Vegas", "business_count": 10},
            {"city": "Phoenix", "business_count": 8},
        ]
    )

    assert charts._find_chart_columns(dataframe) == ("bar", "city", "business_count")


def test_find_chart_columns_returns_line_for_year_and_numeric_columns() -> None:
    dataframe = pd.DataFrame(
        [
            {"review_year": "2021", "review_count": 5},
            {"review_year": "2022", "review_count": 9},
        ]
    )

    assert charts._find_chart_columns(dataframe) == ("line", "review_year", "review_count")


def test_find_chart_columns_returns_none_for_unsuitable_result_shape() -> None:
    dataframe = pd.DataFrame(
        [
            {"city": "Las Vegas", "business_count": 10, "avg_stars": 4.5},
            {"city": "Phoenix", "business_count": 8, "avg_stars": 4.2},
        ]
    )

    # With the extended logic, a 3-column result now picks the best pair
    result = charts._find_chart_columns(dataframe)
    assert result is not None
    assert result[0] == "bar"
    assert result[1] == "city"
    assert result[2] == "business_count"


def test_find_chart_columns_returns_none_for_all_numeric() -> None:
    """All-numeric results with no label column cannot be charted."""
    dataframe = pd.DataFrame(
        [
            {"total": 100, "average": 4.5},
            {"total": 200, "average": 4.8},
        ]
    )

    assert charts._find_chart_columns(dataframe) is None


def test_find_chart_columns_returns_none_for_single_column() -> None:
    """A single-column result cannot be charted."""
    dataframe = pd.DataFrame([{"count": 42}])

    assert charts._find_chart_columns(dataframe) is None


def test_find_map_columns_detects_latitude_and_longitude() -> None:
    dataframe = pd.DataFrame(
        [
            {"name": "Cafe", "latitude": 36.12, "longitude": -115.17},
            {"name": "Diner", "latitude": 33.45, "longitude": -112.07},
        ]
    )

    assert charts._find_map_columns(dataframe) == ("latlon", "latitude", "longitude")


def test_find_map_columns_detects_city_and_state() -> None:
    dataframe = pd.DataFrame(
        [
            {"city": "Las Vegas", "state": "NV", "business_count": 10},
            {"city": "Phoenix", "state": "AZ", "business_count": 8},
        ]
    )

    assert charts._find_map_columns(dataframe) == ("citystate", "city", "state")


def test_render_chart_uses_bar_chart_for_label_and_numeric_rows(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        charts.st,
        "plotly_chart",
        lambda fig, **kwargs: calls.append(f"plotly:{type(fig).__name__}"),
    )

    charts.render_chart(
        [
            {"city": "Las Vegas", "business_count": 10},
            {"city": "Phoenix", "business_count": 8},
        ]
    )

    assert len(calls) == 1
    assert "plotly" in calls[0]


def test_render_chart_uses_line_chart_for_date_like_rows(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        charts.st,
        "plotly_chart",
        lambda fig, **kwargs: calls.append(f"plotly:{type(fig).__name__}"),
    )

    charts.render_chart(
        [
            {"date": "2023-01-01", "review_count": 5},
            {"date": "2023-01-02", "review_count": 9},
        ]
    )

    assert len(calls) == 1
    assert "plotly" in calls[0]


def test_render_map_uses_plotly_for_latitude_longitude_rows(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        charts.st,
        "plotly_chart",
        lambda fig, **kwargs: calls.append(f"plotly:{type(fig).__name__}"),
    )

    charts.render_map(
        [
            {"name": "Cafe", "latitude": 36.12, "longitude": -115.17},
            {"name": "Diner", "latitude": 33.45, "longitude": -112.07},
        ]
    )

    assert len(calls) == 1
    assert "plotly" in calls[0]


def test_render_map_uses_city_state_geocoding_when_coordinates_are_missing(monkeypatch) -> None:
    calls: list[str] = []

    monkeypatch.setattr(
        charts.st,
        "plotly_chart",
        lambda fig, **kwargs: calls.append(f"plotly:{type(fig).__name__}"),
    )
    monkeypatch.setattr(
        charts,
        "_geocode_city_state",
        lambda city, state: (36.1699, -115.1398) if city == "Las Vegas" and state == "NV" else None,
    )

    charts.render_map(
        [
            {"city": "Las Vegas", "state": "NV", "business_count": 10},
        ]
    )

    assert len(calls) == 1
    assert "plotly" in calls[0]


def test_render_chart_does_nothing_for_empty_rows() -> None:
    """Calling render_chart with no rows should not raise."""
    charts.render_chart([])


def test_build_chart_figure_returns_none_for_empty_rows() -> None:
    assert charts.build_chart_figure([]) is None


def test_export_chart_png_bytes_uses_plotly_image_export(monkeypatch) -> None:
    charts.export_chart_png_bytes.clear()
    monkeypatch.setattr(
        charts,
        "_figure_to_png_bytes",
        lambda figure, **kwargs: b"png-bytes",
    )

    png_bytes = charts.export_chart_png_bytes(
        [
            {"city": "Las Vegas", "business_count": 10},
            {"city": "Phoenix", "business_count": 8},
        ]
    )

    assert png_bytes == b"png-bytes"


def test_export_chart_png_bytes_returns_none_when_export_fails(monkeypatch) -> None:
    charts.export_chart_png_bytes.clear()
    monkeypatch.setattr(
        charts,
        "_figure_to_png_bytes",
        lambda figure, **kwargs: (_ for _ in ()).throw(RuntimeError("Missing image engine")),
    )

    png_bytes = charts.export_chart_png_bytes(
        [
            {"city": "Las Vegas", "business_count": 10},
            {"city": "Phoenix", "business_count": 8},
        ]
    )

    assert png_bytes is None
