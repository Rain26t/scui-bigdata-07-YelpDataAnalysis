from __future__ import annotations

import json
from typing import Any
from urllib.parse import quote_plus
from urllib.request import Request, urlopen

import pandas as pd
import streamlit as st

# ---------------------------------------------------------------------------
# Color palette — premium editorial / BI dashboard theme
# ---------------------------------------------------------------------------
_BAR_COLORS = [
    "#cbaa74",
    "#cbaa74",
    "#cbaa74",
    "#cbaa74",
    "#cbaa74",
    "#cbaa74",
    "#cbaa74",
    "#cbaa74",
]

_LINE_COLOR = "#cbaa74"
_LINE_FILL_COLOR = "rgba(203, 170, 116, 0.14)"
_GRID_COLOR = "rgba(0, 0, 0, 0)"
_FONT_FAMILY = "Manrope, Segoe UI, sans-serif"
_MAP_GLOW_COLOR = "rgba(175, 127, 89, 0.18)"
_MAP_MARKER_COLOR = "#d6af8c"
_MAP_TEXT_COLOR = "#f4ede7"
_MAP_BG_COLOR = "#111111"


# ---------------------------------------------------------------------------
# Column detection helpers (unchanged public API for test compatibility)
# ---------------------------------------------------------------------------

def _is_numeric_column(dataframe: pd.DataFrame, column_name: str) -> bool:
    """Return True when a column is numeric enough for a simple chart."""
    return pd.api.types.is_numeric_dtype(dataframe[column_name])


def _is_date_like_column(dataframe: pd.DataFrame, column_name: str) -> bool:
    """Return True when a column looks like a year, date, or timestamp.

    This stays intentionally simple for a beginner-friendly app:
    - column names such as year/date/month/day/time are treated as date-like
    - values that pandas can mostly parse as datetimes are also treated as date-like
    """
    lower_name = column_name.lower()
    date_words = ("year", "date", "month", "day", "time")
    if any(word in lower_name for word in date_words):
        return True

    parsed_values = pd.to_datetime(dataframe[column_name], errors="coerce", format="mixed")
    return parsed_values.notna().sum() >= max(1, len(dataframe) // 2)


def _find_chart_columns(dataframe: pd.DataFrame) -> tuple[str, str, str] | None:
    """Pick a chart type and the two best columns for it.

    Rules applied in order:
    1. If exactly two columns: one numeric + one label/date → chart
    2. If more columns: find the first (label, numeric) pair that works
    """
    columns = dataframe.columns.tolist()

    if len(columns) < 2:
        return None

    # --- Fast path: exactly two columns (original behavior) ---
    if len(columns) == 2:
        first, second = columns
        first_num = _is_numeric_column(dataframe, first)
        second_num = _is_numeric_column(dataframe, second)

        if first_num == second_num:
            return None

        label_col = second if first_num else first
        num_col = first if first_num else second

        if _is_date_like_column(dataframe, label_col):
            return ("line", label_col, num_col)
        return ("bar", label_col, num_col)

    # --- Extended path: pick the best label+numeric pair from wider tables ---
    numeric_cols = [c for c in columns if _is_numeric_column(dataframe, c)]
    label_cols = [c for c in columns if not _is_numeric_column(dataframe, c)]

    if not numeric_cols or not label_cols:
        return None

    # Prefer the first label column and the first numeric column
    best_label = label_cols[0]
    best_numeric = numeric_cols[0]

    if _is_date_like_column(dataframe, best_label):
        return ("line", best_label, best_numeric)
    return ("bar", best_label, best_numeric)


def _pretty_label(column_name: str) -> str:
    """Turn a SQL column name into a human-friendly axis label."""
    return column_name.replace("_", " ").title()


def _find_matching_column(
    columns: list[str],
    candidate_names: tuple[str, ...],
) -> str | None:
    """Return the first column whose normalized name matches one candidate."""
    normalized_lookup = {column.lower().strip(): column for column in columns}
    for candidate in candidate_names:
        match = normalized_lookup.get(candidate)
        if match:
            return match
    return None


def _find_map_columns(dataframe: pd.DataFrame) -> tuple[str, str, str] | None:
    """Return the best map column pair for lat/lon or city/state results."""
    columns = dataframe.columns.tolist()

    latitude_column = _find_matching_column(columns, ("latitude", "lat"))
    longitude_column = _find_matching_column(columns, ("longitude", "lon", "lng", "long"))
    if latitude_column and longitude_column:
        return ("latlon", latitude_column, longitude_column)

    city_column = _find_matching_column(columns, ("city",))
    state_column = _find_matching_column(columns, ("state",))
    if city_column and state_column:
        return ("citystate", city_column, state_column)

    return None


def _build_map_hover_text(dataframe: pd.DataFrame) -> pd.Series:
    """Create one clean hover label per row for the map."""
    preferred_columns = (
        "name",
        "business_name",
        "city",
        "state",
        "stars",
        "review_count",
        "business_count",
    )
    column_lookup = {column.lower(): column for column in dataframe.columns}
    selected_columns = [
        column_lookup[column_name]
        for column_name in preferred_columns
        if column_name in column_lookup
    ]

    if not selected_columns:
        selected_columns = list(dataframe.columns[:3])

    hover_texts: list[str] = []
    for _, row in dataframe.iterrows():
        parts: list[str] = []
        for column_name in selected_columns:
            value = row[column_name]
            if pd.isna(value):
                continue
            pretty_name = _pretty_label(column_name)
            if column_name.lower() in {"name", "business_name"} and not parts:
                parts.append(f"<b>{value}</b>")
                continue
            parts.append(f"{pretty_name}: {value}")
        hover_texts.append("<br>".join(parts) if parts else "Location")

    return pd.Series(hover_texts)


def _estimate_map_zoom(dataframe: pd.DataFrame, latitude_column: str, longitude_column: str) -> float:
    """Estimate a friendly zoom level from the coordinate spread."""
    latitude_span = abs(dataframe[latitude_column].max() - dataframe[latitude_column].min())
    longitude_span = abs(dataframe[longitude_column].max() - dataframe[longitude_column].min())
    max_span = max(latitude_span, longitude_span)

    if max_span <= 0.02:
        return 11.5
    if max_span <= 0.08:
        return 10.2
    if max_span <= 0.25:
        return 8.8
    if max_span <= 0.75:
        return 7.2
    if max_span <= 2.5:
        return 5.8
    if max_span <= 6:
        return 4.8
    if max_span <= 12:
        return 4.0
    return 3.2


@st.cache_data(show_spinner=False, ttl=60 * 60 * 24)
def _geocode_city_state(city: str, state: str) -> tuple[float, float] | None:
    """Resolve one city/state pair to coordinates using OpenStreetMap Nominatim."""
    query = quote_plus(f"{city}, {state}")
    request = Request(
        (
            "https://nominatim.openstreetmap.org/search"
            f"?format=jsonv2&limit=1&q={query}"
        ),
        headers={
            "User-Agent": "yelp-text-to-sql-streamlit-app/1.0",
            "Accept": "application/json",
        },
    )

    try:
        with urlopen(request, timeout=4) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception:
        return None

    if not payload:
        return None

    try:
        return (float(payload[0]["lat"]), float(payload[0]["lon"]))
    except (KeyError, TypeError, ValueError):
        return None


def _resolve_city_state_coordinates(
    dataframe: pd.DataFrame,
    city_column: str,
    state_column: str,
) -> pd.DataFrame:
    """Attach cached coordinates to a city/state result set when possible."""
    working_dataframe = dataframe.copy()
    coordinate_pairs: list[dict[str, Any]] = []

    unique_locations = (
        working_dataframe[[city_column, state_column]]
        .dropna()
        .drop_duplicates()
        .head(60)
    )

    for _, row in unique_locations.iterrows():
        city_value = str(row[city_column]).strip()
        state_value = str(row[state_column]).strip()
        if not city_value or not state_value:
            continue

        coordinates = _geocode_city_state(city_value, state_value)
        if coordinates is None:
            continue

        latitude_value, longitude_value = coordinates
        coordinate_pairs.append(
            {
                city_column: city_value,
                state_column: state_value,
                "__map_latitude": latitude_value,
                "__map_longitude": longitude_value,
            }
        )

    if not coordinate_pairs:
        return pd.DataFrame()

    coordinates_dataframe = pd.DataFrame(coordinate_pairs)
    merged_dataframe = working_dataframe.copy()
    merged_dataframe[city_column] = merged_dataframe[city_column].astype(str).str.strip()
    merged_dataframe[state_column] = merged_dataframe[state_column].astype(str).str.strip()
    merged_dataframe = merged_dataframe.merge(
        coordinates_dataframe,
        on=[city_column, state_column],
        how="left",
    )
    return merged_dataframe.dropna(subset=["__map_latitude", "__map_longitude"])


def _prepare_map_dataframe(rows: list[dict[str, Any]]) -> tuple[pd.DataFrame, str, str] | None:
    """Return map-ready rows plus resolved latitude/longitude column names."""
    dataframe = pd.DataFrame(rows).copy()
    map_details = _find_map_columns(dataframe)
    if map_details is None:
        return None

    map_mode, first_column, second_column = map_details

    if map_mode == "latlon":
        dataframe[first_column] = pd.to_numeric(dataframe[first_column], errors="coerce")
        dataframe[second_column] = pd.to_numeric(dataframe[second_column], errors="coerce")
        dataframe = dataframe.dropna(subset=[first_column, second_column])
        if dataframe.empty:
            return None
        return (dataframe, first_column, second_column)

    geocoded_dataframe = _resolve_city_state_coordinates(dataframe, first_column, second_column)
    if geocoded_dataframe.empty:
        return None
    return (geocoded_dataframe, "__map_latitude", "__map_longitude")


# ---------------------------------------------------------------------------
# Plotly chart builders
# ---------------------------------------------------------------------------

def _build_bar_chart(dataframe: pd.DataFrame, label_col: str, numeric_col: str) -> Any:
    """Build a polished Plotly horizontal bar chart."""
    import plotly.express as px

    # Assign colors cyclically
    n_bars = len(dataframe)
    colors = [_BAR_COLORS[i % len(_BAR_COLORS)] for i in range(n_bars)]

    fig = px.bar(
        dataframe,
        x=numeric_col,
        y=label_col,
        orientation="h",
        color_discrete_sequence=[_BAR_COLORS[0]],
        labels={
            label_col: _pretty_label(label_col),
            numeric_col: _pretty_label(numeric_col),
        },
    )

    # Apply per-bar colors for visual variety
    fig.update_traces(
        marker_color=colors,
        marker_line_width=0,
        hovertemplate=(
            f"<b>%{{y}}</b><br>"
            f"{_pretty_label(numeric_col)}: %{{x:,.0f}}<extra></extra>"
        ),
    )

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family=_FONT_FAMILY, color="#1c1b1a"),
        margin=dict(l=20, r=20, t=28, b=18),
        xaxis=dict(
            showgrid=False,
            title=_pretty_label(numeric_col),
        ),
        yaxis=dict(
            showgrid=False,
            title=_pretty_label(label_col),
            autorange="reversed",
        ),
        bargap=0.28,
        showlegend=False,
        height=420,
    )

    return fig


def _build_line_chart(dataframe: pd.DataFrame, label_col: str, numeric_col: str) -> Any:
    """Build a polished Plotly line chart with a subtle area fill."""
    import plotly.graph_objects as go

    df = dataframe.copy()
    df[label_col] = pd.to_datetime(df[label_col], errors="coerce", format="mixed")
    df = df.sort_values(by=label_col).dropna(subset=[label_col])

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df[label_col],
        y=df[numeric_col],
        mode="lines+markers",
        line=dict(color=_LINE_COLOR, width=2.5, shape="spline"),
        marker=dict(size=7, color="#fff", line=dict(color=_LINE_COLOR, width=2)),
        fill="tozeroy",
        fillcolor=_LINE_FILL_COLOR,
        hovertemplate=(
            f"<b>%{{x}}</b><br>"
            f"{_pretty_label(numeric_col)}: %{{y:,.0f}}<extra></extra>"
        ),
    ))

    fig.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        font=dict(family=_FONT_FAMILY, color="#1c1b1a"),
        margin=dict(l=20, r=20, t=28, b=18),
        xaxis=dict(
            title=_pretty_label(label_col),
            showgrid=False,
        ),
        yaxis=dict(
            title=_pretty_label(numeric_col),
            showgrid=False,
        ),
        showlegend=False,
        height=420,
    )

    return fig


def _build_location_map(dataframe: pd.DataFrame, latitude_column: str, longitude_column: str) -> Any:
    """Build a dark interactive location map with glowing bronze markers."""
    import plotly.graph_objects as go

    figure = go.Figure()
    hover_text = _build_map_hover_text(dataframe)
    zoom_level = _estimate_map_zoom(dataframe, latitude_column, longitude_column)
    center_latitude = float(dataframe[latitude_column].mean())
    center_longitude = float(dataframe[longitude_column].mean())

    figure.add_trace(
        go.Scattermapbox(
            lat=dataframe[latitude_column],
            lon=dataframe[longitude_column],
            mode="markers",
            hoverinfo="skip",
            marker=dict(
                size=26,
                color=_MAP_GLOW_COLOR,
            ),
            showlegend=False,
        )
    )

    figure.add_trace(
        go.Scattermapbox(
            lat=dataframe[latitude_column],
            lon=dataframe[longitude_column],
            mode="markers",
            text=hover_text,
            hovertemplate="%{text}<extra></extra>",
            marker=dict(
                size=11,
                color=_MAP_MARKER_COLOR,
                opacity=0.92,
            ),
            showlegend=False,
        )
    )

    figure.update_layout(
        mapbox=dict(
            style="carto-darkmatter",
            center=dict(lat=center_latitude, lon=center_longitude),
            zoom=zoom_level,
        ),
        margin=dict(l=10, r=10, t=10, b=10),
        height=460,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(family=_FONT_FAMILY, color=_MAP_TEXT_COLOR),
    )

    return figure


# ---------------------------------------------------------------------------
# Public chart helpers
# ---------------------------------------------------------------------------

def render_map(rows: list[dict[str, Any]]) -> None:
    """Render a dark, interactive map if the result has location data."""
    map_data = _prepare_map_dataframe(rows)
    if map_data is None:
        st.info("The result set does not contain renderable map data.")
        return

    dataframe, latitude_column, longitude_column = map_data
    figure = _build_location_map(dataframe, latitude_column, longitude_column)
    st.plotly_chart(figure, use_container_width=True)


def build_chart_figure(rows: list[dict[str, Any]]) -> Any | None:
    """Return a polished Plotly figure when the result shape is a fit."""
    if not rows:
        return None

    dataframe = pd.DataFrame(rows).copy()
    chart_details = _find_chart_columns(dataframe)

    if chart_details is None:
        return None

    chart_type, label_column, numeric_column = chart_details

    if chart_type == "line":
        return _build_line_chart(dataframe, label_column, numeric_column)
    return _build_bar_chart(dataframe, label_column, numeric_column)


def _figure_to_png_bytes(
    figure: Any,
    *,
    width: int,
    height: int,
    scale: int,
) -> bytes:
    """Convert one Plotly figure into PNG bytes."""
    import plotly.io as pio

    return pio.to_image(
        figure,
        format="png",
        width=width,
        height=height,
        scale=scale,
    )


@st.cache_data(show_spinner=False, ttl=60 * 5)
def export_chart_png_bytes(
    rows: list[dict[str, Any]],
    *,
    width: int = 1600,
    height: int = 900,
    scale: int = 2,
) -> bytes | None:
    """Return high-resolution PNG bytes for one chart when export is possible."""
    figure = build_chart_figure(rows)
    if figure is None:
        return None

    try:
        return _figure_to_png_bytes(
            figure,
            width=width,
            height=height,
            scale=scale,
        )
    except Exception:
        return None


def render_chart(
    rows: list[dict[str, Any]] | pd.DataFrame,
    chart_type: str | None = None,
    x_column: str | None = None,
    y_column: str | None = None,
    **kwargs: Any,
) -> None:
    """Render a chart with backward-compatible call signatures."""
    if isinstance(rows, pd.DataFrame):
        dataframe = rows.copy()
    else:
        if not rows:
            return
        dataframe = pd.DataFrame(rows).copy()

    if chart_type and x_column and y_column:
        resolved = (chart_type, x_column, y_column)
    else:
        resolved = _find_chart_columns(dataframe)
        if resolved is None:
            return

    resolved_type, resolved_x, resolved_y = resolved
    if resolved_type == "line":
        figure = _build_line_chart(dataframe, resolved_x, resolved_y)
    else:
        figure = _build_bar_chart(dataframe, resolved_x, resolved_y)

    st.plotly_chart(figure, use_container_width=True)
