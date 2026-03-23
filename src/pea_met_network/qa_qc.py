"""QA/QC summary functions for meteorological data."""

from __future__ import annotations

import pandas as pd


def missingness_summary(
    frame: pd.DataFrame,
    station_column: str = "station",
) -> pd.DataFrame:
    """Compute missingness counts by station and variable.

    Returns a DataFrame with columns: station, variable,
    missing_count, total_count, missing_pct.
    """
    if station_column not in frame.columns:
        raise ValueError(
            f"Station column '{station_column}' not found in frame"
        )

    data_columns = [c for c in frame.columns if c not in {
        station_column, "timestamp_utc", "source_file", "schema_family",
    }]

    rows: list[dict] = []
    for station, group in frame.groupby(station_column):
        for variable in data_columns:
            if variable not in group.columns:
                continue
            total = len(group)
            missing = int(group[variable].isna().sum())
            rows.append({
                "station": station,
                "variable": variable,
                "missing_count": missing,
                "total_count": total,
                "missing_pct": (
                    round(missing / total * 100, 2)
                    if total > 0 else 0.0
                ),
            })

    return pd.DataFrame(rows)


def duplicate_timestamps(
    frame: pd.DataFrame,
    station_column: str = "station",
    timestamp_column: str = "timestamp_utc",
) -> pd.DataFrame:
    """Detect duplicate (station, timestamp) pairs.

    Returns a DataFrame of duplicated rows.
    """
    if station_column not in frame.columns:
        raise ValueError(f"Station column '{station_column}' not found")
    if timestamp_column not in frame.columns:
        raise ValueError(f"Timestamp column '{timestamp_column}' not found")

    dupes = frame.duplicated(
        subset=[station_column, timestamp_column], keep=False
    )
    return frame[dupes].sort_values([station_column, timestamp_column])


def out_of_range_values(
    frame: pd.DataFrame,
    ranges: dict[str, tuple[float, float]] | None = None,
    station_column: str = "station",
) -> pd.DataFrame:
    """Detect out-of-range values in specified columns.

    Args:
        frame: Input DataFrame.
        ranges: Mapping of column_name -> (min, max). Defaults to reasonable
                meteorological bounds if not provided.

    Returns a DataFrame of rows containing out-of-range values, with an
    additional 'oov_column' and 'oov_value' column.
    """
    if ranges is None:
        ranges = {
            "air_temperature_c": (-60.0, 60.0),
            "relative_humidity_pct": (0.0, 105.0),
            "dew_point_c": (-60.0, 40.0),
            "rain_mm": (0.0, 500.0),
            "wind_speed_kmh": (0.0, 200.0),
            "wind_speed_ms": (0.0, 80.0),
            "wind_gust_speed_kmh": (0.0, 250.0),
            "wind_gust_speed_max_kmh": (0.0, 250.0),
            "solar_radiation_w_m2": (0.0, 1500.0),
            "wind_direction_deg": (0.0, 360.0),
        }

    oov_rows: list[dict] = []
    for col, (lo, hi) in ranges.items():
        if col not in frame.columns:
            continue
        mask = (frame[col] < lo) | (frame[col] > hi)
        for idx in frame.index[mask]:
            row = frame.loc[idx].to_dict()
            row["oov_column"] = col
            row["oov_value"] = frame.loc[idx, col]
            row["oov_range"] = f"[{lo}, {hi}]"
            oov_rows.append(row)

    if not oov_rows:
        return pd.DataFrame()

    return pd.DataFrame(oov_rows)


def coverage_summary(
    frame: pd.DataFrame,
    station_column: str = "station",
    timestamp_column: str = "timestamp_utc",
) -> pd.DataFrame:
    """Compute coverage summary by station and date range.

    Returns a DataFrame with columns: station, first_timestamp, last_timestamp,
    total_records, expected_hourly, coverage_pct.
    """
    if station_column not in frame.columns:
        raise ValueError(f"Station column '{station_column}' not found")

    rows: list[dict] = []
    for station, group in frame.groupby(station_column):
        if timestamp_column in group.columns:
            ts = pd.to_datetime(group[timestamp_column])
            first_ts = ts.min()
            last_ts = ts.max()
            span_hours = (last_ts - first_ts).total_seconds() / 3600.0
            expected = int(span_hours) + 1 if span_hours > 0 else 0
        else:
            first_ts = None
            last_ts = None
            expected = 0

        total = len(group)
        rows.append({
            "station": station,
            "first_timestamp": str(first_ts) if first_ts else None,
            "last_timestamp": str(last_ts) if last_ts else None,
            "total_records": total,
            "expected_hourly": expected,
            "coverage_pct": (
                round(total / expected * 100, 2)
                if expected > 0 else 0.0
            ),
        })

    return pd.DataFrame(rows)
