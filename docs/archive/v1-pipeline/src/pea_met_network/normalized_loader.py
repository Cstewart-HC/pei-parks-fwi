from __future__ import annotations

from pathlib import Path

import pandas as pd

from pea_met_network.manifest import recognize_schema

COLUMN_RENAMES = {
    "Temperature": "air_temperature_c",
    "RH": "relative_humidity_pct",
    "Dew Point": "dew_point_c",
    "Rain": "rain_mm",
    "Wind Direction": "wind_direction_deg",
    "Solar Radiation": "solar_radiation_w_m2",
    "Battery": "battery_v",
    "Average wind speed": "wind_speed_kmh",
    "Wind gust speed": "wind_gust_speed_kmh",
    "Wind Speed": "wind_speed_ms",
    "Gust Speed": "wind_gust_speed_max_kmh",
}

# Timestamp formats by schema family
TIMESTAMP_FORMATS = {
    "minimal_date_time_family": "%m/%d/%Y %H:%M:%S %z",
    "hoboware_date_time_family": "%m/%d/%Y %H:%M:%S %z",
    "legacy_dual_wind_family": "%m/%d/%Y %H:%M:%S %z",
    "single_timestamp_family": None,  # handled separately
}

UNSUPPORTED_FAMILIES = set()


def _normalized_name(column: str) -> str | None:
    """Map a raw CSV column name to its canonical name.

    Returns None for Date/Time columns or unrecognized columns.
    """
    if column in {"Date", "Time"}:
        return column
    prefix = column.split("(", 1)[0].strip()
    # Normalize whitespace (e.g. "Wind gust  speed" -> "Wind gust speed")
    prefix = " ".join(prefix.split())
    if prefix in COLUMN_RENAMES:
        return COLUMN_RENAMES[prefix]
    return None


def _parse_timestamp_date_time(
    frame: pd.DataFrame,
    schema_family: str,
) -> pd.Series:
    """Parse timestamp from separate Date + Time columns."""
    timestamp_text = (
        frame["Date"].astype(str).str.strip()
        + " "
        + frame["Time"].astype(str).str.strip()
    )
    fmt = TIMESTAMP_FORMATS.get(schema_family)
    if fmt is None:
        raise NotImplementedError(
            "No timestamp format configured for "
            f"schema family '{schema_family}'. "
            f"Supported families: {', '.join(TIMESTAMP_FORMATS.keys())}"
        )
    return pd.to_datetime(timestamp_text, format=fmt, utc=True)


def _parse_timestamp_single(
    frame: pd.DataFrame,
    schema_family: str,
) -> pd.Series:
    """Parse timestamp from a single Timestamp column."""
    ts_col = None
    for col in frame.columns:
        if col.lower() == "timestamp":
            ts_col = col
            break
    if ts_col is None:
        raise NotImplementedError(
            f"Schema family '{schema_family}' requires a 'Timestamp' column, "
            f"but none found in columns: {list(frame.columns[:5])}"
        )
    return pd.to_datetime(frame[ts_col], utc=True)


def load_normalized_station_csv(path: Path, station: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    schema = recognize_schema(frame.columns)
    # Build rename map, skipping unmapped columns
    rename_map = {
        col: new_name
        for col in frame.columns
        if (new_name := _normalized_name(col)) is not None
    }
    renamed = frame.rename(columns=rename_map)
    # Deduplicate columns (duplicate sensors map to same name)
    renamed = renamed.loc[:, ~renamed.columns.duplicated()]

    # Parse timestamps based on schema family
    if schema.family in UNSUPPORTED_FAMILIES:
        raise NotImplementedError(
            "Schema family "
            f"'{schema.family}' is recognized but not yet supported. "
            f"Columns: {list(frame.columns[:5])}"
        )

    if schema.family == "single_timestamp_family":
        timestamp_utc = _parse_timestamp_single(renamed, schema.family)
    elif schema.family in TIMESTAMP_FORMATS:
        timestamp_utc = _parse_timestamp_date_time(renamed, schema.family)
    else:
        raise NotImplementedError(
            "Schema family "
            f"'{schema.family}' is recognized but has no timestamp "
            "parsing strategy configured."
        )

    normalized = pd.DataFrame(
        {
            "station": station,
            "timestamp_utc": timestamp_utc,
        }
    )

    for column in renamed.columns:
        if column in {"Date", "Time", "Timestamp"}:
            continue
        normalized[column] = pd.to_numeric(renamed[column], errors="coerce")

    normalized["source_file"] = str(path)
    normalized["schema_family"] = schema.family
    return normalized
