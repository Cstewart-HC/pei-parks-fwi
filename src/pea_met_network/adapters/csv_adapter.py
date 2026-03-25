"""CSV adapter for PEINP archive CSVs and ECCC Stanhope CSVs."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pea_met_network.adapters.base import BaseAdapter
from pea_met_network.adapters.column_maps import (
    derive_wind_speed_kmh,
    rename_columns,
)


def _detect_csv_schema(df: pd.DataFrame) -> str:
    """Detect whether a CSV is PEINP or ECCC format."""
    cols_lower = {c.lower() for c in df.columns}
    # ECCC has columns like "Date/Time (LST)", "Station Name", "Climate ID"
    if any("date/time" in c.lower() for c in df.columns):
        return "eccc"
    if "station name" in cols_lower or "climate id" in cols_lower:
        return "eccc"
    return "peinp"


def _load_peinp_csv(path: Path) -> pd.DataFrame:
    """Load a PEINP-format CSV file."""
    df = pd.read_csv(path)
    if len(df) == 0:
        return pd.DataFrame()

    df = rename_columns(df)
    df = derive_wind_speed_kmh(df)

    # Parse timestamps from Date + Time columns
    if "Date" in df.columns and "Time" in df.columns:
        timestamp_text = df["Date"].astype(str).str.strip() + " " + df["Time"].astype(str).str.strip()
        timestamp_utc = pd.to_datetime(timestamp_text, format="%m/%d/%Y %H:%M:%S %z", utc=True)
    else:
        raise ValueError(
            f"PEINP CSV missing Date/Time columns: {list(df.columns[:5])}"
        )

    result = pd.DataFrame({"timestamp_utc": timestamp_utc})

    # Copy numeric columns
    for col in df.columns:
        if col in {"Date", "Time"}:
            continue
        if col in ("source_file", "schema_family"):
            result[col] = df[col]
        else:
            result[col] = pd.to_numeric(df[col], errors="coerce")

    return result


def _load_eccc_csv(path: Path) -> pd.DataFrame:
    """Load an ECCC Stanhope-format CSV file."""
    df = pd.read_csv(path)
    if len(df) == 0:
        return pd.DataFrame()

    df = rename_columns(df)
    df = derive_wind_speed_kmh(df)

    # ECCC has a combined "Date/Time (LST)" column
    ts_col = None
    for col in df.columns:
        if "date/time" in col.lower():
            ts_col = col
            break

    if ts_col is None:
        raise ValueError(
            f"ECCC CSV missing Date/Time column: {list(df.columns[:5])}"
        )

    # ECCC timestamps are in LST (AST/ADT). Parse as local and convert to UTC.
    # The timezone is inferred from the file data; ECCC LST = AST/ADT.
    timestamp_utc = pd.to_datetime(df[ts_col], utc=True)

    result = pd.DataFrame({"timestamp_utc": timestamp_utc})

    for col in df.columns:
        if "date/time" in col.lower():
            continue
        if col in ("Longitude (x)", "Latitude (y)", "Station Name", "Climate ID",
                    "Year", "Month", "Day", "Time (LST)", "Flag", "Weather"):
            continue
        # Skip flag columns (ending in " Flag")
        if col.strip().endswith("Flag"):
            continue
        result[col] = pd.to_numeric(df[col], errors="coerce")

    result["station"] = "stanhope"
    return result


class CSVAdapter(BaseAdapter):
    """Adapter for CSV files (PEINP and ECCC formats)."""

    def load(self, path: Path) -> pd.DataFrame:
        """Load a CSV file and return a DataFrame with canonical schema columns."""
        df = pd.read_csv(path)
        schema = _detect_csv_schema(df)

        if schema == "eccc":
            result = _load_eccc_csv(path)
        else:
            result = _load_peinp_csv(path)

        result["station"] = "stanhope" if schema == "eccc" else result.get("station", "unknown")
        result["source_file"] = str(path)
        return result
