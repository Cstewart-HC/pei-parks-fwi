"""XLSX adapter for Greenwich 2023 and other Excel files."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pea_met_network.adapters.base import BaseAdapter
from pea_met_network.adapters.column_maps import (
    derive_wind_speed_kmh,
    rename_columns,
)


class XLSXAdapter(BaseAdapter):
    """Adapter for XLSX files (HOBOware / Parks Canada exports)."""

    def load(self, path: Path) -> pd.DataFrame:
        """Load an XLSX file and return a DataFrame with canonical schema columns."""
        # Read raw to detect header row — these files often have a title row
        df_raw = pd.read_excel(path, engine="openpyxl", header=None, nrows=5)

        # Find the row that looks like a header (contains "Date" or "Line#")
        header_row_idx = 0
        for i, row in df_raw.iterrows():
            row_str = " ".join(str(v) for v in row.values if pd.notna(v)).lower()
            if "date" in row_str or "line#" in row_str:
                header_row_idx = i
                break

        # Read with the correct header
        df = pd.read_excel(
            path, engine="openpyxl", header=header_row_idx
        )

        if len(df) == 0:
            return pd.DataFrame()

        # Drop the Line# column if present
        if "Line#" in df.columns:
            df = df.drop(columns=["Line#"])

        # Skip the second header row if present (some files repeat headers)
        if len(df) > 1 and str(df.iloc[0].get("Date", "")).startswith("2") is False:
            # Check if first data row is another header
            first_date = df.iloc[0].get("Date", "")
            if isinstance(first_date, str) and "date" in first_date.lower():
                df = df.iloc[1:].reset_index(drop=True)

        df = rename_columns(df)
        df = derive_wind_speed_kmh(df)

        # Parse timestamps
        if "Date" in df.columns:
            timestamp_utc = pd.to_datetime(df["Date"], utc=True)
            result = pd.DataFrame({"timestamp_utc": timestamp_utc})
        else:
            raise ValueError(
                f"XLSX missing Date column: {list(df.columns[:5])}"
            )

        for col in df.columns:
            if col == "Date":
                continue
            result[col] = pd.to_numeric(df[col], errors="coerce")

        result["source_file"] = str(path)
        return result
