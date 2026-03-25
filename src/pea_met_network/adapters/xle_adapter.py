"""XLE adapter for Solinst logger files (Stanley Bridge 2022)."""

from __future__ import annotations

from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

from pea_met_network.adapters.base import BaseAdapter


class XLEAdapter(BaseAdapter):
    """Adapter for Solinst XLE XML files."""

    def load(self, path: Path) -> pd.DataFrame:
        """Load an XLE file and return a DataFrame with canonical schema columns."""
        tree = ET.parse(path)
        root = tree.getroot()

        # Extract channel info
        channels: dict[str, str] = {}  # chN -> (identification, unit)
        for header in root.iter("Ch1_data_header"):
            ident = header.findtext("Identification", "").strip()
            unit = header.findtext("Unit", "").strip()
            channels["ch1"] = (ident, unit)
        for header in root.iter("Ch2_data_header"):
            ident = header.findtext("Identification", "").strip()
            unit = header.findtext("Unit", "").strip()
            channels["ch2"] = (ident, unit)

        # Extract data rows
        rows: list[dict] = []
        for log in root.iter("Log"):
            date_str = log.findtext("Date", "")
            time_str = log.findtext("Time", "")
            if not date_str or not time_str:
                continue

            timestamp_text = f"{date_str} {time_str}"
            row: dict = {"timestamp_text": timestamp_text}

            for ch_key in sorted(channels.keys()):
                val = log.findtext(ch_key)
                ident, unit = channels[ch_key]
                row[ident.lower()] = val

            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        # Parse timestamp (XLE timestamps appear to be local time)
        df["timestamp_utc"] = pd.to_datetime(
            df["timestamp_text"], format="%Y/%m/%d %H:%M:%S", utc=True
        )
        df = df.drop(columns=["timestamp_text"])

        # Map channel identifications to canonical names
        rename_map: dict[str, str] = {}
        for ch_key, (ident, unit) in channels.items():
            ident_lower = ident.lower().strip()
            if ident_lower == "level":
                rename_map[ident_lower] = "water_level_m"
            elif ident_lower == "temperature":
                rename_map[ident_lower] = "water_temperature_c"
            # Could be other channels — keep as-is if unknown

        df = df.rename(columns=rename_map)

        # Convert numeric columns
        for col in df.columns:
            if col != "timestamp_utc":
                df[col] = pd.to_numeric(df[col], errors="coerce")

        df["source_file"] = str(path)
        return df
