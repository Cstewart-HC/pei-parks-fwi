#!/usr/bin/env python3
"""Build script for FWI Geospatial Dashboard.

Reads pipeline outputs (station_daily.csv) and generates dashboard data files:
  - stations.json   (static station metadata)
  - fwi_daily.json  (daily FWI time series keyed by date)

Usage:
    python scripts/build_dashboard.py [--output-dir dashboard/data]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Station metadata
# ---------------------------------------------------------------------------
STATIONS = {
    "stanhope": {"display_name": "Stanhope", "lat": 46.420, "lon": -63.080, "group": "west"},
    "cavendish": {"display_name": "Cavendish", "lat": 46.491, "lon": -63.379, "group": "central"},
    "greenwich": {"display_name": "Greenwich", "lat": 46.449, "lon": -62.442, "group": "east"},
    "north_rustico": {"display_name": "North Rustico", "lat": 46.451, "lon": -63.330, "group": "central"},
    "stanley_bridge": {"display_name": "Stanley Bridge", "lat": 46.446, "lon": -63.349, "group": "central"},
    "tracadie": {"display_name": "Tracadie", "lat": 46.385, "lon": -62.233, "group": "east"},
}

# Core FWI columns that must be present in daily CSV
FWI_COLS = ["timestamp_utc", "station", "ffmc", "dmc", "dc", "isi", "bui", "fwi"]

# Project root (two levels up from this script)
PROJECT_ROOT = Path(__file__).resolve().parent.parent


def find_processed_dir() -> Path:
    """Locate the data/processed directory."""
    d = PROJECT_ROOT / "data" / "processed"
    if not d.is_dir():
        print(f"ERROR: processed data directory not found: {d}", file=sys.stderr)
        sys.exit(1)
    return d


def read_station_csv(path: Path, station: str) -> pd.DataFrame:
    """Read a station_daily.csv and return only the FWI columns.

    Handles the fact that stanhope has a different column set than the
    other 5 stations by selecting only the columns we need.
    """
    df = pd.read_csv(path, parse_dates=["timestamp_utc"])
    # Keep only columns that exist
    available = [c for c in FWI_COLS if c in df.columns]
    missing = [c for c in FWI_COLS if c not in df.columns]
    if missing:
        print(f"  WARNING: {path.name} missing columns: {missing}", file=sys.stderr)
    return df[available].copy()


def build_fwi_daily(processed_dir: Path) -> dict:
    """Build the fwi_daily.json structure.

    Returns dict keyed by date string "YYYY-MM-DD", each value is a list
    of station records with ffmc, dmc, dc, isi, bui, fwi.
    """
    all_frames: list[pd.DataFrame] = []
    counts: dict[str, int] = {}

    for station_id, meta in STATIONS.items():
        csv_path = processed_dir / station_id / "station_daily.csv"
        if not csv_path.exists():
            print(f"  WARNING: {csv_path} not found, skipping {station_id}", file=sys.stderr)
            counts[station_id] = 0
            continue

        df = read_station_csv(csv_path, station_id)
        # Drop rows where fwi is NaN
        before = len(df)
        df = df.dropna(subset=["fwi"])
        after = len(df)
        counts[station_id] = after

        if before - after > 0:
            print(f"  {station_id}: dropped {before - after} rows with NaN FWI ({before} -> {after})")

        all_frames.append(df)

    if not all_frames:
        print("ERROR: No data loaded from any station.", file=sys.stderr)
        sys.exit(1)

    combined = pd.concat(all_frames, ignore_index=True)

    # Ensure timestamp_utc is a date string
    combined["date"] = pd.to_datetime(combined["timestamp_utc"]).dt.strftime("%Y-%m-%d")

    # Build output dict: {date: [station_records...]}
    fwi_daily: dict[str, list[dict]] = {}
    for _, row in combined.iterrows():
        date = row["date"]
        record = {
            "station": row["station"],
            "ffmc": round(float(row["ffmc"]), 2) if pd.notna(row["ffmc"]) else None,
            "dmc": round(float(row["dmc"]), 2) if pd.notna(row["dmc"]) else None,
            "dc": round(float(row["dc"]), 2) if pd.notna(row["dc"]) else None,
            "isi": round(float(row["isi"]), 2) if pd.notna(row["isi"]) else None,
            "bui": round(float(row["bui"]), 2) if pd.notna(row["bui"]) else None,
            "fwi": round(float(row["fwi"]), 2) if pd.notna(row["fwi"]) else None,
        }
        fwi_daily.setdefault(date, []).append(record)

    # Sort dates
    fwi_daily = dict(sorted(fwi_daily.items()))

    return fwi_daily, counts


def build_stations_json() -> dict:
    """Build the static stations.json."""
    return {
        station_id: {
            "display_name": meta["display_name"],
            "lat": meta["lat"],
            "lon": meta["lon"],
            "group": meta["group"],
        }
        for station_id, meta in STATIONS.items()
    }


def print_summary(fwi_daily: dict, counts: dict[str, int]) -> None:
    """Print a summary of the built data."""
    dates = sorted(fwi_daily.keys())
    if not dates:
        print("No dates found in output.")
        return

    print(f"\n{'='*50}")
    print(f"Dashboard build summary")
    print(f"{'='*50}")
    print(f"  Date range: {dates[0]} to {dates[-1]}")
    print(f"  Total dates: {len(dates)}")
    print(f"  Total records: {sum(len(v) for v in fwi_daily.values())}")
    print(f"\n  Records per station:")
    for station_id, count in counts.items():
        display = STATIONS[station_id]["display_name"]
        print(f"    {display:20s} ({station_id:15s}): {count:>5d}")

    # Check for gaps
    all_station_dates: dict[str, set[str]] = {}
    for date, records in fwi_daily.items():
        for rec in records:
            s = rec["station"]
            all_station_dates.setdefault(s, set()).add(date)

    print(f"\n  Date coverage per station:")
    for station_id in STATIONS:
        s_dates = all_station_dates.get(station_id, set())
        if s_dates:
            s_sorted = sorted(s_dates)
            print(f"    {STATIONS[station_id]['display_name']:20s}: {s_sorted[0]} to {s_sorted[-1]} ({len(s_dates)} days)")
        else:
            print(f"    {STATIONS[station_id]['display_name']:20s}: NO DATA")

    print(f"{'='*50}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build FWI dashboard data files")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="dashboard/data",
        help="Output directory for generated JSON files (default: dashboard/data)",
    )
    args = parser.parse_args()

    output_dir = PROJECT_ROOT / args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    processed_dir = find_processed_dir()

    print("Building stations.json...")
    stations = build_stations_json()
    stations_path = output_dir / "stations.json"
    with open(stations_path, "w") as f:
        json.dump(stations, f, indent=2)
    print(f"  -> {stations_path}")

    print("Building fwi_daily.json...")
    fwi_daily, counts = build_fwi_daily(processed_dir)
    fwi_path = output_dir / "fwi_daily.json"
    with open(fwi_path, "w") as f:
        json.dump(fwi_daily, f)
    size_kb = fwi_path.stat().st_size / 1024
    print(f"  -> {fwi_path} ({size_kb:.0f} KB)")

    print_summary(fwi_daily, counts)
    print("Done.")


if __name__ == "__main__":
    main()
