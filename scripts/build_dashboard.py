#!/usr/bin/env python3
"""Build script for FWI Geospatial Dashboard.

Reads pipeline outputs (station_daily.csv) and forecast CSVs to generate
dashboard data files:
  - stations.json        (static station metadata)
  - fwi_daily.json       (historical daily FWI keyed by date)
  - fwi_forecast.json    (10-day forecast daily FWI keyed by date)
  - forecast_meta.json   (forecast run metadata and staleness)

Usage:
    python scripts/build_dashboard.py [--output-dir dashboard/data] [--no-forecast]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
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


def find_forecast_dir() -> Path | None:
    """Locate the data/forecasts directory. Returns None if missing."""
    d = PROJECT_ROOT / "data" / "forecasts"
    if d.is_dir():
        return d
    return None


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


def build_fwi_forecast(forecast_dir: Path) -> tuple[dict, dict]:
    """Build fwi_forecast.json and forecast_meta.json from forecast CSVs.

    Reads hourly forecast CSVs, aggregates to daily noon (14:00 UTC) snapshot,
    or nearest available hour if 14:00 is missing.

    Returns (fwi_forecast_dict, meta_dict).
    """
    NOON_HOUR = 14
    FORECAST_COLS = ["timestamp_utc", "FFMC", "DMC", "DC", "ISI", "BUI", "FWI"]
    # Map forecast CSV column names (uppercase) to lowercase
    COL_MAP = {"timestamp_utc": "timestamp_utc", "FFMC": "ffmc", "DMC": "dmc",
               "DC": "dc", "ISI": "isi", "BUI": "bui", "FWI": "fwi"}

    all_frames: list[pd.DataFrame] = []
    station_counts: dict[str, int] = {}
    newest_mtime: float = 0.0

    for station_id in STATIONS:
        csv_path = forecast_dir / f"{station_id}_fwi_forecast.csv"
        if not csv_path.exists():
            print(f"  WARNING: {csv_path.name} not found, skipping {station_id}", file=sys.stderr)
            station_counts[station_id] = 0
            continue

        # Track newest mtime for staleness
        mtime = csv_path.stat().st_mtime
        if mtime > newest_mtime:
            newest_mtime = mtime

        df = pd.read_csv(csv_path, parse_dates=["timestamp_utc"])
        # Keep only columns that exist
        available = [c for c in FORECAST_COLS if c in df.columns]
        df = df[available].copy()

        # Add station column
        df["station"] = station_id

        # Extract date and hour for noon selection
        df["date"] = pd.to_datetime(df["timestamp_utc"]).dt.date
        df["hour"] = pd.to_datetime(df["timestamp_utc"]).dt.hour

        # Prefer 14:00 UTC, else closest hour per day
        df["_dist"] = (df["hour"] - NOON_HOUR).abs()
        # Sort: noon rows first (dist=0), then by distance to noon
        df = df.sort_values(["date", "_dist", "hour"])
        # Keep first row per date (closest to noon)
        daily = df.drop_duplicates(subset=["date"], keep="first")
        daily = daily.drop(columns=["hour", "_dist"], errors="ignore")

        # Drop rows where FWI is NaN
        before = len(daily)
        if "FWI" in daily.columns:
            daily = daily[daily["FWI"].notna()]
        after = len(daily)
        station_counts[station_id] = after

        if before - after > 0:
            print(f"  {station_id} forecast: dropped {before - after} rows with NaN FWI")

        all_frames.append(daily)

    if not all_frames:
        print("  WARNING: No forecast data loaded from any station.", file=sys.stderr)
        return {}, _empty_meta()

    combined = pd.concat(all_frames, ignore_index=True)

    # Convert date column to string before building output
    combined["date"] = combined["date"].astype(str)

    # Drop timestamp_utc before building records (it's a Timestamp, not a number)
    combined = combined.drop(columns=["timestamp_utc"], errors="ignore")

    # Build output dict: {date: [station_records...]}
    fwi_forecast: dict[str, list[dict]] = {}
    for _, row in combined.iterrows():
        date_str = row["date"]
        record = {"station": row["station"]}
        for src, dst in COL_MAP.items():
            if src == "timestamp_utc":
                continue
            if src in row.index:
                val = row[src]
                record[dst] = round(float(val), 2) if pd.notna(val) else None
        fwi_forecast.setdefault(date_str, []).append(record)

    fwi_forecast = dict(sorted(fwi_forecast.items()))

    # Build meta
    generated_at = datetime.fromtimestamp(newest_mtime, tz=timezone.utc).isoformat()
    stations_full = [s for s, c in station_counts.items() if c > 0]
    stations_partial = [s for s, c in station_counts.items() if c == 0 and f"{s}_fwi_forecast.csv" in os.listdir(forecast_dir)]

    meta = {
        "generated_at": generated_at,
        "forecast_hours": 240,
        "data_sources": {
            "licor": "0-6h (5 park stations)",
            "owm": "0-48h (all 6 stations)",
            "gdps": "48-240h (3-hourly)",
        },
        "stations_with_data": stations_full,
        "stations_missing": [s for s in STATIONS if station_counts.get(s, 0) == 0],
        "partial_note": (
            "Stanley Bridge and Tracadie lack RH sensors — FWI may be incomplete for some hours"
            if stations_partial else None
        ),
    }

    return fwi_forecast, meta


def _empty_meta() -> dict:
    """Return an empty forecast metadata dict."""
    return {
        "generated_at": None,
        "forecast_hours": 0,
        "data_sources": {},
        "stations_with_data": [],
        "stations_missing": list(STATIONS.keys()),
        "partial_note": None,
    }


def print_summary(fwi_daily: dict, counts: dict[str, int]) -> None:
    """Print a summary of the built data."""
    dates = sorted(fwi_daily.keys())
    if not dates:
        print("No dates found in output.")
        return

    print(f"\n{'='*50}")
    print("Dashboard build summary")
    print(f"{'='*50}")
    print(f"  Date range: {dates[0]} to {dates[-1]}")
    print(f"  Total dates: {len(dates)}")
    print(f"  Total records: {sum(len(v) for v in fwi_daily.values())}")
    print("\n  Records per station:")
    for station_id, count in counts.items():
        display = STATIONS[station_id]["display_name"]
        print(f"    {display:20s} ({station_id:15s}): {count:>5d}")

    # Check for gaps
    all_station_dates: dict[str, set[str]] = {}
    for date, records in fwi_daily.items():
        for rec in records:
            s = rec["station"]
            all_station_dates.setdefault(s, set()).add(date)

    print("\n  Date coverage per station:")
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
    parser.add_argument(
        "--no-forecast",
        action="store_true",
        help="Skip forecast data processing (only build historical data)",
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

    # --- Forecast data ---
    if not args.no_forecast:
        forecast_dir = find_forecast_dir()
        if forecast_dir is not None:
            print("Building fwi_forecast.json...")
            fwi_forecast, meta = build_fwi_forecast(forecast_dir)
            fc_path = output_dir / "fwi_forecast.json"
            with open(fc_path, "w") as f:
                json.dump(fwi_forecast, f)
            fc_kb = fc_path.stat().st_size / 1024
            print(f"  -> {fc_path} ({fc_kb:.0f} KB)")

            meta_path = output_dir / "forecast_meta.json"
            with open(meta_path, "w") as f:
                json.dump(meta, f, indent=2)
            print(f"  -> {meta_path}")

            fc_dates = sorted(fwi_forecast.keys()) if fwi_forecast else []
            if fc_dates:
                print(f"  Forecast range: {fc_dates[0]} to {fc_dates[-1]} ({len(fc_dates)} days)")
            print(f"  Generated at: {meta['generated_at']}")
        else:
            print("  No data/forecasts/ directory found — skipping forecast build.")
            # Write empty forecast files so dashboard handles gracefully
            meta = _empty_meta()
            meta_path = output_dir / "forecast_meta.json"
            with open(meta_path, "w") as f:
                json.dump(meta, f, indent=2)
            with open(output_dir / "fwi_forecast.json", "w") as f:
                json.dump({}, f)
    else:
        print("Forecast build skipped (--no-forecast).")

    print("Done.")


if __name__ == "__main__":
    main()
