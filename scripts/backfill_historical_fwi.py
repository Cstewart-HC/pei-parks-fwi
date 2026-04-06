#!/usr/bin/env python3
"""Backfill historical FWI for all stations from ECCC hourly observations.

Reads the QA/QC'd hourly CSVs in data/processed/{station}/station_hourly.csv,
recomputes the full FWI chain (FFMC → DMC → DC → ISI → BUI → FWI) from the
start of each station's record, and writes the result back into the same file.

The existing hourly files already have ~95% of FWI values computed. This script
fills the remaining gaps and ensures the chain is consistent from day one
(rather than depending on whatever startup indices were used during the
original ETL).

Usage:
    python scripts/backfill_historical_fwi.py [--station STATION] [--dry-run]

Strategy:
  - Two-pass approach per station:
    1. Pre-compute DMC/DC chain from daily aggregates (max temp hour, total rain)
    2. Compute hourly FFMC → ISI → BUI → FWI using the daily DMC/DC values
  - Local date determined from UTC timestamp + ADT offset (AST/ADT based on month)
  - Startup indices: FFMC=85, DMC=6, DC=15 (standard spring defaults)
  - Fill-only mode: preserves existing FWI values and only writes to rows
    where FWI is currently null (chain is still computed across all rows for
    continuity, but original values are restored after)
  - Overwrites only rows where input data is complete (temp, RH, wind all present)
  - Preserves existing _quality_flags and other columns
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pea_met_network import fwi as fwi_calc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Station config
# ---------------------------------------------------------------------------

STATIONS = {
    "stanhope": {"lat": 46.38, "lon": -63.12},
    "cavendish": {"lat": 46.4614, "lon": -63.3917},
    "greenwich": {"lat": 46.4367, "lon": -63.2703},
    "north_rustico": {"lat": 46.4508, "lon": -63.3306},
    "stanley_bridge": {"lat": 46.4272, "lon": -63.2000},
    "tracadie": {"lat": 46.4089, "lon": -63.1483},
}

# Default startup indices (spring)
DEFAULT_FFMC0 = 85.0
DEFAULT_DMC0 = 6.0
DEFAULT_DC0 = 15.0

DATA_DIR = PROJECT_ROOT / "data" / "processed"

# Required columns for FWI computation
REQUIRED_COLS = ["air_temperature_c", "relative_humidity_pct", "wind_speed_kmh", "rain_mm"]

# FWI output columns
FWI_COLS = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]


# ---------------------------------------------------------------------------
# Local date computation
# ---------------------------------------------------------------------------

def _local_date(ts: pd.Timestamp, month: int) -> date:
    """Convert UTC timestamp to local date (ADT/AST)."""
    # PEI: ADT (UTC-3) Apr-Oct, AST (UTC-4) Nov-Mar
    offset_hours = 3 if month in (4, 5, 6, 7, 8, 9, 10) else 4
    local_dt = ts.tz_convert(None) - timedelta(hours=offset_hours)
    return local_dt.date()


# ---------------------------------------------------------------------------
# FWI chain computation
# ---------------------------------------------------------------------------

def compute_fwi_for_station(
    df: pd.DataFrame,
    station_name: str,
    lat: float,
) -> pd.DataFrame:
    """Compute full FWI chain for a station's hourly data.

    Two-pass approach:
    1. Aggregate daily values → compute DMC/DC chain
    2. Compute hourly FFMC → ISI → BUI → FWI using daily DMC/DC
    """
    df = df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df = df.sort_values("timestamp_utc").reset_index(drop=True)
    df["month"] = df["timestamp_utc"].dt.month

    # Determine local date for each row
    df["local_date"] = [
        _local_date(row["timestamp_utc"], row["month"])
        for _, row in df.iterrows()
    ]

    # Flag rows with complete weather data
    df["can_compute"] = df[REQUIRED_COLS].notna().all(axis=1)

    if not df["can_compute"].any():
        logger.warning("No rows with complete weather data for %s", station_name)
        return df

    # --- Pass 1: Daily DMC/DC chain ---
    computable = df[df["can_compute"]].copy()

    daily_agg: dict[date, dict] = {}
    for _, row in computable.iterrows():
        ld = row["local_date"]
        t, rh, r = (
            row["air_temperature_c"],
            row["relative_humidity_pct"],
            row["rain_mm"],
        )
        if ld not in daily_agg:
            daily_agg[ld] = {
                "temp": t, "rh": rh, "rain": r, "month": int(row["month"])
            }
        else:
            entry = daily_agg[ld]
            entry["rain"] += r
            if t > entry["temp"]:
                entry["temp"] = t
                entry["rh"] = rh

    # Chain DMC/DC through all local days
    daily_codes: dict[date, tuple[float, float]] = {}
    cur_dmc, cur_dc = DEFAULT_DMC0, DEFAULT_DC0

    for ld in sorted(daily_agg.keys()):
        entry = daily_agg[ld]
        if entry["temp"] > 0:
            cur_dmc = fwi_calc.duff_moisture_code(
                temp=entry["temp"], rh=entry["rh"], rain=entry["rain"],
                dmc0=cur_dmc, month=entry["month"], lat=lat,
            )
            cur_dc = fwi_calc.drought_code(
                temp=entry["temp"], rh=entry["rh"], rain=entry["rain"],
                dc0=cur_dc, month=entry["month"], lat=lat,
            )
        else:
            # Below zero: DMC/DC don't change but can decrease from rain
            if entry["rain"] > 0:
                cur_dmc = fwi_calc.duff_moisture_code(
                    temp=0.1, rh=entry["rh"], rain=entry["rain"],
                    dmc0=cur_dmc, month=entry["month"], lat=lat,
                )
                cur_dc = fwi_calc.drought_code(
                    temp=0.1, rh=entry["rh"], rain=entry["rain"],
                    dc0=cur_dc, month=entry["month"], lat=lat,
                )
        daily_codes[ld] = (cur_dmc, cur_dc)

    # --- Pass 2: Hourly FFMC → ISI → BUI → FWI ---
    ffmc = DEFAULT_FFMC0
    rows_updated = 0

    for idx, row in df.iterrows():
        if not row["can_compute"]:
            continue

        temp = row["air_temperature_c"]
        rh = row["relative_humidity_pct"]
        wind = row["wind_speed_kmh"]
        rain = row["rain_mm"]
        ld = row["local_date"]

        dmc, dc = daily_codes.get(ld, (cur_dmc, cur_dc))

        ffmc = fwi_calc.hourly_fine_fuel_moisture_code(
            temp=temp, rh=rh, wind=wind, rain=rain, ffmc0=ffmc,
        )

        isi = fwi_calc.initial_spread_index(ffmc=ffmc, wind=wind)
        bui = fwi_calc.buildup_index(dmc=dmc, dc=dc)
        fwi_val = fwi_calc.fire_weather_index(isi=isi, bui=bui)

        df.at[idx, "ffmc"] = round(ffmc, 6)
        df.at[idx, "dmc"] = round(dmc, 6)
        df.at[idx, "dc"] = round(dc, 6)
        df.at[idx, "isi"] = round(isi, 6)
        df.at[idx, "bui"] = round(bui, 6)
        df.at[idx, "fwi"] = round(fwi_val, 6)
        rows_updated += 1

    logger.info(
        "%s: updated %d/%d rows (%d had incomplete data)",
        station_name, rows_updated, len(df), len(df) - rows_updated,
    )
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def process_station(station_name: str, dry_run: bool = False) -> None:
    """Process a single station."""
    config = STATIONS.get(station_name)
    if not config:
        logger.error("Unknown station: %s", station_name)
        return

    hourly_path = DATA_DIR / station_name / "station_hourly.csv"
    if not hourly_path.exists():
        logger.error("No hourly data for %s at %s", station_name, hourly_path)
        return

    logger.info("Processing %s from %s", station_name, hourly_path)
    df = pd.read_csv(hourly_path, low_memory=False)

    before_count = df["fwi"].notna().sum()
    logger.info(
        "  Before: %d/%d rows have FWI (%.1f%%)",
        before_count, len(df), 100 * before_count / len(df),
    )

    # Fill-only mode: compute full chain but only write where FWI is missing.
    # This preserves existing values from the original ETL and avoids
    # introducing floating-point drift across the ~23K rows that are already good.
    gap_mask = df["fwi"].isna()
    gap_count = gap_mask.sum()
    logger.info("  Gap rows (no FWI): %d", gap_count)

    # Temporarily null out existing FWI so compute runs the full chain
    # (chain needs to walk through every row for continuity),
    # then restore originals and only keep new values in gap positions.
    original_fwi = df[FWI_COLS].copy()
    df[FWI_COLS] = np.nan

    df = compute_fwi_for_station(df, station_name, config["lat"])

    # Restore existing values, keep only new computations in gap positions
    for col in FWI_COLS:
        df.loc[~gap_mask, col] = original_fwi.loc[~gap_mask, col]

    after_count = df["fwi"].notna().sum()
    logger.info(
        "  After:  %d/%d rows have FWI (%.1f%%) — filled %d gaps",
        after_count, len(df), 100 * after_count / len(df),
        after_count - before_count,
    )

    if dry_run:
        logger.info("  (dry run — not writing)")
        return

    # Write back
    df.to_csv(hourly_path, index=False)
    logger.info("  Written to %s", hourly_path)


def main():
    parser = argparse.ArgumentParser(description="Backfill historical FWI for PEINP stations")
    parser.add_argument("--station", "-s", help="Process a single station")
    parser.add_argument("--dry-run", "-n", action="store_true", help="Don't write files")
    args = parser.parse_args()

    stations = [args.station] if args.station else list(STATIONS.keys())

    for name in stations:
        process_station(name, dry_run=args.dry_run)

    logger.info("Done.")


if __name__ == "__main__":
    main()
