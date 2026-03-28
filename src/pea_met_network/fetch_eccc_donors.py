"""Standalone script to pre-populate ECCC donor cache.

Fetches hourly climate data from MSC GeoMet API for configured donor stations
and caches as Parquet files under data/raw/eccc/.

Usage:
    python -m pea_met_network.fetch_eccc_donors --start 2022-01-01 --end 2026-03-27
    python -m pea_met_network.fetch_eccc_donors --station st_peters --start 2022-01-01
    python -m pea_met_network.fetch_eccc_donors --dry-run
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

from pea_met_network.eccc_api import ECCC_DONOR_STATIONS, fetch_eccc_hourly

PROJECT_ROOT = Path(__file__).resolve().parents[2]
ECCC_CACHE_DIR = PROJECT_ROOT / "data" / "raw" / "eccc"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch ECCC donor station data from MSC GeoMet API"
    )
    parser.add_argument(
        "--start",
        type=str,
        default="2022-01-01",
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end",
        type=str,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--station",
        type=str,
        default=None,
        help="Fetch specific station key only",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be fetched without making API calls",
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=str(ECCC_CACHE_DIR),
        help="Cache directory path",
    )
    args = parser.parse_args()

    start = pd.Timestamp(args.start, tz="UTC")
    end = pd.Timestamp(args.end, tz="UTC")
    cache_dir = Path(args.cache_dir)

    stations = ECCC_DONOR_STATIONS
    if args.station:
        if args.station not in stations:
            print(f"Unknown station: {args.station}", file=sys.stderr)
            print(f"Available: {', '.join(stations.keys())}", file=sys.stderr)
            sys.exit(1)
        stations = {args.station: ECCC_DONOR_STATIONS[args.station]}

    if args.dry_run:
        print(f"Dry run: would fetch {len(stations)} station(s)")
        print(f"  Date range: {start} to {end}")
        print(f"  Cache dir:  {cache_dir}")
        for key, stn in stations.items():
            print(f"  - {stn.name} ({stn.climate_id})")
        return

    print(f"Fetching {len(stations)} station(s)...")
    for key, station in stations.items():
        print(f"  {station.name} ({station.climate_id})...", end=" ", flush=True)
        try:
            df = fetch_eccc_hourly(
                station, start, end, cache_dir=cache_dir
            )
            print(f"{len(df)} rows")
        except Exception as e:
            print(f"FAILED: {e}", file=sys.stderr)

    print("Done.")


if __name__ == "__main__":
    main()
