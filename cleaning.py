#!/usr/bin/env python3
"""cleaning.py — PEA Met Network pipeline entrypoint.

Loads raw station CSVs from data/raw/, normalizes timestamps,
resamples to hourly and daily frequencies, and writes cleaned
datasets to data/processed/.

Usage:
    python cleaning.py
    python cleaning.py --output-dir /path/to/output
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Ensure src/ is importable when running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from pea_met_network.manifest import build_raw_manifest
from pea_met_network.materialize_resampled import materialize_resampled_outputs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="PEA Met Network cleaning pipeline")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/processed"),
        help="Directory for cleaned output files (default: data/processed/)",
    )
    parser.add_argument(
        "--raw-dir",
        type=Path,
        default=Path("."),
        help="Base directory containing data/raw/ (default: .)",
    )
    args = parser.parse_args(argv)

    raw_base = args.raw_dir
    output_dir = args.output_dir

    # Check raw data directory exists
    raw_data_dir = raw_base / "data" / "raw"
    if not raw_data_dir.exists():
        log.error("Raw data directory not found: %s", raw_data_dir)
        print(
            f"Error: raw data directory not found at {raw_data_dir}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Build manifest
    log.info("Scanning raw data in %s ...", raw_data_dir)
    records = build_raw_manifest(raw_base)
    log.info("Found %d raw files", len(records))

    # Group by station
    stations: dict[str, list] = {}
    for rec in records:
        stations.setdefault(rec.station, []).append(rec)

    log.info("Stations: %s", ", ".join(sorted(stations.keys())))

    # Process each station
    total_hourly_rows = 0
    total_daily_rows = 0
    errors: list[str] = []

    for station, recs in sorted(stations.items()):
        log.info("Processing station: %s (%d files)", station, len(recs))
        station_output = output_dir / station
        station_output.mkdir(parents=True, exist_ok=True)

        for rec in recs:
            try:
                hourly_path, daily_path = materialize_resampled_outputs(
                    source_path=rec.path,
                    station=station,
                    output_dir=station_output,
                )
                import pandas as pd

                h_rows = len(pd.read_csv(hourly_path))
                d_rows = len(pd.read_csv(daily_path))
                total_hourly_rows += h_rows
                total_daily_rows += d_rows
                log.info(
                    "  %s → hourly=%d rows, daily=%d rows",
                    rec.path.name,
                    h_rows,
                    d_rows,
                )
            except Exception as exc:
                msg = f"{station}/{rec.path.name}: {exc}"
                log.warning("  Skipping %s: %s", rec.path.name, exc)
                errors.append(msg)

    # Summary
    log.info("=" * 50)
    log.info("Pipeline complete.")
    log.info("  Stations processed: %d", len(stations))
    log.info("  Total hourly rows: %d", total_hourly_rows)
    log.info("  Total daily rows: %d", total_daily_rows)
    if errors:
        log.warning("  Files skipped: %d", len(errors))
        for e in errors:
            log.warning("    - %s", e)
    log.info("  Output: %s", output_dir.resolve())


if __name__ == "__main__":
    main()
