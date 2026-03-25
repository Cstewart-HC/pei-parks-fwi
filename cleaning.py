#!/usr/bin/env python3
"""cleaning.py — PEA Met Network pipeline entrypoint.

Loads raw station CSVs from data/raw/, normalizes timestamps,
resamples to hourly and daily frequencies, imputes missing values,
ingests Stanhope reference data, computes FWI indices, and writes
cleaned datasets + manifest to data/processed/.

Usage:
    python cleaning.py
    python cleaning.py --output-dir /path/to/output
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

# Ensure src/ is importable when running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from pea_met_network.fwi import (
    buildup_index,
    drought_code,
    duff_moisture_code,
    fine_fuel_moisture_code,
    fire_weather_index,
    initial_spread_index,
)
from pea_met_network.imputation import (
    ImputationConfig,
    audit_trail_to_dataframe,
    impute_frame,
)
from pea_met_network.manifest import build_raw_manifest
from pea_met_network.materialize_resampled import materialize_resampled_outputs
from pea_met_network.stanhope_cache import (
    materialize_stanhope_hourly_range,
    normalize_stanhope_hourly,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def _run_imputation(
    hourly: pd.DataFrame,
    station: str,
) -> tuple[pd.DataFrame, list]:
    """Run imputation on hourly data and return
    imputed frame + audit records."""
    config = ImputationConfig()
    imputed, records = impute_frame(hourly, config=config)
    return imputed, records


def _compute_fwi_codes(
    hourly: pd.DataFrame,
    latitude: float = 46.4,
) -> pd.DataFrame:
    """Compute FFMC, DMC, DC, ISI, BUI, FWI columns on hourly data.

    Requires columns: air_temperature_c, relative_humidity_pct,
    wind_speed_kmh, rain_mm, timestamp_utc.
    """
    df = hourly.copy()
    required = {"air_temperature_c", "relative_humidity_pct",
                "wind_speed_kmh", "rain_mm", "timestamp_utc"}
    missing = required - set(df.columns)
    if missing:
        log.warning("FWI skipped for %s: missing columns %s",
                     df["station"].iloc[0] if "station" in df.columns else "?",
                     missing)
        # Still add FWI columns as NaN so output schema is consistent
        for col in ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]:
            df[col] = float("nan")
        return df

    df = df.sort_values("timestamp_utc").reset_index(drop=True)

    # Initialize carry-over indices with standard start values
    ffmc_prev = 85.0
    dmc_prev = 6.0
    dc_prev = 15.0

    ffmc_list: list[float] = []
    dmc_list: list[float] = []
    dc_list: list[float] = []
    isi_list: list[float] = []
    bui_list: list[float] = []
    fwi_list: list[float] = []

    for _, row in df.iterrows():
        temp = row["air_temperature_c"]
        rh = row["relative_humidity_pct"]
        wind = row["wind_speed_kmh"]
        rain = row["rain_mm"]
        month = row["timestamp_utc"].month

        # Skip if critical inputs are NaN
        if pd.isna(temp) or pd.isna(rh) or pd.isna(wind):
            ffmc_list.append(float("nan"))
            dmc_list.append(float("nan"))
            dc_list.append(float("nan"))
            isi_list.append(float("nan"))
            bui_list.append(float("nan"))
            fwi_list.append(float("nan"))
            continue

        rain_val = rain if not pd.isna(rain) else 0.0

        ffmc = fine_fuel_moisture_code(temp, rh, wind, rain_val, ffmc_prev)
        dmc = duff_moisture_code(temp, rh, rain_val, dmc_prev, month, latitude)
        dc = drought_code(temp, rain_val, dc_prev, month, latitude)
        isi = initial_spread_index(ffmc, wind)
        bui = buildup_index(dmc, dc)
        fwi = fire_weather_index(isi, bui)

        ffmc_list.append(ffmc)
        dmc_list.append(dmc)
        dc_list.append(dc)
        isi_list.append(isi)
        bui_list.append(bui)
        fwi_list.append(fwi)

        ffmc_prev = ffmc
        dmc_prev = dmc
        dc_prev = dc

    df["ffmc"] = ffmc_list
    df["dmc"] = dmc_list
    df["dc"] = dc_list
    df["isi"] = isi_list
    df["bui"] = bui_list
    df["fwi"] = fwi_list
    return df


def _write_manifest(
    artifacts: list[dict],
    output_path: Path,
) -> None:
    """Write pipeline artifact manifest as JSON."""
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "artifacts": artifacts,
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(manifest, indent=2) + "\n")
    log.info(
        "Manifest written to %s (%d artifacts)",
        output_path,
        len(artifacts),
    )


def main(argv: list[str] | None = None) -> None:  # noqa: C901
    parser = argparse.ArgumentParser(
        description="PEA Met Network cleaning pipeline",
    )
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
    parser.add_argument(
        "--skip-stanhope",
        action="store_true",
        help="Skip Stanhope reference data ingestion",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be processed without writing outputs",
    )
    parser.add_argument(
        "--stations",
        nargs="*",
        default=None,
        help="Process only these stations",
    )
    args = parser.parse_args(argv)

    raw_base = args.raw_dir
    output_dir = args.output_dir
    artifacts: list[dict] = []
    all_audit_records: list = []

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

    # Filter to requested stations
    if args.stations:
        filtered = {
            s: recs for s, recs in stations.items()
            if s in args.stations
        }
        stations = filtered

    log.info("Stations: %s", ", ".join(sorted(stations.keys())))

    # --- Dry-run mode: report and exit ---
    if args.dry_run:
        for station, recs in sorted(stations.items()):
            fmt_counts: dict[str, int] = {}
            for rec in recs:
                ext = rec.extension
                fmt_counts[ext] = fmt_counts.get(ext, 0) + 1
            parts = ", ".join(
                f"{ext}={n}" for ext, n in sorted(fmt_counts.items())
            )
            print(f"station={station} files={len(recs)} [{parts}]")
        print(f"Total: {len(records)} files across {len(stations)} stations")
        return

    # --- Unknown format detection (hard error) ---
    from pea_met_network.adapters import ADAPTER_REGISTRY
    known_extensions = set(ADAPTER_REGISTRY.keys())
    skip_extensions = {
        ".txt", ".md", ".png", ".jpg", ".gitignore",
        ".r", ".py", ".pyc", ".pyo", ".toml", ".cfg", ".ini",
        ".pdf", ".zip", ".docx", ".doc",
    }
    # Scan raw directory for any data files with unknown extensions
    # (manifest only yields known formats, so we scan independently)
    # Include explicitly requested stations even if not in manifest
    scan_stations = set(stations.keys())
    if args.stations:
        scan_stations.update(args.stations)
    for raw_path in sorted(raw_data_dir.rglob("*")):
        if not raw_path.is_file():
            continue
        ext = raw_path.suffix.lower()
        if not ext or ext in known_extensions or ext in skip_extensions:
            continue
        rel = raw_path.relative_to(raw_data_dir).as_posix()
        # Check if this file belongs to any of our target stations
        belongs_to_target = any(
            s.lower() in rel.lower() for s in scan_stations
        )
        if not belongs_to_target:
            continue
        log.error(
            "Unknown file format: %s (file: %s)",
            ext, raw_path,
        )
        print(
            f"Error: unknown file format '{ext}' for "
            f"{raw_path}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Process each station
    total_hourly_rows = 0
    total_daily_rows = 0
    errors: list[str] = []

    for station, recs in sorted(stations.items()):
        log.info("Processing station: %s (%d files)", station, len(recs))
        station_output = output_dir / station
        station_output.mkdir(parents=True, exist_ok=True)

        station_hourly_frames: list[pd.DataFrame] = []

        for rec in recs:
            try:
                hourly_path, daily_path = materialize_resampled_outputs(
                    source_path=rec.path,
                    station=station,
                    output_dir=station_output,
                )
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

                # Read hourly for further processing
                station_hourly_frames.append(
                    pd.read_csv(hourly_path, parse_dates=["timestamp_utc"])
                )

                # Track artifacts
                artifacts.append({
                    "station": station,
                    "type": "hourly_resampled",
                    "source": rec.path.name,
                    "path": str(hourly_path),
                    "rows": h_rows,
                    "timestamp": datetime.now(UTC).isoformat(),
                })
                artifacts.append({
                    "station": station,
                    "type": "daily_resampled",
                    "source": rec.path.name,
                    "path": str(daily_path),
                    "rows": d_rows,
                    "timestamp": datetime.now(UTC).isoformat(),
                })
            except Exception as exc:
                msg = f"{station}/{rec.path.name}: {exc}"
                log.warning("  Skipping %s: %s", rec.path.name, exc)
                errors.append(msg)

        # Combine all hourly frames for this station
        if station_hourly_frames:
            # Ensure timestamp_utc is datetime in all frames
            for i, frame in enumerate(station_hourly_frames):
                if not pd.api.types.is_datetime64_any_dtype(frame["timestamp_utc"]):
                    station_hourly_frames[i] = frame.copy()
                    station_hourly_frames[i]["timestamp_utc"] = pd.to_datetime(
                        frame["timestamp_utc"], utc=True
                    )
            combined_hourly = pd.concat(
                station_hourly_frames, ignore_index=True
            )
            combined_hourly = combined_hourly.sort_values("timestamp_utc")
            combined_hourly = combined_hourly.drop_duplicates(
                subset=["timestamp_utc"], keep="first"
            )
            combined_hourly = combined_hourly.reset_index(drop=True)

            # Step 2: Imputation
            log.info("  Running imputation for %s ...", station)
            imputed, audit_records = _run_imputation(combined_hourly, station)
            all_audit_records.extend(audit_records)

            # Step 5: FWI computation
            log.info("  Computing FWI indices for %s ...", station)
            imputed = _compute_fwi_codes(imputed)

            # Write cleaned hourly
            cleaned_hourly_path = station_output / "station_hourly.csv"
            imputed.to_csv(cleaned_hourly_path, index=False)

            # Aggregate daily from cleaned hourly
            cleaned_daily = imputed.groupby(
                imputed["timestamp_utc"].dt.date
            ).agg({
                col: "mean" for col in imputed.select_dtypes(
                    include="number"
                ).columns
            }).reset_index()
            # Preserve station column
            cleaned_daily["station"] = station
            # Reorder: station first, then timestamp, then rest
            cols = ["station"] + [
                c for c in cleaned_daily.columns if c != "station"
            ]
            cleaned_daily = cleaned_daily[cols]
            cleaned_daily_path = station_output / "station_daily.csv"
            cleaned_daily.to_csv(cleaned_daily_path, index=False)

            h_rows = len(imputed)
            d_rows = len(cleaned_daily)
            total_hourly_rows = h_rows  # use final count
            total_daily_rows = d_rows

            artifacts.append({
                "station": station,
                "type": "hourly_cleaned",
                "path": str(cleaned_hourly_path),
                "rows": h_rows,
                "timestamp": datetime.now(UTC).isoformat(),
            })
            artifacts.append({
                "station": station,
                "type": "daily_cleaned",
                "path": str(cleaned_daily_path),
                "rows": d_rows,
                "timestamp": datetime.now(UTC).isoformat(),
            })

    # Step 3: Write imputation report
    imputation_report_path = output_dir / "imputation_report.csv"
    if all_audit_records:
        audit_df = audit_trail_to_dataframe(all_audit_records)
        audit_df.to_csv(imputation_report_path, index=False)
        log.info(
            "Imputation report: %d records → %s",
            len(all_audit_records),
            imputation_report_path,
        )
        artifacts.append({
            "type": "imputation_report",
            "path": str(imputation_report_path),
            "rows": len(audit_df),
            "timestamp": datetime.now(UTC).isoformat(),
        })
    else:
        # Write empty report
        audit_trail_to_dataframe([]).to_csv(imputation_report_path, index=False)
        log.info(
            "Imputation report: no gaps found → %s",
            imputation_report_path,
        )
        artifacts.append({
            "type": "imputation_report",
            "path": str(imputation_report_path),
            "rows": 0,
            "timestamp": datetime.now(UTC).isoformat(),
        })

    # Step 4: Stanhope reference data ingestion
    if not args.skip_stanhope:
        log.info("Ingesting Stanhope reference data ...")
        stanhope_output = output_dir / "stanhope"
        stanhope_output.mkdir(parents=True, exist_ok=True)
        try:
            stanhope_results = materialize_stanhope_hourly_range(
                2022, 1, 2025, 12,
                cache_dir=raw_base / "data" / "raw" / "eccc" / "stanhope",
            )
            downloaded = sum(
                1 for r in stanhope_results
                if r.status == "downloaded"
            )
            cached = sum(1 for r in stanhope_results if r.status == "cached")
            log.info(
                "Stanhope: %d downloaded, %d cached", downloaded, cached,
            )

            # Normalize and combine all Stanhope monthly files
            stanhope_frames: list[pd.DataFrame] = []
            for result in stanhope_results:
                try:
                    norm = normalize_stanhope_hourly(
                        result.cache_path, station="stanhope"
                    )
                    stanhope_frames.append(norm)
                except Exception as exc:
                    log.warning("  Stanhope normalize failed for %s: %s",
                                result.cache_path.name, exc)

            if stanhope_frames:
                # Ensure timestamp_utc is datetime in all frames
                for i, frame in enumerate(stanhope_frames):
                    if not pd.api.types.is_datetime64_any_dtype(frame["timestamp_utc"]):
                        stanhope_frames[i] = frame.copy()
                        stanhope_frames[i]["timestamp_utc"] = pd.to_datetime(
                            frame["timestamp_utc"], utc=True
                        )
                stanhope_combined = pd.concat(
                    stanhope_frames, ignore_index=True
                )
                stanhope_combined = stanhope_combined.sort_values(
                    "timestamp_utc"
                )
                stanhope_combined = stanhope_combined.drop_duplicates(
                    subset=["timestamp_utc"], keep="first"
                )
                stanhope_combined = stanhope_combined.reset_index(drop=True)

                # Run imputation on Stanhope
                log.info(
                    "  Running imputation for stanhope ..."
                )
                stanhope_imputed, stanhope_audit = _run_imputation(
                    stanhope_combined, "stanhope"
                )
                all_audit_records.extend(stanhope_audit)

                # Compute FWI on Stanhope
                log.info(
                    "  Computing FWI indices for stanhope ..."
                )
                stanhope_imputed = _compute_fwi_codes(stanhope_imputed)

                # Write cleaned hourly
                stanhope_hourly_path = stanhope_output / "station_hourly.csv"
                stanhope_imputed.to_csv(
                    stanhope_hourly_path, index=False
                )

                # Aggregate daily from cleaned hourly
                stanhope_daily = stanhope_imputed.groupby(
                    stanhope_imputed["timestamp_utc"].dt.date
                ).agg({
                    col: "mean" for col in stanhope_imputed.select_dtypes(
                        include="number"
                    ).columns
                }).reset_index()
                # Preserve station column
                stanhope_daily["station"] = "stanhope"
                # Reorder: station first, then timestamp, then rest
                cols = ["station"] + [
                    c for c in stanhope_daily.columns if c != "station"
                ]
                stanhope_daily = stanhope_daily[cols]
                stanhope_daily_path = stanhope_output / "station_daily.csv"
                stanhope_daily.to_csv(stanhope_daily_path, index=False)

                artifacts.append({
                    "station": "stanhope",
                    "type": "hourly_cleaned",
                    "path": str(stanhope_hourly_path),
                    "rows": len(stanhope_imputed),
                    "timestamp": datetime.now(UTC).isoformat(),
                })
                artifacts.append({
                    "station": "stanhope",
                    "type": "daily_cleaned",
                    "path": str(stanhope_daily_path),
                    "rows": len(stanhope_daily),
                    "timestamp": datetime.now(UTC).isoformat(),
                })
        except Exception as exc:
            msg = f"Stanhope ingestion failed: {exc}"
            log.warning(msg)
            errors.append(msg)

    # Step 7: Write artifact manifest
    manifest_path = output_dir / "pipeline_manifest.json"
    _write_manifest(artifacts, manifest_path)

    # Summary
    log.info("=" * 50)
    log.info("Pipeline complete.")
    log.info("  Stations processed: %d", len(stations))
    log.info("  Total hourly rows: %d", total_hourly_rows)
    log.info("  Total daily rows: %d", total_daily_rows)
    log.info("  Artifacts tracked: %d", len(artifacts))
    if errors:
        log.warning("  Errors: %d", len(errors))
        for e in errors:
            log.warning("    - %s", e)
    log.info("  Output: %s", output_dir.resolve())


if __name__ == "__main__":
    main()
