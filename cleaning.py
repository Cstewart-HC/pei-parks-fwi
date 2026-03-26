#!/usr/bin/env python3
"""cleaning.py — PEA Met Network pipeline entry point.

Usage:
    python cleaning.py --stations all
    python cleaning.py --stations greenwich
    python cleaning.py --stations greenwich,cavendish

Pipeline: discover raw files → load via adapters → concat → dedup →
          resample hourly → impute → FWI → write outputs.
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# Add project src to path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pea_met_network.adapters.registry import route_by_extension  # noqa: E402
from pea_met_network.qa_qc import generate_qa_qc_report  # noqa: E402

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

ALL_STATIONS = [
    "greenwich",
    "cavendish",
    "north_rustico",
    "stanley_bridge",
    "tracadie",
    "stanhope",
]

FWI_COLUMNS = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]

FWI_REQUIRED = [
    "air_temperature_c",
    "relative_humidity_pct",
    "wind_speed_kmh",
    "rain_mm",
]

# ---------------------------------------------------------------------------
# Data discovery — map station names to raw data files
# ---------------------------------------------------------------------------


def discover_raw_files() -> dict[str, list[Path]]:
    """Scan raw data directories and return {station: [file_paths]}.

    Handles three source trees:
    - data/raw/peinp/ — PEINP CSV/XLSX/XLE files
    - data/raw/eccc/stanhope/ — ECCC Stanhope hourly CSVs
    - data/raw/licor/ — Licor JSON files
    """
    station_files: dict[str, list[Path]] = {s: [] for s in ALL_STATIONS}

    # --- PEINP files (csv, xlsx, xle) ---
    peinp_dir = RAW_DIR / "peinp"
    if peinp_dir.exists():
        for ext in ("*.csv", "*.xlsx", "*.xle"):
            for fpath in peinp_dir.rglob(ext):
                station = _infer_station_from_path(fpath)
                if station:
                    station_files.setdefault(station, []).append(
                        fpath
                    )

    # --- ECCC Stanhope ---
    eccc_dir = RAW_DIR / "eccc" / "stanhope"
    if eccc_dir.exists():
        for fpath in eccc_dir.glob("*.csv"):
            station_files.setdefault("stanhope", []).append(fpath)

    # --- Licor JSON ---
    licor_dir = RAW_DIR / "licor"
    if licor_dir.exists():
        devices_json = licor_dir / "devices.json"
        if devices_json.exists():
            station_files.setdefault("_licor_all", []).append(
                devices_json
            )

    return station_files


def _infer_station_from_path(path: Path) -> str | None:
    """Infer station name from a file path."""
    p = str(path).lower()
    mapping = {
        "greenwich": "greenwich",
        "cavendish": "cavendish",
        "north_rustico": "north_rustico",
        "north rustico": "north_rustico",
        "stanley_bridge": "stanley_bridge",
        "stanley bridge": "stanley_bridge",
        "tracadie": "tracadie",
        "stanhope": "stanhope",
    }
    for keyword, name in mapping.items():
        if keyword in p:
            return name
    return None


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------


def load_all_files(
    station_files: dict[str, list[Path]],
    target_stations: list[str],
) -> dict[str, pd.DataFrame]:
    """Load all raw files per station via adapters."""
    station_dfs: dict[str, list[pd.DataFrame]] = {}
    errors: list[str] = []

    # Handle Licor separately — devices.json loads all stations
    licor_files = station_files.pop("_licor_all", [])
    for fpath in licor_files:
        try:
            adapter = route_by_extension(fpath)
            df = adapter.load(fpath)
            if len(df) > 0 and "station" in df.columns:
                for station_name, group in df.groupby("station"):
                    if station_name in target_stations:
                        station_dfs.setdefault(
                            station_name, []
                        ).append(group)
        except Exception as e:
            errors.append(f"Licor load error {fpath}: {e}")

    # Load per-station files
    for station in target_stations:
        files = station_files.get(station, [])
        for fpath in files:
            try:
                adapter = route_by_extension(fpath)
                df = adapter.load(fpath)
                if len(df) > 0:
                    if "station" not in df.columns:
                        df["station"] = station
                    station_dfs.setdefault(station, []).append(df)
            except Exception as e:
                errors.append(f"Load error {fpath}: {e}")

    if errors:
        warnings.warn(
            f"{len(errors)} file load errors:\n"
            + "\n".join(errors[:5])
            + (
                f"\n... and {len(errors) - 5} more"
                if len(errors) > 5
                else ""
            ),
        )

    # Concatenate per-station
    result: dict[str, pd.DataFrame] = {}
    for station, frames in station_dfs.items():
        if not frames:
            continue
        df = pd.concat(frames, ignore_index=True)
        df["station"] = station
        result[station] = df

    return result


def dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact and timestamp duplicates."""
    if len(df) == 0:
        return df

    if "timestamp_utc" in df.columns:
        df["timestamp_utc"] = pd.to_datetime(
            df["timestamp_utc"], utc=True
        )

    before = len(df)
    df = df.drop_duplicates()
    df = df.drop_duplicates(
        subset=["timestamp_utc"], keep="first"
    ).reset_index(drop=True)
    removed = before - len(df)
    if removed > 0:
        print(f"  Dedup: removed {removed} rows", file=sys.stderr)
    return df


def resample_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Resample to hourly frequency."""
    if len(df) == 0:
        return df

    df = df.copy()
    df["timestamp_utc"] = pd.to_datetime(
        df["timestamp_utc"], utc=True
    )
    df = df.set_index("timestamp_utc").sort_index()

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    rain_cols = [c for c in numeric_cols if "rain" in c.lower()]
    other_numeric = [c for c in numeric_cols if c not in rain_cols]
    keep_cols = (
        [c for c in ["station", "source_file"] if c in df.columns]
    )

    frames = []
    if other_numeric:
        frames.append(df[other_numeric].resample("h").mean())
    if rain_cols:
        frames.append(df[rain_cols].resample("h").sum())
    if keep_cols:
        frames.append(df[keep_cols].resample("h").first())

    if not frames:
        return df.reset_index()

    result = pd.concat(frames, axis=1).dropna(how="all")

    # Forward-fill metadata columns that may have NaN from empty resample bins
    for col in ["station", "source_file"]:
        if col in result.columns:
            result[col] = result[col].ffill()

    return result.reset_index()


def impute(
    df: pd.DataFrame,
    station: str,
    max_gap_hours: int = 6,
) -> tuple[pd.DataFrame, list[dict]]:
    """Impute short gaps with linear interpolation.

    Returns (imputed_df, list_of_report_dicts).
    """
    report_rows: list[dict] = []
    df = df.copy()
    df["timestamp_utc"] = pd.to_datetime(
        df["timestamp_utc"], utc=True
    )

    impute_cols = [
        "air_temperature_c",
        "relative_humidity_pct",
        "wind_speed_kmh",
        "dew_point_c",
        "solar_radiation_w_m2",
        "barometric_pressure_kpa",
        "wind_direction_deg",
    ]

    for col in impute_cols:
        if col not in df.columns:
            continue

        mask = df[col].isna()
        if not mask.any():
            continue

        is_nan = mask.astype(int)
        gap_groups = (is_nan != is_nan.shift()).cumsum()

        for _, group in is_nan.groupby(gap_groups):
            if group.sum() == 0:
                continue
            gap_len = len(group)
            start_idx = group.index[0]
            end_idx = group.index[-1]
            time_start = df.loc[start_idx, "timestamp_utc"]
            time_end = df.loc[end_idx, "timestamp_utc"]

            if gap_len <= max_gap_hours:
                df.loc[start_idx:end_idx, col] = df[col].iloc[
                    start_idx : end_idx + 1
                ].interpolate(method="linear")
                report_rows.append(
                    {
                        "station": station,
                        "variable": col,
                        "time_start": time_start,
                        "time_end": time_end,
                        "method": "linear_interpolation",
                        "count_affected": gap_len,
                    }
                )
            else:
                report_rows.append(
                    {
                        "station": station,
                        "variable": col,
                        "time_start": time_start,
                        "time_end": time_end,
                        "method": "preserve",
                        "count_affected": gap_len,
                    }
                )

    return df, report_rows


# ---------------------------------------------------------------------------
# FWI — Van Wagner (1987) Canadian Fire Weather Index System
# ---------------------------------------------------------------------------


def _ffmc_calc(
    temp: np.ndarray,
    rh: np.ndarray,
    wind: np.ndarray,
    rain: np.ndarray,
    ffmc_prev: float = 85.0,
) -> np.ndarray:
    """Calculate Fine Fuel Moisture Code iteratively."""
    n = len(temp)
    ffmc = np.full(n, np.nan)

    mo_prev = 147.2 * (101.0 - ffmc_prev) / (59.5 + ffmc_prev)

    for i in range(n):
        t = temp[i]
        h = rh[i]
        w = wind[i]
        r = rain[i]

        if np.isnan(t) or np.isnan(h) or np.isnan(w):
            ffmc[i] = np.nan
            mo_prev = np.nan
            continue

        rf = 0.0 if np.isnan(r) else float(r)

        # Rain adjustment
        if rf > 0.5:
            mo_safe = (
                mo_prev if not np.isnan(mo_prev) else 85.0
            )
            if rf < 1.5:
                mo_prev = mo_safe
            else:
                mr = mo_safe + 42.5 * rf * np.exp(
                    -100.0 / (251.0 - mo_safe)
                ) * (1.0 - np.exp(-6.93 / rf))
                mo_prev = min(mr, 150.0)

        if np.isnan(mo_prev):
            mo_prev = 85.0

        # Equilibrium moisture content
        ed = (
            0.942 * h**0.679
            + 11.0 * np.exp((h - 100.0) / 10.0)
            + 0.18 * (21.1 - t)
            * (1.0 - 1.0 / np.exp(0.115 * h))
        )
        ew = (
            0.618 * h**0.753
            + 10.0 * np.exp((h - 100.0) / 10.0)
            + 0.18 * (21.1 - t)
            * (1.0 - 1.0 / np.exp(0.115 * h))
        )

        if mo_prev < ed:
            mo_new = ew + (mo_prev - ew) * 0.5
        else:
            kl = 0.424 * (1.0 - (h / 100.0) ** 1.7) + (
                0.0694 * np.sqrt(max(0, w))
            ) * (1.0 - (h / 100.0) ** 8)
            kl = max(0.0, kl)
            mo_new = ed + (mo_prev - ed) * (10.0 ** (-kl))

        mo_new = max(0.0, mo_new)
        ffmc[i] = 59.5 * (250.0 - mo_new) / (147.2 + mo_new)
        ffmc[i] = max(0.0, min(101.0, ffmc[i]))
        mo_prev = mo_new

    return ffmc


def _dmc_calc(
    temp: np.ndarray,
    rh: np.ndarray,
    rain: np.ndarray,
    month: np.ndarray,
    dmc_prev: float = 6.0,
) -> np.ndarray:
    """Calculate Duff Moisture Code iteratively."""
    n = len(temp)
    dmc = np.full(n, np.nan)

    dl = {
        1: 6.5, 2: 7.5, 3: 9.0, 4: 12.8, 5: 13.9, 6: 13.9,
        7: 12.4, 8: 10.9, 9: 9.4, 10: 8.0, 11: 6.8, 12: 6.0,
    }

    dmc_prev_val = dmc_prev
    for i in range(n):
        t = temp[i]
        h = rh[i]
        r = rain[i]
        m = int(month[i]) if not np.isnan(month[i]) else 7

        if np.isnan(t) or np.isnan(h):
            dmc[i] = np.nan
            continue

        rf = 0.0 if np.isnan(r) else float(r)

        if rf > 1.5:
            re = 0.92 * rf - 1.27
            dp = max(dmc_prev_val, 0.0)
            mo = 20.0 + np.exp(5.6348 - dp / 43.43)
            if dp <= 33.0:
                b = 100.0 / (0.5 + 0.3 * dp)
            elif dp <= 65.0:
                b = 14.0 - 1.3 * np.log(dp)
            else:
                b = 6.2 * np.log(dp) - 17.5
            mr = dp + 1000.0 * re / (48.77 + b * re)
            dmc_prev_val = max(0.0, mr)
        # else: dmc_prev_val unchanged

        k = (
            1.894
            * (t + 1.1)
            * (100.0 - h)
            * dl.get(m, 10.0)
            * 0.0001
        )
        k = max(0.0, k)

        log_arg = 39.83 - dmc_prev_val
        if log_arg <= 0:
            dmc_val = dmc_prev_val
        else:
            dmc_val = 244.72 - 43.43 * np.log(log_arg)

        dmc_val = max(0.0, dmc_val) + k
        dmc[i] = max(0.0, dmc_val)
        dmc_prev_val = dmc[i]

    return dmc


def _dc_calc(
    temp: np.ndarray,
    rain: np.ndarray,
    month: np.ndarray,
    dc_prev: float = 15.0,
) -> np.ndarray:
    """Calculate Drought Code iteratively."""
    n = len(temp)
    dc = np.full(n, np.nan)

    fl = {
        1: -1.6, 2: -1.6, 3: -1.6, 4: 0.9, 5: 3.8, 6: 5.8,
        7: 6.4, 8: 5.0, 9: 2.4, 10: 0.4, 11: -1.6, 12: -1.6,
    }

    dc_prev_val = dc_prev
    for i in range(n):
        t = temp[i]
        r = rain[i]
        m = int(month[i]) if not np.isnan(month[i]) else 7

        if np.isnan(t):
            dc[i] = np.nan
            continue

        rf = 0.0 if np.isnan(r) else float(r)

        if rf > 2.8:
            ra = 0.83 * rf - 1.27
            Q0 = 800.0 * np.exp(-dc_prev_val / 400.0)
            Qr = max(0.0, Q0 + 3.937 * ra)
            dc_prev_val = max(0.0, 400.0 * np.log(800.0 / Qr))

        fl_val = fl.get(m, 0.0)
        dc_val = dc_prev_val + 0.36 * (t + fl_val)
        dc[i] = max(0.0, dc_val)
        dc_prev_val = dc[i]

    return dc


def calculate_fwi(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all FWI indices for hourly station data."""
    df = df.copy()

    missing = [c for c in FWI_REQUIRED if c not in df.columns]
    if missing:
        print(
            f"  FWI: skipping — missing: {missing}",
            file=sys.stderr,
        )
        for col in FWI_COLUMNS:
            df[col] = np.nan
        return df

    temp = df["air_temperature_c"].to_numpy(dtype=float)
    rh = df["relative_humidity_pct"].to_numpy(dtype=float)
    wind = df["wind_speed_kmh"].to_numpy(dtype=float)
    rain = df["rain_mm"].to_numpy(dtype=float)

    df["timestamp_utc"] = pd.to_datetime(
        df["timestamp_utc"], utc=True
    )
    month = df["timestamp_utc"].dt.month.to_numpy(dtype=float)

    ffmc = _ffmc_calc(temp, rh, wind, rain)
    dmc = _dmc_calc(temp, rh, rain, month)
    dc = _dc_calc(temp, rain, month)

    # ISI
    mo = 147.2 * (101.0 - ffmc) / (59.5 + ffmc)
    ff = 19.115 * np.exp(0.14388 * mo)
    w_safe = np.where(np.isnan(wind), 0.0, wind)
    isi = 0.208 * ff * np.exp(0.05039 * w_safe)
    isi = np.where(np.isnan(ffmc), np.nan, isi)

    # BUI
    bui = np.where(
        dmc <= 0.4 * dc,
        dmc + 0.5 * dc,
        0.8 * dc + 0.2 * dmc,
    )
    bui = np.where(np.isnan(dmc) | np.isnan(dc), np.nan, bui)
    bui = np.maximum(0, bui)

    # FWI
    fD = np.where(
        bui <= 80.0,
        0.626 * bui**0.809 + 2.0,
        1000.0 / (25.0 + 108.64 * np.exp(-0.023 * bui)),
    )
    bB = 0.1 * isi * fD
    fwi = np.where(
        bB <= 1.0,
        bB,
        np.exp(2.72 * (0.434 * np.log(np.maximum(bB, 1e-10)))**0.647),
    )
    fwi = np.where(np.isnan(isi) | np.isnan(bui), np.nan, fwi)
    fwi = np.maximum(0, fwi)

    df["ffmc"] = ffmc
    df["dmc"] = dmc
    df["dc"] = dc
    df["isi"] = isi
    df["bui"] = bui
    df["fwi"] = fwi

    return df


def aggregate_daily(hourly_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate hourly data to daily."""
    if len(hourly_df) == 0:
        return hourly_df

    df = hourly_df.copy()
    df["timestamp_utc"] = pd.to_datetime(
        df["timestamp_utc"], utc=True
    )
    df["date"] = df["timestamp_utc"].dt.date

    agg_dict: dict[str, str | list] = {}
    for col in df.columns:
        if col in ("timestamp_utc", "date", "source_file"):
            continue
        if col == "station":
            agg_dict[col] = "first"
        elif col == "rain_mm":
            agg_dict[col] = "sum"
        elif col in FWI_COLUMNS:
            agg_dict[col] = "mean"
        elif pd.api.types.is_numeric_dtype(df[col]):
            agg_dict[col] = "mean"

    if not agg_dict:
        return df

    daily = df.groupby("date").agg(agg_dict).reset_index()
    daily = daily.rename(columns={"date": "timestamp_utc"})
    return daily


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def run_pipeline(stations: list[str]) -> None:
    """Execute the full pipeline for given stations."""
    print(f"Pipeline: {len(stations)} stations")

    station_files = discover_raw_files()
    station_dfs = load_all_files(station_files, stations)

    all_reports: list[dict] = []
    all_hourly: list[pd.DataFrame] = []
    all_daily: list[pd.DataFrame] = []

    for station in stations:
        if station not in station_dfs:
            print(f"  WARNING: no data for {station}", file=sys.stderr)
            continue

        df = station_dfs[station]
        print(
            f"  {station}: {len(df)} raw rows",
            file=sys.stderr,
        )

        df = dedup(df)
        hourly = resample_hourly(df)
        print(
            f"  {station}: {len(hourly)} hourly rows",
            file=sys.stderr,
        )

        hourly, report = impute(hourly, station)
        all_reports.extend(report)

        hourly = calculate_fwi(hourly)
        daily = aggregate_daily(hourly)

        out_dir = PROCESSED_DIR / station
        out_dir.mkdir(parents=True, exist_ok=True)

        hourly_path = out_dir / "station_hourly.csv"
        hourly.to_csv(hourly_path, index=False)
        print(f"  {station}: {len(hourly)} hourly rows")

        daily_path = out_dir / "station_daily.csv"
        daily.to_csv(daily_path, index=False)
        print(f"  {station}: {len(daily)} daily rows")

        all_hourly.append(hourly)
        all_daily.append(daily)

    # Write combined imputation report
    report_df = pd.DataFrame(
        all_reports,
        columns=[
            "station",
            "variable",
            "time_start",
            "time_end",
            "method",
            "count_affected",
        ],
    )
    report_path = PROCESSED_DIR / "imputation_report.csv"
    report_df.to_csv(report_path, index=False)
    print(f"  Imputation report: {len(report_df)} entries")

    # QA/QC report
    if all_hourly and all_daily:
        combined_hourly = pd.concat(all_hourly, ignore_index=True)
        combined_daily = pd.concat(all_daily, ignore_index=True)
        qa_qc_df = generate_qa_qc_report(combined_hourly, combined_daily)
        qa_qc_path = PROCESSED_DIR / "qa_qc_report.csv"
        qa_qc_df.to_csv(qa_qc_path, index=False)
        print(f"  QA/QC report: {len(qa_qc_df)} stations")

        # Register QA/QC report in pipeline manifest
        manifest_path = PROCESSED_DIR / "pipeline_manifest.json"
        if manifest_path.exists():
            manifest = json.loads(manifest_path.read_text())
            # Remove any existing qa_qc_report entries to avoid duplicates
            manifest["artifacts"] = [
                a for a in manifest["artifacts"]
                if a.get("type") != "qa_qc_report"
            ]
            manifest["artifacts"].append({
                "type": "qa_qc_report",
                "path": str(qa_qc_path.relative_to(PROJECT_ROOT)),
                "rows": len(qa_qc_df),
                "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
            })
            manifest["generated_at"] = pd.Timestamp.now(tz="UTC").isoformat()
            manifest_path.write_text(json.dumps(manifest, indent=2))
            print("  Manifest: qa_qc_report registered")

    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PEA Met Network cleaning pipeline"
    )
    parser.add_argument(
        "--stations",
        required=True,
        help=(
            "Comma-separated station names or 'all'. "
            f"Known: {', '.join(ALL_STATIONS)}"
        ),
    )
    args = parser.parse_args()

    if args.stations.lower() == "all":
        target = ALL_STATIONS
    else:
        target = [s.strip() for s in args.stations.split(",")]

    unknown = set(target) - set(ALL_STATIONS)
    if unknown:
        print(
            f"Unknown stations: {unknown}. "
            f"Known: {ALL_STATIONS}",
            file=sys.stderr,
        )
        sys.exit(1)

    run_pipeline(target)


if __name__ == "__main__":
    main()
