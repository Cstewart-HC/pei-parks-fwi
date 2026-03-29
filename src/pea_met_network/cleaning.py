#!/usr/bin/env python3
"""pea_met_network.cleaning — PEA Met Network pipeline entry point.

Usage:
    python -m pea_met_network --stations all
    python -m pea_met_network --stations greenwich
    python -m pea_met_network --stations greenwich,cavendish

Pipeline: discover raw files → load via adapters → concat → dedup →
          resample hourly → impute → FWI → write outputs.
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import shutil
import json
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

# Add project src to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pea_met_network.adapters.registry import route_by_extension  # noqa: E402
from pea_met_network.cross_station_impute import (  # noqa: E402
    DonorAssignment,
    ImputedValue,
    impute_cross_station,
    propagate_fwi_quality_flags,
)
from pea_met_network.fwi_diagnostics import (  # noqa: E402
    chain_breaks_to_dataframe,
    diagnose_chain_breaks,
)
from pea_met_network.qa_qc import generate_qa_qc_report  # noqa: E402
from pea_met_network.quality import (  # noqa: E402
    enforce_fwi_outputs,
    enforce_quality,
    truncate_date_range,
)

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
DONOR_STAGING_DIR = PROCESSED_DIR / ".donor_staging"

DONOR_KEEP_COLS = [
    "timestamp_utc",
    "air_temperature_c",
    "relative_humidity_pct",
    "dew_point_c",
    "wind_speed_kmh",
    "wind_direction_deg",
    "solar_radiation_w_m2",
    "rain_mm",
]

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
# Cross-station donor config — loaded from cleaning-config.json
# ---------------------------------------------------------------------------

ECCC_CACHE_DIR = RAW_DIR / "eccc"

# Map cleaning-config.json variable shorthand to canonical column names
_VARIABLE_NAME_MAP = {
    "rh": "relative_humidity_pct",
    "relative_humidity_pct": "relative_humidity_pct",
    "wind_speed_kmh": "wind_speed_kmh",
    "wind": "wind_speed_kmh",
    "air_temperature_c": "air_temperature_c",
    "temp": "air_temperature_c",
}


def _topological_station_order(
    stations: list[str],
    donor_assignments: list[DonorAssignment],
) -> list[str]:
    """Compute processing order so donors are always available before targets.

    Internal donors must have their staging parquet written before a target
    station needs to read it.  External donors (ECCC) are always on disk
    and impose no ordering constraint.

    Uses Kahn's algorithm on the internal-donor sub-graph.
    Stations with no internal dependencies come first.
    """
    station_set = set(stations)

    # Build adjacency: edge (donor → target) means donor must come first.
    internal_edges: list[tuple[str, str]] = []
    for da in donor_assignments:
        if da.donor_type == "internal" and da.target in station_set:
            if da.donor_key in station_set:
                internal_edges.append((da.donor_key, da.target))

    # In-degree count (only for stations in our set)
    in_degree: dict[str, int] = {s: 0 for s in stations}
    adj: dict[str, list[str]] = {s: [] for s in stations}
    for src, dst in internal_edges:
        adj[src].append(dst)
        in_degree[dst] = in_degree.get(dst, 0) + 1

    # Kahn's algorithm — stable (processes in input order when tied)
    queue = [s for s in stations if in_degree[s] == 0]
    order: list[str] = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for neighbor in adj[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    # If there's a cycle, append any remaining stations (fallback)
    remaining = [s for s in stations if s not in order]
    order.extend(remaining)
    return order


def load_donor_config(
    config: dict | None = None,
) -> tuple[list[DonorAssignment], set[str]]:
    """Load cross-station donor assignments from cleaning-config.json.

    Returns (donor_assignments, target_stations).
    Falls back to empty list if config missing or disabled.
    """
    if config is None:
        config_path = PROJECT_ROOT / "docs" / "cleaning-config.json"
        if config_path.exists():
            config = json.loads(config_path.read_text())
        else:
            return [], set()

    cross_cfg = config.get("cross_station_impute", {})
    if not cross_cfg.get("enabled", False):
        return [], set()

    max_gap = cross_cfg.get("max_gap_hours", 3)
    assignments_raw = cross_cfg.get("donor_assignments", {})

    assignments: list[DonorAssignment] = []
    targets: set[str] = set()

    for variable, entries in assignments_raw.items():
        canonical_var = _VARIABLE_NAME_MAP.get(variable, variable)
        for entry in entries:
            da = DonorAssignment(
                target=entry["target"],
                variable=canonical_var,
                priority=entry["priority"],
                donor_key=entry["donor"],
                donor_type=entry["type"],
                max_gap_hours=max_gap,
            )
            assignments.append(da)
            targets.add(entry["target"])

    return assignments, targets

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


def load_station_files(
    station_files: dict[str, list[Path]],
    station: str,
) -> pd.DataFrame | None:
    """Load raw files for a single station via adapters.

    Returns concatenated DataFrame or None if no data.
    """
    frames: list[pd.DataFrame] = []
    errors: list[str] = []

    # Handle Licor files that may contain this station
    # Cache Licor data to avoid re-reading devices.json for each station
    if not hasattr(load_station_files, "_licor_cache"):
        load_station_files._licor_cache = {}
    licor_files = station_files.get("_licor_all", [])
    for fpath in licor_files:
        try:
            fpath_key = str(fpath)
            if fpath_key not in load_station_files._licor_cache:
                adapter = route_by_extension(fpath)
                load_station_files._licor_cache[fpath_key] = adapter.load(fpath)
            df = load_station_files._licor_cache[fpath_key]
            if len(df) > 0 and "station" in df.columns:
                for station_name, group in df.groupby("station"):
                    if station_name == station:
                        frames.append(group)
        except Exception as e:
            errors.append(f"Licor load error {fpath}: {e}")

    # Load per-station files — incremental concat to limit peak memory
    df: pd.DataFrame | None = None
    for fpath in station_files.get(station, []):
        try:
            adapter = route_by_extension(fpath)
            chunk = adapter.load(fpath)
            if len(chunk) > 0:
                if "station" not in chunk.columns:
                    chunk["station"] = station
                # Dedup per-file to reduce concat memory footprint
                chunk = chunk.drop_duplicates(subset=["timestamp_utc"], keep="first") if "timestamp_utc" in chunk.columns else chunk.drop_duplicates()
                df = pd.concat([df, chunk], ignore_index=True) if df is not None else chunk
                del chunk
        except Exception as e:
            errors.append(f"Load error {fpath}: {e}")

    if errors:
        warnings.warn(
            f"{len(errors)} file load errors for {station}:\n"
            + "\n".join(errors[:5])
            + (
                f"\n... and {len(errors) - 5} more"
                if len(errors) > 5
                else ""
            ),
        )

    if df is None and not frames:
        return None

    if frames:
        licor_df = pd.concat(frames, ignore_index=True)
        del frames
        df = pd.concat([df, licor_df], ignore_index=True) if df is not None else licor_df
        del licor_df
        gc.collect()

    if df is None:
        return None
    df["station"] = station
    return df


def load_all_files(
    station_files: dict[str, list[Path]],
    target_stations: list[str],
) -> dict[str, pd.DataFrame]:
    """Load all raw files per station via adapters.

    Deprecated: prefer load_station_files for memory-efficient
    per-station loading.
    """
    result: dict[str, pd.DataFrame] = {}
    for station in target_stations:
        df = load_station_files(station_files, station)
        if df is not None:
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
    gap_threshold_hours: int = 24,
) -> np.ndarray:
    """Calculate Fine Fuel Moisture Code iteratively.

    When inputs are NaN for >= gap_threshold_hours consecutive hours and
    then become valid again, the chain restarts from startup defaults.
    Short gaps (< threshold) keep the chain broken (NaN propagation).
    """
    n = len(temp)
    ffmc = np.full(n, np.nan)

    mo_prev = 147.2 * (101.0 - ffmc_prev) / (59.5 + ffmc_prev)
    consecutive_nulls = 0

    for i in range(n):
        t = temp[i]
        h = rh[i]
        w = wind[i]
        r = rain[i]

        if np.isnan(t) or np.isnan(h) or np.isnan(w):
            ffmc[i] = np.nan
            mo_prev = np.nan
            consecutive_nulls += 1
            continue

        # Inputs are valid — check if chain should restart
        if np.isnan(mo_prev) and consecutive_nulls >= gap_threshold_hours:
            mo_prev = 147.2 * (101.0 - ffmc_prev) / (59.5 + ffmc_prev)
            consecutive_nulls = 0
        elif np.isnan(mo_prev):
            consecutive_nulls += 1
            ffmc[i] = np.nan
            continue

        consecutive_nulls = 0
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
    gap_threshold_hours: int = 24,
) -> np.ndarray:
    """Calculate Duff Moisture Code iteratively.

    When inputs are NaN for >= gap_threshold_hours consecutive hours and
    then become valid again, the chain restarts from startup defaults.
    """
    n = len(temp)
    dmc = np.full(n, np.nan)

    dl = {
        1: 6.5, 2: 7.5, 3: 9.0, 4: 12.8, 5: 13.9, 6: 13.9,
        7: 12.4, 8: 10.9, 9: 9.4, 10: 8.0, 11: 6.8, 12: 6.0,
    }

    dmc_prev_val = dmc_prev
    consecutive_nulls = 0
    for i in range(n):
        t = temp[i]
        h = rh[i]
        r = rain[i]
        m = int(month[i]) if not np.isnan(month[i]) else 7

        if np.isnan(t) or np.isnan(h):
            dmc[i] = np.nan
            dmc_prev_val = np.nan
            consecutive_nulls += 1
            continue

        # Inputs valid — check if chain should restart
        if np.isnan(dmc_prev_val) and consecutive_nulls >= gap_threshold_hours:
            dmc_prev_val = dmc_prev
            consecutive_nulls = 0
        elif np.isnan(dmc_prev_val):
            consecutive_nulls += 1
            dmc[i] = np.nan
            continue

        consecutive_nulls = 0

        rf = 0.0 if np.isnan(r) else float(r)

        if rf > 1.5:
            re = 0.92 * rf - 1.27
            dp = max(dmc_prev_val, 0.0)
            mo = 20.0 + np.exp(5.6348 - dp / 43.43)  # noqa: F841
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
    gap_threshold_hours: int = 24,
) -> np.ndarray:
    """Calculate Drought Code iteratively.

    When inputs are NaN for >= gap_threshold_hours consecutive hours and
    then become valid again, the chain restarts from startup defaults.
    """
    n = len(temp)
    dc = np.full(n, np.nan)

    fl = {
        1: -1.6, 2: -1.6, 3: -1.6, 4: 0.9, 5: 3.8, 6: 5.8,
        7: 6.4, 8: 5.0, 9: 2.4, 10: 0.4, 11: -1.6, 12: -1.6,
    }

    dc_prev_val = dc_prev
    consecutive_nulls = 0
    for i in range(n):
        t = temp[i]
        r = rain[i]
        m = int(month[i]) if not np.isnan(month[i]) else 7

        if np.isnan(t):
            dc[i] = np.nan
            dc_prev_val = np.nan
            consecutive_nulls += 1
            continue

        # Inputs valid — check if chain should restart
        if np.isnan(dc_prev_val) and consecutive_nulls >= gap_threshold_hours:
            dc_prev_val = dc_prev
            consecutive_nulls = 0
        elif np.isnan(dc_prev_val):
            consecutive_nulls += 1
            dc[i] = np.nan
            continue

        consecutive_nulls = 0

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


def calculate_fwi(
    df: pd.DataFrame,
    gap_threshold_hours: int = 24,
) -> pd.DataFrame:
    """Calculate all FWI indices for hourly station data.

    Args:
        df: Hourly station dataframe with required weather columns.
        gap_threshold_hours: When the FWI chain has been broken for
            this many consecutive hours and valid inputs resume,
            restart the chain from startup defaults.
    """
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

    ffmc = _ffmc_calc(
        temp, rh, wind, rain, gap_threshold_hours=gap_threshold_hours,
    )
    dmc = _dmc_calc(
        temp, rh, rain, month, gap_threshold_hours=gap_threshold_hours,
    )
    dc = _dc_calc(
        temp, rain, month, gap_threshold_hours=gap_threshold_hours,
    )

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
# Determinism helpers
# ---------------------------------------------------------------------------


def compute_checksum(file_path: Path) -> str:
    """Compute SHA256 checksum of a file.

    Returns hex digest string.
    """
    return hashlib.sha256(file_path.read_bytes()).hexdigest()


def verify_determinism(
    output_path: Path,
) -> bool:
    """Verify that re-running the pipeline produces identical output.

    Compares the SHA256 checksum of the given output file against
    a stored reference. Returns True if the file matches the stored
    checksum or if no reference exists yet.
    """
    checksum_file = output_path.with_suffix(".checksum")
    current = compute_checksum(output_path)

    if not checksum_file.exists():
        checksum_file.write_text(current)
        return True

    previous = checksum_file.read_text().strip()
    return current == previous


def should_process(station: str, force: bool = False) -> bool:
    """Check whether a station needs reprocessing.

    Returns True if:
      - force is True, OR
      - no output exists yet, OR
      - any raw input is newer than the most recent output.
    """
    if force:
        return True

    out_dir = PROCESSED_DIR / station
    hourly_path = out_dir / "station_hourly.csv"
    if not hourly_path.exists():
        return True

    # Latest output mtime
    latest_output = max(
        (f.stat().st_mtime for f in out_dir.iterdir() if f.is_file()),
        default=0,
    )

    # Earliest raw input mtime for this station
    station_files = discover_raw_files()
    raw_paths = station_files.get(station, [])
    if not raw_paths:
        return False

    latest_input = max(f.stat().st_mtime for f in raw_paths if f.exists())

    return latest_input > latest_output


# ---------------------------------------------------------------------------
# Donor staging helpers (disk-based cross-station imputation)
# ---------------------------------------------------------------------------


def _save_donor_parquet(station: str, hourly_df: pd.DataFrame) -> Path:
    """Save slimmed donor data to staging parquet for later disk-based loading.
    
    Keeps only DONOR_KEEP_COLS and sets timestamp_utc as index.
    Returns the path to the saved parquet file.
    """
    DONOR_STAGING_DIR.mkdir(parents=True, exist_ok=True)
    
    # Slim to donor-relevant columns
    available = [c for c in DONOR_KEEP_COLS if c in hourly_df.columns]
    slim = hourly_df[available].copy()
    
    # Ensure timestamp_utc is a proper DatetimeIndex
    if "timestamp_utc" in slim.columns:
        slim["timestamp_utc"] = pd.to_datetime(slim["timestamp_utc"], utc=True)
        slim = slim.set_index("timestamp_utc")
    
    out_path = DONOR_STAGING_DIR / f"{station}.parquet"
    slim.to_parquet(out_path, engine="pyarrow", index=True)
    return out_path


def _load_donor_from_disk(station: str) -> pd.DataFrame | None:
    """Load slimmed donor data from staging parquet.
    
    Returns DataFrame with timestamp_utc as DatetimeIndex, or None if not found.
    """
    parquet_path = DONOR_STAGING_DIR / f"{station}.parquet"
    if not parquet_path.exists():
        return None
    df = pd.read_parquet(parquet_path, engine="pyarrow")
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)
    return df


def _cleanup_donor_staging() -> None:
    """Remove the donor staging directory and all its contents."""
    if DONOR_STAGING_DIR.exists():
        shutil.rmtree(DONOR_STAGING_DIR, ignore_errors=True)




# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------


def _collect_qa_qc_data(
    current_hourly: list[pd.DataFrame],
    current_daily: list[pd.DataFrame],
    current_stations: list[str],
) -> tuple[list[pd.DataFrame], list[pd.DataFrame]]:
    """Collect hourly/daily data for QA/QC from current run + disk.

    Includes stations from the current run plus any other stations
    that have existing processed output, so a single-station re-run
    doesn't clobber the multi-station report.
    """
    all_hourly = list(current_hourly)
    all_daily = list(current_daily)
    for station in ALL_STATIONS:
        if station in current_stations:
            continue
        h_path = PROCESSED_DIR / station / "station_hourly.csv"
        d_path = PROCESSED_DIR / station / "station_daily.csv"
        if h_path.exists() and d_path.exists():
            try:
                oh = pd.read_csv(h_path)
                od = pd.read_csv(d_path)
                if "station" not in oh.columns:
                    oh["station"] = station
                else:
                    oh["station"] = oh["station"].fillna(station)
                if "station" not in od.columns:
                    od["station"] = station
                else:
                    od["station"] = od["station"].fillna(station)
                if len(oh) > 0:
                    all_hourly.append(oh)
                if len(od) > 0:
                    all_daily.append(od)
            except Exception:
                pass
    return all_hourly, all_daily


def _register_manifest_artifact(
    artifact_type: str,
    path: Path,
    rows: int,
) -> None:
    """Register a pipeline artifact in the manifest.

    Removes any existing entry of the same type to avoid duplicates.
    """
    manifest_path = PROCESSED_DIR / "pipeline_manifest.json"
    if not manifest_path.exists():
        return
    manifest = json.loads(manifest_path.read_text())
    manifest["artifacts"] = [
        a for a in manifest["artifacts"]
        if a.get("type") != artifact_type
    ]
    manifest["artifacts"].append({
        "type": artifact_type,
        "path": str(path.relative_to(PROJECT_ROOT)),
        "rows": rows,
        "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
    })
    manifest["generated_at"] = pd.Timestamp.now(tz="UTC").isoformat()
    manifest_path.write_text(json.dumps(manifest, indent=2))


def run_pipeline(stations: list[str], force: bool = False) -> None:
    """Execute the full pipeline for given stations.

    Serial disk-based approach:
      1. Topologically sort stations so internal donors are processed first.
      2. For each station (one at a time):
         a. Load → dedup → resample hourly → truncate → quality → impute.
         b. Save donor-relevant columns to staging parquet.
         c. If cross-station target: load donors from staging disk, impute.
         d. Calculate FWI → enforce FWI outputs → chain break diagnostics.
         e. Write hourly/daily CSVs → free memory.
      3. Write aggregate reports (manifest, QA/QC, etc.) reading from disk.
    """
    print(f"Pipeline: {len(stations)} stations")

    station_files = discover_raw_files()

    all_reports: list[dict] = []
    all_quality_actions: list[dict] = []
    all_chain_breaks: list = []
    all_cross_records: list[ImputedValue] = []
    processed_stations: list[str] = []

    # Load cleaning config once
    config_path = PROJECT_ROOT / "docs" / "cleaning-config.json"
    quality_config = (
        json.loads(config_path.read_text())
        if config_path.exists()
        else {}
    )
    fwi_config = quality_config.get("fwi", {})
    gap_threshold = fwi_config.get("gap_threshold_hours", 24)

    # Load cross-station donor config
    donor_assignments, cross_station_targets = load_donor_config(quality_config)

    # Compute serial processing order (donors before targets)
    ordered_stations = _topological_station_order(stations, donor_assignments)
    print(
        f"  Processing order: {', '.join(ordered_stations)}",
        file=sys.stderr,
    )

    # Ensure clean donor staging directory
    _cleanup_donor_staging()
    DONOR_STAGING_DIR.mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # SERIAL PASS: process one station at a time
    # =========================================================================
    for station in ordered_stations:
        # --- Skip stale check ---
        if not should_process(station, force=force):
            print(f"  {station}: skipping (output up to date)")
            continue

        # --- Load and clean through standard pipeline stages ---
        df = load_station_files(station_files, station)
        if df is None:
            print(f"  WARNING: no data for {station}", file=sys.stderr)
            continue
        print(f"  {station}: {len(df)} raw rows", file=sys.stderr)

        df = dedup(df)
        deduped_rows = len(df)
        hourly = resample_hourly(df)
        del df
        gc.collect()
        print(f"  {station}: {deduped_rows} raw → {len(hourly)} hourly rows", file=sys.stderr)

        hourly = truncate_date_range(hourly, quality_config)

        hourly, quality_actions = enforce_quality(hourly, quality_config)
        all_quality_actions.extend(quality_actions)

        hourly, report = impute(hourly, station)
        all_reports.extend(report)

        # --- Save donor staging parquet for downstream stations ---
        _save_donor_parquet(station, hourly)
        gc.collect()

        # --- Cross-station imputation (if this station is a target) ---
        is_cross_target = station in cross_station_targets
        if is_cross_target and donor_assignments:
            hourly, records = impute_cross_station(
                hourly,
                station,
                donor_assignments=donor_assignments,
                internal_hourly=None,  # donors loaded from disk
                eccc_cache_dir=ECCC_CACHE_DIR,
                disk_donor_dir=DONOR_STAGING_DIR,
            )
            all_cross_records.extend(records)

            # Restore timestamp_utc as column for downstream processing
            if isinstance(hourly.index, pd.DatetimeIndex):
                hourly = hourly.reset_index()

            # Propagate quality flags to FWI columns
            hourly = propagate_fwi_quality_flags(hourly)
            del records
            gc.collect()
            print(
                f"  {station}: cross-station imputed",
                file=sys.stderr,
            )

        # --- FWI calculation ---
        hourly = calculate_fwi(hourly, gap_threshold_hours=gap_threshold)

        # --- FWI output enforcement ---
        hourly, fwi_actions = enforce_fwi_outputs(hourly, quality_config)
        all_quality_actions.extend(fwi_actions)

        # --- FWI chain break diagnostics ---
        station_breaks = diagnose_chain_breaks(
            hourly, station, quality_actions
        )
        all_chain_breaks.extend(station_breaks)

        # --- Aggregate daily ---
        daily = aggregate_daily(hourly)

        # --- Write outputs ---
        out_dir = PROCESSED_DIR / station
        out_dir.mkdir(parents=True, exist_ok=True)

        hourly["station"] = station
        daily["station"] = station

        hourly_path = out_dir / "station_hourly.csv"
        hourly = hourly[sorted(hourly.columns)]
        hourly.to_csv(hourly_path, index=False)
        print(f"  {station}: {len(hourly)} hourly rows")

        daily_path = out_dir / "station_daily.csv"
        daily = daily[sorted(daily.columns)]
        daily.to_csv(daily_path, index=False)
        print(f"  {station}: {len(daily)} daily rows")

        processed_stations.append(station)

        # --- Free all per-station memory ---
        del hourly, daily
        gc.collect()

    # Clean up donor staging
    _cleanup_donor_staging()

    # =========================================================================
    # AGGREGATE REPORTS (reading station data from disk as needed)
    # =========================================================================

    # Write cross-station imputation audit trail
    if all_cross_records:
        audit_rows = [
            {
                "station": r.station,
                "timestamp_utc": r.timestamp_utc,
                "variable": r.variable,
                "imputed_value": r.imputed_value,
                "quality_flag": r.quality_flag,
                "source": r.source,
                "method": r.method,
                "donor_priority": r.donor_priority,
            }
            for r in all_cross_records
        ]
        audit_df = pd.DataFrame(audit_rows)
        audit_path = PROCESSED_DIR / "cross_station_imputation_audit.csv"
        audit_df.to_csv(audit_path, index=False)
        print(f"  Cross-station audit: {len(audit_df)} records")

    # Write pipeline manifest with SHA256 checksums
    manifest_path = PROCESSED_DIR / "pipeline_manifest.json"
    manifest = {"artifacts": [], "checksums": {}}
    for station in stations:
        station_dir = PROCESSED_DIR / station
        if not station_dir.exists():
            continue
        for fpath in sorted(station_dir.iterdir()):
            if fpath.is_file() and fpath.suffix == ".csv":
                try:
                    row_count = len(pd.read_csv(fpath))
                except Exception:
                    row_count = -1
                manifest["artifacts"].append({
                    "type": "processed_csv",
                    "path": str(fpath.relative_to(PROJECT_ROOT)),
                    "station": station,
                    "rows": row_count,
                    "timestamp": pd.Timestamp.now(tz="UTC").isoformat(),
                })
                manifest["checksums"][
                    str(fpath.relative_to(PROJECT_ROOT))
                ] = compute_checksum(fpath)
    manifest["generated_at"] = pd.Timestamp.now(tz="UTC").isoformat()

    # Build stations summary
    stations_summary: dict[str, int] = {}
    for a in manifest["artifacts"]:
        s = a.get("station", "unknown")
        stations_summary[s] = stations_summary.get(s, 0) + 1
    manifest["stations"] = stations_summary

    # Compute unprocessed files
    all_station_files = discover_raw_files()
    stations_with_output = {
        s for s in ALL_STATIONS
        if (PROCESSED_DIR / s / "station_hourly.csv").exists()
    }
    unprocessed = sum(
        len(files)
        for st_name, files in all_station_files.items()
        if st_name not in stations_with_output
        and not st_name.startswith("_")
    )
    manifest["unprocessed_count"] = unprocessed

    manifest_path.write_text(json.dumps(manifest, indent=2))
    print(f"  Manifest: {len(manifest['checksums'])} checksums")

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

    # Register imputation report in pipeline manifest
    _register_manifest_artifact(
        "imputation_report", report_path, len(report_df)
    )

    # QA/QC report — read all station data from disk (no in-memory accumulation)
    all_qa_hourly, all_qa_daily = _collect_qa_qc_data(
        [], [], processed_stations
    )

    if all_qa_hourly and all_qa_daily:
        combined_hourly = pd.concat(all_qa_hourly, ignore_index=True)
        combined_daily = pd.concat(all_qa_daily, ignore_index=True)
        qa_qc_df = generate_qa_qc_report(
            combined_hourly, combined_daily, all_quality_actions,
            chain_breaks=all_chain_breaks,
        )
        qa_qc_path = PROCESSED_DIR / "qa_qc_report.csv"
        qa_qc_df.to_csv(qa_qc_path, index=False)
        print(f"  QA/QC report: {len(qa_qc_df)} stations")

        # Register QA/QC report in pipeline manifest
        _register_manifest_artifact(
            "qa_qc_report", qa_qc_path, len(qa_qc_df)
        )
        del combined_hourly, combined_daily
        gc.collect()

    # FWI chain break missingness report
    if all_chain_breaks:
        breaks_df = chain_breaks_to_dataframe(all_chain_breaks)
        missingness_path = PROCESSED_DIR / "fwi_missingness_report.csv"
        breaks_df.to_csv(missingness_path, index=False)
        print(
            f"  FWI missingness report: {len(breaks_df)} chain breaks"
        )
        _register_manifest_artifact(
            "fwi_missingness_report", missingness_path, len(breaks_df)
        )
    else:
        print("  FWI missingness report: no chain breaks detected")
        print("  Manifest: qa_qc_report registered")

    # Quality enforcement report
    if all_quality_actions:
        quality_report_df = pd.DataFrame(all_quality_actions)
        quality_report_path = PROCESSED_DIR / "quality_enforcement_report.csv"
        quality_report_df.to_csv(quality_report_path, index=False)
        print(f"  Quality enforcement report: {len(quality_report_df)} actions")

        # Register quality enforcement report in manifest
        _register_manifest_artifact(
            "quality_enforcement_report",
            quality_report_path,
            len(quality_report_df),
        )
        print("  Manifest: quality_enforcement_report registered")

    print("Done.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="PEA Met Network cleaning pipeline"
    )
    parser.add_argument(
        "--stations",
        default="all",
        help=(
            "Comma-separated station names or 'all' (default). "
            f"Known: {', '.join(ALL_STATIONS)}"
        ),
    )
    parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Force reprocessing even if outputs are up to date",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Report file counts without writing any outputs",
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

    if args.dry_run:
        station_files = discover_raw_files()
        for station in target:
            files = station_files.get(station, [])
            print(f"Station {station}: {len(files)} file(s)")
        total = sum(len(station_files.get(s, [])) for s in target)
        print(f"Total: {total} file(s) across {len(target)} station(s)")
        return

    run_pipeline(target, force=args.force)


if __name__ == "__main__":
    main()
