"""qa_qc.py — QA/QC summary functions for PEA Met Network.

Provides per-station quality assurance metrics:
missingness, duplicate timestamps, out-of-range values, and coverage.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Default valid ranges for meteorological variables.
DEFAULT_RANGES: dict[str, tuple[float, float]] = {
    "air_temperature_c": (-50.0, 60.0),
    "relative_humidity_pct": (0.0, 105.0),
    "wind_speed_kmh": (0.0, 200.0),
    "rain_mm": (0.0, 500.0),
    "wind_direction_deg": (0.0, 360.0),
}


def missingness_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-variable missing count and percentage.

    Parameters
    ----------
    df : DataFrame with at least one column.

    Returns
    -------
    DataFrame with columns: variable, missing_count, missing_pct.
    """
    numeric = df.select_dtypes(include="number").columns
    rows = []
    for col in numeric:
        n_miss = int(df[col].isna().sum())
        rows.append({
            "variable": col,
            "missing_count": n_miss,
            "missing_pct": (
                round(n_miss / len(df) * 100, 2)
                if len(df) > 0
                else 0.0
            ),
        })
    return pd.DataFrame(rows)


def duplicate_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """Return rows that have duplicated timestamps.

    Parameters
    ----------
    df : DataFrame with a 'timestamp_utc' column.

    Returns
    -------
    DataFrame of the duplicated rows (all occurrences).
    """
    if "timestamp_utc" not in df.columns:
        return pd.DataFrame()
    mask = df["timestamp_utc"].duplicated(keep=False)
    return df.loc[mask].reset_index(drop=True)


def out_of_range_values(
    df: pd.DataFrame,
    ranges: dict[str, tuple[float, float]] | None = None,
) -> pd.DataFrame:
    """Return rows with values outside valid ranges.

    Parameters
    ----------
    df : DataFrame.
    ranges : dict mapping column name to (lo, hi).  Defaults to
        ``DEFAULT_RANGES`` for known meteorological columns.

    Returns
    -------
    DataFrame with extra columns: oov_column, oov_value, oov_range.
    """
    if ranges is None:
        ranges = DEFAULT_RANGES

    results: list[pd.DataFrame] = []
    for col, (lo, hi) in ranges.items():
        if col not in df.columns:
            continue
        subset = df[(df[col] < lo) | (df[col] > hi)].copy()
        if len(subset) == 0:
            continue
        subset = subset.assign(
            oov_column=col,
            oov_value=subset[col],
            oov_range=f"({lo}, {hi})",
        )
        results.append(subset)

    if not results:
        return pd.DataFrame()
    return pd.concat(results, ignore_index=True)


def coverage_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Return per-station record counts.

    Parameters
    ----------
    df : DataFrame with a 'station' column.

    Returns
    -------
    DataFrame with columns: station, total_records.
    """
    if "station" not in df.columns:
        raise ValueError("Station column not found in DataFrame")

    return (
        df.groupby("station")
        .size()
        .reset_index(name="total_records")
        .sort_values("station")
        .reset_index(drop=True)
    )


def calculate_completeness(df: pd.DataFrame) -> float:
    """Calculate overall data completeness as a fraction.

    Completeness = fraction of non-NaN values across all numeric columns.

    Parameters
    ----------
    df : DataFrame.

    Returns
    -------
    float between 0.0 and 1.0.
    """
    numeric = df.select_dtypes(include="number")
    if numeric.empty or len(numeric) == 0:
        return 1.0
    total = numeric.size
    non_missing = int(numeric.notna().sum().sum())
    return non_missing / total if total > 0 else 1.0


CORE_MET_VARIABLES = (
    "air_temperature_c",
    "relative_humidity_pct",
    "wind_speed_kmh",
    "rain_mm",
)


FWI_CODES = ("ffmc", "dmc", "dc", "isi", "bui", "fwi")


def pre_imputation_missingness(df: pd.DataFrame) -> dict[str, float]:
    """Return missing percentage for the 4 core meteorological variables.

    Parameters
    ----------
    df : DataFrame (typically hourly, pre-imputation).

    Returns
    -------
    dict with keys ``missing_pct_{variable}`` for each core met variable.
    Values are float between 0.0 and 100.0.
    """
    n = len(df)
    result: dict[str, float] = {}
    for var in CORE_MET_VARIABLES:
        if var in df.columns:
            n_miss = int(df[var].isna().sum())
            result[f"missing_pct_{var}"] = (
                round(n_miss / n * 100, 2) if n > 0 else 0.0
            )
        else:
            result[f"missing_pct_{var}"] = 100.0
    return result


def fwi_descriptive_stats(
    daily: pd.DataFrame, station: str
) -> dict[str, float]:
    """Return descriptive statistics for FWI codes in a daily DataFrame.

    Parameters
    ----------
    daily : DataFrame with FWI columns and a 'station' column.
    station : Station name to filter on.

    Returns
    -------
    dict with keys ``{code}_{stat}`` for each FWI code × (min, max, mean, std).
    """
    station_daily = daily[daily["station"] == station] if "station" in daily.columns else daily
    result: dict[str, float] = {}
    for code in FWI_CODES:
        if code not in station_daily.columns:
            for stat in ("min", "max", "mean", "std"):
                result[f"{code}_{stat}"] = float("nan")
            continue
        series = station_daily[code].dropna()
        result[f"{code}_min"] = float(series.min()) if len(series) > 0 else float("nan")
        result[f"{code}_max"] = float(series.max()) if len(series) > 0 else float("nan")
        result[f"{code}_mean"] = float(series.mean()) if len(series) > 0 else float("nan")
        result[f"{code}_std"] = float(series.std(ddof=1)) if len(series) > 1 else 0.0
    return result


def generate_qa_qc_report(
    hourly: pd.DataFrame,
    daily: pd.DataFrame,
    quality_actions: list[dict] | None = None,
    chain_breaks: list | None = None,
    fwi_mode: str = "hourly",
    pre_imputation_missingness: dict[str, dict[str, float]] | dict[str, float] | None = None,
) -> pd.DataFrame:
    """Generate QA/QC report for all stations.

    Parameters
    ----------
    hourly : Combined hourly DataFrame with a 'station' column.
    daily : Combined daily DataFrame with a 'station' column.
    quality_actions : Optional list of quality enforcement action
        records (from ``enforce_quality`` and ``enforce_fwi_outputs``).
        Each record must contain at least 'station' and 'action' keys.
    chain_breaks : Optional list of chain break records.
    fwi_mode : FWI calculation mode ("hourly" or "compliant").
    pre_imputation_missingness_data : Optional dict of pre-imputation
        missing percentages from ``pre_imputation_missingness()``.

    Returns
    -------
    DataFrame with one row per station containing all QA/QC metrics
    plus Phase 13 additions: fwi_mode, carry_forward_days/pct,
    pre/post imputation columns, and FWI value statistics.
    """
    report_rows: list[dict] = []

    for station in hourly["station"].unique():
        station_hourly = hourly[hourly["station"] == station].copy()
        station_daily = daily[daily["station"] == station]

        # Missingness (post-imputation)
        miss = missingness_summary(station_hourly)
        miss_dict: dict[str, float] = {}
        for _, row in miss.iterrows():
            key = f"post_imp_missing_pct_{row['variable']}"
            miss_dict[key] = row["missing_pct"]

        # Pre-imputation missingness
        # Accepts either dict[str, dict[str, float]] (station → snapshot)
        # or legacy dict[str, float] (flat snapshot for a single station).
        pre_imp_dict: dict[str, float] = {}
        if pre_imputation_missingness is not None:
            station_snapshot = pre_imputation_missingness.get(
                station, pre_imputation_missingness
            )
            for var in CORE_MET_VARIABLES:
                key = f"pre_imp_missing_pct_{var}"
                pre_imp_dict[key] = station_snapshot.get(
                    f"missing_pct_{var}", 0.0
                )

        # Duplicates
        dups = duplicate_timestamps(station_hourly)
        dup_count = len(dups)

        # Out-of-range
        oor = out_of_range_values(station_hourly)
        oor_temp = (
            int((oor["oov_column"] == "air_temperature_c").sum())
            if len(oor) > 0
            else 0
        )
        oor_rh = (
            int((oor["oov_column"] == "relative_humidity_pct").sum())
            if len(oor) > 0
            else 0
        )
        oor_wind = (
            int(
                (oor["oov_column"].isin(
                    ["wind_speed_kmh", "wind_direction_deg"]
                )).sum()
            )
            if len(oor) > 0
            else 0
        )

        # Quality enforcement counts from action records
        enforced_count = 0
        flagged_count = 0
        if quality_actions:
            station_actions = [
                a for a in quality_actions
                if a.get("station") == station
            ]
            for act in station_actions:
                action_val = act.get("action", "")
                if action_val == "set_nan":
                    enforced_count += 1
                elif action_val in ("flag_only", "flagged"):
                    flagged_count += 1

        # Post-enforcement out-of-range: re-check enforced data.
        # After enforcement, values that were set_nan should no longer
        # appear as out-of-range. We compute the remaining OOR count
        # from the already-processed hourly data (post-enforcement).
        oor_post = oor_temp + oor_rh + oor_wind
        if quality_actions:
            # Count how many set_nan actions were for value_range checks
            # on the same variables — those values are now NaN and not OOR.
            station_set_nan = [
                a for a in quality_actions
                if a.get("station") == station
                and a.get("action") == "set_nan"
                and a.get("check_type") == "value_range"
            ]
            oor_post = max(0, oor_post - len(station_set_nan))

        # Date range
        if "timestamp_utc" in station_hourly.columns:
            ts = pd.to_datetime(station_hourly["timestamp_utc"], utc=True)
            date_start = ts.min()
            date_end = ts.max()
        else:
            date_start = pd.NaT
            date_end = pd.NaT

        # FWI value statistics
        fwi_stats = fwi_descriptive_stats(daily, station)

        # Compliant mode diagnostics
        carry_forward_days = 0
        carry_forward_pct = 0.0
        if fwi_mode == "compliant" and len(station_daily) > 0:
            if "carry_forward_used" in station_daily.columns:
                carry_forward_days = int(station_daily["carry_forward_used"].sum())
                total_days = len(station_daily)
                carry_forward_pct = (
                    round(carry_forward_days / total_days * 100, 2)
                    if total_days > 0
                    else 0.0
                )

        report_rows.append({
            "station": station,
            "hourly_rows": len(station_hourly),
            "daily_rows": len(station_daily),
            "date_range_start": date_start,
            "date_range_end": date_end,
            "completeness": round(calculate_completeness(station_hourly), 4),
            "fwi_mode": fwi_mode,
            **pre_imp_dict,
            **miss_dict,
            "duplicate_count": dup_count,
            "out_of_range_temp_count": oor_temp,
            "out_of_range_rh_count": oor_rh,
            "out_of_range_wind_count": oor_wind,
            "quality_enforced_count": enforced_count,
            "quality_flagged_count": flagged_count,
            "out_of_range_pre_enforcement": oor_temp + oor_rh + oor_wind,
            "out_of_range_post_enforcement": oor_post,
            "fwi_chain_breaks": sum(
                1 for b in (chain_breaks or [])
                if b.station == station
            ),
            "carry_forward_days": carry_forward_days,
            "carry_forward_pct": carry_forward_pct,
            **fwi_stats,
        })

    return pd.DataFrame(report_rows)
