"""Stanhope Validation — compare PEINP stations vs ECCC Stanhope reference.

Provides:
    validate_against_reference(station, stanhope_df, station_df)
    compare_station_data(station, stanhope_df, station_df)
"""

import pandas as pd

FWI_COLS = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]


def compare_station_data(
    station: str,
    stanhope_df: pd.DataFrame,
    station_df: pd.DataFrame,
) -> dict:
    """Compare data between a local station and Stanhope reference.

    Finds temporal overlap and computes mean absolute differences for
    shared FWI variables. Returns dict with overlap_days and MAE.
    """
    if stanhope_df.empty or station_df.empty:
        return {"station": station, "overlap_days": 0}

    merged = pd.merge(
        stanhope_df[["timestamp_utc"] + FWI_COLS],
        station_df[["timestamp_utc"] + FWI_COLS],
        on="timestamp_utc",
        how="inner",
        suffixes=("_stanhope", "_local"),
    )

    overlap_days = len(merged)
    if overlap_days == 0:
        return {"station": station, "overlap_days": 0}

    result = {"station": station, "overlap_days": overlap_days}

    for col in FWI_COLS:
        s_col = f"{col}_stanhope"
        l_col = f"{col}_local"
        if s_col in merged.columns and l_col in merged.columns:
            valid = merged[[s_col, l_col]].dropna()
            if len(valid) > 0:
                mae = (valid[s_col] - valid[l_col]).abs().mean()
                result[f"mean_abs_diff_{col}"] = round(float(mae), 4)
            else:
                result[f"mean_abs_diff_{col}"] = None
        else:
            result[f"mean_abs_diff_{col}"] = None

    return result


def validate_against_reference(
    station: str,
    stanhope_df: pd.DataFrame,
    station_df: pd.DataFrame,
) -> dict:
    """Validate a local station against Stanhope ECCC reference.

    Computes FWI metric comparisons. This is the primary validation
    function called by the pipeline.
    """
    return compare_station_data(station, stanhope_df, station_df)
