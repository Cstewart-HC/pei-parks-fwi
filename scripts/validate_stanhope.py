"""Stanhope Validation — compare PEINP stations vs ECCC Stanhope reference.

Imports from src.pea_met_network.validation:
    validate_against_reference, compare_station_data

Usage:
    python scripts/validate_stanhope.py
    -> writes data/processed/stanhope_validation.csv
"""

from pathlib import Path

import pandas as pd

from pea_met_network.validation import validate_against_reference

PROJECT_ROOT = Path(__file__).parent.parent
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"

LOCAL_STATIONS = [
    "greenwich",
    "cavendish",
    "north_rustico",
    "stanley_bridge",
    "tracadie",
]


def load_station_daily(station: str) -> pd.DataFrame:
    """Load consolidated daily CSV for a station."""
    path = DATA_PROCESSED / station / "station_daily.csv"
    if not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True).dt.date
    return df


def run_validation() -> pd.DataFrame:
    """Run validation for all local stations against Stanhope."""
    stanhope_df = load_station_daily("stanhope")
    if stanhope_df.empty:
        raise FileNotFoundError("Stanhope daily CSV not found or empty")

    results = []
    for station in LOCAL_STATIONS:
        station_df = load_station_daily(station)
        result = validate_against_reference(
            station, stanhope_df, station_df
        )
        results.append(result)

    df = pd.DataFrame(results)

    required_cols = [
        "station",
        "overlap_days",
        "mean_abs_diff_ffmc",
        "mean_abs_diff_dmc",
        "mean_abs_diff_dc",
        "mean_abs_diff_fwi",
    ]
    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    df = df[required_cols]
    return df


def main():
    """Generate stanhope_validation.csv."""
    df = run_validation()
    output_path = DATA_PROCESSED / "stanhope_validation.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Wrote {len(df)} station comparisons to {output_path}")
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()
