from __future__ import annotations

from pathlib import Path

from pea_met_network.normalized_loader import load_normalized_station_csv
from pea_met_network.resampling import resample_daily, resample_hourly


def materialize_resampled_outputs(
    source_path: Path,
    station: str,
    output_dir: Path,
) -> tuple[Path, Path]:
    normalized = load_normalized_station_csv(source_path, station=station)
    hourly = resample_hourly(normalized)
    daily = resample_daily(normalized)

    output_dir.mkdir(parents=True, exist_ok=True)
    # Use source stem to avoid clobbering station_hourly.csv / station_daily.csv
    # which are written by the final combined pipeline step in cleaning.py.
    stem = source_path.stem
    hourly_path = output_dir / f'{stem}_hourly.csv'
    daily_path = output_dir / f'{stem}_daily.csv'
    hourly.to_csv(hourly_path, index=False)
    daily.to_csv(daily_path, index=False)
    return hourly_path, daily_path
