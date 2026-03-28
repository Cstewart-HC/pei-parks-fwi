"""ECCC MSC GeoMet API client for fetching hourly climate data.

Fetches from the climate-hourly OGC API collection.
Results cached as Parquet files for one-time fetch.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests


@dataclass(frozen=True)
class EcccStation:
    """Metadata for an ECCC donor station."""

    climate_id: str  # e.g. "8300562"
    stn_id: int  # API numeric ID, e.g. 41903
    name: str  # e.g. "St. Peters"
    local_tz: str  # e.g. "America/Halifax"
    anemometer_height_m: float  # default 10.0 (WMO standard)


def _station_key(station: EcccStation) -> str:
    """Derive a filesystem-safe key from station name.

    Maps to the canonical key used in cleaning-config.json eccc_stations.
    """
    return station.name.lower().replace(" ", "_").replace(".", "")



ECCC_DONOR_STATIONS: dict[str, EcccStation] = {
    "st_peters": EcccStation(
        "8300562", 41903, "St. Peters", "America/Halifax", 10.0
    ),
    "charlottetown_a": EcccStation(
        "8300300", 6526, "Charlottetown A", "America/Halifax", 10.0
    ),
    "harrington_cda": EcccStation(
        "830P001", 30308, "Harrington CDA CS", "America/Halifax", 10.0
    ),
}

# Map config keys to filesystem-safe cache directory names
ECCC_CACHE_KEY_MAP: dict[str, str] = {
    key: _station_key(stn) for key, stn in ECCC_DONOR_STATIONS.items()
}

_BASE_URL = "https://api.weather.gc.ca/collections/climate-hourly/items"


def normalize_eccc_response(
    features: list[dict[str, Any]],
    station_name: str,
    local_tz: str,
) -> pd.DataFrame:
    """Convert OGC API GeoJSON features to normalized DataFrame.

    Returns DataFrame with canonical columns:
        timestamp_utc, air_temperature_c, relative_humidity_pct,
        wind_speed_kmh, wind_direction_deg, rain_mm, dew_point_c
    """
    if not features:
        return pd.DataFrame(
            columns=[
                "timestamp_utc",
                "air_temperature_c",
                "relative_humidity_pct",
                "wind_speed_kmh",
                "wind_direction_deg",
                "rain_mm",
                "dew_point_c",
            ]
        )

    rows: list[dict[str, Any]] = []
    for feat in features:
        props = feat.get("properties", {})
        obs_ts = props.get("OBSERVATION_DATE") or props.get("UTC_DATE")
        if not obs_ts:
            continue

        # Parse timestamp and convert to UTC
        try:
            if obs_ts.endswith("Z"):
                ts = pd.Timestamp(obs_ts, tz="UTC")
            else:
                ts = pd.Timestamp(obs_ts, tz=local_tz).tz_convert("UTC")
        except Exception:
            continue

        rows.append(
            {
                "timestamp_utc": ts,
                "air_temperature_c": _safe_float(props.get("TEMP")),
                "relative_humidity_pct": _safe_float(
                    props.get("RELATIVE_HUMIDITY")
                ),
                "wind_speed_kmh": _safe_float(props.get("WIND_SPEED")),
                "wind_direction_deg": _safe_float(props.get("WIND_DIRECTION")),
                "rain_mm": _safe_float(props.get("PRECIP_AMOUNT")),
                "dew_point_c": _safe_float(props.get("DEW_POINT_TEMP")),
            }
        )

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset="timestamp_utc").sort_values("timestamp_utc").reset_index(drop=True)
    return df


def fetch_eccc_hourly(
    station: EcccStation,
    start: datetime | pd.Timestamp,
    end: datetime | pd.Timestamp,
    *,
    limit: int = 10_000,
    cache_dir: Path | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """Fetch hourly observations from MSC GeoMet climate-hourly collection.

    Returns DataFrame with columns:
        timestamp_utc, air_temperature_c, relative_humidity_pct,
        wind_speed_kmh, wind_direction_deg, rain_mm, dew_point_c

    All numeric columns use errors='coerce'.
    Timestamps converted to UTC.
    Results cached locally as Parquet files.
    """
    # Normalize cache_dir to station key
    station_key = _station_key(station)
    cache_file: Path | None = None
    if cache_dir is not None:
        cache_dir = Path(cache_dir)
        cache_file = cache_dir / station_key / f"{station_key}.csv"
        if cache_file.exists() and not force:
            return pd.read_csv(cache_file, parse_dates=["timestamp_utc"])

    # Build API request
    start_str = pd.Timestamp(start).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = pd.Timestamp(end).strftime("%Y-%m-%dT%H:%M:%SZ")

    all_features: list[dict[str, Any]] = []
    datetime_range = f"{start_str}/{end_str}"
    url = (
        f"{_BASE_URL}"
        f"?CLIMATE_IDENTIFIER={station.climate_id}"
        f"&datetime={datetime_range}"
        f"&limit={limit}"
    )

    while url:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        features = data.get("features", [])
        all_features.extend(features)

        # Follow next link for pagination
        links = data.get("links", [])
        url = None
        for link in links:
            if (
                link.get("rel") == "next"
                and link.get("type") == "application/geo+json"
            ):
                url = link["href"]
                break

        # Rate limiting
        if url:
            time.sleep(1.0)

    df = normalize_eccc_response(all_features, station.name, station.local_tz)

    # Cache result
    if cache_file is not None:
        cache_file = cache_file.with_suffix(".csv")
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(cache_file, index=False)
        # Write provenance
        prov_path = cache_file.parent / "provenance.json"
        prov = {
            "station": station.name,
            "climate_id": station.climate_id,
            "date_range": f"{start_str}/{end_str}",
            "retrieved_at": pd.Timestamp.now(tz="UTC").isoformat(),
            "row_count": len(df),
        }
        prov_path.write_text(json.dumps(prov, indent=2))

    return df


def _safe_float(value: Any) -> float | None:
    """Convert value to float, returning None for missing/invalid."""
    if value is None:
        return None
    try:
        v = float(value)
        if np.isnan(v):
            return None
        return v
    except (ValueError, TypeError):
        return None
