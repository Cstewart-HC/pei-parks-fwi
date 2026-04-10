"""Licor Cloud API adapter — live weather for PEINP park stations.

Fetches recent hourly observations from Licor Cloud API for the 5 PEINP
weather stations.  Returns DataFrames matching the canonical schema used by
the FWI forecast pipeline (air_temperature_c, relative_humidity_pct,
wind_speed_kmh, rain_mm).

Authentication: Bearer token from HC_CS_PEIPCWX_PROD_RO env var.
Rate limiting: 2-second delay between requests (Licor API policy).
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

API_BASE = "https://api.licor.cloud/v2/data"
DEVICES_API = "https://api.licor.cloud/v2/devices"
AUTH_ENV_VAR = "HC_CS_PEIPCWX_PROD_RO"
REQUEST_DELAY = 2.0  # seconds between API calls (Licor policy)

# Path to station/device mapping (not tracked — lives under data/raw/licor/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DEVICES_FILE = PROJECT_ROOT / "data" / "raw" / "licor" / "devices.json"

# How far back to pull when seeding the FWI chain (in hours)
DEFAULT_LOOKBACK_HOURS = 6

# ---------------------------------------------------------------------------
# FWI-required sensors per variable
# ---------------------------------------------------------------------------

# stations missing RH but that have dew_point — we'll derive RH from temp+dp
# Canonical column → Licor API measurement type names.
# The API uses specific strings (e.g. "Average wind speed", "Dew Point")
# that differ from devices.json labels.
LICOR_TYPE_MAP = {
    "temperature": ["Temperature"],
    "rh": ["RH"],
    "dew_point": ["Dew Point"],
    "wind_speed": ["Wind Speed", "Average wind speed", "Average Wind Speed", "Avg Wind speed", "Avg wind speed"],
    "wind_gust": ["Wind gust speed", "Wind Gust Speed", "Gust Speed"],
    "rain": ["Rain"],
}

# Canonical output column → primary Licor measurement key
FWI_SENSORS = {
    "air_temperature_c": "temperature",
    "relative_humidity_pct": "rh",
    "wind_speed_kmh": "wind_speed",
    "rain_mm": "rain",
}

# Stations with no RH or dewpoint sensor — RH will be NaN from Licor,
# must be filled from another source (OWM) in the pipeline.
NO_RH_STATIONS = {"tracadie", "stanley_bridge"}


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------

def _get_token() -> str:
    token = os.environ.get(AUTH_ENV_VAR, "")
    if not token:
        raise EnvironmentError(
            f"{AUTH_ENV_VAR} env var not set. "
            "Add to Moltis vault or shell environment."
        )
    return token


def _api_get(url: str, token: str) -> dict[str, Any]:
    req = Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        raise RuntimeError(f"Licor API error {e.code}: {e.reason}") from e


def _load_devices() -> dict[str, Any]:
    """Load station-to-device mapping from devices.json."""
    if not DEVICES_FILE.exists():
        raise FileNotFoundError(
            f"{DEVICES_FILE} not found. Run scripts/licor_cache.py --sensors-only first."
        )
    return json.loads(DEVICES_FILE.read_text())


# ---------------------------------------------------------------------------
# Data extraction
# ---------------------------------------------------------------------------

def _extract_records(
    response: dict[str, Any],
    device_serial: str,
) -> list[dict[str, Any]]:
    """Extract sensor records from a Licor API response.

    Handles two response formats:
    - sensors is a list of sensor objects, each with data[] containing
      measurementType/units/records
    - sensors is a dict keyed by serial number
    """
    sensors_raw = response.get("sensors", [])
    all_sensors: list[dict] = []

    if isinstance(sensors_raw, list):
        for sensor in sensors_raw:
            serial = sensor["sensorSerialNumber"]
            for measurement in sensor.get("data", []):
                all_sensors.append({
                    "serial": serial,
                    "type": measurement["measurementType"],
                    "units": measurement["units"],
                    "records": measurement.get("records", []),
                })
    elif isinstance(sensors_raw, dict):
        for serial, sensor in sensors_raw.items():
            all_sensors.append({
                "serial": sensor["serialNumber"],
                "type": sensor["measurementType"],
                "units": sensor["units"],
                "records": sensor.get("records", []),
            })

    # Index by canonical key using the type map
    by_type: dict[str, list[tuple[int, float]]] = {}
    for s in all_sensors:
        mtype = s["type"]
        records = s["records"]
        if not records:
            continue
        # Find which canonical key this measurement type maps to
        for canonical_key, aliases in LICOR_TYPE_MAP.items():
            if mtype in aliases:
                by_type.setdefault(canonical_key, []).extend(records)
                break

    return by_type


def _derive_rh_from_dewpoint(
    temp_records: list[tuple[int, float]],
    dp_records: list[tuple[int, float]],
) -> list[tuple[int, float]]:
    """Derive relative humidity from temperature and dewpoint.

    Uses the Magnus formula:
        RH = 100 × exp((17.625 × Td) / (243.04 + Td)) / exp((17.625 × T) / (243.04 + T))

    Records are [timestamp_ms, value] tuples.
    """
    dp_map = {r[0]: r[1] for r in dp_records}
    result = []

    for ts_ms, temp_c in temp_records:
        if ts_ms not in dp_map:
            continue
        dp_c = dp_map[ts_ms]

        # Skip if either value is physically unreasonable
        if temp_c < -40 or dp_c < -40:
            continue

        es = 6.112 * (2.71828 ** ((17.625 * temp_c) / (243.04 + temp_c)))
        ed = 6.112 * (2.71828 ** ((17.625 * dp_c) / (243.04 + dp_c)))
        rh = min(100.0, max(0.0, 100.0 * ed / es))

        result.append((ts_ms, round(rh, 1)))

    return result


def _aggregate_to_hourly(
    by_type: dict[str, list[tuple[int, float]]],
    station_key: str,
) -> pd.DataFrame:
    """Resample ~2-5 minute Licor records to hourly averages.

    Rain is summed (accumulated over the hour), all others are averaged.
    Missing sensors are left as NaN.
    """
    if not by_type:
        return pd.DataFrame()

    # Build per-sensor series
    series: dict[str, pd.Series] = {}
    for mtype, records in by_type.items():
        if not records:
            continue
        ts_vals = [(datetime.fromtimestamp(ts / 1000, tz=timezone.utc), val)
                    for ts, val in records]
        ts_vals.sort(key=lambda x: x[0])
        idx, vals = zip(*ts_vals)
        series[mtype] = pd.Series(vals, index=pd.DatetimeIndex(idx, name="timestamp_utc"))

    # Combine into a DataFrame
    df = pd.DataFrame(series)
    df = df[~df.index.duplicated(keep="first")]

    # Derive RH from dewpoint if station has dew_point but no RH sensor
    has_rh = "rh" in df.columns
    has_dp = "dew_point" in df.columns
    if not has_rh and has_dp and "temperature" in df.columns:
        logger.info("  %s: deriving RH from temperature + dewpoint", station_key)
        rh_records = _derive_rh_from_dewpoint(
            [(int(t.timestamp() * 1000), v) for t, v in zip(df.index, df["temperature"])],
            [(int(t.timestamp() * 1000), v) for t, v in zip(df.index, df["dew_point"])],
        )
        if rh_records:
            rh_idx, rh_vals = zip(*[
                (datetime.fromtimestamp(ts / 1000, tz=timezone.utc), v)
                for ts, v in rh_records
            ])
            df["rh"] = pd.Series(rh_vals, index=pd.DatetimeIndex(rh_idx))
    elif not has_rh and not has_dp:
        if station_key in NO_RH_STATIONS:
            logger.info("  %s: no RH or dewpoint sensor, RH will be NaN", station_key)

    # Resample to hourly
    agg_rules: dict[str, str] = {}
    rename_map: dict[str, str] = {}

    # Map Licor measurement types to canonical names
    if "temperature" in df.columns:
        agg_rules["temperature"] = "mean"
        rename_map["temperature"] = "air_temperature_c"

    if "rh" in df.columns:
        agg_rules["rh"] = "mean"
        rename_map["rh"] = "relative_humidity_pct"

    if "wind_speed" in df.columns:
        # Licor wind_speed is in m/s — convert to km/h for FWI
        agg_rules["wind_speed"] = "mean"
        rename_map["wind_speed"] = "wind_speed_kmh_raw_ms"

    if "rain" in df.columns:
        agg_rules["rain"] = "sum"
        rename_map["rain"] = "rain_mm"

    if not agg_rules:
        return pd.DataFrame()

    hourly = df[list(agg_rules.keys())].resample("1h").agg(agg_rules)
    hourly = hourly.rename(columns=rename_map)

    # Convert wind speed m/s → km/h
    if "wind_speed_kmh_raw_ms" in hourly.columns:
        hourly["wind_speed_kmh"] = hourly["wind_speed_kmh_raw_ms"] * 3.6
        hourly = hourly.drop(columns=["wind_speed_kmh_raw_ms"])

    # Drop hours with no data at all
    hourly = hourly.dropna(how="all")

    return hourly


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class LicorAdapter:
    """Fetch live weather data from Licor Cloud for PEINP park stations.

    Usage::

        adapter = LicorAdapter()
        weather = adapter.fetch_recent(hours=6)
        # weather["cavendish"] -> DataFrame with hourly temp/RH/wind/rain
    """

    def __init__(self, token: str | None = None, delay: float = REQUEST_DELAY):
        self._token = token or _get_token()
        self._delay = delay
        self._devices = _load_devices()

    def fetch_recent(
        self,
        hours: int = DEFAULT_LOOKBACK_HOURS,
        stations: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        """Fetch recent observations for park stations.

        Args:
            hours: How many hours of history to pull (default 6).
            stations: Station keys to fetch (default: all 5 park stations).

        Returns:
            Dict mapping station key → hourly DataFrame with columns:
            air_temperature_c, relative_humidity_pct, wind_speed_kmh, rain_mm.
            Stations with missing sensors will have NaN in those columns.
        """
        if stations is None:
            stations = list(self._devices["stations"].keys())

        now = datetime.now(timezone.utc)
        start_ms = int((now - timedelta(hours=hours)).timestamp() * 1000)
        end_ms = int(now.timestamp() * 1000)

        results: dict[str, pd.DataFrame] = {}
        request_count = 0

        for station_key in stations:
            station_info = self._devices["stations"].get(station_key)
            if not station_info:
                logger.warning("Unknown station: %s, skipping", station_key)
                continue

            device_serial = station_info["device_serial"]
            url = (
                f"{API_BASE}?deviceSerialNumber={device_serial}"
                f"&startTime={start_ms}&endTime={end_ms}"
            )

            if self._delay and request_count > 0:
                time.sleep(self._delay)

            logger.info(
                "Licor: fetching %s (%s, last %dh)",
                station_key, device_serial, hours,
            )

            try:
                response = _api_get(url, self._token)
                request_count += 1
            except RuntimeError as e:
                logger.error("Licor: %s fetch failed: %s", station_key, e)
                continue

            by_type = _extract_records(response, device_serial)
            df = _aggregate_to_hourly(by_type, station_key)

            if df.empty:
                logger.warning("Licor: %s returned no usable data", station_key)
                continue

            # Warn about missing FWI-required columns
            required = {"air_temperature_c", "relative_humidity_pct",
                        "wind_speed_kmh", "rain_mm"}
            missing = required - set(df.columns)
            if missing:
                logger.warning(
                    "Licor: %s missing columns: %s (will be NaN)",
                    station_key, ", ".join(sorted(missing)),
                )

            results[station_key] = df
            logger.info(
                "Licor: %s → %d hourly rows, columns: %s",
                station_key, len(df), ", ".join(df.columns),
            )

        return results

    def fetch_station(
        self,
        station_key: str,
        hours: int = DEFAULT_LOOKBACK_HOURS,
    ) -> pd.DataFrame:
        """Fetch recent observations for a single station."""
        results = self.fetch_recent(hours=hours, stations=[station_key])
        return results.get(station_key, pd.DataFrame())

    # ------------------------------------------------------------------
    # Incremental cache refresh
    # ------------------------------------------------------------------

    def fetch_and_cache(self, device_serial: str, output_dir: Path | None = None) -> int:
        """Incremental fetch: pull data since last cached file for this device.

        Writes JSON files in the same format as licor_cache.py output.
        Only fetches data after the latest end-date found in existing cache.

        Args:
            device_serial: Device serial number (e.g., '21114839').
            output_dir: Directory for cached JSON files
                        (default: data/raw/licor/<device>/).

        Returns:
            Number of new JSON files written (chunks + combined).
        """
        import re

        if output_dir is None:
            output_dir = PROJECT_ROOT / "data" / "raw" / "licor" / device_serial
        output_dir.mkdir(parents=True, exist_ok=True)

        # --- Find latest end-date from existing cache files ---
        latest_end: datetime | None = None
        date_pattern = re.compile(
            r"(\d{4}-\d{2}-\d{2})[_ ](?:to )?(\d{4}-\d{2}-\d{2})"
        )
        for fpath in output_dir.glob("*.json"):
            m = date_pattern.search(fpath.name)
            if m:
                candidate = datetime.strptime(m.group(2), "%Y-%m-%d").replace(
                    tzinfo=timezone.utc
                )
                if latest_end is None or candidate > latest_end:
                    latest_end = candidate

        # Default start if no cache exists
        if latest_end is None:
            latest_end = datetime(2025, 11, 1, tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)
        start_date = latest_end + timedelta(days=1)

        # Nothing to fetch
        if start_date.date() >= now.date():
            logger.info("Licor cache: %s up to date (through %s)",
                        device_serial, latest_end.strftime("%Y-%m-%d"))
            return 0

        start_ms = int(start_date.timestamp() * 1000)
        end_ms = int(now.timestamp() * 1000)
        chunk_days = 7
        chunk_ms = chunk_days * 86400000

        logger.info(
            "Licor cache: fetching %s from %s to %s",
            device_serial,
            start_date.strftime("%Y-%m-%d"),
            now.strftime("%Y-%m-%d"),
        )

        # --- Fetch in weekly chunks ---
        all_chunks: list[dict] = []
        current_ms = start_ms
        files_written = 0
        request_count = 0

        while current_ms < end_ms:
            chunk_end_ms = min(current_ms + chunk_ms, end_ms)

            chunk_start_dt = datetime.fromtimestamp(
                current_ms / 1000, tz=timezone.utc
            )
            chunk_end_dt = datetime.fromtimestamp(
                chunk_end_ms / 1000, tz=timezone.utc
            )
            date_label = (
                f"{chunk_start_dt.strftime('%Y-%m-%d')}"
                f"_{chunk_end_dt.strftime('%Y-%m-%d')}"
            )

            url = (
                f"{API_BASE}?deviceSerialNumber={device_serial}"
                f"&startTime={current_ms}&endTime={chunk_end_ms}"
            )

            if self._delay and request_count > 0:
                time.sleep(self._delay)

            try:
                response = _api_get(url, self._token)
                request_count += 1
            except RuntimeError as e:
                logger.warning(
                    "Licor cache: chunk %s failed: %s", date_label, e
                )
                break  # stop fetching, keep what we have

            total_records = sum(
                s.get("totalRecords", 0) for s in response.get("sensors", [])
            )
            logger.info(
                "Licor cache: %s → %d records", date_label, total_records
            )

            # Save chunk file (raw API format: sensors as list)
            chunk_path = output_dir / f"{date_label}.json"
            with open(chunk_path, "w") as f:
                json.dump(response, f, indent=2)
            files_written += 1
            all_chunks.append(response)

            current_ms = chunk_end_ms

        if not all_chunks:
            return 0

        # --- Build combined file (sensors keyed by serial) ---
        merged_sensors: dict[str, dict] = {}
        for chunk in all_chunks:
            for sensor in chunk.get("sensors", []):
                sid = sensor["sensorSerialNumber"]
                data_entries = sensor.get("data", [])
                if not data_entries:
                    continue
                mtype = data_entries[0].get("measurementType", "unknown")
                units = data_entries[0].get("units", "unknown")
                records: list = []
                for entry in data_entries:
                    records.extend(entry.get("records", []))

                if sid not in merged_sensors:
                    merged_sensors[sid] = {
                        "sensorSerialNumber": sid,
                        "measurementType": mtype,
                        "units": units,
                        "totalRecords": 0,
                        "records": [],
                    }
                merged_sensors[sid]["records"].extend(records)

        # Deduplicate by timestamp and sort
        for sid in merged_sensors:
            seen: set[int] = set()
            deduped: list = []
            for rec in merged_sensors[sid]["records"]:
                ts = rec[0]
                if ts not in seen:
                    seen.add(ts)
                    deduped.append(rec)
            merged_sensors[sid]["records"] = sorted(deduped, key=lambda x: x[0])
            merged_sensors[sid]["totalRecords"] = len(merged_sensors[sid]["records"])

        start_label = start_date.strftime("%Y-%m-%d")
        end_label = now.strftime("%Y-%m-%d")
        combined = {
            "source": "licor_adapter.fetch_and_cache",
            "device": device_serial,
            "start": start_label,
            "end": end_label,
            "chunks": len(all_chunks),
            "errors": 0,
            "fetchTime": now.isoformat(),
            "sensors": merged_sensors,
        }
        combined_path = output_dir / f"{start_label}_{end_label}_combined.json"
        with open(combined_path, "w") as f:
            json.dump(combined, f, indent=2)
        files_written += 1

        logger.info(
            "Licor cache: %s → %d chunk(s) + combined saved",
            device_serial, len(all_chunks),
        )
        return files_written
