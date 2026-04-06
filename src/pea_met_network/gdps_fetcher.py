"""GDPS Forecast Fetcher — Environment Canada Global Deterministic Prediction System.

Fetches 10-day weather forecasts from the MSC GeoMet WMS GetFeatureInfo service.
No API key required, no GRIB2 parsing, no external dependencies beyond ``requests``.

Variables fetched:
  - Air temperature at 2m (GDPS.ETA_TT) [°C]
  - Relative humidity (GDPS.ETA_HR) [%]
  - Wind speed at 10m (GDPS.ETA_WSPD) [m/s]
  - Rain accumulation (GDPS.ETA_RN) [mm] — total since forecast start, differenced to per-period

Strategy:
  - Uses a single bounding box covering all PEINP stations (~30 km span)
  - 4 concurrent requests per timestep (one per variable) → ~32 s for full 10-day fetch
  - Rate-limited to 1 req/sec per worker (4 req/sec total) — respectful of ECCC CDN
  - Caches per model run (00Z / 12Z) — most subsequent runs hit cache instantly
  - ETA_RN is total accumulation; consecutive values are differenced for per-period rain

Usage:
    from pea_met_network.gdps_fetcher import GDPSFetcher

    gdps = GDPSFetcher()
    weather = gdps.fetch(stations)  # dict[station_name, DataFrame]
"""

from __future__ import annotations

import json
import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

WMS_BASE = "https://geo.weather.gc.ca/geomet"

# Variable layers
LAYERS = {
    "temp": "GDPS.ETA_TT",    # Air temperature at 2m [°C]
    "rh": "GDPS.ETA_HR",      # Relative humidity [%]
    "wind": "GDPS.ETA_WSPD",  # Wind speed at 10m [m/s]
    "rain": "GDPS.ETA_RN",    # Rain accumulation [mm] (total since forecast start)
}

# GDPS timestep cadence
TIMESTEP_HOURS = 3

# Concurrency and rate limiting
MAX_WORKERS = 4  # one per variable
MIN_REQUEST_INTERVAL = 1.0  # seconds between requests per worker

# Cache
CACHE_DIR = Path(__file__).resolve().parent.parent.parent / "data" / "gdps_cache"
CACHE_TTL_HOURS = 7  # re-fetch after 7h (runs are every 12h)


@dataclass(frozen=True)
class Station:
    name: str
    lat: float
    lon: float


# ---------------------------------------------------------------------------
# Bounding box
# ---------------------------------------------------------------------------

# All PEINP stations fit within this box.
# Grid resolution is 0.15° (~15 km), so a single GetFeatureInfo query
# returns the nearest grid cell — good enough for forecast-grade FWI.
PEINP_BBOX = "-63.54,46.23,-62.97,46.61"


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

def _cache_filename(run_time: datetime) -> str:
    return f"gdps_{run_time.strftime('%Y%m%dT%H')}.json"


def _load_cache(run_time: datetime) -> dict[str, Any] | None:
    path = CACHE_DIR / _cache_filename(run_time)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Cache read error: %s", e)
        return None

    ts = datetime.fromisoformat(data["_fetched_at"])
    age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
    if age_hours > CACHE_TTL_HOURS:
        logger.info("Cache stale (%.1f h), will re-fetch", age_hours)
        return None

    logger.info("Using cached GDPS %s (%.1f h old)", run_time.strftime("%Y-%m-%d %HZ"), age_hours)
    return data


def _save_cache(run_time: datetime, data: dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / _cache_filename(run_time)
    data["_fetched_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(data))
    logger.info("Cached → %s", path.name)


def _clean_old_cache() -> None:
    if not CACHE_DIR.exists():
        return
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    for p in CACHE_DIR.glob("gdps_*.json"):
        try:
            ts = datetime.fromisoformat(json.loads(p.read_text()).get("_fetched_at", ""))
            if ts < cutoff:
                p.unlink()
                logger.debug("Purged old cache: %s", p.name)
        except (json.JSONDecodeError, OSError, ValueError):
            pass


# ---------------------------------------------------------------------------
# GDPS metadata discovery
# ---------------------------------------------------------------------------

def _discover_latest_run() -> datetime:
    """Find the most recent available GDPS 00Z or 12Z model run.

    Looks at reference_time dimension values and picks the latest one
    that is in the past (not a future run).
    """
    url = f"{WMS_BASE}?service=WMS&version=1.3.0&request=GetCapabilities&layer=GDPS.ETA_TT"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        # reference_time contains actual run times (00Z and 12Z)
        # time dimension contains forecast timesteps — we don't want those
        ref_section = resp.text.split('reference_time')[1].split('>')[0] if 'reference_time' in resp.text else ""
        # Extract all timestamps from the reference_time section
        timestamps = re.findall(r"(\d{4}-\d{2}-\d{2}T[01]\d:00:00Z)", ref_section)
        if not timestamps:
            # Fallback: grab ALL timestamps and filter to recent past
            timestamps = re.findall(r"(\d{4}-\d{2}-\d{2}T[01]\d:00:00Z)", resp.text)

        now = datetime.now(timezone.utc)
        valid = []
        for t_str in timestamps:
            if not (t_str.endswith("T00:00:00Z") or t_str.endswith("T12:00:00Z")):
                continue
            t = datetime.fromisoformat(t_str)
            # Only consider runs that are in the past (with 1h buffer for clock skew)
            if t <= now + timedelta(hours=1):
                valid.append(t)

        if valid:
            run = max(valid)
            logger.info("GDPS run: %s", run.strftime("%Y-%m-%d %HZ"))
            return run
    except Exception as e:
        logger.warning("Run discovery failed: %s", e)

    now = datetime.now(timezone.utc)
    hour = 0 if now.hour < 12 else 12
    run = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    logger.info("Estimated GDPS run: %s (fallback)", run.strftime("%Y-%m-%d %HZ"))
    return run


def _fetch_timesteps(run_time: datetime) -> list[datetime]:
    """Get available forecast timesteps from WMS capabilities."""
    url = f"{WMS_BASE}?service=WMS&version=1.3.0&request=GetCapabilities&layer=GDPS.ETA_TT"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        match = re.search(
            r'name="time"[^>]*>(\d{4}-\d{2}-\d{2}T[^/]+)/([^/]+)/(PT(\d+)H)',
            resp.text,
        )
        if match:
            start = datetime.fromisoformat(match.group(1))
            end = datetime.fromisoformat(match.group(2))
            period = int(match.group(4))
            steps = []
            t = start
            while t <= end:
                steps.append(t)
                t += timedelta(hours=period)
            return steps
    except Exception as e:
        logger.warning("Timestep discovery failed: %s", e)

    return [run_time + timedelta(hours=h) for h in range(0, 241, TIMESTEP_HOURS)]


# ---------------------------------------------------------------------------
# WMS fetch
# ---------------------------------------------------------------------------

def _wms_point_query(layer: str, time_str: str, bbox: str = PEINP_BBOX) -> float | None:
    """Single WMS GetFeatureInfo call. Returns the grid-point value."""
    url = (
        f"{WMS_BASE}?service=WMS&version=1.3.0&request=GetFeatureInfo"
        f"&info_format=application/json&CRS=CRS:84"
        f"&BBOX={bbox}&WIDTH=5&HEIGHT=5&X=2&Y=2"
        f"&layers={layer}&query_layers={layer}"
        f"&time={time_str}"
    )
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features", [])
    if not features:
        return None
    return features[0]["properties"]["value"]


# ---------------------------------------------------------------------------
# GDPS Fetcher
# ---------------------------------------------------------------------------

class GDPSFetcher:
    """Fetches GDPS weather data for PEINP stations.

    Caches aggressively — most calls hit local disk, not the network.
    A fresh fetch of the full 10-day forecast takes ~32 seconds.
    """

    def __init__(
        self,
        max_workers: int = MAX_WORKERS,
        rate_limit: float = MIN_REQUEST_INTERVAL,
        bbox: str = PEINP_BBOX,
    ):
        self.max_workers = max_workers
        self.rate_limit = rate_limit
        self.bbox = bbox
        self._last_request_times: dict[str, float] = {}

    def _throttled_query(self, worker_id: str, layer: str, time_str: str) -> float | None:
        """Rate-limited WMS query per worker."""
        last = self._last_request_times.get(worker_id, 0.0)
        wait = self.rate_limit - (time.time() - last)
        if wait > 0:
            time.sleep(wait)
        self._last_request_times[worker_id] = time.time()

        try:
            return _wms_point_query(layer, time_str, self.bbox)
        except Exception as e:
            logger.warning("GDPS %s at %s: %s", layer, time_str[:16], e)
            return None

    def fetch(
        self,
        stations: list[Station],
        max_hours: int = 240,
    ) -> dict[str, pd.DataFrame]:
        """Fetch GDPS weather for all stations.

        Args:
            stations: Station list (used for DataFrame keys; bbox covers all).
            max_hours: Maximum forecast lead time (default 240 = 10 days).

        Returns:
            Dict mapping station name → DataFrame with columns:
            timestamp_utc, air_temperature_c, relative_humidity_pct,
            wind_speed_kmh, rain_mm
        """
        run_time = _discover_latest_run()

        # Try cache
        cached = _load_cache(run_time)
        if cached is not None:
            return self._from_cache(cached, [s.name for s in stations])

        # Get timesteps
        all_steps = _fetch_timesteps(run_time)
        steps = [t for t in all_steps if (t - run_time).total_seconds() / 3600 <= max_hours]
        logger.info(
            "Fetching GDPS: %s, %d timesteps, %d workers",
            run_time.strftime("%Y-%m-%d %HZ"), len(steps), self.max_workers,
        )

        # Fetch all vars × all timesteps concurrently
        # Results: {var: {time_str: value}}
        results: dict[str, dict[str, float | None]] = {v: {} for v in LAYERS}

        total = len(LAYERS) * len(steps)
        done = 0

        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {}
            for var_name, layer in LAYERS.items():
                for ts in steps:
                    time_str = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
                    future = pool.submit(
                        self._throttled_query, var_name, layer, time_str,
                    )
                    futures[future] = (var_name, time_str)

            for future in as_completed(futures):
                var_name, time_str = futures[future]
                try:
                    results[var_name][time_str] = future.result()
                except Exception:
                    results[var_name][time_str] = None
                done += 1
                if done % 40 == 0 or done == total:
                    logger.info("  Progress: %d/%d", done, total)

        # Build DataFrame
        rows = []
        for ts in steps:
            time_str = ts.strftime("%Y-%m-%dT%H:%M:%SZ")
            temp = results["temp"].get(time_str)
            rh = results["rh"].get(time_str)
            wind_ms = results["wind"].get(time_str)
            rain_total = results["rain"].get(time_str)

            if any(v is None for v in (temp, rh, wind_ms, rain_total)):
                continue

            rows.append({
                "timestamp_utc": ts,
                "air_temperature_c": float(temp),
                "relative_humidity_pct": float(rh),
                "wind_speed_kmh": float(wind_ms) * 3.6,
                "rain_mm_total": float(rain_total),
            })

        if not rows:
            raise RuntimeError("GDPS fetch returned no data")

        df = pd.DataFrame(rows).set_index("timestamp_utc").sort_index()

        # Difference total rain to per-period
        df["rain_mm"] = df["rain_mm_total"].diff().fillna(0.0).clip(lower=0.0)
        df = df.drop(columns=["rain_mm_total"])

        # Cache
        station_names = [s.name for s in stations]
        cache_df = df.copy()
        cache_df["timestamp_utc"] = cache_df.index.strftime("%Y-%m-%dT%H:%M:%SZ")
        cache_payload = {
            "run_time": run_time.isoformat(),
            "weather": {
                name: cache_df.to_dict(orient="records") for name in station_names
            },
        }
        _save_cache(run_time, cache_payload)
        _clean_old_cache()

        logger.info(
            "GDPS fetch complete: %d timesteps, T[%.1f, %.1f]°C, RH[%.0f, %.0f]%%",
            len(df),
            df["air_temperature_c"].min(), df["air_temperature_c"].max(),
            df["relative_humidity_pct"].min(), df["relative_humidity_pct"].max(),
        )

        return {s.name: df.copy() for s in stations}

    @staticmethod
    def _from_cache(
        cached: dict[str, Any],
        station_names: list[str],
    ) -> dict[str, pd.DataFrame]:
        weather = cached.get("weather", {})
        results = {}
        for name in station_names:
            if name not in weather:
                logger.warning("Station %s not in cache", name)
                continue
            df = pd.DataFrame(weather[name])
            df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
            df = df.set_index("timestamp_utc").sort_index()
            results[name] = df
            logger.info("%s (cached): %d timesteps", name, len(df))
        return results
