"""FWI Forecast Pipeline — direct OWM fetch per station, compute FWI.

Fetches OWM One Call 3.0 for each station independently (6 API calls),
then computes hourly FWI from the raw grid-interpolated weather data.

No OLS translation needed — OWM resolves distinct values at each coordinate.

Usage:
    python -m pea_met_network.fwi_forecast
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import urllib.request
import urllib.error

from pea_met_network import fwi as fwi_calc

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Station metadata
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Station:
    name: str
    lat: float
    lon: float

STANHOPE = Station("stanhope", 46.38, -63.12)

PARK_STATIONS = [
    Station("cavendish", 46.4614, -63.3917),
    Station("greenwich", 46.4367, -63.2703),
    Station("north_rustico", 46.4508, -63.3306),
    Station("stanley_bridge", 46.4272, -63.2000),
    Station("tracadie", 46.4089, -63.1483),
]

ALL_STATIONS = [STANHOPE] + PARK_STATIONS

# Default paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent

# ---------------------------------------------------------------------------
# OWM One Call 3.0 fetch
# ---------------------------------------------------------------------------

OWM_BASE = "https://api.openweathermap.org/data/3.0/onecall"


def fetch_forecast(station: Station) -> dict[str, Any]:
    """Fetch hourly forecast from OWM One Call 3.0 for a single station."""
    key = os.environ.get("openweather_key")
    if not key:
        raise EnvironmentError("openweather_key env var not set. Add to Moltis vault.")

    url = (
        f"{OWM_BASE}?lat={station.lat}&lon={station.lon}"
        f"&appid={key}&units=metric"
    )
    logger.info("Fetching OWM for %s (%.4f, %.4f)", station.name, station.lat, station.lon)

    try:
        resp = urllib.request.urlopen(url, timeout=15)
    except urllib.error.HTTPError as e:
        if e.code == 401:
            raise RuntimeError("OWM 401 — check openweather_key and One Call subscription status")
        raise

    data = json.loads(resp.read())
    logger.info("  %d hourly entries, tz %s", len(data.get("hourly", [])), data.get("timezone"))
    return data


def fetch_all_stations(stations: list[Station] | None = None) -> dict[str, pd.DataFrame]:
    """Fetch OWM for multiple stations and return parsed weather DataFrames.

    Returns:
        Dict mapping station name → DataFrame with FWI input columns.
    """
    if stations is None:
        stations = ALL_STATIONS

    weather = {}
    for stn in stations:
        data = fetch_forecast(stn)
        weather[stn.name] = parse_hourly_weather(data)
    return weather


# ---------------------------------------------------------------------------
# OWM response → weather DataFrame
# ---------------------------------------------------------------------------

def parse_hourly_weather(data: dict[str, Any]) -> pd.DataFrame:
    """Convert OWM hourly response into a clean DataFrame.

    Columns: air_temperature_c, relative_humidity_pct, wind_speed_kmh, rain_mm
    Index: timestamp_utc
    """
    rows = []
    for h in data["hourly"]:
        ts = datetime.fromtimestamp(h["dt"], tz=timezone.utc)
        rain = h.get("rain")
        rain_mm = rain.get("1h", 0.0) if isinstance(rain, dict) else 0.0

        rows.append({
            "timestamp_utc": ts,
            "air_temperature_c": h["temp"],
            "relative_humidity_pct": h["humidity"],
            "wind_speed_kmh": h["wind_speed"] * 3.6,  # m/s → km/h
            "rain_mm": rain_mm,
        })

    df = pd.DataFrame(rows)
    df = df.set_index("timestamp_utc").sort_index()
    return df


# ---------------------------------------------------------------------------
# FWI computation over hourly series
# ---------------------------------------------------------------------------

def compute_fwi_series(
    weather: pd.DataFrame,
    station: Station,
    ffmc0: float = 85.0,
    dmc0: float = 6.0,
    dc0: float = 15.0,
) -> pd.DataFrame:
    """Compute FWI components for each hour in the weather DataFrame.

    Uses hourly FFMC (Van Wagner hourly equation) and daily aggregates
    for DMC/DC.  Daily aggregation uses the warmest hour's temp/RH and
    accumulated total rain per local calendar day.

    Args:
        weather: DataFrame with temp, RH, wind, rain columns, UTC timestamp index.
        station: Station with lat for DMC/DC calculations.
        ffmc0, dmc0, dc0: Startup indices (use yesterday's values or defaults).

    Returns:
        DataFrame with FFMC, DMC, DC, ISI, BUI, FWI columns.
    """
    weather = weather.copy()
    weather["month"] = weather.index.month

    # --- Pre-compute local date for each row ---
    def _local_date(ts: pd.Timestamp, month: int) -> "datetime.date":
        offset = 3 if month in (4, 5, 6, 7, 8, 9, 10) else 4
        return (ts.tz_convert(None) - pd.Timedelta(hours=offset)).date()

    local_dates = [
        _local_date(ts, row["month"]) for ts, row in weather.iterrows()
    ]
    weather["local_date"] = local_dates

    # --- Aggregate daily values: max temp at that temp's RH, total rain ---
    daily_agg: dict["datetime.date", dict] = {}
    for _, row in weather.iterrows():
        ld = row["local_date"]
        t, rh, r = row["air_temperature_c"], row["relative_humidity_pct"], row["rain_mm"]
        if ld not in daily_agg:
            daily_agg[ld] = {"temp": t, "rh": rh, "rain": r, "month": int(row["month"])}
        else:
            db = daily_agg[ld]
            db["rain"] += r
            if t > db["temp"]:
                db["temp"] = t
                db["rh"] = rh

    # --- Pre-compute DMC/DC chain per local date ---
    daily_codes: dict["datetime.date", tuple[float, float]] = {}
    cur_dmc, cur_dc = dmc0, dc0
    for ld in sorted(daily_agg.keys()):
        db = daily_agg[ld]
        if db["temp"] > 0:
            cur_dmc = fwi_calc.duff_moisture_code(
                temp=db["temp"], rh=db["rh"], rain=db["rain"], dmc0=cur_dmc,
                month=db["month"], lat=station.lat,
            )
            cur_dc = fwi_calc.drought_code(
                temp=db["temp"], rh=db["rh"], rain=db["rain"], dc0=cur_dc,
                month=db["month"], lat=station.lat,
            )
        daily_codes[ld] = (cur_dmc, cur_dc)

    # --- Pass 2: compute hourly FWI ---
    ffmc = ffmc0
    results = []

    for ts, row in weather.iterrows():
        temp = row["air_temperature_c"]
        rh = row["relative_humidity_pct"]
        wind = row["wind_speed_kmh"]
        rain = row["rain_mm"]
        ld = row["local_date"]
        dmc, dc = daily_codes[ld]

        # Hourly FFMC
        ffmc = fwi_calc.hourly_fine_fuel_moisture_code(
            temp=temp, rh=rh, wind=wind, rain=rain, ffmc0=ffmc
        )

        isi = fwi_calc.initial_spread_index(ffmc=ffmc, wind=wind)
        bui = fwi_calc.buildup_index(dmc=dmc, dc=dc)
        fwi_val = fwi_calc.fire_weather_index(isi=isi, bui=bui)

        results.append({
            "timestamp_utc": ts,
            "temp": temp,
            "rh": rh,
            "wind": wind,
            "rain": rain,
            "FFMC": ffmc,
            "DMC": dmc,
            "DC": dc,
            "ISI": isi,
            "BUI": bui,
            "FWI": fwi_val,
        })

    return pd.DataFrame(results).set_index("timestamp_utc")


# ---------------------------------------------------------------------------
# End-to-end pipeline
# ---------------------------------------------------------------------------

def run_forecast(
    stations: list[Station] | None = None,
    ffmc0: float = 85.0,
    dmc0: float = 6.0,
    dc0: float = 15.0,
) -> dict[str, pd.DataFrame]:
    """Run the full FWI forecast pipeline.

    Fetches OWM directly for each station → computes FWI per station.

    Returns:
        Dict mapping station name → DataFrame with FWI components per hour.
    """
    if stations is None:
        stations = ALL_STATIONS

    # 1. Fetch all stations
    weather_data = fetch_all_stations(stations)

    # 2. Compute FWI per station
    results = {}
    for stn in stations:
        fwi_df = compute_fwi_series(weather_data[stn.name], stn, ffmc0=ffmc0, dmc0=dmc0, dc0=dc0)
        results[stn.name] = fwi_df
        logger.info(
            "%s: FWI [%.1f, %.1f], max ISI %.1f, max FFMC %.1f",
            stn.name,
            fwi_df["FWI"].min(), fwi_df["FWI"].max(),
            fwi_df["ISI"].max(), fwi_df["FFMC"].max(),
        )

    return results


def format_summary(results: dict[str, pd.DataFrame]) -> str:
    """Format a human-readable summary of forecast results."""
    lines = ["FWI Forecast Summary (direct OWM)", "=" * 50]

    for station, df in results.items():
        max_fwi = df["FWI"].max()
        max_isi = df["ISI"].max()
        max_ffmc = df["FFMC"].max()
        lines.append(f"\n{station}:")
        lines.append(f"  Max FWI:  {max_fwi:.1f}")
        lines.append(f"  Max ISI:  {max_isi:.1f}")
        lines.append(f"  Max FFMC: {max_ffmc:.1f}")
        lines.append(f"  DMC: {df['DMC'].iloc[-1]:.1f}  DC: {df['DC'].iloc[-1]:.1f}")

        if max_fwi < 5:
            lines.append(f"  Class: LOW")
        elif max_fwi < 10:
            lines.append(f"  Class: MODERATE")
        elif max_fwi < 20:
            lines.append(f"  Class: HIGH")
        elif max_fwi < 30:
            lines.append(f"  Class: VERY HIGH")
        else:
            lines.append(f"  Class: EXTREME")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    print("Running FWI forecast pipeline (direct OWM, 6 stations)...")
    results = run_forecast()
    print(format_summary(results))

    out_dir = PROJECT_ROOT / "data" / "forecasts"
    out_dir.mkdir(parents=True, exist_ok=True)
    for station, df in results.items():
        path = out_dir / f"{station}_fwi_forecast.csv"
        df.to_csv(path)
        print(f"Saved {path}")


if __name__ == "__main__":
    main()
