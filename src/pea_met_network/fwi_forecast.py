"""FWI Forecast Pipeline — Licor live obs + OWM + GDPS, compute FWI.

Three data sources merged per station:
  1. Licor Cloud (0–3h): live park station observations
  2. OWM One Call 3.0 (3–48h): hourly model forecast per station
  3. GDPS (48–240h): EC 3-hourly model forecast

Park stations without RH sensors (Tracadie, Stanley Bridge) get RH
via cross-station vapor pressure continuity from donors with RH (Cavendish,
North Rustico) — same method as the static ETL.

Startup indices (FFMC, DMC, DC) persist between runs via JSON so the daily
DMC/DC chain carries forward.  Falls back to defaults on first run or if
the state file is stale (>72h old).

Also fetches CWFIS SCRIBE forecast FWI for comparison when the national
fire weather network is active (skips gracefully during off-season when
CWFIS reports sentinel value -101).

Usage:
    python -m pea_met_network.fwi_forecast
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import urllib.request
import urllib.error
import urllib.parse

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

# Default startup indices (spring defaults)
DEFAULT_FFMC0 = 85.0
DEFAULT_DMC0 = 6.0
DEFAULT_DC0 = 15.0

# CWFIS sentinel value meaning "not computed / off-season"
CWFIS_SENTINEL = -101.0

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FORECASTS_DIR = PROJECT_ROOT / "data" / "forecasts"
STATE_FILE = FORECASTS_DIR / "startup_state.json"

# ---------------------------------------------------------------------------
# Startup index persistence
# ---------------------------------------------------------------------------

def load_startup_state(path: Path = STATE_FILE) -> dict[str, dict[str, float]]:
    """Load persisted startup indices from previous run.

    Returns dict like: {"stanhope": {"ffmc": 85.0, "dmc": 6.0, "dc": 15.0, "timestamp": "..."}}
    Returns empty dict if file missing or stale (>72h).
    """
    if not path.exists():
        return {}

    try:
        data = json.loads(path.read_text())
    except (json.JSONDecodeError, OSError):
        return {}

    # Check staleness — reject if older than 72 hours
    ts_str = data.get("_timestamp", "")
    if ts_str:
        try:
            ts = datetime.fromisoformat(ts_str)
            if datetime.now(timezone.utc) - ts > timedelta(hours=72):
                logger.warning("Startup state stale (%s), using defaults", ts_str)
                return {}
        except ValueError:
            return {}

    # Strip metadata key
    return {k: v for k, v in data.items() if k != "_timestamp"}


def save_startup_state(
    results: dict[str, pd.DataFrame],
    path: Path = STATE_FILE,
) -> None:
    """Persist the final FWI indices from each station for next run."""
    state = {"_timestamp": datetime.now(timezone.utc).isoformat()}

    for station_name, df in results.items():
        last = df.iloc[-1]
        state[station_name] = {
            "ffmc": round(float(last["FFMC"]), 2),
            "dmc": round(float(last["DMC"]), 2),
            "dc": round(float(last["DC"]), 2),
        }

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2) + "\n")
    logger.info("Saved startup state → %s", path)


def get_startup_indices(
    station_name: str,
    state: dict[str, dict[str, float]],
) -> tuple[float, float, float]:
    """Get (ffmc0, dmc0, dc0) for a station from state, falling back to defaults."""
    if station_name in state:
        s = state[station_name]
        return s["ffmc"], s["dmc"], s["dc"]
    return DEFAULT_FFMC0, DEFAULT_DMC0, DEFAULT_DC0

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
    """Fetch OWM for multiple stations and return parsed weather DataFrames."""
    if stations is None:
        stations = ALL_STATIONS

    weather = {}
    for stn in stations:
        data = fetch_forecast(stn)
        weather[stn.name] = parse_hourly_weather(data)
    return weather


def parse_hourly_weather(data: dict[str, Any]) -> pd.DataFrame:
    """Convert OWM hourly response into a clean DataFrame."""
    rows = []
    for h in data["hourly"]:
        ts = datetime.fromtimestamp(h["dt"], tz=timezone.utc)
        rain = h.get("rain")
        rain_mm = rain.get("1h", 0.0) if isinstance(rain, dict) else 0.0

        rows.append({
            "timestamp_utc": ts,
            "air_temperature_c": h["temp"],
            "relative_humidity_pct": h["humidity"],
            "wind_speed_kmh": h["wind_speed"] * 3.6,
            "rain_mm": rain_mm,
        })

    df = pd.DataFrame(rows)
    df = df.set_index("timestamp_utc").sort_index()
    return df

# ---------------------------------------------------------------------------
# CWFIS SCRIBE forecast comparison
# ---------------------------------------------------------------------------

CWFIS_WFS = "https://cwfis.cfs.nrcan.gc.ca/geoserver/wfs"

# CWFIS station IDs for PEI area (nearest to our stations)
# Stanhope → HAR (Harrington, 46.35, -63.17)
# No exact park station matches, but these are the closest CWFIS fire weather stations
CWFIS_PE_STATIONS = {
    "HAR": "Harrington",
    "YYG": "Charlottetown",
    "YSU": "Summerside",
    "KEN": "Kensington",
}


def fetch_cwfis_forecast() -> dict[str, list[dict[str, Any]]]:
    """Fetch CWFIS SCRIBE 48h FWI forecast for PEI stations.

    Returns dict mapping CWFIS station ID → list of forecast dicts.
    Returns empty dict if off-season (all FWI = sentinel -101) or on error.
    """
    try:
        url = (
            f"{CWFIS_WFS}?service=WFS&version=1.0.0&request=GetFeature"
            f"&typeName=public:firewx_scribe_fcst&outputFormat=JSON"
            f"&maxFeatures=2440"
        )
        resp = urllib.request.urlopen(url, timeout=20)
        data = json.loads(resp.read())
    except Exception as e:
        logger.warning("CWFIS fetch failed: %s", e)
        return {}

    pe_ids = set(CWFIS_PE_STATIONS.keys())
    results: dict[str, list[dict[str, Any]]] = {}

    for f in data.get("features", []):
        p = f.get("properties", {})
        sid = p.get("id", "")
        if sid not in pe_ids:
            continue

        # Skip sentinel values
        if p.get("ffmc") is not None and p["ffmc"] < CWFIS_SENTINEL + 1:
            continue

        if sid not in results:
            results[sid] = []
        results[sid].append({
            "rep_date": p["rep_date"],
            "temp": p.get("temp"),
            "rh": p.get("rh"),
            "ws": p.get("ws"),
            "precip": p.get("precip"),
            "ffmc": p.get("ffmc"),
            "dmc": p.get("dmc"),
            "dc": p.get("dc"),
            "isi": p.get("isi"),
            "bui": p.get("bui"),
            "fwi": p.get("fwi"),
        })

    if not results:
        logger.info("CWFIS: off-season, no FWI data available for PEI")
    else:
        for sid, rows in results.items():
            logger.info("CWFIS %s (%s): %d forecast entries", sid, CWFIS_PE_STATIONS[sid], len(rows))

    return results


def format_cwfis_comparison(cwfis: dict[str, list[dict]]) -> str:
    """Format CWFIS comparison data for the summary."""
    if not cwfis:
        return ["\n  CWFIS comparison: off-season (no FWI data available)"]

    lines = ["\n  CWFIS SCRIBE Forecast Comparison:"]
    for sid in sorted(cwfis.keys()):
        name = CWFIS_PE_STATIONS[sid]
        lines.append(f"\n    {sid} ({name}):")
        for r in sorted(cwfis[sid], key=lambda x: x["rep_date"]):
            lines.append(
                f"      {r['rep_date'][:10]}  T={r['temp']:.1f}  "
                f"RH={r['rh']:.0f}  WS={r['ws']:.1f}  "
                f"FFMC={r['ffmc']:.1f}  DMC={r['dmc']:.1f}  "
                f"DC={r['dc']:.1f}  FWI={r['fwi']:.1f}"
            )
    return lines

# ---------------------------------------------------------------------------
# FWI computation over hourly series
# ---------------------------------------------------------------------------

def compute_fwi_series(
    weather: pd.DataFrame,
    station: Station,
    ffmc0: float = DEFAULT_FFMC0,
    dmc0: float = DEFAULT_DMC0,
    dc0: float = DEFAULT_DC0,
) -> pd.DataFrame:
    """Compute FWI components for each hour in the weather DataFrame.

    Uses hourly FFMC (Van Wagner hourly equation) and daily aggregates
    for DMC/DC.  Daily aggregation uses the warmest hour's temp/RH and
    accumulated total rain per local calendar day.
    """
    weather = weather.copy()
    weather["month"] = weather.index.month

    def _local_date(ts: pd.Timestamp, month: int) -> "datetime.date":
        offset = 3 if month in (4, 5, 6, 7, 8, 9, 10) else 4
        return (ts.tz_convert(None) - pd.Timedelta(hours=offset)).date()

    local_dates = [
        _local_date(ts, row["month"]) for ts, row in weather.iterrows()
    ]
    weather["local_date"] = local_dates

    # --- Aggregate daily values ---
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

    # --- Compute hourly FWI ---
    ffmc = ffmc0
    results = []

    for ts, row in weather.iterrows():
        temp = row["air_temperature_c"]
        rh = row["relative_humidity_pct"]
        wind = row["wind_speed_kmh"]
        rain = row["rain_mm"]
        ld = row["local_date"]
        dmc, dc = daily_codes[ld]

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
# OWM RH bias correction
# ---------------------------------------------------------------------------

# Stations that lack RH sensors — candidates for OWM bias-corrected fallback
_NO_RH_STATIONS = {"tracadie", "stanley_bridge"}

# Donor stations with RH sensors, used for bias estimation
_RH_DONORS = {"cavendish", "north_rustico", "greenwich"}


def owm_bias_correct_rh(
    obs_data: dict[str, pd.DataFrame],
    owm_data: dict[str, pd.DataFrame],
    weather_data: dict[str, pd.DataFrame],
    targets: set[str] | None = None,
    donors: set[str] | None = None,
) -> dict[str, int]:
    """Fill remaining RH gaps using OWM spatial bias correction.

    For each target station still missing RH after cross-station imputation:
      1. At donor stations, compute per-hour bias where both Licor obs
         and raw OWM RH exist:
             bias_h = OWM_RH_donor(h) - Licor_RH_donor(h)
      2. Take median bias across all (donor, hour) pairs for robustness.
      3. Apply: corrected_RH_target(h) = OWM_RH_target(h) - median_bias

    Only fills hours where raw OWM has RH data for the target.
    Returns dict mapping station name → number of hours filled.
    """
    if targets is None:
        targets = _NO_RH_STATIONS
    if donors is None:
        donors = _RH_DONORS

    filled: dict[str, int] = {}

    for target in targets:
        if target not in weather_data:
            continue
        target_df = weather_data[target]
        if "relative_humidity_pct" not in target_df.columns:
            continue

        # Find hours still missing RH after cross-station imputation
        missing_mask = target_df["relative_humidity_pct"].isna()
        if not missing_mask.any():
            continue

        missing_hours = target_df.index[missing_mask]
        if len(missing_hours) == 0:
            continue

        # Need raw OWM RH for the target at those hours
        if target not in owm_data:
            continue
        owm_target = owm_data[target]
        if "relative_humidity_pct" not in owm_target.columns:
            continue

        # Collect biases from donor stations where we have both obs and OWM
        biases: list[float] = []
        for donor in donors:
            if donor not in obs_data or donor not in owm_data:
                continue
            obs_donor = obs_data[donor]
            owm_donor = owm_data[donor]
            if "relative_humidity_pct" not in obs_donor.columns:
                continue
            if "relative_humidity_pct" not in owm_donor.columns:
                continue

            # Hours where both Licor obs and raw OWM exist at donor
            overlap = obs_donor.index.intersection(owm_donor.index)
            overlap = overlap[obs_donor.loc[overlap, "relative_humidity_pct"].notna()]
            overlap = overlap[owm_donor.loc[overlap, "relative_humidity_pct"].notna()]

            for ts in overlap:
                obs_val = float(obs_donor.at[ts, "relative_humidity_pct"])
                owm_val = float(owm_donor.at[ts, "relative_humidity_pct"])
                biases.append(owm_val - obs_val)

        if len(biases) < 2:
            logger.warning(
                "OWM bias: %s has %d bias samples, too few, skipping",
                target, len(biases),
            )
            continue

        median_bias = float(pd.Series(biases).median())
        logger.info(
            "OWM bias: %s median bias = %.1f%% RH (from %d donor-hours)",
            target, median_bias, len(biases),
        )

        # Apply bias correction to missing hours
        n_filled = 0
        for ts in missing_hours:
            if ts not in owm_target.index:
                continue
            owm_rh = owm_target.at[ts, "relative_humidity_pct"]
            if pd.isna(owm_rh):
                continue
            corrected = owm_rh - median_bias
            target_df.at[ts, "relative_humidity_pct"] = min(max(corrected, 0.0), 100.0)
            n_filled += 1

        if n_filled > 0:
            logger.info("  %s: OWM bias-corrected %d RH values", target, n_filled)
        filled[target] = n_filled

    return filled


# ---------------------------------------------------------------------------
# End-to-end pipeline
# ---------------------------------------------------------------------------

def run_forecast(
    stations: list[Station] | None = None,
    startup_state: dict[str, dict[str, float]] | None = None,
    include_gdps: bool = True,
) -> dict[str, pd.DataFrame]:
    """Run the full FWI forecast pipeline.

    Three data sources merged per station:
      1. Licor Cloud (0–3h): live park station observations
      2. OWM (3–48h): hourly model forecast
      3. GDPS (48–240h): 3-hourly EC model forecast

    Park stations without RH sensors (Tracadie, Stanley Bridge) get RH
    via cross-station direct RH donation from Cavendish/N. Rustico donors.

    Computes FWI per station. Persists startup indices for next run.

    Args:
        stations: Stations to forecast for.
        startup_state: Pre-loaded startup indices (loaded from disk if None).
        include_gdps: If True, also fetch GDPS 10-day data and merge.

    Returns:
        Dict mapping station name → DataFrame with FWI components per timestep.
    """
    if stations is None:
        stations = ALL_STATIONS
    if startup_state is None:
        startup_state = load_startup_state()

    if startup_state:
        logger.info("Loaded startup state for %d stations", len(startup_state))

    # 1. Fetch live observations from Licor Cloud for park stations
    obs_data: dict[str, pd.DataFrame] = {}
    try:
        from pea_met_network.licor_adapter import LicorAdapter
        adapter = LicorAdapter()
        obs_data = adapter.fetch_recent(hours=6)
        if obs_data:
            # Cross-station RH imputation for stations with no RH sensor
            from pea_met_network.cross_station_impute import (
                DonorAssignment,
                impute_cross_station,
            )
            rh_donors = [
                DonorAssignment("tracadie", "relative_humidity_pct", 1, "cavendish", "internal"),
                DonorAssignment("tracadie", "relative_humidity_pct", 2, "north_rustico", "internal"),
                DonorAssignment("stanley_bridge", "relative_humidity_pct", 1, "cavendish", "internal"),
                DonorAssignment("stanley_bridge", "relative_humidity_pct", 2, "north_rustico", "internal"),
            ]
            for stn_name in list(obs_data.keys()):
                if obs_data[stn_name]["relative_humidity_pct"].notna().any():
                    continue
                imputed_df, records = impute_cross_station(
                    obs_data[stn_name],
                    station=stn_name,
                    donor_assignments=rh_donors,
                    internal_hourly=obs_data,
                )
                obs_data[stn_name] = imputed_df
                if records:
                    logger.info(
                        "  %s: cross-station imputed %d RH values",
                        stn_name, len(records),
                    )
            logger.info("Licor live obs: %d stations", len(obs_data))
    except Exception as e:
        logger.warning("Licor fetch failed, using OWM for all hours: %s", e)

    # 2. Fetch OWM for all stations (0–48h, hourly)
    owm_raw = fetch_all_stations(stations)
    weather_data = owm_raw  # start with OWM; obs will be merged in next step

    # 3. Merge Licor live obs into OWM (obs preferred in overlap zone)
    if obs_data:
        for stn_name, obs_df in obs_data.items():
            if stn_name not in weather_data:
                continue
            owm_df = weather_data[stn_name]
            # Live obs cover recent hours; replace overlapping OWM entries
            # with real observations (observed > model forecast).
            # combine_first: obs values win where both exist, OWM fills the rest.
            weather_data[stn_name] = obs_df.combine_first(owm_df).sort_index()
            obs_hours = len(obs_df.index.intersection(owm_df.index))
            logger.info("  %s: %d OWM hours replaced with Licor obs", stn_name, obs_hours)

    # 3b. OWM bias-corrected RH fallback for stations still missing RH
    #     (Tracadie, Stanley Bridge if cross-station imputation couldn't fill all hours)
    if obs_data:
        bias_filled = owm_bias_correct_rh(obs_data, owm_raw, weather_data)
        for stn, n in bias_filled.items():
            if n > 0:
                logger.info("  %s: %d additional hours via OWM bias correction", stn, n)

    # 4. Optionally extend with GDPS (0–240h, 3-hourly)
    gdps_data: dict[str, pd.DataFrame] | None = None
    if include_gdps:
        try:
            from pea_met_network.gdps_fetcher import GDPSFetcher, Station as GDPSStation
            gdps_stations = [GDPSStation(s.name, s.lat, s.lon) for s in stations]
            fetcher = GDPSFetcher()
            gdps_raw = fetcher.fetch(gdps_stations, max_hours=240)
            if gdps_raw:
                gdps_data = gdps_raw
                gdps_hours = max(len(df) for df in gdps_raw.values()) * 3
                logger.info("GDPS extended forecast: %dh available", gdps_hours)
        except Exception as e:
            logger.warning("GDPS fetch failed, using OWM only: %s", e)

    # 5. Merge OWM and GDPS per station
    if gdps_data:
        for stn in stations:
            if stn.name not in gdps_data:
                continue
            owm_df = weather_data[stn.name]
            gdps_df = gdps_data[stn.name]

            # OWM covers 0–48h hourly; GDPS covers 0–240h at 3h.
            # For hours where OWM has data, prefer it (higher resolution).
            # GDPS fills hours beyond OWM's range.
            gdps_beyond = gdps_df.index.difference(owm_df.index)
            if len(gdps_beyond) > 0:
                extra = gdps_df.loc[gdps_beyond]
                weather_data[stn.name] = pd.concat([owm_df, extra]).sort_index()
                logger.info(
                    "%s: merged %d OWM + %d GDPS timesteps (%dh total)",
                    stn.name, len(owm_df), len(extra),
                    len(weather_data[stn.name]),
                )

    # 6. Compute FWI per station
    results = {}
    for stn in stations:
        ffmc0, dmc0, dc0 = get_startup_indices(stn.name, startup_state)
        logger.info("%s startup: FFMC=%.1f DMC=%.1f DC=%.1f", stn.name, ffmc0, dmc0, dc0)

        fwi_df = compute_fwi_series(weather_data[stn.name], stn, ffmc0=ffmc0, dmc0=dmc0, dc0=dc0)
        results[stn.name] = fwi_df
        logger.info(
            "%s: FWI [%.1f, %.1f], max ISI %.1f, max FFMC %.1f, %d hours",
            stn.name,
            fwi_df["FWI"].min(), fwi_df["FWI"].max(),
            fwi_df["ISI"].max(), fwi_df["FFMC"].max(),
            len(fwi_df),
        )

    # 7. Persist final indices for next run
    save_startup_state(results)

    return results


def format_summary(
    results: dict[str, pd.DataFrame],
    cwfis: dict[str, list[dict]] | None = None,
) -> str:
    """Format a human-readable summary of forecast results."""
    lines = ["FWI Forecast Summary (Licor obs + OWM + GDPS)", "=" * 50]

    for station, df in results.items():
        max_fwi = df["FWI"].max()
        max_isi = df["ISI"].max()
        max_ffmc = df["FFMC"].max()
        hours = len(df)
        span_h = (df.index[-1] - df.index[0]).total_seconds() / 3600
        lines.append(f"\n{station}: ({span_h:.0f}h, {hours} timesteps)")
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

    # CWFIS comparison
    if cwfis is not None:
        lines.extend(format_cwfis_comparison(cwfis))

    return "\n".join(lines)

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    print("Running FWI forecast pipeline (Licor + OWM + GDPS, 6 stations)...")

    # Fetch CWFIS comparison data
    cwfis = fetch_cwfis_forecast()

    # Run forecast
    results = run_forecast()

    print(format_summary(results, cwfis))

    # Save CSVs
    FORECASTS_DIR.mkdir(parents=True, exist_ok=True)
    for station, df in results.items():
        path = FORECASTS_DIR / f"{station}_fwi_forecast.csv"
        df.to_csv(path)
        print(f"Saved {path}")


if __name__ == "__main__":
    main()
