"""JSON adapter for Licor Cloud API responses."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pea_met_network.adapters.base import BaseAdapter
from pea_met_network.adapters.column_maps import derive_wind_speed_kmh


# Map Licor measurement types to canonical column names.
# Wind speed comes in m/s from the API, so we store as wind_speed_ms
# and let derive_wind_speed_kmh convert it.
LICOR_MEASUREMENT_MAP: dict[str, str] = {
    "Temperature": "air_temperature_c",
    "RH": "relative_humidity_pct",
    "Dew Point": "dew_point_c",
    "Rain": "rain_mm",
    "Accumulated Rain": "accumulated_rain_mm",  # will be dropped
    "Solar Radiation": "solar_radiation_w_m2",
    "Wind Speed": "wind_speed_ms",
    "Gust Speed": "wind_gust_speed_kmh",  # API returns in m/s actually
    "Wind Direction": "wind_direction_deg",
    "Barometric Pressure": "barometric_pressure_kpa",
}

# Units that need conversion
UNIT_CONVERSIONS: dict[str, dict[str, float]] = {
    "Wind Speed": {"m/s": 3.6},  # multiply to get km/h
    "Gust Speed": {"m/s": 3.6},
}


def _load_devices_json(path: Path) -> dict:
    """Load the devices.json mapping file."""
    with open(path) as f:
        return json.load(f)


def _serial_to_station(devices: dict, serial: str) -> str | None:
    """Look up a station name from a device serial number."""
    for station_name, info in devices.get("stations", {}).items():
        if info.get("device_serial") == serial:
            return station_name
    return None


class JSONAdapter(BaseAdapter):
    """Adapter for Licor Cloud API JSON files."""

    def load(self, path: Path) -> pd.DataFrame:
        """Load a Licor JSON file and return a DataFrame with canonical schema columns."""
        with open(path) as f:
            data = json.load(f)

        # Skip non-sensor files (devices.json, etc.)
        if "sensors" not in data:
            return pd.DataFrame()

        # Locate the devices.json file (sibling or parent directory)
        devices_path = path.parent.parent / "devices.json"
        if not devices_path.exists():
            devices_path = path.parent / "devices.json"
        devices: dict = {}
        if devices_path.exists():
            devices = _load_devices_json(devices_path)

        # Infer station from directory name (device serial)
        device_serial = path.parent.name
        station = _serial_to_station(devices, device_serial)

        # Parse sensor data
        sensors = data.get("sensors", [])
        if isinstance(sensors, dict):
            sensors = [sensors]

        series_map: dict[str, pd.Series] = {}

        for sensor in sensors:
            serial = sensor.get("sensorSerialNumber", "")
            data_entries = sensor.get("data", [])
            if isinstance(data_entries, dict):
                data_entries = [data_entries]

            for entry in data_entries:
                measurement_type = entry.get("measurementType", "")
                units = entry.get("units", "")
                records = entry.get("records", [])

                if not records:
                    continue

                canonical_name = LICOR_MEASUREMENT_MAP.get(measurement_type)
                if canonical_name is None:
                    continue

                # Skip accumulated rain — not in canonical schema
                if canonical_name == "accumulated_rain_mm":
                    continue

                # Build timestamp -> value series
                ts_vals: dict[pd.Timestamp, float] = {}
                for record in records:
                    if len(record) < 2:
                        continue
                    ts_ms, val = record[0], record[1]
                    try:
                        ts = pd.Timestamp(ts_ms, unit="ms", tz="UTC")
                        ts_vals[ts] = float(val)
                    except (ValueError, TypeError):
                        continue

                if not ts_vals:
                    continue

                # Apply unit conversions
                conv = UNIT_CONVERSIONS.get(measurement_type, {})
                multiplier = conv.get(units, 1.0)

                s = pd.Series(ts_vals, name=canonical_name)
                if multiplier != 1.0:
                    s = s * multiplier
                    # Rename if conversion changed the semantic (m/s -> km/h)
                    if measurement_type == "Wind Speed" and units == "m/s":
                        s.name = "wind_speed_kmh"
                    elif measurement_type == "Gust Speed" and units == "m/s":
                        s.name = "wind_gust_speed_kmh"

                series_map[s.name] = s

        if not series_map:
            return pd.DataFrame()

        # Combine all series into a DataFrame
        df = pd.DataFrame(series_map)
        df = df.sort_index()
        df.index.name = "timestamp_utc"
        df = df.reset_index()

        if station:
            df["station"] = station

        df["source_file"] = str(path)
        return df
