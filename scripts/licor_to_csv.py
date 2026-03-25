#!/usr/bin/env python3
"""Convert raw Licor Cloud API JSON responses to PEINP-compatible CSVs.

Reads JSON files from data/raw/licor/<device>/ and writes monthly CSVs
to data/raw/peinp/ in the same format as the existing HOBOlink exports.

Usage:
    python scripts/licor_to_csv.py --all
    python scripts/licor_to_csv.py --device 21114831

Output CSVs are placed in:
    data/raw/peinp/<Station Folder> (Licor)/<YEAR>/<Prefix>_WeatherStn_<Mon>YYYY.csv

Read-only: This script only reads JSON and writes CSV. No API calls.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

# ── Config ──────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent.parent
LICOR_DIR = BASE_DIR / "data" / "raw" / "licor"
PEINP_DIR = BASE_DIR / "data" / "raw" / "peinp"
DEVICES_JSON = LICOR_DIR / "devices.json"

SENSOR_COLUMN_MAP = {
    "Temperature": "Temperature",
    "RH": "RH",
    "Dew Point": "Dew Point",
    "Rain": "Rain",
    "Accumulated Rain": "Accumulated Rain",
    "Solar Radiation": "Solar Radiation",
    "Wind Speed": "Average wind speed",
    "Gust Speed": "Wind gust speed",
    "Wind Direction": "Wind Direction",
    "Barometric Pressure": "Barometric Pressure",
    "Water Pressure": "Water Pressure",
    "Diff Pressure": "Diff Pressure",
    "Water Temperature": "Water Temperature",
    "Water Level": "Water Level",
    "Water Flow": "Water Flow",
    "Battery": "Battery",
}

STATION_META = {
    "cavendish": {"folder": "Cavendish", "csv_prefix": "PEINP_CAV_WeatherStn"},
    "north_rustico": {"folder": "North_Rustico", "csv_prefix": "PEINP_NR_WeatherStn"},
    "tracadie": {"folder": "Tracadie Wharf", "csv_prefix": "PEINP_TR_WeatherStn"},
    "greenwich": {"folder": "Greenwich", "csv_prefix": "PEINP_GR_WeatherStn"},
    "stanley_bridge": {"folder": "Stanley_Bridge", "csv_prefix": "PEINP_SB_WeatherStn"},
}

MONTH_ABBR = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]


# ── Helpers ──────────────────────────────────────���──────────────────────────

def load_devices() -> dict:
    if not DEVICES_JSON.exists():
        print(f"ERROR: {DEVICES_JSON} not found.")
        sys.exit(1)
    with open(DEVICES_JSON) as f:
        return json.load(f)


def _extract_sensors_from_file(data: dict) -> list[dict]:
    """Extract sensor data from a single JSON file.

    Handles two formats:
    1. API format: sensors is a list, each sensor has data[] with measurementType/units/records
    2. Combined format: sensors is a dict keyed by serial, each has measurementType/units/records
    """
    sensors_raw = data.get("sensors", [])
    result = []

    if isinstance(sensors_raw, list):
        # API format: list of sensor objects with nested data[]
        for sensor in sensors_raw:
            serial = sensor["sensorSerialNumber"]
            for measurement in sensor.get("data", []):
                result.append({
                    "serial": serial,
                    "type": measurement["measurementType"],
                    "units": measurement["units"],
                    "records": measurement["records"],
                })
    elif isinstance(sensors_raw, dict):
        # Combined format: dict keyed by serial
        for serial, sensor in sensors_raw.items():
            result.append({
                "serial": sensor["sensorSerialNumber"],
                "type": sensor["measurementType"],
                "units": sensor["units"],
                "records": sensor["records"],
            })

    return result


def load_json_data(device_dir: Path) -> list[dict]:
    """Load and merge all JSON files from a device directory."""
    sensor_data: dict[str, dict] = {}

    for json_file in sorted(device_dir.glob("*.json")):
        with open(json_file) as f:
            data = json.load(f)

        for sd in _extract_sensors_from_file(data):
            key = f"{sd['serial']}:{sd['type']}"
            if key not in sensor_data:
                sensor_data[key] = {
                    "serial": sd["serial"],
                    "type": sd["type"],
                    "units": sd["units"],
                    "records": [],
                }
            existing_ts = {r[0] for r in sensor_data[key]["records"]}
            new_records = [r for r in sd["records"] if r[0] not in existing_ts]
            sensor_data[key]["records"].extend(new_records)

    for sd in sensor_data.values():
        sd["records"].sort(key=lambda r: r[0])

    return list(sensor_data.values())


def build_dataframe(sensor_data: list[dict], device_serial: str, station_name: str) -> pd.DataFrame:
    """Build a unified DataFrame from sensor data, pivoting to wide format."""
    all_timestamps = set()
    for sd in sensor_data:
        for record in sd["records"]:
            all_timestamps.add(record[0])

    if not all_timestamps:
        return pd.DataFrame()

    ts_list = sorted(all_timestamps)

    columns = {}
    for sd in sensor_data:
        mtype = sd["type"]
        col_name = SENSOR_COLUMN_MAP.get(mtype, mtype)
        units = sd["units"]
        col_header = f"{col_name} ({mtype} {device_serial}:{sd['serial']}),{units},{station_name}"

        record_map = {r[0]: r[1] for r in sd["records"]}
        values = [record_map.get(ts, "") for ts in ts_list]
        columns[col_header] = values

    df = pd.DataFrame(columns)

    dates = []
    times = []
    for ts_ms in ts_list:
        dt_utc = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        dt_ast = dt_utc.astimezone(timezone(timedelta(hours=-4)))
        dates.append(dt_ast.strftime("%m/%d/%Y"))
        times.append(dt_ast.strftime("%H:%M:%S %z"))

    df.insert(0, "Time", times)
    df.insert(0, "Date", dates)

    return df


def split_by_month(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Split a DataFrame into monthly chunks keyed by 'MonYYYY'."""
    months: dict[str, list[int]] = {}
    for i, date_str in enumerate(df["Date"]):
        dt = datetime.strptime(date_str, "%m/%d/%Y")
        key = f"{MONTH_ABBR[dt.month - 1]}{dt.year}"
        if key not in months:
            months[key] = []
        months[key].append(i)

    return {key: df.iloc[indices].reset_index(drop=True) for key, indices in months.items()}


def write_monthly_csvs(
    df: pd.DataFrame,
    station_key: str,
    csv_prefix: str,
    station_folder: str,
) -> list[Path]:
    """Split DataFrame by month and write CSVs to PEINP directory."""
    monthly = split_by_month(df)
    written = []

    for month_key, month_df in sorted(monthly.items()):
        year = month_key[3:]
        out_dir = PEINP_DIR / f"{station_folder} (Licor)" / year
        out_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{csv_prefix}_{month_key}.csv"
        out_path = out_dir / filename

        month_df.to_csv(out_path, index=False)
        written.append(out_path)
        print(f"  Wrote {out_path.relative_to(BASE_DIR)} ({len(month_df)} rows)")

    return written


# ── Main ────────────────────────────────────────────────────────────────────

def process_device(station_key: str, device_serial: str, station_name: str) -> list[Path]:
    """Process a single device: load JSON, convert to CSV, write monthly files."""
    device_dir = LICOR_DIR / device_serial
    if not device_dir.exists():
        print(f"  No data directory found: {device_dir}")
        return []

    print(f"\nProcessing {station_key} ({device_serial})...")

    sensor_data = load_json_data(device_dir)
    total_records = sum(len(sd["records"]) for sd in sensor_data)
    print(f"  Loaded {len(sensor_data)} sensors, {total_records} total records")

    if not sensor_data:
        print("  No sensor data found, skipping.")
        return []

    df = build_dataframe(sensor_data, device_serial, station_name)
    if df.empty:
        print("  Empty DataFrame, skipping.")
        return []

    print(f"  Unified DataFrame: {len(df)} rows x {len(df.columns)} columns")

    meta = STATION_META[station_key]
    written = write_monthly_csvs(df, station_key, meta["csv_prefix"], meta["folder"])
    return written


def main():
    parser = argparse.ArgumentParser(description="Convert Licor API JSON to PEINP-compatible CSVs")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--device", type=str, help="Device serial number to process")
    group.add_argument("--all", action="store_true", help="Process all known devices")
    args = parser.parse_args()

    devices = load_devices()
    all_written = []

    if args.device:
        found_key = None
        for key, info in devices["stations"].items():
            if info["device_serial"] == args.device:
                found_key = key
                break
        if not found_key:
            print(f"ERROR: Device {args.device} not found in devices.json")
            sys.exit(1)
        written = process_device(found_key, args.device, devices["stations"][found_key]["name"])
        all_written.extend(written)

    elif args.all:
        for station_key, info in devices["stations"].items():
            written = process_device(station_key, info["device_serial"], info["name"])
            all_written.extend(written)

    print(f"\nDone. {len(all_written)} CSV files written.")


if __name__ == "__main__":
    main()
