"""Column name mappings shared across PEINP/ECCC CSV and XLSX adapters."""

from __future__ import annotations

import pandas as pd

# Maps common raw column prefixes (before the first parenthesis)
# to canonical output column names.
COLUMN_MAPS: dict[str, str] = {
    # Temperature
    "S-THB: Temp - °C": "air_temperature_c",
    "S-THC: Temp - °C": "air_temperature_c",
    "S-TMB: Temp - °C": "air_temperature_c",
    "Temp (°C)": "air_temperature_c",
    "Temperature (°C)": "air_temperature_c",
    # Humidity
    "S-THB: RH - %": "relative_humidity_pct",
    "S-THC: RH - %": "relative_humidity_pct",
    "RH (%)": "relative_humidity_pct",
    "RH": "relative_humidity_pct",
    # Rain
    "Rain - mm": "rain_mm",
    "Precip (mm)": "rain_mm",
    # Wind speed (km/h)
    "Wind Speed - km/h": "wind_speed_kmh",
    "Average Wind Speed": "wind_speed_kmh",
    "Average wind speed": "wind_speed_kmh",
    "Avg Wind speed": "wind_speed_kmh",
    "wind speed": "wind_speed_kmh",
    # Wind speed (m/s) — intermediate, converted to km/h later
    "S-WCF-M: Wind Speed - m/s": "wind_speed_ms",
    "Wind Speed - m/s": "wind_speed_ms",
    # Wind gust
    "Wind gust  speed": "wind_gust_speed_kmh",
    "Wind gust speed": "wind_gust_speed_kmh",
    "Gust (km/h)": "wind_gust_speed_kmh",
    # Wind direction
    "S-WCF-M: Wind Direction - °": "wind_direction_deg",
    "Wind Direction - °": "wind_direction_deg",
    "Wind Dir (°)": "wind_direction_deg",
    "Wind Dir (10s deg)": "wind_direction_deg",
    # Solar radiation
    "Solar Rad - W/m²": "solar_radiation_w_m2",
    "Solar Radiation": "solar_radiation_w_m2",
    # Dew point
    "Dew Point (°C)": "dew_point_c",
    "Dew Point": "dew_point_c",
    # Pressure
    "Pressure (hPa)": "pressure_hpa",
    "Barometric Pressure": "barometric_pressure_kpa",
    "Stn Press (kPa)": "barometric_pressure_kpa",
    # Water level (coastal stations)
    "Water Level - m": "water_level_m",
    "Water Level": "water_level_m",
    "Water Pressure - kPa": "water_pressure_kpa",
    "Water Pressure": "water_pressure_kpa",
    "Water Temp - °C": "water_temperature_c",
    "Water Temperature": "water_temperature_c",
    # Bare prefixes (XLSX headers with sensor serials in parens)
    "Rain": "rain_mm",
    "Temperature": "air_temperature_c",
    "Wind Direction": "wind_direction_deg",
    "Wind Speed": "wind_speed_kmh",
    "Gust Speed": "wind_gust_speed_kmh",
    "Wind Gust Speed": "wind_gust_speed_kmh",
    # Battery
    "Battery - V": "battery_v",
    "Battery": "battery_v",
    # Temperature from ECCC (prefix form after extract_prefix strips parens)
    "Dew Point Temp (°C)": "dew_point_c",
    "Dew Point Temp": "dew_point_c",
    "Rel Hum (%)": "relative_humidity_pct",
    "Rel Hum": "relative_humidity_pct",
    "Precip. Amount (mm)": "rain_mm",
    "Precip. Amount": "rain_mm",
    "Wind Spd (km/h)": "wind_speed_kmh",
    "Wind Spd": "wind_speed_kmh",
    "Wind Dir (10s deg)": "wind_direction_deg",
    "Wind Dir": "wind_direction_deg",
    "Stn Press (kPa)": "barometric_pressure_kpa",
    "Stn Press": "barometric_pressure_kpa",
    "Temp (°C)": "air_temperature_c",
    "Temp": "air_temperature_c",
}

# Columns that should be excluded from the output
SKIP_COLUMNS: set[str] = {
    "accumulated_rain_mm",
    "accumulated_rain",
}

# Prefixes that should be dropped entirely (never reach output)
SKIP_PREFIXES: set[str] = {
    "Accumulated Rain",
    "Diff Pressure",
}


def extract_prefix(column: str) -> str:
    """Extract the meaningful prefix before the first parenthesis.

    Handles formats like:
    - 'Rain (Rain 21038161:21038325-1),mm,Stanley Bridge Harbour'
    - 'Temperature (Temperature 21038161:21098954-1),°C,Stanley Bridge Harbour'
    - 'Temp (°C)'
    - 'RH (%)'
    """
    if "(" in column:
        prefix = column.split("(", 1)[0].strip()
    else:
        prefix = column.strip()
    # Normalize internal whitespace
    return " ".join(prefix.split())


def rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename DataFrame columns using the column maps.

    Columns not found in the map are kept as-is.
    Columns whose prefix matches SKIP_PREFIXES are dropped.
    """
    rename_map: dict[str, str] = {}
    drop_cols: list[str] = []
    for col in df.columns:
        prefix = extract_prefix(col)
        if prefix in SKIP_PREFIXES:
            drop_cols.append(col)
            continue
        if prefix in COLUMN_MAPS:
            target = COLUMN_MAPS[prefix]
            if target in SKIP_COLUMNS:
                drop_cols.append(col)
                continue
            rename_map[col] = target
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df.rename(columns=rename_map)


def coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Combine columns that share the same name by forward-filling NaNs.

    When multiple raw columns map to the same canonical name (e.g., two
    temperature sensors), instead of keeping only the last one, merge them
    so that non-null values from earlier columns fill gaps in later ones.

    This prevents dead/empty sensor columns from overwriting good data.
    """
    if not df.columns.duplicated().any():
        return df

    # Use iloc to access duplicate columns individually (positional indexing
    # avoids the 2D DataFrame that df[col_name] returns for duplicates).
    groups: dict[str, list[int]] = {}
    for i, col in enumerate(df.columns):
        groups.setdefault(col, []).append(i)

    keep_indices: list[int] = []
    series_map: dict[str, pd.Series] = {}

    for col_name, indices in groups.items():
        if len(indices) == 1:
            keep_indices.append(indices[0])
        else:
            # Coalesce: first non-null value wins across duplicates
            combined = pd.Series(pd.NA, index=df.index, dtype=object)
            for idx in indices:
                s = pd.to_numeric(df.iloc[:, idx], errors="coerce")
                combined = combined.where(
                    combined.notna() & (combined == combined), s
                )
            series_map[col_name] = combined

    # Build result: non-duplicate columns by position, coalesced by name
    result = df.iloc[:, keep_indices].copy()
    for col_name, s in series_map.items():
        result[col_name] = s

    return result


def derive_wind_speed_kmh(df: pd.DataFrame) -> pd.DataFrame:
    """Derive wind_speed_kmh from wind_speed_ms if only m/s is available."""
    if "wind_speed_ms" in df.columns and "wind_speed_kmh" not in df.columns:
        df = df.copy()
        df["wind_speed_kmh"] = df["wind_speed_ms"] * 3.6
    return df