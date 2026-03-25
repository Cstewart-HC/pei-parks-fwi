"""Canonical schema definition for the PEA Met Network pipeline.

Every adapter must produce a DataFrame containing these columns.
Only the core columns are *required*; the rest are present when
the source data provides them.
"""

CANONICAL_SCHEMA = [
    # Required — every adapter must produce these
    "station",
    "timestamp_utc",
    # FWI-required (should be present for meteorological stations)
    "air_temperature_c",
    "relative_humidity_pct",
    "wind_speed_kmh",
    "rain_mm",
    # Optional — present when source has them
    "wind_direction_deg",
    "wind_gust_speed_kmh",
    "dew_point_c",
    "solar_radiation_w_m2",
    "barometric_pressure_kpa",
    # Coastal / water stations
    "water_level_m",
    "water_pressure_kpa",
    "water_temperature_c",
]
