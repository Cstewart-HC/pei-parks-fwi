# Quality Enforcement

Implemented in `src/pea_met_network/quality.py`. Applied during Pass 1 per station.

## Configuration

All thresholds in `docs/cleaning-config.json` under top-level keys.

## Checks

### 1. Value Range Checks (`value_ranges`)

| Variable | Min | Max |
|---|---|---|
| `air_temperature_c` | -50.0 | 60.0 |
| `relative_humidity_pct` | 0.0 | 105.0 |
| `wind_speed_kmh` | 0.0 | 200.0 |
| `rain_mm` | 0.0 | 500.0 |

Values outside these ranges are flagged and actioned per enforcement config.

### 2. Rate-of-Change Detection (`rate_of_change`)

1-hour window. Detects physically implausible jumps between consecutive readings:

| Variable | Max Δ per hour |
|---|---|
| `air_temperature_c` | 8.0 °C |
| `relative_humidity_pct` | 30.0 % |
| `wind_speed_kmh` | 40.0 km/h |

### 3. Cross-Variable Checks (`cross_variable_checks`)

**Rain with low RH**: If `rain_mm > 0` and `relative_humidity_pct < 70`, the rain value is flagged. Physical rationale: rainfall requires sufficient atmospheric moisture.

### 4. Flatline Detection (`flatline`)

Detects stale sensor readings — constant value for ≥6 consecutive hours.

| Variables Monitored | Threshold |
|---|---|
| `air_temperature_c` | 6 hours |
| `relative_humidity_pct` | 6 hours |
| `wind_speed_kmh` | 6 hours |

**Note:** Flatline detection uses `flag_only` enforcement (does not set NaN), since flat values may be physically valid (e.g., calm wind, stable temperature).

## Enforcement Actions

Configured under `enforcement` in `cleaning-config.json`:

| Check Type | Action | Effect |
|---|---|---|
| `out_of_range` | `set_nan` | Value replaced with NaN |
| `rate_of_change` | `set_nan` | Flagged value replaced with NaN |
| `cross_variable` | `set_nan` | Flagged value replaced with NaN |
| `flatline` | `flag_only` | Flag added to `_quality_flags`, value preserved |
| `fwi_out_of_range` | `set_nan` | FWI output outside valid range → NaN |
| *(default)* | `set_nan` | Any unconfigured check type |

## Quality Flags in Output

Every row in `station_hourly.csv` carries:
- `_quality_flags` — JSON array of applied flags (e.g., `["value_range:air_temperature_c","rate_of_change:wind_speed_kmh"]`), produced by `enforce_quality()` in `quality.py`

Per-variable quality and provenance columns are created by cross-station imputation (`cross_station_impute.py`) and only appear for variables that received cross-station fills:
- `{var}_qf` — per-variable quality flag (0 = observed, 1 = synthetic, 2 = uncertain, 9 = failed)
- `{var}_method` — transfer method used (e.g., `VP_CONTINUITY`, `HEIGHT_SCALED`, `SPATIAL_PROXY`)
- `{var}_src` — donor source identifier (e.g., `INTERNAL:cavendish`, `ECCC:8300300`)

## Quality Enforcement Report

`data/processed/quality_enforcement_report.csv` — one row per enforced action:
- station, timestamp_utc, check_type, variable, original_value, action, rule
