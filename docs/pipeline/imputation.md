# Imputation

Two layers: intra-station (within each station's own time series) and cross-station (borrowing from donor stations).

## Intra-Station Imputation

Implemented as `impute()` in `src/pea_met_network/cleaning.py`. Applied during Pass 1 after quality enforcement.

> **Note:** `src/pea_met_network/imputation.py` exists as a standalone module with configurable gap thresholds (`ImputationConfig`) but is not used by the pipeline — only by tests. The production `impute()` function uses a fixed 6-hour threshold.

### Gap-Length Strategy

| Gap Length | Method | Details |
|---|---|---|
| ≤ 6 hours | Linear interpolation | Bounded by neighboring valid values |
| > 6 hours | Preserve NaN | Long gaps left missing for cross-station fill |

The threshold (`max_gap_hours=6`) is hardcoded in `impute()`. Only meteorological columns are imputed: `air_temperature_c`, `relative_humidity_pct`, `wind_speed_kmh`, `dew_point_c`, `solar_radiation_w_m2`, `barometric_pressure_kpa`, `wind_direction_deg`.

### Audit Trail

Each filled gap produces an `AuditRecord`:
- station, variable, time_start, time_end, method, count_affected

Aggregated into `data/processed/imputation_report.csv`.

## Cross-Station Imputation

Implemented in `src/pea_met_network/cross_station_impute.py`. Applied during Pass 2 for target stations.

### Purpose

Fill gaps in stations that lack sensors or have sparse data by borrowing from physically nearby stations with overlapping records.

### Targets & Variables

| Target Station | Variables Filled | Why |
|---|---|---|
| stanley_bridge | RH, wind speed, temperature | No RH sensor |
| tracadie | RH, wind speed, temperature | No RH sensor |
| greenwich | RH, wind speed, temperature | Gap filling from external donors |
| north_rustico | wind speed | Gap filling |
| cavendish | wind speed | Gap filling |

### Blocked Donors

Stanley Bridge and Tracadie are **never used as donors** (100% missing RH makes them unreliable).

### Donor Priority

Each target/variable pair has up to 3 donors in priority order. Example for `rh` → `stanley_bridge`:

| Priority | Donor | Type |
|---|---|---|
| 1 | cavendish | Internal |
| 2 | charlottetown_a | External (ECCC) |
| 3 | north_rustico | Internal |

Internal donors come from other PEINP stations (in-memory). External donors are ECCC climate stations fetched via API with on-disk caching.

### Transfer Methods

- **Temperature**: Direct transfer with outlier caps (±2°C warm bias, ±3°C cool bias)
- **RH**: Derived via vapor pressure transfer — donor's temperature + dew point → actual vapor pressure → target RH using target temperature
- **Wind speed**: Direct transfer with height correction (log wind profile, α=0.14, default 10m anemometer height)

### Safety Constraints

| Constraint | Value | Rationale |
|---|---|---|
| Max gap hours | 3 | Only fill short gaps |
| Warm bias cap | 2.0 °C | Reject warm outliers |
| Cool bias cap | 3.0 °C | Allow slightly larger cool spread |
| Min overlap for height correction | 168 hours | Need ≥7 days of concurrent data |
| Empirical height correction | Enabled | Uses overlapping period to estimate actual height ratio |

### External ECCC Donors

| Station | Climate ID | Stn ID | Anemometer Height |
|---|---|---|---|
| Charlottetown A | 8300300 | 6526 | 10.0 m |
| St. Peters | 8300562 | 41903 | 10.0 m |
| Harrington CDA CS | 830P001 | 30308 | 10.0 m |

### Audit Trail

`data/processed/cross_station_imputation_audit.csv` — one row per imputed value:
- station, timestamp_utc, variable, imputed_value, quality_flag, source, method, donor_priority

### Quality Flag Propagation

After cross-station imputation, `propagate_fwi_quality_flags()` ensures downstream FWI components inherit flags from imputed inputs.
