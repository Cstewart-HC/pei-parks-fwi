# Ingestion — Raw Data & Adapters

## Source Data Layout

```
data/raw/
├── eccc/          # ECCC reference stations (CSV, JSON)
├── licor/         # Licor loggers (CSV, XLSX)
└── peinp/         # PEINP station data (CSV, XLSX, XLE)
```

## Data Sources

### ECCC (`data/raw/eccc/`)

| Station | Climate ID | Type |
|---|---|---|
| Stanhope | — | Internal station, primary ECCC data |
| Charlottetown A | 8300300 | External donor (cross-station imputation) |
| St. Peters | 8300562 | External donor (cross-station imputation) |
| Harrington CDA CS | 830P001 | External donor (cross-station imputation) |

ECCC data fetched via `src/pea_met_network/eccc_api.py` with on-disk caching.

### Licor (`data/raw/licor/`)

HOBOlink/Licor loggers for Greenwich, Cavendish, North Rustico. Mixed column naming:
- Modern CSVs: lowercase with units in parentheses — `Temp (C)`, `RH (%)`
- Old North Rustico CSVs (Dec 2022–Mar 2023): CamelCase — `Temperature_C`, `Solar_Radiation_Wm2`, `Barometric_Pressure_kPa`

### PEINP (`data/raw/peinp/`)

Parks Canada station data for all 6 stations. Mixed formats (CSV, XLSX, XLE).

## Adapter Registry

`src/pea_met_network/adapters/registry.py` routes by file extension:

| Extension | Adapter | Source |
|---|---|---|
| `.csv` | `CSVAdapter` | `adapters/csv_adapter.py` |
| `.xlsx` | `XLSXAdapter` | `adapters/xlsx_adapter.py` |
| `.xle` | `XLEAdapter` | `adapters/xle_adapter.py` |
| `.json` | `JSONAdapter` | `adapters/json_adapter.py` |

All adapters inherit from `BaseAdapter` (`adapters/base.py`) and apply the same column rename logic.

## Column Normalization

`src/pea_met_network/adapters/column_maps.py` — 60 unique raw column prefixes mapped to canonical names.

**Key functions:**
- `extract_prefix(col)` — strips everything after the first parenthesis to get the raw prefix
- `rename_columns(df)` — maps prefixes to canonical names

**Canonical columns** (implicitly defined across `cleaning.py` and `column_maps.py`):
- Required: `station`, `timestamp_utc`
- FWI-required: `air_temperature_c`, `relative_humidity_pct`, `wind_speed_kmh`, `rain_mm`
- Optional: `wind_direction_deg`, `wind_gust_speed_kmh`, `dew_point_c`, `solar_radiation_w_m2`, `barometric_pressure_kpa`

**Skip prefixes** (columns always dropped):
- `Water Flow` — 20 files, 100% null across NR + Stanley Bridge
- `Accumulated Rain` — accumulated precipitation totals (not instantaneous)
- `Diff Pressure` — differential pressure readings

## Column Audit Status

All 60 raw column prefixes across 40 CSV headers verified:
- ✅ All mapped or intentionally skipped
- ✅ CamelCase mappings added for old NR Licor CSVs (3 mappings, ~21k rows each)
- ✅ Dead keys removed (`Precip (mm)`, `Gust (km/h)`, `Pressure (hPa)` — prefixes didn't match after `extract_prefix()`)

## Output Column Stripping

`OUTPUT_DROP_COLUMNS` in `cleaning.py` — 12 columns removed from final CSV output:
- Non-meteorological: `battery_v`, `water_level_m`, `water_pressure_kpa`, `water_temperature_c`
- Pipeline metadata: `source_file`, `schema_family`
- Raw slip-throughs: `Hmdx`, `Visibility (km)`, `Wind Chill`, `LEVEL`, `TEMPERATURE`, `ms`
