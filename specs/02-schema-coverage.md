# Phase 2: Format Adapters — CSV, XLSX, XLE Coverage

## Context

The PEINP archive has mixed file formats: CSV (majority), XLSX (Greenwich 2023),
and XLE (Stanley Bridge 2022 Solinst logger). Phase 1 created the adapter
architecture — this phase fills in every adapter so zero files are skipped.

The canonical schema is defined in Phase 1 (spec 01-pipeline-refactor.md).
Every adapter must output a DataFrame matching that schema.

## Goal

Every raw file in `data/raw/` loads through its adapter and produces the
canonical schema. Zero silent drops. Zero "unsupported format" warnings.

## Scope

### CSV Adapter (enhance existing `normalized_loader.py`)

1. Audit all CSV headers in `data/raw/peinp/` and document distinct schemas.
2. Add column mappings to `normalized_loader.py`:
   - `S-TMB-*` sensor prefix → `air_temperature_c`
   - Case-insensitive matching for wind columns
   - Whitespace-tolerant matching (`Wind gust  speed` → `wind_gust_speed_kmh`)
   - `wind_speed_kmh` derivation from `wind_speed_ms` where only m/s available (×3.6)
3. Add water-level columns (`water_level_m`, `water_pressure_kpa`,
   `water_temperature_c`, `barometric_pressure_kpa`) to canonical output.
4. Add `accumulated_rain_mm` to skip list (running total, not useful for FWI).
5. ECCC Stanhope CSVs handled by separate `stanhope_cache.py` adapter
   (already works).

### XLSX Adapter (new)

1. Use `pd.read_excel(engine='openpyxl')`.
2. Parse same HOBOware-style column headers as CSV adapter.
3. Reuse the same column rename logic (extract into shared helper).
4. Handle Greenwich 2023 `.xlsx` files as the primary test case.

### XLE Adapter (new)

1. Solinst XLE files are XML-based with a specific structure.
2. Parse with `xml.etree.ElementTree` (standard library, no extra deps).
3. Key XLE elements: `<Header>` (logger info, timezone), `<Data>` (time-series
   readings), `<Channel>` definitions (sensor names).
4. XLE timestamps are in logger local time — convert to UTC.
5. Stanley Bridge 2022 `.xle` files as the primary test case.
6. Map Solinst channel names to canonical columns:
   - "Level" → `water_level_m`
   - "Temperature" → `water_temperature_c` (note: XLE water temp, not air temp)
   - "Baro" / "Barometric Pressure" → `barometric_pressure_kpa`

### Shared Column Mapping

Extract the column rename logic from `normalized_loader.py` into a shared
module (`src/pea_met_network/adapters/column_maps.py`) so all three PEINP
adapters (CSV, XLSX, XLE) use the same mapping. This prevents drift.

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-FMT-1 | `build_raw_manifest()` discovers ALL files (csv, xlsx, xle) and reports 0 unprocessed |
| AC-FMT-2 | All PEINP CSVs normalize without error |
| AC-FMT-3 | Greenwich `.xlsx` files load and normalize to canonical schema |
| AC-FMT-4 | Stanley Bridge `.xle` files load and normalize to canonical schema |
| AC-FMT-5 | `wind_speed_kmh` derived from `wind_speed_ms` when only m/s available |
| AC-FMT-6 | Water-level columns appear in output for stations that have them |
| AC-FMT-7 | `accumulated_rain_mm` excluded from output |
| AC-FMT-8 | Shared column mapping module used by all PEINP adapters |
| AC-FMT-9 | Full test suite passes: `.venv/bin/pytest tests/ -q` |

## Exit Gate

```bash
.venv/bin/pytest tests/test_normalized_loader.py tests/test_pipeline_execution.py::TestAC_PIPE_2_SchemaCoverage -q
```
