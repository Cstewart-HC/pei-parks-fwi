# Phase 1: Pipeline Refactor — Adapter Architecture

## Context

`cleaning.py` currently processes raw files per station but has a structural
flaw: it iterates files one at a time and materializes resampled outputs
per-file, overwriting the output CSV each time. The `main()` compensates by
reading back each hourly CSV and concatenating — fragile and redundant.

The module library (`normalized_loader`, `resampling`, `imputation`, `fwi`,
`manifest`, `qa_qc`, `stanhope_cache`) is solid. The problem is the
orchestration script that wires them together.

**Critical design constraint:** The pipeline must have a SINGLE entry point
with format adapters. No file format may be silently skipped. Every file
under `data/raw/` must be accounted for.

## Architecture

```
                 SINGLE ENTRY POINT: cleaning.py
                          │
                   discover_raw_files()
                   (all formats: csv, xlsx, xle, json)
                          │
          ┌───────────────┼───────────────┬───────────────┐
          ▼               ▼               ▼               ▼
     .csv adapter    .xlsx adapter    .xle adapter    .json adapter
     (PEINP CSVs)    (openpyxl)      (Solinst XML)   (Licor Cloud)
          │               │               │               │
          └───────────────┴───────────────┴───────────────┘
                          │
                   canonical DataFrame
                   (single exit schema)
                          │
              concat → dedup → resample → impute → FWI → QA/QC → write
```

## Canonical Output Schema

Every adapter must produce a DataFrame with these columns (extras allowed):

| Column | Type | Required | Aggregation |
|--------|------|----------|-------------|
| station | str | ✅ | first |
| timestamp_utc | datetime64[ns, UTC] | ✅ | — |
| air_temperature_c | float | ✅ for FWI | mean |
| relative_humidity_pct | float | ✅ for FWI | mean |
| wind_speed_kmh | float | ✅ for FWI | mean |
| wind_direction_deg | float | — | first |
| wind_gust_speed_kmh | float | — | max |
| rain_mm | float | ✅ for FWI | sum |
| dew_point_c | float | — | mean |
| solar_radiation_w_m2 | float | — | mean |
| barometric_pressure_kpa | float | — | mean |
| water_level_m | float | — | mean |
| water_pressure_kpa | float | — | mean |
| water_temperature_c | float | — | mean |

## Scope

1. Rewrite `cleaning.py` main flow:
   - Single entry: `python cleaning.py` processes ALL stations, ALL formats
   - New module `src/pea_met_network/adapters/` with:
     - `__init__.py` — adapter registry and router
     - `csv_adapter.py` — wraps existing `normalized_loader.py`
     - `xlsx_adapter.py` — uses pandas `read_excel` (new)
     - `xle_adapter.py` — Solinst XLE XML parser (new)
     - `json_adapter.py` — wraps existing `licor_to_csv.py` logic (new)
   - Each adapter function signature: `load_file(path: Path, station: str) -> pd.DataFrame`
   - All adapters output the canonical schema above
   - Router: `route_by_extension(ext: str) -> AdapterFn`

2. New pipeline flow in `cleaning.py`:
   ```
   discover_raw_files() → group by station → for each file: adapter.load()
   → concat all per-station DataFrames → dedup timestamps
   → resample_hourly() → resample_daily() → impute() → FWI() → write
   ```

3. File discovery: `build_raw_manifest()` must find ALL files
   (`.csv`, `.xlsx`, `.xle`, `.json`) and log them. Zero unprocessed files
   allowed — if an adapter doesn't exist for a format, that's a HARD ERROR,
   not a warning.

4. Stanhope path:
   - Keep `stanhope_cache.py` download/cache mechanism as-is
   - Add daily resampling for Stanhope
   - Compute FWI indices for Stanhope (latitude ~46.4)
   - Stanhope adapter wraps existing `normalize_stanhope_hourly()`

5. CLI interface:
   - `--stations` flag to process a subset
   - `--dry-run` flag that reports what would be processed without writing
   - `--verbose` for per-file logging

6. Pipeline stays serial, pandas-based, single-process.

## What NOT to Change

- `resampling.py` — solid, well-tested
- `fwi.py` — Van Wagner implementation is correct
- `imputation.py` — conservative strategy is right
- `stanhope_cache.py` — ECCC download/cache works
- `qa_qc.py` — reporting functions are good
- Existing tests that still pass

## Files to Create

- `src/pea_met_network/adapters/__init__.py`
- `src/pea_met_network/adapters/csv_adapter.py`
- `src/pea_met_network/adapters/xlsx_adapter.py`
- `src/pea_met_network/adapters/xle_adapter.py`
- `src/pea_met_network/adapters/json_adapter.py`

## Files to Modify

- `cleaning.py` — complete rewrite of main flow
- `src/pea_met_network/normalized_loader.py` — extract into csv_adapter
- `src/pea_met_network/manifest.py` — add xlsx/xle/json discovery
- `src/pea_met_network/resampling.py` — add water-level columns to AggregationPolicy

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-PIPE-1 | `python cleaning.py --dry-run` runs without error and reports every file under `data/raw/` with its assigned adapter |
| AC-PIPE-2 | `python cleaning.py --stations stanhope` produces hourly + daily CSVs with FWI columns |
| AC-PIPE-3 | No file is silently skipped — unknown formats raise a hard error |
| AC-PIPE-4 | Each adapter produces a DataFrame matching the canonical schema |
| AC-PIPE-5 | Duplicate timestamps within a station are logged and deduplicated (keep first) |
| AC-PIPE-6 | `--stations` flag processes only the specified stations |
| AC-PIPE-7 | Pipeline manifest lists all input files with format, adapter, and status |
| AC-PIPE-8 | Full test suite passes: `.venv/bin/pytest tests/ -q` |

## Exit Gate

```bash
.venv/bin/pytest tests/test_pipeline_execution.py::TestAC_PIPE_1_PipelineRuns -q
```
