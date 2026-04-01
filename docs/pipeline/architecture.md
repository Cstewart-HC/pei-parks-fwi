# Pipeline Architecture

## Overview

Entry point: `src/pea_met_network/cleaning.py` → `run_pipeline()`
Invocation: `python -m pea_met_network --stations all`

Two-pass in-memory architecture keeps all donor station data resident for cross-station imputation without disk staging.

## Stations

| Station | Has RH Sensor | Has Solar | Has Wind Gust |
|---|---|---|---|
| greenwich | ✅ | ✅ | ✅ |
| cavendish | ✅ | ✅ | ✅ |
| north_rustico | ✅ | ✅ | ✅ |
| stanhope | ✅ | ❌ | ❌ |
| stanley_bridge | ❌ | ✅ | ✅ |
| tracadie | ❌ | ✅ | ✅ |

## Two-Pass Design

### Pass 1 — Per-Station Processing

Stations are processed in **topological order** (donors before targets) so cross-station donors are ready in memory.

1. **Discover** — `discover_raw_files()` scans `data/raw/` subdirectories
2. **Load** — `load_station_files()` routes files through adapter registry, normalizes columns, concatenates
3. **Dedup** — removes duplicate timestamps
4. **Resample** — `resample_hourly()` mean-aggregates to hourly intervals
5. **Truncate** — drops records before configured start date (2023-04-01)
6. **Quality enforcement** — `enforce_quality()` applies all checks from `cleaning-config.json`
7. **Intra-station imputation** — `impute()` fills short/medium gaps

Result: `hourly_frames[station]` dict holds all stations in memory (~150 MB total).

### Pass 2 — Cross-Station + FWI + Outputs

8. **Cross-station imputation** — `impute_cross_station()` for targets using in-memory donors
9. **FWI calculation** — hourly or compliant mode
10. **FWI output enforcement** — validates FWI ranges
11. **Write outputs** — per-station CSVs + reports

### Fallback: Disk-Based Staging

When running a subset of stations (not `--stations all`), the pipeline falls back to topologically-ordered serial processing with parquet staging files in `data/processed/.donor_staging/`.

## Processing Order

Kahn's algorithm sorts donors before targets, but a mutual dependency cycle exists between cavendish and north_rustico (each donates wind to the other). Stations with zero in-degree process first; remaining stations are appended in `ALL_STATIONS` list order.

Actual order:
```
stanhope → greenwich, cavendish, north_rustico, stanley_bridge, tracadie
```

## Execution Requirements

- Python 3.11, pandas 3.x, numpy 2.x
- `OPENBLAS_NUM_THREADS=1 OMP_NUM_THREADS=1` to avoid OpenBLAS memory errors
- Venv: `.venv/bin/python`

## Configuration

All tuning lives in `docs/cleaning-config.json`. See individual docs for per-section details.
