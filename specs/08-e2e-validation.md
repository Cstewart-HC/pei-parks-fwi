# Phase 8: Full End-to-End Validation

## Context

This is the final gate. All 7 preceding phases must be complete.
The full pipeline must run clean on all raw data, producing complete
outputs for all 6 stations across all file formats.

## Goal

Verify the entire pipeline runs end-to-end: every raw file discovered,
routed through its adapter, concatenated, deduplicated, resampled,
imputed, and enriched with FWI — with full audit trail.

## Scope

1. Run full pipeline on all raw data (all 6 stations, all formats:
   CSV, XLSX, XLE, JSON, ECCC Stanhope).

2. Verify all 6 stations have:
   - `data/processed/<station>/station_hourly.csv`
   - `data/processed/<station>/station_daily.csv`
   - FWI columns populated (stations with sufficient RH/temp/rain/wind data)

3. Verify all pipeline artifacts exist:
   - `data/processed/imputation_report.csv`
   - `data/processed/qa_qc_report.csv`
   - `data/processed/stanhope_validation.csv`
   - Pipeline manifest with full file inventory

4. Verify manifest reports 0 unprocessed files across `data/raw/`.

5. Cross-validate Stanhope FWI against physical plausibility.

6. Generate a final coverage report summarizing:
   - rows per station, date ranges, completeness percentage

7. Full test suite passes with zero failures.

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-E2E-1 | All 6 stations have hourly + daily CSVs in `data/processed/` |
| AC-E2E-2 | Greenwich and Stanhope have populated FWI columns in daily output |
| AC-E2E-3 | North Rustico, Stanley Bridge, Tracadie, Cavendish have FWI columns where RH data is available |
| AC-E2E-4 | Imputation report, QA/QC report, and Stanhope validation report all exist |
| AC-E2E-5 | Manifest reports 0 unprocessed files |
| AC-E2E-6 | `.venv/bin/pytest tests/ -q` — all tests pass, zero failures |
| AC-E2E-7 | No warnings about unprocessed files — every raw file accounted for |

## Exit Gate

```bash
.venv/bin/pytest tests/ -q
```
