# Phase 3: Imputation Wired + Audit Trail

## Context

`imputation.py` exists with a conservative gap-filling strategy and audit
trail support. It must be wired into the pipeline after concat+dedup and
before FWI computation.

The key requirement: imputation runs on the full concatenated per-station
DataFrame (not per-file), and the audit trail must be complete.

## Goal

Imputation runs as part of the pipeline with a full per-station, per-variable
audit trail. No silent overwrites.

## Scope

1. Verify `imputation.py` handles multi-station DataFrames correctly
   (groups by station before imputing).

2. Ensure imputation runs AFTER concat+dedup in the pipeline flow.

3. FWI computation runs on imputed data (not raw gaps).

4. Generate `data/processed/imputation_report.csv` with columns:
   - station, variable, method, values_affected, time_range_start, time_range_end

5. Add imputation coverage metric to QA/QC output:
   - per-station, per-variable: pre-impute NaN count, post-impute NaN count,
     values filled, fill percentage.

6. Verify imputation doesn't fill long gaps (>24h for hourly, >3 days for daily)
   — these should remain NaN per the conservative policy.

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-IMP-1 | `data/processed/imputation_report.csv` exists after pipeline run |
| AC-IMP-2 | Report contains rows for every station that has gaps |
| AC-IMP-3 | No station has 100% NaN in any required FWI input column after imputation |
| AC-IMP-4 | Long gaps (>24h hourly, >3d daily) remain NaN (not imputed) |
| AC-IMP-5 | Imputation report values match actual DataFrame state (auditable) |
| AC-IMP-6 | Full test suite passes: `.venv/bin/pytest tests/ -q` |

## Exit Gate

```bash
.venv/bin/pytest tests/test_imputation.py tests/test_pipeline_execution.py::TestAC_PIPE_3_ImputationAudit -q
```
