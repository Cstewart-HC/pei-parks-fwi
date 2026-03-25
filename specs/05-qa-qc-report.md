# Phase 5: QA/QC Report Generation

## Context

`qa_qc.py` has reporting functions (missingness, duplicates, out-of-range,
coverage) but they are standalone — not wired into the pipeline.
The pipeline should produce a QA/QC summary artifact on every run.

## Goal

Pipeline produces a comprehensive QA/QC report for every run, covering
all stations.

## Scope

1. Wire `qa_qc.py` reporting into `cleaning.py` pipeline (after imputation,
   before FWI computation).

2. Generate `data/processed/qa_qc_report.csv` per pipeline run with columns:
   - station, variable, total_rows, missing_count, missing_pct,
     duplicate_timestamps, out_of_range_count, date_range_start, date_range_end

3. QA/QC runs on the cleaned (post-imputation, pre-FWI) hourly data.

4. Include QA/QC metadata in the pipeline manifest.

5. Report includes station-level summary: total records, date range,
   variables present, variables with >50% missingness flagged.

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-QC-1 | `data/processed/qa_qc_report.csv` exists after pipeline run |
| AC-QC-2 | Report has rows for every station processed |
| AC-QC-3 | Report includes per-variable missingness percentages |
| AC-QC-4 | Duplicate timestamp counts are reported per station |
| AC-QC-5 | Out-of-range values are flagged (temp outside -50/60°C, RH outside 0/105%, wind speed < 0) |
| AC-QC-6 | Full test suite passes: `.venv/bin/pytest tests/ -q` |

## Exit Gate

```bash
.venv/bin/pytest tests/test_qa_qc.py tests/test_pipeline_execution.py::TestAC_PIPE_5_QAQCReport -q
```
