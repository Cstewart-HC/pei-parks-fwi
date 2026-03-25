# Phase 6: Determinism + Reproducibility

## Context

A pipeline is only useful if it produces identical output on re-run from
the same raw data. Currently there's no guarantee of this.

## Goal

`python cleaning.py` produces byte-identical CSV outputs on re-run from
the same raw data.

## Scope

1. Sort all outputs by (station, timestamp_utc) before writing.

2. Deterministic column ordering in CSVs — use the canonical column list
   defined in Phase 1, applied everywhere.

3. Pipeline manifest includes:
   - list of input files with mtimes
   - pipeline run timestamp
   - python version
   - total row counts per station

4. Add `--force` flag to overwrite existing outputs.
   Default behavior: skip a station if its output CSV is newer than all
   its input source files.

5. Fix the `test_cleaning_py_runs` race condition with pytest-xdist:
   mark the subprocess-based test as serial or mock the pipeline call.

6. Ensure `pandas` doesn't introduce non-determinism:
   - `sort=False` on all groupby operations
   - explicit `float_format` in `to_csv()` to avoid platform-dependent formatting

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-DET-1 | Two consecutive runs of `python cleaning.py` produce byte-identical CSV outputs (verified by checksum) |
| AC-DET-2 | All CSVs have columns in deterministic order (canonical schema order) |
| AC-DET-3 | All CSVs are sorted by (station, timestamp_utc) |
| AC-DET-4 | Pipeline manifest exists with input file list and mtimes |
| AC-DET-5 | `--force` flag forces overwrite; default skips if output is newer |
| AC-DET-6 | `test_cleaning_py_runs` passes reliably (no race condition) |
| AC-DET-7 | Full test suite passes: `.venv/bin/pytest tests/ -q` |

## Exit Gate

```bash
.venv/bin/pytest tests/test_data_refresh.py tests/test_pipeline_execution.py::TestAC_PIPE_6_Determinism -q
```
