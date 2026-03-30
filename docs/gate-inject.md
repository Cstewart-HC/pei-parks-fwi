# Gate Injection — Phase 12 Pre-Flight

**Generated:** 2026-03-30T01:25:15.074310+00:00

## Lint (ruff)

**Exit code:** 1

```
90	E501	[ ] line-too-long
 5	W293	[-] blank-line-with-whitespace
 4	I001	[*] unsorted-imports
 3	C901	[ ] complex-structure
 3	F601	[ ] multi-value-repeated-key-literal
 2	F401	[-] unused-import
 2	F541	[*] f-string-missing-placeholders
 1	W292	[*] missing-newline-at-end-of-file
Found 110 errors.
[*] 11 fixable with the `--fix` option (5 hidden fixes can be enabled with the `--unsafe-fixes` option).
```

## Test Inventory

**Total test files:** 23

- `tests/test_analysis_notebook.py`
- `tests/test_cross_station_impute.py`
- `tests/test_data_refresh.py`
- `tests/test_deliverables.py`
- `tests/test_explore_smoke.py`
- `tests/test_fwi_vectors.py`
- `tests/test_imputation.py`
- `tests/test_manifest.py`
- `tests/test_materialize_resampled.py`
- `tests/test_normalized_loader.py`
- `tests/test_phase11_dual_mode_fwi.py`
- `tests/test_pipeline_execution.py`
- `tests/test_qa_qc.py`
- `tests/test_quality_enforcement.py`
- `tests/test_real_resampling_pipeline.py`
- `tests/test_redundancy.py`
- `tests/test_repo_shape.py`
- `tests/test_resampling_policy.py`
- `tests/test_smoke.py`
- `tests/test_stanhope_cache.py`
- `tests/test_uncertainty.py`
- `tests/test_v2_pipeline.py`
- `tests/test_validation.py`

**Total collected tests:** tests/test_validation.py: 8

## Validation State

**Verdict:** PASS
**Last reviewed:** 0131bec

## Working Tree

**Status:** DIRTY
