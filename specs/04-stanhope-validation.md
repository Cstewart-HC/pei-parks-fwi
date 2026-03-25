# Phase 4: Stanhope Validation Integration

## Context

Stanhope (ECCC station 8300590) is the reference station. The parks group
gets daily FWI numbers from ECCC, so we must compute FWI for Stanhope too.
Currently Stanhope has hourly output but no daily resampling and no FWI
indices.

Stanhope's role is validation: where Stanhope and Greenwich overlap
temporally, we should be able to compare FWI values.

Phase 1 wires Stanhope daily + FWI into the pipeline. This phase focuses
on validation reporting and cross-comparison.

## Goal

Stanhope is a first-class station with daily output, FWI indices, and
serves as the cross-validation reference against Greenwich.

## Scope

1. Stanhope daily resampling uses the same `AggregationPolicy` as other
   stations (from `resampling.py`).

2. Compute FWI indices for Stanhope using latitude 46.4°N.

3. Stanhope daily CSV has columns: station, timestamp_utc, ffmc, dmc, dc,
   isi, bui, fwi plus the standard measurement columns.

4. Validation comparison: where Stanhope and Greenwich overlap temporally,
   log FWI divergence statistics (mean absolute difference, max difference)
   to a `data/processed/stanhope_validation.csv` artifact.

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-VAL-1 | `data/processed/stanhope/station_daily.csv` has ffmc, dmc, dc, isi, bui, fwi columns |
| AC-VAL-2 | Stanhope FWI values are physically plausible (FFMC 0-101, DMC 0-300, DC 0-800, ISI 0-∞, BUI 0-∞, FWI 0-∞) |
| AC-VAL-3 | `data/processed/stanhope_validation.csv` exists with overlap-period FWI comparison between Stanhope and Greenwich |
| AC-VAL-4 | Validation report includes mean absolute difference and count of overlapping days |
| AC-VAL-5 | Full test suite passes: `.venv/bin/pytest tests/ -q` |

## Exit Gate

```bash
.venv/bin/pytest tests/test_pipeline_execution.py::TestAC_PIPE_4_StanhopeValidation -q
```
