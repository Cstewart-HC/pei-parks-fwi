# Spec 04: Stanhope Validation

**Phase:** 4  
**Status:** Pending  
**Depends on:** Phase 3 (Pipeline Integration)

---

## Goal

Stanhope (ECCC government station) serves as a validation reference for the local PEINP stations. Compare FWI values where temporal overlap exists.

---

## Background

- Stanhope uses ECCC schema, processed through `csv_adapter` like any other station
- ECCC data is quality-controlled by Environment Canada
- Stanhope is the closest government weather station to PEINP sites
- We calculate FWI for Stanhope to have a common comparison metric

---

## Deliverables

### 1. Stanhope Daily Output

Stanhope must have:
- `data/processed/stanhope/station_hourly.csv` (already exists)
- `data/processed/stanhope/station_daily.csv` (needs to be generated)
- FWI columns populated in both

### 2. Validation Report

`data/processed/stanhope_validation.csv`:

```csv
station,overlap_days,mean_abs_diff_ffmc,mean_abs_diff_dmc,mean_abs_diff_dc,mean_abs_diff_fwi
greenwich,245,2.3,5.1,12.4,1.8
stanley_bridge,180,3.1,6.2,15.3,2.1
...
```

### 3. Validation Script

`scripts/validate_stanhope.py`:

```python
def compare_fwi(station: str, stanhope_df: pd.DataFrame, station_df: pd.DataFrame):
    """Compare FWI values between Stanhope and a local station."""
    # Find overlapping dates
    # Calculate mean absolute difference for each FWI component
    # Return comparison metrics
```

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-VAL-1 | Stanhope has `station_daily.csv` with FWI columns |
| AC-VAL-2 | `data/processed/stanhope_validation.csv` exists |
| AC-VAL-3 | Validation report includes row for each local station with temporal overlap |
| AC-VAL-4 | Validation report includes: overlap day count, mean absolute difference for ffmc/dmc/dc/fwi |
| AC-VAL-5 | Greenwich-Stanhope comparison shows reasonable agreement (MAE < 5 for FWI) |

---

## Exit Gate

```bash
pytest tests/test_v2_pipeline.py::TestAC_PIPE_4_StanhopeValidation -v
```

All tests must pass.
