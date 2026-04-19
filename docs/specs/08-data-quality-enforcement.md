# Spec 08: Data Quality Enforcement

**Phase:** 8  
**Status:** Draft  
**Depends on:** Phase 5 (QA/QC Reporting), Phase 7 (E2E Validation)

---

## Goal

Add a quality enforcement step to the pipeline that detects and neutralizes impossible values **before** imputation and FWI calculation. Currently the pipeline detects out-of-range values (Phase 5) but never acts on them — they pass straight through to imputation and FWI, producing garbage outputs.

---

## Context

### What exists today

The pipeline flow is:

```
load → dedup → resample → impute → FWI → output
```

`qa_qc.py` has `out_of_range_values()` which **detects** impossible values, and `generate_qa_qc_report()` which **counts** them. Neither function modifies the data. The QA/QC report is generated **after** all processing, so its findings are informational only.

### The problem

- Out-of-range temperatures (e.g., -999°C) pass through imputation, which interpolates **across** the impossible value
- FWI calculations produce garbage from garbage inputs (e.g., RH = -30%)
- Cross-variable inconsistencies (e.g., rain with low humidity) go undetected
- Stuck sensors (flatline) are not flagged
- Rate-of-change anomalies are not detected

### Non-FWI columns

The following columns are **not** used for FWI calculation and are treated as **pass-through** — they are preserved in output but receive no quality enforcement:

| Column | Status |
|---|---|
| `dew_point_c` | Pass-through, not quality-controlled |
| `solar_radiation_w_m2` | Pass-through, not quality-controlled |
| `barometric_pressure_kpa` | Pass-through, not quality-controlled |
| `wind_direction_deg` | Pass-through, not quality-controlled |
| `wind_gust_speed_kmh` | Pass-through, not quality-controlled |
| `accumulated_rain_mm` | Pass-through, not quality-controlled |
| `visibility_km` | Pass-through, not quality-controlled |
| `station_pressure_kpa` | Pass-through, not quality-controlled |

These columns pass through the pipeline unchanged. They will be documented in the data dictionary as "not quality-controlled."

---

## Deliverables

### 1. Cleaning Config (`docs/cleaning-config.json`)

A version-controlled JSON config that defines all quality rules. This is the single source of truth for what the enforcement step checks and how it responds.

```json
{
  "version": 1,
  "date_range": {
    "start": "2023-04-01T00:00:00Z",
    "end": null
  },
  "value_ranges": {
    "air_temperature_c": [-50.0, 60.0],
    "relative_humidity_pct": [0.0, 105.0],
    "wind_speed_kmh": [0.0, 200.0],
    "rain_mm": [0.0, 500.0]
  },
  "fwi_output_ranges": {
    "ffmc": [0.0, 101.0],
    "dmc": [0.0, null],
    "dc": [0.0, null],
    "isi": [0.0, null],
    "bui": [0.0, null],
    "fwi": [0.0, null]
  },
  "cross_variable_checks": {
    "rain_rh_correlation": {
      "enabled": true,
      "rule": "rain_mm > 0 implies relative_humidity_pct >= 70",
      "variables": ["rain_mm", "relative_humidity_pct"],
      "threshold_rh": 70.0
    }
  },
  "rate_of_change": {
    "window_hours": 1,
    "max_delta": {
      "air_temperature_c": 8.0,
      "relative_humidity_pct": 30.0,
      "wind_speed_kmh": 40.0
    }
  },
  "flatline": {
    "enabled": true,
    "variables": ["air_temperature_c", "relative_humidity_pct", "wind_speed_kmh"],
    "threshold_hours": 6
  },
  "enforcement": {
    "default_action": "set_nan",
    "actions": {
      "out_of_range": "set_nan",
      "fwi_out_of_range": "set_nan",
      "cross_variable": "set_nan",
      "rate_of_change": "set_nan",
      "flatline": "flag_only"
    }
  }
}
```

**Notes on config design:**
- `null` upper bound means no upper limit (only ≥ 0 check)
- `flatline` uses `flag_only` — adds a quality flag column but does not modify values
- Station-specific ranges can be added later as nested keys under `value_ranges`
- The config is read by the enforcement step at runtime, not hardcoded

### 2. Enforcement Step (`src/pea_met_network/quality.py`)

New module: `enforce_quality(df, config) -> tuple[pd.DataFrame, list[dict]]`

Returns the cleaned DataFrame and a list of action records (what was changed, why, where).

**Pipeline insertion point:**

```
load → dedup → resample → ★ enforce_quality ★ → impute → FWI → enforce_fwi_outputs → output
```

Quality enforcement runs **after** resample and **before** impute. This ensures:
- Impossible values don't pollute interpolation
- Imputation only fills gaps between valid values
- FWI receives clean inputs

A second enforcement pass runs **after** FWI calculation to validate FWI output ranges.

### 3. Quality Action Records

Each enforcement action produces a structured record:

```python
{
    "station": "greenwich",
    "timestamp_utc": "2023-07-15T14:00:00Z",
    "check_id": "VR001",
    "check_type": "value_range",
    "variable": "air_temperature_c",
    "original_value": -999.0,
    "action": "set_nan",
    "rule": "air_temperature_c must be in [-50.0, 60.0]"
}
```

For `flag_only` actions:

```python
{
    "station": "greenwich",
    "timestamp_utc": "2023-08-01T00:00:00Z",
    "check_id": "FL001",
    "check_type": "flatline",
    "variable": "air_temperature_c",
    "original_value": 22.5,
    "action": "flagged",
    "rule": "air_temperature_c unchanged for 8 consecutive hours (threshold: 6)"
}
```

### 4. Quality Flag Column

The enforcement step adds a `_quality_flags` column (JSON string) to the output CSV:

```csv
timestamp_utc,station,air_temperature_c,...,_quality_flags
2023-07-15T14:00:00Z,greenwich,,...,"[{\"check\":\"VR001\",\"variable\":\"air_temperature_c\",\"action\":\"set_nan\"}]"
2023-08-01T00:00:00Z,greenwich,22.5,...,"[{\"check\":\"FL001\",\"variable\":\"air_temperature_c\",\"action\":\"flagged\"}]"
```

### 5. Quality Enforcement Report

New output artifact: `data/processed/quality_enforcement_report.csv`

```
station,check_id,check_type,variable,count_affected,action
greenwich,VR001,value_range,air_temperature_c,3,set_nan
cavendish,RoC001,rate_of_change,air_temperature_c,1,set_nan
greenwich,FL001,flatline,air_temperature_c,8,flagged
```

Registered in pipeline manifest as a new artifact type.

### 6. Enhanced QA/QC Report

The existing `qa_qc_report.csv` gains new columns:

| Column | Description |
|---|---|
| `quality_enforced_count` | Total values set to NaN by enforcement |
| `quality_flagged_count` | Total values flagged but not modified |
| `out_of_range_pre_enforcement` | Out-of-range count BEFORE enforcement (historical comparison) |
| `out_of_range_post_enforcement` | Out-of-range count AFTER enforcement (should be 0 for enforced vars) |

### 7. Date Range Truncation

The pipeline discards all records before `2023-04-01T00:00:00Z` (the date when all 6 PEINP stations have continuous coverage). This happens at the load stage — records outside the config's `date_range` are filtered out immediately after loading.

Records from Stanhope (ECCC) before April 2023 exist but represent a period when the PEINP network was incomplete. Processing them wastes time and produces outputs for a period we can't analyze at the network level.

---

## Check Catalog

### Value Range Checks

| ID | Variable | Valid Range | Action |
|---|---|---|---|
| VR001 | `air_temperature_c` | [-50.0, 60.0] | set_nan |
| VR002 | `relative_humidity_pct` | [0.0, 105.0] | set_nan |
| VR003 | `wind_speed_kmh` | [0.0, 200.0] | set_nan |
| VR004 | `rain_mm` | [0.0, 500.0] | set_nan |

### FWI Output Checks

| ID | Variable | Valid Range | Action |
|---|---|---|---|
| FWI001 | `ffmc` | [0.0, 101.0] | set_nan |
| FWI002 | `dmc` | [0.0, ∞) | set_nan |
| FWI003 | `dc` | [0.0, ∞) | set_nan |
| FWI004 | `isi` | [0.0, ∞) | set_nan |
| FWI005 | `bui` | [0.0, ∞) | set_nan |
| FWI006 | `fwi` | [0.0, ∞) | set_nan |

### Cross-Variable Checks

| ID | Check | Variables | Action |
|---|---|---|---|
| CV001 | Rain requires high humidity | `rain_mm > 0 → RH >= 70%` | set_nan |

### Rate-of-Change Checks

| ID | Variable | Max Δ (1h) | Action |
|---|---|---|---|
| RoC001 | `air_temperature_c` | 8.0°C/hour | set_nan |
| RoC002 | `relative_humidity_pct` | 30%/hour | set_nan |
| RoC003 | `wind_speed_kmh` | 40 km/h/hour | set_nan |

### Flatline Checks

| ID | Variable | Threshold | Action |
|---|---|---|---|
| FL001 | `air_temperature_c` | 6 consecutive hours | flag_only |
| FL002 | `relative_humidity_pct` | 6 consecutive hours | flag_only |
| FL003 | `wind_speed_kmh` | 6 consecutive hours | flag_only |

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-DQ-1 | `docs/cleaning-config.json` exists with all config sections populated |
| AC-DQ-2 | `src/pea_met_network/quality.py` implements `enforce_quality()` function |
| AC-DQ-3 | Pipeline inserts quality enforcement after resample, before impute |
| AC-DQ-4 | Pipeline inserts FWI output enforcement after `calculate_fwi()` |
| AC-DQ-5 | `data/processed/quality_enforcement_report.csv` exists after pipeline run |
| AC-DQ-6 | Quality enforcement report is registered in pipeline manifest |
| AC-DQ-7 | Output CSVs contain `_quality_flags` column |
| AC-DQ-8 | QA/QC report includes enforcement counts (pre/post) |
| AC-DQ-9 | All records before 2023-04-01 are excluded from output |
| AC-DQ-10 | `pytest tests/ -m 'not e2e'` passes (fast suite) |
| AC-DQ-11 | No out-of-range FWI inputs reach `calculate_fwi()` |
| AC-DQ-12 | Flatline detection produces flags but does not modify values |

---

## Exit Gate

```bash
pytest tests/ -m 'not e2e' -v
```

All tests must pass. The fast suite must complete in under 30 seconds.

---

## What This Phase Does NOT Do

- **Does not** add station-specific value ranges (deferred — start wide, tighten later)
- **Does not** modify non-FWI pass-through columns
- **Does not** change imputation logic
- **Does not** change FWI calculation logic
- **Does not** add step-change detection (CV015 from analysis — deferred)
- **Does not** remove existing tests or change Phase 1–7 acceptance criteria
