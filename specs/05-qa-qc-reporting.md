# Spec 05: QA/QC Reporting

**Phase:** 5  
**Status:** Pending  
**Depends on:** Phase 3 (Pipeline Integration)

---

## Goal

Wire `qa_qc.py` into the pipeline to generate a quality assurance report for every run.

---

## Deliverables

### 1. QA/QC Report Integration

Add to `cleaning.py`:

```python
def generate_qa_qc_report(hourly: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    """Generate QA/QC report for all stations."""
    from pea_met_network.qa_qc import (
        missingness_summary,
        duplicate_timestamps,
        out_of_range_values,
        coverage_summary,
    )
    
    report_rows = []
    
    for station in hourly["station"].unique():
        station_hourly = hourly[hourly["station"] == station]
        station_daily = daily[daily["station"] == station]
        
        report_rows.append({
            "station": station,
            "hourly_rows": len(station_hourly),
            "daily_rows": len(station_daily),
            "date_range_start": station_hourly["timestamp_utc"].min(),
            "date_range_end": station_hourly["timestamp_utc"].max(),
            **missingness_summary(station_hourly),
            **duplicate_timestamps(station_hourly),
            **out_of_range_values(station_hourly),
        })
    
    return pd.DataFrame(report_rows)
```

### 2. QA/QC Report Schema

`data/processed/qa_qc_report.csv`:

```csv
station,hourly_rows,daily_rows,date_range_start,date_range_end,
missing_pct_air_temperature_c,missing_pct_relative_humidity_pct,missing_pct_rain_mm,
duplicate_count,out_of_range_temp_count,out_of_range_rh_count,out_of_range_wind_count
```

### 3. Out-of-Range Thresholds

| Variable | Valid Range |
|----------|-------------|
| `air_temperature_c` | -50 to 60 |
| `relative_humidity_pct` | 0 to 105 |
| `wind_speed_kmh` | 0 to 200 |
| `rain_mm` | 0 to 500 |
| `wind_direction_deg` | 0 to 360 |

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-QC-1 | `data/processed/qa_qc_report.csv` exists after pipeline run |
| AC-QC-2 | Report has row for every station processed |
| AC-QC-3 | Report includes per-variable missingness percentages |
| AC-QC-4 | Report includes duplicate timestamp counts per station |
| AC-QC-5 | Report flags out-of-range values (temp -50/60°C, RH 0/105%, wind < 0) |
| AC-QC-6 | Report is added to pipeline manifest artifacts |

---

## Exit Gate

```bash
pytest tests/test_v2_pipeline.py::TestAC_PIPE_5_QAQCReporting -v
```

All tests must pass.
