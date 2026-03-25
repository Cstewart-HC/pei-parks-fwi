# Spec 03: Pipeline Integration

**Phase:** 3  
**Status:** Pending  
**Depends on:** Phase 2 (Format Adapters)

---

## Goal

Wire all adapters into `cleaning.py` to create the full pipeline: discover → adapt → concat → dedup → resample → impute → FWI. Single SSOT output per station.

---

## Pipeline Flow

```
1. DISCOVER: Find all raw files in data/raw/
2. ROUTE: Route each file to adapter by extension
3. ADAPT: Load each file → canonical DataFrame
4. CONCAT: Group by station, concatenate all files
5. DEDUP: Remove duplicate timestamps (keep first)
6. RESAMPLE: Hourly → Daily aggregation
7. IMPUTE: Fill short gaps in required columns
8. FWI: Calculate Fire Weather Index
9. WRITE: Output to data/processed/<station>/
```

---

## Deliverables

### 1. `cleaning.py` — Main Pipeline

```python
def main():
    args = parse_args()
    
    if args.dry_run:
        return report_dry_run()
    
    # 1. Discover all raw files
    raw_files = discover_raw_files(RAW_DIR)
    
    # 2-3. Load all files through adapters
    all_dfs = []
    for path in raw_files:
        adapter = route_by_extension(path)
        df = adapter.load(path)
        all_dfs.append(df)
    
    # 4. Concat all DataFrames
    combined = pd.concat(all_dfs, ignore_index=True)
    
    # 5. Deduplicate by (station, timestamp_utc)
    combined = combined.drop_duplicates(subset=["station", "timestamp_utc"], keep="first")
    
    # 6. Resample hourly
    hourly = resample_hourly(combined)
    
    # 7. Impute missing values
    hourly, imputation_report = impute_gaps(hourly)
    
    # 8. Calculate FWI
    hourly = calculate_fwi(hourly)
    
    # 9. Resample to daily
    daily = resample_daily(hourly)
    
    # 10. Write outputs
    for station in hourly["station"].unique():
        write_station_outputs(station, hourly, daily)
    
    # 11. Write reports
    write_imputation_report(imputation_report)
    write_pipeline_manifest()
```

### 2. Imputation Integration

Wire `imputation.py` into the pipeline:

```python
def impute_gaps(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Impute short gaps, return DataFrame + report."""
    # Short gaps: <= 6 hours for hourly data
    # Long gaps: remain NaN (not imputed)
    
    imputed_df = df.copy()
    report_rows = []
    
    for station in df["station"].unique():
        station_df = df[df["station"] == station].copy()
        
        for col in ["air_temperature_c", "relative_humidity_pct", "wind_speed_kmh", "rain_mm"]:
            missing_before = station_df[col].isna().sum()
            
            # Linear interpolation for short gaps
            station_df[col] = station_df[col].interpolate(method="linear", limit=6)
            
            missing_after = station_df[col].isna().sum()
            imputed_count = missing_before - missing_after
            
            report_rows.append({
                "station": station,
                "column": col,
                "missing_before": missing_before,
                "missing_after": missing_after,
                "imputed_count": imputed_count,
            })
        
        imputed_df[imputed_df["station"] == station] = station_df
    
    report = pd.DataFrame(report_rows)
    return imputed_df, report
```

### 3. FWI Integration

Wire `fwi.py` into the pipeline:

```python
def calculate_fwi(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate FWI indices for each station."""
    # FWI requires: temp, RH, wind, rain, latitude
    # Run per-station to maintain date continuity
    
    result = df.copy()
    
    for station in df["station"].unique():
        station_df = df[df["station"] == station].sort_values("timestamp_utc")
        
        # Get latitude for station
        lat = STATION_LATITUDES.get(station, 46.0)
        
        # Calculate moisture codes
        result.loc[result["station"] == station, "ffmc"] = calculate_ffmc(...)
        result.loc[result["station"] == station, "dmc"] = calculate_dmc(...)
        result.loc[result["station"] == station, "dc"] = calculate_dc(...)
        
        # Calculate indices
        result.loc[result["station"] == station, "isi"] = calculate_isi(...)
        result.loc[result["station"] == station, "bui"] = calculate_bui(...)
        result.loc[result["station"] == station, "fwi"] = calculate_fwi_index(...)
    
    return result
```

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-INT-1 | `cleaning.py` runs end-to-end without error |
| AC-INT-2 | All 6 stations (5 PEINP + Stanhope) have `data/processed/<station>/station_hourly.csv` |
| AC-INT-3 | All 6 stations have `data/processed/<station>/station_daily.csv` |
| AC-INT-4 | Hourly CSVs have FWI columns (ffmc, dmc, dc, isi, bui, fwi) |
| AC-INT-5 | Daily CSVs have FWI columns |
| AC-INT-6 | Imputation report exists at `data/processed/imputation_report.csv` |
| AC-INT-7 | Imputation runs after concat+dedup (not per-file) |
| AC-INT-8 | Long gaps (>6h hourly, >3d daily) remain NaN |
| AC-INT-9 | FWI calculated on imputed data |

---

## Exit Gate

```bash
pytest tests/test_v2_pipeline.py::TestAC_PIPE_3_PipelineIntegration -v
```

All tests must pass.
