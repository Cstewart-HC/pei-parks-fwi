# Outputs & Artifacts

## Per-Station Output Files

Each station produces files under `data/processed/<station>/`:

| File | Description |
|---|---|
| `station_hourly.csv` | Primary hourly dataset with all variables + quality flags |
| `station_daily.csv` | Daily aggregates (mean for continuous, sum for rain) |
| `<station>_daily_compliant.csv` | Compliant daily — noon observations only, with carry-forward |

## Hourly CSV Schema

Columns (sorted alphabetically in output):

**Core:**
- `station` — station identifier
- `timestamp_utc` — ISO 8601 UTC timestamp

**Meteorological variables:**
- `air_temperature_c`
- `relative_humidity_pct`
- `wind_speed_kmh`
- `wind_direction_deg`
- `wind_gust_speed_kmh`
- `dew_point_c`
- `solar_radiation_w_m2`
- `barometric_pressure_kpa`
- `rain_mm`

**FWI components (both modes):**
- `ffmc`, `dmc`, `dc`, `isi`, `bui`, `fwi`

**Quality flags & provenance (from cross-station imputation):**
- `_quality_flags` — JSON array of quality enforcement flags
- Per-variable quality flag columns: `{var}_qf` (0 = clean)
- Per-variable imputation metadata: `{var}_method`, `{var}_src`

**Compliant daily adds:**
- `carry_forward_used` — boolean flag for carry-forward days

## Pipeline-Level Reports

| File | Content |
|---|---|
| `pipeline_manifest.json` | Run metadata, per-station row counts at each stage, timestamps |
| `qa_qc_report_hourly.csv` | Per-station missingness, duplicate counts, out-of-range counts, coverage |
| `qa_qc_report_compliant.csv` | Same for compliant daily output |
| `quality_enforcement_report.csv` | Every enforced action (station, timestamp, variable, original value, action) |
| `imputation_report.csv` | Intra-station gap-fill audit (station, variable, time range, method) |
| `fwi_missingness_report_hourly.csv` | FWI component missingness per station |
| `cross_station_imputation_audit.csv` | Cross-station donor audit (station, timestamp, variable, donor, method) |

## Notebook Figures

`notebooks/figures/` — analysis visualizations:

| Figure | Description |
|---|---|
| `temporal_coverage.png` | Hourly record density per station |
| `missingness_heatmap.png` | % missing per variable per station (pre-imputation) |
| `post_imputation_missingness.png` | % missing after all imputation |
| `imputation_summary.png` | Gaps filled by station |
| `fwi_timeseries.png` | FWI component time series (compliant daily) |
| `chain_breaks.png` | FWI chain breaks by station and cause |
| `pca_biplot.png` | PCA biplot of inter-station similarity |
| `pca_scree.png` | PCA scree plot (variance explained) |
| `clustering_dendrogram.png` | Hierarchical clustering of stations |
| `uncertainty_risk.png` | Imputation uncertainty/risk assessment |

## Known Sensor Gaps (Legitimate, Not Imputed)

These are 100% missing due to sensors never deployed — not data gaps:

| Variable | Stations | Impact |
|---|---|---|
| `barometric_pressure_kpa` | Greenwich, Cavendish, Stanhope | No BP sensor |
| `solar_radiation_w_m2` | Stanhope | No solar sensor |
| `wind_gust_speed_kmh` | Stanhope | No gust sensor |
| `relative_humidity_pct` | Stanley Bridge, Tracadie | No RH sensor → cross-station imputed |
| `dew_point_c` | Stanley Bridge, Tracadie | Derived from RH → also imputed |

## Staging Directory

`data/processed/.donor_staging/` — temporary parquet files used by the disk-based fallback path (partial station runs). Not present when running `--stations all`.
