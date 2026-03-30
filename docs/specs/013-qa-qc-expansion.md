# Phase 13: QA/QC Report Expansion — Dual-Mode Diagnostics

**Status:** Active  
**Branch:** `feature/phase13-qa-qc-expansion`  
**Exit Gate:** `pytest tests/test_phase13_qa_qc_expansion.py -v`

## 1. Problem

The QA/QC infrastructure has five reports, but they have critical gaps for a dual-mode (compliant + hourly) pipeline:

1. **No pre-imputation snapshot.** Reports run on post-imputation data only. Can't measure improvement.
2. **No `fwi_mode` in reports.** Can't tell if a report is from a compliant or hourly run.
3. **Compliant mode is blind.** Chain break detection only runs for hourly mode. Carry-forward days aren't reported.
4. **No FWI value statistics.** No min/max/mean/std for FWI codes per station. Can't sanity-check outputs.
5. **Mode overwrite.** Both modes write to the same `qa_qc_report.csv`, so running one mode erases the other's report.

## 2. Scope

### 2.1 Pre/Post Imputation Missingness

Capture NaN counts for each input variable *before* imputation runs, and include both pre and post in the QA/QC report.

**Where:** In `run_pipeline()` (`cleaning.py`), capture a pre-imputation missingness snapshot per station after `enforce_quality()` but before `impute()`.

**Report columns added to `qa_qc_report.csv`:**
- `pre_imp_missing_pct_air_temperature_c`
- `pre_imp_missing_pct_relative_humidity_pct`
- `pre_imp_missing_pct_wind_speed_kmh`
- `pre_imp_missing_pct_rain_mm`
- (Post-imputation columns renamed to `post_imp_missing_pct_*` for clarity; keep backward-compat aliases)

**Function:** Add `pre_imputation_missingness(df: pd.DataFrame) -> dict[str, float]` to `qa_qc.py`.

### 2.2 FWI Mode in All Reports

Add `fwi_mode` column to:
- `qa_qc_report.csv`
- `pipeline_manifest.json` (already has it in metadata, confirm)
- `fwi_missingness_report.csv` (when generated)

### 2.3 Compliant Mode Diagnostics

When `fwi_mode == "compliant"`, write carry-forward summary:

**Report:** `qa_qc_report.csv` gets additional columns:
- `carry_forward_days` — count of days where noon inputs were missing
- `carry_forward_pct` — percentage of total days

Data source: the `carry_forward_used` column already exists in the daily compliant output DataFrame from `calculate_fwi_daily()`.

When `fwi_mode == "hourly"`, write chain break summary (existing behavior via `fwi_chain_breaks` column).

### 2.4 FWI Value Statistics

Add descriptive statistics for FWI codes to the QA/QC report.

**Report columns added:**
- `ffmc_min`, `ffmc_max`, `ffmc_mean`, `ffmc_std`
- `dmc_min`, `dmc_max`, `dmc_mean`, `dmc_std`
- `dc_min`, `dc_max`, `dc_mean`, `dc_std`
- `isi_min`, `isi_max`, `isi_mean`, `isi_std`
- `bui_min`, `bui_max`, `bui_mean`, `bui_std`
- `fwi_min`, `fwi_max`, `fwi_mean`, `fwi_std`

Values come from the daily output DataFrame (compliant or hourly daily aggregation).

**Function:** Add `fwi_descriptive_stats(daily: pd.DataFrame, station: str) -> dict[str, float]` to `qa_qc.py`.

### 2.5 Mode-Specific Report Filenames

Write reports to mode-specific filenames:
- `data/processed/qa_qc_report_{mode}.csv` (e.g., `qa_qc_report_compliant.csv`, `qa_qc_report_hourly.csv`)
- `data/processed/fwi_missingness_report_{mode}.csv` (hourly only)
- Keep a `qa_qc_report.csv` symlink/copy of the latest run for backward compatibility

**Implementation:** Pass `fwi_mode` to the report writing section in `run_pipeline()`. Use `PROCESSED_DIR / f"qa_qc_report_{mode}.csv"`.

### 2.6 Per-Stage Row Count Audit

Capture row counts at each pipeline stage per station, write to pipeline manifest.

**Stages to track:**
1. `raw` — after loading
2. `deduped` — after deduplication
3. `hourly` — after resample to hourly
4. `truncated` — after date range truncation
5. `post_quality` — after quality enforcement
6. `post_imputation` — after linear interpolation
7. `post_cross_station` — after cross-station imputation (if applicable)

**Output:** Add `stage_row_counts` dict to pipeline manifest per station.

## 3. Files Changed

| File | Change |
|------|--------|
| `src/pea_met_network/qa_qc.py` | Add `pre_imputation_missingness()`, `fwi_descriptive_stats()`. Update `generate_qa_qc_report()` signature and output columns. |
| `src/pea_met_network/cleaning.py` | Capture pre-imputation snapshot. Pass `fwi_mode` to report generation. Write mode-specific filenames. Capture per-stage row counts. |
| `tests/test_phase13_qa_qc_expansion.py` | New test file. |

## 4. What Is NOT in Scope

- Temporal coverage gap detection (deferred — medium effort, not blocking)
- Changes to imputation logic
- Changes to FWI calculation
- New dependencies

## 5. Estimated Effort

- ~150 lines of new code in `qa_qc.py` and `cleaning.py`
- ~200 lines of tests
- No new dependencies
