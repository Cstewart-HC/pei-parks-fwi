# FWI Forecast Pipeline — Implementation Plan

## Branch
`feat/fwi-prediction-from-correlates`

## Status
**Operational pipeline complete.** Produces 240h FWI forecasts for all 6 PEINP stations
using Licor live obs (0–6h) → OWM (0–48h hourly) → GDPS (48–240h 3-hourly).
Historical FWI backfilled to 99.6% for Stanhope. Cross-station RH imputation validated
against 2+ years of data. Ready for scheduling and validation archiving.

## Goal
Produce multi-day FWI forecasts for 6 PEINP stations (Stanhope + 5 park stations)
using Licor Cloud live observations, OpenWeatherMap, Environment Canada GDPS,
and the standard FWI equations.

## Three-Layer Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  STATIC (run once, archive)                                 │
│  • Redundancy analysis (done)                               │
│  • Historical FWI from ECCC hourly data (backfilled)        │
│  • RH imputation validation (done, 22K+ hours)             │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  OPERATIONAL (runs on schedule)                             │
│  • Licor Cloud live obs (0–6h, 5 park stations)            │
│  • Cross-station direct RH donation (Tracadie, Stanley Br) │
│  • OWM bias-corrected RH fallback (P3)                     │
│  • OWM 0–48h (hourly, all 6 stations)                      │
│  • GDPS 48–240h (3-hourly)                                 │
│  → merge → compute FWI → archive timestamped files         │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  VALIDATION (on demand / continuous)                        │
│  • Archive each forecast run with timestamp                 │
│  • Compare archived forecasts vs actual obs as they arrive │
│  • Score per data source (Licor vs OWM vs GDPS)            │
└─────────────────────────────────────────────────────────────┘
```

### Layer details

**STATIC** — Analysis and coefficients that don't change between runs.
- Redundancy analysis notebook (done, committed)
- Historical FWI backfill: Stanhope 95.6% → 99.6%, park stations +43–364 rows
- RH imputation validation: direct RH donation validated against Greenwich (pretend missing),
  22K+ hours, FWI-aware scoring. VP continuity dropped (amplified temp errors).

**OPERATIONAL** — The live forecast pipeline.
- Licor Cloud live obs (0–6h): Actual park station observations for 5 PEINP stations.
  Resampled from ~2-5 min records to hourly. Tracadie/Stanley Bridge have no RH sensor.
- Cross-station direct RH donation: Cavendish (P1) and N. Rustico (P2) donate RH directly
  to Tracadie and Stanley Bridge. Validated: FFMC MAE 2.70 (mean of two donors) vs 5.39 (VP continuity)
  during fire season hours with FWI > 0.
- OWM bias-corrected RH fallback (P3): Estimates median OWM RH bias from donor overlap,
  applies to target stations for any remaining gaps after cross-station imputation.
- OWM (0–48h): 6 API calls, hourly resolution. All 6 stations fetched directly (no OLS translation).
- GDPS (48–240h): EC WMS GetFeatureInfo, 4 workers, per-run JSON cache with 7h TTL.
  Variables: temperature 2m, relative humidity, wind speed 10m, accumulated rain.
- Merge: Licor obs win in overlap with OWM; OWM preferred vs GDPS; GDPS fills beyond 48h.
- Output: timestamped CSVs, never overwritten. Startup state persists FFMC/DMC/DC between runs.

**VALIDATION** — Scoring and comparison when observations are available.
- Archive each forecast run with timestamp
- Compare against actual observations as they arrive
- Per-source accuracy: which data source (Licor vs OWM vs GDPS) is most accurate at each horizon

## Pipeline Architecture (current)
```
Licor Cloud API (5 park stations, last 6h live obs)
  → cross-station direct RH donation (Cavendish/N. Rustico → Tracadie/Stanley Bridge)
  → merge into OWM One Call 3.0 (obs wins in overlap, 0–48h)
  → OWM bias-corrected RH fallback (P3, for remaining gaps)
  → GDPS extension (48–240h, EC WMS)
  → FWI computation (hourly FFMC chain, daily DMC/DC → ISI → BUI → FWI)
    → startup state persistence (JSON, 72h TTL)
    → Optional CWFIS comparison (when stations are active)
```

Stanhope has no Licor source — OWM-only for Stanhope (ECCC ~20h lag, not useful for ops).

## Design Decisions

### OLS nowcast — removed
- OLS coefficients fitted on ECCC-to-ECCC station pairs (~26K hours, R²=0.81–0.94 for temp)
- Problem 1: OWM grid resolution already distinguishes stations 10–25 km apart — OLS translation added noise
- Problem 2: Stanhope is a staffed ECCC station with ~20h data lag — no real-time feed exists
- Problem 3: Licor Cloud provides actual live park station observations, making OLS unnecessary
- `ols_nowcast.py` deleted in commit `8560405`
- Coefficients preserved in `data/processed/ols_coefficients.json` for reference

### Licor Cloud live adapter
- Auth via `HC_CS_PEIPCWX_PROD_RO` env var (stored in Moltis vault, never hardcoded)
- 5 stations: cavendish, north_rustico, greenwich, stanley_bridge, tracadie
- Resamples ~2-5 min records to hourly (mean for temp/RH/wind, sum for rain)
- Wind speed converted m/s → km/h
- Tracadie and Stanley Bridge have no RH sensor — filled by cross-station imputation
- 2s rate limit between API calls per Licor policy
- Commit `e59da41`

### Cross-station RH imputation — direct donation over VP continuity
- Tracadie and Stanley Bridge lack RH sensors; donors are Cavendish (P1) and N. Rustico (P2)
- **VP continuity** (old method): Transfer vapor pressure from donor, recompute RH at target temperature.
  Mathematically correct but amplifies temperature errors through nonlinear saturation curve.
  R² = -0.321 below 0°C; 1-2% worse FFMC MAE across all seasons.
- **Direct RH donation** (current method): Return donor RH as-is.
  Validated against Greenwich (22K+ hours, 2+ years) with FWI-aware scoring:
  - Fire season FFMC MAE: 2.70 (mean of donors) vs 5.39 (VP continuity)
  - High-danger hours (FWI>14): FFMC MAE 0.67 (N. Rustico) vs 1.50 (VP)
  - FWI rating class: all methods ≥83.9% exact, 100% within ±1 class
- Method label in audit trail: `DIRECT_RH`
- Validation script: `scripts/validate_rh_imputation_historical.py`
- Commit `03d8f37`

### OWM bias-corrected RH fallback (P3)
- After cross-station imputation, any remaining RH gaps filled from OWM with spatial bias correction
- Computes median OWM RH bias from donor stations where both Licor obs and raw OWM exist
- Applies correction to target station's raw OWM RH
- Clamped to 0–100%
- Only activates when cross-station imputation can't fill a gap and OWM has data for the target

### Direct OWM fetch (no OLS translation in OWM path)
- OWM grid resolution distinguishes coordinates 10–25 km apart (tested: 48/48 hours differ)
- 6 API calls per run (~1.2s total), well under 1K/day free tier
- See `scripts/validate_ols_vs_direct.py` for the full comparison

### GDPS integration
- EC WMS GetFeatureInfo works at station coordinates (tested all 4 variables)
- GRIB2 files too large (~50MB each globally); WCS broken (500 errors)
- Multi-layer queries rejected by EC server; solved with 4 concurrent single-variable workers
- Benchmark: 4 parallel calls = 400ms; full 10-day fetch (324 calls) ≈ 85s fresh, 1.3s cached
- `ETA_RN` is accumulated from forecast start — differenced to per-period rain; no data at T000
- Run discovery parses `reference_time` from GetCapabilities, filters to past 00Z/12Z runs
- Per-run JSON cache in `data/gdps_cache/`, 7h TTL, auto-cleanup of files >24h old

### Storage: JSON over SQLite (for now)
- Startup state (`startup_state.json`) stores 6 stations × 3 float values + timestamp
- JSON wins: zero dependencies, human-readable, git-friendly, no schema migrations
- SQLite warranted when: historical tracking needed, concurrent writes, or archiving run outputs for backtesting
- Decision tracked here — re-evaluate when data volume or query needs change

### Historical FWI backfill
- Existing hourly CSVs had ~85–96% FWI computed from original ETL
- Gaps caused by ETL restart windows (~22h blocks at 1–3 week intervals)
- `scripts/backfill_historical_fwi.py`: two-pass approach (daily DMC/DC → hourly FFMC/ISI/BUI/FWI)
- Fill-only mode: preserves existing values, only writes where FWI is null
- Spring startup defaults: FFMC=85, DMC=6, DC=15
- Results: Stanhope 95.6% → 99.6% (+974 rows); park stations +43–364 rows
- Remaining gaps: missing raw weather observations (sensor downtime), not computation issues

### FWI computation
- Reuse existing `src/pea_met_network/fwi.py`
- Inputs: temp (°C), RH (%), wind (km/h), rain (mm)
- Outputs: FFMC, DMC, DC, ISI, BUI, FWI
- Hourly FFMC chain with daily DMC/DC at 14:00 UTC (noon solar)

### API key protection
- OWM key read from `os.environ["openweather_key"]` — hard fail if missing
- Licor key read from `os.environ["HC_CS_PEIPCWX_PROD_RO"]` — hard fail if missing
- Both stored in Moltis vault, not in repo
- `.env` already in `.gitignore`

## Outputs
- DataFrame: station × hour × FWI components
- Timestamped CSV per station per run
- CLI entry point: `python -m pea_met_network.fwi_forecast`

## Completed Steps

### Step 1–4: OLS coefficients, pipeline, validation, comparison
- See earlier commits on branch

### Step 5: Drop OLS, fetch all 6 stations directly from OWM
- OWM grid resolution validated — per-station fetch is better than OLS translation
- Pipeline simplified to 6 direct OWM calls + FWI computation
- Commit `7336330`

### Step 6: Startup index persistence
- `data/forecasts/startup_state.json` — saves final FFMC/DMC/DC per station after each run
- Loads on next run; 72h TTL auto-rejects stale state
- Commit `addbc5a`

### Step 7: CWFIS comparison
- CWFIS WFS endpoint (`public:firewx_scribe_fcst`) has 48h FWI for 2,440 national stations
- Off-season: all PEI stations report sentinel value -101
- Comparison code fetches and prints when available, skips gracefully otherwise
- Commit `addbc5a`

### Step 8: GDPS 10-day forecast extension
- `src/pea_met_network/gdps_fetcher.py` — GDPSFetcher with per-run JSON cache
- 4 EC WMS variables: temperature 2m, RH, wind speed 10m, accumulated rain
- Run discovery from GetCapabilities `reference_time`, 7h cache TTL
- Integrated into `fwi_forecast.py` via `include_gdps=True` parameter
- OWM (0–48h) preferred in overlap; GDPS fills 48–240h at 3h
- Commit `091c8fc`

### Step 9: Historical FWI backfill
- `scripts/backfill_historical_fwi.py` — two-pass chain computation
- Fill-only mode: preserves existing ETL values
- Stanhope 95.6% → 99.6%; park stations +43–364 rows
- Commit `109fecd`

### Step 10: OLS nowcast (built then removed)
- `ols_nowcast.py` built, wired into pipeline, then removed
- Removed because: Stanhope ECCC has ~20h lag (staffed station, no real-time feed),
  and Licor Cloud provides actual live park station observations
- Commit `c2affdc` (added), `e59da41` (Licor adapter), `82c13b3` (replaced OLS), `8560405` (deleted)

### Step 11: Licor Cloud live adapter
- `src/pea_met_network/licor_adapter.py` — fetches live hourly weather for 5 PEINP stations
- Replaces OLS nowcast with actual park station observations
- Commit `e59da41`

### Step 12: Cross-station RH imputation (direct donation)
- Replaced VP continuity with direct RH donation for internal donors
- Validated against 22K+ hours of Greenwich data with FWI-aware scoring
- Commit `03d8f37`

## Remaining Steps

### Step 13: Validation layer
- Archive each forecast run with timestamp and source attribution
- Score archived forecasts against actual observations as they arrive
- Per-horizon accuracy: Licor (0–6h) vs OWM (0–48h) vs GDPS (48–240h)
- Requires SQLite migration for forecast archive storage

### Step 14: Operational scheduling
- Cron job every 3–6 hours to run the pipeline
- Monitor for failures / data staleness
- Startup state persistence ensures DMC/DC continuity between runs
