# FWI Forecast Pipeline — Implementation Plan

## Branch
`feat/fwi-prediction-from-correlates`

## Status
**MVP + 10-day extension complete.** Pipeline produces 240h FWI forecasts for all 6 PEINP stations
using OWM (0–48h hourly) + GDPS (48–240h 3-hourly). Historical FWI backfilled to 99.6% for Stanhope.

## Goal
Produce multi-day FWI forecasts for 6 PEINP stations (Stanhope + 5 park stations)
using OpenWeatherMap, Environment Canada GDPS, and the standard FWI equations.

## Three-Layer Architecture

```
┌───────────────────────────────���─────────────────────────────┐
│  STATIC (run once, archive)                                 │
│  • Redundancy analysis (done)                               │
│  • Historical FWI from ECCC hourly data (backfilled)        │
│  • OLS coefficients (Stanhope → park station nowcast)       │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│  OPERATIONAL (runs on schedule)                             │
│  • OLS nowcast 0–3h (real ECCC obs → park stations)        │
│  • OWM 3–48h (hourly)                                       │
│  • GDPS 48–240h (3-hourly)                                  │
│  → merge → compute FWI → archive timestamped files         │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────���───────────────────────────────────────────────┐
│  VALIDATION (on demand)                                     │
│  • Compare archived forecasts vs actual ECCC observations   │
│  • Score per data source (OLS vs OWM vs GDPS)              │
└─────────────────────────────────────────────────────────────┘
```

### Layer details

**STATIC** — Analysis and coefficients that don't change between runs.
- Redundancy analysis notebook (done, committed)
- Historical FWI backfill: Stanhope 95.6% → 99.6%, park stations +43–364 rows
- OLS coefficients fitted from ~26K paired hours (preserved in `data/processed/ols_coefficients.json`)

**OPERATIONAL** — The live forecast pipeline.
- OLS nowcast (0–3h): Use real Stanhope ECCC observations to predict park station weather.
  Revived because OLS was trained on ECCC-to-ECCC pairs — the original coefficients were
  fine, the problem was feeding them OWM grid data instead of real obs.
- OWM (3–48h): 6 API calls, hourly resolution.
- GDPS (48–240h): EC WMS GetFeatureInfo, 4 workers, per-run JSON cache with 7h TTL.
  Variables: temperature 2m, relative humidity, wind speed 10m, accumulated rain.
- Merge: OWM preferred in overlap zone; GDPS fills beyond 48h.
- Output: timestamped CSVs (`stanhope_fwi_20260406T14Z.csv`), never overwritten.

**VALIDATION** — Scoring and comparison when observations are available.
- Archive each forecast run with timestamp
- Compare against ECCC hourly observations as they arrive
- Per-source accuracy: which data source (OLS/OWM/GDPS) is most accurate at each horizon

## Pipeline Architecture (current)
```
OWM One Call 3.0 (all 6 station coords, 6 API calls) → 0–48h hourly
GDPS WMS GetFeatureInfo (1 bounding box, 4 workers)  → 0–240h 3-hourly
  → merge (OWM preferred in overlap)
  → FWI computation (FFMC → DMC → DC → ISI → BUI → FWI)
    → timestamped CSV per station
    → Optional CWFIS comparison (when stations are active)
```

## Design Decisions

### Direct OWM fetch (OLS dropped from OWM path)
- OWM grid resolution distinguishes coordinates 10–25 km apart (tested: 48/48 hours differ at every station)
- OLS translation trained on ECCC point sensors but applied to OWM modeled grid data — introduced systematic bias
- 6 API calls per run (~1.2s total), well under 1K/day free tier
- See `scripts/validate_ols_vs_direct.py` for the full comparison

### OLS nowcast (revived for ECCC obs path)
- Original OLS coefficients were fitted on ECCC-to-ECCC station pairs — they work for that data
- Problem was feeding OWM grid data through the coefficients, not the coefficients themselves
- Using real Stanhope ECCC obs to predict park stations 1–3h ahead is what the coefficients were built for
- Status: coefficients exist, wiring into live pipeline is next step

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

### API key protection
- Read from `os.environ["openweather_key"]` — hard fail if missing
- Key is in Moltis vault, not in repo
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
- Uncommitted

### Step 9: Historical FWI backfill
- `scripts/backfill_historical_fwi.py` — two-pass chain computation
- Fill-only mode: preserves existing ETL values
- Stanhope 95.6% → 99.6%; park stations +43–364 rows
- Uncommitted

## Remaining Steps

### Step 10: OLS nowcast wiring
- Wire real Stanhope ECCC hourly observations into the pipeline
- Apply OLS coefficients to predict park station weather 0–3h ahead
- Merge with OWM at the 3h boundary
- Coefficients already fitted; needs live ECCC data ingestion

### Step 11: Operational scheduling
- Cron job every 3–6 hours to run the pipeline
- Archive timestamped forecast files
- Startup state persistence ensures DMC/DC continuity between runs

### Step 12: Validation layer
- Compare archived forecasts against actual ECCC observations
- Score per data source (OLS vs OWM vs GDPS) at different horizons
- Requires at least one fire season of archived forecasts + observations
