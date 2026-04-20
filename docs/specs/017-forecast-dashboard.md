# Phase 17 — FWI Forecast Dashboard Integration

## Status
**Planning**

## Goal
Surface the 240h FWI forecast pipeline output on the GitHub Pages dashboard, giving Parks Canada ops staff a visual 10-day fire weather outlook alongside historical data.

## Current State

### What exists
- **Historical dashboard** (`dashboard/`): Leaflet map, date slider, per-station markers colored by FWI class, popups with FWI breakdown + 30-day sparkline. Data: `fwi_daily.json` (daily, keyed by date).
- **Forecast pipeline** (`src/pea_met_network/fwi_forecast.py`): Produces 240h hourly CSVs per station in `data/forecasts/<station>_fwi_forecast.csv`. Columns: `timestamp_utc,temp,rh,wind,rain,FFMC,DMC,DC,ISI,BUI,FWI`.
- **Build script** (`scripts/build_dashboard.py`): Reads `data/processed/<station>/station_daily.csv`, writes `fwi_daily.json` and `stations.json`.
- **Forecast data** is 11 days stale (last run 2026-04-09). Startup state persists DMC/DC/FFMC between runs.

### Constraints
- **Static site only** — no server-side rendering, no API calls from the browser
- **GitHub Pages** — all data must be committed as JSON files
- **Forecast freshness** — CSVs are overwritten per run (not timestamped). Needs a rebuild step in the CI pipeline or manual rebuild-and-commit workflow
- **API keys** — `HC_CS_PEIPCWX_PROD_RO` and `openweather_key` are vault-stored, unavailable in GitHub Actions (no secrets configured)

## Architecture

### Data Flow
```
fwi_forecast.py (local, on schedule)
  → data/forecasts/<station>_fwi_forecast.csv
  → scripts/build_dashboard.py --include-forecast
    → dashboard/data/fwi_daily.json        (historical, existing)
    → dashboard/data/fwi_forecast.json     (NEW: 10-day hourly forecast)
    → dashboard/data/forecast_meta.json    (NEW: run timestamp, data sources, staleness)
  → git commit + push
    → GitHub Actions deploy
```

### Dashboard UX

**Mode switch**: Toggle between "Historical" and "Forecast" views.

| Aspect | Historical (current) | Forecast (new) |
|--------|---------------------|----------------|
| Data source | `fwi_daily.json` | `fwi_forecast.json` |
| Time resolution | Daily | Daily (aggregated from hourly) |
| Date range | 2023-04-01 → last processed day | Last processed day → +10 days |
| Slider | All historical dates | Forecast days (day 0 = today) |
| Marker color | FWI class | FWI class + dashed outline for forecast |
| Sparkline | Last 30 days historical | 10-day forecast trend |
| Popup | FWI breakdown table | FWI breakdown + data source label |
| Staleness banner | N/A | Warning if forecast > 12h old |

### New JSON Schemas

**`fwi_forecast.json`** — same structure as `fwi_daily.json`:
```json
{
  "2026-04-20": [
    {"station": "cavendish", "ffmc": 84.5, "dmc": 12.3, "dc": 45.2, "isi": 4.1, "bui": 15.8, "fwi": 5.2},
    ...
  ],
  "2026-04-21": [...]
}
```
- Hourly CSVs aggregated to daily: noon observation (14:00 UTC) or closest available hour
- Forecast days beyond historical data range only

**`forecast_meta.json`**:
```json
{
  "generated_at": "2026-04-20T12:00:00Z",
  "forecast_hours": 240,
  "data_sources": {
    "licor": "0-6h (5 park stations)",
    "owm": "0-48h (all 6 stations)",
    "gdps": "48-240h (3-hourly)"
  },
  "stations_with_full_fwi": ["stanhope", "cavendish", "greenwich", "north_rustico"],
  "stations_partial": ["stanley_bridge", "tracadie"],
  "partial_note": "Stanley Bridge and Tracadie lack RH sensors — FWI may be incomplete for some hours"
}
```

## Implementation Steps

### Step 1: Extend build script
**File**: `scripts/build_dashboard.py`

- Add `--include-forecast` flag (default: true)
- Read `data/forecasts/<station>_fwi_forecast.csv` for each station
- Aggregate hourly to daily (noon obs or nearest)
- Merge forecast dates into `fwi_daily.json` (or write separate `fwi_forecast.json`)
- Write `forecast_meta.json` with run timestamp from file mtimes
- Handle stale/missing forecast files gracefully (write empty forecast, log warning)

### Step 2: Dashboard UI — forecast layer
**File**: `dashboard/js/app.js`

- Add mode toggle control (Historical / Forecast) — top-left, below header
- Load `fwi_forecast.json` and `forecast_meta.json` alongside existing data
- In forecast mode: extend date slider to include forecast dates
- Mark forecast dates visually (dashed marker outlines, "FCST" badge in popup)
- Show staleness warning banner if forecast > 12h old
- Sparkline in popup: show forecast trend instead of historical when in forecast mode

### Step 3: Dashboard CSS
**File**: `dashboard/css/style.css`

- Mode toggle button styles
- Forecast marker styling (dashed border, slightly different opacity)
- Staleness warning banner
- "FCST" badge in popup

### Step 4: CI workflow update
**File**: `.github/workflows/deploy-dashboard.yml`

- Add `fwi_forecast.json` and `forecast_meta.json` to the file verification step
- No Python build in CI (data committed to repo — local rebuild-and-push model)

### Step 5: Documentation
**File**: `docs/specs/017-forecast-dashboard.md`

- Spec document with architecture, schemas, and UX decisions

## Decisions Needed

1. **Single vs separate JSON**: Merge forecast into `fwi_daily.json` (simpler frontend) or keep `fwi_forecast.json` separate (cleaner separation)? → Recommend: separate, with frontend merging at load time. Allows independent staleness tracking.

2. **Aggregation method**: Forecast CSVs are hourly. Daily aggregation — noon (14:00 UTC) snapshot or daily max FWI? → Recommend: noon snapshot, consistent with compliant daily mode.

3. **Forecast refresh cadence**: How often should the forecast be rebuilt and pushed? → Recommend: cron every 6h, triggered locally or via Moltis scheduled task.

4. **Historical-forecast overlap**: The forecast starts from the last processed historical day. Should we show both or prefer forecast for the overlap period? → Recommend: forecast wins for overlap (it includes live obs).

## Success Criteria

- [ ] Dashboard shows 10-day FWI forecast for all 6 stations
- [ ] Mode toggle switches between historical and forecast views
- [ ] Staleness warning displays when forecast data is > 12h old
- [ ] Forecast markers visually distinct from historical
- [ ] Build script produces `fwi_forecast.json` and `forecast_meta.json`
- [ ] Zero new external dependencies (pure vanilla JS, no build step)
- [ ] GitHub Pages deploys successfully
- [ ] Stations with partial FWI (Stanley Bridge, Tracadie) handled gracefully

## Out of Scope

- Forecast validation/scoring (Step 13 from forecast plan)
- Operational scheduling (Step 14 from forecast plan — separate phase)
- SQLite migration for forecast archive
- Interactive weather variable charts
