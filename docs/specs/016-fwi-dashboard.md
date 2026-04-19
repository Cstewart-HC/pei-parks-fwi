# Phase 16 — FWI Geospatial Dashboard

**Status:** proposed  
**Branch:** `feature/phase16-fwi-dashboard`  
**Depends on:** Phases 1–15 (all merged to `main`), pipeline outputs current

---

## Problem

The pipeline produces daily FWI values for 6 PEI National Park weather stations, but there is no spatial visualization. Parks Canada fire management staff need an at-a-glance view of fire danger across the park, with the ability to inspect historical conditions. A static, deployable dashboard on GitHub Pages provides zero-cost, zero-maintenance hosting.

## Scope

### Station Coordinates

| Station | Lat | Lon | Source |
|---------|-----|-----|--------|
| stanhope | 46.420 | -63.080 | ECCC CSV header (verified) |
| cavendish | 46.491 | -63.379 | Cavendish, PEI (46°29′29″N 63°22′43″W) |
| greenwich | 46.449 | -62.442 | Greenwich, PEI (easternmost park region) |
| north_rustico | 46.451 | -63.330 | North Rustico, PEI |
| stanley_bridge | 46.446 | -63.349 | Stanley Bridge, PEI |
| tracadie | 46.385 | -62.233 | Tracadie area, PEI (east end of park) |

These are approximate centroids of each station's service area. If more precise coordinates become available (Licor device metadata, survey data), they can be updated in a single config file.

### FWI Color Classification

Standard Canadian Fire Weather Index System thresholds:

| Class | FWI Range | Color |
|-------|-----------|-------|
| Low | 0–5 | #2ecc71 (green) |
| Moderate | 5.1–14 | #f1c40f (yellow) |
| High | 14.1–24 | #e67e22 (orange) |
| Very High | 24.1–36 | #e74c3c (red) |
| Extreme | 36.1+ | #8e44ad (purple) |

### Architecture

```
dashboard/
├── index.html              ← Leaflet map shell
├── css/
│   └── style.css           ← map styling, legend, controls
├── js/
│   └── app.js              ← map init, slider, markers, popups
└── data/
    ├── stations.json       ← station metadata + coordinates
    ├── fwi_daily.json      ← {date: [{station, ffmc, dmc, dc, isi, bui, fwi}]}
    └── park_boundary.geojson ← PEI National Park outline
```

Build script:
```
scripts/build_dashboard.py  ← reads pipeline outputs, generates dashboard/data/*
```

CI/CD:
```
.github/workflows/deploy-dashboard.yml  ← push to gh-pages on main merge
```

### Component Breakdown

#### 1. `scripts/build_dashboard.py` — Data Build Script

**Input:** `data/processed/{station}/station_daily.csv` (6 stations)  
**Output:** `dashboard/data/stations.json`, `dashboard/data/fwi_daily.json`

Logic:
1. Read all 6 `station_daily.csv` files
2. For each row, extract: `timestamp_utc`, `station`, `ffmc`, `dmc`, `dc`, `isi`, `bui`, `fwi`
3. Skip rows where `fwi` is NaN or empty
4. Build `fwi_daily.json` — dict keyed by date string (`YYYY-MM-DD`), each value is an array of station objects
5. Build `stations.json` — static station metadata (name, display name, lat, lon, group)
6. Print summary: date range, row counts per station, any gaps

**CLI:** `python scripts/build_dashboard.py [--output-dir dashboard/data]`

**Columns read from daily CSV:**
- `timestamp_utc` (col 23 — but use header names, not indices)
- `station` (col 22)
- `ffmc` (col 11), `dmc` (col 9), `dc` (col 6), `isi` (col 15), `bui` (col 4), `fwi` (col 13)

#### 2. `dashboard/data/park_boundary.geojson` — Park Outline

Source: Parks Canada Open Data or OpenStreetMap. A simplified GeoJSON polygon for the PEI National Park boundary. Three separate polygons (Cavendish-North Rustico, Brackley-Dalvay, Greenwich) or a single multipolygon.

**Fallback:** If no boundary data is available at build time, the dashboard works without it — markers are still positioned correctly. The boundary is decorative context, not functional.

**Acquisition strategy:**
1. Check OpenStreetMap Overpass API for `boundary=national_park` + `name="Prince Edward Island National Park"`
2. Simplify to ~50 points max (reduce file size)
3. Store as static file — no runtime dependency

#### 3. `dashboard/index.html` — Map Shell

- Leaflet.js CDN (v1.9.x) + CSS
- OpenStreetMap tile layer (free, no API key)
- Container div for the map
- Link to `css/style.css` and `js/app.js`
- Link to `data/stations.json` and `data/fwi_daily.json`
- Minimal, semantic HTML5

#### 4. `dashboard/js/app.js` — Interactivity

**Initialization:**
- Load `stations.json` and `fwi_daily.json` via `fetch()`
- Create Leaflet map centered on PEI (`[46.45, -63.0]`, zoom 9)
- Add OpenStreetMap tile layer
- Add park boundary GeoJSON layer (if available)
- Create marker group for each station

**Markers:**
- `L.circleMarker()` per station, radius 12
- Color set by FWI class function
- Tooltip on hover: station name + current FWI class label
- Popup on click: full FWI breakdown table + mini sparkline (last 30 days)

**Date Slider:**
- `L.Control` extension at bottom of map
- HTML range input (`<input type="range">`) spanning full date range
- Display current date + "Today" button
- On change: update all marker colors, update tooltips

**Legend:**
- `L.Control` extension at top-right
- Color swatches for all 5 FWI classes
- Styled consistently with map theme

**Popup Content (on marker click):**
```
┌──────────────────────────┐
│ 📍 Cavendish             │
│ 2026-04-18               │
│                          │
│ FFMC    82.3             │
│ DMC     14.2             │
│ DC      96.8             │
│ ISI     5.4              │
│ BUI     18.9             │
│ FWI     7.2  ◀ Moderate  │
│                          │
│ [30-day sparkline chart] │
└──────────────────────────┘
```

Sparkline: simple SVG polyline (no chart library needed), last 30 days of FWI values.

**Responsive:**
- Map fills viewport on desktop, stacks controls on mobile
- Tiles: OpenStreetMap (free) with attribution

#### 5. `dashboard/css/style.css` — Styling

- Dark/light neutral theme matching Parks Canada branding
- Clean legend, slider, popup styling
- Mobile-first responsive layout
- Popup table styling (monospace numbers, aligned columns)

#### 6. `.github/workflows/deploy-dashboard.yml` — CI/CD

Trigger: push to `main` (or manual `workflow_dispatch`)

Steps:
1. Checkout repo
2. Set up Python 3.11
3. Install project: `pip install -e .`
4. Run build script: `python scripts/build_dashboard.py`
5. Deploy `dashboard/` directory to `gh-pages` branch using `peaceiris/actions-gh-pages@v4`

**Note:** Only the `dashboard/` directory is deployed. No pipeline data, no source code, no secrets.

### Data Flow

```
data/processed/{station}/station_daily.csv
  ↓ scripts/build_dashboard.py
dashboard/data/fwi_daily.json + stations.json
  ↓ GitHub Actions (on push to main)
gh-pages branch → https://cstewart-hc.github.io/pea-met-network/
```

### Performance Constraints

- `fwi_daily.json`: ~1,100 days × 6 stations ≈ 6,600 records. At ~100 bytes each ≈ 660 KB. Acceptable.
- Park boundary GeoJSON: < 50 KB after simplification.
- Total page load: < 1 MB (tiles excluded — cached by browser).
- No server-side computation. All filtering happens in-browser.

## Files Changed

| File | Action |
|------|--------|
| `scripts/build_dashboard.py` | Create — data build script |
| `dashboard/index.html` | Create — map shell |
| `dashboard/css/style.css` | Create — styling |
| `dashboard/js/app.js` | Create — interactivity |
| `dashboard/data/park_boundary.geojson` | Create — park outline (static) |
| `dashboard/data/stations.json` | Generated — station metadata |
| `dashboard/data/fwi_daily.json` | Generated — FWI time series |
| `.github/workflows/deploy-dashboard.yml` | Create — CI/CD |
| `tests/test_dashboard_build.py` | Create — build script tests |

## Acceptance Criteria

1. [ ] `python scripts/build_dashboard.py` runs without errors and produces `stations.json` + `fwi_daily.json`
2. [ ] `fwi_daily.json` covers full date range (2023-04-01 to pipeline run date)
3. [ ] All 6 stations present in output with non-null FWI values
4. [ ] `dashboard/index.html` loads in browser with Leaflet map
5. [ ] All 6 station markers visible and color-coded by current FWI class
6. [ ] Date slider scrubs through history, marker colors update correctly
7. [ ] Clicking a marker shows popup with full FWI breakdown (FFMC, DMC, DC, ISI, BUI, FWI)
8. [ ] Popup includes 30-day FWI sparkline
9. [ ] Legend displays all 5 FWI classes with correct colors
10. [ ] Park boundary renders as overlay on map
11. [ ] Responsive on mobile viewport
12. [ ] GitHub Actions workflow deploys to `gh-pages` on push
13. [ ] No secrets or pipeline data in deployed site
14. [ ] Build script tests pass (`tests/test_dashboard_build.py`)

## Out of Scope

- Real-time data refresh (static build only — rebuild on pipeline run)
- User authentication or access control
- Forecast data (current dashboard shows historical + latest daily only)
- Download/export functionality
- Additional weather variables beyond FWI sub-indices
- Alternative basemaps beyond OpenStreetMap
- Server-side API or database

## Implementation Order

| Step | Task | Est. |
|------|------|------|
| 1 | Create `scripts/build_dashboard.py` — read daily CSVs, emit JSON | 30 min |
| 2 | Create `dashboard/data/stations.json` — static station metadata | 5 min |
| 3 | Acquire + simplify park boundary GeoJSON | 15 min |
| 4 | Create `dashboard/index.html` — Leaflet shell | 15 min |
| 5 | Create `dashboard/js/app.js` — markers, colors, slider, popups, sparklines | 60 min |
| 6 | Create `dashboard/css/style.css` — styling, responsive | 20 min |
| 7 | Create `tests/test_dashboard_build.py` — build script validation | 15 min |
| 8 | Create `.github/workflows/deploy-dashboard.yml` | 10 min |
| 9 | End-to-end validation — build → open in browser → verify all AC | 15 min |

**Total estimate: ~3 hours**

## Risks

| Risk | Mitigation |
|------|------------|
| Park boundary GeoJSON unavailable | Dashboard works without it; markers are the primary UI |
| Stanhope daily CSV has different column order | Build script uses header names, not column indices |
| GitHub Pages CDN latency | All data < 1 MB; tiles cached by browser |
| Station coordinates imprecise | Approximate centroids sufficient for park-scale visualization; easily updated in `stations.json` |
