# Licor Ingestion Plan — Automated Long-Term Data Fetching

**Status: Implemented** (2026-04-09)

## Problem

The clean pipeline (`python -m pea_met_network`) skips all stations when Licor JSON cache is stale. `licor_cache.py` is manual. The forecast pipeline fetches live 6h data but discards it. Result: the historical dataset rots between manual cache runs.

## Architecture

### What changes

| Component | Change |
|-----------|--------|
| `licor_adapter.py` | Add `fetch_and_cache()` method — incremental fetch since last cached file, writes JSON in same format as `licor_cache.py` |
| `cleaning.py` | Add pre-fetch step before `discover_raw_files()` — calls `LicorAdapter.fetch_and_cache()` to refresh cache |
| New: `licor_cache_manager.py` | Cache compaction logic — consolidate old weekly chunks into monthly combined files |
| `licor_cache.py` (script) | Refactor to use `LicorAdapter.fetch_and_cache()` internally, eliminating duplication |
| `fetch_eccc_donors.py` | Add incremental mode — fetch only since last cached file for Stanhope |

### What stays

| Component | Why |
|-----------|-----|
| `json_adapter.py` | Reads cached JSON as-is. New files are same format. No changes needed. |
| `discover_raw_files()` | Still scans `data/raw/licor/`. New files appear automatically. No changes needed. |
| Freshness check (mtime) | Continues to work — new JSON files have newer mtimes, pipeline processes stations. |
| `licor_to_csv.py` | Optional conversion still works. Not in the critical path. |
| Forecast pipeline (`fwi_forecast.py`) | Continues to use `LicorAdapter.fetch_recent()` for live 6h. No changes needed. |

## Data Flow

```
                          ┌─────────────────────────────────────────┐
                          │          Licor Cloud API                │
                          │   (5 stations, 2-5 min intervals)      │
                          └──────────────┬──────────────────────────┘
                                         │
                    ┌────────────────────┴────────────────────┐
                    │                                         │
          ┌─────────▼──────────┐                  ┌──────────▼─────────┐
          │  CLEAN PIPELINE    │                  │  FORECAST PIPELINE │
          │  (periodic)        │                  │  (every 3-6h)      │
          │                    │                  │                    │
          │  1. Pre-fetch:     │                  │  1. fetch_recent() │
          │     LicorAdapter   │                  │     (last 6h live) │
          │     .fetch_and_    │                  │                    │
          │      cache()       │                  │  2. OWM 0-48h      │
          │     (incremental)  │                  │  3. GDPS 48-240h   │
          │         │          │                  │  4. FWI compute    │
          │         ▼          │                  │  5. Save forecast  │
          │  data/raw/licor/   │                  │                    │
          │  <device>/         │                  │  No disk caching.  │
          │    new JSON files  │                  │  Lives in memory.  │
          │         │          │                  └────────────────────┘
          │         ▼          │
          │  2. discover_raw_  │                  ┌──────────────────────┐
          │     files()        │                  │  ECCC MSC GeoMet API │
          │     (scans licor/) │                  │  (Stanhope ref stn)  │
          │         │          │                  └──────────┬───────────┘
          │         ▼          │                             │
          │  3. load + dedup + │                  ┌──────────▼───────────┐
          │     resample +     │                  │  fetch_eccc_donors   │
          │     quality +      │                  │  (incremental mode)  │
          │     impute + FWI   │                  │  → data/raw/eccc/    │
          │         │          │                  └──────────────────────┘
          │         ▼          │
          │  data/processed/   │
          │  (station CSVs)    │
          └────────────────────┘

          ┌──────────────────────────────────────┐
          │  CACHE COMPACTION (maintenance)      │
          │                                      │
          │  Weekly chunks older than 30 days    │
          │  → consolidated into monthly JSON    │
          │  Original chunks deleted             │
          │  json_adapter reads either format    │
          └──────────────────────────────────────┘
```

## Implementation Steps

### Step 1: Add `fetch_and_cache()` to LicorAdapter

**File:** `src/pea_met_network/licor_adapter.py`

Add method to `LicorAdapter`:

```python
def fetch_and_cache(self, device_serial: str, output_dir: Path) -> int:
    """Incremental fetch: pull data since last cached file for this device.
    
    Returns number of new JSON files written.
    """
```

Logic:
1. Scan `output_dir` for existing JSON files for this device
2. Parse end-date from filenames (format: `YYYY-MM-DD_YYYY-MM-DD.json` or `*_combined.json`)
3. If no files exist, start from `devices.json` first-seen date (or configurable default)
4. Fetch from last end-date to now in weekly chunks (reuse existing API fetch logic)
5. Write each chunk as `YYYY-MM-DD_YYYY-MM-DD.json` — same format as `licor_cache.py`
6. Write combined file as `<start>_<end>_combined.json`
7. Respect 2s rate limit between requests
8. Return count of new files written

Effort: ~100 lines added to licor_adapter.py

### Step 2: Add cache manager for compaction

**File:** `src/pea_met_network/licor_cache_manager.py` (new)

```python
def compact_device_cache(device_dir: Path, older_than_days: int = 30) -> int:
    """Consolidate weekly JSON chunks older than threshold into monthly files.
    
    Returns number of chunk files removed.
    """
```

Logic:
1. Find all non-combined JSON files in device directory
2. Group by month (from filename start date)
3. For groups older than threshold: merge records, write `YYYY-MM_combined.json`, delete originals
4. Keep combined files untouched — they're the historical archive

Effort: ~80 lines, new file

### Step 3: Wire pre-fetch into clean pipeline

**File:** `src/pea_met_network/cleaning.py`

Add before `discover_raw_files()` call in the main pipeline function:

```python
def prefetch_licor_data() -> int:
    """Fetch new Licor data since last cache. Returns files written."""
    try:
        from pea_met_network.licor_adapter import LicorAdapter
        adapter = LicorAdapter()
        devices = adapter._devices
        total = 0
        for station_key, info in devices["stations"].items():
            serial = info["device_serial"]
            device_dir = RAW_DIR / "licor" / serial
            n = adapter.fetch_and_cache(serial, device_dir)
            total += n
            if n:
                print(f"  Licor pre-fetch: {station_key} → {n} new files")
        return total
    except Exception as e:
        print(f"  Licor pre-fetch skipped: {e}", file=sys.stderr)
        return 0
```

Add `--no-fetch` flag to skip pre-fetch when desired (e.g., offline runs).

Effort: ~30 lines added, minor arg parsing change

### Step 4: Refactor `licor_cache.py` to use shared logic

**File:** `scripts/licor_cache.py`

Replace inline fetch logic with call to `LicorAdapter.fetch_and_cache()`. Script becomes a thin CLI wrapper. Keeps backward compatibility for manual use.

Effort: ~50 lines changed, net reduction

### Step 5: Add incremental mode to ECCC fetcher

**File:** `src/pea_met_network/fetch_eccc_donors.py`

Add `--incremental` flag that:
1. Scans `data/raw/eccc/stanhope/` for latest monthly CSV
2. Parses end-date from filename or file content
3. Fetches only from that date to now
4. Appends to existing file or creates new monthly file

Wire into clean pipeline pre-fetch alongside Licor.

Effort: ~40 lines added

### Step 6: Add cache compaction to pipeline or cron

**File:** `src/pea_met_network/cleaning.py` (or as standalone cron)

Run `compact_device_cache()` for all devices. Options:
- As a `--compact` flag on the clean pipeline
- As a separate cron job (monthly)
- As an automatic step when chunk count exceeds threshold (e.g., >8 files per device)

Recommend: automatic when chunk count > 8, with `--compact` flag for manual use.

Effort: ~15 lines integration

## Scheduling Considerations

| Pipeline | Frequency | Licor fetch | ECCC fetch |
|----------|-----------|-------------|------------|
| Clean pipeline | Daily or on-demand | Pre-fetch incremental (weeks of data) | Pre-fetch incremental (days) |
| Forecast pipeline | Every 3-6h | Live 6h (no caching) | N/A (uses OWM) |
| Cache compaction | Monthly or auto | Compact old chunks | N/A (CSV, small) |

The clean pipeline's pre-fetch will typically find 0-1 new weekly chunks per device (data arrives continuously). The forecast pipeline's live 6h fetch is independent and doesn't need to cache — it's for real-time FWI, not historical records.

No double-fetching concern: clean pipeline fetches the *gap* between last cache and now. Forecast pipeline fetches only the last 6 hours. They may overlap by a few hours, but the clean pipeline writes to the JSON cache while the forecast pipeline stays in-memory.

## Risks and Concerns

1. **API token rotation**: `HC_CS_PEIPCWX_PROD_RO` must be available in the environment. If the token expires or rotates, pre-fetch fails silently (pipeline falls back to existing cache). Current behavior: hard fail on missing token. Recommendation: warn and continue with stale cache rather than blocking the entire pipeline.

2. **Rate limiting**: 5 stations × N weekly chunks. In steady state (daily runs), each device needs 0-1 chunks = 5 API calls + 4 × 2s delays = ~8 seconds. On first run after a long gap (e.g., 3 months), each device needs ~12 chunks = 60 calls × 2s = ~2 minutes. Acceptable.

3. **Disk growth**: Each weekly chunk is ~3-4 MB per device. Monthly compaction keeps this manageable. 5 stations × 12 months × ~15 MB/month = ~900 MB/year. Within budget.

4. **json_adapter read performance**: Currently reads ALL JSON files in all device subdirectories on every pipeline run. As cache grows, this becomes slow. Mitigation: the `_load_all_sensor_files()` method in json_adapter should be updated to skip `_combined.json` files when per-chunk files cover the same period (or vice versa). Can defer this until it's actually a problem.

5. **Stanhope gap**: Stanhope has no Licor sensor. ECCC data has ~20h lag. The clean pipeline will always be slightly behind for Stanhope. This is inherent to the data source, not fixable by architecture.

6. **Concurrent writes**: If the clean pipeline and forecast pipeline are refactored to both write to the JSON cache, there's a risk of concurrent writes. Recommendation: keep forecast pipeline write-free. Only the clean pipeline (and its pre-fetch) writes to `data/raw/licor/`.

## Files Changed Summary

| File | Action | Lines |
|------|--------|-------|
| `src/pea_met_network/licor_adapter.py` | Add `fetch_and_cache()` | +100 |
| `src/pea_met_network/licor_cache_manager.py` | New file | +80 |
| `src/pea_met_network/cleaning.py` | Add pre-fetch + `--no-fetch` flag | +30 |
| `scripts/licor_cache.py` | Refactor to shared logic | ~50 changed |
| `src/pea_met_network/fetch_eccc_donors.py` | Add `--incremental` mode | +40 |
| **Total** | | ~300 lines |
