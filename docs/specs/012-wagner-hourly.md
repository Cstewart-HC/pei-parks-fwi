# Phase 12 — Wagner Hourly: Fix Extended Mode to Use Canonical Van Wagner (1977) Hourly FFMC

## Objective

Replace the broken hourly FWI in the extended mode with the canonical hourly FFMC from the cffdrs R package (Van Wagner 1977). DMC and DC remain daily-only. This gives us two correct, well-sourced modes for the first release.

## Background & Problem Statement

The current extended mode (`calculate_fwi()` in `cleaning.py`) runs the **daily** Van Wagner (1987) FFMC/DMC/DC equations every hour. This is wrong in three ways:

1. **FFMC drying rate is 10× too fast.** The daily equation uses `0.581` as the temperature scaling factor. The hourly equation uses `0.0579` (10× smaller). Running the daily rate 24 times/day over-estimates drying enormously.
2. **Rain threshold is wrong for hourly.** Daily FFMC ignores rain < 0.5mm. Hourly FFMC applies all rain > 0mm (no threshold).
3. **DMC and DC are computed hourly.** They should be daily — the duff and deep soil layers don't respond to hourly weather. The cffdrs package has no hourly DMC/DC.

### Source

All equations are from the **cffdrs R package v1.9.2** (`hffmc.R`), maintained by Wang, Wotton, Cantin, Moore, Flannigan (NRCan). The hourly FFMC is based on **Van Wagner (1977)** "A method of computing fine fuel moisture behaviour throughout the diurnal cycle." DMC and DC equations are from **Van Wagner (1987)** — same as our existing `fwi.py`.

## Requirements

### 12.1 New Function: `_hffmc_calc`

Replace `_ffmc_calc` with `_hffmc_calc` implementing the canonical hourly FFMC.

**Signature:**
```python
def _hffmc_calc(
    temp: np.ndarray,
    rh: np.ndarray,
    wind: np.ndarray,
    rain: np.ndarray,
    ffmc_prev: float = 85.0,
    gap_threshold_hours: int = 24,
) -> np.ndarray:
```

**Algorithm (from cffdrs `hffmc.R`):**

1. Convert `ffmc_prev` to moisture content:
   ```
   mo_prev = 147.2 * (101.0 - ffmc_prev) / (59.5 + ffmc_prev)
   ```

2. For each hour `i`:
   - If temp, RH, or wind is NaN → FFMC[i] = NaN, increment gap counter
   - On gap recovery (valid inputs after ≥ `gap_threshold_hours` of NaN) → reset `mo_prev` from `ffmc_prev` startup default (85.0)
   - On gap recovery after < threshold → FFMC[i] = NaN, chain stays broken

3. **Rain adjustment** (applied BEFORE drying/wetting):
   - If `rain > 0` (NO threshold — all rain counts):
     - `rf = rain`
     - If `mo_prev <= 150.0`:
       ```
       mr = mo_prev + 42.5 * rf * exp(-100.0 / (251.0 - mo_prev)) * (1.0 - exp(-6.93 / rf))
       mo_prev = min(mr, 150.0)
       ```
     - If `mo_prev > 150.0` (equilibrium wetting phase — daily-only branch, unlikely hourly):
       ```
       mr = mo_prev + 42.5 * rf * exp(-100.0 / (251.0 - mo_prev)) * (1.0 - exp(-6.93 / rf))
       mr += 0.0015 * (mo_prev - 150.0)^2 * sqrt(rf)
       mo_prev = min(mr, 250.0)
       ```
   - Missing rain → treat as 0.0mm (no adjustment)

4. **Equilibrium moisture content** (same as daily):
   ```
   ed = 0.942 * h^0.679 + 11.0 * exp((h - 100) / 10) + 0.18 * (21.1 - t) * (1 - exp(-0.115 * h))
   ew = 0.618 * h^0.753 + 10.0 * exp((h - 100) / 10) + 0.18 * (21.1 - t) * (1 - exp(-0.115 * h))
   ```

5. **Drying or wetting branch:**

   **If `mo_prev < ed` (wetting — fuel gaining moisture):**
   ```
   k0w = 0.424 * (1 - ((100 - h) / 100)^1.7) + 0.0694 * sqrt(w) * (1 - ((100 - h) / 100)^8)
   kw = k0w * 0.0579 * exp(0.0365 * t)     ← KEY: 0.0579, not 0.581
   mo = ew - (ew - mo_prev) / (10^kw)
   ```

   **Elif `mo_prev > ed` (drying — fuel losing moisture):**
   ```
   k0d = 0.424 * (1 - (h / 100)^1.7) + 0.0694 * sqrt(w) * (1 - (h / 100)^8)
   kd = k0d * 0.0579 * exp(0.0365 * t)     ← KEY: 0.0579, not 0.581
   mo = ed + (mo_prev - ed) / (10^kd)
   ```

   **Else (`mo_prev == ed`):**
   ```
   mo = mo_prev   (at equilibrium, no change)
   ```

6. **Convert back to FFMC:**
   ```
   mo = max(0.0, mo)
   ffmc = 59.5 * (250.0 - mo) / (147.2 + mo)
   ffmc = max(0.0, ffmc)    ← NO upper cap of 101 in hourly
   ```

**Key differences from daily (current broken code):**

| Aspect | Daily (current) | Hourly (correct) |
|--------|----------------|-----------------|
| Temp scale factor | `0.581` | **`0.0579`** |
| Rain threshold | `0.5mm` | **`0.0mm`** (all rain) |
| Rain < 1.5mm branch | Skips moisture update | **No special branch** |
| FFMC upper cap | 101.0 | **None** (only ≥ 0) |
| Wetting/drying | Single conditional | **Three branches** (wetting/drying/equilibrium) |
| k0 calculation | One formula | **Two formulas** (k0w for wetting, k0d for drying) |

### 12.2 New Function: `_daily_dmc_dc_calc`

Compute DMC and DC once per day from daily-aggregated inputs, not hourly.

**Signature:**
```python
def _daily_dmc_dc_calc(
    hourly_df: pd.DataFrame,
    dmc_prev: float = 6.0,
    dc_prev: float = 15.0,
    lat: float = 46.4,
    halifax_tz: ZoneInfo = ZoneInfo("America/Halifax"),
) -> tuple[np.ndarray, np.ndarray]:
```

**Algorithm:**
1. Convert `timestamp_utc` to local time using `halifax_tz`
2. Extract `local_date` and `local_hour`
3. For each local date:
   - **Temperature:** Value at 14:00 LST (local afternoon peak). If 14:00 missing, use the nearest available hour.
   - **RH:** Value at 14:00 LST (minimum RH coincides with max temp). If missing, nearest hour.
   - **Rain:** Sum of all hourly rain for the local date (24h accumulation).
   - **Month:** From the date
4. Feed into existing `fwi.py` reference functions:
   - `duff_moisture_code(temp, rh, rain, dmc_prev, month, lat)`
   - `drought_code(temp, rain, dc_prev, month, lat)`
5. Return arrays of length `len(hourly_df)` — DMC/DC values repeated for each hour of their respective day (for output alignment)

### 12.3 New Function: `calculate_fwi_hourly`

Replace `calculate_fwi()` as the entry point for extended mode.

**Signature:**
```python
def calculate_fwi_hourly(
    df: pd.DataFrame,
    lat: float = DEFAULT_FWI_LATITUDE,
    gap_threshold_hours: int = 24,
) -> pd.DataFrame:
```

**Algorithm:**
1. Validate required columns (temp, RH, wind, rain)
2. Compute hourly FFMC via `_hffmc_calc` (Van Wagner 1977)
3. Compute daily DMC/DC via `_daily_dmc_dc_calc` (Van Wagner 1987, one value per day)
4. Expand DMC/DC to hourly frequency (repeat daily value across 24 hours)
5. Compute ISI, BUI, FWI from hourly FFMC + daily DMC/DC (same algebra as current)
6. Return DataFrame with all 6 FWI columns at hourly resolution

### 12.4 Rename Current Function

- Rename `calculate_fwi` → `_calculate_fwi_legacy` (keep for comparison testing, mark deprecated)
- New `calculate_fwi_hourly` becomes the active extended-mode function

### 12.5 Pipeline Routing Update

In `run_pipeline()`:
```python
if fwi_mode == "compliant":
    # ... existing compliant path (unchanged)
elif fwi_mode == "hourly":
    hourly = calculate_fwi_hourly(hourly, lat=station_lat, gap_threshold_hours=gap_threshold)
    # ... existing enforcement, diagnostics, aggregation
else:
    # "extended" or default → calculate_fwi_hourly (same as "hourly")
    hourly = calculate_fwi_hourly(hourly, lat=station_lat, gap_threshold_hours=gap_threshold)
```

Note: `extended` and `hourly` become aliases. The `--fwi-mode` values are:
- `compliant` → Van Wagner daily (Phase 11, unchanged)
- `hourly` → Van Wagner hourly (Phase 12, new)
- `extended` → alias for `hourly` (backward compat)

### 12.6 CLI & Config

- `--fwi-mode` accepts: `compliant`, `hourly`, `extended` (default: `hourly`)
- `cleaning-config.json` `fwi.fwi_mode`: `hourly` (was `extended`)
- `cleaning-config.json` `fwi.station_latitudes`: used for hourly DMC/DC day-length factors

### 12.7 Diagnostics

- Report chain breaks for hourly FFMC (same as current)
- Report days where 14:00 LST observation was missing (DMC/DC used nearest-hour fallback)
- No carry-forward concept in hourly mode — chain breaks are the quality signal

### 12.8 Tests

1. **Unit: `_hffmc_calc` against known cffdrs vectors**
   - Single-step: given inputs, verify output matches cffdrs `hffmc()` output
   - Multi-step: 24-hour sequence → verify final FFMC matches cffdrs
   - Rain > 0 threshold: verify all rain applied (no 0.5mm cutoff)
   - Drying vs wetting branch: construct inputs where mo < ed and mo > ed
   - Gap recovery: verify chain restart after ≥ 24h gap

2. **Unit: `_daily_dmc_dc_calc`**
   - Single day: temp/RH/rain at 14:00 → verify DMC/DC match `fwi.py` reference
   - Missing 14:00: verify nearest-hour fallback
   - Daily rain sum: verify hourly rain accumulates correctly
   - Month boundaries: verify month/day-length factor changes correctly

3. **Unit: `calculate_fwi_hourly`**
   - End-to-end: hourly inputs → hourly FFMC + daily DMC/DC → hourly ISI/BUI/FWI
   - DMC/DC constant within day: verify same DMC/DC value for all 24 hours
   - DMC/DC changes between days: verify step changes at midnight local

4. **Integration: legacy comparison**
   - Run same data through `_calculate_fwi_legacy` and `calculate_fwi_hourly`
   - Verify FFMC values are substantially different (the old code was broken)
   - Verify DMC/DC values are close but not identical (daily aggregation vs hourly)

5. **Regression: compliant mode unchanged**
   - Run compliant mode → verify output identical to Phase 11 baseline

6. **CLI: `--fwi-mode hourly`**
   - Verify pipeline runs with hourly mode
   - Verify `--fwi-mode extended` is treated as alias

### 12.9 Output Files

- Hourly CSV: `{station}_hourly.csv` (same as current, now with correct hourly FFMC)
- Daily CSV: `{station}_daily.csv` (aggregated from hourly — FFMC will be daily mean, DMC/DC will be the daily value repeated)
- New column in hourly: `dmc_dc_source_date` (the local date used for that row's DMC/DC calculation, for auditability)

### 12.10 Non-Goals

- No Wotton (2009) "interpolated moisture index" mode — that paper is about spatial interpolation, not hourly FFI
- No per-station latitude for DMC/DC day-length factors in this phase (use default 46.4°, same as Phase 11)
- No changes to compliant mode (Phase 11) — it's correct and stays untouched

## Estimated Scope

- **New code:** ~250 lines (`_hffmc_calc`, `_daily_dmc_dc_calc`, `calculate_fwi_hourly`)
- **Modified:** ~30 lines (pipeline routing in `run_pipeline`, CLI args)
- **Tests:** ~300 lines (6 test categories above)
- **Deleted:** 0 lines (legacy function kept for comparison)

## References

- Van Wagner (1977). "A method of computing fine fuel moisture behaviour throughout the diurnal cycle." Environment Canada, Forestry Service, Information Report PS-X-69.
- Van Wagner (1987). "Development and Structure of the Canadian Forest Fire Weather Index System." Forestry Technical Report 35.
- cffdrs R package v1.9.2, `hffmc.R` — https://cran.r-project.org/package=cffdrs
