# Fire Weather Index (FWI) Calculation

Implemented in `src/pea_met_network/fwi.py`. Applied during Pass 2 after cross-station imputation.

## FWI Chain

Standard Canadian Forest Fire Weather Index system. Sequential moisture codes:

```
FFMC (Fine Fuel Moisture Code)
  → DMC (Duff Moisture Code)
    → DC (Drought Code)
      → ISI (Initial Spread Index)
        → BUI (Buildup Index = f(DMC, DC))
          → FWI = f(ISI, BUI)
```

Each code is a day-over-day recurrence — today's value depends on yesterday's value plus today's weather inputs.

## Modes

### Hourly Mode (default)

- FFMC is stepped hourly via `_hffmc_calc()` — each hour advances the fine fuel moisture code
- DMC and DC are computed **daily** via `_daily_dmc_dc_calc()` — nearest 14:00 LST observation per local date, then broadcast back onto all hourly rows for that date
- ISI, BUI, FWI are derived from FFMC (hourly) × DMC/DC (daily-aligned) at each hour
- On startup (no previous day's value), uses defaults from config
- On gap detection (>24h since last valid observation), resets FFMC to startup defaults
- Chain break diagnostics identify reset points

### Compliant Mode

- Filters to local noon (12:00 in the station's local time, i.e., 15:00 UTC during ADT, 16:00 UTC during AST) observations only
- Daily step — one FWI value per day
- Carry-forward on missing noon obs (flagged with `carry_forward_used`)

## Configuration (`cleaning-config.json` → `fwi`)

| Setting | Value | Notes |
|---|---|---|
| `fwi_mode` | `hourly` | `hourly` or `compliant` |
| `gap_threshold_hours` | 24 | Reset moisture codes after gap |
| `startup_defaults.ffmc` | 85.0 | Used on chain start/reset |
| `startup_defaults.dmc` | 6.0 | Used on chain start/reset |
| `startup_defaults.dc` | 15.0 | Used on chain start/reset |
| `station_latitudes` | `{}` | Override per-station; defaults to PEI (~46.4°N) |

## Required Inputs

| Variable | Purpose |
|---|---|
| `air_temperature_c` | All moisture codes |
| `relative_humidity_pct` | FFMC, DMC |
| `wind_speed_kmh` | FFMC, ISI |
| `rain_mm` | All moisture codes (precipitation dampening) |

## FWI Output Ranges (`fwi_output_ranges`)

| Component | Min | Max |
|---|---|---|
| FFMC | 0.0 | 101.0 |
| DMC | 0.0 | ∞ |
| DC | 0.0 | ∞ |
| ISI | 0.0 | ∞ |
| BUI | 0.0 | ∞ |
| FWI | 0.0 | ∞ |

Values outside ranges are set to NaN (enforcement action: `fwi_out_of_range`).

## Chain Break Diagnostics

`src/pea_met_network/fwi_diagnostics.py` — `diagnose_chain_breaks()` identifies:
- Startup resets (no previous value)
- Gap resets (exceeded `gap_threshold_hours`)
- Records carried forward (compliant mode)

Reported in pipeline logs and available via `chain_breaks_to_dataframe()`.

## Known: Stanley Bridge & Tracadie

These stations lack RH sensors (100% missing `relative_humidity_pct`). Without RH, FWI cannot be computed natively. Cross-station imputation fills RH from Cavendish (primary), Charlottetown A, and North Rustico donors, enabling downstream FWI calculation.
