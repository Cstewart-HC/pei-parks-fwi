# Spec 09: FWI Missingness Diagnostics & Chain Recovery

**Phase:** 9
**Status:** Pending
**Depends on:** Phase 8 (Data Quality Enforcement)

---

## Goal

Add diagnostic instrumentation and chain-recovery logic to the FWI calculation pipeline so that (a) missingness in intermediate moisture codes is properly traced and reported, (b) state-chain breaks are logged, and (c) the FWI calculator can optionally restart its chain after a gap rather than propagating NaNs indefinitely from the first missing input.

---

## Context

### What exists today

The FWI calculation is a sequential chain: each hour's FFMC depends on the previous hour's FFMC, each DMC on the previous DMC, each DC on the previous DC. ISI depends on FFMC, BUI depends on DMC+DC, and FWI depends on ISI+BUI.

```
temp + RH + wind + rain → FFMC(n) depends on FFMC(n-1)
temp + RH + rain        → DMC(n) depends on DMC(n-1)
temp + rain             → DC(n)  depends on DC(n-1)
FFMC + wind             → ISI
DMC + DC                → BUI
ISI + BUI               → FWI
```

When any input is NaN at timestep *i*, the current code sets the output to NaN **and resets the carry-over state to NaN**. From that point forward, every subsequent timestep also produces NaN — the chain is permanently broken.

### The problem

1. **Chain break propagation**: A single missing hour (e.g., a sensor reboot) permanently kills all subsequent FWI values. The chain never recovers. For Greenwich, a 1762-hour RH gap (Jan–Mar 2024) kills DMC/FFMC for the rest of the dataset — even though RH comes back. The only code that survives is DC (because it doesn't need RH).

2. **No diagnostics**: There is no logging or reporting of where chains break, why, or how many rows are affected. The QA/QC report shows aggregate null counts but doesn't distinguish "NaN because input was missing" from "NaN because chain was broken upstream."

3. **No gap-crossing**: Other FWI implementations (e.g., cffdrs R package) use startup values to restart the chain after a gap exceeding a threshold. Our implementation has no such logic — it uses fixed startup defaults only at the very beginning.

### Verified missingness (2026-03-27 pipeline run)

| Station | Temp | RH | Wind | Rain | FFMC | DMC | DC | ISI | BUI | FWI |
|---|---|---|---|---|---|---|---|---|---|---|
| Stanhope | 99.7% | 99.7% | 98.7% | 99.8% | 99.6% | 99.6% | 99.7% | 99.6% | 99.6% | 99.6% |
| North Rustico | 95.9% | 95.9% | 87.0% | 100.0% | 84.1% | 95.9% | 95.9% | 84.1% | 95.9% | 84.1% |
| Cavendish | 85.5% | 85.5% | 81.6% | 100.0% | 82.2% | 85.5% | 85.5% | 82.2% | 85.5% | 82.2% |
| Greenwich | 61.1% | 83.8% | 80.5% | 99.9% | 52.6% | 60.1% | 61.1% | 52.6% | 60.1% | 52.6% |
| Stanley Bridge | 82.8% | 0.0% | 65.8% | 100.0% | 0.0% | 0.0% | 82.8% | 0.0% | 0.0% | 0.0% |
| Tracadie | 58.3% | 0.0% | 72.1% | 100.0% | 0.0% | 0.0% | 87.2% | 0.0% | 0.0% | 0.0% |

Key observations:
- **Stanley Bridge / Tracadie RH = 0%**: Genuine — no RH sensor deployed. Raw CSVs confirm no humidity column exists. Not a bug.
- **Greenwich FFMC = 52.6% vs temp = 61.1%**: FFMC requires temp + RH + wind simultaneously. All three must be non-null. The 52.6% is the intersection coverage — correct behavior, not a bug.
- **Greenwich 1762-hour RH gap (Jan–Mar 2024)**: After this gap, the DMC/FFMC chains are permanently broken. Even after RH returns, the codes stay NaN because the carry-over state was poisoned.
- **North Rustico FFMC = 84.1% vs DMC = 95.9%**: FFMC requires wind (87.0%), DMC does not. Wind is the bottleneck for FFMC. Correct.
- **Stanhope near-complete**: All codes ~99.6%. Clean station.

### What this phase does NOT fix

- **Stanley Bridge / Tracadie missing RH sensor** — no data to recover. These stations cannot produce FFMC/DMC/ISI/BUI/FWI.
- **Sensor gaps in raw data** — the data is what it is. This phase addresses how the pipeline *responds* to gaps, not the gaps themselves.

---

## Deliverables

### 1. Chain Break Diagnostics (`src/pea_met_network/fwi_diagnostics.py`)

New module that produces a structured report of FWI chain breaks.

```python
@dataclass
class ChainBreak:
    station: str
    code: str              # "ffmc", "dmc", "dc"
    break_start: str       # ISO timestamp
    break_end: str | None  # ISO timestamp (None if chain never recovers)
    cause: str             # "input_missing", "quality_enforcement", "startup"
    missing_input: str     # which input column was NaN (e.g., "relative_humidity_pct")
    rows_affected: int     # total rows lost due to this break

def diagnose_chain_breaks(
    hourly_df: pd.DataFrame,
    station: str,
    quality_actions: list[dict] | None = None,
) -> list[ChainBreak]:
    """Analyze FWI columns to identify where state chains break and why."""
```

The function compares the null patterns of each FWI code against its input dependencies:
- FFMC breaks when `temp | RH | wind` is NaN but FFMC's own null region extends beyond the input null region
- DMC breaks when `temp | RH` is NaN but DMC's null region extends beyond
- DC breaks when `temp` is NaN but DC's null region extends beyond

For each chain break, it reports the break start (first NaN after valid values), the break end (next valid value, if any), the cause, and how many rows were lost.

### 2. Chain Recovery Logic (`_ffmc_calc`, `_dmc_calc`, `_dc_calc` in `cleaning.py`)

Add a `gap_threshold_hours` parameter (default: 24) to the FWI calc functions. When the chain has been broken for more than `gap_threshold_hours` consecutive hours and valid inputs resume, restart the chain from startup defaults instead of remaining NaN.

**Current behavior:**
```
Hour 100: RH NaN → FFMC NaN, mo_prev = NaN
Hour 101: RH valid → FFMC NaN (mo_prev still NaN) ← CHAIN DEAD FOREVER
```

**New behavior:**
```
Hour 100: RH NaN → FFMC NaN, mo_prev = NaN, consecutive_nulls = 1
Hour 101: RH valid, consecutive_nulls < threshold → FFMC NaN (gap too short to restart)
...
Hour 124: RH valid, consecutive_nulls >= threshold → FFMC = startup default, chain RESTARTS
```

Implementation approach:
- Track `consecutive_null_count` alongside the carry-over state
- When inputs become valid again after `gap_threshold_hours` null hours, reset carry-over to startup default and resume calculation
- The startup values remain: `ffmc_prev=85.0`, `dmc_prev=6.0`, `dc_prev=15.0`

**Important**: Do NOT restart mid-gap. Only restart when valid inputs are actually present again. Short gaps (< threshold) should continue to produce NaN — this preserves the correctness that short gaps can't be accurately estimated.

### 3. FWI Missingness Report (`data/processed/fwi_missingness_report.csv`)

New pipeline output artifact. One row per chain break event per station.

```csv
station,code,break_start,break_end,cause,missing_input,rows_affected,chain_restarted
greenwich,dmc,2024-01-01T00:00:00Z,2024-03-14T15:00:00Z,input_missing,relative_humidity_pct,1762,true
greenwich,ffmc,2024-01-01T00:00:00Z,2024-03-14T15:00:00Z,input_missing,relative_humidity_pct,1762,true
greenwich,dmc,2026-02-08T14:00:00Z,,input_missing,relative_humidity_pct,1022,false
```

Registered in pipeline manifest as artifact type `fwi_missingness_report`.

### 4. Configuration

Add to `docs/cleaning-config.json`:

```json
{
  "fwi": {
    "gap_threshold_hours": 24,
    "startup_defaults": {
      "ffmc": 85.0,
      "dmc": 6.0,
      "dc": 15.0
    }
  }
}
```

### 5. Pipeline Integration

In `run_pipeline()`:
1. After `calculate_fwi(hourly)`, call `diagnose_chain_breaks()` to produce the report
2. Write `fwi_missingness_report.csv`
3. Register in pipeline manifest

### 6. QA/QC Report Enhancement

The QA/QC report gains a column:

| Column | Description |
|---|---|
| `fwi_chain_breaks` | Count of chain break events for this station |

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-FWI-1 | `src/pea_met_network/fwi_diagnostics.py` exists with `diagnose_chain_breaks()` function |
| AC-FWI-2 | `diagnose_chain_breaks()` correctly identifies chain breaks where null output extends beyond null inputs |
| AC-FWI-3 | `_ffmc_calc()`, `_dmc_calc()`, `_dc_calc()` accept `gap_threshold_hours` parameter |
| AC-FWI-4 | When inputs resume after `gap_threshold_hours` null hours, the chain restarts from startup defaults |
| AC-FWI-5 | When inputs resume before `gap_threshold_hours`, the chain remains NaN (no premature restart) |
| AC-FWI-6 | `docs/cleaning-config.json` contains `fwi` section with `gap_threshold_hours` and `startup_defaults` |
| AC-FWI-7 | `data/processed/fwi_missingness_report.csv` is generated by pipeline with correct columns |
| AC-FWI-8 | FWI missingness report is registered in pipeline manifest |
| AC-FWI-9 | QA/QC report includes `fwi_chain_breaks` column |
| AC-FWI-10 | `pytest tests/ -m 'not e2e' -v` passes (fast suite) |
| AC-FWI-11 | Greenwich FFMC coverage increases after chain recovery (pre-gap + post-gap both populated) |
| AC-FWI-12 | Stanley Bridge FFMC/DMC remain 0% (no RH sensor — recovery cannot help) |

---

## Exit Gate

```bash
pytest tests/ -m 'not e2e' -v
```

All tests must pass. The fast suite must complete in under 30 seconds.

---

## What This Phase Does NOT Do

- **Does not** impute missing weather inputs (that's the imputation step's job, and it already handles gaps ≤ 6 hours)
- **Does not** add station-specific startup values (startup defaults are global)
- **Does not** change FWI calculation formulas (only chain restart logic)
- **Does not** modify non-FWI columns
- **Does not** remove existing tests or change Phase 1–8 acceptance criteria

---

## Expected Impact

After this phase, stations with intermittent sensor gaps (Greenwich, North Rustico) should see **higher FWI code coverage** because chains will restart after long gaps instead of dying permanently. Stations with permanently missing sensors (Stanley Bridge RH, Tracadie RH) will see **no change** — recovery requires valid inputs to resume.

Estimated coverage improvement for Greenwich:
- FFMC: 52.6% → ~65–70% (chains restart after the 1762-hour and 1022-hour RH gaps)
- DMC: 60.1% → ~72–77% (same)
