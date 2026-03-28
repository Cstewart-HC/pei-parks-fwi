# Spec 10: Cross-Station Variable Imputation

**Phase:** 10
**Status:** Draft
**Depends on:** Phase 9 (FWI Missingness Diagnostics & Chain Recovery)
**Supersedes:** Original draft `10-cross-station-rh-imputation.md` (expanded scope)

---

## Goal

Synthesize missing meteorological variables (RH, wind speed, temperature) for stations that lack sensors or have intermittent gaps, using physically-grounded transfer methods from donor stations. This enables FWI calculation for the two stations that currently produce 0% FWI output and improves coverage across the entire network.

---

## Context

### The problem

Six PEI weather stations feed the FWI pipeline. The verified missingness (Phase 9) shows multiple gap types:

| Station | Temp | RH | Wind | Rain | FFMC | DMC | DC | ISI | BUI | FWI |
|---|---|---|---|---|---|---|---|---|---|---|
| Stanhope | 99.7% | 99.7% | **98.7%** | 99.8% | 99.6% | 99.6% | 99.7% | 99.6% | 99.6% | 99.6% |
| North Rustico | 95.9% | 95.9% | **87.0%** | 100.0% | 84.1% | 95.9% | 95.9% | 84.1% | 95.9% | 84.1% |
| Cavendish | 85.5% | 85.5% | **81.6%** | 100.0% | 82.2% | 85.5% | 85.5% | 82.2% | 85.5% | 82.2% |
| Greenwich | 61.1% | 83.8% | **80.5%** | 99.9% | 52.6% | 60.1% | 61.1% | 52.6% | 60.1% | 52.6% |
| Stanley Bridge | 82.8% | **0.0%** | 65.8% | 100.0% | 0.0% | 0.0% | 82.8% | 0.0% | 0.0% | 0.0% |
| Tracadie | 58.3% | **0.0%** | 72.1% | 100.0% | 0.0% | 0.0% | 87.2% | 0.0% | 0.0% | 0.0% |

**Three gap categories:**

1. **Permanent sensor absence**: Stanley Bridge and Tracadie have no RH sensor → 0% RH, 0% FWI (all moisture-dependent codes).
2. **Intermittent sensor gaps**: Greenwich has a 1762-hour RH gap (Jan–Mar 2024) + scattered wind gaps. North Rustico, Cavendish have wind gaps that bottleneck FFMC/ISI below their DMC/DC coverage.
3. **Wind as FFMC/ISI bottleneck**: At North Rustico, Cavendish, and Greenwich, wind coverage (80–87%) is the limiting factor for FFMC/ISI — not RH.

### Physical basis for each variable

#### RH: Vapor Pressure Continuity
Water vapor pressure (e) is a spatially continuous field at the mesoscale (~10–50 km). Over PEI's short inter-station distances (5–40 km), actual vapor pressure changes gradually. Temperature can differ (microclimate, elevation, marine effects), but absolute moisture content is similar.

**Method:**
1. At donor, calculate actual vapor pressure: `e = (RH / 100) × es(T_donor)`
2. Transfer `e` to target (assume spatial continuity)
3. Solve for target RH using target's local temperature: `RH_target = 100 × e / es(T_target)`
4. Cap at 100%

Where `es(T)` is the August-Roche-Magnus formula:
```
es(T) = 0.61094 × exp(17.625 × T / (T + 243.04))   [kPa]
```

**Critical**: We transfer *moisture*, not RH. This respects local temperature differences between stations.

#### RH: Dew Point Derivation (Precision Path)
When the ECCC API provides both `DEW_POINT_TEMP` and `TEMP`, RH can be derived directly via inverse Magnus-Tetens — this is **more precise** than using the API's pre-calculated `RELATIVE_HUMIDITY` field, which is an integer (0–100) and may have rounding errors.

At 25°C, 1% RH rounding = ~0.032 kPa vapor pressure uncertainty. A 0.1°C dew point precision = ~0.007 kPa. The dew point path is ~4.5× more precise.

**Priority for RH derivation from ECCC donors:**
1. `DEW_POINT_TEMP` + `TEMP` → inverse Magnus (highest precision)
2. `RELATIVE_HUMIDITY` (integer, fallback only)

#### Wind Speed: Spatial Proxy with Height Correction
Wind is spatially continuous at the mesoscale. Cross-station wind transfer is reasonable — wind speed and direction at nearby PEI stations should correlate well.

**Height correction (critical):** ECCC stations typically report at 10m (WMO standard). PEINP stations may use 2–3m tripod-mounted anemometers. The power law correction:

```
v(z) = v(z_ref) × (z / z_ref)^α
```

where α ≈ 0.14 for open terrain.

**10m → 3m example:** `v(3) = v(10) × (3/10)^0.14 = v(10) × 0.827` — a 17.3% reduction.

ISI scales non-linearly with wind speed, so uncorrected height differences would compound in FWI.

**Height metadata requirement:** If PEINP anemometer heights are unknown, derive scaling factor `k` empirically from overlapping good-data periods between ECCC and PEINP stations (minimum 1 week of concurrent valid data).

#### Temperature: Spatial Proxy with Outlier Guard
Temperature is highly spatially correlated at PEI scale. However, marine inversions can cause 2–3°C differences between coastal (Tracadie Wharf) and inland (Harrington) stations on summer afternoons.

**Asymmetric outlier cap:** For fire weather, warm bias (donor warmer than target) understates RH → inflates fire danger. Cool bias overstates RH → understates fire danger. The cap must be asymmetric to err on the side of caution:

| Scenario | Cap | Rationale |
|---|---|---|
| Donor warmer than target | ±2°C | More dangerous — understates fire risk |
| Donor cooler than target | ±3°C | Less dangerous — overstates fire risk |

Values exceeding the cap are flagged as `qf=2` (uncertain) rather than rejected outright.

### What exists today

The pipeline flow is:
```
load → dedup → resample → enforce_quality → impute → calculate_fwi → enforce_fwi_outputs → output
```

- `impute()` fills gaps ≤ 6 hours via linear interpolation. It does **not** cross station boundaries.
- Each station is processed independently in `run_pipeline()`.
- Quality enforcement produces `_quality_flags` column (JSON array of action strings).
- The ECCC Stanhope adapter (`stanhope_cache.py`) downloads bulk CSV data via legacy endpoint.
- No cross-station data transfer exists.

### Data source: MSC GeoMet OGC API

The **MSC GeoMet API** (`https://api.weather.gc.ca/`) provides the `climate-hourly` collection with standardized hourly observations.

**API endpoint:**
```
https://api.weather.gc.ca/collections/climate-hourly/items
  ?CLIMATE_IDENTIFIER=8300562
  &datetime=2022-01-01T00:00:00Z/2026-03-27T23:59:59Z
  &limit=10000
```

**Key API fields → pipeline columns:**
| API Field | Pipeline Column | Precision | Notes |
|---|---|---|---|
| `DEW_POINT_TEMP` | `dew_point_c` | 0.1°C | **Primary RH source** — most precise |
| `RELATIVE_HUMIDITY` | `relative_humidity_pct` | Integer (0–100) | Fallback only — rounding imprecise |
| `TEMP` | `air_temperature_c` | 0.1°C | Standard precision |
| `WIND_SPEED` | `wind_speed_kmh` | 1 km/h | Requires height correction |
| `WIND_DIRECTION` | `wind_direction_deg` | 10° | Standard |
| `PRECIP_AMOUNT` | `rain_mm` | 0.1 mm | Not used for cross-station (spatially discontinuous) |

**PEINP stations are NOT on this API.** Confirmed: Cavendish, Greenwich, North Rustico, Stanley Bridge, and Tracadie (PEI) are absent from the `climate-hourly` collection. Internal station data continues from Parks Canada CSVs.

**What the API does NOT have:**
- Solar radiation — not addressable via this API
- Precipitation is available but spatially discontinuous (convective showers) — not suitable for cross-station transfer (deferred)

### Donor Station Registry

#### External (ECCC) Donor Stations

| Station | Climate ID | STN_ID | API Records | RH | Td | Wind | Temp | Notes |
|---|---|---|---|---|---|---|---|---|
| **St. Peters** | `8300562` | 41903 | 195,844 | ✅ | ✅ | ✅ | ✅ | Greenwich P1. East PEI. Closest to Greenwich. |
| **Charlottetown A** | `8300300` | 6526 | 522,806 | ✅ | ✅ | ✅ | ✅ | Stanley Bridge P2, Stanhope P3, Tracadie P3. Central PEI. Last data ~Jan 2024. |
| **Harrington CDA CS** | `830P001` | 30308 | 184,000+ | ✅ | ✅ | ✅ | ✅ | Greenwich P2. Central PEI. |

#### Internal (PEINP) Donor Stations

| Station | Internal ID | Has RH | Has Wind | Has Temp | Notes |
|---|---|---|---|---|---|
| **Cavendish** | `cavendish` | ✅ | ✅ | ✅ | Stanley Bridge P1, Tracadie P1. ~5 km from Stanley Bridge. |
| **North Rustico** | `north_rustico` | ✅ | ✅ | ✅ | Tracadie P2, Greenwich P3. ~15 km from Tracadie. |
| **Stanhope** | `stanhope` | ✅ | ✅ | ✅ | ECCC station, internal to pipeline. ~15 km from Greenwich. |

#### Stations That Must NEVER Be Donors

| Station | Reason |
|---|---|
| **Stanley Bridge** | No RH sensor — 100% RH missing. All variables unreliable as donor. |
| **Tracadie** | No RH sensor — 100% RH missing. All variables unreliable as donor. |

### Donor Priority Assignments

Each variable type has its own donor priority list per target station. This reflects that the best RH donor may not be the best wind donor.

#### RH Donor Priorities

| Target Station | P1 (nearest/primary) | P2 | P3 |
|---|---|---|---|
| **Stanley Bridge** | Cavendish (~5 km) | Charlottetown A (~30 km) | North Rustico (~20 km) |
| **Tracadie** | Cavendish (~25 km) | North Rustico (~15 km) | Charlottetown A (~35 km) |
| **Greenwich** | St. Peters (~15 km) | Harrington CDA (~25 km) | North Rustico (~20 km) |

#### Wind Speed Donor Priorities

| Target Station | P1 | P2 | P3 |
|---|---|---|---|
| **Stanley Bridge** | Cavendish (~5 km) | Charlottetown A (~30 km) | North Rustico (~20 km) |
| **Tracadie** | North Rustico (~15 km) | Cavendish (~25 km) | Charlottetown A (~35 km) |
| **Greenwich** | St. Peters (~15 km) | Stanhope (~15 km) | Harrington CDA (~25 km) |
| **North Rustico** | Stanhope (~15 km) | Charlottetown A (~25 km) | Cavendish (~10 km) |
| **Cavendish** | North Rustico (~10 km) | Stanhope (~20 km) | Charlottetown A (~20 km) |

#### Temperature Donor Priorities

| Target Station | P1 | P2 | P3 |
|---|---|---|---|
| **Stanley Bridge** | Cavendish (~5 km) | Charlottetown A (~30 km) | North Rustico (~20 km) |
| **Tracadie** | North Rustico (~15 km) | Cavendish (~25 km) | Charlottetown A (~35 km) |
| **Greenwich** | St. Peters (~15 km) | Stanhope (~15 km) | Harrington CDA (~25 km) |

**Note:** Internal stations (Cavendish, North Rustico) with RH sensors don't need RH imputation for themselves — they serve as donors for no-sensor stations and as gap-fillers for Greenwich. They DO benefit from wind and temperature imputation when their own sensors have gaps.

---

## Deliverables

### 1. MSC GeoMet Client (`src/pea_met_network/eccc_api.py`)

New module for fetching hourly climate data from the MSC GeoMet OGC API.

```python
@dataclass(frozen=True)
class EcccStation:
    climate_id: str           # e.g. "8300562"
    stn_id: int               # API numeric ID, e.g. 41903
    name: str                 # e.g. "St. Peters"
    local_tz: str             # e.g. "America/Halifax"
    anemometer_height_m: float  # default 10.0 (WMO standard)

# Verified station registry
ECCC_DONOR_STATIONS: dict[str, EcccStation] = {
    "st_peters":      EcccStation("8300562", 41903, "St. Peters", "America/Halifax", 10.0),
    "charlottetown_a": EcccStation("8300300", 6526, "Charlottetown A", "America/Halifax", 10.0),
    "harrington_cda": EcccStation("830P001", 30308, "Harrington CDA CS", "America/Halifax", 10.0),
}

def fetch_eccc_hourly(
    station: EcccStation,
    start: datetime,
    end: datetime,
    *,
    limit: int = 10_000,
    cache_dir: Path | None = None,
    force: bool = False,
) -> pd.DataFrame:
    """Fetch hourly observations from MSC GeoMet climate-hourly collection.

    Returns DataFrame with columns:
        timestamp_utc, air_temperature_c, relative_humidity_pct,
        wind_speed_kmh, wind_direction_deg, rain_mm, dew_point_c

    All numeric columns use errors='coerce'.
    Timestamps converted to UTC.
    Results cached locally as Parquet files.
    """

def normalize_eccc_response(
    features: list[dict],
    station_name: str,
    local_tz: str,
) -> pd.DataFrame:
    """Convert OGC API GeoJSON features to normalized DataFrame."""
```

**Key design decisions:**
- **Caching**: Parquet under `data/raw/eccc/{station_key}/`. One-time fetch.
- **Pagination**: Automatic `next` link following.
- **Rate limiting**: 1-second delay between requests.
- **Provenance**: `provenance.json` alongside cache files (station, date range, retrieved_at, row count).
- **No API key required**: MSC GeoMet is open access.
- **Anemometer height metadata**: Stored in `EcccStation` for height correction.

### 2. Vapor Pressure Module (`src/pea_met_network/vapor_pressure.py`)

Pure math module — no I/O, no pandas dependency beyond numpy arrays.

```python
def saturation_vapor_pressure(temp_c: np.ndarray) -> np.ndarray:
    """August-Roche-Magnus formula. Returns es in kPa.

    es(T) = 0.61094 × exp(17.625 × T / (T + 243.04))
    """

def actual_vapor_pressure(temp_c: np.ndarray, rh_pct: np.ndarray) -> np.ndarray:
    """Calculate actual vapor pressure from T and RH.

    e = (RH / 100) × es(T)
    """

def rh_from_vapor_pressure(
    temp_c: np.ndarray,
    vapor_pressure_kpa: np.ndarray,
) -> np.ndarray:
    """Derive RH from temperature and vapor pressure.

    RH = 100 × e / es(T)
    Capped at 100.0.
    """

def rh_from_dew_point(
    temp_c: np.ndarray,
    dew_point_c: np.ndarray,
) -> np.ndarray:
    """Derive RH from temperature and dew point (inverse Magnus-Tetens).

    This is the HIGHEST PRECISION path for RH from ECCC donors.
    Uses dew point to compute actual vapor pressure, then divides
    by saturation vapor pressure at the observed temperature.

    Preferred over direct RELATIVE_HUMIDITY field because:
    - ECCC RELATIVE_HUMIDITY is integer (1% precision floor)
    - Dew point is 0.1°C precision → ~4.5× more precise vapor pressure

    Returns RH capped at 100.0.
    """
```

### 3. Cross-Station Imputer (`src/pea_met_network/cross_station_impute.py`)

Core module that orchestrates donor data loading, temporal merge, and variable synthesis for all three variable types.

```python
@dataclass(frozen=True)
class DonorAssignment:
    target: str            # internal station key (e.g. "stanley_bridge")
    variable: str          # "relative_humidity_pct" | "wind_speed_kmh" | "air_temperature_c"
    priority: int          # 1 = first choice
    donor_key: str         # internal key for PEINP donors, ECCC key for external
    donor_type: str        # "internal" | "external"
    max_gap_hours: int     # default: 3 — fall back to next donor if gap exceeds this

@dataclass(frozen=True)
class HeightCorrection:
    """Wind speed height correction parameters."""
    donor_height_m: float        # anemometer height at donor station
    target_height_m: float       # anemometer height at target station
    alpha: float = 0.14          # power law exponent (open terrain)
    empirically_derived: bool = False  # True if k was fit from overlapping data

@dataclass(frozen=True)
class ImputedValue:
    """Record of a single imputed value for the audit trail."""
    station: str
    timestamp_utc: str
    variable: str
    imputed_value: float
    quality_flag: int       # 0=observed, 1=synthetic, 2=uncertain, 9=failed
    source: str             # "INTERNAL:cavendish" | "ECCC:8300562"
    method: str             # "VP_CONTINUITY" | "TD_DERIVED" | "SPATIAL_PROXY" | "HEIGHT_SCALED"
    donor_priority: int     # 1, 2, or 3


def impute_cross_station(
    target_df: pd.DataFrame,
    station: str,
    donor_assignments: list[DonorAssignment] | None = None,
    height_corrections: dict[str, HeightCorrection] | None = None,
    internal_hourly: dict[str, pd.DataFrame] | None = None,
    eccc_cache_dir: Path | None = None,
) -> tuple[pd.DataFrame, list[ImputedValue]]:
    """Synthesize missing variables using cross-station transfer.

    For each missing hour at target station, for each variable:
    1. Try P1 donor. If donor has valid data AND no gap > max_gap_hours
       → transfer using appropriate method (VP continuity, spatial proxy, etc.)
    2. If P1 unavailable, try P2, then P3
    3. If no donor available, value stays NaN, flag = 9

    Variable-specific methods:
    - RH: Vapor pressure continuity (internal donors) or Td-derived (ECCC donors)
    - Wind: Spatial proxy with height correction
    - Temp: Spatial proxy with asymmetric outlier cap

    Adds audit columns to the returned DataFrame (see Audit Trail section).

    Returns (augmented_df, imputation_records).
    """


def derive_height_correction_factor(
    target_df: pd.DataFrame,
    donor_df: pd.DataFrame,
    min_overlap_hours: int = 168,  # 1 week
) -> HeightCorrection | None:
    """Empirically derive wind speed height correction from overlapping good data.

    Fits k = median(v_donor / v_target) over concurrent non-NaN hours.
    Returns None if insufficient overlap data.

    Used when anemometer heights are unknown — derive the correction
    empirically rather than assuming standard heights.
    """


def _rh_from_donor(
    donor_row: pd.Series,
    target_temp: float,
    is_eccc: bool,
) -> tuple[float, str]:
    """Derive RH at target station from donor data.

    For ECCC donors: prefer Td+T path, fall back to RH integer.
    For internal donors: use VP continuity (T + RH at donor).

    Returns (rh_value, method_string).
    """


def _transfer_wind(
    donor_wind: float,
    height_correction: HeightCorrection | None,
) -> tuple[float, str]:
    """Transfer wind speed with height correction.

    If height_correction provided and not empirical:
        Apply power law: v_target = v_donor × (z_target / z_donor)^α

    If height_correction is empirical:
        Apply derived k factor: v_target = v_donor × k

    If no correction: return donor_wind directly, method="SPATIAL_PROXY_RAW"

    Returns (wind_value, method_string).
    """


def _transfer_temp(
    donor_temp: float,
    target_station: str,
) -> tuple[float, int]:
    """Transfer temperature with asymmetric outlier guard.

    Caps:
    - Donor warmer than typical: ±2°C (dangerous for fire risk)
    - Donor cooler than typical: ±3°C (less dangerous)

    Returns (temp_value, quality_flag).
    qf=1 if within cap, qf=2 if cap applied, None if rejected.
    """
```

**Chain-break rule:**
When a donor's data has a gap > `max_gap_hours` (default 3), that donor is **skipped for the entire gap period** and the next priority donor is tried. This prevents stitching across large gaps where spatial continuity may not hold.

**Temporal alignment:**
All donor data is aligned to hourly timestamps in UTC before merging. Internal PEINP donor data is read from `data/processed/{station}/station_hourly.csv`. External ECCC donor data is loaded from the OGC API Parquet cache.

### 4. Audit Trail & Quality Flagging

This is the most critical non-functional requirement. Every imputed value must be traceable to its source, method, and confidence level.

#### Quality Flag Schema

| Flag | Value | Meaning |
|---|---|---|
| `qf` | 0 | Observed — original sensor reading (existing behavior) |
| `qf` | 1 | Synthetic — cross-station imputation, within normal bounds |
| `qf` | 2 | Uncertain — synthetic but outlier cap applied or marginal conditions |
| `qf` | 9 | Failed — no donor available, value remains NaN |

#### Per-Variable Audit Columns

Each imputed variable gets a triplet of audit columns:

| Column | Type | Example | Description |
|---|---|---|---|
| `{var}_qf` | int | `1` | Quality flag for this specific variable |
| `{var}_src` | str | `INTERNAL:cavendish` or `ECCC:8300562` | Source station (standardized format) |
| `{var}_method` | str | `VP_CONTINUITY`, `TD_DERIVED`, `SPATIAL_PROXY`, `HEIGHT_SCALED` | How the value was derived |

**Source format convention:**
```
INTERNAL:{station_name}    e.g., INTERNAL:NORTH_RUSTICO
ECCC:{climate_id}          e.g., ECCC:8300562
```

This makes programmatic parsing trivial.

**Method strings:**
| Method | Variable | Meaning |
|---|---|---|
| `VP_CONTINUITY` | RH | Vapor pressure transferred from donor T+RH, solved at target T |
| `TD_DERIVED` | RH | Dew point + temp at donor → RH (ECCC precision path) |
| `RH_INTEGER` | RH | Direct ECCC RELATIVE_HUMIDITY field (fallback, lower precision) |
| `SPATIAL_PROXY` | Wind, Temp | Direct spatial transfer |
| `HEIGHT_SCALED` | Wind | Spatial proxy with anemometer height correction applied |
| `INTERPOLATED_DONOR` | Any | Donor had short internal gap (< 3h), interpolated across it |

#### Data Lineage Logic (Per Hour, Per Variable)

```
1. Check Local:  Is value non-null?        → qf=0, src=null, method=null
2. Check P1:     Is P1 donor available?    → qf=1, src=P1, method=appropriate
3. Check P2:     Is P2 donor available?    → qf=1, src=P2, method=appropriate
4. Check P3:     Is P3 donor available?    → qf=1, src=P3, method=appropriate
5. Final Fail:   All donors null            → qf=9, src=null, method=null
```

**Uncertain flag (qf=2) applied when:**
- Temperature transfer exceeds asymmetric cap (value kept but flagged)
- Donor had marginal data (near gap threshold)
- Height correction is empirical with low overlap confidence

#### FWI Quality Flag Propagation

Any synthetic input → FWI flagged synthetic. This is non-negotiable for Parks Canada reporting.

```python
# After cross-station imputation, before FWI calculation:
# Build a composite input quality flag per row
input_qf = max(row["relative_humidity_pct_qf"],
               row["wind_speed_kmh_qf"],
               row["air_temperature_c_qf"],
               row["rain_mm_qf"])

# After FWI calculation:
# Propagate to FWI output columns
for fwicol in ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]:
    df.loc[input_qf > 0, f"{fwicol}_qf"] = input_qf
    df.loc[input_qf == 0, f"{fwicol}_qf"] = 0
```

**Preventing "synthetic inflation" of fire risk in official reports**: The FWI_qf column ensures analysts can filter out synthetic values. A Parks Canada report should present observed and synthetic FWI separately.

#### Example Output Row (Tracadie Wharf)

| Timestamp | Temp | Temp_qf | Temp_src | Temp_method | RH | RH_qf | RH_src | RH_method | Wind | Wind_qf | Wind_src | Wind_method |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 2026-07-15 12:00 | 22.4 | 0 | — | — | 72.4 | 1 | INTERNAL:cavendish | VP_CONTINUITY | 15.2 | 0 | — | — |
| 2026-07-15 13:00 | 21.8 | 0 | — | — | 78.1 | 1 | ECCC:830P001 | TD_DERIVED | 18.5 | 1 | ECCC:830P001 | HEIGHT_SCALED |
| 2026-07-15 14:00 | 23.1 | 1 | INTERNAL:north_rustico | SPATIAL_PROXY | 71.2 | 1 | INTERNAL:cavendish | VP_CONTINUITY | 14.0 | 2 | ECCC:830P001 | HEIGHT_SCALED |

### 5. Configuration

Add to `docs/cleaning-config.json`:

```json
{
  "cross_station_impute": {
    "enabled": true,
    "max_gap_hours": 3,
    "height_corrections": {
      "default_donor_height_m": 10.0,
      "default_target_height_m": null,
      "alpha": 0.14,
      "empirical_min_overlap_hours": 168
    },
    "temp_outlier_caps": {
      "warm_bias_max_c": 2.0,
      "cool_bias_max_c": 3.0
    },
    "donor_assignments": {
      "rh": [
        {"target": "stanley_bridge", "priority": 1, "donor": "cavendish",       "type": "internal"},
        {"target": "stanley_bridge", "priority": 2, "donor": "charlottetown_a", "type": "external"},
        {"target": "stanley_bridge", "priority": 3, "donor": "north_rustico",   "type": "internal"},
        {"target": "tracadie",       "priority": 1, "donor": "cavendish",       "type": "internal"},
        {"target": "tracadie",       "priority": 2, "donor": "north_rustico",   "type": "internal"},
        {"target": "tracadie",       "priority": 3, "donor": "charlottetown_a", "type": "external"},
        {"target": "greenwich",      "priority": 1, "donor": "st_peters",       "type": "external"},
        {"target": "greenwich",      "priority": 2, "donor": "harrington_cda",  "type": "external"},
        {"target": "greenwich",      "priority": 3, "donor": "north_rustico",   "type": "internal"}
      ],
      "wind_speed_kmh": [
        {"target": "stanley_bridge", "priority": 1, "donor": "cavendish",       "type": "internal"},
        {"target": "stanley_bridge", "priority": 2, "donor": "charlottetown_a", "type": "external"},
        {"target": "stanley_bridge", "priority": 3, "donor": "north_rustico",   "type": "internal"},
        {"target": "tracadie",       "priority": 1, "donor": "north_rustico",   "type": "internal"},
        {"target": "tracadie",       "priority": 2, "donor": "cavendish",       "type": "internal"},
        {"target": "tracadie",       "priority": 3, "donor": "charlottetown_a", "type": "external"},
        {"target": "greenwich",      "priority": 1, "donor": "st_peters",       "type": "external"},
        {"target": "greenwich",      "priority": 2, "donor": "stanhope",        "type": "internal"},
        {"target": "greenwich",      "priority": 3, "donor": "harrington_cda",  "type": "external"},
        {"target": "north_rustico",  "priority": 1, "donor": "stanhope",        "type": "internal"},
        {"target": "north_rustico",  "priority": 2, "donor": "charlottetown_a", "type": "external"},
        {"target": "north_rustico",  "priority": 3, "donor": "cavendish",       "type": "internal"},
        {"target": "cavendish",      "priority": 1, "donor": "north_rustico",   "type": "internal"},
        {"target": "cavendish",      "priority": 2, "donor": "stanhope",        "type": "internal"},
        {"target": "cavendish",      "priority": 3, "donor": "charlottetown_a", "type": "external"}
      ],
      "air_temperature_c": [
        {"target": "stanley_bridge", "priority": 1, "donor": "cavendish",       "type": "internal"},
        {"target": "stanley_bridge", "priority": 2, "donor": "charlottetown_a", "type": "external"},
        {"target": "stanley_bridge", "priority": 3, "donor": "north_rustico",   "type": "internal"},
        {"target": "tracadie",       "priority": 1, "donor": "north_rustico",   "type": "internal"},
        {"target": "tracadie",       "priority": 2, "donor": "cavendish",       "type": "internal"},
        {"target": "tracadie",       "priority": 3, "donor": "charlottetown_a", "type": "external"},
        {"target": "greenwich",      "priority": 1, "donor": "st_peters",       "type": "external"},
        {"target": "greenwich",      "priority": 2, "donor": "stanhope",        "type": "internal"},
        {"target": "greenwich",      "priority": 3, "donor": "harrington_cda",  "type": "external"}
      ]
    },
    "eccc_stations": {
      "st_peters":       {"climate_id": "8300562", "stn_id": 41903, "name": "St. Peters",         "anemometer_height_m": 10.0},
      "charlottetown_a": {"climate_id": "8300300", "stn_id": 6526,  "name": "Charlottetown A",   "anemometer_height_m": 10.0},
      "harrington_cda":  {"climate_id": "830P001", "stn_id": 30308, "name": "Harrington CDA CS",  "anemometer_height_m": 10.0}
    }
  }
}
```

### 6. Pipeline Integration

The cross-station imputation step inserts **after** `impute()` and **before** `calculate_fwi()`:

```
load → dedup → resample → enforce_quality → impute → ★ cross_station_impute ★ → calculate_fwi → enforce_fwi_outputs → output
```

**Phase interaction:** Phase 10 cross-station imputation runs **before** Phase 9 chain recovery (which is embedded in `calculate_fwi`). Filled data may reduce the number of chain breaks that Phase 9's gap-threshold restart needs to handle. Phase 10 effectively improves Phase 9's outcomes without changing Phase 9's logic.

**Pre-loading requirement:** The pipeline currently processes stations independently (`run_pipeline()` loops per-station). Cross-station imputation requires donor station data to be available when processing a target station.

**Implementation approach (Option A — recommended):**
1. At the start of `run_pipeline()`, pre-load all internal station hourly data into a `dict[str, pd.DataFrame]`
2. For stations in the donor assignment list, call `impute_cross_station()` passing both the target DataFrame and the pre-loaded internal hourly data
3. External ECCC donor data is loaded from Parquet cache on demand (fetched once via `fetch_eccc_donors.py`)
4. After cross-station imputation, proceed to `calculate_fwi()` which now receives more complete inputs
5. After FWI calculation, propagate quality flags to FWI output columns

**Integration points in `cleaning.py`:**
```python
# In run_pipeline(), after the impute() call:

if cross_station_config.get("enabled", False):
    hourly, impute_records = impute_cross_station(
        hourly,
        station,
        donor_assignments=build_assignments(cross_station_config, station),
        internal_hourly=all_internal_hourly,  # pre-loaded
        eccc_cache_dir=ECCC_CACHE_DIR,
    )
    all_impute_records.extend(impute_records)

# After calculate_fwi():
# Propagate input quality flags to FWI outputs
hourly = propagate_fwi_quality_flags(hourly)
```

### 7. ECCC Donor Data Fetch Script (`src/pea_met_network/fetch_eccc_donors.py`)

Standalone script to pre-populate the ECCC donor cache before pipeline runs.

```bash
# Fetch all configured donor stations
python -m pea_met_network.fetch_eccc_donors --start 2022-01-01 --end 2026-03-27

# Fetch a specific station
python -m pea_met_network.fetch_eccc_donors --station st_peters --start 2022-01-01

# Dry run (report row counts without fetching)
python -m pea_met_network.fetch_eccc_donors --dry-run
```

This can be run as a one-time setup step or added to the orchestrator's pre-pipeline phase.

### 8. Cross-Station Imputation Report

New pipeline output artifact: `data/processed/cross_station_impute_report.csv`

```csv
station,timestamp_utc,variable,imputed_value,quality_flag,source,method,donor_priority
stanley_bridge,2023-07-15T14:00:00Z,relative_humidity_pct,75.2,1,INTERNAL:cavendish,VP_CONTINUITY,1
tracadie,2023-08-01T08:00:00Z,relative_humidity_pct,64.9,1,INTERNAL:cavendish,VP_CONTINUITY,1
greenwich,2024-01-15T12:00:00Z,relative_humidity_pct,92.8,1,ECCC:8300562,TD_DERIVED,1
greenwich,2024-06-20T15:00:00Z,wind_speed_kmh,12.4,1,ECCC:8300562,HEIGHT_SCALED,1
tracadie,2024-07-10T13:00:00Z,air_temperature_c,23.1,2,INTERNAL:north_rustico,SPATIAL_PROXY,2
```

Registered in pipeline manifest as artifact type `cross_station_impute_report`.

### 9. QA/QC Report Enhancement

The QA/QC report gains columns:

| Column | Description |
|---|---|
| `rh_imputed_count` | Hours where RH was synthesized |
| `rh_imputed_pct` | Percentage of total hours with synthesized RH |
| `rh_primary_donor` | Donor station that filled the most hours |
| `rh_method_breakdown` | JSON: count per method (VP_CONTINUITY, TD_DERIVED, RH_INTEGER) |
| `wind_imputed_count` | Hours where wind was synthesized |
| `wind_imputed_pct` | Percentage of total hours with synthesized wind |
| `temp_imputed_count` | Hours where temperature was synthesized |
| `temp_imputed_pct` | Percentage of total hours with synthesized temperature |
| `fwi_synthetic_pct` | Percentage of FWI values derived from any synthetic input |

### 10. Tests

New test module: `tests/test_cross_station_impute.py`

| Test | Description |
|---|---|
| `test_saturation_vapor_pressure_known_values` | ARM formula against published reference values |
| `test_actual_vapor_pressure_roundtrip` | e(T,RH) → RH(T,e) returns original RH |
| `test_rh_from_dew_point_precision` | Td-derived RH matches expected precision |
| `test_rh_capped_at_100` | Synthesized RH never exceeds 100% |
| `test_rh_nan_when_donor_missing` | NaN propagated when no donor has data |
| `test_rh_dew_point_preferred_over_integer` | Td path used when both Td and RH available |
| `test_donor_fallback_on_gap` | P2 used when P1 has gap > max_gap_hours |
| `test_wind_height_correction` | Power law applied correctly for 10m→3m |
| `test_wind_height_empirical` | Empirical k derived from overlapping data |
| `test_temp_asymmetric_cap` | Warm bias capped tighter than cool bias |
| `test_audit_columns_present` | `{var}_qf`, `{var}_src`, `{var}_method` in output |
| `test_fwi_quality_flag_propagation` | Synthetic input → FWI_qf > 0 |
| `test_stanley_bridge_fwi_coverage` | FWI > 0% after imputation (currently 0%) |
| `test_tracadie_fwi_coverage` | FWI > 0% after imputation (currently 0%) |
| `test_observed_values_unchanged` | Non-null values are never overwritten |
| `test_guardrail_no_synthetic_donors` | Stanley Bridge and Tracadie rejected as donors |
| `test_eccc_api_client_response_parsing` | Mock API response parsed correctly |
| `test_eccc_cache_write_read` | Parquet cache roundtrip preserves data |
| `test_source_format_standardized` | Sources are `INTERNAL:name` or `ECCC:id` |
| `test_quality_flag_values_valid` | Only 0, 1, 2, 9 appear in qf columns |

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-10-01 | `src/pea_met_network/vapor_pressure.py` exists with `saturation_vapor_pressure()`, `actual_vapor_pressure()`, `rh_from_vapor_pressure()`, `rh_from_dew_point()` |
| AC-10-02 | `src/pea_met_network/eccc_api.py` exists with `fetch_eccc_hourly()` function |
| AC-10-03 | `src/pea_met_network/cross_station_impute.py` exists with `impute_cross_station()` function |
| AC-10-04 | Donor data fetched from MSC GeoMet API and cached as Parquet |
| AC-10-05 | Cross-station imputation step inserts between `impute()` and `calculate_fwi()` in pipeline |
| AC-10-06 | RH imputation uses vapor pressure continuity for internal donors |
| AC-10-07 | RH imputation prefers dew point derivation over integer RH field for ECCC donors |
| AC-10-08 | Wind speed imputation applies height correction (power law or empirical) |
| AC-10-09 | Temperature imputation applies asymmetric outlier cap (±2°C warm, ±3°C cool) |
| AC-10-10 | Stanley Bridge FWI coverage > 0% (currently 0%) |
| AC-10-11 | Tracadie FWI coverage > 0% (currently 0%) |
| AC-10-12 | Greenwich FWI coverage improves from 52.6% baseline |
| AC-10-13 | North Rustico FFMC/ISI coverage improves from 84.1% baseline |
| AC-10-14 | Observed values are never overwritten by synthesized values |
| AC-10-15 | Every imputed variable has `{var}_qf`, `{var}_src`, `{var}_method` audit columns |
| AC-10-16 | Quality flag values are restricted to {0, 1, 2, 9} |
| AC-10-17 | Source format is standardized: `INTERNAL:{name}` or `ECCC:{climate_id}` |
| AC-10-18 | FWI output columns have `fwi_qf` propagated from input quality flags |
| AC-10-19 | `data/processed/cross_station_impute_report.csv` generated by pipeline |
| AC-10-20 | Cross-station report registered in pipeline manifest |
| AC-10-21 | QA/QC report includes imputation breakdown columns |
| AC-10-22 | `docs/cleaning-config.json` contains `cross_station_impute` section |
| AC-10-23 | Donor gap > max_gap_hours triggers fallback to next priority donor |
| AC-10-24 | Stanley Bridge and Tracadie are never used as donors (guardrail enforced) |
| AC-10-25 | Synthesized RH is capped at 100.0% |
| AC-10-26 | `pytest tests/ -m 'not e2e' -v` passes (fast suite under 90s) |

---

## Exit Gate

```bash
pytest tests/ -m 'not e2e' -v
```

All tests must pass. The fast suite must complete in under 90 seconds (increased from 60s due to additional cross-station mock tests).

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| MSC GeoMet API rate limiting | Low | Medium | 1s delay between requests; Parquet cache means fetch is one-time |
| Charlottetown A data ends Jan 2024 | Certain | Low | Only affects P2/P3 slots; primary donors have current data |
| PEINP anemometer heights unknown | Medium | Medium | Empirical derivation from overlapping good data (≥1 week); if no overlap, flag as uncertain |
| Vapor pressure continuity breaks in extreme weather | Low | Low | RH capped at 100%; chain-break rule prevents long-gap stitching |
| Marine inversion bias at Tracadie Wharf | Medium | Medium | Asymmetric temp cap; wind from North Rustico (also coastal) |
| Wind height correction introduces systematic bias | Medium | Medium | Height metadata in config; empirical fallback; audit trail exposes correction method |
| Pipeline runtime increase from pre-loading | Medium | Low | Internal stations already loaded; only 3 external stations to cache |
| API schema changes | Low | Medium | Pin field names in normalize function; tests with mock data |
| Charlottetown A end-of-data affects long temp/wind gaps | Certain | Low | P2/P3 role only; primary donors (Cavendish, St. Peters) have current data |

---

## What This Phase Does NOT Do

- **Does not** impute precipitation from other stations (spatially discontinuous — convective showers; imputed rain could artificially lower FFMC and understate fire risk)
- **Does not** impute solar radiation (not available in the API; Stanhope already uses standard FWI no-solar equations)
- **Does not** change the existing `impute()` function or its 6-hour gap limit (cross-station is a separate step)
- **Does not** modify FWI calculation formulas
- **Does not** add real-time forecasting capability
- **Does not** remove existing tests or change Phase 1–9 acceptance criteria
- **Does not** add spatial interpolation or kriging (physically-grounded transfer is simpler and more defensible)
- **Does not** add gridded/reanalysis data sources (e.g., MSC GEOMET solar products — different data source, future phase)

---

## Expected Impact

### Per-Station Coverage Improvement

| Station | Current FWI Missing | Expected After Phase 10 | Primary Improvement |
|---|---|---|---|
| Stanley Bridge | 100% | ~5–15% | RH from Cavendish (VP continuity). Remaining gaps from donor RH+T+wind gaps. |
| Tracadie | 100% | ~5–15% | RH from Cavendish/North Rustico. Same constraints. Wind gap recovery. |
| Greenwich | 47.4% | ~25–35% | RH from St. Peters (Td-derived). Wind from St. Peters (height-corrected). |
| North Rustico | 15.9% | ~10–15% | Wind gap fill from Stanhope/Charlottetown A. Minor RH gap fill. |
| Cavendish | 17.8% | ~12–17% | Wind gap fill from North Rustico/Stanhope. Minor temp gap fill. |
| Stanhope | 0.4% | ~0.4% | Already near-complete. No donor needed. |

### Combined with Phase 9

Phase 10 cross-station imputation runs **before** Phase 9 chain recovery. Filled data eliminates some chain breaks entirely, reducing the number of restarts Phase 9 needs.

**Greenwich projection:** Phase 9 alone → ~65–70% FFMC. Phase 10 + Phase 9 → potentially ~70–80% FFMC (Phase 10 fills RH and wind gaps, Phase 9 handles remaining chain breaks with restart logic).

### Network-Level Impact

| Metric | Current | After Phase 10 |
|---|---|---|
| Stations with 0% FWI | 2 (Stanley Bridge, Tracadie) | 0 |
| Stations with >90% FWI | 1 (Stanhope) | 1 |
| Stations with >80% FWI | 1 (Stanhope) | 2–3 |
| Network-average FWI coverage | ~38% | ~60–65% |

**The leap from 0% to ~85%+ usable records at Stanley Bridge and Tracadie turns "dead" stations into research assets** — with appropriate asterisks via the quality flag system.

---

## Implementation Order (for Ralph)

1. **`vapor_pressure.py`** — pure math, no dependencies, test immediately. Include `rh_from_dew_point()`.
2. **`eccc_api.py`** — API client with caching, test with mocks. Include anemometer height metadata.
3. **`fetch_eccc_donors.py`** — standalone script, run to populate cache.
4. **`cross_station_impute.py`** — core imputation logic:
   - Start with RH only (VP continuity + Td derivation)
   - Add wind speed (spatial proxy + height correction)
   - Add temperature (spatial proxy + asymmetric cap)
   - Add audit trail columns (qf, src, method triplets)
   - Add FWI quality flag propagation
5. **Pipeline integration** — modify `run_pipeline()` in `cleaning.py`:
   - Add pre-loading of internal station hourly data
   - Insert cross-station step between impute() and calculate_fwi()
   - Add FWI quality flag propagation after calculate_fwi()
6. **Config update** — add `cross_station_impute` section to `cleaning-config.json`
7. **QA/QC report update** — add imputation breakdown columns
8. **Full pipeline run** — validate all stations produce expected output with correct audit trails
9. **Height correction validation** — verify wind scaling against overlapping data (or flag as empirical if heights unknown)

---

## Appendix A: Physical Constants & Formulas Reference

### August-Roche-Magnus (Saturation Vapor Pressure)
```
es(T) = 0.61094 × exp(17.625 × T / (T + 243.04))   [kPa, T in °C]
```
Valid for -40°C ≤ T ≤ 60°C.

### Actual Vapor Pressure from RH
```
e = (RH / 100) × es(T)   [kPa]
```

### RH from Vapor Pressure
```
RH = 100 × e / es(T)   [%]
```

### RH from Dew Point (Inverse Magnus-Tetens)
```
e = 0.61094 × exp(17.625 × Td / (Td + 243.04))
RH = 100 × e / es(T)
```

### Wind Speed Power Law (Height Correction)
```
v(z) = v(z_ref) × (z / z_ref)^α
```
where α ≈ 0.14 for open terrain (WMO standard).

## Appendix B: Donor Distance Reference

| From → To | Approx. Distance | Notes |
|---|---|---|
| Stanley Bridge → Cavendish | ~5 km | Nearest pair. Both north shore. |
| Tracadie → North Rustico | ~15 km | Both north shore. |
| Greenwich → St. Peters | ~15 km | Both east PEI. |
| Greenwich → Stanhope | ~15 km | Cross-island. |
| Greenwich → Harrington CDA | ~25 km | Central PEI. |
| Stanley Bridge → North Rustico | ~20 km | North shore. |
| Stanley Bridge → Charlottetown A | ~30 km | North to central. |
| Tracadie → Cavendish | ~25 km | North shore. |
| Tracadie → Charlottetown A | ~35 km | North to central. |
| Cavendish → North Rustico | ~10 km | North shore. |
| Cavendish → Stanhope | ~20 km | North shore. |
