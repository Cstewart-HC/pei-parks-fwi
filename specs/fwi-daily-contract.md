# FWI-Ready Daily Contract

## Scope
This contract defines the cleaned daily dataset required as input to
Canadian Fire Weather Index calculations.

It refines, but does not replace, the general processed daily contract
in `specs/processed-data-contract.md`.

## Purpose
FWI calculations need a stable daily input table with explicit units,
well-defined daily semantics, and station-level reproducibility.

The contract applies to FWI-ready daily records for at least:
- Cavendish
- Greenwich
- Stanhope reference data when normalized into the same schema

## Grain
One row per:
- `station`
- `timestamp_utc`

`timestamp_utc` is the left-labeled UTC day bucket representing the
start of the aggregation day.

Duplicate `station` + `timestamp_utc` keys are invalid.

## Required identifier columns
Every FWI-ready daily dataset must include, in this order:
1. `station`
2. `timestamp_utc`

## Required meteorological columns
The dataset must include these columns with these meanings:
- `air_temperature_c`: daily mean air temperature in degrees Celsius
- `relative_humidity_pct`: daily mean relative humidity in percent
- `wind_speed_kmh`: daily mean wind speed in kilometres per hour
- `rain_mm`: daily total precipitation in millimetres

These four variables are the minimum contract for moisture code
calculation inputs.

## Optional supporting columns
The dataset may also include:
- `wind_speed_ms`
- `dew_point_c`
- `wind_direction_deg`
- `wind_gust_speed_kmh`
- `wind_gust_speed_max_kmh`
- `solar_radiation_w_m2`
- `battery_v`

Optional columns must preserve their existing processed-data meanings
and units.

## Daily aggregation semantics
Unless a later task defines station-specific overrides, FWI-ready daily
inputs inherit the processed aggregation policy:
- mean: `air_temperature_c`, `relative_humidity_pct`,
  `dew_point_c`, `wind_speed_kmh`, `wind_speed_ms`,
  `wind_gust_speed_kmh`, `solar_radiation_w_m2`, `battery_v`
- sum: `rain_mm`
- max: `wind_gust_speed_max_kmh`
- first: `wind_direction_deg`

These are cleaned daily inputs. They are not yet noon-observation FWI
oracles, and any deviation from operational ECCC daily conventions must
be documented during validation.

## Units and normalization rules
- `air_temperature_c` must be Celsius
- `relative_humidity_pct` must be percent on a 0 to 100 scale
- `wind_speed_kmh` must be kilometres per hour
- `rain_mm` must be millimetres over the UTC day bucket
- `timestamp_utc` must remain timezone-aware UTC
- `station` must use canonical lowercase station keys

No implicit unit conversion is allowed at FWI runtime.
Any required conversion must happen before data satisfies this
contract.

## Missingness rules
- Missing required identifier fields are invalid
- Missing required meteorological values are allowed only as explicit
  missing values
- FWI code computation must not silently invent required inputs
- Any future imputation used for FWI inputs must remain auditable under
  the Phase 2 imputation policy

## Validation expectations
A valid FWI-ready daily dataset must satisfy all of the following:
- required columns exist in the expected order prefix
- no duplicate `station` + `timestamp_utc` keys
- `timestamp_utc` is timezone-aware UTC
- required meteorological columns use the contract names and units
- rows are reproducible from normalized station inputs using the stated
  aggregation rules

## Known limitation
This contract defines the cleaned daily input interface for project FWI
work. It does not claim that UTC daily aggregates are operationally
identical to official noon-local FWI products.

That gap must be handled in reference validation artifacts rather than
hidden in the schema.
