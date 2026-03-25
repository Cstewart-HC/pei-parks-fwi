# Phase 7: Licor JSON Adapter + Continuous Coverage

## Context

The Licor Cloud API provides JSON data for all 5 PEINP stations via
`data/raw/licor/<device>/`. This data covers Nov 2025 → present,
filling the gap after PEINP CSV coverage ends (~Sep 2025).

Phase 1 created the adapter architecture. Phase 7 completes the JSON
adapter and verifies continuous temporal coverage across all formats.

`scripts/licor_to_csv.py` already exists as a standalone converter.
This phase integrates its logic into the adapter framework and verifies
the PEINP→Licor boundary produces no gaps or duplicates.

## Goal

Licor JSON data flows through the same adapter pipeline as all other
formats. All 5 PEINP stations have continuous processed data from their
earliest PEINP record through the latest Licor record.

## Scope

1. Build `src/pea_met_network/adapters/json_adapter.py`:
   - Reads raw Licor JSON files directly (not pre-converted CSVs)
   - Uses `data/raw/licor/devices.json` for serial→station mapping
   - Outputs canonical schema DataFrame
   - Reuses column mapping logic from `scripts/licor_to_csv.py`

2. Licor sensor mapping to canonical columns:
   - Temperature → `air_temperature_c`
   - RH → `relative_humidity_pct`
   - Dew Point → `dew_point_c`
   - Wind Speed → `wind_speed_kmh`
   - Gust Speed → `wind_gust_speed_kmh`
   - Wind Direction → `wind_direction_deg`
   - Rain → `rain_mm`
   - Solar Radiation → `solar_radiation_w_m2`
   - Barometric Pressure → `barometric_pressure_kpa`
   - Water Level → `water_level_m`
   - Water Temperature → `water_temperature_c`

3. Licor data has ~2-5 min resolution — resampled to hourly by the
   standard `resampling.py` (no special handling needed).

4. PEINP→Licor boundary handling:
   - PEINP CSVs end ~Sep 2025, Licor JSONs start Nov 2025
   - Pipeline concats both; dedup handles any overlap
   - Verify no temporal gap > 48h at the boundary

5. Update `manifest.py` to discover Licor JSON files alongside other formats.

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-LIC-1 | JSON adapter produces canonical schema DataFrame for all 5 stations |
| AC-LIC-2 | All 5 stations have continuous processed data from earliest PEINP record through latest Licor record |
| AC-LIC-3 | No duplicate timestamps at the PEINP→Licor boundary |
| AC-LIC-4 | No temporal gap > 48h at any station's PEINP→Licor boundary |
| AC-LIC-5 | Pipeline processes all Licor files without errors |
| AC-LIC-6 | Full test suite passes: `.venv/bin/pytest tests/ -q` |

## Exit Gate

```bash
.venv/bin/pytest tests/test_pipeline_execution.py::TestAC_PIPE_7_LicorIntegration -q
```
