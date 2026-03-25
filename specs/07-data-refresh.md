# Phase 12: Data Refresh & Extended Coverage

## Context

The original analysis (Phases 1-11) used PEINP station data from Oct 2022 to
Oct 2025. During post-completion data auditing, we discovered:

1. **Licor Cloud API** provides direct access to all 5 PEINP stations via
   HOBOlink RX3000 data loggers (device serials documented in
   `data/raw/licor/devices.json`, excluded from git).
2. **Excel gap-fill files** in the original data delivery contain earlier
   data that was not ingested by the original pipeline:
   - North Rustico Dec 2022 – Mar 2023 (3 sensors: temp, baro, solar)
   - Multi-station file covering Apr–Sep 2023 for all 5 stations
   - Greenwich Oct 2022 – Mar 2023
3. **New Licor CSVs** fill Nov–Dec 2025 gaps for all stations.
4. **API retention is 12 months** — older data must come from file-based
   sources.

This phase re-ingests the expanded dataset and reruns the pipeline and
analysis with the improved coverage.

## Updated Coverage Summary

| Station | Original Months | New Months | Added |
|---|---|---|---|
| Cavendish | 39 (Oct 2022 – Dec 2025) | 39 | 0 (Licor 2025 only fills existing) |
| Greenwich | 39 (Oct 2022 – Dec 2025) | 39 | 0 |
| North Rustico | 33 (Apr 2023 – Dec 2025) | 37 (Dec 2022 – Dec 2025) | +4 months |
| Stanley Bridge | 29 (Jul 2023 – Dec 2025) | 29 | 0 |
| Tracadie | 31 (Jun 2023 – Dec 2025) | 31 | 0 |
| Stanhope | 48 (Jan 2022 – Dec 2025) | 48 | 0 |

**Longest continuous all-PEINP coverage:** Jul 2023 – Dec 2025 (30 months).

## Goal

Re-run the full pipeline and update all analysis artifacts to reflect the
expanded dataset, particularly the North Rustico early-coverage extension.

## Acceptance Criteria

| ID | Criterion |
|---|---|
| AC-REF-1 | `manifest.py` discovers all raw CSVs including Licor-sourced files and XLSX-converted files |
| AC-REF-2 | `normalized_loader.py` handles the 3-sensor North Rustico CSVs (temp, baro, solar only) without errors — missing columns treated as absent, not broken |
| AC-REF-3 | Pipeline runs end-to-end on the expanded dataset without errors |
| AC-REF-4 | Processed hourly and daily datasets exist in `data/processed/` for all stations including the new North Rustico Dec 2022 – Mar 2023 period |
| AC-REF-5 | Imputation report reflects the updated gap profile |
| AC-REF-6 | FWI computed for all stations with sufficient data coverage |
| AC-REF-7 | Analysis notebook (EDA, FWI, PCA, redundancy, uncertainty, conclusion) updated with expanded time range and findings |
| AC-REF-8 | Analysis notebook cells execute top-to-bottom without errors |
| AC-REF-9 | `docs/data-sources.md` documents all data sources (ECCC, HOBOlink/Licor, XLSX) with provenance |
| AC-REF-10 | Full test suite passes with zero failures |

## Exit Gate

```bash
.venv/bin/pytest tests/test_data_refresh.py -q
```

## Constraints

- Do NOT modify library code to pass tests — fix root causes.
- North Rustico Dec 2022 – Mar 2023 has only 3 sensors (temperature,
  barometric pressure, solar radiation). FWI cannot be computed for this
  period. This is a data limitation, not a pipeline bug.
- The Licor CSV column schema differs from the PEINP CSV schema. The
  normalized loader must handle both without breaking existing data.
- `data/processed/` is gitignored. Tests verify file existence and shape,
  not content checksums.
- Do not modify existing passing tests unless they test behavior that has
  legitimately changed.
- The analysis time range should be updated to reflect the new coverage
  boundaries (not extended to 2026 — that is out of scope).
