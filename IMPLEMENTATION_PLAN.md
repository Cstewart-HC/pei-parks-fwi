# Implementation Plan

## Phase Roadmap

| Phase | Name | Status |
|---|---|---|
| 1 | Obtain | ✅ done |
| 2 | Scrub | ✅ done |
| 3 | Explore | ⬚ not started |
| 4 | Model: Reference + FWI | 🔄 in progress |
| 5 | Model: Redundancy | ⬚ not started |
| 6 | Interpret | ⬚ not started |

## Current Phase: 4 — Model: Reference + FWI

### Task Queue

Each task has a **gate** — a command the loop must run to verify completion.
The loop may NOT mark a task done unless its gate exits 0.

#### Active (next up)

- [ ] normalize cached Stanhope hourly data into project reference schema
  gate: python -c "from pea_met_network.stanhope_cache import normalize_stanhope_hourly; print('ok')"
  depends: [stanhope-cache-scaffold, stanhope-range-materialization]

#### Completed

- [x] add Stanhope hourly cache fetch scaffolding with local reuse
  gate: test_stanhope_cache.py::test_fetch_reuses_cache
- [x] record Stanhope download provenance for cached files
  gate: test_stanhope_cache.py::test_provenance_written
- [x] encode anti-429 behavior with coarse monthly fetches and delay hooks
  gate: test_stanhope_cache.py::test_429_stops_cleanly
- [x] script bounded multi-month or multi-year Stanhope cache materialization
  gate: test_stanhope_cache.py passes
- [x] produce first cleaned hourly and daily datasets
  gate: test_materialize_resampled.py passes
- [x] prove real-file normalization-to-resampling path on canonical CSV
  gate: test_real_resampling_pipeline.py passes
- [x] add bounded canonical CSV normalization loader
  gate: test_normalized_loader.py passes
- [x] expose first-class hourly and daily resampling helpers
  gate: test_resampling_policy.py passes

#### Queued (not yet started)

- [ ] define FWI-ready cleaned daily contract
  gate: test existence of specs/fwi-daily-contract.md
  depends: [normalize-stanhope]
- [ ] implement FFMC moisture code
  gate: pytest tests/test_fwi.py::test_ffmc
  depends: [fwi-daily-contract]
- [ ] implement DMC moisture code
  gate: pytest tests/test_fwi.py::test_dmc
  depends: [fwi-daily-contract]
- [ ] implement DC moisture code
  gate: pytest tests/test_fwi.py::test_dc
  depends: [fwi-daily-contract]
- [ ] implement full FWI chain
  gate: pytest tests/test_fwi.py
  depends: [ffmc, dmc, dc]
- [ ] validate FWI against external reference values
  gate: pytest tests/test_fwi.py::test_external_reference
  depends: [fwi-full-chain]

## Carried-Forward Decisions

- use OSEMN as project framing, not as a software framework
- bias toward assignment compliance with sane internal structure
- use both PCA and clustering for redundancy analysis
- implement full FWI chain if data supports it
- treat local cached data as canonical
- enforce hard line length 80, style target 50
- enforce McCabe hard cap 15, target less than 10
- native Python FWI implementation remains the target approach

## Blockers

None currently.
