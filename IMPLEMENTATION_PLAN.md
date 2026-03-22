# Implementation Plan

## Current Milestone
Phase 1 — Obtain

## Current Objective
Create a trustworthy data inventory and schema audit so later pipeline
work is grounded in the actual files, stations, and date coverage.

## Immediate Next Tasks
- [ ] inspect project data layout and identify canonical raw data paths
- [ ] generate a raw file inventory artifact with station, file type,
      and path information
- [ ] map files to stations and approximate date coverage
- [ ] identify CSV vs Excel-family parsing needs
- [ ] record major schema unknowns and anomalies

## Queued Tasks
- [ ] scaffold Python package under `src/pea_met_network/`
- [ ] add project quality config (`pyproject.toml`) with Ruff rules
- [ ] add initial pytest scaffolding
- [ ] implement inventory loader utilities
- [ ] draft cleaning pipeline entrypoint contract

## Validation Expectations
For current scope:
- inventory scripts should run reproducibly
- generated artifacts should be inspectable
- no cleaning assumptions should be baked in yet

## Blockers
- none currently

## Recent Decisions
- use OSEMN as project framing, not as a software framework
- bias toward assignment compliance with sane internal structure
- use both PCA and clustering for redundancy analysis
- implement full FWI chain if data supports it
- treat local cached data as canonical
- enforce hard line length 80, style target 50
- enforce McCabe hard cap 15, target less than 10
- diary entries should use factual + reflective Option C style

## Notes to Future Loops
Do not jump ahead into modeling until inventory and schema understanding
exist. Early certainty here prevents fake progress later.

## Decision Log — 2026-03-22

- Confirmed `cffdrs` is R-only and removed it from Python dependency assumptions.
- Chosen path: implement FWI natively in `src/pea_met_network/` with small typed functions.
- `gagreene/cffdrs` will be used as an MIT-licensed Python reference and possible test oracle.
- `cffdrs/cffdrs_r` will be used as an authoritative behavioral reference only; do not copy from GPL-2 sources into project code.

## Next FWI-related prerequisite tasks

1. Inspect station file schemas and identify the exact columns needed for daily FWI inputs.
2. Define the cleaned daily dataset contract required by the FWI module.
3. Add FWI validation fixtures/tests only after the cleaned daily contract exists.
