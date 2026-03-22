# Status

## Current milestone
Phase 2 — Scrub

## Active target
Hourly and daily resampling on normalized station data

## Verified progress
- explicit normalized aggregation policy exists
- hourly resampling exists and is tested
- daily resampling exists through the shared resampling engine and is tested
- processed data contract draft exists for hourly and daily outputs

## Quality gate
- `ruff check .` passes
- `pytest` passes

## Next target
Connect the resampling engine to real normalized station frames and emit the
first processed hourly and daily datasets.
