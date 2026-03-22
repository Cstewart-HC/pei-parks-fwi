# Implementation Plan

## Current Milestone
Phase 4 — Model: Reference + FWI

## Current Objective
Implement cached Stanhope reference ingestion with provenance and anti-429
behavior as the first bounded step toward reference benchmarking and FWI
validation.

## Immediate Next Tasks
- [x] add Stanhope hourly cache fetch scaffolding with local reuse
- [x] record Stanhope download provenance for cached files
- [x] encode anti-429 behavior with coarse monthly fetches and delay hooks
- [x] script bounded multi-month or multi-year Stanhope cache materialization
- [ ] normalize cached Stanhope hourly data into project reference schema

## Queued Tasks
- [ ] implement imputation audit framework
- [ ] encode conservative missing-data handling rules
- [x] produce first cleaned hourly and daily datasets
- [x] prove real-file normalization-to-resampling path on canonical CSV
- [x] add bounded canonical CSV normalization loader
- [x] expose first-class hourly and daily resampling helpers
- [ ] script bounded multi-month or multi-year Stanhope cache materialization
- [ ] normalize cached Stanhope hourly data into project reference schema
- [ ] define FWI-ready cleaned daily contract
- [x] validate repo-wide lint and tests before milestone commit

## Validation Expectations
For current scope:
- repeated Stanhope requests should reuse local cache when present
- coarse monthly fetches and explicit delay hooks should minimize API load
- provenance records should be inspectable for each downloaded cache file
- rate-limit failures should stop cleanly without partial cache writes

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
- manual and scheduled loop runs must be observable by default
- native Python FWI implementation remains the target approach

## Notes to Future Loops
The ingestion groundwork is now real: audit, manifest loading, schema
recognition, and cross-family timestamp normalization exist. Build forward
from that baseline. Do not reopen settled foundation work unless tests or
artifacts prove a real defect.

## Pre-autonomy checkpoint
Completed and verified:
- initial data inventory and schema audit
- planning stack and quality rails
- raw manifest loader and schema recognition
- timestamp normalization for primary schema family
- timestamp normalization across remaining schema families

The next sprint should begin with resampling, not more planning.

## Sprint Update (2026-03-22 08:09:54 UTC)
- Milestone in progress: Phase 4 Stanhope reference ingestion bootstrap.
- Verified target status: no existing Stanhope ingestion/cache module or tests were present; only spec/docs and legacy source notes existed.
- Current loop result: added bounded monthly-range Stanhope cache materialization on top of the hourly cache fetch helper, with year-boundary coverage and focused tests.
