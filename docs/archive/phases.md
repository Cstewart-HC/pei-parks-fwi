# Phase Plan

## Phase 1 — Obtain
- inventory raw data files
- identify station coverage and schemas
- document file types, date ranges, and station mappings
- define canonical raw data layout

Exit criteria:
- inventory artifact exists
- station/file mapping is known
- major schema inconsistencies are documented

## Phase 2 — Scrub
- implement ingestion pipeline
- normalize columns and timestamps
- produce hourly and daily cleaned outputs
- implement QA/QC summaries
- implement imputation audit framework

Exit criteria:
- cleaned datasets are reproducible
- QA/QC artifacts exist
- imputation policy is encoded and traceable

## Phase 3 — Explore
- build analysis notebook scaffolding
- inspect missingness, ranges, and correlations
- produce early visuals and station comparisons

Exit criteria:
- notebook runs on cleaned data
- exploratory summaries exist

## Phase 4 — Model: Reference + FWI
- ingest and cache Stanhope reference data
- validate overlap windows
- implement moisture codes
- extend to full FWI chain if supported
- validate against external reference values

Exit criteria:
- Stanhope cache exists
- moisture codes are reproducible
- validation artifacts exist

## Phase 5 — Model: Redundancy
- implement PCA view
- implement clustering view
- benchmark stations against Stanhope
- prepare recommendation logic

Exit criteria:
- redundancy evidence exists from multiple methods
- recommendation logic is documented

## Phase 6 — Interpret
- implement uncertainty quantification
- combine evidence into recommendation-ready outputs
- strengthen README and assignment-facing entrypoints
- prepare notebook/report scaffolding

Exit criteria:
- uncertainty outputs exist
- recommendation is supportable
- repo is runnable by a fresh user
