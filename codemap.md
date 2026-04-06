# Repository Atlas: pea-met-network

> **PEI National Park Fire Weather Index Pipeline**
> DATA-3210 PEI meteorological network optimization project
> Python 3.11+ | 29 source files | ~6,300 lines | 394 tests

---

## Project Overview

A multi-stage ETL pipeline that ingests raw meteorological data from 6 PEI National Park weather stations (PEINP sensors + ECCC reference), performs quality enforcement, gap imputation (including cross-station spatial transfer), and computes the Canadian Fire Weather Index (FWI) system on hourly and daily time scales.

## Build & Development

| Target | Command |
|--------|---------|
| Install | `make install` |
| Test | `make test` (or `pytest`) |
| Lint | `make lint` (or `ruff check .`) |
| Full check | `make check` (lint + test) |
| Run pipeline | `python -m pea_met_network` |

**Key dependencies:** pandas ≥3.0, numpy ≥2.4, scipy, scikit-learn, openpyxl, requests

## Root Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | Project metadata, dependencies, ruff config, pytest config |
| `Makefile` | Build shortcuts (install, test, lint, check) |
| `requirements.txt` | Runtime dependencies |
| `requirements-dev.txt` | Dev/test dependencies |
| `cleaning.py` | Legacy standalone pipeline entry (superseded by `src/` package) |

## Directory Map

| Directory | Description | Codemap |
|-----------|-------------|---------|
| `src/pea_met_network/` | Core pipeline, FWI computation, quality, imputation, analysis | [codemap](codemap-src_pea_met_network.md) |
| `src/pea_met_network/adapters/` | File-format adapters (CSV, XLSX, XLE, JSON) | [codemap](codemap-src_pea_met_network_adapters.md) |
| `tests/` | 394 tests (unit + integration) | — |
| `docs/` | Configuration (`cleaning-config.json`), reports, figures | — |
| `data/` | Raw and processed station data | — |

## High-Level Pipeline Flow

```
Raw Files (CSV/XLSX/XLE/JSON)
         │
    ┌────▼────┐
    │  Adapters│  ← Strategy pattern, 4 formats
    └────┬────┘
         │  Canonical DataFrame
    ┌────▼────┐
    │  Dedup  │
    └────┬────┘
         │
    ┌────▼────┐
    │ Resample│  ← Hourly (mean/sum policy)
    └────┬────┘
         │
    ┌────▼────┐
    │ Quality │  ← Range, rate-of-change, cross-variable, flatline
    └────┬────┘
         │
    ┌────▼────┐
    │  Impute │  ← Linear interpolation (short gaps)
    └────┬────┘
         │
    ┌────▼──────────┐
    │ Cross-Station │  ← Donor cascade P1→P2→P3 (RH, Wind, Temp)
    │   Impute      │
    └────┬──────────┘
         │
    ┌────▼────┐
    │   FWI   │  ← FFMC→DMC→DC→ISI→BUI→FWI (Van Wagner 1987)
    └────┬────┘
         │
    ┌────▼────┐
    │ Aggregate│  ← Hourly → Daily summaries
    └────┬────┘
         │
    ┌────▼────┐
    │ QA/QC   │  ← Missingness, duplicates, OOR, chain breaks
    │  Report │
    └────┬────┘
         │
    Station Hourly CSV
    Station Daily CSV
    Imputation Report
    Cross-Station Audit
    QA/QC Report
```

## Key Architectural Decisions

1. **Adapter Strategy** — `registry.route_by_extension()` selects the correct adapter polymorphically; new formats need only a new adapter class + registry entry.
2. **Topological Station Ordering** — Kahn's algorithm ensures internal donor stations are processed before target stations that depend on them.
3. **Two-Pass Pipeline** — First pass processes all stations and stages donor data to Parquet; second pass performs cross-station imputation using the staged data.
4. **Audit Trail** — Every imputation (both generic and cross-station) produces structured audit records with quality flags, source station, and method.
5. **Quality Enforcement as Pipeline Stage** — Quality checks are pluggable rules (`_check_*` functions) with configurable actions (set NaN, flag only), producing structured action records consumed by diagnostics.
6. **FWI Chain-Break Diagnostics** — Post-hoc detection of FWI continuity breaks with root cause attribution: startup detection (DMC/DC before first 14:00 LST), cascade analysis (FFMC→DMC→DC NaN propagation), and quality enforcement correlation.
7. **Analysis Notebook** — Phase 15 delivered `analysis.ipynb` with automated pipeline validation and visualization.
