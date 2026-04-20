# PEI National Park FWI

Fire Weather Index pipeline for Parks Canada Agency (PEI Field Unit).

This project determines weather-station redundancy across Prince Edward
Island National Park and automates Canadian Fire Weather Index (FWI)
calculation for localized wildfire risk management.

## OSEMN Pipeline Structure

The project follows the **OSEMN** (Obtain, Scrub, Explore, Model, iNterpret)
framework:

1. **Obtain** — raw station CSVs inventoried and schema-audited
2. **Scrub** — ingestion, timestamp normalization, hourly/daily resampling, imputation
3. **Explore** — EDA, QA/QC summaries, exploratory notebooks
4. **Model** — Stanhope reference calibration, FWI chain, PCA redundancy analysis
5. **iNterpret** — probabilistic uncertainty quantification and recommendations

## Setup and Installation

```bash
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Or use:

```bash
make install
```

## Running the Pipeline

### Data Cleaning (`pea_met_network.cleaning`)

`pea_met_network.cleaning` is the end-to-end pipeline entrypoint. It loads raw station
CSVs from `data/raw/`, normalizes timestamps, resamples to hourly and daily
frequencies, applies imputation, and writes cleaned datasets to
`data/processed/`.

```bash
python -m pea_met_network
python -m pea_met_network --output-dir /custom/path
```

No manual steps are required between start and finished output. If raw data
directories are missing, a clear error message is shown.

### Analysis Notebook (`analysis.ipynb`)

`analysis.ipynb` contains the full analytical narrative with sections for
EDA, redundancy analysis, FWI logic, and uncertainty quantification. Each
section includes visualizations and markdown explanations.

To run:

```bash
jupyter lab analysis.ipynb
```

## Key Outputs

- **Cleaned datasets** — hourly and daily resampled data for all PCA stations
- **FWI values** — full FWI chain (FFMC → DMC → DC → ISI → BUI → FWI)
- **Redundancy results** — PCA biplot and clustering analysis of station overlap
- **Uncertainty distributions** — probabilistic quantification of imputation and model uncertainty

## Quality Checks

```bash
make lint
make test
make check
```

## Repository Structure

```text
pei-parks-fwi/
├── .github/workflows/        # CI/CD (dashboard deploy)
├── analysis.ipynb            # Analytical narrative notebook
├── dashboard/                # FWI geospatial dashboard (Phase 16)
├── data/
│   ├── raw/                  # Raw station data (CSV, JSON, XLSX, XLE)
│   └── processed/            # Pipeline output (gitignored)
├── docs/
│   ├── cleaning-config.json  # Pipeline configuration
│   ├── pipeline/             # Architecture documentation
│   └── specs/                # Phase specifications (01-16)
├── notebooks/                # Historical notebooks
├── scripts/                  # Utility and build scripts
├── src/
│   └── pea_met_network/      # Pipeline source code
├── tests/                    # Test suite
├── AGENTS.md                 # Agent workspace rules
├── Makefile
├── README.md
├── pyproject.toml
├── requirements.txt
└── requirements-dev.txt
```

## Assignment Context

**DATA-3210: Advanced Concepts in Data — Semester Project**

Client: Parks Canada Agency (PEI Field Unit)

Required themes:
- Python-based data pipeline and QA/QC
- Station redundancy analysis using PCA and/or clustering
- FWI calculation and validation
- Probabilistic uncertainty quantification
