# PEI National Park FWI

---

## For Stakeholders & General Readers

**Fire Weather Index system for Parks Canada Agency (PEI Field Unit)**

This project provides:
- **Automated wildfire risk assessment** across Prince Edward Island National Park
- **Interactive FWI dashboard** displaying current conditions at weather stations
- **Station redundancy analysis** to identify optimal sensor placement
- **Future FWI forecasts** using Environment and Climate Change Canada (ECCC) GDPS model data

> **Note:** The dashboard displays static data generated from the pipeline. For truly live/real-time data, the pipeline must be deployed to a hosted environment with scheduled runs and API credentials configured.

### Live Dashboard & Reports

🔗 **[Fire Weather Index Dashboard](https://cstewart-hc.github.io/pei-parks-fwi/)** — Interactive map with FWI values and 7-day forecasts (static data, updated on manual pipeline runs)

**Analysis Reports:**
- [Network Analysis (visuals only)](https://cstewart-hc.github.io/pei-parks-fwi/analysis.html) — Exploratory data analysis, redundancy results, FWI validation
- [Network Analysis (with code)](https://cstewart-hc.github.io/pei-parks-fwi/analysis_full.html) — Full analytical notebook including code
- [Redundancy Module](https://cstewart-hc.github.io/pei-parks-fwi/redundancy.html) — PCA-based station overlap analysis source code

### What We Deliver

| Output | Description |
|--------|-------------|
| **FWI Dashboard** | Interactive map showing FWI, DMC, DC, ISI, BUI values at each station (static data) |
| **7-Day Forecasts** | GDPS-driven FWI projections for all park weather stations |
| **Redundancy Report** | PCA biplot showing which stations provide overlapping vs. unique coverage |
| **Cleaned Data** | Quality-controlled hourly and daily weather datasets |
| **Uncertainty Bounds** | Probabilistic risk assessment for station removal decisions (KDE-based confidence intervals) |

### Coverage Area

Weather stations across PEI National Park:
- Cavendish
- Greenwich
- North Rustico
- Stanhope (reference station, ECCC data source)
- Stanley Bridge
- Tracadie

---

## For Technical Users

### Overview

This project implements an end-to-end OSEMN (Obtain, Scrub, Explore, Model, iNterpret) pipeline for Fire Weather Index calculation and weather-station redundancy analysis. It processes raw station data, validates against Environment and Climate Change Canada standards, and provides both interactive dashboards and programmatic access to results.

**Key components:**
1. **Data cleaning pipeline** (`src/pea_met_network/cleaning.py`) — normalization, resampling, imputation
2. **FWI calculation engine** — standard Canadian FWI chain (FFMC → DMC → DC → ISI → BUI → FWI)
3. **Redundancy analysis** (`src/pea_met_network/redundancy.py`) — PCA-based station overlap detection
4. **Forecast pipeline** — GDPS model ingestion with FWI chain propagation
5. **Interactive dashboard** — Leaflet.js visualization with static data (can be made live with scheduled deployment)
6. **Analysis notebook** (`analysis.ipynb`) — full EDA, validation, and uncertainty quantification

### OSEMN Framework

| Stage | Implementation |
|-------|----------------|
| **Obtain** | Raw station CSVs inventoried and schema-audited; GDPS model data fetched via ECCC API |
| **Scrub** | Ingestion, timestamp normalization, hourly/daily resampling, missing-value imputation |
| **Explore** | EDA notebooks, QA/QC summaries, correlation analysis |
| **Model** | Stanhope reference data ingestion, FWI chain execution, PCA redundancy analysis |
| **iNterpret** | Probabilistic uncertainty quantification, station consolidation recommendations |

### Installation

```bash
# Clone repository
git clone https://github.com/Cstewart-HC/pei-parks-fwi.git
cd pei-parks-fwi

# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # or `.venv\Scripts\activate` on Windows

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

Or use the Makefile shortcut:

```bash
make install
```

### Running the Pipeline

#### Data Cleaning Pipeline

The main entry point is `pea_met_network.cleaning`. It processes raw station CSVs from `data/raw/`, normalizes timestamps, resamples to hourly/daily frequencies, applies imputation, and writes cleaned datasets to `data/processed/`.

```bash
python -m pea_met_network
python -m pea_met_network --stations all
python -m pea_met_network --stations cavendish,greenwich --force
python -m pea_met_network --fwi-mode compliant
python -m pea_met_network --dry-run
```

Options: `--stations` (comma-separated or `all`), `--force` (reprocess), `--dry-run` (report only), `--fwi-mode` (hourly|compliant|extended), `--no-fetch` (skip downloads).

Missing raw data directories trigger clear error messages.

#### Analysis Notebook

`analysis.ipynb` contains the full analytical narrative:
- Exploratory data analysis
- Redundancy analysis (PCA biplots, clustering)
- FWI logic validation
- Uncertainty quantification

```bash
jupyter lab analysis.ipynb
```

#### Running Tests

```bash
make lint    # Ruff linting
make test    # pytest test suite
make check   # Type checking + linting + tests
```

### Key Technical Outputs

| Output | Location | Description |
|--------|----------|-------------|
| Cleaned hourly data | `data/processed/` | Quality-controlled hourly and daily resampled data |
| FWI values | `data/processed/` | Full FWI chain (FFMC → DMC → DC → ISI → BUI → FWI) |
| FWI forecasts | `data/forecasts/*_fwi_forecast.csv` | 7-day GDPS-driven projections |
| GDPS cache | `data/gdps_cache/` | Raw model data (YYYYMMDDTHH.json format) |
| Redundancy results | `analysis.ipynb` | PCA biplot, clustering dendrograms |
| Dashboard HTML | `dashboard/` | Standalone Leaflet.js application |

### Repository Structure

```text
pei-parks-fwi/
├── .github/workflows/        # CI/CD (GitHub Actions, dashboard deploy)
├── analysis.ipynb            # Analytical narrative notebook
├── dashboard/                # FWI geospatial dashboard
│   ├── index.html            # Main dashboard page
│   ├── analysis.html         # Notebook HTML (outputs only)
│   ├── analysis_full.html    # Notebook HTML (with code)
│   ├── redundancy.html       # Redundancy module source
│   ├── css/                  # Dashboard styles
│   ├── js/                   # Dashboard JavaScript
│   └── data/                 # Static data for dashboard
├── data/
│   ├── raw/                  # Raw station data (CSV, JSON, XLSX, XLE)
│   ├── processed/            # Pipeline output (gitignored)
│   ├── forecasts/            # FWI forecast CSVs + startup_state.json
│   └── gdps_cache/           # Cached GDPS model data
├── docs/
│   ├── cleaning-config.json  # Pipeline configuration
│   ├── data-sources.md       # Data source documentation
│   ├── fwi-forecast-plan.md  # Forecast pipeline design
│   ├── licor-ingestion-plan.md  # LICOR sensor ingestion design
│   ├── working-agreement.md  # Project working agreement
│   ├── pipeline/             # Architecture documentation
│   └── specs/                # Phase specifications (01-16)
├��─ notebooks/                # Historical notebooks
├── scripts/                  # Utility and build scripts
├── src/
│   └── pea_met_network/      # Pipeline source code
│       ├── cleaning.py       # Main cleaning pipeline
│       ├── fwi.py            # FWI chain (vendored cffdrs_py wrapper)
│       ├── fwi_forecast.py   # GDPS forecast pipeline
│       ├── gdps_fetcher.py   # ECCC GDPS data fetcher
│       ├── stanhope_cache.py # Stanhope ECCC data ingestion
│       ├── redundancy.py     # PCA redundancy analysis
│       ├── uncertainty.py    # Probabilistic uncertainty quantification
│       ├── qa_qc.py          # QA/QC checks
│       ├── imputation.py     # Missing value imputation
│       ├── cross_station_impute.py  # Cross-station imputation
│       ├── validation.py     # Data validation
│       ├── vapor_pressure.py # Vapor pressure calculations
│       └── vendor/cffdrs/    # Vendored cffdrs_py (Van Wagner FWI)
├── tests/                    # Test suite
├── AGENTS.md                 # Agent workspace rules
├── Makefile
├── README.md
├── pyproject.toml            # Package metadata (source of truth for versions)
├── requirements.txt          # Pinned runtime dependencies
└── requirements-dev.txt      # Pinned dev dependencies (pytest, ruff)
```

### Environment Variables

No required environment variables for basic pipeline operation. Forecast pipeline may use optional ECCC API credentials (configured via `data/forecasts/startup_state.json`).

### Deployment for Real-Time Updates

The current dashboard displays **static data**. To enable live/real-time updates, you would need:

| Component | Requirement |
|-----------|-------------|
| **Hosted environment** | Cloud server (e.g., AWS, GCP, Azure) or on-premise machine |
| **Scheduler** | Cron job or GitHub Actions workflow to run pipeline hourly/daily |
| **API credentials** | ECCC GDPS API key (or alternative weather data provider) |
| **Data publishing** | Workflow to push updated CSVs to `dashboard/data/` and trigger GitHub Pages deploy |
| **Storage** | Persistent storage for forecast cache (`data/gdps_cache/`) |

Example: A GitHub Actions workflow scheduled every 6 hours could:
1. Fetch latest GDPS data from ECCC API
2. Run FWI pipeline
3. Update `dashboard/data/` files
4. Commit and push to `main` branch
5. Trigger automatic Pages deploy

### Dependencies

`pyproject.toml` is the source of truth for versions. `requirements.txt` and `requirements-dev.txt` provide minimum pins.

Core:
- `pandas`, `numpy` — Data manipulation
- `scipy`, `scikit-learn` — Statistical analysis, PCA, clustering
- `matplotlib`, `seaborn` — Visualization
- `requests`, `httpx` — HTTP clients for ECCC data
- `openpyxl`, `defusedxml` — Excel/XML format support
- `arrow`, `PyYAML`, `jsonschema` — Utilities

Development:
- `pytest` — Testing (e2e, slow, integration markers defined)
- `ruff` — Linting and formatting

Note: `requirements.txt` contains minimum version pins; `pyproject.toml` defines the full dependency set including `jupyter`, `nbconvert`, `prometheus_client`, `psutil`, `lark`, and `Jinja2`.

---

## Assignment Context

**DATA-3210: Advanced Concepts in Data — Semester Project**

Client: Parks Canada Agency (PEI Field Unit)

Required themes:
- Python-based data pipeline and QA/QC
- Station redundancy analysis using PCA and/or clustering
- FWI calculation and validation
- Probabilistic uncertainty quantification

---

**License:** See project repository for license information.
**Contact:** For dashboard issues or questions, contact Parks Canada PEI Field Unit.
