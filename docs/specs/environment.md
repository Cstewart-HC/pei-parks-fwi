
## Available Libraries & Tools

The project venv (`.venv/`) has 120+ packages installed. Use them. Do NOT
reinvent wheels with raw Python when a library exists. Below is the curated
reference of what's available and how to use it for this project.

### Core Data Stack (ALREADY IN USE — know these cold)

```python
import pandas as pd                    # DataFrames — your primary data structure
import numpy as np                     # Numerical operations, arrays, NaN handling
from scipy import stats                # Statistical tests, distributions
from scipy.stats import gaussian_kde   # Kernel density estimation (used in uncertainty.py)
```

**Pandas patterns — use these, not loops:**
```python
# Column checks
df.columns.tolist()                    # Get column names
"col_name" in df.columns               # Check column exists
df.dtypes                              # Inspect types

# Missing data — DO NOT write manual NaN checks
df.isna().sum()                        # Count NaNs per column
df.dropna(subset=["col"])              # Drop rows with NaN in specific column
df.fillna(method="ffill")              # Forward-fill (use sparingly)
df.interpolate(method="time")          # Time-aware interpolation

# Schema validation — DO NOT write manual schema checkers
expected = {"station", "timestamp_utc", "air_temperature_c"}
assert expected.issubset(set(df.columns)), f"Missing: {expected - set(df.columns)}"
assert len(df.columns) == 19, f"Expected 19 cols, got {len(df.columns)}"

# Resampling — DO NOT write manual groupby-by-hour logic
df.set_index("timestamp_utc").resample("1h").mean()     # Hourly mean
df.set_index("timestamp_utc").resample("1D").agg({...})   # Daily with mixed agg

# Value ranges — DO NOT write manual min/max checks
df.describe()                          # Quick statistical summary
df[(df["col"] < 0) | (df["col"] > 100)]  # Find out-of-range values
```

**NumPy patterns:**
```python
np.nan, np.inf                         # Sentinel values
np.isnan(), np.isfinite()             # Checks
np.where(condition, x, y)             # Vectorized conditional
np.clip(values, min, max)             # Clamp values to range
```

### Machine Learning (ALREADY IN USE — redundancy.py)

```python
from sklearn.cluster import DBSCAN, KMeans           # Clustering stations
from sklearn.decomposition import PCA                 # Dimensionality reduction
from sklearn.preprocessing import StandardScaler       # Feature normalization
from sklearn.metrics import silhouette_score           # Cluster quality
```

**DO NOT** implement your own clustering, PCA, or normalization. Use sklearn.

### Pipeline Modules (importable — use these instead of reimplementing)

```python
# File discovery
from pea_met_network.manifest import build_raw_manifest, recognize_schema

# Data loading — adapters route by file extension automatically
from pea_met_network.adapters import route_by_extension, load_file

# Resampling
from pea_met_network.resampling import resample_hourly, resample_daily, AggregationPolicy

# FWI computation — the full Canadian FWI system (Van Wagner 1987)
from pea_met_network.fwi import (
    fine_fuel_moisture_code,    # FFMC
    duff_moisture_code,         # DMC
    drought_code,               # DC
    initial_spread_index,       # ISI
    buildup_index,              # BUI
    fire_weather_index,         # FWI
)

# Imputation — conservative gap-filling with audit trail
from pea_met_network.imputation import impute_column, impute_frame, audit_trail_to_dataframe

# QA/QC — data quality checks
from pea_met_network.qa_qc import (
    missingness_summary,
    duplicate_timestamps,
    out_of_range_values,
    coverage_summary,
)

# Stanhope — ECCC reference station download + cache
from pea_met_network.stanhope_cache import (
    fetch_stanhope_hourly_month,
    materialize_stanhope_hourly_range,
    normalize_stanhope_hourly,
)

# Validation — schema and data validation
from pea_met_network.validation import validate_station_output

# Uncertainty — uncertainty quantification
from pea_met_network.uncertainty import compute_uncertainty

# Redundancy — cross-station redundancy analysis
from pea_met_network.redundancy import detect_redundant_stations
```

### Entry Points (how to run things)

| Task | Command |
|------|---------|
| **Full pipeline** | `python cleaning.py` |
| **Single station** | `python cleaning.py --stations cavendish` |
| **Skip Stanhope download** | `python cleaning.py --skip-stanhope` |
| **Run tests** | `.venv/bin/pytest tests/ -q` |
| **Run specific test** | `.venv/bin/pytest tests/test_fwi.py -q` |
| **Lint check** | `.venv/bin/ruff check .` |
| **Auto-fix lint** | `.venv/bin/ruff check --fix .` |
| **Validate artifacts** | `python scripts/validate_artifacts.py` |
| **Sync state machine** | `python scripts/sync_state.py` |
| **Record verdict** | `python scripts/record_verdict.py PASS "reason"` |

### Additional Libraries Available (use when needed)

| Library | Use For | Import |
|---------|---------|--------|
| `matplotlib.pyplot` | Plotting, figures, charts | `import matplotlib.pyplot as plt` |
| `seaborn` | Statistical visualization | `import seaborn as sns` |
| `openpyxl` | XLSX read/write (used by xlsx_adapter) | `import openpyxl` |
| `requests` | HTTP requests (used by stanhope_cache) | `import requests` |
| `httpx` | Async HTTP client | `import httpx` |
| `json` (stdlib) | JSON I/O | `import json` |
| `pathlib` (stdlib) | File path manipulation | `from pathlib import Path` |
| `xml.etree.ElementTree` (stdlib) | XML parsing (used by xle_adapter) | `import xml.etree.ElementTree as ET` |
| `csv` (stdlib) | CSV I/O (but prefer pandas) | `import csv` |
| `datetime` (stdlib) | Date/time manipulation | `from datetime import datetime, timedelta` |
| `re` (stdlib) | Regex | `import re` |
| `subprocess` (stdlib) | Shell commands (but prefer exec tool) | `import subprocess` |
| `PyYAML` | YAML I/O | `import yaml` |
| `jsonschema` | JSON Schema validation | `import jsonschema` |
| `prometheus_client` | Metrics export | `import prometheus_client` |
| `psutil` | System monitoring | `import psutil` |
| `Pillow` | Image processing | `from PIL import Image` |
| `arrow` | Human-friendly dates | `import arrow` |
| `Jinja2` | Template rendering | `from jinja2 import Template` |

### Approach Guidance

**DO:**
- Use `pd.read_csv()` / `df.to_csv()` — not `csv` module or manual parsing
- Use `df.groupby()` / `df.resample()` — not manual loops over rows
- Use `df.isna().sum()` — not manual NaN counting
- Use `df.describe()` — not manual min/max/mean calculation
- Use `df.merge()` / `pd.concat()` — not manual row-by-row joining
- Use `sklearn` for any clustering, PCA, or normalization
- Use `scipy.stats` for any statistical tests
- Use `assert` statements in tests — not `if not x: raise Exception`
- Use `pathlib.Path` — not `os.path`

**DO NOT:**
- Write `for i, row in df.iterrows():` to compute values — use vectorized pandas/numpy
- Implement your own CSV parser — pandas handles this
- Implement your own statistical functions — scipy handles this
- Write manual schema validation when `pea_met_network.validation` exists
- Use `os.system()` or `subprocess` to run pipeline commands — use the exec tool
- Reinstall packages — they're already in the venv
- Write one-off scripts in `/tmp` — put them in `scripts/` and commit them

### ⚠️ exec Tool: Do NOT pass node parameter

When using the `exec` tool, do NOT pass the `node` parameter. Omit it entirely.
Passing `node: ""` or `node: null` causes a "node not found" error and wastes
iterations on retries.

---

