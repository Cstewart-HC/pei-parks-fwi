# Spec 01: Pipeline Refactor — Adapter Architecture

**Phase:** 1  
**Status:** Pending  
**Depends on:** None

---

## Goal

Establish a single entry point pipeline with an adapter architecture that routes all raw file formats to a canonical schema. No files are skipped — unknown formats are hard errors.

---

## Architecture

```
data/raw/ (all formats)
        │
        ▼
┌───────────────────────────────┐
│    SINGLE ENTRY POINT         │
│    cleaning.py                │
│                               │
│    discover → route → adapt   │
└─────────────────���─────────────┘
        │
        ▼ (canonical DataFrame per station)
┌───────────────────────────────┐
│    concat → dedup → resample  │
│    → impute → FWI             │
└───────────────────────────────┘
        │
        ▼
data/processed/ (SSOT)
```

---

## Deliverables

### 1. `src/pea_met_network/adapters/__init__.py`

- `BaseAdapter` abstract class with `load(path: Path) -> pd.DataFrame`
- `ADAPTER_REGISTRY: Dict[str, Type[BaseAdapter]]`
- `route_by_extension(path: Path) -> BaseAdapter`
- `CANONICAL_SCHEMA: List[str]` constant

### 2. `src/pea_met_network/adapters/base.py`

```python
from abc import ABC, abstractmethod
import pandas as pd
from pathlib import Path

class BaseAdapter(ABC):
    @abstractmethod
    def load(self, path: Path) -> pd.DataFrame:
        """Load raw file and return DataFrame with canonical schema."""
        pass
```

### 3. `src/pea_met_network/adapters/registry.py`

```python
ADAPTER_REGISTRY = {
    ".csv": CSVAdapter,
    ".xlsx": XLSXAdapter,
    ".xle": XLEAdapter,
    ".json": JSONAdapter,
}

def route_by_extension(path: Path) -> BaseAdapter:
    ext = path.suffix.lower()
    if ext not in ADAPTER_REGISTRY:
        raise ValueError(f"Unknown file format: {ext} (file: {path})")
    return ADAPTER_REGISTRY[ext]()
```

### 4. Canonical Schema Constant

```python
CANONICAL_SCHEMA = [
    "station",
    "timestamp_utc",
    "air_temperature_c",
    "relative_humidity_pct",
    "rain_mm",
    "wind_speed_kmh",
    "wind_direction_deg",
    "wind_gust_speed_kmh",
    "solar_radiation_w_m2",
    "dew_point_c",
    "pressure_hpa",
    # Water-level columns (for coastal stations)
    "water_level_m",
    "water_pressure_kpa", 
    "water_temperature_c",
    "barometric_pressure_kpa",
    # Battery/power
    "battery_v",
]
```

### 5. `cleaning.py` — Dry-Run Flag

```python
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", 
                        help="Report what would be processed without writing outputs")
    parser.add_argument("--stations", nargs="*", 
                        help="Process only these stations")
    args = parser.parse_args()
    
    if args.dry_run:
        # Discover files, report counts per station/format, exit
        report_dry_run()
        return
```

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-ARCH-1 | `src/pea_met_network/adapters/` module exists with `__init__.py`, `base.py`, `registry.py` |
| AC-ARCH-2 | `ADAPTER_REGISTRY` has entries for `.csv`, `.xlsx`, `.xle`, `.json` |
| AC-ARCH-3 | `route_by_extension()` raises `ValueError` for unknown formats (not silent skip) |
| AC-ARCH-4 | `CANONICAL_SCHEMA` constant is defined and exported |
| AC-ARCH-5 | `BaseAdapter` abstract class exists with `load()` method |
| AC-ARCH-6 | `cleaning.py --dry-run` reports file counts per station/format without writing outputs |
| AC-ARCH-7 | `cleaning.py --dry-run` exits 0 without creating any files in `data/processed/` |
| AC-ARCH-8 | Unknown file format in `data/raw/` causes hard error (exit non-zero) |

---

## Exit Gate

```bash
pytest tests/test_v2_pipeline.py::TestAC_PIPE_1_AdapterArchitecture -v
```

All 8 tests must pass.
