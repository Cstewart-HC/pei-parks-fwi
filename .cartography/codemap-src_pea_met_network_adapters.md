# Codemap: `src/pea_met_network/adapters/`

> PEA Met Network — File-format adapter layer.
> Each adapter normalizes raw meteorological data files into a **canonical pandas DataFrame**
> with columns defined in `schema.py` (`station`, `timestamp_utc`, plus optional measurement columns).

---

## Summary Table

| File | Purpose | Key Classes / Functions | Design Patterns |
|---|---|---|---|
| `__init__.py` | Package facade — re-exports public API symbols | `ADAPTER_REGISTRY`, `CANONICAL_SCHEMA`, `route_by_extension`, `BaseAdapter` | Facade |
| `base.py` | Abstract base class that all adapters must implement | `BaseAdapter` (ABC) | Template Method / Strategy |
| `column_maps.py` | Shared column-name mappings and normalization helpers | `COLUMN_MAPS`, `SKIP_COLUMNS`, `SKIP_PREFIXES`, `extract_prefix()`, `rename_columns()`, `coalesce_duplicate_columns()`, `derive_wind_speed_kmh()` | Data Mapper / Utility |
| `csv_adapter.py` | Loads PEINP archive CSVs, ECCC Stanhope CSVs, and Licor metadata-prefixed CSVs | `CSVAdapter`, `_skip_licor_metadata()`, `_detect_csv_schema()`, `_load_peinp_csv()`, `_load_eccc_csv()` | Strategy, Guard Clause |
| `json_adapter.py` | Loads Licor Cloud API JSON responses | `JSONAdapter`, `LICOR_MEASUREMENT_MAP`, `UNIT_CONVERSIONS`, `_load_devices_json()`, `_serial_to_station()` | Strategy |
| `registry.py` | Maps file extensions to adapter classes; factory function | `ADAPTER_REGISTRY`, `KNOWN_EXTENSIONS`, `route_by_extension()` | Registry / Factory |
| `schema.py` | Defines the canonical column schema for all adapters | `CANONICAL_SCHEMA` | Schema / Contract |
| `xle_adapter.py` | Loads Solinst XLE XML logger files | `XLEAdapter`, `_infer_station()` | Strategy |
| `xlsx_adapter.py` | Loads HOBOware / Parks Canada XLSX Excel files | `XLSXAdapter`, `_infer_station()`, `_is_date_value()` | Strategy |

---

## Architecture

```
                    ┌──────────────────┐
                    │   __init__.py    │  (Facade — re-exports)
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              │              │              │
     ┌────────▼──────┐  ┌───▼────┐  ┌─────▼──────────┐
     │  registry.py  │  │schema.py│  │   base.py      │
     │  (Factory)    │  │(Contract)│  │ (ABC: .load()) │
     └──┬───┬───┬───┘  └────────┘  └───────┬─────────┘
        │   │   │                          │
   ┌────┘   │   └────┐      ┌─────────────┼─────────────┐
   │        │        │      │             │             │
┌──▼──┐ ┌───▼───┐ ┌──▼──┐ ┌─▼───┐  ┌─────▼─────┐ ┌───▼────┐
│ .csv│ │ .xlsx │ │.json│ │ .xle│  │csv_adapter │ │xle_ad. │
└─────┘ └───────┘ └─────┘ └─────┘  └─────┬─────┘ └────────┘
                                       │         ┌──────────┐
                                       │    ┌────▼────┐ ┌───▼──────┐
                                       └────│xlsx_ad. │ │json_ad. │
                                            └────┬────┘ └──────────┘
                                                 │
                                            ┌────▼────────┐
                                            │ column_maps  │
                                            │ (rename,     │
                                            │  coalesce,   │
                                            │  derive)     │
                                            └─────────────┘
```

---

## Detailed Per-File Documentation

### `__init__.py`

**Purpose:** Serves as the package facade, re-exporting the four public API symbols that downstream code depends on.

**Key Classes/Functions:**
- Imports and re-exports: `BaseAdapter`, `ADAPTER_REGISTRY`, `route_by_extension`, `CANONICAL_SCHEMA`

**Design Patterns:** Facade — consolidates the public surface area of the adapters sub-package into one import point.

**Data Flow:** N/A (pure re-export module).

**Integration Points:**
- Imports from `base`, `registry`, `schema`.
- Consumed by any code that does `from pea_met_network.adapters import ...` (likely the pipeline/orchestration layer).

---

### `base.py`

**Purpose:** Defines the abstract interface (`BaseAdapter`) that every format-specific adapter must implement.

**Key Classes/Functions:**
- `BaseAdapter` (ABC) — abstract class with a single abstract method `load(self, path: Path) -> pd.DataFrame`.

**Design Patterns:** Strategy (the interface of the Strategy pattern); Template Method (abstract base enforces a contract).

**Data Flow:**
- **In:** A `Path` to a data file.
- **Out:** A `pd.DataFrame` with at minimum `station` (str) and `timestamp_utc` (datetime64[ns, UTC]) columns.

**Integration Points:**
- Imported by every concrete adapter (`csv_adapter`, `json_adapter`, `xlsx_adapter`, `xle_adapter`) and by `registry.py`.
- Used by the pipeline to invoke `adapter.load(path)` polymorphically.

---

### `column_maps.py`

**Purpose:** Provides shared column-name mappings and DataFrame normalization helpers used by CSV and XLSX adapters.

**Key Classes/Functions:**
- `COLUMN_MAPS` — `dict[str, str]` mapping raw column name prefixes to canonical column names (e.g., `"Rain - mm"` → `"rain_mm"`).
- `SKIP_COLUMNS` — `set[str]` of canonical column names to discard (e.g., `"accumulated_rain_mm"`).
- `SKIP_PREFIXES` — `set[str]` of raw column prefixes that should be dropped entirely.
- `extract_prefix(column: str) -> str` — strips parenthetical sensor serial numbers and normalizes whitespace.
- `rename_columns(df: pd.DataFrame) -> pd.DataFrame` — applies `COLUMN_MAPS` to rename and drop columns.
- `coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame` — merges columns sharing the same canonical name by forward-filling NaNs (prevents dead sensors from overwriting good data).
- `derive_wind_speed_kmh(df: pd.DataFrame) -> pd.DataFrame` — converts `wind_speed_ms` to `wind_speed_kmh` (×3.6) when only m/s is available.

**Design Patterns:** Data Mapper — translates between heterogeneous source column names and a unified canonical schema.

**Data Flow:**
- **In:** Raw `pd.DataFrame` with source-specific column names.
- **Out:** `pd.DataFrame` with canonical column names; duplicate columns coalesced; unwanted columns dropped.

**Integration Points:**
- Imported by `csv_adapter.py` and `xlsx_adapter.py`.
- Not imported by `json_adapter.py` or `xle_adapter.py` (they handle their own mappings).

---

### `csv_adapter.py`

**Purpose:** Loads CSV files in PEINP archive format, ECCC Stanhope format, or Licor metadata-prefixed format, auto-detecting the schema and normalizing to canonical columns.

**Key Classes/Functions:**
- `CSVAdapter(BaseAdapter)` — concrete adapter; `load()` detects Licor preamble, then dispatches to the correct sub-loader.
- `_skip_licor_metadata(path: Path) -> int | None` — detects Licor-style CSV with `Serial_number:` preamble block; returns 0-based row index of the actual header line.
- `_detect_csv_schema(df: pd.DataFrame) -> str` — heuristically determines `"eccc"` vs `"peinp"` from column names.
- `_load_peinp_csv(path: Path, skiprows)` — parses PEINP CSVs: applies column maps, parses `Date`+`Time` columns (with ISO fallback for Licor), converts all measurement columns to numeric.
- `_load_eccc_csv(path: Path) -> pd.DataFrame` — parses ECCC CSVs: applies column maps, finds `Date/Time (LST)` column, parses as UTC, drops metadata/flag columns, hard-codes station to `"stanhope"`.

**Design Patterns:** Strategy (concrete implementation of `BaseAdapter`); Double Dispatch (internal routing between PEINP and ECCC sub-parsers).

**Data Flow:**
- **In:** `Path` to a `.csv` file.
- **Out:** `pd.DataFrame` with `timestamp_utc`, `station`, `source_file`, and any available canonical measurement columns.

**Integration Points:**
- Inherits from `base.BaseAdapter`.
- Uses `column_maps.rename_columns`, `column_maps.coalesce_duplicate_columns`, `column_maps.derive_wind_speed_kmh`.
- Registered in `registry.ADAPTER_REGISTRY` under `.csv`.

---

### `json_adapter.py`

**Purpose:** Loads Licor Cloud API JSON files, resolving device serials to station names and converting measurement types to canonical columns.

**Key Classes/Functions:**
- `JSONAdapter(BaseAdapter)` — concrete adapter; handles both individual sensor JSON files and batch loading via `devices.json`.
- `LICOR_MEASUREMENT_MAP` — `dict[str, str]` mapping Licor measurement type names to canonical column names.
- `UNIT_CONVERSIONS` — `dict[str, dict[str, float]]` defining unit multipliers (e.g., Wind Speed m/s → km/h).
- `_load_devices_json(path: Path) -> dict` — reads the `devices.json` metadata file.
- `_serial_to_station(devices: dict, serial: str) -> str | None` — resolves a device serial to a station name.

**Design Patterns:** Strategy (concrete `BaseAdapter`); Mapper (Licor measurement types → canonical names).

**Data Flow:**
- **In:** `Path` to a `.json` file.
- **Out:** `pd.DataFrame` with `timestamp_utc`, `station`, `source_file`, and canonical measurement columns.

**Integration Points:**
- Inherits from `base.BaseAdapter`.
- Does NOT use `column_maps.py` — maintains its own mapping.
- Registered in `registry.ADAPTER_REGISTRY` under `.json`.

---

### `registry.py`

**Purpose:** Maps file extensions to adapter classes and provides a factory function to instantiate the correct adapter.

**Key Classes/Functions:**
- `ADAPTER_REGISTRY` — `dict[str, type[BaseAdapter]]` mapping `".csv"`, `".xlsx"`, `".xle"`, `".json"` to their respective adapter classes.
- `KNOWN_EXTENSIONS` — `set[str]` derived from `ADAPTER_REGISTRY.keys()`.
- `route_by_extension(path: Path) -> BaseAdapter` — factory function; looks up the file suffix, raises `ValueError` for unknown formats.

**Design Patterns:** Registry (static map of extensions → classes); Factory Method (`route_by_extension`).

**Data Flow:**
- **In:** A `Path` object.
- **Out:** An instance of the appropriate `BaseAdapter` subclass.

**Integration Points:**
- Imports all four concrete adapters and `BaseAdapter`.
- Re-exported via `__init__.py`.
- Called by the pipeline/orchestration layer to select an adapter for a given file.

---

### `schema.py`

**Purpose:** Defines the canonical column schema that every adapter's output DataFrame must satisfy.

**Key Classes/Functions:**
- `CANONICAL_SCHEMA` — `list[str]` of column names in priority order: required (`station`, `timestamp_utc`), FWI-required (`air_temperature_c`, `relative_humidity_pct`, `wind_speed_kmh`, `rain_mm`), optional meteorological, and coastal/water variables.

**Design Patterns:** Schema / Contract — acts as a data contract between adapters and downstream consumers.

**Data Flow:** Pure data definition; consumed as a reference by validation, merging, or pipeline code.

**Integration Points:**
- Re-exported via `__init__.py`.

---

### `xle_adapter.py`

**Purpose:** Loads Solinst XLE XML logger files (used for coastal water-level stations like Stanley Bridge 2022).

**Key Classes/Functions:**
- `XLEAdapter(BaseAdapter)` — concrete adapter; parses XML to extract channel data and timestamps.
- `_infer_station(path: Path) -> str | None` — static method; maps path keywords to station identifiers.

**Design Patterns:** Strategy (concrete `BaseAdapter`).

**Data Flow:**
- **In:** `Path` to an `.xle` file.
- **Out:** `pd.DataFrame` with `timestamp_utc`, `station`, `source_file`, and channel-derived columns (`water_level_m`, `water_temperature_c`).

**Integration Points:**
- Inherits from `base.BaseAdapter`. Has its own inline rename map.
- Registered in `registry.ADAPTER_REGISTRY` under `.xle`.

---

### `xlsx_adapter.py`

**Purpose:** Loads HOBOware and Parks Canada XLSX Excel files, auto-detecting the header row, filtering non-data rows, and normalizing to canonical columns.

**Key Classes/Functions:**
- `XLSXAdapter(BaseAdapter)` — concrete adapter; handles header-row detection, unit-row filtering, multi-station file rejection, Date/Time merge, and column normalization.
- `_is_date_value(val) -> bool` — static method; distinguishes actual date values from format strings ("mm/dd/yy", "hh:mm:ss").
- `_MAX_SINGLE_STATION_COLS` — column threshold (20) to reject multi-station summary exports early.
- `_merge_date_time(df) -> pd.DataFrame` — static method; merges separate Date/Time columns handling mixed datetime objects + strings from PEINP exports.
- `_infer_station(path: Path) -> str | None` — static method; maps path keywords to station identifiers.

**Design Patterns:** Strategy (concrete `BaseAdapter`); Guard Clause (skips non-data rows).

**Data Flow:**
- **In:** `Path` to an `.xlsx` file.
- **Out:** `pd.DataFrame` with `timestamp_utc`, `station`, `source_file`, and canonical measurement columns.

**Integration Points:**
- Inherits from `base.BaseAdapter`.
- Uses `column_maps.rename_columns` and `column_maps.derive_wind_speed_kmh`.
- Registered in `registry.ADAPTER_REGISTRY` under `.xlsx`.
