# Spec 02: Format Adapters — All Formats (CSV/XLSX/XLE/JSON)

**Phase:** 2  
**Status:** Pending  
**Depends on:** Phase 1 (Adapter Architecture)

---

## Goal

Build ALL format adapters in one phase. Each adapter reads its format and outputs the canonical schema. No format is deferred to a later phase.

---

## Adapters to Build

| Adapter | Formats | Sources |
|---------|---------|---------|
| `csv_adapter.py` | `.csv` | PEINP archive CSVs, ECCC Stanhope CSVs |
| `xlsx_adapter.py` | `.xlsx` | Greenwich 2023 Excel files |
| `xle_adapter.py` | `.xle` | Stanley Bridge 2022 Solinst logger files |
| `json_adapter.py` | `.json` | Licor Cloud API responses |

---

## Deliverables

### 1. `src/pea_met_network/adapters/csv_adapter.py`

- Extends `BaseAdapter`
- Handles both PEINP schema and ECCC schema
- Uses `column_maps.py` for header normalization
- Outputs canonical schema columns

```python
class CSVAdapter(BaseAdapter):
    def load(self, path: Path) -> pd.DataFrame:
        # Detect schema (PEINP vs ECCC)
        # Normalize column names using column_maps
        # Parse timestamps to UTC
        # Return DataFrame with canonical schema
```

### 2. `src/pea_met_network/adapters/xlsx_adapter.py`

- Uses `pandas.read_excel()` with `openpyxl` engine
- Same column normalization as CSV
- Greenwich 2023 files have same schema as PEINP CSVs

```python
class XLSXAdapter(BaseAdapter):
    def load(self, path: Path) -> pd.DataFrame:
        df = pd.read_excel(path, engine='openpyxl')
        # Normalize columns, parse timestamps
        # Return DataFrame with canonical schema
```

### 3. `src/pea_met_network/adapters/xle_adapter.py`

- XLE is Solinst logger format (XML-based)
- Parse with `xml.etree.ElementTree` or `lxml`
- Extract timestamp and measurement columns
- Stanley Bridge 2022 files

```python
class XLEAdapter(BaseAdapter):
    def load(self, path: Path) -> pd.DataFrame:
        tree = ET.parse(path)
        # Extract data from XML structure
        # Return DataFrame with canonical schema
```

### 4. `src/pea_met_network/adapters/json_adapter.py`

- Licor Cloud API JSON format
- 5 stations with device serial numbers
- Map device serial to station name using `devices.json`
- Handle nested data structures

```python
class JSONAdapter(BaseAdapter):
    def load(self, path: Path) -> pd.DataFrame:
        with open(path) as f:
            data = json.load(f)
        # Flatten nested structure
        # Map device serial to station name
        # Return DataFrame with canonical schema
```

### 5. `src/pea_met_network/adapters/column_maps.py`

Centralized column name mappings for all PEINP/ECCC variants:

```python
COLUMN_MAPS = {
    # Temperature
    "S-THB: Temp - °C": "air_temperature_c",
    "S-THC: Temp - °C": "air_temperature_c",
    "S-TMB: Temp - °C": "air_temperature_c",
    "Temp (°C)": "air_temperature_c",
    "Temperature (°C)": "air_temperature_c",
    
    # Humidity
    "S-THB: RH - %": "relative_humidity_pct",
    "S-THC: RH - %": "relative_humidity_pct",
    "RH (%)": "relative_humidity_pct",
    
    # Rain
    "Rain - mm": "rain_mm",
    "Precip (mm)": "rain_mm",
    
    # Wind speed (km/h)
    "Wind Speed - km/h": "wind_speed_kmh",
    "Average Wind Speed": "wind_speed_kmh",
    "wind speed": "wind_speed_kmh",
    
    # Wind speed (m/s)
    "S-WCF-M: Wind Speed - m/s": "wind_speed_ms",
    "Wind Speed - m/s": "wind_speed_ms",
    
    # Wind gust
    "Wind gust  speed": "wind_gust_speed_kmh",  # double-space variant
    "Wind gust speed": "wind_gust_speed_kmh",
    "Gust (km/h)": "wind_gust_speed_kmh",
    
    # Wind direction
    "S-WCF-M: Wind Direction - °": "wind_direction_deg",
    "Wind Direction - °": "wind_direction_deg",
    "Wind Dir (°)": "wind_direction_deg",
    
    # Solar radiation
    "Solar Rad - W/m²": "solar_radiation_w_m2",
    
    # Dew point
    "Dew Point (°C)": "dew_point_c",
    
    # Pressure
    "Pressure (hPa)": "pressure_hpa",
    "Barometric Pressure": "barometric_pressure_kpa",
    
    # Water level (coastal stations)
    "Water Level - m": "water_level_m",
    "Water Pressure - kPa": "water_pressure_kpa",
    "Water Temp - °C": "water_temperature_c",
    
    # Battery
    "Battery - V": "battery_v",
}

# Columns to skip (not useful for FWI)
SKIP_COLUMNS = [
    "accumulated_rain_mm",  # running total, not instantaneous
]

def wind_speed_kmh_from_ms(df: pd.DataFrame) -> pd.DataFrame:
    """Derive wind_speed_kmh from wind_speed_ms if only m/s available."""
    if "wind_speed_ms" in df.columns and "wind_speed_kmh" not in df.columns:
        df["wind_speed_kmh"] = df["wind_speed_ms"] * 3.6
    return df
```

---

## Acceptance Criteria

| ID | Criterion |
|----|-----------|
| AC-FMT-1 | `csv_adapter.py` loads PEINP archive CSVs (all 5 stations) |
| AC-FMT-2 | `csv_adapter.py` loads ECCC Stanhope CSVs with different schema |
| AC-FMT-3 | `xlsx_adapter.py` loads Greenwich 2023 Excel files |
| AC-FMT-4 | `xle_adapter.py` loads Stanley Bridge 2022 Solinst files |
| AC-FMT-5 | `json_adapter.py` loads Licor Cloud API JSON files |
| AC-FMT-6 | All adapters output DataFrame with canonical schema columns |
| AC-FMT-7 | `wind_speed_kmh` derived from `wind_speed_ms` (×3.6) when only m/s available |
| AC-FMT-8 | Water-level columns (`water_level_m`, `water_pressure_kpa`, `water_temperature_c`) present for coastal stations |
| AC-FMT-9 | `accumulated_rain_mm` excluded from output (not in canonical schema) |

---

## Exit Gate

```bash
pytest tests/test_v2_pipeline.py::TestAC_PIPE_2_AllFormatAdapters -v
```

All tests must pass.
