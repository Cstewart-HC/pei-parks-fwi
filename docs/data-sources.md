# Data Sources and Limitations

## Overview
This project uses multiple data sources for PEI meteorological data and Fire Weather Index calculations.

## PEINP Stations (Parks Canada)

### Stations
| Station | Operator | Data Source |
|---|---|---|
| Stanhope | ECCC | ECCC Climate API |
| Greenwich | UPEI (Donald Jardine) | HOBOlink |
| Red Head | MCPEI (Donald Jardine) | HOBOlink |
| Tracadie | UPEI (Donald Jardine) | HOBOlink |
| North Rustico | UPEI (Donald Jardine) | HOBOlink |
| Cavendish | UPEI (Donald Jardine) | HOBOlink |
| Stanley Bridge | UPEI (Donald Jardine) | HOBOlink |

### Data Access

#### HOBOlink (Onset)
- Public URLs available for each station
- Historical data accessible via HOBOlink web interface
- API integration potential (Licor Cloud API documented below)
- Primary source for 5 of 6 PEINP stations

#### ECCC Climate API
- Stanhope station monitored by ECCC
- Bulk data available at: https://climate.weather.gc.ca/historical_data/search_historic_data_e.html
- Automated downloader implemented: `scripts/stanhope_cache.py`
- Pulls hourly meteorological data by month

## Licor Cloud API

### Purpose
Real-time and historical data access for PEINP stations via Onset's cloud platform.

### Endpoints
- Devices: `https://api.licor.cloud/v1/docs/#/default/get_v2_devices`
- Timeseries data: `https://api.licor.cloud/v2/docs/#/default/get_v2_data`

### Usage
- **Read-only access only**
- Respectful rate limiting required
- Do not flood the API
- API token stored in environment: `HC_CS_PEIPCWX_PROD_RO`

### Example Request
```bash
curl -X GET "https://api.licor.cloud/v2/data?deviceSerialNumber=21114831&startTime=${START}&endTime=${END}" \
  -H "Authorization: Bearer $HC_CS_PEIPCWX_PROD_RO" \
  -H "Content-Type: application/json"
```

### Known Sensors (North Rustico - Device 21114831)
| Sensor Serial | Type | Units |
|---|---|---|
| 21548413-1 | Barometric Pressure | kPa |
| 21411214-1 | Water Pressure | kPa |
| 21411214-2 | Diff Pressure | kPa |
| 21411214-3 | Water Temperature | °C |
| 21113174-1 | Avg Wind speed | Km/h |
| 21113174-2 | Wind gust speed | Km/h |
| 21113174-3 | Wind Direction | ° |
| 21411214-4 | Water Level | meters |
| 21035308-1 | Rain | mm |
| 21105865-1 | Solar Radiation | W/m² |
| 21648581-1 | Temperature | °C |
| 21648581-2 | RH | % |
| 21648581-3 | Dew Point | °C |
| 21035308-2 | Accumulated Rain | mm |

## CWFIS API (NRCan)

### Purpose
Canadian Fire Weather Information System API for fire weather station data and calculated FWI values.

### Endpoint
- WMS capabilities: `https://cwfis.cfs.nrcan.gc.ca/geoserver/wms?&version=1.3.0&service=WMS&request=getCapabilities`

### Notes
- Fire weather station locations available via WMS
- Historical data availability requires further investigation
- Potential source for reference data or validation

## Data Limitations

### Winter Precipitation Underreporting

**Issue:**
- PEINP stations are not equipped with heated rain sensors
- During winter months, precipitation falls as snow/ice
- Standard tipping bucket rain gauges cannot measure frozen precipitation without heaters

**Impact:**
- Winter precipitation is systematically underreported or absent
- Fire Weather Index (FWI) calculations rely heavily on precipitation data
- Drought indices (DC, DMC) will drift inaccurately during snowfall events
- FWI values during winter periods may not reflect actual fuel moisture conditions

**Evidence:**
- API test (March 2026) showed only 6 records for Accumulated Rain sensor vs. 2,000+ for other sensors
- Consistent with physical limitation of unheated rain gauges in winter conditions

**Recommendation:**
- Document this limitation clearly in all analysis outputs
- Consider proxy winter precipitation data from heated reference stations (e.g., ECCC Stanhope) if needed for FWI accuracy
- Clearly flag winter-period FWI calculations as having reduced confidence due to missing precipitation data

### North Rustico Data Gaps

**Issue:**
- North Rustico station data begins April 2023
- No data for January, February, March 2023
- Sparse coverage overall compared to other stations

**API Retention Limit:**
- Licor Cloud API has a rolling 12-month retention window
- As of 2026-03-24, earliest accessible date is approximately 2025-03-26
- Jan–Mar 2023 gap is **outside the API retention window** — cannot be filled via API
- Historical data prior to ~March 2025 must come from other sources (manual CSV delivery, UPEI/Donald Jardine archives)

**Current Status:**
- Jan–Mar 2023 gap: **cannot be filled via API** (outside retention window)
- Jan 2026 – present: **fetched via API** (see below)
- Stanhope proxy available (19.4 km distance) but not implemented
- Contact UPEI/Donald Jardine for pre-API historical data if needed

**Fetched via Licor Cloud API:**
- Device: 21114831 (North Rustico Wharf)
- Range: 2026-01-01 to 2026-03-24
- Records: ~590,000 total across 13 sensors
- Script: `scripts/licor_cache.py`
- Raw data: `data/raw/licor/21114831/`
- Combined file: `data/raw/licor/21114831/2026-01-01_2026-03-24_combined.json`
- Fetched: 2026-03-24
- Method: 7-day chunks, 2-second delay between requests (respectful rate limiting)

## Data Acquisition Notes

### Manual Data Delivery
- Initial PEINP station data provided as manual CSV files
- Located in: `data/raw/peinp/PEINP Weather Station Data 2022-2025/`
- No automated ingestion pipeline for initial data load

### Automation Potential
- ECCC Stanhope data: Already automated via `stanhope_cache.py`
- HOBOlink data: Public URLs available, API integration possible via Licor Cloud API
- CWFIS data: API available, historical coverage uncertain

## References
- ECCC Historical Climate Data: https://climate.weather.gc.ca/historical_data/search_historic_data_e.html
- CWFIS Fire Weather Maps: https://cwfis.cfs.nrcan.gc.ca/
- PEI Weather and Climate App: Public web interface for PEINP station data
- Licor Cloud API docs: https://api.licor.cloud/v2/docs/

---

## Licor Cloud API (HOBOlink External API)

### Authentication
- **Type:** Bearer token
- **Environment variable:** `HC_CS_PEIPCWX_PROD_RO`
- **Policy:** READ-ONLY. Respect rate limits. Minimum 2-second delay between requests. Do not flood.

### Base URL
```
https://api.licor.cloud/v2
```

### Endpoints

#### `GET /v2/devices`
Fetch a list of devices for the organization.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `includeSensors` | boolean | No | Include sensors data (default: true) |

**Response (200 OK):**
```json
{
  "total": 51,
  "devices": [
    {
      "deviceName": "string",
      "deviceSerialNumber": "string",
      "productCode": "string",
      "lastConnectionTime": "ISO8601",
      "loggingState": "LOGGING|STOPPED",
      "alarmed": true,
      "unitSystem": "SI",
      "sensors": [
        {
          "sensorSerialNumber": "string",
          "measurementType": "string",
          "units": "string",
          "latest": float|null
        }
      ]
    }
  ]
}
```

**Error codes:** 400 (invalid params), 401 (unauthorized), 500 (server error)

#### `GET /v2/data`
Fetch timeseries data for a device (and optionally a sensor) within a time range.

| Parameter | Type | Required | Description |
|---|---|---|---|
| `deviceSerialNumber` | string | Yes | Device serial number |
| `sensorSerialNumber` | string | No | Specific sensor serial number |
| `startTime` | number | Yes | Start time (Unix ms) |
| `endTime` | number | Yes | End time (Unix ms) |

**Response (200 OK):**
```json
{
  "moreResults": true,
  "sensors": [
    {
      "totalRecords": 0,
      "sensorSerialNumber": "string",
      "latestTimestamp": 0,
      "data": [
        {
          "measurementType": "string",
          "dataType": "CURRENT",
          "units": "string",
          "records": [["timestamp_ms", "value"], ...]
        }
      ]
    }
  ]
}
```

**Error codes:** 400 (invalid params), 401 (unauthorized), 500 (server error)

### Known Limitations
- **12-month rolling retention:** Data older than ~365 days is unavailable via the API
- **No server-side aggregation:** All data returned at raw resolution (~2-5 min intervals). Client must resample.
- **Winter precipitation:** PEINP stations lack heated rain sensors; frozen precipitation not captured accurately

### PEINP Device Serials
See `data/raw/licor/devices.json` for complete device and sensor serial mappings (gitignored — contains serials).

| Station | Device SN | Sensors | Notes |
|---|---|---|---|
| Cavendish Green Gables | 21114839 | 9 | No water sensors |
| North Rustico Wharf | 21114831 | 15 | Full suite (weather + water) |
| Tracadie Wharf | 21038195 | 13 | Weather + water (no RH/dew point cluster) |
| Greenwich PEINP | 21114835 | 9 | No water sensors, no baro |
| Stanley Bridge Harbour | 21038161 | 13 | Weather + water (no RH/dew point cluster) |
