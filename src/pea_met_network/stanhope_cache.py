from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import urlopen

import pandas as pd

STANHOPE_STATION_ID = "8300590"
STANHOPE_CLIMATE_ID = "1108299"
STANHOPE_WEATHERCAN_STATION_ID = 6545
REQUEST_DELAY_SECONDS = 1.0
RAW_CACHE_DIR = Path("data/raw/eccc/stanhope")
PROVENANCE_FILENAME = "provenance.json"


class StanhopeIngestionError(RuntimeError):
    pass


@dataclass(frozen=True)
class StanhopeRequest:
    year: int
    month: int
    interval: str = "hourly"

    def cache_filename(self) -> str:
        return f"stanhope_{self.interval}_{self.year}_{self.month:02d}.csv"

    def coverage_period(self) -> str:
        return f"{self.year:04d}-{self.month:02d}"


@dataclass(frozen=True)
class ProvenanceRecord:
    station_identifier: str
    climate_identifier: str
    interval: str
    year: int
    month: int
    coverage_period: str
    source_url: str
    retrieved_at_utc: str
    local_cache_path: str
    status: str


class StanhopeClient:
    def fetch(self, url: str) -> bytes:
        with urlopen(url) as response:  # noqa: S310
            return response.read()


@dataclass(frozen=True)
class StanhopeMaterializationResult:
    year: int
    month: int
    cache_path: Path
    status: str


def iter_month_requests(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
) -> list[StanhopeRequest]:
    if not 1 <= start_month <= 12 or not 1 <= end_month <= 12:
        raise StanhopeIngestionError("Month must be between 1 and 12.")

    start_key = (start_year, start_month)
    end_key = (end_year, end_month)
    if start_key > end_key:
        raise StanhopeIngestionError(
            "Start year/month must be before or equal to end year/month."
        )

    requests: list[StanhopeRequest] = []
    year = start_year
    month = start_month

    while (year, month) <= end_key:
        requests.append(StanhopeRequest(year=year, month=month))
        month += 1
        if month == 13:
            year += 1
            month = 1

    return requests


def materialize_stanhope_hourly_range(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    *,
    cache_dir: Path = RAW_CACHE_DIR,
    client: StanhopeClient | None = None,
    sleep_seconds: float = REQUEST_DELAY_SECONDS,
    force: bool = False,
) -> list[StanhopeMaterializationResult]:
    results: list[StanhopeMaterializationResult] = []

    for request in iter_month_requests(
        start_year=start_year,
        start_month=start_month,
        end_year=end_year,
        end_month=end_month,
    ):
        cache_path, status = fetch_stanhope_hourly_month(
            request.year,
            request.month,
            cache_dir=cache_dir,
            client=client,
            sleep_seconds=sleep_seconds,
            force=force,
        )
        results.append(
            StanhopeMaterializationResult(
                year=request.year,
                month=request.month,
                cache_path=cache_path,
                status=status,
            )
        )

    return results


def build_hourly_url(request: StanhopeRequest) -> str:
    if request.interval != "hourly":
        raise StanhopeIngestionError(
            f"Unsupported interval for URL builder: {request.interval}"
        )

    return (
        "https://climate.weather.gc.ca/climate_data/bulk_data_e.html?"
        f"format=csv&stationID={STANHOPE_WEATHERCAN_STATION_ID}"
        f"&Year={request.year}&Month={request.month}"
        "&Day=1&timeframe=1&submit=Download+Data"
    )


def _provenance_path(cache_dir: Path) -> Path:
    return cache_dir / PROVENANCE_FILENAME


def _load_provenance(cache_dir: Path) -> list[dict[str, object]]:
    path = _provenance_path(cache_dir)
    if not path.exists():
        return []
    return json.loads(path.read_text())


def _save_provenance(cache_dir: Path, records: list[dict[str, object]]) -> None:
    path = _provenance_path(cache_dir)
    path.write_text(json.dumps(records, indent=2, sort_keys=True) + "\n")


def fetch_stanhope_hourly_month(
    year: int,
    month: int,
    *,
    cache_dir: Path = RAW_CACHE_DIR,
    client: StanhopeClient | None = None,
    sleep_seconds: float = REQUEST_DELAY_SECONDS,
    force: bool = False,
) -> tuple[Path, str]:
    request = StanhopeRequest(year=year, month=month)
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path = cache_dir / request.cache_filename()

    if cache_path.exists() and not force:
        return cache_path, "cached"

    source_url = build_hourly_url(request)
    active_client = client or StanhopeClient()

    if sleep_seconds > 0:
        time.sleep(sleep_seconds)

    try:
        payload = active_client.fetch(source_url)
    except HTTPError as exc:
        if exc.code == 429:
            raise StanhopeIngestionError(
                "Stanhope retrieval hit HTTP 429; keep cache and retry later."
            ) from exc
        raise StanhopeIngestionError(
            f"Stanhope retrieval failed with HTTP {exc.code}."
        ) from exc

    cache_path.write_bytes(payload)
    record = ProvenanceRecord(
        station_identifier=STANHOPE_STATION_ID,
        climate_identifier=STANHOPE_CLIMATE_ID,
        interval=request.interval,
        year=year,
        month=month,
        coverage_period=request.coverage_period(),
        source_url=source_url,
        retrieved_at_utc=datetime.now(UTC).isoformat(),
        local_cache_path=str(cache_path),
        status="downloaded",
    )
    records = _load_provenance(cache_dir)
    records.append(asdict(record))
    _save_provenance(cache_dir, records)
    return cache_path, "downloaded"


STANHOPE_COLUMN_RENAMES = {
    "Temp (°C)": "air_temperature_c",
    "Temp Flag": "air_temperature_flag",
    "Dew Point Temp (°C)": "dew_point_c",
    "Dew Point Temp Flag": "dew_point_flag",
    "Rel Hum (%)": "relative_humidity_pct",
    "Rel Hum Flag": "relative_humidity_flag",
    "Precip. Amount (mm)": "rain_mm",
    "Precip. Amount Flag": "rain_mm_flag",
    "Wind Dir (10s deg)": "wind_direction_tens_deg",
    "Wind Dir Flag": "wind_direction_flag",
    "Wind Spd (km/h)": "wind_speed_kmh",
    "Wind Spd Flag": "wind_speed_flag",
    "Visibility (km)": "visibility_km",
    "Visibility Flag": "visibility_flag",
    "Stn Press (kPa)": "station_pressure_kpa",
    "Stn Press Flag": "station_pressure_flag",
    "Hmdx": "humidex",
    "Hmdx Flag": "humidex_flag",
    "Wind Chill": "wind_chill_c",
    "Wind Chill Flag": "wind_chill_flag",
    "Weather": "weather_text",
}


def _stanhope_timestamp_utc(frame: pd.DataFrame) -> pd.Series:
    timestamp_text = frame["Date/Time (LST)"].astype(str).str.strip()
    timestamp = pd.to_datetime(timestamp_text, format="%Y-%m-%d %H:%M")
    return (
        timestamp
        .dt.tz_localize(
            "America/Halifax", nonexistent="shift_forward", ambiguous="infer"
        )
        .dt.tz_convert("UTC")
    )


def normalize_stanhope_hourly(
    source_path: Path,
    station: str = "stanhope",
) -> pd.DataFrame:
    frame = pd.read_csv(source_path)
    rename_map = {
        column: STANHOPE_COLUMN_RENAMES[column]
        for column in frame.columns
        if column in STANHOPE_COLUMN_RENAMES
    }
    renamed = frame.rename(columns=rename_map)

    normalized = pd.DataFrame(
        {
            "station": station,
            "timestamp_utc": _stanhope_timestamp_utc(frame),
        }
    )

    for column in rename_map.values():
        if column in {"weather_text"}:
            normalized[column] = renamed[column].astype("string")
            continue
        normalized[column] = pd.to_numeric(renamed[column], errors="coerce")

    if "wind_direction_tens_deg" in normalized.columns:
        normalized["wind_direction_deg"] = (
            normalized["wind_direction_tens_deg"] * 10
        )

    normalized["source_file"] = str(source_path)
    normalized["schema_family"] = "stanhope_hourly_eccc"
    return normalized
