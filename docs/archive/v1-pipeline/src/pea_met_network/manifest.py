from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

ROOT_DATA_DIR = Path("data/raw")

STATION_PATTERNS = {
    "cavendish": ["cavendish", "_cav_"],
    "greenwich": ["greenwich", "_gr_"],
    "north_rustico": ["north rustico", "north_rustico", "_nr_"],
    "stanley_bridge": ["stanley bridge", "stanley_bridge", "_sb_"],
    "tracadie": ["tracadie wharf", "tracadie", "_tr_"],
    "stanhope": ["stanhope", "eccc"],
}


@dataclass(frozen=True)
class RawFileRecord:
    path: Path
    relative_path: str
    station: str
    year: int | None
    extension: str


@dataclass(frozen=True)
class SchemaSignature:
    column_count: int
    timestamp_columns: tuple[str, ...]
    has_temperature: bool
    has_relative_humidity: bool
    has_wind_speed: bool
    has_rain: bool


@dataclass(frozen=True)
class SchemaMatch:
    family: str
    signature: SchemaSignature


class ManifestError(RuntimeError):
    pass


class SchemaRecognitionError(RuntimeError):
    pass


def iter_raw_files(base_dir: Path) -> Iterable[Path]:
    raw_dir = base_dir / ROOT_DATA_DIR
    if not raw_dir.exists():
        raise ManifestError(f"Raw data directory not found: {raw_dir}")

    for path in sorted(raw_dir.rglob("*")):
        if path.is_file() and path.suffix.lower() in {".csv", ".xlsx", ".xle"}:
            yield path


def infer_station(relative_path: str) -> str:
    lowered = relative_path.lower()
    for station, patterns in STATION_PATTERNS.items():
        if any(pattern in lowered for pattern in patterns):
            return station
    return "unknown"


def infer_year(relative_path: str) -> int | None:
    for part in Path(relative_path).parts:
        if part.isdigit() and len(part) == 4:
            return int(part)
    return None


def build_raw_manifest(base_dir: Path) -> list[RawFileRecord]:
    records: list[RawFileRecord] = []
    for path in iter_raw_files(base_dir):
        relative_path = path.relative_to(base_dir / ROOT_DATA_DIR).as_posix()
        records.append(
            RawFileRecord(
                path=path,
                relative_path=relative_path,
                station=infer_station(relative_path),
                year=infer_year(relative_path),
                extension=path.suffix.lower(),
            )
        )
    return records


TEMPERATURE_MARKERS = ("temperature",)
HUMIDITY_MARKERS = ("relative humidity", "rh")
WIND_MARKERS = ("wind speed", "average wind speed")
RAIN_MARKERS = ("rain", "accumulated rain")


def _normalize_columns(columns: Iterable[str]) -> tuple[str, ...]:
    return tuple(column.strip() for column in columns)


def _contains_any(columns: tuple[str, ...], markers: tuple[str, ...]) -> bool:
    lowered = [column.lower() for column in columns]
    return any(marker in column for column in lowered for marker in markers)


def recognize_schema(columns: Iterable[str]) -> SchemaMatch:
    normalized = _normalize_columns(columns)
    lowered = [column.lower() for column in normalized]

    timestamp_columns = tuple(
        column for column in normalized if column.lower() in {"date", "time"}
    )
    has_date_time = set(timestamp_columns) == {"Date", "Time"}
    has_timestamp = any("timestamp" == column for column in lowered)

    signature = SchemaSignature(
        column_count=len(normalized),
        timestamp_columns=timestamp_columns,
        has_temperature=_contains_any(normalized, TEMPERATURE_MARKERS),
        has_relative_humidity=_contains_any(normalized, HUMIDITY_MARKERS),
        has_wind_speed=_contains_any(normalized, WIND_MARKERS),
        has_rain=_contains_any(normalized, RAIN_MARKERS),
    )

    if has_date_time:
        if len(normalized) >= 20:
            family = "legacy_dual_wind_family"
        elif len(normalized) >= 14:
            family = "hoboware_date_time_family"
        else:
            family = "minimal_date_time_family"
        return SchemaMatch(family=family, signature=signature)

    if has_timestamp:
        return SchemaMatch(
            family="single_timestamp_family",
            signature=signature,
        )

    raise SchemaRecognitionError(
        "Could not recognize schema for columns: "
        + ", ".join(normalized[:5])
    )
