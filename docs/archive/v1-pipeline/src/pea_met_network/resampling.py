from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

TIMESTAMP_COLUMN = "timestamp_utc"
STATION_COLUMN = "station"

MEAN_VARIABLES = {
    "air_temperature_c",
    "relative_humidity_pct",
    "dew_point_c",
    "wind_speed_kmh",
    "wind_speed_ms",
    "wind_gust_speed_kmh",
    "solar_radiation_w_m2",
    "battery_v",
}

SUM_VARIABLES = {
    "rain_mm",
}

MAX_VARIABLES = {
    "wind_gust_speed_max_kmh",
}

FIRST_VARIABLES = {
    "wind_direction_deg",
}

SUPPORTED_FREQUENCIES = {"hourly": "1h", "daily": "1D"}


@dataclass(frozen=True)
class AggregationPolicy:
    mean_variables: frozenset[str]
    sum_variables: frozenset[str]
    max_variables: frozenset[str]
    first_variables: frozenset[str]

    def for_column(self, column: str) -> str:
        if column in self.mean_variables:
            return "mean"
        if column in self.sum_variables:
            return "sum"
        if column in self.max_variables:
            return "max"
        if column in self.first_variables:
            return "first"
        raise KeyError(f"No aggregation rule defined for column: {column}")


DEFAULT_POLICY = AggregationPolicy(
    mean_variables=frozenset(MEAN_VARIABLES),
    sum_variables=frozenset(SUM_VARIABLES),
    max_variables=frozenset(MAX_VARIABLES),
    first_variables=frozenset(FIRST_VARIABLES),
)

REQUIRED_COLUMNS = {TIMESTAMP_COLUMN, STATION_COLUMN}


def validate_normalized_frame(frame: pd.DataFrame) -> None:
    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        missing_text = ", ".join(sorted(missing))
        raise ValueError(
            "Normalized frame missing required columns: "
            f"{missing_text}"
        )

    timestamp_series = frame[TIMESTAMP_COLUMN]
    if not isinstance(timestamp_series.dtype, pd.DatetimeTZDtype):
        raise ValueError("timestamp_utc must be timezone-aware")

    timezone = str(timestamp_series.dt.tz)
    if timezone != "UTC":
        raise ValueError("timestamp_utc must be UTC-based")


def build_aggregation_map(
    frame: pd.DataFrame,
    policy: AggregationPolicy = DEFAULT_POLICY,
) -> dict[str, str]:
    aggregation_map: dict[str, str] = {}
    skipped_columns = REQUIRED_COLUMNS | {"source_file", "schema_family"}
    skipped_unknown: list[str] = []

    for column in frame.columns:
        if column in skipped_columns:
            continue
        try:
            aggregation_map[column] = policy.for_column(column)
        except KeyError:
            skipped_unknown.append(column)

    if skipped_unknown:
        import logging
        logger = logging.getLogger(__name__)
        logger.debug(
            "Skipped columns with no aggregation rule: %s",
            ", ".join(sorted(skipped_unknown)),
        )

    return aggregation_map


def _prepare_resample_frame(frame: pd.DataFrame) -> pd.DataFrame:
    validate_normalized_frame(frame)
    return frame.sort_values([STATION_COLUMN, TIMESTAMP_COLUMN]).copy()


def resample_normalized_frame(
    frame: pd.DataFrame,
    frequency: str,
    policy: AggregationPolicy = DEFAULT_POLICY,
) -> pd.DataFrame:
    if frequency not in SUPPORTED_FREQUENCIES:
        allowed = ", ".join(sorted(SUPPORTED_FREQUENCIES))
        raise ValueError(
            f"Unsupported frequency '{frequency}'. Expected one of: {allowed}"
        )

    prepared = _prepare_resample_frame(frame)
    aggregation_map = build_aggregation_map(prepared, policy=policy)

    grouped = prepared.groupby(
        [
            STATION_COLUMN,
            pd.Grouper(
                key=TIMESTAMP_COLUMN,
                freq=SUPPORTED_FREQUENCIES[frequency],
                label="left",
                closed="left",
            ),
        ],
        sort=True,
        dropna=False,
    )

    resampled = grouped.agg(aggregation_map).reset_index()
    ordered_columns = [
        STATION_COLUMN,
        TIMESTAMP_COLUMN,
        *aggregation_map.keys(),
    ]
    return resampled.loc[:, ordered_columns]


def resample_hourly(
    frame: pd.DataFrame,
    policy: AggregationPolicy = DEFAULT_POLICY,
) -> pd.DataFrame:
    return resample_normalized_frame(frame, frequency="hourly", policy=policy)


def resample_daily(
    frame: pd.DataFrame,
    policy: AggregationPolicy = DEFAULT_POLICY,
) -> pd.DataFrame:
    return resample_normalized_frame(frame, frequency="daily", policy=policy)

