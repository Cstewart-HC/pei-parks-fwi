from __future__ import annotations

import pandas as pd
import pytest

from pea_met_network.resampling import (
    build_aggregation_map,
    resample_daily,
    resample_hourly,
    resample_normalized_frame,
    validate_normalized_frame,
)


def test_validate_normalized_frame_requires_required_columns() -> None:
    frame = pd.DataFrame({"timestamp_utc": []})

    with pytest.raises(ValueError, match="station"):
        validate_normalized_frame(frame)


def test_validate_normalized_frame_requires_utc_timezone() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(
                ["2024-01-01T00:00:00Z"]
            ).tz_convert("America/Halifax"),
            "station": ["cavendish"],
        }
    )

    with pytest.raises(ValueError, match="UTC-based"):
        validate_normalized_frame(frame)


def test_build_aggregation_map_uses_explicit_rules() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(["2024-01-01T00:00:00Z"]),
            "station": ["cavendish"],
            "air_temperature_c": [1.2],
            "relative_humidity_pct": [81.0],
            "rain_mm": [0.4],
            "wind_direction_deg": [270.0],
            "source_file": ["x.csv"],
            "schema_family": ["hoboware_date_time_family"],
        }
    )

    aggregation_map = build_aggregation_map(frame)

    assert aggregation_map == {
        "air_temperature_c": "mean",
        "relative_humidity_pct": "mean",
        "rain_mm": "sum",
        "wind_direction_deg": "first",
    }


def test_build_aggregation_map_skips_unknown_variable() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(["2024-01-01T00:00:00Z"]),
            "station": ["cavendish"],
            "mystery_signal": [1],
        }
    )

    aggregation_map = build_aggregation_map(frame)

    assert "mystery_signal" not in aggregation_map
    assert aggregation_map == {}


def test_resample_hourly_aggregates_by_station_and_hour() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(
                [
                    "2024-01-01T00:05:00Z",
                    "2024-01-01T00:35:00Z",
                    "2024-01-01T01:15:00Z",
                    "2024-01-01T00:10:00Z",
                ]
            ),
            "station": [
                "cavendish",
                "cavendish",
                "cavendish",
                "greenwich",
            ],
            "air_temperature_c": [0.0, 2.0, 4.0, 10.0],
            "rain_mm": [0.2, 0.3, 0.4, 0.1],
            "wind_direction_deg": [90.0, 180.0, 270.0, 45.0],
        }
    )

    result = resample_hourly(frame)

    assert result.to_dict(orient="records") == [
        {
            "station": "cavendish",
            "timestamp_utc": pd.Timestamp("2024-01-01T00:00:00Z"),
            "air_temperature_c": 1.0,
            "rain_mm": 0.5,
            "wind_direction_deg": 90.0,
        },
        {
            "station": "cavendish",
            "timestamp_utc": pd.Timestamp("2024-01-01T01:00:00Z"),
            "air_temperature_c": 4.0,
            "rain_mm": 0.4,
            "wind_direction_deg": 270.0,
        },
        {
            "station": "greenwich",
            "timestamp_utc": pd.Timestamp("2024-01-01T00:00:00Z"),
            "air_temperature_c": 10.0,
            "rain_mm": 0.1,
            "wind_direction_deg": 45.0,
        },
    ]


def test_resample_daily_aggregates_by_station_and_day() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(
                [
                    "2024-01-01T00:05:00Z",
                    "2024-01-01T12:35:00Z",
                    "2024-01-02T01:15:00Z",
                ]
            ),
            "station": ["cavendish", "cavendish", "cavendish"],
            "air_temperature_c": [0.0, 4.0, 10.0],
            "rain_mm": [0.2, 0.8, 1.0],
            "wind_direction_deg": [90.0, 180.0, 270.0],
        }
    )

    result = resample_daily(frame)

    assert result.to_dict(orient="records") == [
        {
            "station": "cavendish",
            "timestamp_utc": pd.Timestamp("2024-01-01T00:00:00Z"),
            "air_temperature_c": 2.0,
            "rain_mm": 1.0,
            "wind_direction_deg": 90.0,
        },
        {
            "station": "cavendish",
            "timestamp_utc": pd.Timestamp("2024-01-02T00:00:00Z"),
            "air_temperature_c": 10.0,
            "rain_mm": 1.0,
            "wind_direction_deg": 270.0,
        },
    ]


def test_resample_normalized_frame_supports_daily_frequency() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(
                [
                    "2024-01-01T00:05:00Z",
                    "2024-01-01T12:35:00Z",
                    "2024-01-02T01:15:00Z",
                ]
            ),
            "station": ["cavendish", "cavendish", "cavendish"],
            "air_temperature_c": [0.0, 4.0, 10.0],
            "rain_mm": [0.2, 0.8, 1.0],
            "wind_direction_deg": [90.0, 180.0, 270.0],
        }
    )

    result = resample_normalized_frame(frame, frequency="daily")

    assert result.to_dict(orient="records") == [
        {
            "station": "cavendish",
            "timestamp_utc": pd.Timestamp("2024-01-01T00:00:00Z"),
            "air_temperature_c": 2.0,
            "rain_mm": 1.0,
            "wind_direction_deg": 90.0,
        },
        {
            "station": "cavendish",
            "timestamp_utc": pd.Timestamp("2024-01-02T00:00:00Z"),
            "air_temperature_c": 10.0,
            "rain_mm": 1.0,
            "wind_direction_deg": 270.0,
        },
    ]


def test_resample_normalized_frame_rejects_unknown_frequency() -> None:
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(["2024-01-01T00:00:00Z"]),
            "station": ["cavendish"],
            "air_temperature_c": [1.0],
        }
    )

    with pytest.raises(ValueError, match="Unsupported frequency"):
        resample_normalized_frame(frame, frequency="weekly")
