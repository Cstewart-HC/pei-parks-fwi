"""Tests for QA/QC summary functions."""

from __future__ import annotations

import pandas as pd
import pytest

from pea_met_network.qa_qc import (
    coverage_summary,
    duplicate_timestamps,
    missingness_summary,
    out_of_range_values,
)


@pytest.fixture
def sample_frame() -> pd.DataFrame:
    return pd.DataFrame({
        "station": ["a", "a", "a", "b", "b", "b"],
        "timestamp_utc": pd.date_range(
            "2024-01-01", periods=6, freq="h", tz="UTC"
        ),
        "air_temperature_c": [10.0, float("nan"), 12.0, -70.0, 15.0, 16.0],
        "relative_humidity_pct": [50.0, 55.0, 110.0, 60.0, 65.0, 70.0],
        "rain_mm": [0.0, 0.0, 0.5, 0.0, 0.0, -1.0],
    })


class TestMissingnessSummary:
    def test_detects_missing_values(self, sample_frame: pd.DataFrame) -> None:
        df = missingness_summary(sample_frame)
        assert len(df) > 0
        temp_row = df[df["variable"] == "air_temperature_c"]
        assert temp_row.iloc[0]["missing_count"] == 1

    def test_no_missing(self) -> None:
        frame = pd.DataFrame({
            "station": ["a", "a"],
            "timestamp_utc": pd.date_range(
                "2024-01-01", periods=2, freq="h", tz="UTC"
            ),
            "temp": [10.0, 11.0],
        })
        df = missingness_summary(frame)
        assert df.iloc[0]["missing_count"] == 0


class TestDuplicateTimestamps:
    def test_detects_duplicates(self) -> None:
        frame = pd.DataFrame({
            "station": ["a", "a", "b"],
            "timestamp_utc": [
                pd.Timestamp("2024-01-01", tz="UTC"),
                pd.Timestamp("2024-01-01", tz="UTC"),
                pd.Timestamp("2024-01-02", tz="UTC"),
            ],
            "temp": [10.0, 12.0, 15.0],
        })
        dupes = duplicate_timestamps(frame)
        assert len(dupes) == 2
        assert set(dupes["station"]) == {"a"}

    def test_no_duplicates(self, sample_frame: pd.DataFrame) -> None:
        dupes = duplicate_timestamps(sample_frame)
        assert len(dupes) == 0


class TestOutOfRangeValues:
    def test_detects_out_of_range(self, sample_frame: pd.DataFrame) -> None:
        oov = out_of_range_values(sample_frame)
        assert len(oov) > 0
        # temp -70 is below -60
        assert "air_temperature_c" in oov["oov_column"].values
        # rh 110 is above 105
        assert "relative_humidity_pct" in oov["oov_column"].values
        # rain -1 is below 0
        assert "rain_mm" in oov["oov_column"].values

    def test_custom_ranges(self) -> None:
        frame = pd.DataFrame({
            "station": ["a"],
            "timestamp_utc": pd.date_range(
                "2024-01-01", periods=1, freq="h", tz="UTC"
            ),
            "temp": [50.0],
        })
        oov = out_of_range_values(frame, ranges={"temp": (0.0, 30.0)})
        assert len(oov) == 1
        assert oov.iloc[0]["oov_value"] == 50.0

    def test_no_oov(self) -> None:
        frame = pd.DataFrame({
            "station": ["a"],
            "timestamp_utc": pd.date_range(
                "2024-01-01", periods=1, freq="h", tz="UTC"
            ),
            "air_temperature_c": [20.0],
        })
        oov = out_of_range_values(frame)
        assert len(oov) == 0


class TestCoverageSummary:
    def test_coverage_counts(self, sample_frame: pd.DataFrame) -> None:
        df = coverage_summary(sample_frame)
        assert len(df) == 2
        assert set(df["station"]) == {"a", "b"}
        assert df.iloc[0]["total_records"] == 3

    def test_missing_station_column_raises(self) -> None:
        frame = pd.DataFrame({"temp": [1.0, 2.0]})
        with pytest.raises(ValueError, match="Station column"):
            coverage_summary(frame)
