"""Tests for imputation framework with audit trail."""

from __future__ import annotations

import pandas as pd

from pea_met_network.imputation import (
    AuditRecord,
    ImputationConfig,
    audit_trail_to_dataframe,
    impute_column,
    impute_frame,
)


class TestImputeColumn:
    def test_no_missing_returns_empty_audit(self) -> None:
        s = pd.Series([1.0, 2.0, 3.0], name="temp")
        result, records = impute_column(s)
        assert result.tolist() == [1.0, 2.0, 3.0]
        assert records == []

    def test_short_gap_interpolated(self) -> None:
        s = pd.Series([10.0, float("nan"), 20.0], name="temp")
        config = ImputationConfig(short_gap_max_hours=3)
        result, records = impute_column(s, config)
        assert result.isna().sum() == 0
        assert abs(result.iloc[1] - 15.0) < 0.01
        assert len(records) == 1
        assert records[0].method == "interpolate"
        assert records[0].count_affected == 1

    def test_long_gap_preserved(self) -> None:
        s = pd.Series([10.0] + [float("nan")] * 5 + [20.0], name="temp")
        config = ImputationConfig(short_gap_max_hours=2, medium_gap_max_hours=4)
        result, records = impute_column(s, config)
        assert result.iloc[1:6].isna().all()
        assert len(records) == 1
        assert records[0].method == "preserve"
        assert records[0].count_affected == 5

    def test_audit_record_fields(self) -> None:
        s = pd.Series([1.0, float("nan"), 3.0], name="humidity")
        _, records = impute_column(s)
        r = records[0]
        assert isinstance(r, AuditRecord)
        assert r.variable == "humidity"
        assert isinstance(r.count_affected, int)
        assert isinstance(r.method, str)
        assert isinstance(r.time_start, str)
        assert isinstance(r.time_end, str)

    def test_ffill_strategy(self) -> None:
        s = pd.Series([10.0, float("nan"), float("nan"), 20.0], name="temp")
        config = ImputationConfig(
            short_gap_max_hours=5, short_gap_method="ffill"
        )
        result, records = impute_column(s, config)
        assert result.iloc[1] == 10.0
        assert result.iloc[2] == 10.0


class TestImputeFrame:
    def test_imputes_all_numeric_columns(self) -> None:
        frame = pd.DataFrame({
            "station": ["a", "a", "a"],
            "timestamp_utc": pd.date_range(
                "2024-01-01", periods=3, freq="h", tz="UTC"
            ),
            "temp": [10.0, float("nan"), 20.0],
            "rh": [50.0, float("nan"), 60.0],
        })
        result, records = impute_frame(frame)
        assert result["temp"].isna().sum() == 0
        assert result["rh"].isna().sum() == 0
        assert len(records) == 2  # one per variable

    def test_per_station_audit(self) -> None:
        frame = pd.DataFrame({
            "station": ["a", "a", "b", "b"],
            "timestamp_utc": pd.date_range(
                "2024-01-01", periods=4, freq="h", tz="UTC"
            ),
            "temp": [10.0, float("nan"), 20.0, float("nan")],
        })
        result, records = impute_frame(frame)
        stations = {r.station for r in records}
        assert "a" in stations
        assert "b" in stations


class TestAuditTrailToDataFrame:
    def test_empty_records(self) -> None:
        df = audit_trail_to_dataframe([])
        assert list(df.columns) == [
            "station", "variable", "time_start", "time_end",
            "method", "count_affected",
        ]
        assert len(df) == 0

    def test_records_to_dataframe(self) -> None:
        records = [
            AuditRecord(
                station="cavendish", variable="temp",
                time_start="2024-01-01 02:00", time_end="2024-01-01 04:00",
                method="interpolate", count_affected=3,
            ),
        ]
        df = audit_trail_to_dataframe(records)
        assert len(df) == 1
        assert df.iloc[0]["station"] == "cavendish"
        assert df.iloc[0]["count_affected"] == 3
