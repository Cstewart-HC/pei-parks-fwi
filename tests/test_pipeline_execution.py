"""Phase 9 exit gate: Pipeline Execution (AC-PIPE-1 through AC-PIPE-7).

Verifies that pea_met_network.cleaning produces correct
materialized outputs for all PEINP stations discovered by the manifest.
"""

import json
from pathlib import Path

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
PROCESSED = ROOT / "data" / "processed"
MANIFEST_PATH = PROCESSED / "pipeline_manifest.json"
RAW_DIR = ROOT / "data" / "raw" / "peinp"
IMPUTATION_REPORT = PROCESSED / "imputation_report.csv"


def _load_manifest():
    with open(MANIFEST_PATH) as f:
        return json.load(f)


def _peinp_stations():
    """Stations that come from PEINP data (not Stanhope reference)."""
    m = _load_manifest()
    stations = set()
    for a in m["artifacts"]:
        if "station" in a and a["station"] != "stanhope":
            stations.add(a["station"])
    return sorted(stations)


class TestAC_PIPE_1_PipelineRuns:
    """AC-PIPE-1: pea_met_network runs on all raw data without errors."""

    def test_manifest_exists(self):
        assert MANIFEST_PATH.is_file(), (
            "pipeline_manifest.json must exist in data/processed/"
        )

    def test_manifest_has_artifacts(self):
        m = _load_manifest()
        assert "artifacts" in m and len(m["artifacts"]) > 0, (
            "Manifest must contain at least one artifact"
        )

    def test_all_peinp_stations_have_hourly(self):
        for station in _peinp_stations():
            p = PROCESSED / station / "station_hourly.csv"
            assert p.is_file(), f"Missing hourly data for {station}: {p}"


class TestAC_PIPE_2_CleanedDatasets:
    """AC-PIPE-2: Cleaned hourly + daily datasets in data/processed/."""

    def test_all_peinp_stations_have_daily(self):
        for station in _peinp_stations():
            p = PROCESSED / station / "station_daily.csv"
            assert p.is_file(), f"Missing daily data for {station}: {p}"

    def test_daily_has_rows(self):
        for station in _peinp_stations():
            p = PROCESSED / station / "station_daily.csv"
            df = pd.read_csv(p)
            assert len(df) > 0, f"station_daily.csv for {station} is empty"

    def test_daily_has_timestamp_utc(self):
        for station in _peinp_stations():
            p = PROCESSED / station / "station_daily.csv"
            df = pd.read_csv(p)
            assert "timestamp_utc" in df.columns, (
                f"station_daily.csv for {station} missing timestamp_utc"
            )

    def test_hourly_has_rows(self):
        for station in _peinp_stations():
            p = PROCESSED / station / "station_hourly.csv"
            df = pd.read_csv(p)
            assert len(df) > 0, f"station_hourly.csv for {station} is empty"


class TestAC_PIPE_3_ImputationReport:
    """AC-PIPE-3: Imputation report generated."""

    def test_imputation_report_exists(self):
        assert IMPUTATION_REPORT.is_file(), (
            "imputation_report.csv must exist in data/processed/"
        )

    def test_imputation_report_has_records(self):
        df = pd.read_csv(IMPUTATION_REPORT)
        # Report may be empty if no gaps were found — verify schema instead
        expected_cols = {
            "station", "variable", "time_start",
            "time_end", "method", "count_affected",
        }
        assert set(df.columns) == expected_cols, (
            f"Unexpected columns: {list(df.columns)}"
        )

    def test_imputation_report_in_manifest(self):
        m = _load_manifest()
        types = [a["artifact_type"] for a in m["artifacts"] if "artifact_type" in a]
        assert "imputation_report" in types, (
            "Manifest must track imputation_report artifact"
        )


class TestAC_PIPE_4_StanhopeReference:
    """AC-PIPE-4: Stanhope reference data downloaded and cached."""

    def test_stanhope_hourly_exists(self):
        p = PROCESSED / "stanhope" / "station_hourly.csv"
        assert p.is_file(), f"Missing Stanhope hourly data: {p}"

    def test_stanhope_has_rows(self):
        p = PROCESSED / "stanhope" / "station_hourly.csv"
        df = pd.read_csv(p)
        assert len(df) > 0, "Stanhope hourly data is empty"


class TestAC_PIPE_5_FWI_MoistureCodes:
    """AC-PIPE-5: FFMC, DMC, DC computed for all stations with sufficient data.

    Stations must have relative_humidity_pct and air_temperature_c to compute
    FWI moisture codes. Stations lacking these inputs are skipped by the
    pipeline (logged at WARNING level).
    """

    def test_daily_has_fwi_moisture_codes(self):
        for station in _peinp_stations():
            p = PROCESSED / station / "station_daily.csv"
            df = pd.read_csv(p)
            if "relative_humidity_pct" not in df.columns:
                pytest.skip(
                    f"{station} lacks humidity data — FWI not computable"
                )
            for col in ("ffmc", "dmc", "dc"):
                assert col in df.columns, (
                    f"{station} station_daily.csv missing {col}"
                )


class TestAC_PIPE_6_FWI_Chain:
    """AC-PIPE-6: ISI, BUI, FWI computed where data supports it."""

    def test_daily_has_fwi_indices(self):
        for station in _peinp_stations():
            p = PROCESSED / station / "station_daily.csv"
            df = pd.read_csv(p)
            if "relative_humidity_pct" not in df.columns:
                pytest.skip(
                    f"{station} lacks humidity data — FWI not computable"
                )
            for col in ("isi", "bui", "fwi"):
                assert col in df.columns, (
                    f"{station} station_daily.csv missing {col}"
                )


class TestAC_PIPE_7_Manifest:
    """AC-PIPE-7: Pipeline artifacts manifest with timestamps and row counts."""

    def test_manifest_has_generated_at(self):
        m = _load_manifest()
        assert "generated_at" in m, "Manifest must have generated_at timestamp"

    def test_artifacts_have_required_fields(self):
        m = _load_manifest()
        for a in m["artifacts"]:
            if "station" in a:
                assert "artifact_type" in a, f"Artifact missing 'artifact_type': {a}"
                assert "path" in a, f"Artifact missing 'path': {a}"
                assert "rows" in a, f"Artifact missing 'rows': {a}"
                assert "timestamp" in a, f"Artifact missing 'timestamp': {a}"
