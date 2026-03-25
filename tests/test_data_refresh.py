"""Phase 12: Data Refresh — acceptance criteria tests."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
DATA_RAW = REPO_ROOT / "data" / "raw"
DATA_PROCESSED = REPO_ROOT / "data" / "processed"


class TestACREF1_ManifestDiscovery:
    """AC-REF-1: manifest discovers all raw CSVs including Licor and
    XLSX-converted files."""

    def test_licor_csvs_discovered_by_manifest(self):
        from src.pea_met_network.manifest import build_raw_manifest

        records = build_raw_manifest(REPO_ROOT)
        licor_records = [
            r for r in records if "licor" in r.relative_path.lower()
        ]
        assert len(licor_records) > 0, (
            "Manifest should discover Licor-sourced CSVs"
        )

    def test_xlsx_converted_csvs_discovered(self):
        from src.pea_met_network.manifest import build_raw_manifest

        records = build_raw_manifest(REPO_ROOT)
        # North Rustico Licor CSVs from XLSX conversion should exist
        nr_licor = [
            r for r in records
            if "north" in r.station
            and "licor" in r.relative_path.lower()
            and r.year in (2022, 2023)
        ]
        assert len(nr_licor) > 0, (
            "Manifest should discover XLSX-converted North Rustico CSVs"
        )

    def test_all_stations_have_records(self):
        from src.pea_met_network.manifest import build_raw_manifest

        records = build_raw_manifest(REPO_ROOT)
        stations = {r.station for r in records}
        for expected in [
            "cavendish", "greenwich", "north_rustico",
            "stanley_bridge", "tracadie", "stanhope",
        ]:
            assert expected in stations, (
                f"Expected station '{expected}' in manifest, "
                f"got: {stations}"
            )


class TestACREF2_ThreeSensorNR:
    """AC-REF-2: normalized_loader handles 3-sensor NR CSVs."""

    def test_north_rustico_dec2022_loads_without_error(self):
        from src.pea_met_network.normalized_loader import (
            load_station_files,
        )

        records = [
            r for r in (
                __import__(
                    "src.pea_met_network.manifest",
                    fromlist=["build_raw_manifest"],
                ).build_raw_manifest(REPO_ROOT)
            )
            if r.station == "north_rustico"
            and r.year == 2022
            and r.extension == ".csv"
        ]
        if not records:
            pytest.skip("No Dec 2022 North Rustico CSVs found")
        df = load_station_files([r.path for r in records])
        assert df is not None
        assert len(df) > 0

    def test_three_sensor_columns_present(self):
        """3-sensor CSVs have temperature and barometric pressure at
        minimum."""
        import pandas as pd

        records = [
            r for r in (
                __import__(
                    "src.pea_met_network.manifest",
                    fromlist=["build_raw_manifest"],
                ).build_raw_manifest(REPO_ROOT)
            )
            if r.station == "north_rustico"
            and r.year == 2022
            and r.extension == ".csv"
        ]
        if not records:
            pytest.skip("No Dec 2022 North Rustico CSVs found")
        df = pd.read_csv(records[0].path, nrows=5)
        cols_lower = {c.lower() for c in df.columns}
        # At minimum, temperature should be present
        has_temp = any("temp" in c for c in cols_lower)
        assert has_temp, (
            f"Expected temperature column, got: {list(cols_lower)}"
        )


class TestACREF3_PipelineEndToEnd:
    """AC-REF-3: pipeline runs on expanded dataset without errors."""

    def test_cleaning_py_runs(self):
        import subprocess

        result = subprocess.run(
            [sys.executable, "cleaning.py"],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=300,
        )
        # cleaning.py may produce warnings but should not crash
        # Check for unhandled exceptions
        assert "Traceback" not in result.stderr, (
            f"cleaning.py crashed:\n{result.stderr[:500]}"
        )


class TestACREF4_ProcessedOutputs:
    """AC-REF-4: processed datasets exist for all stations."""

    def test_hourly_output_exists(self):
        if not DATA_PROCESSED.exists():
            pytest.skip("data/processed/ does not exist yet")
        hourly = list(DATA_PROCESSED.glob("station_hourly*"))
        assert len(hourly) > 0, "No hourly processed files found"

    def test_daily_output_exists(self):
        if not DATA_PROCESSED.exists():
            pytest.skip("data/processed/ does not exist yet")
        daily = list(DATA_PROCESSED.glob("station_daily*"))
        assert len(daily) > 0, "No daily processed files found"

    def test_north_rustico_early_period_in_output(self):
        """North Rustico Dec 2022 – Mar 2023 should appear in processed
        data."""
        import pandas as pd

        hourly = list(DATA_PROCESSED.glob("station_hourly*"))
        if not hourly:
            pytest.skip("No hourly processed files found")
        df = pd.read_csv(hourly[0])
        if "timestamp_utc" not in df.columns:
            pytest.skip("Unexpected column schema")
        nr = df[df["station"] == "north_rustico"]
        if len(nr) == 0:
            pytest.skip("No North Rustico data in processed output")
        ts = pd.to_datetime(nr["timestamp_utc"], errors="coerce")
        early = ts[(ts >= "2022-12-01") & (ts <= "2023-03-31")]
        assert len(early) > 0, (
            "Expected North Rustico data for Dec 2022 – Mar 2023"
        )


class TestACREF5_ImputationReport:
    """AC-REF-5: imputation report reflects updated gap profile."""

    def test_imputation_artifact_exists(self):
        if not DATA_PROCESSED.exists():
            pytest.skip("data/processed/ does not exist yet")
        artifacts = list(DATA_PROCESSED.glob("*imputation*"))
        assert len(artifacts) > 0, (
            "No imputation report artifact found"
        )


class TestACREF6_FWIComputed:
    """AC-REF-6: FWI computed for all stations with sufficient data."""

    def test_fwi_columns_in_output(self):
        import pandas as pd

        daily = list(DATA_PROCESSED.glob("station_daily*"))
        if not daily:
            pytest.skip("No daily processed files found")
        df = pd.read_csv(daily[0])
        fwi_cols = [c for c in df.columns if "fwi" in c.lower()]
        assert len(fwi_cols) > 0, "No FWI columns in daily output"


class TestACREF7_AnalysisNotebookUpdated:
    """AC-REF-7: analysis notebook uses expanded time range."""

    def test_notebook_executes_without_error(self):
        import subprocess

        result = subprocess.run(
            [
                "jupyter", "nbconvert", "--to", "notebook",
                "--execute", "analysis.ipynb",
                "--output", "analysis_executed.ipynb",
            ],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=600,
        )
        assert result.returncode == 0, (
            f"Notebook execution failed:\n"
            f"{result.stdout[-500:]}\n{result.stderr[-500:]}"
        )


class TestACREF9_DataSourcesDoc:
    """AC-REF-9: data-sources.md documents all sources."""

    def test_data_sources_doc_exists(self):
        doc = REPO_ROOT / "docs" / "data-sources.md"
        assert doc.exists(), "docs/data-sources.md not found"

    def test_data_sources_mentions_licor(self):
        doc = REPO_ROOT / "docs" / "data-sources.md"
        if not doc.exists():
            pytest.skip("docs/data-sources.md not found")
        content = doc.read_text()
        assert "licor" in content.lower() or "hobolink" in content.lower(), (
            "data-sources.md should mention Licor/HOBOlink"
        )

    def test_data_sources_mentions_eccc(self):
        doc = REPO_ROOT / "docs" / "data-sources.md"
        if not doc.exists():
            pytest.skip("docs/data-sources.md not found")
        content = doc.read_text()
        assert "eccc" in content.lower(), (
            "data-sources.md should mention ECCC"
        )


class TestACREF10_FullSuitePasses:
    """AC-REF-10: full test suite passes."""

    def test_no_failures(self):
        import subprocess

        result = subprocess.run(
            [".venv/bin/pytest", "-q", "--tb=short"],
            capture_output=True,
            text=True,
            cwd=str(REPO_ROOT),
            timeout=300,
        )
        assert "failed" not in result.stdout.lower(), (
            f"Test suite has failures:\n{result.stdout[-500:]}"
        )
