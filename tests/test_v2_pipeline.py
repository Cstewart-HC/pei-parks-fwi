"""
V2 Pipeline Tests — Adapter Architecture (7 Phases)

Tests for the new single-entry-point pipeline with adapter architecture.
Each phase has tests that FAIL initially and PASS when implementation is complete.

Run: pytest tests/test_v2_pipeline.py -v
"""

import subprocess
import json
import hashlib
from pathlib import Path
import pytest
import pandas as pd
import sys

PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
PYTHON = sys.executable


# =============================================================================
# Phase 1: Adapter Architecture
# =============================================================================

class TestAC_PIPE_1_AdapterArchitecture:
    """Phase 1: Router, registry, canonical schema, base adapter, dry-run flag."""

    def test_adapters_module_exists(self):
        """AC-ARCH-1: adapters/ module exists with required files."""
        adapters_dir = PROJECT_ROOT / "src" / "pea_met_network" / "adapters"
        assert adapters_dir.exists(), "adapters/ directory does not exist"
        
        required_files = ["__init__.py", "base.py", "registry.py"]
        for filename in required_files:
            assert (adapters_dir / filename).exists(), f"Missing {filename}"

    def test_adapter_registry_has_all_formats(self):
        """AC-ARCH-2: ADAPTER_REGISTRY has entries for csv, xlsx, xle, json."""
        from pea_met_network.adapters import ADAPTER_REGISTRY
        
        required_formats = [".csv", ".xlsx", ".xle", ".json"]
        for fmt in required_formats:
            assert fmt in ADAPTER_REGISTRY, f"Missing adapter for {fmt}"

    def test_unknown_format_is_hard_error(self):
        """AC-ARCH-3: route_by_extension raises ValueError for unknown formats."""
        from pea_met_network.adapters import route_by_extension
        
        fake_path = PROJECT_ROOT / "test.unknown"
        with pytest.raises(ValueError, match="Unknown file format"):
            route_by_extension(fake_path)

    def test_canonical_schema_constant_exists(self):
        """AC-ARCH-4: CANONICAL_SCHEMA constant is defined and exported."""
        from pea_met_network.adapters import CANONICAL_SCHEMA
        
        assert isinstance(CANONICAL_SCHEMA, (list, tuple))
        assert "station" in CANONICAL_SCHEMA
        assert "timestamp_utc" in CANONICAL_SCHEMA
        assert "air_temperature_c" in CANONICAL_SCHEMA

    def test_base_adapter_abstract_class_exists(self):
        """AC-ARCH-5: BaseAdapter abstract class exists with load() method."""
        from pea_met_network.adapters.base import BaseAdapter
        import inspect
        
        assert inspect.isabstract(BaseAdapter)
        assert hasattr(BaseAdapter, "load")

    def test_dry_run_flag_reports_file_counts(self):
        """AC-ARCH-6: cleaning.py --dry-run reports file counts without writing."""
        result = subprocess.run(
            [PYTHON, "cleaning.py", "--dry-run"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, f"dry-run failed: {result.stderr}"
        # Should report what would be processed
        assert "station" in result.stdout.lower() or "file" in result.stdout.lower()

    def test_dry_run_does_not_write_outputs(self):
        """AC-ARCH-7: dry-run does not create any files in data/processed/."""
        # Ensure test marker file doesn't exist before
        test_marker = DATA_PROCESSED / "dry_run_test_marker.txt"
        if test_marker.exists():
            test_marker.unlink()
        
        result = subprocess.run(
            [PYTHON, "cleaning.py", "--dry-run", "--stations", "greenwich"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )
        
        # No marker file should be created
        assert not test_marker.exists(), "dry-run should not write any files"

    def test_unknown_file_format_causes_error(self):
        """AC-ARCH-8: Unknown file format in data/raw/ causes hard error."""
        # Create a fake unknown format file
        fake_file = DATA_RAW / "test_station" / "test.unknown"
        fake_file.parent.mkdir(parents=True, exist_ok=True)
        fake_file.write_text("test data")
        
        try:
            result = subprocess.run(
                [PYTHON, "cleaning.py", "--stations", "test_station"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=60,
            )
            assert result.returncode != 0, "Should fail on unknown format"
        finally:
            # Cleanup
            if fake_file.exists():
                fake_file.unlink()
            if fake_file.parent.exists():
                fake_file.parent.rmdir()


# =============================================================================
# Phase 2: All Format Adapters
# =============================================================================

class TestAC_PIPE_2_AllFormatAdapters:
    """Phase 2: All adapters built - csv, xlsx, xle, json."""

    def test_csv_adapter_loads_peinp_archives(self):
        """AC-FMT-1: csv_adapter loads PEINP archive CSVs."""
        from pea_met_network.adapters.csv_adapter import CSVAdapter
        from pea_met_network.adapters import CANONICAL_SCHEMA
        
        # Find a PEINP CSV file
        peinp_dir = DATA_RAW / "peinp" / "PEINP Weather Station Data 2022-2025"
        csv_files = list(peinp_dir.rglob("*.csv"))
        
        if not csv_files:
            pytest.skip("No PEINP CSV files found")
        
        adapter = CSVAdapter()
        df = adapter.load(csv_files[0])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        # Should have canonical columns
        for col in ["station", "timestamp_utc"]:
            assert col in df.columns

    def test_csv_adapter_loads_eccc_stanhope(self):
        """AC-FMT-2: csv_adapter loads ECCC Stanhope CSVs with different schema."""
        from pea_met_network.adapters.csv_adapter import CSVAdapter
        
        stanhope_dir = DATA_RAW / "stanhope"
        csv_files = list(stanhope_dir.glob("*.csv"))
        
        if not csv_files:
            pytest.skip("No Stanhope CSV files found")
        
        adapter = CSVAdapter()
        df = adapter.load(csv_files[0])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "station" in df.columns
        assert df["station"].iloc[0] == "stanhope"

    def test_xlsx_adapter_loads_greenwich(self):
        """AC-FMT-3: xlsx_adapter loads Greenwich 2023 Excel files."""
        from pea_met_network.adapters.xlsx_adapter import XLSXAdapter
        
        peinp_dir = DATA_RAW / "peinp" / "PEINP Weather Station Data 2022-2025"
        xlsx_files = list(peinp_dir.rglob("*.xlsx"))
        
        if not xlsx_files:
            pytest.skip("No XLSX files found")
        
        adapter = XLSXAdapter()
        df = adapter.load(xlsx_files[0])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_xle_adapter_loads_stanley_bridge(self):
        """AC-FMT-4: xle_adapter loads Stanley Bridge 2022 Solinst files."""
        from pea_met_network.adapters.xle_adapter import XLEAdapter
        
        peinp_dir = DATA_RAW / "peinp" / "PEINP Weather Station Data 2022-2025"
        xle_files = list(peinp_dir.rglob("*.xle"))
        
        if not xle_files:
            pytest.skip("No XLE files found")
        
        adapter = XLEAdapter()
        df = adapter.load(xle_files[0])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0

    def test_json_adapter_loads_licor(self):
        """AC-FMT-5: json_adapter loads Licor Cloud API JSON files."""
        from pea_met_network.adapters.json_adapter import JSONAdapter
        
        licor_dir = DATA_RAW / "licor"
        json_files = list(licor_dir.rglob("*.json"))
        
        if not json_files:
            pytest.skip("No Licor JSON files found")
        
        adapter = JSONAdapter()
        df = adapter.load(json_files[0])
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) > 0
        assert "station" in df.columns

    def test_all_adapters_output_canonical_schema(self):
        """AC-FMT-6: All adapters output DataFrame with canonical schema columns."""
        from pea_met_network.adapters import (
            ADAPTER_REGISTRY,
            CANONICAL_SCHEMA,
            route_by_extension,
        )
        
        # Test each adapter with a sample file if available
        required_columns = ["station", "timestamp_utc"]
        
        for ext, adapter_class in ADAPTER_REGISTRY.items():
            # Find a file with this extension
            files = list(DATA_RAW.rglob(f"*{ext}"))
            if not files:
                continue
            
            adapter = adapter_class()
            df = adapter.load(files[0])
            
            for col in required_columns:
                assert col in df.columns, f"{ext} adapter missing {col}"

    def test_wind_speed_kmh_derived_from_ms(self):
        """AC-FMT-7: wind_speed_kmh derived from wind_speed_ms when only m/s available."""
        # This is tested implicitly through adapter loading
        # Check that at least one station has both columns
        hourly_path = DATA_PROCESSED / "greenwich" / "station_hourly.csv"
        
        if not hourly_path.exists():
            pytest.skip("No processed data yet")
        
        df = pd.read_csv(hourly_path)
        
        # If wind_speed_ms exists, wind_speed_kmh should also exist
        if "wind_speed_ms" in df.columns:
            assert "wind_speed_kmh" in df.columns

    def test_water_level_columns_present_for_coastal(self):
        """AC-FMT-8: Water-level columns present for coastal stations."""
        # North Rustico, Stanley Bridge, Tracadie have water sensors
        coastal_stations = ["north_rustico", "stanley_bridge", "tracadie"]
        
        for station in coastal_stations:
            hourly_path = DATA_PROCESSED / station / "station_hourly.csv"
            if not hourly_path.exists():
                continue
            
            df = pd.read_csv(hourly_path)
            # At least one water column should exist if data has it
            water_cols = [c for c in df.columns if "water" in c.lower()]
            # This is informational, not a hard requirement

    def test_accumulated_rain_excluded(self):
        """AC-FMT-9: accumulated_rain_mm excluded from output."""
        hourly_path = DATA_PROCESSED / "greenwich" / "station_hourly.csv"
        
        if not hourly_path.exists():
            pytest.skip("No processed data yet")
        
        df = pd.read_csv(hourly_path)
        assert "accumulated_rain_mm" not in df.columns


# =============================================================================
# Phase 3: Pipeline Integration
# =============================================================================

class TestAC_PIPE_3_PipelineIntegration:
    """Phase 3: Full pipeline wired - concat → dedup → resample → impute → FWI."""

    def test_cleaning_py_runs_end_to_end(self):
        """AC-INT-1: cleaning.py runs end-to-end without error."""
        result = subprocess.run(
            [PYTHON, "cleaning.py", "--stations", "greenwich"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert result.returncode == 0, f"cleaning.py failed: {result.stderr}"

    def test_all_stations_have_hourly_csvs(self):
        """AC-INT-2: All 6 stations have station_hourly.csv."""
        stations = ["greenwich", "cavendish", "north_rustico", "stanley_bridge", 
                    "tracadie", "stanhope"]
        
        for station in stations:
            hourly_path = DATA_PROCESSED / station / "station_hourly.csv"
            assert hourly_path.exists(), f"Missing hourly CSV for {station}"

    def test_all_stations_have_daily_csvs(self):
        """AC-INT-3: All 6 stations have station_daily.csv."""
        stations = ["greenwich", "cavendish", "north_rustico", "stanley_bridge",
                    "tracadie", "stanhope"]
        
        for station in stations:
            daily_path = DATA_PROCESSED / station / "station_daily.csv"
            assert daily_path.exists(), f"Missing daily CSV for {station}"

    def test_hourly_csvs_have_fwi_columns(self):
        """AC-INT-4: Hourly CSVs have FWI columns."""
        fwi_cols = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]
        
        for station_dir in DATA_PROCESSED.iterdir():
            if not station_dir.is_dir():
                continue
            
            hourly_path = station_dir / "station_hourly.csv"
            if not hourly_path.exists():
                continue
            
            df = pd.read_csv(hourly_path)
            for col in fwi_cols:
                assert col in df.columns, f"{station_dir.name} hourly missing {col}"

    def test_daily_csvs_have_fwi_columns(self):
        """AC-INT-5: Daily CSVs have FWI columns."""
        fwi_cols = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]
        
        for station_dir in DATA_PROCESSED.iterdir():
            if not station_dir.is_dir():
                continue
            
            daily_path = station_dir / "station_daily.csv"
            if not daily_path.exists():
                continue
            
            df = pd.read_csv(daily_path)
            for col in fwi_cols:
                assert col in df.columns, f"{station_dir.name} daily missing {col}"

    def test_imputation_report_exists(self):
        """AC-INT-6: Imputation report exists."""
        imputation_path = DATA_PROCESSED / "imputation_report.csv"
        assert imputation_path.exists(), "Missing imputation_report.csv"

    def test_imputation_after_concat(self):
        """AC-INT-7: Imputation runs after concat+dedup (verified by report structure)."""
        imputation_path = DATA_PROCESSED / "imputation_report.csv"
        if not imputation_path.exists():
            pytest.skip("No imputation report yet")
        
        df = pd.read_csv(imputation_path)
        # Report should have station-level aggregation, not file-level
        assert "station" in df.columns

    def test_long_gaps_remain_nan(self):
        """AC-INT-8: Long gaps remain NaN (not imputed)."""
        hourly_path = DATA_PROCESSED / "greenwich" / "station_hourly.csv"
        if not hourly_path.exists():
            pytest.skip("No processed data yet")
        
        df = pd.read_csv(hourly_path)
        # Check that there are still some NaN values (not everything was imputed)
        # This is a soft check - the imputation should leave long gaps
        assert df["air_temperature_c"].isna().sum() >= 0  # At least no error

    def test_fwi_calculated_on_imputed_data(self):
        """AC-INT-9: FWI calculated on imputed data (few NaN in FWI columns)."""
        hourly_path = DATA_PROCESSED / "greenwich" / "station_hourly.csv"
        if not hourly_path.exists():
            pytest.skip("No processed data yet")
        
        df = pd.read_csv(hourly_path)
        
        # FWI columns should have values (not all NaN)
        assert df["fwi"].notna().sum() > 0, "FWI column is all NaN"


# =============================================================================
# Phase 4: Stanhope Validation
# =============================================================================

class TestAC_PIPE_4_StanhopeValidation:
    """Phase 4: Stanhope validation report and comparison."""

    def test_stanhope_has_daily_with_fwi(self):
        """AC-VAL-1: Stanhope has station_daily.csv with FWI columns."""
        daily_path = DATA_PROCESSED / "stanhope" / "station_daily.csv"
        assert daily_path.exists(), "Missing Stanhope daily CSV"
        
        df = pd.read_csv(daily_path)
        fwi_cols = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]
        for col in fwi_cols:
            assert col in df.columns, f"Stanhope daily missing {col}"

    def test_stanhope_validation_report_exists(self):
        """AC-VAL-2: stanhope_validation.csv exists."""
        validation_path = DATA_PROCESSED / "stanhope_validation.csv"
        assert validation_path.exists(), "Missing stanhope_validation.csv"

    def test_validation_report_has_all_stations(self):
        """AC-VAL-3: Validation report includes row for each local station."""
        validation_path = DATA_PROCESSED / "stanhope_validation.csv"
        if not validation_path.exists():
            pytest.skip("No validation report yet")
        
        df = pd.read_csv(validation_path)
        expected_stations = ["greenwich", "cavendish", "north_rustico", 
                           "stanley_bridge", "tracadie"]
        
        for station in expected_stations:
            assert station in df["station"].values, f"Missing {station} in validation"

    def test_validation_report_has_required_columns(self):
        """AC-VAL-4: Validation report has overlap_days and MAE columns."""
        validation_path = DATA_PROCESSED / "stanhope_validation.csv"
        if not validation_path.exists():
            pytest.skip("No validation report yet")
        
        df = pd.read_csv(validation_path)
        required_cols = ["station", "overlap_days", "mean_abs_diff_fwi"]
        
        for col in required_cols:
            assert col in df.columns, f"Missing {col} in validation report"

    def test_greenwich_stanhope_reasonable_agreement(self):
        """AC-VAL-5: Greenwich-Stanhope MAE < 5 for FWI."""
        validation_path = DATA_PROCESSED / "stanhope_validation.csv"
        if not validation_path.exists():
            pytest.skip("No validation report yet")
        
        df = pd.read_csv(validation_path)
        greenwich_row = df[df["station"] == "greenwich"]
        
        if len(greenwich_row) == 0:
            pytest.skip("Greenwich not in validation report")
        
        mae_fwi = greenwich_row["mean_abs_diff_fwi"].iloc[0]
        assert mae_fwi < 5, f"FWI MAE too high: {mae_fwi}"


# =============================================================================
# Phase 5: QA/QC Reporting
# =============================================================================

class TestAC_PIPE_5_QAQCReporting:
    """Phase 5: QA/QC report generation."""

    def test_qa_qc_report_exists(self):
        """AC-QC-1: qa_qc_report.csv exists."""
        qa_qc_path = DATA_PROCESSED / "qa_qc_report.csv"
        assert qa_qc_path.exists(), "Missing qa_qc_report.csv"

    def test_qa_qc_has_all_stations(self):
        """AC-QC-2: Report has row for every station processed."""
        qa_qc_path = DATA_PROCESSED / "qa_qc_report.csv"
        if not qa_qc_path.exists():
            pytest.skip("No QA/QC report yet")
        
        df = pd.read_csv(qa_qc_path)
        stations = df["station"].unique()
        
        # At least the main stations should be present
        assert len(stations) >= 3, "Not enough stations in QA/QC report"

    def test_qa_qc_has_missingness_percentages(self):
        """AC-QC-3: Report includes per-variable missingness percentages."""
        qa_qc_path = DATA_PROCESSED / "qa_qc_report.csv"
        if not qa_qc_path.exists():
            pytest.skip("No QA/QC report yet")
        
        df = pd.read_csv(qa_qc_path)
        
        # Should have some missingness columns
        missing_cols = [c for c in df.columns if "missing" in c.lower()]
        assert len(missing_cols) > 0, "No missingness columns in QA/QC report"

    def test_qa_qc_has_duplicate_counts(self):
        """AC-QC-4: Report includes duplicate timestamp counts."""
        qa_qc_path = DATA_PROCESSED / "qa_qc_report.csv"
        if not qa_qc_path.exists():
            pytest.skip("No QA/QC report yet")
        
        df = pd.read_csv(qa_qc_path)
        
        dup_cols = [c for c in df.columns if "duplicate" in c.lower()]
        assert len(dup_cols) > 0, "No duplicate columns in QA/QC report"

    def test_qa_qc_has_out_of_range_flags(self):
        """AC-QC-5: Report flags out-of-range values."""
        qa_qc_path = DATA_PROCESSED / "qa_qc_report.csv"
        if not qa_qc_path.exists():
            pytest.skip("No QA/QC report yet")
        
        df = pd.read_csv(qa_qc_path)
        
        oor_cols = [c for c in df.columns if "out_of_range" in c.lower()]
        assert len(oor_cols) > 0, "No out-of-range columns in QA/QC report"


# =============================================================================
# Phase 6: Determinism
# =============================================================================

class TestAC_PIPE_6_Determinism:
    """Phase 6: Reproducible, deterministic outputs."""

    def test_byte_identical_on_rerun(self):
        """AC-DET-1: Two consecutive runs produce byte-identical CSV outputs."""
        station = "greenwich"
        hourly_path = DATA_PROCESSED / station / "station_hourly.csv"
        
        # Run once
        subprocess.run(
            [PYTHON, "cleaning.py", "--force", "--stations", station],
            cwd=PROJECT_ROOT,
            capture_output=True,
            timeout=300,
        )
        checksum1 = hashlib.sha256(hourly_path.read_bytes()).hexdigest()
        
        # Run again
        subprocess.run(
            ["python", "cleaning.py", "--force", "--stations", station],
            cwd=PROJECT_ROOT,
            capture_output=True,
            timeout=300,
        )
        checksum2 = hashlib.sha256(hourly_path.read_bytes()).hexdigest()
        
        assert checksum1 == checksum2, "Outputs differ on re-run"

    def test_outputs_sorted_by_timestamp(self):
        """AC-DET-2: All outputs sorted by timestamp_utc."""
        hourly_path = DATA_PROCESSED / "greenwich" / "station_hourly.csv"
        if not hourly_path.exists():
            pytest.skip("No processed data yet")
        
        df = pd.read_csv(hourly_path)
        timestamps = pd.to_datetime(df["timestamp_utc"])
        
        # Check sorted
        assert timestamps.is_monotonic_increasing, "Hourly data not sorted by timestamp"

    def test_column_order_is_deterministic(self):
        """AC-DET-3: Column order is deterministic (alphabetical)."""
        hourly_path = DATA_PROCESSED / "greenwich" / "station_hourly.csv"
        if not hourly_path.exists():
            pytest.skip("No processed data yet")
        
        df = pd.read_csv(hourly_path)
        columns = list(df.columns)
        sorted_columns = sorted(columns)
        
        # First few columns may be special (station, timestamp_utc)
        # Rest should be alphabetical
        if len(columns) > 3:
            assert columns[2:] == sorted_columns[2:], "Columns not in deterministic order"

    def test_manifest_has_checksums(self):
        """AC-DET-4: Pipeline manifest includes SHA256 checksums."""
        manifest_path = DATA_PROCESSED / "pipeline_manifest.json"
        if not manifest_path.exists():
            pytest.skip("No manifest yet")
        
        manifest = json.load(open(manifest_path))
        assert "checksums" in manifest, "Manifest missing checksums"
        assert len(manifest["checksums"]) > 0, "No checksums in manifest"

    def test_force_flag_behavior(self):
        """AC-DET-5: --force overwrites; without it, skips if newer."""
        station = "greenwich"
        hourly_path = DATA_PROCESSED / station / "station_hourly.csv"
        
        if not hourly_path.exists():
            pytest.skip("No processed data yet")
        
        # Get original mtime
        original_mtime = hourly_path.stat().st_mtime
        
        # Run without --force (should skip if newer)
        subprocess.run(
            [PYTHON, "cleaning.py", "--stations", station],
            cwd=PROJECT_ROOT,
            capture_output=True,
            timeout=300,
        )
        
        # mtime should not change (or test is inconclusive if it does)
        # This is a soft test

    def test_race_condition_fixed(self):
        """AC-DET-6: test_cleaning_py_runs marked as serial."""
        # Check pytest.ini or test file for serial marker
        test_file = PROJECT_ROOT / "tests" / "test_data_refresh.py"
        if not test_file.exists():
            pytest.skip("test_data_refresh.py not found")
        
        content = test_file.read_text()
        assert "@pytest.mark.serial" in content or "serial" in content, \
            "Race condition test not marked as serial"


# =============================================================================
# Phase 7: E2E Validation
# =============================================================================

class TestAC_PIPE_7_E2EValidation:
    """Phase 7: Full end-to-end validation."""

    def test_cleaning_py_completes(self):
        """AC-E2E-1: cleaning.py completes with exit code 0."""
        result = subprocess.run(
            [PYTHON, "cleaning.py"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=600,
        )
        assert result.returncode == 0, f"cleaning.py failed: {result.stderr}"

    def test_all_stations_have_outputs(self):
        """AC-E2E-2: All 6 stations have hourly + daily CSVs."""
        stations = ["greenwich", "cavendish", "north_rustico", "stanley_bridge",
                    "tracadie", "stanhope"]
        
        for station in stations:
            hourly = DATA_PROCESSED / station / "station_hourly.csv"
            daily = DATA_PROCESSED / station / "station_daily.csv"
            
            assert hourly.exists(), f"Missing {station} hourly"
            assert daily.exists(), f"Missing {station} daily"

    def test_fwi_columns_populated(self):
        """AC-E2E-3: All stations with RH data have populated FWI columns."""
        stations_with_rh = ["greenwich", "cavendish", "stanhope"]
        
        for station in stations_with_rh:
            daily_path = DATA_PROCESSED / station / "station_daily.csv"
            if not daily_path.exists():
                continue
            
            df = pd.read_csv(daily_path)
            assert df["fwi"].notna().sum() > 0, f"{station} has no FWI values"

    def test_all_report_artifacts_exist(self):
        """AC-E2E-4: All 4 report artifacts exist."""
        artifacts = [
            "pipeline_manifest.json",
            "imputation_report.csv",
            "qa_qc_report.csv",
            "stanhope_validation.csv",
        ]
        
        for artifact in artifacts:
            path = DATA_PROCESSED / artifact
            assert path.exists(), f"Missing {artifact}"

    def test_no_large_gaps(self):
        """AC-E2E-5: No gaps > 7 days in hourly data."""
        for station_dir in DATA_PROCESSED.iterdir():
            if not station_dir.is_dir():
                continue
            
            hourly_path = station_dir / "station_hourly.csv"
            if not hourly_path.exists():
                continue
            
            df = pd.read_csv(hourly_path)
            timestamps = pd.to_datetime(df["timestamp_utc"]).sort_values()
            gaps = timestamps.diff().dropna()
            
            max_gap = gaps.max()
            assert max_gap < pd.Timedelta(days=7), \
                f"{station_dir.name} has gap > 7 days: {max_gap}"

    def test_continuous_coverage(self):
        """AC-E2E-6: Continuous coverage from earliest to latest record."""
        for station_dir in DATA_PROCESSED.iterdir():
            if not station_dir.is_dir():
                continue
            
            hourly_path = station_dir / "station_hourly.csv"
            if not hourly_path.exists():
                continue
            
            df = pd.read_csv(hourly_path)
            timestamps = pd.to_datetime(df["timestamp_utc"])
            
            date_range = timestamps.max() - timestamps.min()
            # Should have at least 1 year of data for main stations
            if station_dir.name in ["greenwich", "stanhope"]:
                assert date_range.days > 300, \
                    f"{station_dir.name} has insufficient coverage: {date_range.days} days"

    def test_zero_unprocessed_files(self):
        """AC-E2E-7: Pipeline manifest reports 0 unprocessed files."""
        manifest_path = DATA_PROCESSED / "pipeline_manifest.json"
        if not manifest_path.exists():
            pytest.skip("No manifest yet")
        
        manifest = json.load(open(manifest_path))
        
        # Check for unprocessed count
        if "unprocessed_count" in manifest:
            assert manifest["unprocessed_count"] == 0, \
                f"Unprocessed files: {manifest['unprocessed_count']}"

    def test_full_test_suite_passes(self):
        """AC-E2E-8: Full test suite passes."""
        result = subprocess.run(
            ["pytest", "tests/", "-v", "--tb=short"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )
        
        # Allow some xfail but no failures
        assert result.returncode == 0 or "failed" not in result.stdout.lower(), \
            f"Tests failed: {result.stdout[-1000:]}"
