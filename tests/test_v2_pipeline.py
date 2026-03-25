"""
v2 Pipeline Tests — Adapter Architecture

These tests validate the v2 pipeline requirements:
- Single entry point with adapter routing
- No files skipped (unknown formats are hard errors)
- Single canonical output schema
- Deterministic, reproducible outputs

Tests are designed to FAIL until Ralph implements the v2 architecture.
"""

import subprocess
import json
import pandas as pd
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
DATA_RAW = PROJECT_ROOT / "data" / "raw"
DATA_PROCESSED = PROJECT_ROOT / "data" / "processed"
SRC_MODULE = PROJECT_ROOT / "src" / "pea_met_network"


# =============================================================================
# Phase 1: Adapter Architecture
# =============================================================================

class TestAC_PIPE_1_AdapterArchitecture:
    """Phase 1 exit gate: adapter module exists, router works, dry-run flag."""
    
    def test_adapters_module_exists(self):
        """The adapters/ module must exist and be importable."""
        from pea_met_network.adapters import route_by_extension, load_with_adapter
        assert callable(route_by_extension)
        assert callable(load_with_adapter)
    
    def test_adapter_registry_has_all_formats(self):
        """Registry must have adapters for .csv, .xlsx, .xle, .json."""
        from pea_met_network.adapters import ADAPTER_REGISTRY
        required = {".csv", ".xlsx", ".xle", ".json"}
        assert required.issubset(set(ADAPTER_REGISTRY.keys()))
    
    def test_dry_run_flag_reports_file_counts(self):
        """--dry-run must report what would be processed without writing."""
        result = subprocess.run(
            ["python", "cleaning.py", "--dry-run"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120
        )
        # Should succeed
        assert result.returncode == 0
        # Should report stations
        assert "would be processed" in result.stdout.lower() or "dry run" in result.stdout.lower()
    
    def test_dry_run_does_not_write_outputs(self):
        """--dry-run must not modify any processed files."""
        # Get mtime of an existing output file
        hourly_csv = DATA_PROCESSED / "greenwich" / "station_hourly.csv"
        if hourly_csv.exists():
            mtime_before = hourly_csv.stat().st_mtime
        else:
            mtime_before = None
        
        subprocess.run(
            ["python", "cleaning.py", "--dry-run", "--stations", "greenwich"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if hourly_csv.exists():
            mtime_after = hourly_csv.stat().st_mtime
            assert mtime_after == mtime_before, "dry-run modified output file"
    
    def test_unknown_format_is_hard_error(self):
        """Unknown file extensions must cause a hard error, not silent skip."""
        # Create a temp .foo file in raw data
        temp_file = DATA_RAW / "peinp" / "test_unknown.foo"
        temp_file_exists_before = temp_file.exists()
        
        try:
            temp_file.write_text("not a real data file\n")
            
            result = subprocess.run(
                ["python", "cleaning.py", "--dry-run"],
                cwd=PROJECT_ROOT,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            # Should fail with non-zero exit
            assert result.returncode != 0, "Unknown format should cause hard error"
            # Should mention the unknown format
            assert ".foo" in result.stderr or "unknown" in result.stderr.lower()
        
        finally:
            if not temp_file_exists_before and temp_file.exists():
                temp_file.unlink()
    
    def test_canonical_schema_constant_exists(self):
        """CANONICAL_COLUMNS must be defined in adapters module."""
        from pea_met_network.adapters import CANONICAL_COLUMNS
        assert isinstance(CANONICAL_COLUMNS, (list, tuple, set))
        # Must include the core FWI inputs
        required = {"station", "timestamp_utc", "air_temperature_c", 
                    "relative_humidity_pct", "wind_speed_kmh", "precipitation_mm"}
        assert required.issubset(set(CANONICAL_COLUMNS))


class TestAC_PIPE_1_SingleEntryPoint:
    """Phase 1: cleaning.py is a single entry point, not per-file."""
    
    def test_cleaning_py_discovers_all_formats(self):
        """cleaning.py must discover .csv, .xlsx, .xle, .json files."""
        result = subprocess.run(
            ["python", "cleaning.py", "--dry-run"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120
        )
        output = result.stdout + result.stderr
        
        # Should discover multiple formats
        formats_found = sum(1 for ext in [".csv", ".xlsx", ".xle", ".json"] 
                          if ext in output.lower())
        assert formats_found >= 2, "Should discover multiple file formats"
    
    def test_cleaning_py_stations_flag(self):
        """--stations flag must filter processing to named stations."""
        result = subprocess.run(
            ["python", "cleaning.py", "--dry-run", "--stations", "greenwich"],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=120
        )
        assert result.returncode == 0
        assert "greenwich" in result.stdout.lower()


# =============================================================================
# Phase 2: Format Adapters — CSV, XLSX, XLE
# =============================================================================

class TestAC_PIPE_2_FormatAdapters:
    """Phase 2 exit gate: all format adapters work, no files skipped."""
    
    def test_csv_adapter_loads_peinp_files(self):
        """csv_adapter must load PEINP archive CSVs."""
        from pea_met_network.adapters import csv_adapter
        
        # Find a PEINP CSV file
        peinp_dir = DATA_RAW / "peinp"
        csv_files = list(peinp_dir.rglob("*.csv"))
        assert len(csv_files) > 0, "No PEINP CSV files found"
        
        df = csv_adapter.load(csv_files[0])
        assert df is not None
        assert len(df) > 0
        assert "timestamp_utc" in df.columns
    
    def test_xlsx_adapter_loads_greenwich_2023(self):
        """xlsx_adapter must load Greenwich 2023 .xlsx files."""
        from pea_met_network.adapters import xlsx_adapter
        
        xlsx_files = list((DATA_RAW / "peinp").rglob("greenwich/**/*.xlsx"))
        if not xlsx_files:
            pytest.skip("No Greenwich xlsx files found")
        
        df = xlsx_adapter.load(xlsx_files[0])
        assert df is not None
        assert len(df) > 0
        assert "timestamp_utc" in df.columns
    
    def test_xle_adapter_loads_stanley_bridge_2022(self):
        """xle_adapter must load Stanley Bridge 2022 .xle files (Solinst XML)."""
        from pea_met_network.adapters import xle_adapter
        
        xle_files = list((DATA_RAW / "peinp").rglob("stanley*/**/*.xle"))
        if not xle_files:
            pytest.skip("No xle files found")
        
        df = xle_adapter.load(xle_files[0])
        assert df is not None
        assert len(df) > 0
        assert "timestamp_utc" in df.columns
    
    def test_column_maps_handles_s_tmb_sensors(self):
        """column_maps must recognize S-TMB temperature sensors."""
        from pea_met_network.adapters.column_maps import map_columns
        
        # S-TMB is a different sensor type than S-THB/S-THC
        test_cols = ["Timestamp", "S-TMB", "RH", "Wind Speed"]
        mapped = map_columns(test_cols)
        
        assert "air_temperature_c" in mapped.values(), \
            "S-TMB must map to air_temperature_c"
    
    def test_column_maps_handles_wind_speed_case_variants(self):
        """column_maps must handle 'Average Wind Speed' (capital A)."""
        from pea_met_network.adapters.column_maps import map_columns
        
        test_cols = ["Timestamp", "Average Wind Speed", "Wind gust  speed"]
        mapped = map_columns(test_cols)
        
        # Double space in "Wind gust  speed" must still map
        assert "wind_speed_kmh" in mapped.values()
        assert "wind_gust_speed_kmh" in mapped.values()
    
    def test_no_files_skipped_in_manifest(self):
        """After pipeline run, manifest must report 0 unprocessed files."""
        manifest_path = DATA_PROCESSED / "pipeline_manifest.json"
        if not manifest_path.exists():
            pytest.skip("Pipeline manifest not found - run pipeline first")
        
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        # Must have unprocessed_files key
        assert "unprocessed_files" in manifest
        # Must be empty
        assert manifest["unprocessed_files"] == [], \
            f"Files were skipped: {manifest['unprocessed_files']}"


# =============================================================================
# Phase 3: Imputation
# =============================================================================

class TestAC_PIPE_3_ImputationAudit:
    """Phase 3 exit gate: imputation runs with full audit trail."""
    
    def test_imputation_report_exists(self):
        """imputation_report.csv must exist after pipeline run."""
        report_path = DATA_PROCESSED / "imputation_report.csv"
        assert report_path.exists(), "imputation_report.csv not found"
    
    def test_imputation_report_has_all_stations(self):
        """Report must include all processed stations."""
        report_path = DATA_PROCESSED / "imputation_report.csv"
        if not report_path.exists():
            pytest.skip("imputation_report.csv not found")
        
        df = pd.read_csv(report_path)
        stations = df["station"].unique()
        
        # Should have multiple stations
        assert len(stations) >= 3, f"Only {len(stations)} stations in report"
    
    def test_no_station_has_100pct_nan_after_imputation(self):
        """No station should have 100% NaN in required FWI inputs."""
        for station_dir in DATA_PROCESSED.iterdir():
            if not station_dir.is_dir():
                continue
            hourly_csv = station_dir / "station_hourly.csv"
            if not hourly_csv.exists():
                continue
            
            df = pd.read_csv(hourly_csv)
            required_cols = ["air_temperature_c", "relative_humidity_pct", 
                           "wind_speed_kmh", "precipitation_mm"]
            
            for col in required_cols:
                if col in df.columns:
                    nan_pct = df[col].isna().mean()
                    assert nan_pct < 1.0, \
                        f"{station_dir.name} has 100% NaN in {col}"


# =============================================================================
# Phase 4: Stanhope Validation
# =============================================================================

class TestAC_PIPE_4_StanhopeValidation:
    """Phase 4 exit gate: Stanhope has FWI, daily output, serves as reference."""
    
    def test_stanhope_daily_csv_exists(self):
        """Stanhope must have station_daily.csv."""
        daily_path = DATA_PROCESSED / "stanhope" / "station_daily.csv"
        assert daily_path.exists(), "Stanhope station_daily.csv not found"
    
    def test_stanhope_daily_has_fwi_columns(self):
        """Stanhope daily must have FWI indices."""
        daily_path = DATA_PROCESSED / "stanhope" / "station_daily.csv"
        if not daily_path.exists():
            pytest.skip("Stanhope station_daily.csv not found")
        
        df = pd.read_csv(daily_path)
        fwi_cols = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]
        
        for col in fwi_cols:
            assert col in df.columns, f"Stanhope missing {col}"
    
    def test_stanhope_fwi_values_reasonable(self):
        """Stanhope FWI values must be in reasonable ranges."""
        daily_path = DATA_PROCESSED / "stanhope" / "station_daily.csv"
        if not daily_path.exists():
            pytest.skip("Stanhope station_daily.csv not found")
        
        df = pd.read_csv(daily_path)
        
        # FWI components have physical bounds
        assert df["ffmc"].min() >= 0 and df["ffmc"].max() <= 101
        assert df["dmc"].min() >= 0
        assert df["dc"].min() >= 0
        assert df["isi"].min() >= 0
        assert df["bui"].min() >= 0
        assert df["fwi"].min() >= 0


# =============================================================================
# Phase 5: QA/QC Report
# =============================================================================

class TestAC_PIPE_5_QAQCReport:
    """Phase 5 exit gate: QA/QC report generated for every run."""
    
    def test_qa_qc_report_exists(self):
        """qa_qc_report.csv must exist after pipeline run."""
        report_path = DATA_PROCESSED / "qa_qc_report.csv"
        assert report_path.exists(), "qa_qc_report.csv not found"
    
    def test_qa_qc_report_has_coverage_metrics(self):
        """Report must include date range coverage per station."""
        report_path = DATA_PROCESSED / "qa_qc_report.csv"
        if not report_path.exists():
            pytest.skip("qa_qc_report.csv not found")
        
        df = pd.read_csv(report_path)
        
        # Should have coverage-related columns
        assert "station" in df.columns
        # Either date_range, coverage_pct, or start_date/end_date
        coverage_cols = [c for c in df.columns 
                        if "date" in c.lower() or "coverage" in c.lower()]
        assert len(coverage_cols) >= 2, "Missing coverage metrics"


# =============================================================================
# Phase 6: Determinism
# =============================================================================

class TestAC_PIPE_6_Determinism:
    """Phase 6 exit gate: identical outputs on re-run."""
    
    def test_outputs_sorted_by_station_timestamp(self):
        """All output CSVs must be sorted by (station, timestamp_utc)."""
        for station_dir in DATA_PROCESSED.iterdir():
            if not station_dir.is_dir():
                continue
            for csv_file in station_dir.glob("*.csv"):
                df = pd.read_csv(csv_file)
                if "timestamp_utc" in df.columns and "station" in df.columns:
                    sorted_df = df.sort_values(["station", "timestamp_utc"]).reset_index(drop=True)
                    pd.testing.assert_frame_equal(
                        df.reset_index(drop=True), 
                        sorted_df,
                        check_like=True,
                        obj=f"{csv_file} is not sorted"
                    )
    
    def test_column_order_deterministic(self):
        """Column order must be consistent across runs."""
        # Run pipeline twice and compare column order
        # This test requires the pipeline to be run twice
        pytest.skip("Requires two pipeline runs - manual validation")
    
    def test_manifest_includes_input_file_list(self):
        """Manifest must include list of all input files processed."""
        manifest_path = DATA_PROCESSED / "pipeline_manifest.json"
        if not manifest_path.exists():
            pytest.skip("Pipeline manifest not found")
        
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        assert "input_files" in manifest or "sources" in manifest


# =============================================================================
# Phase 7: Licor JSON Integration
# =============================================================================

class TestAC_PIPE_7_LicorIntegration:
    """Phase 7 exit gate: Licor JSON integrated, continuous coverage."""
    
    def test_json_adapter_loads_licor_files(self):
        """json_adapter must load Licor Cloud JSON files."""
        from pea_met_network.adapters import json_adapter
        
        json_files = list((DATA_RAW / "licor").rglob("*.json"))
        if not json_files:
            pytest.skip("No Licor JSON files found")
        
        df = json_adapter.load(json_files[0])
        assert df is not None
        assert len(df) > 0
        assert "timestamp_utc" in df.columns
    
    def test_continuous_coverage_no_gap_larger_than_48h(self):
        """PEINP→Licor boundary gap must be < 48 hours."""
        for station in ["greenwich", "cavendish", "north_rustico", "tracadie", "stanley_bridge"]:
            hourly_path = DATA_PROCESSED / station / "station_hourly.csv"
            if not hourly_path.exists():
                continue
            
            df = pd.read_csv(hourly_path, parse_dates=["timestamp_utc"])
            df = df.sort_values("timestamp_utc")
            
            # Find gaps
            df["time_diff"] = df["timestamp_utc"].diff()
            max_gap = df["time_diff"].max()
            
            # Max gap should be <= 48 hours (in nanoseconds)
            max_gap_hours = max_gap.total_seconds() / 3600 if pd.notna(max_gap) else 0
            assert max_gap_hours <= 48, \
                f"{station} has gap of {max_gap_hours:.1f} hours"


# =============================================================================
# Phase 8: End-to-End
# =============================================================================

class TestAC_PIPE_8_EndToEnd:
    """Phase 8 exit gate: full pipeline works, all stations complete."""
    
    def test_all_stations_have_hourly_csv(self):
        """All 6 stations must have hourly output."""
        stations = ["greenwich", "cavendish", "north_rustico", "tracadie", 
                   "stanley_bridge", "stanhope"]
        
        for station in stations:
            hourly_path = DATA_PROCESSED / station / "station_hourly.csv"
            assert hourly_path.exists(), f"{station} missing station_hourly.csv"
    
    def test_all_stations_have_daily_csv(self):
        """All 6 stations must have daily output."""
        stations = ["greenwich", "cavendish", "north_rustico", "tracadie",
                   "stanley_bridge", "stanhope"]
        
        for station in stations:
            daily_path = DATA_PROCESSED / station / "station_daily.csv"
            assert daily_path.exists(), f"{station} missing station_daily.csv"
    
    def test_all_peinp_stations_have_fwi_columns(self):
        """All 5 PEINP stations must have FWI columns in daily output."""
        peinp_stations = ["greenwich", "cavendish", "north_rustico", 
                         "tracadie", "stanley_bridge"]
        fwi_cols = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]
        
        for station in peinp_stations:
            daily_path = DATA_PROCESSED / station / "station_daily.csv"
            if not daily_path.exists():
                continue
            
            df = pd.read_csv(daily_path)
            for col in fwi_cols:
                assert col in df.columns, f"{station} missing {col}"
    
    def test_pipeline_manifest_complete(self):
        """Manifest must report completion with all artifacts."""
        manifest_path = DATA_PROCESSED / "pipeline_manifest.json"
        assert manifest_path.exists(), "Pipeline manifest not found"
        
        with open(manifest_path) as f:
            manifest = json.load(f)
        
        assert manifest.get("status") == "complete"
        assert "artifacts" in manifest
        assert len(manifest["artifacts"]) >= 10  # At least 10 output files
