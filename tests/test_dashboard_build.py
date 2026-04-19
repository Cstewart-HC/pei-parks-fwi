"""Tests for scripts/build_dashboard.py — dashboard data build script."""

import json
import os
import sys
from pathlib import Path

import pytest

# Use the same Python interpreter that is running pytest
PYTHON = sys.executable
PROJECT_ROOT = Path(__file__).resolve().parent.parent
BUILD_SCRIPT = str(PROJECT_ROOT / "scripts" / "build_dashboard.py")


STATIONS = [
    "stanhope", "cavendish", "greenwich",
    "north_rustico", "stanley_bridge", "tracadie",
]


@pytest.fixture
def output_dir(tmp_path):
    """Run build_dashboard.py into a temp directory and return it."""
    import subprocess
    result = subprocess.run(
        [PYTHON, BUILD_SCRIPT, "--output-dir", str(tmp_path)],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=60,
    )
    assert result.returncode == 0, f"Build script failed:\n{result.stderr}"
    return tmp_path


class TestStationsJSON:
    """Validate stations.json output."""

    def test_file_exists(self, output_dir):
        assert (output_dir / "stations.json").exists()

    def test_valid_json(self, output_dir):
        with open(output_dir / "stations.json") as f:
            data = json.load(f)
        assert isinstance(data, dict)

    def test_all_six_stations_present(self, output_dir):
        with open(output_dir / "stations.json") as f:
            data = json.load(f)
        for station in STATIONS:
            assert station in data, f"Missing station: {station}"

    def test_station_has_required_fields(self, output_dir):
        with open(output_dir / "stations.json") as f:
            data = json.load(f)
        for station_id, meta in data.items():
            assert "display_name" in meta, f"{station_id}: missing display_name"
            assert "lat" in meta, f"{station_id}: missing lat"
            assert "lon" in meta, f"{station_id}: missing lon"
            assert "group" in meta, f"{station_id}: missing group"
            assert isinstance(meta["lat"], float)
            assert isinstance(meta["lon"], float)
            assert meta["lat"] > 40 and meta["lat"] < 50  # PEI latitude range
            assert meta["lon"] > -65 and meta["lon"] < -60  # PEI longitude range


class TestFwiDailyJSON:
    """Validate fwi_daily.json output."""

    def test_file_exists(self, output_dir):
        assert (output_dir / "fwi_daily.json").exists()

    def test_valid_json(self, output_dir):
        with open(output_dir / "fwi_daily.json") as f:
            data = json.load(f)
        assert isinstance(data, dict)

    def test_dates_are_sorted(self, output_dir):
        with open(output_dir / "fwi_daily.json") as f:
            data = json.load(f)
        date_list = list(data.keys())
        assert date_list == sorted(date_list), "Dates are not sorted"

    def test_date_format_yyyy_mm_dd(self, output_dir):
        with open(output_dir / "fwi_daily.json") as f:
            data = json.load(f)
        import re
        pattern = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        for date in data.keys():
            assert pattern.match(date), f"Invalid date format: {date}"

    def test_min_date_range(self, output_dir):
        """Should have data starting from 2023-04-01 or earlier."""
        with open(output_dir / "fwi_daily.json") as f:
            data = json.load(f)
        dates = sorted(data.keys())
        assert len(dates) >= 100, f"Expected at least 100 dates, got {len(dates)}"
        assert dates[0] <= "2023-04-01", f"Earliest date {dates[0]} is after 2023-04-01"

    def test_records_have_fwi_fields(self, output_dir):
        with open(output_dir / "fwi_daily.json") as f:
            data = json.load(f)
        required = {"station", "ffmc", "dmc", "dc", "isi", "bui", "fwi"}
        # Check a sample of dates
        sample_dates = sorted(data.keys())[::max(1, len(data) // 10)]
        for date in sample_dates:
            for rec in data[date]:
                assert required.issubset(rec.keys()), (
                    f"Date {date}, station {rec.get('station')}: missing fields "
                    f"{required - set(rec.keys())}"
                )

    def test_fwi_values_not_null(self, output_dir):
        """All fwi values in the output should be non-null (NaN rows are dropped)."""
        with open(output_dir / "fwi_daily.json") as f:
            data = json.load(f)
        for date, records in data.items():
            for rec in records:
                assert rec["fwi"] is not None, (
                    f"Date {date}, station {rec['station']}: fwi is null"
                )

    def test_all_stations_have_data(self, output_dir):
        """All 6 stations should appear in the data."""
        with open(output_dir / "fwi_daily.json") as f:
            data = json.load(f)
        found_stations = set()
        for records in data.values():
            for rec in records:
                found_stations.add(rec["station"])
        for station in STATIONS:
            assert station in found_stations, f"Station {station} has no data in fwi_daily.json"

    def test_file_size_reasonable(self, output_dir):
        """fwi_daily.json should be under 2 MB."""
        size = (output_dir / "fwi_daily.json").stat().st_size
        assert size < 2 * 1024 * 1024, f"fwi_daily.json is {size / 1024:.0f} KB, exceeds 2 MB limit"


class TestBuildScriptCLI:
    """Test CLI interface of build script."""

    def test_default_output_dir(self, tmp_path, monkeypatch):
        """Running without --output-dir should create dashboard/data/ relative to project root."""
        import subprocess
        # Run from project root, output goes to dashboard/data
        result = subprocess.run(
            [PYTHON, BUILD_SCRIPT],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            timeout=60,
        )
        assert result.returncode == 0
        assert (PROJECT_ROOT / "dashboard" / "data" / "stations.json").exists()
        assert (PROJECT_ROOT / "dashboard" / "data" / "fwi_daily.json").exists()

    def test_custom_output_dir(self, tmp_path):
        """--output-dir should create files in the specified directory."""
        import subprocess
        custom_dir = tmp_path / "custom_output"
        result = subprocess.run(
            [PYTHON, BUILD_SCRIPT, "--output-dir", str(custom_dir)],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            timeout=60,
        )
        assert result.returncode == 0
        assert (custom_dir / "stations.json").exists()
        assert (custom_dir / "fwi_daily.json").exists()

    def test_help_flag(self):
        """--help should print usage and exit 0."""
        import subprocess
        result = subprocess.run(
            [PYTHON, BUILD_SCRIPT, "--help"],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            timeout=10,
        )
        assert result.returncode == 0
        assert "output-dir" in result.stdout


class TestParkBoundary:
    """Validate park_boundary.geojson if it exists."""

    def test_boundary_file_exists(self):
        boundary_path = PROJECT_ROOT / "dashboard" / "data" / "park_boundary.geojson"
        if not boundary_path.exists():
            pytest.skip("park_boundary.geojson not yet created")
        with open(boundary_path) as f:
            data = json.load(f)
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) > 0

    def test_boundary_features_are_polygons(self):
        boundary_path = PROJECT_ROOT / "dashboard" / "data" / "park_boundary.geojson"
        if not boundary_path.exists():
            pytest.skip("park_boundary.geojson not yet created")
        with open(boundary_path) as f:
            data = json.load(f)
        for feature in data["features"]:
            assert feature["geometry"]["type"] in ("Polygon", "MultiPolygon")

    def test_boundary_file_size(self):
        boundary_path = PROJECT_ROOT / "dashboard" / "data" / "park_boundary.geojson"
        if not boundary_path.exists():
            pytest.skip("park_boundary.geojson not yet created")
        size = boundary_path.stat().st_size
        assert size < 50 * 1024, f"park_boundary.geojson is {size / 1024:.0f} KB, exceeds 50 KB limit"
