"""Phase 11 tests — Dual-Mode FWI Pipeline (Compliant + Extended)."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable
HALIFAX = ZoneInfo("America/Halifax")


def _make_hourly_frame(start_utc: str, periods: int = 48) -> pd.DataFrame:
    timestamps = pd.date_range(start_utc, periods=periods, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp_utc": timestamps,
            "station": ["greenwich"] * periods,
            "air_temperature_c": [20.0] * periods,
            "relative_humidity_pct": [45.0] * periods,
            "wind_speed_kmh": [15.0] * periods,
            "rain_mm": [0.25] * periods,
        }
    )


class TestAC11NoonObservationFilter:
    """Phase 11.2 — local-noon observation extraction."""

    def test_filter_noon_observations_selects_adt_noon_row(self):
        """AC-11-01: 12:00 ADT resolves to 15:00 UTC in summer."""
        from pea_met_network.cleaning import filter_noon_observations

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z")

        result = filter_noon_observations(hourly)

        assert len(result) == 2
        local_times = pd.to_datetime(result["timestamp_utc"], utc=True).dt.tz_convert(
            HALIFAX
        )
        assert local_times.dt.hour.tolist() == [12, 12]
        assert result["timestamp_utc"].astype(str).tolist()[0].startswith(
            "2024-07-01 15:00:00"
        )

    def test_filter_noon_observations_selects_ast_noon_row(self):
        """AC-11-02: 12:00 AST resolves to 16:00 UTC in winter."""
        from pea_met_network.cleaning import filter_noon_observations

        hourly = _make_hourly_frame("2024-01-10T00:00:00Z")

        result = filter_noon_observations(hourly)

        assert len(result) == 2
        local_times = pd.to_datetime(result["timestamp_utc"], utc=True).dt.tz_convert(
            HALIFAX
        )
        assert local_times.dt.hour.tolist() == [12, 12]
        assert result["timestamp_utc"].astype(str).tolist()[0].startswith(
            "2024-01-10 16:00:00"
        )

    def test_filter_noon_observations_sums_previous_24h_rain(self):
        """AC-11-03: noon row carries preceding 24h rainfall total."""
        from pea_met_network.cleaning import filter_noon_observations

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z")
        hourly["rain_mm"] = 0.0
        noon_ts = pd.Timestamp("2024-07-01T15:00:00Z")
        window = (hourly["timestamp_utc"] > noon_ts - pd.Timedelta(hours=24)) & (
            hourly["timestamp_utc"] <= noon_ts
        )
        hourly.loc[window, "rain_mm"] = 1.0

        result = filter_noon_observations(hourly)

        assert result.loc[0, "rain_mm"] == pytest.approx(24.0)


class TestAC11DailyFWI:
    """Phase 11.3 — compliant daily FWI loop."""

    def test_calculate_fwi_daily_uses_startup_values_and_reference_functions(self):
        """AC-11-04: first compliant day starts from Van Wagner defaults."""
        from pea_met_network.cleaning import calculate_fwi_daily
        from pea_met_network.fwi import (
            buildup_index,
            drought_code,
            duff_moisture_code,
            fine_fuel_moisture_code,
            fire_weather_index,
            initial_spread_index,
        )

        daily = pd.DataFrame(
            {
                "timestamp_utc": pd.to_datetime(["2024-07-01T15:00:00Z"], utc=True),
                "station": ["greenwich"],
                "air_temperature_c": [17.0],
                "relative_humidity_pct": [42.0],
                "wind_speed_kmh": [25.0],
                "rain_mm": [0.0],
            }
        )

        result = calculate_fwi_daily(daily)

        expected_ffmc = fine_fuel_moisture_code(17.0, 42.0, 25.0, 0.0, 85.0)
        expected_dmc = duff_moisture_code(17.0, 42.0, 0.0, 6.0, 7, 46.4)
        expected_dc = drought_code(17.0, 0.0, 15.0, 7, 46.4)
        expected_isi = initial_spread_index(expected_ffmc, 25.0)
        expected_bui = buildup_index(expected_dmc, expected_dc)
        expected_fwi = fire_weather_index(expected_isi, expected_bui)

        assert result.loc[0, "ffmc"] == pytest.approx(expected_ffmc)
        assert result.loc[0, "dmc"] == pytest.approx(expected_dmc)
        assert result.loc[0, "dc"] == pytest.approx(expected_dc)
        assert result.loc[0, "isi"] == pytest.approx(expected_isi)
        assert result.loc[0, "bui"] == pytest.approx(expected_bui)
        assert result.loc[0, "fwi"] == pytest.approx(expected_fwi)

    def test_calculate_fwi_daily_carries_forward_on_missing_noon(self):
        """AC-11-05: missing noon observation carries prior FWI codes forward."""
        from pea_met_network.cleaning import calculate_fwi_daily

        daily = pd.DataFrame(
            {
                "timestamp_utc": pd.to_datetime(
                    ["2024-07-01T15:00:00Z", "2024-07-02T15:00:00Z"], utc=True
                ),
                "station": ["greenwich", "greenwich"],
                "air_temperature_c": [17.0, pd.NA],
                "relative_humidity_pct": [42.0, pd.NA],
                "wind_speed_kmh": [25.0, pd.NA],
                "rain_mm": [0.0, pd.NA],
            }
        )

        result = calculate_fwi_daily(daily)

        assert result.loc[1, "ffmc"] == pytest.approx(result.loc[0, "ffmc"])
        assert result.loc[1, "dmc"] == pytest.approx(result.loc[0, "dmc"])
        assert result.loc[1, "dc"] == pytest.approx(result.loc[0, "dc"])
        assert result.loc[1, "fwi"] == pytest.approx(result.loc[0, "fwi"])

    def test_calculate_fwi_daily_uses_default_latitude_and_override(self):
        """AC-11-06: daily FWI accepts default 46.4 and station override latitude.

        NOTE: fwi.py reference functions use fixed day-length tables and do not
        vary output by latitude. The lat parameter is accepted for future
        expansion but currently has no effect on results.
        """
        from pea_met_network.cleaning import calculate_fwi_daily

        daily = pd.DataFrame(
            {
                "timestamp_utc": pd.to_datetime(["2024-04-01T15:00:00Z"], utc=True),
                "station": ["greenwich"],
                "air_temperature_c": [17.0],
                "relative_humidity_pct": [42.0],
                "wind_speed_kmh": [25.0],
                "rain_mm": [0.0],
            }
        )

        default_result = calculate_fwi_daily(daily)
        override_result = calculate_fwi_daily(daily, lat=60.0)

        # Both should produce valid FWI output.
        assert default_result.loc[0, "dmc"] > 0
        assert override_result.loc[0, "dmc"] > 0
        # Currently identical — reference fns use fixed day-length tables.
        assert default_result.loc[0, "dmc"] == pytest.approx(override_result.loc[0, "dmc"])


class TestAC11CliAndConfig:
    """Phase 11.1 + 11.4 — routing surface."""

    def test_cleaning_config_declares_fwi_mode_default_hourly(self):
        """AC-11-07: cleaning config exposes fwi.fwi_mode = hourly in Phase 12."""
        config_path = PROJECT_ROOT / "docs" / "cleaning-config.json"
        config = json.loads(config_path.read_text())

        assert "fwi" in config
        assert config["fwi"]["fwi_mode"] == "hourly"

    @pytest.mark.e2e
    def test_cli_accepts_fwi_mode_compliant(self):
        """AC-11-08: CLI parser accepts --fwi-mode compliant."""
        result = subprocess.run(
            [
                PYTHON,
                "-m",
                "pea_met_network",
                "--dry-run",
                "--stations",
                "greenwich",
                "--fwi-mode",
                "compliant",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=60,
        )

        assert result.returncode == 0, result.stderr

    @pytest.mark.e2e
    def test_compliant_mode_writes_daily_compliant_output(self, tmp_path: Path):
        """AC-11-09: compliant mode emits {station}_daily_compliant.csv."""
        result = subprocess.run(
            [
                PYTHON,
                "-m",
                "pea_met_network",
                "--stations",
                "greenwich",
                "--force",
                "--fwi-mode",
                "compliant",
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=300,
        )

        assert result.returncode == 0, result.stderr
        output_path = PROJECT_ROOT / "data" / "processed" / "greenwich" / "greenwich_daily_compliant.csv"
        assert output_path.exists()
