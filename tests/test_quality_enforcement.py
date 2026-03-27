"""Tests for Phase 8: Data Quality Enforcement.

TDD — these tests define expected behaviour for:
  - enforce_quality() in src/pea_met_network/quality.py
  - cleaning-config.json loading
  - quality flag column in output DataFrames
  - FWI output range enforcement
  - flatline detection (flag_only, no mutation)
  - rate-of-change detection
  - cross-variable checks (rain/RH correlation)
  - date range truncation

Run: .venv/bin/pytest tests/test_quality_enforcement.py -v
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CONFIG_PATH = Path("docs/cleaning-config.json")
QUALITY_MODULE = "pea_met_network.quality"


@pytest.fixture
def cleaning_config():
    """Load the project cleaning config."""
    with open(CONFIG_PATH) as f:
        return json.load(f)


@pytest.fixture
def sample_hourly():
    """24 rows of clean hourly data for a single station."""
    return pd.DataFrame(
        {
            "timestamp_utc": pd.date_range(
                "2024-06-01", periods=24, freq="h", tz="UTC"
            ),
            "station": ["test_station"] * 24,
            "air_temperature_c": [20.0 + 0.5 * i for i in range(24)],
            "relative_humidity_pct": [60.0 + 0.3 * i for i in range(24)],
            "wind_speed_kmh": [15.0 + 0.2 * i for i in range(24)],
            "rain_mm": [0.0] * 24,
        }
    )


@pytest.fixture
def multi_station_hourly():
    """48 rows: 2 stations × 24 hours."""
    dates = pd.date_range("2024-06-01", periods=24, freq="h", tz="UTC")
    return pd.concat(
        [
            pd.DataFrame(
                {
                    "timestamp_utc": dates,
                    "station": "alpha",
                    "air_temperature_c": [20.0] * 24,
                    "relative_humidity_pct": [60.0] * 24,
                    "wind_speed_kmh": [15.0] * 24,
                    "rain_mm": [0.0] * 24,
                }
            ),
            pd.DataFrame(
                {
                    "timestamp_utc": dates,
                    "station": "beta",
                    "air_temperature_c": [18.0] * 24,
                    "relative_humidity_pct": [55.0] * 24,
                    "wind_speed_kmh": [12.0] * 24,
                    "rain_mm": [0.0] * 24,
                }
            ),
        ],
        ignore_index=True,
    )


# ---------------------------------------------------------------------------
# AC-DQ-1: cleaning-config.json exists and is well-formed
# ---------------------------------------------------------------------------


class TestCleaningConfig:
    """AC-DQ-1: Config file exists with all required sections."""

    def test_config_file_exists(self):
        assert CONFIG_PATH.exists(), "docs/cleaning-config.json must exist"

    def test_config_is_valid_json(self):
        with open(CONFIG_PATH) as f:
            data = json.load(f)
        assert isinstance(data, dict)

    def test_config_has_version(self, cleaning_config):
        assert "version" in cleaning_config
        assert cleaning_config["version"] >= 1

    def test_config_has_date_range(self, cleaning_config):
        assert "date_range" in cleaning_config
        assert "start" in cleaning_config["date_range"]

    def test_config_date_range_start_is_2023_04_01(self, cleaning_config):
        start = cleaning_config["date_range"]["start"]
        assert start.startswith("2023-04-01")

    def test_config_has_value_ranges(self, cleaning_config):
        vr = cleaning_config.get("value_ranges", {})
        assert "air_temperature_c" in vr
        assert "relative_humidity_pct" in vr
        assert "wind_speed_kmh" in vr
        assert "rain_mm" in vr

    def test_config_value_ranges_are_tuples(self, cleaning_config):
        for var, bounds in cleaning_config["value_ranges"].items():
            assert isinstance(bounds, list)
            assert len(bounds) == 2
            assert bounds[0] <= bounds[1]

    def test_config_has_fwi_output_ranges(self, cleaning_config):
        fwi = cleaning_config.get("fwi_output_ranges", {})
        for key in ("ffmc", "dmc", "dc", "isi", "bui", "fwi"):
            assert key in fwi, f"Missing FWI output range for {key}"

    def test_config_has_cross_variable_checks(self, cleaning_config):
        cv = cleaning_config.get("cross_variable_checks", {})
        assert "rain_rh_correlation" in cv

    def test_config_has_rate_of_change(self, cleaning_config):
        roc = cleaning_config.get("rate_of_change", {})
        assert "window_hours" in roc
        assert "max_delta" in roc
        for var in ("air_temperature_c", "relative_humidity_pct", "wind_speed_kmh"):
            assert var in roc["max_delta"]

    def test_config_has_flatline_section(self, cleaning_config):
        fl = cleaning_config.get("flatline", {})
        assert fl.get("enabled") is True
        for var in ("air_temperature_c", "relative_humidity_pct", "wind_speed_kmh"):
            assert var in fl["variables"]
        assert "threshold_hours" in fl

    def test_config_has_enforcement_section(self, cleaning_config):
        enf = cleaning_config.get("enforcement", {})
        assert "default_action" in enf
        assert "actions" in enf
        assert enf["actions"]["flatline"] == "flag_only"


# ---------------------------------------------------------------------------
# AC-DQ-2: enforce_quality() function exists and returns correct types
# ---------------------------------------------------------------------------


class TestEnforceQualitySignature:
    """AC-DQ-2: quality.py implements enforce_quality()."""

    def test_module_importable(self):
        """quality module can be imported."""
        import pea_met_network.quality  # noqa: F401

    def test_enforce_quality_exists(self):
        """enforce_quality function is defined in quality module."""
        from pea_met_network.quality import enforce_quality

        assert callable(enforce_quality)

    def test_enforce_quality_returns_tuple(self, sample_hourly, cleaning_config):
        from pea_met_network.quality import enforce_quality

        result = enforce_quality(sample_hourly, cleaning_config)
        assert isinstance(result, tuple)
        assert len(result) == 2

    def test_enforce_quality_returns_dataframe_and_list(
        self, sample_hourly, cleaning_config
    ):
        from pea_met_network.quality import enforce_quality

        df_out, actions = enforce_quality(sample_hourly, cleaning_config)
        assert isinstance(df_out, pd.DataFrame)
        assert isinstance(actions, list)

    def test_clean_data_passes_through_unchanged(self, sample_hourly, cleaning_config):
        """Clean data (all in range) should not be modified."""
        from pea_met_network.quality import enforce_quality

        df_out, actions = enforce_quality(sample_hourly, cleaning_config)
        assert len(df_out) == len(sample_hourly)
        assert len(actions) == 0


# ---------------------------------------------------------------------------
# Value Range Checks (VR001–VR004)
# ---------------------------------------------------------------------------


class TestValueRangeChecks:
    """Out-of-range values are set to NaN with set_nan action."""

    def test_out_of_range_temperature_set_nan(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=3, freq="h", tz="UTC"),
                "station": ["test"] * 3,
                "air_temperature_c": [20.0, -999.0, 25.0],
                "relative_humidity_pct": [60.0, 60.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0, 15.0],
                "rain_mm": [0.0, 0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert pd.isna(df_out["air_temperature_c"].iloc[1])
        assert df_out["air_temperature_c"].iloc[0] == 20.0
        assert df_out["air_temperature_c"].iloc[2] == 25.0

    def test_out_of_range_humidity_set_nan(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, 20.0],
                "relative_humidity_pct": [60.0, -30.0],
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert pd.isna(df_out["relative_humidity_pct"].iloc[1])

    def test_out_of_range_wind_set_nan(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, 20.0],
                "relative_humidity_pct": [60.0, 60.0],
                "wind_speed_kmh": [15.0, 999.0],
                "rain_mm": [0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert pd.isna(df_out["wind_speed_kmh"].iloc[1])

    def test_out_of_range_rain_set_nan(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, 20.0],
                "relative_humidity_pct": [60.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 600.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert pd.isna(df_out["rain_mm"].iloc[1])

    def test_boundary_values_pass(self, cleaning_config):
        """Values exactly at range boundaries are valid."""
        from pea_met_network.quality import enforce_quality

        vr = cleaning_config["value_ranges"]
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=1, freq="h", tz="UTC"),
                "station": ["test"],
                "air_temperature_c": [vr["air_temperature_c"][0]],
                "relative_humidity_pct": [vr["relative_humidity_pct"][1]],
                "wind_speed_kmh": [vr["wind_speed_kmh"][1]],
                "rain_mm": [vr["rain_mm"][1]],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert len(actions) == 0

    def test_nan_inputs_are_ignored(self, cleaning_config):
        """Existing NaN values should not generate actions."""
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, float("nan")],
                "relative_humidity_pct": [60.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert len(actions) == 0


# ---------------------------------------------------------------------------
# Action Records (structured output)
# ---------------------------------------------------------------------------


class TestActionRecords:
    """Each enforcement action produces a structured record."""

    def test_action_record_structure(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["alpha"] * 2,
                "air_temperature_c": [20.0, -999.0],
                "relative_humidity_pct": [60.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert len(actions) == 1
        rec = actions[0]
        assert rec["station"] == "alpha"
        assert rec["check_type"] == "value_range"
        assert rec["variable"] == "air_temperature_c"
        assert rec["original_value"] == -999.0
        assert rec["action"] == "set_nan"
        assert "rule" in rec

    def test_action_record_has_timestamp(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        ts = pd.Timestamp("2024-06-01T05:00:00Z", tz="UTC")
        df = pd.DataFrame(
            {
                "timestamp_utc": [ts],
                "station": ["test"],
                "air_temperature_c": [-999.0],
                "relative_humidity_pct": [60.0],
                "wind_speed_kmh": [15.0],
                "rain_mm": [0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert actions[0]["timestamp_utc"] == ts.isoformat()

    def test_multiple_violations_multiple_records(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=3, freq="h", tz="UTC"),
                "station": ["test"] * 3,
                "air_temperature_c": [-999.0, 20.0, -999.0],
                "relative_humidity_pct": [60.0, -30.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0, 15.0],
                "rain_mm": [0.0, 0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        # 2 out-of-range temps + 1 out-of-range humidity = 3
        assert len(actions) == 3


# ---------------------------------------------------------------------------
# AC-DQ-7: Quality Flag Column
# ---------------------------------------------------------------------------


class TestQualityFlagsColumn:
    """Output DataFrames contain _quality_flags column."""

    def test_quality_flags_column_added(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, -999.0],
                "relative_humidity_pct": [60.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert "_quality_flags" in df_out.columns

    def test_quality_flags_empty_for_clean_rows(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, 20.0],
                "relative_humidity_pct": [60.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        assert "_quality_flags" in df_out.columns
        # Clean rows should have empty/null quality flags
        assert all(
            pd.isna(df_out["_quality_flags"].iloc[i]) or df_out["_quality_flags"].iloc[i] == "[]"
            for i in range(len(df_out))
        )

    def test_quality_flags_populated_for_violations(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, -999.0],
                "relative_humidity_pct": [60.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        # Row 1 (index 1) should have a quality flag
        flag = df_out["_quality_flags"].iloc[1]
        assert flag is not None and flag != "[]" and not pd.isna(flag)


# ---------------------------------------------------------------------------
# AC-DQ-12: Flatline Detection (flag_only — no mutation)
# ---------------------------------------------------------------------------


class TestFlatlineDetection:
    """Flatline detection flags but does not modify values."""

    def test_flatline_detected(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        # 8 consecutive identical temperature values (threshold is 6)
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=8, freq="h", tz="UTC"),
                "station": ["test"] * 8,
                "air_temperature_c": [22.5] * 8,
                "relative_humidity_pct": [60.0] * 8,
                "wind_speed_kmh": [15.0] * 8,
                "rain_mm": [0.0] * 8,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        flatline_actions = [a for a in actions if a["check_type"] == "flatline"]
        assert len(flatline_actions) > 0

    def test_flatline_does_not_modify_values(self, cleaning_config):
        """flag_only action must not change the data."""
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=8, freq="h", tz="UTC"),
                "station": ["test"] * 8,
                "air_temperature_c": [22.5] * 8,
                "relative_humidity_pct": [60.0] * 8,
                "wind_speed_kmh": [15.0] * 8,
                "rain_mm": [0.0] * 8,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        # No temperature values should be NaN
        assert df_out["air_temperature_c"].notna().all()

    def test_flatline_action_is_flagged(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=8, freq="h", tz="UTC"),
                "station": ["test"] * 8,
                "air_temperature_c": [22.5] * 8,
                "relative_humidity_pct": [60.0] * 8,
                "wind_speed_kmh": [15.0] * 8,
                "rain_mm": [0.0] * 8,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        flatline_actions = [a for a in actions if a["check_type"] == "flatline"]
        for action in flatline_actions:
            assert action["action"] == "flag_only"

    def test_no_flatline_below_threshold(self, cleaning_config):
        """5 identical values should not trigger flatline (threshold=6)."""
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=5, freq="h", tz="UTC"),
                "station": ["test"] * 5,
                "air_temperature_c": [22.5] * 5,
                "relative_humidity_pct": [60.0] * 5,
                "wind_speed_kmh": [15.0] * 5,
                "rain_mm": [0.0] * 5,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        flatline_actions = [a for a in actions if a["check_type"] == "flatline"]
        assert len(flatline_actions) == 0

    def test_flatline_with_nan_gap(self, cleaning_config):
        """NaN gaps should break flatline sequences."""
        from pea_met_network.quality import enforce_quality

        temps = [22.5] * 3 + [float("nan")] + [22.5] * 3
        rh = [60.0] * 3 + [float("nan")] + [60.0] * 3
        wind = [15.0] * 3 + [float("nan")] + [15.0] * 3
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=7, freq="h", tz="UTC"),
                "station": ["test"] * 7,
                "air_temperature_c": temps,
                "relative_humidity_pct": rh,
                "wind_speed_kmh": wind,
                "rain_mm": [0.0] * 7,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        flatline_actions = [a for a in actions if a["check_type"] == "flatline"]
        assert len(flatline_actions) == 0


# ---------------------------------------------------------------------------
# Rate-of-Change Checks (RoC001–RoC003)
# ---------------------------------------------------------------------------


class TestRateOfChange:
    """Rate-of-change violations are detected and set to NaN."""

    def test_temperature_spike_detected(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        temps = [20.0, 20.0, 20.0, 30.0]  # 10°C jump in 1h exceeds 8°C/h threshold
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=4, freq="h", tz="UTC"),
                "station": ["test"] * 4,
                "air_temperature_c": temps,
                "relative_humidity_pct": [60.0] * 4,
                "wind_speed_kmh": [15.0] * 4,
                "rain_mm": [0.0] * 4,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        roc_actions = [a for a in actions if a["check_type"] == "rate_of_change"]
        assert len(roc_actions) > 0
        assert any(a["variable"] == "air_temperature_c" for a in roc_actions)

    def test_humidity_spike_detected(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        humidity = [50.0, 50.0, 90.0]  # 40% jump exceeds 30%/h threshold
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=3, freq="h", tz="UTC"),
                "station": ["test"] * 3,
                "air_temperature_c": [20.0] * 3,
                "relative_humidity_pct": humidity,
                "wind_speed_kmh": [15.0] * 3,
                "rain_mm": [0.0] * 3,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        roc_actions = [a for a in actions if a["check_type"] == "rate_of_change"]
        assert any(a["variable"] == "relative_humidity_pct" for a in roc_actions)

    def test_wind_spike_detected(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        wind = [10.0, 10.0, 60.0]  # 50 km/h jump exceeds 40 km/h/h threshold
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=3, freq="h", tz="UTC"),
                "station": ["test"] * 3,
                "air_temperature_c": [20.0] * 3,
                "relative_humidity_pct": [60.0] * 3,
                "wind_speed_kmh": wind,
                "rain_mm": [0.0] * 3,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        roc_actions = [a for a in actions if a["check_type"] == "rate_of_change"]
        assert any(a["variable"] == "wind_speed_kmh" for a in roc_actions)

    def test_gradual_change_not_flagged(self, cleaning_config):
        """Small changes within threshold should not trigger rate-of-change."""
        from pea_met_network.quality import enforce_quality

        temps = [20.0, 22.0, 24.0, 26.0]  # 2°C/h changes, well within 8°C/h
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=4, freq="h", tz="UTC"),
                "station": ["test"] * 4,
                "air_temperature_c": temps,
                "relative_humidity_pct": [60.0] * 4,
                "wind_speed_kmh": [15.0] * 4,
                "rain_mm": [0.0] * 4,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        roc_actions = [a for a in actions if a["check_type"] == "rate_of_change"]
        assert len(roc_actions) == 0

    def test_roc_across_nan_is_skipped(self, cleaning_config):
        """Rate-of-change should not be calculated across NaN gaps."""
        from pea_met_network.quality import enforce_quality

        temps = [20.0, float("nan"), 35.0]  # NaN breaks the delta calc
        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=3, freq="h", tz="UTC"),
                "station": ["test"] * 3,
                "air_temperature_c": temps,
                "relative_humidity_pct": [60.0] * 3,
                "wind_speed_kmh": [15.0] * 3,
                "rain_mm": [0.0] * 3,
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        roc_actions = [a for a in actions if a["check_type"] == "rate_of_change"]
        assert len(roc_actions) == 0


# ---------------------------------------------------------------------------
# Cross-Variable Checks (CV001: rain/RH correlation)
# ---------------------------------------------------------------------------


class TestCrossVariableChecks:
    """Rain with low humidity is flagged as inconsistent."""

    def test_rain_with_low_humidity_flagged(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, 20.0],
                "relative_humidity_pct": [60.0, 50.0],  # 50% < 70% threshold
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 5.0],  # rain > 0 but RH < 70%
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        cv_actions = [a for a in actions if a["check_type"] == "cross_variable"]
        assert len(cv_actions) > 0

    def test_rain_with_high_humidity_ok(self, cleaning_config):
        """Rain with high humidity should not trigger cross-variable check."""
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, 20.0],
                "relative_humidity_pct": [60.0, 85.0],  # 85% >= 70% threshold
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 5.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        cv_actions = [a for a in actions if a["check_type"] == "cross_variable"]
        assert len(cv_actions) == 0

    def test_no_rain_no_cross_check(self, cleaning_config):
        """No rain means cross-variable check should not fire."""
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, 20.0],
                "relative_humidity_pct": [60.0, 50.0],
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        cv_actions = [a for a in actions if a["check_type"] == "cross_variable"]
        assert len(cv_actions) == 0


# ---------------------------------------------------------------------------
# AC-DQ-4: FWI Output Enforcement
# ---------------------------------------------------------------------------


class TestFWIOutputEnforcement:
    """FWI outputs are validated and out-of-range values set to NaN."""

    def test_fwi_enforce_function_exists(self):
        from pea_met_network.quality import enforce_fwi_outputs

        assert callable(enforce_fwi_outputs)

    def test_ffmc_above_101_set_nan(self, cleaning_config):
        from pea_met_network.quality import enforce_fwi_outputs

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "ffmc": [85.0, 105.0],
                "dmc": [10.0, 10.0],
                "dc": [100.0, 100.0],
                "isi": [5.0, 5.0],
                "bui": [15.0, 15.0],
                "fwi": [8.0, 8.0],
            }
        )
        df_out, actions = enforce_fwi_outputs(df, cleaning_config)
        assert pd.isna(df_out["ffmc"].iloc[1])
        assert df_out["ffmc"].iloc[0] == pytest.approx(85.0)

    def test_ffmc_negative_set_nan(self, cleaning_config):
        from pea_met_network.quality import enforce_fwi_outputs

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=1, freq="h", tz="UTC"),
                "station": ["test"],
                "ffmc": [-1.0],
                "dmc": [10.0],
                "dc": [100.0],
                "isi": [5.0],
                "bui": [15.0],
                "fwi": [8.0],
            }
        )
        df_out, actions = enforce_fwi_outputs(df, cleaning_config)
        assert pd.isna(df_out["ffmc"].iloc[0])

    def test_dmc_negative_set_nan(self, cleaning_config):
        from pea_met_network.quality import enforce_fwi_outputs

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=1, freq="h", tz="UTC"),
                "station": ["test"],
                "ffmc": [85.0],
                "dmc": [-5.0],
                "dc": [100.0],
                "isi": [5.0],
                "bui": [15.0],
                "fwi": [8.0],
            }
        )
        df_out, actions = enforce_fwi_outputs(df, cleaning_config)
        assert pd.isna(df_out["dmc"].iloc[0])

    def test_fwi_negative_set_nan(self, cleaning_config):
        from pea_met_network.quality import enforce_fwi_outputs

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=1, freq="h", tz="UTC"),
                "station": ["test"],
                "ffmc": [85.0],
                "dmc": [10.0],
                "dc": [100.0],
                "isi": [5.0],
                "bui": [15.0],
                "fwi": [-2.0],
            }
        )
        df_out, actions = enforce_fwi_outputs(df, cleaning_config)
        assert pd.isna(df_out["fwi"].iloc[0])

    def test_valid_fwi_passes_through(self, cleaning_config):
        from pea_met_network.quality import enforce_fwi_outputs

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=1, freq="h", tz="UTC"),
                "station": ["test"],
                "ffmc": [85.0],
                "dmc": [15.0],
                "dc": [200.0],
                "isi": [8.0],
                "bui": [20.0],
                "fwi": [10.0],
            }
        )
        df_out, actions = enforce_fwi_outputs(df, cleaning_config)
        assert len(actions) == 0
        assert df_out["ffmc"].iloc[0] == pytest.approx(85.0)
        assert df_out["fwi"].iloc[0] == pytest.approx(10.0)


# ---------------------------------------------------------------------------
# AC-DQ-9: Date Range Truncation
# ---------------------------------------------------------------------------


class TestDateRangeTruncation:
    """Records before 2023-04-01 are excluded."""

    def test_truncate_before_cutoff(self, cleaning_config):
        from pea_met_network.quality import truncate_date_range

        assert callable(truncate_date_range)

    def test_old_records_removed(self, cleaning_config):
        from pea_met_network.quality import truncate_date_range

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.to_datetime(
                    ["2023-03-31T23:00:00Z", "2023-04-01T00:00:00Z", "2023-04-01T01:00:00Z"],
                    utc=True,
                ),
                "station": ["test"] * 3,
                "air_temperature_c": [10.0, 11.0, 12.0],
            }
        )
        df_out = truncate_date_range(df, cleaning_config)
        assert len(df_out) == 2
        assert df_out["timestamp_utc"].min() >= pd.Timestamp("2023-04-01", tz="UTC")

    def test_all_records_after_cutoff_preserved(self, cleaning_config):
        from pea_met_network.quality import truncate_date_range

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=10, freq="h", tz="UTC"),
                "station": ["test"] * 10,
                "air_temperature_c": [20.0] * 10,
            }
        )
        df_out = truncate_date_range(df, cleaning_config)
        assert len(df_out) == 10

    def test_empty_dataframe_returns_empty(self, cleaning_config):
        from pea_met_network.quality import truncate_date_range

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.Series([], dtype="datetime64[ns, UTC]"),
                "station": [],
                "air_temperature_c": [],
            }
        )
        df_out = truncate_date_range(df, cleaning_config)
        assert len(df_out) == 0


# ---------------------------------------------------------------------------
# AC-DQ-5: Quality Enforcement Report
# ---------------------------------------------------------------------------


class TestQualityEnforcementReport:
    """enforce_quality produces a report-compatible action list."""

    def test_actions_are_serializable(self, cleaning_config):
        """All action records must be JSON-serializable."""
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=3, freq="h", tz="UTC"),
                "station": ["test"] * 3,
                "air_temperature_c": [20.0, -999.0, 25.0],
                "relative_humidity_pct": [60.0, 60.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0, 15.0],
                "rain_mm": [0.0, 0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        # Must not raise
        json_str = json.dumps(actions)
        assert len(json_str) > 0

    def test_actions_contain_required_fields(self, cleaning_config):
        from pea_met_network.quality import enforce_quality

        df = pd.DataFrame(
            {
                "timestamp_utc": pd.date_range("2024-06-01", periods=2, freq="h", tz="UTC"),
                "station": ["test"] * 2,
                "air_temperature_c": [20.0, -999.0],
                "relative_humidity_pct": [60.0, 60.0],
                "wind_speed_kmh": [15.0, 15.0],
                "rain_mm": [0.0, 0.0],
            }
        )
        df_out, actions = enforce_quality(df, cleaning_config)
        required = {"station", "timestamp_utc", "check_type", "variable", "original_value", "action", "rule"}
        for action in actions:
            assert required.issubset(action.keys()), f"Missing fields: {required - action.keys()}"
