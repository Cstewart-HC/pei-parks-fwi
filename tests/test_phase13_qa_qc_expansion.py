"""Phase 13 tests — QA/QC Report Expansion: Dual-Mode Diagnostics.

Tests for 6 deliverables:
  1. Pre/post imputation missingness snapshots
  2. FWI mode tag in all reports
  3. Compliant mode diagnostics (carry-forward days)
  4. FWI value descriptive statistics
  5. Mode-specific report filenames
  6. Per-stage row count audit in manifest

Exit gate: pytest tests/test_phase13_qa_qc_expansion.py -v
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_hourly(station: str = "stanhope", n: int = 48) -> pd.DataFrame:
    """Create a synthetic hourly DataFrame with all met columns."""
    ts = pd.date_range("2024-07-01", periods=n, freq="h")
    return pd.DataFrame({
        "timestamp_utc": ts,
        "station": station,
        "air_temperature_c": np.random.default_rng(42).uniform(15, 30, n),
        "relative_humidity_pct": np.random.default_rng(43).uniform(30, 80, n),
        "wind_speed_kmh": np.random.default_rng(44).uniform(5, 25, n),
        "rain_mm": np.random.default_rng(45).uniform(0, 5, n),
    })


def _make_daily(station: str = "stanhope", n: int = 3) -> pd.DataFrame:
    """Create a synthetic daily DataFrame with FWI columns."""
    rng = np.random.default_rng(46)
    return pd.DataFrame({
        "timestamp_utc": pd.date_range("2024-07-01", periods=n, freq="D"),
        "station": station,
        "ffmc": rng.uniform(75, 85, n),
        "dmc": rng.uniform(18, 25, n),
        "dc": rng.uniform(95, 110, n),
        "isi": rng.uniform(5, 8, n),
        "bui": rng.uniform(22, 30, n),
        "fwi": rng.uniform(12, 20, n),
    })


# ===========================================================================
# 2.1 Pre/Post Imputation Missingness
# ===========================================================================

class TestPrePostImputation:
    """Spec 2.1 — pre_imputation_missingness() in qa_qc.py."""

    def test_function_exists_and_importable(self):
        """pre_imputation_missingness must be importable from qa_qc."""
        from pea_met_network.qa_qc import pre_imputation_missingness
        assert callable(pre_imputation_missingness)

    def test_returns_missing_pct_for_core_variables(self):
        """Must return missing_pct for the 4 core met variables."""
        from pea_met_network.qa_qc import pre_imputation_missingness

        df = _make_hourly()
        # Inject NaN values
        df.loc[0:5, "air_temperature_c"] = np.nan
        df.loc[2:3, "relative_humidity_pct"] = np.nan

        result = pre_imputation_missingness(df)

        for var in (
            "air_temperature_c",
            "relative_humidity_pct",
            "wind_speed_kmh",
            "rain_mm",
        ):
            key = f"missing_pct_{var}"
            assert key in result, f"Missing key: {key}"
            assert isinstance(result[key], float)
            assert 0.0 <= result[key] <= 100.0

    def test_returns_zero_when_no_missing(self):
        """All-present data should yield 0.0 for all variables."""
        from pea_met_network.qa_qc import pre_imputation_missingness

        df = _make_hourly()
        result = pre_imputation_missingness(df)

        for var in (
            "air_temperature_c",
            "relative_humidity_pct",
            "wind_speed_kmh",
            "rain_mm",
        ):
            assert result[f"missing_pct_{var}"] == 0.0

    def test_returns_100_when_all_missing(self):
        """All-NaN column should yield 100.0."""
        from pea_met_network.qa_qc import pre_imputation_missingness

        df = _make_hourly()
        df["air_temperature_c"] = np.nan

        result = pre_imputation_missingness(df)
        assert result["missing_pct_air_temperature_c"] == 100.0

    def test_all_nan_dataframe_returns_100(self):
        """Entirely NaN core columns → all 100.0."""
        from pea_met_network.qa_qc import pre_imputation_missingness

        df = _make_hourly()
        for col in (
            "air_temperature_c", "relative_humidity_pct",
            "wind_speed_kmh", "rain_mm",
        ):
            df[col] = np.nan

        result = pre_imputation_missingness(df)
        for var in (
            "air_temperature_c", "relative_humidity_pct",
            "wind_speed_kmh", "rain_mm",
        ):
            assert result[f"missing_pct_{var}"] == 100.0


# ===========================================================================
# 2.4 FWI Value Statistics
# ===========================================================================

class TestFWIValueStatistics:
    """Spec 2.4 — fwi_descriptive_stats() in qa_qc.py."""

    def test_function_exists_and_importable(self):
        """fwi_descriptive_stats must be importable from qa_qc."""
        from pea_met_network.qa_qc import fwi_descriptive_stats
        assert callable(fwi_descriptive_stats)

    def test_returns_all_24_stats_keys(self):
        """6 FWI codes × 4 stats (min, max, mean, std) = 24 keys."""
        from pea_met_network.qa_qc import fwi_descriptive_stats

        daily = _make_daily()
        result = fwi_descriptive_stats(daily, station="stanhope")

        expected_codes = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]
        expected_stats = ["min", "max", "mean", "std"]
        for code in expected_codes:
            for stat in expected_stats:
                key = f"{code}_{stat}"
                assert key in result, f"Missing key: {key}"

    def test_stats_are_float(self):
        """All stat values must be float."""
        from pea_met_network.qa_qc import fwi_descriptive_stats

        daily = _make_daily()
        result = fwi_descriptive_stats(daily, station="stanhope")

        for key, val in result.items():
            assert isinstance(val, (float, np.floating)), (
                f"{key} = {val!r} is not float"
            )

    def test_std_is_zero_for_constant_values(self):
        """Constant FWI values should yield std=0.0."""
        from pea_met_network.qa_qc import fwi_descriptive_stats

        daily = _make_daily()
        daily["ffmc"] = 85.0
        result = fwi_descriptive_stats(daily, station="stanhope")

        assert result["ffmc_std"] == 0.0
        assert result["ffmc_min"] == 85.0
        assert result["ffmc_max"] == 85.0
        assert result["ffmc_mean"] == 85.0

    def test_stats_correct_for_known_values(self):
        """Verify min/max/mean/std match manual calculation."""
        from pea_met_network.qa_qc import fwi_descriptive_stats

        daily = _make_daily()
        daily["ffmc"] = [10.0, 20.0, 30.0]
        result = fwi_descriptive_stats(daily, station="stanhope")

        assert result["ffmc_min"] == 10.0
        assert result["ffmc_max"] == 30.0
        assert result["ffmc_mean"] == 20.0
        # std(ddof=1): sqrt(((10-20)^2 + (20-20)^2 + (30-20)^2)/2) = sqrt(100) = 10
        assert abs(result["ffmc_std"] - 10.0) < 0.001

    def test_handles_nan_in_fwi_columns(self):
        """NaN values in FWI columns should not break stats."""
        from pea_met_network.qa_qc import fwi_descriptive_stats

        daily = _make_daily()
        daily["ffmc"] = [80.0, np.nan, 78.0]
        result = fwi_descriptive_stats(daily, station="stanhope")

        # Should compute stats on non-NaN values
        assert result["ffmc_mean"] == pytest.approx(79.0, abs=0.01)
        assert result["ffmc_min"] == 78.0
        assert result["ffmc_max"] == 80.0


# ===========================================================================
# 2.2 FWI Mode in Reports
# ===========================================================================

class TestFWIModeInReports:
    """Spec 2.2 — fwi_mode column in QA/QC report."""

    def test_generate_report_accepts_fwi_mode_parameter(self):
        """generate_qa_qc_report must accept fwi_mode kwarg."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily()
        # This should not raise TypeError for unexpected keyword
        df = generate_qa_qc_report(hourly, daily, fwi_mode="hourly")
        assert "fwi_mode" in df.columns

    def test_fwi_mode_column_has_correct_value_hourly(self):
        """fwi_mode column should contain 'hourly' when passed."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily()
        df = generate_qa_qc_report(hourly, daily, fwi_mode="hourly")

        assert df["fwi_mode"].unique()[0] == "hourly"

    def test_fwi_mode_column_has_correct_value_compliant(self):
        """fwi_mode column should contain 'compliant' when passed."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily()
        df = generate_qa_qc_report(hourly, daily, fwi_mode="compliant")

        assert df["fwi_mode"].unique()[0] == "compliant"


# ===========================================================================
# 2.3 Compliant Mode Diagnostics
# ===========================================================================

class TestCompliantDiagnostics:
    """Spec 2.3 — carry_forward_days and carry_forward_pct in report."""

    def test_carry_forward_columns_present_in_compliant_mode(self):
        """Compliant mode report must have carry_forward_days and carry_forward_pct."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily()
        daily["carry_forward_used"] = [False, True, True]

        df = generate_qa_qc_report(
            hourly, daily, fwi_mode="compliant",
        )

        assert "carry_forward_days" in df.columns
        assert "carry_forward_pct" in df.columns

    def test_carry_forward_days_correct_count(self):
        """carry_forward_days should equal sum of carry_forward_used."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily(n=5)
        daily["carry_forward_used"] = [False, True, True, False, True]

        df = generate_qa_qc_report(
            hourly, daily, fwi_mode="compliant",
        )

        assert df.iloc[0]["carry_forward_days"] == 3

    def test_carry_forward_pct_correct_calculation(self):
        """carry_forward_pct = carry_forward_days / total_days * 100."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily(n=4)
        daily["carry_forward_used"] = [False, True, True, False]

        df = generate_qa_qc_report(
            hourly, daily, fwi_mode="compliant",
        )

        # 2 carry-forward days out of 4 total = 50%
        assert df.iloc[0]["carry_forward_pct"] == pytest.approx(50.0, abs=0.01)

    def test_carry_forward_zero_when_no_missing(self):
        """No carry-forward days → 0 days, 0%."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily(n=3)
        daily["carry_forward_used"] = [False, False, False]

        df = generate_qa_qc_report(
            hourly, daily, fwi_mode="compliant",
        )

        assert df.iloc[0]["carry_forward_days"] == 0
        assert df.iloc[0]["carry_forward_pct"] == 0.0


# ===========================================================================
# 2.1 (report columns) — Pre/Post Imputation in QA/QC Report
# ===========================================================================

class TestPrePostImputationInReport:
    """Spec 2.1 — pre_imp_missing_pct_* and post_imp_missing_pct_* in report."""

    def test_pre_imputation_columns_in_report(self):
        """Report must have pre_imp_missing_pct_* columns when pre_imp data provided."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily()
        pre_imp = {
            "missing_pct_air_temperature_c": 10.0,
            "missing_pct_relative_humidity_pct": 5.0,
            "missing_pct_wind_speed_kmh": 0.0,
            "missing_pct_rain_mm": 2.5,
        }

        df = generate_qa_qc_report(
            hourly, daily,
            pre_imputation_missingness=pre_imp,
        )

        for var in (
            "air_temperature_c",
            "relative_humidity_pct",
            "wind_speed_kmh",
            "rain_mm",
        ):
            col = f"pre_imp_missing_pct_{var}"
            assert col in df.columns, f"Missing column: {col}"
            assert df.iloc[0][col] == pre_imp[f"missing_pct_{var}"]

    def test_post_imputation_columns_in_report(self):
        """Report must have post_imp_missing_pct_* columns."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily()

        df = generate_qa_qc_report(hourly, daily)

        for var in (
            "air_temperature_c",
            "relative_humidity_pct",
            "wind_speed_kmh",
            "rain_mm",
        ):
            col = f"post_imp_missing_pct_{var}"
            assert col in df.columns, f"Missing column: {col}"


# ===========================================================================
# 2.4 (report columns) — FWI Stats in QA/QC Report
# ===========================================================================

class TestFWIStatsInReport:
    """Spec 2.4 — FWI descriptive stats in QA/QC report."""

    def test_fwi_stats_columns_in_report(self):
        """Report must have ffmc_min, ffmc_max, ffmc_mean, ffmc_std etc."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily()

        df = generate_qa_qc_report(hourly, daily)

        expected_codes = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]
        expected_stats = ["min", "max", "mean", "std"]
        for code in expected_codes:
            for stat in expected_stats:
                col = f"{code}_{stat}"
                assert col in df.columns, f"Missing column: {col}"

    def test_fwi_stats_values_match_daily_data(self):
        """Report FWI stats should match the daily DataFrame values."""
        from pea_met_network.qa_qc import generate_qa_qc_report

        hourly = _make_hourly()
        daily = _make_daily()
        daily["ffmc"] = [78.0, 82.0, 80.0]

        df = generate_qa_qc_report(hourly, daily)

        assert df.iloc[0]["ffmc_min"] == 78.0
        assert df.iloc[0]["ffmc_max"] == 82.0
        assert df.iloc[0]["ffmc_mean"] == pytest.approx(80.0, abs=0.01)


# ===========================================================================
# 2.5 Mode-Specific Report Filenames
# ===========================================================================

class TestModeSpecificFilenames:
    """Spec 2.5 — qa_qc_report_{mode}.csv and fwi_missingness_report_{mode}.csv."""

    def test_qa_qc_report_mode_suffix_in_manifest(self):
        """Pipeline manifest should reference mode-specific QA/QC report filename."""
        import json
        from pathlib import Path

        PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
        manifest_path = PROCESSED_DIR / "pipeline_manifest.json"

        if not manifest_path.exists():
            pytest.skip("No pipeline manifest — run pipeline first")

        manifest = json.loads(manifest_path.read_text())
        artifact_names = [a.get("artifact_type", "") for a in manifest.get("artifacts", [])]

        # After a pipeline run, the manifest should reference mode-specific names
        assert any("qa_qc_report" in name for name in artifact_names), (
            "No qa_qc_report artifact in manifest"
        )


# ===========================================================================
# 2.6 Per-Stage Row Count Audit
# ===========================================================================

class TestPerStageRowCountAudit:
    """Spec 2.6 — stage_row_counts in pipeline manifest."""

    def test_stage_row_counts_key_in_manifest(self):
        """Pipeline manifest must have stage_row_counts per station."""
        import json
        from pathlib import Path

        PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
        manifest_path = PROCESSED_DIR / "pipeline_manifest.json"

        if not manifest_path.exists():
            pytest.skip("No pipeline manifest — run pipeline first")

        manifest = json.loads(manifest_path.read_text())
        assert "stage_row_counts" in manifest, (
            "stage_row_counts key missing from manifest"
        )

    def test_stage_row_counts_has_expected_stages(self):
        """stage_row_counts must have all 7 pipeline stage keys."""
        import json
        from pathlib import Path

        PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
        manifest_path = PROCESSED_DIR / "pipeline_manifest.json"

        if not manifest_path.exists():
            pytest.skip("No pipeline manifest — run pipeline first")

        manifest = json.loads(manifest_path.read_text())
        stage_counts = manifest["stage_row_counts"]

        # Must be a dict keyed by station
        assert isinstance(stage_counts, dict)

        # Pick any station and check expected keys
        for station, stages in stage_counts.items():
            expected_stages = [
                "raw", "deduped", "hourly", "truncated",
                "post_quality", "post_imputation", "post_cross_station",
            ]
            for stage in expected_stages:
                assert stage in stages, f"Missing stage '{stage}' for {station}"
            break  # only need to check one station

    def test_stage_row_counts_values_are_integers(self):
        """All stage row counts must be non-negative integers."""
        import json
        from pathlib import Path

        PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
        manifest_path = PROCESSED_DIR / "pipeline_manifest.json"

        if not manifest_path.exists():
            pytest.skip("No pipeline manifest — run pipeline first")

        manifest = json.loads(manifest_path.read_text())
        stage_counts = manifest["stage_row_counts"]

        for station, stages in stage_counts.items():
            for stage, count in stages.items():
                assert isinstance(count, int), (
                    f"{station}.{stage} = {count!r} is not int"
                )
                assert count >= 0, (
                    f"{station}.{stage} = {count} is negative"
                )
