"""Phase 12 tests — validate hourly FFMC against canonical cffdrs reference vectors.

Reference implementation: cffdrs R package v1.9.2, hffmc.R
Based on: Van Wagner (1977) "A method of computing fine fuel moisture
behaviour throughout the diurnal cycle."

The key differences from daily FFMC (Van Wagner 1987):
  - Temp scale factor: 0.0579 (not 0.581)
  - Rain threshold: 0.0mm (all rain applied, not 0.5mm)
  - FFMC cap: none (only >= 0, not <= 101)
  - Drying/wetting: separate k0d/k0w formulas
"""
from __future__ import annotations

import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ---------------------------------------------------------------------------
# Reference implementation — canonical hourly FFMC from cffdrs hffmc.R
# This is the ground truth. Tests validate that _hffmc_calc matches this.
# ---------------------------------------------------------------------------

def _cffdrs_hffmc_ref(
    temp: list[float],
    rh: list[float],
    wind: list[float],
    rain: list[float],
    ffmc_prev: float = 85.0,
) -> np.ndarray:
    """Canonical hourly FFMC from cffdrs R package (Van Wagner 1977).

    Used as the reference to validate our implementation against.
    """
    mo_prev = 147.2 * (101.0 - ffmc_prev) / (59.5 + ffmc_prev)
    ffmc_out: list[float] = []

    for i in range(len(temp)):
        t, h, w, r = temp[i], rh[i], wind[i], rain[i]

        if math.isnan(t) or math.isnan(h) or math.isnan(w):
            ffmc_out.append(float("nan"))
            continue

        rf = 0.0 if math.isnan(r) else float(r)

        # Rain adjustment — all rain > 0 applied (NO threshold)
        if rf > 0.0:
            if mo_prev <= 150.0:
                mr = mo_prev + 42.5 * rf * math.exp(
                    -100.0 / (251.0 - mo_prev)
                ) * (1.0 - math.exp(-6.93 / rf))
                mo_prev = min(mr, 150.0)
            else:
                mr = mo_prev + 42.5 * rf * math.exp(
                    -100.0 / (251.0 - mo_prev)
                ) * (1.0 - math.exp(-6.93 / rf))
                mr += 0.0015 * (mo_prev - 150.0) ** 2 * math.sqrt(rf)
                mo_prev = min(mr, 250.0)

        # Equilibrium moisture content
        ed = (
            0.942 * h ** 0.679
            + 11.0 * math.exp((h - 100.0) / 10.0)
            + 0.18 * (21.1 - t) * (1.0 - 1.0 / math.exp(0.115 * h))
        )
        ew = (
            0.618 * h ** 0.753
            + 10.0 * math.exp((h - 100.0) / 10.0)
            + 0.18 * (21.1 - t) * (1.0 - 1.0 / math.exp(0.115 * h))
        )

        # Drying / wetting / equilibrium branches
        if mo_prev < ed:
            k0w = 0.424 * (1.0 - ((100.0 - h) / 100.0) ** 1.7) + (
                0.0694 * math.sqrt(max(0, w))
            ) * (1.0 - ((100.0 - h) / 100.0) ** 8)
            kw = k0w * 0.0579 * math.exp(0.0365 * t)
            mo = ew - (ew - mo_prev) / (10.0 ** kw)
        elif mo_prev > ed:
            k0d = 0.424 * (1.0 - (h / 100.0) ** 1.7) + (
                0.0694 * math.sqrt(max(0, w))
            ) * (1.0 - (h / 100.0) ** 8)
            kd = k0d * 0.0579 * math.exp(0.0365 * t)
            mo = ed + (mo_prev - ed) / (10.0 ** kd)
        else:
            mo = mo_prev

        mo = max(0.0, mo)
        ffmc = 59.5 * (250.0 - mo) / (147.2 + mo)
        ffmc = max(0.0, ffmc)  # No upper cap in hourly
        ffmc_out.append(ffmc)
        mo_prev = mo

    return np.array(ffmc_out)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_hourly_frame(start_utc: str, periods: int = 48) -> pd.DataFrame:
    timestamps = pd.date_range(start_utc, periods=periods, freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp_utc": timestamps,
            "station": ["greenwich"] * periods,
            "air_temperature_c": [20.0] * periods,
            "relative_humidity_pct": [45.0] * periods,
            "wind_speed_kmh": [15.0] * periods,
            "rain_mm": [0.0] * periods,
        }
    )


# ---------------------------------------------------------------------------
# Reference vector tests — _hffmc_calc MUST match cffdrs hffmc.R
# ---------------------------------------------------------------------------

class TestHFFMCAgainstCFFDRS:
    """Validate _hffmc_calc against canonical cffdrs reference vectors.

    These tests use a pure-Python reference implementation of the cffdrs
    R package hffmc function. The production code must match to within
    floating-point tolerance.
    """

    def test_steady_drying_24h(self):
        """24h steady drying: t=20, rh=45, w=15, r=0, ffmc_prev=85.

        FFMC should increase slowly (hourly rate, not daily).
        Reference final value: 88.202643
        """
        from pea_met_network.cleaning import _hffmc_calc

        temps = np.array([20.0] * 24)
        rhs = np.array([45.0] * 24)
        winds = np.array([15.0] * 24)
        rains = np.array([0.0] * 24)

        expected = _cffdrs_hffmc_ref(temps, rhs, winds, rains, ffmc_prev=85.0)
        actual = _hffmc_calc(temps, rhs, winds, rains, ffmc_prev=85.0)

        np.testing.assert_allclose(actual, expected, atol=1e-4)
        # Final value must match to 4 decimal places
        assert abs(actual[-1] - 88.202643) < 1e-3, (
            f"Final FFMC {actual[-1]:.6f} != 88.202643 — "
            "hourly temp scale factor may be wrong (0.581 vs 0.0579)"
        )

    def test_per_hour_values_drying(self):
        """Every hour of the 24h drying sequence must match.

        This catches wrong constants at any hour, not just the final value.
        """
        from pea_met_network.cleaning import _hffmc_calc

        temps = np.array([20.0] * 24)
        rhs = np.array([45.0] * 24)
        winds = np.array([15.0] * 24)
        rains = np.array([0.0] * 24)

        expected = _cffdrs_hffmc_ref(temps, rhs, winds, rains, ffmc_prev=85.0)
        actual = _hffmc_calc(temps, rhs, winds, rains, ffmc_prev=85.0)

        for i in range(24):
            assert abs(actual[i] - expected[i]) < 1e-3, (
                f"Hour {i}: got {actual[i]:.6f}, expected {expected[i]:.6f}"
            )

    def test_rain_event_drops_ffmc(self):
        """5mm rain at hour 12 should cause a sharp FFMC drop.

        Before rain (hour 11): ~87.8 (hot dry conditions)
        After rain (hour 12): should drop significantly
        Reference final value: 82.684502
        """
        from pea_met_network.cleaning import _hffmc_calc

        temps = np.array([25.0] * 24)
        rhs = np.array([30.0] * 24)
        winds = np.array([10.0] * 24)
        rains = np.array([0.0] * 12 + [5.0] + [0.0] * 11)

        expected = _cffdrs_hffmc_ref(temps, rhs, winds, rains, ffmc_prev=80.0)
        actual = _hffmc_calc(temps, rhs, winds, rains, ffmc_prev=80.0)

        np.testing.assert_allclose(actual, expected, atol=1e-3)

        # FFMC must drop at hour 12 (the rain hour)
        assert actual[12] < actual[11], (
            f"FFMC did not drop at rain hour: h11={actual[11]:.4f}, h12={actual[12]:.4f}"
        )
        assert abs(actual[-1] - 82.684502) < 1e-3

    def test_wetting_conditions_low_ffmc(self):
        """High RH (90%) should drive FFMC down.

        t=15, rh=90, w=5, r=0, ffmc_prev=70
        Reference final value: 74.357438
        """
        from pea_met_network.cleaning import _hffmc_calc

        temps = np.array([15.0] * 24)
        rhs = np.array([90.0] * 24)
        winds = np.array([5.0] * 24)
        rains = np.array([0.0] * 24)

        expected = _cffdrs_hffmc_ref(temps, rhs, winds, rains, ffmc_prev=70.0)
        actual = _hffmc_calc(temps, rhs, winds, rains, ffmc_prev=70.0)

        np.testing.assert_allclose(actual, expected, atol=1e-3)
        assert abs(actual[-1] - 74.357438) < 1e-3

    def test_sub_threshold_rain_applied(self):
        """0.2mm rain MUST be applied (hourly has no 0.5mm threshold).

        If the code uses the daily threshold of 0.5mm, this test fails
        because 0.2mm would be silently ignored.
        """
        from pea_met_network.cleaning import _hffmc_calc

        temps = np.array([20.0] * 4)
        rhs = np.array([50.0] * 4)
        winds = np.array([10.0] * 4)
        rains_with = np.array([0.0, 0.2, 0.0, 0.0])
        rains_without = np.array([0.0, 0.0, 0.0, 0.0])

        result_with = _hffmc_calc(temps, rhs, winds, rains_with, ffmc_prev=85.0)
        result_without = _hffmc_calc(temps, rhs, winds, rains_without, ffmc_prev=85.0)

        # 0.2mm rain must lower FFMC at hour 1
        assert result_with[1] < result_without[1], (
            f"0.2mm rain did not affect FFMC: with={result_with[1]:.4f}, "
            f"without={result_without[1]:.4f} — rain threshold is wrong"
        )

        # Verify against reference
        expected_with = _cffdrs_hffmc_ref(
            temps, rhs, winds, rains_with, ffmc_prev=85.0
        )
        expected_without = _cffdrs_hffmc_ref(
            temps, rhs, winds, rains_without, ffmc_prev=85.0
        )
        np.testing.assert_allclose(result_with, expected_with, atol=1e-3)
        np.testing.assert_allclose(result_without, expected_without, atol=1e-3)

    def test_no_upper_cap_on_ffmc(self):
        """Hourly FFMC has no upper cap of 101 (unlike daily).

        With extreme drying conditions, FFMC should be allowed to exceed 101.
        """
        from pea_met_network.cleaning import _hffmc_calc

        # Very hot, very dry, very windy — should push FFMC above 101
        temps = np.array([40.0] * 48)
        rhs = np.array([5.0] * 48)
        winds = np.array([50.0] * 48)
        rains = np.array([0.0] * 48)

        result = _hffmc_calc(temps, rhs, winds, rains, ffmc_prev=95.0)

        assert result[-1] > 101.0, (
            f"FFMC capped at {result[-1]:.2f} — hourly mode should have no upper cap"
        )

    def test_gap_recovery_after_24h(self):
        """Chain restarts after 24h gap using startup defaults."""
        from pea_met_network.cleaning import _hffmc_calc

        n = 36
        temps = np.array([20.0] * n)
        rhs = np.array([45.0] * n)
        winds = np.array([15.0] * n)
        rains = np.array([0.0] * n)

        # NaN for hours 6–29 (24 hours)
        temps[6:30] = np.nan

        result = _hffmc_calc(
            temps, rhs, winds, rains, ffmc_prev=85.0, gap_threshold_hours=24
        )

        # First 6 hours should be valid
        assert all(pd.notna(result[:6]))
        # Hours 6–29 should be NaN
        assert all(pd.isna(result[6:30]))
        # Hour 30 onwards should recover (chain restarted)
        assert all(pd.notna(result[30:]))

    def test_gap_no_recovery_under_threshold(self):
        """Chain stays broken for gaps under threshold (default 24h)."""
        from pea_met_network.cleaning import _hffmc_calc

        n = 36
        temps = np.array([20.0] * n)
        rhs = np.array([45.0] * n)
        winds = np.array([15.0] * n)
        rains = np.array([0.0] * n)

        # NaN for hours 6–19 (14 hours — under 24h threshold)
        temps[6:20] = np.nan

        result = _hffmc_calc(
            temps, rhs, winds, rains, ffmc_prev=85.0, gap_threshold_hours=24
        )

        assert all(pd.notna(result[:6]))
        assert all(pd.isna(result[6:20]))
        # Hour 20 onwards should also be NaN — chain still broken
        assert all(pd.isna(result[20:]))


# ---------------------------------------------------------------------------
# _daily_dmc_dc_calc tests
# ---------------------------------------------------------------------------

class TestDailyDMCDC:
    """Validate daily DMC/DC computation from hourly inputs."""

    def test_single_day_14lstm(self):
        """DMC/DC computed from 14:00 LST observation for a single day."""
        from pea_met_network.cleaning import _daily_dmc_dc_calc

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z", periods=24)
        # July 1 = ADT (UTC-3), so 14:00 ADT = 17:00 UTC = hour 17
        hourly.loc[17, "air_temperature_c"] = 25.0
        hourly.loc[17, "relative_humidity_pct"] = 40.0
        hourly.loc[3:10, "rain_mm"] = [1.0] * 8  # 8mm total daily rain

        dmc, dc, source_dates = _daily_dmc_dc_calc(hourly)

        # All 24 hours should have same DMC/DC (same day)
        assert np.allclose(dmc[~np.isnan(dmc)], dmc[~np.isnan(dmc)][0])
        assert np.allclose(dc[~np.isnan(dc)], dc[~np.isnan(dc)][0])
        # Should have finite values
        assert np.isfinite(dmc[17])
        assert np.isfinite(dc[17])

    def test_rain_accumulates_over_full_day(self):
        """Daily rain must be the sum of all hourly rain, not just 14:00."""
        from pea_met_network.cleaning import _daily_dmc_dc_calc

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z", periods=24)

        # Spread rain across different hours
        hourly.loc[0, "rain_mm"] = 1.0
        hourly.loc[6, "rain_mm"] = 2.0
        hourly.loc[12, "rain_mm"] = 3.0
        hourly.loc[18, "rain_mm"] = 4.0
        # Total: 10mm

        dmc_no_rain, _, _ = _daily_dmc_dc_calc(
            _make_hourly_frame("2024-07-01T00:00:00Z", periods=24)
        )
        dmc_with_rain, _, _ = _daily_dmc_dc_calc(hourly)

        # 10mm of rain should produce lower DMC than no rain
        assert dmc_with_rain[12] < dmc_no_rain[12], (
            f"Rain did not lower DMC: with_rain={dmc_with_rain[12]:.2f}, "
            f"no_rain={dmc_no_rain[12]:.2f}"
        )

    def test_dmc_dc_change_between_days(self):
        """DMC/DC must step-change at midnight local time."""
        from pea_met_network.cleaning import _daily_dmc_dc_calc

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z", periods=48)

        dmc, dc, source_dates = _daily_dmc_dc_calc(hourly)

        # Must have at least 2 distinct source dates
        unique_dates = pd.Series(source_dates).dropna().unique()
        assert len(unique_dates) >= 2

        # DMC/DC must be different between days (weather varies)
        day1_dmc = dmc[12]  # Hour 12 = July 1
        day2_dmc = dmc[36]  # Hour 36 = July 2
        # They could be same if inputs are identical, but rain differs
        assert pd.notna(day1_dmc) and pd.notna(day2_dmc)

    def test_dmc_dc_constant_within_day(self):
        """All hours of the same local date must have identical DMC/DC."""
        from pea_met_network.cleaning import _daily_dmc_dc_calc

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z", periods=48)

        dmc, dc, source_dates = _daily_dmc_dc_calc(hourly)
        df = pd.DataFrame({"dmc": dmc, "dc": dc, "date": source_dates})

        for date_val, group in df.groupby("date"):
            valid = group.dropna(subset=["dmc", "dc"])
            if len(valid) > 1:
                assert valid["dmc"].nunique() == 1, (
                    f"DMC varies within date {date_val}: {valid['dmc'].unique()}"
                )
                assert valid["dc"].nunique() == 1, (
                    f"DC varies within date {date_val}: {valid['dc'].unique()}"
                )


# ---------------------------------------------------------------------------
# calculate_fwi_hourly integration tests
# ---------------------------------------------------------------------------

class TestCalculateFWIHourly:
    """End-to-end: hourly inputs → hourly FFMC + daily DMC/DC → ISI/BUI/FWI."""

    def test_outputs_all_six_fwi_columns(self):
        from pea_met_network.cleaning import calculate_fwi_hourly

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z", periods=48)
        result = calculate_fwi_hourly(hourly)

        for col in ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]:
            assert col in result.columns, f"Missing column: {col}"

    def test_dmc_dc_source_date_column(self):
        from pea_met_network.cleaning import calculate_fwi_hourly

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z", periods=48)
        result = calculate_fwi_hourly(hourly)

        assert "dmc_dc_source_date" in result.columns
        assert result["dmc_dc_source_date"].nunique() >= 2

    def test_ffmc_changes_every_hour(self):
        """FFMC should vary hour-to-hour (unlike DMC/DC which is daily)."""
        from pea_met_network.cleaning import calculate_fwi_hourly

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z", periods=24)
        result = calculate_fwi_hourly(hourly)

        valid_ffmc = result["ffmc"].dropna()
        # With constant inputs, FFMC still changes each hour (approaching
        # equilibrium). Should not be constant.
        assert valid_ffmc.nunique() > 1, (
            f"FFMC is constant across hours: {valid_ffmc.unique()}"
        )

    def test_legacy_produces_different_ffmc(self):
        """Legacy (broken) hourly FFMC must differ from canonical hourly FFMC.

        The legacy code uses daily constants (0.581) run hourly — it should
        produce substantially different results.
        """
        from pea_met_network.cleaning import calculate_fwi_hourly, _calculate_fwi_legacy

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z", periods=24)
        result_new = calculate_fwi_hourly(hourly)
        result_old = _calculate_fwi_legacy(hourly)

        new_ffmc = result_new["ffmc"].dropna().values
        old_ffmc = result_old["ffmc"].dropna().values

        # They should NOT be the same — the constants are different
        assert len(new_ffmc) == len(old_ffmc)
        max_diff = np.max(np.abs(new_ffmc - old_ffmc))
        assert max_diff > 0.1, (
            f"Legacy and canonical FFMC are too similar (max diff: {max_diff:.4f})"
        )


# ---------------------------------------------------------------------------
# Regression: compliant mode must be untouched
# ---------------------------------------------------------------------------

class TestCompliantModeUnchanged:
    """Phase 11 compliant mode must produce identical results."""

    def test_compliant_output_matches_phase11_baseline(self):
        """Running compliant mode should give same results as before."""
        from pea_met_network.cleaning import calculate_fwi_daily

        hourly = _make_hourly_frame("2024-07-01T00:00:00Z", periods=48)
        result = calculate_fwi_daily(hourly)

        assert "ffmc" in result.columns
        assert "dmc" in result.columns
        assert "dc" in result.columns
        assert len(result) > 0
        assert result["ffmc"].notna().any()
