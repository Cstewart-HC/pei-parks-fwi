"""Tests for Phase 10: Cross-Station Variable Imputation.

Tests are organized by deliverable module:
  - VaporPressure: pure math (no I/O, no mocks)
  - EcccApiClient: API fetch + normalize (mocked network)
  - CrossStationImpute: core imputation logic (synthetic DataFrames)
  - Config: cleaning-config.json structure
  - Guardrails: donor restrictions, observed-value preservation

All tests use in-memory synthetic data. No network, no production files.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Section 1: Vapor Pressure Module (vapor_pressure.py)
# AC-10-01: saturation_vapor_pressure, actual_vapor_pressure,
#           rh_from_vapor_pressure, rh_from_dew_point
# ---------------------------------------------------------------------------
from pea_met_network.vapor_pressure import (  # noqa: E402
    actual_vapor_pressure,
    rh_from_dew_point,
    rh_from_vapor_pressure,
    saturation_vapor_pressure,
)


class TestSaturationVaporPressure:
    """AC-10-01, AC-10-25: ARM formula vs published reference values."""

    def test_zero_celsius(self) -> None:
        """es(0 deg C) = 0.61094 kPa (by definition)."""
        result = saturation_vapor_pressure(np.array([0.0]))
        assert result[0] == pytest.approx(0.61094, abs=0.0001)

    def test_twenty_celsius(self) -> None:
        """es(20 deg C) ~ 2.3388 kPa (WMO standard reference)."""
        result = saturation_vapor_pressure(np.array([20.0]))
        assert result[0] == pytest.approx(2.3388, abs=0.001)

    def test_thirty_celsius(self) -> None:
        """es(30 deg C) ~ 4.2450 kPa."""
        result = saturation_vapor_pressure(np.array([30.0]))
        assert result[0] == pytest.approx(4.2450, abs=0.001)

    def test_negative_ten_celsius(self) -> None:
        """es(-10 deg C) ~ 0.2600 kPa."""
        result = saturation_vapor_pressure(np.array([-10.0]))
        assert result[0] == pytest.approx(0.2600, abs=0.001)

    def test_returns_array_same_shape(self) -> None:
        """Output shape matches input shape."""
        temps = np.array([10.0, 20.0, 30.0])
        result = saturation_vapor_pressure(temps)
        assert result.shape == (3,)

    def test_scalar_input(self) -> None:
        """Accepts scalar numpy input and returns scalar-like output."""
        result = saturation_vapor_pressure(np.float64(25.0))
        assert result == pytest.approx(3.1690, abs=0.01)


class TestActualVaporPressure:
    """AC-10-01: e = (RH/100) x es(T)."""

    def test_known_value(self) -> None:
        """At 20 deg C, RH=50%: e = 0.5 x 2.3388 ~ 1.1694 kPa."""
        result = actual_vapor_pressure(
            np.array([20.0]),
            np.array([50.0]),
        )
        assert result[0] == pytest.approx(1.1694, abs=0.001)

    def test_zero_rh(self) -> None:
        """RH=0% -> e=0."""
        result = actual_vapor_pressure(
            np.array([25.0]),
            np.array([0.0]),
        )
        assert result[0] == pytest.approx(0.0, abs=0.0001)

    def test_full_rh_equals_saturation(self) -> None:
        """RH=100% -> e = es(T)."""
        t = np.array([15.0])
        es = saturation_vapor_pressure(t)
        e = actual_vapor_pressure(t, np.array([100.0]))
        assert e[0] == pytest.approx(es[0], abs=0.0001)


class TestRhFromVaporPressure:
    """AC-10-01, AC-10-25: RH = 100 x e / es(T), capped at 100."""

    def test_roundtrip_preserves_rh(self) -> None:
        """e(T,RH) -> RH(T,e) returns original RH."""
        temps = np.array([10.0, 20.0, 30.0, -5.0, 40.0])
        rhs = np.array([40.0, 60.0, 80.0, 90.0, 20.0])
        e = actual_vapor_pressure(temps, rhs)
        recovered = rh_from_vapor_pressure(temps, e)
        assert np.allclose(recovered, rhs, atol=0.01)

    def test_capped_at_100(self) -> None:
        """RH never exceeds 100% even if e exceeds saturation."""
        t = np.array([20.0])
        oversaturated_e = np.array([5.0])
        result = rh_from_vapor_pressure(t, oversaturated_e)
        assert result[0] == pytest.approx(100.0, abs=0.01)

    def test_zero_vapor_pressure(self) -> None:
        """e=0 -> RH=0%."""
        t = np.array([25.0])
        result = rh_from_vapor_pressure(t, np.array([0.0]))
        assert result[0] == pytest.approx(0.0, abs=0.01)


class TestRhFromDewPoint:
    """AC-10-01, AC-10-07: Dew point derivation > integer RH."""

    def test_dew_equals_temp_gives_100_rh(self) -> None:
        """When Td == T, air is saturated -> RH = 100%."""
        t = np.array([20.0])
        td = np.array([20.0])
        result = rh_from_dew_point(t, td)
        assert result[0] == pytest.approx(100.0, abs=0.01)

    def test_dew_far_below_temp_gives_low_rh(self) -> None:
        """Td = 0 deg C, T = 25 deg C -> RH ~ 32.1%."""
        t = np.array([25.0])
        td = np.array([0.0])
        result = rh_from_dew_point(t, td)
        assert result[0] == pytest.approx(32.1, abs=0.5)

    def test_precision_advantage_over_integer_rh(self) -> None:
        """0.1 deg C dew point change produces measurable RH change."""
        t = np.array([25.0])
        td1 = np.array([15.0])
        td2 = np.array([15.1])
        rh1 = rh_from_dew_point(t, td1)
        rh2 = rh_from_dew_point(t, td2)
        delta = rh2 - rh1
        assert delta > 0.3

    def test_capped_at_100(self) -> None:
        """If Td > T (physically impossible), RH capped at 100%."""
        t = np.array([10.0])
        td = np.array([20.0])
        result = rh_from_dew_point(t, td)
        assert result[0] == pytest.approx(100.0, abs=0.01)

    def test_returns_array(self) -> None:
        """Output is array with same shape as inputs."""
        t = np.array([10.0, 20.0, 30.0])
        td = np.array([5.0, 10.0, 20.0])
        result = rh_from_dew_point(t, td)
        assert result.shape == (3,)


# ---------------------------------------------------------------------------
# Section 2: ECCC API Client (eccc_api.py)
# AC-10-02: fetch_eccc_hourly, normalize_eccc_response, EcccStation
# ---------------------------------------------------------------------------

from pea_met_network.eccc_api import (  # noqa: E402
    ECCC_DONOR_STATIONS,
    EcccStation,
    fetch_eccc_hourly,
    normalize_eccc_response,
)


class TestEcccStation:
    """AC-10-02: Station registry dataclass and constants."""

    def test_registry_has_three_stations(self) -> None:
        assert len(ECCC_DONOR_STATIONS) == 3

    def test_st_peters_in_registry(self) -> None:
        st = ECCC_DONOR_STATIONS["st_peters"]
        assert st.climate_id == "8300562"
        assert st.stn_id == 41903
        assert st.name == "St. Peters"
        assert st.anemometer_height_m == 10.0

    def test_charlottetown_in_registry(self) -> None:
        st = ECCC_DONOR_STATIONS["charlottetown_a"]
        assert st.climate_id == "8300300"
        assert st.stn_id == 6526

    def test_harrington_in_registry(self) -> None:
        st = ECCC_DONOR_STATIONS["harrington_cda"]
        assert st.climate_id == "830P001"
        assert st.stn_id == 30308

    def test_station_is_frozen_dataclass(self) -> None:
        st = EcccStation("8300562", 41903, "Test", "America/Halifax", 10.0)
        with pytest.raises(AttributeError):
            st.climate_id = "OTHER"  # type: ignore[misc]


class TestNormalizeEcccResponse:
    """AC-10-02: OGC API GeoJSON -> normalized DataFrame."""

    def test_basic_parsing(self) -> None:
        """Single feature produces one-row DataFrame with canonical cols."""
        features = [_make_eccc_feature(
            dt="2024-07-15T12:00:00Z",
            temp=22.5, rh=65, wind=15.0, wind_dir=180,
            rain=0.0, dew_point=15.2,
        )]
        df = normalize_eccc_response(
            features, "st_peters", "America/Halifax",
        )
        assert len(df) == 1
        assert "timestamp_utc" in df.columns
        assert "air_temperature_c" in df.columns
        assert "relative_humidity_pct" in df.columns
        assert "wind_speed_kmh" in df.columns
        assert "dew_point_c" in df.columns

    def test_missing_field_coerced_to_nan(self) -> None:
        """Missing optional fields become NaN, not error."""
        features = [_make_eccc_feature(
            dt="2024-07-15T12:00:00Z",
            temp=22.5, rh=None, wind=15.0, wind_dir=180,
            rain=0.0, dew_point=None,
        )]
        df = normalize_eccc_response(
            features, "st_peters", "America/Halifax",
        )
        assert pd.isna(df["relative_humidity_pct"].iloc[0])
        assert pd.isna(df["dew_point_c"].iloc[0])

    def test_timestamp_converted_to_utc(self) -> None:
        """Timestamps are timezone-aware UTC."""
        features = [_make_eccc_feature(
            dt="2024-07-15T12:00:00Z",
            temp=20.0, rh=50, wind=10.0, wind_dir=90,
            rain=0.0, dew_point=10.0,
        )]
        df = normalize_eccc_response(
            features, "st_peters", "America/Halifax",
        )
        ts = df["timestamp_utc"].iloc[0]
        assert ts.tz is not None
        assert ts.tz.zone == "UTC"

    def test_empty_features_returns_empty_dataframe(self) -> None:
        df = normalize_eccc_response(
            [], "st_peters", "America/Halifax",
        )
        assert len(df) == 0


class TestFetchEcccHourly:
    """AC-10-02, AC-10-04: API fetch with caching."""

    @patch("pea_met_network.eccc_api.requests.get")
    def test_fetch_returns_dataframe(
        self, mock_get: MagicMock,
    ) -> None:
        """Mock API returns valid DataFrame."""
        mock_get.return_value = _make_mock_response(
            features=[_make_eccc_feature(
                dt="2024-07-15T12:00:00Z",
                temp=22.5, rh=65, wind=15.0, wind_dir=180,
                rain=0.0, dew_point=15.2,
            )],
        )
        station = ECCC_DONOR_STATIONS["st_peters"]
        result = fetch_eccc_hourly(
            station,
            pd.Timestamp("2024-07-01", tz="UTC"),
            pd.Timestamp("2024-07-31", tz="UTC"),
            cache_dir=None,
        )
        assert isinstance(result, pd.DataFrame)
        assert len(result) >= 1
        assert "air_temperature_c" in result.columns

    @patch("pea_met_network.eccc_api.requests.get")
    def test_cache_write_and_read(
        self, mock_get: MagicMock, tmp_path: Path,
    ) -> None:
        """Parquet cache roundtrip preserves data."""
        mock_get.return_value = _make_mock_response(
            features=[_make_eccc_feature(
                dt="2024-07-15T12:00:00Z",
                temp=22.5, rh=65, wind=15.0, wind_dir=180,
                rain=0.0, dew_point=15.2,
            )],
        )
        station = ECCC_DONOR_STATIONS["st_peters"]
        df1 = fetch_eccc_hourly(
            station,
            pd.Timestamp("2024-07-01", tz="UTC"),
            pd.Timestamp("2024-07-31", tz="UTC"),
            cache_dir=tmp_path,
        )
        df2 = fetch_eccc_hourly(
            station,
            pd.Timestamp("2024-07-01", tz="UTC"),
            pd.Timestamp("2024-07-31", tz="UTC"),
            cache_dir=tmp_path,
        )
        assert len(df1) == len(df2)
        assert set(df1.columns) == set(df2.columns)


# ---------------------------------------------------------------------------
# Section 3: Cross-Station Imputer (cross_station_impute.py)
# AC-10-03 through AC-10-25
# ---------------------------------------------------------------------------

from pea_met_network.cross_station_impute import (  # noqa: E402
    DonorAssignment,
    HeightCorrection,
    _rh_from_donor,
    _transfer_temp,
    _transfer_wind,
    derive_height_correction_factor,
    impute_cross_station,
)


class TestDonorAssignment:
    """AC-10-03: DonorAssignment dataclass."""

    def test_fields(self) -> None:
        da = DonorAssignment(
            target="stanley_bridge",
            variable="relative_humidity_pct",
            priority=1,
            donor_key="cavendish",
            donor_type="internal",
            max_gap_hours=3,
        )
        assert da.target == "stanley_bridge"
        assert da.variable == "relative_humidity_pct"
        assert da.priority == 1
        assert da.donor_key == "cavendish"
        assert da.donor_type == "internal"
        assert da.max_gap_hours == 3


class TestHeightCorrection:
    """AC-10-08: Wind height correction dataclass and power law."""

    def test_power_law_10m_to_3m(self) -> None:
        """10m->3m: v(3) = v(10) x (3/10)^0.14 ~ 0.827 x v(10)."""
        hc = HeightCorrection(
            donor_height_m=10.0,
            target_height_m=3.0,
            alpha=0.14,
        )
        ratio = (hc.target_height_m / hc.donor_height_m) ** hc.alpha
        assert ratio == pytest.approx(0.827, abs=0.005)

    def test_empirical_flag(self) -> None:
        hc = HeightCorrection(
            donor_height_m=10.0,
            target_height_m=3.0,
            alpha=0.14,
            empirically_derived=True,
        )
        assert hc.empirically_derived is True


class TestRhFromDonor:
    """AC-10-06, AC-10-07: RH derivation from donor data."""

    def test_internal_donor_uses_vp_continuity(self) -> None:
        """Internal donor: T=20, RH=50% -> VP continuity at T=25."""
        donor_row = pd.Series({
            "air_temperature_c": 20.0,
            "relative_humidity_pct": 50.0,
            "dew_point_c": float("nan"),
        })
        rh, method = _rh_from_donor(
            donor_row, target_temp=25.0, is_eccc=False,
        )
        assert method == "VP_CONTINUITY"
        assert rh == pytest.approx(36.9, abs=1.0)

    def test_eccc_donor_prefers_dew_point(self) -> None:
        """ECCC donor with both Td and RH -> uses Td path."""
        donor_row = pd.Series({
            "air_temperature_c": 20.0,
            "relative_humidity_pct": 50,
            "dew_point_c": 10.0,
        })
        rh, method = _rh_from_donor(
            donor_row, target_temp=20.0, is_eccc=True,
        )
        assert method == "TD_DERIVED"

    def test_eccc_donor_falls_back_to_integer_rh(self) -> None:
        """ECCC donor without Td -> uses integer RH field."""
        donor_row = pd.Series({
            "air_temperature_c": 20.0,
            "relative_humidity_pct": 55,
            "dew_point_c": float("nan"),
        })
        rh, method = _rh_from_donor(
            donor_row, target_temp=20.0, is_eccc=True,
        )
        assert method == "RH_INTEGER"
        assert rh == pytest.approx(55.0, abs=0.5)

    def test_rh_nan_when_donor_missing(self) -> None:
        """All donor fields NaN -> RH stays NaN."""
        donor_row = pd.Series({
            "air_temperature_c": float("nan"),
            "relative_humidity_pct": float("nan"),
            "dew_point_c": float("nan"),
        })
        rh, method = _rh_from_donor(
            donor_row, target_temp=20.0, is_eccc=False,
        )
        assert pd.isna(rh)


class TestTransferWind:
    """AC-10-08: Wind speed transfer with height correction."""

    def test_height_correction_applied(self) -> None:
        """10m donor -> 3m target: wind reduced by ~17%."""
        hc = HeightCorrection(
            donor_height_m=10.0, target_height_m=3.0, alpha=0.14,
        )
        wind, method = _transfer_wind(20.0, hc)
        assert method == "HEIGHT_SCALED"
        assert wind == pytest.approx(20.0 * 0.827, abs=0.3)

    def test_no_correction_raw_proxy(self) -> None:
        """No height correction -> raw spatial proxy."""
        wind, method = _transfer_wind(15.0, None)
        assert method == "SPATIAL_PROXY_RAW"
        assert wind == pytest.approx(15.0, abs=0.01)

    def test_empirical_correction(self) -> None:
        """Empirically derived k factor applied."""
        hc = HeightCorrection(
            donor_height_m=10.0, target_height_m=3.0,
            alpha=0.14, empirically_derived=True,
        )
        wind, method = _transfer_wind(20.0, hc)
        assert method == "HEIGHT_SCALED"


class TestTransferTemp:
    """AC-10-09: Temperature transfer with asymmetric outlier cap."""

    def test_within_cap_qf_1(self) -> None:
        """Small donor-target diff -> qf=1 (within bounds)."""
        temp, qf = _transfer_temp(22.0, "stanley_bridge")
        assert qf == 1

    def test_warm_bias_capped_at_2c(self) -> None:
        """Donor much warmer -> capped at +/-2 deg C, qf=2."""
        temp, qf = _transfer_temp(50.0, "stanley_bridge")
        assert qf == 2

    def test_cool_bias_capped_at_3c(self) -> None:
        """Donor much cooler -> capped at +/-3 deg C, qf=2."""
        temp, qf = _transfer_temp(-30.0, "stanley_bridge")
        assert qf == 2


class TestImputeCrossStation:
    """AC-10-03, AC-10-05, AC-10-10 through AC-10-17."""

    def test_rh_imputed_for_stanley_bridge(self) -> None:
        """AC-10-10: Stanley Bridge gets RH from Cavendish donor."""
        target = _make_station_hourly(
            station="stanley_bridge", hours=24, rh_missing=True,
        )
        donor = _make_station_hourly(
            station="cavendish", hours=24, rh_missing=False,
        )
        assignments = [
            DonorAssignment(
                target="stanley_bridge",
                variable="relative_humidity_pct",
                priority=1,
                donor_key="cavendish",
                donor_type="internal",
                max_gap_hours=3,
            ),
        ]
        result_df, records = impute_cross_station(
            target, "stanley_bridge",
            donor_assignments=assignments,
            internal_hourly={"cavendish": donor},
        )
        filled = result_df["relative_humidity_pct"].notna().sum()
        assert filled > 0
        assert "relative_humidity_pct_qf" in result_df.columns
        assert "relative_humidity_pct_src" in result_df.columns
        assert "relative_humidity_pct_method" in result_df.columns

    def test_rh_imputed_for_tracadie(self) -> None:
        """AC-10-11: Tracadie gets RH from North Rustico donor."""
        target = _make_station_hourly(
            station="tracadie", hours=24, rh_missing=True,
        )
        donor = _make_station_hourly(
            station="north_rustico", hours=24, rh_missing=False,
        )
        assignments = [
            DonorAssignment(
                target="tracadie",
                variable="relative_humidity_pct",
                priority=1,
                donor_key="north_rustico",
                donor_type="internal",
                max_gap_hours=3,
            ),
        ]
        result_df, records = impute_cross_station(
            target, "tracadie",
            donor_assignments=assignments,
            internal_hourly={"north_rustico": donor},
        )
        filled = result_df["relative_humidity_pct"].notna().sum()
        assert filled > 0

    def test_observed_values_never_overwritten(self) -> None:
        """AC-10-14: Existing non-null values stay unchanged."""
        target = _make_station_hourly(
            station="greenwich", hours=24, rh_missing=False,
        )
        donor = _make_station_hourly(
            station="cavendish", hours=24, rh_missing=False,
        )
        original_rh = target["relative_humidity_pct"].copy()
        assignments = [
            DonorAssignment(
                target="greenwich",
                variable="relative_humidity_pct",
                priority=1,
                donor_key="cavendish",
                donor_type="internal",
                max_gap_hours=3,
            ),
        ]
        result_df, _ = impute_cross_station(
            target, "greenwich",
            donor_assignments=assignments,
            internal_hourly={"cavendish": donor},
        )
        pd.testing.assert_series_equal(
            original_rh,
            result_df["relative_humidity_pct"],
            check_names=False,
        )

    def test_audit_columns_have_correct_format(self) -> None:
        """AC-10-15, AC-10-17: Standardized INTERNAL:{name} format."""
        target = _make_station_hourly(
            station="stanley_bridge", hours=24, rh_missing=True,
        )
        donor = _make_station_hourly(
            station="cavendish", hours=24, rh_missing=False,
        )
        assignments = [
            DonorAssignment(
                target="stanley_bridge",
                variable="relative_humidity_pct",
                priority=1,
                donor_key="cavendish",
                donor_type="internal",
                max_gap_hours=3,
            ),
        ]
        result_df, _ = impute_cross_station(
            target, "stanley_bridge",
            donor_assignments=assignments,
            internal_hourly={"cavendish": donor},
        )
        src_vals = result_df["relative_humidity_pct_src"].dropna().unique()
        for src in src_vals:
            assert src.startswith("INTERNAL:"), f"Bad format: {src}"

    def test_quality_flags_valid(self) -> None:
        """AC-10-16: QF values restricted to {0, 1, 2, 9}."""
        target = _make_station_hourly(
            station="stanley_bridge", hours=24, rh_missing=True,
        )
        donor = _make_station_hourly(
            station="cavendish", hours=24, rh_missing=False,
        )
        assignments = [
            DonorAssignment(
                target="stanley_bridge",
                variable="relative_humidity_pct",
                priority=1,
                donor_key="cavendish",
                donor_type="internal",
                max_gap_hours=3,
            ),
        ]
        result_df, _ = impute_cross_station(
            target, "stanley_bridge",
            donor_assignments=assignments,
            internal_hourly={"cavendish": donor},
        )
        valid_qf = {0, 1, 2, 9}
        qf_vals = result_df["relative_humidity_pct_qf"].dropna().unique()
        for qf in qf_vals:
            assert int(qf) in valid_qf, f"Invalid QF: {qf}"

    def test_donor_fallback_on_gap(self) -> None:
        """AC-10-23: P2 used when P1 has gap > max_gap_hours."""
        target = _make_station_hourly(
            station="stanley_bridge", hours=24, rh_missing=True,
        )
        p1_donor = _make_station_hourly(
            station="cavendish", hours=24, rh_missing=False,
        )
        p1_donor.loc[
            p1_donor.index[10:], "relative_humidity_pct"
        ] = float("nan")
        p2_donor = _make_station_hourly(
            station="north_rustico", hours=24, rh_missing=False,
        )
        assignments = [
            DonorAssignment(
                target="stanley_bridge",
                variable="relative_humidity_pct",
                priority=1,
                donor_key="cavendish",
                donor_type="internal",
                max_gap_hours=3,
            ),
            DonorAssignment(
                target="stanley_bridge",
                variable="relative_humidity_pct",
                priority=2,
                donor_key="north_rustico",
                donor_type="internal",
                max_gap_hours=3,
            ),
        ]
        result_df, records = impute_cross_station(
            target, "stanley_bridge",
            donor_assignments=assignments,
            internal_hourly={
                "cavendish": p1_donor,
                "north_rustico": p2_donor,
            },
        )
        filled_by_p2 = result_df.loc[
            result_df.index[10:], "relative_humidity_pct"
        ].notna().sum()
        assert filled_by_p2 > 0
        src_vals = result_df.loc[
            result_df.index[10:], "relative_humidity_pct_src"
        ].dropna().unique()
        assert any(
            "north_rustico" in str(s) for s in src_vals
        )


class TestGuardrailNoSyntheticDonors:
    """AC-10-24: Stanley Bridge and Tracadie rejected as donors."""

    def test_stanley_bridge_rejected_as_donor(self) -> None:
        """SB donor with no RH sensor -> zero fills."""
        target = _make_station_hourly(
            station="greenwich", hours=24, rh_missing=True,
        )
        bad_donor = _make_station_hourly(
            station="stanley_bridge", hours=24, rh_missing=True,
        )
        assignments = [
            DonorAssignment(
                target="greenwich",
                variable="relative_humidity_pct",
                priority=1,
                donor_key="stanley_bridge",
                donor_type="internal",
                max_gap_hours=3,
            ),
        ]
        result_df, records = impute_cross_station(
            target, "greenwich",
            donor_assignments=assignments,
            internal_hourly={"stanley_bridge": bad_donor},
        )
        filled = result_df["relative_humidity_pct"].notna().sum()
        assert filled == 0

    def test_tracadie_rejected_as_donor(self) -> None:
        """Tracadie donor with no RH sensor -> zero fills."""
        target = _make_station_hourly(
            station="greenwich", hours=24, rh_missing=True,
        )
        bad_donor = _make_station_hourly(
            station="tracadie", hours=24, rh_missing=True,
        )
        assignments = [
            DonorAssignment(
                target="greenwich",
                variable="relative_humidity_pct",
                priority=1,
                donor_key="tracadie",
                donor_type="internal",
                max_gap_hours=3,
            ),
        ]
        result_df, records = impute_cross_station(
            target, "greenwich",
            donor_assignments=assignments,
            internal_hourly={"tracadie": bad_donor},
        )
        filled = result_df["relative_humidity_pct"].notna().sum()
        assert filled == 0


class TestDeriveHeightCorrectionFactor:
    """AC-10-08: Empirical height correction from overlap."""

    def test_returns_correction_with_overlap(self) -> None:
        """Sufficient overlap -> HeightCorrection with empirical flag."""
        np.random.seed(42)
        n = 200
        target_wind = np.random.uniform(5, 20, n)
        donor_wind = target_wind * 1.2
        target_df = pd.DataFrame({
            "timestamp_utc": pd.date_range(
                "2024-07-01", periods=n, freq="h",
            ),
            "wind_speed_kmh": target_wind,
        })
        donor_df = pd.DataFrame({
            "timestamp_utc": pd.date_range(
                "2024-07-01", periods=n, freq="h",
            ),
            "wind_speed_kmh": donor_wind,
        })
        result = derive_height_correction_factor(
            target_df, donor_df,
        )
        assert result is not None
        assert result.empirically_derived is True
        assert result.donor_height_m > 0

    def test_returns_none_with_insufficient_overlap(self) -> None:
        """Less than 1 week overlap -> returns None."""
        target_df = pd.DataFrame({
            "timestamp_utc": pd.date_range(
                "2024-07-01", periods=10, freq="h",
            ),
            "wind_speed_kmh": [10.0] * 10,
        })
        donor_df = pd.DataFrame({
            "timestamp_utc": pd.date_range(
                "2024-07-01", periods=10, freq="h",
            ),
            "wind_speed_kmh": [12.0] * 10,
        })
        result = derive_height_correction_factor(
            target_df, donor_df,
        )
        assert result is None


# ---------------------------------------------------------------------------
# Section 4: Config (cleaning-config.json) — AC-10-22
# ---------------------------------------------------------------------------

_CONFIG_PATH = (
    Path(__file__).resolve().parents[1] / "docs" / "cleaning-config.json"
)


def _load_config() -> dict:
    with open(_CONFIG_PATH) as f:
        return json.load(f)


class TestCrossStationConfig:
    """AC-10-22: cleaning-config.json has cross_station_impute."""

    def test_config_has_cross_station_section(self) -> None:
        assert "cross_station_impute" in _load_config()

    def test_config_has_enabled_flag(self) -> None:
        csi = _load_config()["cross_station_impute"]
        assert "enabled" in csi
        assert isinstance(csi["enabled"], bool)

    def test_config_has_max_gap_hours(self) -> None:
        csi = _load_config()["cross_station_impute"]
        assert "max_gap_hours" in csi
        assert csi["max_gap_hours"] == 3

    def test_config_has_donor_assignments(self) -> None:
        csi = _load_config()["cross_station_impute"]
        da = csi["donor_assignments"]
        assert "rh" in da
        assert "wind_speed_kmh" in da
        assert "air_temperature_c" in da

    def test_config_has_eccc_stations(self) -> None:
        csi = _load_config()["cross_station_impute"]
        es = csi["eccc_stations"]
        assert "st_peters" in es
        assert "charlottetown_a" in es
        assert "harrington_cda" in es

    def test_config_height_corrections_section(self) -> None:
        hc = _load_config()["cross_station_impute"]["height_corrections"]
        assert "alpha" in hc
        assert hc["alpha"] == pytest.approx(0.14, abs=0.01)

    def test_config_temp_outlier_caps(self) -> None:
        caps = _load_config()["cross_station_impute"]["temp_outlier_caps"]
        assert caps["warm_bias_max_c"] == pytest.approx(2.0, abs=0.1)
        assert caps["cool_bias_max_c"] == pytest.approx(3.0, abs=0.1)


# ---------------------------------------------------------------------------
# Section 5: FWI Quality Flag Propagation — AC-10-18
# ---------------------------------------------------------------------------

from pea_met_network.cross_station_impute import (  # noqa: E402
    propagate_fwi_quality_flags,
)


class TestFwiQualityFlagPropagation:
    """AC-10-18: Synthetic input -> FWI_qf > 0."""

    def test_synthetic_rh_propagates_to_fwi_qf(self) -> None:
        """When RH is synthetic (qf=1), FWI output qf should be 1."""
        df = pd.DataFrame({
            "timestamp_utc": pd.date_range(
                "2024-07-01", periods=5, freq="h",
            ),
            "relative_humidity_pct_qf": [1, 0, 1, 0, 9],
            "wind_speed_kmh_qf": [0, 0, 0, 0, 0],
            "air_temperature_c_qf": [0, 0, 0, 0, 0],
            "rain_mm_qf": [0, 0, 0, 0, 0],
        })
        result = propagate_fwi_quality_flags(df)
        assert "ffmc_qf" in result.columns
        assert "fwi_qf" in result.columns
        assert result["fwi_qf"].iloc[0] == 1
        assert result["fwi_qf"].iloc[1] == 0
        assert result["fwi_qf"].iloc[4] == 9

    def test_all_observed_gives_zero_fwi_qf(self) -> None:
        """All inputs observed -> all FWI outputs qf=0."""
        df = pd.DataFrame({
            "timestamp_utc": pd.date_range(
                "2024-07-01", periods=3, freq="h",
            ),
            "relative_humidity_pct_qf": [0, 0, 0],
            "wind_speed_kmh_qf": [0, 0, 0],
            "air_temperature_c_qf": [0, 0, 0],
            "rain_mm_qf": [0, 0, 0],
        })
        result = propagate_fwi_quality_flags(df)
        assert (result["ffmc_qf"] == 0).all()
        assert (result["fwi_qf"] == 0).all()


# ---------------------------------------------------------------------------
# Helpers: synthetic data factories
# ---------------------------------------------------------------------------


def _make_eccc_feature(
    dt: str,
    temp: float | None,
    rh: float | None,
    wind: float | None,
    wind_dir: float | None,
    rain: float | None,
    dew_point: float | None,
) -> dict[str, Any]:
    """Build a minimal OGC API GeoJSON feature for testing."""
    props: dict[str, Any] = {}
    if temp is not None:
        props["TEMP"] = temp
    if rh is not None:
        props["RELATIVE_HUMIDITY"] = rh
    if wind is not None:
        props["WIND_SPEED"] = wind
    if wind_dir is not None:
        props["WIND_DIRECTION"] = wind_dir
    if rain is not None:
        props["PRECIP_AMOUNT"] = rain
    if dew_point is not None:
        props["DEW_POINT_TEMP"] = dew_point
    return {
        "type": "Feature",
        "id": "test-feature-1",
        "properties": props,
        "geometry": {"type": "Point", "coordinates": [-63.0, 46.4]},
    }


def _make_mock_response(features: list[dict]) -> MagicMock:
    """Build a mock requests.Response with OGC API GeoJSON."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = {
        "type": "FeatureCollection",
        "features": features,
        "links": [],
    }
    return mock_resp


def _make_station_hourly(
    station: str,
    hours: int = 24,
    rh_missing: bool = False,
    wind_missing: bool = False,
    temp_missing: bool = False,
) -> pd.DataFrame:
    """Build a synthetic hourly station DataFrame for testing."""
    ts = pd.date_range("2024-07-15", periods=hours, freq="h", tz="UTC")
    df = pd.DataFrame({
        "timestamp_utc": ts,
        "station": station,
        "air_temperature_c": (
            [float("nan")] * hours if temp_missing else [20.0] * hours
        ),
        "relative_humidity_pct": (
            [float("nan")] * hours if rh_missing else [60.0] * hours
        ),
        "wind_speed_kmh": (
            [float("nan")] * hours if wind_missing else [15.0] * hours
        ),
        "wind_direction_deg": [180.0] * hours,
        "rain_mm": [0.0] * hours,
    })
    return df.set_index("timestamp_utc")
