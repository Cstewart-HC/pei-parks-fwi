"""Vapor pressure calculations for cross-station RH imputation.

Pure math module — no I/O, no pandas dependency beyond numpy arrays.
Uses August-Roche-Magnus (ARM) formula for saturation vapor pressure.
"""

from __future__ import annotations

import numpy as np


def saturation_vapor_pressure(temp_c: np.ndarray) -> np.ndarray:
    """August-Roche-Magnus formula. Returns es in kPa.

    es(T) = 0.61094 * exp(17.625 * T / (T + 243.04))

    Valid for -40 <= T <= 60 deg C.
    """
    t = np.asarray(temp_c, dtype=np.float64)
    return 0.61094 * np.exp(17.625 * t / (t + 243.04))


def actual_vapor_pressure(
    temp_c: np.ndarray,
    rh_pct: np.ndarray,
) -> np.ndarray:
    """Calculate actual vapor pressure from T and RH.

    e = (RH / 100) * es(T)
    """
    t = np.asarray(temp_c, dtype=np.float64)
    rh = np.asarray(rh_pct, dtype=np.float64)
    es = saturation_vapor_pressure(t)
    return (rh / 100.0) * es


def rh_from_vapor_pressure(
    temp_c: np.ndarray,
    vapor_pressure_kpa: np.ndarray,
) -> np.ndarray:
    """Derive RH from temperature and vapor pressure.

    RH = 100 * e / es(T)
    Capped at 100.0.
    """
    t = np.asarray(temp_c, dtype=np.float64)
    e = np.asarray(vapor_pressure_kpa, dtype=np.float64)
    es = saturation_vapor_pressure(t)
    with np.errstate(divide="ignore", invalid="ignore"):
        rh = np.where(es > 0, 100.0 * e / es, 0.0)
    return np.minimum(rh, 100.0)


def rh_from_dew_point(
    temp_c: np.ndarray,
    dew_point_c: np.ndarray,
) -> np.ndarray:
    """Derive RH from temperature and dew point (inverse Magnus-Tetens).

    Computes actual vapor pressure from dew point via ARM formula,
    then divides by saturation vapor pressure at observed temperature.

    This is the HIGHEST PRECISION path for RH from ECCC donors.
    Preferred over direct RELATIVE_HUMIDITY field because:
    - ECCC RELATIVE_HUMIDITY is integer (1% precision floor)
    - Dew point is 0.1 deg C precision -> ~4.5x more precise vapor pressure

    Returns RH capped at 100.0.
    """
    td = np.asarray(dew_point_c, dtype=np.float64)
    t = np.asarray(temp_c, dtype=np.float64)
    # Actual vapor pressure from dew point
    e_td = saturation_vapor_pressure(td)
    # RH at observed temperature
    es_t = saturation_vapor_pressure(t)
    with np.errstate(divide="ignore", invalid="ignore"):
        rh = np.where(es_t > 0, 100.0 * e_td / es_t, 0.0)
    return np.minimum(rh, 100.0)
