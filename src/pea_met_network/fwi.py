"""Canadian Fire Weather Index — thin wrapper around vendored cffdrs_py.

All single-step FWI calculations delegate to the official cffdrs_py
reference implementations (github.com/cffdrs/cffdrs_py).  This module
preserves the original pea_met_network public API so downstream code
needs no changes.

Vendored source: src/pea_met_network/vendor/cffdrs/
Reference: Van Wagner (1985), Van Wagner & Pickett (1985), FCFDG (1992).
"""

from __future__ import annotations

from pea_met_network.vendor.cffdrs import (
    fwi as _cffdrs,
    hourly_fine_fuel_moisture_code as _hffmc,
)

FFMC_COEFFICIENT = 250.0 * 59.5 / 101.0


# ---------------------------------------------------------------------------
# Public API — daily single-step functions
# ---------------------------------------------------------------------------

def fine_fuel_moisture_code(
    temp: float,
    rh: float,
    wind: float,
    rain: float,
    ffmc0: float,
) -> float:
    """Return daily FFMC from previous day's FFMC and weather inputs."""
    return _cffdrs.fine_fuel_moisture_code(
        ffmc_yda=ffmc0, temp=temp, rh=rh, ws=wind, prec=rain,
    )


def duff_moisture_code(
    temp: float,
    rh: float,
    rain: float,
    dmc0: float,
    month: int,
    lat: float,
) -> float:
    """Return daily DMC from previous day's DMC and weather inputs."""
    return _cffdrs.duff_moisture_code(
        dmc_yda=dmc0, temp=temp, rh=rh, prec=rain, lat=lat, mon=month,
    )


def drought_code(
    temp: float,
    rain: float,
    dc0: float,
    month: int,
    lat: float,
    rh: float = 50.0,
) -> float:
    """Return daily DC from previous day's DC and weather inputs."""
    return _cffdrs.drought_code(
        dc_yda=dc0, temp=temp, rh=rh, prec=rain, lat=lat, mon=month,
    )


def initial_spread_index(
    ffmc: float,
    wind: float,
) -> float:
    """Compute Initial Spread Index from FFMC and wind speed."""
    return _cffdrs.initial_spread_index(ffmc=ffmc, ws=wind)


def buildup_index(
    dmc: float,
    dc: float,
) -> float:
    """Compute Buildup Index from DMC and DC."""
    return _cffdrs.buildup_index(dmc=dmc, dc=dc)


def fire_weather_index(
    isi: float,
    bui: float,
) -> float:
    """Compute Fire Weather Index from ISI and BUI."""
    return _cffdrs.fire_weather_index(isi=isi, bui=bui)


# ---------------------------------------------------------------------------
# Hourly FFMC — single-step
# ---------------------------------------------------------------------------

def hourly_fine_fuel_moisture_code(
    temp: float,
    rh: float,
    wind: float,
    rain: float,
    ffmc0: float,
    t0: float = 1.0,
) -> float:
    """Hourly FFMC from previous time step's FFMC and weather inputs.

    Parameters
    ----------
    t0 : float
        Hours since previous FFMC value (default 1.0).
    """
    # cffdrs hffmc has no 0.5mm rain offset but hits ZeroDivisionError
    # when prec==0 in the rain-phase branch. Pass a vanishingly small
    # positive value to skip rain while avoiding the error.
    if rain <= 0.0:
        rain = 1e-10
    return _hffmc.hourly_fine_fuel_moisture_code(
        temp=temp, rh=rh, ws=wind, prec=rain, fo=ffmc0, t0=t0,
    )
