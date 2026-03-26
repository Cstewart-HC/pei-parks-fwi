"""Canadian Fire Weather Index moisture-code helpers."""

from __future__ import annotations

import math


def fine_fuel_moisture_code(
    temp: float,
    rh: float,
    wind: float,
    rain: float,
    ffmc0: float,
) -> float:
    """Return daily FFMC from previous day's FFMC and weather inputs."""
    ffmc_coefficient = 250.0 * 59.5 / 101.0
    mo = ffmc_coefficient * (101.0 - ffmc0) / (59.5 + ffmc0)

    if rain > 0.5:
        rf = rain - 0.5
        delta_mrf = (
            42.5
            * rf
            * math.exp(-100.0 / (251.0 - mo))
            * (1.0 - math.exp(-6.93 / rf))
        )
        mr = mo + delta_mrf
        if mo > 150.0:
            mr += 0.0015 * (mo - 150.0) * (mo - 150.0) * math.sqrt(rf)
        mo = min(mr, 250.0)

    ed = (
        0.942 * (rh**0.679)
        + 11.0 * math.exp((rh - 100.0) / 10.0)
        + 0.18 * (21.1 - temp) * (1.0 - math.exp(-0.115 * rh))
    )
    ew = (
        0.618 * (rh**0.753)
        + 10.0 * math.exp((rh - 100.0) / 10.0)
        + 0.18 * (21.1 - temp) * (1.0 - math.exp(-0.115 * rh))
    )

    if mo < ew:
        k0w = 0.424 * (1.0 - ((100.0 - rh) / 100.0) ** 1.7)
        k0w += 0.0694 * math.sqrt(wind) * (
            1.0 - ((100.0 - rh) / 100.0) ** 8
        )
        kw = k0w * 0.581 * math.exp(0.0365 * temp)
        m = ew - (ew - mo) / (10.0**kw)
    elif mo > ed:
        k0d = 0.424 * (1.0 - (rh / 100.0) ** 1.7)
        k0d += 0.0694 * math.sqrt(wind) * (1.0 - (rh / 100.0) ** 8)
        kd = k0d * 0.581 * math.exp(0.0365 * temp)
        m = ed + (mo - ed) / (10.0**kd)
    else:
        m = mo

    m = min(max(m, 0.0), 250.0)
    ffmc = 59.5 * (250.0 - m) / (ffmc_coefficient + m)
    return min(max(ffmc, 0.0), 101.0)


# DMC day-length factors (latitude >= 30N, default).
# Reference: gagreene/cffdrs cffwis.py dailyDMC.
_DMC_LE = [6.5, 7.5, 9.0, 12.8, 13.9, 13.9, 12.4, 10.9, 9.4, 8.0, 7.0, 6.0]

# DC day-length adjustment factors (latitude >= 20N, default).
# Reference: gagreene/cffdrs cffwis.py dailyDC.
_DC_LF = [
    -1.6, -1.6, -1.6, 0.9, 3.8, 5.8, 6.4, 5.0, 2.4, 0.4, -1.6, -1.6,
]


def duff_moisture_code(
    temp: float,
    rh: float,
    rain: float,
    dmc0: float,
    month: int,
    lat: float,
) -> float:
    """Return daily DMC from previous day's DMC and weather inputs.

    Follows gagreene/cffdrs (Van Wagner, 1987).
    """
    # Day-length factor for the month (1-indexed).
    le = _DMC_LE[month - 1]

    # Log drying rate.
    k = 1.894 * (temp + 1.1) * (100.0 - rh) * le * 1e-4

    # Moisture content from yesterday's DMC.
    mo = 20.0 + 280.0 / math.exp(0.023 * dmc0)

    # Rain phase.
    if rain > 1.5:
        re = 0.92 * rain - 1.27
        if dmc0 <= 33.0:
            b = 100.0 / (0.5 + 0.3 * dmc0)
        elif dmc0 <= 65.0:
            b = 14.0 - 1.3 * math.log(dmc0)
        else:
            b = 6.2 * math.log(dmc0) - 17.2
        mr = mo + 1000.0 * re / (48.77 + b * re)
        mr = max(mr, 0.0)
        dmc = 244.72 - 43.43 * math.log(mr - 20.0)
    else:
        dmc = dmc0

    dmc += k
    return max(dmc, 0.0)


def drought_code(
    temp: float,
    rain: float,
    dc0: float,
    month: int,
    lat: float,
) -> float:
    """Return daily DC from previous day's DC and weather inputs.

    Follows gagreene/cffdrs (Van Wagner, 1987).
    """
    # Day-length adjustment factor for the month (1-indexed).
    lf = _DC_LF[month - 1]

    # Potential evapotranspiration.
    v = 0.36 * (temp + 2.8) + lf

    # Yesterday's moisture equivalent.
    q0 = 800.0 / math.exp(dc0 / 400.0)

    # Rain phase (threshold 2.8 mm, not 0.5).
    if rain > 2.8:
        rd = 0.83 * rain - 1.27
        qr = q0 + 3.937 * rd
        dc = 400.0 * math.log(800.0 / qr) + 0.5 * v
    else:
        dc = dc0 + 0.5 * v

    return max(dc, 0.0)


def initial_spread_index(
    ffmc: float,
    wind: float,
) -> float:
    """Compute Initial Spread Index from FFMC and wind speed.

    Follows Van Wagner (1987) / cffdrs reference.
    Wind speed in km/h.
    """
    mo = 147.2 * (101.0 - ffmc) / (59.5 + ffmc)
    fwi_factor = 91.9 * math.exp(-0.1386 * mo)
    return (
        fwi_factor
        * (1.0 + mo**5.31)
        / (1.0 + 2.93 * mo**5.31)
        * math.exp(0.02339 * wind)
    )


def buildup_index(
    dmc: float,
    dc: float,
) -> float:
    """Compute Buildup Index from DMC and DC.

    Follows Van Wagner (1987) / cffdrs reference.
    """
    return max(dmc + 0.394 * dc, 0.0)


def fire_weather_index(
    isi: float,
    bui: float,
) -> float:
    """Compute Fire Weather Index from ISI and BUI.

    Follows Van Wagner (1987) / cffdrs reference.
    """
    if bui <= 0.0:
        return 0.0

    if bui <= 80.0:
        fBB = 0.1 * isi / (0.626 * bui**1.5 + 10.0)
    else:
        fBB = 1000.0 / (0.626 * bui**1.5 + 10.0)

    return max(0.1 * isi * fBB, 0.0)
