"""Reference moisture-code vectors derived from gagreene/cffdrs."""

from __future__ import annotations

import pytest

from pea_met_network.fwi import (
    drought_code,
    duff_moisture_code,
    fine_fuel_moisture_code,
)

TOLERANCE = 0.01

FFMC_VECTORS = [
    pytest.param(
        85.0,
        {"temp": 17.0, "rh": 42.0, "wind": 25.0, "rain": 0.0},
        87.6493555020996,
        id="ffmc-drying-baseline",
    ),
    pytest.param(
        85.0,
        {"temp": 6.0, "rh": 90.0, "wind": 15.0, "rain": 0.6},
        79.39974896674352,
        id="ffmc-light-rain-wetting",
    ),
    pytest.param(
        92.0,
        {"temp": 28.0, "rh": 18.0, "wind": 35.0, "rain": 1.2},
        93.97717349811505,
        id="ffmc-hot-windy-recovery",
    ),
]

DMC_VECTORS = [
    pytest.param(
        6.0,
        {"temp": 17.0, "rh": 42.0, "rain": 0.0, "month": 4, "lat": 46.4},
        8.545051136,
        id="dmc-spring-drying",
    ),
    pytest.param(
        20.0,
        {"temp": 10.0, "rh": 80.0, "rain": 12.0, "month": 10, "lat": 46.4},
        9.674116769007487,
        id="dmc-heavy-rain-reset",
    ),
    pytest.param(
        40.0,
        {"temp": 28.0, "rh": 25.0, "rain": 0.4, "month": 7, "lat": 46.4},
        45.1257322,
        id="dmc-summer-drying",
    ),
]

DC_VECTORS = [
    pytest.param(
        15.0,
        {"temp": 17.0, "rain": 0.0, "month": 4, "lat": 46.4},
        19.014,
        id="dc-spring-drying",
    ),
    pytest.param(
        250.0,
        {"temp": 22.0, "rain": 10.0, "month": 8, "lat": 46.4},
        231.91135794039346,
        id="dc-heavy-rain-recharge",
    ),
    pytest.param(
        80.0,
        {"temp": -3.0, "rain": 0.0, "month": 1, "lat": 46.4},
        80.0,
        id="dc-winter-limited-drying",
    ),
]


@pytest.mark.parametrize(("previous_code", "weather", "expected"), FFMC_VECTORS)
def test_ffmc_vectors(
    previous_code: float,
    weather: dict[str, float],
    expected: float,
) -> None:
    result = fine_fuel_moisture_code(
        ffmc0=previous_code,
        temp=weather["temp"],
        rh=weather["rh"],
        wind=weather["wind"],
        rain=weather["rain"],
    )
    assert abs(result - expected) < TOLERANCE, (
        f"FFMC mismatch: got {result}, expected {expected}"
    )


@pytest.mark.parametrize(("previous_code", "weather", "expected"), DMC_VECTORS)
def test_dmc_vectors(
    previous_code: float,
    weather: dict[str, float],
    expected: float,
) -> None:
    result = duff_moisture_code(
        dmc0=previous_code,
        temp=weather["temp"],
        rh=weather["rh"],
        rain=weather["rain"],
        month=int(weather["month"]),
        lat=weather["lat"],
    )
    assert abs(result - expected) < TOLERANCE, (
        f"DMC mismatch: got {result}, expected {expected}"
    )


@pytest.mark.parametrize(("previous_code", "weather", "expected"), DC_VECTORS)
def test_dc_vectors(
    previous_code: float,
    weather: dict[str, float],
    expected: float,
) -> None:
    result = drought_code(
        dc0=previous_code,
        temp=weather["temp"],
        rain=weather["rain"],
        month=int(weather["month"]),
        lat=weather["lat"],
    )
    assert abs(result - expected) < TOLERANCE, (
        f"DC mismatch: got {result}, expected {expected}"
    )
