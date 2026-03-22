from __future__ import annotations

import pandas as pd

from pea_met_network.uncertainty import quantify_station_removal_risk


def _sample_similarity_frame() -> pd.DataFrame:
    """Synthetic fixture shaped like Stanhope benchmark output.

    The rows mimic three stations with strong, moderate, and weak
    similarity to a reference station, plus explicit overlap counts
    so the interval-width behavior is testable.
    """
    return pd.DataFrame(
        {
            "station": ["alpha", "beta", "gamma"],
            "reference_station": ["stanhope", "stanhope", "stanhope"],
            "overlap_count": [120, 72, 12],
            "mean_abs_diff": [0.15, 0.55, 1.2],
            "correlation": [0.98, 0.83, 0.41],
        }
    )


def test_quantify_station_removal_risk_returns_distributional_bounds() -> None:
    risk = quantify_station_removal_risk(_sample_similarity_frame())

    assert list(risk["station"]) == ["alpha", "beta", "gamma"]
    assert set(risk.columns) == {
        "station",
        "reference_station",
        "risk_probability",
        "ci_lower",
        "ci_upper",
        "risk_band",
        "assumptions",
        "limitations",
    }
    assert risk.loc[risk["station"] == "alpha", "risk_band"].iloc[0] == "low"
    assert risk.loc[risk["station"] == "gamma", "risk_band"].iloc[0] == "high"
    assert (
        risk.loc[risk["station"] == "alpha", "risk_probability"].iloc[0]
        < risk.loc[risk["station"] == "beta", "risk_probability"].iloc[0]
        < risk.loc[risk["station"] == "gamma", "risk_probability"].iloc[0]
    )
    assert risk["risk_probability"].between(0.0, 1.0).all()
    assert risk["ci_lower"].between(0.0, 1.0).all()
    assert risk["ci_upper"].between(0.0, 1.0).all()
    assert (risk["ci_lower"] <= risk["risk_probability"]).all()
    assert (risk["risk_probability"] <= risk["ci_upper"]).all()


def test_quantify_station_removal_risk_intervals_widen_with_less_overlap(
) -> None:
    risk = quantify_station_removal_risk(_sample_similarity_frame())

    alpha = risk.loc[risk["station"] == "alpha"].iloc[0]
    beta = risk.loc[risk["station"] == "beta"].iloc[0]
    gamma = risk.loc[risk["station"] == "gamma"].iloc[0]

    alpha_width = alpha["ci_upper"] - alpha["ci_lower"]
    beta_width = beta["ci_upper"] - beta["ci_lower"]
    gamma_width = gamma["ci_upper"] - gamma["ci_lower"]

    assert alpha_width < beta_width < gamma_width


def test_quantify_station_removal_risk_surfaces_sample_size_limitations(
) -> None:
    risk = quantify_station_removal_risk(_sample_similarity_frame())

    gamma_limitations = risk.loc[
        risk["station"] == "gamma", "limitations"
    ].iloc[0]
    alpha_limitations = risk.loc[
        risk["station"] == "alpha", "limitations"
    ].iloc[0]

    assert "insufficient" in gamma_limitations.lower()
    assert "adequate" in alpha_limitations.lower()


def test_quantify_station_removal_risk_reports_distributional_assumptions(
) -> None:
    risk = quantify_station_removal_risk(_sample_similarity_frame())

    assumptions = risk.loc[risk["station"] == "beta", "assumptions"].iloc[0]

    assert "gaussian_kde" in assumptions
    assert "distributional" in assumptions.lower()


def test_quantify_station_removal_risk_assigns_moderate_band_to_mixed_signal(
) -> None:
    risk = quantify_station_removal_risk(_sample_similarity_frame())

    beta_band = risk.loc[risk["station"] == "beta", "risk_band"].iloc[0]

    assert beta_band == "moderate"

