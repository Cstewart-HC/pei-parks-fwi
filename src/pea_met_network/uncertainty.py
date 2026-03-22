from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import gaussian_kde


def _risk_band(probability: float) -> str:
    if probability < 0.34:
        return "low"
    if probability < 0.67:
        return "moderate"
    return "high"


def _limitations(overlap_count: int) -> str:
    if overlap_count < 24:
        return (
            "Insufficient overlap for reliable uncertainty estimation; "
            "intervals are intentionally wide."
        )
    return "Sample support is adequate for a coarse uncertainty bound."


def _clip_probability(value: float) -> float:
    return float(np.clip(value, 0.0, 1.0))


def _distribution_samples(
    *,
    mean_abs_diff: float,
    correlation: float,
    overlap_count: int,
    sample_size: int = 256,
) -> np.ndarray:
    divergence = min(mean_abs_diff / 1.5, 1.0)
    decorrelation = 1.0 - max(min(correlation, 1.0), 0.0)
    center = _clip_probability((0.6 * divergence) + (0.4 * decorrelation))

    support = max(overlap_count, 1)
    spread = min(0.35, max(0.03, 0.85 / np.sqrt(support)))
    synthetic = np.array(
        [
            _clip_probability(center - spread),
            _clip_probability(center - (spread * 0.33)),
            center,
            _clip_probability(center + (spread * 0.33)),
            _clip_probability(center + spread),
        ]
    )
    kde = gaussian_kde(synthetic)
    grid = np.linspace(0.0, 1.0, sample_size)
    density = kde(grid)
    density_sum = density.sum()
    if density_sum <= 0:
        return np.full(sample_size, center)
    weights = density / density_sum
    choice = np.random.default_rng(0).choice(
        sample_size,
        size=sample_size,
        p=weights,
    )
    return grid[choice]


def quantify_station_removal_risk(
    benchmark: pd.DataFrame,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for row in benchmark.to_dict(orient="records"):
        samples = _distribution_samples(
            mean_abs_diff=float(row["mean_abs_diff"]),
            correlation=float(row["correlation"]),
            overlap_count=int(row["overlap_count"]),
        )
        probability = float(np.mean(samples))
        ci_lower, ci_upper = np.quantile(samples, [0.1, 0.9])
        rows.append(
            {
                "station": row["station"],
                "reference_station": row["reference_station"],
                "risk_probability": probability,
                "ci_lower": float(ci_lower),
                "ci_upper": float(ci_upper),
                "risk_band": _risk_band(probability),
                "assumptions": (
                    "Distributional uncertainty is estimated with "
                    "scipy.stats.gaussian_kde over a bounded synthetic "
                    "risk distribution tied to divergence, correlation, "
                    "and overlap support."
                ),
                "limitations": _limitations(int(row["overlap_count"])),
            }
        )
    return pd.DataFrame(rows)
