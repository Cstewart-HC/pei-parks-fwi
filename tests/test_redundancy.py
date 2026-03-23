from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering

from pea_met_network.redundancy import (
    benchmark_to_stanhope,
    build_station_matrix,
    build_station_recommendations,
    cluster_station_order,
    pairwise_station_correlation,
    pca_station_loadings,
)


def _sample_frame() -> pd.DataFrame:
    """Synthetic fixture mimicking real PEINP station data structure.

    Provides 4 hourly timestamps to simulate basic temporal variation,
    4 stations (3 park + 1 reference), with correlated temperature readings
    scaled to mimic real °C values. Stations have small differences to
    simulate good correlation with the reference stanhope.
    """
    timestamps = pd.date_range(
        "2024-03-01 00:00:00+00:00",
        periods=4,
        freq="h",
    )
    return pd.DataFrame(
        {
            "timestamp_utc": list(timestamps) * 4,
            "station": (
                ["alpha"] * 4
                + ["beta"] * 4
                + ["gamma"] * 4
                + ["stanhope"] * 4
            ),
            "air_temperature_c": [
                1.0,
                2.0,
                3.0,
                4.0,
                1.1,
                2.1,
                3.1,
                4.1,
                4.0,
                3.0,
                2.0,
                1.0,
                0.9,
                1.9,
                2.9,
                3.9,
            ],
        }
    )


def test_build_station_matrix_pivots_hourly_values() -> None:
    frame = _sample_frame()

    matrix = build_station_matrix(frame, value_column="air_temperature_c")

    assert list(matrix.columns) == ["alpha", "beta", "gamma", "stanhope"]
    assert matrix.index.name == "timestamp_utc"
    assert matrix.loc[matrix.index[0], "alpha"] == 1.0
    assert matrix.loc[matrix.index[-1], "stanhope"] == 3.9


def test_pairwise_station_correlation_reports_similarity() -> None:
    frame = _sample_frame()
    matrix = build_station_matrix(frame, value_column="air_temperature_c")

    correlation = pairwise_station_correlation(matrix)

    assert list(correlation.columns) == ["alpha", "beta", "gamma", "stanhope"]
    assert correlation.loc["alpha", "beta"] > 0.99
    assert correlation.loc["alpha", "stanhope"] > 0.99


def test_pca_station_loadings_returns_station_weights() -> None:
    frame = _sample_frame()
    matrix = build_station_matrix(frame, value_column="air_temperature_c")

    loadings = pca_station_loadings(matrix)

    assert set(loadings.columns) == {
        "station",
        "component",
        "loading",
        "explained_variance_ratio",
    }
    assert set(loadings["station"]) == {"alpha", "beta", "gamma", "stanhope"}
    assert set(loadings["component"]) == {"PC1", "PC2"}
    assert (loadings["explained_variance_ratio"] >= 0.0).all()


def test_cluster_station_order_groups_similar_stations() -> None:
    frame = _sample_frame()
    matrix = build_station_matrix(frame, value_column="air_temperature_c")

    order = cluster_station_order(matrix)

    assert set(order) == {"alpha", "beta", "gamma", "stanhope"}
    assert len(order) == 4

    # Verify clustering behavior: stations in same cluster are more similar
    corr = pairwise_station_correlation(matrix)
    distance = 1 - corr

    # Use AgglomerativeClustering to get labels
    clustering = AgglomerativeClustering(
        n_clusters=2,
        metric="precomputed",
        linkage="average",
    )
    labels = clustering.fit_predict(distance)

    # Compute intra and inter cluster distances
    intra_distances = []
    inter_distances = []
    for i in range(len(matrix.columns)):
        for j in range(i + 1, len(matrix.columns)):
            dist = distance.iloc[i, j]
            if labels[i] == labels[j]:
                intra_distances.append(dist)
            else:
                inter_distances.append(dist)

    avg_intra = np.mean(intra_distances) if intra_distances else 0
    avg_inter = np.mean(inter_distances) if inter_distances else 0

    # Intra-cluster distances should be smaller than inter-cluster
    assert avg_intra < avg_inter


def test_benchmark_to_stanhope_summarizes_distance_and_overlap() -> None:
    frame = _sample_frame()
    matrix = build_station_matrix(frame, value_column="air_temperature_c")

    benchmark = benchmark_to_stanhope(matrix, reference_station="stanhope")

    assert set(benchmark.columns) == {
        "station",
        "reference_station",
        "overlap_count",
        "mean_abs_diff",
        "correlation",
        "observations",
    }
    alpha_row = benchmark.loc[benchmark["station"] == "alpha"].iloc[0]
    assert alpha_row["reference_station"] == "stanhope"
    assert alpha_row["overlap_count"] == 4
    assert alpha_row["mean_abs_diff"] < 0.2
    assert alpha_row["correlation"] > 0.99
    assert np.allclose(alpha_row["observations"], [0.1, 0.1, 0.1, 0.1])


def test_build_station_recommendations_references_uncertainty() -> None:
    frame = _sample_frame()
    matrix = build_station_matrix(frame, value_column="air_temperature_c")
    benchmark = benchmark_to_stanhope(matrix, reference_station="stanhope")

    recommendations = build_station_recommendations(benchmark)

    assert set(recommendations["recommendation"]).issubset(
        {"keep", "remove", "defer"}
    )
    assert "risk_probability" in recommendations.columns
    assert "ci_lower" in recommendations.columns
    assert "ci_upper" in recommendations.columns
    assert recommendations["evidence"].str.contains("uncertainty=").all()
    assert recommendations["evidence"].str.contains("benchmark").all()


def test_recommendations_synthesize_pca_clustering_and_benchmark(
) -> None:
    frame = _sample_frame()
    matrix = build_station_matrix(frame, value_column="air_temperature_c")
    benchmark = benchmark_to_stanhope(matrix, reference_station="stanhope")
    loadings = pca_station_loadings(matrix)
    clustering = cluster_station_order(matrix)

    recommendations = build_station_recommendations(
        benchmark,
        pca_loadings=loadings,
        cluster_order=clustering,
    )

    # Evidence must reference all three analytical methods
    assert recommendations["evidence"].str.contains("pca", case=False).all(), (
        "Evidence string must reference PCA analysis"
    )
    assert recommendations["evidence"].str.contains(
        "cluster", case=False,
    ).all(), (
        "Evidence string must reference clustering analysis"
    )
    assert recommendations["evidence"].str.contains(
        "benchmark", case=False,
    ).all(), (
        "Evidence string must reference benchmarking"
    )
    assert recommendations["evidence"].str.contains(
        "uncertainty", case=False,
    ).all(), (
        "Evidence string must reference uncertainty"
    )


def test_write_redundancy_summary_creates_interpretable_artifact(
    tmp_path: Path,
) -> None:
    from pea_met_network.redundancy import write_redundancy_summary

    frame = _sample_frame()
    output_path = tmp_path / "redundancy_summary.md"

    write_redundancy_summary(
        frame,
        value_column="air_temperature_c",
        output_path=output_path,
        reference_station="stanhope",
    )

    content = output_path.read_text()

    assert output_path.exists()
    assert "# Redundancy Analysis Summary" in content
    assert "## Correlation" in content
    assert "## PCA Loadings" in content
    assert "## Clustering Order" in content
    assert "## Stanhope Benchmark" in content
    assert "## Recommendations" in content
