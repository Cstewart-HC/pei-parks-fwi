from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.cluster import AgglomerativeClustering
from sklearn.decomposition import PCA

from pea_met_network.uncertainty import quantify_station_removal_risk


def build_station_matrix(
    frame: pd.DataFrame,
    *,
    value_column: str,
) -> pd.DataFrame:
    matrix = frame.pivot_table(
        index="timestamp_utc",
        columns="station",
        values=value_column,
        aggfunc="mean",
    )
    return matrix.sort_index().sort_index(axis="columns")


def pairwise_station_correlation(matrix: pd.DataFrame) -> pd.DataFrame:
    return matrix.corr(numeric_only=True)


def _zscore_columns(matrix: pd.DataFrame) -> pd.DataFrame:
    centered = matrix - matrix.mean(axis=0)
    scaled = centered / matrix.std(axis=0, ddof=1)
    return scaled.fillna(0.0)


def pca_station_loadings(matrix: pd.DataFrame) -> pd.DataFrame:
    normalized = _zscore_columns(matrix.dropna(axis="index", how="any"))
    pca = PCA(n_components=min(2, normalized.shape[1]))
    pca.fit(normalized)
    rows: list[dict[str, object]] = []
    for component_index, component_values in enumerate(pca.components_):
        component = f"PC{component_index + 1}"
        for station, loading in zip(
            normalized.columns,
            component_values,
            strict=True,
        ):
            rows.append(
                {
                    "station": station,
                    "component": component,
                    "loading": float(loading),
                    "explained_variance_ratio": float(
                        pca.explained_variance_ratio_[component_index]
                    ),
                }
            )
    return pd.DataFrame(rows)


def cluster_station_order(matrix: pd.DataFrame) -> list[str]:
    correlation = pairwise_station_correlation(matrix).fillna(0.0)
    distance = 1.0 - correlation
    model = AgglomerativeClustering(
        metric="precomputed",
        linkage="average",
        n_clusters=max(1, min(2, len(distance.columns))),
    )
    labels = model.fit_predict(distance)
    summary = pd.DataFrame(
        {
            "station": distance.index,
            "cluster": labels,
            "distance_to_stanhope": distance.get("stanhope", 0.0).values,
        }
    )
    ordered = summary.sort_values(
        ["cluster", "distance_to_stanhope", "station"]
    )
    return ordered["station"].tolist()


def benchmark_to_stanhope(
    matrix: pd.DataFrame,
    *,
    reference_station: str = "stanhope",
) -> pd.DataFrame:
    if reference_station not in matrix.columns:
        raise ValueError(f"Reference station missing: {reference_station}")

    reference = matrix[reference_station]
    rows: list[dict[str, object]] = []
    for station in matrix.columns:
        if station == reference_station:
            continue
        pair = pd.concat([matrix[station], reference], axis=1).dropna()
        divergence = (pair.iloc[:, 0] - pair.iloc[:, 1]).abs()
        rows.append(
            {
                "station": station,
                "reference_station": reference_station,
                "overlap_count": int(len(pair)),
                "mean_abs_diff": float(divergence.mean()),
                "correlation": float(pair.iloc[:, 0].corr(pair.iloc[:, 1])),
                "observations": divergence.to_numpy(dtype=float),
            }
        )
    return pd.DataFrame(rows)


def _recommendation_from_row(row: pd.Series) -> str:
    if row["risk_band"] == "high" or row["ci_upper"] >= 0.7:
        return "keep"
    if row["correlation"] >= 0.95 and row["risk_band"] == "low":
        return "remove"
    return "defer"


def build_station_recommendations(
    benchmark: pd.DataFrame,
    *,
    pca_loadings: pd.DataFrame | None = None,
    cluster_order: list[str] | None = None,
) -> pd.DataFrame:
    uncertainty = quantify_station_removal_risk(benchmark)
    merged = benchmark.merge(
        uncertainty,
        on=["station", "reference_station"],
        how="inner",
    )

    # Build PCA evidence string per station.
    pca_evidence: dict[str, str] = {}
    if pca_loadings is not None:
        for station in merged["station"]:
            station_loadings = pca_loadings[
                pca_loadings["station"] == station
            ]
            parts: list[str] = []
            for _, row in station_loadings.iterrows():
                parts.append(
                    f"{row['component']}={row['loading']:.3f}"
                )
            if parts:
                pca_evidence[station] = ", ".join(parts)

    # Build cluster evidence string per station.
    cluster_evidence: dict[str, str] = {}
    if cluster_order is not None:
        cluster_positions = {
            station: position
            for position, station in enumerate(cluster_order)
        }
        n = len(cluster_order)
        for station in merged["station"]:
            if station in cluster_positions:
                pos = cluster_positions[station]
                cluster_evidence[station] = f"position {pos + 1}/{n}"

    merged["recommendation"] = merged.apply(
        _recommendation_from_row,
        axis=1,
    )
    merged["evidence"] = merged.apply(
        lambda row: _build_evidence_string(
            row,
            pca_evidence=pca_evidence,
            cluster_evidence=cluster_evidence,
        ),
        axis=1,
    )
    return merged[
        [
            "station",
            "reference_station",
            "recommendation",
            "risk_probability",
            "ci_lower",
            "ci_upper",
            "risk_band",
            "evidence",
            "assumptions",
            "limitations",
        ]
    ]


def _build_evidence_string(
    row: pd.Series,
    *,
    pca_evidence: dict[str, str],
    cluster_evidence: dict[str, str],
) -> str:
    parts = [
        f"benchmark correlation={row['correlation']:.3f}",
        f"uncertainty={row['risk_band']} "
        f"({row['ci_lower']:.2f}-{row['ci_upper']:.2f})",
    ]
    station = row["station"]
    if station in pca_evidence:
        parts.append(f"pca=[{pca_evidence[station]}]")
    if station in cluster_evidence:
        parts.append(f"cluster={cluster_evidence[station]}")
    return "; ".join(parts)


def _frame_to_markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "(no rows)"
    columns = list(frame.columns)
    header = "| " + " | ".join(columns) + " |"
    divider = "| " + " | ".join(["---"] * len(columns)) + " |"
    body = [
        "| " + " | ".join(str(row[column]) for column in columns) + " |"
        for row in frame.to_dict(orient="records")
    ]
    return "\n".join([header, divider, *body])


def write_redundancy_summary(
    frame: pd.DataFrame,
    *,
    value_column: str,
    output_path: Path,
    reference_station: str = "stanhope",
) -> Path:
    matrix = build_station_matrix(frame, value_column=value_column)
    correlation = pairwise_station_correlation(matrix)
    loadings = pca_station_loadings(matrix)
    clustering = cluster_station_order(matrix)
    benchmark = benchmark_to_stanhope(
        matrix,
        reference_station=reference_station,
    )
    recommendations = build_station_recommendations(
        benchmark,
        pca_loadings=loadings,
        cluster_order=clustering,
    )

    sections = [
        "# Redundancy Analysis Summary",
        "",
        "## Correlation",
        _frame_to_markdown_table(correlation.reset_index(names="station")),
        "",
        "## PCA Loadings",
        _frame_to_markdown_table(loadings),
        "",
        "## Clustering Order",
        "\n".join(f"- {station}" for station in clustering),
        "",
        "## Stanhope Benchmark",
        _frame_to_markdown_table(benchmark),
        "",
        "## Recommendations",
        _frame_to_markdown_table(recommendations),
        "",
    ]
    output_path.write_text("\n".join(sections))
    return output_path
