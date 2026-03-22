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
        rows.append(
            {
                "station": station,
                "reference_station": reference_station,
                "overlap_count": int(len(pair)),
                "mean_abs_diff": float(
                    (pair.iloc[:, 0] - pair.iloc[:, 1]).abs().mean()
                ),
                "correlation": float(pair.iloc[:, 0].corr(pair.iloc[:, 1])),
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
) -> pd.DataFrame:
    uncertainty = quantify_station_removal_risk(benchmark)
    merged = benchmark.merge(
        uncertainty,
        on=["station", "reference_station"],
        how="inner",
    )
    merged["recommendation"] = merged.apply(
        _recommendation_from_row,
        axis=1,
    )
    merged["evidence"] = merged.apply(
        lambda row: (
            "benchmark correlation="
            f"{row['correlation']:.3f}; uncertainty="
            f"{row['risk_band']} "
            f"({row['ci_lower']:.2f}-{row['ci_upper']:.2f})"
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
    recommendations = build_station_recommendations(benchmark)

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
