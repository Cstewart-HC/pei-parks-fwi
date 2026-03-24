"""Build analysis.ipynb programmatically — lint-clean version."""
import nbformat
from pathlib import Path

nb = nbformat.v4.new_notebook()
cells = []

# ── Cell 0: Setup ──
cells.append(nbformat.v4.new_code_cell("""\
import warnings

warnings.filterwarnings("ignore")

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from pathlib import Path

from sklearn.decomposition import PCA

from pea_met_network.qa_qc import coverage_summary
from pea_met_network.redundancy import (
    benchmark_to_stanhope,
    build_station_matrix,
    build_station_recommendations,
    cluster_station_order,
    pairwise_station_correlation,
    pca_station_loadings,
)

PROJECT_ROOT = Path(".").resolve()
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FIGURES_DIR = PROJECT_ROOT / "notebooks" / "figures"
FIGURES_DIR.mkdir(parents=True, exist_ok=True)

sns.set_style("whitegrid")
plt.rcParams["figure.dpi"] = 120
"""))

# ── Cell 1: Load all processed data ──
cells.append(nbformat.v4.new_markdown_cell("""\
## 1. Data Loading

Load hourly processed data for all PEINP stations and the Stanhope reference.
"""))

cells.append(nbformat.v4.new_code_cell("""\
stations = sorted([
    d.name for d in PROCESSED_DIR.iterdir()
    if d.is_dir() and (d / "station_hourly.csv").exists()
])
print(f"Stations found: {stations}")

hourly_frames = []
for station in stations:
    fpath = PROCESSED_DIR / station / "station_hourly.csv"
    df = pd.read_csv(fpath, parse_dates=["timestamp_utc"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    hourly_frames.append(df)
    tmin = df["timestamp_utc"].min()
    tmax = df["timestamp_utc"].max()
    print(f"  {station}: {len(df)} rows  ({tmin} to {tmax})")

hourly_all = pd.concat(hourly_frames, ignore_index=True)
n = hourly_all["station"].nunique()
print(f"\\nCombined: {len(hourly_all)} rows, {n} stations")
"""))

# ── Cell 2: EDA — Station Coverage Table ──
cells.append(nbformat.v4.new_markdown_cell("""\
## 2. Exploratory Data Analysis

### 2.1 Station Coverage Table

Summary of temporal coverage per station.
"""))

cells.append(nbformat.v4.new_code_cell("""\
coverage_frames = []
for station in stations:
    fpath = PROCESSED_DIR / station / "station_hourly.csv"
    df = pd.read_csv(fpath, parse_dates=["timestamp_utc"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    coverage_frames.append(df[["station", "timestamp_utc"]])

coverage_df = pd.concat(coverage_frames, ignore_index=True)
cov_summary = coverage_summary(coverage_df)
print(cov_summary.to_string(index=False))
"""))

# ── Cell 3: EDA — Temporal Coverage ──
cells.append(nbformat.v4.new_markdown_cell("""\
### 2.2 Temporal Coverage Summary

Visual overview of data availability by station over time.
"""))

cells.append(nbformat.v4.new_code_cell("""\
fig, ax = plt.subplots(figsize=(12, 4))

for station in stations:
    fpath = PROCESSED_DIR / station / "station_hourly.csv"
    df = pd.read_csv(fpath, parse_dates=["timestamp_utc"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    start = df["timestamp_utc"].min().floor("D")
    end = df["timestamp_utc"].max().floor("D")
    date_range = pd.date_range(start, end, freq="D")
    daily_counts = (
        df.set_index("timestamp_utc").resample("D").size()
    )
    daily_counts = daily_counts.reindex(date_range, fill_value=0)
    ax.fill_between(
        daily_counts.index, 0, daily_counts.values,
        alpha=0.4, label=station,
    )
    ax.plot(daily_counts.index, daily_counts.values, linewidth=0.8)

ax.set_xlabel("Date")
ax.set_ylabel("Hourly Records per Day")
ax.set_title("Temporal Coverage by Station")
ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=8)
plt.tight_layout()
fig.savefig(FIGURES_DIR / "temporal_coverage.png", bbox_inches="tight")
plt.show()
print("Saved: notebooks/figures/temporal_coverage.png")
"""))

# ── Cell 4: EDA — Missingness Heatmap ──
cells.append(nbformat.v4.new_markdown_cell("""\
### 2.3 Missingness Heatmap

Percentage of missing values per variable per station.
"""))

cells.append(nbformat.v4.new_code_cell("""\
key_vars = [
    "air_temperature_c", "relative_humidity_pct", "rain_mm",
    "wind_speed_kmh", "wind_direction_deg", "solar_radiation_w_m2",
]

missing_frames = []
for station in stations:
    fpath = PROCESSED_DIR / station / "station_hourly.csv"
    df = pd.read_csv(fpath)
    row = {"station": station}
    for var in key_vars:
        if var in df.columns:
            row[var] = df[var].isna().mean() * 100
        else:
            row[var] = 100.0
    missing_frames.append(row)

missing_df = pd.DataFrame(missing_frames).set_index("station")
missing_df = missing_df[key_vars]

fig, ax = plt.subplots(figsize=(10, 4))
sns.heatmap(
    missing_df, annot=True, fmt=".1f", cmap="YlOrRd",
    ax=ax, cbar_kws={"label": "% Missing"},
)
ax.set_title("Missingness Heatmap (% Missing per Variable per Station)")
ax.set_xlabel("Variable")
ax.set_ylabel("Station")
plt.tight_layout()
fig.savefig(FIGURES_DIR / "missingness_heatmap.png", bbox_inches="tight")
plt.show()
print("Saved: notebooks/figures/missingness_heatmap.png")
print("\\nMissingness summary:")
print(missing_df.round(1).to_string())
"""))

# ── Cell 5: FWI Time Series ──
cells.append(nbformat.v4.new_markdown_cell("""\
## 3. Fire Weather Index Analysis

Time series of FWI system components for stations with computed indices.
"""))

cells.append(nbformat.v4.new_code_cell("""\
fwi_stations = []
for station in stations:
    fpath = PROCESSED_DIR / station / "station_hourly.csv"
    df = pd.read_csv(fpath, parse_dates=["timestamp_utc"])
    if "fwi" in df.columns and df["fwi"].notna().sum() > 100:
        fwi_stations.append(station)

print(f"Stations with FWI data: {fwi_stations}")
"""))

cells.append(nbformat.v4.new_code_cell("""\
fwi_vars = ["ffmc", "dmc", "dc", "isi", "bui", "fwi"]

# Sort by available FWI records descending
fwi_sorted = sorted(
    fwi_stations,
    key=lambda s: pd.read_csv(
        PROCESSED_DIR / s / "station_hourly.csv",
        parse_dates=["timestamp_utc"],
    )["fwi"].notna().sum(),
    reverse=True,
)
plot_stations = fwi_sorted[: min(2, len(fwi_sorted))]

fig, axes = plt.subplots(
    len(plot_stations), 1,
    figsize=(14, 3 * len(plot_stations)),
    sharex=True,
)
if len(plot_stations) == 1:
    axes = [axes]

for idx, station in enumerate(plot_stations):
    fpath = PROCESSED_DIR / station / "station_hourly.csv"
    df = pd.read_csv(fpath, parse_dates=["timestamp_utc"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    df_daily = (
        df.set_index("timestamp_utc")[fwi_vars].resample("D").mean()
    )
    ax = axes[idx]
    for var in fwi_vars:
        if var in df_daily.columns:
            ax.plot(
                df_daily.index, df_daily[var],
                label=var, alpha=0.8, linewidth=0.9,
            )
    ax.set_ylabel("Value")
    ax.set_title(f"FWI Components - {station}")
    ax.legend(fontsize=7, ncol=3)

axes[-1].set_xlabel("Date")
fig.suptitle(
    "Fire Weather Index Time Series (Daily Means)", y=1.02,
)
plt.tight_layout()
fig.savefig(FIGURES_DIR / "fwi_timeseries.png", bbox_inches="tight")
plt.show()
print(f"Plotted FWI for: {plot_stations}")
print("Saved: notebooks/figures/fwi_timeseries.png")
"""))

# ── Cell 6: PCA ──
cells.append(nbformat.v4.new_markdown_cell("""\
## 4. Principal Component Analysis

PCA on hourly temperature to identify station similarity patterns.
"""))

cells.append(nbformat.v4.new_code_cell("""\
temp_frames = []
for station in stations:
    fpath = PROCESSED_DIR / station / "station_hourly.csv"
    df = pd.read_csv(fpath, parse_dates=["timestamp_utc"])
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True)
    if "air_temperature_c" in df.columns:
        temp_frames.append(
            df[["station", "timestamp_utc", "air_temperature_c"]]
        )

temp_all = pd.concat(temp_frames, ignore_index=True)
matrix = build_station_matrix(temp_all, value_column="air_temperature_c")
print(f"Station matrix shape: {matrix.shape}")
print(f"Stations: {list(matrix.columns)}")
"""))

cells.append(nbformat.v4.new_code_cell("""\
loadings = pca_station_loadings(matrix)
print("PCA Loadings:")
print(loadings.to_string(index=False))
"""))

cells.append(nbformat.v4.new_code_cell("""\
normalized = matrix.dropna(axis="index", how="any")
normalized = (normalized - normalized.mean()) / normalized.std()

n_comp = min(len(matrix.columns), normalized.shape[0])
pca_full = PCA(n_components=n_comp)
pca_full.fit(normalized)

fig, ax = plt.subplots(figsize=(6, 4))
ax.plot(range(1, n_comp + 1), pca_full.explained_variance_ratio_, "o-")
ax.set_xlabel("Principal Component")
ax.set_ylabel("Explained Variance Ratio")
ax.set_title("Scree Plot")
ax.set_xticks(range(1, n_comp + 1))
plt.tight_layout()
fig.savefig(FIGURES_DIR / "pca_scree.png", bbox_inches="tight")
plt.show()
print("Saved: notebooks/figures/pca_scree.png")
ratios = pca_full.explained_variance_ratio_.round(4).tolist()
print(f"Explained variance ratios: {ratios}")
"""))

cells.append(nbformat.v4.new_code_cell("""\
scores = pca_full.transform(normalized)

fig, ax = plt.subplots(figsize=(8, 6))
ax.scatter(scores[:, 0], scores[:, 1], s=80, zorder=5)
for i, station in enumerate(matrix.columns):
    ax.annotate(
        station, (scores[i, 0], scores[i, 1]),
        textcoords="offset points", xytext=(5, 5), fontsize=9,
    )
ax.set_xlabel(f"PC1 ({pca_full.explained_variance_ratio_[0]:.1%})")
ax.set_ylabel(f"PC2 ({pca_full.explained_variance_ratio_[1]:.1%})")
ax.set_title("PCA Biplot - Station Scores")
ax.axhline(0, color="gray", linewidth=0.5)
ax.axvline(0, color="gray", linewidth=0.5)
plt.tight_layout()
fig.savefig(FIGURES_DIR / "pca_biplot.png", bbox_inches="tight")
plt.show()
print("Saved: notebooks/figures/pca_biplot.png")
"""))

# ── Cell 7: Clustering ──
cells.append(nbformat.v4.new_markdown_cell("""\
## 5. Hierarchical Clustering

Stations clustered by temperature correlation to identify redundancy groups.
"""))

cells.append(nbformat.v4.new_code_cell("""\
from scipy.cluster.hierarchy import dendrogram, linkage
from scipy.spatial.distance import squareform

corr = matrix.corr()
distance = 1 - corr
dist_array = squareform(distance.values, checks=False)

link = linkage(dist_array, method="average")

fig, ax = plt.subplots(figsize=(8, 5))
dendrogram(link, labels=list(matrix.columns), ax=ax, leaf_rotation=45)
ax.set_ylabel("Distance (1 - correlation)")
ax.set_title("Hierarchical Clustering Dendrogram")
plt.tight_layout()
fig.savefig(FIGURES_DIR / "clustering_dendrogram.png", bbox_inches="tight")
plt.show()
print("Saved: notebooks/figures/clustering_dendrogram.png")
"""))

cells.append(nbformat.v4.new_code_cell("""\
cluster_order = cluster_station_order(matrix)
print("Cluster assignments:")
for entry in cluster_order:
    print(f"  {entry}")

corr = pairwise_station_correlation(matrix).fillna(0.0)
dist = 1 - corr
print("\\nPairwise distance matrix:")
print(dist.round(3).to_string())
"""))

# ── Cell 8: Redundancy Analysis ──
cells.append(nbformat.v4.new_markdown_cell("""\
## 6. Redundancy Analysis

Combining PCA, clustering, and benchmarking against the Stanhope reference
to identify redundant stations.
"""))

cells.append(nbformat.v4.new_code_cell("""\
try:
    benchmark = benchmark_to_stanhope(
        matrix, reference_station="stanhope",
    )
    print("Benchmark results:")
    print(benchmark.to_string(index=False))
except ValueError as e:
    print(f"Stanhope benchmark skipped: {e}")
    print("Using inter-station metrics only.")
    benchmark = None
"""))

cells.append(nbformat.v4.new_code_cell("""\
if benchmark is not None and len(benchmark) > 0:
    recommendations = build_station_recommendations(
        benchmark,
        pca_loadings=loadings,
        cluster_order=cluster_order,
    )
    print("Station Recommendations:")
    print(recommendations.to_string(index=False))
else:
    print("Cannot build recommendations without benchmark.")
    recommendations = None
"""))

# ── Cell 9: Uncertainty ──
cells.append(nbformat.v4.new_markdown_cell("""\
## 7. Uncertainty Quantification

Risk probabilities and confidence intervals for station removal.
"""))

cells.append(nbformat.v4.new_code_cell("""\
from pea_met_network.uncertainty import quantify_station_removal_risk

if benchmark is not None and len(benchmark) > 0:
    uncertainty = quantify_station_removal_risk(benchmark)
    print("Uncertainty Quantification (KDE-based risk):")
    print(uncertainty.to_string(index=False))

    fig, ax = plt.subplots(figsize=(8, 4))
    stations_plot = uncertainty["station"].tolist()
    risks = uncertainty["risk_probability"].tolist()
    ci_lo = uncertainty["ci_lower"].tolist()
    ci_hi = uncertainty["ci_upper"].tolist()

    x = range(len(stations_plot))
    ax.bar(x, risks, color="steelblue", alpha=0.7)
    yerr_lo = [r - lo for r, lo in zip(risks, ci_lo)]
    yerr_hi = [hi - r for r, hi in zip(risks, ci_hi)]
    ax.errorbar(
        x, risks, yerr=[yerr_lo, yerr_hi],
        fmt="none", c="black", capsize=5,
    )
    ax.set_xticks(list(x))
    ax.set_xticklabels(stations_plot, rotation=45, ha="right")
    ax.set_ylabel("Removal Risk Probability")
    ax.set_title(
        "Station Removal Risk with 95% Confidence Intervals",
    )
    plt.tight_layout()
    fig.savefig(FIGURES_DIR / "uncertainty_risk.png", bbox_inches="tight")
    plt.show()
    print("Saved: notebooks/figures/uncertainty_risk.png")
else:
    print("Uncertainty analysis requires benchmark data (skipped).")
"""))

# ── Cell 10: Conclusion ──
cells.append(nbformat.v4.new_markdown_cell("""\
## 8. Conclusion

### Key Findings

1. **Data Coverage**: The PEINP network comprises 5 automated weather
   stations (Cavendish, Greenwich, North Rustico, Stanley Bridge,
   Tracadie) plus the Stanhope ECCC reference station. Coverage periods
   vary — Cavendish has the longest record starting October 2022, while
   Stanley Bridge and Tracadie begin mid-2023. Stanhope provides
   continuous reference data from January 2022.

2. **Data Quality**: Missingness varies by station and sensor. Stanley
   Bridge and Tracadie lack humidity and dew point sensors entirely,
   limiting their utility for FWI computation. All stations have some
   gaps in wind and solar radiation data.

3. **Fire Weather Index**: Three stations (Cavendish, Greenwich, North
   Rustico) have complete FWI chains computed. Stanley Bridge and
   Tracadie cannot compute FWI due to missing humidity inputs. FWI
   values remain low across the network, consistent with the humid
   maritime climate of Prince Edward Island.

4. **Station Redundancy**: PCA on air temperature shows that PC1
   captures the majority of variance, with stations loading similarly —
   indicating strong spatial coherence in temperature patterns across
   the island. Hierarchical clustering confirms this: stations form
   tight groups with low inter-station distances (high correlation).

5. **Redundancy Recommendations**: Stations with high correlation to
   Stanhope and low removal risk are candidates for potential
   redundancy. The combination of PCA loadings, cluster assignments,
   and KDE-based uncertainty quantification provides an evidence-based
   framework for network optimization decisions.

### Recommendations

- **Stanley Bridge and Tracadie** should not be considered for
  redundancy analysis until humidity sensors are added — their limited
  variable sets make them incomparable to other stations.
- **Cavendish, Greenwich, and North Rustico** show strong temperature
  correlation, suggesting some redundancy in spatial coverage. Final
  redundancy decisions should incorporate wind and precipitation
  patterns in addition to temperature.
- The Stanhope reference station provides essential baseline data and
  should be retained as the benchmark for all redundancy assessments.
"""))

nb.cells = cells
nb.metadata["language_info"] = {"name": "python", "version": "3.12"}
nb.metadata["kernelspec"] = {
    "display_name": "Python 3",
    "language": "python",
    "name": "python3",
}

out_path = Path("/mnt/fast_data/workspaces/pea-met-network") / "analysis.ipynb"
with open(out_path, "w") as f:
    nbformat.write(nb, f)
print(f"Wrote {out_path}")
print(f"Cells: {len(nb.cells)}")
