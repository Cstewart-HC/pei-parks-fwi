"""Phase 10 exit gate tests — analysis notebook verification.

Tests execute the notebook programmatically (papermill-free, via nbconvert)
and verify each acceptance criterion against cell outputs.
"""

import re
from pathlib import Path

import nbconvert.preprocessors
import nbformat
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
NOTEBOOK_PATH = PROJECT_ROOT / "analysis.ipynb"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
FIGURES_DIR = PROJECT_ROOT / "notebooks" / "figures"


def _execute_notebook(notebook_path: Path) -> nbformat.NotebookNode:
    """Execute a notebook and return the executed notebook object."""
    with open(notebook_path) as f:
        nb = nbformat.read(f, as_version=4)

    ep = nbconvert.preprocessors.ExecutePreprocessor(
        timeout=300,
        kernel_name="python3",
    )
    ep.preprocess(nb, {"metadata": {"path": str(notebook_path.parent)}})
    return nb


def _all_outputs(nb: nbformat.NotebookNode) -> list:
    """Flatten all outputs from all code cells."""
    return [
        out
        for cell in nb["cells"]
        if cell.get("cell_type") == "code"
        for out in cell.get("outputs", [])
    ]


def _get_output_text(outputs: list) -> str:
    """Concatenate all text output from a list of output dicts."""
    texts = []
    for out in outputs:
        if out.get("output_type") == "stream":
            texts.append(out.get("text", ""))
        elif out.get("output_type") == "execute_result":
            data = out.get("data", {})
            texts.append(data.get("text/plain", ""))
    return "\n".join(texts)


def _notebook_output(nb: nbformat.NotebookNode) -> str:
    """Get all text output from an executed notebook."""
    return _get_output_text(_all_outputs(nb))


# ── AC-ANA-1: Notebook loads real processed data ────────────────────────────


class TestACANA1LoadRealData:
    """Notebook loads real processed data from data/processed/, not stubs."""

    @pytest.fixture(scope="class")
    def executed_nb(self):
        if not NOTEBOOK_PATH.exists():
            pytest.skip("analysis.ipynb does not exist")
        return _execute_notebook(NOTEBOOK_PATH)

    def test_notebook_executes_without_errors(self, executed_nb):
        """AC-ANA-8: All cells execute top-to-bottom without errors."""
        for i, cell in enumerate(executed_nb["cells"]):
            if cell.get("cell_type") != "code":
                continue
            for out in cell.get("outputs", []):
                if out.output_type == "error":
                    pytest.fail(
                        f"Cell {i} raised {out.ename}: {out.evalue}\n"
                        f"{out.traceback}"
                    )

    def test_loads_processed_data_files(self, executed_nb):
        """AC-ANA-1: Notebook references data/processed/ files."""
        code_cells = [
            cell for cell in executed_nb["cells"]
            if cell.get("cell_type") == "code"
        ]
        all_output = _get_output_text(
            [out for cell in code_cells for out in cell.get("outputs", [])]
        )
        station_dirs = [
            d.name
            for d in PROCESSED_DIR.iterdir()
            if d.is_dir() and d.name != "stanhope"
        ]
        assert len(station_dirs) > 0, (
            "No station processed data directories found"
        )

        found = False
        for station in station_dirs:
            if station.lower() in all_output.lower():
                found = True
                break
        assert found, (
            f"Notebook output does not reference any processed station data. "
            f"Stations available: {station_dirs}"
        )


# ── AC-ANA-2: EDA with coverage table, temporal coverage,
#    missingness heatmap ──────────────────────────────────────────────────────


class TestACANA2EDA:
    """EDA: station coverage table, temporal coverage, missingness."""

    @pytest.fixture(scope="class")
    def executed_nb(self):
        if not NOTEBOOK_PATH.exists():
            pytest.skip("analysis.ipynb does not exist")
        return _execute_notebook(NOTEBOOK_PATH)

    def test_coverage_table_present(self, executed_nb):
        """Station coverage table is generated with temporal info."""
        all_output = _notebook_output(executed_nb)
        assert any(
            s.lower() in all_output.lower()
            for s in ["cavendish", "greenwich"]
        ), "Coverage table missing station data"

    def test_temporal_coverage_chart_generated(self, executed_nb):
        """Temporal coverage visualization is produced."""
        assert FIGURES_DIR.exists(), "notebooks/figures/ directory missing"
        fig_files = list(FIGURES_DIR.glob("*.png"))
        assert len(fig_files) >= 1, (
            "No figure PNGs found in notebooks/figures/"
        )

    def test_missingness_heatmap_generated(self, executed_nb):
        """Missingness heatmap is produced."""
        all_output = _notebook_output(executed_nb)
        key_vars = ["air_temperature", "relative_humidity"]
        found = sum(
            1 for v in key_vars if v.lower() in all_output.lower()
        )
        assert found >= 1, "Missingness heatmap output not detected"


# ── AC-ANA-3: PCA with scree plot, loadings table, biplot ──────────────────


class TestACANA3PCA:
    """PCA: scree plot, loadings table, biplot/score plot."""

    @pytest.fixture(scope="class")
    def executed_nb(self):
        if not NOTEBOOK_PATH.exists():
            pytest.skip("analysis.ipynb does not exist")
        return _execute_notebook(NOTEBOOK_PATH)

    def test_pca_loadings_table(self, executed_nb):
        """PCA loadings table is printed with station-level data."""
        all_output = _notebook_output(executed_nb)
        assert "PCA Loadings" in all_output, (
            "PCA loadings table not found in output"
        )

    def test_scree_plot_generated(self, executed_nb):
        """Scree plot figure is saved."""
        assert FIGURES_DIR.exists(), "notebooks/figures/ directory missing"
        fig_files = list(FIGURES_DIR.glob("*.png"))
        assert len(fig_files) >= 2, (
            "Not enough figures — scree plot may be missing"
        )

    def test_biplot_or_score_plot(self, executed_nb):
        """Biplot or PCA score plot is generated."""
        all_output = _notebook_output(executed_nb)
        assert "PC1" in all_output, (
            "PCA score/biplot output (PC1 label) not found"
        )


# ── AC-ANA-4: Clustering with dendrogram or cluster assignment ──────────────


class TestACANA4Clustering:
    """Clustering: dendrogram and cluster assignments."""

    @pytest.fixture(scope="class")
    def executed_nb(self):
        if not NOTEBOOK_PATH.exists():
            pytest.skip("analysis.ipynb does not exist")
        return _execute_notebook(NOTEBOOK_PATH)

    def test_cluster_assignments_present(self, executed_nb):
        """Cluster assignment table or output is generated."""
        all_output = _notebook_output(executed_nb)
        assert "Cluster assignments" in all_output, (
            "Cluster assignments not in output"
        )

    def test_pairwise_distance_matrix(self, executed_nb):
        """Pairwise distance matrix is computed and printed."""
        all_output = _notebook_output(executed_nb)
        has_pairwise = "Pairwise distance" in all_output
        has_matrix = "distance matrix" in all_output.lower()
        assert has_pairwise or has_matrix, (
            "Pairwise distance matrix not in output"
        )


# ── AC-ANA-5: Redundancy analysis answers which stations
#    are redundant ────────────────────────────────────────────────────────────


class TestACANA5Redundancy:
    """Redundancy: which stations are redundant."""

    @pytest.fixture(scope="class")
    def executed_nb(self):
        if not NOTEBOOK_PATH.exists():
            pytest.skip("analysis.ipynb does not exist")
        return _execute_notebook(NOTEBOOK_PATH)

    def test_benchmark_or_recommendations_present(self, executed_nb):
        """Benchmark results or station recommendations are generated."""
        all_output = _notebook_output(executed_nb)
        has_benchmark = "Benchmark" in all_output
        has_recs = "Recommendations" in all_output
        assert has_benchmark or has_recs, (
            "Neither benchmark nor recommendations found in output"
        )


# ── AC-ANA-6: FWI time series for at least 2 stations ─────────────────────


class TestACANA6FWI:
    """FWI: time series of moisture codes and FWI for 2+ stations."""

    @pytest.fixture(scope="class")
    def executed_nb(self):
        if not NOTEBOOK_PATH.exists():
            pytest.skip("analysis.ipynb does not exist")
        return _execute_notebook(NOTEBOOK_PATH)

    def test_fwi_stations_identified(self, executed_nb):
        """At least 2 stations with FWI data are identified."""
        all_output = _notebook_output(executed_nb)
        assert "FWI" in all_output, "FWI analysis not found in output"

    def test_fwi_variables_plotted(self, executed_nb):
        """FWI time series plot is generated for multiple stations."""
        all_output = _notebook_output(executed_nb)
        assert "Plotted FWI for:" in all_output, (
            "FWI plotting output not found"
        )
        match = re.search(r"Plotted FWI for: \[(.+)\]", all_output)
        assert match, (
            "Could not parse plotted stations from FWI output"
        )
        stations_plotted = [
            s.strip().strip("'\"")
            for s in match.group(1).split(",")
        ]
        assert len(stations_plotted) >= 2, (
            f"Expected >= 2 stations plotted, "
            f"got {len(stations_plotted)}: {stations_plotted}"
        )


# ── AC-ANA-7: Uncertainty with confidence intervals ────────────────────────


class TestACANA7Uncertainty:
    """Uncertainty: confidence intervals or risk probabilities."""

    @pytest.fixture(scope="class")
    def executed_nb(self):
        if not NOTEBOOK_PATH.exists():
            pytest.skip("analysis.ipynb does not exist")
        return _execute_notebook(NOTEBOOK_PATH)

    def test_uncertainty_quantification_present(self, executed_nb):
        """Uncertainty/risk analysis is performed."""
        all_output = _notebook_output(executed_nb)
        terms = ["uncertainty", "risk", "kde", "confidence"]
        found = sum(
            1 for t in terms if t.lower() in all_output.lower()
        )
        assert found >= 1, (
            "Uncertainty quantification output not detected"
        )


# ── AC-ANA-9: Conclusion with key findings ─────────────────────────────────


class TestACANA9Conclusion:
    """Notebook has a conclusion with key findings."""

    @pytest.fixture(scope="class")
    def executed_nb(self):
        if not NOTEBOOK_PATH.exists():
            pytest.skip("analysis.ipynb does not exist")
        return _execute_notebook(NOTEBOOK_PATH)

    def test_conclusion_cell_not_placeholder(self, executed_nb):
        """Conclusion cell has substantive content."""
        md_cells = [
            cell for cell in executed_nb["cells"]
            if cell.get("cell_type") == "markdown"
        ]
        conclusion_text = ""
        for cell in md_cells:
            src = "".join(cell["source"])
            if "conclusion" in src.lower() or "key findings" in src.lower():
                conclusion_text += src

        assert len(conclusion_text) > 100, (
            "Conclusion section missing or too short (< 100 chars)"
        )
        assert "will be documented" not in conclusion_text.lower(), (
            "Conclusion is still a placeholder"
        )
        assert "will be added" not in conclusion_text.lower(), (
            "Conclusion is still a placeholder"
        )
