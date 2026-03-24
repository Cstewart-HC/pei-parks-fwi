"""Phase 10 exit gate tests — analysis notebook verification.

Tests execute the notebook programmatically (papermill-free, via nbconvert)
and verify each acceptance criterion against cell outputs.
"""

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


def _get_cell_outputs(nb: nbformat.NotebookNode, cell_type="code") -> list:
    """Extract outputs from all code cells."""
    outputs = []
    for cell in nb.cells:
        if cell.cell_type == cell_type:
            outputs.append(cell.get("outputs", []))
    return outputs


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
        # The notebook should produce output mentioning processed station data
        # Check that at least one station's processed data is loaded
        station_dirs = [
            d.name
            for d in PROCESSED_DIR.iterdir()
            if d.is_dir() and d.name != "stanhope"  # stanhope is reference
        ]
        assert len(station_dirs) > 0, (
            "No station processed data directories found"
        )

        # The notebook output should reference at least one station by name
        # or show row counts from processed files
        found = False
        for station in station_dirs:
            if station.lower() in all_output.lower():
                found = True
                break
        assert found, (
            f"Notebook output does not reference any processed station data. "
            f"Stations available: {station_dirs}"
        )
