"""Smoke tests for Phase 8 deliverables (AC-DLV-5)."""

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class TestCleaningEntrypoint:
    """AC-DLV-2: cleaning.py pipeline entrypoint."""

    @classmethod
    def setup_class(cls):
        import sys
        from pathlib import Path
        repo_root = Path(__file__).resolve().parents[1]
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))

    def test_cleaning_module_importable(self):
        from pea_met_network import cleaning  # noqa: F401

    def test_cleaning_main_exists(self):
        from pea_met_network import cleaning

        assert callable(getattr(cleaning, "main", None)), (
            "cleaning.main() must exist and be callable"
        )


class TestAnalysisNotebook:
    """AC-DLV-3: analysis.ipynb exists and is valid."""

    def test_analysis_notebook_exists(self):
        nb_path = ROOT / "analysis.ipynb"
        assert nb_path.is_file(), "analysis.ipynb must exist at repository root"

    def test_analysis_notebook_valid_json(self):
        nb_path = ROOT / "analysis.ipynb"
        with open(nb_path) as f:
            nb = json.load(f)
        assert nb.get("nbformat") == 4, "notebook must be nbformat v4"
        assert nb.get("metadata", {}).get(
            "kernelspec", {}
        ).get("language") == "python"


class TestReadme:
    """AC-DLV-1: README.md with setup and execution instructions."""

    def test_readme_exists(self):
        readme = ROOT / "README.md"
        assert readme.is_file(), "README.md must exist at repository root"

    def test_readme_mentions_parks_canada(self):
        readme = (ROOT / "README.md").read_text()
        assert "Parks Canada" in readme, "README must mention Parks Canada"

    def test_readme_has_installation_instructions(self):
        readme = (ROOT / "README.md").read_text()
        lower = readme.lower()
        assert ("installation" in lower or "install" in lower), (
            "README must contain installation instructions"
        )

    def test_readme_describes_cleaning_pipeline(self):
        readme = (ROOT / "README.md").read_text()
        assert "pea_met_network" in readme or "cleaning" in readme, (
            "README must describe how to run the pipeline"
        )

    def test_readme_describes_analysis_notebook(self):
        readme = (ROOT / "README.md").read_text()
        assert "analysis.ipynb" in readme, (
            "README must describe how to run analysis.ipynb"
        )

    def test_readme_mentions_osemn(self):
        readme = (ROOT / "README.md").read_text()
        assert "OSEMN" in readme, (
            "README must reference the OSEMN pipeline structure"
        )

    def test_readme_lists_key_outputs(self):
        readme = (ROOT / "README.md").read_text()
        lower = readme.lower()
        # At least two of: cleaned datasets, fwi, redundancy, uncertainty
        hits = sum(
            kw in lower
            for kw in ["cleaned", "fwi", "redundancy", "uncertainty"]
        )
        assert hits >= 2, (
            f"README must list key outputs (found {hits}/2 required keywords)"
        )
