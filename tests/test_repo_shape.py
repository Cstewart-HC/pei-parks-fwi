from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

ALLOWED_ROOT_FILES = {
    "README.md",
    "pyproject.toml",
    "requirements.txt",
    ".gitignore",
    ".pre-commit-config.yaml",
    "analysis.ipynb",
    "Makefile",
    "requirements-dev.txt",
    "codemap.md",
    "requirements.lock",
    "analysis_executed.ipynb",
}

ALLOWED_ROOT_DIRS = {
    "src",
    "tests",
    "docs",
    "data",
    "notebooks",
    "specs",
    "scripts",
    "__pycache__",
    ".git",
    ".pytest_cache",
    ".ruff_cache",
    ".mypy_cache",
    ".venv",
    ".cartography",
}


def test_repo_root_shape_is_whitelisted() -> None:
    entries = sorted(REPO_ROOT.iterdir(), key=lambda p: p.name)
    unexpected: list[str] = []

    for entry in entries:
        if entry.is_file() and entry.name not in ALLOWED_ROOT_FILES:
            unexpected.append(entry.name)
            continue

        if entry.is_dir() and entry.name not in ALLOWED_ROOT_DIRS:
            unexpected.append(entry.name + "/")

    assert unexpected == [], (
        "Unexpected repo-root entries found: "
        + ", ".join(unexpected)
    )
