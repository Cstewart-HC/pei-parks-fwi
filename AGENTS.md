# Workspace Agent Rules

Work deliberately. Keep command churn low. Prefer reliable changes over clever ones.

## Python
- Always use `.venv/bin/python` and `.venv/bin/pip`. Never install to system Python.
- If `.venv` doesn't exist and the project has a `pyproject.toml` or `requirements.txt`, create one first: `python3 -m venv .venv && .venv/bin/pip install -e ".[dev]"`
