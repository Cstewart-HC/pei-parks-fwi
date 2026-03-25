"""Adapter registry — maps file extensions to adapter classes."""

from __future__ import annotations

from pathlib import Path

from pea_met_network.adapters.base import BaseAdapter
from pea_met_network.adapters.csv_adapter import CSVAdapter
from pea_met_network.adapters.json_adapter import JSONAdapter
from pea_met_network.adapters.xle_adapter import XLEAdapter
from pea_met_network.adapters.xlsx_adapter import XLSXAdapter

# Extension -> adapter class mapping
ADAPTER_REGISTRY: dict[str, type[BaseAdapter]] = {
    ".csv": CSVAdapter,
    ".xlsx": XLSXAdapter,
    ".xle": XLEAdapter,
    ".json": JSONAdapter,
}

# Known data extensions — anything else is an error
KNOWN_EXTENSIONS = set(ADAPTER_REGISTRY.keys())


def route_by_extension(path: Path) -> BaseAdapter:
    """Return the appropriate adapter for a file based on its extension.

    Raises ValueError for unknown file formats.
    """
    ext = path.suffix.lower()
    if ext not in KNOWN_EXTENSIONS:
        raise ValueError(
            f"Unknown file format '{ext}' for file: {path}. "
            f"Supported formats: {', '.join(sorted(KNOWN_EXTENSIONS))}"
        )
    return ADAPTER_REGISTRY[ext]()
