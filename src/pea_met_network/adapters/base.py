"""Abstract base class for all format adapters."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd


class BaseAdapter(ABC):
    """Base class that all format adapters must extend."""

    @abstractmethod
    def load(self, path: Path) -> pd.DataFrame:
        """Load a file and return a DataFrame with canonical schema columns.

        The returned DataFrame must contain at minimum:
        - station (str)
        - timestamp_utc (datetime64[ns, UTC])
        """
        ...
