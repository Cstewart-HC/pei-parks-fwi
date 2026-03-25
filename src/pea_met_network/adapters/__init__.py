"""PEA Met Network adapters package.

Exports:
    ADAPTER_REGISTRY — extension-to-adapter class mapping
    CANONICAL_SCHEMA — list of canonical column names
    route_by_extension — factory function for adapters
    BaseAdapter — abstract base class
"""

from pea_met_network.adapters.registry import ADAPTER_REGISTRY, route_by_extension
from pea_met_network.adapters.schema import CANONICAL_SCHEMA
from pea_met_network.adapters.base import BaseAdapter

__all__ = [
    "ADAPTER_REGISTRY",
    "CANONICAL_SCHEMA",
    "route_by_extension",
    "BaseAdapter",
]
