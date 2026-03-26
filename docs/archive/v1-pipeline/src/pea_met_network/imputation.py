"""Conservative, auditable imputation for meteorological time series."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True)
class ImputationConfig:
    """Configuration for gap-length-based imputation strategy."""

    short_gap_max_hours: int = 3
    medium_gap_max_hours: int = 12

    # Strategy for short gaps: "interpolate" | "ffill" | "bfill"
    short_gap_method: str = "interpolate"
    # Strategy for long gaps: "preserve" (keep NaN) | "interpolate" | "ffill"
    long_gap_method: str = "preserve"


@dataclass(frozen=True)
class AuditRecord:
    """A single imputation audit entry."""

    station: str
    variable: str
    time_start: str
    time_end: str
    method: str
    count_affected: int


def impute_column(
    series: pd.Series,
    config: ImputationConfig | None = None,
) -> tuple[pd.Series, list[AuditRecord]]:
    """Impute a single variable column and return audit records.

    Short gaps (<= short_gap_max_hours): bounded interpolation or ffill/bfill.
    Long gaps (> medium_gap_max_hours): preserve missingness by default.

    Returns the imputed series and a list of audit records.
    """
    if config is None:
        config = ImputationConfig()

    result = series.copy()
    records: list[AuditRecord] = []

    if result.isna().sum() == 0:
        return result, records

    # Identify gap lengths by labeling consecutive NaN groups
    is_nan = result.isna()
    groups = (is_nan != is_nan.shift()).cumsum()
    nan_groups = groups[is_nan]

    if nan_groups.empty:
        return result, records

    for group_id in nan_groups.unique():
        mask = groups == group_id
        gap_length = mask.sum()

        # Infer station from series name if possible
        station = "unknown"
        variable = series.name if series.name else "unknown"

        # Determine time range of gap
        gap_indices = result.index[mask]
        time_start = str(gap_indices[0])
        time_end = str(gap_indices[-1])

        if gap_length <= config.short_gap_max_hours:
            method = config.short_gap_method
            if method == "interpolate":
                filled = result.interpolate(
                    method="linear", limit_direction="both"
                )
                result[mask] = filled[mask]
            elif method == "ffill":
                result[mask] = result.ffill()[mask]
            elif method == "bfill":
                result[mask] = result.bfill()[mask]
        elif gap_length <= config.medium_gap_max_hours:
            # Medium gaps: use short_gap_method but flag it
            method = config.short_gap_method + "_medium"
            if config.short_gap_method == "interpolate":
                filled = result.interpolate(
                    method="linear", limit_direction="both"
                )
                result[mask] = filled[mask]
            elif config.short_gap_method == "ffill":
                result[mask] = result.ffill()[mask]
        else:
            # Long gaps: preserve missingness
            method = config.long_gap_method

        records.append(AuditRecord(
            station=station,
            variable=variable,
            time_start=time_start,
            time_end=time_end,
            method=method,
            count_affected=int(gap_length),
        ))

    return result, records


def impute_frame(
    frame: pd.DataFrame,
    variables: list[str] | None = None,
    config: ImputationConfig | None = None,
    station_column: str = "station",
) -> tuple[pd.DataFrame, list[AuditRecord]]:
    """Impute multiple variables in a DataFrame.

    Returns the imputed frame and a consolidated list of audit records.
    """
    if config is None:
        config = ImputationConfig()

    result = frame.copy()
    all_records: list[AuditRecord] = []

    # Determine which columns to impute
    skip = {station_column, "timestamp_utc", "source_file", "schema_family"}
    if variables is None:
        variables = [c for c in frame.columns if c not in skip]

    for variable in variables:
        if variable not in result.columns:
            continue
        series = result[variable]

        # Process per-station if station column exists
        if station_column in result.columns:
            station_records: list[AuditRecord] = []
            for station, group in result.groupby(station_column):
                idx = group.index
                imputed, records = impute_column(
                    series.loc[idx].copy(), config
                )
                result.loc[idx, variable] = imputed
                # Override station in audit records
                for r in records:
                    station_records.append(AuditRecord(
                        station=station,
                        variable=variable,
                        time_start=r.time_start,
                        time_end=r.time_end,
                        method=r.method,
                        count_affected=r.count_affected,
                    ))
            all_records.extend(station_records)
        else:
            imputed, records = impute_column(series, config)
            result[variable] = imputed
            all_records.extend(records)

    return result, all_records


def audit_trail_to_dataframe(records: list[AuditRecord]) -> pd.DataFrame:
    """Convert audit records to a DataFrame for inspection or export."""
    if not records:
        return pd.DataFrame(columns=[
            "station", "variable", "time_start", "time_end",
            "method", "count_affected",
        ])
    return pd.DataFrame([vars(r) for r in records])
