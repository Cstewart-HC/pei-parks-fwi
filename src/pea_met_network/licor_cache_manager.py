"""Licor JSON cache compaction — consolidate old weekly chunks into monthly files.

Run periodically or automatically when chunk count exceeds threshold.
Keeps the cache directory manageable without losing any data.

Usage:
    python -m pea_met_network.licor_cache_manager
    python -m pea_met_network.licor_cache_manager --dry-run
    python -m pea_met_network.licor_cache_manager --older-than 60
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

logger_name = __name__

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
LICOR_DIR = PROJECT_ROOT / "data" / "raw" / "licor"

# Chunks older than this many days get compacted
DEFAULT_OLDER_THAN_DAYS = 30

# Filename patterns
CHUNK_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})\.json$")
COMBINED_PATTERN = re.compile(r"(.+)_(\d{4}-\d{2}-\d{2})_combined\.json$")
MONTHLY_PATTERN = re.compile(r"(\d{4}-\d{2})_monthly\.json$")


def _parse_end_date(filename: str) -> datetime | None:
    """Extract end date from a JSON cache filename."""
    m = CHUNK_PATTERN.match(filename) or COMBINED_PATTERN.match(filename)
    if m:
        return datetime.strptime(m.group(2), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    # Old format: 2025-11-01_to_2025-12-31.json
    m2 = re.match(r"(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.json$", filename)
    if m2:
        return datetime.strptime(m2.group(2), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return None


def _parse_start_date(filename: str) -> datetime | None:
    """Extract start date from a JSON cache filename."""
    m = CHUNK_PATTERN.match(filename)
    if m:
        return datetime.strptime(m.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    m2 = re.match(r"(\d{4}-\d{2}-\d{2})_to_(\d{4}-\d{2}-\d{2})\.json$", filename)
    if m2:
        return datetime.strptime(m2.group(1), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return None


def compact_device_cache(
    device_dir: Path,
    older_than_days: int = DEFAULT_OLDER_THAN_DAYS,
    dry_run: bool = False,
) -> int:
    """Consolidate weekly JSON chunks older than threshold into monthly files.

    Reads all non-combined chunk files in device_dir, groups by month,
    merges sensors, writes monthly combined JSON, deletes original chunks.

    Args:
        device_dir: Directory containing chunk JSON files for one device.
        older_than_days: Only compact chunks whose end-date is this many days old.
        dry_run: If True, report what would happen without writing or deleting.

    Returns:
        Number of chunk files removed (or that would be removed in dry-run).
    """
    import logging
    logger = logging.getLogger(logger_name)

    if not device_dir.is_dir():
        return 0

    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - (older_than_days * 86400)

    # Collect compactable chunks (non-combined, older than threshold)
    chunks_by_month: dict[str, list[Path]] = {}
    for fpath in sorted(device_dir.glob("*.json")):
        # Skip combined files and existing monthly files
        if fpath.name == "devices.json":
            continue
        if "_combined.json" in fpath.name:
            continue
        if MONTHLY_PATTERN.match(fpath.name):
            continue

        end_dt = _parse_end_date(fpath.name)
        if end_dt is None:
            continue
        if end_dt.timestamp() >= cutoff:
            continue  # too recent

        # Group by year-month of the START date
        start_dt = _parse_start_date(fpath.name)
        if start_dt is None:
            continue
        month_key = start_dt.strftime("%Y-%m")
        chunks_by_month.setdefault(month_key, []).append(fpath)

    if not chunks_by_month:
        return 0

    total_removed = 0

    for month_key, chunk_files in sorted(chunks_by_month.items()):
        # Merge sensors from all chunks for this month
        merged_sensors: dict[str, dict] = {}
        for chunk_path in chunk_files:
            try:
                with open(chunk_path) as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError):
                continue

            for sensor in data.get("sensors", []):
                sid = sensor.get("sensorSerialNumber", "")
                data_entries = sensor.get("data", [])
                records: list = []
                for entry in data_entries:
                    records.extend(entry.get("records", []))

                if not records:
                    continue

                if sid not in merged_sensors:
                    merged_sensors[sid] = {
                        "sensorSerialNumber": sid,
                        "measurementType": (
                            data_entries[0].get("measurementType", "unknown")
                            if data_entries else "unknown"
                        ),
                        "units": (
                            data_entries[0].get("units", "unknown")
                            if data_entries else "unknown"
                        ),
                        "totalRecords": 0,
                        "records": [],
                    }
                merged_sensors[sid]["records"].extend(records)

        # Deduplicate and sort
        for sid in merged_sensors:
            seen: set[int] = set()
            deduped: list = []
            for rec in merged_sensors[sid]["records"]:
                ts = rec[0]
                if ts not in seen:
                    seen.add(ts)
                    deduped.append(rec)
            merged_sensors[sid]["records"] = sorted(deduped, key=lambda x: x[0])
            merged_sensors[sid]["totalRecords"] = len(merged_sensors[sid]["records"])

        if not merged_sensors:
            continue

        total_records = sum(s["totalRecords"] for s in merged_sensors.values())

        if dry_run:
            logger.info(
                "  %s: would compact %d chunks → %s_monthly.json (%d records)",
                device_dir.name, len(chunk_files), month_key, total_records,
            )
            total_removed += len(chunk_files)
            continue

        # Write monthly file
        monthly_path = device_dir / f"{month_key}_monthly.json"
        monthly_data = {
            "source": "licor_cache_manager.compact",
            "device": device_dir.name,
            "month": month_key,
            "chunks_compacted": len(chunk_files),
            "fetchTime": now.isoformat(),
            "sensors": merged_sensors,
        }
        with open(monthly_path, "w") as f:
            json.dump(monthly_data, f, indent=2)

        logger.info(
            "  %s: compacted %d chunks → %s (%d records)",
            device_dir.name, len(chunk_files), monthly_path.name, total_records,
        )

        # Remove original chunks
        for chunk_path in chunk_files:
            chunk_path.unlink()
        total_removed += len(chunk_files)

    return total_removed


def compact_all_devices(
    older_than_days: int = DEFAULT_OLDER_THAN_DAYS,
    dry_run: bool = False,
) -> int:
    """Run compaction for all device directories under data/raw/licor/.

    Returns total chunk files removed.
    """
    if not LICOR_DIR.exists():
        return 0

    total = 0
    for entry in sorted(LICOR_DIR.iterdir()):
        if entry.is_dir() and entry.name != "devices.json":
            total += compact_device_cache(entry, older_than_days, dry_run)
    return total


def should_compact(device_dir: Path, threshold: int = 8) -> bool:
    """Check if a device directory has enough chunk files to warrant compaction."""
    if not device_dir.is_dir():
        return False
    count = sum(
        1 for f in device_dir.glob("*.json")
        if f.name != "devices.json"
        and "_combined.json" not in f.name
        and not MONTHLY_PATTERN.match(f.name)
        and CHUNK_PATTERN.match(f.name)
    )
    return count >= threshold


def main() -> None:
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s: %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Compact old Licor JSON cache chunks into monthly files"
    )
    parser.add_argument(
        "--older-than",
        type=int,
        default=DEFAULT_OLDER_THAN_DAYS,
        help=f"Compact chunks older than N days (default: {DEFAULT_OLDER_THAN_DAYS})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be compacted without making changes",
    )
    args = parser.parse_args()

    total = compact_all_devices(args.older_than, args.dry_run)
    label = "would be" if args.dry_run else "were"
    print(f"Compaction: {total} chunk files {label} removed.")


if __name__ == "__main__":
    main()
