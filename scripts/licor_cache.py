#!/usr/bin/env python3
"""
Licor Cloud API data fetcher for PEINP stations.

Read-only, respectful rate limiting. Pulls timeseries data in weekly chunks
with 2-second delays between requests.

Usage:
    python scripts/licor_cache.py --device 21114831 --start 2026-01-01 --end 2026-03-24
    python scripts/licor_cache.py --device 21114831 --start 2026-01-01 --end 2026-03-24 --sensors-only
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

API_BASE = "https://api.licor.cloud/v2/data"
DELAY_SECONDS = 2  # Respectful delay between requests
CHUNK_DAYS = 7     # Pull one week at a time

def get_token():
    token = os.environ.get("HC_CS_PEIPCWX_PROD_RO", "")
    if not token:
        print("ERROR: HC_CS_PEIPCWX_PROD_RO environment variable not set", file=sys.stderr)
        sys.exit(1)
    return token

def fetch_data(device, start_ms, end_ms, token, sensor=None):
    """Fetch timeseries data for a device within a time range."""
    url = f"{API_BASE}?deviceSerialNumber={device}&startTime={start_ms}&endTime={end_ms}"
    if sensor:
        url += f"&sensorSerialNumber={sensor}"

    req = Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })

    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        return {"error": e.code, "message": str(e)}

def fetch_devices(token):
    """Fetch list of devices."""
    url = "https://api.licor.cloud/v2/devices?includeSensors=true"
    req = Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })
    try:
        with urlopen(req, timeout=30) as resp:
            return json.loads(resp.read().decode())
    except HTTPError as e:
        return {"error": e.code, "message": str(e)}

def dt_to_ms(dt_str):
    """Convert ISO date string to unix milliseconds."""
    dt = datetime.strptime(dt_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)

def ms_to_date(ms):
    """Convert unix milliseconds to date string."""
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d")

def main():
    parser = argparse.ArgumentParser(description="Fetch PEINP station data from Licor Cloud API (read-only)")
    parser.add_argument("--device", required=True, help="Device serial number")
    parser.add_argument("--start", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--sensors-only", action="store_true", help="Only list sensors, don't fetch data")
    parser.add_argument("--sensor", help="Optional: fetch specific sensor only")
    parser.add_argument("--output-dir", default=None, help="Output directory (default: data/raw/licor/<device>/)")
    parser.add_argument("--chunk-days", type=int, default=CHUNK_DAYS, help="Days per request chunk")
    parser.add_argument("--delay", type=float, default=DELAY_SECONDS, help="Delay between requests")
    args = parser.parse_args()

    token = get_token()

    # Setup output directory
    repo_root = Path(__file__).resolve().parent.parent
    output_dir = Path(args.output_dir) if args.output_dir else repo_root / "data" / "raw" / "licor" / args.device
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.sensors_only:
        print(f"Fetching device info for {args.device}...")
        result = fetch_devices(token)
        if "error" in result:
            print(f"ERROR: {result['message']}", file=sys.stderr)
            sys.exit(1)
        for dev in result.get("devices", []):
            if dev["deviceSerialNumber"] == args.device:
                print(f"Device: {dev['deviceName']} ({dev['deviceSerialNumber']})")
                print(f"  Product: {dev['productCode']}")
                print(f"  Last connection: {dev['lastConnectionTime']}")
                print("  Sensors:")
                for s in dev.get("sensors", []):
                    print(f"    {s['sensorSerialNumber']}: {s['measurementType']} ({s['units']})")
                return
        print("Device not found", file=sys.stderr)
        sys.exit(1)

    start_ms = dt_to_ms(args.start)
    end_ms = dt_to_ms(args.end)

    print(f"Fetching data for device {args.device}")
    print(f"  Range: {args.start} to {args.end}")
    print(f"  Chunk: {args.chunk_days} days, delay: {args.delay}s")
    print(f"  Output: {output_dir}")
    if args.sensor:
        print(f"  Sensor filter: {args.sensor}")

    all_chunks = []
    current = start_ms
    chunk_num = 0
    errors = 0

    while current < end_ms:
        chunk_end = min(current + (args.chunk_days * 86400000), end_ms)
        chunk_num += 1

        date_label = f"{ms_to_date(current)}_{ms_to_date(chunk_end - 1)}"
        print(f"  [{chunk_num}] {date_label}...", end=" ", flush=True)

        result = fetch_data(args.device, current, chunk_end, token, args.sensor)

        if "error" in result:
            print(f"ERROR {result['error']}: {result['message']}")
            errors += 1
            if result["error"] == 401:
                print("  Unauthorized - stopping", file=sys.stderr)
                sys.exit(1)
        else:
            total_records = sum(s.get("totalRecords", 0) for s in result.get("sensors", []))
            print(f"OK ({total_records} records)")

            # Save chunk
            chunk_file = output_dir / f"{date_label}.json"
            with open(chunk_file, "w") as f:
                json.dump(result, f, indent=2)
            all_chunks.append(result)

        if current < end_ms:
            time.sleep(args.delay)

        current = chunk_end

    # Save combined result
    combined_file = output_dir / f"{args.start}_{args.end}_combined.json"
    # Merge sensors across chunks
    merged = {"source": "licor_cache.py", "device": args.device,
              "start": args.start, "end": args.end, "chunks": chunk_num,
              "errors": errors, "fetchTime": datetime.now(timezone.utc).isoformat(),
              "sensors": {}}

    for chunk in all_chunks:
        for sensor in chunk.get("sensors", []):
            sid = sensor["sensorSerialNumber"]
            if sid not in merged["sensors"]:
                merged["sensors"][sid] = {
                    "sensorSerialNumber": sid,
                    "measurementType": sensor["data"][0]["measurementType"] if sensor.get("data") else "unknown",
                    "units": sensor["data"][0]["units"] if sensor.get("data") else "unknown",
                    "totalRecords": 0,
                    "records": []
                }
            merged["sensors"][sid]["totalRecords"] += sensor.get("totalRecords", 0)
            for d in sensor.get("data", []):
                merged["sensors"][sid]["records"].extend(d.get("records", []))

    # Deduplicate records by timestamp
    for sid in merged["sensors"]:
        seen = set()
        deduped = []
        for rec in merged["sensors"][sid]["records"]:
            if rec[0] not in seen:
                seen.add(rec[0])
                deduped.append(rec)
        merged["sensors"][sid]["records"] = sorted(deduped, key=lambda x: x[0])
        merged["sensors"][sid]["totalRecords"] = len(merged["sensors"][sid]["records"])

    with open(combined_file, "w") as f:
        json.dump(merged, f, indent=2)

    total = sum(s["totalRecords"] for s in merged["sensors"].values())
    print(f"\nDone: {chunk_num} chunks, {errors} errors, {total} total records")
    print(f"Combined file: {combined_file}")

if __name__ == "__main__":
    main()
