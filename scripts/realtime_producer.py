"""
High-speed batch producer.
Sends processed dataset to /traffic/ingest in large batches via HTTP.
Target: tens of thousands of vehicles/second.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path

import httpx

from processing.offline_pipeline import PROCESSED_DIR, ensure_processed_dataset

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RealtimeProducer")

OFFSET_FILE = PROCESSED_DIR / "producer_offset.json"
DEFAULT_BATCH_SIZE = int(os.getenv("PRODUCER_BATCH_SIZE", "500"))
DEFAULT_INTERVAL = float(os.getenv("PRODUCER_INTERVAL", "0.05"))  # 50ms between batches


def load_offset(offset_file: Path) -> int:
    if not offset_file.exists():
        return 0
    try:
        with offset_file.open("r", encoding="utf-8") as f:
            return int(json.load(f).get("offset", 0))
    except Exception:
        return 0


def save_offset(offset_file: Path, offset: int):
    offset_file.parent.mkdir(parents=True, exist_ok=True)
    with offset_file.open("w", encoding="utf-8") as f:
        json.dump({"offset": int(offset)}, f)


def row_to_payload(row: dict) -> dict:
    event_time = row.get("event_time")
    if hasattr(event_time, "isoformat"):
        event_time = event_time.isoformat()
    elif event_time is None:
        event_time = ""
    return {
        "event_time": event_time,
        "location_key": row.get("location_key", ""),
        "road_id": row.get("road_id", row.get("location_key", "")),
        "road_name": row.get("road_name", ""),
        "district": row.get("district", ""),
        "lat": float(row.get("lat", 0) or 0),
        "lng": float(row.get("lng", 0) or 0),
        "speed_kmph": float(row.get("speed_kmph", 0) or 0),
        "vehicle_count": int(row.get("vehicle_count", 0) or 0),
        "weather_temp_c": float(row.get("weather_temp_c", 0) or 0),
        "humidity_pct": float(row.get("humidity_pct", 0) or 0),
        "weather_condition": str(row.get("weather_condition", "Unknown") or "Unknown"),
        "accident_severity": float(row.get("accident_severity", 0) or 0),
        "congestion_km": float(row.get("congestion_km", 0) or 0),
    }


def run(api_base_url: str, batch_size: int, interval_seconds: float, reset_offset: bool):
    df = ensure_processed_dataset(force_rebuild=False)
    if df.empty:
        logger.warning("Processed dataset is empty, producer stopped")
        return

    if reset_offset:
        save_offset(OFFSET_FILE, 0)

    offset = load_offset(OFFSET_FILE)
    if offset >= len(df):
        logger.info("Offset at end of dataset (%d/%d). Resetting to 0.", offset, len(df))
        offset = 0
        save_offset(OFFSET_FILE, 0)

    logger.info("Starting batch producer: offset=%d/%d batch_size=%d interval=%.3fs",
                offset, len(df), batch_size, interval_seconds)

    total_sent = 0
    t_start = time.time()

    with httpx.Client(timeout=30.0, limits=httpx.Limits(max_connections=10)) as client:
        idx = offset
        while idx < len(df):
            batch_end = min(idx + batch_size, len(df))
            batch = [row_to_payload(df.iloc[i].to_dict()) for i in range(idx, batch_end)]

            try:
                response = client.post(f"{api_base_url}/traffic/ingest/batch", json=batch)
                if response.status_code >= 300:
                    logger.warning("Batch ingest failed status=%s idx=%d", response.status_code, idx)
                    time.sleep(1.0)
                    continue

                total_sent += len(batch)
                idx = batch_end
                save_offset(OFFSET_FILE, idx)

                elapsed = time.time() - t_start
                rate = total_sent / elapsed if elapsed > 0 else 0
                logger.info("Sent %d/%d | %.0f vehicles/sec", idx, len(df), rate)

            except Exception as exc:
                logger.error("Producer request failed idx=%d err=%s", idx, exc)
                time.sleep(2.0)
                continue

            if interval_seconds > 0:
                time.sleep(interval_seconds)

    logger.info("Producer finished. Total sent: %d in %.1fs", total_sent, time.time() - t_start)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="High-speed batch producer")
    parser.add_argument("--api-base-url", default=os.getenv("API_BASE_URL", "http://localhost:8000"))
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    parser.add_argument("--reset", action="store_true", help="Reset offset to zero")
    args = parser.parse_args()
    run(args.api_base_url, args.batch_size, args.interval, args.reset)
