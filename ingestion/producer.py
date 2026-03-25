"""
Kafka Producer Service
- Batch send with configurable batch.size & linger.ms
- Retry with exponential backoff
- Two modes: --simulate (synthetic) / --file (read existing JSON)
"""

import json
import time
import sys
import argparse
import logging
from pathlib import Path
from typing import List
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

from ingestion.schema import (
    TrafficEvent,
    generate_synthetic_event,
    generate_batch,
    convert_old_to_new,
    ROAD_SEGMENTS,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TrafficProducer")

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "traffic-stream"
BATCH_SIZE = 32768       # 32KB batch
LINGER_MS = 10           # Wait up to 10ms to batch
MAX_RETRIES = 5
RETRY_BACKOFF_MS = 500


class TrafficProducer:
    """Kafka Producer with batch send & retry"""

    def __init__(self, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS):
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self._connect()

    def _connect(self):
        """Connect to Kafka with retry"""
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    batch_size=BATCH_SIZE,
                    linger_ms=LINGER_MS,
                    retries=MAX_RETRIES,
                    retry_backoff_ms=RETRY_BACKOFF_MS,
                    acks="all",
                    compression_type="gzip",
                    max_in_flight_requests_per_connection=5,
                )
                logger.info(f"✅ Connected to Kafka at {self.bootstrap_servers}")
                return
            except NoBrokersAvailable:
                wait = RETRY_BACKOFF_MS * attempt / 1000
                logger.warning(f"⏳ Attempt {attempt}/{MAX_RETRIES} — Kafka not available. Retry in {wait}s...")
                time.sleep(wait)

        logger.error("❌ Cannot connect to Kafka after retries")
        sys.exit(1)

    def send_event(self, event: TrafficEvent):
        """Send single event with road_id as key (partition by road)"""
        try:
            future = self.producer.send(
                TOPIC,
                key=event.road_id,
                value=event.to_dict(),
            )
            return future
        except KafkaError as e:
            logger.error(f"❌ Send failed: {e}")
            return None

    def send_batch(self, events: List[TrafficEvent]):
        """Send batch of events"""
        futures = []
        for event in events:
            future = self.send_event(event)
            if future:
                futures.append(future)

        # Flush to ensure all sent
        if self.producer:
            self.producer.flush()

        success = 0
        failed = 0
        for f in futures:
            try:
                f.get(timeout=10)
                success += 1
            except Exception:
                failed += 1

        return success, failed

    def close(self):
        if self.producer:
            try:
                self.producer.flush()
                self.producer.close()
            except Exception:
                pass
            logger.info("🛑 Producer closed")


def run_simulate_mode(rate=10, duration=None):
    """Generate and stream synthetic traffic data"""
    producer = TrafficProducer()
    logger.info(f"🚀 Simulate mode: {rate} events/sec")

    total_sent = 0
    start_time = time.time()
    interval = 1.0 / rate

    try:
        while True:
            event = generate_synthetic_event()
            producer.send_event(event)
            total_sent += 1

            if total_sent % 100 == 0:
                elapsed = time.time() - start_time
                actual_rate = total_sent / elapsed if elapsed > 0 else 0
                logger.info(
                    f"📡 Sent {total_sent} events | "
                    f"Rate: {actual_rate:.0f}/s | "
                    f"Last: {event.road_id} speed={event.speed}km/h count={event.vehicle_count}"
                )

            if duration and (time.time() - start_time) >= duration:
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        logger.info("🛑 Stopping producer...")
    finally:
        producer.close()
        elapsed = time.time() - start_time
        logger.info(f"📊 Total sent: {total_sent} events in {elapsed:.1f}s")


import ijson

def run_file_mode(file_path: str, batch_size=500):
    """Read existing JSON file and convert → send to Kafka using streaming"""
    producer = TrafficProducer()
    path = Path(file_path)

    if not path.exists():
        logger.error(f"❌ File not found: {file_path}")
        return

    logger.info(f"📂 Reading file (streaming): {file_path}")
    logger.info(f"   Batch size: {batch_size}")

    try:
        total_sent: int = 0
        total_failed: int = 0
        total_skipped: int = 0
        batch: List[TrafficEvent] = []

        with open(path, "rb") as f:
            # ijson expects binary file
            parser = ijson.items(f, "item")
            
            for i, record in enumerate(parser):
                # Convert old format → new
                event = convert_old_to_new(record)
                if event is None:
                    total_skipped += 1
                    continue

                batch.append(event)

                if len(batch) >= batch_size:
                    success, failed = producer.send_batch(batch)
                    total_sent += success
                    total_failed += failed
                    batch = []

                    if (i + 1) % 1000 == 0:
                        logger.info(f"📡 Progress: {i + 1} records processed | Sent: {total_sent}")

        # Send remaining
        if batch:
            success, failed = producer.send_batch(batch)
            total_sent += success
            total_failed += failed

        logger.info(f"✅ File ingestion complete:")
        logger.info(f"   Processed: {total_sent + total_failed + total_skipped} | Sent: {total_sent} | Failed: {total_failed} | Skipped: {total_skipped}")

    except ijson.common.IncompleteJSONError as e:
        logger.error(f"❌ Incomplete JSON: {e}")
    except Exception as e:
        logger.error(f"❌ Error during file ingestion: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        producer.close()


def main():
    parser = argparse.ArgumentParser(description="Traffic Kafka Producer")
    parser.add_argument("--mode", choices=["simulate", "file"], default="simulate",
                        help="simulate: synthetic data / file: read JSON file")
    parser.add_argument("--rate", type=int, default=10,
                        help="Events per second (simulate mode)")
    parser.add_argument("--duration", type=int, default=None,
                        help="Duration in seconds (simulate mode, None=infinite)")
    parser.add_argument("--file", type=str, default=None,
                        help="Path to JSON file (file mode)")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Batch size for file ingestion")

    args = parser.parse_args()

    if args.mode == "simulate":
        run_simulate_mode(rate=args.rate, duration=args.duration)
    elif args.mode == "file":
        if not args.file:
            logger.error("❌ --file is required for file mode")
            sys.exit(1)
        run_file_mode(args.file, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
