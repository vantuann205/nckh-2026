"""
Stream Processing Consumer
Python Kafka Consumer with:
- JSON deserialization & validation
- In-memory rolling aggregation per road_id
- Tumbling window processing (5s / 10s / 1min)
- Congestion detection
- Output to Redis (latest + rolling window)
- Redis Pub/Sub for WebSocket push
"""

import json
import time
import signal
import logging
import threading
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Optional

import redis
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

from stream_processing.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP,
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_CHANNEL,
    ROAD_DATA_TTL, ROAD_WINDOW_TTL, SUMMARY_TTL, CONGESTED_TTL,
    CONGESTION_SPEED_THRESHOLD, CONGESTION_VEHICLE_THRESHOLD,
    SLOW_SPEED_THRESHOLD, PRIMARY_WINDOW,
    CONSUMER_POLL_TIMEOUT_MS, CONSUMER_MAX_POLL_RECORDS,
    AGGREGATION_INTERVAL,
)
from ingestion.schema import TrafficEvent, validate_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("StreamProcessor")


class RoadAggregator:
    """In-memory aggregator for a single road segment"""

    def __init__(self, road_id: str):
        self.road_id = road_id
        self.events: List[dict] = []
        self.last_flush = time.time()

    def add(self, event: dict):
        self.events.append(event)

    def flush(self) -> Optional[dict]:
        """Aggregate current events → summary for this road"""
        if not self.events:
            return None

        speeds = [e["speed"] for e in self.events]
        counts = [e["vehicle_count"] for e in self.events]
        lats = [e["lat"] for e in self.events]
        lngs = [e["lng"] for e in self.events]

        avg_speed = sum(speeds) / len(speeds)
        total_vehicles = sum(counts)
        avg_vehicles = total_vehicles / len(counts)

        # Congestion detection
        if avg_speed < CONGESTION_SPEED_THRESHOLD and avg_vehicles > CONGESTION_VEHICLE_THRESHOLD:
            status = "congested"
        elif avg_speed < SLOW_SPEED_THRESHOLD:
            status = "slow"
        else:
            status = "normal"

        result = {
            "road_id": self.road_id,
            "avg_speed": round(avg_speed, 1),
            "max_speed": round(max(speeds), 1),
            "min_speed": round(min(speeds), 1),
            "vehicle_count": total_vehicles,
            "avg_vehicle_count": round(avg_vehicles, 1),
            "event_count": len(self.events),
            "status": status,
            "lat": round(sum(lats) / len(lats), 6),
            "lng": round(sum(lngs) / len(lngs), 6),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        self.events.clear()
        self.last_flush = time.time()
        return result


class StreamProcessor:
    """Main stream processor: Kafka → Process → Redis"""

    def __init__(self):
        self.running = False
        self.aggregators: Dict[str, RoadAggregator] = defaultdict(
            lambda: RoadAggregator("")
        )
        self.redis_client: Optional[redis.Redis] = None
        self.consumer: Optional[KafkaConsumer] = None

        # Stats
        self.stats = {
            "total_received": 0,
            "total_processed": 0,
            "total_invalid": 0,
            "total_flushed": 0,
            "start_time": None,
        }

    def _connect_redis(self):
        """Connect to Redis"""
        try:
            self.redis_client = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=5,
            )
            self.redis_client.ping()
            logger.info(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
        except redis.ConnectionError as e:
            logger.error(f"❌ Cannot connect to Redis: {e}")
            raise

    def _connect_kafka(self):
        """Connect to Kafka consumer"""
        retries = 5
        for attempt in range(1, retries + 1):
            try:
                self.consumer = KafkaConsumer(
                    KAFKA_TOPIC,
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    group_id=KAFKA_CONSUMER_GROUP,
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    enable_auto_commit=True,
                    max_poll_records=CONSUMER_MAX_POLL_RECORDS,
                    consumer_timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
                )
                logger.info(f"✅ Connected to Kafka topic '{KAFKA_TOPIC}'")
                return
            except NoBrokersAvailable:
                wait = attempt * 2
                logger.warning(f"⏳ Attempt {attempt}/{retries} — Kafka not available. Retry in {wait}s...")
                time.sleep(wait)

        logger.error("❌ Cannot connect to Kafka")
        raise Exception("Kafka connection failed")

    def _process_message(self, raw_data: dict):
        """Process a single Kafka message"""
        self.stats["total_received"] += 1

        # Validate
        event = validate_event(raw_data)
        if event is None:
            self.stats["total_invalid"] += 1
            return

        self.stats["total_processed"] += 1

        # Add to aggregator
        road_id = event.road_id
        if road_id not in self.aggregators:
            self.aggregators[road_id] = RoadAggregator(road_id)
        self.aggregators[road_id].add(event.to_dict())

    def _flush_aggregations(self):
        """Flush all aggregators to Redis"""
        if not self.redis_client:
            return

        now = time.time()
        flushed_roads = []
        pipeline = self.redis_client.pipeline()
        congested_roads = []
        all_speeds = []
        all_vehicles = []

        for road_id, aggregator in self.aggregators.items():
            result = aggregator.flush()
            if result is None:
                continue

            flushed_roads.append(road_id)
            self.stats["total_flushed"] += 1

            # === Write to Redis ===

            # 1. Latest data: road:{road_id}
            road_key = f"road:{road_id}"
            pipeline.hset(road_key, mapping=result)
            pipeline.expire(road_key, ROAD_DATA_TTL)

            # 2. Rolling window: road:{road_id}:window (sorted set, score=timestamp)
            window_key = f"road:{road_id}:window"
            score = now
            pipeline.zadd(window_key, {json.dumps(result): score})
            # Trim: remove entries older than 5 min
            pipeline.zremrangebyscore(window_key, 0, now - ROAD_WINDOW_TTL)
            pipeline.expire(window_key, ROAD_WINDOW_TTL)

            # Track congestion
            if result["status"] == "congested":
                congested_roads.append(road_id)

            all_speeds.append(result["avg_speed"])
            all_vehicles.append(result["vehicle_count"])

        # 3. Congested set
        if congested_roads:
            pipeline.delete("traffic:congested")
            pipeline.sadd("traffic:congested", *congested_roads)
            pipeline.expire("traffic:congested", CONGESTED_TTL)

        # 4. Global summary
        if all_speeds:
            summary = {
                "total_roads": len(flushed_roads),
                "avg_speed": round(sum(all_speeds) / len(all_speeds), 1),
                "total_vehicles": sum(all_vehicles),
                "congested_roads": len(congested_roads),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            pipeline.hset("traffic:summary", mapping=summary)
            pipeline.expire("traffic:summary", SUMMARY_TTL)

        # Execute pipeline
        pipeline.execute()

        # 5. Pub/Sub: notify backend WebSocket
        if flushed_roads:
            update_msg = json.dumps({
                "type": "traffic_update",
                "roads": flushed_roads,
                "congested": congested_roads,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            self.redis_client.publish(REDIS_CHANNEL, update_msg)

        if flushed_roads:
            logger.info(
                f"📤 Flushed {len(flushed_roads)} roads | "
                f"Congested: {len(congested_roads)} | "
                f"Total processed: {self.stats['total_processed']}"
            )

    def _aggregation_loop(self):
        """Background thread: periodically flush aggregations"""
        while self.running:
            time.sleep(AGGREGATION_INTERVAL)
            try:
                self._flush_aggregations()
            except Exception as e:
                logger.error(f"❌ Flush error: {e}")

    def start(self):
        """Start the stream processor"""
        logger.info("=" * 60)
        logger.info("🚀 STREAM PROCESSOR STARTING")
        logger.info("=" * 60)

        # Connect
        self._connect_redis()
        self._connect_kafka()

        self.running = True
        self.stats["start_time"] = time.time()

        # Start aggregation flush thread
        flush_thread = threading.Thread(target=self._aggregation_loop, daemon=True)
        flush_thread.start()
        logger.info(f"⏱️  Aggregation interval: {AGGREGATION_INTERVAL}s")

        # Signal handler for graceful shutdown
        def handle_shutdown(signum, frame):
            logger.info("🛑 Shutdown signal received...")
            self.stop()

        signal.signal(signal.SIGINT, handle_shutdown)
        signal.signal(signal.SIGTERM, handle_shutdown)

        logger.info("📡 Listening for events...")

        # Main consumer loop
        while self.running:
            try:
                # Poll messages (non-blocking with timeout)
                message_batch = self.consumer.poll(
                    timeout_ms=CONSUMER_POLL_TIMEOUT_MS,
                    max_records=CONSUMER_MAX_POLL_RECORDS,
                )

                for topic_partition, messages in message_batch.items():
                    for message in messages:
                        self._process_message(message.value)

            except StopIteration:
                continue
            except Exception as e:
                logger.error(f"❌ Consumer error: {e}")
                time.sleep(1)

        # Final flush
        self._flush_aggregations()
        self._print_stats()

    def stop(self):
        """Stop the processor"""
        self.running = False
        if self.consumer:
            self.consumer.close()
        logger.info("🛑 Stream processor stopped")

    def _print_stats(self):
        """Print final stats"""
        elapsed = time.time() - (self.stats["start_time"] or time.time())
        rate = self.stats["total_processed"] / elapsed if elapsed > 0 else 0
        logger.info("=" * 60)
        logger.info("📊 STREAM PROCESSOR STATS")
        logger.info(f"   Duration:  {elapsed:.1f}s")
        logger.info(f"   Received:  {self.stats['total_received']}")
        logger.info(f"   Processed: {self.stats['total_processed']}")
        logger.info(f"   Invalid:   {self.stats['total_invalid']}")
        logger.info(f"   Flushed:   {self.stats['total_flushed']} aggregations")
        logger.info(f"   Rate:      {rate:.0f} events/s")
        logger.info("=" * 60)


def main():
    processor = StreamProcessor()
    processor.start()


if __name__ == "__main__":
    main()
