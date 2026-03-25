"""
Load & Latency Test
Simulate high-volume traffic and measure pipeline performance.
Target: 100k - 1M events, < 2s E2E latency
"""

import json
import time
import argparse
import logging
import threading
from datetime import datetime, timezone
from typing import List

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("LoadTest")


def test_producer_throughput(events=100000, rate=5000):
    """Test: How fast can the producer send events?"""
    from ingestion.schema import generate_batch, TrafficEvent
    from kafka import KafkaProducer

    logger.info(f"📊 PRODUCER THROUGHPUT TEST: {events} events at target rate {rate}/s")

    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            batch_size=65536,
            linger_ms=20,
            compression_type="gzip",
            acks=1,
        )
    except Exception as e:
        logger.error(f"❌ Cannot connect to Kafka: {e}")
        return

    total_sent = 0
    start = time.time()
    batch_size = min(500, events)
    interval = batch_size / rate

    try:
        while total_sent < events:
            batch = generate_batch(batch_size)
            for event in batch:
                producer.send("traffic-stream", key=event.road_id, value=event.to_dict())
                total_sent += 1

            if total_sent % 10000 == 0:
                elapsed = time.time() - start
                actual_rate = total_sent / elapsed
                logger.info(f"   📡 {total_sent}/{events} sent ({actual_rate:.0f}/s)")

            # Pace control
            expected_time = total_sent / rate
            elapsed = time.time() - start
            if elapsed < expected_time:
                time.sleep(expected_time - elapsed)

        producer.flush()
        elapsed = time.time() - start

        logger.info(f"\n✅ PRODUCER RESULTS:")
        logger.info(f"   Events:        {total_sent}")
        logger.info(f"   Duration:      {elapsed:.2f}s")
        logger.info(f"   Throughput:    {total_sent / elapsed:.0f} events/s")
        logger.info(f"   Target rate:   {rate}/s")
        logger.info(f"   Achieved:      {'✅ YES' if total_sent / elapsed >= rate * 0.9 else '❌ NO'}")

    except Exception as e:
        logger.error(f"❌ Error: {e}")
    finally:
        producer.close()


def test_e2e_latency():
    """Test: End-to-end latency from producer → Redis"""
    import redis

    logger.info("📊 END-TO-END LATENCY TEST")

    try:
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        r.ping()
    except Exception as e:
        logger.error(f"❌ Cannot connect to Redis: {e}")
        return

    from kafka import KafkaProducer
    from ingestion.schema import generate_synthetic_event

    try:
        producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks=1,
        )
    except Exception as e:
        logger.error(f"❌ Cannot connect to Kafka: {e}")
        return

    latencies = []
    test_count = 50

    for i in range(test_count):
        event = generate_synthetic_event()
        road_key = f"road:{event.road_id}"

        # Record send time
        send_time = time.time()

        # Send to Kafka
        producer.send("traffic-stream", key=event.road_id, value=event.to_dict())
        producer.flush()

        # Poll Redis for update (max 10s)
        updated = False
        for _ in range(100):  # 100 * 0.1s = 10s timeout
            data = r.hgetall(road_key)
            if data and data.get("updated_at", ""):
                try:
                    update_time = datetime.fromisoformat(data["updated_at"].replace("Z", "+00:00"))
                    if update_time.timestamp() >= send_time - 1:
                        latency = time.time() - send_time
                        latencies.append(latency)
                        updated = True
                        break
                except Exception:
                    pass
            time.sleep(0.1)

        if not updated:
            latencies.append(10.0)  # timeout

        if (i + 1) % 10 == 0:
            logger.info(f"   Progress: {i + 1}/{test_count}")

    producer.close()

    # Results
    latencies.sort()
    if latencies:
        avg = sum(latencies) / len(latencies)
        p50 = latencies[len(latencies) // 2]
        p95 = latencies[int(len(latencies) * 0.95)]
        p99 = latencies[int(len(latencies) * 0.99)]

        logger.info(f"\n✅ LATENCY RESULTS ({test_count} samples):")
        logger.info(f"   Average:  {avg * 1000:.0f}ms")
        logger.info(f"   P50:      {p50 * 1000:.0f}ms")
        logger.info(f"   P95:      {p95 * 1000:.0f}ms")
        logger.info(f"   P99:      {p99 * 1000:.0f}ms")
        logger.info(f"   Min:      {min(latencies) * 1000:.0f}ms")
        logger.info(f"   Max:      {max(latencies) * 1000:.0f}ms")
        logger.info(f"   < 2s:     {'✅ YES' if p95 < 2.0 else '❌ NO'}")


def test_redis_memory():
    """Test: Redis memory usage under load"""
    import redis

    logger.info("📊 REDIS MEMORY TEST")

    try:
        r = redis.Redis(host="localhost", port=6379, decode_responses=True)
        info = r.info("memory")
        logger.info(f"   Used memory:     {info['used_memory_human']}")
        logger.info(f"   Peak memory:     {info['used_memory_peak_human']}")
        logger.info(f"   Fragmentation:   {info.get('mem_fragmentation_ratio', 'N/A')}")

        # Count keys
        road_keys = sum(1 for _ in r.scan_iter(match="road:*"))
        logger.info(f"   Road keys:       {road_keys}")
        logger.info(f"   Congested:       {r.scard('traffic:congested')}")

    except Exception as e:
        logger.error(f"❌ Cannot connect to Redis: {e}")


def main():
    parser = argparse.ArgumentParser(description="Load & Latency Test")
    parser.add_argument("--events", type=int, default=100000, help="Number of events")
    parser.add_argument("--rate", type=int, default=5000, help="Events per second")
    parser.add_argument("--test", choices=["throughput", "latency", "memory", "all"], default="all")

    args = parser.parse_args()

    logger.info("=" * 60)
    logger.info("🧪 LOAD & LATENCY TEST")
    logger.info("=" * 60)

    if args.test in ("throughput", "all"):
        test_producer_throughput(events=args.events, rate=args.rate)

    if args.test in ("latency", "all"):
        test_e2e_latency()

    if args.test in ("memory", "all"):
        test_redis_memory()

    logger.info("\n✅ TESTS COMPLETE")


if __name__ == "__main__":
    main()
