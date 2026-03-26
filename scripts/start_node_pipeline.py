"""
Start Pipeline với Node.js Consumer
Luồng: Producer (Python) → Kafka → Node.js Consumer → Redis → WebSocket → Frontend

Usage:
  python scripts/start_node_pipeline.py                          # simulate mode
  python scripts/start_node_pipeline.py --file data/traffic_data_0.json
"""

import subprocess
import sys
import os
import time
import signal
import logging
import argparse
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Pipeline")

PROJECT_ROOT = Path(__file__).parent.parent
processes = []


def check_service(name, check_fn):
    try:
        check_fn()
        logger.info(f"✅ {name} is running")
        return True
    except Exception:
        logger.warning(f"⚠️  {name} is NOT running")
        return False


def check_redis():
    import redis
    redis.Redis(host="localhost", port=6379, socket_connect_timeout=2).ping()


def check_kafka():
    from kafka import KafkaConsumer
    c = KafkaConsumer(bootstrap_servers="localhost:9092", request_timeout_ms=3000)
    c.topics()
    c.close()


def start(name, cmd, cwd=None):
    proc = subprocess.Popen(
        cmd, cwd=str(cwd or PROJECT_ROOT),
        stdout=None, stderr=None,
        shell=True,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
    )
    processes.append((name, proc))
    logger.info(f"🚀 {name} started (PID: {proc.pid})")
    return proc


def stop_all():
    logger.info("\n🛑 Stopping all services...")
    for name, proc in reversed(processes):
        try:
            proc.terminate()
            proc.wait(timeout=5)
            logger.info(f"   ✅ {name} stopped")
        except Exception:
            proc.kill()


def main(args):
    logger.info("=" * 55)
    logger.info("  TRAFFIC PIPELINE  —  Node.js Consumer Mode")
    logger.info("  Producer → Kafka → Node.js → Redis → WS → UI")
    logger.info("=" * 55)

    # 1. Check prerequisites
    redis_ok = check_service("Redis", check_redis)
    kafka_ok = check_service("Kafka", check_kafka)

    if not redis_ok or not kafka_ok:
        logger.error("\n❌ Start missing services first:")
        if not redis_ok:
            logger.error("   redis-server")
        if not kafka_ok:
            logger.error("   scripts/start_kafka.bat  (set KAFKA_HOME first)")
        sys.exit(1)

    # 2. Create Kafka topic
    logger.info("\n📋 Creating Kafka topic...")
    subprocess.run(
        [sys.executable, "-m", "ingestion.kafka_setup"],
        cwd=str(PROJECT_ROOT),
    )
    time.sleep(1)

    # 3. Start Node.js Consumer (Kafka → Redis → WebSocket → REST API)
    logger.info("\n📋 Starting Node.js Consumer (port 8000)...")
    start("Node Consumer", "node index.js", cwd=PROJECT_ROOT / "node-consumer")
    time.sleep(3)

    # 4. Start Frontend (Vite)
    logger.info("\n📋 Starting Frontend (Vite)...")
    start("Frontend", "npm run dev", cwd=PROJECT_ROOT / "dashboard")
    time.sleep(2)

    # 5. Start Producer
    if args.file:
        logger.info(f"\n📋 Starting Producer → file: {args.file}")
        start(
            "Producer",
            f"{sys.executable} -m ingestion.producer --mode file --file \"{args.file}\"",
        )
    else:
        logger.info("\n📋 Starting Producer (simulate mode, 10 events/s)...")
        start(
            "Producer",
            f"{sys.executable} -m ingestion.producer --mode simulate --rate 10",
        )

    logger.info("\n" + "=" * 55)
    logger.info("✅ PIPELINE RUNNING")
    logger.info("   Dashboard : http://localhost:5173")
    logger.info("   API       : http://localhost:8000")
    logger.info("   WebSocket : ws://localhost:8000/ws")
    logger.info("   Ctrl+C to stop all")
    logger.info("=" * 55 + "\n")

    try:
        while True:
            time.sleep(2)
            for name, proc in processes:
                if proc.poll() is not None:
                    logger.warning(f"⚠️  {name} exited (code {proc.returncode})")
    except KeyboardInterrupt:
        pass
    finally:
        stop_all()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, default=None,
                        help="JSON file to ingest (e.g. data/traffic_data_0.json)")
    main(parser.parse_args())
