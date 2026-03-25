"""
Start All Services — Local development
Starts: Redis → Kafka (manual) → Consumer → Backend → Frontend
No Docker — everything runs locally.
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
logger = logging.getLogger("StartAll")

PROJECT_ROOT = Path(__file__).parent.parent
processes = []


def check_redis():
    """Check if Redis is running"""
    try:
        import redis
        r = redis.Redis(host="localhost", port=6379, socket_connect_timeout=2)
        r.ping()
        logger.info("✅ Redis is running")
        return True
    except Exception:
        logger.warning("⚠️ Redis is NOT running")
        logger.info("   → Start Redis: redis-server")
        return False


def check_kafka():
    """Check if Kafka is running"""
    try:
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(bootstrap_servers="localhost:9092", request_timeout_ms=3000)
        consumer.topics()
        consumer.close()
        logger.info("✅ Kafka is running")
        return True
    except Exception:
        logger.warning("⚠️ Kafka is NOT running")
        logger.info("   → Start Zookeeper: bin\\windows\\zookeeper-server-start.bat config\\zookeeper.properties")
        logger.info("   → Start Kafka:     bin\\windows\\kafka-server-start.bat config\\server.properties")
        return False


def start_process(name, cmd, cwd=None):
    """Start a process"""
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=cwd or str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            shell=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0,
        )
        processes.append((name, proc))
        logger.info(f"🚀 Started {name} (PID: {proc.pid})")
        return proc
    except Exception as e:
        logger.error(f"❌ Failed to start {name}: {e}")
        return None


def stop_all():
    """Stop all started processes"""
    logger.info("\n🛑 Stopping all services...")
    for name, proc in reversed(processes):
        try:
            if sys.platform == "win32":
                proc.terminate()
            else:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait(timeout=5)
            logger.info(f"   ✅ {name} stopped")
        except Exception:
            proc.kill()
            logger.info(f"   ⚠️ {name} force killed")
    processes.clear()


def main():
    logger.info("=" * 60)
    logger.info("🚀 REALTIME TRAFFIC PIPELINE — LOCAL STARTUP")
    logger.info("=" * 60)

    # Step 1: Check prerequisites
    logger.info("\n📋 Checking prerequisites...")

    redis_ok = check_redis()
    kafka_ok = check_kafka()

    if not redis_ok or not kafka_ok:
        logger.error("\n❌ Prerequisites not met. Please start the missing services first:")
        if not redis_ok:
            logger.error("   • Redis: redis-server")
        if not kafka_ok:
            logger.error("   • Kafka: see scripts/start_kafka.bat")
        logger.info("\nThen re-run: python scripts/start_all.py")
        sys.exit(1)

    # Step 2: Setup Kafka topic
    logger.info("\n📋 Setting up Kafka topic...")
    subprocess.run(
        [sys.executable, "-m", "ingestion.kafka_setup"],
        cwd=str(PROJECT_ROOT),
    )

    # Step 3: Start Stream Processor (Consumer)
    logger.info("\n📋 Starting Stream Processor...")
    start_process(
        "Stream Consumer",
        f"{sys.executable} -m stream_processing.consumer",
    )
    time.sleep(2)

    # Step 4: Start Backend API
    logger.info("\n📋 Starting Backend API...")
    start_process(
        "Backend API",
        f"{sys.executable} -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload",
    )
    time.sleep(2)

    # Step 5: Start Frontend (Vite dev server)
    logger.info("\n📋 Starting Frontend...")
    dashboard_path = PROJECT_ROOT / "dashboard"
    start_process(
        "Frontend (Vite)",
        "npm run dev",
        cwd=str(dashboard_path),
    )
    time.sleep(2)

    # Step 6: Start Producer
    if getattr(args, "file", None):
        logger.info(f"\n📋 Starting Producer (file mode: {args.file})...")
        start_process(
            "Kafka Producer",
            f"{sys.executable} -m ingestion.producer --mode file --file \"{args.file}\"",
        )
    else:
        logger.info("\n📋 Starting Producer (simulate mode)...")
        start_process(
            "Kafka Producer",
            f"{sys.executable} -m ingestion.producer --mode simulate --rate 10",
        )

    # Summary
    logger.info("\n" + "=" * 60)
    logger.info("✅ ALL SERVICES STARTED")
    logger.info("=" * 60)
    logger.info("   📊 Dashboard:  http://localhost:5173")
    logger.info("   🔌 API:        http://localhost:8000")
    logger.info("   🔌 WebSocket:  ws://localhost:8000/ws")
    logger.info("   📡 API Docs:   http://localhost:8000/docs")
    logger.info("")
    logger.info("   Press Ctrl+C to stop all services")
    logger.info("=" * 60)

    # Wait for Ctrl+C
    try:
        while True:
            time.sleep(1)
            # Check if any process died
            for name, proc in processes:
                if proc.poll() is not None:
                    logger.warning(f"⚠️ {name} exited with code {proc.returncode}")
    except KeyboardInterrupt:
        pass
    finally:
        stop_all()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start All Services")
    parser.add_argument("--file", type=str, help="Path to JSON file for ingestion")
    args = parser.parse_args()
    
    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        stop_all()
