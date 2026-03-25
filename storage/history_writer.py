"""
History Writer (Optional)
Write long-term traffic history to SQLite for analysis.
Redis only keeps 5min rolling window — this handles the rest.
"""

import json
import sqlite3
import time
import logging
import threading
from pathlib import Path
from datetime import datetime

import redis

from stream_processing.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_CHANNEL

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("HistoryWriter")

DB_PATH = Path(__file__).parent.parent / "traffic_history.db"


class HistoryWriter:
    """Subscribe to Redis Pub/Sub and write history to SQLite"""

    def __init__(self, db_path=DB_PATH):
        self.db_path = str(db_path)
        self.running = False
        self._init_db()

    def _init_db(self):
        """Create SQLite table"""
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS traffic_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                road_id TEXT NOT NULL,
                avg_speed REAL,
                vehicle_count INTEGER,
                status TEXT,
                lat REAL,
                lng REAL,
                recorded_at TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_road_time 
            ON traffic_history(road_id, recorded_at)
        """)
        conn.commit()
        conn.close()
        logger.info(f"✅ History DB initialized at {self.db_path}")

    def _write_records(self, roads_data: list):
        """Write road data to SQLite"""
        conn = sqlite3.connect(self.db_path)
        try:
            for road_id in roads_data:
                # Fetch latest data from Redis
                r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
                data = r.hgetall(f"road:{road_id}")
                r.close()

                if data:
                    conn.execute(
                        """INSERT INTO traffic_history 
                           (road_id, avg_speed, vehicle_count, status, lat, lng, recorded_at) 
                           VALUES (?, ?, ?, ?, ?, ?, ?)""",
                        (
                            road_id,
                            float(data.get("avg_speed", 0)),
                            int(data.get("vehicle_count", 0)),
                            data.get("status", "unknown"),
                            float(data.get("lat", 0)),
                            float(data.get("lng", 0)),
                            data.get("updated_at", datetime.utcnow().isoformat()),
                        ),
                    )
            conn.commit()
        except Exception as e:
            logger.error(f"❌ Write error: {e}")
        finally:
            conn.close()

    def start(self):
        """Start listening to Redis Pub/Sub for history writes"""
        logger.info("🚀 History writer started")
        self.running = True

        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        pubsub = r.pubsub()
        pubsub.subscribe(REDIS_CHANNEL)

        try:
            for message in pubsub.listen():
                if not self.running:
                    break
                if message["type"] == "message":
                    try:
                        data = json.loads(message["data"])
                        roads = data.get("roads", [])
                        if roads:
                            self._write_records(roads)
                    except Exception as e:
                        logger.error(f"❌ Parse error: {e}")
        except KeyboardInterrupt:
            pass
        finally:
            pubsub.unsubscribe()
            r.close()
            logger.info("🛑 History writer stopped")

    def stop(self):
        self.running = False


if __name__ == "__main__":
    writer = HistoryWriter()
    writer.start()
