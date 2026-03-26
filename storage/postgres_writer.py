"""Background Postgres batch writer for history records."""

from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
from typing import Dict, List, Optional

import psycopg


logger = logging.getLogger("PostgresWriter")


class PostgresBatchWriter:
    def __init__(self, batch_size: int = 50, flush_interval_seconds: int = 3):
        self.batch_size = max(1, min(batch_size, 100))
        self.flush_interval_seconds = max(1, flush_interval_seconds)
        self.queue: queue.Queue = queue.Queue()
        self.running = False
        self.worker_thread: Optional[threading.Thread] = None
        self.last_error: str = ""
        self.dsn = os.getenv(
            "POSTGRES_DSN",
            "postgresql://traffic:traffic@localhost:5432/traffic_db",
        )

    def _connect(self):
        return psycopg.connect(self.dsn)

    def init_schema(self):
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS traffic_history (
                        id BIGSERIAL PRIMARY KEY,
                        location_key TEXT NOT NULL,
                        road_id TEXT,
                        district TEXT,
                        road_name TEXT,
                        event_time TIMESTAMPTZ,
                        speed_kmph DOUBLE PRECISION,
                        vehicle_count INTEGER,
                        status TEXT,
                        risk_score DOUBLE PRECISION,
                        accident_severity DOUBLE PRECISION,
                        weather_condition TEXT,
                        payload JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS processed_traffic (
                        id BIGSERIAL PRIMARY KEY,
                        location_key TEXT,
                        event_time TIMESTAMPTZ,
                        speed_kmph DOUBLE PRECISION,
                        vehicle_count INTEGER,
                        status TEXT,
                        risk_score DOUBLE PRECISION,
                        payload JSONB,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_history_location_time ON traffic_history(location_key, event_time)"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_history_event_time ON traffic_history(event_time DESC)"
                )
                conn.commit()

    def ping(self) -> bool:
        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            return True
        except Exception as exc:
            self.last_error = str(exc)
            return False

    def enqueue(self, record: Dict):
        self.queue.put(record)

    def enqueue_many(self, records: List[Dict]):
        for record in records:
            self.queue.put(record)

    def load_latest_location_states(self, limit: int = 5000) -> List[Dict]:
        """Load latest snapshot per location from persistent history for startup warm cache."""
        query = """
            SELECT location_key, road_id, district, road_name, event_time,
                   speed_kmph, vehicle_count, status, risk_score,
                   accident_severity, weather_condition
            FROM (
                SELECT DISTINCT ON (location_key)
                    location_key, road_id, district, road_name, event_time,
                    speed_kmph, vehicle_count, status, risk_score,
                    accident_severity, weather_condition, id
                FROM traffic_history
                WHERE location_key IS NOT NULL AND location_key <> ''
                ORDER BY location_key, event_time DESC, id DESC
            ) latest
            ORDER BY event_time DESC NULLS LAST
            LIMIT %s
        """

        rows: List[Dict] = []
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (max(1, int(limit)),))
                records = cur.fetchall()
                for rec in records:
                    rows.append(
                        {
                            "location_key": rec[0],
                            "road_id": rec[1] or rec[0],
                            "district": rec[2] or "",
                            "road_name": rec[3] or "",
                            "event_time": rec[4].isoformat() if rec[4] else "",
                            "avg_speed": float(rec[5] or 0),
                            "speed_kmph": float(rec[5] or 0),
                            "vehicle_count": int(rec[6] or 0),
                            "status": rec[7] or "normal",
                            "risk_score": float(rec[8] or 0),
                            "accident_severity": float(rec[9] or 0),
                            "weather_condition": rec[10] or "Unknown",
                        }
                    )
        return rows

    def start(self):
        self.init_schema()
        self.running = True
        self.worker_thread = threading.Thread(target=self._run_loop, daemon=True)
        self.worker_thread.start()

    def stop(self):
        self.running = False
        if self.worker_thread and self.worker_thread.is_alive():
            self.worker_thread.join(timeout=5)

    def _flush(self, records: List[Dict]):
        if not records:
            return

        rows = []
        for item in records:
            payload_json = json.dumps(item, ensure_ascii=True)
            rows.append(
                (
                    item.get("location_key", ""),
                    item.get("road_id", ""),
                    item.get("district", ""),
                    item.get("road_name", ""),
                    item.get("event_time"),
                    float(item.get("speed_kmph", 0) or 0),
                    int(item.get("vehicle_count", 0) or 0),
                    item.get("status", "normal"),
                    float(item.get("risk_score", 0) or 0),
                    float(item.get("accident_severity", 0) or 0),
                    item.get("weather_condition", ""),
                    payload_json,
                )
            )

        try:
            with self._connect() as conn:
                with conn.cursor() as cur:
                    cur.executemany(
                        """
                        INSERT INTO traffic_history (
                            location_key, road_id, district, road_name, event_time,
                            speed_kmph, vehicle_count, status, risk_score,
                            accident_severity, weather_condition, payload
                        ) VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s,
                            %s, %s, %s::jsonb
                        )
                        """,
                        rows,
                    )
                    conn.commit()
        except Exception as exc:
            self.last_error = str(exc)
            logger.error("Postgres flush failed: %s", exc)

    def _run_loop(self):
        batch: List[Dict] = []
        last_flush = time.time()

        while self.running:
            timeout = max(0.1, self.flush_interval_seconds - (time.time() - last_flush))
            try:
                item = self.queue.get(timeout=timeout)
                batch.append(item)
            except queue.Empty:
                pass

            now = time.time()
            should_flush = False
            if len(batch) >= self.batch_size:
                should_flush = True
            elif batch and now - last_flush >= self.flush_interval_seconds:
                should_flush = True

            if should_flush:
                self._flush(batch)
                batch = []
                last_flush = now

        # Final flush on shutdown.
        if batch:
            self._flush(batch)
