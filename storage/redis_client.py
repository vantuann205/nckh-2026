"""
Redis Client for Traffic Data
Key design:
  - road:{road_id}        → Hash (latest data), TTL 120s
  - road:{road_id}:window → Sorted Set (5min rolling), TTL 300s
  - traffic:summary       → Hash (global KPIs), TTL 30s
  - traffic:congested     → Set (congested road_ids), TTL 60s
"""

import json
import time
import logging
from typing import Dict, List, Optional, Any

import redis

from stream_processing.config import (
    REDIS_HOST, REDIS_PORT, REDIS_DB,
    ROAD_DATA_TTL, ROAD_WINDOW_TTL, SUMMARY_TTL, CONGESTED_TTL,
)

logger = logging.getLogger("RedisClient")


class TrafficRedisClient:
    """Redis client for traffic data access"""

    def __init__(self, host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB):
        self.client = redis.Redis(
            host=host, port=port, db=db,
            decode_responses=True,
            socket_connect_timeout=5,
            retry_on_timeout=True,
        )
        self._check_connection()

    def _check_connection(self):
        try:
            self.client.ping()
            logger.info(f"✅ Redis connected at {REDIS_HOST}:{REDIS_PORT}")
        except redis.ConnectionError as e:
            logger.error(f"❌ Redis connection failed: {e}")
            raise

    # === READ: Road Data ===

    def get_road_data(self, road_id: str) -> Optional[dict]:
        """Get latest data for a specific road"""
        data = self.client.hgetall(f"road:{road_id}")
        return data if data else None

    def get_all_roads(self) -> List[dict]:
        """Get latest data for all roads"""
        roads = []
        # Scan for all road:* keys (exclude :window suffix)
        for key in self.client.scan_iter(match="road:*", count=100):
            if ":window" in key:
                continue
            data = self.client.hgetall(key)
            if data:
                roads.append(data)
        return roads

    def get_road_window(self, road_id: str, minutes: int = 5) -> List[dict]:
        """Get rolling window data for a road (last N minutes)"""
        window_key = f"road:{road_id}:window"
        min_score = time.time() - (minutes * 60)
        entries = self.client.zrangebyscore(window_key, min_score, "+inf")
        return [json.loads(e) for e in entries]

    # === READ: Summary & Congestion ===

    def get_summary(self) -> dict:
        """Get global traffic summary"""
        data = self.client.hgetall("traffic:summary")
        return data if data else {
            "total_roads": "0",
            "avg_speed": "0",
            "total_vehicles": "0",
            "congested_roads": "0",
            "updated_at": "",
        }

    def get_congested_roads(self) -> List[str]:
        """Get list of currently congested road IDs"""
        return list(self.client.smembers("traffic:congested"))

    # === WRITE: Road Data ===

    def set_road_data(self, road_id: str, data: dict):
        """Set latest data for a road"""
        key = f"road:{road_id}"
        self.client.hset(key, mapping=data)
        self.client.expire(key, ROAD_DATA_TTL)

    def add_to_window(self, road_id: str, data: dict):
        """Add entry to rolling window"""
        window_key = f"road:{road_id}:window"
        score = time.time()
        self.client.zadd(window_key, {json.dumps(data): score})
        # Trim old entries
        self.client.zremrangebyscore(window_key, 0, score - ROAD_WINDOW_TTL)
        self.client.expire(window_key, ROAD_WINDOW_TTL)

    def set_summary(self, summary: dict):
        """Set global summary"""
        self.client.hset("traffic:summary", mapping=summary)
        self.client.expire("traffic:summary", SUMMARY_TTL)

    def set_congested(self, road_ids: List[str]):
        """Set congested roads"""
        pipe = self.client.pipeline()
        pipe.delete("traffic:congested")
        if road_ids:
            pipe.sadd("traffic:congested", *road_ids)
        pipe.expire("traffic:congested", CONGESTED_TTL)
        pipe.execute()

    # === UTILITY ===

    def flush_all(self):
        """Clear all traffic data from Redis (dev only)"""
        for key in self.client.scan_iter(match="road:*"):
            self.client.delete(key)
        self.client.delete("traffic:summary")
        self.client.delete("traffic:congested")
        logger.info("🗑️ All traffic data flushed from Redis")

    def get_stats(self) -> dict:
        """Get Redis stats"""
        info = self.client.info("memory")
        road_count = sum(1 for _ in self.client.scan_iter(match="road:*", count=100)
                         if ":window" not in _)
        return {
            "connected": True,
            "used_memory": info.get("used_memory_human", "N/A"),
            "road_count": road_count,
            "congested_count": self.client.scard("traffic:congested"),
        }

    def close(self):
        self.client.close()
