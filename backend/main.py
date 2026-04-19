"""FastAPI backend for realtime ingest, cache, history, analytics, and prediction."""

import json
import asyncio
import logging
import math
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import orjson
import pandas as pd
import redis
from fastapi import BackgroundTasks, FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from storage.redis_client import TrafficRedisClient
from storage.postgres_writer import PostgresBatchWriter
from backend.ws_manager import ConnectionManager
from processing.model_service import load_model, predict_probability, train_model_once
from processing.offline_pipeline import ensure_processed_dataset
from stream_processing.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_CHANNEL
from stream_processing.async_loader import PROGRESS, load_all_data, load_single_file, get_data_files

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("TrafficAPI")

# === App Setup ===
app = FastAPI(
    title="Realtime Traffic API",
    description="Realtime traffic monitoring API with WebSocket support",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Globals
redis_client: Optional[TrafficRedisClient] = None
history_writer: Optional[PostgresBatchWriter] = None
processed_df: Optional[pd.DataFrame] = None
model_bundle: Optional[Dict] = None
weather_payload: Dict = {}
accidents_payload: List[Dict] = []
ws_manager = ConnectionManager()
_pubsub_thread = None
_main_loop = None
_file_watcher_observer = None

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"


def _load_static_payloads():
    global weather_payload, accidents_payload
    try:
        weather_path = PROJECT_ROOT / "data" / "retrievebulkdataset.json"
        with weather_path.open("r", encoding="utf-8") as f:
            weather_payload = json.load(f)
        logger.info("✅ Weather dataset loaded")
    except Exception as exc:
        weather_payload = {}
        logger.warning("⚠️ Weather dataset unavailable: %s", exc)

    try:
        accidents_path = PROJECT_ROOT / "data" / "traffic_accidents.json"
        with accidents_path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
        accidents_payload = payload if isinstance(payload, list) else []
        logger.info("✅ Accident dataset loaded (%d rows)", len(accidents_payload))
    except Exception as exc:
        accidents_payload = []
        logger.warning("⚠️ Accident dataset unavailable: %s", exc)


class RealtimeEvent(BaseModel):
    event_time: str = Field(default="")
    location_key: str = Field(default="")
    road_id: str = Field(default="")
    road_name: str = Field(default="")
    district: str = Field(default="")
    lat: float = Field(default=0)
    lng: float = Field(default=0)
    speed_kmph: float = Field(default=0)
    vehicle_count: int = Field(default=0)
    weather_temp_c: float = Field(default=0)
    humidity_pct: float = Field(default=0)
    weather_condition: str = Field(default="Unknown")
    accident_severity: float = Field(default=0)
    congestion_km: float = Field(default=0)


def _compute_status(speed_kmph: float) -> str:
    if speed_kmph < 20:
        return "congested"
    if speed_kmph < 40:
        return "slow"
    return "normal"


def _compute_risk(event: RealtimeEvent) -> float:
    speed_risk = max(0.0, 50.0 - float(event.speed_kmph))
    weather_risk = 0.0
    weather_text = (event.weather_condition or "").lower()
    if "rain" in weather_text or "storm" in weather_text:
        weather_risk += 20
    if float(event.humidity_pct or 0) > 85:
        weather_risk += 10
    accident_risk = float(event.accident_severity or 0) * 12 + float(event.congestion_km or 0) * 3
    score = speed_risk * 0.6 + weather_risk * 0.2 + accident_risk * 0.2
    return float(min(100.0, round(score, 2)))


def _compute_decision(status: str, accident_severity: float) -> str:
    if status == "congested":
        return "increase_green_light"
    if accident_severity >= 2:
        return "reroute"
    return "monitor"


def _safe_float(value, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad, lon1_rad = math.radians(lat1), math.radians(lon1)
    lat2_rad, lon2_rad = math.radians(lat2), math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return 6371.0 * c


def _road_key(road: dict) -> str:
    return str(road.get("road_id") or road.get("location_key") or "").strip().lower()


def _find_road(roads: List[dict], road_query: str) -> Optional[dict]:
    needle = str(road_query or "").strip().lower()
    if not needle:
        return None

    for road in roads:
        if _road_key(road) == needle:
            return road

    for road in roads:
        road_name = str(road.get("road_name") or "").strip().lower()
        if road_name == needle:
            return road
    return None


def _build_route_suggestions(roads: List[dict], from_road: dict, to_road: dict) -> Tuple[List[dict], float]:
    from_id = _road_key(from_road)
    to_id = _road_key(to_road)

    from_lat = _safe_float(from_road.get("lat"))
    from_lng = _safe_float(from_road.get("lng"))
    to_lat = _safe_float(to_road.get("lat"))
    to_lng = _safe_float(to_road.get("lng"))

    direct_distance_km = max(0.2, _haversine_km(from_lat, from_lng, to_lat, to_lng))
    midpoint_lat = (from_lat + to_lat) / 2.0
    midpoint_lng = (from_lng + to_lng) / 2.0

    candidate_mid = []
    for road in roads:
        key = _road_key(road)
        if not key or key in {from_id, to_id}:
            continue

        lat = _safe_float(road.get("lat"))
        lng = _safe_float(road.get("lng"))
        dist_to_mid = _haversine_km(midpoint_lat, midpoint_lng, lat, lng)
        same_region = (
            str(road.get("district") or "") == str(from_road.get("district") or "")
            or str(road.get("district") or "") == str(to_road.get("district") or "")
        )
        if dist_to_mid <= 8.0 or same_region:
            candidate_mid.append(road)

    if not candidate_mid:
        candidate_mid = [r for r in roads if _road_key(r) not in {from_id, to_id}]

    def _fast_score(r: dict) -> float:
        speed = _safe_float(r.get("avg_speed"))
        delay = _safe_float(r.get("estimated_delay") or r.get("estimated_delay_minutes"))
        risk = _safe_float(r.get("risk_score"))
        return speed * 1.3 - delay * 1.1 - risk * 0.7

    def _safe_score(r: dict) -> float:
        speed = _safe_float(r.get("avg_speed"))
        delay = _safe_float(r.get("estimated_delay") or r.get("estimated_delay_minutes"))
        risk = _safe_float(r.get("risk_score"))
        return speed * 0.8 - delay * 0.9 - risk * 1.6

    fast_mid = max(candidate_mid, key=_fast_score) if candidate_mid else None
    safe_mid = max(candidate_mid, key=_safe_score) if candidate_mid else None

    route_defs: List[Tuple[str, List[dict]]] = [("direct", [from_road, to_road])]
    if fast_mid:
        route_defs.append(("fast", [from_road, fast_mid, to_road]))
    if safe_mid and _road_key(safe_mid) != _road_key(fast_mid or {}):
        route_defs.append(("safe", [from_road, safe_mid, to_road]))

    raw_routes: List[dict] = []
    seen_paths = set()

    for route_type, route_roads in route_defs:
        route_keys = [k for k in (_road_key(r) for r in route_roads) if k]
        if len(route_keys) < 2:
            continue
        path_key = "|".join(route_keys)
        if path_key in seen_paths:
            continue
        seen_paths.add(path_key)

        distance_km = 0.0
        for idx in range(len(route_roads) - 1):
            a = route_roads[idx]
            b = route_roads[idx + 1]
            distance_km += _haversine_km(
                _safe_float(a.get("lat")),
                _safe_float(a.get("lng")),
                _safe_float(b.get("lat")),
                _safe_float(b.get("lng")),
            )
        if distance_km <= 0:
            distance_km = direct_distance_km * (1.0 + 0.12 * max(0, len(route_roads) - 2))

        speeds = [_safe_float(r.get("avg_speed"), 20.0) for r in route_roads]
        delays = [
            _safe_float(r.get("estimated_delay") or r.get("estimated_delay_minutes"), 0.0)
            for r in route_roads
        ]
        risks = [_safe_float(r.get("risk_score"), 0.0) for r in route_roads]

        avg_speed = max(8.0, sum(speeds) / len(speeds))
        delay_minutes = max(0.0, sum(delays) * 0.75)
        travel_minutes = max(1.0, distance_km / avg_speed * 60.0)
        total_minutes = travel_minutes + delay_minutes
        avg_risk_score = sum(risks) / len(risks)

        raw_score = avg_speed * 1.2 - delay_minutes * 1.5 - avg_risk_score * 0.8
        avoid_roads = sorted(
            (
                r for r in candidate_mid
                if _road_key(r) not in route_keys
            ),
            key=lambda r: _safe_float(r.get("risk_score")),
            reverse=True,
        )[:3]

        if avg_speed < 18 or avg_risk_score > 70:
            recommendation = "Nên tránh tuyến này vào giờ cao điểm"
        elif delay_minutes > 18:
            recommendation = "Có thể đi nhưng nên chuẩn bị tuyến dự phòng"
        else:
            recommendation = "Khuyến nghị ưu tiên tuyến này"

        raw_routes.append(
            {
                "route_type": route_type,
                "route_roads": [str(r.get("road_name") or r.get("road_id") or "") for r in route_roads],
                "route_road_ids": route_keys,
                "distance_km": round(distance_km, 2),
                "avg_speed_kmh": round(avg_speed, 2),
                "delay_minutes": round(delay_minutes, 2),
                "travel_minutes": round(travel_minutes, 2),
                "total_minutes": round(total_minutes, 2),
                "avg_risk_score": round(avg_risk_score, 2),
                "raw_score": raw_score,
                "recommendation": recommendation,
                "avoid_roads": [str(r.get("road_name") or r.get("road_id") or "") for r in avoid_roads],
            }
        )

    if not raw_routes:
        return [], round(direct_distance_km, 2)

    min_score = min(r["raw_score"] for r in raw_routes)
    max_score = max(r["raw_score"] for r in raw_routes)
    score_span = max(1e-6, max_score - min_score)
    for route in raw_routes:
        normalized = (route["raw_score"] - min_score) / score_span
        route["route_score"] = round(100.0 * normalized, 1)
        route.pop("raw_score", None)

    raw_routes.sort(key=lambda r: r["route_score"], reverse=True)
    for idx, route in enumerate(raw_routes, start=1):
        route["rank"] = idx
        route["route_id"] = f"R{idx}"

    return raw_routes, round(direct_distance_km, 2)


def _read_float_hash(redis_conn, key: str) -> Dict[str, float]:
    raw = redis_conn.hgetall(key)
    if not raw:
        return {}
    return {k: _safe_float(v) for k, v in raw.items()}


# === Lifecycle ===

@app.on_event("startup")
async def startup():
    global redis_client, history_writer, processed_df, model_bundle, _pubsub_thread, _main_loop
    logger.info("🚀 Starting Realtime Traffic API...")
    
    # Capture the main event loop for cross-thread broadcasts
    _main_loop = asyncio.get_running_loop()
    _load_static_payloads()

    # Load preprocessed dataset, building once if it does not exist.
    try:
        processed_df = ensure_processed_dataset(force_rebuild=False)
        logger.info("✅ Processed dataset ready: %d rows", len(processed_df))
    except Exception as e:
        logger.error(f"❌ Processed dataset init failed: {e}")
        processed_df = pd.DataFrame()

    # Train once if model does not exist, then keep in-memory model for serving.
    try:
        if processed_df is not None and not processed_df.empty:
            train_model_once(processed_df)
            model_bundle = load_model()
        else:
            model_bundle = load_model()
    except Exception as e:
        logger.error(f"❌ Model setup failed: {e}")
        model_bundle = None

    # Connect Redis
    try:
        redis_client = TrafficRedisClient()
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        logger.warning("⚠️ API will start without Redis — endpoints may return empty data")

    # Start background Postgres writer
    try:
        history_writer = PostgresBatchWriter(batch_size=50, flush_interval_seconds=3)
        history_writer.start()
        logger.info("✅ Background Postgres writer started")
    except Exception as e:
        logger.error(f"❌ Postgres writer failed: {e}")
        history_writer = None

    # Start Redis Pub/Sub listener in background thread
    _pubsub_thread = threading.Thread(target=_redis_listener, args=(_main_loop,), daemon=True)
    _pubsub_thread.start()

    # Start fast data loader in background (non-blocking)
    # Throttle: broadcast at most once per second during loading
    _last_broadcast_ts = [0.0]

    def _do_broadcast():
        if not (_main_loop and _main_loop.is_running()):
            return
        now = time.time()
        if now - _last_broadcast_ts[0] < 1.0:
            return
        _last_broadcast_ts[0] = now
        try:
            progress = PROGRESS.to_dict()
            # Update only road-level fields; keep accumulated counters from loader.
            if redis_client:
                roads = redis_client.get_all_roads()
                if roads:
                    total = len(roads)
                    avg_spd = round(sum(float(r.get("avg_speed") or 0) for r in roads) / total, 2)
                    cong = sum(1 for r in roads if r.get("status") == "congested")
                    veh_snapshot = sum(int(r.get("vehicle_count") or 0) for r in roads)
                    redis_client.client.hset("traffic:summary", mapping={
                        "total_roads": total,
                        "avg_speed": avg_spd,
                        "congested_roads": cong,
                        "current_vehicles_snapshot": veh_snapshot,
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                    })
                    summary = redis_client.get_summary()
                else:
                    summary = redis_client.get_summary()
            else:
                summary = {}
            msg = {
                "type": "traffic_update",
                "roads": [],
                "summary": summary,
                "loading": progress,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            asyncio.run_coroutine_threadsafe(ws_manager.broadcast(msg), _main_loop)
        except Exception:
            pass

    def _do_broadcast_final():
        """Full broadcast after loading completes — send actual roads."""
        if not (_main_loop and _main_loop.is_running() and redis_client):
            return
        try:
            roads = redis_client.get_all_roads()
            summary = redis_client.get_summary()
            msg = {
                "type": "traffic_update",
                "roads": roads,
                "summary": summary,
                "loading": PROGRESS.to_dict(),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
            asyncio.run_coroutine_threadsafe(ws_manager.broadcast(msg), _main_loop)
        except Exception:
            pass

    def _run_loader():
        load_all_data(redis_client, _do_broadcast)
        # After all done, send full data once
        _do_broadcast_final()

    if redis_client:
        loader_thread = threading.Thread(target=_run_loader, daemon=True)
        loader_thread.start()
        logger.info("✅ Background data loader started")

    # Start file watcher for data/ folder
    def _watcher_broadcast():
        _do_broadcast_final()

    _start_file_watcher(_watcher_broadcast)

    logger.info("✅ API ready!")


@app.on_event("shutdown")
async def shutdown():
    global history_writer, _file_watcher_observer
    if history_writer:
        history_writer.stop()
    if redis_client:
        redis_client.close()
    if _file_watcher_observer:
        _file_watcher_observer.stop()
        _file_watcher_observer.join()
    logger.info("🛑 API shutdown")


# === File Watcher ===

def _start_file_watcher(broadcast_fn):
    """Watch data/ folder for new/changed traffic_data_*.json files."""
    global _file_watcher_observer
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler
        import time as _time

        class _Handler(FileSystemEventHandler):
            def __init__(self):
                self._debounce: Dict[str, float] = {}
                self._lock = threading.Lock()

            def _should_handle(self, path: str) -> bool:
                name = Path(path).name
                if not name.startswith("traffic_data_") or not name.endswith(".json"):
                    return False
                if name.endswith("traffic_data_demo.json"):
                    return False
                return True

            def _debounced_load(self, path: str):
                # Debounce: wait 1s after last event before loading
                with self._lock:
                    self._debounce[path] = _time.time()

                def _run():
                    _time.sleep(1.0)
                    with self._lock:
                        last = self._debounce.get(path, 0)
                    if _time.time() - last < 0.9:
                        return  # another event came in, skip
                    if redis_client:
                        logger.info("File watcher triggered: %s", Path(path).name)
                        load_single_file(Path(path), redis_client, broadcast_fn)

                threading.Thread(target=_run, daemon=True).start()

            def on_created(self, event):
                if not event.is_directory and self._should_handle(event.src_path):
                    self._debounced_load(event.src_path)

            def on_modified(self, event):
                if not event.is_directory and self._should_handle(event.src_path):
                    self._debounced_load(event.src_path)

        observer = Observer()
        observer.schedule(_Handler(), str(DATA_DIR), recursive=False)
        observer.start()
        _file_watcher_observer = observer
        logger.info("✅ File watcher started on %s", DATA_DIR)
    except ImportError:
        logger.warning("⚠️ watchdog not installed — file watcher disabled. Run: pip install watchdog")
    except Exception as e:
        logger.error("❌ File watcher failed: %s", e)


# === Redis Pub/Sub → WebSocket Bridge ===

def _redis_listener(main_loop: asyncio.AbstractEventLoop):
    """Background thread: listen to Redis Pub/Sub and trigger WS broadcasts"""
    try:
        r = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
            decode_responses=True,
        )
        pubsub = r.pubsub()
        pubsub.subscribe(REDIS_CHANNEL)

        logger.info(f"📡 Listening on Redis channel: {REDIS_CHANNEL}")

        for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    roads = data.get("roads", [])
                    congested = data.get("congested", [])

                    # Fetch updated data from Redis
                    if redis_client:
                        all_roads_data = redis_client.get_all_roads()
                        summary = redis_client.get_summary()

                        broadcast_msg = {
                            "type": "traffic_update",
                            "roads": all_roads_data,
                            "congested": congested,
                            "summary": summary,
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                        }

                        # Schedule broadcast on the MAIN event loop from this thread
                        if main_loop and main_loop.is_running():
                            asyncio.run_coroutine_threadsafe(
                                ws_manager.broadcast(broadcast_msg, road_ids=roads),
                                main_loop
                            )

                except Exception as e:
                    logger.error(f"❌ Pub/Sub handler error: {e}")

    except Exception as e:
        logger.error(f"❌ Redis listener error: {e}")


# === REST API Endpoints ===

@app.get("/")
async def root():
    return {
        "service": "Realtime Traffic API",
        "version": "3.0.0",
        "endpoints": [
            "/traffic/ingest",
            "/traffic/realtime",
            "/traffic/analysis",
            "/traffic/predict",
            "/traffic/advanced-analytics",
            "/traffic/route-suggestions",
            "/traffic/route-estimate",
            "/traffic/{road_id}",
            "/traffic/summary",
            "/traffic/congested",
            "/ws",
        ],
        "status": "healthy",
    }


@app.get("/health")
async def health():
    redis_ok = False
    postgres_ok = False
    redis_stats = {}
    if redis_client:
        try:
            redis_stats = redis_client.get_stats()
            redis_ok = True
        except Exception:
            pass

    if history_writer:
        postgres_ok = history_writer.ping()

    return {
        "status": "healthy" if (redis_ok and postgres_ok) else "degraded",
        "redis": {"connected": redis_ok, **redis_stats},
        "postgres": {
            "connected": postgres_ok,
            "last_error": history_writer.last_error if history_writer else "",
        },
        "processed_rows": int(len(processed_df)) if processed_df is not None else 0,
        "model_loaded": model_bundle is not None,
        "websocket_clients": ws_manager.connection_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.post("/traffic/ingest")
async def ingest_event(event: RealtimeEvent, background_tasks: BackgroundTasks):
    """Ingest realtime event with cache-first sync write and queued DB write."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    try:
        event_time = pd.to_datetime(event.event_time, utc=True, errors="coerce")
    except Exception:
        event_time = pd.Timestamp.utcnow()
    if pd.isna(event_time):
        event_time = pd.Timestamp.utcnow()

    status = _compute_status(float(event.speed_kmph))
    risk_score = _compute_risk(event)
    decision = _compute_decision(status, float(event.accident_severity))

    location_key = event.location_key or event.road_id or f"{event.district}:{event.road_name}"
    road_id = event.road_id or location_key

    payload = {
        "road_id": road_id,
        "location_key": location_key,
        "road_name": event.road_name,
        "district": event.district,
        "lat": event.lat,
        "lng": event.lng,
        "avg_speed": round(float(event.speed_kmph), 2),
        "vehicle_count": int(event.vehicle_count),
        "status": status,
        "risk_score": risk_score,
        "accident_severity": float(event.accident_severity),
        "weather_condition": event.weather_condition,
        "event_time": event_time.isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "decision": decision,
        "alerts": [
            "congestion_warning" if status == "congested" else "",
            "speeding" if float(event.speed_kmph) > 60 else "",
        ],
    }

    # Sync path: write realtime cache first.
    redis_client.set_location_state(location_key, payload)
    redis_client.add_to_window(location_key, payload)
    redis_client.client.hincrbyfloat("traffic:summary", "total_vehicles", 1)
    redis_client.client.hincrbyfloat("traffic:summary", "total_records_loaded", 1)

    roads = redis_client.get_all_roads()
    congested = [r.get("road_id") for r in roads if r.get("status") == "congested"]
    summary = {
        "total_roads": len(roads),
        "avg_speed": round(
            sum(float(r.get("avg_speed", 0) or 0) for r in roads) / len(roads), 2
        ) if roads else 0,
        "current_vehicles_snapshot": int(sum(int(r.get("vehicle_count", 0) or 0) for r in roads)),
        "congested_roads": len(congested),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    redis_client.client.hset("traffic:summary", mapping=summary)
    redis_client.set_congested(congested)
    redis_client.publish_update(
        {
            "type": "traffic_update",
            "roads": [location_key],
            "congested": congested,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        },
        channel=REDIS_CHANNEL,
    )

    # Async path: enqueue history write in background.
    if history_writer:
        background_tasks.add_task(history_writer.enqueue, payload)

    return {
        "accepted": True,
        "location_key": location_key,
        "status": status,
        "risk_score": risk_score,
        "decision": decision,
    }


@app.get("/traffic/stats")
async def get_stats():
    """Full statistics from all loaded vehicle data."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")
    rc = redis_client.client
    summary = redis_client.get_summary()

    # Read breakdown counters from Redis hashes (accumulated during load)
    def _hgetall_int(key: str) -> dict:
        raw = rc.hgetall(key)
        return {k: int(float(v)) for k, v in raw.items()} if raw else {}

    vehicle_types     = _hgetall_int("traffic:stats:vehicle_types")
    congestion_levels = _hgetall_int("traffic:stats:congestion_levels")
    weather           = _hgetall_int("traffic:stats:weather")

    return {
        "summary":           summary,
        "vehicle_types":     vehicle_types,
        "congestion_levels": congestion_levels,
        "weather":           weather,
        "timestamp":         datetime.now(timezone.utc).isoformat(),
    }


@app.get("/traffic/loading-progress")
async def get_loading_progress():
    """Get current data loading progress."""
    return PROGRESS.to_dict()


@app.post("/traffic/ingest/batch")
async def ingest_batch(events: List[RealtimeEvent], background_tasks: BackgroundTasks):
    """High-speed batch ingest: write many events to Redis in one pipeline."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    rows = []
    for event in events:
        try:
            event_time = pd.to_datetime(event.event_time, utc=True, errors="coerce")
        except Exception:
            event_time = pd.Timestamp.utcnow()
        if pd.isna(event_time):
            event_time = pd.Timestamp.utcnow()

        status = _compute_status(float(event.speed_kmph))
        risk_score = _compute_risk(event)
        decision = _compute_decision(status, float(event.accident_severity))
        location_key = event.location_key or event.road_id or f"{event.district}:{event.road_name}"
        road_id = event.road_id or location_key

        payload = {
            "road_id": road_id,
            "location_key": location_key,
            "road_name": event.road_name,
            "district": event.district,
            "lat": event.lat,
            "lng": event.lng,
            "avg_speed": round(float(event.speed_kmph), 2),
            "vehicle_count": int(event.vehicle_count),
            "status": status,
            "risk_score": risk_score,
            "accident_severity": float(event.accident_severity),
            "weather_condition": event.weather_condition,
            "event_time": event_time.isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "decision": decision,
        }
        rows.append(payload)
        if history_writer:
            background_tasks.add_task(history_writer.enqueue, payload)

    # Batch write to Redis
    redis_client.set_location_state_batch(rows)
    redis_client.client.hincrbyfloat("traffic:summary", "total_vehicles", len(rows))
    redis_client.client.hincrbyfloat("traffic:summary", "total_records_loaded", len(rows))

    # Update summary
    all_roads = redis_client.get_all_roads()
    congested = [r.get("road_id") for r in all_roads if r.get("status") == "congested"]
    summary = {
        "total_roads": len(all_roads),
        "avg_speed": round(sum(float(r.get("avg_speed", 0) or 0) for r in all_roads) / len(all_roads), 2) if all_roads else 0,
        "current_vehicles_snapshot": int(sum(int(r.get("vehicle_count", 0) or 0) for r in all_roads)),
        "congested_roads": len(congested),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    redis_client.client.hset("traffic:summary", mapping=summary)
    redis_client.set_congested(congested)

    # Broadcast via WebSocket
    if _main_loop and _main_loop.is_running():
        broadcast_msg = {
            "type": "traffic_update",
            "roads": all_roads,
            "summary": summary,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        asyncio.run_coroutine_threadsafe(ws_manager.broadcast(broadcast_msg), _main_loop)

    return {"accepted": True, "count": len(rows)}


@app.get("/traffic/roads/latest")
async def get_roads_latest():
    """Get latest state for all unique roads (for charts)."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")
    roads = redis_client.get_all_roads()
    return {"roads": roads, "count": len(roads)}


@app.get("/traffic/realtime")
async def get_realtime():
    """Get real-time traffic data for all roads"""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    roads = redis_client.get_all_roads()
    summary = redis_client.get_summary()

    return {
        "roads": roads,
        "summary": summary,
        "count": len(roads),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/traffic/analysis")
async def get_analysis():
    """Serve precomputed analytics without recomputing heavy pipeline in request path."""
    if processed_df is None or processed_df.empty:
        return {
            "traffic_by_hour": [],
            "peak_hour": None,
            "hotspots": [],
            "accident_vs_delay": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    df = processed_df.copy()
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["event_time"])
    df["hour"] = df["event_time"].dt.hour

    hourly = (
        df.groupby("hour", as_index=False)
        .agg(
            avg_speed=("speed_kmph", "mean"),
            avg_risk=("risk_score", "mean"),
            events=("event_time", "count"),
        )
        .sort_values("hour")
    )
    peak_hour = int(hourly.sort_values("events", ascending=False).iloc[0]["hour"]) if not hourly.empty else None

    hotspots_df = (
        df[df["status"] == "congested"]
        .groupby("location_key", as_index=False)
        .agg(congested_events=("status", "count"), avg_risk=("risk_score", "mean"))
        .sort_values("congested_events", ascending=False)
        .head(10)
    )

    accident_delay_df = (
        df.groupby("accident_severity", as_index=False)
        .agg(avg_delay=("estimated_delay_minutes", "mean"))
        .sort_values("accident_severity")
    )

    return {
        "traffic_by_hour": [
            {
                "hour": int(row.hour),
                "avg_speed": round(float(row.avg_speed), 2),
                "avg_risk": round(float(row.avg_risk), 2),
                "events": int(row.events),
            }
            for row in hourly.itertuples()
        ],
        "peak_hour": peak_hour,
        "hotspots": [
            {
                "location_key": row.location_key,
                "congested_events": int(row.congested_events),
                "avg_risk": round(float(row.avg_risk), 2),
            }
            for row in hotspots_df.itertuples()
        ],
        "accident_vs_delay": [
            {
                "accident_severity": float(row.accident_severity),
                "avg_delay": round(float(row.avg_delay), 2),
            }
            for row in accident_delay_df.itertuples()
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/traffic/predict")
async def get_predict(minutes: int = Query(default=10, ge=10, le=60)):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    if minutes not in {10, 30, 60}:
        raise HTTPException(status_code=422, detail="minutes must be one of: 10, 30, 60")

    roads = redis_client.get_all_roads()
    if not roads:
        return {
            "predictions": [],
            "minutes": minutes,
            "supported_horizons": [10, 30, 60],
            "top_alerts": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    feature_rows: List[dict] = []
    road_metrics: List[dict] = []
    for road in roads:
        speed = _safe_float(road.get("avg_speed"))
        risk = _safe_float(road.get("risk_score"))
        weather_cond = str(road.get("weather_condition") or "Unknown")
        delay_now = _safe_float(road.get("estimated_delay") or road.get("estimated_delay_minutes"))
        weather_l = weather_cond.lower()
        weather_severity = 0.0
        if "storm" in weather_l or "thunder" in weather_l:
            weather_severity = 1.0
        elif "rain" in weather_l or "drizzle" in weather_l:
            weather_severity = 0.65
        elif "cloud" in weather_l:
            weather_severity = 0.25

        road_metrics.append(
            {
                "speed": speed,
                "risk": risk,
                "weather_condition": weather_cond,
                "weather_severity": weather_severity,
                "delay_now": delay_now,
                "status": str(road.get("status") or "normal").lower(),
                "congestion_level": str(road.get("congestion_level") or "Low"),
            }
        )

        feature_rows.append(
            {
                "speed_kmph": speed,
                "weather_temp_c": _safe_float(road.get("weather_temp_c"), 30.0),
                "humidity_pct": _safe_float(road.get("humidity_pct"), 70.0),
                "accident_severity": _safe_float(road.get("accident_severity"), 0.0),
                "congestion_km": _safe_float(road.get("congestion_km"), 0.0),
            }
        )

    if model_bundle:
        probs = predict_probability(model_bundle, feature_rows)
    else:
        probs = [_clamp((40.0 - row["speed_kmph"]) / 40.0, 0.0, 1.0) for row in feature_rows]

    horizon_multiplier = {10: 1.0, 30: 1.18, 60: 1.35}[minutes]

    predictions = []
    for road, base_prob, metrics in zip(roads, probs, road_metrics):
        speed = metrics["speed"]
        risk = metrics["risk"]
        weather_severity = metrics["weather_severity"]
        delay_now = metrics["delay_now"]
        status = metrics["status"]

        trend = 0.0
        if status == "congested" or speed < 18:
            trend += 0.22
        elif status == "slow" or speed < 35:
            trend += 0.10
        if risk > 65:
            trend += 0.10
        if weather_severity > 0:
            trend += weather_severity * 0.08

        prob = _clamp(base_prob * horizon_multiplier + trend * (horizon_multiplier - 0.85), 0.01, 0.995)

        # Delay model is bounded by horizon to avoid unrealistic spikes.
        delay_now = _clamp(delay_now, 0.0, max(5.0, minutes * 0.8))
        speed_factor = _clamp((35.0 - speed) / 35.0, 0.0, 1.0)
        risk_factor = _clamp(risk / 100.0, 0.0, 1.0)
        status_factor = 0.14 if status == "congested" else 0.06 if status == "slow" else 0.0

        delay_base = minutes * (0.04 + 0.28 * prob + 0.22 * speed_factor)
        delay_minutes = delay_base
        delay_minutes += delay_now * 0.35
        delay_minutes += minutes * 0.12 * risk_factor
        delay_minutes += minutes * 0.06 * _clamp(weather_severity, 0.0, 1.0)
        delay_minutes += minutes * status_factor

        delay_cap = minutes * (1.25 if status == "congested" else 0.9)
        delay_minutes = round(_clamp(delay_minutes, 0.3, max(1.0, delay_cap)), 1)

        delay_range = {
            "min": round(max(0.0, delay_minutes * 0.78), 1),
            "max": round(delay_minutes * 1.28 + 1.0, 1),
        }

        certainty = abs(prob - 0.5) * 2
        if certainty >= 0.55:
            confidence = "Cao"
        elif certainty >= 0.30:
            confidence = "Trung bình"
        else:
            confidence = "Thấp"

        if prob >= 0.85:
            severity_level = "Rất cao"
        elif prob >= 0.70:
            severity_level = "Cao"
        elif prob >= 0.50:
            severity_level = "Trung bình"
        elif prob >= 0.35:
            severity_level = "Thấp"
        else:
            severity_level = "Rất thấp"

        if prob >= 0.80 or delay_minutes >= minutes * 1.4:
            recommendation = f"Nên tránh tuyến này trong {minutes} phút tới"
        elif prob >= 0.60:
            recommendation = "Nên chọn tuyến thay thế để giảm trễ"
        elif prob >= 0.40:
            recommendation = "Theo dõi thêm, có thể chậm theo thời điểm"
        else:
            recommendation = "Lưu thông tương đối ổn định"

        speed_impact = _clamp((45.0 - speed) / 45.0, 0.0, 1.0)
        risk_impact = _clamp(risk / 100.0, 0.0, 1.0)
        weather_impact = _clamp(weather_severity, 0.0, 1.0)
        reason_summary = "Tốc độ thấp + rủi ro cao" if speed_impact + risk_impact > 1.0 else (
            "Biến động thời tiết ảnh hưởng lưu thông" if weather_impact > 0.5 else "Điều kiện giao thông ổn định"
        )

        predicted_level = "High" if prob >= 0.70 else "Moderate" if prob >= 0.35 else "Low"

        predictions.append(
            {
                "road_id": road.get("road_id", ""),
                "location_key": road.get("location_key", road.get("road_id", "")),
                "congestion_probability": round(float(prob), 4),
                "predicted_status": "congested" if float(prob) >= 0.5 else "normal",
                "predicted_congestion_level": predicted_level,
                "predicted_delay_minutes": delay_minutes,
                "predicted_delay_range_minutes": delay_range,
                "confidence": confidence,
                "severity_level": severity_level,
                "recommendation": recommendation,
                "reason_summary": reason_summary,
                "top_factors": [
                    {
                        "factor": "speed",
                        "label": "Tốc độ hiện tại",
                        "impact": round(speed_impact, 3),
                        "value": round(speed, 2),
                        "unit": "km/h",
                    },
                    {
                        "factor": "risk_score",
                        "label": "Điểm rủi ro",
                        "impact": round(risk_impact, 3),
                        "value": round(risk, 2),
                        "unit": "/100",
                    },
                    {
                        "factor": "weather",
                        "label": "Ảnh hưởng thời tiết",
                        "impact": round(weather_impact, 3),
                        "value": metrics["weather_condition"],
                        "unit": "",
                    },
                ],
                "current_speed": round(speed, 2),
                "risk_score": round(risk, 2),
                "weather_condition": metrics["weather_condition"],
                "estimated_delay_current": round(delay_now, 2),
            }
        )

    predictions.sort(key=lambda item: item["congestion_probability"], reverse=True)
    top_alerts = [
        {
            "road_id": p["road_id"],
            "probability": p["congestion_probability"],
            "delay": p["predicted_delay_minutes"],
            "recommendation": p["recommendation"],
        }
        for p in predictions
        if p["congestion_probability"] >= 0.55
    ][:8]

    return {
        "minutes": minutes,
        "supported_horizons": [10, 30, 60],
        "predictions": predictions,
        "top_alerts": top_alerts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/traffic/advanced-analytics")
async def get_advanced_analytics():
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    roads = redis_client.get_all_roads()
    if not roads:
        return {
            "road_risk_ranking": [],
            "speed_anomalies": [],
            "low_fuel_forecast": [],
            "district_congestion": [],
            "district_peak_forecast": [],
            "road_list": [],
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    rc = redis_client.client
    total_by_road = _read_float_hash(rc, "traffic:stats:total_by_road")
    high_by_road = _read_float_hash(rc, "traffic:stats:high_congestion_by_road")
    delay_sum_by_road = _read_float_hash(rc, "traffic:stats:delay_sum_by_road")
    risk_sum_by_road = _read_float_hash(rc, "traffic:stats:risk_sum_by_road")
    speeding_by_road = _read_float_hash(rc, "traffic:stats:speeding_by_road")

    road_groups: Dict[str, List[dict]] = {}
    for road in roads:
        road_name = str(road.get("road_name") or road.get("road_id") or "").strip()
        if not road_name:
            continue
        road_groups.setdefault(road_name, []).append(road)

    road_risk_ranking = []
    road_names = set(road_groups.keys()) | set(total_by_road.keys())
    for road_name in road_names:
        group = road_groups.get(road_name, [])
        district = str(group[0].get("district") if group else "Unknown")
        speed_avg = (
            sum(_safe_float(r.get("avg_speed")) for r in group) / len(group)
            if group else 0.0
        )
        total = max(1.0, total_by_road.get(road_name, float(len(group) or 1)))
        high_pct = round((high_by_road.get(road_name, 0.0) / total) * 100.0, 1)
        avg_delay = round(delay_sum_by_road.get(road_name, 0.0) / total, 2)
        avg_risk = round(risk_sum_by_road.get(road_name, 0.0) / total, 2)
        if avg_risk == 0.0 and group:
            avg_risk = round(sum(_safe_float(r.get("risk_score")) for r in group) / len(group), 2)
        if avg_delay == 0.0 and group:
            avg_delay = round(
                sum(_safe_float(r.get("estimated_delay") or r.get("estimated_delay_minutes")) for r in group) / len(group),
                2,
            )
        speeding_rate = round((speeding_by_road.get(road_name, 0.0) / total) * 100.0, 1)

        composite = _clamp(
            high_pct * 0.35
            + min(avg_delay * 4.0, 40.0) * 0.25
            + avg_risk * 0.30
            + speeding_rate * 0.10,
            0.0,
            100.0,
        )
        if composite >= 55:
            risk_level = "Cao"
        elif composite >= 30:
            risk_level = "Trung bình"
        else:
            risk_level = "Thấp"

        road_risk_ranking.append(
            {
                "road_name": road_name,
                "district": district,
                "avg_speed": round(speed_avg, 2),
                "high_pct": high_pct,
                "avg_delay": avg_delay,
                "speeding_rate": speeding_rate,
                "composite_risk": round(composite, 2),
                "risk_level": risk_level,
            }
        )
    road_risk_ranking.sort(key=lambda item: item["composite_risk"], reverse=True)

    district_speed: Dict[str, List[float]] = {}
    for road in roads:
        district = str(road.get("district") or "Unknown")
        district_speed.setdefault(district, []).append(_safe_float(road.get("avg_speed")))

    district_avg_speed = {
        district: (sum(values) / len(values)) if values else 0.0
        for district, values in district_speed.items()
    }

    speed_anomalies = []
    for road in roads:
        district = str(road.get("district") or "Unknown")
        road_speed = _safe_float(road.get("avg_speed"))
        district_avg = district_avg_speed.get(district, road_speed)
        deviation = round(max(0.0, district_avg - road_speed), 2)
        if deviation < 8:
            continue

        severity = "Cao" if deviation >= 15 else "Trung bình"
        speed_anomalies.append(
            {
                "road_name": str(road.get("road_name") or road.get("road_id") or ""),
                "district": district,
                "road_speed": round(road_speed, 2),
                "district_avg": round(district_avg, 2),
                "deviation": deviation,
                "severity": severity,
                "alert": "Khả năng ùn tắc cục bộ",
            }
        )
    speed_anomalies.sort(key=lambda item: item["deviation"], reverse=True)

    district_congestion_map: Dict[str, dict] = {}
    for road in roads:
        district = str(road.get("district") or "Unknown")
        agg = district_congestion_map.setdefault(
            district,
            {"district": district, "roads": 0, "high": 0, "delay_sum": 0.0, "risk_sum": 0.0},
        )
        agg["roads"] += 1
        if str(road.get("status") or "").lower() == "congested" or _safe_float(road.get("avg_speed")) < 20:
            agg["high"] += 1
        agg["delay_sum"] += _safe_float(road.get("estimated_delay") or road.get("estimated_delay_minutes"))
        agg["risk_sum"] += _safe_float(road.get("risk_score"))

    district_congestion = []
    district_peak_forecast = []
    for district, agg in district_congestion_map.items():
        road_count = max(1, agg["roads"])
        high_pct = round(agg["high"] / road_count * 100.0, 1)
        avg_delay = round(agg["delay_sum"] / road_count, 2)
        avg_risk = round(agg["risk_sum"] / road_count, 2)

        district_congestion.append(
            {
                "district": district,
                "high_road_pct": high_pct,
                "avg_delay": avg_delay,
                "avg_risk": avg_risk,
                "road_count": road_count,
            }
        )

        if high_pct >= 50:
            peak_window = "10 phút tới"
        elif high_pct >= 25:
            peak_window = "30 phút tới"
        else:
            peak_window = "60 phút tới"
        district_peak_forecast.append(
            {
                "district": district,
                "peak_window": peak_window,
                "peak_score": round(_clamp(high_pct * 0.6 + avg_risk * 0.4, 0.0, 100.0), 1),
            }
        )

    district_congestion.sort(key=lambda item: item["high_road_pct"], reverse=True)
    district_peak_forecast.sort(key=lambda item: item["peak_score"], reverse=True)

    fuel_groups: Dict[str, dict] = {}
    for road in roads:
        road_name = str(road.get("road_name") or road.get("road_id") or "").strip()
        district = str(road.get("district") or "Unknown")
        fuel_level = _safe_float(road.get("fuel_level"), 0.0)
        speed = max(10.0, _safe_float(road.get("avg_speed"), 20.0))

        if fuel_level <= 0:
            continue

        agg = fuel_groups.setdefault(
            road_name,
            {
                "road_name": road_name,
                "district": district,
                "count": 0,
                "low_count": 0,
                "fuel_sum": 0.0,
                "speed_sum": 0.0,
            },
        )
        agg["count"] += 1
        agg["fuel_sum"] += fuel_level
        agg["speed_sum"] += speed
        if fuel_level < 20:
            agg["low_count"] += 1

    low_fuel_forecast = []
    for agg in fuel_groups.values():
        count = max(1, agg["count"])
        avg_fuel = agg["fuel_sum"] / count
        low_rate = (agg["low_count"] / count) * 100.0
        if avg_fuel > 35 and low_rate < 30:
            continue

        avg_speed = agg["speed_sum"] / count
        range_km = (avg_fuel / 100.0) * 450.0
        hours_to_empty = range_km / max(12.0, avg_speed)
        urgency = "Khẩn cấp" if avg_fuel < 15 or hours_to_empty < 1.5 else "Cảnh báo"

        low_fuel_forecast.append(
            {
                "road_name": agg["road_name"],
                "district": agg["district"],
                "avg_fuel_pct": round(avg_fuel, 1),
                "range_km": round(range_km, 1),
                "hours_to_empty": round(hours_to_empty, 1),
                "low_fuel_rate": round(low_rate, 1),
                "urgency": urgency,
            }
        )
    low_fuel_forecast.sort(key=lambda item: item["avg_fuel_pct"])

    road_list = [
        {
            "road_id": str(road.get("road_id") or road.get("location_key") or ""),
            "road_name": str(road.get("road_name") or road.get("road_id") or ""),
            "district": str(road.get("district") or "Unknown"),
        }
        for road in roads
    ]

    road_list.sort(key=lambda item: (item["district"], item["road_name"]))

    return {
        "road_risk_ranking": road_risk_ranking[:30],
        "speed_anomalies": speed_anomalies[:30],
        "low_fuel_forecast": low_fuel_forecast[:20],
        "district_congestion": district_congestion[:20],
        "district_peak_forecast": district_peak_forecast[:20],
        "road_list": road_list,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/traffic/route-suggestions")
async def get_route_suggestions(
    from_road_id: str = Query(..., min_length=1),
    to_road_id: str = Query(..., min_length=1),
):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")
    if from_road_id.strip().lower() == to_road_id.strip().lower():
        raise HTTPException(status_code=400, detail="from_road_id and to_road_id must be different")

    roads = redis_client.get_all_roads()
    from_road = _find_road(roads, from_road_id)
    to_road = _find_road(roads, to_road_id)
    if not from_road or not to_road:
        raise HTTPException(status_code=404, detail="Road not found")

    suggestions, direct_distance = _build_route_suggestions(roads, from_road, to_road)
    return {
        "from_road_id": from_road.get("road_id", ""),
        "from_road_name": from_road.get("road_name", ""),
        "to_road_id": to_road.get("road_id", ""),
        "to_road_name": to_road.get("road_name", ""),
        "direct_distance_km": direct_distance,
        "priorities": {
            "speed": "high",
            "delay": "low",
            "risk": "low",
        },
        "suggestions": suggestions,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/traffic/route-estimate")
async def get_route_estimate(
    from_road: str = Query(..., min_length=1),
    to_road: str = Query(..., min_length=1),
):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    roads = redis_client.get_all_roads()
    from_data = _find_road(roads, from_road)
    to_data = _find_road(roads, to_road)
    if not from_data or not to_data:
        raise HTTPException(status_code=404, detail="Road not found")

    suggestions, direct_distance = _build_route_suggestions(roads, from_data, to_data)
    if not suggestions:
        raise HTTPException(status_code=422, detail="Unable to estimate route")

    best = suggestions[0]
    return {
        "from_road": from_data.get("road_name") or from_data.get("road_id"),
        "from_district": from_data.get("district", "Unknown"),
        "to_road": to_data.get("road_name") or to_data.get("road_id"),
        "to_district": to_data.get("district", "Unknown"),
        "distance_km": best["distance_km"],
        "direct_distance_km": direct_distance,
        "avg_speed_kmh": best["avg_speed_kmh"],
        "travel_minutes": best["travel_minutes"],
        "delay_minutes": best["delay_minutes"],
        "total_minutes": best["total_minutes"],
        "avg_risk_score": best["avg_risk_score"],
        "route_score": best["route_score"],
        "route_roads_count": len(best["route_roads"]),
        "route_roads": best["route_roads"],
        "recommendation": best["recommendation"],
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/weather")
async def get_weather():
    days = []
    source_days = weather_payload.get("days", []) if isinstance(weather_payload, dict) else []
    for day in source_days:
        hours = []
        for hour in day.get("hours", []):
            hours.append(
                {
                    "time": hour.get("datetime", ""),
                    "temp": round((float(hour.get("temp", day.get("temp", 77.0))) - 32) * 5 / 9, 1),
                    "humidity": float(hour.get("humidity", day.get("humidity", 0) or 0)),
                    "conditions": hour.get("conditions", day.get("conditions", "Unknown")),
                }
            )

        days.append(
            {
                "date": day.get("datetime", ""),
                "tempmax": round((float(day.get("tempmax", 77.0)) - 32) * 5 / 9, 1),
                "tempmin": round((float(day.get("tempmin", 77.0)) - 32) * 5 / 9, 1),
                "temp": round((float(day.get("temp", 77.0)) - 32) * 5 / 9, 1),
                "humidity": float(day.get("humidity", 0) or 0),
                "precipprob": float(day.get("precipprob", 0) or 0),
                "windspeed": float(day.get("windspeed", 0) or 0),
                "windgust": float(day.get("windgust", 0) or 0),
                "uvindex": float(day.get("uvindex", 0) or 0),
                "conditions": day.get("conditions", "Unknown"),
                "icon": day.get("icon", "cloudy"),
                "hours": hours,
            }
        )

    return {"days": days, "count": len(days)}


@app.get("/accidents")
async def get_accidents(limit: int = Query(default=300, ge=1, le=1000)):
    return {
        "accidents": accidents_payload[:limit],
        "count": len(accidents_payload),
    }


@app.get("/accidents/stats")
async def get_accident_stats():
    by_district: Dict[str, int] = {}
    by_severity: Dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    total_congestion = 0.0

    for item in accidents_payload:
        district = item.get("district", "Unknown")
        by_district[district] = by_district.get(district, 0) + 1
        sev = int(item.get("accident_severity", 0) or 0)
        if sev in by_severity:
            by_severity[sev] += 1
        total_congestion += float(item.get("congestion_km", 0) or 0)

    return {
        "total": len(accidents_payload),
        "total_congestion_km": round(total_congestion, 2),
        "by_district": [
            {"district": key, "count": value}
            for key, value in sorted(by_district.items(), key=lambda kv: kv[1], reverse=True)
        ],
        "by_severity": [
            {"severity": key, "count": value}
            for key, value in sorted(by_severity.items(), key=lambda kv: kv[0])
        ],
    }


@app.get("/traffic/summary")
async def get_summary():
    """Get global traffic KPIs."""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    roads = redis_client.get_all_roads()
    rc = redis_client.client

    total_roads = len(roads)
    speeds = [float(r.get("avg_speed") or 0) for r in roads]
    avg_speed = round(sum(speeds) / total_roads, 2) if total_roads else 0
    congested = sum(1 for r in roads if r.get("status") == "congested")

    # total_vehicles = total records loaded (accumulated via HINCRBY during load)
    total_vehicles = int(float(rc.hget("traffic:summary", "total_vehicles") or 0))
    total_passengers = int(float(rc.hget("traffic:summary", "total_passengers") or 0))
    speeding_alerts = int(float(rc.hget("traffic:summary", "speeding_alerts") or 0))
    low_fuel_alerts = int(float(rc.hget("traffic:summary", "low_fuel_alerts") or 0))
    fuel_sum   = float(rc.hget("traffic:summary", "_fuel_sum") or 0)
    fuel_count = float(rc.hget("traffic:summary", "_fuel_count") or 0)
    avg_fuel   = round(fuel_sum / fuel_count, 1) if fuel_count > 0 else 0

    # Only update road-level fields — don't overwrite accumulated counters
    redis_client.client.hset("traffic:summary", mapping={
        "total_roads":     total_roads,
        "avg_speed":       avg_speed,
        "congested_roads": congested,
        "total_passengers": total_passengers,
        "speeding_alerts": speeding_alerts,
        "low_fuel_alerts": low_fuel_alerts,
        "avg_fuel_level":  avg_fuel,
        "updated_at":      datetime.now(timezone.utc).isoformat(),
    })
    return {
        "total_roads":      total_roads,
        "avg_speed":        avg_speed,
        "total_vehicles":   total_vehicles,
        "congested_roads":  congested,
        "total_passengers": total_passengers,
        "speeding_alerts":  speeding_alerts,
        "low_fuel_alerts":  low_fuel_alerts,
        "avg_fuel_level":   avg_fuel,
        "updated_at":       datetime.now(timezone.utc).isoformat(),
    }


@app.get("/traffic/congested")
async def get_congested():
    """Get list of congested roads"""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    congested_ids = redis_client.get_congested_roads()
    congested_data = []

    for road_id in congested_ids:
        data = redis_client.get_road_data(road_id)
        if data:
            congested_data.append(data)

    return {
        "congested": congested_data,
        "count": len(congested_data),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/traffic/{road_id}")
async def get_road(road_id: str):
    """Get specific road data + rolling window"""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    data = redis_client.get_road_data(road_id)
    if not data:
        raise HTTPException(status_code=404, detail=f"Road '{road_id}' not found")

    window = redis_client.get_road_window(road_id, minutes=5)

    return {
        "current": data,
        "window": window,
        "window_size": len(window),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# === WebSocket Endpoint ===

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """
    WebSocket endpoint for real-time traffic updates.
    
    Client can send:
      {"subscribe": "all"}           → receive all updates
      {"subscribe": "road_q1_01"}    → receive updates for specific road
      {"subscribe": "region:q1"}     → receive updates for region
      {"action": "ping"}             → health check
    """
    client_id = await ws_manager.connect(websocket)

    # Send initial data
    if redis_client:
        try:
            roads = redis_client.get_all_roads()
            summary = redis_client.get_summary()
            loading = PROGRESS.to_dict()
            await ws_manager.send_personal(client_id, {
                "type": "initial_data",
                "roads": roads,
                "summary": summary,
                "loading": loading,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            logger.error(f"❌ Initial data error: {e}")

    try:
        while True:
            data = await websocket.receive_text()
            await ws_manager.handle_client_message(client_id, data)
    except WebSocketDisconnect:
        ws_manager.disconnect(client_id)
    except Exception as e:
        logger.error(f"❌ WS error for {client_id}: {e}")
        ws_manager.disconnect(client_id)


# === Run ===

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
