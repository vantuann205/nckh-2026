"""FastAPI backend for realtime ingest, cache, history, analytics, and prediction."""

import json
import asyncio
import logging
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

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
async def get_predict(minutes: int = Query(default=5, ge=5, le=60)):
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    roads = redis_client.get_all_roads()
    if not roads:
        return {
            "predictions": [],
            "minutes": minutes,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    feature_rows: List[dict] = []
    for road in roads:
        speed = float(road.get("avg_speed", 0) or 0)
        vehicle_count = int(road.get("vehicle_count", 0) or 0)
        risk_score = float(road.get("risk_score", 0) or 0)
        weather_cond = (road.get("weather_condition") or "").lower()

        # Weather severity score
        weather_severity = 0.0
        if "storm" in weather_cond or "thunder" in weather_cond:
            weather_severity = 3.0
        elif "rain" in weather_cond or "drizzle" in weather_cond:
            weather_severity = 2.0
        elif "cloud" in weather_cond or "overcast" in weather_cond:
            weather_severity = 1.0

        feature_rows.append({
            "speed_kmph": speed,
            "weather_temp_c": float(road.get("weather_temp_c", 30) or 30),
            "humidity_pct": float(road.get("humidity_pct", 70) or 70),
            "accident_severity": float(road.get("accident_severity", 0) or 0),
            "congestion_km": float(road.get("congestion_km", 0) or 0),
            # Extra features for richer prediction
            "_vehicle_count": vehicle_count,
            "_risk_score": risk_score,
            "_weather_severity": weather_severity,
        })

    if model_bundle:
        probs = predict_probability(model_bundle, feature_rows)
    else:
        # Heuristic fallback: multi-factor probability
        probs = []
        for row in feature_rows:
            speed = row["speed_kmph"]
            risk = row["_risk_score"]
            weather_sev = row["_weather_severity"]
            acc = row["accident_severity"]

            # Speed-based base probability (logistic curve)
            speed_prob = 1 / (1 + (speed / 20) ** 2) if speed > 0 else 0.8

            # Combine factors
            combined = (
                speed_prob * 0.50 +
                min(1.0, risk / 100) * 0.25 +
                min(1.0, weather_sev / 3) * 0.15 +
                min(1.0, acc / 5) * 0.10
            )
            probs.append(round(min(1.0, max(0.0, combined)), 4))

    predictions = []
    for road, prob, feat in zip(roads, probs, feature_rows):
        speed = feat["speed_kmph"]
        # Delay estimate: more realistic formula
        if prob >= 0.5:
            delay_est = round((1.0 - min(1.0, speed / 40.0)) * minutes * 8, 1)
        else:
            delay_est = round(prob * minutes * 2, 1)

        # Confidence level
        if prob >= 0.75:
            confidence = "Cao"
        elif prob >= 0.45:
            confidence = "Trung bình"
        else:
            confidence = "Thấp"

        # Recommendation
        if prob >= 0.75:
            recommendation = "Chuyển tuyến ngay"
        elif prob >= 0.5:
            recommendation = "Giảm tốc độ, chuẩn bị dừng"
        elif prob >= 0.3:
            recommendation = "Theo dõi chặt chẽ"
        else:
            recommendation = "Lưu thông bình thường"

        predictions.append({
            "road_id": road.get("road_id", ""),
            "location_key": road.get("location_key", road.get("road_id", "")),
            "congestion_probability": round(float(prob), 4),
            "predicted_status": "congested" if float(prob) >= 0.5 else "normal",
            "predicted_delay_minutes": delay_est,
            "confidence": confidence,
            "recommendation": recommendation,
            "current_speed": round(feat["speed_kmph"], 1),
            "risk_score": round(feat["_risk_score"], 1),
            "weather_condition": road.get("weather_condition", "Unknown"),
        })

    # Sort by probability descending
    predictions.sort(key=lambda x: x["congestion_probability"], reverse=True)

    return {
        "minutes": minutes,
        "total_roads": len(predictions),
        "congested_count": sum(1 for p in predictions if p["predicted_status"] == "congested"),
        "predictions": predictions,
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
