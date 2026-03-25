"""
FastAPI Backend Server
- REST API: /traffic/realtime, /traffic/{road_id}, /traffic/summary, /traffic/congested
- WebSocket: /ws with subscription filtering
- Redis Pub/Sub listener → auto-broadcast via WebSocket
"""

import json
import asyncio
import logging
import threading
from datetime import datetime, timezone
from typing import Optional

import redis
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from storage.redis_client import TrafficRedisClient
from backend.ws_manager import ConnectionManager
from stream_processing.config import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_CHANNEL

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
ws_manager = ConnectionManager()
_pubsub_thread = None
_main_loop = None


# === Lifecycle ===

@app.on_event("startup")
async def startup():
    global redis_client, _pubsub_thread, _main_loop
    logger.info("🚀 Starting Realtime Traffic API...")
    
    # Capture the main event loop for cross-thread broadcasts
    _main_loop = asyncio.get_running_loop()

    # Connect Redis
    try:
        redis_client = TrafficRedisClient()
    except Exception as e:
        logger.error(f"❌ Redis connection failed: {e}")
        logger.warning("⚠️ API will start without Redis — endpoints may return empty data")

    # Start Redis Pub/Sub listener in background thread
    _pubsub_thread = threading.Thread(target=_redis_listener, args=(_main_loop,), daemon=True)
    _pubsub_thread.start()
    logger.info("✅ API ready!")


@app.on_event("shutdown")
async def shutdown():
    if redis_client:
        redis_client.close()
    logger.info("🛑 API shutdown")


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
        "version": "2.0.0",
        "endpoints": [
            "/traffic/realtime",
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
    redis_stats = {}
    if redis_client:
        try:
            redis_stats = redis_client.get_stats()
            redis_ok = True
        except Exception:
            pass

    return {
        "status": "healthy" if redis_ok else "degraded",
        "redis": {"connected": redis_ok, **redis_stats},
        "websocket_clients": ws_manager.connection_count,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


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


@app.get("/traffic/summary")
async def get_summary():
    """Get global traffic KPIs"""
    if not redis_client:
        raise HTTPException(status_code=503, detail="Redis not available")

    return redis_client.get_summary()


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
            await ws_manager.send_personal(client_id, {
                "type": "initial_data",
                "roads": roads,
                "summary": summary,
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
