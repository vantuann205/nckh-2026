import os
import time
import json
import duckdb
import pandas as pd
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TrafficAPI")

app = FastAPI(title="Traffic Lakehouse API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATA_DIR = "f:/nckh-2026/analyse-data/data"
DELTA_PATH = "f:/nckh-2026/analyse-data/lakehouse/delta"
DB_PATH = "f:/nckh-2026/analyse-data/traffic.db"
con_lock = threading.Lock()
con = duckdb.connect(DB_PATH)

def setup_schema():
    with con_lock:
        # Load Delta extension for DuckDB
        con.execute("INSTALL delta; LOAD delta;")
        
        # Create views pointing to Delta tables for easy access
        try:
            con.execute(f"CREATE OR REPLACE VIEW traffic_silver AS SELECT * FROM delta_scan('{DELTA_PATH}/silver_traffic')")
            con.execute(f"CREATE OR REPLACE VIEW traffic_gold AS SELECT * FROM delta_scan('{DELTA_PATH}/gold_kpis')")
            logger.info("✅ Delta views created successfully")
        except Exception as e:
            logger.warning(f"⚠️ Could not create Delta views (might be empty yet): {e}")

        # Fallback for store if delta is not yet ready
        con.execute("""
            CREATE TABLE IF NOT EXISTS traffic_store_local (
                vehicle_id VARCHAR, vehicle_type VARCHAR, speed_kmph DOUBLE,
                street VARCHAR, district VARCHAR, city VARCHAR,
                owner_name VARCHAR, license_number VARCHAR,
                lat DOUBLE, lng DOUBLE, congestion VARCHAR,
                weather VARCHAR, ts TIMESTAMP, fuel_level DOUBLE
            )
        """)

def ingest_incremental():
    """Now handled by Spark Streaming. This function now just refreshes views."""
    with con_lock:
        try:
            con.execute(f"CREATE OR REPLACE VIEW traffic_silver AS SELECT * FROM delta_scan('{DELTA_PATH}/silver_traffic')")
            con.execute(f"CREATE OR REPLACE VIEW traffic_gold AS SELECT * FROM delta_scan('{DELTA_PATH}/gold_kpis')")
            logger.info("🔄 Synced with Delta Lake layers")
        except:
            pass

def init_db():
    setup_schema()
    # Initial sync
    ingest_incremental()

# --- API ENDPOINTS ---

@app.get("/api/summary")
def get_summary():
    with con_lock:
        try:
            # Try Gold Table first (most efficient)
            res = con.execute("""
                SELECT 
                    SUM(total_vehicles) as total,
                    AVG(avg_speed) as speed,
                    SUM(total_vehicles) as active,
                    SUM(alerts) as alerts,
                    0 as congested
                FROM traffic_gold
            """).fetchone()
            
            if res and res[0]:
                return {
                    "total": int(res[0]), "avgSpeed": round(res[1] or 0, 1), "active": int(res[2]), "alerts": int(res[3]), "congested": 0
                }
            
            # Fallback to Silver
            res = con.execute("""
                SELECT count(*), avg(speed_kmph), count(*), sum(case when speed_kmph > 80 then 1 else 0 end)
                FROM traffic_silver
            """).fetchone()
            return {
                "total": res[0] or 0, "avgSpeed": round(res[1] or 0, 1), "active": res[2] or 0, "alerts": res[3] or 0, "congested": 0
            }
        except Exception as e:
            return {"total": 0, "avgSpeed": 0, "active": 0, "alerts": 0, "congested": 0}

@app.get("/api/explorer")
def get_explorer(search: str = "", vtype: str = "", district: str = "", limit: int = 100):
    with con_lock:
        try:
            query = "SELECT * FROM traffic_silver WHERE 1=1"
            params = []
            if search:
                query += " AND (vehicle_id ILIKE ? OR owner_name ILIKE ?)"
                params.extend([f"%{search}%", f"%{search}%"])
            if vtype: query += " AND vehicle_type = ?" ; params.append(vtype)
            if district: query += " AND district = ?" ; params.append(district)
            query += f" ORDER BY ts DESC LIMIT {limit}"
            return con.execute(query, params).df().to_dict(orient="records")
        except: return []

@app.get("/api/map")
def get_map_data(limit: int = 2000):
    with con_lock:
        try:
            return con.execute(f"SELECT vehicle_id, lat, lng, speed_kmph, vehicle_type, congestion_level as congestion FROM traffic_silver LIMIT {limit}").df().to_dict(orient="records")
        except Exception as e:
            print(f"Map Err: {e}")
            return []

@app.get("/api/stats/flow")
def get_flow_stats():
    with con_lock:
        try:
            return con.execute("SELECT hour(ts) as hour, count(*) as count FROM traffic_silver GROUP BY 1 ORDER BY 1").df().to_dict(orient="records")
        except: return []

@app.get("/api/stats/types")
def get_type_stats():
    with con_lock:
        try:
            return con.execute("SELECT vehicle_type, count(*) as count FROM traffic_silver GROUP BY 1").df().to_dict(orient="records")
        except: return []

@app.get("/api/stats/speed")
def get_speed_stats():
    with con_lock:
        try:
            return con.execute("""
                SELECT 
                    CASE 
                        WHEN speed_kmph < 20 THEN '0-20'
                        WHEN speed_kmph < 40 THEN '21-40'
                        WHEN speed_kmph < 60 THEN '41-60'
                        WHEN speed_kmph < 80 THEN '61-80'
                        ELSE '81+'
                    END as bucket,
                    count(*) as count
                FROM traffic_silver GROUP BY 1 ORDER BY bucket
            """).df().to_dict(orient="records")
        except: return []

@app.get("/api/stats/weather")
def get_weather_stats():
    with con_lock:
        try:
            return con.execute("SELECT weather, count(*) as count FROM traffic_silver GROUP BY 1").df().to_dict(orient="records")
        except: return []

@app.get("/api/stats/districts")
def get_district_stats():
    with con_lock:
        try:
            return con.execute("SELECT district, count(*) as count FROM traffic_silver GROUP BY 1 ORDER BY 2 DESC").df().to_dict(orient="records")
        except: return []

@app.get("/api/status")
def get_status():
    with con_lock:
        try:
            # Check the actual silver table for freshness
            count = con.execute("SELECT count(*) FROM traffic_silver").fetchone()[0]
            # Get latest processing time from logs if possible, or just use current
            return {"status": "healthy", "total_records": count, "last_refresh": time.strftime("%Y-%m-%d %H:%M:%S")}
        except: return {"status": "error"}

class DataHandler(FileSystemEventHandler):
    _last_trigger = 0
    def on_modified(self, event):
        # Watch for Delta log changes to trigger re-scan
        if '_delta_log' in event.src_path and (time.time() - self._last_trigger > 1):
            self._last_trigger = time.time()
            threading.Thread(target=ingest_incremental).start()

if __name__ == "__main__":
    init_db()
    observer = Observer()
    observer.schedule(DataHandler(), DATA_DIR, recursive=False)
    observer.start()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
