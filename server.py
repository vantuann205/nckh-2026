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
DB_PATH = "f:/nckh-2026/analyse-data/traffic.db"
con_lock = threading.Lock()
con = duckdb.connect(DB_PATH)

def setup_schema():
    with con_lock:
        # Table to track ingested files (filename is PK)
        con.execute("CREATE TABLE IF NOT EXISTS processed_files (filename TEXT PRIMARY KEY, last_modified FLOAT, size BIGINT)")
        
        # Main persistent table
        con.execute("""
            CREATE TABLE IF NOT EXISTS traffic_store (
                vehicle_id VARCHAR,
                vehicle_type VARCHAR,
                speed_kmph DOUBLE,
                street VARCHAR,
                district VARCHAR,
                city VARCHAR,
                owner_name VARCHAR,
                license_number VARCHAR,
                lat DOUBLE,
                lng DOUBLE,
                congestion VARCHAR,
                weather VARCHAR,
                ts TIMESTAMP,
                fuel_level DOUBLE
            )
        """)
        
        # Indexes for fast aggregations
        con.execute("CREATE INDEX IF NOT EXISTS idx_vtype ON traffic_store (vehicle_type)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_district ON traffic_store (district)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_ts ON traffic_store (ts)")

def ingest_incremental():
    with con_lock:
        logger.info("🔍 Checking for new data files...")
        files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
        
        for f in files:
            path = os.path.join(DATA_DIR, f)
            mtime = os.path.getmtime(path)
            size = os.path.getsize(path)
            
            # Strict check using PK
            res = con.execute("SELECT 1 FROM processed_files WHERE filename = ? AND last_modified = ? AND size = ?", (f, mtime, size)).fetchone()
            if res:
                continue

            logger.info(f"💾 Ingesting new file: {f}")
            try:
                json_path = path.replace("\\", "/")
                # Create a temp view for this file
                con.execute(f"CREATE OR REPLACE VIEW temp_json AS SELECT * FROM read_json_auto('{json_path}', format='array')")
                
                # Append to main store
                con.execute("""
                    INSERT INTO traffic_store
                    SELECT 
                        vehicle_id,
                        vehicle_type,
                        COALESCE(CAST(speed_kmph AS DOUBLE), 0),
                        road.street,
                        road.district,
                        road.city,
                        owner.name,
                        owner.license_number,
                        coordinates.latitude,
                        coordinates.longitude,
                        COALESCE(traffic_status.congestion_level, 'Normal'),
                        COALESCE(weather_condition.condition, 'Clear'),
                        CAST(timestamp AS TIMESTAMP),
                        0 as fuel_level
                    FROM temp_json
                """)
                
                # Mark as processed (metadata check)
                con.execute("INSERT OR REPLACE INTO processed_files VALUES (?, ?, ?)", (f, mtime, size))
                logger.info(f"✔️ Finished indexing: {f}")
                
            except Exception as e:
                logger.error(f"❌ Failed to ingest {f}: {e}")
        
        total = con.execute("SELECT count(*) FROM traffic_store").fetchone()[0]
        logger.info(f"📊 Current Lakehouse records: {total:,}")

def init_db():
    setup_schema()
    ingest_incremental()

# --- API ENDPOINTS ---

@app.get("/api/summary")
def get_summary():
    with con_lock:
        try:
            res = con.execute("""
                SELECT 
                    count(*) as total,
                    round(avg(speed_kmph), 1),
                    count(CASE WHEN speed_kmph > 0 THEN 1 END),
                    count(CASE WHEN speed_kmph > 80 THEN 1 END),
                    count(CASE WHEN congestion = 'High' THEN 1 END)
                FROM traffic_store
            """).fetchone()
            return {
                "total": res[0] or 0, "avgSpeed": res[1] or 0, "active": res[2] or 0, "alerts": res[3] or 0, "congested": res[4] or 0
            }
        except Exception: return {"total": 0, "avgSpeed": 0, "active": 0, "alerts": 0, "congested": 0}

@app.get("/api/explorer")
def get_explorer(search: str = "", vtype: str = "", district: str = "", limit: int = 100):
    with con_lock:
        try:
            query = "SELECT * FROM traffic_store WHERE 1=1"
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
            return con.execute(f"SELECT vehicle_id, lat, lng, speed_kmph, vehicle_type, congestion FROM traffic_store USING SAMPLE {limit}").df().to_dict(orient="records")
        except: return []

@app.get("/api/stats/flow")
def get_flow_stats():
    with con_lock:
        try:
            return con.execute("SELECT hour(ts) as hour, count(*) as count FROM traffic_store GROUP BY 1 ORDER BY 1").df().to_dict(orient="records")
        except: return []

@app.get("/api/stats/types")
def get_type_stats():
    with con_lock:
        try:
            return con.execute("SELECT vehicle_type, count(*) as count FROM traffic_store GROUP BY 1").df().to_dict(orient="records")
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
                FROM traffic_store GROUP BY 1 ORDER BY bucket
            """).df().to_dict(orient="records")
        except: return []

@app.get("/api/stats/weather")
def get_weather_stats():
    with con_lock:
        try:
            return con.execute("SELECT weather, count(*) as count FROM traffic_store GROUP BY 1").df().to_dict(orient="records")
        except: return []

@app.get("/api/stats/districts")
def get_district_stats():
    with con_lock:
        try:
            return con.execute("SELECT district, count(*) as count FROM traffic_store GROUP BY 1 ORDER BY 2 DESC").df().to_dict(orient="records")
        except: return []

@app.get("/api/status")

def get_status():
    with con_lock:
        try:
            total = con.execute("SELECT count(*) FROM traffic_store").fetchone()[0]
            return {"status": "healthy", "total_records": total, "last_refresh": time.strftime("%Y-%m-%d %H:%M:%S")}
        except: return {"status": "error"}

class DataHandler(FileSystemEventHandler):
    _last_trigger = 0
    def on_modified(self, event):
        if event.src_path.endswith('.json') and (time.time() - self._last_trigger > 2):
            self._last_trigger = time.time()
            threading.Thread(target=ingest_incremental).start()

if __name__ == "__main__":
    init_db()
    observer = Observer()
    observer.schedule(DataHandler(), DATA_DIR, recursive=False)
    observer.start()
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
