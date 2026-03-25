"""
Smart Traffic Data Server với File Watcher và Caching
Chỉ làm mới khi có thay đổi thực sự trong data folder
"""
import os
import json
import time
import hashlib
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import threading
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SmartTrafficAPI")

app = FastAPI(title="Smart Traffic Data API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global cache
DATA_CACHE = {
    "raw_data": [],
    "summary": {},
    "stats": {},
    "last_update": None,
    "file_hash": None,
    "total_records": 0
}

DATA_DIR = Path("data")
CACHE_LOCK = threading.Lock()

def calculate_file_hash(file_path: Path) -> str:
    """Tính hash của file để detect thay đổi"""
    hasher = hashlib.md5()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hasher.update(chunk)
    return hasher.hexdigest()

def load_all_data() -> List[Dict]:
    """Load tất cả data từ folder data"""
    all_data = []
    
    if not DATA_DIR.exists():
        logger.warning(f"Data directory {DATA_DIR} không tồn tại!")
        return all_data
    
    json_files = sorted(DATA_DIR.glob("traffic_data_*.json"))
    
    for file_path in json_files:
        try:
            logger.info(f"📂 Đang load {file_path.name}...")
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
            logger.info(f"✅ Loaded {len(data) if isinstance(data, list) else 1} records từ {file_path.name}")
        except Exception as e:
            logger.error(f"❌ Lỗi khi load {file_path}: {e}")
    
    return all_data

def process_data(raw_data: List[Dict]) -> Dict:
    """Xử lý và tính toán statistics từ raw data"""
    if not raw_data:
        return {
            "summary": {"total": 0, "avgSpeed": 0, "active": 0, "alerts": 0, "congested": 0},
            "stats": {"flow": [], "types": [], "speed": [], "weather": [], "districts": []}
        }
    
    # Summary calculations
    total = len(raw_data)
    speeds = [r.get('speed_kmph', 0) for r in raw_data]
    avg_speed = sum(speeds) / len(speeds) if speeds else 0
    
    alerts = sum(1 for r in raw_data if r.get('speed_kmph', 0) > 80 or 
                 r.get('fuel_level_percentage', 100) < 20)
    
    congested = sum(1 for r in raw_data if 
                   r.get('traffic_status', {}).get('congestion_level') == 'High')
    
    summary = {
        "total": total,
        "avgSpeed": round(avg_speed, 1),
        "active": total,  # Giả định tất cả đang active
        "alerts": alerts,
        "congested": congested
    }
    
    # Stats calculations
    # 1. Flow by hour
    hour_counts = {}
    for r in raw_data:
        ts = r.get('timestamp', '')
        if ts:
            try:
                hour = datetime.fromisoformat(ts.replace('Z', '+00:00')).hour
                hour_counts[hour] = hour_counts.get(hour, 0) + 1
            except:
                pass
    
    flow = [{"hour": h, "count": hour_counts.get(h, 0)} for h in range(24)]
    
    # 2. Vehicle types
    type_counts = {}
    for r in raw_data:
        vtype = r.get('vehicle_type', 'Unknown')
        type_counts[vtype] = type_counts.get(vtype, 0) + 1
    
    types = [{"vehicle_type": k, "count": v} for k, v in type_counts.items()]
    
    # 3. Speed distribution
    speed_buckets = {"0-20": 0, "21-40": 0, "41-60": 0, "61-80": 0, "81+": 0}
    for speed in speeds:
        if speed <= 20:
            speed_buckets["0-20"] += 1
        elif speed <= 40:
            speed_buckets["21-40"] += 1
        elif speed <= 60:
            speed_buckets["41-60"] += 1
        elif speed <= 80:
            speed_buckets["61-80"] += 1
        else:
            speed_buckets["81+"] += 1
    
    speed_dist = [{"bucket": k, "count": v} for k, v in speed_buckets.items()]
    
    # 4. Weather
    weather_counts = {}
    for r in raw_data:
        weather = r.get('weather_condition', {}).get('condition', 'Unknown')
        weather_counts[weather] = weather_counts.get(weather, 0) + 1
    
    weather = [{"weather": k, "count": v} for k, v in weather_counts.items()]
    
    # 5. Districts
    district_counts = {}
    for r in raw_data:
        district = r.get('road', {}).get('district', 'Unknown')
        district_counts[district] = district_counts.get(district, 0) + 1
    
    districts = [{"district": k, "count": v} for k, v in sorted(district_counts.items(), 
                                                                 key=lambda x: x[1], reverse=True)]
    
    stats = {
        "flow": flow,
        "types": types,
        "speed": speed_dist,
        "weather": weather,
        "districts": districts
    }
    
    return {"summary": summary, "stats": stats}

def refresh_cache():
    """Làm mới cache - chỉ gọi khi có thay đổi"""
    with CACHE_LOCK:
        logger.info("🔄 Bắt đầu refresh cache...")
        start_time = time.time()
        
        # Load data
        raw_data = load_all_data()
        
        # Process data
        processed = process_data(raw_data)
        
        # Update cache
        DATA_CACHE["raw_data"] = raw_data
        DATA_CACHE["summary"] = processed["summary"]
        DATA_CACHE["stats"] = processed["stats"]
        DATA_CACHE["last_update"] = datetime.now().isoformat()
        DATA_CACHE["total_records"] = len(raw_data)
        
        elapsed = time.time() - start_time
        logger.info(f"✅ Cache refreshed! {len(raw_data)} records trong {elapsed:.2f}s")

class DataFileHandler(FileSystemEventHandler):
    """File watcher để detect thay đổi trong data folder"""
    
    def __init__(self):
        self.last_refresh = 0
        self.debounce_seconds = 2  # Chờ 2s trước khi refresh
    
    def on_created(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.json'):
            logger.info(f"📁 File mới được tạo: {event.src_path}")
            self._trigger_refresh()
    
    def on_deleted(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.json'):
            logger.info(f"🗑️ File bị xóa: {event.src_path}")
            self._trigger_refresh()
    
    def on_modified(self, event):
        if event.is_directory:
            return
        if event.src_path.endswith('.json'):
            logger.info(f"✏️ File được sửa: {event.src_path}")
            self._trigger_refresh()
    
    def _trigger_refresh(self):
        """Trigger refresh với debounce"""
        current_time = time.time()
        if current_time - self.last_refresh > self.debounce_seconds:
            self.last_refresh = current_time
            threading.Thread(target=refresh_cache, daemon=True).start()

# API Endpoints
@app.on_event("startup")
async def startup_event():
    """Khởi động server"""
    logger.info("🚀 Smart Traffic API đang khởi động...")
    
    # Load initial data
    refresh_cache()
    
    # Start file watcher
    event_handler = DataFileHandler()
    observer = Observer()
    observer.schedule(event_handler, str(DATA_DIR), recursive=False)
    observer.start()
    
    # Store observer in app state
    app.state.observer = observer
    
    logger.info("👀 File watcher đã được kích hoạt")
    logger.info("✅ Server sẵn sàng!")

@app.on_event("shutdown")
async def shutdown_event():
    """Tắt server"""
    if hasattr(app.state, 'observer'):
        app.state.observer.stop()
        app.state.observer.join()
    logger.info("🛑 Server đã tắt")

@app.get("/")
async def root():
    return {
        "message": "Smart Traffic Data API",
        "version": "2.0.0",
        "status": "healthy",
        "features": ["file_watcher", "smart_caching", "auto_refresh"]
    }

@app.get("/api/status")
async def get_status():
    """Trạng thái hệ thống"""
    return {
        "status": "healthy",
        "total_records": DATA_CACHE["total_records"],
        "last_refresh": DATA_CACHE["last_update"],
        "cache_active": True
    }

@app.get("/api/summary")
async def get_summary():
    """Tổng quan dữ liệu"""
    return DATA_CACHE["summary"]

@app.get("/api/stats/flow")
async def get_flow_stats():
    """Thống kê theo giờ"""
    return DATA_CACHE["stats"]["flow"]

@app.get("/api/stats/types")
async def get_type_stats():
    """Thống kê theo loại xe"""
    return DATA_CACHE["stats"]["types"]

@app.get("/api/stats/speed")
async def get_speed_stats():
    """Phân bố tốc độ"""
    return DATA_CACHE["stats"]["speed"]

@app.get("/api/stats/weather")
async def get_weather_stats():
    """Thống kê thời tiết"""
    return DATA_CACHE["stats"]["weather"]

@app.get("/api/stats/districts")
async def get_district_stats():
    """Thống kê theo quận"""
    return DATA_CACHE["stats"]["districts"]

@app.get("/api/explorer")
async def get_explorer_data(
    search: str = "",
    vtype: str = "",
    district: str = "",
    limit: int = 100
):
    """Tra cứu dữ liệu với filter"""
    data = DATA_CACHE["raw_data"]
    
    # Apply filters
    filtered = data
    
    if search:
        search_lower = search.lower()
        filtered = [r for r in filtered if 
                   search_lower in r.get('vehicle_id', '').lower() or
                   search_lower in r.get('owner', {}).get('name', '').lower()]
    
    if vtype:
        filtered = [r for r in filtered if r.get('vehicle_type') == vtype]
    
    if district:
        filtered = [r for r in filtered if r.get('road', {}).get('district') == district]
    
    # Limit results
    filtered = filtered[:limit]
    
    # Format for frontend
    result = []
    for r in filtered:
        result.append({
            "vehicle_id": r.get('vehicle_id', ''),
            "owner_name": r.get('owner', {}).get('name', ''),
            "license_number": r.get('owner', {}).get('license_number', ''),
            "speed_kmph": r.get('speed_kmph', 0),
            "street": r.get('road', {}).get('street', ''),
            "district": r.get('road', {}).get('district', ''),
            "fuel_level": r.get('fuel_level_percentage', 0),
            "congestion": r.get('traffic_status', {}).get('congestion_level', 'Low')
        })
    
    return result

@app.get("/api/map")
async def get_map_data(limit: int = 2000):
    """Dữ liệu cho bản đồ"""
    data = DATA_CACHE["raw_data"][:limit]
    
    result = []
    for r in data:
        coords = r.get('coordinates', {})
        result.append({
            "vehicle_id": r.get('vehicle_id', ''),
            "lat": coords.get('latitude', 0),
            "lng": coords.get('longitude', 0),
            "speed_kmph": r.get('speed_kmph', 0),
            "vehicle_type": r.get('vehicle_type', ''),
            "congestion": r.get('traffic_status', {}).get('congestion_level', 'Low'),
            "district": r.get('road', {}).get('district', '')
        })
    
    return result

@app.post("/api/refresh")
async def manual_refresh():
    """Làm mới thủ công"""
    threading.Thread(target=refresh_cache, daemon=True).start()
    return {"message": "Refresh triggered", "timestamp": datetime.now().isoformat()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
