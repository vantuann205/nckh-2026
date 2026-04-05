"""
Ultra-fast data loader.
- orjson read + MSET pipeline → 200k+ rec/s
- In-memory accumulation → flush stats ONCE per file (not per batch)
- Parallel workers for Redis IO
- Cache .bin files for fast restart
"""
from __future__ import annotations

import glob
import logging
import os
import queue
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional

import orjson

logger = logging.getLogger("AsyncLoader")

PROJECT_ROOT      = Path(__file__).resolve().parent.parent
DATA_DIR          = PROJECT_ROOT / "data"
LOADER_STATE_FILE = PROJECT_ROOT / "data_loader_state.json"

BATCH_SIZE    = int(os.getenv("LOADER_BATCH_SIZE",    "20000"))
FLUSH_WORKERS = int(os.getenv("LOADER_FLUSH_WORKERS", "6"))


# ── Progress ──────────────────────────────────────────────────────────────────

class LoaderProgress:
    def __init__(self):
        self._lock = threading.Lock()
        self.status: str = "idle"
        self.total_vehicles: int = 0
        self.vehicles_per_sec: float = 0.0
        self.estimated_remaining_sec: float = 0.0
        self.files: Dict[str, dict] = {}
        self.error: str = ""
        self._last_count: int = 0
        self._last_ts: float = 0.0
        self._total_estimate: int = 0

    def start(self, file_totals: Dict[str, int]):
        with self._lock:
            self.status = "loading"
            self.total_vehicles = 0
            self.vehicles_per_sec = 0.0
            self.estimated_remaining_sec = 0.0
            self.files = {f: {"total": t, "processed": 0} for f, t in file_totals.items()}
            self.error = ""
            self._last_count = 0
            self._last_ts = time.monotonic()
            self._total_estimate = sum(file_totals.values())

    def update(self, filename: str, delta: int):
        with self._lock:
            if filename in self.files:
                self.files[filename]["processed"] += delta
            self.total_vehicles += delta
            now = time.monotonic()
            elapsed = now - self._last_ts
            if elapsed >= 0.25:
                self.vehicles_per_sec = round(
                    (self.total_vehicles - self._last_count) / elapsed, 0)
                self._last_count = self.total_vehicles
                self._last_ts = now
                remaining = self._total_estimate - self.total_vehicles
                if self.vehicles_per_sec > 0:
                    self.estimated_remaining_sec = round(
                        remaining / self.vehicles_per_sec, 1)

    def complete(self):
        with self._lock:
            self.status = "completed"
            self.vehicles_per_sec = 0.0
            self.estimated_remaining_sec = 0.0

    def fail(self, error: str):
        with self._lock:
            self.status = "error"
            self.error = error

    def to_dict(self) -> dict:
        with self._lock:
            return {
                "status": self.status,
                "total_vehicles": self.total_vehicles,
                "vehicles_per_sec": int(self.vehicles_per_sec),
                "estimated_remaining_sec": self.estimated_remaining_sec,
                "files": {k: dict(v) for k, v in self.files.items()},
                "error": self.error,
            }


PROGRESS = LoaderProgress()


# ── State persistence ─────────────────────────────────────────────────────────

def _load_state() -> dict:
    if LOADER_STATE_FILE.exists():
        try:
            return orjson.loads(LOADER_STATE_FILE.read_bytes())
        except Exception:
            pass
    return {"loaded_files": {}}


def _save_state(state: dict):
    try:
        LOADER_STATE_FILE.write_bytes(orjson.dumps(state, option=orjson.OPT_INDENT_2))
    except Exception as e:
        logger.warning("Could not save loader state: %s", e)


# ── Normalize ─────────────────────────────────────────────────────────────────

_NOW_ISO = ""

def _refresh_now():
    global _NOW_ISO
    _NOW_ISO = datetime.now(timezone.utc).isoformat()


def _normalize_batch(records: list) -> List[dict]:
    _refresh_now()
    now = _NOW_ISO
    out: List[dict] = []
    ap = out.append
    for raw in records:
        try:
            road   = raw.get("road") or {}
            coords = raw.get("coordinates") or {}
            wx     = raw.get("weather_condition") or {}
            ts_obj = raw.get("traffic_status") or {}
            alerts = raw.get("alerts") or []

            road_name = road.get("street")   or raw.get("road_name") or ""
            district  = road.get("district") or raw.get("district")  or ""

            try: lat   = float(coords.get("latitude")  or raw.get("lat")  or 0)
            except: lat = 0.0
            try: lng   = float(coords.get("longitude") or raw.get("lng")  or 0)
            except: lng = 0.0
            try: speed = float(raw.get("speed_kmph") or 0)
            except: speed = 0.0
            try: vc    = int(raw.get("vehicle_count") or raw.get("passenger_count") or 0)
            except: vc = 0
            try: fuel  = float(raw.get("fuel_level_percentage") or raw.get("fuel_level") or 0)
            except: fuel = 0.0
            try: pax   = int(raw.get("passenger_count") or 0)
            except: pax = 0
            try: wtemp = float(wx.get("temperature_celsius") or raw.get("weather_temp_c") or 0)
            except: wtemp = 0.0
            try: hum   = float(wx.get("humidity_percentage") or raw.get("humidity_pct") or 0)
            except: hum = 0.0
            try: asev  = float(raw.get("accident_severity") or 0)
            except: asev = 0.0
            try: ckm   = float(raw.get("congestion_km") or 0)
            except: ckm = 0.0
            try: delay = float(ts_obj.get("estimated_delay_minutes") or raw.get("estimated_delay") or 0)
            except: delay = 0.0

            wcond      = wx.get("condition") or raw.get("weather_condition") or "Unknown"
            vtype      = raw.get("vehicle_type") or "Unknown"
            cong_level = ts_obj.get("congestion_level") or raw.get("congestion_level") or "Low"
            vehicle_id = raw.get("vehicle_id") or ""

            alert_types  = [a.get("type", "") for a in alerts if isinstance(a, dict)]
            has_speeding = "Speeding" in alert_types
            has_low_fuel = "Low Fuel" in alert_types

            loc = f"{district}:{road_name}".lower()
            rid = raw.get("road_id") or loc

            if speed < 20:   status = "congested"
            elif speed < 40: status = "slow"
            else:             status = "normal"

            sr = 50.0 - speed if speed < 50 else 0.0
            wl = str(wcond).lower()
            wr = 20.0 if ("rain" in wl or "storm" in wl) else 0.0
            if hum > 85: wr += 10.0
            risk = sr * 0.6 + wr * 0.2 + (asev * 12.0 + ckm * 3.0) * 0.2
            if risk > 100.0: risk = 100.0

            ap({
                "road_id":            rid,
                "location_key":       loc,
                "road_name":          road_name,
                "district":           district,
                "lat":                lat,
                "lng":                lng,
                "avg_speed":          round(speed, 2),
                "vehicle_count":      vc,
                "vehicle_id":         vehicle_id,
                "vehicle_type":       vtype,
                "fuel_level":         round(fuel, 1),
                "passenger_count":    pax,
                "status":             status,
                "congestion_level":   cong_level,
                "estimated_delay":    round(delay, 1),
                "risk_score":         round(risk, 2),
                "weather_condition":  str(wcond),
                "weather_temp_c":     wtemp,
                "humidity_pct":       hum,
                "accident_severity":  asev,
                "congestion_km":      ckm,
                "has_speeding_alert": has_speeding,
                "has_low_fuel_alert": has_low_fuel,
                "alert_count":        len(alerts),
                "event_time":         raw.get("timestamp") or now,
                "updated_at":         now,
            })
        except Exception:
            continue
    return out


# ── Redis flush (pure MSET, no per-batch stats) ───────────────────────────────

def _flush_mset_only(rows: List[dict], redis_client):
    """Write rows to Redis using MSET + track unique roads. No stats here."""
    if not rows:
        return
    rc = redis_client.client
    mset_map = {f"road:{r['road_id']}": orjson.dumps(r) for r in rows}
    rc.mset(mset_map)
    road_ids = [r["road_id"] for r in rows]
    if road_ids:
        rc.sadd("traffic:unique_roads", *road_ids)


def _flush_stats_once(acc: dict, redis_client):
    """Flush all accumulated stats to Redis in ONE pipeline after file load."""
    rc = redis_client.client
    pipe = rc.pipeline(transaction=False)

    pipe.hincrbyfloat("traffic:summary", "total_vehicles",   acc["count"])
    pipe.hincrbyfloat("traffic:summary", "speeding_alerts",  acc["speeding"])
    pipe.hincrbyfloat("traffic:summary", "low_fuel_alerts",  acc["low_fuel"])
    pipe.hincrbyfloat("traffic:summary", "total_passengers", acc["pax"])
    pipe.hincrbyfloat("traffic:summary", "_fuel_sum",        acc["fuel_sum"])
    pipe.hincrbyfloat("traffic:summary", "_fuel_count",      acc["fuel_cnt"])
    pipe.hincrbyfloat("traffic:summary", "_speed_sum",       acc["speed_sum"])
    pipe.hincrbyfloat("traffic:summary", "_speed_count",     acc["count"])

    for k, v in acc["vtype"].items():    pipe.hincrbyfloat("traffic:stats:vehicle_types",     k, v)
    for k, v in acc["cong"].items():     pipe.hincrbyfloat("traffic:stats:congestion_levels", k, v)
    for k, v in acc["wx"].items():       pipe.hincrbyfloat("traffic:stats:weather",           k, v)
    for k, v in acc["district"].items(): pipe.hincrbyfloat("traffic:stats:districts",         k, v)
    for k, v in acc["street"].items():   pipe.hincrbyfloat("traffic:stats:streets",           k, v)
    for k, v in acc["road_total"].items():   pipe.hincrbyfloat("traffic:stats:total_by_road",            k, v)
    for k, v in acc["road_high"].items():    pipe.hincrbyfloat("traffic:stats:high_congestion_by_road",  k, v)
    for k, v in acc["road_delay"].items():   pipe.hincrbyfloat("traffic:stats:delay_sum_by_road",        k, v)
    for k, v in acc["road_risk"].items():    pipe.hincrbyfloat("traffic:stats:risk_sum_by_road",         k, v)
    for k, v in acc["road_speed_viol"].items(): pipe.hincrbyfloat("traffic:stats:speeding_by_road",      k, v)
    for k, v in acc["vtype_speed"].items():  pipe.hincrbyfloat("traffic:stats:speeding_by_vtype",        k, v)
    for k, v in acc["fuel_dist"].items():    pipe.hincrbyfloat("traffic:stats:fuel_distribution",        k, v)

    pipe.execute()


def _accumulate(acc: dict, rows: List[dict]):
    """Accumulate stats from a batch into in-memory dict (thread-safe via lock)."""
    for r in rows:
        acc["count"]     += 1
        acc["speed_sum"] += float(r.get("avg_speed") or 0)
        acc["pax"]       += int(r.get("passenger_count") or 0)
        fuel = float(r.get("fuel_level") or 0)
        if fuel > 0:
            acc["fuel_sum"] += fuel
            acc["fuel_cnt"] += 1
        if r.get("has_speeding_alert"): acc["speeding"] += 1
        if r.get("has_low_fuel_alert"): acc["low_fuel"] += 1

        vt = r.get("vehicle_type") or "Unknown"
        acc["vtype"][vt] = acc["vtype"].get(vt, 0) + 1
        cl = r.get("congestion_level") or "Low"
        acc["cong"][cl]  = acc["cong"].get(cl, 0) + 1
        wx = r.get("weather_condition") or "Unknown"
        acc["wx"][wx]    = acc["wx"].get(wx, 0) + 1
        dt = r.get("district") or "Unknown"
        acc["district"][dt] = acc["district"].get(dt, 0) + 1
        st = r.get("road_name") or "Unknown"
        acc["street"][st]   = acc["street"].get(st, 0) + 1

        road = r.get("road_name") or r.get("road_id") or ""
        if road:
            acc["road_total"][road]  = acc["road_total"].get(road, 0) + 1
            acc["road_delay"][road]  = acc["road_delay"].get(road, 0.0) + float(r.get("estimated_delay") or 0)
            acc["road_risk"][road]   = acc["road_risk"].get(road, 0.0)  + float(r.get("risk_score") or 0)
            if cl == "High":
                acc["road_high"][road] = acc["road_high"].get(road, 0) + 1
            if r.get("has_speeding_alert"):
                acc["road_speed_viol"][road] = acc["road_speed_viol"].get(road, 0) + 1
                acc["vtype_speed"][vt]       = acc["vtype_speed"].get(vt, 0) + 1

        bucket = ("0-20%" if fuel < 20 else "20-40%" if fuel < 40 else
                  "40-60%" if fuel < 60 else "60-80%" if fuel < 80 else "80-100%")
        acc["fuel_dist"][bucket] = acc["fuel_dist"].get(bucket, 0) + 1


def _new_acc() -> dict:
    return {
        "count": 0, "speed_sum": 0.0, "pax": 0,
        "fuel_sum": 0.0, "fuel_cnt": 0,
        "speeding": 0, "low_fuel": 0,
        "vtype": {}, "cong": {}, "wx": {},
        "district": {}, "street": {},
        "road_total": {}, "road_high": {}, "road_delay": {},
        "road_risk": {}, "road_speed_viol": {}, "vtype_speed": {},
        "fuel_dist": {},
    }


# ── Road summary ──────────────────────────────────────────────────────────────

def _recompute_road_summary(redis_client):
    """Compute avg_speed from _speed_sum/_speed_count, total_roads from unique set."""
    try:
        rc = redis_client.client
        unique_ids = list(rc.smembers("traffic:unique_roads"))
        total_roads = len(unique_ids)
        congested = 0
        if unique_ids:
            vals = rc.mget([f"road:{rid}" for rid in unique_ids])
            for v in vals:
                if v:
                    try:
                        r = orjson.loads(v)
                        if r.get("status") == "congested":
                            congested += 1
                    except Exception:
                        pass

        speed_sum   = float(rc.hget("traffic:summary", "_speed_sum")   or 0)
        speed_count = float(rc.hget("traffic:summary", "_speed_count") or 0)
        avg_speed   = round(speed_sum / speed_count, 2) if speed_count > 0 else 0

        fuel_sum   = float(rc.hget("traffic:summary", "_fuel_sum")   or 0)
        fuel_count = float(rc.hget("traffic:summary", "_fuel_count") or 0)
        avg_fuel   = round(fuel_sum / fuel_count, 1) if fuel_count > 0 else 0

        total_streets   = rc.hlen("traffic:stats:streets")
        total_districts = rc.hlen("traffic:stats:districts")

        rc.hset("traffic:summary", mapping={
            "total_roads":      total_roads,
            "avg_speed":        avg_speed,
            "congested_roads":  congested,
            "avg_fuel_level":   avg_fuel,
            "total_streets":    total_streets,
            "total_districts":  total_districts,
            "updated_at":       _NOW_ISO or datetime.now(timezone.utc).isoformat(),
        })
    except Exception as e:
        logger.warning("Road summary recompute error: %s", e)


# ── Redis client patch ────────────────────────────────────────────────────────

def patch_redis_client(redis_client):
    """Patch get_all_roads to decode JSON strings (MSET format)."""
    def _get_all_roads():
        rc = redis_client.client
        keys = [k for k in rc.scan_iter(match="road:*", count=500) if ":window" not in k]
        if not keys:
            return []
        values = rc.mget(keys)
        roads = []
        for v in values:
            if v:
                try:
                    roads.append(orjson.loads(v))
                except Exception:
                    pass
        return roads

    def _get_road_data(road_id: str):
        v = redis_client.client.get(f"road:{road_id}")
        if not v:
            return None
        try:
            return orjson.loads(v)
        except Exception:
            return None

    redis_client.get_all_roads    = _get_all_roads
    redis_client.get_road_data    = _get_road_data
    redis_client.get_location_state = _get_road_data


# ── Cache path ────────────────────────────────────────────────────────────────

def _get_cache_path(file_path: Path) -> Path:
    return file_path.parent / "processed" / (file_path.stem + ".cache.bin")


# ── Core file loader ──────────────────────────────────────────────────────────

def _load_file(file_path: Path, redis_client, broadcast_fn: Optional[Callable] = None) -> int:
    """
    Load one file into Redis as fast as possible.
    - Try cache first (fast restart)
    - Parallel MSET workers
    - Accumulate all stats in memory, flush ONCE at end
    """
    filename   = file_path.name
    cache_path = _get_cache_path(file_path)
    t0 = time.monotonic()

    # Read records
    records = None
    if cache_path.exists() and cache_path.stat().st_mtime >= file_path.stat().st_mtime:
        try:
            raw = cache_path.read_bytes()
            records = orjson.loads(raw)
            del raw
            logger.info("Cache hit %s: %d records in %.2fs",
                        filename, len(records), time.monotonic() - t0)
        except Exception as e:
            logger.warning("Cache read failed %s: %s", filename, e)
            records = None

    if records is None:
        try:
            raw_bytes = file_path.read_bytes()
            raw_list  = orjson.loads(raw_bytes)
            del raw_bytes
            if not isinstance(raw_list, list):
                raw_list = []
        except Exception as e:
            logger.error("Failed to read %s: %s", file_path, e)
            return 0

        t_read = time.monotonic() - t0
        logger.info("Read %s: %d records in %.2fs (%.0f rec/s)",
                    filename, len(raw_list), t_read, len(raw_list)/t_read if t_read else 0)

        # Normalize all records
        records = []
        for i in range(0, len(raw_list), BATCH_SIZE):
            records.extend(_normalize_batch(raw_list[i: i + BATCH_SIZE]))
        del raw_list

        # Save cache
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(orjson.dumps(records))
            logger.info("Cache saved: %s (%.1f MB)", cache_path.name,
                        cache_path.stat().st_size / 1024 / 1024)
        except Exception as e:
            logger.warning("Cache save failed: %s", e)

    total = len(records)
    with PROGRESS._lock:
        if filename in PROGRESS.files:
            PROGRESS.files[filename]["total"] = total
        PROGRESS._total_estimate = sum(v["total"] for v in PROGRESS.files.values())

    # Parallel MSET workers + in-memory accumulation
    loaded = 0
    acc = _new_acc()
    acc_lock = threading.Lock()
    flush_q: queue.Queue = queue.Queue(maxsize=FLUSH_WORKERS * 2)

    def _worker():
        while True:
            item = flush_q.get()
            if item is None:
                flush_q.task_done()
                break
            rows, delta = item
            try:
                _flush_mset_only(rows, redis_client)
                with acc_lock:
                    _accumulate(acc, rows)
                PROGRESS.update(filename, delta)
                if broadcast_fn:
                    broadcast_fn()
            except Exception as e:
                logger.error("Flush error: %s", e)
            finally:
                flush_q.task_done()

    workers = [threading.Thread(target=_worker, daemon=True) for _ in range(FLUSH_WORKERS)]
    for w in workers: w.start()

    for i in range(0, total, BATCH_SIZE):
        chunk = records[i: i + BATCH_SIZE]
        if chunk:
            flush_q.put((chunk, len(chunk)))
            loaded += len(chunk)

    del records
    for _ in range(FLUSH_WORKERS): flush_q.put(None)
    flush_q.join()
    for w in workers: w.join()

    # Flush all stats ONCE
    _flush_stats_once(acc, redis_client)
    _recompute_road_summary(redis_client)

    elapsed = time.monotonic() - t0
    logger.info("Loaded %s: %d records in %.1fs → %.0f rec/s",
                filename, loaded, elapsed, loaded / elapsed if elapsed else 0)
    return loaded


# ── Public API ────────────────────────────────────────────────────────────────

def get_data_files() -> List[Path]:
    return sorted(
        Path(p) for p in glob.glob(str(DATA_DIR / "traffic_data_*.json"))
        if not p.endswith("traffic_data_demo.json")
    )


def load_all_data(redis_client, broadcast_fn: Optional[Callable] = None):
    patch_redis_client(redis_client)

    files = get_data_files()
    if not files:
        logger.info("No traffic_data_*.json files found")
        PROGRESS.complete()
        return

    state        = _load_state()
    loaded_files = state.get("loaded_files", {})

    # Fast path: Redis already has complete data
    try:
        rc = redis_client.client
        existing = int(rc.scard("traffic:unique_roads") or 0)
        if existing > 0:
            all_done = all(
                loaded_files.get(f.name, {}).get("mtime") == f.stat().st_mtime and
                loaded_files.get(f.name, {}).get("completed")
                for f in files
            )
            if all_done:
                logger.info("Redis has %d roads, all files loaded — fast path", existing)
                total_loaded = sum(v.get("count", 0) for v in loaded_files.values())
                rc.hset("traffic:summary", "total_vehicles", total_loaded)
                _recompute_road_summary(redis_client)
                PROGRESS.status = "completed"
                PROGRESS.total_vehicles = total_loaded
                if broadcast_fn:
                    broadcast_fn()
                return
    except Exception:
        pass

    # Reset stats for fresh load
    try:
        rc = redis_client.client
        rc.hset("traffic:summary", mapping={
            "total_roads": 0, "total_vehicles": 0, "congested_roads": 0,
            "avg_speed": 0, "total_passengers": 0, "speeding_alerts": 0,
            "low_fuel_alerts": 0, "avg_fuel_level": 0,
            "_fuel_sum": 0, "_fuel_count": 0, "_speed_sum": 0, "_speed_count": 0,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        for key in ["traffic:unique_roads",
                    "traffic:stats:vehicle_types", "traffic:stats:congestion_levels",
                    "traffic:stats:weather", "traffic:stats:districts", "traffic:stats:streets",
                    "traffic:stats:total_by_road", "traffic:stats:high_congestion_by_road",
                    "traffic:stats:delay_sum_by_road", "traffic:stats:risk_sum_by_road",
                    "traffic:stats:speeding_by_road", "traffic:stats:speeding_by_vtype",
                    "traffic:stats:fuel_distribution"]:
            rc.delete(key)
    except Exception:
        pass

    file_totals = {f.name: max(1000, int(f.stat().st_size / (1024 * 1024) * 500))
                   for f in files}
    PROGRESS.start(file_totals)
    logger.info("Loading %d file(s) in parallel: %s", len(files), [f.name for f in files])

    t_global = time.monotonic()
    errors: List[str] = []

    def _load_one(f: Path):
        try:
            count = _load_file(f, redis_client, broadcast_fn)
            loaded_files[f.name] = {
                "mtime": f.stat().st_mtime,
                "count": count,
                "completed": True,
            }
            _save_state({"loaded_files": loaded_files})
        except Exception as e:
            errors.append(f"{f.name}: {e}")
            logger.error("Error loading %s: %s", f.name, e)

    threads = [threading.Thread(target=_load_one, args=(f,), daemon=True) for f in files]
    for t in threads: t.start()
    for t in threads: t.join()

    if errors:
        PROGRESS.fail("; ".join(errors))
        return

    elapsed = time.monotonic() - t_global
    total   = PROGRESS.total_vehicles
    logger.info("All done: %d records in %.1fs → %.0f rec/s",
                total, elapsed, total / elapsed if elapsed else 0)

    # Force Redis BGSAVE
    try:
        redis_client.client.bgsave()
    except Exception:
        pass

    PROGRESS.complete()
    if broadcast_fn:
        broadcast_fn()


def load_single_file(file_path: Path, redis_client, broadcast_fn: Optional[Callable] = None):
    """Called by file watcher when new file detected."""
    patch_redis_client(redis_client)
    state        = _load_state()
    loaded_files = state.get("loaded_files", {})
    filename     = file_path.name
    size_est     = max(1000, int(file_path.stat().st_size / (1024 * 1024) * 500))

    logger.info("File watcher: loading %s (~%d records)", filename, size_est)
    PROGRESS.start({filename: size_est})

    try:
        count = _load_file(file_path, redis_client, broadcast_fn)
        loaded_files[filename] = {
            "mtime": file_path.stat().st_mtime,
            "count": count,
            "completed": True,
        }
        _save_state({"loaded_files": loaded_files})
        PROGRESS.complete()
    except Exception as e:
        logger.error("File watcher load error: %s", e)
        PROGRESS.fail(str(e))

    if broadcast_fn:
        broadcast_fn()
