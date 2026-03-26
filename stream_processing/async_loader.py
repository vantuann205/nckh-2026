"""
Ultra-fast loader — target 100k-300k+ records/sec.

Key insight from benchmarks:
  HSET(17 fields) = ~32k rec/s
  SET(json)       = ~128k rec/s
  MSET(json)      = ~312k rec/s  ← use this

Strategy:
  1. orjson: read entire file into RAM (fastest JSON parser)
  2. _normalize_batch: inline field extraction, no function call overhead
  3. MSET: single Redis command for entire batch (no per-key round-trip)
  4. Multi-thread: 4 workers overlap normalize(CPU) with MSET(IO)
  5. Summary via HINCRBY: no read-modify-write
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

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR     = PROJECT_ROOT / "data"
LOADER_STATE_FILE = PROJECT_ROOT / "data_loader_state.json"

BATCH_SIZE    = int(os.getenv("LOADER_BATCH_SIZE",    "20000"))
FLUSH_WORKERS = int(os.getenv("LOADER_FLUSH_WORKERS", "4"))


# ── Progress ───────────────────────────────────────────────────────────────────

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


# ── Persistent State ───────────────────────────────────────────────────────────

def _load_state() -> dict:
    if LOADER_STATE_FILE.exists():
        try:
            return orjson.loads(LOADER_STATE_FILE.read_bytes())
        except Exception:
            pass
    return {"loaded_files": {}}


def _save_state(state: dict):
    try:
        LOADER_STATE_FILE.write_bytes(
            orjson.dumps(state, option=orjson.OPT_INDENT_2))
    except Exception as e:
        logger.warning("Could not save loader state: %s", e)


def _clean_loaded_files_state(loaded_files: Dict[str, dict]) -> Dict[str, dict]:
    cleaned: Dict[str, dict] = {}
    for name, entry in (loaded_files or {}).items():
        if not isinstance(entry, dict):
            continue
        cleaned[name] = {k: v for k, v in entry.items() if k != "_last_save_ts"}
    return cleaned


# ── Normalize ─────────────────────────────────────────────────────────────────

_NOW_ISO = ""

def _refresh_now():
    global _NOW_ISO
    _NOW_ISO = datetime.now(timezone.utc).isoformat()


def _normalize_batch(records: list) -> List[dict]:
    """Inline normalize — extract all useful fields from raw record."""
    _refresh_now()
    now = _NOW_ISO
    out: List[dict] = []
    ap = out.append

    for raw in records:
        try:
            road   = raw.get("road") or {}
            coords = raw.get("coordinates") or {}
            wx     = raw.get("weather_condition") or {}
            ts     = raw.get("traffic_status") or {}
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
            try: fuel  = float(raw.get("fuel_level_percentage") or 0)
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
            try: delay = float(ts.get("estimated_delay_minutes") or raw.get("estimated_delay_minutes") or 0)
            except: delay = 0.0

            wcond      = wx.get("condition") or raw.get("weather_condition") or "Unknown"
            vtype      = raw.get("vehicle_type") or "Unknown"
            cong_level = ts.get("congestion_level") or "Unknown"
            vehicle_id = raw.get("vehicle_id") or ""

            # Alert flags
            alert_types = [a.get("type", "") for a in alerts if isinstance(a, dict)]
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


# ── Redis Flush (MSET) ─────────────────────────────────────────────────────────

def _flush_mset(rows: List[dict], redis_client):
    """
    Write rows using MSET + accumulate global stats via pipeline.
    Stats (vehicle_types, congestion_levels, etc.) counted from ALL records.
    """
    if not rows:
        return

    rc = redis_client.client
    mset_map: dict = {}
    road_ids: List[str] = []

    # Accumulators for this batch
    vtype_counts: Dict[str, int] = {}
    cong_counts: Dict[str, int] = {}
    wx_counts: Dict[str, int] = {}
    speeding = 0
    low_fuel = 0
    total_alerts = 0
    pax_total = 0
    fuel_sum = 0.0
    fuel_count = 0

    for r in rows:
        mset_map[f"road:{r['road_id']}"] = orjson.dumps(r)
        road_ids.append(r["road_id"])

        vt = r.get("vehicle_type") or "Unknown"
        vtype_counts[vt] = vtype_counts.get(vt, 0) + 1

        cl = r.get("congestion_level") or "Unknown"
        cong_counts[cl] = cong_counts.get(cl, 0) + 1

        wx = r.get("weather_condition") or "Unknown"
        wx_counts[wx] = wx_counts.get(wx, 0) + 1

        if r.get("has_speeding_alert"):
            speeding += 1
        if r.get("has_low_fuel_alert"):
            low_fuel += 1
        total_alerts += int(r.get("alert_count") or 0)
        pax_total += int(r.get("passenger_count") or 0)
        fuel = float(r.get("fuel_level") or 0)
        if fuel > 0:
            fuel_sum += fuel
            fuel_count += 1

    # MSET all road keys
    rc.mset(mset_map)

    # Pipeline: track unique roads + increment global counters
    pipe = rc.pipeline(transaction=False)
    if road_ids:
        pipe.sadd("traffic:unique_roads", *road_ids)

    # Cumulative counters for persisted load progress.
    pipe.hincrbyfloat("traffic:summary", "total_vehicles", len(rows))
    pipe.hincrbyfloat("traffic:summary", "total_records_loaded", len(rows))
    pipe.hincrbyfloat("traffic:summary", "speeding_alerts", speeding)
    pipe.hincrbyfloat("traffic:summary", "low_fuel_alerts", low_fuel)
    pipe.hincrbyfloat("traffic:summary", "total_alerts", total_alerts)
    pipe.hincrbyfloat("traffic:summary", "total_passengers", pax_total)
    pipe.hincrbyfloat("traffic:summary", "_fuel_sum", fuel_sum)
    pipe.hincrbyfloat("traffic:summary", "_fuel_count", fuel_count)

    # Increment vehicle_type counters
    for vt, cnt in vtype_counts.items():
        pipe.hincrbyfloat("traffic:stats:vehicle_types", vt, cnt)
    for cl, cnt in cong_counts.items():
        pipe.hincrbyfloat("traffic:stats:congestion_levels", cl, cnt)
    for wx, cnt in wx_counts.items():
        pipe.hincrbyfloat("traffic:stats:weather", wx, cnt)

    pipe.execute()

    # Update road-level summary (avg_speed, total_roads, congested_roads)
    _recompute_road_summary(redis_client)


def _recompute_road_summary(redis_client):
    """Recompute realtime snapshot KPIs from latest state of each road."""
    try:
        rc = redis_client.client
        unique_ids = list(rc.smembers("traffic:unique_roads"))
        if not unique_ids:
            return
        keys = [f"road:{rid}" for rid in unique_ids]
        values = rc.mget(keys)

        total_roads = 0
        speed_sum = 0.0
        vehicle_sum = 0
        congested = 0

        for v in values:
            if not v:
                continue
            try:
                r = orjson.loads(v)
                total_roads += 1
                speed_sum += float(r.get("avg_speed") or 0)
                vehicle_sum += int(r.get("vehicle_count") or 0)
                if r.get("status") == "congested":
                    congested += 1
            except Exception:
                continue

        avg_speed = round(speed_sum / total_roads, 2) if total_roads > 0 else 0
        fuel_sum   = float(rc.hget("traffic:summary", "_fuel_sum") or 0)
        fuel_count = float(rc.hget("traffic:summary", "_fuel_count") or 0)
        avg_fuel   = round(fuel_sum / fuel_count, 1) if fuel_count > 0 else 0

        rc.hset("traffic:summary", "total_roads",     total_roads)
        rc.hset("traffic:summary", "avg_speed",       avg_speed)
        # total_vehicles is cumulative; keep realtime snapshot separate.
        rc.hset("traffic:summary", "current_vehicles_snapshot", vehicle_sum)
        rc.hset("traffic:summary", "congested_roads", congested)
        rc.hset("traffic:summary", "avg_fuel_level",  avg_fuel)
        updated_at = _NOW_ISO or datetime.now(timezone.utc).isoformat()
        rc.hset("traffic:summary", "updated_at",      updated_at)
    except Exception as e:
        logger.warning("Road summary recompute error: %s", e)


def _recompute_summary_from_roads(redis_client):
    """Legacy alias."""
    _recompute_road_summary(redis_client)



# ── get_all_roads patch ────────────────────────────────────────────────────────
# Since we now store JSON strings (not hashes), patch get_all_roads to decode them.

def patch_redis_client(redis_client):
    """Monkey-patch get_all_roads to decode JSON strings instead of hgetall."""
    import redis as _redis

    def _get_all_roads_fast():
        rc = redis_client.client
        keys = [k for k in rc.scan_iter(match="road:*", count=500)
                if ":window" not in k]
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

    redis_client.get_all_roads = _get_all_roads_fast

    def _get_road_data_fast(road_id: str):
        v = redis_client.client.get(f"road:{road_id}")
        if not v:
            return None
        try:
            return orjson.loads(v)
        except Exception:
            return None

    redis_client.get_road_data = _get_road_data_fast
    redis_client.get_location_state = _get_road_data_fast


# ── Core Loader ────────────────────────────────────────────────────────────────

def _get_cache_path(file_path: Path) -> Path:
    """Cache file path — stores normalized records as msgpack for fast reload."""
    return file_path.parent / "processed" / (file_path.stem + ".cache.bin")


def _load_file_single(
    file_path: Path,
    redis_client,
    broadcast_fn: Optional[Callable] = None,
    loaded_files: Optional[Dict[str, dict]] = None,
    state_lock: Optional[threading.Lock] = None,
) -> int:
    """Load one file — tries cache first, falls back to JSON parse."""
    filename  = file_path.name
    cache_path = _get_cache_path(file_path)
    t0 = time.monotonic()
    src_mtime = file_path.stat().st_mtime

    start_offset = 0
    if loaded_files is not None:
        entry = loaded_files.get(filename, {})
        if entry.get("mtime") == src_mtime:
            try:
                start_offset = max(0, int(entry.get("offset") or entry.get("count") or 0))
            except Exception:
                start_offset = 0

    records = None

    # ── Try cache (orjson binary, much smaller than raw JSON) ──
    if cache_path.exists():
        cache_mtime = cache_path.stat().st_mtime
        src_mtime   = file_path.stat().st_mtime
        if cache_mtime >= src_mtime:
            try:
                raw = cache_path.read_bytes()
                records = orjson.loads(raw)
                del raw
                t_read = time.monotonic() - t0
                logger.info("Cache hit %s: %d records in %.2fs (%.0f rec/s)",
                            filename, len(records), t_read, len(records)/t_read if t_read else 0)
            except Exception as e:
                logger.warning("Cache read failed for %s: %s — rebuilding", filename, e)
                records = None

    # ── Parse raw JSON + build cache ──
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

        # Save cache (normalized orjson — much smaller, loads in ~1s)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_bytes(orjson.dumps(records))
            logger.info("Cache saved: %s (%.1f MB)",
                        cache_path.name, cache_path.stat().st_size / 1024 / 1024)
        except Exception as e:
            logger.warning("Cache save failed: %s", e)

    total = len(records)
    if start_offset > total:
        start_offset = 0

    with PROGRESS._lock:
        if filename in PROGRESS.files:
            PROGRESS.files[filename]["total"] = total
            PROGRESS.files[filename]["processed"] = start_offset
        PROGRESS._total_estimate = sum(v["total"] for v in PROGRESS.files.values())

    def _persist_offset(current_offset: int, force: bool = False):
        if loaded_files is None:
            return
        now = time.monotonic()
        last = float(loaded_files.get(filename, {}).get("_last_save_ts") or 0)
        if (not force) and (now - last < 1.0):
            return

        entry = {
            "mtime": src_mtime,
            "offset": int(current_offset),
            "count": int(current_offset),
            "total": int(total),
            "completed": bool(current_offset >= total),
            "_last_save_ts": now,
        }
        if state_lock:
            with state_lock:
                loaded_files[filename] = entry
                _save_state({"loaded_files": _clean_loaded_files_state(loaded_files)})
        else:
            loaded_files[filename] = entry
            _save_state({"loaded_files": _clean_loaded_files_state(loaded_files)})

    if start_offset >= total:
        _persist_offset(total, force=True)
        logger.info("Skip %s: already completed (%d/%d)", filename, start_offset, total)
        return 0

    logger.info("Resume %s from offset %d/%d", filename, start_offset, total)

    loaded = 0
    for i in range(start_offset, total, BATCH_SIZE):
        chunk = records[i: i + BATCH_SIZE]
        # Records from cache are already normalized dicts
        if chunk and isinstance(chunk[0], dict) and "road_id" in chunk[0]:
            norm = chunk
        else:
            norm = _normalize_batch(chunk)
        if norm:
            delta = len(norm)
            _flush_mset(norm, redis_client)
            loaded += delta
            PROGRESS.update(filename, delta)
            _persist_offset(i + delta)
            if broadcast_fn:
                broadcast_fn()

    del records
    _persist_offset(total, force=True)

    elapsed = time.monotonic() - t0
    logger.info("Loaded %s: %d records in %.1fs → %.0f rec/s",
                filename, loaded, elapsed, loaded / elapsed if elapsed else 0)
    return loaded


def _load_file_ultra(
    file_path: Path,
    redis_client,
    broadcast_fn: Optional[Callable] = None,
) -> int:
    filename = file_path.name

    # Reset summary + unique roads tracker
    try:
        redis_client.client.hset("traffic:summary", mapping={
            "total_roads": 0, "total_vehicles": 0, "congested_roads": 0,
            "avg_speed": 0, "updated_at": datetime.now(timezone.utc).isoformat(),
        })
        redis_client.client.delete("traffic:unique_roads")
        redis_client.client.set("traffic:records_loaded", 0)
    except Exception:
        pass

    t0 = time.monotonic()

    # Read with orjson
    try:
        raw_bytes = file_path.read_bytes()
        records   = orjson.loads(raw_bytes)
        del raw_bytes
        if not isinstance(records, list):
            records = []
    except Exception as e:
        logger.error("Failed to read %s: %s", file_path, e)
        return 0

    total = len(records)
    t_read = time.monotonic() - t0
    logger.info("orjson read %d records in %.2fs (%.0f rec/s)",
                total, t_read, total / t_read if t_read else 0)

    # Update progress total
    with PROGRESS._lock:
        if filename in PROGRESS.files:
            PROGRESS.files[filename]["total"] = total
        PROGRESS._total_estimate = sum(v["total"] for v in PROGRESS.files.values())

    loaded = 0
    flush_q: queue.Queue = queue.Queue(maxsize=FLUSH_WORKERS * 2)

    def _worker():
        while True:
            item = flush_q.get()
            if item is None:
                flush_q.task_done()
                break
            rows, delta = item
            try:
                _flush_mset(rows, redis_client)
                PROGRESS.update(filename, delta)
                if broadcast_fn:
                    broadcast_fn()
            except Exception as e:
                logger.error("Flush error: %s", e)
            finally:
                flush_q.task_done()

    workers = [threading.Thread(target=_worker, daemon=True)
               for _ in range(FLUSH_WORKERS)]
    for w in workers:
        w.start()

    # Normalize + enqueue
    for i in range(0, total, BATCH_SIZE):
        chunk      = records[i: i + BATCH_SIZE]
        normalized = _normalize_batch(chunk)
        if normalized:
            flush_q.put((normalized, len(normalized)))
            loaded += len(normalized)

    del records  # free RAM

    for _ in range(FLUSH_WORKERS):
        flush_q.put(None)
    flush_q.join()
    for w in workers:
        w.join()

    elapsed = time.monotonic() - t0
    rate    = loaded / elapsed if elapsed > 0 else 0
    logger.info("Loaded %d records from %s in %.1fs → %.0f rec/s",
                loaded, filename, elapsed, rate)
    return loaded


# ── Public API ─────────────────────────────────────────────────────────────────

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

    state = _load_state()
    loaded_files = state.get("loaded_files", {})
    state_lock = threading.Lock()

    def _state_entry(file_path: Path) -> dict:
        return loaded_files.get(file_path.name, {})

    def _compatible(file_path: Path, entry: dict) -> bool:
        return bool(entry) and entry.get("mtime") == file_path.stat().st_mtime

    def _completed(file_path: Path, entry: dict) -> bool:
        if not _compatible(file_path, entry):
            return False
        total = int(entry.get("total") or entry.get("count") or 0)
        offset = int(entry.get("offset") or entry.get("count") or 0)
        return bool(entry.get("completed")) and total > 0 and offset >= total

    all_files_completed = all(_completed(f, _state_entry(f)) for f in files)
    force_full_reload = False

    # ── Fast path: Redis already has complete persisted data ──
    try:
        rc = redis_client.client
        existing_roads = int(redis_client.client.scard("traffic:unique_roads") or 0)
        if existing_roads > 0 and all_files_completed:
            summary = rc.hgetall("traffic:summary") or {}
            required_summary_keys = {
                "total_passengers",
                "speeding_alerts",
                "low_fuel_alerts",
                "avg_fuel_level",
                "total_records_loaded",
                "total_vehicles",
            }
            has_required_summary = required_summary_keys.issubset(set(summary.keys()))
            has_breakdown_hashes = (
                int(rc.hlen("traffic:stats:vehicle_types") or 0) > 0 and
                int(rc.hlen("traffic:stats:congestion_levels") or 0) > 0 and
                int(rc.hlen("traffic:stats:weather") or 0) > 0
            )
            unique_ids = list(rc.smembers("traffic:unique_roads"))
            sample_ids = unique_ids[:50]
            live_key_count = 0
            if sample_ids:
                sample_values = rc.mget([f"road:{rid}" for rid in sample_ids])
                live_key_count = sum(1 for v in sample_values if v)
            has_live_snapshot = live_key_count > 0

            if has_required_summary and has_breakdown_hashes and has_live_snapshot:
                logger.info("Redis already has %d roads with complete persisted dataset — skipping reload", existing_roads)
                total_loaded = sum(v.get("count", 0) for v in state.get("loaded_files", {}).values())
                if total_loaded > 0:
                    rc.hset("traffic:summary", "total_records_loaded", total_loaded)
                    rc.hset("traffic:summary", "total_vehicles", total_loaded)
                _recompute_road_summary(redis_client)
                PROGRESS.status = "completed"
                PROGRESS.total_vehicles = total_loaded or existing_roads
                if broadcast_fn:
                    broadcast_fn()
                return

            logger.info(
                "Redis has %d roads but snapshot/stats are missing-stale (live keys=%d) — forcing full reload",
                existing_roads, live_key_count,
            )
            force_full_reload = True
    except Exception:
        pass

    if force_full_reload:
        loaded_files = {}
        _save_state({"loaded_files": loaded_files})
        try:
            redis_client.client.delete("traffic:unique_roads")
            redis_client.client.delete("traffic:summary")
            redis_client.client.delete("traffic:stats:vehicle_types")
            redis_client.client.delete("traffic:stats:congestion_levels")
            redis_client.client.delete("traffic:stats:weather")
            for key in redis_client.client.scan_iter(match="road:*"):
                redis_client.client.delete(key)
        except Exception:
            pass

    # If Redis is empty after reboot but checkpoint says partially loaded, restart from 0
    # to avoid missing historical records in cumulative counters.
    try:
        if int(redis_client.client.scard("traffic:unique_roads") or 0) == 0:
            has_checkpoint = any(int((_state_entry(f).get("offset") or _state_entry(f).get("count") or 0)) > 0 for f in files)
            if has_checkpoint:
                logger.warning("Redis snapshot is empty while checkpoints exist; restarting full load from 0")
                loaded_files = {}
                _save_state({"loaded_files": loaded_files})
    except Exception:
        pass

    files_needing_cache_rebuild = [
        f for f in files
        if loaded_files.get(f.name, {}).get("mtime") != f.stat().st_mtime
    ]
    if files_needing_cache_rebuild:
        logger.info("Files changed (will rebuild cache): %s",
                    [f.name for f in files_needing_cache_rebuild])

    # Reset summary only on fresh start; resume mode keeps previously loaded counters.
    fresh_start = not any(_compatible(f, _state_entry(f)) for f in files)
    if fresh_start:
        try:
            redis_client.client.hset("traffic:summary", mapping={
                "total_roads": 0, "total_vehicles": 0, "congested_roads": 0,
                "total_records_loaded": 0,
                "avg_speed": 0, "total_passengers": 0, "total_alerts": 0,
                "speeding_alerts": 0, "low_fuel_alerts": 0, "avg_fuel_level": 0,
                "current_vehicles_snapshot": 0,
                "_fuel_sum": 0, "_fuel_count": 0,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            })
            redis_client.client.delete("traffic:unique_roads")
            redis_client.client.delete("traffic:stats:vehicle_types")
            redis_client.client.delete("traffic:stats:congestion_levels")
            redis_client.client.delete("traffic:stats:weather")
        except Exception:
            pass

    file_totals = {f.name: max(1000, int(f.stat().st_size / (1024 * 1024) * 500))
                   for f in files}
    PROGRESS.start(file_totals)

    already_loaded = 0
    for f in files:
        entry = _state_entry(f)
        if _compatible(f, entry):
            offset = int(entry.get("offset") or entry.get("count") or 0)
            if f.name in PROGRESS.files:
                PROGRESS.files[f.name]["processed"] = min(offset, PROGRESS.files[f.name]["total"])
            already_loaded += max(0, offset)
    with PROGRESS._lock:
        PROGRESS.total_vehicles = already_loaded

    logger.info("Loading %d file(s) into Redis (parallel): %s",
                len(files), [f.name for f in files])

    t_global = time.monotonic()
    results: Dict[str, int] = {}
    errors: List[str] = []

    def _load_one(f: Path):
        try:
            count = _load_file_single(
                f,
                redis_client,
                broadcast_fn,
                loaded_files=loaded_files,
                state_lock=state_lock,
            )
            results[f.name] = count
        except Exception as e:
            errors.append(f"{f.name}: {e}")
            logger.error("Error loading %s: %s", f.name, e)

    threads = [threading.Thread(target=_load_one, args=(f,), daemon=True) for f in files]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    if errors:
        PROGRESS.fail("; ".join(errors))
        return

    _save_state({"loaded_files": _clean_loaded_files_state(loaded_files)})

    elapsed = time.monotonic() - t_global
    total_loaded = sum(results.values())
    logger.info("All done: %d records in %.1fs → %.0f rec/s",
                total_loaded, elapsed, total_loaded / elapsed if elapsed else 0)

    PROGRESS.complete()
    if broadcast_fn:
        broadcast_fn()

    # Force Redis RDB snapshot so data persists across machine restarts
    try:
        redis_client.client.bgsave()
        logger.info("Redis BGSAVE triggered — data will persist across restarts")
    except Exception:
        pass


def load_single_file(file_path: Path, redis_client,
                     broadcast_fn: Optional[Callable] = None):
    patch_redis_client(redis_client)
    state        = _load_state()
    loaded_files = state.get("loaded_files", {})
    state_lock   = threading.Lock()
    filename     = file_path.name
    size_est     = max(1000, int(file_path.stat().st_size / (1024 * 1024) * 500))

    logger.info("File watcher: loading %s (~%d records)", filename, size_est)
    PROGRESS.start({filename: size_est})

    try:
        # _load_file_single uses cache — fast on 2nd+ load
        _load_file_single(
            file_path,
            redis_client,
            broadcast_fn,
            loaded_files=loaded_files,
            state_lock=state_lock,
        )
        PROGRESS.complete()
    except Exception as e:
        logger.error("File watcher load error: %s", e)
        PROGRESS.fail(str(e))

    if broadcast_fn:
        broadcast_fn()
