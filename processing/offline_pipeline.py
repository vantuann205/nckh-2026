"""Offline data pipeline for one-time preprocessing and persisted dataset output."""

from __future__ import annotations

import glob
import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pandas as pd


logger = logging.getLogger("OfflinePipeline")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
PROCESSED_DIR = DATA_DIR / "processed"
PROCESSED_PATH = PROCESSED_DIR / "unified_traffic.parquet"
MAX_VEHICLE_ROWS = int(os.getenv("MAX_VEHICLE_ROWS", "0"))
USE_DEMO_DATASET = os.getenv("USE_DEMO_DATASET", "0") == "1"
STRICT_TARGET_VALIDATION = os.getenv("STRICT_TARGET_VALIDATION", "1") == "1"
STRICT_RAW_VALIDATION = os.getenv("STRICT_RAW_VALIDATION", "1") == "1"
TRAIN_TRAFFIC_FILES = [
    name.strip()
    for name in os.getenv("TRAIN_TRAFFIC_FILES", "traffic_data_0.json,traffic_data_1.json").split(",")
    if name.strip()
]
TRAIN_FILES_FALLBACK_ALL = os.getenv("TRAIN_FILES_FALLBACK_ALL", "1") == "1"


@dataclass
class DatasetBundle:
    vehicle_df: pd.DataFrame
    weather_df: pd.DataFrame
    accident_df: pd.DataFrame


def _to_ascii_lower(value: str) -> str:
    if value is None:
        return ""
    text = str(value).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace("duong", "").replace("quan", "")
    text = " ".join(text.split())
    return text


def _location_key(road_name: str, district: str) -> str:
    return f"{_to_ascii_lower(district)}:{_to_ascii_lower(road_name)}"


def _fahrenheit_to_celsius(value: float) -> float:
    return round((float(value) - 32.0) * 5.0 / 9.0, 2)


def _load_json_array(file_path: Path) -> List[dict]:
    with file_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    return payload if isinstance(payload, list) else []


def _infer_alert_type(alert_type: str, description: str) -> str:
    raw_type = str(alert_type or "").strip().lower()
    desc = str(description or "").strip().lower()

    if re.search(r"fuel|nhien lieu|xang", desc):
        return "LowFuel"
    if re.search(r"speed|vuot toc do|over the speed", desc):
        return "Speeding"

    if raw_type in {"speeding", "overspeed"}:
        return "Speeding"
    if raw_type in {"lowfuel", "low_fuel", "fuel"}:
        return "LowFuel"
    if raw_type:
        return raw_type.title()
    return "Unknown"


def _normalize_alerts(value: object) -> List[dict]:
    if not isinstance(value, list):
        return []

    normalized: List[dict] = []
    for item in value:
        if not isinstance(item, dict):
            continue
        inferred = _infer_alert_type(item.get("type"), item.get("description"))
        normalized.append(
            {
                "type": inferred,
                "description": str(item.get("description") or "").strip(),
                "severity": str(item.get("severity") or "Unknown").strip(),
                "timestamp": str(item.get("timestamp") or "").strip(),
            }
        )

    return normalized


def _raw_record_consistency_flags(df: pd.DataFrame) -> pd.Series:
    speed = pd.to_numeric(df.get("speed_kmph"), errors="coerce")
    eta_ts = pd.to_datetime(df.get("eta_raw"), utc=True, errors="coerce")
    event_ts = pd.to_datetime(df.get("event_time"), utc=True, errors="coerce")
    delay = pd.to_numeric(df.get("estimated_delay_minutes"), errors="coerce")

    raw_level = df.get("congestion_level_raw", pd.Series([""] * len(df), index=df.index)).astype(str).str.strip()
    expected = pd.Series("Low", index=df.index)
    expected[(speed < 40) & (speed >= 20)] = "Moderate"
    expected[speed < 20] = "High"

    issues = pd.Series(False, index=df.index)
    issues = issues | speed.isna() | (speed < 0) | (speed > 180)
    issues = issues | event_ts.isna()
    issues = issues | (~eta_ts.isna() & (eta_ts < event_ts))
    issues = issues | (~delay.isna() & (delay < 0))
    issues = issues | ((raw_level != "") & (raw_level != expected))
    return issues


def load_vehicle_dataset() -> pd.DataFrame:
    demo_file = DATA_DIR / "traffic_data_demo.json"
    if USE_DEMO_DATASET and demo_file.exists():
        files = [str(demo_file)]
    else:
        selected = []
        for file_name in TRAIN_TRAFFIC_FILES:
            file_path = DATA_DIR / file_name
            if file_path.exists():
                selected.append(str(file_path))

        if selected:
            files = sorted(selected)
        elif TRAIN_FILES_FALLBACK_ALL:
            logger.warning(
                "No configured TRAIN_TRAFFIC_FILES found. Falling back to all traffic_data_*.json"
            )
            files = [
                path for path in sorted(glob.glob(str(DATA_DIR / "traffic_data_*.json")))
                if not path.endswith("traffic_data_demo.json")
            ]
        else:
            raise FileNotFoundError(
                "Configured TRAIN_TRAFFIC_FILES were not found and fallback is disabled"
            )

    logger.info("Training dataset files: %s", [Path(path).name for path in files])
    all_records: List[dict] = []
    for file_path in files:
        all_records.extend(_load_json_array(Path(file_path)))

    if not all_records:
        return pd.DataFrame()

    vehicle_df = pd.json_normalize(all_records)
    if MAX_VEHICLE_ROWS > 0 and len(vehicle_df) > MAX_VEHICLE_ROWS:
        vehicle_df = vehicle_df.head(MAX_VEHICLE_ROWS).copy()
    vehicle_df = vehicle_df.rename(
        columns={
            "road.street": "road_name",
            "road.district": "district",
            "road.city": "city",
            "coordinates.latitude": "lat",
            "coordinates.longitude": "lng",
            "weather_condition.condition": "weather_condition_inline",
            "weather_condition.temperature_celsius": "weather_temp_inline",
            "weather_condition.humidity_percentage": "weather_humidity_inline",
            "estimated_time_of_arrival.eta": "eta_raw",
            "traffic_status.congestion_level": "congestion_level_raw",
            "traffic_status.estimated_delay_minutes": "estimated_delay_minutes",
        }
    )

    vehicle_df["event_time"] = pd.to_datetime(vehicle_df.get("timestamp"), utc=True, errors="coerce")
    vehicle_df["road_name"] = vehicle_df.get("road_name", "").fillna("")
    vehicle_df["district"] = vehicle_df.get("district", "").fillna("")
    vehicle_df["speed_kmph"] = pd.to_numeric(vehicle_df.get("speed_kmph"), errors="coerce")
    vehicle_df["lat"] = pd.to_numeric(vehicle_df.get("lat"), errors="coerce")
    vehicle_df["lng"] = pd.to_numeric(vehicle_df.get("lng"), errors="coerce")

    vehicle_df = vehicle_df.dropna(subset=["event_time", "speed_kmph", "lat", "lng"])
    vehicle_df = vehicle_df[(vehicle_df["speed_kmph"] >= 0) & (vehicle_df["speed_kmph"] <= 180)]
    vehicle_df = vehicle_df[(vehicle_df["lat"].between(-90, 90)) & (vehicle_df["lng"].between(-180, 180))]

    vehicle_df["district_norm"] = vehicle_df["district"].map(_to_ascii_lower)
    vehicle_df["road_norm"] = vehicle_df["road_name"].map(_to_ascii_lower)
    vehicle_df["location_key"] = vehicle_df.apply(
        lambda row: _location_key(row.get("road_name", ""), row.get("district", "")), axis=1
    )

    # Normalize alert semantics to avoid contradictory labels in raw demo data.
    vehicle_df["alerts_norm"] = vehicle_df.get("alerts", pd.Series([[]] * len(vehicle_df))).apply(_normalize_alerts)

    issue_flags = _raw_record_consistency_flags(vehicle_df)
    issue_count = int(issue_flags.sum())
    if issue_count > 0:
        msg = f"Raw record consistency issues: {issue_count}/{len(vehicle_df)}"
        if STRICT_RAW_VALIDATION:
            raise ValueError(msg)
        logger.warning(msg)
        vehicle_df = vehicle_df.loc[~issue_flags].copy()

    return vehicle_df


def load_weather_dataset() -> pd.DataFrame:
    file_path = DATA_DIR / "retrievebulkdataset.json"
    with file_path.open("r", encoding="utf-8") as f:
        payload = json.load(f)

    rows: List[dict] = []
    days = payload.get("days", [])
    for day in days:
        day_value = day.get("datetime")
        for hour in day.get("hours", []):
            hour_value = hour.get("datetime")
            if not day_value or not hour_value:
                continue
            rows.append(
                {
                    "event_time": f"{day_value}T{hour_value}+07:00",
                    "weather_condition": hour.get("conditions") or day.get("conditions") or "Unknown",
                    "weather_temp_c": _fahrenheit_to_celsius(hour.get("temp", day.get("temp", 77.0))),
                    "humidity_pct": float(hour.get("humidity", day.get("humidity", 0.0) or 0.0)),
                    "wind_kmph": float(hour.get("windspeed", day.get("windspeed", 0.0) or 0.0)),
                    "city": payload.get("resolvedAddress", "TP Ho Chi Minh"),
                }
            )

    weather_df = pd.DataFrame(rows)
    if weather_df.empty:
        return weather_df

    weather_df["event_time"] = pd.to_datetime(weather_df["event_time"], utc=True, errors="coerce")
    weather_df = weather_df.dropna(subset=["event_time"])
    weather_df = weather_df.sort_values("event_time")
    return weather_df


def load_accident_dataset() -> pd.DataFrame:
    file_path = DATA_DIR / "traffic_accidents.json"
    accidents = _load_json_array(file_path)
    if not accidents:
        return pd.DataFrame()

    accident_df = pd.DataFrame(accidents)
    accident_df["event_time"] = pd.to_datetime(accident_df.get("accident_time"), utc=True, errors="coerce")
    accident_df["district"] = accident_df.get("district", "").fillna("")
    accident_df["road_name"] = accident_df.get("road_name", "").fillna("")
    accident_df["accident_severity"] = pd.to_numeric(accident_df.get("accident_severity"), errors="coerce").fillna(0)
    accident_df["congestion_km"] = pd.to_numeric(accident_df.get("congestion_km"), errors="coerce").fillna(0.0)
    accident_df["district_norm"] = accident_df["district"].map(_to_ascii_lower)
    accident_df["road_norm"] = accident_df["road_name"].map(_to_ascii_lower)
    accident_df["location_key"] = accident_df.apply(
        lambda row: _location_key(row.get("road_name", ""), row.get("district", "")), axis=1
    )
    accident_df = accident_df.dropna(subset=["event_time"])
    return accident_df


def load_datasets() -> DatasetBundle:
    return DatasetBundle(
        vehicle_df=load_vehicle_dataset(),
        weather_df=load_weather_dataset(),
        accident_df=load_accident_dataset(),
    )


def integrate_datasets(bundle: DatasetBundle) -> pd.DataFrame:
    if bundle.vehicle_df.empty:
        return pd.DataFrame()

    vehicle_df = bundle.vehicle_df.sort_values("event_time").copy()

    if not bundle.weather_df.empty:
        weather_df = bundle.weather_df.sort_values("event_time")
        vehicle_df = pd.merge_asof(
            vehicle_df,
            weather_df,
            on="event_time",
            direction="nearest",
            tolerance=pd.Timedelta(minutes=5),
            suffixes=("", "_weather"),
        )

    accidents = bundle.accident_df.sort_values("event_time") if not bundle.accident_df.empty else pd.DataFrame()
    if not accidents.empty:
        exact = pd.merge_asof(
            vehicle_df.sort_values("event_time"),
            accidents[["location_key", "event_time", "accident_severity", "congestion_km", "number_of_vehicles", "description"]]
            .sort_values("event_time"),
            on="event_time",
            by="location_key",
            direction="nearest",
            tolerance=pd.Timedelta(minutes=5),
            suffixes=("", "_acc"),
        )
        vehicle_df = exact
        vehicle_df["match_level"] = vehicle_df["accident_severity"].apply(
            lambda value: "exact" if pd.notna(value) and float(value) > 0 else "none"
        )
    else:
        vehicle_df["accident_severity"] = 0
        vehicle_df["congestion_km"] = 0.0
        vehicle_df["number_of_vehicles"] = 0
        vehicle_df["description"] = ""
        vehicle_df["match_level"] = "none"

    return vehicle_df


def precompute_features(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    out = df.copy()
    out["weather_temp_c"] = pd.to_numeric(out.get("weather_temp_c"), errors="coerce").fillna(
        pd.to_numeric(out.get("weather_temp_inline"), errors="coerce")
    )
    out["humidity_pct"] = pd.to_numeric(out.get("humidity_pct"), errors="coerce").fillna(
        pd.to_numeric(out.get("weather_humidity_inline"), errors="coerce")
    )
    out["weather_condition"] = out.get("weather_condition", "").fillna(out.get("weather_condition_inline", "Unknown"))

    out["accident_severity"] = pd.to_numeric(out.get("accident_severity"), errors="coerce").fillna(0)
    out["congestion_km"] = pd.to_numeric(out.get("congestion_km"), errors="coerce").fillna(0.0)
    out["estimated_delay_minutes"] = pd.to_numeric(out.get("estimated_delay_minutes"), errors="coerce").fillna(0)

    # --- Canonical target 1: congestion_level (Low / Moderate / High) ---
    out["congestion_level"] = "Low"
    out.loc[(out["speed_kmph"] < 40) & (out["speed_kmph"] >= 20), "congestion_level"] = "Moderate"
    out.loc[out["speed_kmph"] < 20, "congestion_level"] = "High"

    out["congestion_flag"] = (out["congestion_level"] == "High").astype(int)
    weather_risk = out["weather_condition"].astype(str).str.lower().str.contains("rain|storm|thunder").astype(int) * 20
    weather_risk = weather_risk + (out["humidity_pct"].fillna(0) > 85).astype(int) * 10
    accident_risk = out["accident_severity"] * 12 + out["congestion_km"] * 3
    speed_risk = (50 - out["speed_kmph"]).clip(lower=0)
    out["risk_score"] = (speed_risk * 0.6 + weather_risk * 0.2 + accident_risk * 0.2).clip(0, 100).round(2)

    # --- Canonical target 2: estimated_delay_minutes (regression target) ---
    base_delay = ((40.0 - out["speed_kmph"]).clip(lower=0) / 40.0) * 15.0
    incident_delay = out["accident_severity"] * 1.5 + out["congestion_km"] * 1.2
    out["estimated_delay_minutes"] = (base_delay + incident_delay).clip(lower=0, upper=120).round(1)

    out["status"] = "normal"
    out.loc[(out["speed_kmph"] < 40) & (out["speed_kmph"] >= 20), "status"] = "slow"
    out.loc[out["speed_kmph"] < 20, "status"] = "congested"

    out["event_time"] = pd.to_datetime(out["event_time"], utc=True, errors="coerce")

    # --- Canonical target 3: travel_time_minutes / eta_minutes (regression target) ---
    eta_ts = pd.to_datetime(out.get("eta_raw"), utc=True, errors="coerce")
    travel_minutes = ((eta_ts - out["event_time"]).dt.total_seconds() / 60.0).round(1)
    travel_minutes = travel_minutes.where(travel_minutes >= 0)

    # Fallback for missing ETA: baseline trip + predicted delay
    baseline_trip = 12.0 + (out["congestion_km"].fillna(0) * 1.6)
    out["travel_time_minutes"] = travel_minutes.fillna((baseline_trip + out["estimated_delay_minutes"]).round(1))
    out["travel_time_minutes"] = out["travel_time_minutes"].clip(lower=1.0, upper=240.0)
    out["eta_minutes"] = out["travel_time_minutes"]

    out = out.dropna(subset=["event_time"])
    out = out.sort_values("event_time")

    out = _validate_and_enforce_target_consistency(out, strict=STRICT_TARGET_VALIDATION)
    return out


def _validate_and_enforce_target_consistency(df: pd.DataFrame, strict: bool = True) -> pd.DataFrame:
    """Validate that forecast targets are internally consistent.

    Rules:
    - congestion_level must match speed thresholds exactly.
    - estimated_delay_minutes must be >= 0.
    - travel_time_minutes / eta_minutes must be >= estimated_delay_minutes.
    """
    if df.empty:
        return df

    out = df.copy()

    expected_level = pd.Series("Low", index=out.index)
    expected_level[(out["speed_kmph"] < 40) & (out["speed_kmph"] >= 20)] = "Moderate"
    expected_level[out["speed_kmph"] < 20] = "High"
    out["congestion_level"] = expected_level

    out["estimated_delay_minutes"] = pd.to_numeric(
        out.get("estimated_delay_minutes"), errors="coerce"
    ).fillna(0).clip(lower=0).round(1)

    out["travel_time_minutes"] = pd.to_numeric(
        out.get("travel_time_minutes"), errors="coerce"
    ).fillna(out["estimated_delay_minutes"] + 12.0)
    out["travel_time_minutes"] = out["travel_time_minutes"].clip(lower=1.0, upper=240.0)

    out["eta_minutes"] = pd.to_numeric(out.get("eta_minutes"), errors="coerce").fillna(out["travel_time_minutes"])
    out["eta_minutes"] = out["eta_minutes"].clip(lower=1.0, upper=240.0)

    # Enforce ETA/travel-time always >= delay
    min_travel = out["estimated_delay_minutes"] + 1.0
    out["travel_time_minutes"] = pd.concat(
        [out["travel_time_minutes"], min_travel], axis=1
    ).max(axis=1).round(1)
    out["eta_minutes"] = pd.concat(
        [out["eta_minutes"], out["travel_time_minutes"]], axis=1
    ).max(axis=1).round(1)

    inconsistent = (
        (out["congestion_level"] != expected_level)
        | (out["estimated_delay_minutes"] < 0)
        | (out["travel_time_minutes"] < out["estimated_delay_minutes"])
        | (out["eta_minutes"] < out["estimated_delay_minutes"])
    )

    inconsistent_count = int(inconsistent.sum())
    if inconsistent_count > 0:
        msg = f"Target consistency violations: {inconsistent_count}/{len(out)}"
        if strict:
            raise ValueError(msg)
        logger.warning(msg)

    return out


def build_processed_dataset(output_path: Path = PROCESSED_PATH) -> pd.DataFrame:
    bundle = load_datasets()
    merged_df = integrate_datasets(bundle)
    processed_df = precompute_features(merged_df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    processed_df.to_parquet(output_path, index=False)
    logger.info("Processed dataset saved: %s (%d rows)", output_path, len(processed_df))
    return processed_df


def ensure_processed_dataset(force_rebuild: bool = False, output_path: Path = PROCESSED_PATH) -> pd.DataFrame:
    logger.info("ensure_processed_dataset called: force_rebuild=%s, output_path exists=%s", force_rebuild, output_path.exists())
    if output_path.exists() and not force_rebuild:
        logger.info("Loading processed dataset from %s", output_path)
        df = pd.read_parquet(output_path)
        required_targets = [
            "congestion_level",
            "estimated_delay_minutes",
            "travel_time_minutes",
            "eta_minutes",
        ]
        missing = [col for col in required_targets if col not in df.columns]
        if missing:
            logger.info("Upgrading processed dataset with missing targets: %s", missing)
            df = precompute_features(df)
            df.to_parquet(output_path, index=False)
            logger.info("Processed dataset upgraded and saved: %s", output_path)
        return df

    logger.info("Processed dataset not found or force rebuild requested; rebuilding")
    return build_processed_dataset(output_path=output_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    force = os.getenv("FORCE_REBUILD_PROCESSED", "0") == "1"
    df = ensure_processed_dataset(force_rebuild=force)
    print(f"Rows: {len(df)}")
