"""Generate a data-quality report for traffic raw JSON files.

Checks:
- speed range and coordinates validity
- ETA >= event_time
- delay non-negative
- congestion_level consistency with speed thresholds
- alert semantic consistency (Speeding vs LowFuel)
"""

from __future__ import annotations

import argparse
import glob
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import pandas as pd


@dataclass
class QualitySummary:
    total_rows: int
    valid_rows: int
    invalid_rows: int
    invalid_pct: float
    checks: Dict[str, int]


def infer_alert_type(alert_type: str, description: str) -> str:
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


def load_records(pattern: str) -> pd.DataFrame:
    files = sorted(glob.glob(pattern))
    all_rows: List[dict] = []
    for file_path in files:
        payload = json.loads(Path(file_path).read_text(encoding="utf-8"))
        if isinstance(payload, list):
            all_rows.extend(payload)

    if not all_rows:
        return pd.DataFrame()

    df = pd.json_normalize(all_rows)
    df = df.rename(
        columns={
            "coordinates.latitude": "lat",
            "coordinates.longitude": "lng",
            "estimated_time_of_arrival.eta": "eta_raw",
            "traffic_status.congestion_level": "congestion_level_raw",
            "traffic_status.estimated_delay_minutes": "estimated_delay_minutes",
        }
    )
    return df


def analyze(df: pd.DataFrame) -> QualitySummary:
    if df.empty:
        return QualitySummary(0, 0, 0, 0.0, {})

    speed = pd.to_numeric(df.get("speed_kmph"), errors="coerce")
    lat = pd.to_numeric(df.get("lat"), errors="coerce")
    lng = pd.to_numeric(df.get("lng"), errors="coerce")
    event_time = pd.to_datetime(df.get("timestamp"), utc=True, errors="coerce")
    eta_time = pd.to_datetime(df.get("eta_raw"), utc=True, errors="coerce")
    delay = pd.to_numeric(df.get("estimated_delay_minutes"), errors="coerce")

    expected_level = pd.Series("Low", index=df.index)
    expected_level[(speed < 40) & (speed >= 20)] = "Moderate"
    expected_level[speed < 20] = "High"
    raw_level = df.get("congestion_level_raw", pd.Series([""] * len(df), index=df.index)).astype(str)

    speed_invalid = speed.isna() | (speed < 0) | (speed > 180)
    coord_invalid = lat.isna() | lng.isna() | ~lat.between(-90, 90) | ~lng.between(-180, 180)
    time_invalid = event_time.isna() | (~eta_time.isna() & (eta_time < event_time))
    delay_invalid = ~delay.isna() & (delay < 0)
    level_invalid = (raw_level != "") & (raw_level != expected_level)

    alert_issue = pd.Series(False, index=df.index)
    alerts_col = df.get("alerts", pd.Series([[]] * len(df), index=df.index))
    for idx, alerts in alerts_col.items():
        if not isinstance(alerts, list) or not alerts:
            continue
        first = alerts[0]
        if not isinstance(first, dict):
            continue
        inferred = infer_alert_type(first.get("type"), first.get("description"))
        raw_type = str(first.get("type") or "").strip()
        if raw_type and inferred != raw_type and inferred != "Unknown":
            alert_issue.loc[idx] = True

    invalid = speed_invalid | coord_invalid | time_invalid | delay_invalid | level_invalid | alert_issue

    checks = {
        "speed_invalid": int(speed_invalid.sum()),
        "coord_invalid": int(coord_invalid.sum()),
        "time_invalid": int(time_invalid.sum()),
        "delay_invalid": int(delay_invalid.sum()),
        "congestion_level_mismatch": int(level_invalid.sum()),
        "alert_semantic_mismatch": int(alert_issue.sum()),
    }

    total = len(df)
    invalid_rows = int(invalid.sum())
    valid_rows = total - invalid_rows
    invalid_pct = round((invalid_rows / total) * 100, 2) if total else 0.0

    return QualitySummary(
        total_rows=total,
        valid_rows=valid_rows,
        invalid_rows=invalid_rows,
        invalid_pct=invalid_pct,
        checks=checks,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Traffic raw data quality report")
    parser.add_argument(
        "--pattern",
        default="data/traffic_data_*.json",
        help="Glob pattern for raw traffic files",
    )
    args = parser.parse_args()

    df = load_records(args.pattern)
    summary = analyze(df)

    print("=== DATA QUALITY REPORT ===")
    print(f"Rows total      : {summary.total_rows}")
    print(f"Rows valid      : {summary.valid_rows}")
    print(f"Rows invalid    : {summary.invalid_rows}")
    print(f"Invalid percent : {summary.invalid_pct}%")
    print("\nBreakdown:")
    for key, value in summary.checks.items():
        print(f"- {key}: {value}")


if __name__ == "__main__":
    main()
