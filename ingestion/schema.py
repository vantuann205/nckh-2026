"""
Traffic Event Schema & Validation
Chuẩn hóa format: road_id, speed, vehicle_count, lat, lng, timestamp
"""

import json
import random
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator


class TrafficEvent(BaseModel):
    """Schema chuẩn cho traffic event gửi vào Kafka"""
    road_id: str = Field(..., min_length=1, max_length=50, description="ID đoạn đường")
    speed: float = Field(..., ge=0, le=200, description="Tốc độ trung bình (km/h)")
    vehicle_count: int = Field(..., ge=0, le=10000, description="Số lượng xe")
    lat: float = Field(..., ge=-90, le=90, description="Vĩ độ")
    lng: float = Field(..., ge=-180, le=180, description="Kinh độ")
    timestamp: str = Field(..., description="ISO 8601 timestamp")

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """Validate timestamp format"""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            raise ValueError(f"Invalid timestamp format: {v}. Expected ISO 8601.")
        return v

    def to_json(self) -> str:
        return self.model_dump_json()

    def to_dict(self) -> dict:
        return self.model_dump()

    @classmethod
    def from_json(cls, raw: str) -> "TrafficEvent":
        """Parse JSON string → TrafficEvent (validate)"""
        return cls.model_validate_json(raw)


def validate_event(data: dict) -> Optional[TrafficEvent]:
    """Validate dict → TrafficEvent. Return None if invalid."""
    try:
        return TrafficEvent(**data)
    except Exception:
        return None


# === Converter: Old nested JSON → New flat schema ===

# Mapping old district → road_id
DISTRICT_ROAD_MAP = {
    "Quận 1": ["road_q1_01", "road_q1_02", "road_q1_03"],
    "Quận 3": ["road_q3_01", "road_q3_02"],
    "Quận 5": ["road_q5_01", "road_q5_02"],
    "Quận 7": ["road_q7_01", "road_q7_02"],
    "Quận 10": ["road_q10_01", "road_q10_02"],
    "Bình Thạnh": ["road_bt_01", "road_bt_02"],
    "Gò Vấp": ["road_gv_01", "road_gv_02"],
    "Thủ Đức": ["road_td_01", "road_td_02"],
    "Tân Bình": ["road_tb_01", "road_tb_02"],
    "Tân Phú": ["road_tp_01", "road_tp_02"],
}

# Street → road segment mapping
STREET_ROAD_MAP = {
    "Lê Lợi": "road_q1_01",
    "Nguyễn Huệ": "road_q1_02",
    "CMT8": "road_q3_01",
    "Võ Văn Kiệt": "road_q5_01",
    "Điện Biên Phủ": "road_bt_01",
    "Nam Kỳ Khởi Nghĩa": "road_q3_02",
}


def convert_old_to_new(old_record: dict) -> Optional[TrafficEvent]:
    """
    Convert old nested JSON format → new flat TrafficEvent.
    Old format: {vehicle_id, speed_kmph, road: {street, district}, coordinates: {latitude, longitude}, timestamp, ...}
    New format: {road_id, speed, vehicle_count, lat, lng, timestamp}
    """
    try:
        # Extract road_id from street or district
        street = old_record.get("road", {}).get("street", "")
        district = old_record.get("road", {}).get("district", "")

        road_id = STREET_ROAD_MAP.get(street)
        if not road_id:
            roads = DISTRICT_ROAD_MAP.get(district, [])
            road_id = roads[0] if roads else f"road_{district.lower().replace(' ', '_')}"

        # Extract coordinates
        coords = old_record.get("coordinates", {})
        lat = coords.get("latitude", 0)
        lng = coords.get("longitude", 0)

        # Speed
        speed = old_record.get("speed_kmph", 0)

        # Vehicle count: old format doesn't have this, estimate from congestion
        congestion = old_record.get("traffic_status", {}).get("congestion_level", "Low")
        if congestion == "High":
            vehicle_count = random.randint(80, 150)
        elif congestion == "Medium":
            vehicle_count = random.randint(30, 79)
        else:
            vehicle_count = random.randint(5, 29)

        # Timestamp
        ts = old_record.get("timestamp", datetime.now(timezone.utc).isoformat())

        return TrafficEvent(
            road_id=road_id,
            speed=speed,
            vehicle_count=vehicle_count,
            lat=lat,
            lng=lng,
            timestamp=ts,
        )
    except Exception:
        return None


# === Synthetic data generator ===

# Ho Chi Minh City road segments with realistic coordinates
ROAD_SEGMENTS = [
    {"road_id": "road_q1_01", "name": "Lê Lợi", "lat": 10.7726, "lng": 106.6981},
    {"road_id": "road_q1_02", "name": "Nguyễn Huệ", "lat": 10.7740, "lng": 106.7030},
    {"road_id": "road_q1_03", "name": "Đồng Khởi", "lat": 10.7769, "lng": 106.7009},
    {"road_id": "road_q3_01", "name": "CMT8", "lat": 10.7866, "lng": 106.6637},
    {"road_id": "road_q3_02", "name": "Nam Kỳ Khởi Nghĩa", "lat": 10.7845, "lng": 106.6921},
    {"road_id": "road_q5_01", "name": "Võ Văn Kiệt", "lat": 10.7514, "lng": 106.6638},
    {"road_id": "road_q5_02", "name": "Trần Hưng Đạo", "lat": 10.7579, "lng": 106.6742},
    {"road_id": "road_q7_01", "name": "Nguyễn Thị Thập", "lat": 10.7397, "lng": 106.7219},
    {"road_id": "road_q7_02", "name": "Nguyễn Hữu Thọ", "lat": 10.7329, "lng": 106.7180},
    {"road_id": "road_q10_01", "name": "3 Tháng 2", "lat": 10.7700, "lng": 106.6685},
    {"road_id": "road_q10_02", "name": "Lý Thường Kiệt", "lat": 10.7748, "lng": 106.6618},
    {"road_id": "road_bt_01", "name": "Điện Biên Phủ", "lat": 10.7994, "lng": 106.7130},
    {"road_id": "road_bt_02", "name": "Xô Viết Nghệ Tĩnh", "lat": 10.8020, "lng": 106.6940},
    {"road_id": "road_gv_01", "name": "Quang Trung", "lat": 10.8340, "lng": 106.6620},
    {"road_id": "road_gv_02", "name": "Nguyễn Oanh", "lat": 10.8393, "lng": 106.6680},
    {"road_id": "road_td_01", "name": "Võ Văn Ngân", "lat": 10.8500, "lng": 106.7590},
    {"road_id": "road_td_02", "name": "Xa Lộ Hà Nội", "lat": 10.8445, "lng": 106.7710},
    {"road_id": "road_tb_01", "name": "Cộng Hòa", "lat": 10.8030, "lng": 106.6520},
    {"road_id": "road_tb_02", "name": "Hoàng Văn Thụ", "lat": 10.7980, "lng": 106.6660},
    {"road_id": "road_tp_01", "name": "Lũy Bán Bích", "lat": 10.7730, "lng": 106.6332},
    {"road_id": "road_tp_02", "name": "Âu Cơ", "lat": 10.7820, "lng": 106.6390},
]


def generate_synthetic_event(road: dict = None) -> TrafficEvent:
    """Generate a single synthetic traffic event"""
    if road is None:
        road = random.choice(ROAD_SEGMENTS)

    # Simulate realistic traffic patterns
    hour = datetime.now().hour

    # Rush hours: 7-9, 17-19 → lower speed, higher vehicle count
    if 7 <= hour <= 9 or 17 <= hour <= 19:
        base_speed = random.uniform(5, 35)
        base_count = random.randint(50, 200)
    elif 22 <= hour or hour <= 5:
        # Night: high speed, low count
        base_speed = random.uniform(40, 80)
        base_count = random.randint(2, 20)
    else:
        # Normal hours
        base_speed = random.uniform(20, 60)
        base_count = random.randint(15, 80)

    # Add some noise to coordinates
    lat = road["lat"] + random.uniform(-0.002, 0.002)
    lng = road["lng"] + random.uniform(-0.002, 0.002)

    return TrafficEvent(
        road_id=road["road_id"],
        speed=round(base_speed, 1),
        vehicle_count=base_count,
        lat=round(lat, 6),
        lng=round(lng, 6),
        timestamp=datetime.now(timezone.utc).isoformat(),
    )


def generate_batch(count: int = 100) -> List[TrafficEvent]:
    """Generate a batch of synthetic traffic events"""
    events = []
    for _ in range(count):
        events.append(generate_synthetic_event())
    return events
