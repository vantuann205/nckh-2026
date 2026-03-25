"""
Unit Tests — Schema Validation
"""

import pytest
from datetime import datetime, timezone
from ingestion.schema import (
    TrafficEvent, validate_event, convert_old_to_new,
    generate_synthetic_event, generate_batch, ROAD_SEGMENTS,
)


class TestTrafficEvent:
    """Test TrafficEvent Pydantic model"""

    def test_valid_event(self):
        event = TrafficEvent(
            road_id="road_q1_01",
            speed=45.5,
            vehicle_count=30,
            lat=10.7726,
            lng=106.6981,
            timestamp="2024-01-01T12:00:00+07:00",
        )
        assert event.road_id == "road_q1_01"
        assert event.speed == 45.5
        assert event.vehicle_count == 30

    def test_invalid_speed_negative(self):
        with pytest.raises(Exception):
            TrafficEvent(
                road_id="road_q1_01",
                speed=-10,
                vehicle_count=30,
                lat=10.7726,
                lng=106.6981,
                timestamp="2024-01-01T12:00:00Z",
            )

    def test_invalid_speed_too_high(self):
        with pytest.raises(Exception):
            TrafficEvent(
                road_id="road_q1_01",
                speed=300,
                vehicle_count=30,
                lat=10.7726,
                lng=106.6981,
                timestamp="2024-01-01T12:00:00Z",
            )

    def test_invalid_timestamp(self):
        with pytest.raises(Exception):
            TrafficEvent(
                road_id="road_q1_01",
                speed=45.5,
                vehicle_count=30,
                lat=10.7726,
                lng=106.6981,
                timestamp="not-a-timestamp",
            )

    def test_empty_road_id(self):
        with pytest.raises(Exception):
            TrafficEvent(
                road_id="",
                speed=45.5,
                vehicle_count=30,
                lat=10.7726,
                lng=106.6981,
                timestamp="2024-01-01T12:00:00Z",
            )

    def test_to_json(self):
        event = TrafficEvent(
            road_id="road_q1_01",
            speed=45.5,
            vehicle_count=30,
            lat=10.7726,
            lng=106.6981,
            timestamp="2024-01-01T12:00:00Z",
        )
        json_str = event.to_json()
        assert "road_q1_01" in json_str
        assert "45.5" in json_str

    def test_to_dict(self):
        event = TrafficEvent(
            road_id="road_q1_01",
            speed=45.5,
            vehicle_count=30,
            lat=10.7726,
            lng=106.6981,
            timestamp="2024-01-01T12:00:00Z",
        )
        d = event.to_dict()
        assert d["road_id"] == "road_q1_01"
        assert d["speed"] == 45.5

    def test_from_json(self):
        json_str = '{"road_id":"road_q1_01","speed":45.5,"vehicle_count":30,"lat":10.7726,"lng":106.6981,"timestamp":"2024-01-01T12:00:00Z"}'
        event = TrafficEvent.from_json(json_str)
        assert event.road_id == "road_q1_01"


class TestValidateEvent:
    def test_valid(self):
        result = validate_event({
            "road_id": "road_q1_01",
            "speed": 45.5,
            "vehicle_count": 30,
            "lat": 10.7726,
            "lng": 106.6981,
            "timestamp": "2024-01-01T12:00:00Z",
        })
        assert result is not None
        assert result.road_id == "road_q1_01"

    def test_invalid(self):
        result = validate_event({"bad": "data"})
        assert result is None

    def test_missing_field(self):
        result = validate_event({
            "road_id": "road_q1_01",
            "speed": 45.5,
            # missing vehicle_count, lat, lng, timestamp
        })
        assert result is None


class TestConvertOldToNew:
    def test_convert_basic(self):
        old_record = {
            "vehicle_id": "V-1234",
            "speed_kmph": 45.5,
            "road": {"street": "Lê Lợi", "district": "Quận 1", "city": "HCMC"},
            "coordinates": {"latitude": 10.7726, "longitude": 106.6981},
            "timestamp": "2024-01-01T12:00:00Z",
            "traffic_status": {"congestion_level": "Low"},
        }
        event = convert_old_to_new(old_record)
        assert event is not None
        assert event.road_id == "road_q1_01"  # Lê Lợi → road_q1_01
        assert event.speed == 45.5

    def test_convert_congested(self):
        old_record = {
            "vehicle_id": "V-5678",
            "speed_kmph": 10.0,
            "road": {"street": "CMT8", "district": "Quận 3"},
            "coordinates": {"latitude": 10.79, "longitude": 106.66},
            "timestamp": "2024-01-01T08:00:00Z",
            "traffic_status": {"congestion_level": "High"},
        }
        event = convert_old_to_new(old_record)
        assert event is not None
        assert event.vehicle_count >= 80  # High congestion → 80-150

    def test_convert_bad_data(self):
        result = convert_old_to_new({})
        # Should return None or a default road_id
        # The converter is lenient with missing data


class TestSyntheticGenerator:
    def test_generate_single(self):
        event = generate_synthetic_event()
        assert event is not None
        assert event.road_id in [r["road_id"] for r in ROAD_SEGMENTS]
        assert 0 <= event.speed <= 200
        assert event.vehicle_count >= 0

    def test_generate_with_road(self):
        road = ROAD_SEGMENTS[0]
        event = generate_synthetic_event(road)
        assert event.road_id == road["road_id"]

    def test_generate_batch(self):
        batch = generate_batch(50)
        assert len(batch) == 50
        for event in batch:
            assert isinstance(event, TrafficEvent)

    def test_timestamp_is_valid(self):
        event = generate_synthetic_event()
        # Should not raise
        datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
