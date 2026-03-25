"""
API Tests — FastAPI endpoints with mocked Redis
"""

import pytest
from unittest.mock import patch, MagicMock
from fastapi.testclient import TestClient


@pytest.fixture
def mock_redis():
    """Mock Redis client for testing"""
    mock = MagicMock()
    mock.get_all_roads.return_value = [
        {
            "road_id": "road_q1_01",
            "avg_speed": "45.5",
            "vehicle_count": "30",
            "status": "normal",
            "lat": "10.7726",
            "lng": "106.6981",
            "updated_at": "2024-01-01T12:00:00Z",
        },
        {
            "road_id": "road_q3_01",
            "avg_speed": "12.3",
            "vehicle_count": "120",
            "status": "congested",
            "lat": "10.7866",
            "lng": "106.6637",
            "updated_at": "2024-01-01T12:00:00Z",
        },
    ]
    mock.get_summary.return_value = {
        "total_roads": "2",
        "avg_speed": "28.9",
        "total_vehicles": "150",
        "congested_roads": "1",
    }
    mock.get_road_data.return_value = {
        "road_id": "road_q1_01",
        "avg_speed": "45.5",
        "vehicle_count": "30",
        "status": "normal",
        "lat": "10.7726",
        "lng": "106.6981",
    }
    mock.get_road_window.return_value = []
    mock.get_congested_roads.return_value = ["road_q3_01"]
    mock.get_stats.return_value = {
        "connected": True,
        "used_memory": "1.5M",
        "road_count": 2,
        "congested_count": 1,
    }
    return mock


@pytest.fixture
def client(mock_redis):
    """Create test client with mocked Redis"""
    with patch("backend.main.redis_client", mock_redis):
        with patch("backend.main._pubsub_thread"):
            from backend.main import app
            with TestClient(app) as c:
                yield c


class TestRootEndpoint:
    def test_root(self, client):
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["service"] == "Realtime Traffic API"


class TestHealthEndpoint:
    def test_health(self, client, mock_redis):
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert "redis" in data
        assert "websocket_clients" in data


class TestRealtimeEndpoint:
    def test_get_realtime(self, client, mock_redis):
        response = client.get("/traffic/realtime")
        assert response.status_code == 200
        data = response.json()
        assert "roads" in data
        assert "summary" in data
        assert len(data["roads"]) == 2


class TestSummaryEndpoint:
    def test_get_summary(self, client, mock_redis):
        response = client.get("/traffic/summary")
        assert response.status_code == 200
        data = response.json()
        assert "total_roads" in data


class TestRoadEndpoint:
    def test_get_road(self, client, mock_redis):
        response = client.get("/traffic/road_q1_01")
        assert response.status_code == 200
        data = response.json()
        assert "current" in data
        assert "window" in data

    def test_get_road_not_found(self, client, mock_redis):
        mock_redis.get_road_data.return_value = None
        response = client.get("/traffic/road_nonexistent")
        assert response.status_code == 404


class TestCongestedEndpoint:
    def test_get_congested(self, client, mock_redis):
        response = client.get("/traffic/congested")
        assert response.status_code == 200
        data = response.json()
        assert "congested" in data
        assert "count" in data
