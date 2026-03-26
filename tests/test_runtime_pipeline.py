import threading
import time

import pandas as pd
import pytest


def test_producer_resume_checkpoint(monkeypatch, tmp_path):
    from scripts import realtime_producer

    df = pd.DataFrame(
        [
            {
                "event_time": "2026-03-26T10:00:00+00:00",
                "location_key": "q1:le loi",
                "road_id": "q1:le loi",
                "road_name": "Le Loi",
                "district": "Quan 1",
                "lat": 10.77,
                "lng": 106.70,
                "speed_kmph": 25,
                "vehicle_count": 30,
                "weather_temp_c": 30,
                "humidity_pct": 80,
                "weather_condition": "Cloudy",
                "accident_severity": 0,
                "congestion_km": 0,
            },
            {
                "event_time": "2026-03-26T10:01:00+00:00",
                "location_key": "q1:nguyen hue",
                "road_id": "q1:nguyen hue",
                "road_name": "Nguyen Hue",
                "district": "Quan 1",
                "lat": 10.77,
                "lng": 106.70,
                "speed_kmph": 15,
                "vehicle_count": 50,
                "weather_temp_c": 31,
                "humidity_pct": 86,
                "weather_condition": "Rain",
                "accident_severity": 2,
                "congestion_km": 1.2,
            },
        ]
    )

    monkeypatch.setattr(realtime_producer, "ensure_processed_dataset", lambda force_rebuild=False: df)
    monkeypatch.setattr(realtime_producer, "OFFSET_FILE", tmp_path / "producer_offset.json")

    sent = {"count": 0}

    class FakeResponse:
        status_code = 200

    class FakeClient:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, _url, json):
            assert json["location_key"]
            sent["count"] += 1
            return FakeResponse()

    monkeypatch.setattr(realtime_producer.httpx, "Client", FakeClient)

    realtime_producer.run("http://localhost:8000", 0, False)
    assert sent["count"] == len(df)
    assert realtime_producer.load_offset(realtime_producer.OFFSET_FILE) == len(df)

    # Resume should not replay from beginning when offset is at end.
    sent["count"] = 0
    realtime_producer.run("http://localhost:8000", 0, False)
    assert sent["count"] == 0


def test_queue_writer_flush_on_batch(monkeypatch):
    pytest.importorskip("psycopg")

    from storage.postgres_writer import PostgresBatchWriter

    writer = PostgresBatchWriter(batch_size=2, flush_interval_seconds=10)
    flushed_batches = []

    def fake_flush(records):
        flushed_batches.append(list(records))

    monkeypatch.setattr(writer, "_flush", fake_flush)
    writer.running = True

    thread = threading.Thread(target=writer._run_loop, daemon=True)
    thread.start()

    writer.enqueue({"location_key": "a"})
    writer.enqueue({"location_key": "b"})
    time.sleep(0.5)

    writer.running = False
    thread.join(timeout=2)

    assert flushed_batches
    assert len(flushed_batches[0]) == 2


def test_queue_writer_flush_on_timeout(monkeypatch):
    pytest.importorskip("psycopg")

    from storage.postgres_writer import PostgresBatchWriter

    writer = PostgresBatchWriter(batch_size=10, flush_interval_seconds=1)
    flushed_batches = []

    def fake_flush(records):
        flushed_batches.append(list(records))

    monkeypatch.setattr(writer, "_flush", fake_flush)
    writer.running = True

    thread = threading.Thread(target=writer._run_loop, daemon=True)
    thread.start()

    writer.enqueue({"location_key": "single"})
    time.sleep(1.5)

    writer.running = False
    thread.join(timeout=2)

    assert flushed_batches
    assert len(flushed_batches[0]) == 1
