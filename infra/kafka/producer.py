import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

def get_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def generate_traffic_event():
    districts = ['Quận 1', 'Quận 3', 'Quận 5', 'Quận 7', 'Quận 10', 'Bình Thạnh', 'Gò Vấp', 'Thủ Đức', 'Tân Bình', 'Tân Phú']
    v_types = ['Motorbike', 'Car', 'Bus', 'Truck', 'Taxi', 'Electric Car']
    streets = ['Lê Lợi', 'Nguyễn Huệ', ' CMT8', 'Võ Văn Kiệt', 'Điện Biên Phủ', 'Nam Kỳ Khởi Nghĩa']
    
    vehicle_id = f"V-{random.randint(1000, 9999)}"
    speed = random.randint(0, 110)
    
    return {
        "vehicle_id": vehicle_id,
        "vehicle_type": random.choice(v_types),
        "speed_kmph": speed,
        "road": {
            "street": random.choice(streets),
            "district": random.choice(districts),
            "city": "Ho Chi Minh City"
        },
        "owner": {
            "name": f"User {random.randint(1, 500)}",
            "license_number": f"59-{random.choice(['A','B','C','D'])}-{random.randint(10000, 99999)}"
        },
        "coordinates": {
            "latitude": 10.762622 + (random.random() - 0.5) * 0.1,
            "longitude": 106.660172 + (random.random() - 0.5) * 0.1
        },
        "timestamp": datetime.now().isoformat(),
        "fuel_level_percentage": random.randint(5, 100),
        "passenger_count": random.randint(1, 45),
        "weather_condition": {
            "temperature_celsius": 30 + random.uniform(-2, 2),
            "humidity_percentage": 70 + random.uniform(-5, 5),
            "condition": random.choice(['Clear', 'Rainy', 'Cloudy'])
        },
        "traffic_status": {
            "congestion_level": "High" if speed < 20 else "Medium" if speed < 40 else "Low",
            "estimated_delay_minutes": random.randint(0, 30) if speed < 40 else 0
        },
        "alerts": []
    }

def stream_data():
    producer = get_producer()
    print("🚀 Kafka Producer started. Streaming real-time traffic data...")
    
    try:
        while True:
            event = generate_traffic_event()
            producer.send('traffic-data', value=event)
            print(f"📡 Sent event: {event['vehicle_id']} at {event['speed_kmph']} km/h")
            time.sleep(1) # Stream 1 event per second
    except KeyboardInterrupt:
        print("🛑 Producer stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    stream_data()
