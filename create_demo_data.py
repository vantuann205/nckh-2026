#!/usr/bin/env python3
"""
Tạo dữ liệu demo để test hệ thống
"""
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

def generate_demo_data(num_records=1000):
    """Tạo dữ liệu demo"""
    
    # Danh sách các giá trị mẫu
    vehicle_types = ["Motorbike", "Car", "Bus", "Truck", "Taxi", "Electric Car"]
    districts = ["Quận 1", "Quận 3", "Quận 5", "Quận 7", "Quận 10", "Bình Thạnh", "Gò Vấp", "Thủ Đức", "Tân Bình", "Tân Phú"]
    streets = ["Nguyễn Huệ", "Lê Lợi", "Hai Bà Trưng", "Trần Hưng Đạo", "Võ Văn Tần", "Cách Mạng Tháng 8", "Lê Văn Sỹ", "Phan Xích Long"]
    weather_conditions = ["Sunny", "Cloudy", "Rainy", "Foggy"]
    congestion_levels = ["Low", "Medium", "High"]
    
    # Tọa độ TP.HCM (xung quanh trung tâm)
    base_lat = 10.7769
    base_lng = 106.7009
    
    data = []
    
    for i in range(num_records):
        # Tạo timestamp ngẫu nhiên trong 24h qua
        timestamp = datetime.now() - timedelta(hours=random.randint(0, 24), minutes=random.randint(0, 59))
        
        # Tạo tọa độ ngẫu nhiên xung quanh TP.HCM
        lat = base_lat + random.uniform(-0.1, 0.1)
        lng = base_lng + random.uniform(-0.1, 0.1)
        
        # Tạo dữ liệu xe
        vehicle_type = random.choice(vehicle_types)
        speed = random.randint(0, 120)
        fuel_level = random.randint(5, 100)
        
        # Tạo alerts dựa trên điều kiện
        alerts = []
        if speed > 80:
            alerts.append({
                "type": "Speeding",
                "description": f"Vehicle exceeding speed limit: {speed} km/h",
                "severity": "High" if speed > 100 else "Medium",
                "timestamp": timestamp.isoformat()
            })
        
        if fuel_level < 20:
            alerts.append({
                "type": "Low Fuel",
                "description": f"Fuel level critically low: {fuel_level}%",
                "severity": "High" if fuel_level < 10 else "Medium",
                "timestamp": timestamp.isoformat()
            })
        
        record = {
            "vehicle_id": f"VH{i+1:06d}",
            "vehicle_type": vehicle_type,
            "speed_kmph": speed,
            "fuel_level_percentage": fuel_level,
            "passenger_count": random.randint(1, 8 if vehicle_type == "Bus" else 4),
            "timestamp": timestamp.isoformat(),
            "coordinates": {
                "latitude": lat,
                "longitude": lng
            },
            "road": {
                "street": random.choice(streets),
                "district": random.choice(districts),
                "city": "Ho Chi Minh City"
            },
            "owner": {
                "name": f"Nguyen Van {chr(65 + i % 26)}",
                "license_number": f"B{random.randint(1, 9)}-{random.randint(10000, 99999)}"
            },
            "weather_condition": {
                "condition": random.choice(weather_conditions),
                "temperature_celsius": random.randint(25, 35),
                "humidity_percentage": random.randint(60, 90)
            },
            "traffic_status": {
                "congestion_level": random.choice(congestion_levels),
                "estimated_delay_minutes": random.randint(0, 30)
            },
            "alerts": alerts
        }
        
        data.append(record)
    
    return data

def main():
    """Tạo và lưu dữ liệu demo"""
    print("🔧 Đang tạo dữ liệu demo...")
    
    # Tạo folder data nếu chưa có
    data_dir = Path("data")
    data_dir.mkdir(exist_ok=True)
    
    # Tạo dữ liệu
    demo_data = generate_demo_data(1000)
    
    # Lưu vào file
    output_file = data_dir / "traffic_data_demo.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(demo_data, f, ensure_ascii=False, indent=2)
    
    print(f"✅ Đã tạo {len(demo_data)} bản ghi demo")
    print(f"📁 Lưu tại: {output_file}")
    print(f"📊 Thống kê:")
    
    # Thống kê nhanh
    vehicle_counts = {}
    for record in demo_data:
        vtype = record["vehicle_type"]
        vehicle_counts[vtype] = vehicle_counts.get(vtype, 0) + 1
    
    for vtype, count in vehicle_counts.items():
        print(f"   • {vtype}: {count} xe")
    
    print("\n🚀 Bây giờ bạn có thể chạy hệ thống:")
    print("   • Windows: run_simple.bat")
    print("   • Backend: python run_backend.py")
    print("   • Frontend: python run_frontend.py")

if __name__ == "__main__":
    main()