"""
Script test hệ thống Smart Traffic Analytics
Kiểm tra API endpoints và tính năng file watcher
"""
import requests
import json
import time
import os
from pathlib import Path

API_BASE = "http://localhost:8000"

def test_api_connection():
    """Test kết nối API"""
    print("🔗 Testing API connection...")
    try:
        response = requests.get(f"{API_BASE}/", timeout=5)
        if response.status_code == 200:
            print("✅ API connection successful")
            print(f"   Response: {response.json()}")
            return True
        else:
            print(f"❌ API returned status {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ API connection failed: {e}")
        return False

def test_all_endpoints():
    """Test tất cả API endpoints"""
    endpoints = [
        "/api/status",
        "/api/summary", 
        "/api/stats/flow",
        "/api/stats/types",
        "/api/stats/speed",
        "/api/stats/weather",
        "/api/stats/districts",
        "/api/explorer?limit=10",
        "/api/map?limit=100"
    ]
    
    print("\n📡 Testing all API endpoints...")
    results = {}
    
    for endpoint in endpoints:
        try:
            response = requests.get(f"{API_BASE}{endpoint}", timeout=10)
            if response.status_code == 200:
                data = response.json()
                results[endpoint] = {
                    "status": "✅ OK",
                    "data_length": len(data) if isinstance(data, list) else "N/A",
                    "sample": str(data)[:100] + "..." if len(str(data)) > 100 else str(data)
                }
                print(f"   ✅ {endpoint}")
            else:
                results[endpoint] = {"status": f"❌ HTTP {response.status_code}"}
                print(f"   ❌ {endpoint} - HTTP {response.status_code}")
        except Exception as e:
            results[endpoint] = {"status": f"❌ Error: {e}"}
            print(f"   ❌ {endpoint} - Error: {e}")
    
    return results

def test_file_watcher():
    """Test file watcher functionality"""
    print("\n👀 Testing file watcher...")
    
    # Tạo file test
    test_file = Path("data/test_watcher.json")
    test_data = [
        {
            "vehicle_id": "TEST001",
            "vehicle_type": "Car",
            "speed_kmph": 45.5,
            "timestamp": "2024-01-01T12:00:00Z",
            "coordinates": {"latitude": 10.7769, "longitude": 106.7009},
            "road": {"street": "Test Street", "district": "Test District"},
            "owner": {"name": "Test Owner", "license_number": "TEST123"},
            "fuel_level_percentage": 75,
            "weather_condition": {"condition": "Sunny", "temperature_celsius": 28},
            "traffic_status": {"congestion_level": "Low"}
        }
    ]
    
    try:
        # Lấy summary trước khi thêm file
        print("   📊 Getting initial summary...")
        initial_response = requests.get(f"{API_BASE}/api/summary", timeout=5)
        initial_total = initial_response.json().get('total', 0) if initial_response.status_code == 200 else 0
        print(f"   Initial total: {initial_total}")
        
        # Tạo file test
        print("   📁 Creating test file...")
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, indent=2)
        
        # Chờ file watcher detect
        print("   ⏳ Waiting for file watcher (5 seconds)...")
        time.sleep(5)
        
        # Kiểm tra summary sau khi thêm file
        print("   📊 Getting updated summary...")
        updated_response = requests.get(f"{API_BASE}/api/summary", timeout=5)
        updated_total = updated_response.json().get('total', 0) if updated_response.status_code == 200 else 0
        print(f"   Updated total: {updated_total}")
        
        # Kiểm tra xem có thay đổi không
        if updated_total > initial_total:
            print("   ✅ File watcher working! Data updated automatically")
            watcher_working = True
        else:
            print("   ⚠️  File watcher might not be working or data not changed")
            watcher_working = False
        
        # Xóa file test
        print("   🗑️  Cleaning up test file...")
        test_file.unlink()
        
        # Chờ và kiểm tra lại
        time.sleep(3)
        final_response = requests.get(f"{API_BASE}/api/summary", timeout=5)
        final_total = final_response.json().get('total', 0) if final_response.status_code == 200 else 0
        print(f"   Final total after cleanup: {final_total}")
        
        return watcher_working
        
    except Exception as e:
        print(f"   ❌ File watcher test failed: {e}")
        # Cleanup
        if test_file.exists():
            test_file.unlink()
        return False

def test_data_integrity():
    """Test tính toàn vẹn dữ liệu"""
    print("\n🔍 Testing data integrity...")
    
    try:
        # Test explorer data
        explorer_data = requests.get(f"{API_BASE}/api/explorer?limit=100", timeout=10).json()
        
        if not explorer_data:
            print("   ⚠️  No explorer data found")
            return False
        
        # Kiểm tra các field bắt buộc
        required_fields = ['vehicle_id', 'speed_kmph', 'district']
        missing_fields = []
        
        for item in explorer_data[:10]:  # Check first 10 items
            for field in required_fields:
                if field not in item or item[field] is None:
                    missing_fields.append(field)
        
        if missing_fields:
            print(f"   ⚠️  Missing fields found: {set(missing_fields)}")
        else:
            print("   ✅ All required fields present")
        
        # Test data ranges
        speeds = [item.get('speed_kmph', 0) for item in explorer_data]
        fuel_levels = [item.get('fuel_level', 0) for item in explorer_data]
        
        print(f"   📊 Speed range: {min(speeds):.1f} - {max(speeds):.1f} km/h")
        print(f"   ⛽ Fuel range: {min(fuel_levels):.1f} - {max(fuel_levels):.1f}%")
        
        # Validate ranges
        invalid_speeds = [s for s in speeds if s < 0 or s > 200]
        invalid_fuel = [f for f in fuel_levels if f < 0 or f > 100]
        
        if invalid_speeds:
            print(f"   ⚠️  Invalid speeds found: {len(invalid_speeds)} items")
        if invalid_fuel:
            print(f"   ⚠️  Invalid fuel levels found: {len(invalid_fuel)} items")
        
        if not invalid_speeds and not invalid_fuel:
            print("   ✅ Data ranges are valid")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Data integrity test failed: {e}")
        return False

def main():
    """Chạy tất cả tests"""
    print("🧪 Smart Traffic Analytics System Test")
    print("=" * 50)
    
    # Test 1: API Connection
    api_ok = test_api_connection()
    
    if not api_ok:
        print("\n❌ API not available. Please start the backend server first:")
        print("   python smart_server.py")
        return
    
    # Test 2: All endpoints
    endpoint_results = test_all_endpoints()
    
    # Test 3: File watcher
    watcher_ok = test_file_watcher()
    
    # Test 4: Data integrity
    data_ok = test_data_integrity()
    
    # Summary
    print("\n" + "=" * 50)
    print("📋 TEST SUMMARY")
    print("=" * 50)
    
    print(f"🔗 API Connection: {'✅ PASS' if api_ok else '❌ FAIL'}")
    
    endpoint_pass = sum(1 for r in endpoint_results.values() if '✅' in r['status'])
    endpoint_total = len(endpoint_results)
    print(f"📡 API Endpoints: ✅ {endpoint_pass}/{endpoint_total} PASS")
    
    print(f"👀 File Watcher: {'✅ PASS' if watcher_ok else '⚠️  UNCERTAIN'}")
    print(f"🔍 Data Integrity: {'✅ PASS' if data_ok else '❌ FAIL'}")
    
    overall_status = api_ok and (endpoint_pass == endpoint_total) and data_ok
    print(f"\n🎯 OVERALL: {'✅ SYSTEM HEALTHY' if overall_status else '⚠️  ISSUES DETECTED'}")
    
    if not overall_status:
        print("\n💡 Troubleshooting tips:")
        if not api_ok:
            print("   - Start backend: python smart_server.py")
        if endpoint_pass < endpoint_total:
            print("   - Check data files in data/ folder")
        if not data_ok:
            print("   - Verify JSON format in data files")
        if not watcher_ok:
            print("   - File watcher may need time to initialize")

if __name__ == "__main__":
    main()