#!/usr/bin/env python3
"""
Script khởi chạy toàn bộ hệ thống Smart Traffic Analytics
Chạy cả Backend API và Frontend Streamlit
"""
import subprocess
import sys
import time
import os
import signal
import threading
from pathlib import Path

def print_banner():
    """In banner khởi động"""
    banner = """
╔══════════════════════════════════════════════════════════════╗
║                🚦 SMART TRAFFIC ANALYTICS SYSTEM 🚦          ║
║                                                              ║
║  🔥 Tính năng:                                               ║
║     • File Watcher - Tự động phát hiện thay đổi dữ liệu     ║
║     • Smart Caching - Chỉ làm mới khi cần thiết             ║
║     • Real-time Dashboard - Cập nhật tức thời                ║
║     • Interactive Maps - Bản đồ tương tác                    ║
║     • Advanced Analytics - Phân tích chuyên sâu              ║
║                                                              ║
║  🚀 Đang khởi động hệ thống...                               ║
╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def check_requirements():
    """Kiểm tra requirements"""
    print("🔍 Kiểm tra dependencies...")
    
    required_packages = [
        'streamlit', 'pandas', 'plotly', 'requests', 
        'fastapi', 'uvicorn', 'watchdog'
    ]
    
    missing = []
    for package in required_packages:
        try:
            __import__(package)
        except ImportError:
            missing.append(package)
    
    if missing:
        print(f"❌ Thiếu packages: {', '.join(missing)}")
        print("📦 Cài đặt bằng lệnh: pip install -r requirements-optimized.txt")
        return False
    
    print("✅ Tất cả dependencies đã sẵn sàng!")
    return True

def check_data_folder():
    """Kiểm tra folder data"""
    data_dir = Path("data")
    if not data_dir.exists():
        print("📁 Tạo folder data...")
        data_dir.mkdir(exist_ok=True)
    
    json_files = list(data_dir.glob("traffic_data_*.json"))
    if not json_files:
        print("⚠️  Không tìm thấy file dữ liệu trong folder data/")
        print("💡 Hãy đặt các file traffic_data_*.json vào folder data/")
        return False
    
    print(f"✅ Tìm thấy {len(json_files)} file dữ liệu")
    return True

def start_backend():
    """Khởi động Backend API"""
    print("🚀 Đang khởi động Backend API...")
    
    cmd = [sys.executable, "smart_server.py"]
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )
    
    # Monitor backend startup
    def monitor_backend():
        for line in iter(process.stdout.readline, ''):
            if line.strip():
                print(f"[API] {line.strip()}")
    
    threading.Thread(target=monitor_backend, daemon=True).start()
    
    return process

def start_frontend():
    """Khởi động Frontend Streamlit"""
    print("🎨 Đang khởi động Frontend Dashboard...")
    
    cmd = [
        sys.executable, "-m", "streamlit", "run", 
        "optimized_app.py",
        "--server.port=8501",
        "--server.address=0.0.0.0",
        "--browser.gatherUsageStats=false",
        "--server.headless=false"
    ]
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
        bufsize=1
    )
    
    # Monitor frontend startup
    def monitor_frontend():
        for line in iter(process.stdout.readline, ''):
            if line.strip():
                print(f"[WEB] {line.strip()}")
    
    threading.Thread(target=monitor_frontend, daemon=True).start()
    
    return process

def wait_for_services():
    """Chờ services khởi động"""
    import requests
    
    print("⏳ Chờ Backend API khởi động...")
    for i in range(30):  # Chờ tối đa 30 giây
        try:
            response = requests.get("http://localhost:8000/", timeout=2)
            if response.status_code == 200:
                print("✅ Backend API đã sẵn sàng!")
                break
        except:
            pass
        time.sleep(1)
        print(f"   Đang chờ... ({i+1}/30)")
    else:
        print("❌ Backend API không khởi động được!")
        return False
    
    print("⏳ Chờ Frontend khởi động...")
    time.sleep(3)  # Streamlit cần thời gian khởi động
    
    return True

def main():
    """Main function"""
    print_banner()
    
    # Kiểm tra requirements
    if not check_requirements():
        sys.exit(1)
    
    # Kiểm tra data
    if not check_data_folder():
        print("💡 Bạn có thể tiếp tục nhưng sẽ không có dữ liệu để hiển thị")
        response = input("Tiếp tục? (y/N): ")
        if response.lower() != 'y':
            sys.exit(1)
    
    processes = []
    
    try:
        # Khởi động Backend
        backend_process = start_backend()
        processes.append(backend_process)
        
        # Chờ backend sẵn sàng
        time.sleep(3)
        
        # Khởi động Frontend
        frontend_process = start_frontend()
        processes.append(frontend_process)
        
        # Chờ services
        if wait_for_services():
            print("\n" + "="*60)
            print("🎉 HỆ THỐNG ĐÃ KHỞI ĐỘNG THÀNH CÔNG!")
            print("="*60)
            print("🌐 Backend API:      http://localhost:8000")
            print("📊 Frontend Dashboard: http://localhost:8501")
            print("="*60)
            print("📝 Tính năng:")
            print("   • File Watcher: Tự động phát hiện thay đổi trong folder data/")
            print("   • Smart Cache: Chỉ làm mới khi có dữ liệu mới")
            print("   • Real-time: Cập nhật tức thời không cần F5")
            print("="*60)
            print("⚠️  Nhấn Ctrl+C để dừng hệ thống")
            print()
            
            # Chờ user dừng
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\n🛑 Đang dừng hệ thống...")
        
    except Exception as e:
        print(f"❌ Lỗi khởi động: {e}")
    
    finally:
        # Dừng tất cả processes
        for process in processes:
            try:
                process.terminate()
                process.wait(timeout=5)
            except:
                try:
                    process.kill()
                except:
                    pass
        
        print("✅ Hệ thống đã dừng hoàn toàn!")

if __name__ == "__main__":
    main()