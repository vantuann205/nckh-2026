"""
Script khởi động toàn bộ hệ thống Smart Traffic Analytics
Chạy cả backend API và frontend Streamlit
"""
import subprocess
import sys
import time
import os
import signal
from pathlib import Path

def install_requirements():
    """Cài đặt các thư viện cần thiết"""
    print("📦 Đang cài đặt các thư viện cần thiết...")
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "-r", "requirements_smart.txt"
        ])
        print("✅ Đã cài đặt thành công các thư viện")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Lỗi cài đặt thư viện: {e}")
        return False

def check_data_files():
    """Kiểm tra file dữ liệu"""
    data_dir = Path("data")
    if not data_dir.exists():
        print("📁 Tạo thư mục data...")
        data_dir.mkdir()
    
    json_files = list(data_dir.glob("traffic_data_*.json"))
    if not json_files:
        print("⚠️  Không tìm thấy file dữ liệu trong thư mục data/")
        print("Vui lòng copy các file traffic_data_*.json vào thư mục data/")
        return False
    
    print(f"✅ Tìm thấy {len(json_files)} file dữ liệu")
    return True

def start_backend():
    """Khởi động backend API"""
    print("🔧 Khởi động Backend API...")
    try:
        process = subprocess.Popen([
            sys.executable, "smart_server.py"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Chờ một chút để API khởi động
        time.sleep(3)
        
        # Kiểm tra xem process có chạy không
        if process.poll() is None:
            print("✅ Backend API đã khởi động thành công")
            return process
        else:
            stdout, stderr = process.communicate()
            print(f"❌ Lỗi khởi động backend: {stderr.decode()}")
            return None
    except Exception as e:
        print(f"❌ Lỗi khởi động backend: {e}")
        return None

def start_frontend():
    """Khởi động frontend Streamlit"""
    print("🌐 Khởi động Web Dashboard...")
    try:
        process = subprocess.Popen([
            sys.executable, "-m", "streamlit", "run", "optimized_app.py",
            "--server.port", "8501",
            "--server.headless", "false"
        ])
        return process
    except Exception as e:
        print(f"❌ Lỗi khởi động frontend: {e}")
        return None

def main():
    """Hàm chính"""
    print("=" * 50)
    print("    🚦 Smart Traffic Analytics System")
    print("=" * 50)
    print()
    
    # Kiểm tra Python
    print(f"🐍 Python version: {sys.version}")
    
    # Cài đặt requirements
    if not install_requirements():
        input("Nhấn Enter để thoát...")
        return
    
    # Kiểm tra dữ liệu
    if not check_data_files():
        input("Nhấn Enter để thoát...")
        return
    
    print()
    print("🚀 Khởi động hệ thống...")
    print()
    
    # Khởi động backend
    backend_process = start_backend()
    if not backend_process:
        input("Nhấn Enter để thoát...")
        return
    
    # Khởi động frontend
    frontend_process = start_frontend()
    if not frontend_process:
        if backend_process:
            backend_process.terminate()
        input("Nhấn Enter để thoát...")
        return
    
    print()
    print("=" * 50)
    print("   ✅ Hệ thống đang chạy:")
    print("   📡 API Backend: http://localhost:8000")
    print("   🌐 Web Dashboard: http://localhost:8501")
    print("=" * 50)
    print()
    print("Nhấn Ctrl+C để dừng hệ thống")
    print()
    
    try:
        # Chờ cho đến khi người dùng dừng
        while True:
            time.sleep(1)
            
            # Kiểm tra xem các process có còn chạy không
            if backend_process.poll() is not None:
                print("❌ Backend đã dừng bất ngờ")
                break
            
            if frontend_process.poll() is not None:
                print("❌ Frontend đã dừng bất ngờ")
                break
    
    except KeyboardInterrupt:
        print("\n🛑 Đang dừng hệ thống...")
    
    finally:
        # Dừng các process
        if backend_process and backend_process.poll() is None:
            backend_process.terminate()
            backend_process.wait()
        
        if frontend_process and frontend_process.poll() is None:
            frontend_process.terminate()
            frontend_process.wait()
        
        print("✅ Hệ thống đã dừng hoàn toàn")

if __name__ == "__main__":
    main()