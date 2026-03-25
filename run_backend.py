#!/usr/bin/env python3
"""
Chạy Backend API đơn giản
"""
import sys
import os

# Thêm thư mục hiện tại vào Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from smart_server import app
    import uvicorn
    
    print("🚀 Đang khởi động Smart Traffic Backend API...")
    print("📡 API sẽ chạy tại: http://localhost:8000")
    print("📖 API docs: http://localhost:8000/docs")
    print("⚠️  Nhấn Ctrl+C để dừng")
    print()
    
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
    
except ImportError as e:
    print(f"❌ Lỗi import: {e}")
    print("💡 Hãy cài đặt dependencies:")
    print("   pip install fastapi uvicorn watchdog")
    sys.exit(1)
except Exception as e:
    print(f"❌ Lỗi khởi động: {e}")
    sys.exit(1)