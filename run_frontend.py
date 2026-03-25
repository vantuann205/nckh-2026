#!/usr/bin/env python3
"""
Chạy Frontend Streamlit đơn giản
"""
import sys
import os
import subprocess

# Thêm thư mục hiện tại vào Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    import streamlit
    
    print("🎨 Đang khởi động Smart Traffic Frontend...")
    print("🌐 Dashboard sẽ chạy tại: http://localhost:8501")
    print("⚠️  Nhấn Ctrl+C để dừng")
    print()
    
    # Chạy Streamlit
    cmd = [
        sys.executable, "-m", "streamlit", "run", 
        "optimized_app.py",
        "--server.port=8501",
        "--server.address=0.0.0.0",
        "--browser.gatherUsageStats=false"
    ]
    
    subprocess.run(cmd)
    
except ImportError as e:
    print(f"❌ Lỗi import: {e}")
    print("💡 Hãy cài đặt dependencies:")
    print("   pip install streamlit pandas plotly requests")
    sys.exit(1)
except Exception as e:
    print(f"❌ Lỗi khởi động: {e}")
    sys.exit(1)