@echo off
echo ========================================
echo    Smart Traffic Analytics System
echo ========================================
echo.

REM Kiểm tra Python
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python không được cài đặt!
    echo Vui lòng cài đặt Python từ https://python.org
    pause
    exit /b 1
)

echo ✅ Python đã được cài đặt

REM Cài đặt dependencies
echo.
echo 📦 Đang cài đặt các thư viện cần thiết...
pip install streamlit pandas plotly requests watchdog fastapi uvicorn

if errorlevel 1 (
    echo ❌ Lỗi cài đặt thư viện!
    pause
    exit /b 1
)

echo ✅ Đã cài đặt thành công các thư viện

REM Tạo thư mục data nếu chưa có
if not exist "data" (
    echo 📁 Tạo thư mục data...
    mkdir data
)

REM Kiểm tra file dữ liệu
if not exist "data\traffic_data_0.json" (
    echo ⚠️  Không tìm thấy file dữ liệu traffic_data_0.json trong thư mục data
    echo Vui lòng copy file dữ liệu vào thư mục data/
    pause
)

echo.
echo 🚀 Khởi động hệ thống...
echo.

REM Khởi động backend API trong cửa sổ mới
echo 🔧 Khởi động Backend API...
start "Smart Traffic API" cmd /k "python smart_server.py"

REM Chờ 3 giây để API khởi động
timeout /t 3 /nobreak >nul

REM Khởi động Streamlit app
echo 🌐 Khởi động Web Dashboard...
echo.
echo ========================================
echo   Hệ thống đang chạy:
echo   📡 API: http://localhost:8000
echo   🌐 Web: http://localhost:8501
echo ========================================
echo.
echo Nhấn Ctrl+C để dừng hệ thống
echo.

streamlit run optimized_app.py --server.port 8501 --server.headless false

echo.
echo 🛑 Hệ thống đã dừng
pause