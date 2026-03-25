@echo off
echo ========================================
echo   SMART TRAFFIC ANALYTICS SYSTEM
echo ========================================
echo.
echo Dang khoi dong he thong...
echo.

REM Kiem tra Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Loi: Python chua duoc cai dat!
    echo Vui long cai dat Python tu https://python.org
    pause
    exit /b 1
)

REM Chay script chinh
python start_system.py

echo.
echo He thong da dung!
pause