@echo off
setlocal enabledelayedexpansion
title Traffic Pipeline
set LOG_DIR=%~dp0logs
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
set STARTUP_LOG=%LOG_DIR%\startup.log
echo [%DATE% %TIME%] Startup begin > "%STARTUP_LOG%"

echo.
echo  ==========================================
echo   TRAFFIC PIPELINE - STARTING...
echo  ==========================================
echo.

echo [1/6] Starting Docker services (Redis + Postgres)...
echo [%DATE% %TIME%] Start docker services >> "%STARTUP_LOG%"
docker compose up -d redis postgres
if errorlevel 1 (
	echo Docker compose failed. Stop startup.
	exit /b 1
)

set MAX_RETRY=10
set RETRY_SLEEP=2

echo.
echo [2/6] Waiting for Redis ping OK...
echo [%DATE% %TIME%] Wait Redis readiness >> "%STARTUP_LOG%"
set /a TRY=0
:WAIT_REDIS
set /a TRY+=1
docker exec traffic-redis redis-cli ping | findstr /I "PONG" > nul 2>&1
if not errorlevel 1 goto REDIS_OK
if !TRY! geq %MAX_RETRY% (
	echo Redis readiness failed after %MAX_RETRY% retries.
	exit /b 1
)
timeout /t %RETRY_SLEEP% /nobreak > nul
goto WAIT_REDIS
:REDIS_OK
echo Redis is ready.
echo [%DATE% %TIME%] Redis ready >> "%STARTUP_LOG%"

echo.
echo [3/6] Waiting for Postgres connection OK...
echo [%DATE% %TIME%] Wait Postgres readiness >> "%STARTUP_LOG%"
set /a TRY=0
:WAIT_POSTGRES
set /a TRY+=1
docker exec traffic-postgres pg_isready -U traffic -d traffic_db > nul 2>&1
if not errorlevel 1 goto POSTGRES_OK
if !TRY! geq %MAX_RETRY% (
	echo Postgres readiness failed after %MAX_RETRY% retries.
	exit /b 1
)
timeout /t %RETRY_SLEEP% /nobreak > nul
goto WAIT_POSTGRES
:POSTGRES_OK
echo Postgres is ready.
echo [%DATE% %TIME%] Postgres ready >> "%STARTUP_LOG%"

echo.
echo [4/6] Starting FastAPI backend (port 8000)...
echo [%DATE% %TIME%] Start API process >> "%STARTUP_LOG%"
start "Traffic API" cmd /k "cd /d %~dp0 && uvicorn backend.main:app --host 0.0.0.0 --port 8000 1>>logs\api.log 2>>&1"

echo.
echo [5/6] Waiting for API health 200...
echo [%DATE% %TIME%] Wait API readiness >> "%STARTUP_LOG%"
set /a TRY=0
:WAIT_API
set /a TRY+=1
powershell -NoProfile -Command "$r = Invoke-WebRequest -UseBasicParsing http://localhost:8000/health -TimeoutSec 3; if ($r.StatusCode -eq 200) { exit 0 } else { exit 1 }" > nul 2>&1
if not errorlevel 1 goto API_OK
if !TRY! geq %MAX_RETRY% (
	echo API readiness failed after %MAX_RETRY% retries.
	exit /b 1
)
timeout /t %RETRY_SLEEP% /nobreak > nul
goto WAIT_API
:API_OK
echo API is ready.
echo [%DATE% %TIME%] API ready >> "%STARTUP_LOG%"

echo.
echo [6/6] Starting producer + frontend...
echo [%DATE% %TIME%] Start producer and frontend >> "%STARTUP_LOG%"
start "Realtime Producer" cmd /k "cd /d %~dp0 && python scripts/realtime_producer.py 1>>logs\producer.log 2>>&1"
start "Frontend" cmd /k "cd /d %~dp0dashboard && npm run dev 1>>..\logs\frontend.log 2>>&1"

echo.
echo  ==========================================
echo   ALL SERVICES RUNNING
echo   Dashboard : http://localhost:3000
echo   API       : http://localhost:8000
echo  ==========================================
echo.
timeout /t 5 /nobreak > nul
echo [%DATE% %TIME%] Startup complete >> "%STARTUP_LOG%"
start http://localhost:3000
