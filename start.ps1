Write-Host ""
Write-Host " ==========================================" -ForegroundColor Cyan
Write-Host "  TRAFFIC PIPELINE - STARTING..." -ForegroundColor Cyan
Write-Host " ==========================================" -ForegroundColor Cyan
Write-Host ""

$maxRetry = 10
$sleepSeconds = 2

function Stop-ProcessOnPort {
	param(
		[int]$Port,
		[string]$Label
	)

	$connections = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
	if (-not $connections) {
		return
	}

	$pids = $connections | Select-Object -ExpandProperty OwningProcess -Unique
	foreach ($procId in $pids) {
		if ($procId -le 0) { continue }
		try {
			$proc = Get-Process -Id $procId -ErrorAction Stop
			Write-Host "Stopping blocker on port $Port ($Label): PID=$procId Name=$($proc.ProcessName)" -ForegroundColor DarkYellow
			Stop-Process -Id $procId -Force -ErrorAction Stop
		} catch {
			Write-Host "Could not stop PID=$procId on port $Port ($Label): $($_.Exception.Message)" -ForegroundColor Red
		}
	}
}

Write-Host "[0/6] Cleaning old blockers (API/frontend ports)..." -ForegroundColor Yellow
Stop-ProcessOnPort -Port 8000 -Label "API"
Stop-ProcessOnPort -Port 3000 -Label "Dashboard"
Stop-ProcessOnPort -Port 5173 -Label "Dashboard (Vite)"
Start-Sleep -Seconds 1

Write-Host "[1/6] Starting Docker services (Redis + Postgres)..." -ForegroundColor Yellow
docker compose up -d redis postgres
if ($LASTEXITCODE -ne 0) {
	Write-Host "Docker compose failed." -ForegroundColor Red
	exit 1
}

Write-Host "[2/6] Waiting for Redis ping OK..." -ForegroundColor Yellow
$ok = $false
for ($i = 1; $i -le $maxRetry; $i++) {
	docker exec traffic-redis redis-cli ping | Out-Null
	if ($LASTEXITCODE -eq 0) { $ok = $true; break }
	Start-Sleep -Seconds $sleepSeconds
}
if (-not $ok) {
	Write-Host "Redis readiness failed after $maxRetry retries." -ForegroundColor Red
	exit 1
}

Write-Host "[3/6] Waiting for Postgres connection OK..." -ForegroundColor Yellow
$ok = $false
for ($i = 1; $i -le $maxRetry; $i++) {
	docker exec traffic-postgres pg_isready -U traffic -d traffic_db | Out-Null
	if ($LASTEXITCODE -eq 0) { $ok = $true; break }
	Start-Sleep -Seconds $sleepSeconds
}
if (-not $ok) {
	Write-Host "Postgres readiness failed after $maxRetry retries." -ForegroundColor Red
	exit 1
}

Write-Host "[4/6] Starting FastAPI backend..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot'; uvicorn backend.main:app --host 0.0.0.0 --port 8000"

Write-Host "[5/6] Waiting for API /health = 200..." -ForegroundColor Yellow
$ok = $false
for ($i = 1; $i -le $maxRetry; $i++) {
	try {
		$res = Invoke-WebRequest -UseBasicParsing http://localhost:8000/health -TimeoutSec 3
		if ($res.StatusCode -eq 200) { $ok = $true; break }
	} catch {
	}
	Start-Sleep -Seconds $sleepSeconds
}
if (-not $ok) {
	Write-Host "API readiness failed after $maxRetry retries." -ForegroundColor Red
	exit 1
}

Write-Host "[6/6] Starting producer + frontend..." -ForegroundColor Yellow
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot'; python scripts/realtime_producer.py --batch-size 500 --interval 0.05"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot\dashboard'; npm run dev"

Write-Host ""
Write-Host " ==========================================" -ForegroundColor Green
Write-Host "  ALL SERVICES RUNNING" -ForegroundColor Green
Write-Host "  Dashboard : http://localhost:3000" -ForegroundColor Green
Write-Host "  API       : http://localhost:8000" -ForegroundColor Green
Write-Host " ==========================================" -ForegroundColor Green
Write-Host ""

Start-Process "http://localhost:3000"
