# ============================================================
#  TRAFFIC PIPELINE — ONE-CLICK SETUP & RUN
#  Chạy file này là xong, không cần cài gì thêm thủ công.
#  Yêu cầu: Windows 10/11, kết nối internet
# ============================================================

param(
    [switch]$SkipInstall   # Bỏ qua cài đặt nếu đã cài rồi
)

$ErrorActionPreference = "Stop"
$PSScriptRoot_ = Split-Path -Parent $MyInvocation.MyCommand.Definition

function Write-Step($n, $msg) {
    Write-Host ""
    Write-Host "  [$n] $msg" -ForegroundColor Cyan
}
function Write-OK($msg)   { Write-Host "      OK  $msg" -ForegroundColor Green }
function Write-Warn($msg) { Write-Host "      !!  $msg" -ForegroundColor Yellow }
function Write-Fail($msg) { Write-Host "      XX  $msg" -ForegroundColor Red; exit 1 }

Write-Host ""
Write-Host " ================================================" -ForegroundColor Magenta
Write-Host "   TRAFFIC PIPELINE  —  AUTO SETUP & RUN"         -ForegroundColor Magenta
Write-Host " ================================================" -ForegroundColor Magenta

# ── 1. Python ────────────────────────────────────────────────
Write-Step 1 "Kiểm tra Python 3.9+"
try {
    $pyver = python --version 2>&1
    if ($pyver -match "Python (\d+)\.(\d+)") {
        $major = [int]$Matches[1]; $minor = [int]$Matches[2]
        if ($major -lt 3 -or ($major -eq 3 -and $minor -lt 9)) {
            Write-Fail "Cần Python >= 3.9, hiện có: $pyver. Tải tại https://python.org/downloads"
        }
        Write-OK $pyver
    }
} catch {
    Write-Fail "Python chưa được cài. Tải tại https://python.org/downloads (nhớ tick 'Add to PATH')"
}

# ── 2. Node.js ───────────────────────────────────────────────
Write-Step 2 "Kiểm tra Node.js 18+"
try {
    $nodever = node --version 2>&1
    Write-OK "Node $nodever"
} catch {
    Write-Fail "Node.js chưa được cài. Tải tại https://nodejs.org (LTS)"
}

# ── 3. Docker Desktop ────────────────────────────────────────
Write-Step 3 "Kiểm tra Docker Desktop"
try {
    $dockerver = docker --version 2>&1
    Write-OK $dockerver
} catch {
    Write-Fail "Docker Desktop chưa được cài. Tải tại https://www.docker.com/products/docker-desktop"
}

# Kiểm tra Docker daemon đang chạy
try {
    docker info 2>&1 | Out-Null
    Write-OK "Docker daemon đang chạy"
} catch {
    Write-Fail "Docker Desktop chưa khởi động. Mở Docker Desktop rồi chạy lại."
}

# ── 4. Python packages ───────────────────────────────────────
if (-not $SkipInstall) {
    Write-Step 4 "Cài Python packages (requirements.txt)"
    try {
        # Thêm orjson + ijson vào requirements nếu chưa có
        $req = Get-Content "$PSScriptRoot_\requirements.txt" -Raw
        $extras = @("orjson", "ijson")
        foreach ($pkg in $extras) {
            if ($req -notmatch $pkg) {
                Add-Content "$PSScriptRoot_\requirements.txt" "`n$pkg"
            }
        }
        python -m pip install --upgrade pip --quiet
        python -m pip install -r "$PSScriptRoot_\requirements.txt" --quiet
        Write-OK "Tất cả Python packages đã cài"
    } catch {
        Write-Fail "Lỗi cài Python packages: $_"
    }
} else {
    Write-Step 4 "Bỏ qua cài Python packages (--SkipInstall)"
}

# ── 5. Node packages (dashboard) ─────────────────────────────
if (-not $SkipInstall) {
    Write-Step 5 "Cài Node packages (dashboard/)"
    try {
        Push-Location "$PSScriptRoot_\dashboard"
        npm install --silent 2>&1 | Out-Null
        Pop-Location
        Write-OK "npm install xong"
    } catch {
        Pop-Location -ErrorAction SilentlyContinue
        Write-Fail "Lỗi npm install: $_"
    }
} else {
    Write-Step 5 "Bỏ qua npm install (--SkipInstall)"
}

# ── 6. Docker services (Redis + Postgres) ────────────────────
Write-Step 6 "Khởi động Redis + Postgres (Docker)"
try {
    Set-Location $PSScriptRoot_
    docker compose up -d redis postgres 2>&1 | Out-Null
    Write-OK "Containers đang khởi động..."
} catch {
    Write-Fail "docker compose up thất bại: $_"
}

# Chờ Redis
Write-Host "      Chờ Redis..." -NoNewline
$ok = $false
for ($i = 1; $i -le 20; $i++) {
    Start-Sleep -Seconds 2
    $ping = docker exec traffic-redis redis-cli ping 2>&1
    if ($ping -match "PONG") { $ok = $true; break }
    Write-Host "." -NoNewline
}
if (-not $ok) { Write-Fail "Redis không phản hồi sau 40s" }
Write-Host ""
Write-OK "Redis sẵn sàng"

# Chờ Postgres
Write-Host "      Chờ Postgres..." -NoNewline
$ok = $false
for ($i = 1; $i -le 20; $i++) {
    Start-Sleep -Seconds 2
    $pg = docker exec traffic-postgres pg_isready -U traffic -d traffic_db 2>&1
    if ($pg -match "accepting") { $ok = $true; break }
    Write-Host "." -NoNewline
}
if (-not $ok) { Write-Warn "Postgres chưa sẵn sàng — tiếp tục (không bắt buộc)" }
else { Write-Host ""; Write-OK "Postgres sẵn sàng" }

# ── 7. FastAPI backend ───────────────────────────────────────
Write-Step 7 "Khởi động FastAPI backend (port 8000)"
$backendJob = Start-Process powershell -ArgumentList `
    "-NoExit", "-Command", `
    "cd '$PSScriptRoot_'; Write-Host '[Backend]' -ForegroundColor Cyan; uvicorn backend.main:app --host 0.0.0.0 --port 8000 --log-level info" `
    -PassThru

# Chờ API /health
Write-Host "      Chờ API..." -NoNewline
$ok = $false
for ($i = 1; $i -le 30; $i++) {
    Start-Sleep -Seconds 2
    try {
        $res = Invoke-WebRequest -UseBasicParsing http://localhost:8000/health -TimeoutSec 3 -ErrorAction Stop
        if ($res.StatusCode -eq 200) { $ok = $true; break }
    } catch {}
    Write-Host "." -NoNewline
}
if (-not $ok) { Write-Fail "API không phản hồi sau 60s. Xem cửa sổ Backend để debug." }
Write-Host ""
Write-OK "API sẵn sàng tại http://localhost:8000"

# ── 8. Frontend ──────────────────────────────────────────────
Write-Step 8 "Khởi động Frontend (port 3000)"
$frontendJob = Start-Process powershell -ArgumentList `
    "-NoExit", "-Command", `
    "cd '$PSScriptRoot_\dashboard'; Write-Host '[Frontend]' -ForegroundColor Cyan; npm run dev" `
    -PassThru

Start-Sleep -Seconds 4
Write-OK "Frontend đang chạy tại http://localhost:3000"

# ── 9. Mở browser ────────────────────────────────────────────
Write-Step 9 "Mở trình duyệt"
Start-Sleep -Seconds 2
Start-Process "http://localhost:3000"
Write-OK "Đã mở http://localhost:3000"

# ── Done ─────────────────────────────────────────────────────
Write-Host ""
Write-Host " ================================================" -ForegroundColor Green
Write-Host "   TẤT CẢ DỊCH VỤ ĐÃ CHẠY"                        -ForegroundColor Green
Write-Host ""
Write-Host "   Dashboard : http://localhost:3000"               -ForegroundColor White
Write-Host "   API       : http://localhost:8000"               -ForegroundColor White
Write-Host "   API Docs  : http://localhost:8000/docs"          -ForegroundColor White
Write-Host ""
Write-Host "   Dữ liệu tự động load từ folder data/"            -ForegroundColor White
Write-Host "   Thêm file traffic_data_*.json vào data/"         -ForegroundColor White
Write-Host "   → hệ thống tự detect và load ngay"               -ForegroundColor White
Write-Host ""
Write-Host "   Để dừng: đóng các cửa sổ PowerShell"            -ForegroundColor Gray
Write-Host "   Để dừng Docker: docker compose down"             -ForegroundColor Gray
Write-Host " ================================================" -ForegroundColor Green
Write-Host ""
