# Force rebuild of processed dataset from full source files
# This script ensures the parquet is rebuilt with all data from traffic_data_0.json and traffic_data_1.json

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  FORCE REBUILD PROCESSED DATASET" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""

# Set environment variables
$env:FORCE_REBUILD_PROCESSED = "1"
$env:USE_DEMO_DATASET = "0"
$env:MAX_VEHICLE_ROWS = "0"

Write-Host "[1/3] Checking current parquet file..." -ForegroundColor Yellow
$parquetPath = "data\processed\unified_traffic.parquet"
if (Test-Path $parquetPath) {
    $fileInfo = Get-ChildItem $parquetPath
    Write-Host "Found: $parquetPath ($([math]::Round($fileInfo.Length/1MB, 2)) MB)" -ForegroundColor Yellow
    Write-Host "This will be replaced with full dataset." -ForegroundColor Yellow
} else {
    Write-Host "No existing parquet file found - will create new one." -ForegroundColor Green
}

Write-Host ""
Write-Host "[2/3] Starting rebuild (this may take 10-30 minutes)..." -ForegroundColor Yellow
Write-Host "Processing traffic_data_0.json and traffic_data_1.json (2GB total)..." -ForegroundColor Cyan
Write-Host ""

$pythonCmd = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $pythonCmd)) {
    $pythonCmd = "python"
}

& $pythonCmd scripts\bootstrap_assets.py

if ($LASTEXITCODE -ne 0) {
    Write-Host ""
    Write-Host "ERROR: Rebuild failed!" -ForegroundColor Red
    Write-Host "Check logs/bootstrap.log for details" -ForegroundColor Red
    exit 1
}

Write-Host ""
Write-Host "[3/3] Verifying rebuilt dataset..." -ForegroundColor Yellow
$fileInfo = Get-ChildItem $parquetPath
Write-Host "SUCCESS! Rebuilt parquet file:" -ForegroundColor Green
Write-Host "  Path: $parquetPath" -ForegroundColor Green
Write-Host "  Size: $([math]::Round($fileInfo.Length/1MB, 2)) MB" -ForegroundColor Green
Write-Host "  Last Modified: $($fileInfo.LastWriteTime)" -ForegroundColor Green
Write-Host ""
Write-Host "Now run: ./start.ps1" -ForegroundColor Green
