# Monitor parquet file growth during rebuild
# Run this in a separate PowerShell terminal to watch progress

Write-Host ""
Write-Host "========================================" -ForegroundColor Cyan
Write-Host "  REBUILD PROGRESS MONITOR" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Checking parquet file growth every 10 seconds..." -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop" -ForegroundColor Yellow
Write-Host ""

$parquetPath = "data\processed\unified_traffic.parquet"
$lastSize = 0
$startTime = Get-Date

while ($true) {
    if (Test-Path $parquetPath) {
        $fileInfo = Get-ChildItem $parquetPath
        $currentSize = $fileInfo.Length
        $sizeMB = [math]::Round($currentSize / 1MB, 2)
        $sizeGB = [math]::Round($currentSize / 1GB, 4)
        
        $elapsed = (Get-Date) - $startTime
        $elapsedMin = [math]::Round($elapsed.TotalMinutes, 1)
        
        if ($currentSize -gt $lastSize) {
            $growth = $currentSize - $lastSize
            $growthMB = [math]::Round($growth / 1MB, 2)
            Write-Host "[${elapsedMin}min] Parquet: ${sizeMB} MB (${sizeGB} GB) | Growth: +${growthMB} MB" -ForegroundColor Green
            $lastSize = $currentSize
        } else {
            Write-Host "[${elapsedMin}min] Parquet: ${sizeMB} MB (${sizeGB} GB) | No change" -ForegroundColor Yellow
        }
    } else {
        Write-Host "Waiting for parquet file to be created..." -ForegroundColor Yellow
    }
    
    Start-Sleep -Seconds 10
}
