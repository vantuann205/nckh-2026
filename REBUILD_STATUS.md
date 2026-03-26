# Full Dataset Rebuild - Status & Monitoring Guide

## What's Happening Right Now (19:15 UTC+7)

Your system is running the full dataset rebuild with the command:
```powershell
$env:FORCE_REBUILD_PROCESSED="1"; ./start.ps1
```

**Current Process:**
1. ✅ Docker services (Redis/Postgres) - Started
2. 🔄 **Bootstrap rebuilding parquet** - IN PROGRESS
   - Processing: traffic_data_0.json + traffic_data_1.json (~2GB)
   - Expected time: **10-30 minutes** (JSON parsing is CPU-intensive)
3. ⏳ FastAPI backend - Will start after bootstrap completes
4. ⏳ Producer & Dashboard - Will start after API is ready

**Why the wait?**
- The bootstrap must finish before API can start
- API startup is blocked during bootstrap (1 process holds the log output)
- This is normal and expected behavior

---

## Monitoring Progress

### Option 1: Watch Log File (Simple)
```powershell
# Watch bootstrap log in real-time
Get-Content logs/bootstrap.log -Wait
```

### Option 2: Monitor File Growth (Recommended)
You created a monitoring script for this:
```powershell
# Run in a SEPARATE PowerShell terminal
./monitor_rebuild.ps1
```

This script will show:
- Current parquet file size (growing from 0 to several GB)
- Growth rate (MB per 10 seconds)
- Elapsed time since rebuild started

### Option 3: Check File Size Manually
```powershell
Get-ChildItem data\processed\unified_traffic.parquet | Select-Object Name, Length, LastWriteTime
```

Expected progression:
- Start: ~90KB (old demo file)
- Mid-process: Grows continuously (500MB → 1GB → 2GB+)
- Complete: 3-5 GB with millions of rows
- File timestamp updates as data is written

---

## What to Do While Waiting

### Monitor Progress (Recommended)
Open a new PowerShell terminal and run:
```powershell
./monitor_rebuild.ps1
```

### Check Logs
```powershell
# View bootstrap progress
Get-Content logs/bootstrap.log -Tail 20

# View startup progress
Get-Content logs/startup.log -Tail 20
```

### Do Other Work
The rebuild can take 15-30 minutes. You can:
- Work on other tasks
- Let it run in background
- Come back in 20 minutes to check progress

---

## When Rebuild Completes

You'll see:
1. **In logs:** `"Processed dataset saved: ... (XXXXXXX rows)"`
2. **In parquet file:** Size is now 3-5 GB
3. **In bootstrap.log:** `"Bootstrap assets ready"`
4. **API starts:** Next startup log phase begins
5. **Dashboard loads:** Frontend will open at http://localhost:3000

---

## If Something Goes Wrong

### If Bootstrap Takes Too Long (>45 minutes)
1. Check CPU usage: Is Python at 100%? (Normal for JSON parsing)
2. Check available disk: Do you have 10GB free space?
3. Check logs/bootstrap.log for errors

### If Bootstrap Fails
The error will be in logs/bootstrap.log. Common issues:
- **Out of memory:** File too large for your system
- **Syntax error in JSON:** Data corruption
- **Disk full:** Not enough space for parquet

**To retry:**
```powershell
# Stop current process (Ctrl+C in the terminal)
# Then run the force rebuild script:
./force_rebuild.ps1
```

This script will:
1. Show parquet file status
2. Run bootstrap with explicit rebuild flag
3. Show final parquet size and success message

---

## After Rebuild Completes

Once API and producer are running:

### Check Dashboard
```powershell
Start-Process http://localhost:3000
```

### Monitor Real-Time Data Flow
```powershell
# Check producer output (batch ranges)
Get-Content logs/producer.log -Tail 20

# Check API requests
Get-Content logs/api.log -Tail 20
```

### Verify Data Loading
```powershell
# Check total vehicle count in Redis
docker exec traffic-redis redis-cli GET "traffic:summary" | ConvertFrom-Json
```

---

## Reference: Helper Scripts

You now have two helper scripts:

### 1. force_rebuild.ps1
Force a rebuild of processed dataset (useful if rebuild fails):
```powershell
./force_rebuild.ps1
```

### 2. monitor_rebuild.ps1  
Watch parquet file growth in real-time:
```powershell
./monitor_rebuild.ps1
```

---

## Expected Final Outcome

When complete, your system will have:
- ✅ **Full dataset loaded:** All vehicles from traffic_data_0.json + _1.json
- ✅ **Persistent storage:** Data in parquet + Postgres survives restarts
- ✅ **Resume capability:** Producer resumes from last offset (no replay)
- ✅ **High throughput:** Batch ingest at 50+ records/request
- ✅ **Real-time updates:** Dashboard updates every WebSocket message

---

## Timeline

| Time | Action | Status |
|------|--------|--------|
| 19:15 | Startup with rebuild flag | ✅ Initiated |
| 19:15-19:40+ | Bootstrap rebuilding parquet | 🔄 IN PROGRESS (10-30 min) |
| ~19:40 | Bootstrap complete, API starts | ⏳ Pending |
| ~19:42 | Ready for traffic (API + producer running) | ⏳ Pending |

---

**You can safely assume it's working if:**
- Parquet file is growing (monitor_rebuild.ps1 shows +MB every 10 sec)
- CPU usage from Python is high (JSON parsing is intensive)
- No error messages in logs/bootstrap.log

**Questions?** Check logs/bootstrap.log for specific error messages.
