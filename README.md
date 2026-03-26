# Traffic Pipeline — Realtime Dashboard

Hệ thống giám sát giao thông realtime: load dữ liệu JSON → Redis → WebSocket → Dashboard.

## Yêu cầu

| Phần mềm | Tải về |
|---|---|
| Python 3.9+ | https://python.org/downloads (tick **Add to PATH**) |
| Node.js 18+ (LTS) | https://nodejs.org |
| Docker Desktop | https://www.docker.com/products/docker-desktop |

## Chạy lần đầu (người mới)

```powershell
.\setup.ps1
```

Script tự động:
1. Kiểm tra Python / Node / Docker
2. Cài tất cả Python packages (`requirements.txt`)
3. Cài Node packages (`dashboard/`)
4. Khởi động Redis + Postgres qua Docker
5. Khởi động FastAPI backend (port 8000)
6. Khởi động Vite frontend (port 3000)
7. Mở trình duyệt tự động

## Chạy lại (đã cài rồi)

```powershell
.\setup.ps1 -SkipInstall
```

Hoặc dùng script gốc:

```powershell
.\start.ps1
```

## Thêm dữ liệu

Đặt file `traffic_data_*.json` vào folder `data/` — hệ thống tự detect và load ngay, không cần restart.

## Cấu trúc

```
data/               ← đặt file JSON vào đây
backend/main.py     ← FastAPI API + WebSocket
stream_processing/  ← loader tốc độ cao (MSET ~300k rec/s)
dashboard/          ← Vite + Chart.js frontend
storage/            ← Redis client
docker-compose.yml  ← Redis + Postgres
setup.ps1           ← one-click setup
```

## Endpoints

| URL | Mô tả |
|---|---|
| http://localhost:3000 | Dashboard |
| http://localhost:8000/docs | API Swagger |
| http://localhost:8000/traffic/loading-progress | Tiến độ load dữ liệu |
| http://localhost:8000/traffic/summary | KPI tổng quan |
