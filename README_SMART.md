# 🚦 Smart Traffic Analytics System

Hệ thống phân tích giao thông thông minh với **File Watcher** và **Smart Caching** - chỉ làm mới khi có thay đổi thực sự trong dữ liệu.

## ✨ Tính năng chính

- 🔄 **Smart Refresh**: Chỉ làm mới khi có thay đổi file trong folder `data`
- ⚡ **Tốc độ cao**: Caching thông minh, không làm mới liên tục
- 📊 **Dashboard đầy đủ**: Hiển thị tất cả dữ liệu, không có chỗ trống
- 🗺️ **Bản đồ tương tác**: Hiển thị vị trí xe theo thời gian thực
- 📈 **Phân tích chi tiết**: Tra cứu, lọc, xuất dữ liệu
- ⚠️ **Cảnh báo thông minh**: Giám sát vi phạm và nhiên liệu

## 🚀 Cách chạy nhanh

### Cách 1: Chạy bằng file .bat (Windows)
```bash
# Chỉ cần double-click file này
run_simple.bat
```

### Cách 2: Chạy bằng Python script
```bash
python run_system.py
```

### Cách 3: Chạy thủ công từng bước

1. **Cài đặt thư viện:**
```bash
pip install -r requirements_smart.txt
```

2. **Khởi động Backend API:**
```bash
python smart_server.py
```

3. **Khởi động Web Dashboard (cửa sổ mới):**
```bash
streamlit run optimized_app.py
```

## 📁 Cấu trúc dữ liệu

Đặt các file dữ liệu JSON vào thư mục `data/`:
```
data/
├── traffic_data_0.json
├── traffic_data_1.json
└── ...
```

## 🌐 Truy cập hệ thống

- **🌐 Web Dashboard**: http://localhost:8501
- **📡 API Backend**: http://localhost:8000
- **📚 API Docs**: http://localhost:8000/docs

## 🔧 Cách hoạt động

### File Watcher System
- Giám sát thư mục `data/` 24/7
- Tự động detect khi có file mới/sửa/xóa
- Chỉ làm mới cache khi cần thiết
- Debounce 2 giây để tránh refresh liên tục

### Smart Caching
- Cache dữ liệu trong memory
- API endpoints với cache TTL
- Streamlit caching cho performance
- Chỉ reload khi có thay đổi thực sự

### Architecture
```
📁 data/ (File Watcher)
    ↓
🔧 smart_server.py (FastAPI + Cache)
    ↓
🌐 optimized_app.py (Streamlit Dashboard)
```

## 📊 Các trang chính

1. **📊 Tổng quan**: KPIs, charts, thống kê tổng thể
2. **🗺️ Bản đồ**: Vị trí xe, heatmap tốc độ
3. **📈 Phân tích dữ liệu**: Tra cứu, lọc, xuất CSV
4. **🚗 Phân tích xe**: Performance theo loại xe
5. **⚠️ Cảnh báo**: Vi phạm tốc độ, nhiên liệu thấp

## 🛠️ Troubleshooting

### Lỗi không kết nối API
```bash
# Kiểm tra port 8000 có bị chiếm không
netstat -an | findstr :8000

# Restart backend
python smart_server.py
```

### Lỗi không tìm thấy dữ liệu
- Đảm bảo có file `traffic_data_*.json` trong folder `data/`
- Kiểm tra format JSON hợp lệ
- Xem log trong console

### Performance chậm
- Giảm `limit` trong API calls
- Tăng cache TTL trong code
- Sử dụng sample data cho charts lớn

## 🔄 Auto-refresh Logic

```python
# File thay đổi → Watchdog event → Refresh cache → Update UI
data/traffic_data_0.json (modified)
    ↓
FileSystemEventHandler.on_modified()
    ↓
refresh_cache() (background thread)
    ↓
Streamlit auto-rerun (if enabled)
```

## 📈 Performance Tips

1. **Dữ liệu lớn**: Sử dụng pagination và sampling
2. **Charts**: Limit số điểm hiển thị
3. **Maps**: Giới hạn markers (< 2000)
4. **Cache**: Tăng TTL cho dữ liệu ít thay đổi

## 🎯 Tính năng nâng cao

- **Real-time monitoring**: File watcher 24/7
- **Smart caching**: Memory + HTTP cache
- **Responsive design**: Mobile-friendly
- **Export data**: CSV, JSON download
- **Interactive maps**: Leaflet integration
- **Alert system**: Violation detection

---

**🚦 Smart Traffic Analytics** - Powered by FastAPI, Streamlit & Python ⚡