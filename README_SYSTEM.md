# 🚦 Smart Traffic Analytics System

## Tính năng chính

### 🔥 File Watcher & Smart Caching
- **Tự động phát hiện thay đổi**: Hệ thống sẽ tự động phát hiện khi có file mới được thêm/xóa/sửa trong folder `data/`
- **Chỉ làm mới khi cần**: Không làm mới liên tục, chỉ khi có thay đổi thực sự
- **Tốc độ cao**: Cache thông minh giúp tải dữ liệu nhanh chóng

### 📊 Dashboard Real-time
- **Cập nhật tự động**: Giao diện tự động cập nhật khi có dữ liệu mới
- **Không cần F5**: Không cần refresh trang thủ công
- **Hiển thị đầy đủ**: Tất cả dữ liệu được thống kê và hiển thị

### 🗺️ Bản đồ tương tác
- **Hiển thị vị trí xe**: Bản đồ real-time với vị trí các phương tiện
- **Phân loại theo màu**: Màu sắc khác nhau theo tốc độ và tình trạng
- **Thông tin chi tiết**: Click vào điểm để xem thông tin chi tiết

## Cài đặt và chạy

### Bước 1: Cài đặt dependencies
```bash
pip install -r requirements-optimized.txt
```

### Bước 2: Chuẩn bị dữ liệu
- Đặt các file `traffic_data_*.json` vào folder `data/`
- Hệ thống sẽ tự động load tất cả file JSON trong folder này

### Bước 3: Khởi chạy hệ thống

#### Windows:
```bash
# Cách 1: Chạy file .bat
start_system.bat

# Cách 2: Chạy Python script
python start_system.py
```

#### Linux/Mac:
```bash
# Cách 1: Chạy shell script
./start_system.sh

# Cách 2: Chạy Python script
python3 start_system.py
```

### Bước 4: Truy cập hệ thống
- **Backend API**: http://localhost:8000
- **Frontend Dashboard**: http://localhost:8501

## Cấu trúc hệ thống

```
📁 analyse-data/
├── 📄 smart_server.py          # Backend API với File Watcher
├── 📄 optimized_app.py         # Frontend Streamlit tối ưu
├── 📄 start_system.py          # Script khởi chạy tự động
├── 📄 start_system.bat         # Script Windows
├── 📄 start_system.sh          # Script Linux/Mac
├── 📄 requirements-optimized.txt # Dependencies
├── 📁 data/                    # Folder chứa dữ liệu JSON
│   ├── traffic_data_0.json
│   ├── traffic_data_1.json
│   └── ...
└── 📁 pages/                   # Các trang Streamlit cũ (backup)
```

## API Endpoints

### Backend API (Port 8000)
- `GET /` - Thông tin API
- `GET /api/status` - Trạng thái hệ thống
- `GET /api/summary` - Tổng quan dữ liệu
- `GET /api/stats/flow` - Thống kê theo giờ
- `GET /api/stats/types` - Thống kê loại xe
- `GET /api/stats/speed` - Phân bố tốc độ
- `GET /api/stats/weather` - Thống kê thời tiết
- `GET /api/stats/districts` - Thống kê theo quận
- `GET /api/explorer` - Tra cứu dữ liệu với filter
- `GET /api/map` - Dữ liệu cho bản đồ
- `POST /api/refresh` - Làm mới thủ công

## Tính năng File Watcher

### Hoạt động tự động
- **Thêm file mới**: Khi bạn thêm file JSON mới vào folder `data/`, hệ thống tự động load và cập nhật
- **Xóa file**: Khi xóa file, dữ liệu sẽ được cập nhật ngay lập tức
- **Sửa file**: Khi sửa nội dung file, hệ thống phát hiện và reload

### Debounce Protection
- Hệ thống có cơ chế debounce 2 giây để tránh reload quá nhiều lần
- Chỉ reload một lần duy nhất khi có nhiều thay đổi liên tiếp

## Tối ưu hiệu suất

### Smart Caching
- Cache dữ liệu trong memory để truy cập nhanh
- Chỉ reload khi có thay đổi thực sự
- API response được cache 30 giây

### Lazy Loading
- Dữ liệu chỉ được load khi cần thiết
- Pagination cho dữ liệu lớn
- Limit số điểm hiển thị trên bản đồ

## Troubleshooting

### Lỗi thường gặp

1. **Port đã được sử dụng**
   ```
   Lỗi: [Errno 98] Address already in use
   Giải pháp: Đổi port hoặc kill process đang dùng port
   ```

2. **Không tìm thấy dữ liệu**
   ```
   Lỗi: Không tìm thấy file dữ liệu
   Giải pháp: Đặt file traffic_data_*.json vào folder data/
   ```

3. **Lỗi import module**
   ```
   Lỗi: ModuleNotFoundError
   Giải pháp: pip install -r requirements-optimized.txt
   ```

### Debug Mode
Để bật debug mode, thêm biến môi trường:
```bash
export DEBUG=1
python start_system.py
```

## Monitoring

### Logs
- Backend logs hiển thị trong console với prefix `[API]`
- Frontend logs hiển thị với prefix `[WEB]`
- File watcher events được log với timestamp

### Health Check
- Truy cập `http://localhost:8000/api/status` để kiểm tra trạng thái
- Dashboard hiển thị indicator trạng thái hệ thống

## Mở rộng

### Thêm tính năng mới
1. Thêm endpoint mới trong `smart_server.py`
2. Thêm trang mới trong `optimized_app.py`
3. Cập nhật navigation trong sidebar

### Tích hợp database
- Có thể thay thế file JSON bằng database
- Chỉ cần sửa hàm `load_all_data()` trong `smart_server.py`

### Deploy production
- Sử dụng Gunicorn cho backend
- Nginx reverse proxy
- Docker containerization

## Liên hệ hỗ trợ

Nếu gặp vấn đề, vui lòng:
1. Kiểm tra logs trong console
2. Đảm bảo tất cả dependencies đã được cài đặt
3. Kiểm tra folder `data/` có chứa file JSON
4. Restart hệ thống bằng Ctrl+C và chạy lại