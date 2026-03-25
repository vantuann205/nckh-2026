# 🚦 HƯỚNG DẪN SỬ DỤNG HỆ THỐNG SMART TRAFFIC ANALYTICS

## 🎯 Tính năng chính

### ✨ File Watcher thông minh
- **Tự động phát hiện**: Hệ thống tự động phát hiện khi có file mới/sửa/xóa trong folder `data/`
- **Chỉ làm mới khi cần**: Không làm mới liên tục, chỉ khi có thay đổi thực sự
- **Tốc độ cao**: Cache thông minh, tải dữ liệu nhanh chóng

### 📊 Dashboard Real-time
- **Cập nhật tự động**: Giao diện tự động cập nhật khi có dữ liệu mới
- **Hiển thị đầy đủ**: Tất cả dữ liệu được thống kê chi tiết
- **Không cần F5**: Không cần refresh trang thủ công

## 🚀 CÁCH CHẠY HỆ THỐNG

### Bước 1: Chuẩn bị dữ liệu
```bash
# Tạo dữ liệu demo (nếu chưa có)
python create_demo_data.py
```

### Bước 2: Chạy hệ thống

#### Cách 1: Chạy tự động (Windows)
```bash
# Chạy file .bat và chọn tùy chọn
run_simple.bat
```

#### Cách 2: Chạy thủ công
```bash
# Terminal 1: Backend API
python run_backend.py

# Terminal 2: Frontend Dashboard  
python run_frontend.py
```

### Bước 3: Truy cập
- **Backend API**: http://localhost:8000
- **Frontend Dashboard**: http://localhost:8501
- **API Docs**: http://localhost:8000/docs

## 📁 Cấu trúc file

```
📁 analyse-data/
├── 🔧 smart_server.py          # Backend API với File Watcher
├── 🎨 optimized_app.py         # Frontend Streamlit tối ưu
├── 🚀 run_backend.py           # Chạy Backend
├── 🚀 run_frontend.py          # Chạy Frontend
├── 🚀 run_simple.bat           # Script Windows
├── 🔧 create_demo_data.py      # Tạo dữ liệu demo
├── 📋 requirements-optimized.txt # Dependencies
└── 📁 data/                    # Folder chứa dữ liệu JSON
    └── traffic_data_demo.json
```

## 🔄 Tính năng File Watcher

### Hoạt động tự động
- **Thêm file**: Thêm file JSON mới → Tự động load
- **Sửa file**: Sửa nội dung → Tự động cập nhật  
- **Xóa file**: Xóa file → Dữ liệu được cập nhật

### Test File Watcher
1. Chạy hệ thống
2. Thêm file mới vào folder `data/`
3. Xem console - sẽ thấy log "📁 File mới được tạo"
4. Dashboard tự động cập nhật sau 2-3 giây

## 📊 Các trang Dashboard

### 1. Dashboard chính
- KPI cards: Tổng xe, tốc độ TB, cảnh báo
- Biểu đồ lưu lượng theo giờ
- Phân bố loại xe
- Phân bố tốc độ
- Top quận có nhiều xe

### 2. Tra cứu dữ liệu
- Tìm kiếm theo ID xe, tên chủ xe
- Filter theo loại xe, quận
- Xuất CSV
- Hiển thị bảng chi tiết

### 3. Bản đồ
- Hiển thị vị trí xe trên bản đồ
- Màu sắc theo tốc độ
- Thống kê theo khu vực

## 🛠️ Troubleshooting

### Lỗi thường gặp

1. **"No module named 'streamlit'"**
   ```bash
   pip install streamlit pandas plotly requests fastapi uvicorn watchdog
   ```

2. **"Port already in use"**
   - Đổi port trong code hoặc kill process đang dùng port

3. **"Không tìm thấy dữ liệu"**
   - Chạy `python create_demo_data.py` để tạo dữ liệu demo
   - Hoặc copy file JSON vào folder `data/`

### Debug
- Xem console logs để biết trạng thái hệ thống
- Backend logs có prefix `[API]`
- File watcher events được log với timestamp

## 💡 Mẹo sử dụng

1. **Thêm dữ liệu mới**: Chỉ cần copy file JSON vào folder `data/`
2. **Xem API**: Truy cập http://localhost:8000/docs
3. **Tắt hệ thống**: Nhấn Ctrl+C trong console
4. **Làm mới thủ công**: Click nút "🔄 Làm mới" trong dashboard