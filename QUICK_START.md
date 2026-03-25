# 🚀 HƯỚNG DẪN NHANH - Smart Traffic Analytics

## ⚡ Chạy ngay trong 3 bước

### Bước 1: Chuẩn bị dữ liệu
```bash
# Tạo thư mục data (nếu chưa có)
mkdir data

# Copy file dữ liệu vào thư mục data/
# Ví dụ: traffic_data_0.json, traffic_data_1.json
```

### Bước 2: Chạy hệ thống
**Cách 1 - Đơn giản nhất (Windows):**
```bash
# Double-click file này
run_simple.bat
```

**Cách 2 - Python script:**
```bash
python run_system.py
```

### Bước 3: Truy cập
- 🌐 **Web Dashboard**: http://localhost:8501
- 📡 **API Backend**: http://localhost:8000

---

## 🔧 Nếu gặp lỗi

### Lỗi: "Python không được cài đặt"
```bash
# Tải và cài Python từ: https://python.org
# Hoặc dùng Microsoft Store (Windows)
```

### Lỗi: "Không tìm thấy dữ liệu"
```bash
# Đảm bảo có file JSON trong thư mục data/
ls data/  # Linux/Mac
dir data\ # Windows
```

### Lỗi: "Port đã được sử dụng"
```bash
# Tìm và kill process đang dùng port 8000 hoặc 8501
netstat -ano | findstr :8000  # Windows
lsof -ti:8000 | xargs kill    # Linux/Mac
```

---

## 🧪 Test hệ thống

```bash
# Chạy test tự động
python test_system.py
```

---

## 📊 Tính năng chính

- ✅ **Smart Refresh**: Chỉ làm mới khi file thay đổi
- ✅ **Real-time Dashboard**: 5 trang phân tích đầy đủ
- ✅ **Interactive Map**: Bản đồ với vị trí xe
- ✅ **Data Export**: Xuất CSV, JSON
- ✅ **Alert System**: Cảnh báo vi phạm tự động

---

## 🎯 Cách sử dụng

1. **📊 Tổng quan**: Xem KPIs và biểu đồ tổng thể
2. **🗺️ Bản đồ**: Theo dõi vị trí xe real-time  
3. **📈 Phân tích**: Tra cứu, lọc, xuất dữ liệu
4. **🚗 Xe**: Phân tích performance theo loại xe
5. **⚠️ Cảnh báo**: Giám sát vi phạm và nhiên liệu

---

## 🔄 Auto-refresh

Hệ thống tự động làm mới khi:
- ➕ Thêm file mới vào `data/`
- ✏️ Sửa file hiện có
- 🗑️ Xóa file

**Không cần refresh thủ công!** 🎉

---

**Cần hỗ trợ?** Xem file `README_SMART.md` để biết chi tiết.