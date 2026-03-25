#!/bin/bash

echo "========================================"
echo "   SMART TRAFFIC ANALYTICS SYSTEM"
echo "========================================"
echo ""
echo "Đang khởi động hệ thống..."
echo ""

# Kiểm tra Python
if ! command -v python3 &> /dev/null; then
    echo "Lỗi: Python3 chưa được cài đặt!"
    echo "Vui lòng cài đặt Python3"
    exit 1
fi

# Chạy script chính
python3 start_system.py

echo ""
echo "Hệ thống đã dừng!"