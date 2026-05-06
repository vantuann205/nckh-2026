# Kết Quả Đánh Giá Model Phân Loại Ùn Tắc Giao Thông

## Thông Tin Model
- **Thuật toán**: Gradient Boosting Classifier (fallback từ XGBoost)
- **Tập dữ liệu**: Traffic Data (1000 rows, 80% train / 20% test)
  - Train set: 800 rows
  - Test set: 200 rows (169 normal, 31 congested)
- **Số lượng features**: 11 features (5 base + 6 engineered)
- **Target**: Binary Classification (Normal vs Congested)
- **Class imbalance**: ~84.5% Normal, ~15.5% Congested

## Base Features
1. `speed_kmph` - Tốc độ xe (km/h)
2. `weather_temp_c` - Nhiệt độ thời tiết (°C)
3. `humidity_pct` - Độ ẩm (%)
4. `accident_severity` - Mức độ nghiêm trọng tai nạn
5. `congestion_km` - Độ dài ùn tắc (km)

## Engineered Features
6. `speed_sq` - Bình phương tốc độ (speed²)
7. `speed_inv` - Nghịch đảo tốc độ (1/speed)
8. `weather_risk` - Rủi ro thời tiết (temp × humidity)
9. `accident_x_congestion` - Tương tác tai nạn × ùn tắc
10. `low_speed_flag` - Cờ tốc độ thấp (< 20 km/h)
11. `very_low_speed_flag` - Cờ tốc độ rất thấp (< 10 km/h)

## Kết Quả Đánh Giá

### Bảng Metrics Chi Tiết

| Chỉ số | Nhãn "Bình thường" (Normal) | Nhãn "Ùn tắc" (Congested) |
|--------|----------------------------|---------------------------|
| **Precision** | 1.00 | 1.00 |
| **Recall** | 1.00 | 1.00 |
| **F1-Score** | 1.00 | 1.00 |
| **Support** | 169 | 31 |

### Metrics Tổng Thể

| Metric | Giá trị |
|--------|---------|
| **Accuracy** | 1.0000 (100%) |
| **AUC-ROC** | 1.0000 |
| **Macro Avg Precision** | 1.00 |
| **Macro Avg Recall** | 1.00 |
| **Macro Avg F1-Score** | 1.00 |
| **Weighted Avg Precision** | 1.00 |
| **Weighted Avg Recall** | 1.00 |
| **Weighted Avg F1-Score** | 1.00 |

## Giải Thích Metrics

### 1. Precision (Độ chính xác)
- **Công thức**: TP / (TP + FP)
- **Ý nghĩa**: Trong số các trường hợp model dự đoán là "Ùn tắc", có bao nhiêu % thực sự ùn tắc
- **Kết quả**: 100% - Model không có False Positive

### 2. Recall (Độ phủ / Sensitivity)
- **Công thức**: TP / (TP + FN)
- **Ý nghĩa**: Trong số các trường hợp thực tế ùn tắc, model phát hiện được bao nhiêu %
- **Kết quả**: 100% - Model không bỏ sót trường hợp nào (không có False Negative)

### 3. F1-Score (Điểm F1)
- **Công thức**: 2 × (Precision × Recall) / (Precision + Recall)
- **Ý nghĩa**: Trung bình điều hòa giữa Precision và Recall
- **Kết quả**: 1.00 - Cân bằng hoàn hảo

### 4. AUC-ROC (Area Under ROC Curve)
- **Ý nghĩa**: Khả năng phân biệt giữa 2 lớp (Normal vs Congested)
- **Kết quả**: 1.0000 - Model phân loại hoàn hảo
- **Đánh giá**: 
  - 0.9 - 1.0: Xuất sắc
  - 0.8 - 0.9: Tốt
  - 0.7 - 0.8: Chấp nhận được
  - < 0.7: Kém

## Confusion Matrix (Dự đoán)

|  | Predicted Normal | Predicted Congested |
|---|-----------------|---------------------|
| **Actual Normal** | True Negative (TN) | False Positive (FP) = 0 |
| **Actual Congested** | False Negative (FN) = 0 | True Positive (TP) |

## Xử Lý Imbalanced Data

Model sử dụng `scale_pos_weight` để xử lý dữ liệu mất cân bằng:

```python
scale_pos_weight = neg_count / pos_count
```

Điều này giúp model không bị thiên vị về lớp đa số.

## Hyperparameters XGBoost

| Parameter | Giá trị | Mục đích |
|-----------|---------|----------|
| `n_estimators` | 300 | Số lượng cây quyết định |
| `max_depth` | 6 | Độ sâu tối đa của cây |
| `learning_rate` | 0.05 | Tốc độ học |
| `subsample` | 0.8 | Tỷ lệ mẫu con cho mỗi cây |
| `colsample_bytree` | 0.8 | Tỷ lệ features cho mỗi cây |
| `min_child_weight` | 3 | Trọng số tối thiểu của node con |
| `gamma` | 0.1 | Ngưỡng giảm loss để split |
| `reg_alpha` | 0.1 | L1 regularization |
| `reg_lambda` | 1.0 | L2 regularization |

## Feature Importance

Model Gradient Boosting tự động tính toán độ quan trọng của từng feature:

| Rank | Feature | Importance | Ý nghĩa |
|------|---------|-----------|---------|
| 1 | `speed_inv` | 0.4873 (48.73%) | Nghịch đảo tốc độ - quan trọng nhất |
| 2 | `low_speed_flag` | 0.4467 (44.67%) | Cờ tốc độ thấp (< 20 km/h) |
| 3 | `speed_kmph` | 0.0457 (4.57%) | Tốc độ gốc |
| 4 | `speed_sq` | 0.0203 (2.03%) | Bình phương tốc độ |
| 5 | `weather_risk` | ~0.0000 | Rủi ro thời tiết (không đóng góp) |
| 6-11 | Các features khác | 0.0000 | Không đóng góp |

**Nhận xét**: 
- 93.4% độ quan trọng đến từ 2 features: `speed_inv` và `low_speed_flag`
- Các features liên quan đến tai nạn và thời tiết không đóng góp vào model
- Model chủ yếu dựa vào tốc độ để phân loại

## Kết Luận

Model đạt hiệu suất **hoàn hảo** (100%) trên tập test với:
- ✅ Không có False Positive (không cảnh báo nhầm)
- ✅ Không có False Negative (không bỏ sót)
- ✅ AUC-ROC = 1.0000 (phân loại hoàn hảo)

### ⚠️ Phân Tích Kết Quả

**Tại sao đạt 100% accuracy?**

1. **Target được tính toán từ features**: 
   - `congestion_level` được tính trực tiếp từ `speed_kmph`
   - Model học được công thức: `if speed < 20 → congested`
   - Đây là **data leakage** - model học được rule thay vì pattern thực tế

2. **Features có tương quan hoàn hảo với target**:
   - `speed_inv` (1/speed) và `low_speed_flag` (speed < 20) chiếm 93.4% importance
   - Khi speed < 20 → congested (theo định nghĩa)
   - Model chỉ cần học ngưỡng này

3. **Dataset nhỏ và đơn giản**:
   - Chỉ 1000 rows, test set 200 rows
   - Pattern rất rõ ràng, không có noise
   - Không có trường hợp biên (edge cases)

### 🔴 Vấn Đề và Khuyến Nghị

**Vấn đề**:
- ❌ **Data Leakage**: Target được tính từ features → model học rule thay vì dự đoán
- ❌ **Overfitting risk**: Kết quả quá tốt trên tập nhỏ, có thể không generalize
- ❌ **Thiếu features độc lập**: Tai nạn, thời tiết không đóng góp (importance = 0)

**Khuyến nghị cải thiện**:

1. **Tách biệt target và features**:
   - Sử dụng target thực tế từ dữ liệu lịch sử (không tính toán)
   - Hoặc dự đoán trước 15-30 phút (time-series forecasting)

2. **Tăng kích thước dataset**:
   - Thu thập thêm dữ liệu (> 10,000 rows)
   - Bao gồm nhiều điều kiện khác nhau (giờ cao điểm, sự kiện, thời tiết xấu)

3. **Cross-validation**:
   - Sử dụng K-fold cross-validation (k=5 hoặc 10)
   - Đánh giá trên nhiều tập test khác nhau

4. **Thêm features độc lập**:
   - Giờ trong ngày, ngày trong tuần
   - Sự kiện đặc biệt (lễ hội, concert)
   - Dữ liệu lịch sử (tốc độ trung bình cùng giờ)

5. **Kiểm tra trên dữ liệu thực tế**:
   - Deploy model và theo dõi performance
   - So sánh dự đoán với tình trạng thực tế
   - Tính toán metrics trên production data

### 📊 Kết Quả Thực Tế Mong Đợi

Với dataset và features tốt hơn, kết quả thực tế nên là:
- **Accuracy**: 85-92%
- **Precision (Congested)**: 75-85%
- **Recall (Congested)**: 70-80%
- **AUC-ROC**: 0.88-0.95

Đây là mức hiệu suất tốt cho bài toán phân loại ùn tắc giao thông trong thực tế.

## Công Thức Tính Congestion Level

```python
if speed >= 40:
    congestion_level = "Low" (Bình thường)
elif 20 <= speed < 40:
    congestion_level = "Moderate" (Chậm)
else:  # speed < 20
    congestion_level = "High" (Ùn tắc)
```

Model dự đoán `congestion_flag = 1` khi `congestion_level == "High"`.
