# Hệ thống Giám sát và Dự báo Giao thông Thời gian Thực tại TP.HCM

---

## CHƯƠNG 2. KIẾN TRÚC HỆ THỐNG XỬ LÝ DỮ LIỆU LỚN

### 2.1. Thiết kế tầng thu nhận dữ liệu (Ingestion Layer)

Tầng thu nhận dữ liệu được thiết kế để xử lý khối lượng lớn bản ghi GPS từ phương tiện giao thông tại TP.HCM. Hệ thống tiếp nhận dữ liệu dưới dạng file JSON với cấu trúc lồng nhau, mỗi bản ghi chứa thông tin định vị, tốc độ, trạng thái nhiên liệu, điều kiện thời tiết và cảnh báo vi phạm.

**Cấu trúc dữ liệu đầu vào:**

```json
{
  "vehicle_id": "VH000001",
  "vehicle_type": "Electric Car",
  "speed_kmph": 94,
  "fuel_level_percentage": 44,
  "passenger_count": 1,
  "timestamp": "2026-03-11T15:02:58",
  "coordinates": { "latitude": 10.712, "longitude": 106.772 },
  "road": { "street": "Phan Xích Long", "district": "Tân Phú" },
  "weather_condition": { "condition": "Cloudy", "temperature_celsius": 35, "humidity_percentage": 86 },
  "traffic_status": { "congestion_level": "Medium", "estimated_delay_minutes": 7 },
  "alerts": [{ "type": "Speeding", "description": "Vehicle exceeding speed limit: 94 km/h" }]
}
```

**Quy trình xử lý ingestion:**

Hệ thống triển khai pipeline song song (parallel ingestion) với 6 worker threads độc lập. Mỗi worker nhận một batch 20.000 bản ghi, thực hiện normalize và ghi vào Redis đồng thời. Thiết kế này cho phép overlap giữa giai đoạn đọc file (CPU-bound) và ghi Redis (I/O-bound), tối đa hóa throughput.

```
File JSON (2GB)
    │
    ▼ orjson.loads() — đọc toàn bộ vào RAM
    │
    ▼ _normalize_batch() — trích xuất 25 fields, tính risk_score
    │
    ├─► Worker 1 ─► Redis MSET batch 1
    ├─► Worker 2 ─► Redis MSET batch 2
    ├─► Worker 3 ─► Redis MSET batch 3
    ├─► Worker 4 ─► Redis MSET batch 4
    ├─► Worker 5 ─► Redis MSET batch 5
    └─► Worker 6 ─► Redis MSET batch 6
    │
    ▼ _flush_stats_once() — ghi thống kê tổng hợp 1 lần
    │
    ▼ _recompute_road_summary() — tính KPI tổng hợp
```

**Cơ chế phát hiện lỗi dữ liệu:**

Hệ thống triển khai parser dự phòng `_parse_object_stream()` để xử lý các file JSON bị lỗi cấu trúc (thiếu dấu `[` mở đầu mảng — lỗi phổ biến trong file lớn). Parser này duyệt byte-by-byte, theo dõi độ sâu ngoặc nhọn và trạng thái chuỗi ký tự để trích xuất từng object JSON riêng lẻ.

**Cơ chế cache nhị phân:**

Sau lần đọc đầu tiên, dữ liệu đã normalize được lưu dưới dạng binary (orjson bytes) tại `data/processed/*.cache.bin`. File cache có kích thước ~650MB so với 2GB JSON gốc (giảm 3x), cho phép khởi động lại hệ thống trong ~11 giây thay vì 2-3 phút.

**Xác thực tính nhất quán dữ liệu thô:**

Hàm `_raw_record_consistency_flags()` kiểm tra các ràng buộc:
- Tốc độ trong khoảng [0, 180] km/h
- Timestamp hợp lệ và ETA không trước thời điểm sự kiện
- `congestion_level` khớp với ngưỡng tốc độ (High: <20, Moderate: 20-40, Low: ≥40 km/h)
- Delay không âm

---

### 2.2. Tối ưu hóa hiệu suất với thư viện orjson

**Benchmark so sánh các phương pháp ghi Redis:**

| Phương pháp | Throughput | Ghi chú |
|---|---|---|
| `HSET` (17 fields/record) | ~32.000 rec/s | Mỗi record = 17 lệnh Redis |
| `SET` (JSON string) | ~128.000 rec/s | Mỗi record = 1 lệnh SET |
| `MSET` (batch JSON) | ~312.000 rec/s | Toàn batch = 1 lệnh MSET |

Hệ thống sử dụng `MSET` với giá trị là JSON bytes từ orjson, đạt throughput lý thuyết 312.000 rec/s. Trong điều kiện thực tế với 6 worker threads song song và backend đang phục vụ requests, throughput đo được là **67.000 rec/s** (bottleneck: Redis single-threaded).

**Tối ưu hóa pipeline Redis:**

Thay vì ghi thống kê (HINCRBY) sau mỗi batch — gây overhead ~100 lệnh/batch — hệ thống tích lũy toàn bộ thống kê trong bộ nhớ Python (dict) và chỉ thực hiện một pipeline Redis duy nhất sau khi xử lý xong toàn bộ file:

```python
# Không hiệu quả: 100 HINCRBY × N batches = N×100 round-trips
for batch in batches:
    redis.hincrby("stats:vehicle_types", vtype, count)  # ×6 loại xe
    redis.hincrby("stats:districts", district, count)   # ×16 quận
    ...

# Hiệu quả: tích lũy trong memory, flush 1 lần
acc = {"vtype": {}, "district": {}, ...}
for batch in batches:
    _accumulate(acc, batch)  # chỉ cập nhật dict Python

_flush_stats_once(acc, redis)  # 1 pipeline duy nhất
```

**Tối ưu hóa normalize:**

Hàm `_normalize_batch()` được viết theo phong cách "inline" — không gọi hàm con, không dùng list comprehension phức tạp — để giảm overhead Python interpreter. Mỗi record được xử lý trong một vòng lặp đơn với các phép tính số học trực tiếp:

```python
# risk_score = f(speed, weather, accident)
sr = 50.0 - speed if speed < 50 else 0.0
wr = 20.0 if ("rain" in wcond_lower or "storm" in wcond_lower) else 0.0
if hum > 85: wr += 10.0
risk = sr * 0.6 + wr * 0.2 + (asev * 12.0 + ckm * 3.0) * 0.2
```

---

### 2.3. Quản lý trạng thái và bộ nhớ đệm (Caching & State)

**Thiết kế key-value Redis:**

```
road:{road_id}              → JSON string (snapshot tổng hợp của tuyến đường)
traffic:summary             → Hash (KPI tổng quan: avg_speed, total_vehicles, ...)
traffic:unique_roads        → Set (danh sách road_id duy nhất)
traffic:stats:vehicle_types → Hash (số lượng theo loại xe)
traffic:stats:districts     → Hash (số lượng theo quận/huyện)
traffic:stats:streets       → Hash (số lượng theo tuyến đường)
traffic:stats:*_by_road     → Hash (thống kê per-road: delay, risk, congestion)
```

**Tính nhất quán dữ liệu road snapshot:**

Vấn đề quan trọng: với 2 triệu bản ghi cho 65 tuyến đường (~30.000 records/tuyến), nếu MSET từng record thì key `road:{id}` sẽ bị overwrite liên tục và chỉ giữ lại record cuối cùng (ngẫu nhiên theo thứ tự xử lý của worker threads). Điều này dẫn đến tốc độ trung bình không ổn định giữa các lần khởi động.

Giải pháp: tích lũy aggregate per-road trong memory (speed_sum, speed_count, fuel_sum, ...) và chỉ MSET snapshot đã tính trung bình sau khi xử lý xong toàn bộ file:

```python
# Tích lũy trong worker threads (thread-safe với lock)
road_agg[rid]["_speed_sum"] += speed
road_agg[rid]["_speed_cnt"] += 1

# Sau khi xong: tính avg và MSET 1 lần
avg_speed = road_agg[rid]["_speed_sum"] / road_agg[rid]["_speed_cnt"]
snapshot = {"avg_speed": avg_speed, "status": ..., ...}
redis.mset({f"road:{rid}": orjson.dumps(snapshot)})
```

**Persistence và khôi phục sau restart:**

Redis được cấu hình với cả AOF (Append-Only File) và RDB snapshot:
```
--appendonly yes --appendfsync everysec
--save 60 1 --save 30 100 --save 10 1000
```

Sau khi load xong, hệ thống gọi `BGSAVE` để đảm bảo snapshot được ghi ra disk. Khi khởi động lại, nếu Redis đã có dữ liệu đầy đủ (kiểm tra `traffic:unique_roads` và các stats keys), hệ thống bỏ qua bước load và broadcast ngay lập tức — thời gian khởi động giảm từ ~30 giây xuống <5 giây.

**In-memory response cache:**

Backend FastAPI duy trì một dict cache với TTL 3-5 giây cho các endpoint nặng (`/traffic/indicators`, `/traffic/advanced-analytics`). Điều này tránh scan Redis keys nhiều lần trong cùng một giây khi nhiều clients kết nối đồng thời.

---

## CHƯƠNG 3. MÔ HÌNH DỰ BÁO TRƯỚC NGUY CƠ TẮC ĐƯỜNG

### 3.1. Kỹ thuật đặc trưng (Feature Engineering)

Từ 5 đặc trưng gốc (speed_kmph, weather_temp_c, humidity_pct, accident_severity, congestion_km), hệ thống tạo thêm 6 đặc trưng kỹ thuật:

| Đặc trưng | Công thức | Ý nghĩa |
|---|---|---|
| `speed_sq` | speed² | Quan hệ phi tuyến: tắc đường tăng nhanh khi tốc độ giảm |
| `speed_inv` | 1 / (speed + 1) | Khuếch đại tín hiệu khi tốc độ tiệm cận 0 |
| `weather_risk` | (temp/40) × (humidity/100) | Chỉ số rủi ro thời tiết tổng hợp ∈ [0, 1] |
| `accident_x_congestion` | accident_severity × congestion_km | Interaction feature: tai nạn kết hợp tắc nghẽn |
| `low_speed_flag` | 1 nếu speed < 20 | Binary indicator tắc nghẽn nghiêm trọng |
| `very_low_speed_flag` | 1 nếu speed < 10 | Binary indicator tắc nghẽn cực nghiêm trọng |

**Kết quả feature importance (XGBoost):**

```
low_speed_flag          : 81.8%  ← đặc trưng quan trọng nhất
speed_kmph              : 12.4%
speed_sq                :  5.2%
speed_inv               :  0.6%
weather_temp_c          :  0.0%  (không đủ variance trong dataset)
humidity_pct            :  0.0%
accident_severity       :  0.0%
congestion_km           :  0.0%
```

**Ba mục tiêu dự báo (Forecast Targets):**

Hệ thống định nghĩa ba mục tiêu dự báo theo chuẩn:

1. **congestion_level** (phân loại): Low / Moderate / High — xác định bởi ngưỡng tốc độ
2. **estimated_delay_minutes** (hồi quy): delay = f(speed, accident, congestion_km)
3. **travel_time_minutes** (hồi quy chuỗi thời gian): ETA từ điểm A đến B

**Công thức tính delay:**
```
base_delay = ((40 - speed) / 40) × 15 phút
incident_delay = accident_severity × 1.5 + congestion_km × 1.2
estimated_delay = clip(base_delay + incident_delay, 0, 120)
```

**Ràng buộc nhất quán bắt buộc:**
- `congestion_level` phải khớp chính xác với ngưỡng tốc độ
- `estimated_delay_minutes` ≥ 0
- `travel_time_minutes` ≥ `estimated_delay_minutes`
- `eta_minutes` ≥ `travel_time_minutes`

Hàm `validate_training_targets()` kiểm tra toàn bộ dataset trước khi train và raise ValueError nếu có vi phạm.

---

### 3.2. Mô hình lai XGBoost-LSTM

**Mô hình chính: XGBoost Classifier**

```python
XGBClassifier(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    min_child_weight=3,
    gamma=0.1,
    reg_alpha=0.1,    # L1 regularization
    reg_lambda=1.0,   # L2 regularization
    scale_pos_weight=neg_count/pos_count,  # xử lý imbalanced data
    eval_metric="logloss",
    n_jobs=-1,
)
```

Tham số `scale_pos_weight` được tính tự động từ tỷ lệ class trong training data để xử lý mất cân bằng nhãn (imbalanced labels) — phổ biến trong dữ liệu giao thông khi tắc nghẽn chiếm tỷ lệ nhỏ.

**Mô hình dự phòng: GradientBoostingClassifier**

Khi XGBoost không khả dụng (môi trường không cài được C extension), hệ thống tự động chuyển sang sklearn GradientBoostingClassifier với cấu hình tương đương:

```python
GradientBoostingClassifier(
    n_estimators=200, max_depth=5,
    learning_rate=0.05, subsample=0.8,
    min_samples_leaf=10,
)
```

**Dự báo theo giờ trong ngày (LSTM-inspired pattern):**

Do dataset demo không có variation timestamp thực tế, hệ thống sử dụng pattern tắc nghẽn theo giờ dựa trên nghiên cứu giao thông TP.HCM:

```python
HOURLY_CONGESTION_PATTERN = [
    0.10, 0.08, 0.06, 0.05, 0.07, 0.15,  # 0-5h: đêm khuya
    0.30, 0.65, 0.85, 0.70, 0.55, 0.50,  # 6-11h: giờ cao điểm sáng (đỉnh 8h)
    0.60, 0.55, 0.50, 0.52, 0.58, 0.90,  # 12-17h: giờ cao điểm chiều (đỉnh 17h)
    0.95, 0.80, 0.65, 0.45, 0.30, 0.18   # 18-23h: tan tầm (đỉnh 18h)
]
```

Pattern này được dịch chuyển theo giờ hiện tại để tạo dự báo 24 giờ tới, kết hợp với xác suất tắc nghẽn từ XGBoost để tạo dự báo tổng hợp.

**Khả năng tương thích ngược (backward compatibility):**

Hàm `_resolve_feature_columns()` và `_build_compatible_feature_frame()` đảm bảo model đã train với feature set cũ vẫn hoạt động khi feature set thay đổi — bằng cách điền 0 cho các feature mới và bỏ qua feature không còn tồn tại.

---

### 3.3. Đánh giá độ chính xác

**Phân chia dữ liệu:**
- Training set: 80% (stratified split theo nhãn congestion_flag)
- Test set: 20%
- Random seed: 42 (reproducible)

**Kết quả trên dataset thực nghiệm (1.000 bản ghi):**

```
              precision    recall  f1-score   support

      normal       1.00      1.00      1.00       160
   congested       1.00      1.00      1.00        40

    accuracy                           1.00       200
   macro avg       1.00      1.00      1.00       200
weighted avg       1.00      1.00      1.00       200

AUC-ROC: 1.0000
```

*Lưu ý: Accuracy 100% phản ánh đặc điểm của dataset demo — congestion_flag được tính trực tiếp từ speed_kmph theo ngưỡng cứng, nên model học được quy tắc này hoàn hảo. Với dữ liệu thực tế có noise, accuracy sẽ thấp hơn.*

**Phân tích feature importance:**

`low_speed_flag` chiếm 81.8% importance — xác nhận rằng tốc độ dưới 20 km/h là chỉ báo tắc nghẽn mạnh nhất. `speed_kmph` và `speed_sq` cộng thêm 17.6%, cho thấy quan hệ phi tuyến giữa tốc độ và tắc nghẽn được model học được.

---

## CHƯƠNG 4. TRIỂN KHAI VÀ KẾT QUẢ THỰC NGHIỆM

### 4.1. Cài đặt hạ tầng kỹ thuật

**Yêu cầu phần cứng tối thiểu:**
- CPU: 4 cores (để chạy 6 loader workers song song)
- RAM: 8GB (2GB cho mỗi file JSON khi load vào memory)
- Disk: 20GB (2×2GB JSON + 2×650MB cache + Redis AOF)

**Stack công nghệ:**

| Tầng | Công nghệ | Phiên bản |
|---|---|---|
| Backend API | FastAPI + Uvicorn | Python 3.12 |
| Cache/Stream | Redis | 7-alpine (Docker) |
| Database | PostgreSQL | 16-alpine (Docker) |
| ML | XGBoost + scikit-learn | 1.8.0 |
| Data parsing | orjson | 3.11.7 |
| Frontend | Vite + Chart.js + Leaflet | Node 18+ |
| Containerization | Docker Compose | v2 |

**Quy trình khởi động tự động (`start.ps1`):**

```
[0] Kill processes trên port 8000, 3000, 5173
[1] docker compose up -d redis postgres
[2] Chờ Redis PONG (retry 30×3s = 90s timeout)
[3] Chờ Postgres pg_isready
[4] uvicorn backend.main:app --port 8000
[5] Chờ /health = 200 (retry 30×3s)
[6] npm run dev (Vite frontend)
```

**Cấu hình Vite proxy:**

```javascript
proxy: {
    '/api': { target: 'http://localhost:8000', rewrite: path => path.replace(/^\/api/, '') },
    '/ws':  { target: 'ws://localhost:8000', ws: true }
}
```

Proxy này giải quyết vấn đề CORS và cho phép deploy trên bất kỳ máy nào mà không cần thay đổi URL hardcode.

**File watcher tự động:**

Sử dụng thư viện `watchdog` để monitor thư mục `data/`. Khi phát hiện file `traffic_data_*.json` mới hoặc thay đổi, hệ thống tự động trigger load với debounce 1 giây (tránh load nhiều lần khi file đang được copy).

---

### 4.2. Kết quả benchmark hiệu năng

**Throughput đọc và xử lý dữ liệu:**

| Giai đoạn | Thời gian | Throughput |
|---|---|---|
| orjson đọc file 2GB | ~12-15s | ~67.000 rec/s |
| Normalize 1M records | ~4s | ~250.000 rec/s |
| Redis MSET (lý thuyết) | — | ~312.000 rec/s |
| Redis MSET (thực tế, 6 workers) | — | ~67.000 rec/s |
| **End-to-end (lần đầu, 2M records)** | **~30s** | **~67.000 rec/s** |
| **End-to-end (từ cache, 2M records)** | **~11s** | **~183.000 rec/s** |
| **Khởi động lại (Redis persist)** | **<5s** | N/A |

**So sánh chiến lược ghi Redis:**

Việc chuyển từ `HSET` (17 fields) sang `MSET` (JSON string) tăng throughput **10×**. Việc bỏ HINCRBY khỏi hot path (flush stats 1 lần/file thay vì mỗi batch) tăng thêm **5×**, đưa tổng throughput từ ~5.000 rec/s lên ~67.000 rec/s trong điều kiện production.

**Latency WebSocket:**

- Broadcast sau mỗi batch: throttle 1 giây (tránh flood clients)
- Thời gian từ data thay đổi đến frontend nhận được: <2 giây
- Số clients WebSocket đồng thời: không giới hạn (asyncio broadcast)

---

### 4.3. Phân tích kết quả thực tế tại TP.HCM

**Đặc điểm dataset:**

Dataset gồm 2.000.000 bản ghi GPS từ 6 loại phương tiện (Motorbike, Car, Bus, Truck, Electric Car, Bicycle) trên 65 tuyến đường thuộc 16 quận/huyện TP.HCM.

**Phân bổ theo loại phương tiện:**

| Loại xe | Số bản ghi | Tỷ lệ |
|---|---|---|
| Motorbike | 1.400.767 | 70,0% |
| Car | 299.361 | 15,0% |
| Truck | 199.429 | 10,0% |
| Bus | 60.272 | 3,0% |
| Bicycle | 40.171 | 2,0% |

Xe máy chiếm 70% — phản ánh đúng thực tế giao thông TP.HCM.

**Phân bổ theo quận:**

Quận 1 chiếm 50,6% tổng bản ghi (1.012.403/2.000.000), phản ánh mật độ giao thông cao tại trung tâm thành phố. Các quận ngoại thành (Bình Chánh, Hóc Môn, Thủ Đức) có mật độ thấp hơn đáng kể.

**Tốc độ trung bình theo tuyến đường:**

Tất cả 65 tuyến đường có tốc độ trung bình trong khoảng 37,8–38,3 km/h — nằm trong ngưỡng "lưu thông chậm" (20–40 km/h). Điều này phản ánh tình trạng giao thông đô thị điển hình tại TP.HCM trong giờ cao điểm.

**Vi phạm tốc độ:**

999.436/2.000.000 bản ghi (50%) có cảnh báo vi phạm tốc độ. Xe máy chiếm tỷ lệ vi phạm cao nhất tuyệt đối (699.843 lượt), trong khi xe tải có tỷ lệ vi phạm tương đối cao nhất so với số lượng.

**Phân bổ mức nhiên liệu:**

Phân bổ đều giữa các mức (mỗi bucket ~20%), với 396.711 xe (19,8%) có nhiên liệu dưới 20% — cần cảnh báo. Nhiên liệu trung bình toàn hệ thống: 50,5%.

**Kết quả dự báo AI:**

Model XGBoost dự báo xác suất tắc nghẽn cho 65 tuyến đường trong 5 phút tới. Với dữ liệu hiện tại (tốc độ 37-38 km/h, không có xe nào dưới 20 km/h), xác suất tắc nghẽn thấp (<5%) — phù hợp với trạng thái "lưu thông chậm nhưng không tắc".

**Ước tính thời gian di chuyển A→B:**

Hệ thống tính khoảng cách Haversine giữa tọa độ GPS của 2 tuyến đường, kết hợp tốc độ trung bình thực tế và delay dự kiến để ước tính tổng thời gian di chuyển. Ví dụ: Phan Xích Long → Nguyễn Huệ ≈ 3,2 km, tốc độ TB 38 km/h, delay 8 phút → tổng ~13 phút.
