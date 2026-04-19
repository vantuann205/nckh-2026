/**
 * Pages Module — HTML templates for each page
 * Simplified for new traffic data model
 */

const PAGES = {};

// === DASHBOARD ===
PAGES.dashboard = () => {
  const s = DB.summary || {};
  const fmt = n => Number(n || 0).toLocaleString();

  return `
    <div class="page-header">
      <h1>📊 Bảng điều khiển</h1>
      <div class="header-actions">
        <span class="ws-badge" id="ws-badge">⏳ Connecting...</span>
        <span class="last-update" id="last-update"></span>
      </div>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-icon" style="background: linear-gradient(135deg, #3b82f6, #2563eb);">
          <i data-lucide="map-pin" style="color:#fff"></i>
        </div>
        <div class="kpi-content">
          <div class="kpi-label">Tổng đoạn đường</div>
          <div class="kpi-value" id="kpi-total">${fmt(s.total_roads)}</div>
        </div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon" style="background: linear-gradient(135deg, #22c55e, #16a34a);">
          <i data-lucide="gauge" style="color:#fff"></i>
        </div>
        <div class="kpi-content">
          <div class="kpi-label">Tốc độ TB</div>
          <div class="kpi-value" id="kpi-speed">${s.avg_speed || 0} <span style="font-size:14px;color:var(--text3)">km/h</span></div>
        </div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon" style="background: linear-gradient(135deg, #f59e0b, #d97706);">
          <i data-lucide="car" style="color:#fff"></i>
        </div>
        <div class="kpi-content">
          <div class="kpi-label">Tổng phương tiện</div>
          <div class="kpi-value" id="kpi-vehicles">${fmt(s.total_vehicles)}</div>
        </div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon" style="background: linear-gradient(135deg, #ef4444, #dc2626);">
          <i data-lucide="alert-triangle" style="color:#fff"></i>
        </div>
        <div class="kpi-content">
          <div class="kpi-label">Đoạn tắc nghẽn</div>
          <div class="kpi-value" id="kpi-congested">${fmt(s.congested_roads)}</div>
        </div>
      </div>
    </div>

    <div class="charts-grid">
      <div class="chart-card">
        <div class="chart-header">
          <h3>Phân bổ tốc độ</h3>
          <span class="chart-badge live">LIVE</span>
        </div>
        <div class="chart-body"><canvas id="chart-speed-dist"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-header">
          <h3>Trạng thái giao thông</h3>
          <span class="chart-badge live">LIVE</span>
        </div>
        <div class="chart-body"><canvas id="chart-status"></canvas></div>
      </div>
      <div class="chart-card full-width">
        <div class="chart-header">
          <h3>Tốc độ theo tuyến đường</h3>
          <span class="chart-badge live">LIVE</span>
        </div>
        <div class="chart-body tall"><canvas id="chart-speed-overview"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-header">
          <h3>Traffic vs Time</h3>
          <span class="chart-badge live">API</span>
        </div>
        <div class="chart-body"><canvas id="chart-traffic-time"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-header">
          <h3>Accident vs Delay</h3>
          <span class="chart-badge live">API</span>
        </div>
        <div class="chart-body"><canvas id="chart-accident-delay"></canvas></div>
      </div>
    </div>
  `;
};

// === MAP ===
PAGES.map = () => `
  <div class="page-header">
    <h1>🗺️ Bản đồ giao thông</h1>
    <div class="header-actions">
      <span class="ws-badge" id="ws-badge">⏳</span>
      <div class="map-legend">
        <span class="legend-item"><span class="legend-dot" style="background:#22c55e"></span> Vùng thông thoáng</span>
        <span class="legend-item"><span class="legend-dot" style="background:#f59e0b"></span> Vùng chậm</span>
        <span class="legend-item"><span class="legend-dot" style="background:#ef4444"></span> Vùng tắc nghẽn</span>
      </div>
    </div>
  </div>
  <div class="map-wrapper">
    <div id="map-container" style="width:100%; height:600px; border-radius:16px; overflow:hidden; box-shadow: 0 4px 24px rgba(0,0,0,0.08);"></div>
  </div>
`;

// === EXPLORER — Road Data ===
PAGES.explorer = () => `
  <div class="page-header">
    <h1>🔍 Tra cứu dữ liệu</h1>
  </div>
  <div class="explorer-controls">
    <input id="ex-search" type="text" placeholder="Tìm road_id..." oninput="updateExplorer()">
    <select id="ex-status" onchange="updateExplorer()">
      <option value="">Tất cả trạng thái</option>
      <option value="normal">Bình thường</option>
      <option value="slow">Chậm</option>
      <option value="congested">Tắc nghẽn</option>
    </select>
  </div>
  <div class="table-container">
    <table>
      <thead>
        <tr>
          <th>Road ID</th>
          <th>Tốc độ TB</th>
          <th>Số xe</th>
          <th>Vĩ độ</th>
          <th>Kinh độ</th>
          <th>Trạng thái</th>
          <th>Cập nhật</th>
        </tr>
      </thead>
      <tbody id="explorer-tbody"></tbody>
    </table>
  </div>
`;

// === VEHICLE ANALYTICS ===
PAGES.vehicle = () => `
  <div class="page-header">
    <h1>🚗 Phân tích mật độ xe</h1>
  </div>
  <div class="charts-grid">
    <div class="chart-card full-width">
      <div class="chart-header">
        <h3>Mật độ xe theo tuyến đường</h3>
        <span class="chart-badge live">LIVE</span>
      </div>
      <div class="chart-body tall"><canvas id="chart-vehicle-density"></canvas></div>
    </div>
  </div>
  <div class="road-list" id="road-list"></div>
`;

// === MONITOR ===
PAGES.monitor = () => `
  <div class="page-header">
    <h1>⚙️ Giám sát hệ thống</h1>
  </div>
  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-icon" style="background:linear-gradient(135deg,#22c55e,#16a34a);">
        <i data-lucide="wifi" style="color:#fff"></i>
      </div>
      <div class="kpi-content">
        <div class="kpi-label">WebSocket</div>
        <div class="kpi-value" id="mon-ws">-</div>
      </div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon" style="background:linear-gradient(135deg,#3b82f6,#2563eb);">
        <i data-lucide="database" style="color:#fff"></i>
      </div>
      <div class="kpi-content">
        <div class="kpi-label">Redis</div>
        <div class="kpi-value" id="mon-redis">-</div>
      </div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon" style="background:linear-gradient(135deg,#8b5cf6,#7c3aed);">
        <i data-lucide="activity" style="color:#fff"></i>
      </div>
      <div class="kpi-content">
        <div class="kpi-label">Tổng đoạn đường</div>
        <div class="kpi-value" id="mon-roads">-</div>
      </div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon" style="background:linear-gradient(135deg,#ec4899,#db2777);">
        <i data-lucide="clock" style="color:#fff"></i>
      </div>
      <div class="kpi-content">
        <div class="kpi-label">Last Update</div>
        <div class="kpi-value" id="mon-update" style="font-size:14px">-</div>
      </div>
    </div>
  </div>
  <div class="charts-grid">
    <div class="chart-card full-width">
      <div class="chart-header"><h3>Latency</h3></div>
      <div class="chart-body"><canvas id="chart-latency"></canvas></div>
    </div>
  </div>
`;

// Expose globally
window.PAGES = PAGES;
window.renderViolations = function () {
  const roads = DB.state.roads || [];
  const congested = roads.filter(r => r.status === 'congested');

  const list = document.getElementById('congestion-list');
  if (!list) return;

  if (congested.length === 0) {
    list.innerHTML = '<div style="text-align:center;color:var(--text3);padding:20px;">✅ Không có tắc nghẽn</div>';
    return;
  }

  list.innerHTML = congested.map(r => `
    <div class="alert-item congested">
      <div class="alert-icon">🔴</div>
      <div class="alert-content">
        <div class="alert-title">${r.road_id}</div>
        <div class="alert-detail">Tốc độ: ${r.avg_speed} km/h | Xe: ${r.vehicle_count}</div>
      </div>
    </div>
  `).join('');
};

window.renderSparkJobs = function () {};
window.renderWorkerNodes = function () {};

// === WEATHER ===
PAGES.weather = () => `
  <div class="page-header">
    <h1>🌤️ Thời tiết TP. Hồ Chí Minh</h1>
    <div class="header-actions">
      <span id="weather-location" style="font-size:12px;color:var(--text3);font-family:var(--mono)"></span>
    </div>
  </div>

  <div class="kpi-grid" id="weather-kpi"></div>

  <div class="charts-grid">
    <div class="chart-card full-width">
      <div class="chart-header"><h3>Nhiệt độ 15 ngày (°C)</h3></div>
      <div class="chart-body"><canvas id="chart-temp-range"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Độ ẩm & Lượng mưa</h3></div>
      <div class="chart-body"><canvas id="chart-humidity-precip"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Tốc độ gió (km/h)</h3></div>
      <div class="chart-body"><canvas id="chart-wind"></canvas></div>
    </div>
    <div class="chart-card full-width">
      <div class="chart-header"><h3>Nhiệt độ theo giờ — Hôm nay</h3></div>
      <div class="chart-body"><canvas id="chart-hourly-temp"></canvas></div>
    </div>
  </div>
`;

// === ACCIDENTS ===
PAGES.accidents = () => `
  <div class="page-header">
    <h1>🚨 Tai nạn giao thông</h1>
    <div class="header-actions">
      <span id="acc-total-badge" style="font-size:12px;background:var(--red);color:#fff;padding:4px 12px;border-radius:99px;font-weight:700"></span>
    </div>
  </div>

  <div class="kpi-grid" id="acc-kpi"></div>

  <div class="charts-grid">
    <div class="chart-card">
      <div class="chart-header"><h3>Tai nạn theo quận</h3></div>
      <div class="chart-body tall"><canvas id="chart-acc-district"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Mức độ nghiêm trọng</h3></div>
      <div class="chart-body"><canvas id="chart-acc-severity"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Phân bố theo giờ trong ngày</h3></div>
      <div class="chart-body"><canvas id="chart-acc-hour"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Loại phương tiện liên quan</h3></div>
      <div class="chart-body"><canvas id="chart-acc-vehicles"></canvas></div>
    </div>
  </div>

  <div class="table-container" style="margin-top:8px">
    <div style="padding:16px 20px;border-bottom:1px solid var(--border);display:flex;gap:12px;flex-wrap:wrap;align-items:center">
      <span style="font-weight:700;font-size:14px">Danh sách tai nạn</span>
      <select id="acc-filter-severity" onchange="filterAccidents()" style="padding:6px 12px;border:1px solid var(--border);border-radius:8px;font-size:12px;background:var(--bg2)">
        <option value="">Tất cả mức độ</option>
        <option value="5">Nghiêm trọng (5)</option>
        <option value="4">Nặng (4)</option>
        <option value="3">Trung bình (3)</option>
        <option value="2">Nhẹ (2)</option>
        <option value="1">Rất nhẹ (1)</option>
      </select>
    </div>
    <table>
      <thead><tr>
        <th>Đường</th><th>Quận</th><th>Mức độ</th><th>Thời gian</th><th>Tắc nghẽn</th><th>Số xe</th>
      </tr></thead>
      <tbody id="acc-tbody"></tbody>
    </table>
  </div>
`;

// === PHÂN TÍCH NÂNG CAO ===
PAGES.advanced = () => `
  <div class="page-header">
    <h1>Phân tích nâng cao</h1>
    <div class="header-actions">
      <span class="ws-badge disconnected" id="ws-badge">Disconnected</span>
      <span class="last-update" id="last-update"></span>
    </div>
  </div>

  <!-- A→B Travel Time Estimator -->
  <div class="section-label">Ước tính thời gian di chuyển A → B</div>
  <div style="padding:0 0 16px;font-size:13px;color:var(--text3)">
    Chọn 2 tuyến đường để ước tính thời gian di chuyển dựa trên tốc độ thực tế và delay hiện tại
  </div>
  <div style="display:flex;gap:12px;flex-wrap:wrap;align-items:flex-end;margin-bottom:16px">
    <div style="flex:1;min-width:200px">
      <div style="font-size:11px;font-weight:700;color:var(--text3);margin-bottom:6px;text-transform:uppercase">Từ tuyến đường</div>
      <select id="route-from" style="width:100%;padding:10px 12px;border-radius:8px;background:var(--surface-container);color:var(--on-surface);border:1px solid var(--border);font-size:13px">
        <option value="">-- Chọn tuyến --</option>
      </select>
    </div>
    <div style="flex:1;min-width:200px">
      <div style="font-size:11px;font-weight:700;color:var(--text3);margin-bottom:6px;text-transform:uppercase">Đến tuyến đường</div>
      <select id="route-to" style="width:100%;padding:10px 12px;border-radius:8px;background:var(--surface-container);color:var(--on-surface);border:1px solid var(--border);font-size:13px">
        <option value="">-- Chọn tuyến --</option>
      </select>
    </div>
    <button onclick="estimateRoute()" style="padding:10px 24px;border-radius:8px;background:var(--accent,#3b82f6);color:#fff;font-weight:700;font-size:13px;border:none;cursor:pointer;white-space:nowrap">
      Ước tính
    </button>
  </div>
  <div id="route-result" style="display:none;background:var(--surface-container);border-radius:12px;padding:20px;margin-bottom:24px;border:1px solid var(--border)">
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:16px" id="route-result-grid"></div>
    <div id="route-recommendation" style="margin-top:12px;padding:10px 16px;border-radius:8px;font-weight:600;font-size:13px"></div>
  </div>

  <!-- Risk Ranking -->
  <div class="section-label">Xếp hạng rủi ro tuyến đường</div>
  <div style="padding:0 0 12px;font-size:13px;color:var(--text3)">
    Tổng hợp: tỷ lệ tắc nghẽn cao + delay + vi phạm tốc độ → risk score tổng hợp
  </div>
  <div class="charts-grid">
    <div class="chart-card full-width">
      <div class="chart-header"><h3>Risk Score tổng hợp theo tuyến đường</h3><span class="chart-badge live">LIVE</span></div>
      <div class="chart-body tall" style="height:480px"><canvas id="chart-adv-risk"></canvas></div>
    </div>
  </div>

  <!-- Speed Anomalies -->
  <div class="section-label" style="margin-top:24px">Phát hiện bất thường tốc độ</div>
  <div style="padding:0 0 12px;font-size:13px;color:var(--text3)">
    Tuyến đường có tốc độ thấp hơn trung bình quận/huyện đáng kể — có thể có tai nạn hoặc sự cố
  </div>
  <div class="table-container">
    <table>
      <thead><tr>
        <th>Tuyến đường</th><th>Quận</th><th>Tốc độ tuyến</th><th>TB quận</th><th>Chênh lệch</th><th>Cảnh báo</th><th>Mức độ</th>
      </tr></thead>
      <tbody id="adv-anomaly-tbody">
        <tr><td colspan="7" style="text-align:center;color:var(--text3);padding:20px">Đang tải...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- District Congestion -->
  <div class="section-label" style="margin-top:24px">Mức độ tắc nghẽn theo Quận/Huyện</div>
  <div class="charts-grid">
    <div class="chart-card full-width">
      <div class="chart-header"><h3>Tỷ lệ tuyến tắc nghẽn cao theo Quận/Huyện</h3><span class="chart-badge live">LIVE</span></div>
      <div class="chart-body"><canvas id="chart-adv-district"></canvas></div>
    </div>
    <div class="chart-card full-width">
      <div class="chart-header"><h3>Delay trung bình theo Quận/Huyện (phút)</h3><span class="chart-badge live">LIVE</span></div>
      <div class="chart-body"><canvas id="chart-adv-district-delay"></canvas></div>
    </div>
  </div>

  <!-- Low Fuel Forecast -->
  <div class="section-label" style="margin-top:24px">Dự báo xe sắp hết nhiên liệu</div>
  <div style="padding:0 0 12px;font-size:13px;color:var(--text3)">
    Tuyến đường có tỷ lệ xe nhiên liệu thấp cao — ước tính quãng đường còn lại
  </div>
  <div class="table-container">
    <table>
      <thead><tr>
        <th>Tuyến đường</th><th>Quận</th><th>Nhiên liệu TB</th><th>Quãng đường còn lại</th><th>Giờ còn lại</th><th>Tỷ lệ xe thấp nhiên liệu</th><th>Mức độ</th>
      </tr></thead>
      <tbody id="adv-fuel-tbody">
        <tr><td colspan="7" style="text-align:center;color:var(--text3);padding:20px">Đang tải...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- Risk Table -->
  <div class="section-label" style="margin-top:24px">Chi tiết xếp hạng rủi ro</div>
  <div class="table-container">
    <table>
      <thead><tr>
        <th>Tuyến đường</th><th>Quận</th><th>Tốc độ TB</th><th>Tắc cao %</th><th>Delay TB</th><th>Vi phạm %</th><th>Risk Score</th><th>Mức rủi ro</th>
      </tr></thead>
      <tbody id="adv-risk-tbody">
        <tr><td colspan="8" style="text-align:center;color:var(--text3);padding:20px">Đang tải...</td></tr>
      </tbody>
    </table>
  </div>
`;

// === PREDICTION (10m/30m/60m) ===
PAGES.prediction = () => `
  <div class="page-header forecast-page-header">
    <h1>Dự báo tắc nghẽn 10/30/60 phút</h1>
    <div class="header-actions forecast-header-actions">
      <span class="forecast-kicker">Model train: traffic_data_0 + traffic_data_1</span>
      <select id="pred-horizon" class="forecast-select" onchange="loadPredictions()">
        <option value="10">10 phút tới</option>
        <option value="30">30 phút tới</option>
        <option value="60">60 phút tới</option>
      </select>
      <button class="btn-primary" onclick="loadPredictions()">Cập nhật dự báo</button>
    </div>
  </div>

  <div class="kpi-grid">
    <div class="kpi-card"><div class="kpi-label">Tuyến có nguy cơ tắc</div><div class="kpi-value" id="pred-kpi-danger">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Xác suất tắc trung bình</div><div class="kpi-value" id="pred-kpi-avg-prob">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Delay trung bình</div><div class="kpi-value" id="pred-kpi-avg-delay">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Tuyến tương đối an toàn</div><div class="kpi-value" id="pred-kpi-safe">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Tuyến rủi ro cao</div><div class="kpi-value" id="pred-kpi-high-risk">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Lần cập nhật gần nhất</div><div class="kpi-value" id="pred-kpi-time" style="font-size:13px">-</div></div>
  </div>

  <div class="forecast-insights" id="pred-insights"></div>

  <div class="section-label">Gợi ý tuyến nên đi tiếp theo từ vị trí hiện tại</div>
  <div class="forecast-route-controls">
    <div class="forecast-route-field forecast-route-field-wide">
      <div class="forecast-field-label">Vị trí hiện tại (chọn tuyến đang đứng)</div>
      <input id="pred-current-road-input" class="forecast-input" list="pred-current-road-list" placeholder="Nhập hoặc chọn road_id hiện tại">
      <datalist id="pred-current-road-list"></datalist>
    </div>
    <button class="btn-primary" onclick="suggestNextRoutes()">Gợi ý nên đi tiếp</button>
  </div>

  <div class="table-container" style="margin-bottom:18px">
    <table class="forecast-table">
      <thead><tr>
        <th>#</th><th>Tuyến đề xuất</th><th>Quận</th><th>Cách vị trí hiện tại</th><th>Tốc độ hiện tại</th><th>Xác suất tắc</th><th>Delay dự báo</th><th>Điểm khuyến nghị</th><th>Lý do chọn</th>
      </tr></thead>
      <tbody id="pred-next-route-tbody">
        <tr><td colspan="9" style="text-align:center;padding:18px;color:var(--text3)">Chọn vị trí hiện tại để nhận gợi ý tuyến nên đi tiếp.</td></tr>
      </tbody>
    </table>
  </div>

  <div class="charts-grid">
    <div class="chart-card">
      <div class="chart-header"><h3>Phân bố xác suất tắc nghẽn</h3><span class="chart-badge live">ML</span></div>
      <div class="chart-body"><canvas id="chart-pred-dist"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Top tuyến có delay cao</h3><span class="chart-badge live">ML</span></div>
      <div class="chart-body"><canvas id="chart-pred-delay"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Top 10 tuyến rủi ro cao</h3><span class="chart-badge live">ALERT</span></div>
      <div class="chart-body tall"><canvas id="chart-pred-top10"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Top 10 tuyến thông thoáng</h3><span class="chart-badge live">SAFE</span></div>
      <div class="chart-body tall"><canvas id="chart-pred-safe10"></canvas></div>
    </div>
    <div class="chart-card full-width">
      <div class="chart-header"><h3>Dự báo theo giờ</h3><span class="chart-badge live">FORECAST</span></div>
      <div class="chart-body"><canvas id="chart-pred-hourly"></canvas></div>
    </div>
  </div>

  <div class="table-container forecast-table-shell">
    <div class="forecast-toolbar">
      <span class="forecast-toolbar-title">Chi tiết dự báo theo tuyến đường</span>
      <input id="pred-search" class="forecast-input" type="text" placeholder="Tìm theo road_id..." oninput="filterPredTable()">
      <select id="pred-filter-status" class="forecast-select" onchange="filterPredTable()">
        <option value="">Tất cả trạng thái</option>
        <option value="congested">Sắp tắc</option>
        <option value="normal">Bình thường</option>
      </select>
      <select id="pred-filter-confidence" class="forecast-select" onchange="filterPredTable()">
        <option value="">Mọi mức tin cậy</option>
        <option value="Cao">Cao</option>
        <option value="Trung bình">Trung bình</option>
        <option value="Thấp">Thấp</option>
      </select>
      <select id="pred-sort" class="forecast-select" onchange="filterPredTable()">
        <option value="prob_desc">Xác suất tắc cao nhất</option>
        <option value="prob_asc">Xác suất tắc thấp nhất</option>
        <option value="delay_desc">Delay cao nhất</option>
        <option value="risk_desc">Điểm rủi ro cao nhất</option>
      </select>
    </div>
    <table class="forecast-table">
      <thead><tr>
        <th>Road ID</th><th>Tốc độ hiện tại</th><th>Xác suất tắc</th><th>Trạng thái dự báo</th><th>Delay dự báo</th><th>Tin cậy</th><th>Yếu tố chính</th><th>Khuyến nghị</th>
      </tr></thead>
      <tbody id="pred-tbody"></tbody>
    </table>
  </div>
`;

// Keep compatibility with older routes
PAGES.analysis = PAGES.prediction;
PAGES.advanced = PAGES.prediction;
