/**
 * Pages Module — HTML templates for each page
 * UX Enhanced - MAP LAYERS Design
 */

const PAGES = {};

// === DASHBOARD ===
PAGES.dashboard = () => {
  const s = DB.summary || {};
  const hasData = s.total_roads !== undefined && s.total_roads !== null;
  const kpiValue = (val, isSpeed) => hasData 
    ? (isSpeed ? `${val} <span style="font-size:14px;color:var(--text3)">km/h</span>` : Number(val||0).toLocaleString()) 
    : `<div class="skeleton-box kpi-skeleton"><div class="kpi-value"></div></div>`;
  const kpiFuel = (val) => hasData 
    ? `${val} <span style="font-size:14px;color:var(--text3)">%</span>` 
    : `<div class="skeleton-box kpi-skeleton"><div class="kpi-value"></div></div>`;

  return `
    <div class="page-header">
      <h1>Bảng điều khiển</h1>
      <div class="header-actions">
        <span class="last-update" id="last-update"></span>
      </div>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card"><div class="kpi-label">Tổng đoạn đường</div><div class="kpi-value" id="kpi-total">${kpiValue(s.total_roads)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Tốc độ TB</div><div class="kpi-value" id="kpi-speed">${kpiValue(s.avg_speed, true)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Tổng phương tiện</div><div class="kpi-value" id="kpi-vehicles">${kpiValue(s.total_vehicles)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Đoạn tắc nghẽn</div><div class="kpi-value" id="kpi-congested">${kpiValue(s.congested_roads)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Tổng hành khách</div><div class="kpi-value" id="kpi-passengers">${kpiValue(s.total_passengers)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Nhiên liệu TB</div><div class="kpi-value" id="kpi-fuel">${kpiFuel(s.avg_fuel_level)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Cảnh báo tốc độ</div><div class="kpi-value" id="kpi-speeding">${kpiValue(s.speeding_alerts)}</div></div>
      <div class="kpi-card"><div class="kpi-label">Cảnh báo nhiên liệu</div><div class="kpi-value" id="kpi-lowfuel">${kpiValue(s.low_fuel_alerts)}</div></div>
    </div>

    <div class="charts-grid">
      <div class="chart-card"><div class="chart-header"><h3>Tốc độ theo tuyến đường</h3><span class="chart-badge status-sync">LIVE</span></div><div class="chart-body"><canvas id="chart-speed-dist"></canvas></div></div>
      <div class="chart-card"><div class="chart-header"><h3>Trạng thái giao thông</h3><span class="chart-badge status-sync">LIVE</span></div><div class="chart-body"><canvas id="chart-status"></canvas></div></div>
      <div class="chart-card"><div class="chart-header"><h3>Loại phương tiện</h3><span class="chart-badge status-sync">LIVE</span></div><div class="chart-body"><canvas id="chart-vehicle-types"></canvas></div></div>
      <div class="chart-card"><div class="chart-header"><h3>Mức độ tắc nghẽn</h3><span class="chart-badge status-sync">LIVE</span></div><div class="chart-body"><canvas id="chart-congestion-levels"></canvas></div></div>
      <div class="chart-card"><div class="chart-header"><h3>Thời tiết</h3><span class="chart-badge status-sync">LIVE</span></div><div class="chart-body"><canvas id="chart-weather"></canvas></div></div>
      <div class="chart-card"><div class="chart-header"><h3>Traffic vs Time</h3><span class="chart-badge status-sync">API</span></div><div class="chart-body"><canvas id="chart-traffic-time"></canvas></div></div>
      <div class="chart-card full-width"><div class="chart-header"><h3>Tốc độ chi tiết theo tuyến đường</h3><span class="chart-badge status-sync">LIVE</span></div><div class="chart-body tall"><canvas id="chart-speed-overview"></canvas></div></div>
    </div>
  `;
};

// === MAP ===
PAGES.map = () => `
  <div class="page-header">
    <h1>🗺️ Bản đồ giao thông</h1>
    <div class="header-actions" style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">
      <div style="display:flex;gap:6px;align-items:center;font-size:11px">
        <span style="width:10px;height:10px;border-radius:50%;background:#22c55e;display:inline-block"></span>Low
        <span style="width:10px;height:10px;border-radius:50%;background:#f59e0b;display:inline-block;margin-left:6px"></span>Medium
        <span style="width:10px;height:10px;border-radius:50%;background:#ef4444;display:inline-block;margin-left:6px"></span>High
      </div>
    </div>
  </div>

  <!-- Stats bar -->
  <div style="display:flex;gap:10px;margin-bottom:10px;flex-wrap:wrap">
    <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:8px 14px;display:flex;gap:8px;align-items:center">
      <span>📍</span><div><div style="font-size:10px;color:var(--text3)">Tổng điểm</div><div style="font-weight:800;font-size:15px" id="map-stat-total">-</div></div>
    </div>
    <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:8px 14px;display:flex;gap:8px;align-items:center">
      <span>🔴</span><div><div style="font-size:10px;color:var(--text3)">Tắc cao</div><div style="font-weight:800;font-size:15px;color:#ef4444" id="map-stat-high">-</div></div>
    </div>
    <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:8px 14px;display:flex;gap:8px;align-items:center">
      <span>🟡</span><div><div style="font-size:10px;color:var(--text3)">Chậm</div><div style="font-weight:800;font-size:15px;color:#f59e0b" id="map-stat-medium">-</div></div>
    </div>
    <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:8px 14px;display:flex;gap:8px;align-items:center">
      <span>🟢</span><div><div style="font-size:10px;color:var(--text3)">Bình thường</div><div style="font-weight:800;font-size:15px;color:#22c55e" id="map-stat-low">-</div></div>
    </div>
    <div style="background:var(--bg2);border:1px solid var(--border);border-radius:10px;padding:8px 14px;display:flex;gap:8px;align-items:center">
      <span>⚡</span><div><div style="font-size:10px;color:var(--text3)">Tốc độ TB</div><div style="font-weight:800;font-size:15px" id="map-stat-speed">-</div></div>
    </div>
  </div>

  <!-- Map container — full height -->
  <div id="map-container" style="width:100%;height:600px;border-radius:16px;overflow:hidden;border:1px solid var(--border)"></div>
`;

// === EXPLORER — Road Data ===
PAGES.explorer = () => `
  <div class="page-header">
    <h1>Tra cứu dữ liệu</h1>
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
        <tr><th>Road ID</th><th>Tốc độ TB</th><th>Số xe</th><th>Vĩ độ</th><th>Kinh độ</th><th>Trạng thái</th><th>Cập nhật</th></tr>
      </thead>
      <tbody id="explorer-tbody"></tbody>
    </table>
  </div>
`;

// === VEHICLE ANALYTICS ===
PAGES.vehicle = () => `
  <div class="page-header">
    <h1>Phân tích mật độ xe</h1>
  </div>
    <div class="charts-grid">
      <div class="chart-card full-width">
        <div class="chart-header"><h3>Mật độ xe theo tuyến đường</h3><span class="chart-badge status-sync">LIVE</span></div>
        <div class="chart-body tall"><canvas id="chart-vehicle-density"></canvas></div>
      </div>
    </div>
  <div class="road-list" id="road-list"></div>
`;

// === ALERTS ===
PAGES.alerts = () => `
  <div class="page-header">
    <h1>Cảnh báo tắc nghẽn</h1>
  </div>
  <div class="charts-grid">
    <div class="chart-card">
      <div class="chart-header"><h3>Phân bố trạng thái</h3></div>
      <div class="chart-body"><canvas id="chart-alerts"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Danh sách tắc nghẽn</h3></div>
      <div class="chart-body" id="congestion-list" style="overflow-y:auto; padding:16px;"></div>
    </div>
  </div>
`;

// === MONITOR ===
PAGES.monitor = () => `
  <div class="page-header">
    <h1>Giám sát hệ thống</h1>
  </div>
  <div class="kpi-grid">
    <div class="kpi-card"><div class="kpi-label">WebSocket</div><div class="kpi-value" id="mon-ws">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Redis</div><div class="kpi-value" id="mon-redis">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Tổng đoạn đường</div><div class="kpi-value" id="mon-roads">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Last Update</div><div class="kpi-value" id="mon-update" style="font-size:14px">-</div></div>
  </div>
  <div class="charts-grid">
    <div class="chart-card full-width">
      <div class="chart-header"><h3>Latency</h3></div>
      <div class="chart-body"><canvas id="chart-latency"></canvas></div>
    </div>
  </div>
`;

// Expose globally

// === DỰ ĐOÁN TẮC ĐƯỜNG ===
PAGES.prediction = () => `
  <div class="page-header">
    <h1>Dự báo lưu lượng</h1>
    <div class="header-actions">
      <select id="pred-horizon" onchange="loadPredictions()" style="padding:8px 12px;border-radius:8px;background:var(--surface-container);color:var(--on-surface)">
        <option value="5">5 phút tới</option>
        <option value="10">10 phút tới</option>
        <option value="15">15 phút tới</option>
        <option value="30">30 phút tới</option>
      </select>
      <button onclick="loadPredictions()" class="btn-primary">Cập nhật</button>
    </div>
  </div>

  <div class="kpi-grid">
    <div class="kpi-card"><div class="kpi-label">Đường sắp tắc</div><div class="kpi-value" id="pred-kpi-danger">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Xác suất TB</div><div class="kpi-value" id="pred-kpi-avg-prob">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Trễ TB (phút)</div><div class="kpi-value" id="pred-kpi-avg-delay">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Đường an toàn</div><div class="kpi-value" id="pred-kpi-safe">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Rủi ro cao</div><div class="kpi-value" id="pred-kpi-high-risk">-</div></div>
    <div class="kpi-card"><div class="kpi-label">Cập nhật</div><div class="kpi-value" id="pred-kpi-time" style="font-size:13px">-</div></div>
  </div>

  <div class="charts-grid">
    <div class="chart-card">
      <div class="chart-header"><h3>Phân bố xác suất tắc đường</h3><span class="chart-badge status-sync">ML</span></div>
      <div class="chart-body"><canvas id="chart-pred-dist"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Thời gian trễ dự kiến (phút)</h3><span class="chart-badge status-sync">ML</span></div>
      <div class="chart-body"><canvas id="chart-pred-delay"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Top 10 đường nguy hiểm nhất</h3><span class="chart-badge" style="background:var(--error-container);color:var(--error)">HIGH RISK</span></div>
      <div class="chart-body tall"><canvas id="chart-pred-top10"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Top 10 đường thông thoáng nhất</h3><span class="chart-badge" style="background:var(--tertiary-container);color:var(--tertiary)">SAFE</span></div>
      <div class="chart-body tall"><canvas id="chart-pred-safe10"></canvas></div>
    </div>
  </div>

  <div class="charts-grid">
    <div class="chart-card full-width">
      <div class="chart-header"><h3>Dự báo tắc đường theo khung giờ</h3><span class="chart-badge status-sync">FORECAST</span></div>
      <div class="chart-body"><canvas id="chart-pred-hourly"></canvas></div>
    </div>
  </div>

  <div class="charts-grid">
    <div class="chart-card">
      <div class="chart-header"><h3>Ảnh hưởng thời tiết đến tắc đường</h3></div>
      <div class="chart-body"><canvas id="chart-pred-weather-impact"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>Tốc độ hiện tại vs Xác suất tắc</h3></div>
      <div class="chart-body"><canvas id="chart-pred-speed-vs-prob"></canvas></div>
    </div>
  </div>

  <div class="table-container" style="margin-top:8px">
    <div style="padding:16px 20px;display:flex;gap:12px;flex-wrap:wrap;align-items:center">
      <span style="font-weight:700;font-size:14px">Chi tiết dự đoán tất cả tuyến đường</span>
      <input id="pred-search" type="text" placeholder="Tìm road_id..." oninput="filterPredTable()"
        style="padding:6px 12px;border-radius:8px;font-size:12px;background:var(--surface-container);color:var(--on-surface);flex:1;max-width:240px">
      <select id="pred-filter-status" onchange="filterPredTable()"
        style="padding:6px 12px;border-radius:8px;font-size:12px;background:var(--surface-container);color:var(--on-surface)">
        <option value="">Tất cả</option>
        <option value="congested">Sắp tắc</option>
        <option value="normal">Bình thường</option>
      </select>
      <select id="pred-sort" onchange="filterPredTable()"
        style="padding:6px 12px;border-radius:8px;font-size:12px;background:var(--surface-container);color:var(--on-surface)">
        <option value="prob_desc">Xác suất cao nhất</option>
        <option value="prob_asc">Xác suất thấp nhất</option>
        <option value="delay_desc">Trễ nhiều nhất</option>
      </select>
    </div>
    <table>
      <thead><tr>
        <th>Tuyến đường</th><th>Tốc độ hiện tại</th><th>Xác suất tắc</th>
        <th>Trạng thái dự đoán</th><th>Trễ dự kiến</th><th>Mức rủi ro</th><th>Khuyến nghị</th>
      </tr></thead>
      <tbody id="pred-tbody"></tbody>
    </table>
  </div>
`;

// Expose globally
window.PAGES = PAGES;

// === PHÂN TÍCH & DỰ BÁO ===
PAGES.analysis = () => `
  <div class="page-header">
    <h1>Phân tích & Dự báo</h1>
    <div class="header-actions">
      <span class="ws-badge disconnected" id="ws-badge">Disconnected</span>
      <span class="last-update" id="last-update"></span>
    </div>
  </div>

  <!-- SECTION 1: Trạng thái giao thông hiện tại -->
  <div class="section-label">Trạng thái giao thông hiện tại</div>
  <div class="kpi-grid" style="margin-bottom:24px">
    <div class="kpi-card kpi-danger">
      <div class="kpi-label">Tắc nghẽn (dưới 20 km/h)</div>
      <div class="kpi-value" id="an-congested">-</div>
      <div class="kpi-sub">tuyến đường</div>
    </div>
    <div class="kpi-card kpi-warn">
      <div class="kpi-label">Lưu thông chậm (20–40 km/h)</div>
      <div class="kpi-value" id="an-slow">-</div>
      <div class="kpi-sub">tuyến đường</div>
    </div>
    <div class="kpi-card kpi-ok">
      <div class="kpi-label">Lưu thông bình thường</div>
      <div class="kpi-value" id="an-normal">-</div>
      <div class="kpi-sub">tuyến đường</div>
    </div>
    <div class="kpi-card kpi-info">
      <div class="kpi-label">Tốc độ trung bình toàn mạng</div>
      <div class="kpi-value" id="an-avgspeed">-</div>
      <div class="kpi-sub">km/h</div>
    </div>
  </div>

  <!-- SECTION 2: Vi phạm & Cảnh báo -->
  <div class="section-label">Vi phạm & Cảnh báo an toàn</div>
  <div class="kpi-grid" style="margin-bottom:24px">
    <div class="kpi-card kpi-danger">
      <div class="kpi-label">Vi phạm tốc độ</div>
      <div class="kpi-value" id="an-speeding">-</div>
      <div class="kpi-sub" id="an-speeding-rate">-% tổng phương tiện</div>
    </div>
    <div class="kpi-card kpi-warn">
      <div class="kpi-label">Cảnh báo nhiên liệu thấp</div>
      <div class="kpi-value" id="an-lowfuel">-</div>
      <div class="kpi-sub" id="an-lowfuel-rate">-% tổng phương tiện</div>
    </div>
    <div class="kpi-card kpi-info">
      <div class="kpi-label">Nhiên liệu trung bình</div>
      <div class="kpi-value" id="an-fuel-avg">-</div>
      <div class="kpi-sub">%</div>
    </div>
    <div class="kpi-card kpi-ok">
      <div class="kpi-label">Quãng đường còn lại (TB)</div>
      <div class="kpi-value" id="an-range">-</div>
      <div class="kpi-sub">km (ước tính)</div>
    </div>
  </div>

  <div class="charts-grid">
    <div class="chart-card">
      <div class="chart-header">
        <h3>Vi phạm tốc độ theo loại xe</h3>
        <span class="chart-badge live">LIVE</span>
      </div>
      <div style="padding:8px 16px;font-size:12px;color:var(--text3)">Phân bổ số lượt vi phạm tốc độ theo từng loại phương tiện trong 2M bản ghi</div>
      <div class="chart-body"><canvas id="chart-an-vtype"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header">
        <h3>Phân bổ mức nhiên liệu</h3>
        <span class="chart-badge live">LIVE</span>
      </div>
      <div style="padding:8px 16px;font-size:12px;color:var(--text3)">Tỷ lệ phương tiện theo mức nhiên liệu — dưới 20% cần chú ý</div>
      <div class="chart-body"><canvas id="chart-an-fuel"></canvas></div>
    </div>
  </div>

  <!-- SECTION 3: Điểm ùn tắc thường xuyên -->
  <div class="section-label" style="margin-top:24px">Điểm ùn tắc thường xuyên</div>
  <div style="padding:0 0 12px;font-size:13px;color:var(--text3)">
    Phân tích từ 2,000,000 bản ghi — tuyến nào có tỷ lệ tắc nghẽn cao nhất, delay trung bình bao nhiêu phút
  </div>
  <div class="charts-grid">
    <div class="chart-card full-width">
      <div class="chart-header">
        <h3>Tỷ lệ tắc nghẽn cao (High %) theo tuyến đường</h3>
        <span class="chart-badge live">LIVE</span>
      </div>
      <div style="padding:8px 16px;font-size:12px;color:var(--text3)">Màu đỏ = tắc nghẽn cao thường xuyên. Đây là các điểm đen cần ưu tiên điều tiết giao thông.</div>
      <div class="chart-body tall" style="height:480px"><canvas id="chart-an-hotspot"></canvas></div>
    </div>
    <div class="chart-card full-width">
      <div class="chart-header">
        <h3>Thời gian trễ trung bình theo tuyến (phút)</h3>
        <span class="chart-badge live">LIVE</span>
      </div>
      <div style="padding:8px 16px;font-size:12px;color:var(--text3)">Delay dự kiến khi đi qua tuyến này trong điều kiện bình thường. Màu đỏ = trễ nhiều.</div>
      <div class="chart-body tall" style="height:480px"><canvas id="chart-an-delay"></canvas></div>
    </div>
  </div>

  <!-- SECTION 4: Dự báo ML -->
  <div class="section-label" style="margin-top:24px">Dự báo tắc nghẽn (Machine Learning)</div>
  <div style="padding:0 0 12px;font-size:13px;color:var(--text3)">
    Model ML dự đoán xác suất tắc nghẽn trong 5–10 phút tới dựa trên tốc độ hiện tại, thời tiết, lịch sử
  </div>
  <div class="kpi-grid" style="margin-bottom:24px">
    <div class="kpi-card kpi-danger">
      <div class="kpi-label">Tuyến sắp tắc (xác suất ≥ 50%)</div>
      <div class="kpi-value" id="an-pred-danger">-</div>
      <div class="kpi-sub">tuyến đường</div>
    </div>
    <div class="kpi-card kpi-ok">
      <div class="kpi-label">Tuyến thông thoáng</div>
      <div class="kpi-value" id="an-pred-safe">-</div>
      <div class="kpi-sub">tuyến đường</div>
    </div>
    <div class="kpi-card kpi-info">
      <div class="kpi-label">Xác suất tắc TB toàn mạng</div>
      <div class="kpi-value" id="an-pred-avg">-</div>
      <div class="kpi-sub">%</div>
    </div>
    <div class="kpi-card kpi-warn">
      <div class="kpi-label">Delay dự kiến TB</div>
      <div class="kpi-value" id="an-pred-delay">-</div>
      <div class="kpi-sub">phút</div>
    </div>
  </div>
  <div class="charts-grid">
    <div class="chart-card full-width">
      <div class="chart-header">
        <h3>Xác suất tắc nghẽn theo tuyến đường (5 phút tới)</h3>
        <span class="chart-badge" style="background:#fee2e2;color:#dc2626">ML</span>
      </div>
      <div style="padding:8px 16px;font-size:12px;color:var(--text3)">Đỏ = nguy hiểm cao (≥70%), Vàng = cần theo dõi (40–70%), Xanh = an toàn (&lt;40%)</div>
      <div class="chart-body tall" style="height:400px"><canvas id="chart-an-pred"></canvas></div>
    </div>
  </div>

  <!-- SECTION 5: Tuyến đang tắc realtime -->
  <div class="section-label" style="margin-top:24px">Tuyến đường đang tắc nghẽn ngay lúc này</div>
  <div class="table-container">
    <table>
      <thead><tr>
        <th>Tuyến đường</th><th>Quận</th><th>Tốc độ hiện tại</th><th>Delay dự kiến</th><th>Mức rủi ro</th><th>Khuyến nghị</th>
      </tr></thead>
      <tbody id="an-cong-tbody">
        <tr><td colspan="6" style="text-align:center;color:var(--text3);padding:20px">Đang tải...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- SECTION 6: Top vi phạm -->
  <div class="section-label" style="margin-top:24px">Top tuyến đường vi phạm tốc độ nhiều nhất</div>
  <div style="padding:0 0 12px;font-size:13px;color:var(--text3)">Tổng hợp từ 2M bản ghi — tuyến nào có nhiều phương tiện vượt tốc độ cho phép nhất</div>
  <div class="table-container">
    <table>
      <thead><tr><th>Tuyến đường</th><th>Số lượt vi phạm</th><th>Tỷ lệ / tổng xe tuyến</th></tr></thead>
      <tbody id="an-viol-tbody">
        <tr><td colspan="3" style="text-align:center;color:var(--text3);padding:20px">Đang tải...</td></tr>
      </tbody>
    </table>
  </div>
`;
window.renderViolations = function () {
  const roads = DB.state.roads || [];
  const congested = roads.filter(r => r.status === 'congested');
  const list = document.getElementById('congestion-list');
  if (!list) return;
  if (congested.length === 0) {
    list.innerHTML = '<div class="empty-state">✅ Không có tắc nghẽn</div>';
    return;
  }
  list.innerHTML = congested.map(r => `
    <div class="alert-item congested">
      <div class="alert-content">
        <div class="alert-title">${r.road_id}</div>
        <div class="alert-detail">Tốc độ: ${r.avg_speed} km/h | Xe: ${r.vehicle_count}</div>
      </div>
    </div>
  `).join('');
};
window.renderSparkJobs = function () {};
window.renderWorkerNodes = function () {};

// === SMART INDICATORS PAGE ===
PAGES.indicators = () => `
  <div class="page-header">
    <h1>🧠 Chỉ báo thông minh</h1>
    <div class="header-actions">
      <span class="ws-badge disconnected" id="ws-badge">🔴 Disconnected</span>
      <span class="last-update" id="last-update"></span>
    </div>
  </div>

  <div style="margin-bottom:8px;font-size:12px;font-weight:700;color:var(--text3);text-transform:uppercase;letter-spacing:.05em">📡 Trạng thái realtime</div>
  <div class="kpi-grid" style="margin-bottom:24px">
    <div class="kpi-card" style="border-left:4px solid #ef4444">
      <div class="kpi-label">🔴 Đang tắc nghẽn</div><div class="kpi-value" id="ind-congested">-</div>
    </div>
    <div class="kpi-card" style="border-left:4px solid #f59e0b">
      <div class="kpi-label">🟡 Đang chậm</div><div class="kpi-value" id="ind-slow">-</div>
    </div>
    <div class="kpi-card" style="border-left:4px solid #22c55e">
      <div class="kpi-label">🟢 Bình thường</div><div class="kpi-value" id="ind-normal">-</div>
    </div>
    <div class="kpi-card" style="border-left:4px solid #f97316">
      <div class="kpi-label">⚡ Vi phạm tốc độ</div><div class="kpi-value" id="ind-speeding">-</div>
    </div>
    <div class="kpi-card" style="border-left:4px solid #8b5cf6">
      <div class="kpi-label">⛽ Cảnh báo nhiên liệu</div><div class="kpi-value" id="ind-lowfuel">-</div>
    </div>
    <div class="kpi-card" style="border-left:4px solid #06b6d4">
      <div class="kpi-label">🛣️ Quãng đường TB còn lại</div>
      <div class="kpi-value" id="ind-range">- <span style="font-size:14px;color:var(--text3)">km</span></div>
    </div>
    <div class="kpi-card" style="border-left:4px solid #ec4899">
      <div class="kpi-label">📊 Tỷ lệ vi phạm</div>
      <div class="kpi-value" id="ind-viol-rate">- <span style="font-size:14px;color:var(--text3)">%</span></div>
    </div>
    <div class="kpi-card" style="border-left:4px solid #10b981">
      <div class="kpi-label">⛽ Nhiên liệu TB</div>
      <div class="kpi-value" id="ind-fuel-avg">- <span style="font-size:14px;color:var(--text3)">%</span></div>
    </div>
  </div>

  <div class="charts-grid">
    <div class="chart-card">
      <div class="chart-header"><h3>⚡ Vi phạm tốc độ theo loại xe</h3>${_liveBadge()}</div>
      <div class="chart-body"><canvas id="chart-ind-vtype"></canvas></div>
    </div>
    <div class="chart-card">
      <div class="chart-header"><h3>⛽ Phân bổ mức nhiên liệu</h3>${_liveBadge()}</div>
      <div class="chart-body"><canvas id="chart-ind-fuel"></canvas></div>
    </div>
    <div class="chart-card full-width">
      <div class="chart-header"><h3>🔥 Điểm ùn tắc nặng nhất (High %)</h3>${_liveBadge()}</div>
      <div class="chart-body tall" style="height:460px"><canvas id="chart-ind-hotspot"></canvas></div>
    </div>
    <div class="chart-card full-width">
      <div class="chart-header"><h3>⏱️ Delay dự báo theo tuyến đường (phút)</h3>${_liveBadge()}</div>
      <div class="chart-body tall" style="height:460px"><canvas id="chart-ind-delay"></canvas></div>
    </div>
    <div class="chart-card full-width">
      <div class="chart-header"><h3>⚠️ Rủi ro trung bình theo tuyến</h3>${_liveBadge()}</div>
      <div class="chart-body tall" style="height:360px"><canvas id="chart-ind-risk"></canvas></div>
    </div>
  </div>

  <div class="table-container" style="margin-top:8px">
    <div style="padding:16px 20px;border-bottom:1px solid var(--border);font-weight:700;font-size:14px">
      ⚡ Top tuyến đường vi phạm tốc độ nhiều nhất
    </div>
    <table>
      <thead><tr><th>Tuyến đường</th><th>Số vi phạm</th></tr></thead>
      <tbody id="ind-viol-tbody"></tbody>
    </table>
  </div>

  <div class="table-container" style="margin-top:8px">
    <div style="padding:16px 20px;border-bottom:1px solid var(--border);font-weight:700;font-size:14px">
      🔴 Tuyến đường đang tắc nghẽn
    </div>
    <table>
      <thead><tr><th>Tuyến đường</th><th>Quận</th><th>Tốc độ</th><th>Delay</th><th>Rủi ro</th></tr></thead>
      <tbody id="ind-cong-tbody"></tbody>
    </table>
  </div>
`;
