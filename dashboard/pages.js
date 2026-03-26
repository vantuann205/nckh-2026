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
      <div class="kpi-card">
        <div class="kpi-icon" style="background: linear-gradient(135deg, #8b5cf6, #7c3aed);">
          <i data-lucide="users" style="color:#fff"></i>
        </div>
        <div class="kpi-content">
          <div class="kpi-label">Tổng hành khách</div>
          <div class="kpi-value" id="kpi-passengers">${fmt(s.total_passengers)}</div>
        </div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon" style="background: linear-gradient(135deg, #06b6d4, #0891b2);">
          <i data-lucide="fuel" style="color:#fff"></i>
        </div>
        <div class="kpi-content">
          <div class="kpi-label">Nhiên liệu TB</div>
          <div class="kpi-value" id="kpi-fuel">${s.avg_fuel_level || 0} <span style="font-size:14px;color:var(--text3)">%</span></div>
        </div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon" style="background: linear-gradient(135deg, #f97316, #ea580c);">
          <i data-lucide="zap" style="color:#fff"></i>
        </div>
        <div class="kpi-content">
          <div class="kpi-label">Cảnh báo tốc độ</div>
          <div class="kpi-value" id="kpi-speeding">${fmt(s.speeding_alerts)}</div>
        </div>
      </div>
      <div class="kpi-card">
        <div class="kpi-icon" style="background: linear-gradient(135deg, #ec4899, #db2777);">
          <i data-lucide="battery-low" style="color:#fff"></i>
        </div>
        <div class="kpi-content">
          <div class="kpi-label">Cảnh báo nhiên liệu</div>
          <div class="kpi-value" id="kpi-lowfuel">${fmt(s.low_fuel_alerts)}</div>
        </div>
      </div>
    </div>

    <div class="charts-grid">
      <div class="chart-card">
        <div class="chart-header">
          <h3>Tốc độ theo tuyến đường</h3>
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
      <div class="chart-card">
        <div class="chart-header">
          <h3>Loại phương tiện</h3>
          <span class="chart-badge live">LIVE</span>
        </div>
        <div class="chart-body"><canvas id="chart-vehicle-types"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-header">
          <h3>Mức độ tắc nghẽn</h3>
          <span class="chart-badge live">LIVE</span>
        </div>
        <div class="chart-body"><canvas id="chart-congestion-levels"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-header">
          <h3>Thời tiết</h3>
          <span class="chart-badge live">LIVE</span>
        </div>
        <div class="chart-body"><canvas id="chart-weather"></canvas></div>
      </div>
      <div class="chart-card">
        <div class="chart-header">
          <h3>Traffic vs Time</h3>
          <span class="chart-badge live">API</span>
        </div>
        <div class="chart-body"><canvas id="chart-traffic-time"></canvas></div>
      </div>
      <div class="chart-card full-width">
        <div class="chart-header">
          <h3>Tốc độ chi tiết theo tuyến đường</h3>
          <span class="chart-badge live">LIVE</span>
        </div>
        <div class="chart-body tall"><canvas id="chart-speed-overview"></canvas></div>
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
        <span class="legend-item"><span class="legend-dot" style="background:#22c55e"></span> Bình thường</span>
        <span class="legend-item"><span class="legend-dot" style="background:#f59e0b"></span> Chậm</span>
        <span class="legend-item"><span class="legend-dot" style="background:#ef4444"></span> Tắc nghẽn</span>
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

// === ALERTS ===
PAGES.alerts = () => `
  <div class="page-header">
    <h1>🚨 Cảnh báo tắc nghẽn</h1>
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
