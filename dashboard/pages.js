// ─── PAGE HTML TEMPLATES ──────────────────────────────────────────────────────

const PAGES = {

  // ── PAGE 1: DASHBOARD ──────────────────────────────────────────────────────
  dashboard() {
    const s = DB.summary;
    const fmt = n => Number(n).toLocaleString();
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Bảng điều khiển phân tích</div>
      <div class="page-sub">Giám sát giao thông thời gian thực · Trạng thái hệ thống: Ổn định</div>
    </div>
    <div style="display:flex;gap:12px">
      <button class="btn btn-ghost" onclick="navigate('explorer')">Tra cứu dữ liệu</button>
      <button class="btn btn-primary" onclick="navigate('map')">Xem bản đồ</button>
    </div>
  </div>

  <!-- KPI GRID -->
  <div class="kpi-grid">
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="car"></i></div>
        <div class="kpi-delta-tag up"><i data-lucide="trending-up"></i> 12.5%</div>
      </div>
      <div class="kpi-label">Phương tiện đã xử lý</div>
      <div class="kpi-value" id="kpi-total">${fmt(s.total)}</div>
      <div class="kpi-sub">Tổng số bản ghi trong ngày</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="zap"></i></div>
        <div class="kpi-delta-tag up" style="color:var(--accent)"><i data-lucide="trending-up"></i> 23.1%</div>
      </div>
      <div class="kpi-label">Tốc độ trung bình</div>
      <div class="kpi-value" id="kpi-speed">${s.avgSpeed} <span style="font-size:14px;color:var(--text3)">km/h</span></div>
      <div class="kpi-sub">Tốc độ TB trên toàn mạng lưới</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="activity"></i></div>
        <div class="kpi-delta-tag up"><i data-lucide="trending-up"></i> 8.2%</div>
      </div>
      <div class="kpi-label">Phương tiện đang hoạt động</div>
      <div class="kpi-value" id="kpi-active">${fmt(s.active)}</div>
      <div class="kpi-sub">Số xe đang di chuyển hiện tại</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="alert-triangle"></i></div>
        <div class="kpi-delta-tag down"><i data-lucide="trending-down"></i> -2.1%</div>
      </div>
      <div class="kpi-label">Cảnh báo giao thông</div>
      <div class="kpi-value" id="kpi-alerts">${fmt(s.alerts)}</div>
      <div class="kpi-sub">Các vi phạm mức độ cao</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="layers"></i></div>
        <div class="kpi-delta-tag up"><i data-lucide="trending-up"></i> 15.7%</div>
      </div>
      <div class="kpi-label">Khu vực ùn tắc</div>
      <div class="kpi-value" id="kpi-cong">${fmt(s.congested)}</div>
      <div class="kpi-sub">Các điểm có mật độ xe cao</div>
    </div>
  </div>

  <!-- CHARTS ROW 1 -->
  <div class="grid-31">
    <div class="card">
      <div class="card-header">
        <div><div class="card-title">Hiệu suất lưu lượng</div><div class="card-sub">Lưu lượng xe 24 giờ qua</div></div>
        <select class="filter-select"><option>12 tháng qua</option></select>
      </div>
      <div class="chart-wrap tall"><canvas id="chart-flow"></canvas></div>
    </div>
    <div class="card">
      <div class="card-header">
        <div><div class="card-title">Cơ cấu phương tiện</div><div class="card-sub">Phân bổ theo loại xe</div></div>
      </div>
      <div class="chart-wrap tall"><canvas id="chart-type-pie"></canvas></div>
    </div>
  </div>

  <!-- BOTTOM ROW -->
  <div class="grid-2">
    <div class="card">
      <div class="card-header">
        <div><div class="card-title">Hoạt động hệ thống gần đây</div></div>
        <button class="btn btn-ghost" onclick="navigate('monitor')">Xem tất cả</button>
      </div>
      <div class="activity-list" id="activity-log-summary"></div>
    </div>
    <div class="card">
      <div class="card-header">
        <div><div class="card-title">Phân bổ tốc độ</div></div>
        <span class="card-badge">Biểu đồ tần suất</span>
      </div>
      <div class="chart-wrap"><canvas id="chart-speed-dist"></canvas></div>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 2: TRAFFIC MAP ────────────────────────────────────────────────────
  map() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Bản đồ Giao thông</div>
      <div class="page-sub">Tọa độ thời gian thực · Lớp mật độ giao thông · Hệ thống đang cập nhật</div>
    </div>
    <div style="display:flex;gap:12px">
      <button class="btn btn-ghost" id="map-refresh-btn" onclick="refreshMapPoints()"><i data-lucide="refresh-cw" style="width:14px;height:14px;margin-right:6px"></i> Cập nhật</button>
      <button class="btn btn-primary" onclick="toggleHeatmap()">Chế độ nhiệt</button>
    </div>
  </div>

  <div class="grid-31">
    <!-- MAP AREA -->
    <div class="card">
      <div class="map-controls">
        <div class="search-bar" style="max-width:300px">
          <i data-lucide="search"></i>
          <input type="text" placeholder="Tìm phương tiện..." id="map-search-input">
        </div>
        <select class="filter-select" id="map-district" onchange="applyMapFilter()">
          <option value="">Tất cả Quận/Huyện</option>
          ${DISTRICTS.map(d => `<option>${d}</option>`).join('')}
        </select>
        <div class="map-legend">
          <div class="legend-item"><div class="legend-dot" style="background:#ef4444"></div>Dày đặc</div>
          <div class="legend-item"><div class="legend-dot" style="background:#f59e0b"></div>Trung bình</div>
          <div class="legend-item"><div class="legend-dot" style="background:#10b981"></div>Thông thoáng</div>
        </div>
      </div>
      <div id="traffic-map"></div>
    </div>

    <!-- SIDE PANEL -->
    <div style="display:flex;flex-direction:column;gap:20px">
      <div class="card">
        <div class="card-title" style="margin-bottom:14px">Điểm nóng giao thông</div>
        <div id="hotspot-list"></div>
      </div>
      <div class="card">
        <div class="card-title" style="margin-bottom:14px">Hiệu suất khu vực</div>
        <div id="speed-meter-list"></div>
      </div>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 3: DATA EXPLORER ──────────────────────────────────────────────────
  explorer() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Tra cứu dữ liệu</div>
      <div class="page-sub">Truy xuất thông tin cho ${Number(DB.totalGenerated).toLocaleString()} phương tiện</div>
    </div>
    <div style="display:flex;gap:12px">
      <button class="btn btn-ghost" onclick="exportCSV()">Tải CSV</button>
      <button class="btn btn-primary" onclick="updateExplorer()">Áp dụng lọc</button>
    </div>
  </div>

  <div class="card">
    <div class="filter-bar">
      <div class="search-bar" style="flex:1">
        <i data-lucide="search"></i>
        <input id="ex-search" placeholder="Tìm mã xe, chủ sở hữu..." oninput="updateExplorer()"/>
      </div>
      <select class="filter-select" id="ex-vtype" onchange="updateExplorer()">
        <option value="">Tất cả loại xe</option>
        ${V_TYPES.map(v => `<option>${v}</option>`).join('')}
      </select>
      <select class="filter-select" id="ex-district" onchange="updateExplorer()">
        <option value="">Tất cả Quận/Huyện</option>
        ${DISTRICTS.map(d => `<option>${d}</option>`).join('')}
      </select>
      <select class="filter-select" id="ex-limit" onchange="updateExplorer()">
        <option value="50">50 dòng</option>
        <option value="100" selected>100 dòng</option>
        <option value="500">500 dòng</option>
      </select>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th onclick="sortExplorer('vehicle_id')">Mã xe</th>
            <th onclick="sortExplorer('owner_name')">Chủ sở hữu</th>
            <th>Biển số</th>
            <th onclick="sortExplorer('speed_kmph')">Tốc độ</th>
            <th>Tên đường</th>
            <th onclick="sortExplorer('district')">Quận/Huyện</th>
            <th>Nhiên liệu</th>
            <th>Trạng thái</th>
          </tr>
        </thead>
        <tbody id="explorer-tbody"></tbody>
      </table>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 4: VEHICLE ANALYTICS ──────────────────────────────────────────────
  vehicle() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Phân tích phương tiện</div>
      <div class="page-sub">Phân đoạn phương tiện · Mô hình hóa hành vi · Đánh giá tác động</div>
    </div>
  </div>

  <div class="grid-2">
    <div class="card">
      <div class="card-header">
        <div class="card-title">Phân bổ tốc độ</div>
        <span class="card-badge blue">Dữ liệu tổng hợp</span>
      </div>
      <div class="chart-wrap tall"><canvas id="chart-speed-hist"></canvas></div>
    </div>
    <div class="card">
      <div class="card-header">
        <div class="card-title">Giám sát mức nhiên liệu</div>
        <span class="card-badge yellow">Phân tích hiệu quả</span>
      </div>
      <div class="chart-wrap tall"><canvas id="chart-fuel"></canvas></div>
    </div>
  </div>

  <div class="grid-3">
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">Lượng hành khách</div>
      <div class="chart-wrap"><canvas id="chart-pass-type"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">Ảnh hưởng thời tiết</div>
      <div class="chart-wrap"><canvas id="chart-weather"></canvas></div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">Lưu lượng theo Quận</div>
      <div class="chart-wrap"><canvas id="chart-dist-share"></canvas></div>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 5: ALERTS & VIOLATIONS ────────────────────────────────────────────
  alerts() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">Cảnh báo & Vi phạm</div>
      <div class="page-sub">Tự động phát hiện rủi ro an toàn · Hàng đợi thông báo trực tiếp</div>
    </div>
    <div style="display:flex;gap:12px">
      <button class="btn btn-ghost" onclick="clearAlerts()">Bỏ qua tất cả</button>
      <button class="btn btn-primary" onclick="exportViolations()">Xuất lưu trữ</button>
    </div>
  </div>

  <div class="kpi-grid" style="margin-bottom:24px">
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="zap" style="color:var(--red)"></i></div>
        <div class="kpi-delta-tag down">Nghiêm trọng</div>
      </div>
      <div class="kpi-label">Sự kiện quá tốc độ</div>
      <div class="kpi-value" id="vio-speed-count">0</div>
      <div class="kpi-sub">Ngưỡng > 80 km/h</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="droplet" style="color:var(--yellow)"></i></div>
        <div class="kpi-delta-tag up">Cảnh báo</div>
      </div>
      <div class="kpi-label">Cảnh báo nhiên liệu thấp</div>
      <div class="kpi-value" id="vio-fuel-count">0</div>
      <div class="kpi-sub">Mức < 15%</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-icon-row">
        <div class="kpi-icon-box"><i data-lucide="alert-circle" style="color:var(--purple)"></i></div>
        <div class="kpi-delta-tag up">Đang xảy ra</div>
      </div>
      <div class="kpi-label">Sự cố ùn tắc</div>
      <div class="kpi-value" id="vio-cong-count">0</div>
      <div class="kpi-sub">Phát hiện điểm nghẽn</div>
    </div>
  </div>

  <div class="card">
    <div class="card-header">
      <div class="card-title">Luồng hồ sơ vi phạm</div>
      <select class="filter-select" id="vio-filter-type" onchange="renderViolations()">
        <option value="">Tất cả mức độ</option>
        <option>QUÁ_TỐC_ĐỘ</option><option>HẾT_XĂNG</option><option>ÙN_TẮC</option>
      </select>
    </div>
    <div class="table-wrap">
      <table>
        <thead>
          <tr>
            <th>Phương tiện</th><th>Chủ sở hữu</th><th>Loại vi phạm</th>
            <th>Thông số</th><th>Vị trí</th><th>Thời gian</th><th>Thao tác</th>
          </tr>
        </thead>
        <tbody id="violations-tbody"></tbody>
      </table>
    </div>
  </div>
</div>`;
  },

  // ── PAGE 6: QUERY ENGINE ───────────────────────────────────────────────────
  // Query Engine section removed as requested for non-admin interface
  query() {
    return `<div style="padding:40px; text-align:center; color:var(--text3)">Trang này đã bị gỡ theo yêu cầu người dùng.</div>`;
  },

  // ── PAGE 7: SYSTEM MONITOR ─────────────────────────────────────────────────
  monitor() {
    return `
<div class="page-enter">
  <div class="page-header">
    <div>
      <div class="page-title">System Monitor</div>
      <div class="page-sub">Infrastructure health · Node resource management</div>
    </div>
    <span class="card-badge green">● Operational</span>
  </div>

  <div class="grid-2">
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">Spark Job Queue</div>
      <div id="spark-jobs"></div>
    </div>
    <div class="card">
      <div class="card-title" style="margin-bottom:20px">Cluster Resource Usage</div>
      <div class="chart-wrap"><canvas id="chart-kafka"></canvas></div>
    </div>
  </div>

  <div class="card">
    <div class="card-title" style="margin-bottom:20px">Worker Health Metadata</div>
    <div id="worker-nodes"></div>
  </div>
</div>`;
  },
};
window.PAGES = PAGES;
