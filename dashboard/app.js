// ─── APP CONTROLLER ───────────────────────────────────────────────────────────
let CURRENT_PAGE = 'dashboard';
let EXPLORER_SORT = { col: 'speed_kmph', dir: -1 };
let VIOLATIONS_DATA = [];

// ── ROUTER ────────────────────────────────────────────────────────────────────
async function navigate(page, navEl) {
  // Avoid full re-render if we are already on this page
  if (page === CURRENT_PAGE && !navEl) {
    console.log(`♻️ Skipping full re-render for ${page}, updating data instead...`);
    updatePageData(page);
    return;
  }

  CURRENT_PAGE = page;

  // Update Sidebar Active State
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  if (navEl && navEl.classList) {
    navEl.classList.add('active');
  } else {
    // Find item with matching onclick or data-page
    const items = document.querySelectorAll('.nav-item');
    items.forEach(item => {
      if (item.getAttribute('onclick')?.includes(`'${page}'`)) {
        item.classList.add('active');
      }
    });
  }

  // Update Breadcrumbs
  const crumbs = {
    dashboard: 'Bảng điều khiển / Tổng quan',
    map: 'Bảng điều khiển / Bản đồ giao thông',
    explorer: 'Quản lý dữ liệu / Tra cứu',
    vehicle: 'Quản lý dữ liệu / Phân tích',
    alerts: 'Vận hành / Cảnh báo',
    monitor: 'Hệ thống / Giám sát',
  };
  const bcSpan = document.querySelector('.breadcrumbs span');
  if (bcSpan) bcSpan.textContent = crumbs[page] || `Bảng điều khiển / ${page}`;

  // Inject Template
  const content = document.getElementById('content');
  const template = PAGES[page];
  if (!template) {
    content.innerHTML = `<div style="padding:40px;color:var(--text2)">Không tìm thấy trang "${page}"</div>`;
    return;
  }

  console.log(`🚀 Rendering page: ${page}`);
  content.innerHTML = template();
  content.scrollTop = 0;

  // Initialize Lucide Icons
  if (window.lucide) lucide.createIcons();

  // Post-render setup
  switch (page) {
    case 'dashboard':
      renderDashboardCharts();
      break;
    case 'map':
      console.log('🗺️ Map page detected, calling initMap...');
      setTimeout(() => {
        if (window.initMap) window.initMap();
        else console.warn('🗺️ window.initMap not found!');
      }, 150);
      break;
    case 'explorer':
      updateExplorer();
      break;
    case 'vehicle':
      renderVehicleCharts();
      break;
    case 'alerts':
      const rows = await DB.query({ limit: 100 });
      VIOLATIONS_DATA = rows;
      if (window.renderAlertCharts) renderAlertCharts(VIOLATIONS_DATA);
      if (window.renderViolations) renderViolations();
      break;
    case 'monitor':
      if (window.renderMonitorCharts) renderMonitorCharts();
      if (window.renderSparkJobs) renderSparkJobs();
      if (window.renderWorkerNodes) renderWorkerNodes();
      break;
  }
}

// ── EXPLORER ──────────────────────────────────────────────────────────────────
async function updateExplorer() {
  const search = document.getElementById('ex-search')?.value || '';
  const vtype = document.getElementById('ex-vtype')?.value || '';
  const district = document.getElementById('ex-district')?.value || '';
  const limit = parseInt(document.getElementById('ex-limit')?.value || 100);

  let rows = await DB.query({ search, vtype, district, limit });

  // Sort
  rows = rows.slice().sort((a, b) => {
    const aVal = a[EXPLORER_SORT.col];
    const bVal = b[EXPLORER_SORT.col];
    return typeof aVal === 'number' ? (aVal - bVal) * EXPLORER_SORT.dir : (String(aVal).localeCompare(String(bVal))) * EXPLORER_SORT.dir;
  });

  renderExplorerTable(rows);
}

function renderExplorerTable(rows) {
  const tbody = document.getElementById('explorer-tbody');
  if (!tbody) return;
  const fuelColor = f => f < 15 ? 'var(--red)' : f < 40 ? '#d97706' : 'var(--green)';
  const speedColor = s => s > 80 ? 'var(--red)' : s > 60 ? '#d97706' : 'var(--text)';

  tbody.innerHTML = rows.map(r => `
    <tr>
      <td class="mono" style="color:var(--accent); font-weight:600">${r.vehicle_id}</td>
      <td style="font-weight:500">${r.owner_name}</td>
      <td class="mono" style="font-size:11px; opacity:0.7">${r.license_number}</td>
      <td><span style="font-weight:700;color:${speedColor(r.speed_kmph)}">${r.speed_kmph}</span> <span style="font-size:11px; color:var(--text3)">km/h</span></td>
      <td style="color:var(--text2)">${r.street}</td>
      <td>${r.district}</td>
      <td><div style="display:flex; align-items:center; gap:8px">
        <div style="flex:1; height:4px; background:#f1f5f9; border-radius:10px; min-width:40px">
            <div style="width:${r.fuel_level}%; height:100%; background:${fuelColor(r.fuel_level)}; border-radius:10px"></div>
        </div>
        <span style="font-size:12px; font-weight:600; color:${fuelColor(r.fuel_level)}">${r.fuel_level || 0}%</span>
      </div></td>
      <td>${r.speed_kmph > 80 ? '<span class="badge red">🏎 Speeding</span>' : r.fuel_level < 15 ? '<span class="badge yellow">⛽ Low Fuel</span>' : '<span class="badge green">✓ Normal</span>'}</td>
    </tr>
  `).join('');
}

// ── BOOT ──────────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  const content = document.getElementById('content');
  content.innerHTML = `
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:24px; color:var(--text)">
      <div style="width:56px; height:56px; border-radius:16px; background:var(--accent); color:#fff; display:grid; place-items:center; box-shadow: 0 10px 15px -3px rgba(37, 99, 235, 0.2)">
        <i data-lucide="zap" style="width:28px; height:28px"></i>
      </div>
      <div style="text-align:center">
        <div style="font-size:24px;font-weight:800; letter-spacing:-0.03em; margin-bottom:4px">Hệ thống Phân tích Giao thông</div>
        <div style="font-size:14px;color:var(--text3)" id="boot-status">Đang kết nối trung tâm dữ liệu...</div>
      </div>
      <div style="width:280px;background:#f1f5f9;border-radius:99px;height:8px; overflow:hidden">
        <div id="boot-bar" style="height:100%;border-radius:99px;background:var(--accent);width:0;transition:width .4s ease-out"></div>
      </div>
      <div style="font-size:12px;font-family:var(--mono);color:var(--text3); opacity:0.6" id="boot-rows">Đang khởi tạo công cụ Big Data...</div>
    </div>`;

  if (window.lucide) lucide.createIcons();

  DB.init(
    (pct) => {
      const bar = document.getElementById('boot-bar');
      const info = document.getElementById('boot-rows');
      if (bar) bar.style.width = pct + '%';
      if (info) info.textContent = `Đang tối ưu hóa dữ liệu: ${pct}%`;
    },
    () => {
      setTimeout(() => navigate('dashboard'), 300);
    }
  );
});

// Real-time Event Listener - Smart Refresh
window.addEventListener('lakehouse-update', () => {
  console.log('📢 Lakehouse update detected. Updating current page data...');
  updatePageData(CURRENT_PAGE);
});

async function updatePageData(page) {
  switch (page) {
    case 'dashboard':
      // Update KPIs in place
      const s = DB.summary;
      const fmt = n => Number(n).toLocaleString();
      if (document.getElementById('kpi-total')) document.getElementById('kpi-total').textContent = fmt(s.total);
      if (document.getElementById('kpi-speed')) document.getElementById('kpi-speed').innerHTML = `${s.avgSpeed} <span style="font-size:14px;color:var(--text3)">km/h</span>`;
      if (document.getElementById('kpi-active')) document.getElementById('kpi-active').textContent = fmt(s.active);
      if (document.getElementById('kpi-alerts')) document.getElementById('kpi-alerts').textContent = fmt(s.alerts);
      if (document.getElementById('kpi-cong')) document.getElementById('kpi-cong').textContent = fmt(s.congested);
      // Charts usually need re-render or update
      renderDashboardCharts();
      break;
    case 'map':
      // Only refresh points, don't re-init map!
      if (window.renderMapPoints) renderMapPoints();
      break;
    case 'explorer':
      updateExplorer();
      break;
    case 'vehicle':
      renderVehicleCharts();
      break;
    case 'alerts':
      renderViolations();
      break;
  }
}

// Globals
window.navigate = navigate;
window.updateExplorer = updateExplorer;
window.toggleSidebar = () => document.getElementById('sidebar').classList.toggle('collapsed');
