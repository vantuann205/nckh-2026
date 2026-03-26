// ─── APP CONTROLLER ───────────────────────────────────────────────────────────
let CURRENT_PAGE = 'dashboard';

// ── ROUTER ────────────────────────────────────────────────────────────────────
async function navigate(page, navEl) {
  if (page === CURRENT_PAGE && !navEl) {
    updatePageData(page);
    return;
  }

  CURRENT_PAGE = page;

  // Update Sidebar Active State
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  if (navEl && navEl.classList) {
    navEl.classList.add('active');
  } else {
    document.querySelectorAll('.nav-item').forEach(item => {
      if (item.getAttribute('onclick')?.includes(`'${page}'`)) item.classList.add('active');
    });
  }

  // Update Breadcrumbs
  const crumbs = {
    dashboard: 'Bảng điều khiển / Tổng quan',
    map: 'Bảng điều khiển / Bản đồ giao thông',
    explorer: 'Quản lý dữ liệu / Tra cứu',
    vehicle: 'Quản lý dữ liệu / Mật độ xe',
    alerts: 'Vận hành / Cảnh báo tắc nghẽn',
    accidents: 'Vận hành / Tai nạn giao thông',
    weather: 'Môi trường / Thời tiết',
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

  content.innerHTML = template();
  content.scrollTop = 0;

  // Initialize Lucide Icons
  if (window.lucide) lucide.createIcons();

  // Update WS badge
  updateWSBadge();

  // Post-render setup
  switch (page) {
    case 'dashboard':
      renderDashboardCharts();
      break;
    case 'map':
      setTimeout(() => { if (window.initMap) window.initMap(); }, 150);
      break;
    case 'explorer':
      updateExplorer();
      break;
    case 'vehicle':
      renderVehicleCharts();
      renderRoadList();
      break;
    case 'alerts':
      if (window.renderAlertCharts) renderAlertCharts();
      if (window.renderViolations) renderViolations();
      break;
    case 'accidents':
      if (window.renderAccidents) renderAccidents();
      break;
    case 'weather':
      if (window.renderWeather) renderWeather();
      break;
    case 'monitor':
      updateMonitor();
      if (window.renderMonitorCharts) renderMonitorCharts();
      break;
  }
}

// ── EXPLORER ──────────────────────────────────────────────────────────────────
function updateExplorer() {
  const search = (document.getElementById('ex-search')?.value || '').toLowerCase();
  const status = document.getElementById('ex-status')?.value || '';

  let roads = DB.state.roads || [];

  // Filter
  if (search) {
    roads = roads.filter(r => (r.road_id || '').toLowerCase().includes(search));
  }
  if (status) {
    roads = roads.filter(r => r.status === status);
  }

  renderExplorerTable(roads);
}

function renderExplorerTable(roads) {
  const tbody = document.getElementById('explorer-tbody');
  if (!tbody) return;

  const statusBadge = (s) => {
    if (s === 'congested') return '<span class="badge red">🔴 Tắc</span>';
    if (s === 'slow') return '<span class="badge yellow">🟡 Chậm</span>';
    return '<span class="badge green">🟢 Bình thường</span>';
  };

  tbody.innerHTML = roads.map(r => `
    <tr>
      <td class="mono" style="color:var(--accent); font-weight:600">${r.road_id || ''}</td>
      <td><span style="font-weight:700;color:${parseFloat(r.avg_speed || 0) < 20 ? 'var(--red)' : parseFloat(r.avg_speed || 0) < 40 ? '#d97706' : 'var(--text)'}">${r.avg_speed || 0}</span> <span style="font-size:11px;color:var(--text3)">km/h</span></td>
      <td style="font-weight:600">${r.vehicle_count || 0}</td>
      <td class="mono" style="font-size:11px">${parseFloat(r.lat || 0).toFixed(4)}</td>
      <td class="mono" style="font-size:11px">${parseFloat(r.lng || 0).toFixed(4)}</td>
      <td>${statusBadge(r.status)}</td>
      <td style="font-size:11px;color:var(--text3)">${r.updated_at ? new Date(r.updated_at).toLocaleTimeString() : '-'}</td>
    </tr>
  `).join('');
}

// ── Road List (Vehicle page) ──
function renderRoadList() {
  const container = document.getElementById('road-list');
  if (!container) return;

  const roads = DB.state.roads || [];
  const sorted = [...roads].sort((a, b) => parseInt(b.vehicle_count || 0) - parseInt(a.vehicle_count || 0));

  container.innerHTML = `
    <div class="road-list-grid">
      ${sorted.slice(0, 12).map(r => {
        const speed = parseFloat(r.avg_speed || 0);
        const status = r.status || 'normal';
        const color = status === 'congested' ? '#ef4444' : status === 'slow' ? '#f59e0b' : '#22c55e';
        return `
          <div class="road-card">
            <div class="road-card-header">
              <span class="road-card-id">${r.road_id}</span>
              <span class="road-card-status" style="background:${color}20;color:${color}">${status}</span>
            </div>
            <div class="road-card-stats">
              <div><span class="stat-label">Tốc độ</span><span class="stat-value">${speed} km/h</span></div>
              <div><span class="stat-label">Xe</span><span class="stat-value">${r.vehicle_count || 0}</span></div>
            </div>
          </div>
        `;
      }).join('')}
    </div>
  `;
}

// ── Monitor ──
async function updateMonitor() {
  const wsEl = document.getElementById('mon-ws');
  const redisEl = document.getElementById('mon-redis');
  const roadsEl = document.getElementById('mon-roads');
  const updateEl = document.getElementById('mon-update');

  if (wsEl) wsEl.textContent = DB.connected ? '🟢 Connected' : '🔴 Disconnected';
  if (roadsEl) roadsEl.textContent = (DB.state.roads || []).length;
  if (updateEl) updateEl.textContent = DB.state.lastUpdate ? new Date(DB.state.lastUpdate).toLocaleTimeString() : '-';

  try {
    const health = await DB.fetchHealth();
    if (redisEl && health) redisEl.textContent = health.redis?.connected ? '🟢 Connected' : '🔴 Down';
  } catch (e) {
    if (redisEl) redisEl.textContent = '❌ Error';
  }
}

// ── WS Status Badge ──
function updateWSBadge() {
  const badge = document.getElementById('ws-badge');
  if (!badge) return;
  if (DB.connected) {
    badge.textContent = '🟢 Live';
    badge.className = 'ws-badge connected';
  } else {
    badge.textContent = '🔴 Offline';
    badge.className = 'ws-badge disconnected';
  }
}

// ── Real-time Updates ──
window.addEventListener('traffic-update', () => {
  updatePageData(CURRENT_PAGE);
});

window.addEventListener('ws-status', () => {
  updateWSBadge();
});

async function updatePageData(page) {
  const s = DB.summary || {};
  const fmt = n => Number(n || 0).toLocaleString();

  switch (page) {
    case 'dashboard':
      if (document.getElementById('kpi-total')) document.getElementById('kpi-total').textContent = fmt(s.total_roads);
      if (document.getElementById('kpi-speed')) document.getElementById('kpi-speed').innerHTML = `${s.avg_speed || 0} <span style="font-size:14px;color:var(--text3)">km/h</span>`;
      if (document.getElementById('kpi-vehicles')) document.getElementById('kpi-vehicles').textContent = fmt(s.total_vehicles);
      if (document.getElementById('kpi-congested')) document.getElementById('kpi-congested').textContent = fmt(s.congested_roads);
      if (document.getElementById('kpi-passengers')) document.getElementById('kpi-passengers').textContent = fmt(s.total_passengers);
      if (document.getElementById('kpi-fuel')) document.getElementById('kpi-fuel').innerHTML = `${s.avg_fuel_level || 0} <span style="font-size:14px;color:var(--text3)">%</span>`;
      if (document.getElementById('kpi-speeding')) document.getElementById('kpi-speeding').textContent = fmt(s.speeding_alerts);
      if (document.getElementById('kpi-lowfuel')) document.getElementById('kpi-lowfuel').textContent = fmt(s.low_fuel_alerts);
      renderDashboardCharts();
      break;
    case 'map':
      if (window.renderMapPoints) renderMapPoints();
      break;
    case 'explorer':
      updateExplorer();
      break;
    case 'vehicle':
      renderVehicleCharts();
      renderRoadList();
      break;
    case 'alerts':
      if (window.renderAlertCharts) renderAlertCharts();
      if (window.renderViolations) renderViolations();
      break;
    case 'monitor':
      updateMonitor();
      break;
  }

  // Update last update timestamp
  const el = document.getElementById('last-update');
  if (el && DB.state.lastUpdate) {
    el.textContent = `Cập nhật: ${new Date(DB.state.lastUpdate).toLocaleTimeString()}`;
  }
  updateWSBadge();
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
        <div style="font-size:24px;font-weight:800; letter-spacing:-0.03em; margin-bottom:4px">Realtime Traffic Monitor</div>
        <div style="font-size:14px;color:var(--text3)" id="boot-status">Đang kết nối hệ thống streaming...</div>
      </div>
      <div style="width:280px;background:#f1f5f9;border-radius:99px;height:8px; overflow:hidden">
        <div id="boot-bar" style="height:100%;border-radius:99px;background:var(--accent);width:0;transition:width .4s ease-out"></div>
      </div>
      <div style="font-size:12px;font-family:var(--mono);color:var(--text3); opacity:0.6" id="boot-rows">Kafka → Redis → WebSocket</div>
    </div>`;

  if (window.lucide) lucide.createIcons();

  DB.init(
    (pct) => {
      const bar = document.getElementById('boot-bar');
      const info = document.getElementById('boot-rows');
      if (bar) bar.style.width = pct + '%';
      if (info) info.textContent = pct < 50 ? 'Connecting to pipeline...' : 'Loading realtime data...';
    },
    () => {
      setTimeout(() => navigate('dashboard'), 300);
      
      // Start loading progress polling
      DB.startLoadingProgressPolling((progress) => {
        const container = document.getElementById('loading-progress-container');
        const fill = document.getElementById('loading-progress-fill');
        const vehiclesCount = document.getElementById('loading-vehicles-count');
        const vehiclesSpeed = document.getElementById('loading-vehicles-speed');
        const eta = document.getElementById('loading-eta');
        
        if (!container) return;
        
        if (progress.status === 'loading') {
          // Show loading bar
          container.classList.remove('loading-progress-hidden');
          
          // Update metrics
          if (vehiclesCount) vehiclesCount.textContent = (progress.total_vehicles || 0).toLocaleString();
          if (vehiclesSpeed) vehiclesSpeed.textContent = (progress.vehicles_per_sec || 0).toLocaleString();
          if (eta) eta.textContent = (progress.estimated_remaining_sec || 0);
          
          // Calculate and update progress bar
          if (progress.files && Object.keys(progress.files).length > 0) {
            const totalToProcess = Object.values(progress.files).reduce((sum, f) => sum + (f.total || 0), 0);
            const totalProcessed = Object.values(progress.files).reduce((sum, f) => sum + (f.processed || 0), 0);
            const progressPercent = totalToProcess > 0 ? Math.round((totalProcessed / totalToProcess) * 100) : 0;
            if (fill) fill.style.width = progressPercent + '%';
          }
        } else {
          // Hide loading bar
          container.classList.add('loading-progress-hidden');
        }
      });
    }
  );
});

// Globals
window.navigate = navigate;
window.updateExplorer = updateExplorer;
window.toggleSidebar = () => document.getElementById('sidebar').classList.toggle('collapsed');
