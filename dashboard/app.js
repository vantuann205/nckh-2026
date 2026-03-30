// ─── APP CONTROLLER ───────────────────────────────────────────────────────────
let CURRENT_PAGE = 'dashboard';

// ── ROUTER ────────────────────────────────────────────────────────────────────
async function navigate(page, navEl) {
  if (page === CURRENT_PAGE && !navEl) {
    updatePageData(page);
    return;
  }

  CURRENT_PAGE = page;

  // Clear all charts to prevent orphaned Chart.js instances when DOM is replaced
  if (window.chartInstances && window.destroyChart) {
    Object.keys(window.chartInstances).forEach(id => window.destroyChart(id));
  }
  
  // Clear animated tracking to replay text animations on page enter
  window._animatedKPIs = new Map();

  // Update Topbar Active State
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  if (navEl && navEl.classList) {
    navEl.classList.add('active');
  } else {
    document.querySelectorAll('.nav-item').forEach(item => {
      if (item.getAttribute('onclick')?.includes(`'${page}'`)) item.classList.add('active');
    });
  }

  // Inject Template
  const content = document.getElementById('content');
  const template = PAGES[page];
  if (!template) {
    content.innerHTML = `<div style="padding:40px;color:var(--on-surface-variant); font-family:var(--mono)">[404] Resource not found: "${page}"</div>`;
    return;
  }

  content.innerHTML = template();
  content.scrollTop = 0;

  // Update WS badge / Header Status / Chart Badges
  updateWSBadge();
  updateStatusBadges();

  // Resize charts to fix the 0px Canvas Bug when switching display:block tabs
  setTimeout(() => {
    Object.values(window.chartInstances || {}).forEach(c => { if(c && typeof c.resize === 'function') c.resize()});
  }, 100);

  // Post-render setup
  switch (page) {
    case 'dashboard':
      if (window.renderDashboardCharts) renderDashboardCharts();
      break;
    case 'map':
      setTimeout(() => { if (window.initMap) window.initMap(); }, 150);
      break;
    case 'explorer':
      updateExplorer();
      break;
    case 'vehicle':
      if (window.renderVehicleCharts) renderVehicleCharts();
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
    case 'prediction':
      setTimeout(() => { if (window._initPredictionPage) window._initPredictionPage(); }, 100);
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
    if (s === 'congested') return '<span class="badge red">Tắc nghẽn</span>';
    if (s === 'slow') return '<span class="badge yellow">Chậm</span>';
    return '<span class="badge green">Bình thường</span>';
  };

  tbody.innerHTML = roads.map(r => `
    <tr>
      <td class="mono" style="color:var(--primary); font-weight:700">${r.road_id || ''}</td>
      <td><span style="font-weight:800;font-family:var(--font-display);color:${parseFloat(r.avg_speed || 0) < 20 ? 'var(--error)' : parseFloat(r.avg_speed || 0) < 40 ? 'var(--status-congested)' : 'var(--on-surface)'}">${parseFloat(r.avg_speed || 0).toFixed(1)}</span> <span style="font-size:11px;color:var(--on-surface-variant)">km/h</span></td>
      <td style="font-weight:700">${r.vehicle_count || 0}</td>
      <td class="mono" style="font-size:11px">${parseFloat(r.lat || 0).toFixed(4)}</td>
      <td class="mono" style="font-size:11px">${parseFloat(r.lng || 0).toFixed(4)}</td>
      <td>${statusBadge(r.status)}</td>
      <td style="font-size:11px;color:var(--on-surface-variant); font-family:var(--mono)">${r.updated_at ? new Date(r.updated_at).toLocaleTimeString() : '-'}</td>
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
        const speed = parseFloat(r.avg_speed || 0).toFixed(1);
        const status = r.status || 'normal';
        const color = status === 'congested' ? 'var(--error)' : status === 'slow' ? 'var(--status-congested)' : 'var(--tertiary)';
        const bg = status === 'congested' ? 'var(--error-container)' : status === 'slow' ? 'var(--status-congested-bg)' : 'var(--tertiary-container)';
        return `
          <div class="road-card">
            <div class="road-card-header">
              <span class="road-card-id">${r.road_id}</span>
              <span class="road-card-status" style="background:${bg};color:${color}">${status}</span>
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

  if (wsEl) wsEl.innerHTML = DB.connected ? '<span style="color:var(--tertiary)">Live</span>' : '<span style="color:var(--error)">Disconnected</span>';
  if (roadsEl) roadsEl.textContent = (DB.state.roads || []).length;
  if (updateEl) updateEl.textContent = DB.state.lastUpdate ? new Date(DB.state.lastUpdate).toLocaleTimeString() : '-';

  try {
    const health = await DB.fetchHealth();
    if (redisEl && health) redisEl.innerHTML = health.redis?.connected ? '<span style="color:var(--tertiary)">Live</span>' : '<span style="color:var(--error)">Down</span>';
  } catch (e) {
    if (redisEl) redisEl.innerHTML = '<span style="color:var(--error)">Down</span>';
  }
}

// ── WS Status Badge & Header Config ──
function updateWSBadge() {
  const badge = document.getElementById('api-status');
  if (!badge) return;
  const text = badge.querySelector('.status-text');
  
  if (DB.connected) {
    badge.className = 'status-indicator live';
    if (text) text.textContent = 'Live';
  } else {
    badge.className = 'status-indicator disconnected';
    if (text) text.textContent = 'Disconnected';
  }
  updateStatusBadges();
}

// ── Update ALL Chart Badges (LIVE/OFFLINE) ──
function updateStatusBadges() {
  const badges = document.querySelectorAll('.status-sync');
  const connected = DB.connected;
  badges.forEach(b => {
    // Preserve the original mode (ML, API, etc) if it's not the default LIVE
    const mode = b.getAttribute('data-mode') || b.textContent;
    if (!b.getAttribute('data-mode')) b.setAttribute('data-mode', mode);

    if (connected) {
      b.textContent = mode;
      b.classList.remove('offline');
      b.classList.add('live');
    } else {
      b.textContent = 'OFFLINE';
      b.classList.remove('live');
      b.classList.add('offline');
    }
  });
}

// Global Filter Change
window.updateFilters = () => {
  const timeWindow = document.getElementById('time-filter')?.value;
  console.log('Filters updated:', timeWindow);
};

// ── KPI Animation Function ──
window._animatedKPIs = new Map();
window.animateKPI = function(elId, endVal, suffix = '', isFloat = false) {
  const el = document.getElementById(elId);
  if (!el) return;

  const numericEnd = Number(endVal || 0);

  if (window._animatedKPIs.get(elId) === numericEnd) {
     el.innerHTML = isFloat ? `${numericEnd.toFixed(1)}${suffix ? `<span>${suffix}</span>` : ''}` : `${numericEnd.toLocaleString()}${suffix ? `<span>${suffix}</span>` : ''}`;
     return;
  }

  const startVal = window._animatedKPIs.has(elId) ? window._animatedKPIs.get(elId) : 0;
  window._animatedKPIs.set(elId, numericEnd);
  
  const duration = startVal === 0 ? 1200 : 500;
  let startTimestamp = null;
  
  const step = (timestamp) => {
    if (!startTimestamp) startTimestamp = timestamp;
    const progress = Math.min((timestamp - startTimestamp) / duration, 1);
    
    // easeOutExpo
    const ease = progress === 1 ? 1 : 1 - Math.pow(2, -10 * progress);
    const curr = startVal + (numericEnd - startVal) * ease;

    const formattedCurr = isFloat ? curr.toFixed(1) : Math.floor(curr).toLocaleString();
    el.innerHTML = `${formattedCurr}${suffix ? `<span>${suffix}</span>` : ''}`;

    if (progress < 1) {
      window.requestAnimationFrame(step);
    } else {
      const formattedFinal = isFloat ? numericEnd.toFixed(1) : numericEnd.toLocaleString();
      el.innerHTML = `${formattedFinal}${suffix ? `<span>${suffix}</span>` : ''}`;
    }
  };
  window.requestAnimationFrame(step);
};

// ── Real-time Updates ──
window.addEventListener('traffic-update', () => {
  updatePageData(CURRENT_PAGE);
});

// ── Connection status change → refresh toàn bộ trang hiện tại ──
let _prevConnected = null;
window.addEventListener('ws-status', (e) => {
  const connected = e.detail?.connected;
  updateWSBadge();
  updateAllOfflineBanners(connected);

  // Khi chuyển từ offline → online: reload lại trang hiện tại để lấy data mới
  if (_prevConnected === false && connected === true) {
    console.log('🟢 Backend online — refreshing current page:', CURRENT_PAGE);
    setTimeout(() => updatePageData(CURRENT_PAGE), 500);
  }
  _prevConnected = connected;
});

// ── Offline banner: hiển thị trên mọi trang khi backend offline ──
function updateAllOfflineBanners(connected) {
  let banner = document.getElementById('global-offline-banner');
  if (!connected) {
    if (!banner) {
      banner = document.createElement('div');
      banner.id = 'global-offline-banner';
      banner.style.cssText = [
        'position:fixed', 'top:0', 'left:0', 'right:0', 'z-index:9999',
        'background:#ef4444', 'color:#fff', 'text-align:center',
        'padding:10px 16px', 'font-size:13px', 'font-weight:700',
        'letter-spacing:0.04em', 'display:flex', 'align-items:center',
        'justify-content:center', 'gap:10px',
        'box-shadow:0 2px 12px rgba(239,68,68,0.4)',
        'animation:slideDown 0.3s ease'
      ].join(';');
      banner.innerHTML = `
        <span style="font-size:16px">🔴</span>
        <span>BACKEND OFFLINE — Đang chờ kết nối...</span>
        <span style="opacity:0.7;font-size:11px;font-weight:400">Tự động kết nối lại sau mỗi 3 giây</span>
        <div style="width:16px;height:16px;border:2px solid rgba(255,255,255,0.4);border-top-color:#fff;border-radius:50%;animation:spin 0.8s linear infinite;margin-left:4px"></div>
      `;
      document.body.appendChild(banner);
    }
    // Đẩy content xuống để không bị che
    const wrapper = document.querySelector('.main-wrapper');
    if (wrapper) wrapper.style.paddingTop = '44px';
  } else {
    if (banner) {
      banner.style.animation = 'slideUp 0.3s ease forwards';
      setTimeout(() => banner?.remove(), 300);
    }
    const wrapper = document.querySelector('.main-wrapper');
    if (wrapper) wrapper.style.paddingTop = '';
  }
}

async function updatePageData(page) {
  const s = DB.summary || {};
  const connected = DB.connected;
  const hasData = connected && s.total_roads !== undefined && s.total_roads !== null && s.total_roads > 0;

  // Cập nhật offline banner trên mọi trang
  updateAllOfflineBanners(connected);

  if (!hasData) {
    // Show Skeletons trên dashboard
    const kpis = ['kpi-total', 'kpi-speed', 'kpi-vehicles', 'kpi-congested', 'kpi-passengers', 'kpi-fuel', 'kpi-speeding', 'kpi-lowfuel'];
    kpis.forEach(id => {
      const el = document.getElementById(id);
      if(el) el.innerHTML = '<div class="skeleton-box kpi-skeleton"><div class="kpi-value"></div></div>';
    });
  }

  switch (page) {
    case 'dashboard':
      if (hasData) {
        animateKPI('kpi-total', s.total_roads);
        animateKPI('kpi-speed', s.avg_speed, 'km/h', true);
        animateKPI('kpi-vehicles', s.total_vehicles);
        animateKPI('kpi-congested', s.congested_roads);
        animateKPI('kpi-passengers', s.total_passengers);
        animateKPI('kpi-fuel', s.avg_fuel_level, '%', true);
        animateKPI('kpi-speeding', s.speeding_alerts);
        animateKPI('kpi-lowfuel', s.low_fuel_alerts);
      }
      if (window.renderDashboardCharts) renderDashboardCharts();
      break;
    case 'map':
      if (window.renderMapPoints) renderMapPoints();
      break;
    case 'explorer':
      updateExplorer();
      break;
    case 'vehicle':
      if (window.renderVehicleCharts) renderVehicleCharts();
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
    case 'prediction':
      if (connected && window._initPredictionPage) window._initPredictionPage();
      break;
  }

  // Update last update timestamp
  const el = document.getElementById('last-update');
  if (el && DB.state.lastUpdate) {
    el.textContent = `LAST PING: ${new Date(DB.state.lastUpdate).toLocaleTimeString()}`;
  }
}

// ── BOOT ──────────────────────────────────────────────────────────────────────
window.addEventListener('DOMContentLoaded', () => {
  const content = document.getElementById('content');
  content.innerHTML = `
    <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;height:100%;gap:24px; color:var(--on-surface)">
      <div style="width:64px; height:64px; border-radius:16px; background:var(--primary); color:#fff; display:grid; place-items:center; box-shadow: 0 4px 12px rgba(56, 189, 248, 0.4)">
        <span style="font-size:28px; font-weight:900; font-family:var(--font-display)">T</span>
      </div>
      <div style="text-align:center">
        <div style="font-size:24px;font-weight:800;font-family:var(--font-display); letter-spacing:-0.03em; margin-bottom:4px">Realtime Traffic Monitor</div>
        <div style="font-size:14px;color:var(--on-surface-variant); font-family:var(--mono)" id="boot-status">INITIALIZING SYSTEM...</div>
      </div>
      <div style="width:280px;background:var(--surface-container-high);border-radius:99px;height:4px; overflow:hidden">
        <div id="boot-bar" style="height:100%;border-radius:99px;background:var(--primary);width:0;transition:width .4s ease-out"></div>
      </div>
    </div>`;

  DB.init(
    (pct) => {
      const bar = document.getElementById('boot-bar');
      const info = document.getElementById('boot-status');
      if (bar) bar.style.width = pct + '%';
      if (info) info.textContent = pct < 80 ? 'ESTABLISHING PROTOCOL...' : 'CONNECTING STREAM...';
    },
    () => {
      setTimeout(() => {
        navigate('dashboard');
        
        // Hide API Status Badge 3 seconds after the Overview page loads for a cleaner demo
        setTimeout(() => {
          const badge = document.getElementById('api-status');
          if (badge) {
            badge.style.transition = 'opacity 0.6s ease';
            badge.style.opacity = '0';
          }
        }, 3000);
      }, 300);
      
      DB.startLoadingProgressPolling((progress) => {
        const container = document.getElementById('loading-progress-container');
        const fill = document.getElementById('loading-progress-fill');
        const vehiclesCount = document.getElementById('loading-vehicles-count');
        const vehiclesSpeed = document.getElementById('loading-vehicles-speed');
        const eta = document.getElementById('loading-eta');
        
        if (!container) return;
        
        if (progress.status === 'loading') {
          container.classList.remove('loading-progress-hidden');
          if (vehiclesCount) vehiclesCount.textContent = (progress.total_vehicles || 0).toLocaleString();
          if (vehiclesSpeed) vehiclesSpeed.textContent = (progress.vehicles_per_sec || 0).toLocaleString();
          if (eta) eta.textContent = (progress.estimated_remaining_sec || 0);
          
          if (progress.files && Object.keys(progress.files).length > 0) {
            const totalToProcess = Object.values(progress.files).reduce((sum, f) => sum + (f.total || 0), 0);
            const totalProcessed = Object.values(progress.files).reduce((sum, f) => sum + (f.processed || 0), 0);
            const progressPercent = totalToProcess > 0 ? Math.round((totalProcessed / totalToProcess) * 100) : 0;
            if (fill) fill.style.width = progressPercent + '%';
          }
        } else {
          container.classList.add('loading-progress-hidden');
        }
      });
    }
  );
});

// Globals
window.navigate = navigate;
window.updateExplorer = updateExplorer;
