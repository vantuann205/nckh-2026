// ─── APP CONTROLLER ───────────────────────────────────────────────────────────
let CURRENT_PAGE = 'dashboard';
let EXPLORER_SORT = { col: 'speed_kmph', dir: -1 };
let VIOLATIONS_DATA = [];

// ── ROUTER ────────────────────────────────────────────────────────────────────
async function navigate(page, navEl) {
  if (page === CURRENT_PAGE && navEl) return;

  CURRENT_PAGE = page;

  // Update Sidebar Active State
  document.querySelectorAll('.nav-item').forEach(el => el.classList.remove('active'));
  if (navEl && navEl.classList) navEl.classList.add('active');
  else {
    const el = document.querySelector(`[data-page="${page}"]`);
    if (el) el.classList.add('active');
  }

  // Update Breadcrumbs
  const crumbs = {
    dashboard: 'Dashboard / Overview',
    map: 'Dashboard / Traffic Map',
    explorer: 'Data Management / Explorer',
    vehicle: 'Data Management / Analytics',
    alerts: 'Operations / Alerts',
    query: 'Operations / SQL Query',
    monitor: 'System / Monitoring',
  };
  const bcSpan = document.querySelector('.breadcrumbs span');
  if (bcSpan) bcSpan.textContent = crumbs[page] || `Dashboard / ${page}`;

  // Inject Template
  const content = document.getElementById('content');
  const template = PAGES[page];
  if (!template) {
    content.innerHTML = `<div style="padding:40px;color:var(--text2)">Page "${page}" not found</div>`;
    return;
  }

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
      setTimeout(initMap, 100);
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
      renderAlertCharts(VIOLATIONS_DATA);
      renderViolations();
      break;
    case 'monitor':
      renderMonitorCharts();
      renderSparkJobs();
      renderWorkerNodes();
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
        <div style="font-size:24px;font-weight:800; letter-spacing:-0.03em; margin-bottom:4px">Smart Traffic Lakehouse</div>
        <div style="font-size:14px;color:var(--text3)" id="boot-status">Warming up Spark Cluster...</div>
      </div>
      <div style="width:280px;background:#f1f5f9;border-radius:99px;height:8px; overflow:hidden">
        <div id="boot-bar" style="height:100%;border-radius:99px;background:var(--accent);width:0;transition:width .4s ease-out"></div>
      </div>
      <div style="font-size:12px;font-family:var(--mono);color:var(--text3); opacity:0.6" id="boot-rows">Initializing Big Data Engine...</div>
    </div>`;

  if (window.lucide) lucide.createIcons();

  DB.init(
    (pct) => {
      const bar = document.getElementById('boot-bar');
      const info = document.getElementById('boot-rows');
      if (bar) bar.style.width = pct + '%';
      if (info) info.textContent = `Optimizing Data Ingestion: ${pct}%`;
    },
    () => {
      setTimeout(() => navigate('dashboard'), 300);
    }
  );
});

// Real-time Event Listener
window.addEventListener('lakehouse-update', () => {
  console.log('📢 Refreshing current page due to real-time sync...');
  navigate(CURRENT_PAGE);
});

// Globals
window.navigate = navigate;
window.updateExplorer = updateExplorer;
window.toggleSidebar = () => document.getElementById('sidebar').classList.toggle('collapsed');
